# reth 2.3.x vs 1.8.x — State Root / Trie 改进

> 对比范围: 上游 reth `branch-1.8.3` (4219741510) → `branch-2.3.0` (9384bc53d8)。
> `git log --oneline branch-1.8.3..branch-2.3.0 -- crates/trie` = 302 commits。
> 目的: 供 gravity-reth 选择性 port。gravity-reth 用**自研 nested trie**(node-per-DB-row / geth 风格)计算 state root,
> 与上游 **sparse-trie / HashBuilder-over-sorted-leaves** 是**互斥的两套引擎**;两者的 on-disk 格式与算法都不兼容,
> 主引擎只能二选一。但上游有若干**可单独借鉴的技巧 / 可移植的 API / 规范**,本文逐条区分。

---

## 概述

### gravity nested-trie(基线,用于对照)

gravity 的 state root 引擎与上游完全不同,核心是 **每个 MPT 节点单独存成一行 DB**(geth 风格):

- 表 `AccountsTrieV2`:`StoredNibbles`(完整 path)→ `StoredNode`(自定义紧凑序列化的 `Node`)。
- 表 `StoragesTrieV2`:dup-sort,key = `hashed_address`,subkey = `StoredNibblesSubKey`(path),value = `StorageNodeEntry{path, node}`。
- 节点类型 `Node`(`crates/trie/common/src/nested_trie/node.rs`):`FullNode`(branch,17 children)/ `ShortNode`(extension 或 leaf)/ `ValueNode` / `HashNode`(指向另一行 DB 节点的 RLP 索引)。
- **懒加载**:`Trie::new` 只读 root,children 以 `HashNode` 占位,访问时按 path 从 reader(DB + 可选 `PersistBlockCache` overlay)按需读入。
- **并行更新** `parallel_update`(`crates/trie/common/src/nested_trie/trie.rs`):按首 nibble 16 路分区,递归下探到 `trie_parallel_levels` 层,每层 `rayon::scope` per-child 并行;叶子层顺序 insert/delete,子树 `hash()` 自底向上、`NodeFlag.rlp` 缓存哈希、`dirty` 标脏。
- **哈希** `hash()`:顶层 16 个 child `thread::scope` 并行 `build_hash`,再算 root。
- **输出** `TrieUpdatesV2 { account_nodes: HashMap, removed_nodes: HashSet, storage_tries: B256Map<StorageTrieUpdatesV2{is_deleted, storage_nodes, removed_nodes}> }`(node-diff)。写库 `write_trie_updatesv2`(`provider.rs`)用 `thread::scope` 并行写 account/storage 两棵树。
- 已有 `Trie::get_proof`(从 nested trie 直接产 `Vec<TrieNode>`)与 `NestedStateRoot::multiproof`(历史块 via reverted_state,部分 `todo!()`);已支持 nested hash 的 `eth_getProof`(#237)。
- 调用点:merkle stage(`crates/stages/stages/src/stages/merkle.rs`)与 engine 侧 `block_view_storage`(`crates/gravity-storage/src/block_view_storage/mod.rs`)直接 `NestedStateRoot::new(tx, cache).calculate(&hashed_state)`。gravity 的链关键路径(`pipe-exec-layer-ext-v2`)**不使用**上游的 sparse-trie payload processor / prewarm / state_root_task。

### 上游 reth 2.3.x 的 state-root 主线

上游这一年在 trie 上的 302 个 commit 大致可归为几条主线:

1. **Sparse-trie 主引擎重构**:`SerialSparseTrie` 删除 → `ParallelSparseTrie`(upper + 256 lower subtrie 并行)→ **`ArenaParallelSparseTrie`**(slotmap arena,default)。**这是上游"逐块增量验证"的主 state-root 引擎**,与 gravity nested-trie 二选一。
2. **Proof V2 重写**:全新 **stack-based「从叶子直接算 trie 节点」** 算法(`ProofCalculator`),一趟深度优先 post-order 遍历排序叶子;引入 `ProofV2Target{key_nibbles,min_len}` / `MultiProofTargetsV2` / `TrieNodeV2`(ext+branch 合一)/ `DecodedMultiProofV2`;2.3 里**删除了 legacy proof,V2-only**。
3. **Sparse trie 当缓存**:revealed trie 跨块保留 + LFU 热点保留,是上游最大的 state-root 提速来源。
4. **Proof/state-root worker 池化**:长驻 worker 各自持有 DB tx + cursor + calculator,专用 rayon 池,后台初始化,cacheline 优化。
5. **TrieWitness 走 proof v2**;删 `DatabaseTrieWitness` / `MaskedTrieCursorFactory` / `TrieNodeProvider`;stateless witness 对齐 draft spec(`ExecutionWitnessMode::Canonical`)。
6. **reorg/历史 state root 重构**:in-memory trie changesets(不再落盘)+ `OverlayStateProvider` + `StateTrieOverlayManager`;changesets 移入 static file;v2 把规范*状态*表移到 hashed(plain 表运行时不写、schema 仍在),changeset 仍明文键,靠 slot preimage DB 在 wipe 时维持明文。
7. **一堆输入/写入/cursor 微优化**:sorted trie writes、`from_reverts`、`PackedStoredNibbles`(65→33B)、cursor overlay 命中跳过 DB seek、并行 merge overlay 等。
8. **正交**(与 state root 无直接关系):ordered root builder(tx/receipt root)、Block Access List(BAL)通知流、state hook。

**关键结论**:上游 sparse-trie(增量引擎)/ HashBuilder-over-sorted-leaves(全量引擎)/ gravity nested-trie(node-per-row 懒加载)是**三种互斥的 root 计算设计**。上游 2.3 的算法主线(主题 1/2/3/5)**不能直接搬进 nested-trie**;真正对 gravity 有价值的是主题 4/6/7 里的**基础设施、写入顺序、cursor overlay、witness/proof 输出格式、RPC 兼容性**这些**可拆出来的技巧**。

---

## 主题详解

### 1. Sparse-trie 主引擎重构:`ArenaParallelSparseTrie`(互斥主引擎)

- **上游做了什么**:sparse trie 是**内存中、只部分物化**的 MPT——只保留重算 root 所需的节点,其余是 blinded(仅存 child 的 32-byte hash)。节点来自 multiproof 的 reveal,而非 DB 行。
  - 节点模型:`EmptyRoot / Leaf{key,value} / Branch{state_mask, children, short_key(=内联的 extension), branch_masks}`;状态机 `Revealed / Cached{rlp} / Dirty`。
  - **两级切分**(`UPPER_TRIE_MAX_DEPTH=2`):path<2 nibble 属 upper trie;path≥2 按前 2 nibble 分到 **256 个互不相干的 lower subtrie**,可 rayon 完全并行 reveal/update/hash/prune。哈希顺序:并行哈希脏 lower subtrie → 串行哈希 upper → root。只重算「脏叶子到 root 的脊线」。
  - **`ArenaParallelSparseTrie`(#22381, `792c8f2558`,2.3 default)**:每个 subtrie 用 `slotmap::SlotMap<_, ArenaSparseNode>` 做 arena,child 用 **直接 slot index** 引用(不再每步重算 `Nibbles` path 再去 `HashMap` 探测)→ 遍历变成连续内存里的指针追逐,cache locality 大幅提升、分配大幅减少;branch 的 children 按 `state_mask` popcount **稠密存**在 `SmallVec<[_;4]>`;`compact_arena` BFS 把存活节点按 parent-before-child 拷进新 slab,自顶向下哈希顺序访存。
  - 配套删繁就简:删 `SerialSparseTrie`(#21808)、`sparse-parallel` crate 并入 `sparse`(#21808)、`ParallelSparseTrie` 成唯一实现并弃用配置开关(#21435)、`SparseTrieExt` 并入 `SparseTrie` trait(#22035)。
- **关键 PR / commit**:`792c8f2558`(#22381)、`c727c61101`(#21808)、`f9ec2fafa0`(#21435)、`dec1cad318`(#22035)。
- **涉及文件**(upstream `branch-2.3.0`):`crates/trie/sparse/src/arena/{mod,nodes,cursor,branch_child_idx}.rs`、`crates/trie/sparse/src/{parallel,lower,trie,traits,state}.rs`。
- **与 gravity nested-trie 的关系**:**互斥主引擎**。sparse trie 需要「先有 proof 才能 reveal 节点」+ 跨块缓存,是完全不同的一套。gravity 的 node-per-row 懒加载 + 16 路 nibble 并行是**第三种设计**,已经天然拥有 sparse trie 想要的「只碰变更子树」特性(通过 node-diff),不需要 reveal-from-proof。
- **是否建议 port**:**否(强)**。这是整套替代引擎,搬进来等于放弃 nested-trie。**可借鉴的抽象点**(弱):arena/slotmap 存节点 + child 用 index 引用、稠密 children、按前 2 nibble 静态分 subtrie 做并行——但 gravity 已用 `Box<Node>` 树 + 16 路递归并行达成类似目标,收益有限、改造伤筋动骨。难度:高。会与 on-disk `StoredNode` 格式无关(纯内存),但与 gravity 现有并行算法冲突。

### 2. Proof V2 重写:stack-based「从叶子算节点」(互斥主引擎 + 可借鉴 API)

- **上游做了什么**:全新 `ProofCalculator`,**一趟深度优先 post-order 遍历 keccak-sorted 的叶子**(`HashedAccounts`/`HashedStorages` 天然按 hash 排序),边走边把已完成子树封成节点:
  - 两个栈:`branch_stack`(在建的 branch,root→深)+ `child_stack`(所有 branch 的 children 扁平化)。`branch_path` = 最深 branch 的 path。
  - `push_leaf(key)`:算 `common_prefix_length(branch_path,key)`;比 `branch_path` 短 → 当前 branch 不会再有新 child(key 只增),`pop_branch` 收尾并回溯父 branch;否则叶子归当前 branch,若该 nibble 已占用则 `push_new_branch` 按公共前缀劈开旧 child(公共前缀成为新 branch 的 parent-extension)。
  - `pop_branch` = **把一个子树精确哈希一次**:排干该 branch 的 children,各自 RLP→`RlpNode`,整体作为父 branch 的一个 `Branch` child 压回。因 key 有序,**只有栈顶最后一个 child 还可能变**,更早的都已 commit 成 `RlpNode` → 每个节点只终结一次,单趟 O(n),无回访。extension 隐式:与 branch 合成一个 `BranchNodeV2` 输出(#22021)。
  - **缓存 branch 短路**(`next_uncached_key_range`,#20075):同时走 trie cursor(存 `BranchNodeCompact`,带 `tree_mask/hash_mask`),若某 child 的 hash_mask 命中、非 target、path 不在 prefix_set(脏集)→ 直接嵌入缓存 hash,**整段子树跳过不下探叶子**。
  - **retain 规则**:节点 path 是某 target `key_nibbles` 的前缀且 `path.len()>=min_len` 才保留成完整节点,否则只留 `RlpNode` hash;`min_len` 支持 **partial proof**(#20336)。`prefix_set` 决定缓存 hash 是否失效(#22946)。
  - **异步/延迟 value 编码**(#20873/#21197):account 叶子的 RLP 需要先算出 storage root,故 account value 编码延迟到 `pop_branch` 时;`AsyncAccountValueEncoder` 预先把 target 账户的 storage-root 任务派给 storage worker 池(结果进 `DashMap` 缓存),account 遍历与 storage-root 计算 **pipeline 重叠**。
  - **2.3 已 V2-only**:`6f9a3242ef`(#22270)删除 legacy proof 全部代码。
- **关键 PR / commit**:`c57792cff4`(#19687 skeleton)、`b72bb6790a`(#19863 核心算法)、`7cfb19c98e`(#21196 reveal/target)、`b79c58d835`(#20336 partial)、`a9e36923e1`(#20075 cached branch)、`f85fcba872`+`2ac7d719f3`(#21214/#21316 account proof)、`3667d3b5aa`(#20873)、`346cc0da71`(#21197)、`117b212e2e`(#22021)、`5e744326a4`(#22946)、`6f9a3242ef`(#22270)。
- **涉及文件**:`crates/trie/trie/src/proof_v2/{mod,node,value,target}.rs`、`crates/trie/trie/src/trie_cursor/depth_first.rs`、`crates/trie/common/src/{target_v2,trie_node_v2,proofs}.rs`、`crates/trie/parallel/src/{value_encoder,proof_task,targets_v2}.rs`。
- **与 gravity nested-trie 的关系**:算法本体是 **HashBuilder 血统的替代引擎**(基于「排序叶子 + cursor」),与 nested-trie 懒加载互斥;但**类型系统 / 输出格式是可移植 API**:`ProofV2Target{key_nibbles,min_len}`、`MultiProofV2`、`TrieNodeV2`(ext+branch 合一)、`DecodedMultiProofV2`——任何引擎(含 nested-trie)都能产出/消费这套结构来表达 partial proof、喂 witness。
- **是否建议 port**:引擎本体 **否(强)**;**proof v2 的 target/结果类型**作为对外 proof/witness API **可弱建议对齐**(利于跨客户端 proof 互操作、stateless),难度中,不触碰 on-disk 格式。gravity 若要增强 `NestedStateRoot::multiproof`(目前含 `todo!()`),可参考 `min_len` 语义做 partial/多目标证明。

### 3. Sparse trie 当缓存 + LFU 热点保留(互斥主引擎技巧)

- **上游做了什么**:上游最大的 state-root 提速——**revealed sparse trie 跨块保留复用**,已在内存的 path 下一块不必再取 proof(#21583)。
  - 保留(#21534):`SharedPreservedSparseTrie`;算完 root 后存 `Anchored{trie, state_root}` 或 `Cleared{trie}`(清数据留分配);下一 payload `into_trie_for(parent_state_root)`:若 == anchor 直接复用(链尖延续,最热),否则 clear 但留分配。#23246 让 payload builder 也共用这条 pipeline。
  - 命中/未命中:block 的叶子变更喂 `update_leaves`,已 reveal 的叶子=命中(从 map 移除);碰到 blinded=未命中,触发 `proof_required_fn(key,min_len)` 去并行 worker 取 proof,reveal 后重试。整块无状态变更时短路返回缓存 root。
  - **LFU 剪枝**(#22766):每块算完把 trie 剪到「热 path」。`BucketedLfu`(按频率 1..=255 分桶,O(1) touch/evict)记录热账户/热 slot;`prune(max_hot_slots,max_hot_accounts)` 并行(`rayon::join`)剪 account/storage 树,非热子树塌回 blinded stub,零热 slot 的 storage 树整棵驱逐。
  - fused prune+compact(#23124):单趟 BFS 只把保留节点拷进新 slab、边界 blind 掉,顺带精确累加 `memory_size`,免二次 compaction。
- **关键 PR / commit**:`19bf580f93`(#21583)、`e1bc6d0f08`(#21534)、`29bab063b7`(#23246)、`e6e637a265`(#22766)、`3c63fb6b1f`(#23124)、`93d546a36d`(#21089)。
- **涉及文件**:`crates/trie/sparse/src/{state,lfu}.rs`、`crates/engine/tree/src/tree/payload_processor/{sparse_trie,preserved_sparse_trie,mod}.rs`。
- **与 gravity nested-trie 的关系**:这是 sparse-trie 独有的「reveal-from-proof + 跨块内存缓存」玩法;gravity 用 `PersistBlockCache`(DB 前的 node overlay,含 tombstone `Some(None)`)达成**类似但不同**的缓存目的——gravity 缓存的是 **DB 节点行**(避免重读磁盘),不是 revealed sparse 结构。
- **是否建议 port**:**否(强)**引擎本体。**可借鉴技巧**(弱):`BucketedLfu` 式 O(1) 热点保留 + 有界内存,可用于给 `PersistBlockCache` 做**带 LFU 的容量上限**(目前 cache 逐块写入,若无界会涨内存);难度低-中,与 nested-trie 算法正交。

### 4. Proof/state-root worker 池化 + cursor overlay 微优化(可借鉴基础设施)

- **上游做了什么**:
  - **worker 池**(#18901/#18887/#18934/#19178/#19203):从「每个 proof 一次 `spawn_blocking` + 每任务重开 tx/cursor」改成 **长驻 storage/account worker 池**,每个 worker 在 recv-loop **之前**开好自己的 DB read-tx + 长驻 trie/hashed cursor + `proof_v2` calculator,**跨任务复用**(摊薄 MDBX tx/cursor 建立成本、cursor 保持热定位)。job 走 crossbeam channel,带 per-job result sender 直接回传,去掉 router / pending queue。account worker 持 `storage_work_tx` 向 storage 池 fan-out;`cached_storage_roots: DashMap<B256,B256>` 复用 storage root。
  - **`WorkerPool` + per-thread `Worker`**(#22154):`thread_local!` 存类型擦除的可复用状态,`broadcast(n,f)` 初始化 n 个线程,支持复用已有分配再 init。
  - **专用 rayon 池**(#22051):proof worker 移到 `proof-strg-*/proof-acct-*/prewarm-*` 专用命名池,与执行/prewarm 隔离,避免争抢。
  - **后台初始化**(#19012):worker 的 tx/cursor 在后台线程里建,不阻塞关键路径。
  - **cacheline 优化**(#23321):worker 可用性从共享 `AtomicUsize`(所有 worker RMW 同一 cacheline)改成 `Vec<CachePadded<AtomicBool>>`(每 worker 独占 cacheline,仅本 worker relaxed store,dispatcher 只读),`has_multiple_idle()` 数到 2 即返回。
  - **小块降配**(#22074):`tx_count<=30` 时 worker 数减半;`<=5` 跳过 prewarm。
  - **cursor/overlay 微优化**:
    - **overlay 精确命中跳过 DB seek**(#23559 `345fbbbfdb`、#24294 `75c327ee56`):in-memory + DB 合并的 cursor,`seek(key)` 先查 overlay,精确命中即返回并**跳过 DB B-tree seek**(仅标记 DB cursor `NeedsPosition`),tombstone `(key,None)`=删除。
    - **ForwardInMemoryCursor 二分**(#21049)+ **fast-path 前向 seek**(#24442):对已排序内存切片,长跳用 `partition_point` O(log n);当前项已满足则立即返回。
    - **并行 merge overlay**(#21473):`merge_ancestors_into_overlay` 用 `rayon::join` 并行 k-way merge(state / trie-updates 两路),`merge_slice` 免迭代器封装开销。
    - **storage proof 按字典序派发**(#21213/#21684):按 address 排序后派发,匹配 account 遍历的消费顺序,减少 head-of-line blocking + cursor 前向局部性。
- **关键 PR / commit**:`e0b7a86313`、`397a30defb`、`11c9949add`、`2eec519bf9`、`ff3a854326`、`eb9b08c696`、`b58827ec2d`、`e4ec836a46`、`1bc07fad8e`、`0720dcd379`、`75c327ee56`、`345fbbbfdb`、`a9bd38a43e`、`7934294988`、`f74e594292`。
- **涉及文件**:`crates/trie/parallel/src/proof_task.rs`、`crates/tasks/src/{pool,runtime}.rs`、`crates/trie/trie/src/forward_cursor.rs`、`crates/trie/trie/src/trie_cursor/in_memory.rs`、`crates/trie/trie/src/hashed_cursor/post_state.rs`、`crates/chain-state/src/deferred_trie.rs`、`crates/engine/tree/src/tree/payload_processor/{multiproof,mod}.rs`。
- **与 gravity nested-trie 的关系**:**基础设施大多与引擎无关、可借鉴**。gravity 现在 `calculate_and_proof` 每次都在 rayon::scope 里 `cursor_dup_read` **临时开 cursor**;池化「长驻 worker 各自持 tx + `AccountsTrieV2`/`StoragesTrieV2` cursor,跨块复用」可直接套到 16 路 nibble 分区上。`cached_storage_roots` 映射到 account→storage-root 缓存也天然契合。cacheline 优化、小块降配是通用并发技巧。overlay 精确命中跳过 DB seek 的**原则** gravity 的 `PersistBlockCache` 点查已天然具备(命中即不读 DB),但那套 `DbCursorState` 状态机是给「合并两个有序 cursor」用的,点查不需要。
- **是否建议 port**:**建议(中-强,分项)**。
  - overlay 命中即跳过 DB 读 + tombstone 语义:gravity 已有,**确认并固化**即可(与你们记忆里的 trie-node tombstone-hole 修复一致)。
  - per-worker `CachePadded` 可用性标志、小块降配、cursor 复用/池化:**可移植**,难度低-中,纯性能,不碰格式。
  - 字典序派发:若 gravity 把 16 路结果**按 key 顺序**合并写 DB,可减少 B-tree 写抖动(见主题 6)。

### 5. TrieWitness 走 proof v2;删 `MaskedTrieCursorFactory`/`DatabaseTrieWitness`/`TrieNodeProvider`(互斥内部 + witness 格式可移植)

- **上游做了什么**:
  - **witness 输出形状不变**:`B256Map<Bytes>` = `keccak256(rlp_node) → rlp_node`,即 geth 风格「按 hash 索引的标准 MPT 节点集合」。
  - **生成路径换成 proof v2**(#22922):对触及的账户/slot 构 `MultiProofTargetsV2` → `Proof::multiproof_v2` → `record_multiproof_nodes` + `SparseStateTrie::reveal_decoded_multiproof_v2` → 在 sparse trie 上 replay `update_leaves`,碰 blinded 就用 callback 追加新 target 再取一轮 proof,循环到无新 target。取代了旧的 `mpsc` + `WitnessTrieNodeProvider` 拦截每次 blinded 读取。
  - **`record_witness_node` 拆节点**:`TrieNodeV2::Branch` 若带非空 `key`(ext+branch 合一),写**两条** witness(extension 的 RLP + 裸 branch 的 RLP),即把合并节点还原成普通 leaf/ext/branch —— 正是 geth 风格 per-node store 期望的。
  - **`MaskedTrieCursorFactory`**(#22564 引入、#22922 删除):曾用来把 DB `BranchNodeCompact` 里「本块已变更 child」的缓存 hash 置空(unset hash_mask、丢 hashes、hash+tree mask 皆空则跳过整节点),强制重算;proof v2 直接吃 prefix_set 自己重算,故此 wrapper 冗余被删。`DatabaseTrieWitness` trait(#22564)、`TrieNodeProvider`(#23658)一并删除。
- **关键 PR / commit**:`bab6c3fe0f`(#22922)、`b2eb061fe2`(#22564)、`3edb271183`(#23658)、`83620dae57`(#22703)、`c6b17848dd`(#20965)、`928bf37297`(#21352)。
- **涉及文件**:`crates/trie/trie/src/witness.rs`、`crates/trie/common/src/{target_v2,trie_node_v2,proofs}.rs`;已删除:`crates/trie/trie/src/trie_cursor/masked.rs`、`crates/trie/db/src/witness.rs`。
- **与 gravity nested-trie 的关系**:内部(sparse trie + proof-v2 + refetch loop)是**互斥引擎**;但 **witness 的扁平 `hash→RLP 节点` 格式是可移植 API**——gravity 的 node-per-row store 每一行本就是一个这样的节点,能天然产出/消费该 witness。`MaskedTrieCursorFactory` 对 gravity **正交**(gravity 懒加载、没有 `BranchNodeCompact` 缓存 hash 要 mask)。
- **是否建议 port**:witness **格式对齐**建议(弱-中),难度低(序列化边界),让 gravity 能产标准 `debug_executionWitness`;`MaskedTrieCursorFactory`/`TrieNodeProvider` **无需 port**。

### 6. Sorted trie writes(`TrieUpdatesSorted` / `from_reverts` / `clone_into_sorted`)(可移植技巧,直接适用)

- **上游做了什么**:`TrieUpdatesSorted` = 已排序 `Vec`(vs `TrieUpdates` 的 HashMap+HashSet)。`write_trie_updates_sorted` 按升序 key 走 `cursor_write` upsert/delete → MDBX B-tree cursor 基本**前向移动(近似 append)**,大幅减少 page split / 随机 seek / 再平衡。默认 `write_trie_updates(TrieUpdates)` 也是先 `into_sorted()` 再走 sorted 路径——**排序是必需的**,预排序只是把成本前移到可并行处。
  - `clone_into_sorted`(#20784):不消费、不 clone HashMap 元数据地产出 sorted 视图。
  - `HashedPostStateSorted::from_reverts`(#20047):直接从 changeset 迭代器(`StorageRevertsIter` 合并 per-slot reverts + wiped)产出**已排序**的 reverted hashed-state,免 post-sort,直接喂 prefix-set / overlay cursor。
  - `TrieUpdatesSorted`/`HashedPostStateSorted` 也直接进 ExEx 通知(#20333)。
- **关键 PR / commit**:`485eb2e8d5`(#20784)、`e0a6f54b42`(#20047)、`d489f80f6b`(#20333)、`4673d77c03`(#20866 ChunkedHashedPostState 排序优化)、`05b3a8668c`(#20653)。
- **涉及文件**:`crates/trie/common/src/updates.rs`、`crates/trie/common/src/hashed_state.rs`、`crates/trie/db/src/state.rs`、`crates/storage/provider/src/changesets_utils/state_reverts.rs`、`crates/storage/provider/src/providers/database/provider.rs`。
- **与 gravity nested-trie 的关系**:**直接适用的可移植技巧**。gravity 的 `TrieUpdatesV2` 是 `HashMap<Nibbles,Node>` + `HashSet<Nibbles>`(**未排序**),`write_trie_updatesv2` 按 HashMap/HashSet 随机顺序 upsert/delete `AccountsTrieV2`/`StoragesTrieV2` → cursor 乱序写、page split 多。gravity 的 16 路 nibble 分区**本就按首 nibble 分桶**,桶内排序即得全局有序,几乎零额外成本。
- **是否建议 port**:**建议(强)**。改造点:写库前把 account 节点、每棵 storage 树的节点按 path 排序(或让 `TrieOutput` 直接产 sorted vec),`write_trie_updatesv2` 用有序 cursor upsert。难度低,纯性能,**不改 on-disk 格式**,收益直接(写放大↓)。`from_reverts` 产已排序 reverted `HashedPostState` 也可用在 gravity 的历史 root 路径(`read_hashed_state` 目前用 HashMap,后续送去 `calculate` 前需要的分区其实不要求全序,但若要写库仍受益)。

### 7. Nibble/key 打包:`PackedStoredNibbles` 65→33 字节(gravity 已有等价实现,收敛)

- **上游做了什么**(#22158 `80bf5532ac`):为其 storage-v2 表新增 `PackedStoredNibbles`/`PackedStoredNibblesSubKey`,**固定 33 字节** = 32 字节 packed(2 nibble/byte,右填零)+ 1 字节 nibble 数;右填零**保持 memcmp 顺序**。相比旧 `StoredNibblesSubKey`(65 字节:1 nibble/byte + 长度)DB key 减半。`PackedAccountsTrie`/`PackedStoragesTrie` 是同一 MDBX 表的 type-level view。配套 `8d97ab63c6`(#22314)用栈上 `[u8;65]` 免堆分配。
  - 注意:上游 `AccountsTrie`/`StoragesTrie` 存的是 **`BranchNodeCompact`**(一行=一个 16 路 branch 的 mask+hash 集合),不是 per-MPT-node。
- **关键 PR / commit**:`80bf5532ac`(#22158)、`8d97ab63c6`(#22314)。
- **涉及文件**:`crates/trie/common/src/nibbles.rs`、`crates/storage/db-api/src/tables/mod.rs`。
- **与 gravity nested-trie 的关系**(需分开看两个 key):
  - **`StoragesTrieV2` 的 dup subkey `StoredNibblesSubKey`:gravity 已变长 packed**(HEAD `to_compact`:1 字节 `len+1` + `pack()` 的 `ceil(len/2)` 字节,最坏 ~33B、平均更短),`StorageNodeEntry` 用 `SubkeyContainedValue` 支持变长 dup subkey。即在 storage 侧 gravity **已达到甚至优于** #22158 的打包密度(变长 < 上游固定 33)。**此处 merge 冲突**:`crates/trie/common/src/nibbles.rs` HEAD=gravity 变长 `pack()` 版,v2.3.0 侧=固定 65 字节 1-nibble/byte 版;应保留 gravity HEAD 侧(与共享的 `from_compact` 变长解码一致)。
  - **`AccountsTrieV2` 的 key `StoredNibbles`:gravity 目前 *未* packed**——`to_compact` 是 `for i in self.0.iter() { put_u8(i) }`,**1 nibble/byte**,深账户路径最长可达 64 字节。这里上游的打包思想是**真实可移植收益**:packed 2 nibble/byte 可把较深内部/叶子节点的 key 约减半。注意 packing 会丢失奇偶信息,变长 key(非 dup)需额外记 nibble 数(上游 `PackedStoredNibbles` 用固定 32B + 1B 计数;gravity 变长表可用「1B 计数 + `pack()`」),属 on-disk 格式变更、需离线 row 重写迁移(不能原地重解释)。
- **是否建议 port**:
  - storage subkey:**否/收敛**(gravity 已等价/更优)。
  - **account key `StoredNibbles`:弱建议 port**——按 storage subkey 同款做变长 packed(1B 计数 + `pack()`),缩小 `AccountsTrieV2` key 字节、page 更密。属 on-disk 格式变更,需迁移方案(offline rewrite),难度中;非热路径正确性风险低但需回滚预案。**行动项**:先解决 `nibbles.rs` 合并冲突(保留 gravity 变长版),再评估 account key 打包。

### 8. In-memory trie changesets + `OverlayStateProvider`(reorg/历史;互斥主引擎 + 架构可借鉴)

- **上游做了什么**:
  - **trie changeset** = 一个块覆盖掉的**旧 trie 节点值**(`TrieUpdatesSorted`,节点是 `BranchNodeCompact` diff;`Some`=块前旧值、`None`=块前不存在→revert 时删)。用于**不从创世重算**地得到祖先/历史块的 state root。
  - **#20997 大转向**:早先(#19068)把 trie changeset **落盘**到 `AccountsTrieChangeSets`/`StoragesTrieChangeSets` 两张 MDBX 表;#20997 **删除落盘**(两表进 `ORPHAN_TABLES` 丢弃),改为**纯内存**按需算 + `ChangesetCache`(块 hash 索引、持久化后显式驱逐、`PendingChangeset` 等待去重)。
  - **应用/回滚**:`InMemoryTrieCursor`(#19277 重写 + proptest)把有序内存 overlay 合并到 DB trie cursor 之上(overlay 恒赢,`None`=删)。整段块范围 newest→oldest 累加(`extend_ref_and_sort`,旧值赢)得到 as-of block N 的累积 trie 状态。
  - **`OverlayStateProvider`**(#18822):包 DB provider + `Arc<TrieUpdatesSorted>` + `Arc<HashedPostStateSorted>`,实现 `TrieCursorFactory`/`HashedCursorFactory`,让 state-root/proof/witness 通过同一 cursor 抽象读到**虚拟的 reverted+overlay 状态**,不落盘。`OverlayBuilder` 定 anchor(`Finish` checkpoint)、校验 revert 范围 vs prune checkpoint(`InsufficientChangesets`,#19207)。
  - **`StateTrieOverlayManager`**(≈ merge_ancestors):持所有已执行未持久化块,`rayon::join` 并行 merge `TrieUpdatesSorted`/`HashedPostStateSorted` 成扁平 overlay,`(anchor,tip)` 缓存 + `ExtendCached` 增量。#23657 直接传 `ExecutedBlocks` 减少要查的 reverts;#23667 历史/RPC 路径改用 overlay builder。
- **关键 PR / commit**:`a74cb9cbc3`(#20997)、`be94d0d393`(#19068)、`7e59141c4b`(#19277)、`d276ce5758`(#18822)、`35b28ea543`(#19207)、`6377a957c1`(#23667)、`344037d04e`(#23657)、`6659080dc0`(#19383)、`da12451c9c`(#21323)。
- **涉及文件**:`crates/trie/trie/src/changesets.rs`、`crates/trie/db/src/changesets.rs`、`crates/trie/trie/src/trie_cursor/in_memory.rs`、`crates/storage/provider/src/providers/state/overlay.rs`、`crates/chain-state/src/state_trie_overlay.rs`。
- **与 gravity nested-trie 的关系**:**互斥主引擎,基本正交**。上游 trie changeset 的载荷是 `BranchNodeCompact` diff,绑定其一行一 branch 布局,**无法直接套 gravity per-node 行**。且 gravity 已经用「读 `AccountChangeSets`/`StorageChangeSets` 反推 reverted `HashedPostState`,再懒加载 + rayon 重算受影响子树」达成历史/reorg root(见 `read_hashed_state` + `NestedStateRoot::multiproof`),**不需要 node 级 trie changeset**。
  - **注意**:当前 merge 树里 `crates/trie/db/src/changesets.rs`(`compute_trie_changesets`/`ChangesetCache`)是**合并带进来的上游代码**,跑在上游 `BranchNodeCompact` 表上,对 gravity nested trie **不适用/接近死代码**,合并收尾时需确认是否保留。
- **是否建议 port**:node 级 trie changeset **否**。**架构可弱借鉴**:`ChangesetCache` 形状(hash 索引 + 持久化后驱逐 + `PendingChangeset` 去重)、overlay 用 `HashedPostStateSorted`(gravity 已能产)包裹读路径而不物化——若 gravity 未来要在内存里叠多块做历史 root,这套 anchor/范围校验(`InsufficientChangesets`)值得参考。难度中,不碰 on-disk 格式。

### 9. Static-file changesets + slot preimage DB + hashed 规范化(部分可移植 / 正交)

- **上游做了什么**:
  - **`StaticFileSegment::AccountChangeSets`/`StorageChangeSets`**(#18882/#20896):把 plain-state changeset 从 MDBX 表移入 append-only static file + 16 字节偏移 sidecar(`[offset u64][num_changes u64]`/块,O(1) 随机访问、崩溃自愈),压缩 + mmap 顺序读、免 B-tree 写放大,正合 `from_reverts` 扫块范围的访问模式。
  - **hashed state 成规范表示 v2**(#21115):改的是**规范*状态*表**——v2 下当前状态走 `HashedAccounts`/`HashedStorages`,运行时(`use_hashed_state()` 门控)跳过写 `PlainAccountState`/`PlainStorageState`;**这两张 plain 表仍在 `tables!` schema 里**(v1/兼容用)。**changeset 表(`AccountChangeSets`/`StorageChangeSets`)始终是明文键**(account subkey=`Address`、storage key=`BlockNumberAddress`+明文 slot),读时才 `keccak256(address)` 查 hashed 表。
  - **slot preimage DB**(#22379):目的是**让 changeset 维持明文 slot**(不是让它变 hashed)。v2 下当前 storage 只在 `HashedStorages`(仅 hashed slot),pre-Cancun SELFDESTRUCT wipe revert 要枚举账户全部旧 slot 的**明文**键——若从 `HashedStorages` walk 只有 hashed,故建独立 MDBX(`db/preimage/mdbx.dat`,`keccak256(slot)→plain slot`),`inject_plain_wipe_slots` 从 bundle 收集 preimage 再把 wipe reverts 改写成明文 slot(源码注释:*"keeping all changeset keys in plain format"*),append-only、不 unwind。#21115 的临时 `StorageSlotKey::{Plain,Hashed}` 枚举被 #22379 删除(`branch-2.3.0` 已无)。
- **关键 PR / commit**:`eed34254f5`(#18882)、`ebe2ca1366`(#20896)、`121160d248`(#21115)、`815037e27d`(#22379)。
- **涉及文件**:`crates/static-file/types/src/{segment,changeset_offsets}.rs`、`crates/stages/stages/src/stages/execution/slot_preimages.rs`。
- **与 gravity nested-trie 的关系**:
  - static-file changesets **正交且可移植**:与 trie 节点格式无关,只是 gravity 已消费的 `AccountChangeSets`/`StorageChangeSets` **存哪里**;换成 static file 能加速 `read_hashed_state` 的范围扫描 + 缩小热 MDBX。
  - slot preimage DB **正交、gravity 不需要**:上游需要它只因把规范 storage 移到了 hashed 表、又要维持明文 changeset。gravity changeset 本就是明文键、读后自己 hash 构 `HashedPostState`,**天然无需** preimage 库。
- **是否建议 port**:static-file changesets **弱建议**(独立收益、难度中);slot preimage DB **否**(它只为"规范 storage 改成 hashed 表、又要维持明文 changeset"而存在;gravity changeset 本就是明文,无此需求)。

### 10. HashedPostState 输入精简(部分可移植,多为 engine-side)

- **上游做了什么**:
  - **避免重复 hash state**(#24354 `f6e3ebad9f`):engine payload_processor 里 sparse-trie task 已经算过的 hashed state 不再重算(prewarm/state-root-task 间共享)。
  - **不给 trie 发未变更账户**(#24432 `0456b4b9d9`):prewarm/state_root_task 过滤掉被 touch 但状态未变的账户,不喂 trie。
  - `FromIterator for HashedPostState` 单趟构造 + 简化 `from_bundle_state`(#20653,直接 map 成 `(hashed_address, account, Option<storage>)` 再 `.collect()`,空 storage 在闭包里过滤、`size_hint` 预留);`ChunkedHashedPostState` 排序优化(#20866:用 `FlattenedStateOrder{Wipe<Update(slot)<Account}` 组合 key 单次 unstable sort 取代「N 次内层 sort + 1 次 stable 外层 sort」);`hashed_post_state` 小批量改**顺序** hash(#22660 `c45ccc3e38`,避免小块并行开销)。
- **关键 PR / commit**:`f6e3ebad9f`(#24354)、`0456b4b9d9`(#24432)、`05b3a8668c`(#20653)、`4673d77c03`(#20866)、`c45ccc3e38`(#22660)。
- **涉及文件**:`crates/engine/tree/src/tree/payload_processor/{mod,prewarm,sparse_trie}.rs`、`crates/trie/parallel/src/state_root_task.rs`、`crates/trie/common/src/hashed_state.rs`。
- **与 gravity nested-trie 的关系**:#24354/#24432 绑定上游 payload_processor / state_root_task pipeline(gravity 不用),但**原则可移植**:gravity 在 `pipe-exec-layer` 里 `HashedPostState::from_bundle_state::<KeccakKeyHasher>`(`lib.rs:668`)是**单线程、对 bundle 里每个账户都 hash**,未过滤「info 与 storage 都没变」的账户。过滤未变更账户(判断 `BundleAccount.info != original_info`,仅过滤 account-info 发射、storage 变更仍要走)可减少送进 `calculate` 的账户数(nibble 分区更小、少建 storage cursor)。注意:gravity 的 `from_bundle_state` **已采用 #20653 的单趟 `.collect()` + `Option<storage>` 过滤空 storage 模式**(见 `hashed_state.rs:49`),这条已收敛;#20866 组合 key 单次排序仅当 gravity 需要 chunk/排序 `HashedPostState` 时才有意义。
- **是否建议 port**:**弱-中建议**「过滤未变更账户」(在 `from_bundle_state` 或调用点判断 `account.info` 与 storage 是否实际变化后再纳入),难度低-中,正交于 on-disk 格式;#22660「小块顺序 hash」也可借鉴(gravity 每块状态量小时避免过度并行)。#24354「不重复 hash」对 gravity 意义小(本就只 hash 一次)。

### 11. Sparse-trie / proof-v2 内部微优化(引擎特定,不可移植)

- **上游做了什么**:一批只对 sparse-trie/proof-v2 有意义的微优化——branch mask 扁平化(`BranchNodeMasks` 4 字节 Copy 取代双 HashMap,#20659/#20664);删 `SparseNode::Hash` 变体、blinded hash 内联到 branch(#22290);`memory_size` 启发式(#21745);reserve proof branch rlp 节点(#24469);空 storage proof 快路径(#24301);account+storage reveal 合并(#24265);跳过已 commit 的 proof child churn(#24225);TrieMask 位迭代(#21676)等。
- **关键 PR / commit**:`240dc8602b`/`0f585f892e`、`37c4f908fa`、`8e21afa9cc`、`37f700186b`、`ada255d989`、`b95b5441d0`、`805f915e82`。
- **与 gravity nested-trie 的关系**:**引擎特定,基本不可移植**——都是围绕 sparse trie 节点表示 / proof reveal 的内部结构。gravity 的 `NodeFlag`(缓存 `RlpNode` + dirty)、`FullNode` 内联 16 字节 child mask 已经是同类思想的等价物。
- **是否建议 port**:**否**。仅作参考:gravity 序列化 `FullNode` 时用 `mask[16]` 记录每 child rlp 长度、blinded child 内联存 `RlpNode`,与上游「mask 扁平化 + blinded hash 内联」殊途同归。

### 12. 正交:ordered root builder / BAL 通知 / state hook(与 state root 无关)

- **上游做了什么**:
  - **open-ended ordered root builder**(#24419 `4da25612f1`)+ **统一 ordered trie encoders**(#24523 `5635fc28c8`)+ **后台增量 receipt root**(#21131 `13707faf1a`):针对 **tx root / receipt root / withdrawals root**(对 `RLP(index)→value` 的顺序列表求 root)的**增量/流式**构建器。与 state root(对 hashed address/slot)算法无关。
  - 机制:这些 root 把叶子按 `rlp(index)` 的 nibble 升序流式喂给无状态的 `alloy_trie::HashBuilder`,取 `.root()`——无 DB、无 per-node 行、无 proof。`rlp(index)` 不单调(`1..=0x7f` 先、`0`→`0x80` 排在其后、`>=0x80` 长格式再后),故流式时唯一待定的叶子是 index 0,在 `0x80` 到达前(长列表)或流末(短列表)插入。#21131 把 receipt root 计算**放进一个后台线程**(`crossbeam_channel` + `tokio::oneshot`),executor 每执行完一笔 tx 就把 receipt 发过去,后台线程与 EVM 执行**并行**做 encode + trie 构建 + `logs_bloom` 聚合,validator 最后 `blocking_recv` 拿 `(root, bloom)`——把 root 哈希延迟**藏进执行时延里**。#24419/#24523 把它改成只缓存 1 个叶子的真流式构建器,可"边出块边算 root"。
  - **BAL 通知流**(#23918 `8940f2f0d6`):Block Access List(EIP-7928 方向)provider/storage-api,供 prewarm/prefetch「本块将访问的状态」。
  - **state hook from `State<DB>`**(#24654 `fa7c66c14e`):执行期通过 hook 捕获状态变更,喂 state-root task / prewarm。engine-side。
- **涉及文件**:`crates/trie/common/src/open_ended_ordered_root.rs`、`crates/storage/provider/src/bal.rs`、`crates/evm/evm/src/execute.rs`。
- **与 gravity nested-trie 的关系**:**正交**。ordered root 是 tx/receipt 层(顺序整数 key、无状态 `HashBuilder`、无节点持久化),与 nested-trie 的 root 算法**完全解耦**;BAL/state-hook 是执行期 prefetch,与 nested-trie 的 root 算法无耦合。
- **是否建议 port**:与 account/storage state root 无关,但有两个**可加性、自包含的移植点**:
  - **后台并行算 receipt/tx root(#21131,弱-中建议)**:把 receipt/tx root 的哈希放进一个后台任务、与 EVM 执行**并行**跑,root 哈希延迟被执行时延吸收——这是**引擎无关、drop-in** 的流水线优化,对 gravity 的 pipe-exec 出块/校验路径同样适用(不依赖 nested-trie / grevm 的任何内部结构)。落地成本低(一个 channel + 后台线程),先量测 gravity 现在 tx/receipt root 是否已在关键路径上占时。
  - **访问集批量预取(加性)**:state hook 的 `EvmState`(或解码后的 BAL)给出的**访问集**恰好是本块会碰到的 MPT path 集合——可据此在算 root 前/中**批量预取(multi-get)这些 `AccountsTrieV2`/`StoragesTrieV2` 节点行**,把大量随机单 key 查变成一次批量读,给懒加载 nested-trie 预热。可选、加性,不改 root 算法。

---

## 汇总表

| # | 主题 | 上游价值 | 与 nested-trie 关系 | 建议 port | 优先级 | 难度 |
|---|------|---------|--------------------|-----------|-------|------|
| 1 | ArenaParallelSparseTrie 主引擎 | 高(上游增量主引擎) | 互斥主引擎 | 否(强) | - | 高 |
| 2 | Proof V2 stack-based 算法 | 高 | 互斥引擎;类型/结果是可移植 API | 引擎否;proof-v2 target/结果类型弱建议对齐 | 低 | 中 |
| 3 | Sparse trie 当缓存 + LFU | 高(最大提速来源) | 互斥;LFU 剪枝技巧可借鉴 | 否(强);LFU 用于 PersistBlockCache 容量上限(弱) | 低 | 中 |
| 4 | Proof/root worker 池化 + cursor overlay 微优化 | 高 | 基础设施多为可借鉴 | **建议(中-强)**:cursor 复用/池化、cacheline、小块降配、overlay 命中跳过 DB seek | 中 | 低-中 |
| 5 | TrieWitness→proof v2;删 masked/provider | 中 | 内部互斥;witness 扁平格式可移植 | witness 格式对齐(弱);masked/provider 无需 port | 低 | 低 |
| 6 | **Sorted trie writes** | 中-高(写放大↓) | **直接可移植技巧** | **建议(强)**:写库前按 path 排序 | **高** | 低 |
| 7 | PackedStoredNibbles 65→33B | 中 | storage subkey 已等价/更优;account key `StoredNibbles` 当前未 packed | subkey 否/收敛;account key 弱建议 packed(需迁移);先解决 nibbles.rs 冲突 | 低 | 中(on-disk 格式) |
| 8 | in-memory trie changesets + Overlay | 高(上游 reorg/历史主线) | 互斥;架构可弱借鉴 | node 级否;ChangesetCache/anchor 校验弱借鉴 | 低 | 中 |
| 9 | static-file changesets / slot preimage / hashed v2 | 中 | 正交 | static-file changesets 弱建议;preimage 否 | 低 | 中 |
| 10 | HashedPostState 输入精简 | 中 | 原则可移植(engine-side) | 弱-中:过滤未变更账户、小块顺序 hash | 中 | 低-中 |
| 11 | sparse/proof-v2 内部微优化 | 低(引擎特定) | 不可移植 | 否 | - | - |
| 12 | ordered root / BAL / state hook | 中(receipt root 后台化) | 正交(非 state root) | 后台并行算 receipt/tx root(弱-中);访问集批量预取(加性) | 中 | 低 |

**给 gravity 的落地优先级建议**:
1. **主题 6 Sorted trie writes(强/高优先/低难度)**:`TrieUpdatesV2` 现为 HashMap/HashSet 无序,写 `AccountsTrieV2`/`StoragesTrieV2` 乱序 → 有序化后 MDBX 前向写、page split↓。与 16 路 nibble 分区天然契合,不碰格式。
2. **主题 4 worker 池化 + cursor 复用(中-强)**:`calculate_and_proof` 每次临时开 cursor,可改长驻 worker 持 tx+cursor 跨块复用;cacheline/小块降配/overlay 命中跳过 DB seek 均通用低难度。
3. **主题 10 过滤未变更账户 + 小块顺序 hash(弱-中)**:`from_bundle_state` 处减少送 trie 的账户。
4. **主题 2/5 proof-v2 / witness 对外格式(弱)**:利于跨客户端 proof/witness 互操作与 stateless。
5. **主题 7 先解决 `nibbles.rs` 合并冲突**(保留 gravity 变长 pack 版)。

---

## 备注 / 存疑

1. **当前 merge 树是 WIP**:`gravity-reth-merge-v2.3.0` 分支处于「team-share checkpoint」中间态,`crates/trie/**` 等大量文件**带已提交的冲突标记**(`<<<<<<< HEAD` / `>>>>>>> v2.3.0`),包括 `crates/trie/common/src/nibbles.rs`(`StoredNibblesSubKey` 编码)、`crates/trie/common/src/{storage,updates}.rs`、`crates/trie/parallel/src/lib.rs` 等。本文的 gravity 侧描述以 HEAD(gravity nested-trie)语义为准。
2. **合并带进来的上游代码可能是死代码**:merge 树里 `crates/trie/db/src/changesets.rs`(`compute_trie_changesets`/`ChangesetCache`)、`crates/trie/db/src/proof.rs`(上游 `Proof` over `BranchNodeCompact`)跑在上游 `AccountsTrie`/`StoragesTrie`(BranchNodeCompact)表上,而 gravity 热路径用 `AccountsTrieV2`/`StoragesTrieV2`(`StoredNode`),两套表不通用。合并收尾需确认这些上游 trie 代码是否需要保留/裁剪(避免混淆与体积)。
3. **on-disk 格式是链关键、不可轻动**:主题 1/2/3/8 的引擎都以 `BranchNodeCompact`「一行一 branch」为前提;gravity 是「一行一 MPT 节点」。任何涉及 on-disk 布局的 port(如主题 7 定长 dup subkey)都需谨慎的迁移与回滚方案。
4. **上游 2.3.0 的 trie crate 布局**:`sparse-parallel` crate 已并入 `crates/trie/sparse`;gravity 树仍保留 1.8.3 的 `sparse-parallel` 目录(合并未收敛)。上游 `crates/trie` 在区间内 302 commits。
5. **`NestedStateRoot::multiproof` 未完成**:含 `todo!("update storage/account proofs")`;若要产完整 nested multiproof,可参考 proof-v2 的 `ProofV2Target{min_len}` 语义与 `DecodedMultiProofV2::from_witness` 的 BFS 重建思路(主题 2/5)。
6. **性能数字未实测**:本文均为机制层面分析;sorted writes、worker 池化等的实际收益需在 gravity 负载上 benchmark 确认(尤其 sorted writes 对 MDBX page split 的影响、worker 池化对 tx 复用的收益)。
</content>
</invoke>
