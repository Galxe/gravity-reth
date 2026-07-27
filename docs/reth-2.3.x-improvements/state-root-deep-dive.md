# reth v2.3.0 State Root 深度解析 — proof_v2 / arena sparse trie / state_root_task 流水线 / sorted overlay API

> 生成日期:2026-07-08。调研基准:上游 reth v2.3.0(`/Users/gx/ws/git/block/reth`,
> commit `9384bc53d8`),文中 `文件:行号` 均指该仓库。
>
> 本文是 [`state-root.md`](./state-root.md)(12 主题 port 决策速览)的**算法级展开**:
> 逐项回答「具体是什么、算法是什么、为什么这么优化、有什么作用、gravity-reth
> 缺少有什么影响」。port 结论以 `state-root.md` 与
> [`../merge-v2.3.0/upstream-optimizations-gap.md`](../merge-v2.3.0/upstream-optimizations-gap.md)
> 为准,本文不改变它们,只提供机制细节。

## 0. 总览:四项工作怎么拼成一条流水线

以太坊客户端出块/验块的三大耗时:执行交易、算 state root、落盘。state root
的本质开销是:对每个被改的账户/槽,重走 MPT 路径、重新 RLP + keccak 沿途
节点,而节点在 DB 里是海量随机读。reth 自 v1.8 起把它改造成**与执行重叠的
流水线**(state root task + sparse trie);v2.3.0 是对这条流水线四个瓶颈的
系统性重写:

```
                  ┌───────────────── §4 sorted overlay ─────────────────┐
                  │ 未持久化祖先块 N-1,N-2… 的 sorted 状态叠加层          │
                  │ (StateTrieOverlayManager:Arc 共享 + k 路归并 + 缓存) │
                  └───────────────┬──────────────────────────────────────┘
                                  │ 垫在 DB 之上的"内存视图"
newPayload(N)                     ▼
──────► tx-iterator ──┬──► prewarm 并行预执行(暖缓存 + 投机取证)
                      │
                      └──► 主线程顺序执行 ──► state hook 流出每笔交易的状态变更
                                                  │
                          ┌───────────────────────▼───────────────┐
                          │ §1 SparseTrieCacheTask 事件循环        │
                          │ (去重 → 派发 proof → reveal → 增量更新)│
                          └──────────┬───────────────┬────────────┘
                    proof targets    │               │ 叶子增量更新
                                     ▼               ▼
                    ┌────────────────────┐  ┌─────────────────────────┐
                    │ §2 proof_v2        │  │ §3 arena sparse trie    │
                    │ worker 池并行取证  │  │ 增量算 root(256 子树并行)│
                    └────────────────────┘  └─────────────────────────┘
```

四者的分工:**§1** 是调度骨架(执行与取证/建树重叠);**§2** 让"取证"
(拿到缺失 trie 节点)本身快一个量级的机制重写;**§3** 让内存里那棵
增量 trie 的更新与哈希变快、变省、可并行;**§4** 让"多个未持久化祖先块
的状态叠加层"从每次重算变成缓存复用。

**gravity-reth 读者须知**:gravity 生产走 pipe-exec(grevm)+
`NestedStateRoot`(`AccountsTrieV2`/`StoragesTrieV2` 表)+
`PersistBlockCache`,**不经过**上述任何路径;上游这套只影响 eth-mode
(`--gravity.disable-pipe-execution`)。合并 v2.3.0 时因两套 trie 引擎
互斥,gravity engine 侧保留了 baseline(~v1.8 时代)流水线,本文各项在
合并树中几乎全为磁盘孤儿。每节末尾有逐项影响分析;§5 进一步给出
nested hash 与本套体系的**逐维度对比**(gravity 是否拥有这些优化、
各自在什么运行域更优)。

---

## 1. state_root_task 流水线(SparseTrieCacheTask 体系)

### 1.1 具体是什么

一套让 **state root 计算与交易执行全程重叠**的多线程流水线。v2.3.0 中
旧的独立 `MultiProofTask` 已并入 `SparseTrieCacheTask`(PR #22780/#23418;
`payload_processor/multiproof.rs` 只剩 84 行薄壳),核心事件循环在
`crates/engine/tree/src/tree/payload_processor/sparse_trie.rs`。

参与者(线程/池,池大小见 `crates/tasks/src/runtime.rs:925-940`,
`default_threads ≈ 核数-1`):

| 角色 | 位置 | 职责 |
|---|---|---|
| `tx-iterator` | `payload_processor/mod.rs:446` | 恢复交易签名,把 `(idx, tx)` 同时发给 prewarm 通道和主执行通道 |
| `prewarm` 池(`default_threads`) | `prewarm.rs:457/122` | 并行预执行交易:暖执行缓存 + 发投机 proof target |
| 主执行线程 | `payload_validator.rs:582` | 顺序正式执行,state hook 逐笔流出 `EvmState` |
| `trie-hashing` | `sparse_trie.rs:141/178` | 把 `EvmState` keccak 成 `HashedPostState`(把哈希开销从事件循环卸载) |
| `sparse-trie` 事件循环 | `sparse_trie.rs:273` | 中枢:去重、派发取证、reveal、增量更新、算 root |
| storage proof worker 池(`2×default_threads`) | `proof_task.rs:618` | 每 worker 常驻一个 DB 只读事务 + 游标 + `StorageProofCalculator` |
| account proof worker 池(`2×default_threads`) | `proof_task.rs:836` | 同上,账户 multiproof;并向 storage 池预派发存储证明 |

消息类型:`StateRootMessage`(`state_root_task.rs:14`,执行/prewarm →
trie-hashing):`PrefetchProofs` / `StateUpdate(EvmState)` /
`HashedStateUpdate` / `FinishedStateUpdates`;`SparseTrieTaskMessage`
(trie-hashing → 事件循环);`AccountWorkerJob` / `StorageWorkerJob` /
`ProofResultMessage`(事件循环 ↔ worker 池)。

### 1.2 算法(完整数据流)

1. `PayloadProcessor::spawn`(`mod.rs:287`)一次拉起 tx-iterator、
   state-root 体系(`mod.rs:382`:建通道 → `ProofWorkerHandle::new` 起
   两个 worker 池 → `spawn_sparse_trie_task`)与 prewarm。
2. **prewarm**:每笔 tx 在 rayon 池上预执行(每线程懒初始化 EVM,
   `prewarm.rs:202`),EVM **关闭 nonce/balance 检查**
   (`prewarm.rs:598-602`)以便预执行依赖前序交易的 tx;结果写共享执行
   缓存;每笔(index>0)把触碰到的地址/槽作为 `PrefetchProofs` 发出——
   **投机取证**,在正式执行到达前就开始抓 trie 节点。与主执行共享
   `executed_tx_index: AtomicUsize`(PR #22647),已被正式执行处理的 tx
   直接跳过。tx<5 跳过 prewarm(spawn 开销 > 收益,`mod.rs:83-85`)。
3. **主执行**逐笔产出 `StateUpdate(EvmState)`;`trie-hashing` 把它转成
   `HashedPostState`(`state_root_task.rs:163`,只取 touched/changed,
   处理 selfdestruct)。
4. **事件循环消费**(`sparse_trie.rs:279`,`select_biased!`):
   - 状态更新 → 缓冲进 `new_account_updates/new_storage_updates`,
     记录 LFU 热度(§3.6),账户先挂 `pending_account_updates`(等
     storage root 算好再回填);
   - prefetch target → 仅标记 `Touched`,不覆盖真实改动。
5. **去重生成 target**:空闲时 `process_leaf_updates`(`sparse_trie.rs:586`)
   对 trie 调 `update_leaves`——命中已 reveal 的路径直接更新(cache hit),
   未命中吐出 `ProofV2Target`;再经 `fetched_account_targets/
   fetched_storage_targets`(记录已请求的 target 及其 `min_len`)去重:
   只有没请求过、或这次要求更浅前缀(`min_len` 更小)才再次请求。
6. **分块派发**:`dispatch_with_chunking`(`multiproof.rs:59`)——target 数
   > 300 或(≥2×chunk_size 且有多个空闲 worker)才按 `chunk_size`(默认 5)
   拆块,避免"碎块过多反而拖慢"(PR #24236);派发给 account worker。
7. **worker 取证**(§2 的 proof_v2):account worker 先把这批涉及的存储
   证明**一次性预派发**给 storage 池(`proof_task.rs:1062`,按地址排序
   派发以匹配账户 trie 字典序遍历、减少队头阻塞),然后账户 trie 遍历与
   存储证明计算**重叠**;结果 `DecodedMultiProofV2` 送回事件循环。
8. **reveal**:事件循环把通道里所有到达的 proof 合并后一次
   `reveal_decoded_multiproof_v2`(`sparse_trie.rs:315-318,524`)灌进
   sparse trie(§3)。
9. **收敛**:执行结束发 `FinishedStateUpdates`;当"执行已结束 + 账户/
   存储 update 全部 drain"(`sparse_trie.rs:342`)时退出循环:先 rayon
   并行算完所有脏 storage trie 的根(`compute_drained_storage_roots`
   :678),回填账户叶子,最后 `root_with_updates()`(:375)。若本块没改
   任何状态(账户 trie 仍 blind),直接返回 parent root(:377-387)。
10. **回退链**(`payload_validator.rs:1757` `plan_state_root_computation`):
    策略一次性选定,`StateRootTask` 与 `ParallelStateRoot` 是**并列**
    顶层策略;StateRootTask 有 timeout(默认 1s)后 spawn 串行计算与
    任务**赛跑**(:1384-1425);两条策略失败都统一回退**同步串行**
    `StateRoot`(:807-832),并重算 hashed state 重跑校验(PR #24506)。
11. **跨 payload 保留**:root 算完后 trie 经 `commit_updates` + LFU
    `prune` + `shrink_to`(1M 节点/值上限,≈120MB/144MB,
    `mod.rs:60-81`)后存入 `SharedPreservedSparseTrie`
    (`preserved_sparse_trie.rs`);下一个块若 parent root 匹配则**直接
    复用剪枝后的树**(continuation),否则 clear 留分配。另有
    `share_sparse_trie_with_payload_builder`(PR #23246,默认关):FCU 时
    把同一条流水线的 `StateRootHandle` 交给 payload builder,出块边构建
    边算根。

### 1.3 为什么这么优化 & 作用

朴素方案里 state root 只能在执行完后开始,且第一步就要从 DB 随机读大量
trie 节点。流水线把三类延迟藏进执行时间:**取证延迟**(prewarm 投机 +
执行中实时派发)、**哈希延迟**(trie-hashing 线程卸载 keccak)、**建树
延迟**(sparse trie 边执行边 reveal/update)。执行结束时,root 通常只差
"最后一批 update + 收尾哈希"。辅以跨块保留(热点路径下块免取证)、
worker 常驻事务/游标复用(免每次开销)、去重(同一 target 只取一次)。
效果:state root 从"执行后的串行大块"变成"执行结束后的一小段收尾"。

### 1.4 gravity-reth 缺少的影响

- **生产(pipe)路径:零影响**。pipe-exec 的状态根由 `NestedStateRoot`
  增量计算,配合 `PersistBlockCache` 与自己的并行层(#219),问题域
  相同、解法不同,且已在线上验证(差分回放测试覆盖)。
- **eth-mode:停留在 v1.8 形态**。合并树的 payload_processor 是 gravity
  baseline 版(自有 66KB multiproof.rs + 自有 sparse-parallel crate):
  有"state root task + sparse trie"的老一代骨架,但没有 v2.3 的
  SparseTrieCacheTask 合并架构、prewarm-rayon 化、worker 可用性调度
  (cacheline 对齐 `AvailabilitySheet`)、timeout 赛跑回退、跨 payload
  保留(§3.6 LFU)。影响面 = eth-mode 的 newPayload 延迟与吞吐。
- **可借鉴思想**(不依赖 sparse trie):worker 常驻事务 + 游标复用
  (`state-root.md` #4 已列为 ★★ port 候选);prewarm 的
  `executed_tx_index` 去重原子计数;"tx<N 走顺序路径"的小块降配
  阈值族。

---

## 2. proof_v2:新一代 multiproof 计算核

### 2.1 具体是什么

multiproof = "一批目标账户/槽的 Merkle 证明节点集合",是 sparse trie
reveal 的原料。proof_v2(`crates/trie/trie/src/proof_v2/`,PR #19687 起)
是对经典 `Proof`(TrieWalker + HashBuilder)的整体重写,自述四特性
(`proof_v2/mod.rs:1-8`):仅用叶子数据、输出按 path 排序、调用后自动
reset、跨调用复用游标、泛型值 + 惰性求值。

### 2.2 为什么:经典路径的成本

经典 `Proof::multiproof`(`proof/mod.rs:193-274`):TrieWalker 带着
prefix_set 走 trie 游标,HashBuilder 挂 `ProofRetainer` 重建路径。三大
开销:
1. **每次调用重建游标**(`Proof` 按值消费,无跨调用复用);
2. **每个被遍历到的账户都触发一次完整存储树遍历**求 storage root
   (`proof/mod.rs:235-242`,即使它不是 target);
3. **保留路径上所有 branch 重复 RLP + keccak**,即使 DB 里已有缓存哈希;
   输出是 `path→bytes` map,消费端还要解码 + 排序。

### 2.3 核心算法:leaf-only 自底向上单遍构造

`ProofCalculator`(`proof_v2/mod.rs:48-92`)不从 DB 读 branch 结构,而是
**按字典序流式消费叶子,自底向上重建 trie 结构**。核心状态:

- `branch_stack`:在建 branch 栈(每个是上一个的子);
- `child_stack`:各 branch 的子节点扁平栈——因为叶子按字典序到达,
  **除栈顶 branch 的最后一个子外,其余子都已定型**,可立即转成 `RlpNode`;
- `cached_branch_stack`:从 trie 游标取到的 DB 缓存 branch
  (`BranchNodeCompact`,含 `state_mask/tree_mask/hash_mask/hashes`);
- 可复用的 RLP 缓冲 free-list(避免每节点分配)。

叶子插入 `push_leaf`(`mod.rs:599-672`):比较新 key 与栈顶 branch 路径
的公共前缀——前缀变短说明栈顶 branch 已完整,`pop_branch` 弹出(把它的
子按序拼成 `BranchNodeRef` 编码、连同父 extension 一起折叠成一个节点压回
`child_stack`);前缀相同则作为新子挂上(必要时劈分已有子造新 branch)。
整个构造对每个节点**恰好编码一次**,天然产出 **depth-first 后序**
(子先于父)的证明节点序列。

**用缓存哈希跳子树**是最大的省力点(`next_uncached_key_range`,
`mod.rs:1011-1297`):对照 DB 缓存 branch 的 `hash_mask`——某个子 nibble
若 ① 缓存里有哈希、② 不在 prefix_set(本块没改它)、③ 不需要被 retain
进证明,则直接取 `hashes[idx]` 作为该子的 `RlpNode` 压栈,**整棵子树的
叶子遍历与 RLP/keccak 全部跳过**。只有"缓存没覆盖 + 被改过 + 要证明"
的 key 区间才回落到 `calculate_key_range` 从叶子重算。

### 2.4 延迟值编码(DeferredValueEncoder)

账户叶子的 RLP 含 `storage_root`,而 storage root 可能正被别的 worker
并行计算。`LeafValueEncoder` trait(`value.rs:32-49`)把"启动计算"
(遍历到 key 时立即调 `deferred_encoder`)与"真正编码"(尽量晚调
`encode`)拆开;并行实现 `AsyncAccountValueEncoder`
(`trie/parallel/src/value_encoder.rs`)三态:`Dispatched`(存储证明已
预派发,`encode` 时才 `recv()` 等结果)/ `FromCache`(跨 worker 共享
`DashMap` 里已有根)/ `Sync`(共享 calculator 同步算)。配合
`commit_last_child` 的延迟转换(`mod.rs:377-378`,PR #20873),账户 trie
遍历与存储证明计算真正重叠。

### 2.5 target、分块与新节点表示

- `ProofV2Target { key_nibbles, min_len }`(`common/src/target_v2.rs:9`):
  "返回 path 是该前缀、长度 ≥ min_len 的节点"——支持"浅层已有、只补
  深层"的精确请求,是去重(§1 步骤 5)能按深度比较的基础。
- `ChunkedMultiProofTargetsV2`(`target_v2.rs:85-202`):按"至多 N 个
  target"切块给 worker 池,同账户的存储 target 尽量同块、跨块不重复
  账户 target。
- `TrieNodeV2`(`common/src/trie_node_v2.rs:104`):**extension 与其子
  branch 合并为单个 `BranchNodeV2`**(PR #22021;MPT 中 extension 几乎
  总是紧跟 branch),mask 内嵌,少一层间接;输出为有序 Vec 而非 map。

### 2.6 作用与消费端

输出 `DecodedMultiProofV2` 按 depth-first 后序排列 → sparse trie 的
`reveal_decoded_multiproof_v2`(`sparse/src/state.rs:300-376`)**零排序**
顺序 reveal(父 reveal 时子已就绪),且按账户/存储树 rayon 并行灌入。
v2.3.0 中 engine 侧 state root 与 TrieWitness 已全走 proof_v2
(#20617/#22922);**对外 `eth_getProof` 仍走经典路径**(RPC →
`Proof::multiproof`),两条路共存。经典 reveal 入口也已统一:内部
`.into()` 转 V2 再 reveal(`state.rs:288-293`)。仓库内无硬编码加速比,
量化对比在 criterion bench(`trie/trie/benches/proof_v2.rs`,PR #19967)。

### 2.7 gravity-reth 缺少的影响

- 合并树中 proof_v2 四文件、`value_encoder.rs` 均为**磁盘孤儿**
  (`trie/src/lib.rs` 未挂 `proof_v2`);`target_v2.rs` 挂载但无消费方。
- **生产路径零影响**:`NestedStateRoot` 的增量计算直接读自己的持久
  V2 表节点,概念上等价于 proof_v2 的"缓存哈希跳子树"——nested trie
  把"证明"这一步整个消掉了(节点常驻表内,不需要向 sparse trie
  reveal),这正是两套引擎互斥的根源。
- **eth-mode**:proof 抓取停留在经典实现(gravity baseline 的
  multiproof 机器),批量取证吞吐低于上游 v2.3;`eth_getProof` 两边
  都是经典路径 + gravity 另有 nested-hash getProof(#237),无差距。
- **可借鉴**:自底向上单遍构造、每节点恰好一次编码、free-list 缓冲、
  延迟值编码——若未来给 nested trie 做批量证明/witness 服务(比如
  对外提供 stateless 数据),这是现成的算法模板;`state-root.md` #2
  的结论(引擎互斥、API 可借鉴)不变。

---

## 3. arena 化 sparse trie

### 3.1 具体是什么:sparse trie 与 blind/reveal

sparse trie 是**只装"被触碰路径 + 兄弟哈希"的内存 MPT 子集**。原理:
MPT 是 Merkle 树,父节点 RLP 只需要子节点的哈希;所以重算 root 只需
从 root 到每个被改叶子的路径被完整展开(reveal),路径上 branch 的
未触碰兄弟子树只留哈希(blind)。`SparseStateTrie`
(`sparse/src/state.rs:34`)= 一棵账户 trie + 每账户一棵 storage trie
(`StorageTries`,含清空复用池 `cleared_tries`)。

v2.3.0 的实现格局(重要更正,勿按旧印象理解):`SerialSparseTrie`
**已删除**(PR #21808);现存两个实现均为"256 子树并行"结构——
`ParallelSparseTrie`(HashMap 存节点,`parallel.rs:107`)与
`ArenaParallelSparseTrie`(arena 存节点,`arena/mod.rs:619`),经
`ConfigurableSparseTrie` 枚举统一,**arena 版是默认**(PR #23131)。

### 3.2 为什么:HashMap 版的三个瓶颈

HashMap 版节点 `SparseNode`(`trie.rs:364-394`)的问题:
1. **指针追逐变成哈希探测**:父→子引用是"拼出子的完整 Nibbles 路径 →
   HashMap 查找",每步都要 hash Nibbles + 桶探测;
2. **per-node 堆分配**:每个 branch 固定携带 `Box<[B256;16]>`(512 字节)
   存盲化子哈希,哪怕只有 2 个子;节点与值各一张 HashMap,扩容即全量
   rehash;
3. **缓存不友好**:节点散落堆上。

### 3.3 arena 化的算法

- **SlotMap 竞技场**(`arena/mod.rs:33-35`):节点存连续 slab,子引用是
  `Index`(带 version 的 slot key,防 ABA/悬垂);分配 = `arena.insert`,
  文档明言目标是 "direct index-based child pointers, avoiding the
  per-node hashing overhead of a HashMap-based trie"(`arena/mod.rs:566`)。
- **密集 children**(`nodes.rs:58-72`):branch 的子存
  `SmallVec<[Child; 4]>`(内联 4 个),`state_mask` 16 位标占用;
  nibble → 密集下标 = `count_ones(state_mask & ((1<<nibble)-1))`。
  子是 `Revealed(Index)` 或 `Blinded(RlpNode)`——彻底去掉
  `Box<[B256;16]>`。extension 合并进 branch(`short_key` 字段),与
  proof_v2 的 `BranchNodeV2` 表示对齐(reveal 零转换)。
- **upper/lower 深度 2 分层**(`arena/mod.rs:41,562-617`):路径长
  < 2 nibble 的节点在 upper;深度恰为 2 处包成 `Subtrie` 节点,最多
  16²=256 棵 lower 子树,**每棵子树拥有独立 arena** → 互不共享、
  无锁并行变更。
- **并行 hash**(`arena/mod.rs:2458-2549`):收集脏子树(`mem::replace`
  成 `TakenSubtrie` 占位取走),脏叶子总数 ≥ 阈值(默认 64)则
  `into_par_iter` rayon 并行算各子树 RLP,否则串行;最后 upper 串行
  收尾拼 root。HashMap 版同构(`parallel.rs:475-530`),另有
  `LowerSparseSubtrie::Blind(Some(box))` 状态机(`lower.rs`)在盲化时
  攥住已 clear 的分配供复用。
- **每块重建而非收缩**:arena 版 `clear` 直接重建 SlotMap
  (PR #23073),用 BFS `compact_arena` 保持父先于子的 top-down 布局。

### 3.4 LFU 跨块保留

无界保留 revealed 节点会爆内存,全清又浪费热点路径。`BucketedLfu`
(`lfu.rs:22-31`):`entries: HashMap<K,(freq,pos)>` + `buckets[freq]`
桶数组(频率封顶 255)+ `min_freq` 指针,`touch`/`evict` 均 O(1)
(桶内 `swap_remove`)。`SparseStateTrie` 用两个 LFU 追踪热账户/热槽
(默认 1000/1500),块结束 `prune`(`state.rs:750-782`):**不是**保留
LFU 中的节点本身,而是把"不是任何热点叶子祖先的 revealed 子树折叠回
哈希 stub"(Revealed → Blinded(cached_rlp)),账户树与存储树 rayon
并行修剪。效果:下一块热点路径免取证,内存有界。(PR #22766)

### 3.5 作用

三类收益:① 子引用 O(1) 直接索引 + 连续内存(替代 Nibbles 哈希 + 桶
探测);② branch 从固定 512B 盲哈希数组变密集 SmallVec,分配大幅下降;
③ 256 子树独立 arena 无锁并行 hash + LFU 有界跨块保留。仓库内无量化
数字,机制收益见 `arena/mod.rs:566-575` 文档与 PR 链
(#22381 引入 → #23131 设默认)。

### 3.6 gravity-reth 缺少的影响

- 合并树中 `arena/`、`parallel.rs`(上游版)、`lfu.rs`、`lower.rs` 均为
  **孤儿**(`sparse/src/lib.rs` 只挂 gravity 自有的 state/trie/traits/
  provider/metrics);gravity 保留自己的 `crates/trie/sparse-parallel`
  crate(baseline 产物,老一代结构)。
- **生产路径零影响**:nested trie 不用 sparse trie。gravity 的"跨块
  内存态"由 `PersistBlockCache` 承担,其容量/淘汰由
  `--gravity.cache.capacity` + 淘汰 daemon 管理——与 LFU prune 是同一
  问题的不同解。
- **eth-mode**:sparse trie 停留在 gravity baseline 版,没有 arena 的
  内存/CPU 收益,也没有 LFU 跨块保留(每块重新取证热点路径)。
- **可借鉴(价值最高的一节)**:arena/SlotMap + 密集 children 是
  **通用的内存 trie 表示技术**,不绑定 sparse 语义。若 profiling 显示
  `NestedStateRoot` 的内存节点结构(或 `PersistBlockCache` 的 trie 节点
  缓存)存在 HashMap 探测/分配热点,这套表示值得评估移植。同理
  `BucketedLfu` 是独立的 O(1) 有界缓存件(带 proptest 模型验证),可
  直接复用于任何"热点保留"场景。

---

## 4. sorted overlay API

### 4.1 具体是什么 & 为什么

newPayload 验证块 N 时,块 N-1、N-2…可能还没持久化,它们的状态改动
(`HashedPostState`)与 trie 节点改动(`TrieUpdates`)必须叠成一个
**overlay(内存视图)**垫在 DB 之上,喂给状态根/证明计算。v1.8 时代的
做法(`TrieInput`,HashMap 表示)有三连成本:每 payload 对 N 个祖先块
逐块 `extend`(HashMap 全量 rehash);每次消费 `prepend_self(clone)`
(深克隆整张聚合表,`memory_overlay.rs:130` 等);而 trie 游标只吃
**排序**切片,用前还得再全量排序一次。祖先越多(持久化滞后、深 reorg),
浪费越大。

v2.3 的解法是把整条链路**排序化 + Arc 化 + 中央缓存化**。

### 4.2 sorted 表示与转换

- `HashedPostStateSorted { accounts: Vec<(B256, Option<Account>)>, storages: B256Map<HashedStorageSorted> }`
  (`hashed_state.rs:516`);`TrieUpdatesSorted` 把原来分离的"更新
  HashMap + 删除 HashSet"**合并成一个排序 Vec,`None` 表示删除**
  (`updates.rs:548`)。全部按 key 升序,构造时 debug_assert 校验。
- 三个转换:`into_sorted`(消费自身,drain + sort_unstable)、
  `clone_into_sorted`(借用,避免克隆 HashMap 容量结构,PR #20784)、
  `into_sorted_ref`(借用产引用视图,序列化用)。
- 一律 `Arc<...Sorted>` 携带:overlay 缓存、跨 (anchor,tip) 共享、
  waiter 结果全部 `Arc::clone` 零拷贝;写时才 `Arc::make_mut`。

### 4.3 归并算法

- **两路**:`extend_sorted_vec`(`utils.rs:31-79`)双指针 O(n+m) 归并
  (PR #21098 从 O(n log n) 修正而来):不重叠区间直接 append;重复 key
  时新块覆盖旧块;`mem::take` 走已有所有权避免克隆。
- **k 路**:`kway_merge_sorted`(`utils.rs:9-27`)用 itertools
  `kmerge_by` + 按优先级 dedup,**首次出现者胜**(切片按 newest→oldest
  排),只对去重幸存者 clone,O(N log k)。`merge_batch` 在块数 ≥ 30 时
  启用 k 路,否则循环两路(`hashed_state.rs:631-653`);storage 归并遇
  wipe/is_deleted 即封口,不再吸收更旧数据。

### 4.4 把排序移出热路径:DeferredTrieData

块验证的关键路径**只存未排序的 Arc**;`ExecutedBlock.trie_data` 是
`DeferredTrieData`(`chain-state/src/deferred_trie.rs`,PR #19894):
内部 `Arc<OnceLock<ComputedTrieData>>`,后台任务 rayon 并行做
`into_sorted`(refcount 允许时 move,否则 `clone_into_sorted`);读侧
`wait_cloned()` 就绪即取、未就绪才阻塞。通知/ExEx 侧另有
`LazyTrieData`(`lazy.rs`,PR #21133):闭包惰性 + OnceLock 缓存,
listener 不取则永不排序。

### 4.5 StateTrieOverlayManager:中央缓存(PR #24184)

`chain-state/src/state_trie_overlay.rs`:
- 维护 `blocks: DashMap<hash, ExecutedBlock>` 与
  `overlays: DashMap<(anchor,tip), OverlayCacheEntry>`。
- 请求 (anchor,tip) 的 overlay 时:若父块 overlay 已缓存 →
  `ExtendCached`(clone 父 `Arc<TrieInputSorted>`,只对**一个新块**
  `extend_ref_and_sort`);否则 `MergeBlocks` 一次 k 路归并全部祖先,
  nodes 与 state 用 `rayon::join` 并行。
- 并发 payload 命中同一 (anchor,tip):`Computing(OverlayWaiter)` 让
  后到者等待同一份计算,不重复干活;`insert_block` 还会**乐观预算**
  已缓存父的子 overlay(PR #21475)。

### 4.6 消费端为什么快

trie/hashed 游标工厂(`InMemoryTrieCursorFactory` /
`HashedPostStateCursorFactory`)用 `ForwardInMemoryCursor`
(`trie/src/forward_cursor.rs:48-89`):对预排序切片 seek,剩余元素
≥128 用 `partition_point` 二分、<128 线性扫(缓存局部性)。这是
排序化的最终红利——trie 前序游走可对 Vec 二分,HashMap 根本无法
有序游走。

### 4.7 作用

三笔账:排序**每块只做一次且异步化**(不再每 payload 重排 N 个祖先);
聚合从"rehash + 深克隆 + 再排序"降为 O(n+m)/O(N log k) 归并 + Arc
零拷贝 + (anchor,tip) 缓存复用;消费端二分 seek。祖先块越多、并发
payload 越多,收益越大。

### 4.8 gravity-reth 缺少的影响

- 合并树中 `extend_from_sorted` 全树零命中;`utils.rs`/`lazy.rs` 为
  孤儿;`memory_overlay.rs`/`in_memory.rs` 已整体还原 baseline
  (merge 台账 executed-block 文档 §6.2 的裁决:拒绝上游集中式
  overlay,保留 `ExecutedBlockWithTrieUpdates` 二分)。
- **生产路径影响很小,但不是零**:pipe 模式下块严格串行 make-canonical
  + 持久化节流(`persistence_threshold=2`),未持久化窗口极浅,
  overlay 聚合本身不构成热点;真正的内存视图由 `PersistBlockCache`
  按"块号 + key"直接服务读,架构上绕开了 TrieInput overlay。
- **eth-mode**:overlay 聚合停留在"HashMap extend + 每消费深克隆"的
  老路径;若 eth-mode 出现持久化滞后(内存里积多个块),这里会被放大。
  `../merge-v2.3.0/upstream-optimizations-gap.md` §B5 与 engine 文档
  开放问题 #5 已把 memory_overlay sorted 快路列为 v2.4+ 跟进。
- **可借鉴**:`extend_sorted_vec`/`kway_merge_sorted` 是无依赖的纯算法
  件;"排序移出热路径 + Arc/OnceLock 惰性发布"的模式对任何
  "执行产出 → 多消费方"的数据(例如 gravity 的 trie updates 传递链)
  都适用。`state-root.md` #6(sorted trie writes,★★★ port 候选)正是
  这个家族里与 nested trie 直接兼容的一件:写库前按 path 排序,
  RocksDB/MDBX 有序 upsert 降写放大。

---

## 5. nested hash 对照:gravity 是否拥有这些优化,谁更优

> 本节基于对 gravity 侧实现的同等深度源码调研(2026-07-08,合并树
> `66ea109527`),gravity 侧引用为本仓路径。核心问题:nested hash 是否
> 具备 reth 2.3 的这些优化?答案:**多数不具备——但其中约一半优化解决
> 的是 nested hash 根本不存在的问题**。两套引擎是不同约束下的最优解,
> 逐维度拆开才能公平比较。

### 5.1 nested hash 的实现形状(对照基准)

- **磁盘模型——整棵 trie 物化为一等真相**。`AccountsTrieV2` /
  `StoragesTrieV2`(`crates/storage/db-api/src/tables/mod.rs:479-501`)
  按**完整 path** 存 **branch/extension/leaf 全部节点**(上游
  `AccountsTrie` 只缓存 branch,叶子靠 HashBuilder 每块从
  `HashedAccounts` 重建)。账户叶直接内嵌
  `RLP(TrieAccount)`(storage_root 已烘入,
  `crates/trie/db/src/nested_hash.rs:371-375`)。account key 为
  1 字节/nibble 的 `StoredNibbles`(Nibbles 序 == 磁盘 memcmp 序),
  storage subkey 为变长打包 `[len+1][pack(nibbles)]`
  (`crates/trie/common/src/nibbles.rs:26-96`);节点 value 用自定义
  紧凑编码(#149,`nested_trie/node.rs:203-313`)。
- **内存表示——go-ethereum 风格懒加载**。`Node` 四态枚举
  (`nested_trie/node.rs:64-86`):`FullNode{children:
  [Option<Box<Node>>;17]}` / `ShortNode` / `ValueNode` / `HashNode`
  (未加载子节点的哈希占位,访问时 `reader.read(path)` 展开);
  `NodeFlag.rlp` 缓存节点哈希、脏路径失效(`node.rs:18-48`)。
- **增量算法**(`nested_hash.rs:274-450`):账户按首 nibble 16 分桶
  `rayon::scope` 并行;每账户任务内先建/更新 storage trie、算出
  storage root、再编账户叶;全部任务 join 后统一 `parallel_update`
  账户树并 `hash()`。insert/delete 为经典 MPT 递归(分裂/塌缩,
  `nested_trie/trie.rs:213-310, 510-686`);自毁 + 重建经
  `is_deleted` 先整组删除再回填(#715 修复,写侧
  `provider.rs:2348-2352`)。
- **并行**(#219):账户级 16 路 + 单树 `parallel_update` 按 nibble
  递归分叉(`--gravity.trie.parallel-level`,默认 1,阈值
  `MIN_PARALLEL_NODES=128`,`nested_trie/trie.rs:18,337-500`)+
  哈希阶段 `std::thread::scope` 并行(`trie.rs:698-712`)。
- **缓存——权威性 PersistBlockCache**
  (`crates/storage/storage-api/src/cache.rs:311-322`):账户 /
  bytecode / storage 槽 / 账户 trie 节点 / storage trie 节点五类,
  DashMap 分片;条目带写入块号 `Tip{value, block}`;**tombstone 与
  wipe-marker 遮蔽陈旧 DB**(`cache.rs:146-279`);淘汰 daemon 按容量
  收紧保留窗口,**水位永不越过持久化高度**(`cache.rs:288-307,
  354-356`)——未持久化条目绝不淘汰,缓存承担正确性职责而非纯加速。
- **流水线——块间重叠**(`pipe-exec-layer-ext-v2/execute/src/
  lib.rs:578-767`):每块一个 tokio task,四个按块号的
  barrier(execute / merklize / seal / make-canonical)串行化各阶段;
  块 N 执行完立即放行 N+1 执行(`lib.rs:696-706`),随后才做自己的
  状态根/密封/持久化——**N+1 的 grevm 执行与 N 的状态根重叠**。
  状态根本身是执行后串行调用(无 state-hook 逐笔流入)。
- **写路径**:`write_trie_updatesv2` 把 removed+updated 合并为按
  path 排序的单列表顺序写(`provider.rs:2303-2398`);持久化拆三个
  独立 RocksDB 实例并行提交(`persistence.rs:286-293`);可选
  merge-blocks 合并 fsync(`persistence.rs:411-522`)。

### 5.2 逐维度对比

| 维度 | reth 2.3(sparse trie 体系) | gravity nested hash | 谁优 |
|---|---|---|---|
| 磁盘模型 | 叶子是真相,trie 表仅缓存 branch;内存 trie 每块临时重建 | 整棵 trie 物化为一等真相(全节点按 path 索引) | 各有代价(空间/写放大 vs 取证税) |
| 取证 | 每块必须 multiproof + reveal;proof_v2 用 15+ 个 PR 优化这一步 | **不存在这一步**:缺节点 = cache/RocksDB 按 path 点读(`nested_hash.rs:45-64,104-116`) | **gravity**(消灭问题 > 优化问题) |
| 与执行重叠 | **块内**:state hook 边执行边算,执行结束 root 只剩尾巴 | **块间**:N+1 执行 ∥ N 状态根/密封/持久化(merklize barrier 串行化,`lib.rs:711-719`) | 平手,目标不同:上游优化单 payload 延迟,gravity 优化连续流吞吐 |
| 内存节点表示 | arena/SlotMap + SmallVec 密集 children(§3.3) | `[Option<Box<Node>>;17]` 定长指针槽 + 每子一次 Box 分配 | **reth 2.3**——nested 最实的可改进点 |
| 并行粒度 | 深度 2 分 256 子树 + rayon + 脏叶阈值 64 | 16 分桶 + 递归 level(默认 1)+ 阈值 128;哈希另用 std::thread | **reth 2.3 略优**(更细、更自适应);gravity 有旋钮可加深 |
| 节点哈希缓存 | `Cached{rlp_node}` | `NodeFlag.rlp` + 脏路径失效 | 平手 |
| 账户叶依赖 storage root | 异步延迟值编码(§2.4 三态 encoder) | 结构性解决:每账户任务先 storage root 后账户叶,账户树最后统一更新 | 平手(gravity 解法更简单) |
| 跨块缓存 | LFU 剪枝保留 sparse trie(机会性,丢了只是变慢) | PersistBlockCache(**权威性**:tombstone/wipe 遮蔽陈旧 DB,#715 类 bug 由它防御) | **gravity**(承担正确性);LFU 淘汰策略可借鉴 |
| overlay 聚合 | sorted overlay + 中央缓存(§4,深未持久化窗口 + 并发 payload 必需) | 不需要:串行链 + persist-gap 背压,cache 直接服务读 | 不构成差距 |
| 分叉/reorg | 任意 fork-choice / 任意 parent / trie changesets | **假设严格串行单链**;unwind 走 changeset 重算(merkle stage) | **reth 2.3**——这是它整套架构存在的理由 |
| proof/witness 服务 | proof_v2 全量支持,witness 已切 v2 | `Trie::get_proof` 有节点级实现(`trie.rs:78-156`),但 `multiproof` 主路径 `todo!()`(#237 step 1,卡 RocksDB 快照一致性,`nested_hash.rs:410,446`) | **reth 2.3** |
| 写路径 | 持久化时写 branch 缓存(sorted,#20784) | 每块写全部变更节点(含叶)→ sorted 单列表 + 三 RocksDB 并行提交 + merge-blocks | 写放大 gravity 更大(物化的代价),工程对冲充分 |
| 磁盘占用 | hashed state + branch 缓存 | hashed state + 全量 trie(账户数据两处冗余) | **reth 2.3** |

### 5.3 裁定:按运行域分,没有全域赢家

**在 gravity 的运行域(确定性共识、无分叉、连续块流、高 TPS),nested
hash 是更优架构**,且优势是结构性的:reth 2.3 之所以需要 sparse trie +
multiproof,根因是以太坊有分叉——内存 trie 必须能建在任意未持久化祖先
之上,只能是临时的,"每块取证"因此成为必须优化的税。gravity 没有分叉,
可以把 trie 完全物化,这笔税整个不用交:proof_v2 的"缓存哈希跳子树"
在 nested trie 里是天然性质(节点常驻);sorted overlay 针对的"深祖先
叠加"在浅持久化窗口 + 权威 cache 下不存在。把 reth 2.3 引擎搬进
gravity 生产路径,等于把税加回来。

**在以太坊 L1 的运行域,nested hash 走不通**:任意 fork-choice 下物化
trie 需要多版本/写时复制,reorg 需要节点级回滚——gravity 全部靠
"共识确定性"绕开了。所以这不是实现水平之争,是约束决定架构。

### 5.4 nested hash 真正值得从 reth 2.3 拿的(按评估顺序)

1. **内存节点表示**(差距最实):FullNode 光指针槽 136B + 每子一次
   Box 分配,对比 arena 连续内存 + 密集 children(§3.3)。nested 的
   内存节点同样是懒加载临时态(HashNode 展开),生命周期与 sparse
   trie 类似,arena 化技术**直接可套**。先 profile
   `Trie::hash`/`insert_inner` 的分配与 cache miss,热点属实再动。
2. **并行标定**:默认 `trie_parallel_levels=1`(16 路)对大块可能吃
   不满核;上游 256 子树 + 脏叶阈值自适应。gravity 旋钮已有,值得
   压测标定;顺带评估把哈希阶段的 std::thread 统一到 rayon。
3. **LFU 淘汰策略**:PersistBlockCache 现按容量 + 持久化水位半窗
   收紧;`BucketedLfu`(§3.4)的 O(1) 热度追踪可提升 trie 节点命中
   率——独立小件,带 proptest 模型,可直接搬。
4. **proof 服务**:若 `eth_getProof`/witness 有业务需求,proof_v2 的
   "自底向上单遍构造 + 延迟编码"(§2.3-2.4)是补完 #237 `todo!()`
   的现成算法模板(前置仍是 RocksDB 快照一致性)。
5. **块内重叠**(收益存疑,列作设计题):grevm 乐观并行,状态到块尾
   才定稿,无法逐笔流出;可行的弱化版是"边执行边预热 trie 节点
   cache"(拿执行触碰的地址提前点读节点),即上游 prewarm 投机取证
   的 nested 版。

反方向,gravity 已有而上游没有的:权威性 cache 的 tombstone/wipe
语义、三 RocksDB 并行提交、merge-blocks 合并 fsync、块间流水线本身。

## 6. 综合判断:gravity-reth 该拿什么

| 层次 | 结论 |
|---|---|
| **整块移植四大件** | 不建议。它们服务 newPayload 的 sparse-trie 状态根引擎,与 nested trie 互斥(两套引擎对 trie 数据的所有权语义冲突,混链 = state root mismatch);gravity 生产路径已有同问题域的自研解(grevm ↔ prewarm/并行,PersistBlockCache ↔ 跨块保留/overlay,nested 持久节点 ↔ proof 免除)。 |
| **eth-mode 整路径升级** | 仅当 gravity 决定运营 eth-mode 验证/RPC 节点时立项(2-4 周级):payload_processor 全目录采上游 + 挂载全部孤儿 + TreeConfig 对齐,保证 sparse 路径对 V2 表只读。 |
| **拆件借鉴(推荐)** | ① sorted trie writes(`state-root.md` ★★★,与 nested trie 直接兼容);② proof/root worker 池化 + cursor 复用(★★);③ arena/SlotMap 节点表示与 `BucketedLfu`——profiling 证实 nested trie / PersistBlockCache 有对应热点后移植;④ `extend_sorted_vec`/`kway_merge_sorted` 纯算法件按需取用。 |
| **正确性红利** | 上游回退链的"timeout 赛跑 + 失败重算 hashed state 重跑校验"(PR #24506)是与引擎无关的防御模式,gravity eth-mode 回退路径可对照自查。 |

> 相关 PR 索引、逐文件挂载状态、port 优先级表:见
> [`state-root.md`](./state-root.md) 汇总表与
> [`../merge-v2.3.0/upstream-optimizations-gap.md`](../merge-v2.3.0/upstream-optimizations-gap.md) §C/§H。
