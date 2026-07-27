# reth 2.3.x vs 1.8.x — Storage 层改进

> 对比范围: 上游 reth `branch-1.8.3` (4219741510) → `branch-2.3.0` (9384bc53d8),`git log branch-1.8.3..branch-2.3.0 -- crates/storage` 共 374 commits(另含 `crates/static-file`、`crates/prune`、`crates/chain-state`)。
> 目的: 供 gravity-reth 选择性 port。gravity-reth 基于 1.8.3,storage 层已用**自研 RocksDB 全量替换 MDBX** + **nested trie**(`AccountsTrieV2`/`StoragesTrieV2` + `NestedStateRoot`)+ `UnifiedStorageWriter` + pipe-exec 持久化 + `ParallelTxRO`/`commit_view` cache 体系。
> 交叉核对基线: gravity baseline `0cb1687c1`(pre-merge 纯 gravity),当前合并工作树分支 `gravity-reth-merge-v2.3.0`。storage 三组冲突已按 `docs/merge-v2.3.0/STORAGE-RESOLUTION-TODO.md` **整体 keep-gravity**(6 个 storage crate 的 `src/` 还原为 baseline)。本文回答:baseline 之外,上游 2.3.x 还有哪些 storage 改进值得**事后选择性 port**。

## 概述

上游 reth 2.3.x 的 storage 主线可归纳为 6 条:

1. **Storage v2 = 原生 RocksDB 双后端**(最大主线):MDBX 仍是主库,额外挂一个原生 `RocksDBProvider` 只承载 3 个 history/index 表(`AccountsHistory`/`StoragesHistory`/`TransactionHashNumbers`),由 `Metadata` 表里的 `StorageSettings{storage_v2}` 单一开关 + `EitherReader`/`EitherWriter` 路由。**这与 gravity「rocksdb 全量替换 mdbx」是两套正交架构,基本不 port。**
2. **Static file 大扩容**:headers/transactions 变为**只读只写 static file**;新增 `TransactionSenders`/`Receipts`/`AccountChangeSets`/`StorageChangeSets` 段;引入变长文件、per-segment blocks-per-file、changeset 偏移 sidecar(`.csoff`)、两阶段 commit(`sync_all`+`finalize`)。
3. **Overlay 状态子系统**:`OverlayStateProvider`/`OverlayBuilder`/`LazyOverlay`/`StateTrieOverlayManager` + `MerkleChangeSets`/`ChangesetCache`,为"快速计算任意近块的 state root / proof"服务。**与 gravity 的 `NestedStateRoot` + cache 功能重叠、算法不等价。**
4. **低层 KV/编码/计时性能**:栈分配 key 编码、`quanta` TSC 计时、cursor metric 预绑定、读事务句柄池、ZFS/`posix_fallocate` 健壮性、MDBX `APPEND`、可关闭 DB metrics。**多为纯性能且不改磁盘格式,是最安全的 port 候选。**
5. **持久化/裁剪工程化**:`save_blocks` 双线程并行 + 线程池 + 跨块批量 trie 写;可配置最小裁剪距离 + Receipts/Bodies 强制留 64 块(reorg 安全);init-state 大导入 OOM 缓解 + 非零 genesis。
6. **新子系统 BAL(EIP-7928 Block Access List)store**:全新、后端无关的抽象,gravity 保留代码为孤儿、不接入。

关键框架性结论:**上游几乎所有 storage-v2 / 原生 rocksdb / overlay 工作都假设「MDBX 为主 + RocksDB 辅助 + static file」三后端并存,而 gravity 已把 MDBX 换成 RocksDB 全量后端并自研 nested trie**——因此这批"重工程"主线基本不可直接 port(耦合与共识风险都最高)。真正值得 port 的是第 4、5 条里与后端解耦的**纯性能/正确性**改进。

---

## 主题详解

### 1. Storage v2:原生 RocksDB 双后端 + StorageSettings/Metadata 门控 + Either 路由

- **上游做了什么(机制)**
  - 新增独立 `RocksDBProvider`(`Arc<RocksDBProviderInner>` 包一个 `rocksdb::DB`),复用 reth 现有的 `Table`/`Encode`/`Compress` trait:每个逻辑表 = 一个名为 `T::NAME` 的 column family,`get/put/delete/write_batch` 泛型 over `T: Table`。共享 128MB LRU block cache、bloom filter、LZ4+ZSTD 压缩。datadir 下新增 `<datadir>/<chain>/rocksdb`。
  - `ProviderFactory` 新增 `rocksdb_provider` 字段(`RocksDBProviderFactory` trait 暴露),`open` 时先起临时 provider **从 MDBX `Metadata` 表读 `StorageSettings`**,fallback legacy。
  - `Metadata` 表(`Key=String, Value=Vec<u8>`)+ `StorageSettings` Compact 模型 + 三个 trait `MetadataProvider`/`MetadataWriter`/`StorageSettingsCache`(工厂上 `RwLock<StorageSettings>` 缓存)。`StorageSettings` 一度膨胀到 8 个 bool,后 `#22042` **坍缩成单个 `storage_v2: bool`**,`b9969c5b1c(#22954)` 移除 `rocksdb`/`edge` feature gate、默认开启。
  - **只有 3 个 history/index 表迁到 RocksDB**:`AccountsHistory`(`ShardedKey<Address>→BlockNumberList`)、`StoragesHistory`(`StorageShardedKey→BlockNumberList`)、`TransactionHashNumbers`(`TxHash→TxNumber`)。其余(headers/bodies/trie/hashed state)留在 MDBX;receipts/senders/changesets 在 v2 下迁 static file。
  - `EitherWriter{Database|StaticFile|RocksDB(WriteBatch)}` / `EitherReader{Database|StaticFile|RocksDB(RocksReadSnapshot)}` 按 `storage_v2` 逐表路由。RocksDB history 读用 raw iterator `history_info`,与 MDBX 共享 `compute_history_rank`/`needs_prev_shard_check`,并用 **`visible_tip`** 让 RocksDB 快照忽略比 MDBX 快照更新的条目以保持一致。
  - **跨后端原子性**:RocksDB 写累积成 pending `WriteBatch`,在 `DatabaseProvider::commit` 里按固定顺序提交(static file finalize → RocksDB → MDBX 最后),崩溃只会留下 MDBX checkpoint 落后;`rocksdb/invariants.rs` 检测 RocksDB 超前/落后并 heal(裁剪多余或请求 pipeline unwind)。
- **关键 PR / commit**:`00ccb2b9b4`(#20071 实现 RocksDBProvider)、`662c0486a1`(#20253 接入 ProviderFactory)、`e20e56b75e`(#19384 Metadata 表 + StorageSettings)、`e3fe6326bc`(#22042 坍缩成单 `storage_v2`)、`b9969c5b1c`(#22954 默认 v2)、`27cf27a984`(#19554 EitherWriter)、`13c32625bc`(#21063 EitherReader)。
- **涉及文件**:`crates/storage/provider/src/providers/rocksdb/{provider,metrics,invariants,mod}.rs`、`.../providers/database/{mod,provider,builder}.rs`、`.../either_writer.rs`、`.../traits/rocksdb_provider.rs`、`crates/storage/db-api/src/models/metadata.rs`、`crates/storage/storage-api/src/metadata.rs`。
- **与 gravity-reth 的关系**:**强冲突/正交**。gravity `a1d7365bd6(#212)` 已把 **RocksDB 作为全量后端替换 MDBX**(`reth-db` `default=["rocksdb"]`、`generic::` 分发、`implementation/rocksdb/{cursor,mod,tx}.rs`,所有表——含 history、trie v2——都在 RocksDB),并有 `c64bd613e4(#225)` sharded rocksdb 实例。上游的 `RocksDBProvider` / `StorageSettings` / `Metadata` 表 / `EitherReader/Writer` 全部**在 gravity storage 层不存在**,已在合并中删除(`providers/rocksdb/`、`either_writer.rs`、`metadata.rs` 均为删除/孤儿)。二者对"RocksDB 该放什么"的答案完全不同。
- **是否建议 port**:**否(强)**。理由:两套后端架构正交,port 等于把 gravity 的 rocksdb 全量后端拆回"MDBX 主 + RocksDB 辅",与 nested-trie/`commit_view`/`UnifiedStorageWriter`/pipe-exec 深度耦合,数周工程量且高共识风险。难度**高**,与 gravity rocksdb 耦合**极高**。可借鉴的**唯一独立思想**:跨后端 commit 的固定顺序 + `invariants.rs` 崩溃 heal 语义——gravity `commit_view`/`recover_block_number`/`check_consistency_pipe_execution` 已覆盖同类需求,可对照其 heal 完整性。

### 2. hashed-state-as-canonical(仅 *state* 表,changeset 仍为明文) + slot preimage 侧信道

> ⚠️ 勘误:本主题早期草稿曾误称"changeset 存 hashed 键 / 不再有 PlainAccountState"。**已按 `branch-2.3.0` 源码逐条核对更正**——(a) `PlainAccountState`/`PlainStorageState`/`AccountChangeSets`/`StorageChangeSets` 表在 `tables!` 里**依然存在**,schema 未删;(b) v2 只是**运行时**不写 plain state 表(`use_hashed_state()` 门控);(c) **changeset 始终是明文键**(`AccountChangeSets` subkey = `Address` 明文,`StorageChangeSets` key = `BlockNumberAddress` 明文 + 明文 slot)。

- **上游做了什么(机制)**
  - `#21115` **"hashed state as canonical"**:改的是**规范状态表**而非 schema。v2 下当前状态写入/读取走 `HashedAccounts`/`HashedStorages`(keccak 键),**运行时**跳过 `PlainAccountState`/`PlainStorageState` 的写入(`provider.rs:2629` `if !use_hashed_state() { 写 plain }`;读走 `provider.rs:1458` `if use_hashed_state() { 读 hashed } else { 读 plain }`)。**这两张 plain 表仍在 `tables!` 声明中**,给 v1/兼容路径用——用户观察到"PlainAccountState 依然存在"正确。
  - **changeset 保持明文键(v1/v2 一致)**:`AccountChangeSets`(`Value=AccountBeforeTx{address:Address}`,`SubKey=Address`)与 `StorageChangeSets`(`Key=BlockNumberAddress`,`SubKey=B256` slot)在两种模式下**都用明文 address / 明文 slot**;读 changeset 后在**读时**临时 `keccak256(address)` 再查 hashed 表(`provider.rs:1371/1459`)。用户观察到"`AccountChangeSets` subkey 仍是 `Address` 而非 hashed"正确。
  - `#21115` 曾引入 `StorageSlotKey{Plain|Hashed}`(在内存里携带 slot 键来源),但**该类型已被 `#22379` 删除**,`branch-2.3.0` 中 `primitives-traits/src/storage.rs` 文件都不存在。
  - `#22379` **slot preimage DB**——目的恰恰是**让 changeset 保持明文 slot**。因为 v2 下当前 storage 只在 `HashedStorages`(仅 hashed slot),pre-Cancun `SELFDESTRUCT`+recreate("wipe")revert 需要枚举账户全部旧 slot 的**明文**键;若从 `HashedStorages` walk 只能拿到 hashed。于是新增独立 MDBX 环境 `db/preimage/`(`keccak256(slot)→slot`,append-only、不 unwind),`ExecutionStage` 的 `inject_plain_wipe_slots`(`slot_preimages.rs:126`)从 bundle state 收集 `keccak256(slot)→slot`,再把 wipe reverts 改写成**明文 slot 键**——源码注释原话:*"keeping all changeset keys in plain format"*。**即:是 *state* 变 hashed,changeset 靠 preimage 维持明文,不是 changeset 变 hashed。**
- **关键 PR / commit**:`121160d248`(#21115)、`815037e27d`(#22379)。
- **涉及文件**:`crates/storage/provider/src/providers/database/provider.rs`(`use_hashed_state()` 路由)、`crates/stages/stages/src/stages/execution/slot_preimages.rs`(preimage env + `inject_plain_wipe_slots`)。(注:`primitives-traits/src/storage.rs` 仅存在于 #21115..#22379 之间,`branch-2.3.0` 已无。)
- **与 gravity-reth 的关系**:**弱冲突/思想相关**。gravity nested-trie 有自己的 hashed 表体系与 state root 语义,不采用上游的 `use_hashed_state()` 运行时门控与 plain-state-skip。但 **slot preimage 侧信道解决的问题(self-destruct+recreate 的 wipe 需要旧 slot 明文键)与 gravity MEMORY 记录的 `#715 nested-trie wipe+recreate` 修复同源**——上游落成独立 preimage MDBX env,gravity 走 cache trie-node tombstone。
- **是否建议 port**:**否(思想弱参考)**。整套 hashed-canonical 状态表方案与 gravity nested-trie 不兼容,不 port。但 `#22379` 的 wipe+recreate 处理思路建议 state-root team **对照** gravity `#715` 方案做正确性交叉验证(是否有 gravity 未覆盖的 pre-Cancun 边界)。难度**高**,耦合**高**。

### 3. Static file 段扩容 + 变长文件 + `.csoff` 偏移 sidecar + 两阶段 commit

- **上游做了什么(机制)**
  - **段扩容**:`058ffdc21e(#18681)` + `e9598ba5ac(#18788)` 把 headers/transactions 变为**只读只写 static file**(删掉 `StorageLocation::Both` 双写与 DB 读 fallback,header by hash 签名 `&BlockHash→BlockHash`);新增段 `TransactionSenders(#19508)`、`Receipts` 默认写 static file `(#19399)`、以及 change-based 段 `AccountChangeSets(#18882)`/`StorageChangeSets(#20896)`。
  - **change-based 段索引模型**:changeset 按块 append、块内按 address 排序(N 行/块),`SegmentHeader` 增第 5 字段 `changeset_offsets`,`ChangesetOffset{offset,num_changes}` 记录每块的起始行与行数;新 `StaticFile{Account,Storage}ChangesetWalker` 惰性逐块迭代。
  - **`.csoff` sidecar(`#21596`)**:把内嵌 `Option<Vec<ChangesetOffset>>`(每块重写整个 header,O(n))改成**定宽 append-only sidecar 文件**`<jar>.csoff`(16B/记录 = `[offset:u64][num_changes:u64]`,O(1) 随机访问 + O(1) append),header 只留 `changeset_offsets_len:u64`。崩溃 heal 三路对账(header len / NippyJar 行数 / `.csoff` 文件大小)。
  - **变长文件(`#19381`)**:从硬编码 500k 块/文件改为 header 携带权威 `expected_block_range`,manager 维护 `expected_block_ranges_by_max_block` 索引;**per-segment `blocks_per_file`(`#19458`)**可配置。
  - **两阶段 commit(`#20984`)**:`NippyJarWriter::commit` 拆成 `sync_all`(fsync `.dat`/`.idx`,不写 config)+ `finalize`(写 `.conf`),config 写入是原子 commit 点、healing 以此为基准。
- **关键 PR / commit**:`058ffdc21e`(#18681)、`eed34254f5`(#18882)、`ebe2ca1366`(#20896)、`6953971c2f`(#21596)、`c5870312e4`(#19381)、`e5c47fe350`(#19458)、`a73e73adef`(#20984)。
- **涉及文件**:`crates/static-file/types/src/{segment,changeset_offsets}.rs`、`crates/storage/nippy-jar/src/{writer,jar,lib}.rs`、`crates/storage/provider/src/providers/static_file/{manager,writer,mod}.rs`、`crates/storage/provider/src/{changeset_walker,either_writer}.rs`、`crates/storage/db-models/src/storage.rs`。
- **与 gravity-reth 的关系**:**部分正交、部分耦合**。gravity baseline `StaticFileSegment` 只有 `Headers/Transactions/Receipts`(已核实),changesets/senders 都在 gravity 的 RocksDB 全量后端里。新增 changeset/senders 段依赖 `EitherWriter`(storage-v2 基建)→**耦合**;但 headers/txs 只读只写 static file 这一步 gravity 已经等价具备(baseline 也是 static-file headers/txs)。**变长文件、per-segment blocks-per-file、两阶段 commit、`.csoff` 这几项是与"哪些段存在"解耦的 static-file 引擎改进**,对 gravity 现有 3 个段同样适用。
- **是否建议 port**:**弱/选择性**。
  - 段扩容(senders/receipts/changesets 迁 static file):**否**——与 gravity rocksdb 全量后端正交冲突,gravity 已把这些放 rocksdb。
  - **两阶段 commit `sync_all`+`finalize`(#20984)** + **变长文件(#19381)** + **per-segment blocks-per-file(#19458)**:**弱建议 port**——纯 static-file 引擎能力,对 gravity 的 headers/txs/receipts 段有价值(更细的 fsync 边界、灵活文件大小);难度**中**,与 nested-trie/rocksdb 无耦合。
  - `.csoff` sidecar:仅当 gravity 将来引入 changeset static file 段才需要,当前**不 port**。

### 4. Static file 一致性检查 & healing 强化

- **上游做了什么(机制)**
  - `535d97f39e(#20508)`:抽出 `maybe_heal_segment(segment)`——可写模式下取 `latest_writer` 自动 heal 文件级不一致(中断 append 后按最后 committed config 截断数据;中断 prune 后检测"应存在却被删"的数据并触发 unwind);只读模式改为 `check_segment_consistency` 报错不改动。
  - `e93bd0a087(#19819)`:`prune_rows` 把整表行全裁光时,offset 文件长度从错误的 `1` 修为 `1 + OFFSET_SIZE_BYTES`(避免生成畸形空 offset list)。
  - `8eaadf52d8(#18628)`:header healing 里 `pruned_rows = expected - actual` 用 `saturating_sub` 防下溢(崩溃在 data sync 后、config commit 前会出现 actual > expected)。
  - `fb763edb43(#19803)`:把 `min_block`/`max_block`/`expected_block_index`/`tx_index` 四个 `RwLock` map 合并成单个 `StaticFileSegmentIndex`,减少锁、保持 per-segment 索引一致。
- **关键 PR / commit**:`535d97f39e`(#20508)、`e93bd0a087`(#19819)、`8eaadf52d8`(#18628)、`fb763edb43`(#19803)。
- **涉及文件**:`crates/storage/provider/src/providers/static_file/{manager,writer,mod}.rs`、`crates/storage/nippy-jar/src/{writer,lib}.rs`。
- **与 gravity-reth 的关系**:gravity `static_file/manager.rs` 是 keep-gravity,并有自研 `check_consistency_pipe_execution` + `has_receipt_pruning` 参数(#253/#255,pipe-exec 恢复契约)。上游这几项是在**通用 static-file 引擎层**,与 gravity 的 pipe-exec 恢复分支**正交**(gravity 分支在返回前拦截,上游逻辑是分支之后的 fallback)。
- **是否建议 port**:**中(选择性)**。`e93bd0a087(#19819)` offset-file 畸形 和 `8eaadf52d8(#18628)` header healing 下溢是**纯 bug fix**,对 gravity 的 headers/txs/receipts 段同样可能触发,**建议 port**(低成本、正交、修的是崩溃恢复正确性)。`535d97f39e`/`fb763edb43` 是较大 refactor,gravity static_file 是 keep-gravity,port 需要与 `check_consistency_pipe_execution` 手工缝合,难度**中**,价值中等,可延后。

### 5. Overlay 状态子系统(OverlayStateProvider / OverlayBuilder / LazyOverlay / StateTrieOverlayManager)+ trie changesets

- **上游做了什么(机制)**
  - 底座:`be94d0d393(#19068)` 引入 trie changeset 表 `AccountsTrieChangeSets`/`StoragesTrieChangeSets`(记录每块修改前的 trie **节点旧值**)+ `MerkleChangeSets` stage + `TrieReader::trie_reverts(from)`;`a74cb9cbc3(#20997)` 把它改为内存计算 + 共享 `ChangesetCache`(`block_hash→Arc<TrieUpdatesSorted>`,含 `get_or_compute_range`)。
  - `OverlayStateProvider`(`d276ce5758 #18822`):把 `InMemoryTrieCursorFactory` + `HashedPostStateCursorFactory` 叠在 DB 读事务上,cursor 走 trie 时透明合并 `Arc` 持有的 sorted overlay,看到未落盘的 post-state。前置改动是把两个 cursor factory 从借用 `&'a T` 改成 owned `T: AsRef<...>`。
  - `OverlayStateProviderFactory`→`OverlayBuilder`(`f88fae0ea1 #19752` 缓存、`6377a957c1 #23667` 抽出与 provider 解耦的 builder):overlay = (DB reverts 回退到 anchor 块)∪(内存 post-state),按 db-tip 缓存,统一历史-DB 与内存-engine 两条路径。
  - `LazyOverlay`(`905bb95f8b #21133`)→ 持有 `Vec<ExecutedBlock>`、多 anchor `DashMap` 缓存(`344037d04e #23657`)→ 中心化 `StateTrieOverlayManager`(`81272f7f5e #24184`,共享内存块图 + `OverlayCacheKey{anchor,tip}` 缓存 + rayon 后台预计算)。目标:块**执行立即开始**,昂贵的 trie-overlay 合并推迟到算 state root 时才做,并跨所有 payload 校验/RPC 共享。
- **关键 PR / commit**:`d276ce5758`(#18822)、`f88fae0ea1`(#19752)、`a74cb9cbc3`(#20997)、`905bb95f8b`(#21133)、`6377a957c1`(#23667)、`81272f7f5e`(#24184)、`be94d0d393`(#19068)。
- **涉及文件**:`crates/storage/provider/src/providers/state/{overlay,historical,latest}.rs`、`crates/trie/{trie,db}/src/changesets.rs`、`crates/chain-state/src/{lazy_overlay,state_trie_overlay}.rs`、`crates/storage/db-api/src/tables/mod.rs`(trie changeset 表)。
- **与 gravity-reth 的关系**:**功能重叠、算法不等价、强冲突**。gravity 已有 `NestedStateRoot`(#149)+ `ParallelStateProvider`(#26/27/28)+ account/state-root cache(#75/#109)+ `AccountsTrieV2`/`StoragesTrieV2` nested 表,覆盖"快速算近块 state root"的同一诉求,但数据结构与算法完全不同。merge 文档明确:**上游的 `OverlayBuilder`/`ChangesetCache` 若替换 gravity 的 `NestedStateRoot::calculate` 会静默改变 state-root 算法 → 同链不同 root → 共识分裂**。
- **是否建议 port**:**否(强)**。整套不 port。可**参考的思想**:`StateTrieOverlayManager` 的 `(anchor,tip)` 缓存键 + 后台 rayon 预计算 overlay + LazyOverlay 的"执行先行、state-root 惰性"分离——这些调度思想可能对 gravity 的 cache 层有启发,但落地必须建在 `NestedStateRoot` 之上而非替换它。难度**高**,耦合**极高**(直接触及共识关键路径)。

### 6. libmdbx 健壮性 / MDBX 引擎 perf(读事务池、ZFS、SafeNoSync、put-append、page-size)

- **上游做了什么(机制)**
  - `7bb5c579e0(#22631)` **读事务句柄池** `ReadTxnPool`(`crossbeam_queue::ArrayQueue`,cap 256):RO txn drop 时 `mdbx_txn_reset` 保留 reader slot 并入队,`begin_ro_txn` 先 `pop`+`mdbx_txn_renew` 复用,绕开 MDBX reader-table 互斥锁 `lck_rdt_lock`——高并发读(prewarm/multiproof)场景明显收益。依赖 `fcfa8287f6(#23378)` 把 `MDBX_NOTLS` 换成 `MDBX_NOSTICKYTHREADS`(读者不绑线程,txn 可跨线程,是池化前提)。
  - `d2212eca1e(#24108)` 构建期 `MDBX_USE_FALLOCATE=0`:ZFS 上 glibc `posix_fallocate` 模拟写零会误报 `ENOSPC`,改用 `ftruncate` 增长文件。`fe7a4c80b6(#23685)` 启动检测 ZFS(`statfs` magic)并 warn(COW 降低 MDBX 性能)。
  - `a767fe3b14(#18945)` `SafeNoSync` sync 模式(`with_sync_mode`,跳 fsync 提吞吐、崩溃只丢最近事务);`1a68d8e968(#18603)` `DbTxMut::append`(`MDBX_APPEND`,单调递增键跳过 B-tree 查找);`55a49080c6(#19594)` `--db.page-size`(抬高 8TB 上限)。
- **关键 PR / commit**:`7bb5c579e0`(#22631)、`d2212eca1e`(#24108)、`fe7a4c80b6`(#23685)、`fcfa8287f6`(#23378)、`a767fe3b14`(#18945)、`1a68d8e968`(#18603)、`55a49080c6`(#19594)。
- **涉及文件**:`crates/storage/libmdbx-rs/src/{txn_pool,environment,flags,transaction}.rs`、`crates/storage/libmdbx-rs/mdbx-sys/build.rs`、`crates/storage/db/src/{mdbx.rs,implementation/mdbx/{mod,tx}.rs}`、`crates/node/core/src/args/database.rs`。
- **与 gravity-reth 的关系**:gravity **默认 rocksdb 后端**,MDBX 仅作为保留但非默认的 `mdbx` feature 存在。这批改进**只惠及 mdbx 路径,对 gravity 主路径零收益**。当前合并树状态(已核实):ZFS 检测(`db/src/mdbx.rs:warn_if_zfs`)与 `MDBX_USE_FALLOCATE=0`(build.rs)**已随非冲突文件带入**;`txn_pool.rs` 文件在但 `mod txn_pool` 未声明(**孤儿、未激活**);`NOSTICKYTHREADS` 随 mdbx-sys 带入。
- **是否建议 port**:**弱**。gravity 不以 MDBX 为主库,收益有限。已带入的 ZFS/`posix_fallocate`/`NOSTICKYTHREADS` 保留即可(低成本、正交)。读事务池(孤儿)若将来需要跑 mdbx 路径(如 debug 工具、双跑对比)可接线激活;`put-append`/`page-size`/`SafeNoSync` 对 gravity rocksdb 无对应(rocksdb LSM 天然顺序写友好、无 page-size 概念)。难度**低**,耦合**低**。

### 7. Key/Value 编码分配优化(栈分配,不改磁盘格式)

- **上游做了什么(机制)**
  - `ec9c7f8d3e(#21279)` `StoredNibbles::Encoded` `Vec<u8>→ArrayVec<u8,64>`(nibble ≤64,栈分配),并引入 `IntoVec` trait 替代 `Into<Vec<u8>>` bound。
  - `8d97ab63c6(#22314)` `StoredNibblesSubKey` 编码 `Vec<u8>→[u8;65]`(64 nibble 右填 0 + 1 计数字节),**字节与旧 `to_compact` 完全一致**(仅去堆分配,in-memory)。
  - `5ef200eaad(#21200)` `ShardedKey<Address>`/`StorageShardedKey` 特化为定长 `[u8;28]`/`[u8;60]`(去堆分配)。
  - `fc6666f6a7(#22089)` `BranchNodeCompact` 的 hash 编解码从 per-`B256` 循环改为整块 `as_flattened()` 单次 `put_slice` + 解码单次 `copy_from_nonoverlapping`(bulk memcpy,同磁盘字节)。
  - ⚠️ 对照:`80bf5532ac(#22158)` 的 65→33 **packed nibbles** 是**破坏性磁盘格式变更**(storage-v2 专用,2 nibble/byte),新增独立 `Packed*` 类型 + 迁移工具,不属本类。
- **关键 PR / commit**:`ec9c7f8d3e`(#21279)、`8d97ab63c6`(#22314)、`5ef200eaad`(#21200)、`fc6666f6a7`(#22089)。(对照排除:`80bf5532ac` #22158)
- **涉及文件**:`crates/storage/db-api/src/models/{mod,sharded_key,storage_sharded_key}.rs`、`crates/storage/db-api/src/table.rs`、`crates/trie/common/src/nibbles.rs`、`crates/storage/codecs/src/alloy/trie.rs`。
- **与 gravity-reth 的关系**:**正交安全**。这些是纯 in-memory 分配优化、**磁盘字节不变**,而 gravity 的 rocksdb 后端同样调用 `Encode`/`Compact` 编码这些 key(`ShardedKey`/`StoredNibbles`/`BranchNodeCompact` 都在 gravity 热路径)。gravity baseline `StoredNibbles` 仍是 `Vec<u8>`、`impl_compression_*` 宏保留在 db-api——这些优化叠加不冲突。**注意**:65→33 packed(#22158)会改磁盘格式,gravity nested-trie 用 `StoredNibbles`(Vec 编码),**不可 port packed**(否则 `AccountsTrieV2`/`StoragesTrieV2` 旧数据读不出)。
- **是否建议 port**:**强(仅格式不变的 4 项)**。`#22089`/`#21200`/`#21279`/`#22314` 建议 port——纯分配削减、对 rocksdb 后端直接有效、正交无风险。难度**低**(逐 hunk),耦合**低**。**明确排除 `#22158` packed nibbles**(磁盘不兼容,难度高、耦合高)。

### 8. Cursor metric 预绑定 + `quanta` 计时 + 可关闭 DB metrics

- **上游做了什么(机制)**
  - `a12454d2e6(#23654)`:cursor 构造时预绑该表的 `TableOperationMetrics = Arc<[OperationMetrics; Operation::COUNT]>`,记录时从 `(&str,op)` 的 `FxHashMap` 查找降为**数组下标** `metrics[op.index()]`。
  - `7594e1513a(#22211)`:metric 计时 `std::time::Instant`→`quanta::Instant`(读 TSC + 1ms upkeep 线程缓存最近时间戳,免 `clock_gettime` 系统调用)。
  - `d36006a4de(#24806)`:`--db.disable-metrics` → `with_metrics_if(enabled)`,关闭时 cursor metrics 为 `None`、零记录开销。
- **关键 PR / commit**:`a12454d2e6`(#23654)、`7594e1513a`(#22211)、`d36006a4de`(#24806)。
- **涉及文件**:`crates/storage/db/src/metrics.rs`、`crates/storage/db/src/implementation/mdbx/{cursor,mod,tx}.rs`、`crates/primitives-traits/src/lib.rs`、`crates/tasks/src/runtime.rs`。
- **与 gravity-reth 的关系**:cursor prebind(`TableOperationMetrics`)只作用于 **MDBX cursor**,gravity 主路径不走 → 低价值(当前合并树核实:cursor.metrics 仍是 baseline 的 `Option<Arc<DatabaseEnvMetrics>>`,**未采纳** prebind)。`quanta` dep 已进 `db/Cargo.toml` 但 storage src 未接线(metrics.rs 无 `quanta::Instant`)。gravity rocksdb 后端有自己的 metric 体系(`implementation/rocksdb/`)。
- **是否建议 port**:**中(选择性)**。`quanta::Instant`(TSC 计时)与 `--db.disable-metrics` 的**思想对 gravity rocksdb metric 路径同样适用**,建议在 gravity rocksdb metric 侧借鉴(TSC 计时 + 可关闭开关);cursor prebind 因只惠及 mdbx cursor,**弱建议/可选**。难度**低**,耦合**低**。

### 9. `save_blocks` 持久化:双线程并行 + 线程池 + 跨块批量 trie + inline tx numbers

- **上游做了什么(机制)**
  - `f012b3391e(#20993)`:`save_blocks` 在 `thread::scope` 里**双线程分工**——1 个 SF 线程写 headers/txs/senders/receipts(static file),主线程做全部 MDBX 工作(`TransactionHashNumbers` 排序批量写、逐块 `insert_block_mdbx_only`、state/hashed/trie、history indices、pipeline stages)。新 `SaveBlocksMode{Full|BlocksOnly}` + `StateWriteConfig{write_receipts,write_account_changesets}` 让主线程**跳过** SF 线程已负责的 receipts/changeset(避免双写)。
  - `6b8e40c061(#21764)`:SF 写扇出改用静态 rayon 池 `STORAGE_POOL`(16 线程,`in_place_scope`),免每次 persistence 新建 OS 线程。
  - `c11c13000f(#21142)`:**跨块合并 trie updates 成一个,单 cursor 一次写**(1 块直接用;<30 块 `extend_ref` + `Arc::make_mut` COW;≥30 块 k 路 `merge_batch`)。
  - `a5ce4866f6(#24318)`:每块起始 tx number 用 `SmallVec<[TxNumber;4]>`(≤4 块内联栈);`8367ba473e(#20878)`:7 个 save_block 分步 histogram + trie input size metrics。
- **关键 PR / commit**:`f012b3391e`(#20993)、`6b8e40c061`(#21764)、`c11c13000f`(#21142)、`a5ce4866f6`(#24318)、`8367ba473e`(#20878)。
- **涉及文件**:`crates/storage/provider/src/providers/database/{provider,metrics}.rs`、`crates/storage/provider/src/{either_writer,storage_threadpool}.rs`、`crates/storage/storage-api/src/state_writer.rs`、`crates/chain-state/src/lazy_overlay.rs`。
- **与 gravity-reth 的关系**:**机制冲突、思想部分可借鉴**。gravity 用 `UnifiedStorageWriter`(`append_blocks_with_state`,保留)+ 自研 sharded rocksdb 持久化(#225)+ pipe-exec `pipe_test` 门控,且并行执行走 grevm。上游双线程 = **MDBX 线程 ∥ SF 线程**,前提是 MDBX+SF 双写;gravity 是 rocksdb+SF,机制不直接套用。`SaveBlocksMode`/`StateWriteConfig`/`WriteStateInput` 在 gravity storage 层不存在(已删,writer 走 `StorageLocation` + `OriginalValuesKnown`)。
- **是否建议 port**:**否(机制)/ 弱(思想)**。整套双线程 `save_blocks` 不 port(与 `UnifiedStorageWriter`/sharded rocksdb 重接工程量大)。但 **`c11c13000f(#21142)` 跨块合并 trie updates 一次写** 的思想对 gravity `write_trie_updatesv2`/nested-trie 批量写有直接借鉴价值(gravity baseline 已有 `c11c13000f` 同源的 `21142` batch 思想?需核实——见备注);`SmallVec` inline、分步 metrics 是低成本正交小优化。难度**高**(整套)/**低**(单点思想),耦合**高**。

### 10. 裁剪改进:可配置最小裁剪距离 + Receipts/Bodies 强制留 64 块 + rocksdb 批量 prune + flush&compact

- **上游做了什么(机制)**
  - `c91845ae44(#23082)`:硬编码 `MINIMUM_UNWIND_SAFE_DISTANCE`(32*2+10000)变成运行时 `PruneConfig.minimum_pruning_distance` + `--prune.minimum-distance`,门控 receipts 何时可裁(`PruneMode::Distance(min).should_prune(first,tip)`)。
  - `013dfdf8c8(#21520)`:新增 `MINIMUM_DISTANCE=64`,`PruneSegment::min_blocks()` 对 `Receipts`/`Bodies` 返回 64(原 0),保证 reorg 时 `canonical_block_by_hash` 能重建 `ExecutedBlock`;`Full` 模式对 `min_blocks>0` 的段退化为 `Distance(min_blocks)`。
  - `89be91de0e(#21767)`:RocksDB history 裁剪从 per-address `iterator_cf` 改为 `raw_iterator_cf` + `prune_*_batch`(对**已排序**目标单遍 seek);`95f6bbe922(#21783)`:`reth prune` 后 `flush_and_compact()`(flush memtable + `compact_range_cf` 全 CF 回收磁盘)。
- **关键 PR / commit**:`c91845ae44`(#23082)、`013dfdf8c8`(#21520)、`89be91de0e`(#21767)、`95f6bbe922`(#21783)。
- **涉及文件**:`crates/config/src/config.rs`、`crates/node/core/src/args/pruning.rs`、`crates/prune/types/src/{target,segment,mode}.rs`、`crates/storage/provider/src/providers/rocksdb/provider.rs`、`crates/prune/prune/src/segments/user/{account_history,storage_history}.rs`、`crates/cli/commands/src/prune.rs`。
- **与 gravity-reth 的关系**:`minimum_pruning_distance` 与 Receipts/Bodies 留 64 块是**后端无关的裁剪逻辑/正确性**(prune-types crate),与 gravity nested-trie/rocksdb 无耦合。rocksdb 批量 prune(#21767)是上游**原生 rocksdb** provider 的实现,gravity rocksdb 结构不同,但"sorted targets 单遍 seek"思想可借鉴;`flush_and_compact` 对 gravity rocksdb 直接适用(gravity 有 `acc458846c` flush 相关经验)。
- **是否建议 port**:**中(选择性)**。`013dfdf8c8(#21520)` Receipts/Bodies 强制留 64 块 **建议 port**(修 reorg 边界正确性,后端无关,难度低);`c91845ae44(#23082)` 可配置 min-distance **弱建议 port**(运维灵活性,后端无关)。rocksdb 批量 prune 思想 + `flush_and_compact` 建议在 gravity rocksdb prune 路径**借鉴实现**(非直接 port,结构不同)。难度**低-中**,耦合**低**。

### 11. BAL(EIP-7928 Block Access List)store

- **上游做了什么(机制)**:`165a80441b(#23596)` 定义后端无关抽象 `BalStore`(`insert(hash,number,Bytes)`/`get_by_hashes`/`get_by_range`)+ `BalStoreHandle`(`Arc<dyn BalStore>`)+ `BalProvider` + `NoopBalStore`,BAL 以不透明 `Bytes` 存储;`ddb3819ec9(#23873)` `InMemoryBalStore` + `BalConfig` 保留期(`BTreeMap<BlockNumber,hashes>` 有序驱逐);`1a45f10d07(#24023)` 显式 `prune(tip)`;`b29668ec7d(#24071)` `flush()` durability hook。整条 trait 表面先于任何持久后端建好。
- **关键 PR / commit**:`165a80441b`(#23596)、`ddb3819ec9`(#23873)、`1a45f10d07`(#24023)、`b29668ec7d`(#24071)、`8940f2f0d6`(#23918 通知流)。
- **涉及文件**:`crates/storage/storage-api/src/bal.rs`、`crates/storage/provider/src/bal.rs`。
- **与 gravity-reth 的关系**:merge 已决策 **BAL 保留代码为孤儿文件**(`storage-api/src/bal.rs`、`provider/src/bal.rs` 在磁盘但 `lib.rs` 不 `mod` 声明),**不接 BAL 并行**(并行走 grevm),全 crate 无 RPC/链上消费方(`grep BalProvider crates/rpc/` 为空)。
- **是否建议 port**:**否**。gravity 当前无 EIP-7928 需求,BAL 是纯新增子系统。保持孤儿即可。将来若支持 EIP-7928 再整体评估。难度 n/a,耦合**低**(隔离文件)。

### 12. init-state / import 健壮性:流式 JSON + 分块 commit + 非零 genesis

- **上游做了什么(机制)**
  - `0b33057414(#23469)`:state-dump 导入不再走高层 `insert_state`,改为 `write_account_to_db` 直接用 cursor `append`/`append_dup` 写各表,`STORAGE_COMMIT_THRESHOLD` 超阈值即 commit 旧 tx 开新 tx(限制脏页、防 OOM),state root 走 `compute_state_root_chunked`。
  - `0e5fdaaaa9(#23825)`:`parse_accounts` 从 `read_line`+`from_str`(累积 `String`)改为 `serde_json::Deserializer::from_reader().into_iter()` **流式**(常数内存);阈值收紧 500k→100k、新增 `STATE_ROOT_COMMIT_THRESHOLD=25_000`、每 chunk 重置 `seen_bytecodes`。
  - `d8acc1e4cf(#19877)`:支持**非零 genesis block number**——`make_genesis_header` 用 `genesis.number`,`init.rs` 全程 `0`→`genesis_block_number`,stage checkpoint `StageCheckpoint::new(genesis_block_number)`,static file `append_header_direct` + `set_block_range(n,n)`。
- **关键 PR / commit**:`0b33057414`(#23469)、`0e5fdaaaa9`(#23825)、`d8acc1e4cf`(#19877)、`6cb04766eb`(#24126 v2 导入,与主题 1 耦合不 port)。
- **涉及文件**:`crates/storage/db-common/src/init.rs`、`crates/cli/commands/src/init_state/mod.rs`、`crates/chainspec/src/spec.rs`、`crates/storage/provider/src/providers/static_file/{writer,manager}.rs`。
- **与 gravity-reth 的关系**:gravity `init.rs` 是 **keep-gravity**(`NestedStateRoot` + `TrieWriterV2` + `insert_world_trie`,rocksdb 路径),上游的 `write_account_to_db`(MDBX append)机制不直接套用。merge **已 cherry-pick** 非零 genesis 的 stage checkpoint 片段(`StageCheckpoint::new(genesis_block_number)` 替代 `Default::default()`——见 STORAGE-RESOLUTION-TODO §5 / storage-db-and-mdbx §init.rs)。完整非零 genesis(static file `append_header_direct` 等)与大导入流式/分块 commit 未 port。
- **是否建议 port**:**中(选择性)**。大 state 导入 OOM 缓解的**思想**(流式 deserializer + 分块 commit 限脏页)对 gravity 的 rocksdb init 路径有价值,但需按 rocksdb/NestedStateRoot 适配(不是照搬)。非零 genesis 若有需求可补齐 static-file 侧;stage-checkpoint 片段已在。难度**中**,耦合**中**(触及 init 与 state-root)。

### 13. Schema / API 简化:移除 total difficulty 表 + `commit()->Result<()>` + `LastSafeBlock` 改名

- **上游做了什么(机制)**:`563ae0d30b(#16660)` 丢弃 total difficulty 表支持、`e21048314c(#19151)` 从 `HeaderProvider` 移除 `header_td*`;`27fbd9a7de(#21077)` `DbTx::commit()` `Result<bool>→Result<()>`(bool 无信息量);`082b5dad37(#18992)` `ChainStateKey::LastSafeBlockBlock`→`LastSafeBlock` 拼写修正。
- **关键 PR / commit**:`563ae0d30b`(#16660)、`e21048314c`(#19151)、`27fbd9a7de`(#21077)、`082b5dad37`(#18992)。
- **涉及文件**:`crates/storage/storage-api/src/{transaction,noop}.rs`、`crates/storage/db-api/src/{transaction,tables/mod}.rs`、`crates/storage/storage-api/src/header.rs`。
- **与 gravity-reth 的关系**:**冲突**。gravity 未跟进 TD 清理(baseline 仍保 `header_td`/`header_td_by_number`,`blockchain_provider`/`database` 多处消费);`commit()` gravity 是 `Result<bool>` + `commit_view()`,**RocksDB view 语义依赖 bool**(`ff103f976a #313`);`ChainStateKey` gravity 仍用 `LastSafeBlockBlock`(编码字节 `[1]` 不变)。
- **是否建议 port**:**弱/否**。TD 移除会级联改下游、价值低,**不建议**(除非做整仓对齐上游 header API);`commit()->Result<()>` 与 gravity `commit_view` 语义冲突,merge 文档建议 **保 gravity `Result<bool>`**(或迁 bool 到 `try_commit`),**不建议直接 port**;`LastSafeBlockBlock→LastSafeBlock` 是纯 rename(字节不变),可选做机械改名。难度**低**但级联广,耦合**中**。

---

## 汇总表

| # | 主题 | 上游价值 | 与 gravity 冲突? | 建议 port | 优先级 | 难度 |
|---|------|---------|------------------|-----------|--------|------|
| 1 | Storage v2 原生 RocksDB 双后端 + StorageSettings/Metadata + Either | 高(对上游) | 强冲突(架构正交) | 否(强) | — | 高 |
| 2 | hashed-state-canonical + slot preimage 侧信道 | 中 | 弱冲突;wipe 思路相关 | 否(思想参考 #715) | 低 | 高 |
| 3 | Static file 段扩容 + 变长文件 + `.csoff` + 两阶段 commit | 高 | 段扩容冲突;引擎改进正交 | 弱(仅 #20984/#19381/#19458) | 中 | 中 |
| 4 | Static file 一致性 & healing 强化 | 中 | 正交(pipe-exec 分支之外) | 中(#19819/#18628 bug fix) | 中 | 低-中 |
| 5 | Overlay 子系统 + trie changesets | 高 | 强冲突(算法不等价,共识风险) | 否(强) | — | 高 |
| 6 | libmdbx 健壮性/perf(读事务池/ZFS/…) | 中 | 正交(仅惠及非默认 mdbx) | 弱(部分已带入) | 低 | 低 |
| 7 | KV 编码栈分配(格式不变的 4 项) | 中 | 正交(rocksdb 同样受益) | 强(排除 #22158 packed) | 高 | 低 |
| 8 | Cursor metric 预绑定 + quanta + 可关 metrics | 中 | 正交 | 中(quanta/disable 思想) | 中 | 低 |
| 9 | save_blocks 双线程 + 批量 trie + inline | 高(对上游) | 机制冲突;#21142 思想可借鉴 | 否(整套)/弱(#21142 思想) | 低-中 | 高 |
| 10 | Prune:min-distance + 留 64 块 + rocksdb 批量/compact | 中 | 后端无关部分正交 | 中(#21520/#23082;rocksdb 借鉴) | 中 | 低-中 |
| 11 | BAL(EIP-7928)store | 低(gravity 无需求) | 无(孤儿隔离) | 否 | — | — |
| 12 | init-state 大导入 OOM + 非零 genesis | 中 | 机制需适配;checkpoint 已 cherry-pick | 中(思想 + 非零 genesis 可选) | 低-中 | 中 |
| 13 | 移除 TD 表 + commit()->Result<()> + LastSafeBlock 改名 | 低 | 冲突(gravity 保 header_td / Result<bool>) | 弱/否 | 低 | 低(级联广) |

**推荐优先 port 清单(高置信、正交、低风险)**:
- **#7** KV 编码栈分配(`#22089`/`#21200`/`#21279`/`#22314`)——纯分配削减,rocksdb 后端直接受益,磁盘格式不变。
- **#10** Receipts/Bodies 强制留 64 块(`#21520`)+ 可配置 min-distance(`#23082`)——reorg 正确性 + 运维灵活性,后端无关。
- **#4** static-file offset/underflow bug fix(`#19819`/`#18628`)——崩溃恢复正确性,正交。
- **#8** `quanta` TSC 计时 + `--db.disable-metrics` 思想——引到 gravity rocksdb metric 路径。

**明确不 port**:#1(原生双后端)、#5(overlay 子系统)、#11(BAL)、#2/#9 整套——均与 gravity rocksdb 全量后端 / nested-trie / UnifiedStorageWriter 深度耦合或存在共识风险。

---

## 备注 / 存疑

1. **合并树是 WIP 快照**:`STORAGE-RESOLUTION-TODO.md`(HEAD `e6b7e5ba32`)记录"storage 6 crate 的 `src/` 整体还原 baseline",但当前分支 HEAD 已到 `200c31273c`,实测有若干非冲突文件带入了上游(`db/src/mdbx.rs` 的 `warn_if_zfs`、`mdbx-sys/build.rs` 的 `MDBX_USE_FALLOCATE=0`、`db/Cargo.toml` 的 `quanta` optional dep、`libmdbx-rs/src/txn_pool.rs` 孤儿文件)。本文的"已带入/未采纳"以当前工作树 `git grep` 实测为准,后续合并推进可能变化,port 前请复核。

2. **#9 `c11c13000f`(跨块批量 trie 写)是否已在 gravity**:gravity baseline `671680af37`/`c11c13000f` 附近是否已含"批量 trie updates 一次写"未逐行核实;`docs/merge-v2.3.0/trie-all-layers.md` 与 state-root team 应确认 `write_trie_updatesv2` 当前是否已跨块合并,以免重复 port。

3. **#7 packed nibbles(#22158)磁盘兼容性红线**:`AccountsTrieV2`/`StoragesTrieV2` 的 `StoredNibbles`/`StoredNibblesSubKey` 现有编码(`Vec<u8>`/`[u8;65]`)是链上磁盘格式,**严禁**换成 33-byte packed(`storage-api-and-traits.md` 开放问题 §1 同此结论)。可 port 的仅是"同字节、去堆分配"的 4 项(#22089/#21200/#21279/#22314),port 后需 `..._matches_to_compact` 类断言验证字节不变。

4. **#4 heal_segment / #10 flush_and_compact 与 gravity 恢复分支的缝合**:gravity 有 `check_consistency_pipe_execution` + `has_receipt_pruning`(#253/#255)与 `acc458846c` flush 经验。port 上游 static-file healing / rocksdb compact 时须确认不与 gravity pipe-exec 恢复路径冲突(上游逻辑应落在 gravity 分支返回**之后**的 fallback)。

5. **#8 gravity rocksdb metric 体系是否独立于 db/src/metrics.rs**:cursor prebind 与 quanta 目前只作用于 `implementation/mdbx/`,gravity rocksdb 走 `implementation/rocksdb/` 自有 metric。要让 quanta/disable-metrics 惠及主路径,需在 rocksdb metric 侧单独落地,不能靠 port mdbx 侧。

6. **#2 slot preimage 与 gravity #715 的正确性对照**:上游用独立 `db/preimage/` MDBX env 处理 self-destruct+recreate 的明文 slot,gravity 用 nested-trie tombstone(MEMORY: `#715`)。二者不是 port 关系,但建议 state-root team 用上游的 wipe 测试用例回归 gravity 方案,查是否有未覆盖的 pre-Cancun 边界。

7. **本文聚焦 top ~13 主题**,未逐一展开全部 374 commits;大量属于同主题的小 perf/lint/clone-removal(如 `#22918`/`#22906`/`#22742` 去 changeset 克隆、`#23386` sort_unstable、`c924902b89` 避免 receipt 克隆)可在对应主题下顺手采纳,价值低但零风险。
