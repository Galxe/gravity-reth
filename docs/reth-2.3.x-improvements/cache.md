# reth 2.3.x vs 1.8.x — Cache / 缓存与预热改进

> 对比范围: 上游 reth `branch-1.8.3` (4219741510) → `branch-2.3.0` (9384bc53d8)
> 目的: 供 gravity-reth 选择性 port。gravity-reth 用 grevm 并行执行 + 自研 account/state-root cache（`PersistBlockCache` + nested-trie + rocksdb），主执行路径是 `crates/pipe-exec-layer-ext-v2`，而非上游 engine-tree 的 `PayloadProcessor`。

---

## 概述

reth 1.8.3 → 2.3.0 在“缓存 / 预热”方向上不是零散小补丁，而是围绕 **`PayloadProcessor` 这一条主线**做了系统化重构，可归纳为四组正交能力：

1. **跨块执行状态缓存**（cross-block execution cache）：把上一个块执行后 warm 的 account/storage/bytecode 保留下来，键到 parent block hash，供下一个块直接命中。1.8.3 用 `mini_moka`(LRU+TTL)，2.3.0 换成 **`fixed-cache`**（arena、O(1) epoch 清空、cache-line 对齐），并抽出独立 crate **`reth-execution-cache`**（PR #23209），引入 **单借出（single-checkout）+ usage guard** 并发模型。
2. **并行交易预热**（prewarming）：在真正的顺序执行之前/同时，用一个 **rayon 专用线程池**投机执行本块交易，只为 warm 上述执行缓存并产出 trie proof targets。2.3.0 从“手写 worker 池”改成 **rayon `par_iter` work-stealing**（#22521 + 专用池 #22108），加入大量启发式（小块跳过、按序发送、禁用 balance 检查、withdrawal 预取、`executed_tx_index` 追赶跳过）。
3. **Sparse trie as cache**（#21583）：把 1.8.3 的一次性 `MultiProofTask` 改成 **`SparseTrieCacheTask` + `PreservedSparseTrie`**，跨块保留已 reveal 的 trie 节点 / storage root / 已取 proof targets，用 **LFU** 限界热数据。这是本区间最大的 trie 缓存结构性变化（**本文只讲缓存面，state-root 算法本身见另一篇**）。
4. **可独立复用的小型缓存**：**precompile cache**（moka + LRU，#20502/#20527，含 stateful 防护 #23619）与 revm 层的 **`CachedReads`**（#25048 容量构造器）。这两者与 executor / engine-tree **解耦**，理论上可整块搬到别的执行器。

此外 2.3.0 新增了一整套 **BAL（EIP-7928 Block Access List）驱动的 prefetch/prewarm**（#20468、#25003 的 128 线程 `BalPrewarmPool`），本质是共识/并行执行特性，缓存只是副产品。

### 与 gravity 的关系（关键判断）

- 当前 merge tree **已经把上游这套 payload_processor / `reth-execution-cache` / precompile_cache 全部代码合入**（`crates/engine/execution-cache/`、`crates/engine/tree/src/tree/payload_processor/{prewarm,sparse_trie,precompile_cache,bal,...}` 均在）。但 **gravity 的块处理热路径是 `crates/pipe-exec-layer-ext-v2` + grevm**，走 `ParallelExecutor` / `ParallelState`，state root 走 `storage.state_root()`（nested-trie），跨块缓存走 **`PersistBlockCache`**（`crates/storage/storage-api/src/cache.rs`，按 block-height 版本化，含 tombstone / wipe，绑定 rocksdb persist watermark）。
- 因此上游“主线 1/2/3”（cross-block execution cache、prewarming、sparse-trie-as-cache）在 gravity 上是 **“代码在树里、但不在 grevm+pipe-exec 热路径上”**：它们的目标（跨块 warm state、并行 warm、跨块保留 trie 节点）**gravity 已用 `PersistBlockCache` + grevm + nested-trie 以另一套机制实现**，属于**重叠 / 平行方案**，不建议整块 port，只应做“思想对照 + 局部借鉴”。
- 真正**正交、可直接复用**的是 **precompile cache** 与 **`CachedReads`**（executor 无关）。其中 precompile cache 对 gravity 需特别注意 **gravity 自带 stateful precompile**（`randomness_by_height`、`mint`、`bls_pop_verify`），必须套用上游 #23619 的“不缓存 stateful precompile”防护。
- **BAL** 与 gravity 正交（gravity 未走 EIP-7928 + reth 引擎树），不作为缓存项 port。

---

## 主题详解

### 1. `PayloadProcessor` 架构总线（预热 + multiproof/sparse-trie + 跨块缓存的编排）

- **上游做了什么**：`PayloadProcessor::spawn`（`payload_processor/mod.rs:287`）在收到一个 payload 时，一次性拉起三条并行流水线并用 channel 串起来：
  1. `spawn_tx_iterator`：把交易同时喂给 **prewarm** 通道和 **execute** 通道；小块（`< SMALL_BLOCK_TX_THRESHOLD=30`）走顺序转换，大块走 rayon `ForEachOrdered`，且**头 4 笔（`PARALLEL_PREFETCH_COUNT`）先串行发出**以避免 rayon 调度让 index 0 延迟 ~1ms（#22305）。
  2. `spawn_state_root` → `spawn_sparse_trie_task`：拉起 proof worker 池 + `SparseTrieCacheTask`；小块（`≤ SMALL_BLOCK_PROOF_WORKER_TX_THRESHOLD=30`）**减半 proof workers**。
  3. `spawn_caching_with`：拉起 `PrewarmCacheTask`，按 `parallel_bal_execution / disable_prewarming / 小块` 选择 `PrewarmMode::{BlockAccessList, Skipped, Transactions}`（`mod.rs:526`）。
  - 执行缓存通过 `cache_for(parent_hash)`（`mod.rs:584`）取出：命中则复用上一个块 warm 的 `SavedCache`，否则新建 `ExecutionCache::new(cross_block_cache_size)`。
  - `wait_for_caches`（`mod.rs:218`）在下一块开始前**并行等待** execution cache 与 sparse trie 两把锁可用，并记录等待时长指标（#21800）。
- **关键 PR / commit**：整体重构横跨 #19822、#21128、#21583、#22521、#23209 等（见下各主题）。
- **涉及文件**：`crates/engine/tree/src/tree/payload_processor/mod.rs`（1.8.3 862 行 → 2.3.0 1464 行）。
- **与 gravity 的关系**：**平行方案**。gravity 的编排在 `pipe-exec-layer-ext-v2/execute/src/lib.rs`（`ParallelExecutor` + `PersistBlockCache` + `storage.state_root`），完全不经过 `PayloadProcessor`。这条主线是理解上游其余缓存主题的“骨架”，但不作为 port 单元。
- **是否建议 port**：**否**（整线）。骨架与 grevm/pipe-exec 深度耦合；只从中挑正交子模块（见主题 5/6）。难度：高。

### 2. 跨块执行状态缓存（`reth-execution-cache`：`ExecutionCache` / `SavedCache` / `PayloadExecutionCache`）

- **上游做了什么**（缓存什么/何时预热/如何失效/目标加速）：
  - **缓存什么**：三张 `FixedCache`（`crates/engine/execution-cache/src/cached_state.rs`）：account = `FixedCache<Address, Option<Account>>`、storage = **扁平** `FixedCache<(Address, StorageKey), StorageValue>`（不再是 1.8.3 的层级 `Address → AccountStorageCache`）、code = `FixedCache<B256, Option<Bytecode>>`。预算按 ~5.56% / 88.88% / 5.56% 切分，默认 `cross_block_cache_size = 4GB`（64-bit，`config.rs:63`）。
  - **何时 warm**：(a) prewarm 任务通过 `CachedStateProvider::new_prewarm`（`CacheFillMode::FillOnMiss`）在投机执行时 miss 即回填；(b) `on_inserted_executed_block`（`mod.rs:740`）在本地构建/插入块后把 `BundleState` 灌入缓存（bytecode→storage→account 顺序），供下一块 warm。
  - **键 / 复用**：`SavedCache` 绑定 `executed_block_hash`；`get_cache_for(parent_hash)` 命中即复用，parent hash 不匹配则 `clear_with_hash`（清 storage+account、**保留 bytecode**，因为 code 不可变）。
  - **如何失效**：① 校验失败 → prewarm 的 `save_cache` 把 slot 清空（#21282，`prewarm.rs`）；② parent 不匹配（reorg/分叉）→ 清 storage/account；③ **单借出模型**：`SavedCache` 内含 `Arc<()>` usage guard，`get_cache_for` 只在 `strong_count==1`（无人占用）时借出，否则返回 `None` 并记 “in use” 指标（#21265）；④ `on_inserted_executed_block` 若发现缓存**正被占用**则**跳过更新**（#24384，`mod.rs:756`），避免 read-under-write；⑤ pre-Dencun 跨 tx SELFDESTRUCT 且账户带 code → 无法精确失效单个 slot，**整表清空 + 一次性告警**。
  - **并发 / 结构演进**：`PayloadExecutionCache` 从 `RwLock` 改 `Mutex`（#23387）；从 engine-tree 抽出独立 crate `reth-execution-cache`（#23209）以便和 payload builder 共享（#23242）；`CacheFillMode` 拆分 policy（`LookupOnly` vs `FillOnMiss`，#24568）；metrics 与 `SavedCache` 解耦（#23552，`CachedStateMetricsSource` = Engine/Builder 标签）。**backing 从 `mini_moka`(LRU+TTL) 换 `fixed-cache`(arena、power-of-two、O(1) epoch 清空、128B cache-line 对齐)**（#21128）。
  - **目标加速**：让下一块的执行/预热对热账户/热槽/热 code **零 DB 命中**。
- **关键 PR / commit**：#23209 (5a66d0064c)、#21128 (c137ed836f)、#23242 (e3dbdbb115)、#24568 (fd96cb2bd6)、#24585 (f88acf7f5a)、#24384 (44e891fc43)、#23387 (83e6677078)、#19822 (194a01adda)、#24875 (84d8e471ea, overlay 死锁)、#21282 (dbdaf068f0)、#21354 (22a68756c7)、#23552 (199b7460a9)、#22895 (12a3022a2a)、#23249 (3208a4a615)、#20143 (64909d33e6, `--engine.disable-state-cache`)。
- **涉及文件**：`crates/engine/execution-cache/src/{lib.rs,cached_state.rs}`（2.3.0 新 crate）；1.8.3 原位 `crates/engine/tree/src/tree/cached_state.rs`；wiring 在 `payload_processor/mod.rs`。
- **与 gravity 的关系**：**重叠 / 平行**。gravity `PersistBlockCache`（`storage-api/src/cache.rs`）已实现同类跨块 account/storage/bytecode + trie-node 缓存，但采用**按 block-height 版本化 + tombstone + wipe（self-destruct）+ persist-watermark 驱逐**，与 rocksdb + nested-trie + grevm 强绑定。上游的 fixed-cache/单借出/parent-hash 键都是为 reth 顺序执行 + engine tree 设计。
- **是否建议 port**：**否（整块）**；**弱建议借鉴**其两点思想到 `PersistBlockCache`：(a) `CacheFillMode` 的 “LookupOnly vs FillOnMiss” 显式策略；(b) SELFDESTRUCT/wipe 导致整表清空的告警可观测性（gravity 已有 wipe 层，语义上更精细，可对照确认无遗漏）。难度：低（仅借鉴），与 grevm/pipe-exec 耦合：低。

### 3. 并行交易预热（`prewarm.rs`）

- **上游做了什么**：预热任务对每笔交易做一次“**只读投机执行**”（`transact`，禁用 nonce/balance 检查以让链内后付款/自筹资金的 tx 也能跑通，#21941），把 miss 到的 account/storage/code 回填进 `CachedStateProvider`（`new_prewarm`，`PREWARM=true` const generic，#22106/#22107），并从执行产生的 `EvmState` 提取 **`MultiProofTargetsV2`**（变更的 account/storage hash）直接发给 `SparseTrieCacheTask`（#1383c151c9）。额外 **预取 withdrawal 地址**（#21966）。
  - **并发模型**：从 1.8.3 的“手写 64 并发 worker + 手动分批”换成 **rayon `par_iter` + `in_place_scope`**（#22521），并使用 **专用 prewarming rayon 池**（#22108，`--engine.prewarming-threads`），避免与主执行抢 CPU。
  - **终止 / 追赶**：`terminate_execution: AtomicBool`（顺序执行完成即置位，各 worker 在分派前/执行前后检查 `should_stop`，#01f3e58229）；`executed_tx_index: AtomicUsize`（顺序执行每完成一笔就 +1，prewarm worker 见到 `index < executed_tx_index` 直接跳过，#22647）——两条路径合力减少无用功。
  - **启发式**：`SMALL_BLOCK_TX_THRESHOLD=5`（模块级，`mod.rs:85`）以下的块直接 `PrewarmMode::Skipped`，避免 spawn 空转 worker（#22066/#22094；#22059 “按 gas 跳过”被 revert 为按 tx count）；按**交易顺序**发送到预热通道以改善缓存局部性（#22650）；`multiproof_targets_from_state` 预 `reserve` 目标集减少分配（#24198）。
  - **目标加速**：在真正顺序执行发生前把执行缓存与 sparse trie “烧热”，让主执行/状态根近乎全命中。
- **关键 PR / commit**：#22521 (c8c5f8886d)、#22108 (0dd47af250)、#22543 (de5688a76e)、#21429 (768a687189)、#22094 (7ff78ca082)、#22066 (aa983b49af)、#21941 (95ed377135)、#21966 (67f89fa4b2)、#22650 (1e2e33e951)、#22647 (dca5852213)、#22305 (8970f82aaf)、#24198 (94fa8ebe1b)、#22106 (cd8ec58703)、#22107 (81c83bba68)、#20445 (5edc16ad85)。
- **涉及文件**：`crates/engine/tree/src/tree/payload_processor/prewarm.rs`（460 → 852 行）；`mod.rs` 的 `spawn_tx_iterator/spawn_caching_with`。
- **与 gravity 的关系**：**重叠且被替代**。gravity 的 **grevm 本身就是并行 MVCC 执行器**——它在执行阶段就并行地读/warm 状态，无需“先投机执行一遍再顺序执行”的两段式预热；上游预热是为 reth 的**顺序**执行器 warm 缓存。此外上游预热产出的是喂给 reth **sparse trie** 的 proof targets，而 gravity 用 **nested-trie**。二者目标一致、机制互斥。
- **是否建议 port**：**否**。与 grevm 属重复功能，且深度耦合 reth executor + sparse trie。难度：高、收益负。（唯一可留意的“思想”是 `executed_tx_index` 式的“预热追赶顺序执行”协作停止——但 grevm 无对应两段式流程，无处安放。）

### 4. Sparse trie as cache（`SparseTrieCacheTask` + `PreservedSparseTrie`）

- **上游做了什么**（仅缓存面）：1.8.3 的 `MultiProofTask` 是**一次性**的（每块 reveal → 算 → 丢）。2.3.0 引入 `SparseTrieCacheTask` + `SharedPreservedSparseTrie`（`Arc<Mutex<Option<PreservedSparseTrie>>>`），**跨块保留整棵 `SparseStateTrie`**：
  - **保留什么**：已 reveal 的 branch/account/storage 节点、**已计算的 storage root 缓存**（`storage_root_cache`，#20838）、**已取的 proof targets**（`fetched_account_targets` / `fetched_storage_targets`，命中则不再向 proof worker 发请求，#21612/#22355）、可复用的 account RLP buffer（#21644）。
  - **两态**：`PreservedSparseTrie::Anchored{trie, state_root}`（上块算完，`parent_state_root` 匹配则**直接续用**）与 `Cleared{trie}`（不匹配则清数据、**保留内存分配**）。`into_trie_for(parent_state_root)` 做校验（`preserved_sparse_trie.rs`）。
  - **如何限界 / 失效**：每块结束 `prune(max_hot_slots, max_hot_accounts)` 走 **LFU**（#22766），默认 `max_hot_slots=1500`、`max_hot_accounts=1000`、`prune_depth=4`；`shrink_to(1M nodes, 1M values)`（约 120–144MB 上限）；state-root 计算失败 → 存为 `Cleared`。`--engine.disable-sparse-trie-cache-pruning` 可完全保留（bench 用）。
  - **默认开启**：2.3.0 默认启用 sparse-trie-as-cache；`--engine.legacy-state-root`（旧名 enable-sparse-trie-as-cache → legacy-trie，#21851）回退到无缓存旧路径。
  - **目标加速**：连续块间避免重复 reveal 节点 / 重复取 proof / 重复算 storage root。
- **关键 PR / commit**：#21583 (19bf580f93, 基础)、#22766 (e6e637a265, LFU)、#21612 (7ccb43ea13)、#22355 (237eb1675c)、#20838 (a06644944f)、#20075 (a9e36923e1, cached branch nodes)、#21702 (102a6944ba)、#21704 (79cabbf89c)、#21644 (3d699ac9c6)、#21967 (3300e404cf)、#22697/#22767/#ff217592bc（指标）、#a5978c593e、#a92aca2549。
- **涉及文件**：`payload_processor/{sparse_trie.rs (277→1046 行), preserved_sparse_trie.rs (新), multiproof.rs (大幅缩小)}`。
- **与 gravity 的关系**：**正交（不同 trie 栈）**。gravity 用 **nested-trie + rocksdb**，跨块 trie-node 缓存已由 `PersistBlockCache` 的 `account_trie: Layer<Nibbles, StoredNode>` 与 `storage_trie`(WipeLayer) 承担；“跨块保留已 reveal 节点 + 限界热数据”的**目标 gravity 已达成**，只是实现在 rocksdb 侧而非内存 sparse trie。
- **是否建议 port**：**否**。与 reth sparse-trie 强绑定，且 gravity 不用 sparse trie。难度：极高、不适用。可做的仅是**对照**：确认 gravity 的 `PersistBlockCache` trie 层在 LFU/内存上限/命中率指标上不弱于上游（gravity 已有 `trie_cache_hit_ratio` 指标）。

### 5. Precompile cache（可独立复用 ✅）

- **上游做了什么**：对 precompile 调用按 **input bytes 为 key** 缓存 `PrecompileOutput`（含 gas_used/output/status），`CacheEntry` 内嵌 `spec_id`，`get(input, spec)` 时校验 spec 匹配。backing 从 1.8.3 的 `Arc<Mutex<LruMap>>`（每次 get 需可变引用→全锁）换成 **`moka::sync::Cache` + LRU 驱逐 + weigher（按 in+out 字节数）**（#20502/#20527），上限 `MAX_CACHE_SIZE = 1 MiB`；`PrecompileCacheMap` 从 `HashMap` 换 `Arc<DashMap<Address, _, FbBuildHasher<20>>>`（细粒度锁，`&self` 共享，#22360）。
  - **正确性护栏**：**不缓存 stateful precompile**（#23619）——插入前校验 `output.reservoir == input.reservoir` 且 `output.state_gas_used == 0`，否则告警不缓存（避免缓存吞掉状态变更导致共识错误）；**只在 `input.gas >= entry.gas_used()` 时才算命中**（#22968）；移除冗余 `initial_capacity`（#25013）。
  - **可观测性**：hits/misses/errors counter + 按 precompile 地址标签（0x01/0x02…）。
  - **目标加速**：跳过对相同输入的 SHA256/KECCAK/椭圆曲线等重复计算，跨块跨线程共享。
- **关键 PR / commit**：#20502 (30162c535e)、#20527 (21d835cf2b)、#22360 (233590cefd)、#23619 (98ebc3454f)、#22968 (451a20f0f5)、#25013 (72fafb577c)。
- **涉及文件**：`crates/engine/tree/src/tree/precompile_cache.rs`（依赖 revm/`reth_evm::precompiles` 稳定接口，逻辑自包含）。
- **与 gravity 的关系**：**正交、可移植**。但 gravity 的 pipe-exec 路径**当前完全没用它**（grep 确认 `pipe-exec-layer-ext-v2` 无 `PrecompileCacheMap` 引用），而 gravity 又带 **stateful precompile**：`randomness_by_height`（随高度变化）、`mint`、`bls_pop_verify`（`execute/src/{randomness,mint,bls}_precompile.rs`）。这些**绝不能被缓存**，正是 #23619 防护要处理的场景。
- **是否建议 port**：**弱~中建议**。收益仅限“同一 input 的重复纯计算 precompile”（对 gravity 工作负载收益需实测）；难度**低**（模块自包含）；**前置条件**：接线到 grevm 的 precompile 调用点时，**必须**为 gravity 的 stateful precompile 加白/走 #23619 式护栏，否则共识风险。与 grevm 耦合：低（只需在 precompile 包装层挂缓存）。

### 6. revm `CachedReads` / bytecode cache（可独立复用 ✅）

- **上游做了什么**：`CachedReads`（`crates/revm/src/cached.rs`）是一个 revm 层的读缓存 overlay，缓存 `accounts: AddressMap<CachedAccount>`（含每账户 storage）、`contracts: B256Map<Bytecode>`、`block_hashes`。主要用于 **payload builder 多轮迭代构建**（同一父块反复试不同 tx 集）时避免重复读 StateProvider。2.3.0 新增 **`with_account_capacity(capacity)` 预分配构造器**（#25048），按上一块账户数预估容量、减少 hashmap 反复 rehash；并补充了 `CachedReadsDbMut/Ref` 的生命周期文档（#19725）。失效为**手动**（上层控制清空 / `extend` 合并）。
- **关键 PR / commit**：#25048 (9f2837e179)、#19725 (ba84eeaccd)。
- **涉及文件**：`crates/revm/src/cached.rs`、`crates/payload/basic/src/lib.rs`（`PrecachedState` 跨块复用）。
- **与 gravity 的关系**：**正交、executor 无关**（纯 revm 数据结构 + 生命周期）。是否有收益取决于 gravity 的 **payload 构建**是否走 `basic` payload builder 并做多轮迭代。grevm 执行本身有自己的 state overlay，不依赖 `CachedReads`。
- **是否建议 port**：**弱建议**（几乎零成本的搭车项）。若 gravity 用上游 basic payload builder，`with_account_capacity` 是免费小优化；否则无需单独 port。难度：极低。与 grevm 耦合：无。

### 7. BAL（EIP-7928）驱动的 prefetch / prewarm（正交，非缓存 port 目标）

- **上游做了什么**：块自带 Block Access List（读写集，consensus-committed）。当 BAL 存在且 `parallel_bal_execution`，走 `PrewarmMode::BlockAccessList`：用 **`BalPrewarmPool`（默认 128 个阻塞线程**，每线程持一个 parent-state 的 MDBX 读事务，#25003）把 BAL 声明的 account/code/storage **批量预取进 `ExecutionCache`**（`FillOnMiss`），并**并行地**把 hashed account/storage 更新**流式**发给 `SparseTrieCacheTask`（且“先发 hashed state 再做 storage prefetch”以让 sparse trie 早点开工，#23761/#23423/#21990）。可用 `--engine.disable-bal-batch-io` 关闭“预取进缓存”这一半（#23770），`--engine.disable-bal-parallel-state-root` 关状态根那一半。128 线程是为了打满 NVMe 队列深度。
- **关键 PR / commit**：#20468 (0b6361afa5)、#25003 (b969584e08)、#23761 (5b10e03c5c)、#23423 (828965c39d)、#21990 (a8ec78fc87)、#23770 (b04346ffe5)、#24616 (56de611699)。
- **涉及文件**：`payload_processor/{bal/*, bal_prewarm_pool.rs, prewarm.rs, mod.rs}`；flags 在 `node/core/src/args/engine.rs`。
- **与 gravity 的关系**：**正交**。BAL 是 EIP-7928 共识特性 + reth 引擎树/并行执行器绑定；gravity 走 grevm，未采用 BAL。其“缓存”只是并行执行的副产品。
- **是否建议 port**：**否**（作为缓存项）。若未来 gravity 采纳 EIP-7928，再单独评估“128 线程 batch-IO 预取”思想。难度：高、且属执行/共识而非缓存。

### 8. 缓存指标、失效与并发治理（横切）

- **上游做了什么**：`--engine.disable-cache-metrics`（大缓存下算指标本身很贵，#21228）；execution cache “不可用（并发占用）”计数（#21265）；`clear` 时重置 cache hash（#22895）；account cache size 双重递减修复（#23249）；`invalid header cache` 的命中即驱逐/重处理阈值与计量修复（#e89b4611e4/#20567/#23670/#23711）；overlay cache 死锁修复（#24875）；`PayloadExecutionCache` RwLock→Mutex（#23387）。这些是把上面几个缓存做“生产可运维”的补丁群。
- **涉及文件**：`execution-cache/*`、`payload_processor/mod.rs`、`tree/{invalid_headers.rs,metrics.rs}`。
- **与 gravity 的关系**：多数针对上游那几个缓存对象；`PersistBlockCache` 已有自己的一套 metrics（`trie_cache_hit_ratio`、`wait_persist_duration` 等）。
- **是否建议 port**：**否/按需**。仅当 gravity 决定复用某个上游缓存时，连带把对应的失效/并发补丁一起带上。难度：低（跟随）。

### 9. 其它可移植小缓存：`keccak-cache-global`

- **上游做了什么**：一个通过 feature flag 开启的**全局 keccak 缓存**（bench 用，#23723 在 `reth-bb` 打开；#20524/#21051 只是把 feature 传播到各 crate）。它缓存 keccak256(preimage) 结果，跨执行复用，属 executor 无关的底层加速。
- **关键 PR / commit**：#23723 (f344f5abfb)、#20524 (29438631be)、#21051 (15f16a5a2e)。
- **与 gravity 的关系**：正交、executor 无关。gravity 计算大量 keccak（地址/槽 hashing、trie）。
- **是否建议 port**：**弱建议 / 实测驱动**。默认关闭、bench 才开，说明收益场景有限且可能有正确性/内存权衡；难度低（feature 传播），但**先量测**再决定。

---

## 汇总表

| 主题 | 上游价值 | 与 grevm/gravity 关系 | 建议 port | 优先级 | 难度 |
|---|---|---|---|---|---|
| 1. `PayloadProcessor` 主线编排 | 高（上游骨架） | 平行方案（gravity 用 pipe-exec 编排） | 否（整线） | — | 高 |
| 2. 跨块执行缓存 `reth-execution-cache` | 高 | 重叠（≈ `PersistBlockCache`） | 否；弱借鉴 FillMode/wipe 告警 | 低 | 低(借鉴)/高(整块) |
| 3. 并行交易预热 prewarm | 高（对顺序执行） | 重叠且被 grevm 替代 | 否 | — | 高 |
| 4. Sparse trie as cache | 高（对 reth trie） | 正交（gravity 用 nested-trie） | 否；仅对照 | — | 极高/不适用 |
| 5. Precompile cache | 中（限重复纯计算） | 正交、可移植 | 弱~中（须护栏 stateful） | 中 | 低 |
| 6. `CachedReads` 容量构造器 | 低~中（payload builder） | 正交、executor 无关 | 弱（搭车） | 低 | 极低 |
| 7. BAL prefetch/prewarm | 高（对 reth+EIP7928） | 正交（gravity 不用 BAL） | 否 | — | 高 |
| 8. 缓存指标/失效/并发治理 | 中（可运维性） | 跟随所复用缓存 | 按需跟随 | 低 | 低 |
| 9. `keccak-cache-global` | 低（bench 场景） | 正交、executor 无关 | 弱、实测驱动 | 低 | 低 |

**给 gravity 的一句话结论**：上游 2.3.x 的缓存“大件”（cross-block execution cache / prewarming / sparse-trie-as-cache / BAL）都**紧耦合 reth 自己的顺序执行器 + engine tree + sparse trie**，而这几件事 gravity 已用 **grevm(并行执行) + `PersistBlockCache`(跨块缓存) + nested-trie(trie 缓存)** 以另一套体系覆盖，属重叠/正交，**不建议整块 port**；真正值得单独拿的是 **precompile cache**（低成本，但**必须**为 gravity 的 stateful precompile 加 #23619 式护栏）与顺带的 **`CachedReads::with_account_capacity`**。

---

## 备注 / 存疑

1. **merge tree 已含上游代码**：当前分支 `gravity-reth-merge-v2.3.0` 已把 `crates/engine/execution-cache/`、`payload_processor/{prewarm,sparse_trie,precompile_cache,bal,...}` 全部合入。所以本文的“port”实为“**是否把这些已在树里的上游缓存接进 grevm+pipe-exec 热路径**”，而非“是否引入代码”。目前它们对 gravity 主路径应为“存在但不生效/未接线”，建议核对**是否有编译/维护负担**（例如 `configured_sparse_trie.rs` 这类 1.8.3 残留是否需清理）。
2. **precompile cache 的实际收益未量测**：只对“相同 input 反复调用的纯计算 precompile”有效；以太坊主网负载下命中率有限。port 前应在 gravity 的目标链负载上量测 hit rate，并**先落实 stateful 白名单**（`randomness_by_height`/`mint`/`bls_pop_verify` 必须旁路）。
3. **`PersistBlockCache` vs 上游 execution cache 的语义差异待确认**：上游 pre-Dencun SELFDESTRUCT 走“整表清空 + 告警”；gravity 用更精细的 `WipeLayer.wipe`。二者在“wipe+recreate（#715）”场景的正确性此前已有专门 e2e（见 memory 中 nested-trie-wipe-recreate / mainnet-replay-pipe-test）；port 借鉴时应保持 gravity 更精细的语义、不要回退到整表清空。
4. **`CachedReads` 是否在 gravity payload 路径上**未逐行确认：仅当 gravity 走上游 `basic` payload builder 且多轮迭代时 `with_account_capacity` 才有意义；若 gravity 自建 builder 则无需 port。
5. **BAL 若未来引入**：`BalPrewarmPool` 的“128 阻塞线程打满 NVMe 队列深度做 batch prefetch”思想，对 gravity 的 rocksdb 冷读预取也许有独立借鉴价值（与 BAL 本身解耦），可作为单独课题评估。
6. 少量 PR 号的合入日期标注来自子代理推断，可能不精确；PR 号/commit hash 已按 `git log branch-1.8.3..branch-2.3.0` 核对。
