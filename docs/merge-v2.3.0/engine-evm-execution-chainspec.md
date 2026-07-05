# Engine / EVM / 执行 / Chainspec 冲突分析

> **Baseline anchor**: `0cb1687c1c`（Galxe/gravity-reth `upstream/main` tip，已含 #373）。
> **Target**: reth `v2.3.0`。
> **分支**: `merge-v2.3.0`。HEAD: `e6b7e5ba32`。

## 分组概要

该组囊括"区块进入链上之前"的全链路：engine tree（payload validator、block
buffer、persistence service、metrics）→ EVM 执行接口（`Executor` / `BlockExecutor`
trait、`ConfigureEvm`）→ 以太坊 EVM 实现（`EthEvmConfig` + 测试 mock）→ 共识/区块预校验
→ Chainspec（trait、struct、constants、re-export）→ 内存链状态（`InMemoryState`、
`MemoryOverlay`、`TestBlockBuilder`）。

冲突分布有清晰的语义簇：

1. **Engine tree backpressure / sparse-trie / BAL / 共享 payload cache** — 上游
   v2.3.0 体量极大的重写（#23280, #23246, #23209, #21115 等），与 gravity 的
   `pipe-exec-layer` + `grevm` 并行执行 + 自有 persistence 节流构成深度冲突。
2. **`Executor` trait surface** — gravity 自 `state_root: implement cache and state
   write (#109)` 起在 `Executor`/`BlockExecutor` 上挂了
   `transact_system_txn` / `apply_state_change` / `take_bundle` / `parallel_executor`
   等扩展，与上游 v2.3.0 引入的 `GasOutput` / `take_bal` / `state-hook from State<DB>`
   重构正面冲突。
3. **Chainspec 50 Gwei 最低 base fee** — `#335` / `#337` 在 baseline 已落地，引入了
   `gravity_min_base_fee` 字段、`gravity_min_base_fee_at_block` trait 方法、
   `next_block_base_fee` 重载与 `GRAVITY_MIN_BASE_FEE` 常量；与上游 v2.3.0 的
   chainspec 字段重组（`amsterdam_time`、`block_access_list_hash` / `slot_number`
   写入 genesis header）正面冲突。
4. **`GravityHardfork` + `gravity_hardforks` 字段** — `#309` / `#312` 引入 hardfork
   stub framework，需要在合并后的 `ChainSpec` 结构体与 `EthChainSpec` trait 里继续
   保留。
5. **Cargo.toml deps 重组** — 上游 v2.3.0 把 `reth-execution-cache` 抽出来
   (#23209)、加 `indexmap`/`alloy-eip7928`/`moka`，gravity 侧把 `mini-moka`/
   `gravity-primitives`/`reth-pipe-exec-layer-event-bus`/`grevm` 挂在同一个 deps
   列表里。
6. **`RecoveredBlock` ↔ `SealedBlock` API 漂移** — 上游 v2.3.0 把
   `BlockBuffer` 改回存 `SealedBlock<B>`，gravity baseline 走 `RecoveredBlock<B>`。
7. **`ExecutedBlock` / `ExecutedBlockWithTrieUpdates` 分裂** — gravity baseline
   在 `chain-state` 里区分二者（trie cache 路径），上游 v2.3.0 用统一的
   `ExecutedBlock` + `LazyTrieData`/`ComputedTrieData`/`StateTrieOverlayManager`
   重写。

## 逐文件分析

### `crates/engine/primitives/src/config.rs`

**模块**: engine tree 默认配置常量与 `TreeConfig` 字段。

**冲突类型**: AA（base 端被识别为新增 — engine/primitives crate 在 baseline
位置不同；实际上文件存在但 diff3 触发 add/add）。

**上游变更** (v1.8.3..v2.3.0)：
- `feat(engine): backpressure, take 2.` (#23280) — 引入
  `DEFAULT_PERSISTENCE_BACKPRESSURE_THRESHOLD = 16` 与 invariant 检查。
- `feat(engine): configure invalid header cache hit eviction` (#23670) —
  `DEFAULT_INVALID_HEADER_HIT_EVICTION_THRESHOLD`。
- `feat(engine): add state root task timeout with sequential fallback` (#22004) —
  `DEFAULT_STATE_ROOT_TASK_TIMEOUT`。
- `feat(engine): add --engine.disable-sparse-trie-cache-pruning flag` (#21967) +
  `feat(engine): add CLI args for sparse trie pruning configuration` (#21703) —
  `DEFAULT_SPARSE_TRIE_PRUNE_DEPTH` / `DEFAULT_SPARSE_TRIE_MAX_HOT_SLOTS` /
  `DEFAULT_SPARSE_TRIE_MAX_HOT_ACCOUNTS`。
- `perf: LFU-based sparse trie cache` (#22766) +
  `chore(engine): disable BAL parallel execution by default` (#23764) — 周边常量。
- `default_cross_block_cache_size()` 改为常量函数（处理 32 位 / test cfg）。
- `DEFAULT_BLOCK_BUFFER_LIMIT` 从字面量改为 `EPOCH_SLOTS as u32 * 2`。

**Gravity 侧变更** (baseline only)：
- `DEFAULT_PERSISTENCE_THRESHOLD = 2` / `DEFAULT_MEMORY_BLOCK_BUFFER_TARGET = 0`
  保留（pipe-exec 调度要求 buffer 几乎为零、persistence 紧贴 head）；这是
  `e775fd5e72 fix: Batch size limiting for block persistence (#170)` 的产物。
- `DEFAULT_MAX_PROOF_TASK_CONCURRENCY = 256`（与上游不同体系，gravity 不走
  sparse-trie multiproof）。
- `DEFAULT_MAX_EXECUTE_BLOCK_BATCH_SIZE = 4`、
  `DEFAULT_CROSS_BLOCK_CACHE_SIZE = 4 GiB`（写死，不区分 32/64 位）。
- 不引入 backpressure/sparse-trie/state-root-task-timeout 三组常量 — gravity 用
  pipe-exec ordering 替代 backpressure。

**影响范围**: `TreeConfig` 构造在合并后必须暴露上游的新字段，否则 v2.3.0
其他文件（`tree/mod.rs`、`metrics.rs`、CLI args parsing）无法编译；同时
gravity 的 `pipe-exec` 路径需要继续读 `persistence_threshold = 2` 的语义。

**解决方案建议**: **mechanical-merge** — 合并 import（`use alloy_eips::merge::
EPOCH_SLOTS` + `core::time::Duration`），保留 gravity 的 `DEFAULT_PERSISTENCE_
THRESHOLD = 2` 与 `DEFAULT_MEMORY_BLOCK_BUFFER_TARGET = 0`，把上游新增的 backpressure
threshold / sparse-trie / cross-block 常量整段引入。`DEFAULT_BLOCK_BUFFER_LIMIT`
改用上游 `EPOCH_SLOTS * 2`（256 vs ~64×2，差别不影响链上语义）。在 `TreeConfig`
结构体里把新字段加上，构造函数 default 使用上游值；gravity 侧 `pipe-exec` 路径
仍按 baseline 阈值 spawn persistence。

**推理**: baseline 的 4 个常量是 gravity-specific（pipe-exec
延迟敏感），不能丢；上游引入的字段是 v2.3.0 后续文件强依赖，丢了无法编译。

---

### `crates/engine/tree/Cargo.toml`

**模块**: engine tree crate 依赖清单。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `refactor(engine): extract PayloadExecutionCache into reth-execution-cache crate`
  (#23209) — 新增 `reth-execution-cache.workspace = true`。
- `feat(engine): use IndexSet for deterministic block buffer child ordering`
  (#22676) — 新增 `indexmap.workspace = true`。
- `feat(engine): add BAL metrics type for EIP-7928` (#21356) +
  `feat: parallel execution` (#23924) — 新增
  `alloy-eip7928 = { workspace = true, features = ["rlp"] }`、
  `revm-state.workspace = true`。
- `perf: properly share precompile cache + use moka` (#20502) —
  `moka = { workspace = true, features = ["sync"] }`。
- `feat: global runtime` (#21934)、`reth-tasks = { ..., features = ["rayon"] }`、
  `reth-revm = { ..., features = ["optional-balance-check"] }`、
  `reth-engine-primitives = { ..., features = ["std"] }`、
  `reth-chain-state = { ..., features = ["rayon"] }`、
  `reth-primitives-traits = { ..., features = ["rayon", "dashmap"] }` 等多个
  feature 切换。
- 新增 `reth-trie-common.workspace = true`。

**Gravity 侧变更** (baseline only)：
- `reth-pipe-exec-layer-event-bus.workspace = true`、`gravity-primitives.workspace
  = true` — gravity 自有 pipe-exec ext。
- `mini-moka = { workspace = true, features = ["sync"] }`（gravity 选 mini-moka
  而非 upstream moka）+ `smallvec.workspace = true`。
- `reth-trie-sparse-parallel = { workspace = true, features = ["std"] }`。
- Feature flag: `config-from-env = ["gravity-primitives/config-from-env"]`（在
  baseline Cargo.toml 末尾，diff 截断显示）— 由 `f6d831dbd2 feat: implement onchain
  config (#146)` 引入。

**影响范围**: 选 moka 还是 mini-moka 决定 `tree/cached_state.rs` 等用 cache 类型
的文件能否编译；少一个 dep 整 crate 编译失败。

**解决方案建议**: **mechanical-merge** — 并集所有依赖：保留 gravity 的
`reth-pipe-exec-layer-event-bus` / `gravity-primitives` / `smallvec` /
`reth-trie-sparse-parallel`；新增 `reth-execution-cache` / `indexmap` /
`alloy-eip7928` / `revm-state` / `reth-trie-common` / 上游 feature flags。
关于 `mini-moka` vs `moka`：v2.3.0 上游下游代码都用 `moka`，gravity baseline
当前使用 mini-moka 仅在 gravity 自有的少量 cache 文件 — 建议切到 `moka`（avoid
keeping two LFU cache crates），同时审计 gravity 自有 cache 使用点是否 API
兼容（`moka::sync::Cache` 大体兼容 `mini_moka::sync::Cache`）。
→ **更正**（2026-07-02）: 核实后推翻——cached_state.rs 为 fork 遗留、上游
execution cache 已迁 `reth-execution-cache`（fixed-cache），按选项 C 执行：
删文件、冲突取 v2.3.0 侧，不存在"切 moka"工作量。

**推理**: 这是合并冲突里风险最低的一类（Cargo.toml 取并集），但 moka/mini-moka
取舍需在合并完后核对 cache 调用站点。

---

### `crates/engine/tree/src/metrics.rs`

**模块**: engine tree 顶层 metrics（`PersistenceMetrics`、`SyncMetrics`）。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `fix(metrics): rename save_blocks_block_count to save_blocks_batch_size` (#21654)
  + `chore: add metric for batch size` (#20610) — `PersistenceMetrics` 新增
  `save_blocks_batch_size: Histogram`。

**Gravity 侧变更** (baseline only)：
- `c0bf0cc429 opt(mdbx): sort trie entries and add mdbx bench (#180)` 与
  `579c368912 chore(persist): more accurate persistence duration (#101)` 引入
  `persist_commit_duration_seconds`、`save_duration_per_block_seconds` 两个
  histogram。

**影响范围**: metrics dashboard / Prometheus 暴露的 metric 名集合；不影响共识。

**解决方案建议**: **mechanical-merge** — `PersistenceMetrics` 同时保留
gravity 的 `persist_commit_duration_seconds` / `save_duration_per_block_seconds`
和上游的 `save_blocks_batch_size`。`persistence.rs` 同步更新 record 调用点。

**推理**: 三个 histogram 互不冲突，仅 struct 字段并集。

---

### `crates/engine/tree/src/persistence.rs`

**模块**: persistence service（处理 `SaveBlocks` / `PruneBefore` /
`SaveFinalizedBlock` 等 action）。

**冲突类型**: UU（diff 867 行，覆盖整个文件结构）。

**上游变更** (v1.8.3..v2.3.0)：
- `perf(engine): acknowledge save_blocks before prune` (#23904) —
  `PersistenceResult { last_block, commit_duration }` 类型化。
- `perf: parallelize save_blocks` (#20993) +
  `perf(persistence): combine save_blocks and prune into single MDBX commit`
  (#21927) — 整个 save_blocks 改写：用 `crossbeam_channel::Sender`、
  `spawn_os_thread`、把 prune 合到同一笔 commit。
- `fix(engine): wait for persistence service thread before RocksDB drop` (#21640) —
  返回 `JoinHandle`、shutdown 协议。
- `fix(provider): fix race between save_blocks and rocksdb pruning` (#23081) —
  `BalProvider`、`SaveBlocksMode` 接入。
- `feat: catch-up for read-only ProviderFactorys` (#23357) —
  `DatabaseProviderFactory<Provider: BlockHashReader + StorageChangeSetReader>`。
- `feat(trie): in-memory trie changesets` (#20997) — `TrieInput` 流转方式变了。
- `fix(engine): flush BAL store after saving blocks` (#24087)、
  `e873a930e fix(engine): prune BAL store from persistence task` (#24084) —
  BAL store 通道。

**Gravity 侧变更** (baseline only)：
- `ff103f976a fix(unwind): commit view and set prune distance for execution
  unwind (#313)` — `prune_before` 调用前先 `commit_view`、设置 prune_distance。
- `c64bd613e4 opt(persist): use sharding rocksdb instances to optimize persist
  stage (#225)`、`a1d7365bd6 feat(rocksdb): Integrating RocksDB into Reth (#212)`、
  `1539b6cafc opt(persist): not write index tables if validator node only (#224)` —
  全部 persistence I/O 走 gravity 自有 rocksdb / `TrieWriterV2` /
  `PERSIST_BLOCK_CACHE`，与上游 MDBX 走的 `UnifiedStorageWriter` 不一样。
- `bcbe31a841 fix(rocksdb): Detected interrupted trie update, but trie has
  idempotency (#236)` — idempotent trie write 处理。
- 整个 `on_save_blocks` 改写：`thread::spawn`+`tokio::sync::oneshot`，依赖
  `gravity_primitives::get_gravity_config` 读 validator-only 标志。
- 使用 `ExecutedBlockWithTrieUpdates` 而非上游 `ExecutedBlock`。

**影响范围**: 这是 gravity validator 节点数据落盘的核心路径。误把上游的
MDBX/save_blocks 通道接进来会破坏 RocksDB 写入，引起 corruption / unwind
失败 / 节点崩溃。

**解决方案建议**: **keep-gravity** — 整体保留 gravity 的 RocksDB-based
persistence service，包括 `UnifiedStorageWriter` / `TrieWriterV2` /
`PERSIST_BLOCK_CACHE` / validator-only 路径 / `ExecutedBlockWithTrieUpdates`。
机械吸收的项目仅限：
1. metrics 字段并集（与 `metrics.rs` 配套），上游的 `save_blocks_batch_size`
   record 点要插。
2. 上游若改了 `PersistenceAction` 的 enum 变体（公共类型），gravity 这边对
   tree/mod.rs 端的 sender 协议也要同步变体集合。
   
**禁止**移植：`crossbeam_channel` 替换 `mpsc`、`BlockExecutionWriter`、
`BalProvider`、`SaveBlocksMode`、BAL flush prune — gravity 数据栈不走 BAL，
也不走 MDBX 路径。

**推理**: 任何把上游 MDBX 路径接进来的尝试都等于回退 #212 #225 #224 三个 PR，
基础属于 chain-halt / data-loss 级别风险。

---

### `crates/engine/tree/src/tree/block_buffer.rs`

**模块**: 用于缓存"父块未到、子块先到"的 block buffer，按 FIFO 驱逐。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `fix(engine): use IndexSet for deterministic block buffer child ordering` (#22676)
  — `parent_to_child` 从 `HashMap<_, HashSet<_>>` 切到 `HashMap<_, IndexSet<_>>`。
- `perf(engine): prevent duplicate block insertion in BlockBuffer` (#20487) —
  `insert_block` 改成 `Entry::Occupied → return`。
- `perf(engine): only recover senders once` (#20118) — 存储类型从
  `SealedBlockWithSenders` 改成 `SealedBlock<B>`（统一 recover 在 buffer 之前完成）。
- `fix(engine): remove redundant parent_to_child removal during eviction` (#18648)。
- `fix(tree): correct block buffer eviction policy comment` (#20512)。

**Gravity 侧变更** (baseline only)：
- gravity 走 `RecoveredBlock<B>`（即 baseline 文件里 `pub(crate) blocks:
  HashMap<BlockHash, RecoveredBlock<B>>`），与上游 `SealedBlock<B>` 不一致 — 这是
  v1.4.8/v1.5.0/v1.6.0 多次 catch-up 合并保留下来的 type alias。
- `insert_block` 直接覆盖（不做 `Entry::Occupied` 短路）— pipe-exec 进入这里时
  保证 hash 唯一，没引 #20487 的优化。
- 用 `let-else` 链 `if let Some(...) && let Some(...)`（Rust 2024 / let-chains 风格），
  与上游 `if let Some(...) { ... }` 写法不同。

**影响范围**: tree 内部 buffer，被 `on_downloaded_block` / `try_connect_buffered`
等调用。type 切换影响整 crate 编译（RecoveredBlock 与 SealedBlock 不同字段）。

**解决方案建议**: **mechanical-merge** with **keep-gravity** for type：保留
gravity 的 `RecoveredBlock<B>`（向上传播到 tree/mod.rs 的 `connect_buffered_blocks`
路径），同时吸收：
- `IndexSet` 替换 `HashSet` for `parent_to_child`（确定性，但行为与
  HashSet 一致 — 直接换无副作用）；
- `Entry::Occupied → return` 短路（gravity 走 pipe-exec 不会触发，但加上无害且对齐
  上游测试用例）；
- `remove_redundant parent_to_child` 修正与注释修正机械同步。

**推理**: 类型切换牵涉 chain-state 一整套（`ExecutedBlockWithTrieUpdates` 流，
见后续 `in_memory.rs`），不能为单文件让步切到 `SealedBlock`；其他都是机械
hunk 合并。

---

### `crates/engine/tree/src/tree/metrics.rs`

**模块**: tree 内部 metrics（execution / validation / cache hit）+ baseline
里多挂了 `execute_metered` 辅助函数。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `feat(engine): add gas-bucketed sub-phase metrics for new_payload` (#22210) +
  `feat(metrics): use 5M first gas bucket` (#22136) +
  `feat(engine): add gas bucket label` (#22067) — 大量 gas bucket 化。
- `feat(engine): backpressure, take 2.` (#23280) 及配套
  (`#23541, #23578, c527c2e7d revert, 93b2201c7`) — backpressure 进入 metric。
- `feat(engine): add metric for state root task fallback success` (#21371)。
- `feat(engine): add BAL metrics type for EIP-7928` (#21356)。
- `refactor(engine): move execution logic from metrics to payload_validator`
  (#21226) — 大块逻辑搬走。
- `feat(engine): reorg depth commitment metric` (#21992)。
- `feat(engine): time_between_forkchoice_updated metric` (#21227) +
  `time_between_new_payloads` (#21158) + `new_payload_interval` (#21159) +
  `forkchoiceUpdated_response → newPayload` (#21380)。
- `refactor(chain-state): manage state trie overlays centrally` (#24184)。

**Gravity 侧变更** (baseline only)：
- 整个 `EngineApiMetrics::execute_metered<E, DB>(...)` 在 metrics.rs 里实现
  block executor 计时／state-hook 串接（baseline 上 #205 catch-up 后保留下来的
  metered execution helper）。包含 `BlockExecutor + Evm<DB: BorrowMut<State<DB>>>`
  约束。
- `EngineApiMetrics`/`EngineMetrics` 字段为 `pub(crate)`，gravity baseline 已扩
  为 `pub`（baseline diff 显示 pub vs pub(crate) 已经一致 — 这一边其实没冲突）。
- 引入 `MeteredStateHook` 在 crate 内部用，与 `tree/mod.rs` 配套（gravity 自有
  state hook 流，对应 #109 `state_root: implement cache and state write`）。

**影响范围**: tree/mod.rs 调用 `execute_metered` 的位置 + 所有 metric 名集合。

**解决方案建议**: **mechanical-merge** 偏 **keep-gravity** — 保留
gravity 的 `execute_metered` helper 与 `MeteredStateHook`（删除后无法满足
gravity tree/mod.rs 调用点）；同时吸收上游 gas-bucket / backpressure /
fallback-success / time-interval 这一大批新 metric 字段（纯加字段，不破坏
gravity 流）。上游 #21226 把逻辑搬到 `payload_validator` 这件事，gravity 不要
跟（gravity 用的是 pipe-exec 自有 validator 流，搬过去会触发别的冲突）。

**推理**: metric 字段并集风险低；helper 删了会断 gravity 的 chain；上游搬迁
拒绝同步是为了避免连锁修改 `payload_validator.rs`。

**勘误**（2026-07-02）: 本小节两处与实测不符：(1) `execute_metered` 并非
gravity 侧新增 — 上游 v1.8.3 原生就有（metrics.rs:60），"Gravity 侧变更
(baseline only)" 是对 v2.3.0 diff 的视角错觉（上游删了它，不是 gravity 加了
它）；(2) 调用点在 v1.8.3 的 `payload_validator.rs:767`，而非 tree/mod.rs。
据此决策已修订为跟进 #21226（删 helper，metrics.rs 取 v2.3.0 侧），见开放
问题 #4。

---

### `crates/engine/tree/src/tree/mod.rs`

**模块**: engine tree 主循环（`EngineApiTreeHandler` / `TreeState` / `on_engine_message`
/ `on_downloaded_block` / `try_connect_buffered` / `make_canonical` / persistence 协调）。

**冲突类型**: UU（diff 3482 行，整文件几乎都改）。

**上游变更** (v1.8.3..v2.3.0)：
- `refactor(chain-state): manage state trie overlays centrally` (#24184)、
  `feat(engine): backpressure, take 2.` (#23280)、
  `feat(engine): share sparse trie pipeline with payload builder` (#23246)、
  `feat: share execution cache with payload builder` (#23242)、
  `feat(engine): suppress persistence during payload building` (#23618)、
  `feat: spawn deferred trie work for directly inserted payloads` (#23935) —
  整个 payload 处理 / state-root / cache 共享体系翻新。
- `refactor(engine): add validation output type` (#24089)、
  `feat(engine): include rejection reason in InvalidBlock event` (#23945)、
  `fix(engine): apply finalized state after syncing FCU head import` (#23838) —
  validator API 变形。
- `fix: skip already-known executed blocks` (#23987)、
  `perf(engine): defer trie overlay computation with LazyOverlay` (#21133) — 复
  执行/trie 计算优化。
- `feat(engine): slow block logs` (#21433) + `aedda7f6a fix: emit slow block log
  immediately`。
- `chore: log transient invalid header cache skips` (#23711)。
- `fix(op): prioritize head over finalized as backfill target on OP Stack`
  (#24159) — OP 专属，不引入。
- `refactor: decouple CachedStateMetrics from SavedCache` (#23552)。
- `perf: use B256Map for hash-keyed maps` (#24372)。

**Gravity 侧变更** (baseline only)：
- `bb9dc94a85 fix: Fix coacker audit (#311)`、`d927b5d96d fix: audit round 3 (#284)`
  — audit-driven 修复。
- `fdd0a11f7e fix(engine): Remove validate_block() when making canonical in
  pipeline (#262)` — pipe-exec 路径下 canonical 化时不重复 validate_block。
- `fd250d53d8 fix(rpc): set safe and finalized block when making canonical (#251)` —
  make_canonical 时同步 safe/finalized 给 RPC。
- `5555eac73a fix(pipe): fix header timestamp in epoch change block (#250)` —
  epoch 变更时 header timestamp 处理。
- `7ea62ca632 fix(pipe): wait PipeExecLayerEventBus ready in a loop (#228)` —
  pipe-exec event bus startup wait。
- `dfa14dcdea refactor: Add gravity configuration arguments (#168)` +
  `f6d831dbd2 feat: implement onchain config (#146)` — `get_gravity_config()`
  全文流转。
- 全文使用 `ExecutedBlockWithTrieUpdates` / `ExecutedTrieUpdates`（gravity 分裂的
  trie-cache 路径）；import 路径含 `reth_pipe_exec_layer_event_bus::{
  get_pipe_exec_layer_event_bus, MakeCanonicalEvent, PipeExecLayerEvent,
  WaitForPersistenceEvent}` — gravity tree 上每个 canonical 化动作要 publish
  event 给 pipe-exec。
- `revm::state::EvmState` 与 `reth_revm::database::StateProviderDatabase`
  在 gravity 串 grevm 并行执行结果时的桥。

**影响范围**: 整个 engine tree 主控制流。错合会导致：1）pipe-exec 等不到事件
→ 共识停摆；2）make_canonical 不发 safe/finalized → RPC `eth_getBlockByNumber
('safe' | 'finalized')` 永远 None；3）trie-cache 路径错乱 → 状态根错误。

**解决方案建议**: **keep-gravity** 为骨架，**needs-port** 上游新机制中
gravity 仍要的部分：
1. **保留** `reth_pipe_exec_layer_event_bus::*` import 与所有 publish 调用
   (#250, #251, #262, #228, #311, #284)；保留 `ExecutedBlockWithTrieUpdates` /
   `ExecutedTrieUpdates`；保留 `get_gravity_config()` 流转。
2. **拒绝** 上游的 backpressure (#23280)、shared payload cache (#23242)、
   `StateTrieOverlayManager` (#24184) — gravity 用 pipe-exec 节流和 `mini-moka`
   缓存替代；不要硬接 `reth-execution-cache`。
3. **可机械吸收**：`fix: skip already-known executed blocks` (#23987)（gravity
   也会受益）、`refactor(engine): add validation output type` (#24089)（如果
   `payload_validator` 配合更新；否则跳过）、metric type alias 变更同步
   `metrics.rs`、`InsertBlockValidationError` 新错误变体（机械加进 error.rs）。
4. **OP 专属**直接 trim：`fc59451fc fix(op)` 不引入。

**禁止**：把上游的 `WorkerPool` / `spawn_os_thread` / `increase_thread_priority`
线程模型整体接进来 — gravity tree 走 tokio + pipe-exec actor，混线程模型会
栈溢出 / deadlock。

**推理**: 这个文件是 gravity 共识/pipe-exec 集成的中心，是 gravity 偏离上游
最深的文件之一。冲突解决必须以 baseline 为模板，逐 hunk 判断"是否非引入
不可"。任何"按上游全盘换"的做法都属于 chain-halt 风险。

---

### `crates/engine/tree/src/tree/tests.rs`

**模块**: engine tree 集成测试。

**冲突类型**: AA。

**上游变更** (v1.8.3..v2.3.0)：
- `fix(engine): apply finalized state after syncing FCU head import` (#23838)、
  `feat(engine): backpressure, take 2.` (#23280)、
  `refactor(chain-state): manage state trie overlays centrally` (#24184) — 测试
  fixture 跟着结构改。
- `refactor: remove PayloadBuilderAttributes` (#23202)、
  `fix: always reinsert reorged blocks` (#23175)、
  `feat(engine): slow block logs` (#21433) — 测试新增 case。
- `convert_payload_to_block` 替代 `ensure_well_formed_payload` —
  `PayloadValidator` trait 重命名。
- 上游 `MockEngineValidator::convert_payload_to_block` 返回 `SealedBlock`
  而 baseline 返回 `RecoveredBlock`（与 `block_buffer.rs` 的类型分歧同源）。

**Gravity 侧变更** (baseline only)：
- `fd250d53d8 fix(rpc): set safe and finalized block when making canonical (#251)`
  + `5781df248c fix: fix ut compilation after merge v1.8.3 (#207)`、
  `5901e7da98 chore(CI): run unit and integration test in CI (#173)` — 集成测试
  自身 fixture 修复，gravity baseline 已通过 `ensure_well_formed_payload`
  + `RecoveredBlock<Self::Block>` 编译。

**影响范围**: 只影响 `cargo test -p reth-engine-tree`；不影响 binary。

**解决方案建议**: **keep-gravity** 为主，**mechanical-merge** 测试新 case：
保留 `ensure_well_formed_payload` + `RecoveredBlock` 返回（与 `block_buffer.rs`
保持一致）。如果 `tree/mod.rs` 合并后引入了上游新的 helper（如
`spawn_os_thread`），同步 import；上游新增的 `always reinsert reorged blocks`
等独立测试 case，机械吸收。`StateTrieOverlayManager`/`ComputedTrieData` 相关
测试整段跳过（与决策不导入这套机制一致）。

**推理**: 测试是 mod.rs 决策的下游；mod.rs 选 keep-gravity，tests.rs 跟随。

---

### `crates/evm/evm/src/either.rs`

**模块**: `Either<A, B>` 在 `Executor`/`BlockExecutor` trait 上的派发实现。

**冲突类型**: AA。

**上游变更** (v1.8.3..v2.3.0)：
- `feat: parallel execution` (#23924) — 重写大量 trait 派发。
- `chore: bump revm 40` (#24395) — revm 接口变形（`TxEnv` / `EvmState` 等
  re-export 路径变了）。
- `take_bal() -> Option<alloy_eip7928::BlockAccessList>` — 新加 trait 方法。

**Gravity 侧变更** (baseline only)：
- `a077894a7d fix(precompiles): execute system transactions as normal transactions
  (#288)` 与 `f6d831dbd2 feat: implement onchain config (#146)` + 早期 #108
  累计在 `Either` 上新增：
  - `take_bundle() -> BundleState`
  - `transact_system_txn(evm_env, precompiles, tx_env) -> ExecutionResult<HaltReason>`
  - `apply_state_change(state_diff: EvmState) -> Result<(), Self::Error>`
- `use alloy_evm::{precompiles::DynPrecompile, EvmEnv}` 与
  `revm::{context::{result::{ExecutionResult, HaltReason}, TxEnv}, state::EvmState}`
  imports 全部是 gravity-only。
- `ba7e949473 feat(eip-2935): serve 8191-block history via Prague-gated activation
  (#341)` 也在这个文件触过 — 进一步把 system-txn 路径打开。

**影响范围**: 整个 EVM crate trait — `Either` 是泛型派发，gravity 自有 trait
方法（`transact_system_txn` / `apply_state_change`）必须在 `Either` 上有派发，
否则只要 gravity 在某个 `Executor` 上用 `Either<EthEvm, GrevmExecutor>` 就编不过。

**解决方案建议**: **keep-gravity** 并 **needs-port** 上游新方法：
保留 `take_bundle` / `transact_system_txn` / `apply_state_change` 三个方法的
Either 派发；新增上游的 `take_bal()` 派发（gravity 不用 BAL → 返回
`None`，或派发到 inner 由 inner 返回 None）。删除 baseline 不再需要的 import
（如果 v2.3.0 trait 上没了某方法）。

**推理**: 三个 gravity 方法在 `execute.rs` 定义、`either.rs` 派发、`noop.rs`
也派发，是 gravity 整个 EVM 子系统的对外契约，不可丢。

---

### `crates/evm/evm/src/execute.rs`

**模块**: `Executor` 与 `BlockExecutor` trait 的定义。

**冲突类型**: AA。

**上游变更** (v1.8.3..v2.3.0)：
- `feat: parallel execution` (#23924)、`chore: bump revm 40` (#24395)、
  `chore: bump revm to v37 (EIP-8037 state gas)` (#23191)。
- `refactor(evm): return gas output from block builder` (#23744) —
  `pub use alloy_evm::block::{..., GasOutput}`。
- `feat(evm): add WithTxEnv constructor` (#24366) +
  `feat(evm): implement ExecutorTx for tx tuples` (#24462)。
- `refactor: integrate state hook from State<DB>` (#24654) —
  `execute_with_state_closure_always` 替代旧的 hook 接口。
- 引入 `alloy_eip7928::{compute_block_access_list_hash, BlockAccessList}` 与
  `revm::state::bal::Bal` — BAL 进入 Executor 主流程。
- `pre-alloc Vec<results>` 通过 `size_hint`（`execute_with_state_closure` 路径）。

**Gravity 侧变更** (baseline only)：
- Trait 上新增 4 个方法（gravity-only）：
  - `take_bundle() -> BundleState`
  - `transact_system_txn(evm_env, precompiles, tx_env) -> ExecutionResult<HaltReason>`
  - `apply_state_change(state_diff: EvmState) -> Result<(), Self::Error>`
  - 不暴露 `GasOutput`（gravity 自己有 `execute_metered`）。
- Imports 自己一套：`alloy_evm::{block::{CommitChanges, ExecutableTx},
  precompiles::DynPrecompile}` 与 `revm::context::{result::{ExecutionResult,
  HaltReason}, TxEnv}`。
- `ba7e949473 feat(eip-2935)` 在这里也触发 — 用于 history serving。

**影响范围**: 上游所有 `BlockExecutor` impl（包括 alloy-evm 提供的
`EthBlockExecutor`）必须实现 trait 上 gravity 加的 4 个方法；这是 gravity 自有
trait surface 的核心。

**解决方案建议**: **keep-gravity** trait surface，**needs-port** 上游 trait
新方法：
1. 保留 gravity 4 个方法在 trait 上；为 inner alloy-evm `EthBlockExecutor`
   提供 default impl 或 newtype wrapper（gravity 已有 `WrapExecutor` 见
   `ethereum/evm/src/lib.rs` 的 `WrapExecutor`，复用之）。
2. 引入上游 `GasOutput` 公开 re-export（如果 gravity 上其它 crate 引用 alloy-evm
   公开类型，会跟着需要）。
3. 引入上游 `take_bal()` trait 方法，默认 `None`。
4. 引入 `execute_with_state_closure_always`（gravity 也可以受益于"failure 路径
   也能拿到 state"）。
5. revm 版本由 workspace deps 决定 — 整 crate 都会跟随 v2.3.0 升 revm 40。

**推理**: trait 是 inter-crate API 边界；gravity 4 方法是 #288 #146 #341
三组 gravity-specific PR 的契约面，去掉会导致 system txn / onchain config /
EIP-2935 history serving 全部 break。

---

### `crates/evm/evm/src/lib.rs`

**模块**: `reth-evm` crate 顶层 — `ConfigureEvm` trait、`BlockBuilder` doc
example、re-export。

**冲突类型**: AA。

**上游变更** (v1.8.3..v2.3.0)：
- `refactor: simplify reth-bb` (#23912)、
  `refactor: expose executor transaction result type` (#23759)、
  `chore: bump revm to v37` (#23191) + `bump alloy-evm` (#23289) + bump 到
  v2.0.0 (#23407) — `BlockBuilder` 接口 + `EvmFactory` 升级。
- `feat: configurable EVM execution limits` (#21088)。
- doc-comment 中 `NextBlockEnvAttributes` 加 `slot_number: None` 字段、
  `builder.finish(state_provider, None)` 新签名。
- `feat(evm): impl ExecutableTxTuple for Either via EitherTxIterator` (#22102)。
- `pub use alloy_evm::block::state_changes as state_change` 上游移除（#20518
  `chore(evm): remove deprecated state_change compatibility alias`）。
- `engine` 模块上游加 `#[cfg(feature = "std")]` 门；并新增 `ConvertTx` /
  `ExecutableTxTuple` re-export。

**Gravity 侧变更** (baseline only)：
- `mod parallel_execute;` + `use parallel_execute::ParallelExecutor;` — gravity
  并行执行（grevm）的 trait/类型 暴露。
- `pub trait ParallelDatabase: revm::DatabaseRef<...>` + blanket impl — 给
  grevm 用。
- `pub use alloy_evm::block::state_changes as state_change` — gravity 还在用
  这个别名（删了上游 alias 的话 gravity 内部用 `state_change::*` 的地方
  会断）。
- `mod engine;` 在 baseline 不加 `#[cfg(feature = "std")]` 门 — gravity 走
  std-only 路径。
- doc example 不含 `slot_number`（因为 gravity 不上 Amsterdam）。

**影响范围**: `reth-evm` 公共 API；下游所有 EVM 用户跟随。

**解决方案建议**: **keep-gravity** + **mechanical-merge**：
- 保留 `parallel_execute` 模块 + `ParallelDatabase` trait + `ParallelExecutor`
  re-export；保留 `state_change` alias（gravity 内部使用）。
- 接受上游 revm/alloy-evm bump 与 `BlockBuilder::finish(state_provider, None)`
  签名（跟着 `crates/evm/evm/src/execute.rs` 联动改）。
- `engine` 模块去掉 `#[cfg(feature = "std")]` 门以保留 gravity 行为（gravity
  不 build no-std），或者跟上游门控但确保 gravity 默认 features 开启 std。
- doc example 加 `slot_number: None`（doctest 跟上游通过即可，链上语义不
  关）；`builder.finish(state_provider, None)` 同步。

**推理**: parallel execution 是 gravity 的核心性能路径（grevm 集成），不能动。
其它机械同步。

---

### `crates/evm/evm/src/noop.rs`

**模块**: `NoopEvmConfig` 测试/类型 hack。

**冲突类型**: AA。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: bump alloy-evm to 0.28.0` (#22636) — `ConfigureEngineEvm` 加了
  `tx_iterator_for_payload` 方法；`NoopEvmConfig` 跟着 impl。
- 把 `ConfigureEngineEvm` impl 用 `#[cfg(feature = "std")]` 门起来。

**Gravity 侧变更** (baseline only)：
- 给 `NoopEvmConfig` 加 `parallel_executor<DB: ParallelDatabase>(&self, db)`
  方法（与 `lib.rs` 中 `ConfigureEvm::parallel_executor` 的扩展配套）。

**影响范围**: 测试与"类型系统占位"使用点。

**解决方案建议**: **mechanical-merge** — 保留 gravity 的 `parallel_executor`
派发；接受上游 `tx_iterator_for_payload` 派发与可选的 `#[cfg(feature = "std")]`
门（如选保留 std-only 则跳过门）。

**推理**: 两边方法互不冲突，机械并集。

---

### `crates/ethereum/evm/Cargo.toml`

**模块**: ethereum EVM impl crate 依赖。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: bump alloy-evm to 0.28.0` (#22636) 与
  `feat: configurable EVM execution limits` (#21088) — 修 `test-utils` feature
  集，新增 `std → reth-storage-errors/std`。
- `test-utils = ["std", ...]`（上游 v2.3.0 把 test-utils 加 std 依赖）。

**Gravity 侧变更** (baseline only)：
- `grevm.workspace = true`、`gravity-primitives.workspace = true` —
  parallel execution + onchain config 依赖。
- `parking_lot = { workspace = true, optional = true }` +
  `derive_more = { workspace = true, optional = true }` 与 `test-utils =
  ["dep:parking_lot", "dep:derive_more", ...]` — gravity 的 `MockEvmConfig` /
  `MockExecutor` 用 `parking_lot::Mutex` + `derive_more::Debug`。
- `[lints.workspace.gravity]` 段或 `ignore = ["grevm"]` 在 Cargo metadata 里
  把 grevm 加到 unused-deps 白名单。
- `"derive_more?/std"` 在 `std` feature 列表里。

**影响范围**: 编译；如果选错 test-utils 依赖集合，`MockEvmConfig` 编不过。

**解决方案建议**: **mechanical-merge** — 并集：保留 gravity 的 grevm /
gravity-primitives / parking_lot / derive_more / config-from-env；新增上游的
`reth-storage-errors/std`。`test-utils` feature 列表也要并集（gravity 的
`dep:parking_lot` + `dep:derive_more`，加上上游的 `std` 与 mock 依赖）。

**推理**: 依赖并集类，风险低；test-utils feature 集合并集即可。

---

### `crates/ethereum/evm/src/lib.rs`

**模块**: `EthEvmConfig` — `ConfigureEvm` 在以太坊 mainnet/genesis 上的具体
实现；`EthEvm` re-export。

**冲突类型**: UU（diff 664 行）。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: bump revm 40 (#24395)` + `chore: bump revm to v37 (#23191)` +
  `chore: bump alloy-evm` (#23289 + #22636) — `EvmFactory` / `BlockEnv` /
  `FromRecoveredTx` / `FromTxWithEncoded` 等签名升级。
- `chore(BAL): added changes for slotnum` (#23605) — `NextEvmEnvAttributes` /
  `NextBlockEnvAttributes` 上加 `slot_number: Option<u64>`。
- `feat: parallelize recovery` (#20169) + `perf: use indexed parallel iterators
  for tx recovery` (#20342)。
- `mod engine;` 上游 `#[cfg(feature = "std")]` 门 + `ConfigureEngineEvm` /
  `ExecutableTxIterator` re-export 改进。
- `precompiles::PrecompilesMap` 进入 `EthEvmConfig`。
- 顶部 import 大规模重排（`alloy_eips::Decodable2718`、`alloy_primitives::Bytes`
  搬动）。

**Gravity 侧变更** (baseline only)：
- `mod parallel_execute;` + `use crate::parallel_execute::GrevmExecutor` —
  gravity 的 grevm executor 入口。
- `mod hardfork;` — gravity hardfork stub framework（`144fb1eea2 feat: add
  hardfork testing framework (#307)`）。
- `364b851665 feat(fee): chainspec floor with code-driven activation schedule
  (#337)` 与 `7d0483e565 feat(fee): enforce 50 Gwei minimum base fee for Gravity
  (#335)` — `EthEvmConfig` 上加 base-fee floor 钩子，与 `chainspec/api.rs` 的
  `gravity_min_base_fee_at_block` 配套。
- `a077894a7d fix(precompiles): execute system transactions as normal transactions
  (#288)` — 把 system txn 当 normal txn 走 EthEvm 全栈。
- `gravity_primitives::get_gravity_config` 整文流转 — 决定 chain id / EVM
  config。
- `parallel_execute::{ParallelExecutor, WrapExecutor}` 用于把 alloy-evm 原生
  executor 包成 gravity trait 兼容形态。
- `alloy_evm::{precompiles::DynPrecompile, EthEvmFactory}` 与
  `revm::{database::{State, WrapDatabaseRef}}` 等额外 import。

**影响范围**: 节点启动时的整 EVM 配置；包括 base-fee floor 校验、grevm 并行
执行入口、hardfork 注入。错合会破坏 base-fee floor（链上语义事故）或
disable grevm（性能 5-10x 退化）。

**解决方案建议**: **keep-gravity** + **needs-port**：
1. **保留** `mod parallel_execute` + `mod hardfork` + `gravity_min_base_fee`
   钩子 + system-txn 走 EthEvm 主流程的所有改动 + `WrapExecutor` 通路。
2. **吸收** 上游 revm 40 / alloy-evm bump（workspace 已统一升级）、
   `NextBlockEnvAttributes::slot_number` 字段（gravity 不用 Amsterdam，置
   `None` 即可，但字段要在）。
3. **不吸收** `#[cfg(feature = "std")]` 门控（gravity 不维护 no-std build）—
   或保留门并确保 default features 含 std。
4. **吸收** `PrecompilesMap` / `FromRecoveredTx` / `FromTxWithEncoded` 等
   alloy-evm 新公共类型并跟随 trait impl。

**推理**: 这是 gravity-specific 经济模型（50 Gwei floor）+ 性能内核（grevm）
+ system txn 行为（验证人交易）三大支柱的汇合点；只能以 gravity 为骨架做
careful merge。

---

### `crates/ethereum/evm/src/test_utils.rs`

**模块**: `MockEvmConfig` / `MockExecutor` 测试 fixture。

**冲突类型**: AA（baseline 与 v2.3.0 几乎完全不同的实现 — baseline 自己
实现一整套 213 行的 `MockExecutor`，上游 v2.3.0 改成 type alias 复用
`NoopEvmConfig`）。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: bump alloy-evm to 0.28.0` (#22636)、`feat: bump alloy and alloy-evm`
  (#21337) — 配合 trait 升级。
- `feat: parallelize recovery` 周边 — 几乎把整个文件简化为：
  ```rust
  pub type MockExecutorProvider = MockEvmConfig;
  pub type MockEvmConfig = NoopEvmConfig<...>;
  ```
  上游 7-8 行搞定。

**Gravity 侧变更** (baseline only)：
- 自带完整 `MockEvmConfig { inner: EthEvmConfig, exec_results: Arc<Mutex<Vec<
  ExecutionOutcome>>> }`、`MockExecutor<'a, DB, I>`、`BlockExecutorFactory`
  impl、`BlockExecutor` impl，覆盖 `execute_transaction_without_commit` /
  `commit_transaction` / `transact_system_txn` / `apply_state_change` /
  `parallel_executor` 等 gravity-extended trait 全部方法。
- 用 `parking_lot::Mutex`、`derive_more::Debug`。

**影响范围**: 仅测试代码；不影响 binary 行为。但 gravity 自有的 trait 扩展
方法（`transact_system_txn` / `apply_state_change` / `parallel_executor`）
必须在 MockExecutor 上 impl，否则 gravity test 编不过。

**解决方案建议**: **keep-gravity** — 保留 gravity 自有的 `MockEvmConfig` /
`MockExecutor` 整套实现（上游的 type alias 形式无法满足 gravity trait
surface）。如果 gravity trait surface 在合并时跟随上游加了新方法（如
`take_bal`），同步在 `MockExecutor` 上 impl 一个返回 `None` 的版本。

**推理**: gravity test 用的 mock 必须实现完整 trait surface（包括 gravity
扩展），上游简化版本不兼容。

---

### `crates/consensus/common/src/validation.rs`

**模块**: 区块预执行校验（ommer hash / tx root / withdrawals / blob gas /
base fee 等）。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `perf(engine): precompute tx root during payload validation` (#22489) —
  新增 `validate_block_pre_execution_with_tx_root<B, ChainSpec>(block, chain_spec,
  transaction_root: Option<B256>)`，把 `validate_block_pre_execution` 改成
  `validate_block_pre_execution_with_tx_root(block, chain_spec, None)` 转发；
  并把 ommer/4844/7934 校验抽到独立函数 `post_merge_hardfork_fields`。
- `fix(consensus): always validate minimum gas limit` (#23441) —
  `validate_header_gas` 加 `MINIMUM_GAS_LIMIT` 下界检查。
- `feat: configurable EVM execution limits` (#21088) —
  `ChainSpec` 上 `EvmLimitParams` 流转，校验路径用到。
- `chore(consensus): Add trait object error variant to ConsensusError` (#20875)。
- `chore: make extra_data_size_limit configurable in EthBeaconConsensus` (#19496)
  — `MAXIMUM_EXTRA_DATA_SIZE` 使用方式变化。
- `chore: sanity check for u64::Max` (#20373)。
- `ChainSpec: EthereumHardforks` trait bound 在 baseline 是
  `EthereumHardforks`，上游已改成 `EthChainSpec + EthereumHardforks`（base-fee
  params 现在通过 `EthChainSpec` 拿）。

**Gravity 侧变更** (baseline only)：
- baseline 已包含 `MAX_RLP_BLOCK_SIZE`、`TxGasLimitTooHighErr`、
  `MAX_TX_GAS_LIMIT_OSAKA`、`transaction::TxHashRef` 这些 import（与上游一致，
  来自 v1.8.3 catch-up）。
- `364b851665 feat(fee): chainspec floor with code-driven activation schedule
  (#337)` + `7d0483e565 feat(fee): enforce 50 Gwei minimum base fee for Gravity
  (#335)` 在这个文件触发 — 加 50 Gwei base fee floor 校验逻辑（hunk 显示
  `alloy_eips::eip1559::INITIAL_BASE_FEE` import 与对应校验代码）。
- baseline `validate_block_pre_execution` 直接调 `block.ensure_transaction_root_valid()`，
  没拆出 `_with_tx_root` 版本（v2.3.0 上游 #22489 引入）。
- `ChainSpec: EthereumHardforks`（不含 `EthChainSpec`）— 与 chainspec 侧的
  `EthChainSpec` trait 加方法的演进同步。

**影响范围**: 区块预执行校验是共识规则的一部分；错合会导致接受/拒绝
区块的行为变化（chain split / accept invalid block）。

**解决方案建议**: **mechanical-merge** + **keep-gravity**：
1. **保留** gravity 的 50 Gwei base-fee floor 校验（#335 / #337）；这部分若被
   覆盖会破坏 gravity 链经济模型。
2. **吸收** 上游 `validate_block_pre_execution_with_tx_root` 拆分（性能优化，
   不改语义）— `validate_block_pre_execution` 仍然存在，转发到 `_with_tx_root(.., None)`。
3. **吸收** `post_merge_hardfork_fields` 抽出（结构性重构，不改语义）。
4. **吸收** `validate_header_gas` 加 `MINIMUM_GAS_LIMIT` 下界（共识规则收紧，
   gravity 也想要 — gravity 不可能用低于此值的 gas limit）。
5. **吸收** `ChainSpec: EthChainSpec + EthereumHardforks` trait bound 升级（与
   chainspec/api.rs 一致）。
6. **trim** 上游 OP/4844 注释级别修正中无 gravity 影响的 doc 改动机械同步。

**推理**: gravity 50 Gwei floor 是 must-keep；上游 #22489 是性能优化与重构，
语义保持向后兼容（`_with_tx_root(.., None)` 即旧行为），可以并入。

---

### `crates/chainspec/src/api.rs`

**模块**: `EthChainSpec` trait — chainspec 公共 API。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: relax ChainSpec impls` (#18894) — `EthChainSpec` impl 从
  `impl EthChainSpec for ChainSpec` 改成 `impl<H: BlockHeader> EthChainSpec for
  ChainSpec<H>`（chainspec 泛化到任意 Header）。
- `refactor(chainspec): use existing paris difficulty getter` (#22474) —
  `final_paris_total_difficulty()` 改用 `self.get_final_paris_total_difficulty()`。
- `next_block_base_fee` 在 trait 默认实现里走纯 EIP-1559 计算（无 floor）。
- `type Header = H` 关联类型（替代 baseline 的 `type Header = Header` 写死）。

**Gravity 侧变更** (baseline only)：
- `d0666f2ab2 feat(chainspec): add GravityHardfork enum and gravity_hardforks
  field (#309)` + `3c7634a6e1 refactor(hardfork): unified hardfork framework
  with stub implementations (#312)` — 在 trait 上新增
  `gravity_hardforks(&self) -> &reth_ethereum_forks::ChainHardforks` 方法。
- `364b851665` + `7d0483e565` (#337 / #335) — trait 上新增：
  - `gravity_min_base_fee_at_block(&self, block: u64) -> Option<u64>`（带默认
    实现 `None`，非 gravity chainspec 自动 fallback）。
  - `next_block_base_fee` 重载：取 `floor = gravity_min_base_fee_at_block`，
    `parent_base_fee = parent.base_fee_per_gas().or(floor)`，clamp 输出 `next
    = next.max(floor)`；当 floor 为 None 时退化到上游 EIP-1559 公式。
- impl block：`type Header = Header`（写死，未泛化）+
  `gravity_min_base_fee_at_block` 实现读 `self.gravity_min_base_fee` 与
  `self.gravity_min_base_fee_activation_block`。

**影响范围**: 这是 gravity base-fee floor 的 trait 入口；所有 EVM 配置 /
共识校验 / RPC 都通过这个 trait 读 base fee。错合 = 链上 base fee 不正确。

**解决方案建议**: **keep-gravity** + **mechanical-merge**：
1. **保留** `gravity_hardforks` trait 方法、`gravity_min_base_fee_at_block`
   trait 方法（含默认实现）、`next_block_base_fee` 的 floor-clamped 重载、
   `ChainSpec` impl 中的 floor 实现。
2. **吸收** 上游 `impl<H: BlockHeader> EthChainSpec for ChainSpec<H>` 泛化（与
   `chainspec/spec.rs` 配套，泛化后 `type Header = H`）。
3. **吸收** `final_paris_total_difficulty` 用 `get_final_paris_total_difficulty`
   helper。

**推理**: gravity 自有 trait 方法 + ChainSpec 泛化两件事互不冲突，机械合并
即可；floor 逻辑必须保留。

---

### `crates/chainspec/src/constants.rs`

**模块**: chainspec 模块的常量集合（gas / prune / base-fee）。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- 无（`git log v1.8.3..v2.3.0 -- crates/chainspec/src/constants.rs` 返回空）。

**Gravity 侧变更** (baseline only)：
- `7d0483e565 feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)` /
  `364b851665 feat(fee): chainspec floor with code-driven activation schedule
  (#337)` 引入 `pub const GRAVITY_MIN_BASE_FEE: u64 = 50_000_000_000;`。

**影响范围**: 常量公开 — `chainspec/lib.rs` 测试与 `chainspec/api.rs` 注释引用；
若误删则下游 doc/test 编不过。

**解决方案建议**: **keep-gravity** — 上游零变更，直接保留 gravity 侧。
冲突在 worktree 上以 UU 形式出现仅因 base 标记问题；应直接选 gravity 侧。

**推理**: 上游无改动，gravity 侧改动唯一来源；纯 gravity-only 文件实质。

---

### `crates/chainspec/src/lib.rs`

**模块**: `reth-chainspec` crate 顶层 — `pub use`、test。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `pub use alloy_evm::EvmLimitParams` (#21088)。
- `feat: configurable EVM execution limits` (#21088) +
  `e53990cf4 fix(chainspec): add ChainConfig to StatelessInput and add
  ChainConfig creator helpers (#20101)` — `pub use spec::{..., blob_params_to_schedule,
  create_chain_config, mainnet_chain_config, ...}`。
- `d148f39cc refactor(chainspec): remove unused once_cell_set utility (#23043)` —
  `once_cell_set<T>` helper 移除。
- `chore: remove doc_auto_cfg feature` (#18758)。
- 测试 `test_centralized_base_fee_calculation`（已存在）在上游 simplify 到
  纯 EIP-1559 算法。

**Gravity 侧变更** (baseline only)：
- `pub use gravity::GravityHardfork`（#309 引入）。
- `pub fn once_cell_set<T>(...) -> OnceLock<T>` 保留（上游已删，gravity 仍用
  作 LazyLock 替代）。
- 测试 `test_centralized_base_fee_calculation` 被改写：包含两个 scenario，
  一个是非 gravity chainspec（上游行为），一个是 gravity main（50 Gwei floor）。

**影响范围**: 公共 re-export 集合 + 测试。

**解决方案建议**: **mechanical-merge** + **keep-gravity**：
1. **保留** `pub use gravity::GravityHardfork`、`once_cell_set` helper（gravity
   内部多处使用，删了要连锁改）、扩展的 base-fee 测试。
2. **吸收** `pub use alloy_evm::EvmLimitParams`、`blob_params_to_schedule` /
   `create_chain_config` / `mainnet_chain_config` 三个新 re-export。
3. **拒绝** 上游测试的 simplification（gravity 版本覆盖更多）。
4. `#![cfg_attr(docsrs, ...)]` 上游 `doc_auto_cfg` feature 移除 — 同步移除
   （doc 行为，不影响 binary）。

**推理**: 单纯的 re-export 并集 + 测试以 gravity 版本为准（覆盖度更高）。

---

### `crates/chainspec/src/spec.rs`

**模块**: `ChainSpec` struct 定义 + mainnet/sepolia/holesky/hoodi 静态实例 +
`make_genesis_header`。

**冲突类型**: UU（diff 925 行）。

**上游变更** (v1.8.3..v2.3.0)：
- `chore: add amsterdam time to chainspec` (#23526) — `ChainSpec` 上新增
  `amsterdam_time: Option<u64>`，`make_genesis_header` 检查 Amsterdam 激活并写
  `block_access_list_hash` / `slot_number` 到 header。
- `chore: bump alloy to 2.0.0` (#23407)、`feat: make ChainSpec generic over
  header` (#18856) — `ChainSpec` 泛化为 `ChainSpec<H = Header>`。
- `feat: support non-zero genesis block numbers` (#19877) —
  `number: genesis.number.unwrap_or_default()`、`parent_hash:
  genesis.parent_hash.unwrap_or_default()` 进入 `make_genesis_header`。
- `feat: schedule fusaka` (#19455) — fork condition 编排更新。
- `feat: display blob params alongside hardfork info` (#19358)。
- `chore: avoid eager evaluation in base_fee_params_at_timestamp` (#21536)。
- `ec50fd40b chore(chainspec): use ..Default::default() in create_chain_config`
  (#21266)。
- `chore(net): remove OP stack bootnodes` (#21984) — `op_nodes` / `op_testnet_nodes`
  从 `reth_network_peers` 移除。
- 其余 doc / clippy / 字段排序。

**Gravity 侧变更** (baseline only)：
- `d0666f2ab2 feat(chainspec): add GravityHardfork enum and gravity_hardforks
  field (#309)` — `ChainSpec` 上加 `gravity_hardforks: ChainHardforks` 字段。
- `7d0483e565 feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)`
  + `364b851665 feat(fee): chainspec floor with code-driven activation schedule
  (#337)` — `ChainSpec` 上加 `gravity_min_base_fee: Option<u64>` 与
  `gravity_min_base_fee_activation_block: u64`。
- `MAINNET` 静态构造里初始化以上 3 个字段（默认 None / 0 / empty
  hardforks）。
- 仍 import `op_nodes` / `op_testnet_nodes`（baseline 上 v1.8.3 时上游还有）。

**影响范围**: `ChainSpec` 结构体是整个 reth 类型系统的根；字段集合错合
导致所有 chainspec 反序列化失败（cannot parse genesis JSON）。

**解决方案建议**: **mechanical-merge** + **keep-gravity**（关键字段）：
1. **保留** `gravity_hardforks` / `gravity_min_base_fee` /
   `gravity_min_base_fee_activation_block` 三个字段，确保
   `serde(default)` 让旧 genesis 不带这些字段时也能反序列化。
2. **吸收** 上游 `amsterdam_time` 字段 + `make_genesis_header` 中
   `block_access_list_hash` / `slot_number` 的写入逻辑（gravity 不激活
   Amsterdam，Option 永远 None，但字段必须在以适配 trait）。
3. **吸收** `ChainSpec<H = Header>` 泛化（与 `api.rs` 一致）。
4. **吸收** `genesis.number.unwrap_or_default()` 等 helper、`create_chain_config`
   helper、`chore: avoid eager evaluation` 改写。
5. **吸收** `op_nodes` / `op_testnet_nodes` 移除（gravity 不发往 OP，import
   清理掉就行）。
6. **保留** gravity 字段在所有静态 chainspec（MAINNET / SEPOLIA / HOLESKY /
   HOODI / DEV）的初始化中。

**推理**: ChainSpec 结构体扩展只能并集，不能取舍 — 任何字段缺失都导致编译
错误或反序列化错误；gravity 三字段 + 上游 amsterdam_time 都必须在。

---

### `crates/chain-state/src/in_memory.rs`

**模块**: `CanonicalInMemoryState` / `InMemoryState` / `BlockState` /
`MemoryOverlayStateProvider` 工厂 — 内存里挂在 head 之前的 unpersisted
chain。

**冲突类型**: UU（diff 867 行）。

**上游变更** (v1.8.3..v2.3.0)：
- `refactor(chain-state): manage state trie overlays centrally` (#24184) +
  `feat(chain-state): add persisted block tracking` (#20876) — 加入
  `StateTrieOverlayManager` / `ComputedTrieData` / `LazyTrieData`，trie 计算
  由 chain-state 集中调度。
- `refactor(db): use hashed state as canonical state representation` (#21115)。
- `perf(engine): defer trie overlay computation with LazyOverlay` (#21133) +
  `perf: make Chain use DeferredTrieData` (#21137) +
  `feat(trie): Merge trie changesets changes into main` (#19068)。
- `refactor: use BlockExecutionOutcome in ExecutedBlock` (#21123) +
  `feat: Add TrieUpdatesSorted and HashedPostStateSorted in all ExEx notifications`
  (#20333) — `ExecutedBlock` 字段重组（合并 trie + state 到 `ComputedTrieData`），
  不再有独立的 `ExecutedBlockWithTrieUpdates` 类型。
- `a3482dfa6 fix(chain-state): publish deferred trie data from task (#24995)`。
- `B256Map` 替换 `HashMap<B256, _>`（perf）。

**Gravity 侧变更** (baseline only)：
- 整文使用 `ExecutedBlockWithTrieUpdates`（gravity baseline 保留了
  `ExecutedBlock` 与 `ExecutedBlockWithTrieUpdates` 的二分），由 #109 引入
  trie cache 路径。
- `TrieUpdates` / `TrieUpdatesV2` 双轨（gravity 用 V2 做 compact serialization
  — `671680af37 perf(state_root): compact trie node serialization (#149)`）。
- `HashMap<B256, Arc<BlockState<N>>>`（alloy `B256Map` 在 gravity baseline 还
  未引入到这里）。
- `set_pending_block(pending: ExecutedBlockWithTrieUpdates<N>)`、
  `update_blocks<I, R>` 用 `ExecutedBlockWithTrieUpdates<N>`。
- `cb2992e451 refactor: fix the compile warning of nested trie (#122)`、
  `66ab036739 opt(cache): add account and state root cache (#75)`、
  `ac4d30767d (fix) Disable trie cache when calculating state root (#84)` —
  gravity 自有的 trie cache 流。

**影响范围**: 整 chain-state crate；下游 engine tree / RPC / sync 都依赖
此处的 `BlockState` / `CanonicalInMemoryState` API。`ExecutedBlock` vs
`ExecutedBlockWithTrieUpdates` 类型选择决定 gravity trie cache 是否仍可工作。

**解决方案建议**: **keep-gravity** + **needs-port**（性能项）：
1. **保留** `ExecutedBlockWithTrieUpdates` / `ExecutedTrieUpdates` 二分（这是
   gravity trie cache 体系的类型基石，删了 #109 / #149 / #75 / #84 / #122 全
   失效）。
2. **拒绝** `LazyTrieData` / `ComputedTrieData` / `StateTrieOverlayManager`
   集中调度（与 gravity 的 pipe-exec 自驱动 trie 计算冲突）。
3. **拒绝** `BlockExecutionOutcome` 合一（这会让 gravity 手动维护的
   `ExecutionOutcome` 接口断）。
4. **机械吸收** `B256Map` 替换 `HashMap<B256, ...>`（纯 perf，等价于
   `HashMap<B256, _, RandomState> → HashMap<B256, _, FxBuildHasher>`，gravity
   也想要）。
5. **机械吸收** `IndexedTx`、`SignedTransaction` 等 trait surface 升级（baseline
   已通过 #205 持有）。

**推理**: `ExecutedBlockWithTrieUpdates` 在 gravity 是 persistence /
trie-cache / state-root 三处流转的核心 type；强行换成上游统一 `ExecutedBlock`
是大量 gravity 模块同时崩。perf 微优化（B256Map）可独立机械吸收。

---

### `crates/chain-state/src/memory_overlay.rs`

**模块**: `MemoryOverlayStateProvider` / `MemoryOverlayStateProviderRef` —
在内存 unpersisted blocks 上做 state lookup 的 overlay。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `refactor(db): use hashed state as canonical state representation` (#21115) +
  `perf(trie): compute and sort trie inputs async` (#19894) +
  `feat: Add TrieUpdatesSorted ... in all ExEx notifications` (#20333) —
  `trie_input()` 改用 `extend_from_sorted` + 倒序遍历；引入 `merged_hashed_storage`
  helper。
- `feat(stateless): make witness generation conform to the draft specs` (#22289)。
- `feat(storage): slot preimage DB for plain changeset keys in v2` (#22379)。
- `chore(db): Remove Sync from DbTx` (#20516)。
- `in_memory: Cow<'a, [ExecutedBlock<N>]>`（替代 baseline `Vec<...>`）— 允许
  borrow path 减一次 clone。
- `pub type MemoryOverlayStateProvider<N> = MemoryOverlayStateProviderRef<...>`
  type alias 重新定义。

**Gravity 侧变更** (baseline only)：
- `in_memory: Vec<ExecutedBlockWithTrieUpdates<N>>` — type 选 gravity 二分。
- `trie_input()` 用 `TrieInput::from_blocks(...)` helper（gravity baseline 引
  的）— 与上游 `extend_from_sorted` 不同。
- 显式 `pub type MemoryOverlayStateProvider<N> = MemoryOverlayStateProviderRef<
  'static, N>;` 写在文件内。

**影响范围**: state lookup 路径；trie cache 准确性 — 错合直接出现错误的
状态根。

**解决方案建议**: **keep-gravity** + **mechanical-merge** for type alias：
1. **保留** `ExecutedBlockWithTrieUpdates` type 与 `Vec<...>` 储存（与
   `in_memory.rs` 一致）；保留 `TrieInput::from_blocks` 调用。
2. **吸收** `merged_hashed_storage` helper（上游 perf 优化，gravity 也用）。
3. **可选吸收** `Cow<'a, [..]>` storage 类型 — 但与 gravity 现有调用站点
   API 不完全兼容，建议暂时保留 `Vec`，仅做 type alias 调整。

**推理**: trie cache 流是 gravity 性能核心，不能换；`Cow` 借用优化属于上游
独立优化，可后续单独 port。

---

### `crates/chain-state/src/test_utils.rs`

**模块**: `TestBlockBuilder` — 测试 fixture 生成 mock chain。

**冲突类型**: UU。

**上游变更** (v1.8.3..v2.3.0)：
- `feat: catch-up for read-only ProviderFactorys` (#23357) — fixture 适配
  read-only provider。
- `revert: undo Chain crate, add LazyTrieData to trie-common` (#21155) +
  `perf: make Chain use DeferredTrieData` (#21137) +
  `refactor: use BlockExecutionOutcome in ExecutedBlock` (#21123) — `ExecutedBlock`
  字段重排，引入 `ComputedTrieData`。
- `fix(chain-state): correct balance deduction in test block builder` (#20308)。
- `perf(engine): only recover senders once` (#20118) — fixture 也从
  `SealedBlockWithSenders` 切到 `SealedBlock<...>`。
- 新增 `post_block_state: B256HashMap<(AccountInfo, U256)>` +
  `with_state: bool` 字段，开启后 fixture 生成完整 BundleState/revert。
- `generate_random_block` 返回 `SealedBlock<reth_ethereum_primitives::Block>`。

**Gravity 侧变更** (baseline only)：
- 引入 `ExecutedBlockWithTrieUpdates` / `ExecutedTrieUpdates` import。
- `generate_random_block` 返回 `RecoveredBlock<...>`（与 `block_buffer.rs` /
  `in_memory.rs` 的类型选一致）。
- `Requests` 引入但 baseline 不上 EIP-7685（仅 import 为兼容）。
- 没有 `post_block_state` / `with_state` 字段 — gravity 测试不依赖完整
  BundleState 生成。

**影响范围**: 只影响 test；但作为 gravity tree/in_memory 测试 fixture，类型
选择必须与生产代码一致。

**解决方案建议**: **keep-gravity** for type，**mechanical-merge** for new
fields：
1. **保留** `RecoveredBlock<...>` 返回类型 + `ExecutedBlockWithTrieUpdates`
   import（与 `block_buffer.rs` / `in_memory.rs` 类型决策一致）。
2. **吸收** 上游 `post_block_state` + `with_state` 字段及对应的 `with_state()`
   开关方法（独立功能，不破坏 gravity 测试 — 默认 `with_state = false` 即
   baseline 行为）。
3. **吸收** `B256HashMap` 替换 `HashMap<B256, ...>`（性能优化）。
4. **吸收** `fix correct balance deduction in test block builder` (#20308) —
   bug fix，gravity 也想要。

**推理**: 测试 fixture 在类型选择上必须跟随生产代码（`RecoveredBlock`），
其他字段是上游测试增强功能，可独立加。

---

## 开放问题

> **决策追踪 checklist**:每条目两个勾选框 — 主框「决策」勾选 = 已拍板,
> 条目末尾「→ **决策**: …」记录结论;嵌套框「冲突解决」勾选 = 该决策已在
> worktree 落地(相关冲突块已按决策解掉,经实测核实,附证据)。未勾选 =
> 待决策 / 待落地。勾选纪律:冲突解决框必须实测(`grep '^<<<<<<<'` 计数、
> `git diff` 等),不得从决策存在与否推断。

- [x] 1. **moka vs mini-moka 全栈切换**：建议在该组合并阶段统一切到 `moka`（与上
   游一致），需要 audit gravity 自有 cache 使用点（`tree/cached_state.rs` 等
   非本组文件）的 API 兼容性。
   → **状态**（2026-07-02）: 核实完成，报告见 `moka-vs-mini-moka-verification.md`。
   结论推翻原问题前提：gravity 的 `cached_state.rs` 与 upstream v1.8.3 同文件
   byte-identical（纯 fork 遗留，零 gravity 定制）；上游 cached_state 从未用过
   moka，v2.3.0 已迁到新 crate `reth-execution-cache`（fixed-cache），worktree
   里该 crate 与全部 caller 已就位，旧 `cached_state.rs` 零调用方。**建议选项 C**：
   删 `cached_state.rs`、engine/tree Cargo.toml 与 mod.rs 冲突取 v2.3.0 侧
   （moka 留给 precompile_cache，mini-moka 彻底移除），零额外开发。待拍板勾选。
   → **决策**（2026-07-02）: 选项 C — 删 cached_state.rs（fork 遗留、零调用方），
   engine/tree Cargo.toml 与 mod.rs 相关冲突块取 v2.3.0 侧，mini-moka 全仓移除；
   已执行，见 moka-vs-mini-moka-verification.md 执行记录。
   - [x] 冲突解决:已落地(commit 7df23663c8)— 实测(2026-07-03)
     `cached_state.rs` 已不存在、`mini-moka` 全仓 `*.toml` 零命中、
     `tree/mod.rs` 无 `cached_state` 引用;`engine/tree/Cargo.toml` 尚余
     8 处冲突块,但冲突块内容 grep moka 零命中,均与本决策无关。
- [x] 2. **`ExecutedBlock` vs `ExecutedBlockWithTrieUpdates` 长期策略**：上游已统
   一为单一 `ExecutedBlock + ComputedTrieData`，gravity baseline 仍二分。本组
   决策是保留二分；下一次 merge（v2.4+）时如果上游进一步深化集中式 trie
   overlay，gravity 需要评估是否一次性 port `LazyTrieData` 体系还是继续维护
   二分 fork — 这是 long-running tech debt。
   → **决策**（2026-07-02）: 本轮保留二分；v2.4+ 长期策略作为 tech debt 单独
   评估，不阻塞本次 merge。（注：`in_memory.rs` 冲突尚未解完，决策待执行。）
   → **参见**（2026-07-02）: pipe-exec make-canonical 链路上该类型的全链路
   分析与未闭环清单见 `executed-block-split-pipe-exec-make-canonical.md`。
   → **进展**（2026-07-03, f89d9d4e23）: storage 组以「整体还原 gravity
   baseline」落地了本决策的 storage/trie 侧 — `trie/common/updates.rs`
   (17→0)、storage-api 四文件(4/3/1/6→0)、`database/provider.rs`
   (101→0)、`blockchain_provider.rs`(46→0,与 baseline 零 diff)等冲突
   全归零(实测);engine/chain-state 侧(`in_memory.rs`/`tree/mod.rs`/
   `persistence.rs`/`tests.rs`)未动。engine 侧实施路线随之反转(原"保
   v2.3.0 validator 骨架"改为"整体复原 baseline"),见
   `executed-block-split-pipe-exec-make-canonical.md` §6.5.1(⟲ 标记)。
   - [ ] 冲突解决:未落地(storage/trie 侧已由 f89d9d4e23 解决,
     engine/chain-state 侧未动)— 实测(2026-07-03)`in_memory.rs` 仍有
     39 处冲突块,类型定义本身在冲突块 HEAD 侧;`memory_overlay.rs` 为
     上游版(归条目 5,决策已拍板待落地)。
- [x] 3. **`NextBlockEnvAttributes::slot_number`**：gravity 不上 Amsterdam，长期填
   `None`。如果未来要走 Amsterdam（EIP-7928 BAL），gravity 需要先决定 BAL
   是否纳入链上语义；目前所有 BAL 相关 trait 方法 gravity 实现都应返回 `None`
   / `Empty`。
   → **决策**（2026-07-02）: 字段引入、gravity 侧填 `None`；是否上
   Amsterdam/BAL 属未来业务决策，不阻塞本次 merge。
   - [ ] 冲突解决:未落地 — 实测(2026-07-03)`crates/evm/evm/src/lib.rs`
     仍有 18 处冲突块,`slot_number` 字段(第 713 行)在冲突块 v2.3.0 侧
     尚未解出;gravity 构造点(pipe-exec execute/src/lib.rs:1180 的
     `NextBlockEnvAttributes { … }`)尚未补 `slot_number: None`(连同
     `extra_data` 字段),解冲突时需一并落地。
- [x] 4. **上游 #21226 `move execution logic from metrics to payload_validator`
   不跟进**：metrics.rs 保留 `execute_metered` helper。如果未来要重构
   `payload_validator`，需要确认 helper 与新 validator 接口不冲突。
   → **决策**（2026-07-02）: 不跟进 #21226，保留 `execute_metered`（已在
   worktree 落地，`tree/metrics.rs:83`）。
   → **决策修订**（2026-07-02）: 原决策前提（"删 helper 会断 gravity 调用点"）
   经实测失效，修订为**跟进 #21226**：metrics.rs 剩余冲突取 v2.3.0 侧，删
   `execute_metered` + `MeteredStateHook` 及其自测试（metrics.rs:885/945）。
   依据三条实测：(a) `execute_metered` 并非 gravity 定制，是纯上游 v1.8.3
   遗产（`gravity-base/v1.8.3-clean-ancestry` 的 metrics.rs:60 /
   payload_validator.rs:767 逐行存在），对 v2.3.0 diff 时才显得像
   baseline-only；(b) worktree `payload_validator.rs` 已与 v2.3.0 tag 零
   diff，原调用点（v1.8.3 payload_validator.rs:767）已随之消失；(c) 全仓
   现存调用方仅剩 metrics.rs:885/945 两个自测试，pipe-exec 路线有自有
   执行 + metrics，从不引用该 helper。执行条件：`tree/mod.rs` 剩余冲突解完
   后确认无 gravity 侧新调用点（main 分支实测 tree/mod.rs 零引用，风险
   极低）。
   → **已执行**（2026-07-03）: metrics.rs 整文件取 v2.3.0 侧（先核实
   gravity 对该文件零定制 — 基线 diff 为零；现与 tag 零 diff、冲突全解），
   `MeteredStateHook` 定义与 impl 已从 tree/mod.rs 移除，全仓
   `execute_metered` / `MeteredStateHook` 零残留。遗留：tree/mod.rs 剩余冲突
   解完后复核无新调用点，并清理失去使用点的 `OnStateHook` /
   `StateChangeSource` / `EvmState` import。
   - [x] 冲突解决:已落地 — 实测(2026-07-03)`metrics.rs` 与 v2.3.0 tag
     零 diff(原 13 处冲突块全解),全仓 `execute_metered` /
     `MeteredStateHook` grep 零命中;遗留仅 tree/mod.rs 收尾时的 import
     清理与复核(见上「已执行」段),不影响本决策落地判定。
- [x] 5. **`trie_input` 在 `memory_overlay.rs` 中 `extend_from_sorted` vs
   `from_blocks` 的性能差**：上游 #19894 / #20333 引入 sorted 路径有明确
   perf gain；gravity baseline 仍走 `from_blocks`。本次合并保守保留 gravity
   实现，但建议作为独立 follow-up 评估是否能在不破坏 gravity 二分类型的
   前提下切到 sorted 路径。
   → **状态**（2026-07-02）: 文档结论与当前 worktree **背离** —
   `memory_overlay.rs` 现为上游版（`Cow<'a, [ExecutedBlock<N>]>` +
   `extend_from_sorted`，第 27 / 56-57 行），`ExecutedBlockWithTrieUpdates` 与
   `from_blocks` 已不在。因其类型上游 `in_memory.rs` 仍满是冲突标记，疑为
   squash checkpoint 带入的上游侧、尚未按本决策改回。收尾时须复核后重新拍板：
   改回 gravity 版，或确认二分类型下可直接走 sorted 路径。
   → **决策**（2026-07-03, f89d9d4e23 后拍板）: **整文件复原 baseline**
   (`git checkout 0cb1687c1c -- crates/chain-state/src/memory_overlay.rs`,
   零补丁)。依据:sorted 路径的三个前提已随 storage 组整体还原消失(实测:
   `extend_from_sorted` 全仓不存在;storage-api `witness` 回无 mode 签名
   trie.rs:93;`TrieInput::from_blocks` 的 Option 签名回归 input.rs:36-38),
   当前上游版文件在还原后的 workspace 里必编译失败,baseline 版零补丁可用。
   sorted 路径 perf follow-up 顺延至 v2.4+ 与二分长期策略(条目 2)一并评估。
   详见 `executed-block-split-pipe-exec-make-canonical.md` §6.2.2(⟲)。
   - [ ] 冲突解决:未落地(决策已定,checkout 待执行;落地证据 = 文件与
     baseline 零 diff + `cargo check -p reth-chain-state`,编译证据待
     cargo workspace 依赖修复后回填)。
