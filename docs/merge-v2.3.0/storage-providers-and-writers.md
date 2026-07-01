# storage-providers-and-writers

> Baseline: `0cb1687c1c` (gravity main, 2026-06-09)
> 已合并的上游 tag: reth `v1.8.3` (#205 `d620fd0eeb`, 2025-11-10)
> 目标: reth `v2.3.0`

## 分组概要

- 文件数: 13 (11 UU + 2 AA)
- 复杂度: 高。`database/provider.rs` 单文件 conflict diff 接近 5.7k 行;`static_file/manager.rs` 约 2.5k 行;`database/mod.rs` 与 `state/historical.rs` 各 1k+ 行。
- 涉及模块功能:
  - `ProviderFactory` (DB + static-file 工厂; 上游 v2.3.0 把 RocksDB / changeset cache / runtime / read-only sync state 提升为 struct 字段)
  - `DatabaseProvider` (重型 RO/RW provider; trie unwind; block 插入/删除)
  - `BlockchainProvider` (内存 overlay + DB 门面;RPC 与 engine 的中心 provider)
  - `ConsistentProvider` (快照视图,`BlockchainProvider` 的后端)
  - `ConsistentDbView` (trie / state-root 并行入口)
  - `HistoricalStateProvider` / `LatestStateProvider` (按区块取状态)
  - `StaticFileProvider` 管理器与 `check_consistency`
  - `UnifiedStorageWriter` (gravity 扩展的写入协调器;**上游已在 v2.3.0 移除**)
  - `MockEthProvider` + `create_test_provider_factory_*` 测试脚手架
- 基线 (baseline `0cb1687c1c`) 上已经存在、本组必须保留的 gravity-only 锚定:
  - `ff103f976a` fix(unwind): commit view + prune distance for execution unwind (#313)
  - `25a86ae6d8` fix(unwind): calculate storage state root for EOA (#249)
  - `727c9e5ffc` / `6e8da10b02` fix(recovery): static-file healing (#253, #255)
  - `605c372de6` feat(trie): support `eth_getProof` nested-hash step 1 (#237)
  - `c64bd613e4` opt(persist): sharding RocksDB instances for persist stage (#225)
  - `1539b6cafc` opt(persist): skip index tables for validator-only nodes (#224)
  - `a1d7365bd6` feat(rocksdb): Integrating RocksDB into Reth (#212)
  - `2dde8ca181` state_root: implement cache and state write (#109)
  - `a6e246ffbe` feat(pipe): parallel state root in pipe execution (#82)
  - `377eb491b2` / `4df0b8b36d` ParallelStateProvider (#26, #29)
- 与其它分组的解决顺序依赖:
  - **storage-api-and-traits** 必须先解决。本组引用了 API 分组决定的 trait 名:
    `StorageLocation`, `StateWriteConfig`, `EitherWriter` / `EitherReader` / `EitherWriterDestination`,
    `BalProvider` / `BalStoreHandle`, `RocksDBProvider` / `RocksDBProviderFactory`,
    `StorageSettingsCache`, `TrieWriterV2`, `MetadataProvider`, `NodeTypesForProvider`,
    `ChainStateBlockReader/Writer`。
    建议在 storage-api 里选择 gravity 风格的扁平 trait API (丢掉上游 `Either*` 间接层与 BAL),
    然后本组的方案随之推进。
  - **trie-and-state-root** 分组负责 `NestedStateRoot`, `TrieInputSorted`, `ChangesetCache`,
    `OverlayBuilder`。本组在 `database/provider.rs` 的 unwind 路径里直接消费 `NestedStateRoot`,
    必须保留;`OverlayBuilder` / `ChangesetCache` 来自上游, 由 trie 分组决定是否引入。
  - **chain-state** 分组负责 `ExecutedBlock` / `ExecutedBlockWithTrieUpdates` /
    `ExecutedTrieUpdates`。`blockchain_provider.rs` 测试用到 gravity 的
    `ExecutedBlockWithTrieUpdates::new(block, trie, triev2)` 三参构造形式。

## 逐文件分析

### `crates/storage/provider/Cargo.toml`

**模块:** crate manifest (reth-provider)
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 新增 `alloy-eip7928` (BAL/EIP-7928 类型)、`alloy-genesis`、`smallvec`、`reth-tokio-util`、`reth-tasks`(rayon feature)、dev 依赖 `tokio-stream` 与 `reth-tracing`、dev `reth-tasks/test-utils`;把 `dashmap` 改为通过 `reth-primitives-traits[dashmap]` feature 引入;`reth-db` 启用 `mdbx` feature;`reth-static-file-types` 启用 `std` feature;新增 `jemalloc = ["rocksdb/jemalloc"]` feature。承载 PR:`#23596`/`#23710`/`#23918` (BAL stack)、`#23357` (global runtime + read-only catch-up)、`#21934` (global runtime)、`#23061` (jemalloc gating)、`#22954` (storage-v2 默认化)。
**Gravity 侧变更 (baseline `0cb1687c1c` 上、相对 reth v1.8.3 的 gravity-only 改动):** 来自 `a1d7365bd6` #212:`gravity-primitives.workspace = true`、移除 `reth-db` 的 `mdbx` feature (rocksdb 集成走 db-rocksdb crate)、`pipe_test = []` feature、test-utils 里的 `reth-evm/test-utils`。保留 `reth-evm.workspace = true`、`dashmap = { workspace = true, features = ["inline"] }` 直接依赖。
**影响范围:** Cargo manifest 是叶子节点 — feature/依赖错配会让本 crate 整体编不过。`gravity-primitives` 被 `blockchain_provider.rs:19` 与 `static_file/manager.rs:19` `use gravity_primitives::get_gravity_config;` 强依赖;移除会破坏 #224、#253、#255 三个承重 commit。`pipe_test` feature 唯一消费方是 `writer/mod.rs:172` 的 `cfg(not(feature = "pipe_test"))` block,用于 pipe-exec 集成测试。`reth-evm.workspace = true` 是 gravity #82 并行 state root 的依赖。
**解决方案建议:** mechanical-merge
  - **保留 gravity:** `gravity-primitives.workspace = true`、`reth-evm.workspace = true`、`pipe_test = []` feature、test-utils 里的 `reth-evm/test-utils`、直接 `dashmap = { workspace = true, features = ["inline"] }`。
  - **采纳上游:** `alloy-eip7928`、`alloy-genesis`、`smallvec`、`reth-tokio-util`、`reth-tasks = { workspace = true, features = ["rayon"] }`、`reth-static-file-types = { workspace = true, features = ["std"] }`、dev 的 `reth-tracing`、`tokio-stream`、`reth-tasks/test-utils`、feature `jemalloc = ["rocksdb/jemalloc"]`。
  - **采纳上游:** `reth-db = { workspace = true, features = ["mdbx"] }`(gravity 的 #212 拿掉了这个 feature, 因 rocksdb 集成在另一 crate 实现;v2.3.0 上游同时既支持 mdbx 又默认 rocksdb 路径, 加回 mdbx feature 与 #212 行为兼容,但要确认 `reth-db` crate 的 mdbx feature 在 gravity 的 db-rocksdb 适配后没有冲突)。
  - **采纳上游(条件):** `reth-primitives-traits = { workspace = true, features = ["reth-codec", "secp256k1", "dashmap"] }` 仅当 storage-api-and-traits 分组同时落地 `dashmap` 这个 feature 时启用。否则保留 gravity 直接 `dashmap` 依赖,在 traits 上不开 `dashmap` feature。建议同时保留直接依赖与 traits feature, 二者无冲突。
  - **删除上游:** `alloy-eip7928` 若 storage-api 分组确认删除 BAL,则本依赖也删 — 其唯一消费方是 `BalProvider`、`InMemoryBalStore`。
  - **审计:** `reth-tracing.workspace = true` (dev) 仅在 v2.3.0 上游测试用 — 若 gravity 测试未引入则可省略,但保留更安全。
**推理:** `gravity-primitives::get_gravity_config()` 是 #224 (`1539b6cafc`)、#253 (`727c9e5ffc`)、#255 (`6e8da10b02`) 的 entry point,baseline `0cb1687c1c` 上 grep 命中两处;**删除会破坏链上恢复语义**。`pipe_test` feature 是 #94/#212 引入的 pipe-exec 集成测试钩子,baseline 上 `writer/mod.rs:172` 唯一使用。`reth-evm` 在 baseline `Cargo.toml:32` 是 gravity #212 显式加的 ProviderFactory 编译依赖。

---

### `crates/storage/provider/src/providers/blockchain_provider.rs`

**模块:** 内存 overlay + DB 门面;RPC 与 engine 使用的中心 provider
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):**
  - `#23596` / `#23710` / `#24084` — BAL 栈:`BalProvider` impl、`bal_store: BalStoreHandle` 字段、persistence task 清理。
  - `#23656` — `encapsulate state fetching in db provider`:`MemoryOverlayStateProvider` 路径下沉,简化 `latest()`。
  - `#23310` — 移除 `changeset count` API;新增 `storage_changesets_range`、`account_changesets_range`、`get_storage_before_block`、`get_account_before_block`。
  - `#22876` — 移除顶层 `ensure_canonical_block` 守卫 (下沉到 ConsistentProvider)。
  - `#22379` — slot preimage DB,新增 `StaticFileSegment::TransactionSenders` 等导入。
  - `#24494` — 用 `tx_hash` 作交易身份;`block_by_transaction_id` 替代 `transaction_block`。
  - `#21934` (global runtime) — `Runtime` 字段穿透。
  - 在 BlockchainProvider 上新增 `RocksDBProviderFactory` impl (`unimplemented!` stub 风格,等待具体集成)。
  - `HeaderProvider::header(BlockHash)` 签名从 `&BlockHash` 改为按值;移除 `header_td*`、`header_td_by_number` (`#19151` 的延续)。
**Gravity 侧变更 (baseline `0cb1687c1c` 上, 相对 v1.8.3):**
  - 文件头 `#![allow(unused)]` (`#205` 合并产物;来自上游同期已删的 unused-imports 修复 `cec30cd9f`)。
  - `use gravity_primitives::get_gravity_config;` (来自 `1539b6cafc` #224)。
  - `StateProviderFactory::latest()` 内的 `if get_gravity_config().validator_node_only` 双分支:有 head state 时 — validator 走 `state.state_provider(latest_historical)`,非 validator 走 `block_state_provider(&state)`;无 head state 时 — validator 走 `self.database.latest()`,非 validator 强制返回 `history_by_block_number(best_block_number)`。来自 #224。
  - `state_by_block_number_or_tag` 手写覆盖 (`BlockchainProvider:535`),对 Latest/Finalized/Safe/Earliest/Pending/Number 做完整 match — 这是为了和上面的 validator 分支闭合。
  - 保留 `header_td(&BlockHash)`、`header_td_by_number(BlockNumber)`、`recover_block_number()`、`transaction_block(TxNumber)`、`get_state(range)` — 这些方法在上游 v1.8.3..v2.3.0 区间被删 (`#19151`、`#19585`、`#23310`、`#23656`)。
  - 测试 (`#[cfg(test)]` 块,约第 760 行起):使用 `UnifiedStorageWriter::commit(provider_rw)?`、`UnifiedStorageWriter::from(&provider_rw, &static_file_provider)`、`ExecutedBlockWithTrieUpdates::new(.., ExecutedTrieUpdates::empty(), Default::default())` 三参构造形式。
**影响范围:**
  - `BlockchainProvider` 是 pipe-exec、RPC、engine-tree 共同的中心 provider。打破 trait 接口会级联到 `crates/pipe-exec-layer-ext-v2/execute/` 与 RPC handler。
  - `validator_node_only` 双分支是承重的 RPC 行为(MEMORY 中提到 gravity RPC 行为分歧:full node 必须服务历史状态,validator 只服务 latest)。删除会回退 #224。
  - `BalProvider` 在 gravity-reth 里没有 RPC / 链上消费方 — `grep -r BalProvider crates/rpc/` 在 baseline 上为空。
**解决方案建议:** mechanical-merge (倾向 keep-gravity)
  - **保留 gravity:** 整段 `StateProviderFactory::latest` 与 `state_by_block_number_or_tag` 的 validator 分支 (锚定 #224)。
  - **保留 gravity:** `recover_block_number`、`transaction_block`、`header_td`、`header_td_by_number`、`get_state` — 这些是 baseline 上 gravity 仍消费的方法 (`crates/cli/commands/`, gravity RPC ext)。仅当 storage-api-and-traits 分组在 `BlockNumReader`/`HeaderProvider` trait 上彻底删掉对应 method 时才一并删除。
  - **删除上游:** `BalProvider` impl + `bal_store: BalStoreHandle` 字段 + 构造函数内 `bal_store = storage.bal_store().clone()`。gravity 无消费方,storage-api 分组建议同步删 BAL。
  - **删除上游:** `RocksDBProviderFactory` impl 的 `unimplemented!` stub — gravity 已经在 #212/#225 把 RocksDB 包装在 `ProviderFactory` 内部,不需要这个间接层。
  - **采纳上游:** `subscribe_persisted_block` / `PersistedBlockSubscriptions` — 对 engine-tree 有用,与 gravity 无冲突。
  - **采纳上游:** `storage_changesets_range`、`account_changesets_range`、`get_storage_before_block`、`get_account_before_block`、`block_by_transaction_id` (委派 ConsistentProvider)。
  - **采纳上游:** `header(BlockHash)` 按值的签名迁移(与 storage-api trait 一致即可)。
  - **采纳上游:** `StaticFileProviderFactory::get_static_file_writer`。
  - **测试块:** 保留 gravity 的 `UnifiedStorageWriter::commit(provider_rw)?` 与 `ExecutedBlockWithTrieUpdates::new(.., ExecutedTrieUpdates::empty(), Default::default())` 三参构造;不要采纳上游 `save_blocks(.., SaveBlocksMode::Full)` (后者要求上游已删的 `UnifiedStorageWriter`,见 `writer/mod.rs` 决议)。
  - **删除** 文件头的 `#![allow(unused)]` — 这是 #205 合并产物。如有 unused warning 应在迁移过程中逐一修复。
**推理:**
  - `validator_node_only` 锚定在 `crates/gravity-primitives/src/config.rs` 字段,由 #224 (`1539b6cafc`) 消费。Baseline `0cb1687c1c:blockchain_provider.rs:515` 与 `:524` 两处命中。
  - `BalProvider` 在 baseline 上 grep 全 crate 零命中。
  - `BlockchainProvider` 作为 pipe-exec 入口尚未做 `with_types_and_provider::<EthereumNode, BlockchainProvider<_>>` 形状的适配 — 本次合并需要从头引入。
  - `header_td*` / `recover_block_number` / `transaction_block` 在 baseline `:194/:198/:259/:381` 全部 grep 命中。

---

### `crates/storage/provider/src/providers/consistent.rs`

**模块:** 快照视图 provider (memory + DB),是 `BlockchainProvider` 的后端
**冲突类型:** AA
**上游变更 (v1.8.3 → v2.3.0):** 约 24 个上游 commit 触碰本文件 — 主要是 `#23656` (encapsulate state fetching)、`#24494` (tx-hash identity)、`#22918`/`#22906`/`#22742` (移除 changeset readers 的克隆)、`#22876` (`ensure_canonical_block` 守卫下沉到本处)、`#23187` (tx-range 查询避免 receipt 克隆)、`#23310` (移除 `changeset count` API)、`#22379` (slot preimage / `PlainStorageRevert`)、`#23009` (移除 `seal_slow`)。
**Gravity 侧变更:** 无。`git log 0cb1687c1c -- crates/storage/provider/src/providers/consistent.rs` 只显示 4 个 merge-bump commit (#205/#144/#131/#108) 与 v1.2.0 #55 — 没有任何 gravity-only 逻辑编辑。Baseline 文件 grep `gravity|nested|grevm|validator_node_only|commit_view|pipe_exec` 零命中。
**影响范围:** 支撑 `BlockchainProvider::consistent_provider()?.X` 的委派;本组里每个 `BlockReader` / `TransactionsProvider` 调用都会触达。Trie/state-root 输入经由此处;`ConsistentDbView` 也基于 ConsistentProvider 做快照。
**解决方案建议:** take-upstream
  - 文件状态是 AA,但实际是 rename-blind 三方合并 (gravity 自 v1.2.0 即继承,merge resolver 未跟随)。
  - 用 stage 3 落地:`git checkout --theirs crates/storage/provider/src/providers/consistent.rs`。
  - 提交前用 `grep -E "(gravity|nested|grevm)" :2:consistent.rs` 再确认一遍 ours 侧无 gravity touch。
**推理:** 两侧都没有 gravity 标记;diff 是纯上游演进。直接采用 v2.3.0 版本是安全的。

---

### `crates/storage/provider/src/providers/consistent_view.rs`

**模块:** `ConsistentDbView` — 并行 trie / state-root 的快照输入 (grevm `NestedStateRoot` 依赖项)
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 3 个上游 commit — `96c77fd8b feat(storage): make insert_block() operate with references (#20504)`、`058ffdc21 feat(storage): write headers and transactions only to static files (#18681)`、`a047a055a chore: bump rust to edition 2024 (#18692)`。其中 #20504 改变了 `insert_block(&recovered_block)` 的形状 (按引用而非按值),v1.8.3 上仍带 `StorageLocation::StaticFiles` / `StorageLocation::Both` 第二参数;v2.3.0 由相关 PR 一起移除。`create_test_provider_factory_with_chain_spec(MAINNET.clone())` 在 v2.3.0 被简化为 `create_test_provider_factory()`。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 无生产代码 gravity-only 编辑。Baseline grep `gravity|nested|grevm|StorageLocation` 仅 `mod tests` 内的 `StorageLocation::{StaticFiles,Both}` 测试参数命中 — 这是 v1.8.3 baseline 的写法,在上游被 v2.3.0 移除,但**不是 gravity 引入的**。本文件 gravity 触史 (`d620fd0eeb`、`cb2992e451`、`6b71a11f88`、`95932ece83`、`a6e246ffbe`) 都没动这里的 `mod tests`。
**影响范围:** 生产侧 `pub struct ConsistentDbView` 与 `provider_ro()` 必须同时为 grevm 并行路径与上游 trie 路径工作 — 但本次冲突 hunk 全部在 `#[cfg(test)] mod tests` 内,生产代码无差异。
**解决方案建议:** mechanical-merge (条件化测试块)
  - **生产代码无操作。**
  - **测试块:** 取决于 storage-api-and-traits 对 `StorageLocation` 的决定 —
    - 若 api 分组**保留** `StorageLocation` 枚举 (推荐方向,gravity pipe-exec 依赖),则**采用 gravity** ours 侧测试体 (`provider_rw.insert_block(recovered_block, StorageLocation::*)` + `remove_blocks_above(0, StorageLocation::Both)`)。
    - 若 api 分组**删除** `StorageLocation`,则**采用上游** theirs 测试体,改为 `provider_rw.insert_block(&recovered_block)` 与无参的 remove。
  - **测试 helper:** `create_test_provider_factory_with_chain_spec(MAINNET.clone())` 与 `create_test_provider_factory()` 二选一应与 `test_utils/mod.rs` 最终的 export 一致。
**推理:** `ConsistentDbView` 是 gravity `NestedStateRoot` (`crates/trie/parallel/src/nested_hash.rs`) 与上游 trie 之间的网关。生产 API 未变;`mod tests` 的形状取决于跨分组对 `StorageLocation` 与 `insert_block` 签名的最终决议。

---

### `crates/storage/provider/src/providers/database/builder.rs`

**模块:** `ProviderFactoryBuilder` (`ProviderFactory::open_read_only` 的 typed-state builder)
**冲突类型:** AA
**上游变更 (v1.8.3 → v2.3.0):** 8 个上游 commit:`#23357` "catch-up for read-only ProviderFactorys" 给 `open_read_only` 加 `runtime: reth_tasks::Runtime` 参数、返回类型从 `Arc<DatabaseEnv>` 解包为 `DatabaseEnv`、要求 `NodeTypesForProvider`;`#23109` 引入 RocksDB secondary-instance 路径(`rocksdb_dir`、`RocksDBProvider::builder().with_read_only(true)`);把 `DatabaseArguments` 的导入路径从 `reth_db::{DatabaseArguments, ...}` 改为 `reth_db::mdbx::{DatabaseArguments, MaxReadTransactionDuration}`;`#22049` 移除 `TypesAnd1-5` staging types;`#21641` 给 `DatabaseEnv` derive `Clone` (因此可去掉 `Arc<>`);`#19384`/`#20253`/`#20416` 把 RocksDB / `with_default_tables` / `Metadata` 接入 builder。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 来自 `a1d7365bd6` #212 的 RocksDB 集成痕迹 —
  - `disable_long_read_transaction_safety` 体内 `// TODO: Implement max_read_transaction_duration for RocksDB` (函数体 noop, baseline `:160-162`);
  - `open_read_only` 返回类型仍为 `Arc<DatabaseEnv>` (上游 v2.3.0 已改为 `DatabaseEnv`)。
  - 基线 329 行 vs v2.3.0 251 行 — 差额来自上游 staging types 移除 (`#22049`) 与 gravity 保留更完整的 builder 表面。
**影响范围:** `ProviderFactoryBuilder` 是公开的只读入口 (`reth db get`、hive-test、debug 工具)。`runtime` 参数的引入会级联到每个 `open_read_only(MAINNET.clone(), "datadir")` 调用方 — 现在必须传 `reth_tasks::Runtime::current()` 或 `Runtime::test()`。`ReadOnlyConfig` 新增 `rocksdb_dir`、`watch` 字段会强制所有手动构造的调用方更新。
**影响范围 (cont.):** `disable_long_read_transaction_safety` 在 v2.3.0 上有实义 (`self.db_args.max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded))`),gravity baseline 仅是 TODO noop — 这是 #212 留下的 RocksDB 兼容性放空,合并时要决定是否补回。
**解决方案建议:** mechanical-merge (主体 take-upstream + 保留 gravity RocksDB 适配)
  - **采纳上游** struct 字段集合与 `open_read_only` 签名:`runtime: reth_tasks::Runtime`、返回 `ProviderFactory<NodeTypesWithDBAdapter<N, DatabaseEnv>>` (非 Arc)、`NodeTypesForProvider` bound、`mdbx::DatabaseArguments` 导入路径、`ReadOnlyConfig { rocksdb_dir, watch }`、`from_datadir` 三目录推导。
  - **采纳上游** RocksDB 二级实例打开路径 (`RocksDBProvider::builder(&rocksdb_dir).with_default_tables().with_read_only(true).build()?`) 以及 `factory.with_read_only_sync(watch)`。
  - **保留 gravity** `disable_long_read_transaction_safety` 的 TODO 注释:若 gravity 的 `RocksDBProvider` 不支持 `max_read_transaction_duration`,在采纳上游 `MaxReadTransactionDuration::Unbounded` 调用的同时把 noop 退化保留 (或同时为 RocksDB 显式 set 为 unbounded)。
  - **级联调用方:** `grep -rn "open_read_only\b" --include='*.rs'` 列出所有调用点,逐个加 `runtime` 参数。CLI 子命令需要把 `reth_tasks::Runtime::current()` 或 `Runtime::test()` 透传。
**推理:** 两侧 AA 都没有链上语义的 gravity 修改 — gravity 侧的 #212 改动只是把 RocksDB 集成挂上,新签名的 #23357 / #23109 与 gravity #212 在结构上一致 (`RocksDBProvider` 是 `ProviderFactory` 字段)。`Arc<DatabaseEnv>→DatabaseEnv` 的解包对调用方影响大,gravity 上的 `gravity_pipe_test.rs` 尚未做这层适配,需要在 pipe-exec 分组里处理。

---

### `crates/storage/provider/src/providers/database/mod.rs`

**模块:** `ProviderFactory` (DB + static-file + (RocksDB) + runtime 聚合器)
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 大幅重构 —
  - **新字段** (v2.3.0):`storage_settings: Arc<RwLock<StorageSettings>>`、`rocksdb_provider: RocksDBProvider` (`#20253`、`#22954`)、`changeset_cache: ChangesetCache` (与 `#21115`/`#23667` 一起)、`bal_store: BalStoreHandle` (`#23710`)、`runtime: reth_tasks::Runtime` (`#23357`、`#21934`)、`minimum_pruning_distance: u64` (`#23082`)、`read_only_sync: Option<Arc<ReadOnlySyncState>>` (`#23357`)。
  - **新方法:** `new_checked`、`assert_consistent`、`check_consistency` (三步:文件级 → rocksdb → static-file checkpoint → `heal_chain_state_block_numbers`)、`unwind_provider_rw` (`#21311` CommitOrder)、`sync_providers_if_needed`、`caught_up_static_file_provider`、`MetadataProvider` impl、`with_minimum_pruning_distance`、`with_read_only_sync`。
  - **移除:** `with_static_files_metrics`、裸 5-参 `new`(被 6-参替换:加入 `rocksdb_provider`、`runtime`)。
  - **trait 签名变化:** `HeaderProvider::header(BlockHash)` 按值;移除 `header_td*`、`recover_block_number` (`BlockNumReader` 上);新增 `block_by_transaction_id`。
  - **`fn new` 内部:** 上游会从 DB 读取持久化的 `StorageSettings`,fallback 到 `StorageSettings::v1()`,然后用读到的设置构造最终 factory。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):**
  - `BlockNumReader for ProviderFactory<N>` impl 内的 `fn recover_block_number(&self) -> ProviderResult<BlockNumber>` (baseline `:343`),委派给 `self.provider()?.recover_block_number()` — 来自 #224。
  - 测试 (`mod tests`,baseline `:701`/`:725`/`:751`) 使用 `provider.commit_view().unwrap()` (来自 #313)。
  - 构造函数仍是 5 参 (`db, chain_spec, static_file_provider, prune_modes, storage`),无 `rocksdb_provider`/`runtime` 字段。
  - 没有 `BalProvider`/`bal_store`/`changeset_cache`/`storage_settings`/`read_only_sync` 字段 — 上游全是新增。
**影响范围:** 这是 crate 根 struct。字段布局变化会被每个 `Clone` / `fmt::Debug` impl 消费。6 参构造函数会级联到:
  - `crates/cli/commands/src/common.rs` (gravity #313 改动过该路径)
  - `crates/storage/db-common/src/init.rs`
  - `crates/storage/provider/src/test_utils/mod.rs` (本组已涵盖)
  - 所有 `ProviderFactory::new(db, chainspec, sf)` 调用点 → 现在必须传 `rocksdb_provider`、`runtime`。
**解决方案建议:** mechanical-merge (工作量大;倾向采纳上游 struct + 保留 gravity 公开 API)
  - **采纳上游** struct 布局:`storage_settings`、`rocksdb_provider`、`changeset_cache`、`runtime`、`minimum_pruning_distance`、`read_only_sync`。Gravity 已有的 RocksDB sharding (#225 `c64bd613e4`) 在 `crates/engine/tree/src/persistence.rs` — 需要把那一层 sharding 重接到上游 `RocksDBProvider` 之上 (它支持 `clone()`)。
  - **删除上游** `bal_store: BalStoreHandle` 字段 + `BalProvider` impl + `InMemoryBalStore` 导入。Gravity 删 BAL。
  - **采纳上游** `new`、`new_checked`、`assert_consistent`、`check_consistency`、`heal_chain_state_block_numbers`、`unwind_provider_rw`、`caught_up_static_file_provider`、`MetadataProvider` impl、`ReadOnlySyncState` — 这些与下方 `provider.rs` 调用方编译耦合,且和 #253/#255 的恢复语义互补(不冲突)。
  - **保留 gravity** `BlockNumReader::recover_block_number` impl (baseline `:343`) — 这是 gravity 公开 API,被 CLI 恢复流程消费。在 trait impl 上作为额外方法保留;若 storage-api 分组保留 `recover_block_number` 入 trait,则一致;若删,则保留为 inherent method。
  - **保留 gravity** `header_td*` 仅当 `blockchain_provider.rs` 决议同样保留;否则与上游对齐删除(推荐:删,因 v1.8.3 上游已开始删,本 baseline 仍残留只是因为 #205 没清干净)。
  - **测试** (`mod tests`):保留 `commit_view().unwrap()`;`StorageLocation` 参数与 `provider.insert_block(.., StorageLocation::Database)` 的命运随 storage-api 分组决议。
  - **审计:** `use notify::{RecommendedWatcher, RecursiveMode, Watcher};` 仅当上游 `read_only_sync` 真用到 file watcher 时保留;否则与上游一并去掉。
**推理:** 上游的 `RocksDBProvider` + `ChangesetCache` 是 v2.3.0 storage-v2 骨架,不能简单跳过(下游 `provider.rs` 调用了 `factory.rocksdb_provider()`、`factory.changeset_cache()`,见 `database/mod.rs:127`)。Gravity #212 的 RocksDB 集成与上游 #20253/#22954 在结构上是平行而非冲突 — 二者都把 RocksDB 当作 ProviderFactory 字段。`recover_block_number` 锚定 #224,baseline `:343` 命中,移除会破坏 CLI 恢复路径 (`crates/cli/commands/src/stage/`)。

---

### `crates/storage/provider/src/providers/database/provider.rs`

**模块:** `DatabaseProvider<TX, N>` — 重型读 + 写 provider (~5.7k 行冲突 diff)
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 40+ 上游 commit。关键:
  - `#23656` `encapsulate state fetching` — 把 overlay 构建移到 DB provider;引入 `EitherReader/EitherWriter/EitherWriterDestination`、`OverlayBuilder`。
  - `#23657` / `#23667` — `OverlayBuilder` + 历史状态路径使用 overlay。
  - `#23310` 移除 `changeset count` API。
  - `#23335` "cap storage_v2 unwind history by MDBX tip" — unwind 路径大改:用 `db_tip_block` + `changeset_cache.get_or_compute_range`。
  - `#23082` `MINIMUM_UNWIND_SAFE_DISTANCE` + `with_minimum_pruning_distance`。
  - `#23083` `disable_long_read_transaction_safety` (避免 heal 中段被 kill)。
  - `#23386` 安全路径使用 `sort_unstable`。
  - `#24318` `keep small save block tx numbers inline` 引入 `SaveBlocksMode`。
  - `#24760` `reject expired recovered blocks`。
  - `#21311` `CommitOrder for RocksDB/MDBX unwind atomicity`。
  - 从公开 API 移除 `commit_view()` (被上游的 commit-order 纪律替换)。
  - `#22158` 打包 `StoredNibblesSubKey` 65→33 字节,泛型 cursor factory。
  - `#22379` slot preimage / `HashedAccount` keying。
  - `#22954` 默认 storage-v2。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):**
  - `commit_view()` 公开方法 (baseline `:791-793`,`pub fn commit_view(&self) -> ProviderResult<bool>`),委派 `self.tx.commit_view()`。来自 #313 (`ff103f976a`)。
  - `unwind_trie_state_range` 内 (baseline `:291-292`) 注释 `// Flush WriteBatch so NestedStateRoot can read the updated hashed state.` + `self.commit_view()?;` + (baseline `:294-296`) `let nested_state_root = NestedStateRoot::new(&self.tx, None);` — gravity 独有的 nested state-root unwind 路径,来自 #313 / #249。
  - 导入 (baseline `:64`、`:68`):`use crate::providers::nested_trie::StorageNodeEntry;` 与 `use reth_trie_db::{nested_hash::NestedStateRoot, DatabaseStorageTrieCursor};`。
  - `BlockNumReader::recover_block_number` impl (baseline `:1161`),返回 `StageId::Execution` checkpoint(来自 #224)。
  - `take_state_above` / `take_block_and_execution_above` / `remove_block_and_execution_above` / `remove_blocks_above` / `insert_block` 上的 `StorageLocation::Both` / `Database` 参数 — baseline 全文超过 30 处命中,与 storage-api 的 `StorageLocation` 枚举耦合。
  - 测试 (baseline `:2740` 起到文件末尾) 在每次 insert/write 之后 `provider.commit_view().unwrap()` (≥20 处);测试体内多次 `StorageLocation::Database`。
**影响范围:** 这是栈最底层 — `ConsistentProvider` 与 `BlockchainProvider` 都把 `database_provider_ro()?` 委派到此处。带 `NestedStateRoot` 的 `unwind_trie_state_range` 路径把 grevm 并行执行与规范 state-root 计算捆在一起 — **处理不当 = unwind 时共识分裂**(critical-finding)。
**解决方案建议:** keep-gravity 主体 + 选择性采纳上游(本组最大单文件决议)
  - **保留 gravity** `commit_view()`、`recover_block_number`、`unwind_trie_state_range` 内的 `NestedStateRoot` 路径(含 flush 注释)、`StorageNodeEntry` 与 `DatabaseStorageTrieCursor` 导入。锚定 #313、#249、#212。
  - **采纳上游(条件):** `EitherReader`/`EitherWriter` 间接层仅在 storage-api 分组同时保留时采纳;否则删除。建议**删除**(gravity 单实现 RocksDB+MDBX,无 either 需求)。
  - **采纳上游(条件):** `BlockExecutionWriter::take_block_and_execution_above(block)` 无 `StorageLocation` 参数签名 — 仅当 storage-api 分组删 `StorageLocation` 时采用;否则**保留** gravity 的 `(block, StorageLocation)` 签名。**强烈建议保留** — pipe-exec 的 "跳过 static-file 写入" 优化依赖。
  - **保留 gravity** 的 unwind trie 路径(`NestedStateRoot::new(&self.tx, None)` + `write_trie_updatesv2`)— 上游的 `changeset_cache.get_or_compute_range` + `write_trie_updates_sorted` 路径若引入会**静默替换 state-root 算法**,同一条链可能产出不同的 state root,这对 #313 是承重的。
  - **采纳上游** 正交新能力:`ReaderTxnTracker`、`with_minimum_pruning_distance`、`disable_long_read_transaction_safety`、`new_unwind_rw`(可考虑在其实现内部调用 gravity 的 `commit_view()`,见开放问题 3)。
  - **采纳上游** `block_by_transaction_id`、`last_finalized_block_number`、`last_safe_block_number`、`save_finalized_block_number`、`save_safe_block_number` — `database/mod.rs::heal_chain_state_block_numbers` 需要。
  - **采纳上游** `SaveBlocksMode` / `CommitOrder` 的导出,如 `database/mod.rs` 也采纳。
  - **删除上游** 构造函数里 `BalProvider` / `bal_store` 字段引用。
  - **测试区域:** 保留 gravity 每次 insert/write 之后的 `commit_view().unwrap()` 与 `StorageLocation::Database` 参数;采纳上游 `PackedKeyAdapter`/`LegacyKeyAdapter`/`cached_storage_settings().is_v2()` 测试断言(储存 v2 关键路径)。
**推理:**
  - `commit_view()` 是 gravity #313 引入的 RocksDB WriteBatch flush-then-keep-txn-open 语义;注释 (`// Flush WriteBatch so NestedStateRoot can read the updated hashed state`) 显式说明了对 RocksDB writebatch-view 的依赖。
  - `NestedStateRoot` 算法是 gravity 的并行 state-root 契约,与上游 `OverlayBuilder`/`ChangesetCache` 不等价,**不能互换**。
  - `StorageLocation` 参数是 pipe-exec 契约:`insert_block(StorageLocation::Database)` 用于跳过内存块的 static-file 写入,这是 gravity #82 / #109 pipe 路径需要的优化。
  - `recover_block_number` 被 CLI 恢复流程消费,来自 #224。

---

### `crates/storage/provider/src/providers/state/historical.rs`

**模块:** `HistoricalStateProvider` — 按历史区块取状态的门面
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 12 个上游 commit。`#23657`/`#23667` `OverlayBuilder` 历史状态路径重构;`#22289` stateless witness 按 spec 实现;`#22922` `Use proof v2 in TrieWitness`;`#23067` 只读使用 `RocksReadSnapshot`;`#22564` 移除 `DatabaseTrieWitness` trait,新增 `MaskedTrieCursorFactory`;`#22379` slot preimage / `HashedAccount` keying;`#22158` 打包 `StoredNibblesSubKey`;`EitherReader` 间接层;`StorageSettingsCache` 与 storage-settings 条件化的 bytecode / storage 读取。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 无。Baseline grep `gravity|nested|grevm|validator_node_only|cache|StateRootCache` 全部零命中。本文件 gravity 触史 (`9974ad0618 #241`、`a1d7365bd6 #212`、`d620fd0eeb #205` 等) 都是 merge-bump 或 lint fix,**没有 gravity 业务逻辑**。
**影响范围:** `HistoricalStateProvider` 支撑历史状态的 `eth_getBalance(block)`、`eth_getStorageAt(block)`。Gravity 的 trie 读取 (`NestedStateRoot`) 走的是 `database/provider.rs` 的 unwind 路径,而非本文件 — 这里仍委派给基础的 `tx().get(HashedAccounts)`。
**解决方案建议:** take-upstream
  - 采用 v2.3.0 文件。
  - **级联依赖:** 需要 storage-api / chain-state 分组里的 `EitherReader`、`StorageSettingsCache`、`OverlayBuilder`、`OverlaySource`。这些分组解决后再核对编译。
  - **核实** `delegate_provider_impls!` 宏在 gravity scope 内 (`providers/state/macros.rs`) 是否有 gravity 改写;若有,保留宏路径,只替换方法体。
**推理:** 无 gravity 标记;纯上游演进。

---

### `crates/storage/provider/src/providers/state/latest.rs`

**模块:** `LatestStateProvider` — tip 状态门面
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** 与 `historical.rs` 同族 — `#22922` proof v2、`#22564` `MaskedTrieCursorFactory`、`#22158` 打包 nibble keys、`#22289` stateless witness、`#22379` slot preimage、`#21115` hashed-state-as-canonical。外加 `StorageSettingsCache` 门控的 `hashed_storage_lookup` (v2 vs v1 storage key 形状)。引入 `DbStateRoot`/`DbStorageRoot`/`DbStorageProof`/`DbProof` 类型别名;新增 `ExecutionWitnessMode`、`TrieInputSorted`。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 无。`grep nested|grevm|validator_node_only|StateRootCache|cache` 在 baseline 上零命中。本文件 gravity 触史中 `2dde8ca181 #109 state_root: implement cache and state write` 只动了共享导入,未在本文件留下持久逻辑。
**影响范围:** `LatestStateProvider` 被 `BlockchainProvider::latest()` 的非 validator 分支使用。trait 接口面对齐即可。
**解决方案建议:** take-upstream
  - 原样采用 v2.3.0 文件。
  - 与 `historical.rs` 一样,`delegate_provider_impls!` 引用必须存活。
  - **核实** `cached_storage_settings()` 的默认值 — gravity 的 RocksDB 集成可能需要 `StorageSettings::v2()` 为默认;但这由 storage-api 分组决定。
**推理:** 无 gravity 标记;纯上游演进。

---

### `crates/storage/provider/src/providers/static_file/manager.rs`

**模块:** `StaticFileProvider` (jar manager — header / tx / receipt segments)
**冲突类型:** UU (~2.5k 行)
**上游变更 (v1.8.3 → v2.3.0):** 30+ 上游 commit。`#23310` 移除 changeset count API;`#22379` slot preimage,新增 `StorageBeforeTx`、`StorageChangesetMask`、`TransactionSenderMask`;`#22497` 让 span context 跨 rayon 边界传播;`#24126` storage-v2 init-state 导入;`#24494` `tx_hash` 用于交易身份;`#23357` `caught_up_static_file_provider` 管线接通;新 `changeset_walker` 模块;新 `EitherWriter`、`EitherWriterDestination` 类型;`PruneSegment` 导入;`StaticFileMap` 重命名;`check_consistency` 大重写 — 加入 `segments_to_check(provider)` 过滤、`or_else` 风格累加器 `update_unwind_target` 闭包、`PruneCheckpointReader + StorageSettingsCache` trait bound、`instrument` 属性、`info_span!` 跟踪。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):**
  - `use gravity_primitives::get_gravity_config;` (baseline `:19`)。
  - `check_consistency<Provider>(.., has_receipt_pruning: bool)` (baseline `:739-742`) — gravity **新增了 `has_receipt_pruning` 参数** (#255 引入,full-node receipt-prune 处理)。
  - `check_consistency` 顶部 (baseline `:748-749`):`if !get_gravity_config().disable_pipe_execution { return self.check_consistency_pipe_execution(provider, has_receipt_pruning); }` — 来自 #253、#255。
  - `check_consistency_pipe_execution<Provider>(.., has_receipt_pruning)` 方法 (baseline `:1023`),含 segment 级 receipt pruning 分支 (baseline `:1039`、`:1052`)。
**影响范围:** `check_consistency` 在每次启动时运行(以及上游引入的 `ProviderFactory::new_checked` 路径)。pipe-execution 分支是 gravity 的恢复契约(#253、#255):删掉会让 validator 节点重启时 static-file 恢复失败(critical-finding)。
**解决方案建议:** mechanical-merge (keep-gravity 恢复分支 + 采纳上游 fallback)
  - **保留 gravity** `use gravity_primitives::get_gravity_config;` 导入。
  - **保留 gravity** `check_consistency` 顶部分派 (`if !get_gravity_config().disable_pipe_execution { return self.check_consistency_pipe_execution(...); }`) 与 `has_receipt_pruning: bool` 参数。
  - **保留 gravity** `check_consistency_pipe_execution` 整段(baseline `:1023+`),含 segment 级 receipt-prune 处理。
  - **采纳上游** `check_consistency` 在 gravity 分支返回**之后**的其余部分:`segments_to_check(provider)` 过滤、`update_unwind_target` 闭包、`PruneCheckpointReader + StorageSettingsCache` trait bound、`#[instrument(skip(provider, has_receipt_pruning))]` 属性(参数列表与 gravity 一致即可)。
  - **采纳上游** `StaticFileMap` 重命名、`find_fixed_range` 重导出、`PruneSegment` / `StorageChangesetMask` / `TransactionSenderMask` 导入、`changeset_walker` 模块重导出、`EitherWriter` / `EitherWriterDestination`(若 api 分组保留)、`StorageBeforeTx`、`tx_hash`-based identity。
  - **保留 gravity** `use dashmap::DashMap` 直接导入(若 Cargo.toml 决议保留直接 `dashmap` 依赖);否则切到 `reth_primitives_traits::dashmap::DashMap`。
  - 在调用 `check_consistency` 的所有上游路径需要把 `has_receipt_pruning` 透传 — 检查 `database/mod.rs::new_checked` 等是否需要补参数。
**推理:**
  - pipe-execution 分支把恢复绑定到 gravity 的 pipe-exec 持久化模型,锚定 #253 (`727c9e5ffc`)、#255 (`6e8da10b02`)、#340 (`acc458846c`)。
  - `has_receipt_pruning` 是 gravity full-node receipt-prune flag;移除会回退 #255。
  - 上游的 `segments_to_check(provider)` 与新 trait bound 是 fallback 路径能针对 v2 trait 接口面编译过去所必需。

---

### `crates/storage/provider/src/test_utils/mock.rs`

**模块:** `MockEthProvider` — RPC / engine 的测试脚手架
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** `#23596` 在 Mock 上新增 `BalStoreHandle` + `BalProvider` impl;`#22289` stateless witness 加上 `bal_store` 字段;`#23310` 移除 changeset-count API,新增 `storage_changesets_range`、`account_changesets_range`;`#22379` `StorageBeforeTx`;`#23923` chore nightly clippy;类型重命名 `AddressMap`、`B256Map`、`StorageEntry`、`PruneCheckpoint`、`PruneSegment` 导入;mock 上的 `StorageSettings`、`StorageSettingsCache` impl。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 无。grep `gravity|nested|pipe_test|validator_node_only|commit_view` 全部零命中。本文件 gravity 触史全是 merge-bump 与 ut 修复 (`#241`、`#205`、`#149`、`#144`、`#134`、`#131`、`#111`、`#109`、`#108`、`#94`、`#69`、`#55`),无 gravity 业务逻辑。
**影响范围:** 被 `crates/rpc/*`、`crates/engine/*` 跨 crate 单测使用。仅 mock,无生产影响。
**解决方案建议:** take-upstream (然后裁 BAL)
  - 采用 v2.3.0 文件。
  - **删除** `bal_store: BalStoreHandle`、`BalProvider` impl、`InMemoryBalStore` 相关导入 — 与 `BlockchainProvider`、`database/mod.rs` 保持一致。
  - **采纳上游** mock 上的 `StorageSettingsCache` impl — `LatestStateProvider`/`HistoricalStateProvider` 的测试要靠它编译。
**推理:** 无 gravity 标记;采纳上游 baseline;裁掉 BAL 以与本组 BAL 删除姿态一致。

---

### `crates/storage/provider/src/test_utils/mod.rs`

**模块:** `create_test_provider_factory*` helpers
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** `#23270` `create_test_provider_factory_with_chain_spec_and_db_args`;`#22158` `NodeTypesForProvider` trait bound 替代 `NodeTypes`;新 `RocksDBBuilder` + `reth_tasks::Runtime::test()` 接线;`tempdir_path()` 统一 datadir (单一临时目录,内含 db + static_files + rocksdb);`mdbx::DatabaseArguments` 导入路径;`#21772` 测试临时目录清理。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):** 无 gravity-only 编辑。`grep gravity|tempdir|rocksdb|RocksDB|Runtime` 在 baseline 上零命中。本文件 gravity 触史 (`9974ad0618 #241`、`d620fd0eeb #205`、`d3f302676b #108`) 都是 merge/ci fixup。
**影响范围:** 被 `crates/storage/provider/` 几乎每个测试使用(mock.rs、blockchain_provider.rs 测试、database/mod.rs 测试、writer/mod.rs 测试、consistent_view.rs 测试)。处理不当 = 本组测试编译全挂。
**解决方案建议:** take-upstream (附带 NodeTypesForProvider 与 Runtime 适配)
  - 采用 v2.3.0 (`RocksDBBuilder` + `Runtime::test()` + 统一 `tempdir_path()`)。
  - **核实** `RocksDBBuilder` 在 gravity #212 的 RocksDB 层上存在;若不存在,接通 gravity 现有的 test-rocksdb helper。
  - **核实** `reth_tasks::Runtime::test()` 在 gravity 的 `reth-tasks` crate 已定义;若没有,从上游 port 过来。
  - **NodeTypesForProvider** trait bound 必须由 node-types 分组先落地,跨组协调。
**推理:** 无 gravity 标记;纯上游演进。注意 `Arc<DatabaseEnv>→DatabaseEnv` 的解包(`#21641`)在 gravity 侧尚未消化,本次合并需要引入。

---

### `crates/storage/provider/src/writer/mod.rs`

**模块:** `UnifiedStorageWriter` (gravity) / 上游已删,只剩 `StateWriter` + 测试
**冲突类型:** UU
**上游变更 (v1.8.3 → v2.3.0):** **上游完全移除了 `UnifiedStorageWriter`。** 整个生产 struct 在 v2.3.0 不存在 — 文件从 1418 行(baseline)缩减到 1194 行(v2.3.0),只剩测试模块。状态写入直接走 `DatabaseProvider::save_blocks(SaveBlocksMode)` 与 `write_state(.., StateWriteConfig)`。删掉 `OriginalValuesKnown` 顶层导入,引入 `StateWriteConfig`。`#22158` `PackedKeyAdapter` / `LegacyKeyAdapter` 测试叠加。
**Gravity 侧变更 (baseline `0cb1687c1c` 上):**
  - `UnifiedStorageWriter<'a, ProviderDB, ProviderSF>` struct + impl(baseline `:21-200`)— **整段是 gravity 保留**,带:
    - `commit(provider)` (末尾 `tx.commit()`)
    - `commit_unwind(provider)` (unwind 时的特定 commit 顺序)
    - `append_blocks_with_state` — 迭代 `ExecutedBlockWithTrieUpdates { block, trie, triev2 }`,顺序调用 `database().insert_block(.., StorageLocation::Both)`、`database().tx_ref().commit_view()`、`write_state(.., StorageLocation::StaticFiles)`、`write_hashed_state`、`write_trie_updates`、`write_trie_updatesv2`(baseline `:170-200`)
    - `remove_blocks_above(block_number)` (`:210-213`),内部 `remove_block_and_execution_above(block_number, StorageLocation::Both)`
  - `cfg(not(feature = "pipe_test"))` 门控 `insert_block`(baseline `:172`)— pipe_test 模式跳过 insert,只写 state/trie(block insert 由 pipe-exec 层做)。
  - 测试体 (baseline `:371`、`:375`、`:436`、`:443`、`:528-530`、`:629-631`、`:710-712`、`:870-872`、`:1037-1039`、`:1087-1089`、`:1176`、`:1381`、`:1397`...) 在 write_state 后调 `provider.commit_view().unwrap()`,使用 `OriginalValuesKnown::Yes, StorageLocation::Database` 参数对。
**影响范围:** `UnifiedStorageWriter` 被下列消费:
  - `crates/engine/tree/src/persistence.rs` (#225 sharding 写入走它)
  - `crates/pipe-exec-layer-ext-v2/execute/`(`pipe_test` feature flag 是集成点)
  - `crates/storage/provider/src/providers/blockchain_provider.rs` 测试脚手架
  移除它需要把每个调用方重新接到 `provider.save_blocks(SaveBlocksMode)` — 工作量远超本组范围。
**解决方案建议:** keep-gravity (保留 `UnifiedStorageWriter`)
  - **保留 gravity** 整个 `UnifiedStorageWriter` struct + impl + `append_blocks_with_state` + `remove_blocks_above` + `commit` / `commit_unwind` (锚定 #313)。
  - **保留 gravity** `cfg(not(feature = "pipe_test"))` 门控 — 对 pipe-exec 测试是承重的。
  - **保留 gravity** 测试体里的 `commit_view().unwrap()` 调用与 `OriginalValuesKnown::Yes, StorageLocation::Database` 参数对(无论 storage-api 是否保留 `StateWriteConfig`,gravity 都需要 `StorageLocation::Database`)。
  - **采纳上游(条件)** 测试体内的 `StateWriteConfig` 引用 — 若 storage-api 分组采用 `OriginalValuesKnown::Yes + StateWriteConfig::default()` 替代 `(OriginalValuesKnown, StorageLocation)`,要 port 进 gravity wrapper。**建议保留** gravity `(OriginalValuesKnown, StorageLocation)` 对 — pipe-exec 需要。
  - **采纳上游** `PackedKeyAdapter` / `LegacyKeyAdapter` / `cached_storage_settings().is_v2()` 测试分支 — 这些门控 storage-v2 vs v1,与 writer 形状正交。
  - **采纳上游** `write_trie_updates_sorted` / `into_sorted()` / `StorageSettingsCache` 的测试 import。
  - **审计:** `OriginalValuesKnown` 导入路径 — gravity 用 `revm_database::OriginalValuesKnown`,上游可能移位,核实。
**推理:**
  - `UnifiedStorageWriter` 锚定 `f2e3993706 state_root: fix genesis block and pipe test`,且 #225 (`c64bd613e4`) 的 `persistence.rs` 调用 `UnifiedStorageWriter::from(&provider_rw, &static_file)` 做 sharded 写入。
  - `pipe_test` feature flag 在 `Cargo.toml` 显式命名(gravity 加的),baseline 上唯一消费方就是本文件的 `cfg(not(feature = "pipe_test"))` block。
  - `commit_view()` 语义(RocksDB write-batch-flush-then-keep-txn-open)是 `database/provider.rs:292` 的 `NestedStateRoot` 计算所必需 — 删掉 = 破坏 gravity nested-state-root 管线 = unwind 时共识分裂(critical-finding,由 provider.rs 决议承载)。

## 分组级解决方案 playbook

**本组内的解决顺序(在 storage-api-and-traits + trie-and-state-root + chain-state 落定之后):**

1. **Cargo.toml 优先** — 确立后续可用的 feature / 依赖(保留 `gravity-primitives`、`reth-evm`、`pipe_test`、直接 `dashmap`;采纳 `alloy-eip7928`(若 BAL 保留)、`smallvec`、`reth-tasks`(rayon)、`alloy-genesis`、`reth-tokio-util`、`jemalloc` feature)。
2. **consistent.rs** (AA → take-upstream) — 纯 rename-blind;`git checkout --theirs`。
3. **builder.rs** (AA → mechanical-merge) — 采纳上游签名 + 保留 gravity 的 RocksDB TODO 兼容。
4. **mock.rs** + **test_utils/mod.rs** — 把测试脚手架先拉起来(take-upstream + 裁 BAL + 接通 gravity `RocksDBBuilder`/`Runtime::test()`)。
5. **database/mod.rs** + **database/provider.rs** — 两个最大的,需要协同推进:provider.rs 的 trait impl 必须匹配 mod.rs 的字段集。策略:采纳上游 struct 布局(`RocksDBProvider` + `ChangesetCache` + `runtime` + `read_only_sync`),删 BAL,保留 `commit_view` / `recover_block_number` / `NestedStateRoot` unwind 路径。
6. **static_file/manager.rs** — 保留 `disable_pipe_execution` 分支 + `has_receipt_pruning` 参数;分支之后的 fallback 走上游(`segments_to_check` + 新 trait bound + `instrument`)。
7. **state/historical.rs** + **state/latest.rs** — 原样采纳上游(无 gravity 标记)。
8. **blockchain_provider.rs** — 把上述一切接到公开门面;保留 `validator_node_only` 分支、`recover_block_number`/`transaction_block`/`header_td*`/`get_state` 公开 API;删 BAL/`RocksDBProviderFactory` stub;采纳上游 tx-identity / changeset / `subscribe_persisted_block`。
9. **consistent_view.rs** — 生产无操作;`mod tests` 取决于 storage-api 对 `StorageLocation` 的最终决定。
10. **writer/mod.rs** — 保留 gravity `UnifiedStorageWriter` 与 `pipe_test` cfg;采纳上游测试侧 `PackedKeyAdapter`/`LegacyKeyAdapter`/`StorageSettingsCache` 更新。

**需要保持的跨切面不变量:**

- `gravity_primitives::get_gravity_config().{validator_node_only, disable_pipe_execution}` 访问点存在(baseline `blockchain_provider.rs:19/:515/:524`、`static_file/manager.rs:19/:748`)。
- `NestedStateRoot`(来自 `reth_trie_db::nested_hash`)仍是 unwind state-root 计算算法(#313 / #249,baseline `database/provider.rs:296`)。
- `commit_view()` 至少对 RocksDB tx 仍是 `DatabaseProvider<TX, N>` 的公开方法(#313,baseline `:791`)。
- `recover_block_number()` 仍在 `BlockNumReader` impl 中(#224,baseline `database/mod.rs:343`、`database/provider.rs:1161`、`blockchain_provider.rs:259`)。
- `StorageLocation` 枚举被广泛消费(baseline 全文 30+ 处)— 命运在 storage-api-and-traits 决定;**强烈建议保留** — 门控 pipe-exec "跳过 static-file 写入" 优化。
- `UnifiedStorageWriter` 保留 gravity 侧(baseline `writer/mod.rs:21+`);`persistence.rs` (#225) 与 pipe-exec 依赖。
- BAL 栈(`BalProvider`、`BalStoreHandle`、`bal_store` 字段、`InMemoryBalStore`)**全面删除** — gravity 无消费方。
- RocksDB 集成:gravity #212 / #225 已经把 RocksDB 包装在持久化层内部;上游 v2.3.0 的 `RocksDBProvider` 作为 `ProviderFactory` 字段在结构上与之兼容 — 采纳字段布局,把 gravity sharding (`engine/tree/persistence.rs`) 重接到上游 `RocksDBProvider::clone()` 语义。
- `has_receipt_pruning: bool` 参数在 `check_consistency` 上保留(#255,baseline `static_file/manager.rs:742`)。

**分组决议后的快速编译期 sanity 检查:**

```bash
cargo check -p reth-provider
cargo check -p reth-provider --features test-utils
cargo check -p reth-provider --features pipe_test
cargo nextest run -p reth-provider --no-run
```

后两条需要 gravity 侧 feature 在 Cargo.toml 决议中存活。

## 开放问题

1. **上游 `RocksDBProvider` 是否提供与 gravity #212 RocksDB 集成相同的 write-batch-flush 语义?** gravity `commit_view()` 依赖 RocksDB 的 `commit_view` 方法 — 核实它在上游 `RocksDBProvider` 上存在,或者把它通过 gravity 包装层导通。若否,`database/provider.rs:292` `unwind_trie_state_range` 中 `NestedStateRoot` 的 flush 注释就被破坏。

2. **要不要采纳上游 `ChangesetCache`?** 上游 unwind 路径用 `changeset_cache.get_or_compute_range`,gravity 走 `NestedStateRoot::read_hashed_state(Some(range))`。两者可共存(上游 cache 作为 gravity 算法之下的一层),但需要 `NestedStateRoot::calculate` 从已见 cache 写入的事务中读取。建议合并后用 #313 验收测试快速验证。

3. **`commit_view` vs 上游 `unwind_provider_rw` 提交顺序纪律:** 上游引入 `unwind_provider_rw()` 构造器,带 `with_reader_txn_tracker(self.db.clone())` 与更严的 commit 顺序(MDBX → RocksDB → static files,见 `#21311`)。Gravity `commit_view()` 是另一做法。建议在 `new_unwind_rw` 内部**包装调用** gravity `commit_view()`,对崩溃后重启恢复(#253/#255 保护的场景)更安全。

4. **`StorageLocation` 枚举命运:** 若 storage-api 分组删除,`database/provider.rs` 约 30 处调用点与 `blockchain_provider.rs` 测试约 6 处需压平。超出本组范围,但影响以上每个文件方案。建议 storage-api worker **保留** — 为 gravity pipe-exec 契约。

5. **`Cargo.toml` 的 `pipe_test` feature:** 若下游消费方(pipe-exec 层测试)都用 `--features pipe_test` 跑,那 `writer/mod.rs:172` 中被门控的 `cfg(not(feature = "pipe_test"))` block 就是 "真正的" 生产路径。用 `grep -rn "features = \[.*pipe_test" Cargo.toml .github/` 核实。

6. **`gravity-primitives` 在 storage/provider 中的边界:** 上游不会接受这个依赖,但对 gravity fork 无所谓。在上游拉过来的代码里新增 `get_gravity_config()` 访问点之前(例如:是否也要在 `historical.rs` 检查 `validator_node_only`,还是只在 `blockchain_provider.rs`?),先和项目负责人确认。当前 baseline 仅 `blockchain_provider.rs` 与 `static_file/manager.rs` 命中。

7. **`reth-db` 的 `mdbx` feature:** gravity #212 从 `reth-provider` 的 `reth-db` 依赖里去掉了 `mdbx` feature(因 RocksDB 是另一 crate);上游 v2.3.0 把它加回来。核实:加回 `mdbx` feature 是否会重新引入 gravity 在 #212 中故意规避的 MDBX 直接依赖,或者只是开了 trait/类型导出。`cargo tree -p reth-provider --features=` 验证。
