# cli-and-commands

## 分组概要

- **文件数：** 14（含一个需要人工决策的 `download` 模块二义）
- **复杂度：** **高**。`common.rs` 是子命令公共入口 (定义 `EnvironmentArgs::init` 签名 + `CliNodeTypes` / `CliNodeComponents`)，gravity 在此处叠加了 `init_genesis` skip-when-checkpoint-set guard 与 `StorageRecoveryHelper.check_and_recover()` 两段 pipe-execution 关键逻辑；上游同时为 storage v2 重写了 `EnvironmentArgs::init` 签名 (新增 `runtime` 参数、引入 `RocksDBProvider`、`StorageSettings`、`BalStoreHandle`、`StaticFileProviderBuilder`、扩展 `AccessRights::RoInconsistent`)。`stage/drop.rs` 与 `stage/run.rs` 也牵涉到 gravity 的 `UnifiedStorageWriter` commit 路径。
- **涉及模块功能：**
  - `EnvironmentArgs` / `Environment` / `AccessRights`（所有子命令的 CLI 初始化）
  - `reth node` 入口（`NodeCommand`，调用 `Launcher` trait）
  - `reth db list` / `reth db stats`（表内检查 / 占位 RocksDB stats）
  - `reth stage drop`（按 stage 截断 + reset checkpoint）
  - `reth stage dump-stage {execution, account-hashing, storage-hashing, merkle}`（离线 stage 调试）
  - `reth stage run`（单 stage 执行器）
  - `reth test-vectors compact`（codec 测试向量生成器）
  - sigsegv 处理器安装（进程级）
  - `crates/cli/commands/src/download{,.rs,/mod.rs}`（snapshot / manifest download，见下方"关键决策"）
- **gravity 关键 commit：**
  - `dfa14dcdea` — `refactor: Add gravity configuration arguments (#168)`：`node.rs` 引入 `GravityArgs` 字段 + `init_gravity_config(gravity.to_config())` 启动接线
  - `a1d7365bd6` — `feat(rocksdb): Integrating RocksDB into Reth (#212)`：`Cargo.toml`、`common.rs`、`db/list.rs`、`db/stats.rs`、`node.rs`、`stage/drop.rs`、`stage/dump/mod.rs` 同步演进；`db/stats.rs` 退化为占位 stats（RocksDB 替代 mdbx 后 `open_db`/`db_stat`/`freelist` 尚未实现）
  - `1224ae1846` — `refactor(parallel): remove the read provider factory (#213)`：`stage/dump/{execution, hashing_account, hashing_storage, merkle}.rs`、`stage/run.rs` 去掉并行读 provider factory
  - `ff103f976a` — `fix(unwind): commit view and set prune distance for execution unwind (#313)`：`common.rs` 引入 `StorageRecoveryHelper::new(&factory).check_and_recover()`、`init_genesis` 改为 stage-checkpoint guarded
  - `9974ad0618` — `fix(test): fix CI test of unit.yml (#241)`：`sigsegv_handler.rs` 保持裸 cast `*const () as libc::sighandler_t` 写法

- **关键决策（人工介入）：`crates/cli/commands/src/download` 模块二义**
  - 现状：worktree 同时存在 `crates/cli/commands/src/download.rs`（gravity 侧的 snapshot-download 单文件实现）与 `crates/cli/commands/src/download/mod.rs`（v2.3.0 upstream 新增的 `download/` 子目录含子模块）。`lib.rs` 中 `pub mod download;` 指向二义，rustfmt hook 会挂住整个格式化。
  - 三种可选路径：
    1. 保留 gravity snapshot-download（删除 `download/` 子目录，`pub mod download;` 指向 `download.rs`）
    2. 采纳 upstream 目录形式（把 `download.rs` 删除或合并进 `download/mod.rs`，snapshot-download 语义转到 `download/` 下的子模块）
    3. 合并：把 gravity snapshot-download 内容作为 `download/` 子目录下的一个子模块并入
  - 三者对 `reth db download-snapshot` 类命令的 CLI 面各有取舍，需要由 CLI owner 拍板；本分组编译不动其他文件亦须先此决策
- **解决顺序依赖：**
  1. `Cargo.toml` — 决定上游新引入的 `reth-tasks` / `reth-storage-api` / `parking_lot` / `metrics` / `url` / `blake3` / `rayon` 等是否可用，直接影响所有 `.rs` 的编译。
  2. `common.rs` — `EnvironmentArgs::init` 签名（是否新增 `runtime: reth_tasks::Runtime` 参数）必须最先确定，本组其他 10 个文件的 `init::<N>` 调用都依赖该签名。
  3. `stage/dump/mod.rs` — 宏 `handle_stage!` 是否需要串接 `runtime` 由步骤 2 决定，同时驱动每个 `stage/dump/*.rs` 调用 `ProviderFactory::new(..., rocksdb_provider, runtime)` 的形态。
  4. `node.rs` — 依赖 `crate::launcher::Launcher`（gravity 在 baseline 已自定义的 `crates/cli/commands/src/launcher.rs`，**本组不涉及**），只需处理 `GravityArgs`/`StaticFilesArgs`/`StorageArgs` 字段共存与 `init_gravity_config` 调用位置。
  5. `db/list.rs`、`db/stats.rs`、`stage/drop.rs`、`stage/run.rs`、`stage/dump/{execution,hashing_account,hashing_storage,merkle}.rs`、`test_vectors/compact.rs`、`sigsegv_handler.rs` — 在上述基础上独立解决。

## 逐文件分析

### `crates/cli/commands/Cargo.toml`
**模块：** `reth-cli-commands` 的 crate manifest
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `reth-db = { workspace = true, features = ["mdbx"] }`（显式回挂 mdbx feature，配合 `b9969c5b1c` storage v2 默认 + edge feature 移除）
- `reth-downloaders` 改为显式 `features = ["file-client"]`（`5dc285771`）
- `reth-prune-types`、`reth-stages-types` 从 optional 改为 non-optional；`reth-trie-common` 从 non-optional 改为 optional（PR #20121 / 后续 storage v2 改造）
- 新增 `reth-tasks`、`reth-storage-api`（PR #21934 global runtime、`0e4f143172`）
- 新增 `parking_lot`、`url`、`metrics`、`blake3`、`rayon`、`reqwest features = ["blocking"]`（`56bbb3ce2c` `reth db prune-checkpoints`、`80bf5532a` perf trie、`fb6248714` alloy 1.8.1、`26f4aab2a` modular snapshot downloads）
- `[dev-dependencies]` 新增 `reth-provider features = ["test-utils"]` + `tempfile`
- `arbitrary` feature 增加 `dep:reth-trie-common`、`reth-trie-common?/test-utils`、`reth-trie-common?/arbitrary`
- 移除 `default = []` 段（保留 `[features]`，但去掉空 default 行）

**Gravity 侧变更（baseline `0cb1687c1c`，相对 reth v1.8.3）：**
- `reth-db = { workspace = true }`（不显式带 mdbx feature，由 workspace 默认决定 — `a1d7365bd6` RocksDB 集成时调整）
- `reth-prune-types`、`reth-stages-types` 仍是 optional（v1.8.3 catch-up `d620fd0eeb` 带入的形态）
- `reth-trie-common` 仍是 non-optional
- 额外 gravity-only 依赖：`reth-engine-tree.workspace = true`、`gravity-primitives.workspace = true`（用于 `common.rs` 中 `StorageRecoveryHelper` 与 `get_gravity_config()`）
- `[features]` 段保留 `default = []`
- `arbitrary` feature 中所有 `reth-trie-common` 引用都不带 `?`（因为是 non-optional）

**影响范围：** 本组其余 13 个文件都基于此处声明的依赖编译。是否引入 `reth-tasks`、`reth-storage-api` 决定 `common.rs` 是否能接受上游 `EnvironmentArgs::init(..., runtime)` 签名；是否引入 `parking_lot` / `metrics` 决定 `stage/run.rs` 是否能 import `reth_node_builder::common::metrics_hooks`。

**解决方案建议：** **mechanical-merge**，偏向上游。
**理由：**
1. 上游把 `reth-prune-types`/`reth-stages-types` 改为 non-optional 是为了配合 storage v2 内部 schema 迁移，gravity 应 follow（baseline 上 optional 形态来自 v1.8.3 catch-up，没有 gravity-specific 语义需求 — `[features].arbitrary` 段当年是为避免循环依赖，但 v2.3.0 上游已用 `?` optional-dep 语法重新表达此约束）。
2. 上游 `reth-trie-common` 改 optional + `arbitrary` 段加 `dep:reth-trie-common` 也接收，保持与上游 feature gate 一致。
3. 接受上游所有新增 dep（`reth-tasks`、`reth-storage-api`、`parking_lot`、`url`、`metrics`、`blake3`、`rayon`、`reqwest blocking`、`reth-downloaders file-client`），因为 `common.rs`、`stage/run.rs` 解决方案需要。
4. **保留 gravity-only**：`reth-engine-tree.workspace = true`、`gravity-primitives.workspace = true`（`common.rs` 中 `use reth_engine_tree::recovery::StorageRecoveryHelper` 与 `use gravity_primitives::get_gravity_config`，引自 `ff103f976a`、`a1d7365bd6`）。
5. `reth-db = { workspace = true, features = ["mdbx"] }` — 即便 gravity 加了 RocksDB，mdbx 仍是 static-file 与 transactional 部分的底座（baseline `db/stats.rs` 仍以 `view(|_tx| ...)` 形式持有 mdbx tx），接受上游显式 `mdbx` feature；同时 RocksDB 由 workspace 通过其他 crate 引入。
6. 移除 `default = []` 段（与上游一致）。
7. 接受 `[dev-dependencies]` 新增的 `reth-provider features = ["test-utils"]` + `tempfile`（若编译失败再 trim）。

引用：gravity `a1d7365bd6`、`ff103f976a`；upstream `b9969c5b1c`、`5ea37acbdb`、`fb6248714`、`56bbb3ce2c`、`80bf5532a`、`26f4aab2a`、`0e4f143172`、`68e4ff1f7`。

---

### `crates/cli/commands/src/common.rs`
**模块：** 所有子命令公用的 CLI Environment helper
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** 大规模 storage v2 重构：
- `EnvironmentArgs` 新增 `pub static_files: StaticFilesArgs`、`pub storage: StorageArgs` 字段（`7f4a9a05e`、`d8acc1e4c`）
- 新增 `EnvironmentArgs::storage_settings()`（`5ea37acbd` storage v2 default）
- `EnvironmentArgs::init` 签名变为 `init<N: CliNodeTypes>(&self, access: AccessRights, runtime: reth_tasks::Runtime)`（`0e4f143172`、`68e4ff1f7` global runtime）
- `init_db` 返回 owned `DatabaseEnv` 而非 `Arc<DatabaseEnv>`；`StaticFileProvider::read_write/read_only` 替换为 `StaticFileProviderBuilder` 流式构造，附 `.with_metrics().with_genesis_block_number(...).build()?`（`d7e740f96`、`4adb1fa5a`、`e8128a3c85`）
- 调用 `RocksDBProvider::builder(data_dir.rocksdb()).with_default_tables().with_database_log_level(...).with_block_cache_size(...).with_read_only(...)`，read-only 路径若 rocksdb 目录缺失会自动 `create_dir_all` + 建空 store（`1ff88e43c`、`49057b1c0`、`fa6b44b03`、`3f7ae3e34`）
- `create_provider_factory` 新增 `RocksDBProvider` + `BalStoreHandle::new(InMemoryBalStore::new(...))` + `with_minimum_pruning_distance(...)`（`c91845ae4`、`3f7ae3e34`、`fa6b44b03`）
- `AccessRights` 新增 `RoInconsistent` 变体并新增 `is_read_only_inconsistent()` helper
- `init_genesis` 改名为 `init_genesis_with_settings(&factory, self.storage_settings())`
- 移除 `pub trait CliHeader`（迁移到 `reth_primitives_traits::header::HeaderMut`，`07c5956ce`），改为 `pub use reth_primitives_traits::header::HeaderMut`
- `FullTypesAdapter<T>` 与 `ProviderFactory` 内的 `Arc<DatabaseEnv>` 改为裸 `DatabaseEnv`（`370a548f3` derive Clone for DatabaseEnv）
- `check_consistency` 签名从 `(provider, has_receipt_pruning)` 简化为 `(provider)`（has_receipt_pruning 由 storage settings 内部读出）
- `is_read_only` 从 `() -> bool` 改为 `() -> eyre::Result<bool>`
- `default_value = C::SUPPORTED_CHAINS[0]` 改为 `default_value = C::default_value()`

**Gravity 侧变更（baseline `0cb1687c1c`，相对 reth v1.8.3）：**
- `a1d7365bd6` 起 import `gravity_primitives::get_gravity_config`、`reth_engine_tree::recovery::StorageRecoveryHelper`
- `ff103f976a` 在 `create_provider_factory` 中：
  - 在 RW 模式 `init_genesis` 调用前增加 `StageId::Execution checkpoint > 0` guard — "Skip init_genesis if the database already has an Execution checkpoint > 0, indicating it has been used. In pipe execution mode, genesis headers may not be in static files or CanonicalHeaders, causing init_genesis to incorrectly re-initialize and reset all stage checkpoints to 0."
  - 在 `create_provider_factory` 末尾新增：`if !get_gravity_config().disable_pipe_execution { StorageRecoveryHelper::new(&factory).check_and_recover()?; }` — 在管线执行模式下恢复被中断的 block writes
- `init_db` 返回的 `DatabaseEnv` 仍包成 `Arc<DatabaseEnv>`（v1.8.3 catch-up 形态）
- `StaticFileProvider::read_write(sf_path)?` / `StaticFileProvider::read_only(sf_path, false)?`（无 Builder 链）
- `FullTypesAdapter<T>` 仍带 `Arc<DatabaseEnv>`
- `EnvironmentArgs` 没有 `static_files` / `storage` 字段
- `init` 签名 `pub fn init<N: CliNodeTypes>(&self, access: AccessRights)`（无 `runtime` 参数）
- `default_value = C::SUPPORTED_CHAINS[0]`（v1.8.3 形态）
- 仍持有 local `pub trait CliHeader { fn set_number(&mut self, number: u64); }`

**影响范围：** `EnvironmentArgs::init` 签名变化级联到本组所有 9 个调用方文件（`node.rs` 不直接调 `init`，但通过 `Launcher::entrypoint` 间接相关）。`AccessRights` 增 `RoInconsistent` 影响 `db/list.rs`、`db/stats.rs` 等只读子命令。

**解决方案建议：** **mechanical-merge**，**必须保留 gravity 两段语义**。
**理由：**
1. **保留** `ff103f976a` 两段逻辑：
   - "Execution checkpoint > 0 → skip init_genesis" guard 是 pipe-execution + grevm 路径下避免 "重启后 stage checkpoint 被 init_genesis 重置回 0" 的关键防护，**不可删除**；要将其叠加到上游新的 `init_genesis_with_settings(&provider_factory, self.storage_settings())` 调用之前。
   - `StorageRecoveryHelper::new(&factory).check_and_recover()?` 必须保留（pipe execution 中断恢复路径，依赖 `reth-engine-tree` + `gravity-primitives`）。
2. **接受**上游：`runtime: reth_tasks::Runtime` 参数、`StaticFileProviderBuilder` 流式构造、`RocksDBProvider` 接入、`BalStoreHandle`、`AccessRights::RoInconsistent`、`init_genesis_with_settings`、`default_value = C::default_value()`、移除 `CliHeader` 并改 `pub use HeaderMut`、`DatabaseEnv` 去 `Arc`。
3. **接受**新字段 `static_files: StaticFilesArgs` 与 `storage: StorageArgs`（gravity 没有 conflict patch；storage v2 切换由 `--storage.v2` flag 控制，对 gravity 业务无副作用）。
4. **`Arc<DatabaseEnv>` → `DatabaseEnv` 去包**：必须连带改 baseline 中所有 `pub fn create_provider_factory(... db: Arc<DatabaseEnv> ...)` 与 `ProviderFactory::<NodeTypesWithDBAdapter<N, Arc<DatabaseEnv>>>` 签名 — 影响 `db/list.rs`、`db/stats.rs`、`stage/dump/*.rs` 中所有 `ProviderNodeTypes<DB = Arc<DatabaseEnv>>` bound。Upstream `370a548f3` 是合理的 derive 优化，接受。
5. **`RocksDBProvider` 接入与 gravity 自己的 RocksDB 集成 (`a1d7365bd6`) 关系**：gravity baseline `db/stats.rs` 走的是 "view(|_tx| ...) 占位" 路径（详见下面 `db/stats.rs` 节），那是因为 `a1d7365bd6` 时上游 mdbx tx 接口尚未给 RocksDB 提供 open_db；上游 v2.3.0 通过 `RocksDBProvider` + `BalStore` 提供了更完整的 storage v2 抽象，gravity 应接受，但 `db/stats.rs` 内的占位逻辑要根据上游 v2.3.0 `rocksdb_stats_table` 重新设计（不是简单 take-upstream）。
6. **CliHeader 移除**：上游已迁移到 `reth_primitives_traits::header::HeaderMut`（`07c5956ce`）。Gravity baseline 仍保留本地 trait — 接受上游 `pub use`，但需 grep `crate::common::CliHeader` 全仓库用法（如有）替换。

引用：gravity `ff103f976a`、`a1d7365bd6`；upstream `0e4f143172`、`68e4ff1f7`、`7f4a9a05e`、`5ea37acbd`、`d7e740f96`、`1ff88e43c`、`fa6b44b03`、`3f7ae3e34`、`c91845ae4`、`370a548f3`、`07c5956ce`、`4adb1fa5a`。

---

### `crates/cli/commands/src/node.rs`
**模块：** `reth node` 入口（`NodeCommand`）
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `args` import 新增 `MetricArgs`、`StaticFilesArgs`、`StorageArgs`（`08c61535d`）
- `NodeCommand` 结构体新增 `static_files: StaticFilesArgs`（next_help_heading "Static Files"）与 `storage: StorageArgs`（next_help_heading "Storage"）字段
- `parse_args` doc comment：`/// Parsers only the default CLI arguments` → `/// Parses only the default CLI arguments` (typo fix `c1ae2af8c`)
- 启动 log 行 `"Starting reth"` → `"Starting {}", version::version_metadata().name_client`（`2e5a155b6` use installed client name）
- 新增 `engine.validate()?` 调用
- `init_db` 返回不再裹 `Arc::new(...)`；改为 `init_db(...)?.with_metrics_if(self.db.metrics_enabled())`（`370a548f3` derive Clone + `d36006a4d` disable database metrics）
- 末尾增加 `impl<C, Ext> NodeCommand<C, Ext> { fn chain_spec() }`

**Gravity 侧变更（baseline `0cb1687c1c`）：**
- `dfa14dcdea` 注入 `use gravity_primitives::init_gravity_config`、`args` import 增加 `GravityArgs`
- `NodeCommand` 结构体新增 `pub gravity: GravityArgs`（未指定 next_help_heading）
- `Self { ... gravity, ... }` 解构并增加：
  ```
  let gravity_config = gravity.to_config();
  tracing::info!(target: "reth::cli", gravity_config = ?gravity_config, "Initializing global gravity config");
  init_gravity_config(gravity_config);
  ```
- `node_config` 结构体增加 `gravity` 字段
- `init_db` 仍 `Arc::new(init_db(db_path.clone(), self.db.database_args())?)`
- 启动 log 仍 `"Starting reth"`（v1.8.3 形态）

**影响范围：** `init_gravity_config` 必须在 `NodeConfig` 构造之前调用（上游 `gravity` 字段从 `NodeConfig` 读，但 `gravity_primitives::get_gravity_config()` 是全局单例，被 `launcher` / `common.rs::create_provider_factory` 在 builder 启动早期就读取）。

**解决方案建议：** **mechanical-merge**，保留 gravity 字段与 `init_gravity_config` 调用。
**理由：**
1. **保留** `pub gravity: GravityArgs` 字段、`init_gravity_config(gravity.to_config())` 调用、`NodeConfig.gravity` 字段（`dfa14dcdea` gravity 配置 — 业务必需）。
2. **接受**上游：`MetricArgs`、`StaticFilesArgs`、`StorageArgs` 字段及对应 `next_help_heading`；`engine.validate()?` 调用；`init_db ... .with_metrics_if(...)` 改造；启动 log 中的 `name_client`；末尾 `chain_spec()` 辅助方法；`Parsers` → `Parses` typo 修复。
3. `init_db` 不再 `Arc::new(...)` — 影响后续传给 `NodeBuilder::with_database(database)` 的类型；上游已确保 `NodeBuilder` 接受 owned `DatabaseEnv`，gravity 同步即可。
4. **解构顺序**：`init_gravity_config(gravity.to_config())` 必须放在所有用到 `get_gravity_config()` 的下游代码（pipe execution / RocksDB 路径）执行之前，即 `NodeBuilder` 构造前；upstream 把 `engine.validate()?` 放在解构后 — gravity 的 `init_gravity_config` 应紧随其后或在其前。
5. `parse_args` typo 是上游修复，直接 take-upstream。

引用：gravity `dfa14dcdea`、`a1d7365bd6`；upstream `08c61535d`、`c1ae2af8c`、`2e5a155b6`、`370a548f3`、`d36006a4d`。

---

### `crates/cli/commands/src/db/list.rs`
**模块：** `reth db list`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `clap` 新增 `builder::RangedU64ValueParser`（`3116adf26` reject zero db list len）
- `reth_db` import 新增 `transaction::DbTx`
- `ListTableViewer<'a, N>::tool` 的 generic 从 `Arc<DatabaseEnv>` 改为 `DatabaseEnv`（`370a548f3`）
- `view<T: Table>` 内：增加 `tx.disable_long_read_transaction_safety()`（`75e9359fe`）；将 `tx.table_entries(table_name)` 替换为 `let table_db = tx.inner().open_db(Some(name)).wrap_err(...)?; let stats = tx.inner().db_stat(table_db.dbi()).wrap_err(...)?; let total_entries = stats.entries();`（`aa5b12af4` + `1265a89c2` 一致化 dbi 接口）
- 移除 `std::sync::Arc`

**Gravity 侧变更（baseline）：**
- `a1d7365bd6` 把 `view` 内 mdbx tx 调用退化为 `tx.table_entries(table_name).wrap_err("Could not open db.")?`（RocksDB 没有 `open_db`/`db_stat`/`dbi` 接口，用单一 `table_entries` 抽象代替）
- `ListTableViewer` 仍以 `Arc<DatabaseEnv>` 持有 `tool`

**影响范围：** `tool` 类型与 `common.rs` 中 `ProviderFactory<NodeTypesWithDBAdapter<N, Arc<DatabaseEnv>>>` 一致；改 owned 需级联。

**解决方案建议：** **mechanical-merge**，需 RocksDB-aware 重新评估上游 `db_stat` 路径。
**理由：**
1. `Arc<DatabaseEnv>` → `DatabaseEnv` 接受上游（与 `common.rs` 决策保持一致）。
2. 接受上游 `RangedU64ValueParser` 与 `disable_long_read_transaction_safety()`。
3. **关键决策**：上游 `tx.inner().open_db(...).db_stat(...)` 是 mdbx-specific 接口。Gravity baseline 的 `tx.table_entries(...)` 是 `a1d7365bd6` 为 RocksDB 引入的统一抽象 — 若 gravity 的 `DatabaseEnv` 在 v2.3.0 仍是 mdbx（RocksDB 通过独立 `RocksDBProvider` 出场），那 `tx.inner().open_db().db_stat()` 调用合法（mdbx 仍存在），但报告的 entries 数只覆盖 mdbx 侧的表，**不包含 RocksDB 表**（如 `AccountsHistory`、`StoragesHistory`、`TransactionHashNumbers` 等 storage v2 已迁到 RocksDB 的表）。
4. 建议：take-upstream `tx.inner().open_db()` 路径用于 mdbx 表；针对 storage v2 已迁出的表，需 fallback 到 `rocksdb_provider.table_entries(name)`（参考 v2.3.0 `db/stats.rs::rocksdb_stats_table` 模式）。如时间紧，先 take-upstream 让 mdbx 路径正确，留 TODO 给 RocksDB 表。

引用：gravity `a1d7365bd6`；upstream `3116adf26`、`370a548f3`、`aa5b12af4`、`1265a89c2`、`75e9359fe`。

---

### `crates/cli/commands/src/db/stats.rs`
**模块：** `reth db stats`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- 新增 `use reth_db::mdbx`（freelist db 引用）
- `reth_provider` import 增加 `RocksDBProviderFactory`
- 移除 `use std::sync::Arc`
- `db_tables.sort()` → `sort_unstable()`（`390def905`）
- `view(|_tx| ...)` 内 `tx.inner().open_db(...)` + `tx.inner().db_stat(...)` 真实统计；`freelist` 通过 `tx.inner().env().freelist()?` + `tx.inner().db_stat(mdbx::Database::freelist_db().dbi())?` 计算
- 新增 `fn rocksdb_stats_table<N: NodeTypesWithDB>(&self, tool: &DbTool<N>) -> ComfyTable`：调用 `tool.provider_factory.rocksdb_provider().table_stats()` 渲染 SST size / memtable size / pending compaction（`37b5db0d4` + `04d4c9a02`）

**Gravity 侧变更（baseline）：**
- `a1d7365bd6` 将 `view(|tx| ...)` 退化为 `view(|_tx| ...)` 占位实现：所有 `open_db`/`db_stat`/`freelist` 注释掉，写死 `page_size = 16384`, `leaf_pages = 0`, `branch_pages = 0`, `overflow_pages = 0`, `freelist = 0`, `freelist_size = 0`，并在源码内留下 `// TODO: Implement open_db and db_stat for RocksDB` 注释
- `db_stats_table` 函数签名仍是 `<N: NodeTypesWithDB<DB = Arc<DatabaseEnv>>>`

**影响范围：** baseline 的 `db stats` 输出是无意义占位，上游 v2.3.0 提供了正确的 mdbx + RocksDB 双路径实现 — 这是 net improvement。

**解决方案建议：** **take-upstream**。
**理由：**
1. Gravity baseline 占位逻辑是 `a1d7365bd6` 集成 RocksDB 时的暂时拖延（TODO 注释自己也写明），没有 gravity-specific 语义，纯粹是工作量不足的占位。
2. 上游 v2.3.0 把 mdbx + RocksDB 双 backend 的 stats 真实接入，gravity 应直接采纳 — `tool.provider_factory.rocksdb_provider()` 在 `common.rs` 已经构造，可用。
3. `Arc<DatabaseEnv>` → `DatabaseEnv` 同 `common.rs` 决策。
4. 接受 `sort_unstable()`、`mdbx::Database::freelist_db()`、`RocksDBProviderFactory` import、`rocksdb_stats_table` 全部新代码。

引用：gravity `a1d7365bd6`；upstream `37b5db0d4`、`04d4c9a02`、`390def905`、`370a548f3`。

---

### `crates/cli/commands/src/stage/drop.rs`
**模块：** `reth stage drop`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- 移除 `use itertools::Itertools`、`use reth_db::static_file::iter_static_files`、`use reth_static_file_types::StaticFileSegment`
- 新增 `use reth_db::mdbx::tx::Tx`
- `reth_provider` import：移除 `writer::UnifiedStorageWriter`，新增 `DBProvider`、`RocksDBProviderFactory`、`StaticFileWriter`、`StorageSettingsCache`
- `execute<N>(self)` 改为 `execute<N>(self, runtime: reth_tasks::Runtime)`；`self.env.init::<N>(AccessRights::RW)?` → `self.env.init::<N>(AccessRights::RW, runtime)?`
- 删除整段 "Delete static file segment data" 前置逻辑（用 `static_file_provider.delete_jar` 直接删 jar；上游 `d10070e6f` 决定 stage drop 不删 jars，改用 `prune_to_unwind_target` + `StaticFileWriter`）
- `provider_factory.database_provider_rw()` → `unwind_provider_rw()`（`12cf3d685` CommitOrder）
- `StageEnum::Headers` 分支移除 `tx.clear::<tables::HeaderTerminalDifficulties>()?`（`563ae0d30` drop total difficulty support）
- `StageEnum::Bodies` 分支移除 `reset_prune_checkpoint(tx, PruneSegment::Transactions)?`（上游不再 prune transactions / headers - `3883df3e6`）
- `StageEnum::AccountHistory` + `StorageHistory` 拆分为两个独立分支（`2aff61776`）；每个分支根据 `provider_rw.cached_storage_settings().storage_v2` 选择 `tx.clear::<...>()` 或 `rocksdb.clear::<...>()`
- `StageEnum::TxLookup` 也按 storage_v2 选择 mdbx 或 RocksDB
- 大量 storage segments / change sets 新增 `prune_to_unwind_target` 调用（`e30e441ad`、`492fc20fd`、`a74cb9cbc`）

**Gravity 侧变更（baseline）：**
- `a1d7365bd6` 引入 RocksDB 时调整了 import 列表（保留 `iter_static_files` + `StaticFileSegment` 等 mdbx 时代路径）
- `1224ae1846` 不直接动 drop.rs，但相关 trait 链同步演进
- baseline 仍是 v1.8.3 catch-up 形态：`AccountHistory | StorageHistory` 合并分支、保留 `HeaderTerminalDifficulties` clear、保留 `reset_prune_checkpoint(tx, PruneSegment::Transactions)`、保留 `database_provider_rw()`、保留 `UnifiedStorageWriter` import

**影响范围：** `init::<N>` 签名级联到 `common.rs` 决策；`AccountHistory`/`StorageHistory` 拆分、`HeaderTerminalDifficulties` 移除、`PruneSegment::Transactions` 移除都属于上游单边数据 schema 演进，gravity 没有对应 patch。

**解决方案建议：** **take-upstream**，对照 `common.rs` 改 `init` 签名。
**理由：**
1. baseline 在 `stage/drop.rs` 上没有 gravity-specific 语义修改（`a1d7365bd6` 触及 import 是为 RocksDB 接入做的桥接，没有独立业务逻辑）。
2. 上游 `e30e441ad`、`492fc20fd`、`d10070e6f`、`2aff61776`、`563ae0d30`、`3883df3e6` 全是 schema/正确性演进，gravity 必须 follow（否则 `reth stage drop` 会泄漏 static file 或 RocksDB 表数据）。
3. 接受 `unwind_provider_rw()` 替代 `database_provider_rw()`（`12cf3d685` CommitOrder for RocksDB/MDBX unwind atomicity 是 gravity 的强需求 — RocksDB + MDBX 双 backend 必须原子 unwind）。
4. **唯一需 verify**：上游 `cached_storage_settings().storage_v2` 分支若 gravity 默认走 `storage_v2 = false`（pipe execution 路径），需确认 `rocksdb` 分支不会被误触发；若 gravity 想保持 v1 行为，可加 default = v1 的 chainspec 配置兜底。

引用：upstream `e30e441ad`、`492fc20fd`、`d10070e6f`、`2aff61776`、`563ae0d30`、`3883df3e6`、`12cf3d685`、`0e4f143172`。

---

### `crates/cli/commands/src/stage/dump/execution.rs`
**模块：** `reth stage dump-stage execution`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `reth_consensus` import 移除 `ConsensusError`（PR #20843 `412f39e22` remove `Consensus::Error` associated type）
- `reth_node_builder::NodeTypesWithDB` → `reth_node_api::{HeaderTy, TxTy}`（`13c5504aa` use node types in execution stage dump）
- `reth_provider::providers` 新增 `RocksDBProvider`
- 函数增加 `#[expect(clippy::too_many_arguments)]`
- 函数签名增加 `runtime: reth_tasks::Runtime` 参数
- 函数 `where` clause：`N: ProviderNodeTypes<DB = Arc<DatabaseEnv>>` → `DB = DatabaseEnv`；`C: FullConsensus<E::Primitives, Error = ConsensusError>` → `C: FullConsensus<E::Primitives>`
- `ProviderFactory::<N>::new(Arc::new(output_db), ..., StaticFileProvider::...)` → `new(output_db, ..., StaticFileProvider, RocksDBProvider::builder(output_datadir.rocksdb()).build()?, runtime)?`（返回 `Result` 而非裸值）

**Gravity 侧变更（baseline）：**
- `1224ae1846` 在 `dump_execution_stage` 中移除了对 read provider factory 的传递（去掉了一个并行读 factory 参数）
- `a1d7365bd6` 引入 `Arc::new(output_db)` 包装（与 `init_db` 返回类型一致）
- 仍是 `N: ProviderNodeTypes<DB = Arc<DatabaseEnv>>` + `C: FullConsensus<E::Primitives, Error = ConsensusError>`

**影响范围：** 全部跟随 `common.rs` 决策。

**解决方案建议：** **take-upstream**。
**理由：**
1. gravity 在 `stage/dump/execution.rs` 上的修改 (`1224ae1846` 移除并行读 factory) 与上游 v2.3.0 重构方向一致 — 上游也只保留单一 provider factory，gravity 修改自然被上游形态吸收。
2. 接受所有 upstream 改造：`Arc<DatabaseEnv>` → `DatabaseEnv`、`Error = ConsensusError` 关联类型移除、`HeaderTy`/`TxTy` 替换 `NodeTypesWithDB`、`RocksDBProvider` 加入、`runtime` 参数添加、`ProviderFactory::new(...)?` 返回 Result。
3. **唯一保留**：无（gravity 没有此文件上的业务语义改动）。

引用：gravity `1224ae1846`、`a1d7365bd6`；upstream `13c5504aa`、`412f39e22`、`370a548f3`、`0e4f143172`、`662c0486a`。

---

### `crates/cli/commands/src/stage/dump/hashing_account.rs`
**模块：** `reth stage dump-stage account-hashing`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `reth_provider::providers` 新增 `RocksDBProvider`
- 函数泛型 `N: ProviderNodeTypes<DB = Arc<DatabaseEnv>>` → `DB = DatabaseEnv`
- 函数新增 `runtime: reth_tasks::Runtime` 参数
- `ProviderFactory::<N>::new(Arc::new(output_db), ...)` → `new(output_db, ..., RocksDBProvider::builder(output_datadir.rocksdb()).build()?, runtime)?`

**Gravity 侧变更（baseline）：**
- `1224ae1846` 移除并行读 factory 相关行（与 `execution.rs` 同样的 cleanup）
- `a1d7365bd6` 加 `Arc::new(output_db)` + `use std::sync::Arc`
- `<DB = Arc<DatabaseEnv>>`

**影响范围：** 同 `execution.rs`。

**解决方案建议：** **take-upstream**。
**理由：** Gravity 此文件没有业务语义修改，纯粹跟着 `Arc<DatabaseEnv>` / parallel-factory cleanup 滑动；上游 v2.3.0 的 `RocksDBProvider` + `runtime` 接入是必经路径。
引用：gravity `1224ae1846`、`a1d7365bd6`；upstream `662c0486a`、`370a548f3`、`0e4f143172`。

---

### `crates/cli/commands/src/stage/dump/hashing_storage.rs`
**模块：** `reth stage dump-stage storage-hashing`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** 与 `hashing_account.rs` 完全对称（`RocksDBProvider` 注入、`Arc<DatabaseEnv>` 去包、`runtime` 参数）；附加 `d8de8afa9` "bound storage hashing stages memory" 不直接动该文件的入口签名。
**Gravity 侧变更（baseline）：** 同 `hashing_account.rs`。
**影响范围：** 同。
**解决方案建议：** **take-upstream**。
**理由：** 同 `hashing_account.rs`。
引用：gravity `1224ae1846`、`a1d7365bd6`；upstream `662c0486a`、`370a548f3`、`0e4f143172`。

---

### `crates/cli/commands/src/stage/dump/merkle.rs`
**模块：** `reth stage dump-stage merkle`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- 同 `execution.rs`：移除 `ConsensusError` import、`<DB = DatabaseEnv>`、`RocksDBProvider`、`runtime` 参数、`#[expect(clippy::too_many_arguments)]`、`ProviderFactory::new(...)?`
- 新增 `use alloy_primitives::Address`、`reth_db_api::models::BlockNumberAddress`、`reth_node_api::HeaderTy`（PR #18022 perf StorageChangeSets import `71c124798` + 后续 type-trait 重命名）
- `consensus: impl FullConsensus<N::Primitives, Error = ConsensusError>` → `impl FullConsensus<N::Primitives>`（`412f39e22`）
- `bdc59799d` 把若干 `unwrap` 换成 `?`（merkle stage 内 error propagation 改进）
- `unwind_and_copy` 内的 `consensus` 参数同步去 `Error = ConsensusError`

**Gravity 侧变更（baseline）：**
- `1224ae1846` 在 `dump_merkle_stage` 中删除 28 行 — 是该 PR 中对此文件影响最大的：去掉了一个 `parallel_provider_factory` 参数与相关的 setup 逻辑
- `a1d7365bd6` 加 `Arc::new(output_db)`
- 仍 `Error = ConsensusError`

**影响范围：** 同 dump 系列。

**解决方案建议：** **take-upstream**。
**理由：** Gravity 此文件 `1224ae1846` 删除并行 read factory 的方向与上游 storage v2 单 provider factory 一致 — 没有冲突业务语义，take-upstream 即可保留 cleanup 效果。
引用：gravity `1224ae1846`、`a1d7365bd6`；upstream `13c5504aa`、`71c124798`、`412f39e22`、`bdc59799d`、`662c0486a`、`370a548f3`。

---

### `crates/cli/commands/src/stage/dump/mod.rs`
**模块：** `reth dump-stage` 入口宏 + execute
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `reth_db::DatabaseArguments` → `reth_db::mdbx::DatabaseArguments`（PR #21641 `370a548f3` + 模块化）
- `handle_stage!` 宏两个 arm 都加上 `$runtime` 参数串接：`$stage_fn($tool, *from, *to, output_datadir, *dry_run, $runtime).await?`
- `Command::execute<N, Comp, F>(self, components: F)` → `execute(self, components: F, runtime: reth_tasks::Runtime)`
- match 每个分支调用 `handle_stage!(..., cmd, ..., runtime.clone())`

**Gravity 侧变更（baseline）：**
- `a1d7365bd6` 影响：`use reth_db::{init_db, DatabaseArguments, DatabaseEnv}`（仍是顶层 path）
- baseline `handle_stage!` 宏没有 `$runtime` 段
- `execute` 签名无 `runtime`

**影响范围：** 决定每个 `stage/dump/*.rs` 文件入口签名是否串接 `runtime`，与 `common.rs` 强相关。

**解决方案建议：** **take-upstream**。
**理由：**
1. baseline 没有 gravity-specific 业务语义；纯粹是 wiring。
2. 接受 `DatabaseArguments` 路径移到 `mdbx` 子模块下、宏 arm 串接 `runtime`、`execute` 增加 `runtime` 参数。

引用：upstream `370a548f3`、`0e4f143172`。

---

### `crates/cli/commands/src/stage/run.rs`
**模块：** `reth stage run`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- 新增 `use reth_node_builder::common::metrics_hooks`
- `Hooks::builder().with_hook({...db.report_metrics()}).with_hook({...sfp.report_metrics()}).build()` 整段替换为 `metrics_hooks(&provider_factory)` 单调用
- `MetricServerConfig` 新增 `data_dir.pprof_dumps()` 参数（`bcd74d021` jeprof pprof dumps dir）
- `UnifiedStorageWriter::commit_unwind(provider_rw)?` 与 `UnifiedStorageWriter::commit(provider_rw)?` 都替换为 `provider_rw.commit()?`（上游 storage v2 把 unified writer 内化到 provider 上）
- 末尾增加 `requires_commit()` helper（`23eb96c20` always commit unwind for headers）
- `init` 调用同步串接 `runtime` 参数（间接：通过 `EnvironmentArgs` 改造，本文件大量调用 `self.env.init`）

**Gravity 侧变更（baseline）：**
- `1224ae1846` 移除并行读 factory 相关接线（`stage/run.rs` 中 12 行变更）
- v1.8.3 形态：使用 `Hooks::builder().with_hook(...).with_hook(...).build()` 手动构造
- 使用 `UnifiedStorageWriter::commit_unwind/commit`

**影响范围：** `metrics_hooks` 是上游对所有子命令的统一抽象；`provider_rw.commit()` 替换是 storage v2 内化路径。

**解决方案建议：** **take-upstream**，对照 `common.rs` 串接 `runtime`。
**理由：**
1. `metrics_hooks(&provider_factory)` 是 upstream 把重复代码下推到 `reth-node-builder` 的 cleanup，gravity 应 follow；同时 baseline 那段 `report_metrics` 调用与上游单调用语义等价。
2. `provider_rw.commit()` 替换 `UnifiedStorageWriter::commit` — 上游已把 unified writer 接入 provider 内部，gravity 此处没有独立业务依赖。
3. `requires_commit()` helper 是上游对 stage 行为的 metadata 暴露，接受。
4. **唯一需 verify**：`metrics_hooks` 内部是否 cover gravity baseline 中 `db.report_metrics()` + `sfp.report_metrics()` 的 metric 标签集合（若上游 metric 名变化，gravity 的 grafana dashboard 可能要同步）。

引用：gravity `1224ae1846`；upstream `bcd74d021`、`23eb96c20`、`0e4f143172`，以及 `metrics_hooks` 由 `reth-node-builder` 引入的统一封装 PR（v1.8.3..v2.3.0 区间）。

---

### `crates/cli/commands/src/test_vectors/compact.rs`
**模块：** `reth test-vectors compact`
**冲突类型：** AA（rename detection 把双方 `compact.rs` 都判作"新增"，但实际两侧文件都在 baseline / v2.3.0 中存在）
**上游变更（v1.8.3 → v2.3.0）：** 仅 nightly clippy 修正（`d2b4ab53d`）：`print!("{}", &type_name)` → `print!("{}", type_name)`、`format!("…/{}.json", &type_name)` → `format!("…/{}.json", type_name)`（去掉不必要的 `&`）。
**Gravity 侧变更（baseline）：** 无 gravity-specific 修改；v1.8.3 catch-up 形态保留 `&type_name`。
**影响范围：** 无业务影响，纯 lint。
**解决方案建议：** **take-upstream**。
**理由：** 上游修复 nightly clippy 的 `needless_borrow` 警告；gravity 没有理由保留 `&`。AA 状态由 git rename detection 误判产生（两侧 blob hash 不同但路径相同），实际是 small text edit。
引用：upstream `d2b4ab53d`。

---

### `crates/cli/util/src/sigsegv_handler.rs`
**模块：** SIGSEGV 处理器安装
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `967edb541` "fix: fix new casting error in signal handler"：`sa.sa_sigaction = print_stack_trace as *const () as libc::sighandler_t` → `as unsafe extern "C" fn(libc::c_int) as libc::sighandler_t`（更严格的函数指针 cast 避免新 rustc 报警）
- `2cae43864` "fix: sigsegv handler"：用 `libc::sysconf(libc::_SC_PAGESIZE)` 动态获取页对齐，并将 `Layout::from_size_align(alt_stack_size, 1)` 改为 `Layout::from_size_align(alt_stack_size, page_sz)`

**Gravity 侧变更（baseline）：**
- `9974ad0618` 把 `sa.sa_sigaction` 回退为 `print_stack_trace as *const () as libc::sighandler_t`（撤回 `967edb541`）— commit message 仅称 "fix CI test of unit.yml"，未说明回退原因；可能是当时 rust toolchain / target triple 组合下 `unsafe extern "C" fn(libc::c_int) as libc::sighandler_t` 编译失败
- baseline `Layout::from_size_align(alt_stack_size, 1)` 仍未引入页对齐（未带 `2cae43864`）

**影响范围：** 进程崩溃栈打印。错误的 cast 在某些 toolchain 下不能编译；缺失页对齐在某些内核下可能触发 `sigaltstack` EINVAL。

**解决方案建议：** **take-upstream**，但需 verify gravity CI 编译环境。
**理由：**
1. 上游 `967edb541` cast 写法在当前 reth v2.3.0 默认 rust 工具链下应已编译通过（gravity 的 `9974ad0618` 是 2026-01 的修复，rust 工具链不断演进，可能已不再需要回退）。
2. 上游 `2cae43864` 的页对齐是 robustness 改进，gravity 应接收。
3. 若 CI 真在某 target triple 上仍编译失败，可 wrap 在 `cfg(target_os = "...")` 中 gravity-specific 分支处理，但默认采纳上游。

引用：gravity `9974ad0618`；upstream `967edb541`、`2cae43864`。

---

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [ ] 1. **`common.rs` 中 `init_genesis_with_settings` 与 gravity stage-checkpoint guard 的叠加**：上游新签名 `init_genesis_with_settings(&provider_factory, self.storage_settings())` 是否会触发 gravity 在 pipe-execution 模式下原有的 "checkpoint 被重置为 0" 问题？需在 e2e 上验证 `should_init = ... is_none_or(|ck| ck.block_number == 0)` guard 是否仍能阻断 `init_genesis_with_settings` 内部的初始化。如果 `init_genesis_with_settings` 自身在 storage v2 路径下已自带 idempotency，则 guard 可以撤销；否则 guard 必保。
   - [ ] 冲突解决:待 e2e 验证后落地;crates/cli/commands/src/common.rs 现存 9 处冲突块(2026-07-03 实测)。
- [ ] 2. **`stage/drop.rs` 中 `cached_storage_settings().storage_v2` 分支判定**：gravity 部署是否始终以 `--storage.v2 false` 启动？若如此，新增的 `if settings.storage_v2 { rocksdb.clear(...) } else { tx.clear(...) }` 分支在 gravity 上永远走 `else` — 但 gravity 的 `AccountsHistory` / `StoragesHistory` / `TransactionHashNumbers` 是否已迁出 mdbx 到 RocksDB？需对照 `a1d7365bd6` 的 table routing 配置。
   - [ ] 冲突解决:待核实部署配置后落地;crates/cli/commands/src/stage/drop.rs 现存 10 处冲突块(2026-07-03 实测)。
- [ ] 3. **`db/list.rs` 对 storage v2 已迁出表的 entries 报告**：上游 `tx.inner().open_db().db_stat()` 只查 mdbx；若 gravity 把若干表迁到 RocksDB（参考 `a1d7365bd6` 的 `bc79cc44c` `--rocksdb.*` table routing），`reth db list <table>` 对这些表会返回 0 entries — 需在 `view` 内增加 `RocksDBProvider::table_entries` fallback。
   - [ ] 冲突解决:待决策后落地;crates/cli/commands/src/db/list.rs 现存 3 处冲突块(2026-07-03 实测)。
- [ ] 4. **`sigsegv_handler.rs` 上游回滚理由**：`9974ad0618` 把 cast 写法回退的真正原因需从 `fix CI test of unit.yml` 关联的 CI log 中追查 — 若是 nightly toolchain 兼容问题且 v2.3.0 默认 toolchain 已升级，可放心 take-upstream；若是 musl / cross-compile target 兼容问题，需保留 `cfg` 分支。
   - [ ] 冲突解决:待追查 CI log 后落地;crates/cli/util/src/sigsegv_handler.rs 现存 1 处冲突块(2026-07-03 实测)。
- [ ] 5. **`stage/run.rs` 中 `metrics_hooks(&provider_factory)` 的 metric 标签**：与 gravity baseline 手写 `db.report_metrics()` + `sfp.report_metrics()` 是否完全等价？若上游统一封装漏掉 RocksDB metrics，需在 gravity 侧 wrap 一层补齐（涉及 grafana dashboard 兼容）。
   - [ ] 冲突解决:待核实 metric 等价性后落地;crates/cli/commands/src/stage/run.rs 现存 5 处冲突块(2026-07-03 实测)。
