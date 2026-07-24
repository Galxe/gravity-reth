# storage-db-and-mdbx

> Baseline: `0cb1687c1c`（gravity main，含 reth v1.8.3 catch-up `d620fd0eeb`）
> 目标 upstream：reth `v2.3.0`
> **分支**: `merge-v2.3.0`。HEAD: `e6b7e5ba32`。

## 分组概要

- **文件数：** 10（全部 UU）
- **复杂度：** 高 — `crates/storage/db/Cargo.toml` 与 `crates/storage/db/src/lib.rs` 承载 rocksdb 默认后端的分发轴；`crates/storage/db-common/src/init.rs` 是 storage-v2 与 gravity rocksdb-only/nested-state-root 体系的整体冲撞点。
- **涉及模块：**
  - `reth-db` crate manifest + lib 入口 + MDBX 驱动（cursor / env / metrics）
  - `reth-libmdbx` crate manifest + Environment 包装
  - `reth-db-common` crate manifest + `init_genesis` 流程
  - `reth-db-models` `AccountBeforeTx` 子键 trait 实现
- **baseline 已有 commit（验证均在 `0cb1687c1c` 历史中）：**
  - `a1d7365bd6` feat(rocksdb): Integrating RocksDB into Reth (#212) — `default = ["rocksdb"]`、`SubkeyContainedValue` trait、`crates/storage/db/src/implementation/rocksdb/`、`pub mod generic;`、`pub mod database;`
  - `c64bd613e4` opt(persist): sharded rocksdb instances (#225) — `ShardingDirectories`
  - `0bd2a4717b` update rocksdb to 0.24.0 (#232)
  - `9974ad0618` fix(test): fix CI test of unit.yml (#241)
  - `66feb1509c` fix(storage): Fix parallel tx (#135) — Cursor `drop_fn` + 手写 `Debug` + `Drop`；`DatabaseEnv::tx()` → `ParallelTxRO`
  - `3ee6ac039e` fix(trie): merkle stage of history sync (#246) — 触及 cursor.rs
  - `ff103f976a` fix(unwind): commit view (#313) — init.rs `debug! → info!`
  - `1224ae1846` refactor(parallel): remove read provider factory (#213) — init.rs trait bound 简化
  - `671680af37` perf(state_root): nested-trie + `NestedStateRoot` (#149) — init.rs 引入 `TrieWriterV2` + `reth_trie_parallel::nested_hash`
  - `c0bf0cc429` opt(mdbx): sort trie entries + `mdbx_bench_tool` bench (#180) — libmdbx-rs Cargo.toml
  - `28561abb17` feat(hardfork): batch_storage_patches (#318) — 顺带把 environment.rs 的 `writeable→writable` 拼写修好了（1 行 doc）
  - `acc458846c` fix(rocksdb): flush batch data (#340) — 相邻 rocksdb 模块；不在本组冲突但约束 init.rs 必须保 rocksdb 路径
- **解决顺序依赖：**
  1. `crates/storage/db/Cargo.toml`（feature 集合是整组下游编译的根）。
  2. `crates/storage/db/src/lib.rs`（决定 re-export 哪个后端的 `DatabaseEnv`/`DatabaseArguments`）。
  3. `crates/storage/db/src/metrics.rs` → `cursor.rs` → `mdbx/mod.rs` 形成一个 `TableOperationMetrics` 类型演进链，按此顺序解决。
  4. `crates/storage/libmdbx-rs/{Cargo.toml, src/environment.rs}` 独立。
  5. `crates/storage/db-models/src/accounts.rs`（subkey trait） → `crates/storage/db-common/{Cargo.toml, src/init.rs}`。

---

## ⟲ f89d9d4e23 实际解法(2026-07-03,本组冲突已全部解决)

> 本组 10 个文件已由 `f89d9d4e23`("resolve storage&cache&state root (#375)")按
> **「src 整体还原 gravity baseline `0cb1687c1c`」** 策略解决(执行记录见
> `STORAGE-RESOLUTION-TODO.md`)。下方多个条目的 mechanical-merge / take-upstream
> 建议被此策略取代——正文保留原样作为决策史与 v2.4+ 再合并输入,**以本节与
> 文末 checklist 的实测结论为准**。

逐文件实测(`grep -c '^<<<<<<<'` 全部归零;`git diff 0cb1687c1c HEAD --` 判定与 baseline 关系):

| 文件 | 原建议 | 实际解法 | 差异要点 |
|---|---|---|---|
| db/Cargo.toml | mechanical-merge(gravity 主体) | baseline +4/-10 | 删 `hash_keys`/`criterion` 两个失效 `[[bench]]`(保 `get`,源文件在);+`quanta`(optional,**未接任何 feature,悬空**)+ unix `libc`;上游 `reth-metrics`/`strum`/`rustc-hash`/`tracing` 未加(metrics prebind 链未采,无需);**`op` feature 行保留未 trim(遗留断点,见下)** |
| mdbx/cursor.rs | mechanical-merge | =baseline 逐字节 | **上游 metric prebind(`TableOperationMetrics`)未采**(全仓不存在,实测);gravity `drop_fn`/`Debug`/`Drop` 完整保留 ✓ |
| mdbx/mod.rs | mechanical-merge | =baseline | 上游 `path`/`with_metrics_if`/`drop_orphan_table`/`DatabaseArguments::test()` 全部未采;gravity `ParallelTxRO`/`sync_mode: Option` 保留 ✓ |
| db/lib.rs | keep-gravity | =baseline | 一致 ✓(`utils.rs` 成孤儿,见 checklist 7) |
| db/metrics.rs | **take-upstream** | =baseline | **方向相反**:`quanta::Instant`/`TableOperationMetrics` 未引入,`std::time::Instant` + 元组 key 保留 |
| libmdbx-rs/Cargo.toml | mechanical-merge | baseline +1/-14 | +`crossbeam-queue`(**悬空**:唯一消费方 `txn_pool.rs` 是孤儿);删 `cursor`/`transaction` `[[bench]]`(源文件已删,正确);**⚠ 连 gravity #180 的 `[[bench]] mdbx_bench_tool` + `criterion`/`rand` dev-deps 也删了,但 `benches/mdbx_bench_tool.rs` 仍在磁盘且 `use rand`(遗留断点,见下)** |
| libmdbx-rs/environment.rs | **take-upstream** | =baseline 逐字节 | **方向相反**:`ReadTxnPool` 未引入(`txn_pool.rs` 孤儿)、`mdbx_stat` 重命名未采 |
| db-common/Cargo.toml | mechanical-merge | baseline +1 | +`reth-tasks`(dev-dep,当前未用,无害);`reth-trie-parallel` 保留 ✓ |
| db-common/init.rs | keep-gravity + cherry-pick | =baseline 逐字节 | 主体一致 ✓;**建议的 `StageCheckpoint::new(genesis_block_number)` cherry-pick 未做**(:158 仍 `Default::default()`,实测)— 非零 genesis 支持列 v2.4+ 评估(TODO 跨 crate 条目 5) |
| db-models/accounts.rs | keep-gravity + 叠加 | =baseline 逐字节 | 主体一致 ✓;建议叠加的 `ValueWithSubKey` 并存与尾部 `impl_compression_for_compact!` 未做(上游版留在孤儿 `db-models/src/storage.rs`) |

**孤儿文件**(在磁盘、不在 mod 树;实测无 `mod` 声明):`db/src/utils.rs`、
`libmdbx-rs/src/txn_pool.rs`、`db-models/src/storage.rs`。

**遗留断点(本组范围)**:
1. **`op` feature 悬挂引用**:`db/Cargo.toml:105` 保留 `op = ["reth-db-api/op", "reth-primitives-traits/op"]`,
   但 db-api 的 Cargo.toml(v2.3.0 侧 + surgical 修补)已无 `op` feature(实测)——cargo 在 feature
   解析期即报错(不需要 `--features op`),当前被 workspace 根缺 ~20 个 dep 的更大错误掩盖。
   修法 = 按本文原建议删除该行(规则 1)。
2. **`mdbx_bench_tool` bench 失挂**:`benches/mdbx_bench_tool.rs`(gravity #180)仍在磁盘并
   `use rand`,但 `[[bench]]` 声明与 `criterion`/`rand` dev-deps 已被删——cargo autobenches
   会把它当默认 harness bench 目标自动发现,`cargo bench/-​-benches -p reth-libmdbx` 编译必挂。
   修法:恢复 `[[bench]] mdbx_bench_tool` + `criterion`/`rand` dev-deps,或删文件(工具已弃则删)。
3. **`SubkeyContainedValue` 链接前置**(与 api 组共享):定义全仓不存在,`db-models/accounts.rs:2,18` /
   `db-api/models/mod.rs:12` 在用,待 primitives-traits 组恢复(TODO 跨 crate 条目 1)。

---

## 逐文件分析

### `crates/storage/db/Cargo.toml`

**模块：** `reth-db` crate manifest — feature / default 集合；后端选择
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `7594e1513` (#22211) `std::time::Instant` → `quanta::Instant`；新增 `quanta = { workspace = true, optional = true }`，并加入 `mdbx` feature 集合。
- `598f228e2` (#22627) 移除 criterion benches、删除 `[[bench]]` 块、移除 `criterion` dev-dep。
- `a12454d2e` (#23654) cursor metric handle 预绑定 — 新增 `reth-metrics`、`strum`、`rustc-hash` 可选依赖，全部纳入 `mdbx` feature。
- `fe7a4c80b` (#23685) ZFS 检测 — 新增 `[target.'cfg(unix)'.dependencies] libc.workspace = true`，新增 `tracing.workspace = true`。
- `62d99888d` (#23697) 把 unix 段移到 `strum` 之后。
- `677d07041` (#23186) `reth-primitives-traits` 调整（影响下游 trait import）。
- `15338b811` (#23198) 上游移除 `op` feature。
- 上游已无 `op`、`failpoints`、`bench` feature；无 `[dependencies.fail]`；无 `[[bench]]` 块；无 `rocksdb` 依赖；`default = ["mdbx"]`。

**Gravity 侧变更（baseline 上相对 reth v1.8.3 的增量）：**
- `a1d7365bd6` (#212) 引入：`default = ["rocksdb"]`、`rocksdb` feature、`rocksdb = "0.24.0"` 依赖；`test-utils`/`bench`/`failpoints` features；`op = ["reth-db-api/op", "reth-primitives-traits/op"]`（baseline 仍然保留这一行）；可选 `fail` 依赖；3 个 `[[bench]]` 块（`hash_keys` / `criterion` / `get`）；`mdbx` 与 `rocksdb` feature 都包含 `reth-storage-errors/std`、`alloy-primitives/std`。
- `0bd2a4717b` (#232) 固定 rocksdb 0.24.0。

**影响范围：** 全 workspace。任何依赖 `reth-db` 的 crate 都会继承本 feature 集合。`default = ["rocksdb"]` 是 gravity 主干语义，反向回到 `default = ["mdbx"]` 会让整棵 rocksdb 后端、`generic::` 分发、`acc458846c` 的 flush 修复全部失效。

**解决方案建议：** **mechanical-merge**（gravity 为主体）
**理由：**
- 保留 gravity 的 `default = ["rocksdb"]`、`rocksdb` feature 及其依赖块、`dep:parking_lot`、`mdbx`/`rocksdb` 中的 `reth-storage-errors/std` + `alloy-primitives/std`、3 个 `[[bench]]` 块、`failpoints` + 可选 `fail` 依赖；这些均源自 `a1d7365bd6` (#212) 与 `0bd2a4717b` (#232)，是 rocksdb 主干语义。
- 采纳上游对 `mdbx` feature 的增量依赖：`tracing.workspace = true`、`quanta`（#22211）、`reth-metrics`（#23654 metrics prebind 链需要）、`strum`、`rustc-hash`。它们都在 `mdbx` 启用时才参与编译，对 rocksdb 默认路径零代价；下游 `metrics.rs` 与 `cursor.rs` 的合并要求它们存在。
- 采纳 `[target.'cfg(unix)'.dependencies] libc.workspace = true`（#23685 ZFS 检测），无条件拉入但实际仅 `mdbx` 路径使用。
- `op` feature：baseline 仍保留 `op = ["reth-db-api/op", "reth-primitives-traits/op"]`，按规则 1（"never implement OP"），合并产物**应直接丢弃**这一行（与上游 `15338b811` 一致），后续若有调用方报错按规则 1 同步 trim。
- 上游 `598f228e2` 的 criterion 删除部分采纳：移除 `criterion.workspace = true` dev-dep；但 `[[bench]] name = "hash_keys"`/`criterion`/`get` 仍属于 gravity rocksdb 后端的 bench 体系 — 需要确认 `crates/storage/db/benches/` 下文件是否还在 worktree（若被 upstream 删除则一同清理；gravity-only 的保留）。

---

### `crates/storage/db/src/implementation/mdbx/cursor.rs`

**模块：** MDBX cursor 包装 — `Cursor<K, T>` 字段、`Drop` 实现
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `a12454d2e` (#23654, perf: prebind cursor operation metrics) 将 `metrics` 字段由 `Option<Arc<DatabaseEnvMetrics>>` 改为 `Option<TableOperationMetrics>`；`record_operation` 改为按 `operation.index()` 直接索引；从 `std::` import 中移除 `sync::Arc`。
- 文件级 doc 修订（小幅）。

**Gravity 侧变更：**
- `66feb1509c` (#135) 添加 `drop_fn: Option<Box<dyn Fn(&mut Self) + Send + Sync + 'static>>` 字段、`#[allow(clippy::type_complexity)]`、手写 `impl Debug for Cursor`、`with_drop_fn` setter、`impl Drop for Cursor` 触发 `drop_fn`；调整 `new_with_metrics` 构造体补 `drop_fn: None`。这是 `ParallelTxRO` cursor 回收钩子。
- `3ee6ac039e` (#246) 也曾触及本文件（merkle stage history sync 修复链路）。

**影响范围：** `Cursor::new_with_metrics` 类型签名变化（`Arc<DatabaseEnvMetrics>` → `TableOperationMetrics`）会级联到 `tx.rs` / `parallel_tx.rs` 所有构造调用点。gravity 的 `parallel_tx.rs` 现以 `Arc<DatabaseEnvMetrics>` 喂入 cursor — 需要在那一侧（不在本组）取出 `TableOperationMetrics` 后再传入。

**解决方案建议：** **mechanical-merge**（保留 gravity `drop_fn`/`Debug`/`Drop` + 采纳上游 metric 类型）
**理由：**
- gravity 的 `drop_fn` 机制（`66feb1509c`）承担并行事务 cursor 的回收，不可丢。手写 `Debug` 在 gravity 测试断言风格中依赖（保留 `assert_eq!(cursor.current(), Ok(Some(...)))` 形式）。
- 上游 metric prebind（`a12454d2e`）是隔离的纯性能改进。两侧不冲突，可逐 hunk 合并：
  - imports：保留 `use std::{borrow::Cow, collections::Bound, fmt, fmt::Debug, marker::PhantomData, ops::RangeBounds, sync::Arc};` 中除 `sync::Arc` 之外的全部条目；`Arc` 是否保留视 `parallel_tx.rs` 是否仍传 `Arc<DatabaseEnvMetrics>` 决定。
  - `metrics` 字段：`Option<TableOperationMetrics>`（上游）。
  - 保留 `#[allow(clippy::type_complexity)]`、`drop_fn` 字段、手写 `Debug`、`with_drop_fn`、`Drop`（gravity）。
  - `new_with_metrics` 接 `Option<TableOperationMetrics>`，构造时 `Self { ..., drop_fn: None }`。

---

### `crates/storage/db/src/implementation/mdbx/mod.rs`

**模块：** MDBX `DatabaseEnv` + `DatabaseArguments` + `Database` 实现 + 约 30 单元测试
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `d36006a4d` (#24806) 新增 `DatabaseEnv::with_metrics_if(enabled)`；新增 `path: PathBuf` 字段。
- `4a6f9cd5c` (#23335) 为 storage-v2 unwind 上限新增 `drop_orphan_table`。
- storage-v2 工作引入 `DatabaseArguments::test()` helper（64MB 小 geometry），把 `sync_mode` 从 `Option<SyncMode>` 改为非 `Option` 并显式默认；新增 `with_geometry_page_size`、`with_sync_mode`。
- 测试重构：`create_test_db` 返回 `(TempDir, DatabaseEnv)` 元组，许多 `assert_eq!(cursor.current(), Ok(Some(...)))` 改为 `assert!(cursor.current().unwrap().is_some())`（Cursor 失去 `PartialEq` derive 的副作用）。
- `a12454d2e` (#23654) 改变 cursor 构造时传入的 metrics 类型。

**Gravity 侧变更（baseline 上的 gravity-only 增量）：**
- `a1d7365bd6` (#212) `pub mod parallel_tx;` + `Database::tx()` 返回 `ParallelTxRO::try_new(self.inner.clone(), self.dbis.clone(), self.metrics.clone())`；同步 `dbis` 字段（rocksdb 后端 dbi 缓存）。
- `66feb1509c` (#135) 保留 `sync_mode: Option<SyncMode>`、`with_sync_mode(Option<SyncMode>)`；测试 helper `create_test_db(kind) -> Arc<DatabaseEnv>` 使用 `keep()`（不采用 tempdir 元组）。

**影响范围：** 这是 rocksdb 默认链路下唯一被启用 `mdbx` feature 后才参与的 MDBX 后端文件；但承载了 `Database for DatabaseEnv` 的 `ParallelTxRO` 绑定。上游的 `sync_mode: SyncMode` 与 gravity 的 `sync_mode: Option<SyncMode>` 是唯一结构性 API 分歧。

**解决方案建议：** **mechanical-merge**（gravity `ParallelTxRO`/`sync_mode: Option`/assert 形式 + 上游 `path`/`with_metrics_if`/`drop_orphan_table`/`DatabaseArguments::test`/`with_geometry_page_size` 叠加）
**理由：**
- 保留 `Database::tx() = ParallelTxRO::try_new(...)`（按 `66feb1509c`） — 移除会破坏并行事务体系。
- 保留 `sync_mode: Option<SyncMode>` 与 `with_sync_mode(Option<SyncMode>)`；`open()` 内部 `let sync_mode = args.sync_mode.unwrap_or(SyncMode::Durable);` 与上游可观测行为一致。
- 采纳上游：`path: PathBuf` 字段（`d36006a4d`），构造时 `path.to_path_buf()`；`with_metrics_if(enabled)`；`drop_orphan_table`；`DatabaseArguments::test()` helper；`with_geometry_page_size`。它们是叠加性能/工具改进，对 rocksdb 默认路径零运行时影响（受 `mdbx` feature 门控）。
- 保留 gravity 的 `create_test_db(kind) -> Arc<DatabaseEnv>`；**不**采纳上游 `(TempDir, DatabaseEnv)` 元组模式 — 改造会级联到大量测试调用点，超出本次合并范围。
- 保留 `assert_eq!(cursor.current(), Ok(Some(...)))` 形式（依赖 gravity 手写 `Debug`）。

---

### `crates/storage/db/src/lib.rs`

**模块：** `reth-db` crate 根 — 后端导出主线
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- 新增 `mod utils; pub use utils::is_database_empty;`（`#[cfg(feature = "mdbx")]`）。
- 保持单后端 MDBX-only 模型：`pub mod mdbx;`、`pub use mdbx::{create_db, init_db, open_db, open_db_read_only, DatabaseEnv, DatabaseEnvKind};`。
- 新增 `DatabaseArguments::test()` 使用点 + `create_test_rw_db_with_datadir` helper。
- 新增 `enable_legacy_multiopen()`（调用 `reth_libmdbx::ffi::mdbx_setup_debug`）。
- `format!("{ERROR_DB_CREATION}: {path:?}")` 把静态字符串换成路径化错误信息（多处）。
- crate 顶部 doc：`#![cfg_attr(docsrs, feature(doc_cfg))]`（移除 `doc_auto_cfg`）。

**Gravity 侧变更：**
- `a1d7365bd6` (#212) 重写为多后端分发：crate doc "Database implementations…"；`pub mod database;`；`pub mod generic;`；删除 `#[cfg(feature = "mdbx")] pub mod mdbx; pub use mdbx::*;`；新增 `pub use generic::{create_db, init_db, open_db, open_db_read_only};`；新增 `pub use crate::implementation::rocksdb::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind, ShardingDirectories};`；保留 `#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]`。
- `9974ad0618` (#241) 在 test_utils 中改 import：`use crate::DatabaseArguments; use reth_db_api::models::ClientVersion;`，调用 `DatabaseArguments::new(ClientVersion::default())`。
- `set_fail_point!` 宏在 `failpoints` feature 下保留。

**影响范围：** 这个文件**是** gravity rocksdb 默认后端的入口枢纽。回退到上游的 `pub mod mdbx; pub use mdbx::*` 会反推整棵 `crates/storage/db/src/implementation/rocksdb/`，并破坏 `c64bd613e4`（sharded persist）与 `acc458846c`（rocksdb flush）。

**解决方案建议：** **keep-gravity**（外加几处小幅上游采纳）
**理由：**
- 保留 gravity 块：crate doc、`pub mod database;`、`pub mod generic;`、`pub use generic::{create_db, init_db, open_db, open_db_read_only}`、`pub use crate::implementation::rocksdb::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind, ShardingDirectories}`、`set_fail_point!`、gravity test_utils imports。保留 `doc_cfg, doc_auto_cfg`（gravity 是上游的超集）。
- 采纳上游 `format!("{ERROR_DB_CREATION}: {path:?}")` 错误信息 — 兼容 rocksdb 后端的 UX 改进，无需类型变更。
- `is_database_empty` / `enable_legacy_multiopen` 严格 MDBX 专属，仅当走 `#[cfg(feature = "mdbx")]` 路径才有意义；gravity rocksdb-default 入口**不**应顶层 re-export 它们。
- `create_test_rw_db_with_datadir` 依赖 `DatabaseArguments::test()`，gravity 的 rocksdb `DatabaseArguments` 尚无此 stub；本次合并不引入，列入开放问题。

---

### `crates/storage/db/src/metrics.rs`

**模块：** 每表操作 / 事务 metric handle
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `7594e1513` (#22211) `use quanta::Instant`；从 `std::time::...` import 中移除 `Instant`。
- `a12454d2e` (#23654) 引入 `pub(crate) type TableOperationMetrics = Arc<[OperationMetrics; Operation::COUNT]>`；`operations` 由 `FxHashMap<(&'static str, Operation), OperationMetrics>` 改为 `FxHashMap<&'static str, TableOperationMetrics>`；记录路径改为 `metrics[operation.index()].record(...)`；新增 `table_operation_metrics(table) -> TableOperationMetrics` 访问器。

**Gravity 侧变更：**
- `a1d7365bd6` (#212) 在 rocksdb 集成中保留全套 `DatabaseEnvMetrics`；保留 `(table, Operation)` 元组 key 路径；保留 `std::time::Instant`。

**影响范围：** 整条 cursor metric prebind 链依赖本文件提供 `TableOperationMetrics`。`quanta::Instant` 替换要求 `db/Cargo.toml` 已加入 `quanta` 依赖（参见上述）。

**解决方案建议：** **take-upstream**
**理由：**
- 纯上游性能演进，gravity 在此文件未引入任何特殊语义。Rocksdb 路径不进入本文件运行时（`record_closed_transaction` 等仍在 `#[cfg(feature = "mdbx")]` 下；rocksdb 后端走的是 `crates/storage/db/src/implementation/rocksdb/` 自己的 metric 体系）。
- 是 `cursor.rs` 与 `mdbx/mod.rs` 合并能编译过的前提。

---

### `crates/storage/libmdbx-rs/Cargo.toml`

**模块：** `reth-libmdbx` manifest
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `598f228e2` (#22627) 移除 criterion benches（`benches/cursor.rs` / `transaction.rs` / `utils.rs` 全删；对应 `[[bench]]` 块也删；移除 `criterion`/`rand` dev-deps）。
- `7bb5c579e` (#22631) 为 read-only txn pool 新增 `crossbeam-queue.workspace = true`。

**Gravity 侧变更：**
- `c0bf0cc429` (#180) opt(mdbx): sort trie entries + 新增 `mdbx_bench_tool` bench；附带 `criterion` + `rand` dev-deps；仍保留 `[[bench]] name = "cursor"` 与 `[[bench]] name = "transaction"`（这两个文件在 baseline 中仍存在，参考 `git ls-tree 0cb1687c1c -- crates/storage/libmdbx-rs/benches/`）。

**影响范围：** 局限于本 crate。Worktree 实测 `crates/storage/libmdbx-rs/benches/` 目录下只剩 `mdbx_bench_tool.rs`（上游删除已生效），baseline 的 `cursor`/`transaction` bench 条目变为悬挂引用。`crossbeam-queue` 在 worktree HEAD 已被 `txn_pool.rs` 引用（该文件由 #22631 引入）。

**解决方案建议：** **mechanical-merge**（保留 gravity bench + 接纳上游 crossbeam-queue + 清理失效 bench 条目）
**理由：**
- 采纳 `crossbeam-queue.workspace = true`（#22631）— `txn_pool.rs` 强依赖。
- 丢弃 `[[bench]] name = "cursor"` 与 `[[bench]] name = "transaction"` 块（对应文件已被上游删除）。
- 保留 `[[bench]] name = "mdbx_bench_tool"`（gravity-only `c0bf0cc429`）与 `criterion` + `rand` dev-deps（撑此 bench）。
- 保留 `tempfile.workspace = true`。

---

### `crates/storage/libmdbx-rs/src/environment.rs`

**模块：** MDBX env 包装 — `Environment`、`Stat`
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `7bb5c579e` (#22631) 新增 `use txn_pool::ReadTxnPool;` import；`Environment` 新增 `ro_txn_pool: ReadTxnPool` 字段 + drop 时排空 pool；同步引入 `crates/storage/libmdbx-rs/src/txn_pool.rs`。
- `7a78044587` (#22630) 拼写修正：`MDB_stat` doc 与方法名 `mdb_stat` → `mdbx_stat`。
- `8b8430ac4` (#23339) 文档拼写 `writeable` → `writable`。

**Gravity 侧变更：**
- `28561abb17` (#318) 在引入 `batch_storage_patches` 时附带把 `writeable → writable` 这一行 doc 修复带上了（与上游 `8b8430ac4` 同效）。其他 hunk 没有 gravity-only 增量。

**影响范围：** 微不足道。本 crate 上游的 `txn_pool` 模块（`crates/storage/libmdbx-rs/src/txn_pool.rs`）只存在于 upstream，需作为 `take-upstream` 的一部分一并接纳（worktree HEAD 已包含对 `Environment::ro_txn_pool` 的引用，证实模块在合并产物中已就位）。`mdb_stat → mdbx_stat` 重命名只有一处内部调用方。

**解决方案建议：** **take-upstream**
**理由：**
- gravity 在本文件上无独立语义（`28561abb17` 仅做拼写修正，与上游 `8b8430ac4` 等效）。
- `txn_pool` 引入与 `mdbx_stat` 重命名是 upstream 性能/拼写改进，机械接纳。
- 合并后需 `rg "\.mdb_stat\(" crates/storage/libmdbx-rs/src/` 验证无残余调用。

---

### `crates/storage/db-common/Cargo.toml`

**模块：** `reth-db-common` manifest
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `b9969c5b1` (#22954, storage-v2 默认化) 移除 dev-dep 上的 `mdbx` feature gate 假设（实际上 v2.3.0 dev-dep 仍写成 `reth-db = { workspace = true, features = ["mdbx"] }`，因为上游 reth-db 默认就是 mdbx）；新增 `reth-tasks.workspace = true` dev-dep。

**Gravity 侧变更：**
- baseline 在 `[dependencies]` 中包含 gravity-only 的 `reth-trie-parallel.workspace = true`（来自 `671680af37` 的 nested-state-root 改造，`init.rs` 需要）。
- dev-deps 写作 `reth-db = { workspace = true }`（无 `mdbx` feature — gravity reth-db `default = ["rocksdb"]` 已满足测试需求）；包含 `alloy-consensus.workspace = true`。

**影响范围：** dev-deps 仅影响 unit test 编译。`reth-trie-parallel` 是 `init.rs` 中 `NestedStateRoot` 的承重依赖。

**解决方案建议：** **mechanical-merge**
**理由：**
- `[dependencies]` 保留 gravity 的 `reth-trie-parallel.workspace = true`（`init.rs` 的 `use reth_trie_parallel::nested_hash::NestedStateRoot;` 依赖它）。
- `[dev-dependencies]` 保留 gravity 的 `reth-db = { workspace = true }`（**不带** `mdbx` feature — gravity 的 reth-db `default = ["rocksdb"]` 已满足测试），保留 `alloy-consensus.workspace = true`。
- 添加上游 `reth-tasks.workspace = true` dev-dep — 即便此次 init.rs 走 keep-gravity 暂不使用，也是 workspace 级 dev-dep，加上后无负担。

---

### `crates/storage/db-common/src/init.rs`

**模块：** `init_genesis` 流程 — chainspec → 写 DB 完成 genesis 区块、state、trie、history、stage checkpoint、static file
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** storage-v2 默认化引发的整体重写：
- `b9969c5b1` (#22954) 引入 `StorageSettings` / `StorageSettingsCache` / `MetadataProvider` / `MetadataWriter` / `RocksDBProviderFactory` / `NodePrimitivesProvider` / `StateWriteConfig` 等 trait bound；`init_genesis_with_settings(...)` 包装。
- `1021fc2c2` (#23919) 新增 `init_genesis_with_settings_and_validate(validate_genesis_hash: bool)`，对应 `--debug.skip-genesis-validation`。
- 多个 storage-v2 init-state 优化（`STORAGE_COMMIT_THRESHOLD = 100_000`、`STATE_ROOT_COMMIT_THRESHOLD = 25_000`、`SealedHeader` trie 进度写入、非零 genesis 时 `AccountChangeSets`/`StorageChangeSets` 段 `expected_block_start` 预写、`HashedPostState` 路径上丢 prefix set 避免 OOM、已知 hash 时跳过 `seal_slow`）。
- 新增 `pub use reth_provider::init::{insert_account_history, insert_genesis_account_history, insert_genesis_history, insert_genesis_storage_history, insert_history, insert_storage_history};` re-export。
- import 新增 `cursor::{DbCursorRW, DbDupCursorRW}`、`models::{StorageShardedKey, AccountBeforeTx, BlockNumberAddress, IntegerList, ShardedKey}`、`SealedHeader`、`AddressMap`/`B256Map`/`B256Set`。

**Gravity 侧变更（baseline 已有）：**
- `ff103f976a` (#313) `Genesis already written` 日志由 `debug!` 改为 `info!`。
- `671680af37` (#149) 重写 `compute_state_root` 路径 — 使用 `reth_trie_parallel::nested_hash::NestedStateRoot` + `TrieWriterV2` 扩展 trait + `insert_world_trie(&provider_rw, alloc.iter())?`（替代上游 `insert_genesis_state` / storage-v2 流程）。
- `1224ae1846` (#213) 移除 parallel reader factory 后 trait bound 集合的简化（`init_genesis<PF>` 仅要求 `DatabaseProviderFactory + StaticFileProviderFactory + ChainSpecProvider + StageCheckpointReader + BlockHashReader`；`ProviderRW` 要求 `+ TrieWriterV2 + TrieWriter`）。
- 上述均与 baseline 的 rocksdb 主干（`a1d7365bd6` 之后）一起组成 gravity init 流程。

**影响范围：** 极大。上游引入的 `StorageSettings` / `MetadataProvider` / `RocksDBProviderFactory` / `NodePrimitivesProvider` / `StateWriteConfig` 等 trait 在 gravity provider crate 中尚未实现 — gravity rocksdb 的 provider 结构与 storage-v2 不兼容。强行采纳 `init_genesis_with_settings*` 会级联到每一个 `init_genesis*` 调用方（`cli/commands/src/common.rs`、`crates/node/builder`、`bin/reth/src/main.rs` 等）。反之，gravity 的 `NestedStateRoot` + `TrieWriterV2` 是链上 state root 语义的必需项，丢失会破坏 history sync。

**解决方案建议：** **keep-gravity**（外加一处 cherry-pick）
**理由：**
- gravity 已为 rocksdb + nested-state-root 重塑整个文件。结构不兼容上游 storage-v2 trait 集合：`MetadataProvider`/`MetadataWriter`/`StorageSettings`/`StorageSettingsCache`/`RocksDBProviderFactory`/`NodePrimitivesProvider`/`StateWriteConfig` / `init_genesis_with_settings*` 全部引用 gravity 未构建的 provider 接口。**强行 take-upstream 不可行**（需要数周工程量来把 storage-v2 trait 基建 port 进 rocksdb provider，超出 v2.3.0 合并范围）。
- 保留 gravity：`init_genesis<PF>` 签名与 trait bound、`insert_world_trie(&provider_rw, alloc.iter())?` 调用、`reth_trie_parallel::nested_hash::NestedStateRoot` import、`info!("Genesis already written, skipping.")`（`ff103f976a`）。
- **Cherry-pick** 上游的 `let checkpoint = StageCheckpoint::new(genesis_block_number); for stage in StageId::ALL { provider_rw.save_stage_checkpoint(stage, checkpoint)?; }`（替代 gravity 现有的 `Default::default()`）— 这修复非零 genesis 的 stage checkpoint bug，与 storage-v2 trait 集合无耦合，`StageCheckpoint::new(N)` 在 gravity 已可用。
- **不**采纳：`STORAGE_COMMIT_THRESHOLD`、`STATE_ROOT_COMMIT_THRESHOLD`、`init_genesis_with_settings*`、`MetadataProvider`/`StorageSettings` trait bound、`expected_block_start` static-file 预写、`pub use reth_provider::init::*` re-export、`AddressMap`/`B256Map`/`B256Set` import、`SealedHeader` 路径。
- 留**开放问题**：上游 init-state OOM 缓解（`STATE_ROOT_COMMIT_THRESHOLD`、prefix-set drop）是否需 port 进 gravity 的 rocksdb init 路径，待 state-root team 评估。

---

### `crates/storage/db-models/src/accounts.rs`

**模块：** `AccountBeforeTx` 存储布局 + subkey trait 实现
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `677d07041` (#23186, refactor: use reth-core deps) 引入新 trait `ValueWithSubKey { type SubKey = Address; fn get_subkey(&self) -> Self::SubKey { self.address } }`（替换上游路径上的旧 `SubkeyContainedValue`）。
- 在文件尾追加 `#[cfg(any(test, feature = "reth-codec"))] reth_codecs::impl_compression_for_compact!(AccountBeforeTx);`。

**Gravity 侧变更：**
- baseline 沿用 `SubkeyContainedValue { fn subkey_length(&self) -> Option<usize> { Some(20) } }` — 这是 gravity rocksdb 后端用于子键前缀长度查询的 trait。其他 baseline 使用方包括 `crates/primitives-traits/src/lib.rs`、`crates/primitives-traits/src/storage.rs`、`crates/storage/db-api/src/models/mod.rs`、`crates/trie/common/src/nested_trie/node.rs`、`crates/trie/common/src/storage.rs`。
- baseline 文件尾**没有** `impl_compression_for_compact!`。

**影响范围：** trait API 分裂。`SubkeyContainedValue::subkey_length()` 返回字节数（rocksdb iterator 前缀），`ValueWithSubKey::get_subkey()` 返回类型化 `Address`（MDBX dup-table 用）。移除 `SubkeyContainedValue` 会断掉 gravity rocksdb 后端 6 处使用点。

**解决方案建议：** **keep-gravity**（外加上游 `impl_compression_for_compact!` 行）
**理由：**
- `SubkeyContainedValue` 是 baseline `a1d7365bd6` 引入的 rocksdb 子键长度查询 trait；移除会破坏 rocksdb 后端，不在本合并范围内可补偿。
- 上游 `ValueWithSubKey` 服务于 MDBX 路径（v2.3.0 `crates/storage/db-api/src/tables/mod.rs`、`crates/cli/commands/src/db/get.rs` 等），与 gravity rocksdb 路径不冲突。两份 impl 可在同一 struct 上并存（trait 名不同）。本文件**同时**实现 `SubkeyContainedValue`（gravity）与 `ValueWithSubKey`（上游 v2.3.0）以保持双侧调用方可用 — 这是机械叠加，无语义冲突。
- 采纳上游尾部 `#[cfg(any(test, feature = "reth-codec"))] reth_codecs::impl_compression_for_compact!(AccountBeforeTx);` — 纯叠加，启用 db-api codec 测试所需 Compact-compression 钩子；上游 worktree 已合入该行，verify via `tail crates/storage/db-models/src/accounts.rs`。

---

## 分组解决手册

按顺序执行；每步确保下游可干净编译：

1. `crates/storage/libmdbx-rs/Cargo.toml` — `crossbeam-queue` 接纳、删 `cursor`/`transaction` `[[bench]]`、保留 `mdbx_bench_tool` + criterion/rand dev-deps。
2. `crates/storage/libmdbx-rs/src/environment.rs` — 全部 take-upstream（`txn_pool::ReadTxnPool` import、`mdbx_stat` 重命名 + doc 修复）。
3. `crates/storage/db/Cargo.toml` — gravity 主体；叠加上游 `mdbx`-feature 依赖（`quanta`、`reth-metrics`、`strum`、`rustc-hash`）+ `tracing` + unix `libc`；丢 `op` feature 行与 `criterion` dev-dep；保留 3 个 `[[bench]]` 与 `failpoints`/`fail`。
4. `crates/storage/db/src/metrics.rs` — 整体 take-upstream（`quanta::Instant` + `TableOperationMetrics` 重构）。
5. `crates/storage/db/src/implementation/mdbx/cursor.rs` — gravity `drop_fn`/`Debug`/`Drop`/`#[allow(clippy::type_complexity)]` + 上游 `metrics: Option<TableOperationMetrics>` 字段类型。
6. `crates/storage/db/src/implementation/mdbx/mod.rs` — gravity `ParallelTxRO`/`sync_mode: Option<SyncMode>`/`with_sync_mode(Option<...>)`/`Arc<DatabaseEnv>` 测试 helper + 上游 `path`/`with_metrics_if`/`drop_orphan_table`/`DatabaseArguments::test`/`with_geometry_page_size`。
7. `crates/storage/db/src/lib.rs` — keep-gravity（rocksdb 默认分发轴）；采纳上游 `format!("{ERROR_DB_CREATION}: {path:?}")`；`is_database_empty`/`enable_legacy_multiopen` 仅在 `#[cfg(feature = "mdbx")]` 下保留。
8. `crates/storage/db-models/src/accounts.rs` — 同时实现 `SubkeyContainedValue`（gravity）与 `ValueWithSubKey`（上游）；接纳尾部 `impl_compression_for_compact!`。
9. `crates/storage/db-common/Cargo.toml` — 保留 `reth-trie-parallel` 依赖 + gravity dev-deps（不带 `mdbx` feature）+ `alloy-consensus`；加入上游 `reth-tasks` dev-dep。
10. `crates/storage/db-common/src/init.rs` — 整体 keep-gravity；cherry-pick `StageCheckpoint::new(genesis_block_number)`。

**完成后校验：**
- `cargo check -p reth-db --no-default-features --features mdbx`（mdbx feature 入口仍能编）。
- `cargo check -p reth-db --no-default-features --features rocksdb`（gravity 默认路径）。
- `cargo check -p reth-libmdbx`。
- `cargo check -p reth-db-common`。
- `cargo +nightly fmt --all`。

---

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [x] 1. **`ValueWithSubKey` 与 `SubkeyContainedValue` 并存** — 双 trait 同 struct 实现在本组解决；db-api 组（同一批文件中另含 `crates/storage/db-api/src/tables/mod.rs` 等）是否计划长期保留二者，或迁 gravity rocksdb 到 `ValueWithSubKey` 并废 `SubkeyContainedValue`？**Owner：db-api 组解析者。** → **决策**(⟲ f89d9d4e23 实际解法,与原建议不同):**不并存**,单轨 gravity `SubkeyContainedValue`;上游 `ValueWithSubKey` 留在孤儿 `db-models/src/storage.rs`,并存/迁移列 v2.4+ 债务。
   - [x] 冲突解决:accounts.rs / table.rs / models/mod.rs 冲突归零且与 baseline 逐字节一致(f89d9d4e23,实测);编译证据待 cargo workspace 依赖修复后回填。⚠ 链接前置:`SubkeyContainedValue` 定义待 primitives-traits 组恢复(见 ⟲ 节遗留断点 3)。

- [x] 2. **rocksdb 后端的 `DatabaseArguments::test()` stub** — 上游 `create_test_rw_db_with_datadir` 依赖该 helper。是否在 `crates/storage/db/src/implementation/rocksdb/` 上添加 stub 让上游测试可跨编译？**Owner：rocksdb 维护者。** → **决策**(f89d9d4e23 后前提消失即结):`create_test_rw_db_with_datadir` / `DatabaseArguments::test()` 均未引入(lib.rs/test_utils 还原 baseline),本轮无需 stub;v2.4+ 采上游测试基建时再议。
   - [x] 冲突解决:无落地动作需要(前提已消失,f89d9d4e23 实测)。

- [x] 3. **测试 helper 返回类型** — 上游 `create_test_db(kind) -> (TempDir, DatabaseEnv)` 修了 tempdir 清理竞态；gravity 仍用 `Arc<DatabaseEnv>` + `keep()`（泄漏目录）。是否长期采纳上游形式？**推迟，超本次合并范围。** → **决策**:维持推迟;f89d9d4e23 保留 gravity 形式(mdbx/mod.rs 与 baseline 逐字节一致,实测)。
   - [x] 冲突解决:mdbx/mod.rs 冲突归零(f89d9d4e23,实测);编译证据待 cargo workspace 依赖修复后回填。

- [x] 4. **`NestedStateRoot` 与上游 init-state 增量写** — 上游 `b9969c5b1` 重构 state-root 流程（按 `STATE_ROOT_COMMIT_THRESHOLD` 增量写 trie 进度）。是否与 gravity 的 `NestedStateRoot` 路径有耦合？**Owner：state-root team。** → **决策**(f89d9d4e23 既成):init.rs 整体 keep-gravity,上游增量写/OOM 缓解未采;**本文原建议的 `StageCheckpoint::new(genesis_block_number)` cherry-pick 也未做**(:158 实测仍 `Default::default()`),非零 genesis 支持列 v2.4+ 评估(TODO 跨 crate 条目 5)。
   - [x] 冲突解决:init.rs 23 处冲突归零且与 baseline 逐字节一致(f89d9d4e23,实测);编译证据待 cargo workspace 依赖修复后回填。

- [x] 5. **Storage-v2 trait 基础设施** — `MetadataProvider`/`MetadataWriter`/`StorageSettings`/`StorageSettingsCache`/`RocksDBProviderFactory`/`NodePrimitivesProvider`/`StateWriteConfig` 在 gravity provider crate 中尚未实现。本次明确**不**port；是否计划长期跟进，或在 gravity 上"drop storage-v2 import pathway"？**Owner：存储架构师。** → **决策**(TODO 文档明示 + 实测确认):本轮不 port;这批符号在 storage 编译树内已不存在(实测 grep 仅命中孤儿文件),下游各组解冲突须对齐 gravity API;长期跟进列 v2.4+ 债务。
   - [x] 冲突解决:无冲突落地动作(策略性不采纳);孤儿清单见 ⟲ 节(f89d9d4e23,实测)。

- [x] 6. **Cursor `Debug` 与断言形式** — 上游测试已从 `assert_eq!(cursor.current(), Ok(Some(...)))` 切换为 `assert!(cursor.current().unwrap().is_some())`。gravity 手写 `Debug` 仍支持前者，本次解决在 mdbx/mod.rs 保留前者；合并后须 `rg 'unwrap\(\)\.is_some\(\)' crates/storage/db/src/implementation/mdbx/mod.rs` 验证无误吸入。→ **决策**:保留 gravity `assert_eq!` 形式(mdbx/mod.rs = baseline)。
   - [x] 冲突解决:28 处冲突归零;断言核验已跑,`unwrap().is_some()` 命中 0,无上游形式误吸入(f89d9d4e23,实测);编译证据待 cargo workspace 依赖修复后回填。

- [x] 7. **`crates/storage/db/src/utils.rs`** — 上游新增 `is_database_empty`；本组未列入冲突清单，但 `lib.rs` 引用它。若它在 worktree 已存在则附加 OK；若是新文件需 git status 交叉确认。**Owner：与处理 `utils.rs` 的 worker 对齐。** → **决策**(f89d9d4e23 既成):lib.rs 还原 baseline 后**不引用** `utils.rs`,该文件为孤儿(无 `mod` 声明,实测),无害保留、防误引用。
   - [x] 冲突解决:无落地动作需要;孤儿状态实测确认(f89d9d4e23)。

- [x] 8. **`op` feature 直接 trim 的级联** — `db/Cargo.toml` 丢弃 `op = ["reth-db-api/op", "reth-primitives-traits/op"]` 后，下游若有 `--features op` 编译路径需同步 trim（规则 1）。⟲ f89d9d4e23 **未执行本条**:db/Cargo.toml:105 该行保留(实测),而 db-api(v2.3.0 侧 + surgical 修补)已无 `op` feature → **cargo feature 解析期断点**(不需 `--features op` 即触发;当前被 workspace 根缺 ~20 个 dep 的更大错误掩盖,见 ⟲ 节遗留断点 1)。修法维持原建议:删除该行。
   - [x] 冲突解决:已落地(2026-07-06,cargo 组):该行已删,`cargo metadata` exit=0 + `cargo check -p reth-db` 通过为证(6a54e53528)。⟲ 遗留断点 1/2 同批关闭:libmdbx-rs 恢复 `[[bench]] mdbx_bench_tool` + criterion/rand dev-deps(baseline 形态);连带主会话修复 db static_file/mod.rs 两处 `.copied()`(v2.3.0 range 值语义)。
