# node-builder & ethereum-node 冲突分析

## 分组概要

本组共 13 个冲突文件，覆盖 `reth-node-builder` / `reth-node-core` / `reth-ethereum-node` / `reth-ethereum-cli` 四个 crate，主要类别：

- **Cargo.toml × 3**：feature 矩阵冲突。v2.3.0 在多个 dep 上加 feature gate（`reth-db [mdbx]`、`reth-tracing [std]`、`reth-engine-primitives [std]`、`reth-evm-ethereum [std]`、`alloy-rpc-types-engine [ssz/jwt-aws-lc-rs]`、`revm [memory_limit, p256-aws-lc-rs]`），并新增 `trie-debug`、`keccak-cache-global`、`otlp/otlp-logs/tracy`、`failpoints` 等 feature；baseline 侧叠加 `gravity-primitives`、`reth-engine-service` dep 以及 gravity-only 的 `op` / `failpoints` feature 集。
- **`args/*.rs` × 5**：CLI 参数模块。v2.3.0 引入 `Default*Values` 全局 OnceLock 模式（PR #20136、#20142、#22801、#23122、#24145、#19562、#23120 等），并彻底重写 `database.rs`（MDBX `mdbx::DatabaseArguments` + `--db.page-size` / `--db.sync-mode` / `--db.balstore-cache-size` / `--db.disable-metrics`）。baseline 侧 `database.rs` 已被 `a1d7365bd6 feat(rocksdb): Integrating RocksDB into Reth (#212)` 完全替换为 RocksDB `DatabaseArguments` + `--db.block-cache-size` / `--db.sharding-directories` 等参数。`args/mod.rs` 再加 `GravityArgs`、`RessArgs`。
- **`node_config.rs`**：`NodeConfig` 字段冲突。v2.3.0 加 `static_files`、`storage`、`MetricArgs`，并将 `total_difficulty` 在 head 推断中固定为 `U256::ZERO`；baseline 加 `gravity: GravityArgs` 字段并保留 `total_difficulty` 从 `header_td_by_number` 读取。
- **`launch/{common,engine}.rs`**：启动流程冲突。v2.3.0 引入 `ChangesetCache`、`RocksDBProvider`、`StorageSettingsCache`、`disabled_stages`、`StorageSettingsInfo`、`init_genesis_with_settings_and_validate`、`PruneConfigKind`、`build_engine_orchestrator`（替代已删除的 `reth-engine-service` crate）等；baseline 侧引入 `gravity_primitives::get_gravity_config`、`StorageRecoveryHelper`，并保留旧 `EngineService`-based 启动路径。
- **`ethereum/node/src/node.rs`**：上游加 `RethAuthHttpMiddleware`、`Stack`、`Either`、`TestingApi`、新 `EngineShutdown`，并把 `RpcAddOns` 泛型扩展加 `AuthHttpMiddleware`；同时 `RpcConvert` trait 把 `Spec/TxEnv` 拆开。baseline 上 #337 `feat(fee): chainspec floor with code-driven activation schedule` 触及该文件，需要保留。
- **`ethereum/cli/src/interface.rs`**：状态 `AA`（两侧都新增了同一路径的不兼容版本）。v2.3.0 大改 logger 初始化（`TracingGuards`、`Layers`、`OtlpInitStatus`、`OtlpLogsStatus`、`TraceArgs`），并删除 `run_commands_with` 入口、加 `download::manifest_cmd`、把 `CliHeader` 重命名为 `HeaderMut`。baseline 上没有任何 gravity-only commit 触及此文件（仅 v1.8.3 catch-up 中转手）。

| 解决方案分布 | 数量 |
|---|---|
| take-upstream | 3 |
| mechanical-merge | 5 |
| keep-gravity | 5 |
| needs-port | 0 |

## 逐文件分析

### `crates/node/builder/Cargo.toml`

- **模块**：`reth-node-builder` crate manifest
- **冲突类型**：UU
- **上游变更**：v2.3.0 在 `reth-db` 上加 `["mdbx"]` feature、`reth-tasks` 加 `["rayon"]`、`reth-tracing` 加 `["std"]`、新增 `reth-trie-db [metrics]` dep 与 `parking_lot` dep；删除 `reth-engine-service` 依赖（v2.3.0 的 `1f3fd5da2 refactor(engine): remove reth-engine-service crate (#22187)`）；`js-tracer` feature 扩展为转发到 `reth-node-ethereum/js-tracer` 与 `reth-rpc-eth-types/js-tracer`；新增 `trie-debug` feature 转发到 `reth-engine-tree/trie-debug`、`reth-engine-primitives/trie-debug`、`reth-node-core/trie-debug`；`test-utils` 加 `reth-trie-db/test-utils` 与 `reth-tasks/test-utils`。
- **Gravity 侧变更**：baseline 的 `0cb1687c1c` 上保留 `reth-engine-service.workspace = true`（v2.3.0 已删除该 crate）、新增 `gravity-primitives.workspace = true`、以及 gravity-only `op` feature 集（`reth-db/op`、`reth-db-api/op`、`reth-engine-local/op`、`reth-evm/op`、`reth-primitives-traits/op`）与 `failpoints` feature 集。这些来自 v1.8.3 catch-up（`d620fd0eeb`）落地的状态，没有 gravity-only commit 触及。
- **影响范围**：编译期。`reth-engine-service` 在 v2.3.0 已被 `reth-engine-tree::launch::build_engine_orchestrator` 取代——`engine.rs` 段必须同步切换，不可只保留 dep 而代码用旧路径。
- **解决方案建议**：mechanical-merge。
  - 删除 `reth-engine-service` dep（与上游对齐）。
  - 把 `reth-db` 改为 `{ workspace = true, features = ["mdbx"] }`（gravity 的 RocksDB 集成不替换 `reth-db` crate 本身，只在 `reth-storage-api`/`reth-provider` 上扩展 `RocksDBProvider`，所以 `mdbx` feature 仍然需要）。
  - `reth-tasks [rayon]`、`reth-tracing [std]`、`reth-trie-db [metrics]`、`parking_lot` 全部 take-upstream。
  - 保留 `gravity-primitives` dep。
  - `js-tracer` feature 取上游扩展形式。
  - `test-utils` 合并：保留 gravity 的 `reth-evm-ethereum/test-utils`、`reth-node-ethereum/test-utils`、`reth-primitives-traits/test-utils`，并加上 v2.3.0 的 `reth-trie-db/test-utils`、`reth-tasks/test-utils`。
  - 新增 `trie-debug` feature 取上游版本。
  - 保留 gravity-only `op` 与 `failpoints` feature 集。
- **推理**：依赖矩阵冲突没有语义冲突，按 union 处理。`reth-engine-service` 必须删除，因为 upstream `22187` 整 crate 删除，留下会立刻找不到 crate。

### `crates/node/core/Cargo.toml`

- **模块**：`reth-node-core` crate manifest
- **冲突类型**：UU
- **上游变更**：`reth-db [mdbx]`、`reth-tracing [std]`、`alloy-rpc-types-engine` 把 `jwt` 升级为 `jwt-aws-lc-rs`、新增 `reth-tasks` dep、新增 `ipnet` dep、新增 `reth-tracing-otlp` dep、新增 `keccak-cache-global` / `otlp` / `otlp-logs` / `tracy` feature；删除 `shellexpand` dep（PR #22514）；feature gate 注释从 `# tracing` 改为 `# obs`。
- **Gravity 侧变更**：baseline 保留 `shellexpand`、`gravity-primitives` dep；保留 `alloy-rpc-types-engine` 的 `jwt` feature；保留 `dirs-next`；没有 gravity-only commit 触及。
- **影响范围**：编译期。
- **解决方案建议**：mechanical-merge。
  - `reth-db [mdbx]`、`reth-tracing [std]` take-upstream。
  - `alloy-rpc-types-engine` feature 取 v2.3.0 的 `jwt-aws-lc-rs`（aws-lc 后端，p256 等加密原语改用 AWS-LC 实现）。
  - 新增 `reth-tasks`、`ipnet`、`reth-tracing-otlp` deps 与 `keccak-cache-global` / `otlp` / `otlp-logs` / `tracy` features take-upstream。
  - 移除 `shellexpand`（gravity baseline 上保留是因为 catch-up 当时 v1.8.3 还在用，v2.3.0 已彻底删；但若 `args/database.rs` gravity-RocksDB 实现里有 path expansion 调用，要确认换成 v2.3.0 替代方案）。
  - 保留 `gravity-primitives` dep。
- **推理**：baseline 上 `shellexpand` 来自 v1.8.3 catch-up，并非 gravity 主动加入；v2.3.0 在 `22514 chore: remove unmaintained shellexpand dependency` 删除以避免 unmaintained 依赖。

### `crates/ethereum/node/Cargo.toml`

- **模块**：`reth-node-ethereum` crate manifest
- **冲突类型**：UU
- **上游变更**：`reth-ethereum-engine-primitives`、`reth-evm-ethereum`、`reth-engine-primitives` 加 `["std"]` feature；`alloy-rpc-types-engine` 加 `["ssz"]` feature（PR #23936 `feat(rpc): add a ssz proxy layer for engine-api methods`）；`revm` 加 `["memory_limit", "p256-aws-lc-rs"]` feature；`tokio` 加 `["sync"]`；新增 dev-deps `reth-testing-utils`、`reth-stages-types`、`tempfile`、`jsonrpsee-core`、`serde`、`enr [rust-secp256k1]`、`alloy-rpc-types-trace`、`similar-asserts`、`reqwest`、`reth-rpc-layer`；新增 `keccak-cache-global` feature 转发；`js-tracer` feature 转发扩展到 `reth-rpc`、`reth-rpc-eth-api`、`reth-rpc-eth-types`；`test-utils` 加 `reth-stages-types/test-utils`、`reth-tasks/test-utils`。
- **Gravity 侧变更**：baseline 加 `alloy-consensus.workspace = true` 与 `failpoints` feature 转发到 `reth-node-builder/failpoints`；没有 gravity-only commit 触及（来自 v1.8.3 catch-up 状态）。
- **影响范围**：编译期。
- **解决方案建议**：mechanical-merge。
  - 全部 upstream feature gate / 新 dep 直接 take-upstream。
  - 保留 baseline 的 `alloy-consensus.workspace = true` 与 `failpoints` feature。
- **推理**：所有 v2.3.0 feature 仅控制编译产物大小/能力，没有 chain-spec 语义冲突。

### `crates/node/core/src/args/database.rs`

- **模块**：CLI `--db.*` 参数解析与转换为 `DatabaseArguments`
- **冲突类型**：UU
- **上游变更**：v2.3.0 在 `database.rs` 上的改动集中在 #19533、#18945、#19594、#23701、#24002、#24806、#21208、#21225：保留 MDBX `mdbx::DatabaseArguments` 与 `mdbx::MaxReadTransactionDuration`、`mdbx::SyncMode`，加 `--db.page-size`、`--db.sync-mode`、`--db.read-transaction-timeout`、`--db.max-readers`、`--db.balstore-cache-size`、`--db.disable-metrics`、`--db.rocksdb-block-cache-size`、`--db.exclusive`、`--db.max-size`、`--db.growth-step`；导入 `reth_db::mdbx::{GIGABYTE, KILOBYTE, MEGABYTE, TERABYTE}` 常量。
- **Gravity 侧变更**：baseline 上由 `a1d7365bd6 feat(rocksdb): Integrating RocksDB into Reth (#212)` 完全替换：删除所有 MDBX 参数，引入 RocksDB 专属参数 `--db.block-cache-size`、`--db.write-buffer-size`、`--db.max-background-jobs`、`--db.max-open-files`、`--db.max-write-buffer-number`、`--db.compaction-readahead-size`、`--db.level0-file-num-compaction-trigger`、`--db.max-bytes-for-level-base`、`--db.bytes-per-sync`、`--db.sharding-directories`，并使用 baseline 内部定义的 `KILOBYTE/MEGABYTE/GIGABYTE` 常量。`DatabaseArguments` 来自 `reth_db::{DatabaseArguments, ShardingDirectories}`（gravity 在 `reth-db` 重新导出）。
- **影响范围**：CLI 表面。两边参数命名空间几乎完全互斥（除了共用的 `--db.log-level`）。
- **解决方案建议**：keep-gravity。
  - 整文件 take baseline 版本（gravity-RocksDB 专属 args）。
  - 上游 `--db.balstore-cache-size`、`--db.disable-metrics`、`--db.rocksdb-block-cache-size` 中后两者与 RocksDB 路径**有关**，应该再 review 是否需要 cherry-pick：
    - `--db.disable-metrics`：上游对 MDBX metrics 而言，gravity RocksDB 自己的 metrics 注册路径在 `reth-rocksdb`，需要单独决策（不在本文件范围）。
    - `--db.rocksdb-block-cache-size`：上游在 #23701 加入这个 flag 是因为 v2.3.0 把 RocksDB 引入上游主线（PR #20253），与 gravity 自有的 RocksDB 集成（PR #212）冲突。应该在 cross-rocksdb 合并任务里统一处理，本文件保留 gravity 实现。
- **推理**：gravity RocksDB 集成（#212）是性能关键路径（9000 TPS vs 900 TPS），整套 `--db.*` flag 都对接 `gravity_storage::RocksDBConfig`，丢掉等于回退到 MDBX。`reth_db::mdbx::DatabaseArguments` 在 baseline 已不被 default storage 路径使用。

### `crates/node/core/src/args/mod.rs`

- **模块**：`args` 子模块出口
- **冲突类型**：UU
- **上游变更**：v2.3.0 重命名 `BenchmarkArgs` 模块并删除（PR #24288 `chore(bench): remove reth-bench`）；新增 `static_files` 模块（`StaticFilesArgs`、`MINIMAL_BLOCKS_PER_FILE`，PR #19562）、`storage` 模块（`StorageArgs`、`DefaultStorageValues`，PR #22042 + #23120）、`trace` 模块（`TraceArgs`、`DefaultTraceValues`、`OtlpInitStatus`、`OtlpLogsStatus`，PR #21039 + #24145）、`metric` 模块（`MetricArgs`，PR #19243 + #20703）；为 `network`、`rpc_server`、`log`、`payload_builder`、`pruning`、`txpool`、`engine` 增加 `Default*` 配套 re-export；`pruning` 还导出 `PruneConfigKind`。
- **Gravity 侧变更**：`dfa14dcdea refactor: Add gravity configuration arguments (#168)` 引入 `gravity` 模块 + `GravityArgs`；v1.8.3 catch-up 引入 `engine`、`ress_args`、`era` 模块；仍留 `benchmark_args` 模块。
- **影响范围**：crate API 表面。下游引用 `BenchmarkArgs` 的代码需要全部清理。
- **解决方案建议**：mechanical-merge。
  - 删除 `benchmark_args` re-export（与上游对齐——`reth-bench` 已删）。
  - 加上 v2.3.0 的 `static_files`、`storage`、`trace`、`metric` 模块出口与 `Default*` 配套 re-export。
  - 保留 gravity 的 `gravity` 模块出口（`GravityArgs`）。
  - 保留 `ress_args` 模块（来自 v1.8.3）。
- **推理**：`BenchmarkArgs` 上游已无源文件，强行保留会编译失败。`GravityArgs` 与 v2.3.0 新模块名字空间不冲突，并列即可。

### `crates/node/core/src/args/network.rs`

- **模块**：`--network.*` / `--discovery.*` CLI args
- **冲突类型**：UU（30+ 个冲突 hunk，diff 1218 行）
- **上游变更**：v2.3.0 引入 `DefaultNetworkArgs` / `DefaultDiscoveryArgs` OnceLock 模式（#22801）；`Discv5` 默认 enabled（#23686）；新增 `--port-discovery`、`--enable-discv5-discovery`、`--persistent-peers-file`、ENR fork-id enforcement (#22013、#23477)；移除 `--bootnodes` 重复定义，重写 trusted nodes、NAT、DNS 解析逻辑（#19784、#20411、#24178、#24013）；session config 来自 config file 修复（#20484）；引入 max ETH message size 配置（#22668）；session/peer 持久化 metadata（#22557）。
- **Gravity 侧变更**：`git log d620fd0eeb..0cb1687c1c -- crates/node/core/src/args/network.rs` 为空——baseline 自 v1.8.3 catch-up 之后没有任何 gravity-only 修改。
- **影响范围**：CLI 表面 + 启动时网络配置。
- **解决方案建议**：take-upstream（整文件接受 v2.3.0 版本）。
  - 因为 baseline 自 catch-up 后未改，冲突来源就是 v1.8.3 vs v2.3.0 上游演进，gravity 没有特殊语义需要保留。
- **推理**：gravity 不实现独立的 p2p 协议（仅消费 reth p2p 同步），网络 CLI args 全套与 gravity 业务无耦合。Discv5 默认开启等行为变化由 chainspec 控制是否启用，不影响 gravity-chain。

### `crates/node/core/src/args/rpc_server.rs`

- **模块**：`--rpc.*` / `--http.*` / `--ws.*` / `--auth.*` CLI args
- **冲突类型**：UU（13 个冲突 hunk）
- **上游变更**：v2.3.0 引入 `DefaultRpcServerArgs` OnceLock 模式（#20312）；新增 `--rpc.evm-memory-limit`（#19279）；blocking IO semaphore（#20289）；historic proof permits 改 global（#20967）；`--testing.skip-invalid-transactions` 默认 true（#21603、#21094）；`rpc_proof_permits` 默认改用 global（#20967）；blob sidecar upcasting opt-in（#21624）；transaction hash caching（#21180）；BAL cache（#24037）；testing API (#24573)；debug verbosity/vmodule（#21497）；移除 `eth_callBundle` 特殊处理；新增 `--rpc.disable-metrics`（#24803）；eth simulate state root flag（#24564）；reth_newPayload 路径 revert（#22500）。
- **Gravity 侧变更**：baseline 自 v1.8.3 catch-up 后无 gravity-only commit 触及（早期 #81 `feat(server): Add config to control whether do http response compression or not` 的内容已并入 catch-up 状态）。
- **影响范围**：CLI 表面 + RPC server 配置。
- **解决方案建议**：take-upstream。
  - 与 `network.rs` 同理：baseline 无 gravity-only 改动，整文件接受 v2.3.0 版本。
- **推理**：RPC server 表面参数与 gravity 业务无耦合；gravity 自有 RPC（`rpc_gravity_*`）走独立 namespace，不通过此 args struct 配置。

### `crates/node/core/src/args/txpool.rs`

- **模块**：`--txpool.*` CLI args
- **冲突类型**：UU（8 个冲突 hunk）
- **上游变更**：v2.3.0 引入 `DefaultTxPoolValues` OnceLock 模式（#20136、#20142）；新增 `--txpool.disable-blobs-support`（#19559）；docs 修复（#24398、#21477）；`TxPoolArgs::Default` 实现改为从 `TXPOOL_DEFAULTS` global 读取；增加 `format_duration_as_secs_or_ms`、`builder::Resettable`。
- **Gravity 侧变更**：baseline 由 gravity-only commits `7d0483e565 feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)` 与 `364b851665 feat(fee): chainspec floor with code-driven activation schedule (#337)` 触及——但这两个 PR 主要改 `txpool/validate/eth.rs` 与 chainspec floor 注册器，对 `args/txpool.rs` 本身只是 import 调整（验证：`git log d620fd0eeb..0cb1687c1c -- crates/node/core/src/args/txpool.rs` 列出这两个 commit，需进一步 diff 检查具体改动）。
- **影响范围**：CLI 表面。`DefaultTxPoolValues` 注入点对 gravity 自定义 fee floor 有意义——如果 gravity 想把 50 Gwei minimum base fee 注册到 global default，应通过 `DefaultTxPoolValues::try_init` 而非硬编码。
- **解决方案建议**：mechanical-merge。
  - 接受 v2.3.0 的 `DefaultTxPoolValues` 结构。
  - 检查 gravity #335/#337 对本文件的实际改动：如果只是 import 调整，直接随上游；如果有 `with_disabled_protocol_base_fee`/`with_protocol_base_fee` 等 gravity-only API，保留 `impl TxPoolArgs` 上对应方法。
  - 注意 baseline 没有 `transactions_backup_path` / `disable_transactions_backup` / `max_batch_size` 等 v2.3.0 新字段，直接 take-upstream。
- **推理**：gravity fee floor 实现核心不在本 args 文件，本文件只承担 CLI 注入。

### `crates/node/core/src/node_config.rs`

- **模块**：`NodeConfig` 结构体
- **冲突类型**：UU
- **上游变更**：v2.3.0 加 `static_files: StaticFilesArgs`、`storage: StorageArgs` 字段；`metrics` 字段类型从 `Option<SocketAddr>` 改为 `MetricArgs`（PR #19243）；删除 `lookup_head` 里的 `total_difficulty` 读取（`refactor: e21048314 chore: remove total difficulty from HeaderProvider (#19151)`），把 `Head.total_difficulty` 固定为 `U256::ZERO`；新增 `with_disabled_discovery` helper；`DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB` 类型从 `u64` 改 `usize`；删除 `DEFAULT_MAX_PROOF_TASK_CONCURRENCY` 常量（PR #19171）；删除 `DEFAULT_PERSISTENCE_THRESHOLD` 本地常量（迁到 `reth-engine-primitives`）。
- **Gravity 侧变更**：baseline 加 `gravity: GravityArgs` 字段（#168）；`metrics: Option<SocketAddr>` 保留旧形式；保留 `total_difficulty` 从 `provider.header_td_by_number` 读取；保留 `DEFAULT_MAX_PROOF_TASK_CONCURRENCY` re-export 与本地 `DEFAULT_PERSISTENCE_THRESHOLD`。自 v1.8.3 catch-up 后无 gravity-only commit。
- **影响范围**：crate API + 启动配置。`Head.total_difficulty` 在 gravity 上是否仍被消费？需要检查 `pipe-exec-layer-ext` 与 gravity_storage——但 v2.3.0 上游已彻底不读 td，gravity-only 用 td 的路径如果存在也只在 gravity-internal 模块。
- **解决方案建议**：mechanical-merge。
  - 加 `static_files: StaticFilesArgs`、`storage: StorageArgs` 字段（take-upstream），同步 `with_components` / `Clone` impl。
  - 保留 `gravity: GravityArgs` 字段。
  - `metrics` 字段切换为 `MetricArgs`（take-upstream），但要核对 gravity-only 代码里 `config.metrics.as_ref()` 这种 `Option<SocketAddr>` 用法的所有 call sites 是否需要适配。
  - `lookup_head` 把 `total_difficulty` 改为 `U256::ZERO`（take-upstream）。如果 gravity 内部确实需要真实 td，应该在 gravity 自有路径单独读 `header_td_by_number`，不要修改 `Head` 语义。
  - 删除 `DEFAULT_MAX_PROOF_TASK_CONCURRENCY` re-export；`DEFAULT_PERSISTENCE_THRESHOLD` 从 `reth_engine_primitives` 取。
  - `DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB` 类型改 `usize`。
- **推理**：所有 v2.3.0 改动是上游随 engine-tree / metrics / total-difficulty 重构的级联，无 chain-spec 语义影响。gravity 只需多挂一个字段。

### `crates/node/builder/src/launch/common.rs`

- **模块**：`LaunchContext` 与启动通用辅助（pruning config 保存、genesis init、provider factory 构造）
- **冲突类型**：UU（30 个冲突 hunk）
- **上游变更**：v2.3.0 引入：`init_genesis_with_settings` 与 `init_genesis_with_settings_and_validate`（PR #23919 `feat(node, db-common): add --debug.skip-genesis-validation`）；`StorageSettingsCache`、`RocksDBProvider`、`RocksDBProviderFactory`、`StaticFileProviderBuilder`、`BalConfig`、`BalStoreHandle`、`InMemoryBalStore`（PR #24002 + #21191 + #20253）；`PruneConfigKind`、`save_pruning_config` 替换 `save_pruning_config_if_full_node`（PR #23919 + #23493 + #23703 + #23082）；`disabled_stages: Vec<StageId>` 注入到 pipeline（PR #24436）；`with_rocksdb_provider`（PR #22970）；`StorageSettingsInfo`（PR #24018）；`throttle` from `reth_tracing`；`ChangesetCache`（PR #20997）；`reserved_cpu_cores` 移除（PR #22221）；thread name shortening / zero-pad（PR #21751、#22113）；`--minimal` flag（#20960）。
- **Gravity 侧变更**：baseline 上 `24f03242db refactor(fmt): nighlty fmt the whole project (#220)` 与 `a1d7365bd6 feat(rocksdb): Integrating RocksDB into Reth (#212)` 触及。#212 加入 `gravity_primitives::get_gravity_config`、`StorageRecoveryHelper`、`reth_chainspec::EthereumHardfork` import；保留旧 `init_genesis`、旧 `save_pruning_config_if_full_node` 语义。
- **影响范围**：启动路径核心。处理不当会导致：(a) chain spec 变化时 genesis 校验缺失，(b) RocksDB provider 与上游新 `RocksDBProviderFactory` 冲突，(c) pruning config 持久化逻辑回归。
- **解决方案建议**：keep-gravity（主体）+ 重新 port 上游必要 hooks。
  - 保留 `gravity_primitives::get_gravity_config` 与 `StorageRecoveryHelper` 调用——这是 gravity RocksDB 持久化恢复路径的关键。
  - 保留 `init_genesis`（gravity-RocksDB 走的是自有 init 路径；不要切到 `init_genesis_with_settings_and_validate`，否则会触发 v2.3.0 storage settings 校验对 gravity RocksDB layout 不兼容）。
  - 保留 `save_pruning_config_if_full_node`（gravity baseline 沿用旧 semantic；#23703 trusted_nodes 合并修复需要 cherry-pick 但与本函数无关）。
  - **不要** import `RocksDBProvider` / `RocksDBProviderFactory` / `BalConfig` / `InMemoryBalStore` / `StorageSettingsCache` 等上游 RocksDB 路径符号——这些与 gravity 自有 RocksDB 路径冲突。
  - 上游 `disabled_stages` 注入（#24436）是上游 pipeline 通用能力，建议 port——`disabled_stages` Vec 默认空对 gravity 无影响，但保留扩展能力。
  - 上游 `throttle` log helper、thread name shortening 这些非语义改动 take-upstream。
  - `ChangesetCache` import——gravity 上 trie 路径走 `pipe-exec-layer-ext` 与 `parallel-storage`，与上游 in-memory changeset cache 没有直接对应；建议 **不引入**，待 trie/storage 分组合并时统一决策。
- **推理**：本文件是 gravity RocksDB 持久化路径的入口之一；任何替换 `init_genesis` 或加入 v2.3.0 `RocksDBProviderFactory` 的修改都会引起数据格式回归。`StorageRecoveryHelper` 是 gravity-only crash recovery 关键路径，必须保留。

### `crates/node/builder/src/launch/engine.rs`

- **模块**：`EngineNodeLauncher`（engine-driven launcher）
- **冲突类型**：UU（23 个冲突 hunk）
- **上游变更**：v2.3.0 用 `build_engine_orchestrator`（来自 `reth_engine_tree::launch`）替换原 `EngineService::new`（PR #22187 删除 `reth-engine-service` crate）；新增 `EngineShutdown` 类型（PR #22956 + #22698 graceful shutdown）；`launch_node` 签名加 `N: Node<RethFullAdapter<DB, N>>` 与 `DB: Database + DatabaseMetrics + Clone + Unpin + 'static` 泛型；引入 `ChangesetCache::new()` 与 `disabled_stages = N::disabled_stages()`；`with_provider_factory` 签名加 `(changeset_cache, rocksdb_provider, disabled_stages)` 三参数；`PruneConfigKind` import；`StorageSettingsCache` import；删除 gravity 的 `expire_pre_merge_transactions()` hook（gravity 也没有该 hook，是 v1.8.3 catch-up 引入）。
- **Gravity 侧变更**：baseline 上 `9974ad0618 fix(test): fix CI test of unit.yml (#241)` 仅加一行 import/use 调整。整体启动序列仍然走 `EngineService` 路径，并保留 `expire_pre_merge_transactions()` 调用、`with_provider_factory()` 无参数版本。
- **影响范围**：engine 启动核心路径。这里的合并决策直接决定 gravity 是否切换到 v2.3.0 的 orchestrator 架构。
- **解决方案建议**：mechanical-merge（被迫 take-upstream 主体 + keep gravity 切入点）。
  - **必须** 切到 `build_engine_orchestrator`：`reth-engine-service` crate 在 v2.3.0 已被删，无法保留 `EngineService::new` 调用。
  - `EngineShutdown`、`ChangesetCache`、`disabled_stages`、`launch_node` 泛型签名扩展全部 take-upstream。
  - `with_provider_factory(changeset_cache, rocksdb_provider, disabled_stages)` 三参数化 take-upstream，但 `rocksdb_provider` 参数对 gravity 是否传 `None`/适配 gravity-RocksDB 需要在 `common.rs` 合并完成后确认。
  - 保留 gravity 的 `expire_pre_merge_transactions()` 调用（如果 gravity 仍然需要 pre-merge expiry——但既然 gravity 不走以太坊主网，这其实是 noop；可以删除）。**建议**：删掉 gravity-only `expire_pre_merge_transactions()`（它本来就是 v1.8.3 catch-up 引入的上游 hook，gravity 没有自己加业务）。
  - `PruneConfigKind` import、`StorageSettingsCache` import take-upstream。
  - `EngineService::new(...)` 整段替换为 `build_engine_orchestrator(...)`，要确认 `pipeline` / `consensus_engine_stream` / `payload_builder` 等参数映射。
- **推理**：v2.3.0 拆掉 `reth-engine-service` crate 是结构性变更，gravity 无法保留旧路径。Orchestrator 架构本身是上游通用启动逻辑，与 gravity-chain 语义无关，take-upstream 是唯一可行路径。

### `crates/ethereum/node/src/node.rs`

- **模块**：`EthereumNode` 类型与 `EthereumAddOns`
- **冲突类型**：UU（26 个冲突 hunk）
- **上游变更**：v2.3.0 重命名/拆分 `RpcConvert` trait，把 `Spec` 与 `TxEnv` 拆出成单独泛型（移除 `EvmFactoryFor` / `SpecFor` / `TxEnvFor` 等 alias、改用 `N::Evm`）；新增 `RethAuthHttpMiddleware`、`Stack`、`Either`（PR #23579 `feat(rpc): expose auth HTTP transport middleware`）；新增 `TestingApi` / `TestingApiServer`（PR #20094 `feat: add support for testing_ rpc namespace`）；`RpcAddOns` 加 `AuthHttpMiddleware` 泛型参数；`provider_factory_builder` 例子加 `runtime` 参数（PR #21934 `feat: global runtime`）；删除 `EthPayloadBuilderAttributes` 关联类型约束（PR #23202 `refactor: remove PayloadBuilderAttributes`）。
- **Gravity 侧变更**：baseline 由 `364b851665 feat(fee): chainspec floor with code-driven activation schedule (#337)` 触及——主要改的是 fee floor 注册路径，对本文件应该只是 import 调整。其他改动均来自 v1.8.3 catch-up（保留 `EthPayloadBuilderAttributes`、`EvmFactoryFor`、`SpecFor`、`TxEnvFor` import）。
- **影响范围**：crate API + EthereumNode trait 实现。
- **解决方案建议**：mechanical-merge。
  - take-upstream `RpcConvert` trait 拆分（移除 `Spec/TxEnv` 关联类型，改用 `Evm = N::Evm`）。
  - take-upstream `RethAuthHttpMiddleware`、`Stack`、`Either`、`TestingApi/TestingApiServer` import 与 `RpcAddOns` 加泛型 `AuthHttpMiddleware`。
  - take-upstream `provider_factory_builder` doc example 改为 `runtime` 参数版本（gravity 自有 reth-bench/integration test 引用此方法的要同步改）。
  - take-upstream 删除 `EthPayloadBuilderAttributes` 关联类型约束。
  - 核查 #337 对本文件的实际改动（应该只是 import）——如果有 `EthereumNode::components` 里 fee floor 注入 hook，保留；否则随上游。
- **推理**：所有 v2.3.0 改动属于 RPC stack 演进（middleware/testing API）与 trait 重构，与 gravity 链语义无关。

### `crates/ethereum/cli/src/interface.rs`

- **模块**：`reth-ethereum-cli` 主入口
- **冲突类型**：AA（两边新增同路径不兼容版本——`git show 0cb1687c1c:<path>` 和 `git show v2.3.0:<path>` 都返回文件；AA 表示 merge-base 上该文件不存在或语义被认作 add/add——可能由 CLI 重组 commit 导致 git rename detection 失败）
- **上游变更**：v2.3.0 重写 logger 初始化（`TracingGuards` 替代 `FileWorkerGuard`、加 `Layers`、`OtlpInitStatus`、`OtlpLogsStatus`、`TraceArgs`）；删除 `run_commands_with` 入口（PR #21934 global runtime）；新增 `download::manifest_cmd`（PR #22246 modular snapshot downloads）；`CliComponentsBuilder` 重命名 `CliHeader` → `HeaderMut`；导入 `RethRpcModule`；新增 `warn` log；删除 `install_prometheus_recorder`（迁到上游 startup hook）。
- **Gravity 侧变更**：baseline 自 v1.8.3 catch-up 后无 gravity-only commit 触及。
- **影响范围**：CLI 主入口。
- **解决方案建议**：take-upstream。
  - 因 baseline 无 gravity-only 改动，整文件接受 v2.3.0 版本，并把所有 caller 改用 `CliApp::run` 新签名 + 新 `TracingGuards` / `Layers` 模型。
  - 注意：如果 gravity 自己有 `bin/reth/src/main.rs` 或 `gravity-reth-cli` 这种 wrapper 引用 `run_commands_with`，需要一并改写——但这不在本文件范围。
- **推理**：CLI 入口冲突纯属上游 logger 重构 + bench/download 命令重组；gravity 没有覆盖此入口的业务逻辑。

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [x] 1. **gravity-RocksDB 与 v2.3.0 upstream RocksDB 路径并存策略**：v2.3.0 通过 PR #20253 / #21191 / #22970 把 RocksDB 引入上游主线（`RocksDBProvider`、`RocksDBProviderFactory`、`with_rocksdb_provider`），同时 gravity 在 baseline 上有完全独立的 `gravity-storage` + `parallel-storage` RocksDB 实现（PR #212）。本组合并先按 keep-gravity 处理 `args/database.rs` / `launch/common.rs`，但跨 crate 的 storage 分组合并完成前，`reth-db` 的 `mdbx` feature 必须保留（gravity-RocksDB 不替换 `reth-db` crate 本身）。
   - → **决策**(⟲ f89d9d4e23,2026-07-05,依据决策总原则②「与 storage 决策冲突则迎合 storage」):**不并存——上游 RocksDB 路径已被 storage 决策整体出局,gravity RocksDB 独存**。实测证据(2026-07-05):①上游定义已删——`providers/rocksdb/provider.rs`(v2.3.0 侧 :427 `pub struct RocksDBProvider`)与 `traits/rocksdb_provider.rs`(:11 `pub trait RocksDBProviderFactory`)在 worktree 不存在,`RocksDBProvider`/`RocksDBProviderFactory`/`rocksdb_write_ctx`/`with_rocksdb_batch` 四符号全仓零定义;②该符号家族 100% 上游血统——`git grep RocksDBProvider 0cb1687c1c` 零命中(v2.3.0 侧 46 个文件);③gravity 后端已随 baseline 回归——`crates/storage/db/src/implementation/rocksdb/` 存在、`db/Cargo.toml:70` `default = ["rocksdb"]`。结论:node-builder 侧所有涉及上游 RocksDB 装配的冲突块一律解向 baseline/HEAD 侧。
   - [x] 冲突解决:**已落地(2026-07-06)**。launch/common.rs 30 块归零(8 块 RocksDB → HEAD;`create_provider_factory` 因公共区已被 v2.3.0 形态污染而**整函数按 baseline 重建**,保住 `get_gravity_config().disable_pipe_execution` + `StorageRecoveryHelper` 恢复路径;etl_path 启动清理段保留,现 :355;其余 22 块按文档解向 v2.3.0,均经符号存活实测);args/database.rs 8 块归零(**整文件 baseline 复原**——公共区被双侧 import 污染,逐块不如整文件稳);launch/engine.rs 23 块归零(RocksDB 块 → HEAD;⟲ **主体解向 v2.3.0 orchestrator 路径**——`reth-engine-service`/`EngineService` 已死、`build_engine_orchestrator`/`EngineShutdown`/`build_tree_validator` 等配套全部实测存活,纯 baseline 解不成立);侧翻断点 states.rs(7 点)/ mod.rs(8 点)按**局部摘除**修复(v2.3.0 增量含大量存活合法演进,整文件回基线反制造断点)。验收:`grep 'with_rocksdb_provider|RocksDBProviderFactory|RocksDBProvider\b' crates/node/` 归零。附带裁决:`metrics_hooks` 保留、仅删其 rocksdb hook;`cached_storage_settings()` 两处 → `node_config().storage_settings()`(孤儿 trait);`create_test_rw_db_with_datadir` → baseline `create_test_rw_db_with_path`。**余量(非本框)**:node/builder/Cargo.toml 与 node/core/Cargo.toml 仍在冲突(前者须删 `reth-engine-service` dep :32);args/mod.rs 零冲突丢 `mod gravity;` 导出已由主会话补回(2026-07-06)。
- [x] 2. **`Head.total_difficulty` 在 gravity 内部是否仍被消费**：v2.3.0 把 `lookup_head` 的 td 写死 `U256::ZERO`。若 gravity-only `pipe-exec-layer-ext` 或 `consensus_layer_handle` 路径读 `Head.total_difficulty`，需要在 gravity 侧改为直接 query `header_td_by_number`。建议在 `pipe-exec-layer-ext` 分组合并时一并核查。
   - → **决策**(2026-07-06,用户拍板):**gravity 内部不消费 `Head.total_difficulty`,采上游 v2.3.0 `lookup_head` td 写死 `U256::ZERO` 形态,无需 gravity 侧 `header_td_by_number` 改造**。实测支撑:crates/pipe-exec-layer-ext-v2/ 内 total_difficulty 零引用(2026-07-03);原「consensus_layer_handle 路径待补测」要求随本拍板撤销。
   - [x] 冲突解决:**已在位,无落地动作**(2026-07-06 实测):td 写死点在 `node_config.rs:412 lookup_head`(:434 `total_difficulty: U256::ZERO`),该文件冲突块归零、v2.3.0 形态已落盘;`launch/common.rs:999` 仅是委托 wrapper 且处公共区(非冲突块)。⟲ 勘误:原「随 OQ1 launch/common.rs 解块携带」表述有误——定义点不在 common.rs。
- [x] 3. **`expire_pre_merge_transactions()` 在 gravity 上是否仍有意义**：本身是上游以太坊主网 hook，gravity 链没有 merge 节点；建议在 `engine.rs` 合并时直接删除。
   - → **决策**(2026-07-06,用户拍板):**直接删除**。该 hook 是上游以太坊主网 merge 语义(v1.8.3 catch-up 引入),gravity 链无 merge 节点,保留无意义。
   - [x] 冲突解决:已落地(2026-07-06):`expire_pre_merge_transactions` 定义(common.rs)与调用点(engine.rs)全删,`grep -rn 'expire_pre_merge' crates/` 归零(exit=1,主会话复测同零);engine.rs 23 块随 OQ1 落地一并归零。
- [x] 4. **`--db.rocksdb-block-cache-size` 与 `--db.disable-metrics`**：v2.3.0 上游 RocksDB 引入的 flag，如果未来 gravity-RocksDB 与上游 RocksDB 合并，需要把这两个 flag 接到 `gravity-storage::RocksDBConfig` 上。当前合并保留 gravity 命名空间（`--db.block-cache-size`），但要在 storage 分组任务里留 issue。
   - → **决策**(⟲ f89d9d4e23,2026-07-05,随开放问题 1 顺带裁决):**本轮不接入上游两 flag,保留 gravity 命名空间(`--db.block-cache-size` 系)**。依据:两 flag 的接线目标(上游 RocksDB 路径/`BalStore`)已随 storage 决策整体出局(问题 1 证据①②),"未来与上游 RocksDB 合并"的前提顺延至 v2.4+ 再合并周期——届时若重引上游 RocksDB 再开 issue,本轮不留。
   - [x] 冲突解决:已落地(2026-07-06,随 OQ1):args/database.rs 整文件 baseline 复原,`--db.rocksdb-block-cache-size`/`--db.disable-metrics`/`--db.balstore-cache-size` 均未接入(grep mdbx/balstore/disable_metrics 归零),gravity `--db.block-cache-size` 命名空间完整保留。
- [x] 5. **`#337 chainspec floor` 对 `args/txpool.rs` / `ethereum/node/src/node.rs` 的实际改动**：本次只通过 commit log 推断；落地时需要 `git show 364b851665 -- crates/node/core/src/args/txpool.rs crates/ethereum/node/src/node.rs` 拿到精确 diff，确认仅是 import / minor adapter 改动，不漏 fee floor 关键 hook。
   - → **决策**(2026-07-07,A 阶段拍板):上游 #337(SHA `364b851665`,已在 gravity mainline)是三段设计——恢复上游默认 fee(chainspec/consensus/evm)、恢复 upstream txpool `MIN_PROTOCOL_BASE_FEE` 默认、注入 gravity chainspec-floor schedule(`ChainSpec.gravity_min_base_fee[_activation_block]` + `EthChainSpec::gravity_min_base_fee_at_block` + `EthereumPoolBuilder::build_pool` 的 `.max(floor)` 抬 pool floor)。txpool.rs 双侧 `MIN_PROTOCOL_BASE_FEE` 已一致(**fee-floor 走 pool builder 动态路径**);node.rs 25 块纯 API adapter 采 v2.3.0;N26 `build_pool` 必须**手写混合体**——v2.3.0 新签名骨架(`evm_config: Evm` + `.eth_builder(provider, evm_config)` + `blobs_disabled` + `.set_eip4844(!blobs_disabled)` + `.spawn_blocking_task`,移除 `.with_head_timestamp`)+ **保留 gravity fee-floor 4 行注入**(`mut pool_config` + `next_block = head+1` + `gravity_min_base_fee_at_block(next_block)` + `minimal_protocol_basefee.max(floor)`)。chainspec `spec.rs` `map_header` E0063/E0027 连坐:`Self` 解构与 `ChainSpec` 重建各补 `gravity_hardforks`/`gravity_min_base_fee`/`gravity_min_base_fee_activation_block` 3 字段(透传,不用 `..Default::default()`)。
   - [x] 冲突解决:**已落地(2026-07-07)**。txpool.rs 8 块全采 v2.3.0(纯 `DefaultTxPoolValues` refactor,两侧 `minimal_protocol_basefee` 默认均为 `MIN_PROTOCOL_BASE_FEE`,零 fee-floor 分歧);node.rs 25 块采 v2.3.0 adapter + N26 `build_pool` 手写混合体(v2.3.0 新签名骨架 + gravity fee-floor 4 行 `mut pool_config` 注入);chainspec spec.rs `map_header` 补 3 gravity 字段透传。工作树 `grep -c '^<<<<<<<' crates/node/core/src/args/txpool.rs crates/ethereum/node/src/node.rs` = 0/0;`cargo +nightly check -p reth-chainspec` 全绿(Finished dev profile);`reth-node-core` / `reth-node-ethereum` 因 transitive 依赖 `reth-trie-sparse`(12 处 `SparseTrieErrorKind::BlindedNode` field-vs-tuple variant)与 `reth-execution-types`(5 处 `as_repr`/`from_repr`/E0107 struct-generic-args)在 workspace `--all-features --keep-going` 复测下未启动 `Checking` 阶段,该二 crate 属 Task #3 尾款、非本框改动 crate,本次三文件改动无新增自身错误面;另 `examples/custom-hardforks` 因新 `EthChainSpec::gravity_hardforks` trait 方法缺 impl 触发 E0046(下游 example 修复,非本框)。
