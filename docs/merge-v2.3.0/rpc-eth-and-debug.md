# rpc-eth-and-debug

**分支**: `merge-v2.3.0`。HEAD: `e6b7e5ba32`。
Baseline: gravity-reth `0cb1687c1c` on `main` (含 `d620fd0eeb feat: merge reth-v1.8.3 (#205)`)
Target: reth `v2.3.0` (`9384bc53d8`)
Upstream diff range: `v1.8.3..v2.3.0` (覆盖 v1.9/v1.10/v1.11/v1.12 直至 v2.3.0 全部 release)

## 分组概要

- 文件数: 13，全部 `UU`
- 复杂度: **高** — `rpc-builder/src/lib.rs` 49 hunks, `rpc/src/debug.rs` 41 hunks, `helpers/trace.rs` 21 hunks, `helpers/transaction.rs` 21 hunks, `error/mod.rs` 14 hunks, `helpers/state.rs` 10 hunks, `cache/db.rs` 9 hunks, `core.rs` 8 hunks, `rpc-api/src/debug.rs` 8 hunks, `rpc/Cargo.toml` 6 hunks, `rpc/src/eth/helpers/transaction.rs` 6 hunks, `helpers/receipt.rs` 4 hunks, `rpc-builder/src/config.rs` 4 hunks
- 上游主干 PR 驱动（`v1.8.3..v2.3.0`）：
  - PR #22052 `refactor(tasks): remove TaskSpawner trait in favor of concrete Runtime` — `Box<dyn TaskSpawner>` → `Runtime` 全链路扩散
  - PR #22795 `accept Recovered<Tx> in build_transaction_receipt`
  - PR #23074 `avoid redundant receipt cache lookup` — `build_transaction_receipt` 新增 `Option<Arc<Vec<ProviderReceipt>>>` 参数
  - PR #23330 / #24037 / #24080 / #24281 — BAL (EIP-7928) eth_/debug_ 端点 + `BalError`/`EvmDatabaseError`
  - PR #24184 `manage state trie overlays centrally` — `CacheDB::new(StateProviderDatabase)` 一律替换为 `State::builder().with_database(...).build()`
  - PR #24284 `remove stale debug endpoints` — 上游删除 8 个 stub: `debug_cpu_profile/start_cpu_profile/stop_cpu_profile/start_go_trace/stop_go_trace/verbosity/vmodule/write_*_profile`
  - PR #24296 `add debug account state endpoints` — 新增 `debug_accountAt/accountInfoAt/accountRange/chainConfig/codeByHash`
  - PR #22719 `implement debug_traceBadBlock` — `DebugApi::new` 新增 `&Runtime` + `Stream<ConsensusEngineEvent>` 参数 + `BadBlockStore`
  - PR #19779 `MeteredBatchRequests(Future)` — 已被 gravity `#282 (2ef67318b3)` backport 引入；上游本体落地后改动一致
  - PR #19330 `replace CacheDB with State<DB>` + PR #19920 `simplify rpc state provider traits` — `StateProviderTraitObjWrapper` 去生命周期
  - PR #16348 `Add configuration option to enable/disable HTTP response compression` — 已在 v1.8.3 落地；gravity `#81 (976c3552a9)` 是同义平行实现
  - PR #23409 `support modifying next_available_nonce_for` — 将 `next_available_nonce` 重命名为 `next_available_nonce_for(req: &RpcTxReq)`
  - PR #24286 `eth_pendingTransactions` + 全套 `send_transaction(origin, WithEncoded<Recovered<…>>)` 重构 (#21969、#21624 blob upcasting)

- **gravity 侧必须保留的 baseline 改动**：
  - `c64bd613e4 (#225)` — `rpc/Cargo.toml` `failpoints` feature + `fail = 0.5`；`rpc-api/src/debug.rs` 中 `debug_setFailpoint` trait 方法；`rpc/src/debug.rs` 中对应 impl
  - `a0d11f2288 (#259)` — `helpers/transaction.rs::LoadTransaction::transaction_by_hash` 中的 `SYSTEM_CALLER` fallback（注：baseline `receipt.rs` 用 `Address::ZERO` fallback，**不是** `SYSTEM_CALLER`；该文件没有等价 gravity patch）
  - 部分 `c8e2080f02 (#177)` 的"忽略 signer recovery 失败"语义已经隐含在 baseline 上的 `try_to_recovered_ref_unchecked().unwrap_or_else(…ZERO)` / `recover_signer_unchecked().unwrap_or(SYSTEM_CALLER)` 中

- **gravity 侧 baseline 改动已被上游吸收或重写**（**不再保留 gravity 写法，采纳上游**）：
  - `2ef67318b3 (#282)` — backport 上游 #19779；上游本体已含相同 `pub use metrics::{MeteredBatchRequestsFuture, …}` 行
  - `976c3552a9 (#81)` — `http_disable_compression` 同义重叠：v1.4.3 起 reth 主线已有此开关（PR #16348），v1.8.3 baseline 已含
  - `061ceb7fdb (#97)` — pending-nonce 修复同义重叠：baseline 已使用 `get_highest_consecutive_transaction_by_sender`；上游 PR #23409 进一步把方法名改为 `next_available_nonce_for`
  - `7d0483e565 (#335)` — `error/mod.rs::From<PoolErrorKind>` 中的 `(_)` → `{ .. }` 一处微调（50 gwei base-fee 上下文中的语法对齐），上游已并入

- **解决顺序依赖**：
  1. `rpc-eth-types/src/cache/db.rs` 先于 `rpc-eth-api/src/helpers/trace.rs` —  `StateCacheDb`、`StateProviderTraitObjWrapper` 类型签名变化是 trace.rs 大量 hunk 的根源
  2. `rpc-eth-types/src/error/mod.rs` 先于 `helpers/*` — helpers 都依赖新增 `BlockAccessListNotAvailablePreAmsterdam`/`CallManyError` 变体
  3. `rpc-eth-api/src/core.rs` 先于 `rpc/src/debug.rs` + `rpc/src/eth/helpers/transaction.rs` — `EthApi` trait 新增 `RawTx: RpcObject` 第 6 个泛型 + 4 个 BAL 方法，下游 impl 必须同步
  4. `rpc-api/src/debug.rs` 先于 `rpc/src/debug.rs` — trait 删除 8 个 stub、新增 `executionWitnessByBlockHash(hash, mode)`/`account*`/`chainConfig`/`codeByHash`，impl 必须配套
  5. `rpc/Cargo.toml` 先于所有 — `failpoints` feature 与上游新增 `reth-ethereum-primitives` / `reth-ethereum-engine-primitives` / `serde` / `mnemonic` / `memory_limit` feature 都门控编译

## 逐文件分析

### `crates/rpc/rpc-api/src/debug.rs`

**模块**: `debug_` 命名空间的 trait 声明（`#[rpc]` 接口）。

**冲突类型**: `UU`（8 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #24284 `fa8851655 / 29d9241f6` — 删除 stub 方法：`debug_cpu_profile`、`debug_start_cpu_profile`、`debug_stop_cpu_profile`、`debug_start_go_trace`、`debug_stop_go_trace`、`debug_verbosity`、`debug_vmodule`、`debug_write_block_profile`、`debug_write_mem_profile`、`debug_write_mutex_profile`
- PR #24296 — 新增 `debug_accountAt`、`debug_accountInfoAt`、`debug_accountRange`、`debug_chainConfig`、`debug_codeByHash`（imports 新增 `Account`、`AccountInfo`、`Index`、`U64`）
- PR #22719 — `executionWitnessByBlockHash(hash, mode: Option<ExecutionWitnessMode>)` 新增 mode 参数
- PR #21678 — `debug_set_head` 参数改 `U64`
- PR #22289 — `ExecutionWitnessMode` 类型从 `reth_trie_common` 引入

**Gravity 侧变更**（baseline `0cb1687c1c` 相对 v1.8.3）：
- `c64bd613e4 (#225)` 在 `DebugApi` trait 末尾追加 `debug_setFailpoint(name: String, actions: String) -> RpcResult<()>` 方法 + cfg-feature `failpoints` 门控
- 此外 baseline 还保留 `DebugExecutionWitnessApi<Attributes>` trait（按照 baseline 历史并非 gravity 新增；属于早期上游遗留，待与 v2.3.0 对比确认是否仍存在）

**影响范围**: 任何实现 `DebugApiServer` 的 crate（`rpc/src/debug.rs`、op-rpc 等）。

**解决方案建议**: **mechanical-merge**
- 采纳上游：删除 8 个 stub、采纳新签名 `executionWitnessByBlockHash(hash, mode)`、采纳 5 个 `account*`/`chainConfig`/`codeByHash` 方法、采纳 `U64` 重命名
- 保留 gravity：`debug_setFailpoint` 方法（gravity-only，feature-gated）
- 保留 `DebugExecutionWitnessApi` 若上游仍存在；若被删除则取上游

**推理**: 上游删除即 net delete（PR #24284 motivation: 这些 stub 从未实现真实逻辑）。新增端点的 trait 方法本身对实现侧无侵入要求。`#225` 的 failpoint 方法是 gravity-only 测试基础设施。

---

### `crates/rpc/rpc-builder/src/config.rs`

**模块**: CLI args → `EthConfig` / `RpcServerConfig` / `EthStateCacheConfig` 的转换。

**冲突类型**: `UU`（4 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #20289 `2e567d665` — `max_blocking_io_requests` 新字段
- PR #24037 `316218c44` — `EthStateCacheConfig::max_bals` 新字段
- PR #19279 `dc8efbf9b` — `rpc_evm_memory_limit` 新字段
- PR #21624 `a9b2c1d45` — `rpc_force_blob_sidecar_upcasting` 新字段
- PR #24803 `4a36609e6` — `RpcServerConfig::with_rpc_metrics_enabled` + `--rpc.disable-metrics`
- PR #19855 `c75dc322d` — `--ws.api` 未启用 ws 时的 warn!
- PR #18729 `8852269a7` — WS CORS 独立于 HTTP 启用
- PR #21180 `ab685579f` — `max_cached_tx_hashes` 新字段

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `976c3552a9 (#81)` — `with_http_disable_compression(self.http_disable_compression)` + `--http.disable-compression` 开关；**注**：v1.8.3 主线已含 `with_http_disable_compression`（PR #16348），#81 仅追加 CLI args 透传

**影响范围**: CLI 启动路径、`reth_node_core::args::RpcServerArgs`。

**解决方案建议**: **mechanical-merge**
- 采纳上游：`max_blocking_io_requests`、`max_bals`、`rpc_evm_memory_limit`、`rpc_force_blob_sidecar_upcasting`、`with_rpc_metrics_enabled`、`--ws.api` warn、WS CORS、`max_cached_tx_hashes`
- 保留 gravity：`with_http_disable_compression(self.http_disable_compression)` 链式调用 + `ws_cors` 链式调用（gravity baseline 中 `http` 分支里同时设了 ws_cors）

**推理**: 4 个冲突 hunk 都是同一函数链式调用增删，机械合并即可。gravity 的 `http_disable_compression` CLI 透传仍需保留（CLI args struct 自身的字段在 `crates/node/core/src/args/rpc_server.rs`，本文件只是消费）。

---

### `crates/rpc/rpc-builder/src/lib.rs`

**模块**: `RpcModuleBuilder` / `RpcRegistryInner` — RPC 模块组装入口，全 RPC handler 实例化。

**冲突类型**: `UU`（49 处 marker；冲突量最大）

**上游变更** (`v1.8.3..v2.3.0`)：
- **PR #22052** `57148eac9` — `executor: Box<dyn TaskSpawner>` → `executor: Option<Runtime>`；`with_executor(Runtime)`；删除 `with_tokio_executor()`
- PR #22504 / #22425 / #22500 (`d3bb2faf2`, `dc35fc825`, `3931affcf`) — `RethEngineApi` 抽出为独立 struct；`build_with_auth_server` 新增 `engine_events: EventSender<ConsensusEngineEvent<N>>` 和 `beacon_engine_handle: ConsensusEngineHandle<Payload>` 参数
- PR #22397 `94818d767` — `reth_getBlockExecutionOutcome` 端点
- PR #22011 `198e457a1` — `subscribeFinalizedChainNotifications`
- PR #20877 `33bcd6034` — `PersistedBlockSubscriptions` provider bound
- PR #20094 `e90cfedf3` — `testing_` 命名空间 + `testing_buildBlockV1`
- PR #19779 `d66069deb` — `MeteredBatchRequests(Future)`（已添加 `pub use metrics::{MeteredBatchRequestsFuture, …}`）
- PR #19199 `08fc0a918` — `eth_fillTransaction`
- PR #20843 `412f39e22` — 移除 `Consensus::Error` 关联类型 → `Consensus: FullConsensus<N>` 不再带 `Error = ConsensusError`
- PR #20158 `56e60a370` — `merge_if_module_configured_with(closure)`
- PR #19266 `ddcfc8a44` — `add_or_replace_if_module_configured`
- PR #4a36609e6 — `with_rpc_metrics_enabled`
- PR #20036 `b3c00ed60` / #20943 `210309ca7` 等 — 多处文档 + 类型小修

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `2ef67318b3 (#282)` — `pub use metrics::{MeteredBatchRequestsFuture, MeteredRequestFuture, RpcRequestMetricsService}`（与上游 #19779 完全相同的一行 export）

**影响范围**: 全节点启动路径、`reth-node-builder`、`reth-node-ethereum`。

**解决方案建议**: **take-upstream**
- 一律取上游：`Runtime`-based executor、`Consensus<N>` 去除 `Error` 关联类型、`build_with_auth_server` 新签名、所有新增 provider bounds、所有新端点、所有 `pub use` re-exports
- `MeteredBatchRequestsFuture` re-export 上游本体已含，gravity `#282` 是 backport-overlap，取上游本体后该 export 已存在

**推理**: lib.rs 的核心 builder/registry 都被 PR #22052/#22425/#22504 等彻底重写，gravity baseline 没有任何与这些重构语义冲突的修改（`#282` 是平行重叠）。`Box<dyn TaskSpawner>` 在整个 codebase 都已被 `Runtime` 取代，强行保留 gravity 写法会阻断 trait bounds 链。

---

### `crates/rpc/rpc-eth-api/src/core.rs`

**模块**: `EthApi` trait 主体（`#[rpc(namespace = "eth")]`）+ `FullEthApiServer` blanket impl。

**冲突类型**: `UU`（8 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #23330 / #23363 / #23615 / #24050 / #24080 / #24281 / #24286 — **新增第 6 个泛型参数** `RawTx: RpcObject`，新增 4 个 BAL 方法（`block_access_list_by_block_hash/number/`、`block_access_list(block_id)`、`block_access_list_raw`）+ `eth_baseFee` + `eth_pendingTransactions`
- PR #24298 `27737df66` — `eth_capabilities`
- PR #19199 `08fc0a918` — `eth_fillTransaction` 方法
- PR #19564 `4d9d712b4` — `send_raw_transaction` 默认 impl
- PR #22186 `2e5560b44` — `eth_getStorageValues`
- PR #19980 / #18674 / #19890 — `RpcConvert` bounds 收紧；`FillTransaction` 引入
- PR #21720 `47ebc79c8` — `eth_getBalanceWithProof` / `eth_getAccountWithProof`
- `transaction_by_hash` impl 端将 `tx_resp_builder()` 改为 `converter()`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `9974ad0618 (#241)` 和 `605c372de6 (#237)` 仅触及编译/测试细节与 `eth_getProof nested hash` 内部辅助；trait 表面无 gravity-only 方法

**影响范围**: 所有 `EthApi` 的实现者（reth-rpc EthApi、op-rpc OpEthApi、gravity ethapi 等）；下游 `rpc/src/debug.rs` 中对 `transaction_by_hash` 的调用。

**解决方案建议**: **take-upstream**
- 一律取上游：新增 `RawTx: RpcObject` 泛型；新增 4 个 BAL 方法 + `eth_baseFee` + `eth_pendingTransactions` + `eth_capabilities`；`tx_resp_builder()` 全部改 `converter()`
- 下游所有 `EthApi<TxReq, T, B, R, H>` 必须扩展为 `EthApi<TxReq, T, B, R, H, RawTx>`

**推理**: gravity baseline 对 `EthApi` trait 表面没有保留性修改。上游变化是协议级新增方法 + 类型签名扩展，全部需要透传。

---

### `crates/rpc/rpc-eth-api/src/helpers/receipt.rs`

**模块**: `LoadReceipt` trait — `eth_getTransactionReceipt`/`eth_getBlockReceipts` 共享的 receipt 装配逻辑。

**冲突类型**: `UU`（4 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #22795 `bb12b72e7` — `build_transaction_receipt(tx)` 改为 `build_transaction_receipt(tx: Recovered<…>)`
- PR #23074 `7f12c9d99` — 再加 `all_receipts: Option<Arc<Vec<ProviderReceipt>>>` 参数，避免 receipts 二次 cache lookup
- `calculate_gas_used_and_next_log_index` 函数从 `helpers/receipt.rs` 移到 `reth_rpc_eth_types::utils`
- `tx_resp_builder()` → `converter()` 重命名
- trait bound 简化为 `EthApiTypes<RpcConvert: RpcConvert<Primitives = Self::Primitives>>`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- 仅有上游 catch-up commits（#205、#108、#144 等），无 gravity-only 改动
- 注意：baseline `receipt.rs` 中的 `unwrap_or_else(|_| Recovered::new_unchecked(&tx, Address::ZERO))` 是 v1.8.3 上游既存代码（PR #c8e2080f02 `#177` 触及的是其他文件），**不是** gravity-only patch；不需要"保留 SYSTEM_CALLER fallback"

**影响范围**: 所有 `LoadReceipt` 实现（reth-rpc、op-rpc、gravity ethapi）；下游 `build_transaction_receipt` 调用点全部加 `all_receipts` 第 4 参数。

**解决方案建议**: **take-upstream**
- 接受新签名 `build_transaction_receipt(tx: Recovered<ProviderTx>, meta, receipt, all_receipts: Option<Arc<Vec<…>>>)`
- 接受 `calculate_gas_used_and_next_log_index` 改成从 `reth_rpc_eth_types::utils` 导入
- 接受 `tx_resp_builder()` → `converter()` 全文件重命名
- 接受 trait bound 简化

**推理**: 此文件 baseline 没有 gravity-only 代码。所有改动都是上游性能优化 + 重命名。

---

### `crates/rpc/rpc-eth-api/src/helpers/state.rs`

**模块**: `EthState` / `LoadState` trait — `eth_getBalance/Storage/Code/Account/Proof/TransactionCount` 等。

**冲突类型**: `UU`（10 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #22552 `624fcbd34` — 抽出 `ensure_within_proof_window(block_id)` helper，把 `eth_getProof` / `get_account` 中的 max-window check 集中化
- PR #23409 `3d5057b97` — `next_available_nonce(addr)` 重命名为 `next_available_nonce_for(&request: &RpcTxReq)`，签名兼顾 7702 authorization-list 场景
- PR #22186 `2e5560b44` — `eth_getStorageValues` 批量读取 + `DEFAULT_MAX_STORAGE_VALUES_SLOTS` 常量
- PR #18685 `6a50aa3ea` — 关键 EVM/RPC 转换 fallible；`spawn_blocking_io_fut(move |this| async move {...})` 改为 `spawn_blocking_io_fut(async move |this| {...})`（async 闭包）
- PR #20691 `0c69e294c` / #20294 `a2a5e03cb` — `evm_env_for_header` helper + `sealed_header_by_id` 直接拉 header
- PR #23841 / #22747 — `sim_bundle` / `eth_call` 路径 evm-env 从 header 派生
- `get_account` 重写为 `async move { ensure_within_proof_window(); spawn_blocking_io_fut(…) }` 二层结构
- 新增 `get_account_info(addr, block_id) -> AccountInfo { balance, nonce, code }`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `061ceb7fdb (#97)` — 把 pending-tag 下的 `get_highest_transaction_by_sender` 改为 `get_highest_consecutive_transaction_by_sender`；**上游 PR #23409 同样使用 `get_highest_consecutive_transaction_by_sender`**，gravity #97 与上游修复语义重叠

**影响范围**: 所有 `EthState`/`LoadState` 实现；上游下游 caller 都要适配 `next_available_nonce_for(&req)` 新签名。

**解决方案建议**: **take-upstream**
- 接受 `ensure_within_proof_window` 抽出
- 接受 `next_available_nonce_for(&request)` 重命名（gravity `#97` 修复语义已被上游覆盖）
- 接受 `get_account_info`、`get_account` 重写
- 接受 `async move |this| {...}` 闭包语法
- 接受 `eth_getStorageValues`、`evm_env_for_header` / `sealed_header_by_id`

**推理**: gravity 的 `#97` 已被上游同义实现替换，没有 gravity-only 行为需要保留。新签名 `next_available_nonce_for(&req)` 引入需 `RpcTxReq` 参数，是为 EIP-7702 authorization-list 提供 nonce 估算入口；gravity 链上也需要正确处理 7702，不应回退。

---

### `crates/rpc/rpc-eth-api/src/helpers/trace.rs`

**模块**: `Trace` trait — `debug_trace*` / `trace_*` 命名空间共享的 inspect/replay 逻辑。

**冲突类型**: `UU`（21 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #19330 `50e88c29b` — `CacheDB::new(StateProviderDatabase::new(state))` 全部替换为 `State::builder().with_database(StateProviderDatabase::new(state)).build()`
- PR #19920 `ee63c7d6b` — 简化 RPC state provider trait — `StateCacheDbRefMutWrapper<'a, 'b>(&'b mut StateCacheDb<'a>)` 完全去除：所有签名改成裸 `&mut StateCacheDb`
- PR #23330 / #23534 — `EvmDatabaseError<ProviderError>`（BAL inspector 错误类型）取代裸 `ProviderError` 作为 `DB::Error`
- PR #22747 / #22726 — `evm_env_for_header(block.sealed_block().sealed_header())` 取代 `evm_env_at(block.hash().into()).await`
- PR #22333 `8fa539225` — 删除 `apply_pre_execution_changes` 在 `Trace` 中的副本（搬到 `BlockExecutor`）；`with_state_at_block(parent_hash, move |this, mut db|)` 新签名直接给 `&mut State<DB>` 而非 `state`
- `trait Trace: LoadState<Error: FromEvmError<Self::Evm>>` → `+ Call`（PR #22747 父 trait 增列约束）
- PR #23700 `378d4052e` — block timestamp 透传给 tx
- PR #18999 / #20627 — revm-v34 / EIP-7702 hook

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- 仅 catch-up commits（#205 / #144 / #131 / #108 / #55），无 gravity-only 改动

**影响范围**: `rpc/src/debug.rs`、`rpc/src/trace.rs`、op-rpc 等下游全部 caller。

**解决方案建议**: **take-upstream**
- 全文件采纳上游：`State::builder()` 替换 `CacheDB::new`、`&mut StateCacheDb` 取代 `StateCacheDbRefMutWrapper`、`EvmDatabaseError<ProviderError>` 类型、`evm_env_for_header` 路径、`trait Trace + Call` bound

**推理**: 上游对 trace.rs 的改动是数据库层重构 + bound 收紧。gravity baseline 在此文件无 gravity-only 行为，没有理由维持 `CacheDB` 写法（会与 cache/db.rs 的新 `StateCacheDb = State<…>` 别名矛盾）。

---

### `crates/rpc/rpc-eth-api/src/helpers/transaction.rs`

**模块**: `EthTransactions` / `LoadTransaction` trait — 整个 `eth_send*` / `eth_getTransaction*` 入口。

**冲突类型**: `UU`（21 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #21969 `32466fe22` — `send_transaction(origin: TransactionOrigin, tx: WithEncoded<Recovered<PoolPooledTx>>)` 新入口，`send_raw_transaction` 改为 default impl，内部调用 `send_transaction`
- PR #21624 `a9b2c1d45` — `force_blob_sidecar_upcasting` 流程；`max_fee_per_blob_gas` / `populate_blob_hashes` 在 `fill_transaction` 中处理
- PR #19199 `08fc0a918` — `fill_transaction(request) -> FillTransaction { raw, tx }` 新方法（独立于 `send_transaction_request`）
- PR #23409 — `send_transaction_request` 中改用 `next_available_nonce_for(&request)`
- PR #22795 / #23074 — `load_transaction_and_receipt` 返回 `(Recovered<Tx>, meta, receipt, Option<Arc<Vec<Receipt>>>)`，新增缓存命中分支：先查 `cache().get_transaction_by_hash(hash)`，再回退 `provider.transaction_by_hash_with_meta`
- PR #21180 / #22725 — `CachedTransaction::to_transaction_source` helper，`transaction_by_hash` 改为先查 cache、未命中再走 provider；旧 `unwrap_or(SYSTEM_CALLER)` fallback 上游改为 `try_into_recovered_unchecked().map_err(|_| EthApiError::InvalidTransactionSignature)?` — 即遇到不可恢复签名时**返回错误**而非 fallback
- PR #23700 — `TransactionInfo.block_timestamp` 字段
- `tx_resp_builder()` → `converter()` 全文件重命名
- `TransactionConversionError::Other(…)` 错误包装

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `a0d11f2288 (#259)` — `LoadTransaction::transaction_by_hash` 中：`let signer = tx.recover_signer_unchecked().unwrap_or(SYSTEM_CALLER);` —  **gravity-only 关键修复**
- `c8e2080f02 (#177)` 谱系 — 同样路径上"signer recovery 失败"行为变化
- 文件顶端定义 `const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000")`

**影响范围**: gravity 链上元交易 / system-tx 的 RPC 返回值。若采纳上游写法（recovery 失败 → 报错），可通过 RPC 查询的 system-caller 元交易会被拒。

**解决方案建议**: **needs-port** + **mechanical-merge**
- 大部分接受上游：`send_transaction(origin, WithEncoded<Recovered<…>>)` 新入口、`fill_transaction`、blob-sidecar upcasting、`load_transaction_and_receipt` 新签名 + cache 命中分支、`tx_resp_builder()` → `converter()` 重命名、`next_available_nonce_for`
- **必须保留 gravity #259**：`transaction_by_hash` 在 `recover_signer_unchecked` 失败时用 `SYSTEM_CALLER` 替代上游的 `?` 报错路径 — 改写策略：把上游 `try_into_recovered_unchecked().map_err(|_| EthApiError::InvalidTransactionSignature)?` 改回 gravity `recover_signer_unchecked().unwrap_or(SYSTEM_CALLER)` + `Recovered::new_unchecked(tx, signer)`，并保留文件顶端 `const SYSTEM_CALLER`
- 注意上游已经新增了 cache 优先分支（`cache().get_transaction_by_hash(hash)` → `to_transaction_source()`），gravity fallback 只应用在 cache miss → provider 命中之后的 recovery 步骤

**推理**: 此处是 gravity 元交易承重路径，直接影响 RPC 端对系统交易的可观测性；不可回退到上游"拒绝不可恢复 tx"的语义。上游新增的 cache-first 优化与 gravity SYSTEM_CALLER fallback 在不同层级，可叠加。

---

### `crates/rpc/rpc-eth-types/src/cache/db.rs`

**模块**: `StateCacheDb` / `StateProviderTraitObjWrapper` — RPC 临时执行用的内存 DB 包装。

**冲突类型**: `UU`（9 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #19330 `50e88c29b` — `StateCacheDb` 别名从 `CacheDB<StateProviderDatabase<…>>` 改为 `State<StateProviderDatabase<…>>`
- PR #19920 `ee63c7d6b` — `StateProviderTraitObjWrapper<'a>(&'a dyn StateProvider)` 改为 owned `StateProviderTraitObjWrapper(StateProviderBox)`；移除所有 `<'a>` 生命周期 + `StateCacheDbRefMutWrapper<'a, 'b>` 整个 wrapper 类型被删除
- PR #22289 `a05960ab0` — `StateProofProvider::witness(input, target, mode)` 新增 `mode: ExecutionWitnessMode` 参数
- PR #22379 `815037e27` / #21115 `121160d24` — `StorageRootProvider` / `AccountReader` / `BytecodeReader` 等 trait 中的方法签名透传上游

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- 仅 catch-up commits，无 gravity-only 改动
- baseline 中实现的 `account_code` / `account_balance` / `account_nonce` 是上游已有方法（v1.8.3 中已存在），并非 gravity 新增

**影响范围**: `helpers/trace.rs`、`rpc/src/debug.rs` 等所有创建 `StateCacheDb` 的位置 — 调用方语法 + 生命周期签名都要重写。

**解决方案建议**: **take-upstream**
- 接受 `StateCacheDb = State<StateProviderDatabase<StateProviderTraitObjWrapper>>` 别名
- 接受 owned `StateProviderTraitObjWrapper(StateProviderBox)`
- 接受删除 `StateCacheDbRefMutWrapper`（trace.rs 调用点直接用 `&mut StateCacheDb`）
- 接受 `witness(…, mode)` 第 3 参

**推理**: cache/db.rs 没有 gravity-only 修改；类型 alias 与 wrapper 形态完全是上游设计决定。

---

### `crates/rpc/rpc-eth-types/src/error/mod.rs`

**模块**: `EthApiError` 主枚举 + 多种 `From<…>` 实现。

**冲突类型**: `UU`（14 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #23330 / #18127 — `EthApiError::CallManyError { bundle_index, tx_index, error: ErrorObject }` 新变体
- PR #23534 — `EthApiError::BlockAccessListNotAvailablePreAmsterdam` 新变体（rpc code 4445）
- PR #18999 / #20627 / #23191 — revm v34/v37：`EVMError<T, TxError>` 改为带 `TxError: reth_evm::InvalidTxError`；`From<EVMError<…>>` 实现中改为 `as_invalid_tx_err()` 试取 + custom error fallback
- PR #21810 `74d4b1f2c` — `DebugInspectorError` 引入
- PR #23330 — `EvmDatabaseError`、`BalError` 引入（`state::bal::BalError`）
- PR #21270 / #20969 / #18844 / #18127 — `PrunedHistoryUnavailable` / `TransactionConversionError(_)` 携带错误信息 / `EthApiError::Internal` 包装

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `7d0483e565 (#335)` — `impl From<PoolErrorKind> for EthApiError`（50 gwei base-fee 上下文）中把模式 `(_)` 改为 `{ .. }`，是 v1.10.0 上游 PR #16407 引入的 `Underpriced` 结构化变体的同步修复；**已并入 baseline**
- `9974ad0618 (#241)` — CI/lint，不触及枚举语义

**影响范围**: 所有 `Self::Error: FromEthApiError`/`IntoEthApiError` 的下游 caller。

**解决方案建议**: **take-upstream**
- 接受 `CallManyError`、`BlockAccessListNotAvailablePreAmsterdam` 新变体
- 接受 `EVMError<T, TxError>` 新签名 + `as_invalid_tx_err()` 转换路径
- 接受 `DebugInspectorError`、`BalError`、`EvmDatabaseError` import
- 接受 `TransactionConversionError(_)` 携参变体（gravity baseline 是无参 `TransactionConversionError`，必须升级以匹配上游 trait）

**推理**: 错误枚举是契约性的扩展，gravity 没有保留性变体（`#335` 是 v1.10 上游修复的同义对齐，已在 baseline）。下游 `helpers/*` 都依赖新变体编译。

---

### `crates/rpc/rpc/Cargo.toml`

**模块**: `reth-rpc` crate 元数据。

**冲突类型**: `UU`（6 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #24284 `29d9241f6` — 删除 `debug_get_block_access_list` 相关依赖
- PR #21497 `31fa93889` — 添加 `--rpc.evm-memory-limit` → `revm` 加 `memory_limit` feature
- PR #18299 `1b830e9ed` — dev-mode mnemonic → `alloy-signer-local` 加 `mnemonic` feature
- PR #22397 `94818d767` — `reth_getBlockExecutionOutcome` → 引入 `reth-execution-types = { workspace = true, features = ["serde"] }`
- 引入 `reth-ethereum-primitives` / `reth-ethereum-engine-primitives` 普通依赖（从 dev-deps 升格）
- `js-tracer` feature 多加 `reth-rpc-eth-api/js-tracer`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `c64bd613e4 (#225)` — `[features] failpoints = ["fail/failpoints"]` + `[dependencies.fail] version = "0.5" optional = true`
- `reth-execution-types.workspace = true`（无 `features = ["serde"]`）
- `reth-ethereum-primitives` 仍在 `[dev-dependencies]`

**影响范围**: `rpc/src/debug.rs::debug_setFailpoint` 实现门控；以及 `reth_getBlockExecutionOutcome` 序列化路径。

**解决方案建议**: **mechanical-merge**
- 接受上游：`reth-execution-types = { workspace = true, features = ["serde"] }`、`alloy-signer-local = { workspace = true, features = ["mnemonic"] }`、`revm` 加 `memory_limit` feature、`reth-ethereum-primitives`/`reth-ethereum-engine-primitives` 升格、`js-tracer` 加 `reth-rpc-eth-api/js-tracer`
- 保留 gravity：`failpoints = ["fail/failpoints"]` + `[dependencies.fail] version = "0.5" optional = true`
- 删除 `[dev-dependencies] reth-ethereum-primitives.workspace = true` (已升格为正式 dep)

**推理**: feature 与 dep 调整无任何函数语义冲突。gravity-only 的 `failpoints` 是测试基础设施（`#225` 引入用于持久化故障注入），下游 `rpc/src/debug.rs` 中的 `debug_setFailpoint` impl 依赖它。

---

### `crates/rpc/rpc/src/debug.rs`

**模块**: `DebugApi` 实现 — `DebugApiServer` trait 的具体逻辑。

**冲突类型**: `UU`（41 处 marker；冲突量第二大）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #22719 `91182f653` — `DebugApi::new` 签名加 `executor: &Runtime` + `mut stream: impl Stream<Item = ConsensusEngineEvent<Eth::Primitives>>`；新增 `BadBlockStore` 字段；新增 `debug_traceBadBlock` + `debug_getBadBlocks`
- PR #24296 `fa8851655` — 新增 `debug_accountAt/accountInfoAt/accountRange/chainConfig/codeByHash` impl
- PR #22289 `a05960ab0` — `debug_executionWitness*` 加 `mode: Option<ExecutionWitnessMode>` 参数；改用 `BundleRetention` + `Database`/`DatabaseCommit` 显式 commit
- PR #24284 `29d9241f6` — 删除 8 个 stub 的 impl
- PR #22052 `57148eac9` — `executor` 改 `Runtime`
- PR #19925 `c2912a733` — 引入 `DebugInspector`（`revm-inspectors` 中的 trait）
- PR #22747 `8402a24a6` / #22726 `2d27a96d9` — `evm_env_for_header(block_header)` 取代 `evm_env_at` 
- PR #22577 `c4cd5c9b7` — `debug_traceCallMany` 加 `apply_pre_execution_changes`
- PR #22542 `626c82db3` — `debug_traceCallAtTxIndex` 用 `replay_transactions_until`
- PR #20780 `7bc3c95f0` — 并行 signature recovery
- PR #23945 — `InvalidBlock` rejection reason
- PR #23128 `f1c71d0c2` / #23162 `cc6d14a2c` — 去除冗余 block id resolution + 避免 clone
- PR #22675 `3d1dc4d9e` — `debug_getRaw*` 缺块返回 error
- `RpcConvert`-based 转换链全部走 `converter()`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `c64bd613e4 (#225)` — 在 impl block 末尾添加 `debug_setFailpoint(name, actions)` 方法体（feature-gated `failpoints`）

**影响范围**: 所有 `debug_*` RPC 端点行为。

**解决方案建议**: **take-upstream** + **keep-gravity** (small)
- 一律采纳上游：`DebugApi::new` 新签名、`BadBlockStore` 字段、`debug_traceBadBlock` / `debug_getBadBlocks`、`account*` / `chainConfig` / `codeByHash` impl、`executionWitness*` 新 mode 参数、删除 8 个 stub impl、`DebugInspector` 引入、`evm_env_for_header` 路径、`Runtime` executor
- 保留 gravity：`debug_setFailpoint` 实现（feature-gated）

**推理**: 所有上游改动是协议级新增 + 内部重构，无 gravity-only 行为受影响（gravity baseline 在此 41 个 hunk 范围内都是 catch-up commit，唯一新增是 `#225` 的 failpoint 方法体）。`DebugApi::new` 签名扩展需要所有 caller（`rpc-builder`、node-builder）同步更新。

---

### `crates/rpc/rpc/src/eth/helpers/transaction.rs`

**模块**: `EthApi<N, Rpc>` 上对 `EthTransactions` 的具体实现 — `send_raw_transaction` / `send_transaction` 落到 pool 的最后一公里。

**冲突类型**: `UU`（6 处 marker）

**上游变更** (`v1.8.3..v2.3.0`)：
- PR #21969 `32466fe22` — 将 `send_raw_transaction(tx: Bytes)` 重构为 default impl，自身实现 `send_transaction(origin, WithEncoded<Recovered<PoolPooledTx>>)`；`add_pool_transaction(origin, …)` 加 origin 参数
- PR #21624 `a9b2c1d45` — 在 `send_transaction` impl 中加 blob sidecar EIP-4844 → EIP-7594 upcasting 分支（依赖 `inner.force_blob_sidecar_upcasting()`）
- imports 更新：`alloy_consensus::BlobTransactionValidationError`、`alloy_eips::eip7594::BlobTransactionSidecarVariant`、`Typed2718`、`reth_primitives_traits::{AlloyBlockHeader, Recovered, WithEncoded}`、`reth_storage_api::BlockReaderIdExt`、`reth_transaction_pool::{Eip4844PoolTransactionError, EthBlobTransactionSidecar, EthPoolTransaction, PoolPooledTx}`

**Gravity 侧变更**（baseline 相对 v1.8.3）：
- `484c4fd59e (#77)` — 内部为插入 tx 添加 metric（add_pool_transaction 调用前后）
- 没有针对 `send_raw_transaction` / `send_transaction` 语义的 gravity-only 行为修改

**影响范围**: 用户 `eth_sendRawTransaction` 入口。

**解决方案建议**: **take-upstream**
- 接受 `send_transaction(origin, WithEncoded<Recovered<PoolPooledTx>>)` 新入口
- 接受 blob sidecar EIP-4844 → EIP-7594 upcasting 分支
- 接受 `add_pool_transaction(origin, …)` 加 origin 参数
- 注意 gravity #77 的 metric 增量已在 `inner.add_pool_transaction` 内部实现，不需要在这一层重复

**推理**: gravity 在此文件没有保留性语义；新签名是为 EIP-7702/EIP-4844 sidecar opt-in 处理。`add_pool_transaction(origin, …)` 加 origin 是 PR #21969 整套改动的一部分，必须跟随。

---

## ⟲ 2026-07-05 现状核实与解块方向修正(f89d9d4e23 之后)

> 本节为开放问题核实时的附带实测发现。背景:f89d9d4e23 已把 storage/trie/prune
> 整体还原 gravity baseline;决策总原则(2026-07-05 用户拍板,记档于
> `executed-block-split-pipe-exec-make-canonical.md` §九):①storage 决策最高
> ②冲突迎合 storage ③不冲突的在不破坏 gravity 功能前提下保留 v2.3.0 设计。

**1. 五个"零冲突侧翻"文件**:分组概要记录的 13 文件中,以下 5 个当前冲突标记
为 0,且实测与 v2.3.0 **逐字节相同**(`git diff v2.3.0 HEAD -- <path>` = 0 行,
自 squash checkpoint e6b7e5ba32 起即如此;概要中的 hunk 数对它们从未成立):
`rpc-builder/src/config.rs`、`rpc-eth-api/src/helpers/receipt.rs`、
`rpc-eth-api/src/helpers/state.rs`、`rpc-eth-types/src/cache/db.rs`、
`rpc/src/eth/helpers/transaction.rs`。其中:

- **`cache/db.rs` 是活断点**:`fn witness`(:100-104)带
  `mode: reth_trie::ExecutionWitnessMode` —— 双重断:①该类型定义文件
  `trie/common/src/execution_witness.rs` 在盘上但 baseline 版 lib.rs 无
  `mod execution_witness;` 挂载(磁盘孤儿,路径编译不可达);
  ②storage-api 的 `StateProofProvider::witness` 已随还原回二参无 mode
  (trie.rs:93)。**须回 baseline 形态**,属本组新增落地项。
- 其余 4 个做过死符号专项扫(ExecutionWitnessMode/BalProvider/bal_store/
  ChangesetCache/StateTrieOverlayManager/trie_data/DatabaseProviderROFactory
  各 0 命中),暂无已知断点,按原则 3 可留 v2.3.0 形态;落地组编译期复核。

**2. 孤儿/死符号对解块方向的修正**(按原则 2,以下 v2.3.0 侧内容**不可采纳**,
解向 baseline;这修正了下方多个逐文件「解决方案建议」):

| 符号 | 现状(实测) | 受影响的解块 |
|---|---|---|
| `ExecutionWitnessMode` | 磁盘孤儿(见上) | `rpc-api/debug.rs` 8 块中 `executionWitnessByBlockHash(hash, mode)` 新签名**不可采纳**(保 baseline 无 mode 签名);`rpc/debug.rs` 41 块中 6 处 mode 引用同理 |
| `BalProvider` | `storage-api/src/bal.rs:283` 在盘,lib.rs 无挂载 = 孤儿 | `core.rs` 8 块中 4 个 BAL 端点方法、BAL 相关 impl 不可采纳 |
| `BalError` | 全仓无定义 | `error/mod.rs` 14 块中 BAL 相关变体不可采纳;非 BAL 的新变体(如 `CallManyError`)逐个验存活后可采纳 |
| `BadBlockStore` | `rpc/debug.rs:2007` 文件内自包含定义 | 存活,`debug_traceBadBlock` 相关块可按原文档建议采纳(其 `&Runtime` 依赖 reth-tasks,存活) |

**3. gravity 保留项风险**:`#259` 的 `SYSTEM_CALLER` fallback 两处
(`rpc-eth-api/src/helpers/transaction.rs` :64 const 定义、:842 使用)**均位于
冲突块内**(awk 分区实测),且对应 impl 文件 `rpc/src/eth/helpers/transaction.rs`
已侧翻为纯 v2.3.0——trait default 方法是该 gravity 语义的唯一承载点,解
21 块时必须保 HEAD 侧,漏保即静默丢失。

**4. 跨组断点交叉引用**:`TryFromTransactionResponse` 已被 v2.3.0 侧
rpc-convert 删除,而 9.4 复原后的 baseline rpc-provider 需要它(4 处使用)——
待裁决(rpc-convert 加回 vs 移植进 rpc-provider),rpc 组认领;若本组解块
涉及 rpc-convert 相关 import,先查该裁决进展。

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [x] 1. **`DebugExecutionWitnessApi<Attributes>` trait** — gravity `rpc-api/src/debug.rs` 中存在，需 `grep -n DebugExecutionWitnessApi` 比对 `gravity-reth@0cb1687c1c` 与 `reth@v2.3.0` 确认其在上游侧的归属。
   ⟲ 归属已核实(2026-07-05,`git grep -l DebugExecutionWitnessApi <ref>`):
   v1.8.3 与 baseline `0cb1687c1c` 均有(rpc-api/debug.rs + crates/optimism 两处),
   **v2.3.0 已删除**;即 v1.8.3 上游遗留、gravity 零定制。当前 worktree 中该
   trait 位于 rpc-api/debug.rs:467 的冲突块 **HEAD 侧**;workspace 内唯一其它
   消费方是 crates/optimism(root Cargo.toml 无 optimism member,不参与编译)。
   → **决策**: 随上游删除(解块取 v2.3.0 侧)。依据:决策总原则第 3 条(上游
   删除不破坏 gravity 功能——gravity 无消费方)+ 本文件原「解决方案建议」
   ("若被删除则取上游")。⚠ 落地注意:同文件 8 块**不能全取 v2.3.0**——
   `executionWitnessByBlockHash(hash, mode)` 新签名因 `ExecutionWitnessMode`
   孤儿化不可采纳(见上方 ⟲ 修正节第 2 条),witness 端点保 baseline 签名。
   - [ ] 冲突解决:待落地;crates/rpc/rpc-api/src/debug.rs 现存 8 处冲突块(2026-07-05 复测未变)。

- [x] 2. **`SYSTEM_CALLER` 是否应当扩展到 `receipt.rs`** — 当前 `receipt.rs` 在 fallback 路径用 `Address::ZERO`（v1.8.3 上游既存），未引入 gravity SYSTEM_CALLER 谱系。是否在 merge-v2.3.0 中把 `Address::ZERO` 一并改成 `SYSTEM_CALLER` 以与 `transaction.rs` 对齐，需要业务决策（非合并语义问题）。
   → **决策**: **不需要扩展**(用户拍板,2026-07-05)。
   ⟲ 现状补充:该问题的代码位点已被上游 #22795 重构消解——
   `build_transaction_receipt` 改收 `Recovered<Tx>`(调用方先完成 recovery),
   receipt 路径不再有 recovery fallback 位点。实测:
   `rpc-eth-api/src/helpers/receipt.rs` 零冲突、与 v2.3.0 逐字节相同(新签名
   :25-30);`rpc/rpc/src/eth/helpers/receipt.rs` 中 `SYSTEM_CALLER` /
   `Address::ZERO` 均 0 命中。
   - [x] 冲突解决:决策为"不扩展" = 零代码动作,现状即终态(证据:上述两文件
     冲突标记为 0、无 fallback 位点残留,2026-07-05 实测)。

- [x] 3. **`Consensus::Error` 移除影响面** — 上游 PR #20843 把 `Consensus: FullConsensus<N, Error = ConsensusError>` 收紧为 `Consensus: FullConsensus<N>`；gravity ethapi/op-rpc 等下游 impl 不再需要透传 `ConsensusError`，但 gravity-only consensus impl（如有）需要 verify 是否仍兼容。
   ⟲ 已核实(2026-07-05):全 workspace `impl … FullConsensus` 仅一处——
   上游自有的 `EthBeaconConsensus`(crates/ethereum/consensus/src/lib.rs:111);
   **gravity 无自有 FullConsensus impl**(gravity 共识走 pipe-exec 的
   Coordinator/event-bus 注入,不经 FullConsensus trait),兼容性问题不存在。
   → **决策**: 采纳上游收紧后的 bound(决策总原则第 3 条;无 gravity 功能受损)。
   - [ ] 冲突解决:无独立代码动作,随 `rpc/src/debug.rs` 等相关文件解块自动
     落地(该文件现存 41 块未解,故暂不勾;解完后凭编译/grep 证据勾)。

- [x] 4. **`failpoints` 在 dev/test 之外是否启用** — `#225` 的 `debug_setFailpoint` 仅在 `cargo build --features failpoints` 下生效；需要确认本次 merge 是否要在 production binary 中启用此 feature。
   ⟲ baseline 实测(`git show 0cb1687c1c:bin/reth/Cargo.toml`):
   `default = ["jemalloc", "reth-revm/portable"]`(:70),`failpoints` 为独立
   feature(:130,`failpoints = ["reth-node-ethereum/failpoints"]`),**不在
   default 中**——即 gravity 既有行为 = production binary 默认不启用,仅
   `--features failpoints` 显式构建时生效。
   → **决策**: 维持 baseline 行为——`failpoints` 保留为 opt-in feature、
   **不进 default**。依据:决策总原则精神(merge 只对齐两侧,不改变 gravity
   既有行为);是否在某次生产构建中显式启用属运维构建选择,不在 merge 范围。
   - [ ] 冲突解决:待落地;bin/reth/Cargo.toml 现存 7 处冲突块(2026-07-05
     复测未变),解块时保住 `failpoints` feature 定义且不加入 default。
