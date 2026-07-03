# transaction-pool 冲突分析

Baseline 锚点：`0cb1687c1c`（gravity main，已合 reth v1.8.3 via #205）。
合入目标：reth v2.3.0。
所有结论从 baseline 与 upstream `v1.8.3..v2.3.0` 双向核对，pipe-exec / batch-insert / 50Gwei 基础费等 gravity 改造均为 baseline 既有，无需再 port。

## 分组概要

| 文件 | 冲突类型 | 主线方案 |
|---|---|---|
| `crates/transaction-pool/Cargo.toml` | UU | mechanical-merge |
| `crates/transaction-pool/src/config.rs` | UU | take-upstream |
| `crates/transaction-pool/src/error.rs` | UU | take-upstream |
| `crates/transaction-pool/src/lib.rs` | UU | mechanical-merge |
| `crates/transaction-pool/src/maintain.rs` | UU | mechanical-merge |
| `crates/transaction-pool/src/metrics.rs` | UU | take-upstream |
| `crates/transaction-pool/src/pool/best.rs` | UU | take-upstream |
| `crates/transaction-pool/src/pool/mod.rs` | UU | mechanical-merge |
| `crates/transaction-pool/src/pool/pending.rs` | UU | take-upstream |
| `crates/transaction-pool/src/pool/txpool.rs` | UU | mechanical-merge |
| `crates/transaction-pool/src/validate/eth.rs` | UU | keep-gravity |
| `crates/transaction-pool/src/validate/task.rs` | UU | take-upstream |

## 逐文件分析

### `crates/transaction-pool/Cargo.toml`

**模块**：crate 元数据
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `14570f325` perf: `BestTransactions` 改用 `imbl::OrdMap` → 新增 `imbl` dep、相关 `serde` / `arbitrary` feature 项
- `b87cde547` feat: configurable EVM execution limits → 新增 `reth-evm` / `reth-evm-ethereum` dep（兼 dev-dep `test-utils`）
- `68e4ff1f7` feat: global runtime → 新增 `reth-tasks/test-utils`
- `40bc9d386` / `c617d25c3` LazyTrieData → 新增 `revm` dep（含 `serde` feature 项）
- `598f228e2` chore: remove criterion benchmarks and codspeed → 删除 criterion dev-dep
- `151f92d43` chore: remove duplicate dev-deps → 删除 `assert_matches` / `tempfile` 重复声明

**Gravity 侧变更**（基于 baseline `0cb1687c1c` 在该文件上的 gravity-only 增量）
- baseline 引入 `gravity-primitives.workspace = true`、`reth-pipe-exec-layer-event-bus.workspace = true`、`revm-interpreter`、`revm-primitives` 依赖（pipe-exec、`get_gravity_config`、EIP-7702 intrinsic gas 计算需要）
- `[features] config-from-env = ["gravity-primitives/config-from-env"]`
- 五条 `[[bench]]` 段：`truncate` / `reorder` / `priority` / `insertion` / `canonical_state_change`，全部 gravity 引入；依赖 dev-dep `criterion` 和 `serde_json`

**影响范围**：依赖图与 feature flag 体系。Cargo 解析逻辑无运行时风险，但若漏掉 `gravity-primitives` 或 `reth-pipe-exec-layer-event-bus` 会导致 `maintain.rs` 编译失败。

**解决方案建议**：mechanical-merge — 同时保留：
- 上游新增的 `reth-evm` / `reth-evm-ethereum` / `revm` / `imbl` 依赖与对应 feature 项（`v2.3.0` 全文需要）
- gravity 端 `gravity-primitives` / `reth-pipe-exec-layer-event-bus` / `revm-interpreter` / `revm-primitives` 依赖
- gravity 的 `config-from-env` feature
- gravity 的五条 `[[bench]]` 段及其需要的 `criterion` / `serde_json` dev-dep（必须保留，否则 PR #92 的 batch-insert / Anvil bench 失效）
- 上游删除的 `assert_matches` / `tempfile` 重复声明（按上游一次出现保留即可）

**推理**：双方变更落在不相交的字段上，无语义冲突。`reth-tasks/test-utils` 上游新增可直接采用。`criterion` 删除属 upstream 内部 bench 清理，但 gravity 自身 bench 仍在使用，需保留。

### `crates/transaction-pool/src/config.rs`

**模块**：`PoolConfig` / `SubPoolLimit` / `LocalTransactionConfig`
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `f53f90d71` refactor：`HashSet<Address>` → `alloy_primitives::map::AddressSet`（FxHash 提速）
- `f5ce6407a` docs：sub-pool limit 注释单复数
- `f79fdf356` perf: pre-alloc removed vec
- 上游也加了 `SubPoolLimit::tx_excess` / `impl Mul<usize>`

**Gravity 侧变更**
- baseline `364b851665` (#337) 与 `7d0483e565` (#335) 落在该文件的 gravity 痕迹**已被 #337 revert 清除**，当前 baseline 上 `config.rs` 与上游 v1.8.3 在 Gravity 自定义字段上**没有持续差异**
- 唯一仍 diverge 的 hunk 是 `impl Mul<usize> for SubPoolLimit` —— gravity 端早于上游引入；v2.3.0 上游也加了同名 impl，**实现完全等价**

**影响范围**：内存布局 / 行为等价；`HashSet<Address>` → `AddressSet` 是 type-alias 切换，调用面兼容。

**解决方案建议**：take-upstream — 直接采用 v2.3.0 全文。`Mul<usize>` 的 gravity 历史 impl 被 upstream 同形 impl 取代即可（两者代码一致）。`AddressSet` 是 `HashMap<Address, _, FxRandomState>` 别名，对外 API 等价。

**推理**：cite `364b851665` (#337) 明确"revert tx_gen.rs / config.rs 的 Gravity-specific 调整"，所以 baseline `config.rs` 已无活动 gravity patch。两个 `Mul` impl 完全等价。

### `crates/transaction-pool/src/error.rs`

**模块**：`InvalidPoolTransactionError` / `Eip4844PoolTransactionError`
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `03dd1c3ae` (#23450) fix: `ExceedsFeeCap` 不再标记为 `is_bad_transaction`（改回 `false`）
- `0aff4cc8d` (#23035) fix(net): 加入 `is_bad_blob_sidecar` / `Eip7594SidecarDisallowed`
- `e3c256340` (#21622) feat: EIP-7594 blob sidecar toggle
- `dc8c4eebd` (#19993) feat: `is_nonce_too_low` 辅助
- `1d58ae1ff` (#19190) feat: oversized data 错误改成 struct-style（`OversizedData { .. }`）+ `is_oversized`
- `df6afe9da` docs

**Gravity 侧变更**
- 该文件 baseline `0cb1687c1c` 上没有 gravity-only patch（最新的 `7d0483e565` 只动了 doc-markdown 反引号；`f41d81ac63` (#2396) 等条目早被 #205 catch-up 收编为 upstream-equivalent）
- 当前 worktree 的 gravity 侧 hunk（`UnexpectedEip7594SidecarBeforeOsaka` arm、`is_oversized` 使用 `OversizedData(_, _)` 元组形式、`ExceedsFeeCap => true`）全部是 **reth v1.8.3 旧文本**，**不是** gravity 添加

**影响范围**：错误分类决定 P2P peer 信誉惩罚（`is_bad_transaction`）。`ExceedsFeeCap` 改回 `false` 是 #23450 显式修复（避免把本地策略误判为 peer 恶意），必须随上游。

**解决方案建议**：take-upstream — 直接采用 v2.3.0 全文，包括 `OversizedData { size, max_size }` struct 形式、`is_bad_blob_sidecar`、`Eip7594SidecarDisallowed`、`is_nonce_too_low`。

**推理**：所有冲突 hunk 均为 reth 自演进，gravity 在该文件没有需要保留的语义。`ExceedsFeeCap` 改 `false` 必须采纳，否则会错误降级 peer。

### `crates/transaction-pool/src/lib.rs`

**模块**：crate root / `Pool` 包装 / `EthTransactionPool` 别名 / 文档示例
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `44b904acb` (#24848) feat: `ValidatingPool` trait
- `578680aef` (#24482) feat: `retain_contains` helper
- `5f85eb7ac` (#23767) feat: `getBlobsV4` endpoint → 新增 `BlobCellsAndProofsV1` / `B128`
- `14570f325` (#23621) perf: `imbl::OrdMap` → `pub use imbl::OrdMap`
- `792ee9245` (#23008) fix: 只读 sender+nonce 查询不再扩张 sender-id map → 引入 `sender_id` 只读方法
- `0ba685386` / `57148eac9` / `68e4ff1f7` (#22263/#22052/#21934) `TaskSpawner` → `Runtime`
- `8171cee92` (#22161) `add_transactions_with_origins` 改签名为 `Vec`
- `32466fe22` (#21969) propagate `TransactionOrigin` through send_transaction
- `b9d774438` (#21765) `prune_transactions` 公开方法
- `df12fee96` (#21742) `is_transaction_ready` trait 方法
- `1bd8fab88` (#21359) `TransactionValidator` 增加 `Block` 关联类型
- `b87cde547` (#21088) configurable EVM execution limits → `EthEvmConfig` 入参贯穿
- 一系列 export / docs 调整（`#![cfg_attr(docsrs, feature(doc_cfg))]` 单一 feature）

**Gravity 侧变更**
- `3eec9c4976` (#92) `perf(mempool): add batch insert pool` → `Pool` 增加 `batch_insert_task_handle` / `batch_insert_task_running` 字段，新增 `validate_all_with_origins`、`get_pooled_transaction_elements`、`to_pooled_transaction` 等方法（baseline 必有；冲突 hunk 中 gravity 侧 `if transactions.len() == 1 ...` 短路块、`validate_all_with_origins` 即此 PR）
- baseline 文档示例仍用 `TokioTaskExecutor` / `TaskManager`（pre-#22052 文本）

**影响范围**：trait 签名 + 公共 API。`Runtime` 替换 `TaskSpawner` 后所有调用站点必须同步；`EthEvmConfig` 入参必须新加。Gravity 自有的 batch-insert / origin-batched API（pipe-exec 调用方依赖）必须保留。

**解决方案建议**：mechanical-merge — 以 v2.3.0 上游骨架为基（`Runtime`、`Evm` 入参、`pub use imbl::OrdMap`、`BlobCellsAndProofsV1`、`HeaderTy` 等全部采纳），在其上叠加 gravity 的：
- `Pool` 结构体的 `batch_insert_task_handle` / `batch_insert_task_running` 字段
- `validate_all`（单 origin 批）与 `validate_all_with_origins`（混合 origin 批，含 `len()==1` 短路）
- `get_pooled_transaction_elements` / `to_pooled_transaction` 辅助方法（如上游已等价提供则去重，否则保留 gravity 版）

**推理**：cite `3eec9c4976` baseline 内 `Pool::new` 已扩字段、`validate_all` 已基于 `validate_transactions` 批接口（PR #92 同时改 `PoolInner::validate_all`）。`Runtime` 切换是 upstream 系列 PR 强制，无 keep-gravity 余地。

### `crates/transaction-pool/src/maintain.rs`

**模块**：pool 与 canonical chain 同步循环
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `91f3802a4` (#24555) perf: preallocate reorg mined hash set → `transaction_hashes_vec()`
- `f148781d1` (#24478) perf: preallocate chain transaction hashes
- `764246d5e` / `8e3bc6567` (#22609/#22426) chore: 用 `to_consensus()` helper
- `57148eac9` / `68e4ff1f7` (#22052/#21934) `TaskSpawner` → `Runtime`（`spawn_blocking` → `spawn_blocking_task`）
- `f53f90d71` (#21686) `AddressSet` / `alloy_primitives::map`
- `485f5b36c` (#20781) fix: finalized block number never decrease（`FinalizedBlockTracker::update` 改 idempotent 写法）
- `c754caf8c` (#20528) fix: stale blob 清理（stale_eviction 添加 `delete_blobs(stale_blobs)`）
- `1bd8fab88` (#21359) TransactionValidator + Block 关联类型 → `Block = N::Block`
- 维护 `MockEthProvider::with_genesis_block` 等 test 调整、加 `EthereumHardforks` bound

**Gravity 侧变更**
- baseline `0cb1687c1c` 该文件包含两段 gravity-only 改造：
  - `a9914e1c92` (#110) / `fe2319f81a` (#67) 引入 `gravity_primitives::get_gravity_config` 与 pipe-exec discard_txs 订阅块：循环开始前若 `!disable_pipe_execution` 则 `tokio::spawn` 一个长驻 task 消费 `get_pipe_exec_layer_event_bus().discard_txs`，把 pipe-exec 拒收的 txs 从 pool 中移除
  - cite `5901e7da98` (#173) / `663dbf46d2` (#55) 的 stale_eviction 路径在 gravity 端使用 `tx.timestamp.elapsed()`（基于 PR #99 `add txn time` 的 `Instant` 字段），未做 stale blob 收集
- baseline 的 `FinalizedBlockTracker::update` 仍是 pre-#20781 的 `replace(...).is_none_or(...)` 写法

**影响范围**：pipe-exec discard 订阅块**绝对**需要保留 —— 这是 gravity execution layer 把 consensus 拒收的 txs 通知回 pool 的唯一通路，丢失会导致已被网络拒收的 txs 滞留 mempool。

**解决方案建议**：mechanical-merge
- 整体采纳 v2.3.0 上游版本作为骨架（`Runtime` / `spawn_blocking_task` / `AddressSet` / `EthereumHardforks` bound / `Block = N::Block` 关联类型约束 / `to_consensus()` / `transaction_hashes_vec()` / 改良后的 `FinalizedBlockTracker::update` / stale_eviction 添加 stale_blobs 收集）
- 在 `loop {` 开始前**保留 gravity 的 pipe-exec discard 订阅块**（`if !get_gravity_config().disable_pipe_execution { ... }`）
- imports 同步加入 `gravity_primitives::get_gravity_config`、`reth_pipe_exec_layer_event_bus::get_pipe_exec_layer_event_bus`
- gravity 的 `tx.timestamp.elapsed()` 与上游的 `now - tx.timestamp` 在语义上等价（前者基于 `Instant::elapsed`），保留上游写法
- gravity 历史 PR #100 引入的 `clone_into_consensus().into_inner()` 已被上游 #22426 的 `to_consensus().into_inner()` 等价替换，直接采纳上游

**推理**：`get_pipe_exec_layer_event_bus().discard_txs.lock().await.take().unwrap()` 在 gravity 启动期是单一消费者，必须在 loop 内启动；其余 hunk 都是上游优化/重构，没有 gravity-specific 语义。

**关键发现**：pipe-exec discard 订阅块缺失 → mempool 与 execution layer 严重失同步（chain 不会 halt，但 pool 内将堆积大量已被网络拒收的 txs，最终 OOM 或 best-tx iterator 反复吐出无效 tx）。

### `crates/transaction-pool/src/metrics.rs`

**模块**：pool 指标定义
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `3a9dbdc84` (#20087) feat: 把 metrics、listener struct、fields 全部 `pub(crate)` → `pub`
- `edc31d23e` (#19965) feat: `total_other_transactions` 指标

**Gravity 侧变更**
- 该文件 baseline 上 gravity-only 改动来自 `aa743bae0c` (#104)、`79286e7152`（add txn time）等早期 PR，均已被 v1.8.3 catch-up 同名结构吸收
- 当前冲突的 gravity 端只是把 v1.8.3 的 `pub(crate)` 字段保留下来 + 新增 `TxPoolValidationMetrics` / `TxPoolValidatorMetrics`（这两个结构上游 v2.3.0 同样存在，**实现完全等价**）

**影响范围**：纯字段可见性 + 一个新 gauge。无运行时语义差异。

**解决方案建议**：take-upstream — 接受 v2.3.0 全文：所有字段 `pub`、新增 `total_other_transactions`、保留 `TxPoolValidationMetrics` / `TxPoolValidatorMetrics`（pub 字段版本）。

**推理**：cite `3a9dbdc84` 明确将所有 metrics 字段公开，gravity 没有需要 `pub(crate)` 隔离的理由。`TxPoolValidationMetrics` 上下游同形。

### `crates/transaction-pool/src/pool/best.rs`

**模块**：`BestTransactions` iterator
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `76576db45` (#24647) perf: `FxHashSet` for invalid senders
- `e550355a9` (#24417) perf: `size_hint` for `BestTransactions`
- `735417251` (#24121) refactor: `mark_invalid` 按值传错误
- `14570f325` (#23621) perf: `imbl::OrdMap` 取代 `BTreeMap`
- `21a4b1382` (#19982) feat: `next_tx_and_priority` 提取（`next()` 改为薄壳）
- `c7b689016` (#19940) fix: skipped 高优先级 tx 跟踪 —— **已被 baseline `ab314aa092` (#211) cherry-pick**
- `2f58f6797` (#19981) accept error by ref
- `f53f90d71` (#21686) `AddressSet`
- `MAX_NEW_TRANSACTIONS_PER_BATCH` 上游 = 16

**Gravity 侧变更**
- baseline 显式将 `MAX_NEW_TRANSACTIONS_PER_BATCH = 1024`（baseline 文本 line 19）—— 这是**唯一持续 gravity 改动**，且经 `ab314aa092` (#211) 验证 cherry-pick 的 #19940 修复仍然兼容
- 其余 hunk（`HashSet<SenderId>`、`HashSet<Address>` for prioritized senders、`BTreeMap` 形态等）均为 reth v1.8.3 文本，并非 gravity 引入

**影响范围**：`MAX_NEW_TRANSACTIONS_PER_BATCH` 决定 `BestTransactions` 每轮从 broadcast channel 拉取的新 pending tx 数量。1024 ≫ 16 — gravity 用更大批是为了配合 pipe-exec 的 batch-insert（cite `3eec9c4976`）。

**解决方案建议**：take-upstream — 整体采纳 v2.3.0（含 `imbl::OrdMap`、`FxHashSet`、`AddressSet`、`next_tx_and_priority`、`size_hint`），但**保留 `const MAX_NEW_TRANSACTIONS_PER_BATCH: usize = 1024`**（一行常量覆盖）。

**推理**：除常量外 gravity 没有自定义逻辑；其它分歧都是底座版本差异。`MAX_NEW_TRANSACTIONS_PER_BATCH = 1024` 是性能调优，保留 gravity 数值无副作用（上限受 channel size 实际约束）。

### `crates/transaction-pool/src/pool/mod.rs`

**模块**：`PoolInner` —— pool 顶层封装
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `a91b9e0df` (#24554) perf: preallocate propagation vectors
- `578680aef` (#24482) feat: `retain_contains` + `append_pooled_transactions[_hashes]`
- `792ee9245` (#23008) fix: 只读 sender+nonce 查询不扩张 sender-id map → 增 `sender_id(&Address) -> Option<SenderId>`、`update_accounts` 改 `filter_map` + 只读 `identifiers.read()`
- `8861e2724` (#22243) fix: `set_block_info` 升迁通知订阅者 → `notify_on_transaction_updates`
- `b9d774438` (#21765) `prune_transactions` 公开
- `f53f90d71` (#21686) `AddressSet` / `HashSet` from `alloy_primitives::map`
- `df12fee96` (#21742) `is_transaction_ready`
- 一系列 listener / event 公开 / 优化（PR #20013/#20028/#19985/#19918/#19476 …）
- `has_event_listeners` 原子优化 + `with_event_listener` helper

**Gravity 侧变更**
- baseline 对 `pool/mod.rs` 的 gravity 痕迹主要来自 `3eec9c4976` (#92) batch-insert 与 `5644347583`、`162f6019d4`、`f8e6e2e3d4` 等已被 v1.8.3 catch-up 收编的 PR
- gravity 侧的 `pooled_transactions_hashes / pooled_transactions / pooled_transactions_max / to_pooled_transaction / get_pooled_transaction_elements` 写法沿用 reth v1.8.3 形态（上游 #24482 / #24554 改成 append 风格）
- `set_block_info` gravity 侧没有调用 `notify_on_transaction_updates`（pre-#22243）
- `get_sender_id`（写锁创建）gravity 侧没有 read-only `sender_id` 兄弟方法（pre-#23008）

**影响范围**：所有变化都关乎 mempool 内部 housekeeping，无 chain 一致性风险。但 #23008 修复**很重要** —— 只读路径不再无限增长 sender-id map（gravity 长跑节点会受益）；#22243 修复 set_block_info 通知 —— 否则 RPC subscriber 漏接 promotion 事件。

**解决方案建议**：mechanical-merge — 整体采纳 v2.3.0（含 `AddressSet`、`HashSet from alloy_primitives::map`、`sender_id`(读)/`get_sender_id`(写) 双方法、`notify_on_transaction_updates`、`append_pooled_transactions*` 系列、`is_transaction_ready`、`prune_transactions`、`has_event_listeners` 原子优化、`with_event_listener` helper）。Gravity 没有需要保留的语义 hunk。

**推理**：所有冲突 hunk 都是 reth 自演进；gravity 端的"看似 gravity"代码（`pooled_transactions_*`、`set_block_info`、`get_sender_id`）实际是 v1.8.3 原版。

### `crates/transaction-pool/src/pool/pending.rs`

**模块**：`PendingPool`
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `14570f325` (#23621) perf: `by_id` 改用 `imbl::OrdMap`
- `d4cb91f0a` (#22528) perf: `txs_by_sender` 用 BTree range 查询
- `0aa922c4e` (#21387) sort_unstable
- `6fafff5f1` (#19301) fix: `remove_transaction` 中 `highest_nonces` 更新逻辑改写 —— 不再走 `ancestor()` 路径，直接通过 `by_id.range(...).last()` 查到该 sender 剩余最高 nonce，**消除了 gravity baseline `ancestor()` 辅助方法**
- `0da9fabf8` (#18778) fix: wrong assertion 文案
- `af1e12fd4` (#20082) feature gate test

**Gravity 侧变更**
- baseline 在 `remove_transaction` 中调用 `ancestor()` 维护 `highest_nonces`（cite `6fafff5f1` 上游已被替换为 BTree range 方案，gravity 端因为 cherry-pick 时机问题保留了旧 helper）
- baseline `assert_invariants` 错误文案 (`independent.len() > all.len()` / `independent_descendants.len() > all.len()`) 是 v1.8.3 旧版（已被上游 #18778 改文案）
- `pool.all().collect::<Vec<_>>().is_empty()` vs 上游 `pool.all().next().is_none()` —— upstream `c0caaa17b` (#18902) 优化（gravity 侧未跟）
- `test_handle_duplicates` 是否带 `#[cfg(debug_assertions)]` —— upstream 已加该 cfg

**影响范围**：mempool pending 子池正确性 + 性能。#19301 是 fix，避免 highest_nonces 错乱；#22528 / #14570f325 是性能改造。gravity baseline 没有任何在 pending 上的 chain 一致性自定义。

**解决方案建议**：take-upstream — 完整采纳 v2.3.0（`OrdMap`、BTree range `txs_by_sender`、`sort_unstable`、`remove_transaction` BTree range 方案、`assert_invariants` 新文案、`#[cfg(debug_assertions)]` 测试、新 `txs_by_sender` pub helper），删除 baseline 的 `ancestor()` 辅助方法。

**推理**：gravity 端在该文件无 chain-critical 自定义；保留旧 `ancestor()` 路径只会与 #19301 修复冲突。

### `crates/transaction-pool/src/pool/txpool.rs`

**模块**：`TxPool` 主体与 `AllTransactions`
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `6b499151d` (#23037) perf: `FxHashMap` / `FxHashSet` for TxHash containers → `B256Map` / `B256Set` / `AddressSet`
- `ac3120703` (#23406) fix: 在 pool 中正确校验 authorities
- `d3d7fb31d` (#23012) fix: replacement tx price bump 用 ceiling division
- `d4cb91f0a` (#22528) perf: pending_txs_by_sender / queued_txs_by_sender BTree range
- `47544d9a7` (#22308) fix: pending subpool 按 nonce 顺序插入 → `insert_tx` 的 `Ok` 分支引入"按 new_nonce 切分 updates、插入前后两批"的交错处理
- `8861e2724` (#22243) fix: `set_block_info` 通知 → 返回 `UpdateOutcome`
- `b9d774438` (#21765) `prune_transactions`
- `df12fee96` (#21742) `is_transaction_ready`
- `40f89af92` (#19634) chore: remove unused `latest_update_kind` from `TxPool`
- `997848c2a` (#20368) fix: remove stale senderinfo —— 把 `sender_info` 字段从 `TxPool` 移到 `AllTransactions`（上游）
- `edc31d23e` (#19965) feat: `total_other_transactions`
- 测试与 import 改造（`HashMap`/`HashSet` 移到 `#[cfg(test)]`）

**Gravity 侧变更**
- baseline 仍在 `TxPool` 上挂 `sender_info` 与 `latest_update_kind`（pre-#20368 / pre-#19634）
- `pending_txs_by_sender` / `queued_txs_by_sender` 仍用 `filter` 迭代实现（pre-#22528）
- `set_block_info` 不返回 `UpdateOutcome`（pre-#22243）
- 缺 `get_pending_transaction_by_sender_and_nonce`、`other_count` 指标、authorities 校验细节、`insert_tx` 的 nonce-顺序交错插入逻辑
- gravity baseline 在该文件**没有**自定义 chain-critical patch；全部冲突 hunk 都是 reth v1.8.3 与 v2.3.0 之间的演进

**影响范围**：核心 mempool data structure。#22308 / #23012 是 nonce-order 与 price-bump 修复，对 RPC 行为正确性必要；#23406 authority 校验对 EIP-7702 必要；#20368 修复 stale sender_info 内存泄露。

**解决方案建议**：mechanical-merge — 整体采纳 v2.3.0：
- `AllTransactions.sender_info`（搬迁 + cleanup）
- 移除 `TxPool.latest_update_kind` 字段
- `set_block_info` 返回 `UpdateOutcome` + `notify_on_transaction_updates`
- `insert_tx` nonce-ordered interleaved 插入
- ceiling-division price bump、authorities 校验
- `txs_by_sender` BTree range helper
- `other_count` 指标
- import 整理（`B256Map` / `B256Set` / `AddressSet` / `#[cfg(test)] HashMap`）

Gravity 不需要保留任何 hunk —— 当前冲突端全部是 v1.8.3 旧文本。

**推理**：所有 cite 的 fix（#22308 nonce 顺序、#23012 price bump、#23406 authorities、#20368 sender_info）都是修正性 PR，不采纳即引入回归。Gravity 端没有任何分歧。

### `crates/transaction-pool/src/validate/eth.rs`

**模块**：`EthTransactionValidator` —— 以太坊交易验证主路径
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `aec6ba00c` / `803839df0` revm 40.x 升级
- `c8979d0a1` (#23612) fix: EIP-8037 active 时跳过 tx gas limit cap
- `6b176abc5` (#23369) feat: 暴露 EVM config → `EthTransactionValidator` 增 `evm_config: Evm` 字段
- `33ec89994` (#23196) feat: `TransactionValidationTaskExecutor::spawn`
- `2580304b4` (#22910) refactor: `validate_stateless` 按引用接受 tx
- `7103088ad` (#22559) feat: additional stateless / stateful 自定义校验
- `57148eac9` / `68e4ff1f7` `TaskSpawner` → `Runtime`
- `ccd15e8a2` (#22008) refactor: 重命名/文档化校验方法
- `a87510069` (#22001) refactor: `IntoIter: Send` bound 避免不必要 collect
- `e4b2b1edf` (#21926) feat: `no_eip7702` / `set_eip7702` builder
- `e3c256340` (#21622) feat: `eip7594` toggle 字段
- `1bd8fab88` (#21359) `Block` 关联类型
- `b87cde547` (#21088) EVM execution limits
- 大量 builder / fmt::Debug / `other_tx_types` bitmap / Osaka boundary 处理

**Gravity 侧变更**
- `46c91f90fe` (#343, audit#668) **gravity 关键 patch**：`validate_one` 中对 EIP-7702 交易加 intrinsic gas 检查 + 单元测试 U-4/U-5（gas 不足 reject、Prague 关时短路为 `TxTypeNotSupported`），约 117 行 net add（cite 的 stat 中 `crates/transaction-pool/src/validate/eth.rs | 117 ++++`）
- baseline 历史还有早期 `5df03fb3c3` (#10709) 拒绝空 7702 auth list、`9e3edcc7e9` (#10702) 允许 EIP-7702 delegated 账户 tx 等条目，这些通过 #205 catch-up 已经融入 v1.8.3
- gravity 没有在该文件引入 chainspec 50Gwei 检查（cite #337 显示该逻辑落在 `chainspec` 与 `node.rs` 不在 validator）

**影响范围**：交易准入。`#343` 的 intrinsic gas 检查直接关系到 7702 链上 panic 漏洞（gravity-audit#668 是已发布安全修复）—— **必须保留**。上游 #23196 / #23369 / #21622 / #21359 等是新功能/重构，必须采纳以保证 builder 一致性与 EVM 配置注入。

**解决方案建议**：keep-gravity + mechanical-merge —
- 整体迁到 v2.3.0 上游骨架：新增 `evm_config: Evm` 字段、`other_tx_types: U256` bitmap、`eip7594` 字段、`additional_stateless_validation` / `additional_stateful_validation` 钩子、`Block` 关联类型、`Runtime` / `spawn`、`fmt::Debug` 手写实现、Osaka boundary 处理
- **重新 port** `#343` 中对 `validate_one`（或 v2.3.0 等价方法 `validate_stateless` / `validate_one`）的 EIP-7702 intrinsic gas 检查 + 对应 `ensure_intrinsic_gas` 单元测试（U-4 / U-5）。注意上游 #22910 已把 `validate_stateless` 改成按引用接受 tx，port 时需匹配新签名
- `eip2718` / `eip1559` / `eip4844` / `eip7702` / `max_tx_input_bytes` 等字段访问器全部已对齐上游，可直接采纳上游

**推理**：cite `46c91f90fe` 的 commit message 明确"pre-execution filter mirror pool's ensure_intrinsic_gas"，这是双向防御 —— 如果只在 pipe-exec filter 侧防护、移除 pool 侧 ensure_intrinsic_gas，等价于让 RPC `eth_sendRawTransaction` 接受会令执行层 panic 的 tx；这是 chain-halt 风险面。

**关键发现**：丢失 #343 在 validate/eth.rs 的 117 行 intrinsic gas 校验 → gravity-audit#668 回归，EIP-7702 低 gas tx 可经 RPC 进入 mempool → 推入 pipe-exec → revm-handler panic → 节点 halt（与 PR description 一致）。

### `crates/transaction-pool/src/validate/task.rs`

**模块**：`TransactionValidationTaskExecutor`
**冲突类型**：UU

**上游变更**（v1.8.3..v2.3.0）
- `33ec89994` (#23196) feat: `TransactionValidationTaskExecutor::spawn` —— 一步式构造 + spawn validation 任务到 `Runtime`
- `57148eac9` / `68e4ff1f7` (#22052/#21934) `TaskSpawner` → `Runtime`
- `a87510069` (#22001) refactor: `IntoIter: Send` bound 避免 collect
- `32466fe22` (#21969) `TransactionOrigin` 贯穿
- `b87cde547` (#21088) configurable EVM execution limits → `eth` / `eth_with_additional_tasks` 加 `evm_config: Evm` 入参
- `1bd8fab88` (#21359) `Block` 关联类型
- `47e8f5162` (#19943) fix: spawn ValidationTask to keep channel open

**Gravity 侧变更**
- baseline 该文件没有 chain-critical 自定义；冲突 hunk gravity 侧的 `tasks: T` 泛型、`validate_transactions(Vec<...>)` 签名、`on_new_head_block<B>` 都是 reth v1.8.3 旧版

**影响范围**：纯 API / 异步抽象切换。`Runtime` 取代 `TaskSpawner` 后所有调用方需要传入 `Runtime`（gravity main 已经在 #205 catch-up 后用到上游 task 抽象，无 gravity 自定义 spawner）。

**解决方案建议**：take-upstream — 完整采纳 v2.3.0：`Runtime` 入参、`evm_config: Evm` 注入、`spawn` 一体方法、`IntoIter: Send` bound、`Block` 关联类型、`new(validator)` 返回 `(Self, ValidationTask)` tuple。

**推理**：所有冲突 hunk 都是 upstream 演进，gravity 没有 task spawner 层面的自定义。

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [ ] 1. **PR #343 port 锚点选择**：v2.3.0 中 `validate_one`（或拆分后的 `validate_stateless` / `validate_stateful`）插入 EIP-7702 intrinsic gas 检查的精确位置需要再核 reth 重构后的方法名（cite #22910 / #22008 改名）。port 时优先放进 `validate_stateless`（无 state 依赖），与 `ensure_intrinsic_gas` 同位置即可。
   - [ ] 冲突解决:待核实 port 锚点后落地;crates/transaction-pool/src/validate/eth.rs 现存 47 处冲突块(2026-07-03 实测)。
- [ ] 2. **`maintain.rs` pipe-exec discard 订阅生命周期**：上游 #20781 改了 stale_eviction 处理，gravity discard_txs 订阅块独立于 stale_eviction，互不影响；但需确认 `tokio::spawn` 与 `task_spawner.spawn_blocking_task` 的运行时一致（v2.3.0 全局 Runtime 后建议改用 `task_spawner` 而非裸 `tokio::spawn` 以共享统一 runtime）。
   - [ ] 冲突解决:待决策后落地;crates/transaction-pool/src/maintain.rs 现存 16 处冲突块(2026-07-03 实测)。
- [ ] 3. **`best.rs` MAX_NEW_TRANSACTIONS_PER_BATCH=1024 与上游 `size_hint`**：上游新引入 `size_hint` 返回 `(0, Some(self.all.len()))`（当 `new_transaction_receiver.is_none()` 时）。大批量值与 size_hint 无冲突，但需 bench 验证 imbl::OrdMap 在大批 add_new_transactions 下的实际表现是否仍优于 1024 阈值原始动机（gravity batch-insert 通常一次喂数百 tx）。
   - [ ] 冲突解决:待 bench 验证后落地;crates/transaction-pool/src/pool/best.rs 现存 13 处冲突块(2026-07-03 实测)。
- [ ] 4. **`config.rs` 上游 `LocalTransactionConfig::local_addresses: AddressSet`**：gravity 调用方（如 pipe-exec、CLI）若用 `HashSet<Address>` 字面量构造，迁到 `AddressSet` 需同步调整 import。
   - [ ] 冲突解决:待核实调用方后落地;crates/transaction-pool/src/config.rs 现存 5 处冲突块(2026-07-03 实测)。
