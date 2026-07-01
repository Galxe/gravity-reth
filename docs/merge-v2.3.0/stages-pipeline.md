# stages-pipeline

## 分组概要

- 文件数：17
- 复杂度：高
- baseline anchor：`0cb1687c1c`（gravity-reth main, 2026-06-09, 含 reth v1.8.3 catch-up `d620fd0eeb` PR #205）
- target：reth v2.3.0
- 涉及模块功能：
  - `crates/stages/api/`：pipeline builder、runner（`run_loop` / `execute_stage_to_completion` / `unwind` / `on_stage_error`）、`Stage` / `StageExt` trait。
  - `crates/stages/stages/`：stage set 装配（`DefaultStages` / `OnlineStages` / `OfflineStages` / `ExecutionStages` / `HashingStages` / `HistoryIndexingStages`）；各 stage 实现（`EraStage`、`HeaderStage`、`BodyStage`、`AccountHashingStage`、`StorageHashingStage`、`IndexAccountHistoryStage`、`IndexStorageHistoryStage`、`MerkleStage`、`SenderRecoveryStage`、`TransactionLookupStage`、`PruneStage`）；`stages/mod.rs` 中的集成测试；crate manifest。
- 落在 baseline 上的 gravity-only commits（按本分组涉及文件统计）：
  - `9acbf22633` PR #178 `fix(merkle): Update merkle in trunk when history sync`（`pipeline/mod.rs`，被 PR #246 回滚）
  - `3ee6ac039e` PR #246 `fix(trie): fix merkle stage of history sync`（`pipeline/mod.rs`、`Cargo.toml`、`merkle.rs`、`prune.rs`）
  - `3cd18422c9` PR #134 `use nested state root in history sync`（`merkle.rs` + 多文件 import 牵连）
  - `671680af37` PR #149 `perf(state_root): compact trie node serialization and remove comptiable trie updates`（`Cargo.toml`、`merkle.rs`）
  - `0b4091726c` PR #176 `fix(nested_hash): HashNode for leaf may not have hash`（`merkle.rs`）
  - `1539b6cafc` PR #224 `opt(persist): not write index tables if validator node only`（`index_account_history.rs` 测试、`index_storage_history.rs` 测试、`stages/mod.rs`）
  - `1224ae1846` PR #213 `refactor(parallel): remove the read provider factory that supports parallel reading`（本分组每个文件机械重命名 `<Provider, ProviderRO>` → `<Provider>`，但当前 worktree 看到的 baseline 文件头泛型反而是 `<ProviderRW>` — 见各文件说明）
  - `24f03242db` PR #220 `refactor(fmt): nighlty fmt`（每个文件的格式化噪声）
  - `9974ad0618` PR #241 `fix(test): fix CI test of unit.yml`（`bodies.rs`、`hashing_account.rs`、`hashing_storage.rs`、`merkle.rs`、`mod.rs`、`sender_recovery.rs`、`tx_lookup.rs` 的测试断言；普遍把 `processed` 改成 `_` 因为 RocksDB `count_entries` 用 estimate-num-keys）
  - `a1d7365bd6` PR #212 `feat(rocksdb): Integrating RocksDB into Reth`（仅触动 `Cargo.toml`，不直接改本分组其它文件）
  - `acc458846c` PR #340 `fix(rocksdb): flush batch data into storage to make sure stage is completed`（`UnifiedStorageWriter::commit` 是 rocksdb batch 刷盘承重点；只读不改本分组文件，但 `pipeline/mod.rs` 中必须保留 `UnifiedStorageWriter::{commit, commit_unwind}` 调用）
- 解决顺序依赖：
  - **trie-all-layers** 先解决：`merkle.rs` 用了 `reth_trie_parallel::nested_hash::NestedStateRoot` 与 `provider.write_trie_updatesv2(&trie_updates_v2)`（gravity 独有）。上游 v2.3.0 改用 `reth_trie_db::with_adapter!(provider, |A| DbStateRoot::<_, A>::incremental_root_with_updates(provider, range))`。`merkle.rs` 跟随 trie-all-layers 分组的落地结果。
  - **storage-db-and-mdbx** 先解决：provider trait bound（`StorageSettingsCache`、`ChangeSetReader`、`StorageChangeSetReader`、上游新的 `RocksDBProviderFactory`）+ `StaticFileProvider::check_consistency` 签名（gravity 多一个 `is_full_node: bool`）+ `BlockWriter::insert_block` vs `insert_historical_block` 命名 + `append_block_bodies` 去掉 `StorageLocation`。这些决策驱动 `prune.rs` / `bodies.rs` / `tx_lookup.rs` / `stages/mod.rs` / `hashing_*.rs` / `index_*_history.rs`。
  - **chainspec/consensus** 先解决：`sets.rs` 的 `FullConsensus<E::Primitives, Error = ConsensusError>`（gravity 带显式 error 关联类型）vs `FullConsensus<E::Primitives>`（上游 PR #20843 收掉 `Consensus::Error` 关联类型）来自 `crates/consensus/consensus/src/lib.rs`。

## 逐文件分析

### `crates/stages/api/src/pipeline/builder.rs`
**模块：** `PipelineBuilder<…>` —— 单泛型 builder；`add_stage` / `add_stages` / `with_max_block` / `with_tip_sender`。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `d278b75c3` PR #19923 `chore(stages): fix naming and simplify add_stages implementation` —— `add_stages` 由手写 for-loop `push` 改成 `extend(stages)`，`reserve_exact` 改为 `reserve`，局部变量 `states` → `stages`。doc 注释 `A receiver` → `A Sender`。
- `3c3944459` PR #19655 `fix(stages): correct tip_tx field comment in PipelineBuilder` —— 同上 doc 注释修正。
- 无语义变化。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `1224ae1846` PR #213 把该文件的泛型参数从上游 v1.8.3 的 `<Provider>` 反向重命名为 `<ProviderRW>`（baseline `git show 0cb1687c1c:…/builder.rs` 显示当前是 `pub struct PipelineBuilder<ProviderRW>`，而上游 v1.8.3 已经是 `<Provider>`）。该重命名是纯文本，无语义变化。
**影响范围：** Public API 签名。每个写成 `PipelineBuilder<DatabaseProviderRW<N>>` / `Stage<DatabaseProviderRW<N>>` 的调用点对泛型形参名无感。
**解决方案建议：** 采纳上游 (take-upstream)
**理由：** 上游 PR #19923 是 no-op 重构；gravity baseline 上 PR #213 的 `<ProviderRW>` 是反向命名，无任何语义价值 — 跟随上游对齐回 `<Provider>`，doc 注释也跟上游修正。

### `crates/stages/api/src/pipeline/mod.rs`
**模块：** `Pipeline<N>` —— 驱动 `run_loop` / `unwind` / `execute_stage_to_completion` / `on_stage_error`。
**冲突类型：** UU（10 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `347c1325c` PR #23814 `fix: skip move_to_static_files for storage.v2` —— 在 imports 加入 `DBProvider`、`StorageSettingsCache`。
- `4a6f9cd5c` PR #23335 `fix(provider): cap storage_v2 unwind history by MDBX tip` —— unwind 时把 `self.provider_factory.database_provider_rw()` 换成 `self.provider_factory.unwind_provider_rw()?.disable_long_read_transaction_safety()`。
- `294e21507` PR #22995 `fix(provider): heal finalized/safe block numbers ahead of highest header` —— 在 unwind commit 块里新增 `last_saved_safe_block_number` 保存路径（注释从 "finalized block" 改为 "finalized and safe block"），同时把 `UnifiedStorageWriter::commit_unwind(provider_rw)?` 改为 `provider_rw.commit()?`（上游已删除 `UnifiedStorageWriter` writer 包装）。
- `12cf3d685` PR #21311 `fix(provider): add CommitOrder for RocksDB/MDBX unwind atomicity` —— 把 `unwind` 入口对 `provider`、`prune_modes`、`checkpoints` 的取值收进一个 RAII 作用域块（避免长读事务跨越后续 unwind 循环）。
- 上游同步把 `MissingStaticFileData` handler 里 `block.block.number - 1` 改为 `block.block.number.saturating_sub(1)`，避免 `block.number == 0` 时整数下溢。
- 上游同步把 `Validation` 分支无条件 reset `MerkleExecute` 收窄为 `if stage_id == StageId::MerkleExecute` 时才执行。
- 上游同步把 `stage(idx)` 的签名从 `&mut dyn Stage<DatabaseProviderRW<N>>` 改为 `&mut dyn Stage<<ProviderFactory<N> as DatabaseProviderFactory>::ProviderRW>`（等价于内部 type alias 展开）。
- 上游同步把 execute commit 路径的 `UnifiedStorageWriter::commit(provider_rw)?` 改为 `provider_rw.commit()?`，并且**没有** `let start = Instant::now()` + `execute_duration_ms` 日志。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：**
- `9acbf22633` PR #178 引入 `SYNC_BATCH_SIZE: u64 = 10000` + `run_batch` 局部 target 循环，把 `to_block: Option<u64>` 透传到 `execute_stage_to_completion`，**已被** `3ee6ac039e` PR #246 反向回滚（baseline 上 `SYNC_BATCH_SIZE` 和 `run_batch` 都已经不存在 —— 在 `0cb1687c1c:.../pipeline/mod.rs` 中 grep 不到这两个符号）。
- `3ee6ac039e` PR #246 在 execute 路径加入 `let start = Instant::now()`（行 ~476）+ commit 后的 `info!(target: "sync::pipeline", stage = %stage_id, prev_block, exec_output, execute_duration_ms = start.elapsed().as_millis(), "Stage has executed, …")`（行 ~483），并保留 `UnifiedStorageWriter::commit(provider_rw)?`。
- baseline 保留 `UnifiedStorageWriter::commit_unwind(provider_rw)?`（行 ~392）和 `UnifiedStorageWriter::commit(provider_rw)?`（行 ~483 / ~589）—— `acc458846c` PR #340 依赖该 writer hook 把 rocksdb `WriteBatchWithIndex` 刷盘，没有它 staged-sync 写入永远停留在内存中。
- baseline 中 `Validation` 分支**无条件** reset `MerkleExecute` 检查点（行 ~580 起），不带 `stage_id == StageId::MerkleExecute` 保护 —— 因为 gravity merkle 写 `TrieUpdatesV2`，任何 stage 失败都可能让 V2 trie 偏离已保存 checkpoint。
- baseline 中 `MissingStaticFileData` 分支为 `block.block.number - 1`（无 `saturating_sub`，行 627）。
- baseline 中 `stage(idx)` 签名为 `&mut dyn Stage<DatabaseProviderRW<N>>`（保留 type alias）。
- baseline 中 unwind provider 构造为 `self.provider_factory.database_provider_rw()?`，没有 `disable_long_read_transaction_safety()`。
**影响范围：** 影响每次节点启停。混合错误会导致 rocksdb 写入丢失（`acc458846c` 修复）或者 unwind 边界一字节偏差。破坏风险：高。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- imports 块：保留 gravity 的 `writer::UnifiedStorageWriter` import（commit_unwind/commit 调用点依赖），同时采纳上游新增的 `DBProvider`、`StorageSettingsCache`（后续 unwind 路径所需）。
- `stage(idx)` 签名块（行 ~128）：采纳上游展开形式 `<ProviderFactory<N> as DatabaseProviderFactory>::ProviderRW`，与 gravity `DatabaseProviderRW<N>` type alias 完全等价；表面变化无副作用。
- `unwind` 入口 provider 作用域块（行 ~317）：采纳上游的 RAII let-bound `(latest_block, prune_modes, checkpoints) = { let provider = …; (…) };` —— `12cf3d685` 的 commit-order 修复必须落地。
- unwind provider 构造（行 ~347）：采纳上游 `self.provider_factory.unwind_provider_rw()?.disable_long_read_transaction_safety()`（`4a6f9cd5c` storage v2 unwind cap 修复）。对 gravity rocksdb 后端 `disable_long_read_transaction_safety` 应为 no-op 或等价占位；若 rocksdb provider 上未实现该方法，本分组无法独立完成 — 必须等 storage-db-and-mdbx 落地后补适配。
- finalized 注释 + safe-block 保存（行 ~411 + ~429）：采纳上游 `294e21507` 的 safe-block 保存路径与注释更新。
- commit_unwind 调用（行 ~432）：**保留 gravity** `UnifiedStorageWriter::commit_unwind(provider_rw)?`（rocksdb 承重点）；安全地紧贴上面新加的 safe-block 保存块。
- execute 路径 `let start = Instant::now()`（行 ~476）：**保留 gravity**（PR #246 marker）。
- execute commit + 日志（行 ~481）：**保留 gravity** `UnifiedStorageWriter::commit(provider_rw)?` + `info!(…, execute_duration_ms, "Stage has executed, …")`（PR #246 marker；`UnifiedStorageWriter::commit` 是 rocksdb batch 刷盘承重点）。
- Validation 分支 reset MerkleExecute（行 ~580）：**保留 gravity** 无条件 reset（gravity 把 merkle 绑定到 TrieWriterV2 状态，任何 validation 失败都可能破坏它）。
- `block.number - 1`（行 ~627）：采纳上游 `saturating_sub(1)`（正确性，gravity 早晚自己也要打这个 patch）。

### `crates/stages/api/src/stage.rs`
**模块：** `Stage<Provider>` trait、`StageExt<Provider>` 扩展 trait。
**冲突类型：** UU（1 个尾部冲突块）
**上游变更（v1.8.3 → v2.3.0）：** 文件末尾加入 `#[cfg(test)] mod tests`，包含单测 `test_exec_input_next_block_range_with_transaction_threshold` —— 使用上游 storage-v2 的 `ProviderFactory::<MockNodeTypesWithDB>::new(create_test_rw_db(), MAINNET.clone(), StaticFileProviderBuilder::read_write(...).with_blocks_per_file(1).build().unwrap(), RocksDBProvider::builder(create_test_rocksdb_dir().0.keep()).build().unwrap(), reth_tasks::Runtime::test())`（5 个 `new` 参数，含上游 storage-v2 的 `RocksDBProvider`）。`662c0486a` PR #20253 `feat(storage): add rocksdb provider into database provider`、`95b8a8535` PR #19662 等多个 PR 累积引入。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** 仅 `1224ae1846` 重命名 + `24f03242db` 格式化 + `3cd18422c9` import 牵连（HEAD 一半到 `impl<Provider, S: Stage<Provider> + ?Sized> StageExt<Provider> for S {}` 之后即收尾，无 mod tests）。
**影响范围：** 无生产代码冲突 —— 仅一个测试函数差异。但上游 mod tests 引入的 `RocksDBProvider`、`StaticFileProviderBuilder`、`reth_tasks::Runtime` 都需要 gravity-side 适配（gravity 的 `ProviderFactory::new` 元数不同，rocksdb provider 接入方式不同）。
**解决方案建议：** 保留 gravity 侧 (keep-gravity) —— 丢弃上游 mod tests
**理由：** 该 mod tests 的 `ProviderFactory::new` 5 参形态对应上游 storage-v2 的 rocksdb 接入；gravity 的 `ProviderFactory::new` 元数自 `a1d7365bd6` PR #212 RocksDB 集成以来与上游分歧，强 port 会触发 storage-db-and-mdbx 级联编译错误。该测试丢失不影响生产路径。开 open question 跟踪后续是否补 port。

### `crates/stages/stages/Cargo.toml`
**模块：** stages crate manifest。
**冲突类型：** UU（11 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `8bb96ace6` PR #23158 `refactor: remove SerdeBincodeCompat trait, use RLP for block serialization` —— 去掉 `reth-primitives-traits` 上的 `serde-bincode-compat` feature。
- `7551d9c5d` PR #23156 `refactor: remove bincode usage from HeaderStage`（与上游 `headers.rs` 联动）。
- `b9969c5b1` PR #22954 `chore: remove rocksdb and edge feature gates, default to storage v2` —— 加入 `reth-libmdbx.workspace = true`、`reth-tasks.workspace = true`，把 `reth-trie = { workspace = true, features = ["metrics"] }` 提升为必选，加入 `reth-trie-db = { workspace = true, features = ["metrics"] }`。dev-deps `reth-db` 加 `mdbx` feature。
- `598f228e2` PR #22627 `chore: remove criterion benchmarks and codspeed` —— 删除 `criterion` dev-dep 与 `[[bench]]` 块。
- `815037e27` PR #22379 `feat(storage): slot preimage DB for plain changeset keys in v2` —— 加入 `page_size.workspace = true`。
- `00f9bd2a9` PR #24494 `fix: use tx_hash for transaction identity` —— dev-deps 重组：`reth-downloaders` 加 `file-client` feature，加入 `reth-storage-api`、`alloy-genesis`、`alloy-eips`、`reth-db-common`。
- 主线把 `alloy-rlp` 从 dev-dep 提升为 dep。
- `[features].test-utils` 中 `reth-chainspec/test-utils`（无 `?`）和 `reth-trie-db/test-utils`、`reth-tasks/test-utils` 被加入。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：**
- `a1d7365bd6` PR #212 RocksDB 集成 —— 影响该 manifest 的 dep 列表（pulls in rocksdb provider 间接依赖）。
- `671680af37` PR #149 —— 把 `reth-trie` 改为 optional。
- `3ee6ac039e` PR #246 —— 加入 `reth-trie = { workspace = true, optional = true }`，并让 `reth-trie-parallel` feature 拉入 `reth-trie`：`reth-trie-parallel = ["dep:reth-trie-parallel", "dep:reth-trie"]`；定义 `default = ["reth-trie-parallel"]`。
- baseline 保留 `bincode.workspace = true` 与 `reth-primitives-traits = { workspace = true, features = ["serde-bincode-compat"] }`（用于 `headers.rs` 的 bincode ETL 路径）。
- baseline 保留 `reth-downloaders.workspace = true`（无 `file-client` feature）。
- baseline 保留 `criterion = { workspace = true, features = ["async_tokio"] }` dev-dep 与文件末尾 `[[bench]] name = "criterion"` 块。
- baseline `reqwest = { workspace = true, default-features = false, features = ["rustls-tls-native-roots", "blocking"] }` —— 为 era downloader 显式锁定 rustls TLS。
- baseline `[features].test-utils` 使用 `dep:reth-chainspec` + `reth-chainspec?/test-utils` optional-prefix 形式（因 `reth-chainspec` 在 dev-deps 中是 optional），同时含 `reth-trie-parallel/test-utils`、`reth-trie-db/test-utils`。
**影响范围：** Crate 编译。上游移除 bincode 而 gravity `headers.rs` baseline 仍依赖 `bincode::deserialize::<serde_bincode_compat::SealedHeader>(…)`；如果 `headers.rs` 决策与 manifest 不配套会编译失败。
**解决方案建议：** 机械合并 (mechanical-merge) —— 与 `headers.rs` 决策配套
**理由：**
- 保留 gravity `reth-trie = { workspace = true, optional = true }` + `reth-trie-parallel` feature 拉入 `reth-trie`（PR #246 marker，必须的，因为 `merkle.rs` 决策保留 `NestedStateRoot`）。
- **与 headers.rs 决策配套**：如果 `headers.rs` 采纳上游 RLP（推荐），则去掉 `bincode.workspace = true` 与 `reth-primitives-traits` 的 `serde-bincode-compat` feature。
- 采纳上游 `reth-libmdbx.workspace = true`、`reth-tasks.workspace = true`、`page_size.workspace = true`、`alloy-rlp.workspace = true`、`reth-trie-db = { workspace = true, features = ["metrics"] }`。
- 保留 gravity `reqwest = { workspace = true, default-features = false, features = ["rustls-tls-native-roots", "blocking"] }`（gravity 构建环境绑 rustls）。
- 保留 gravity `[features] default = ["reth-trie-parallel"]` + `reth-trie-parallel = ["dep:reth-trie-parallel", "dep:reth-trie"]` 块。
- 采纳上游 dev-dep 增加：`alloy-genesis`、`alloy-eips`、`reth-db-common`、`reth-storage-api`、`reth-downloaders` 加 `file-client` feature。
- 保留 gravity `criterion` dev-dep 与 `[[bench]]` 块（gravity 仍有活跃 stage 基准，由 bench 分组确认）。
- 保留 gravity 的 `reth-chainspec?/test-utils` optional-prefix 形式 + `reth-trie-parallel/test-utils` 行。

### `crates/stages/stages/src/sets.rs`
**模块：** Stage-set 装配 —— `DefaultStages` / `OnlineStages` / `OfflineStages` / `ExecutionStages` / `HashingStages` / `HistoryIndexingStages`。
**冲突类型：** UU（11 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `412f39e22` PR #20843 `chore(consensus): Remove associated type Consensus::Error` —— 所有 `FullConsensus<E::Primitives, Error = ConsensusError>` 收为 `FullConsensus<E::Primitives>`，imports 去掉 `ConsensusError`。
- `7efaf4ca9` PR #20836 + `020eb6ad7` PR #19351 —— `EraStage::new` 改为条件插入：`if self.era_import_source.is_some() { builder = builder.add_stage(EraStage::new(self.era_import_source, …)); }`。
- `352430cd8` PR #21918 `fix: skip sender recovery stage when senders fully pruned` —— `OfflineStages` 新增 `sender_recovery_prune_mode: Option<PruneMode>` 字段，透传到 `ExecutionStages::new(.., sender_recovery_prune_mode)`；`PruneStage` 改为无条件 `add_stage`（去掉原本的 `add_stage_opt(self.prune_modes.is_empty().not().then(...))`）。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** 仅 `1224ae1846` 形成的 `Provider`/`ProviderRO` 重命名牵连；本文件无 gravity-specific 业务改动。
**影响范围：** Public stage-set 构造 API。所有 node builder 都要匹配 `FullConsensus<…>` 的新签名以及 `OfflineStages::new` / `ExecutionStages::new` 元数。
**解决方案建议：** 采纳上游 (take-upstream)
**理由：** 自 v1.8.3 catch-up 以来 gravity 无业务改动。`FullConsensus<…, Error = ConsensusError>` → `FullConsensus<…>` 由 chainspec/consensus 分组下游决定 — 默认采纳；`sender_recovery_prune_mode` 是上游新功能管线，无 gravity-specific 反对。

### `crates/stages/stages/src/stages/bodies.rs`
**模块：** `BodyStage<Downloader>` —— `provider.append_block_bodies(...)` 追加 block bodies，unwind 时 `remove_bodies_above(...)`。
**冲突类型：** UU（6 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `b9969c5b1` PR #22954 / `058ffdc21` PR #18681 / `96c77fd8b` PR #20504 —— 去掉 `StorageLocation` 枚举。`provider.append_block_bodies(.., StorageLocation::StaticFiles)` → `provider.append_block_bodies(buffer.iter().map(|r| (r.block_number(), r.body())).collect())`；`remove_bodies_above(unwind_to, StorageLocation::Both)` → `remove_bodies_above(unwind_to)`。
- `563ae0d30` PR #16660 `fix: drop support for total difficulty table` —— 测试 setup 把 `insert_headers_with_td` 切换为 `insert_headers`。
- `39ef6216f` PR #19508 —— 测试 setup 包装 `if let Some((header, hash)) = …`（cursor API 返回 Option）。
- `f53f90d71` PR #21686 —— `alloy_primitives::{Address, B256}` import 加入 `map::B256Map`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `9974ad0618` PR #241 把 mod tests 中多个 `processed == batch_size + 1` 断言改为 `processed: _`，加注释 `// RocksDB can't see uncommitted writes in count_entries`（rocksdb 不像 mdbx 那样能从未提交的 view tx 中看到自己的写入）。
**影响范围：** 编译 + 测试断言。要匹配 storage-db-and-mdbx 分组的 `BlockWriter::append_block_bodies(impl IntoIterator)` 与 `remove_bodies_above(BlockNumber)` 新签名。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- 生产代码部分（imports、`append_block_bodies`、`remove_bodies_above`）：采纳上游 —— 去掉 `StorageLocation`。gravity 没有要保留 `StorageLocation` 语义的业务理由；`BlockWriter` trait 是否真去掉 `StorageLocation` 参数由 storage-db-and-mdbx 分组定，本分组跟随。
- 测试 setup（`insert_headers_with_td` → `insert_headers`、`SealedHeader::new` Some/None 包装、`B256Map` import）：采纳上游 —— gravity 已合入 disable-PoW-rewards，TD 表对 gravity 无意义。
- mod tests 断言部分：**保留 gravity** PR #241 的 `processed: _` + 注释（gravity rocksdb 不能从未提交的 view 里看到自己的写入 — 是真实运行时差异，上游 mdbx 默认能看见）。

### `crates/stages/stages/src/stages/era.rs`
**模块：** `EraStage<H>` —— 把合并前 era1 文件导入 static files + ETL 收集器。
**冲突类型：** AA（两侧独立新增；merge base `75b7172cf7` 早于该文件，gravity 是从 `6b71a11f88` reth v1.5.0 catch-up 携带，upstream 是 `3218b3c63` PR #16008 引入）
**上游变更（v1.8.3 → v2.3.0）：**
- `2ba17cf10` PR #19520 `refactor(era): move era types and file handling to new module` —— 模块路径重排：`reth_era::era1_file::Era1Reader` → `reth_era::era1::file::Era1Reader`；`reth_era::era_file_ops::StreamReader` → `reth_era::common::file_ops::StreamReader`。
- `563ae0d30` PR #16660 + `e21048314` PR #19151 —— 移除 TD 管线：`static_file_provider.header_td_by_number(...)` 调用去掉，era 导入助手的 `&mut td` 参数去掉，测试断言去掉 TD 校验。`HeaderProvider` import 去掉。
- `00f173307` PR #19000 `fix: Set Era pipeline stage to last checkpoint when there is no target` —— 无 era 文件时返回 `max(checkpoint, highest_header, target)`，`done` 条件放宽。
- `020eb6ad7` PR #19351 + `7b2fbdcd5` PR #20516 —— 相关结构清理；imports 中去掉 `ProviderError`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** 仅 `1224ae1846` 重命名 + `24f03242db` 格式化 + `3cd18422c9` import 牵连。gravity "ours" 版本 = v1.5.0 时期形态（pre-PR #19520 / pre-#16660）。
**影响范围：** Era1 文件导入对 gravity 链 post-merge 启动**无业务意义**（无 pre-Merge header 需导入），但该 stage 仍要能编译，因为 `sets.rs` 把它装配进 `DefaultStages`（由 `era_import_source: Option<…>` 门控）。
**解决方案建议：** 采纳上游 (take-upstream)
**理由：** 自 v1.8.3 catch-up 以来 gravity 无业务改动。模块路径重命名 + TD 移除 + "no era files" fallback 均是上游严格改进；与 gravity 已合入 PR #293（disable PoW rewards）方向一致。`sets.rs` 采纳上游的 `if let Some(era_import_source)` 条件插入后，era.rs 在 gravity 部署中实际不会运行。

### `crates/stages/stages/src/stages/hashing_account.rs`
**模块：** `AccountHashingStage` —— 把 account changesets 回放进 `tables::HashedAccounts`。
**冲突类型：** UU（6 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `e3fe6326b` PR #22042 + `ec982f868` PR #22206 —— imports 加入 `provider::StorageSettingsCache`，`reth_stages_api` 加入 `BlockRangeOutput`。
- `936baf123` PR #19176 `refactor: remove FullNodePrimitives` —— 测试约束 `FullNodePrimitives` → `NodePrimitives`。
- `96c77fd8b` PR #20504 `feat(storage): make insert_block() operate with references` —— 测试 setup `provider.insert_historical_block(...)` → `provider.insert_block(&...)`；imports 加入 `BlockWriter`。
- `unwind_account_hashing_range` 调用前去掉旧注释 `// Aggregate all transition changesets …`。
- 测试断言加上 `processed == total &&` 前置条件，`runner.db.table::<…>().unwrap().len()` → `runner.db.count_entries::<…>().unwrap()`（test-utils 访问器改名）。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `9974ad0618` PR #241 把测试断言中的 `processed: _` 替换 `processed == total`（与 bodies.rs 同理）。无生产代码业务改动。
**影响范围：** 仅编译 + 测试。`insert_historical_block` vs `insert_block(&recovered)` 的命名由 storage-db-and-mdbx 分组定。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- 生产路径（imports、trait bounds、`unwind_account_hashing_range` 调用、注释删除）：采纳上游。
- 测试 setup `provider.insert_historical_block(...)` vs `provider.insert_block(&...)`：跟随 storage-db-and-mdbx 决策（默认保留 baseline 的 `insert_historical_block` —— gravity 的 `DatabaseProvider` 在 `crates/storage/provider/src/providers/database/provider.rs` 仍有该方法）。
- 测试 trait bound `FullNodePrimitives` → `NodePrimitives`：采纳上游放宽。
- 测试断言 `processed == total &&`：**保留 gravity** `processed: _` 形态（PR #241 marker — gravity rocksdb 限制）。

### `crates/stages/stages/src/stages/hashing_storage.rs`
**模块：** `StorageHashingStage` —— 把 storage slots 哈希进 `tables::HashedStorages`。
**冲突类型：** UU（5 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `d8de8afa9` PR #22721 `fix(stages): bound storage hashing stages memory` —— 给 in-flight buffer 加上界。
- `bd476289f` 间接 / `815037e27` PR #22379 / `effa0ab4c` PR #21528 —— slot preimage DB；imports 加入 `b256!`、`Address`。
- `121160d24` PR #21115 + 上游 provider 简化：`unwind_storage_hashing_range(BlockNumberAddress::range(range))` → `unwind_storage_hashing_range(range)`（接受裸 `RangeInclusive<BlockNumber>`）。
- 测试 hash collector 改为按块的 `tx_hash_numbers: Vec<(B256, u64)>` 批量插入 + `processed == total` 断言。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `9974ad0618` PR #241 把 mod tests 断言 `processed == total` 改为 `processed: _, total: _`，附注 `// NOTE: Due to RocksDB limitation where count_entries uses estimate-num-keys which may not match actual count from cursor iteration, we only verify checkpoint structure exists.`；同时把 `while let Some((address, entry)) = storage_cursor.next()?` 改为 `storage_cursor.first()?` 定位 + `next()` 推进的循环（rocksdb cursor 行为差异）。
**影响范围：** Provider 调用点签名要匹配 storage-db-and-mdbx 分组的 `unwind_storage_hashing_range` 元数；测试路径有 gravity-specific cursor 处理。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- 生产路径（imports `b256!`、上界 buffer、slot preimage 处理）：采纳上游。
- `unwind_storage_hashing_range` 调用形态：跟随 storage-db-and-mdbx 决策（默认采纳上游裸 `range`，若 provider 仍要 `BlockNumberAddress::range(range)` 则回退该调用）。
- mod tests 断言：**保留 gravity** PR #241 形态（`processed: _, total: _` + 注释）。
- mod tests cursor 循环：**保留 gravity** `storage_cursor.first()?` + `current = storage_cursor.next()?` 模式（rocksdb cursor 行为；本分组无法独立改 cursor 语义）。
- 测试 setup `tx_hash_numbers` 批量插入：采纳上游（不写入 `tables::TransactionHashNumbers` cursor 时同样能 build 起测试 fixture）。

### `crates/stages/stages/src/stages/headers.rs`
**模块：** `HeaderStage<Provider, Downloader>` —— 下载 headers、ETL 收集、写入 static files。
**冲突类型：** UU（12 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `7551d9c5d` PR #23156 `refactor: remove bincode usage from HeaderStage` —— `bincode::deserialize::<serde_bincode_compat::SealedHeader<…>>(&header_buf)` → `SealedHeader::new_unhashed(Decodable::decode(&mut header_buf.as_slice())…)`；collector value 注释从 `BincodeSealedHeader` 改为 `RLP-encoded SealedHeader`；写入路径 `bincode::serialize(&serde_bincode_compat::SealedHeader::from(&header))` → `alloy_rlp::encode(&*header)`。
- `8bb96ace6` PR #23158 —— 同方向，去掉 `serde_bincode_compat` import。
- `e21048314` PR #19151 + `563ae0d30` PR #16660 —— `writer.append_header(header, td, header_hash)` → `writer.append_header(header, header_hash)`；去掉 `// Increase total difficulty` 块；测试去掉 `provider.header_td_by_number` 断言。
- `ff8ac97e3` PR #21258 `fix(stages): clear ETL collectors on HeaderStage error paths` —— 把内联 `self.sync_gap = None` 抽成辅助函数 `self.clear_etl_state()`（错误路径同时清 ETL）。
- imports 去掉 `serde_bincode_compat`、`HeaderSyncGapProvider`、`ProviderError`；加入 `HeaderTy`、`alloy_rlp::Decodable`。
- 测试 setup `random_header_range(.., tip.number..tip.number + 10, ..)` → `tip.number + 1..tip.number + 10`（修复 off-by-one），unwind 测试改用 `provider.database_provider_rw()` + 直接 static file writer 而不是 `append_blocks_with_state`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** 仅 `1224ae1846` 重命名 + `24f03242db` 格式化 + `3cd18422c9` import 牵连。本文件无 gravity-specific 业务改动。
**影响范围：** ETL collector 序列化格式变化（bincode → RLP）。仅在升级时磁盘上残留半成品 ETL 临时目录才有兼容性问题；gravity 启动是全新开始 + reth 上游已在启动时清 ETL（PR #16770），需核对 gravity 是否同步。
**解决方案建议：** 采纳上游 (take-upstream)
**理由：** gravity 在本文件上无业务改动。bincode → RLP 是无 gravity-specific 反对意见的纯改进；TD 移除与 gravity 的 disable-PoW-rewards 方向一致；`clear_etl_state()` 是正确性改进。`Cargo.toml` 决策需与本文件配套去掉 `bincode` + `serde-bincode-compat`。

### `crates/stages/stages/src/stages/index_account_history.rs`
**模块：** `IndexAccountHistoryStage` —— 根据 changesets 构建 `tables::AccountsHistory` shard 索引。
**冲突类型：** UU（3 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `bd144a4c4` PR #21165 + `a0df56111` PR #21334 + `b81489322` PR #21367 + `ab418642b` PR #21374 + `e3fe6326b` PR #22042 + `b9969c5b1` PR #22954 —— 上游把 collect/load 逻辑搬到专门的 `collect_account_history_indices` / `load_account_history`，加入 `provider.with_rocksdb_batch_auto_commit(|rocksdb_batch| { let mut writer = EitherWriter::new_accounts_history(provider, rocksdb_batch)?; load_account_history(collector, first_sync, &mut writer)?; … })` + `if use_rocksdb { provider.commit_pending_rocksdb_batches()?; provider.rocksdb_provider().flush(&[Tables::AccountsHistory.name()])?; }`。注意：**`with_rocksdb_batch_auto_commit` 是上游 v2.3.0 storage-v2 自己实现的 rocksdb API，与 gravity 的同名方法非同源**。
- imports 加入 `EitherWriter`、`RocksDBProviderFactory`、`StorageSettingsCache`、`Tables`；去掉 `alloy_primitives::Address`、`reth_db_api::table::Decode`。
- 测试 imports 加入 `Address`（用于新测试数据构造）。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `1539b6cafc` PR #224 只动了 mod tests（把 `stage.execute(&provider, Box::new(move || factory.database_provider_ro()), input)` 这种三参形态收回到 `stage.execute(&provider, input)` 两参形态，因为 gravity 自己的 read provider factory 在 PR #213 之后已去除）。**baseline 生产路径中无 `with_rocksdb_batch_auto_commit` 调用 —— 用 `git show 0cb1687c1c:crates/stages/stages/src/stages/index_account_history.rs` 核对，execute body 仍是 `collect_history_indices` + `load_history_indices::<_, tables::AccountsHistory, _>`，没有 validator-only 判断、没有 `GravityConfig::disable_index_tables`、没有 rocksdb-batch 包装。**
**影响范围：** Index 写入路径。上游引入 `EitherWriter`、`RocksDBProviderFactory`、`Tables` 全部依赖 storage-db-and-mdbx 分组的 provider 提供。
**解决方案建议：** 跟随 storage-db-and-mdbx (needs-port / take-upstream-after-port)
**理由：** baseline 在本文件**没有** gravity-specific 业务改动（PR #224 只动了测试调用形态）；可以直接采纳上游写法，但前提是 gravity 的 `DBProvider` 实现 `with_rocksdb_batch_auto_commit`、`commit_pending_rocksdb_batches`、`rocksdb_provider().flush(...)`、`RocksDBProviderFactory`、`EitherWriter::new_accounts_history` 这一整套上游 storage-v2 trait/类型 — 这是 storage-db-and-mdbx 分组的工作。如果 storage-db-and-mdbx 决定保留 gravity 自己的 rocksdb 接入（不抄上游 `RocksDBProviderFactory`），本文件需要回退到 baseline 的 `load_history_indices::<_, tables::AccountsHistory, _>(provider, collector, first_sync, ShardedKey::new, ShardedKey::<Address>::decode_owned, |key| key.key)?` 形态。测试调用形态：保留 gravity 的两参形式（PR #224 marker）。

### `crates/stages/stages/src/stages/index_storage_history.rs`
**模块：** `IndexStorageHistoryStage` —— 构建 `tables::StoragesHistory` shard 索引。
**冲突类型：** UU（3 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：** 形态与 `index_account_history.rs` 一致 —— `collect_storage_history_indices` + `load_storage_history`、`with_rocksdb_batch_auto_commit` + `EitherWriter::new_storages_history`、`RocksDBProviderFactory` trait bound、`provider.unwind_storage_history_indices_range(BlockNumberAddress::range(range))` → `provider.unwind_storage_history_indices_range(range)`。imports 去掉 `table::Decode`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** 同 `index_account_history.rs` —— `1539b6cafc` PR #224 只动测试调用形态；baseline 生产路径无 `with_rocksdb_batch_auto_commit`、`disable_index_tables`、`validator-only` 等 gravity-specific 路径。baseline 仍保留 `provider.unwind_storage_history_indices_range(BlockNumberAddress::range(range))` 形态。
**影响范围：** 与 `index_account_history.rs` 同。
**解决方案建议：** 跟随 storage-db-and-mdbx (needs-port / take-upstream-after-port)
**理由：** 同 `index_account_history.rs`。`unwind_storage_history_indices_range` 的参数形态由 storage-db-and-mdbx 落地决定 —— 默认采纳上游裸 `range`，若 provider 仍要 `BlockNumberAddress::range(range)` 则保留 baseline 形态。

### `crates/stages/stages/src/stages/merkle.rs`
**模块：** `MerkleStage` —— 计算中间 state root，写入 `tables::AccountsTrieV2` / `StoragesTrieV2`。
**冲突类型：** UU（13 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `80bf5532a` PR #22158 `perf(trie): pack StoredNibblesSubKey from 65→33 bytes, generic cursor factory` —— 重构 trie cursor factory；引入 `reth_trie_db::with_adapter!(provider, |A| { DbStateRoot::<_, A>::… })` 宏，trie provider 改为 adapter-bound 形式。
- `b9969c5b1` PR #22954 + `e3fe6326b` PR #22042 —— `Stage<Provider>` trait bound 新增 `ChangeSetReader + StorageChangeSetReader + StorageSettingsCache`；imports 加入这三个 + `KECCAK_EMPTY`、`IntermediateStateRootState`、`StateRoot`、`StateRootProgress`、`StoredSubNode`、`reth_trie_db::DatabaseStateRoot`、`StorageRootMerkleCheckpoint`。去掉 `cursor::DbCursorRO`、`HashedPostState`、`HashedStorage`、`EMPTY_ROOT_HASH`、`NestedStateRoot`。
- `52a259237` PR #24267 `fix(stages): fix off-by-one bug` —— 增量 chunk 推进的 off-by-one 修复。
- 增量循环改写为 `for start_block in range.step_by(incremental_threshold as usize) { let chunk_to = std::cmp::min(start_block + incremental_threshold - 1, to_block); … reth_trie_db::with_adapter!(provider, |A| { DbStateRoot::<_, A>::incremental_root_with_updates(provider, chunk_range) })?; provider.write_trie_updates(updates)?; }`，并在循环后强制 `let final_root = final_root.ok_or(StageError::Fatal("Incremental merkle hashing did not produce a final root".into()))?;`。
- "全量重建"路径改用上游的 checkpoint 序列化 + storage root state（含 `StorageRootMerkleCheckpoint`），`provider.write_trie_updates(updates)?`。
- 上游写日志用 `debug!`，gravity 用 `info!`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：**
- `3cd18422c9` PR #134 `use nested state root in history sync` —— 引入 `NestedStateRoot::new(provider.tx_ref(), None).calculate(&hashed_state)`，把 trie 重建分为"chunk-by-chunk 重建" + "增量更新"两条路径。
- `671680af37` PR #149 —— 移除与上游 TrieUpdates V1 的兼容；gravity 改用 `write_trie_updatesv2(&trie_updates_v2)` + `tables::AccountsTrieV2` / `tables::StoragesTrieV2` + `commit_view()`。
- `0b4091726c` PR #176 `fix(nested_hash): HashNode for leaf may not have hash` —— 边角修复。
- `3ee6ac039e` PR #246 —— 最终的 history-sync merkle 修复（与 `pipeline/mod.rs` 配套）。
- baseline 生产路径用 `HashedPostState` / `HashedStorage` 做手工 walk_account 循环（不是上游基于 `range` 的 `incremental_root_with_updates`）；trait bound 含 `TrieWriterV2`；log target 为 `"sync::stages::merkle::exec"`，消息为 `"Rebuilding trie from hashed state"` / `"Incremental updating trie in chunks"`。
**影响范围：** 核心 state root 计算 —— 直接决定共识 state_root 字段。混合会造成 chain-halt（一字节偏差即停链）。
**解决方案建议：** 保留 gravity 侧 (keep-gravity) —— 顺序依赖 trie-all-layers 分组
**理由：** 四个 gravity-marker commits（`3cd18422c9` / `671680af37` / `0b4091726c` / `3ee6ac039e`）落在 baseline 上构成承重路径。`NestedStateRoot` / `TrieUpdatesV2` / `write_trie_updatesv2` / `commit_view` 全是 gravity 独有的符号，必须保留。但本文件依赖的符号是否还存在由 trie-all-layers 分组决定 —— 如果 trie-all-layers 采纳上游 `with_adapter!` + `DbStateRoot::<_, A>` 并移除 `NestedStateRoot`，本文件必须按上游重写（gravity 共识改打 v1 trie 才能保 PR #149 之前的兼容）。**这是 chain-halt 关键路径，必须等 trie-all-layers 给出明确结论后再决定。**

### `crates/stages/stages/src/stages/mod.rs`
**模块：** `stages` 模块 —— re-exports + 集成测试。
**冲突类型：** UU（8 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `a0df56111` PR #21334 + `a74cb9cbc` PR #20997 + `96c77fd8b` PR #20504 —— 测试 imports 加入 `reth_db::mdbx::{cursor::Cursor, RW}`、`BlockWriter`；`provider_rw.insert_historical_block(genesis.try_recover().unwrap())` → 拆成一个跟踪 `head` 变量的循环用 `provider_rw.insert_block(&block.try_recover().unwrap())`（注意上游把 `let mut head = block.hash();` 提到循环上方）。
- `static_file_provider.check_consistency(&provider, is_full_node)` → `check_consistency(&provider)`（上游去掉 `is_full_node` bool）。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：**
- `1539b6cafc` PR #224 大量动测试（baseline 中 `git show 0cb1687c1c:crates/stages/stages/src/stages/mod.rs | grep -n is_full_node` 仍看到 `is_full_node: bool` 参数和四处 `check_consistency(&provider, is_full_node)` / `check_consistency(&provider, false)` 调用；`provider_rw.insert_historical_block(…)` 在第 94/95/107 行仍是原形态）。
- `is_full_node: bool` 是 gravity 区分 archive/full 与 validator-only 的 runtime 标志，传入生产路径 `crates/storage/provider/src/providers/static_file/manager.rs::check_consistency` —— 那个签名决策属于 storage-db-and-mdbx 分组。
**影响范围：** 仅集成测试 —— 但 `check_consistency(provider, is_full_node)` 这个参数透传到生产 manager.rs 的签名，是承重决策点。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- 测试 imports：丢掉上游的 `reth_db::mdbx::{cursor::Cursor, RW}` —— gravity 测试不构造 mdbx-cursor 直接句柄。保留 gravity imports 形态。
- `provider_rw.insert_historical_block` vs `insert_block(&…)`：跟随 storage-db-and-mdbx 决策；默认**保留** gravity baseline 的 `insert_historical_block`（baseline `0cb1687c1c:crates/storage/provider/src/providers/database/provider.rs` 中该方法仍存在）。
- `check_consistency(&provider, is_full_node)` 全部调用点：**保留 gravity** `is_full_node` 参数（PR #224 marker；与 manager.rs 的 gravity 签名匹配）。

### `crates/stages/stages/src/stages/prune.rs`
**模块：** `PruneStage` / `PruneSenderRecoveryStage` —— 在配置的 segments 上跑 pruner。
**冲突类型：** UU（3 个冲突块 —— 全部位于 trait bound 列表)
**上游变更（v1.8.3 → v2.3.0）：**
- `b9969c5b1` PR #22954 + `e3fe6326b` PR #22042 + `9f8c22e2c` PR #21331 —— `PruneStage` / `PruneSenderRecoveryStage` 的 `Provider` trait bound 新增 `ChainStateBlockReader + StageCheckpointReader + StorageSettingsCache + ChangeSetReader + StorageChangeSetReader + RocksDBProviderFactory`。
- imports 中去掉 `use reth_db::transaction::DbTx;`（上游 execute body 中 `provider.tx_ref().commit_view()?` 调用已不存在，因此该 trait import 也无用）。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：**
- `3ee6ac039e` PR #246 在 PruneStage execute body 内 `pruner.run_with_provider(provider, input.target())?` 之后保留 `provider.tx_ref().commit_view()?`（baseline `git show 0cb1687c1c:crates/stages/stages/src/stages/prune.rs:61` 验证）—— `commit_view` 是 gravity rocksdb 的 view-transaction commit（`crates/storage/db-api/src/transaction.rs:47` 定义；`crates/storage/db/src/implementation/rocksdb/tx.rs:264` 实现），对 mdbx 是 no-op (`Ok(false)`)。**该调用在当前 worktree 的冲突 diff 中未出现 —— 说明 baseline 与上游在 execute body 中并未在同一行冲突；冲突仅在 imports + trait bound 列表，需要在合并产物里手动确保这一行保留。**
- 不需要上游的 `RocksDBProviderFactory` trait bound —— gravity 的 rocksdb 通过 `crates/storage/provider/src/providers/rocksdb/provider.rs` 的 `RocksDBProvider` + `DBProvider` 暴露。
**影响范围：** Prune 执行 —— `commit_view()` 是 gravity rocksdb 真正把 prune 删除刷盘的承重点；缺它 pruned 字节直到下一个 stage commit 才落盘。
**解决方案建议：** 机械合并 (mechanical-merge)
**理由：**
- imports：**保留** gravity `use reth_db::transaction::DbTx;`（execute body 中 `commit_view()` 依赖该 trait import）。
- trait bound：丢掉上游 `RocksDBProviderFactory`（gravity rocksdb 接入方式不同；强加该 bound 会触发 gravity provider 树编译错误）。`StorageSettingsCache + ChangeSetReader + StorageChangeSetReader` 三个是否采纳跟随 storage-db-and-mdbx 决策 —— 保守做法：先丢，必要时补 fix-up commit。采纳上游新增的 `ChainStateBlockReader + StageCheckpointReader`（gravity `DBProvider` 实现已经提供）。
- 合并产物里 **必须手动确认** `provider.tx_ref().commit_view()?` 调用仍位于 `pruner.run_with_provider(...)?` 之后 —— 这是 PR #246 marker；在当前 conflict 范围之外，但若上游 merge 工具误删需要立刻补回。

### `crates/stages/stages/src/stages/sender_recovery.rs`
**模块：** `SenderRecoveryStage` —— 并行 ECDSA recover，写入 `tables::TransactionSenders`。
**冲突类型：** UU（12 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `ec982f868` PR #22206 `perf: bound more channels with known upper limits` —— `RecoveryResultSender` 的 `mpsc::Sender` → `mpsc::SyncSender`。
- `46d670eca` PR #20972 + `e86c5fba5` PR #20897 + `cd8fec327` PR #20428 —— 引入 `EitherWriter<'_, CURSOR, Provider::Primitives>` writer 抽象（写 mdbx cursor 或 static file segment）。`recover_range(range, provider, tx_batch_sender, &mut senders_cursor)` → `recover_range(range, block_numbers, provider, tx_batch_sender, &mut writer)`（多出 `block_numbers: Vec<BlockNumber>` 用于 static-file 模式 + `writer.ensure_at_block(end_block)?`）。
- `352430cd8` PR #21918 + `c558c1d10` PR #21988 —— execute 加入 prune 跳过路径：`if let Some((target_prunable_block, prune_mode)) = … { input.checkpoint = Some(StageCheckpoint::new(target_prunable_block)); … provider.save_prune_checkpoint(PruneSegment::SenderRecovery, …); }`。
- `7594e1513` PR #22211 `perf: replace some std::time::Instant with quanta::Instant` —— `use reth_primitives_traits::FastInstant as Instant;` + 外层 `let start = Instant::now()`。
- `386b774ed` PR #21788 `refactor: use spawn_os_thread for better tokio integration` —— `std::thread::spawn(move || …)` → `reth_tasks::spawn_os_thread("sender-recovery", move || …)`。
- 测试 imports 加入 `reth_db_api::models::StorageSettings`、`reth_static_file_types::StaticFileSegment`。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `9974ad0618` PR #241 改了两处测试断言（`processed: 1` → `processed: _`；`assert_eq!` 形态改 `assert_matches!`），无生产代码业务改动。
**影响范围：** 编译 + 性能。上游 `EitherWriter` + `reth_tasks::spawn_os_thread` 依赖 `reth-tasks` 在 Cargo 已是必选（与上游 Cargo 改动配套）。
**解决方案建议：** 采纳上游 (take-upstream)
**理由：** gravity 在本文件无生产侧业务改动。上游 `SyncSender` 限界 channel + `EitherWriter` + `reth_tasks::spawn_os_thread` + prune 跳过 + `FastInstant` 全是严格改进，与 gravity 的 pipe-exec 模型不冲突（gravity 仍需 sender recover 给下游 stage 用）。测试断言部分：保留 gravity PR #241 的 `processed: _` + `assert_matches!` 形态。

### `crates/stages/stages/src/stages/tx_lookup.rs`
**模块：** `TransactionLookupStage` —— 构建 `tables::TransactionHashNumbers`。
**冲突类型：** UU（9 个冲突块）
**上游变更（v1.8.3 → v2.3.0）：**
- `00f9bd2a9` PR #24494 `fix: use tx_hash for transaction identity` —— `alloy_eips::eip2718::Encodable2718` import 改为 `alloy_consensus::transaction::TxHashRef`；imports 加入 `reth_db_api::{table::{Decode, Decompress, Value}, …, Tables}`。
- `b9969c5b1` PR #22954 + `7f970e136` PR #21722 + `b81489322` PR #21367 + `a0df56111` PR #21334 —— trait bound 加入 `StorageSettingsCache + RocksDBProviderFactory`。execute 路径中手工 cursor `txhash_cursor.append/insert` 改为 `provider.with_rocksdb_batch_auto_commit(|rocksdb_batch| { let mut writer = EitherWriter::new_transaction_hash_numbers(provider, rocksdb_batch)?; … writer.put_transaction_hash_number(hash, tx_num, append_only)?; … })`。
- unwind 路径 `tx_hash_number_cursor.seek_exact / delete_current` 改为 `provider.with_rocksdb_batch(|rocksdb_batch| { let mut writer = EitherWriter::new_transaction_hash_numbers(provider, rocksdb_batch)?; … writer.delete_transaction_hash_number(*transaction.tx_hash())?; … })`。
- 测试 imports 去掉 `StaticFileProviderFactory`，加入 `cursor::DbCursorRO`；断言 `static_file_provider().count_entries::<…>()` → `db.count_entries::<tables::Transactions>()` + `processed == total &&` 前置。
**Gravity 侧变更（在 baseline `0cb1687c1c` 上）：** `9974ad0618` PR #241 把测试断言 `processed` 改为 `processed: _`，保留 baseline 的 `runner.db.factory.static_file_provider().count_entries::<tables::Transactions>().unwrap()` 形态。生产路径无 gravity-specific 业务改动 —— execute 是手写 cursor、unwind 是手写 `seek_exact + delete_current`。
**影响范围：** 类型 imports + provider API 形态。**关键风险**：上游的 `with_rocksdb_batch` / `with_rocksdb_batch_auto_commit` 是上游 storage-v2 自己实现的 rocksdb API，命名与 gravity PR #212 在 `crates/storage/provider/src/providers/rocksdb/provider.rs` 上的同名 API 相同，但 closure 签名（接受 `&mut WriteBatchWithIndex` vs 上游自家的 batch 类型）可能不一致。
**解决方案建议：** 跟随 storage-db-and-mdbx (mechanical-merge / take-upstream-after-port)
**理由：** gravity 在本文件无生产业务改动，可采纳上游 `EitherWriter` / `with_rocksdb_batch*` 写法，但前提是 storage-db-and-mdbx 分组确认 gravity 的 `RocksDBProvider` 实现了与上游兼容的 `with_rocksdb_batch_auto_commit(|batch| F)` + `EitherWriter::new_transaction_hash_numbers` + `RocksDBProviderFactory`。如果签名分歧，本文件需要保留 baseline 的手写 cursor 路径。`TxHashRef` import + `Tables` 引入 + 测试 `processed: _` (gravity PR #241) 三块是独立的、可干净落地。

## 分组级解决方案 playbook

按以下顺序执行：

1. **等待 chainspec/consensus 分组**落地 `FullConsensus<…>` 单参 vs 双参（含 `Error = ConsensusError`）。相应更新 `sets.rs`。默认：采纳上游单参。
2. **等待 storage-db-and-mdbx 分组**落定：
   - `StaticFileProvider::check_consistency(&self, provider, is_full_node)` 元数（gravity 必须保留 `is_full_node`）。
   - `BlockWriter::insert_block` / `insert_historical_block` 命名（默认保留 gravity `insert_historical_block`）。
   - `BlockWriter::append_block_bodies(IntoIter)` 是否去掉 `StorageLocation`（默认跟随上游去掉）。
   - `unwind_storage_hashing_range` / `unwind_storage_history_indices_range` 接 `RangeInclusive<BlockNumber>` 还是 `BlockNumberAddress::range(...)`。
   - 上游 `RocksDBProviderFactory` trait —— gravity 大概率**不引入**（gravity 用不同 rocksdb 接入）。
   - 上游 `with_rocksdb_batch` / `with_rocksdb_batch_auto_commit` / `EitherWriter::new_*` 是否与 gravity 同名 API 兼容（PR #212 / #224 / #340）。
3. **等待 trie-all-layers 分组**落定：
   - `reth_trie_parallel::nested_hash::NestedStateRoot` 是否保留（PR #134 / #149）。
   - `provider.write_trie_updatesv2(&trie_updates_v2)` API。
   - `AccountsTrieV2` / `StoragesTrieV2` 表。
4. 上述锁定后，逐文件应用：
   - `Cargo.toml` 优先（解决依赖图）；该文件改完后 `cargo check -p reth-stages`。
   - `stage.rs`：保留 gravity 侧（丢上游 mod tests）。
   - `pipeline/builder.rs`：采纳上游。
   - `pipeline/mod.rs`：按上文 10 块指南机械合并。**保留** `UnifiedStorageWriter::{commit, commit_unwind}` + `Instant::now()` + `execute_duration_ms` 日志 + 无条件 MerkleExecute reset。**采纳上游**的 `unwind_provider_rw().disable_long_read_transaction_safety()`、safe-block 保存、`saturating_sub(1)`、RAII unwind scope。
   - `sets.rs`：采纳上游。
   - `bodies.rs`：生产代码采纳上游；测试断言保留 gravity PR #241。
   - `era.rs`：采纳上游。
   - `headers.rs`：采纳上游（与 Cargo.toml 去 bincode 配套）。
   - `hashing_account.rs`：生产代码采纳上游；测试断言保留 gravity PR #241。
   - `hashing_storage.rs`：生产代码采纳上游；测试断言 + cursor 循环保留 gravity PR #241。
   - `merkle.rs`：保留 gravity 侧（在 trie-all-layers 之后）。
   - `mod.rs`（tests）：保留 gravity `check_consistency(provider, is_full_node)` 调用 + `insert_historical_block` 测试 setup；丢上游 mdbx::Cursor/RW imports。
   - `index_account_history.rs` / `index_storage_history.rs`：跟随 storage-db-and-mdbx；保留 gravity PR #224 的两参测试调用形态。
   - `prune.rs`：机械合并 —— 保留 gravity `commit_view()` 调用 + `use reth_db::transaction::DbTx;` import；丢上游 `RocksDBProviderFactory` bound；采纳上游 `ChainStateBlockReader + StageCheckpointReader`。
   - `sender_recovery.rs`：采纳上游；测试断言保留 gravity PR #241。
   - `tx_lookup.rs`：跟随 storage-db-and-mdbx；`TxHashRef` import + 测试断言 (gravity PR #241) 干净落地。
5. 验证：分组落地后跑 `RUSTFLAGS=-D warnings cargo check -p reth-stages -p reth-stages-api --all-features`。若 trie-all-layers 选了上游 `with_adapter!`，预期 `merkle.rs` 首轮报错；届时补 fix-up commit。

## 开放问题

1. **stage.rs 上游 mod tests 是否值得 port** —— `test_exec_input_next_block_range_with_transaction_threshold` 依赖上游 `ProviderFactory::new` 5 参签名（含 `RocksDBProvider::builder`、`reth_tasks::Runtime::test()`）。storage-db-and-mdbx 落地后可廉价补 port 到 gravity 的 `ProviderFactory::new`。开 tracking issue，非合并阻塞。
2. **prune.rs 上游 trait bound** `StorageSettingsCache + ChangeSetReader + StorageChangeSetReader` —— gravity `DBProvider` 是否实现？若否，丢掉能编译，但会损失上游 PrunerBuilder 中依赖它们的特性（grep `crates/prune/prune/src/` 中 `PrunerBuilder::run_with_provider` 函数体来确认）。
3. **tx_lookup.rs 上游 `with_rocksdb_batch{,_auto_commit}` closure 签名** —— 必须与 gravity PR #212 在 `crates/storage/provider/src/providers/rocksdb/provider.rs:427+` 的实现兼容。若分歧，优先保留 baseline 手写 cursor 路径，留待 storage-db-and-mdbx 拍板。
4. **headers.rs ETL collector 格式变化** —— `Bytes` collector value 从 bincode 改为 RLP（上游 PR #23156）。若 gravity 升级时磁盘上残留半成品 ETL 临时目录，新的 RLP 路径会误解析 bincode payload。需核对 gravity 是否在启动时清 `etl_path`（上游 PR #16770 已做）—— 若否，加启动清理。
5. **merkle.rs vs trie-all-layers** —— 若 trie-all-layers 决定上游 `with_adapter!`/`DbStateRoot::<_, A>` 战胜 gravity `NestedStateRoot`，本文件需手写重写以保 V2 trie 兼容。这是 chain-halt 关键路径，必须等 trie-all-layers 给出明确结论后再动。
6. **pipeline/mod.rs `UnifiedStorageWriter` 符号是否仍存在** —— 上游 v2.3.0 已删除 writer 包装、直接用 `provider_rw.commit()`。但 `acc458846c` PR #340 要求 rocksdb batch 刷盘走 `UnifiedStorageWriter::commit` hook。**必须**核对 gravity 在 `crates/storage/provider/src/writer.rs` 中的 `UnifiedStorageWriter` 在 storage-db-and-mdbx 分组合并后是否仍存在；若被上游内联/删除，需要在 gravity 侧补回 wrapper 或把刷盘 hook 改打到 `provider_rw.commit()` 的实现内部 — 这是 chain-halt / data-loss 风险点。
7. **prune.rs `commit_view()` 调用未在冲突 diff 中** —— 当前 worktree 的冲突 hunk 只覆盖 imports + trait bound，PR #246 在 execute body 中 `pruner.run_with_provider(...)?` 之后的 `provider.tx_ref().commit_view()?` 是否会被自动合并保留需要在 merge 产物中验证。该行如丢失，prune 写入将延迟一个 stage commit，rocksdb 后端有 OOM / 磁盘失序风险。
8. **prune.rs trait bound 上游 `RocksDBProviderFactory`** —— 大概率丢；但要在丢之后确认 `PrunerBuilder::default().segments(...).build::<Provider>(...)` 不依赖该 bound（gravity 的 prune 路径走 mdbx + rocksdb 双后端，需 grep 确认）。
