# Upstream sync 2026-07-09 — merge upstream/main (Galxe) → gravity-reth-merge-v2.3.0

## 范围

- **Base (merge-base)**: `0cb1687c1c` — `deps(execution): remove galxe revm dependency and calculate rewards (#373)`
- **Upstream tip**: `a379a86daa` (Galxe/gravity-reth main) — `fix hardfork test ci coverage (#374)`
- **HEAD before merge**: `b3f42db836` — `fix(pipe-exec/tests): set terminalTotalDifficulty in gravity test genesis`
- **6 upstream commits 一览** (`0cb1687c1c..upstream/main`):
  - `e75679c08b` — feat(hardfork): gas-exempt system txs + zero SYSTEM_CALLER at Alpha (#367)
  - `9f7fb6d2a7` — fix(rpc): register BLS pop-verify precompile unconditionally (split from #367) (#370)
  - `cb79bd0dce` — chore(deps): bump gravity-api-types for epoch-start recovery (#376)
  - `4c6dad050d` — test(rpc): equivalence of execution vs header randomness providers (#371)
  - `e1d2d37686` — fix(rpc): debug_traceTransaction target-tx gas-exempt gap + wire deferred #367 tests (#377)
  - `a379a86daa` — fix hardfork test ci coverage (#374)

## 冲突面(dry-run 归零)

### 9 UU files, 18 hunks — 逐文件裁决

| 文件 | hunks | 决策(1-line) |
|---|---|---|
| `.github/workflows/integration.yml` | 1 | 保留 HEAD 的 `taiki-e/install-action@1f2425cdb…` SHA-pin;吸收 upstream 的 "Check Gravity invariants" 步骤 + `--test gravity_hardfork_test` step name |
| `Cargo.toml` (workspace deps) | 1 | 采纳 upstream `gravity-api-types` rev bump(#376);拒绝 upstream 的 `op-reth = { path = "crates/optimism/bin" }`(遵循项目原则 [[feedback_no_op_stack]]) |
| `crates/ethereum/evm/src/lib.rs::transact_system_txn` | 1 | 保留 HEAD 的 "revm40 移除 set_state_clear_flag" 注释,吸收 upstream 的 gas-exempt gate(`is_system_tx_gas_exempt(chain_spec, block_ts)` → `disable_base_fee` + `disable_balance_check`) |
| `crates/gravity-precompiles/src/bls_pop_verify.rs` | 1 | 保留 HEAD `PrecompileOutput::halt(PrecompileHalt::other(...), 0)`(revm40 API);拒绝 upstream `Err(PrecompileError::Other(...))`(revm36 API);文件路径采纳 upstream 迁移(`bls_precompile.rs` → `gravity-precompiles/bls_pop_verify.rs`) |
| `crates/rpc/rpc-eth-api/Cargo.toml` | 1 | revm feature 取并集: `optional_balance_check` + `optional_block_gas_limit` + `optional_eip3607` + `optional_no_base_fee` + `optional_fee_charge` + `memory_limit`(gas-exempt gate 用 `disable_balance_check`,estimate 路径用 `disable_fee_charge`,call.rs 用 `memory_limit`) |
| `crates/rpc/rpc-eth-api/src/helpers/call.rs` | 2 | imports 并集(`is_gravity_system_caller`, `is_system_tx_gas_exempt`, `ChainSpecProvider`, `EthChainSpec`, `EthereumHardforks`);`replay_transactions_until` 采纳 upstream 的按首交易 kind 预置 cfg + 系统→用户 boundary 单次 rebuild 优化;`.number/.timestamp/.prevrandao` 全部换成 v2.3.0 accessor 方法 |
| `crates/rpc/rpc-eth-api/src/helpers/trace.rs` | 7 | 采纳 upstream 手写 while-loop 版 `trace_block_until_with_inspector`(需要 mid-block cfg-toggle 才能保状态根一致);同时: `StateCacheDbRefMutWrapper` 全部剥离改回 `&mut StateCacheDb`(v2.3.0 不存在该 wrapper 类型,workspace 用 &mut 直连);`Insp: Default+…Wrapper` → `Insp: for<'a> InspectorFor<Self::Evm, &'a mut StateCacheDb>`;`tx_info` 追加 `block_timestamp: Some(...)` 字段(alloy-consensus 2.0.5 `TransactionInfo` 新字段) |
| `crates/rpc/rpc/Cargo.toml` | 1 | 采纳 upstream 新增 dev-deps: `reth-ethereum-primitives` + `reth-pipe-exec-layer-ext-v2`(#371 randomness 等价性 test 需要) |
| `crates/rpc/rpc/src/debug.rs` | 3 | imports 保留 HEAD 的 `parking_lot::RwLock` + `reth_engine_primitives::ConsensusEngineEvent` + `BlockExecutor`(v2.3.0 需要);吸收 upstream `is_gravity_system_caller/is_system_tx_gas_exempt`;`trace_block` 循环采纳 HEAD `spawn_with_state_at_block(_, move \|eth_api, mut db\|)` 双 arg 闭包 + `DebugInspector.get_result(...)` 组合式 API,叠加 upstream 的 `per_tx_evm_env` cfg-toggle;`debug_traceTransaction` 的 target-tx 采纳 upstream `target_evm_env` gate(#377 修复的 gap),按 HEAD API 用 `eth_api.inspect(..., target_evm_env, ...)` + inspector.get_result |

### 24 auto-merged files,其中 3 处需要 semantic 后处理

- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`(+38 upstream) — auto-merge 直落,`system_caller_migration::apply_state_changes_for_block(…)` 已按 EIP-2935 pattern wire 在 execute_ordered_block 内 `apply_state_change` 通道(pipe-exec/lib.rs:1258),EIP-161 nonce 保留策略生效。审计通过。
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs`(#[cfg(test)] 部分) — upstream 测试 helper `system_txn_result` 直接用旧版 `ExecutionResult::Success { gas_used, gas_refunded, … }` 结构字段,v2.3.0 已改成 `gas: ResultGas`(EIP-8037 state gas split)。改写为 `gas: ResultGas::default().with_total_gas_spent(gas_used)`,test 编译通过。
- `crates/pipe-exec-layer-ext-v2/execute/src/tx_filter.rs`(#[cfg(test)] 部分) — 27 处 `AccountInfo { balance, nonce, code_hash, code }` 缺 revm40 新字段 `account_id: Option<AccountId>`。批量补 `account_id: None,`(Python regex 一次性 patch,保留原格式),编译通过。
- `crates/pipe-exec-layer-ext-v2/execute/tests/gravity_system_tx_simulation_anti_spoof_test.rs`(+37 upstream) — upstream 用 `estimate_gas_at(req, block_id, None)` 传 `Option`;v2.3.0 签名收紧为 `overrides: EvmOverrides`(不再接 Option),改成 `EvmOverrides::default()`。

其余 auto-merged 文件(含 `crates/ethereum/evm/src/parallel_execute.rs` +408 行)整体通过 `cargo check --workspace --all-features` 无 API drift。`parallel_execute.rs` 的 grevm-twin `transact_system_txn` 保持与 serial 侧同结构(byte-identical gate,#367 pin — evm/src/parallel_execute.rs:271-287)。

### 7 纯新增(no conflict)

- `crates/pipe-exec-layer-ext-v2/execute/src/system_caller_migration.rs`(430) — 主体代码需要修 3 处 revm40 API: (a) `AccountInfo` 加 `account_id: prev.account_id`; (b) `Account { info, storage, status, transaction_id: 0 }` 结构字面量因私有字段 `original_info` 无法用 struct-literal,改用 `let mut account = Account::default(); account.info = …; account.status = AccountStatus::Touched;`。#[cfg(test)] 部分 6 处 `AccountInfo` 补 `account_id: None`,一处单行 form 手工补。
- 5 个 test 文件 `crates/pipe-exec-layer-ext-v2/execute/tests/gravity_system_tx_{bls_replay,gas_exempt,post_alpha_trace,pre_alpha_replay,simulation_anti_spoof}_test.rs` — 无 `AccountInfo` 引用,编译通过(除上面 simulation_anti_spoof 的 EvmOverrides 修复)。
- `scripts/check-gravity-invariants.sh` — bash + ripgrep 长期回归护栏,已在 integration.yml 中被 wire(Check Gravity invariants step)。

### 1 delete(no conflict)

- `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs` — #370 迁移到 `crates/gravity-precompiles/src/bls_pop_verify.rs`,重命名 auto-detect 为 `R`(rename)。

## 决策(可回指的短条目)

- **op-reth 出局**:遵循 [[feedback_no_op_stack]] — `Cargo.toml:369` 拒绝 upstream 引入的 `op-reth = { path = "crates/optimism/bin" }` workspace dep;workspace members 也未含 `crates/optimism/bin`(HEAD 侧本就无此项),决策与 storage 出局一致。
- **revm40 API 优先**:所有 API drift(precompile `Halt` variant、`AccountInfo::account_id`、`ExecutionResult::Success` 的 `gas: ResultGas`、accessor `.number()/.timestamp()/.prevrandao()`)一律迁上游代码去适配 v2.3.0 API,不动 workspace 的 crate 版本 pin。原则依据:[[project_merge_v230_decision_principle]] "不冲突留 v2.3.0"。
- **StateCacheDbRefMutWrapper 出局**:v2.3.0 rpc-eth-types/cache/db.rs 已重构为 `pub struct StateProviderTraitObjWrapper(pub StateProviderBox)` 单泛型无 lifetime,不再需要 `StateCacheDbRefMutWrapper<'a, 'b>` 包一层。upstream 侧所有 `StateCacheDbRefMutWrapper(&mut db)` 用法在 trace.rs 剥离为 `&mut db`。
- **grevm/EthEvmConfig 非泛型 EvmFactory**:未触碰 — upstream 6 commits 不改这条,保持 executed-block doc §⟲ 落地实录原则 ②。
- **system_caller_migration.rs 落地形态**:采纳 upstream,主体路径 `pipe-exec-layer-ext-v2/execute/src/lib.rs:1258` 已 wire,exec 前置于 system-txn 前(保 R5 EIP-161 verify);API adaption 全在文件内闭环,未污染 revm-state / reth-primitives-traits。
- **#367 gas-exempt 衔接 gravity 本地 #364 决策线**:见 [[project_system_tx_gas_exempt]] — 本地 #363/#364 已在 serial+grevm 两侧 gate 就位;upstream #367 追加了 (a) RPC replay 侧 gate(trace.rs 手写 while-loop + call.rs 首交易 kind 预置 + debug.rs per_tx_evm_env)以修 RPC vs canonical divergence,(b) SYSTEM_CALLER 余额零化 boundary migration。gate 谓词一致 = 被回放块 timestamp(不是 node tip),与本地文档一致。
- **#370 BLS 无条件注册 vs 原 pipe-exec 内注册**:采纳,BLS precompile 迁移到共享 `crates/gravity-precompiles/`,`crates/rpc/rpc/src/eth/helpers/call.rs::register_custom_precompiles` 在 RPC 侧无条件挂载(不再挂 Alpha gate),消除 pre-Alpha `CALL 0x…5001` 在 RPC 回放时与 canonical 发散(见 [[project_system_tx_gas_exempt]] 存量漏洞条目)。

## 已实测证据

- `cargo check --workspace --all-features` = 0 err(evidence: `_local/tmp/upstream-sync/09-check-final.log` 尾行 `Finished dev profile [unoptimized + debuginfo] target(s) in 2.07s`)
- `cargo +nightly fmt --all --check` = clean(evidence: `_local/tmp/upstream-sync/12-fmt.log` 应用后 rc=0)
- 冲突标记归零:`grep -rn '^<<<<<<< \|^=======$\|^>>>>>>> ' crates/ Cargo.toml .github/` = 空
- 5 个新增 gravity_system_tx_*_test.rs 与 tx_filter.rs / system_caller_migration.rs 的 `#[cfg(test)]` block:`cargo check -p reth-pipe-exec-layer-ext-v2 --all-features --tests` = 0 err(evidence: `_local/tmp/upstream-sync/10-pipe-tests.log`)

## 未跑

- integration tests(需要节点启动,耗时;下一波单独跑)
- clippy(可选,warnings only)
- workspace `--tests` 有 **21 处 pre-existing 报错**,均在 `crates/storage/libmdbx-rs/tests/{cursor,transaction}.rs` — 该目录为 vendored 第三方,CLAUDE.md "Never modify libmdbx sources"。签名漂移源自 `f89d9d4e23 (#375) resolve storage&cache&state root` 的 storage 重构;非本 sync 引入。

## 后续

- Push 后开 PR into `gravity-reth-merge-v2.3.0`(本地 review 分支,不是 Galxe upstream)
- 5 个 gravity_system_tx_*_test.rs 已 wire 到 CI(integration.yml `--test gravity_system_tx_*` + `--test gravity_bls_precompile_test / gravity_system_tx_bls_replay_test`),需 CI 实跑证据
- 后续任何 test 引用旧 `AccountInfo { … }` / `ExecutionResult::Success { gas_used, gas_refunded }` 需按同 pattern 补 `account_id` / 改 `gas: ResultGas`,已在测试文件 comment 无明确 memo

### Shutdown race investigation (2026-07-10)

**观察复现失败**。为核对合并后 `gravity_pipe_test` 关机时报 `tokio/shutdown.rs:51 "Cannot drop a runtime in a context where blocking is not allowed"` + `pthread lock: Invalid argument` 的 panic,本轮追加了 3 次 smoke:

| # | 起始 | 结束 | 结果 | tail 关键行 |
|---|---|---|---|---|
| 1 | 02:29 | 02:33 (223.93s) | `test result: ok. 1 passed;` | `Wrote network peers to file` 后无 panic,直接 `ok` |
| 2 | 10:31 | 10:34 (225.27s) | `test result: ok. 1 passed;` | 同上 |
| 3 | 10:40 | 10:44 (223.95s) | `test result: ok. 1 passed;` | 同上 |

3/3 通过,与 Task 上下文首例 log(worktree 本地 smoke-run log,02:16:07)不一致。二进制未变 (`target/debug/deps/gravity_pipe_test-<hash>` 同一份),数据目录每次重新 wipe。

**符号面 diff**(`git diff --cached HEAD -- '*.rs' | grep -E 'tokio::spawn|block_on|block_in_place|Runtime::new|spawn_blocking|OnceLock<Runtime>|Handle::current'`)= **0 命中**。6 个 upstream commit **没有引入新的 tokio runtime、`block_in_place`、`spawn`**;`Cargo.lock` tokio 版本 pre/post 均为 `1.52.3`。`ETH_CALL_RUNTIME` `OnceLock<Runtime>` 位于 `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/base.rs:13` 且此文件本轮**未变**;`WorkloadExecutor` / trie parallel 的 `static RT: OnceLock<Runtime>` 亦均 pre-existing。

**关机顺序** (`Canonical chain committed 999` → `Pipe exec layer channel disconnected` → `Wrote network peers to file` → 2s sleep → 进程退):

- Runtime 类型 = `reth_tasks::Runtime` = `Arc<RuntimeInner>`;`_tokio_runtime: Option<TokioRuntime>` 仅在**最后一个 `Arc` drop** 时 drop。
- `CliRunner::run_command_until_exit` (`crates/cli/runner/src/lib.rs:96-99`) 已经 `graceful_shutdown_with_timeout(5s)` + `runtime_shutdown(self.runtime, true)`(独立 `rt-shutdown` 线程 drop),这条路径本身**不会**在 async context 里 drop runtime。
- 若某个 spawn 到 CliRunner runtime 上的 task 持有 `Arc<Runtime>` 副本、且外层 shutdown-thread drop 时并非最后一个 Arc → 最终 drop 落在 tokio worker thread(async context)→ `Shared::wait` 命中 `try_enter_blocking_region() == None` → panic (`tokio-1.52.3/src/runtime/blocking/shutdown.rs:51`)。
- 命中概率取决于:(a) task manager / graceful task 是否在 5s 内 drain 干净,(b) 各 `Arc` clone 释放时序。这是一个 **pre-existing shutdown race**,不是本 sync 引入。

**结论**:未能定位到本次 merge 引入的 shutdown regression。若后续 CI 或本地再复现,首选修法(记录备查,本轮**未落地**,无 repro 不改):

1. `crates/cli/runner/src/lib.rs:96` 之前追加 `let executor_ref_count = Arc::strong_count(&self.runtime.0);` (需要 `pub(crate)`) 打点,精确定位残留 Arc 归属;
2. 若确认残留 Arc 由某个 spawn task 持有,改造 `Runtime` 让 owned `TokioRuntime` 提取到独立的 `Sender<Runtime>` shutdown-thread,`Arc<RuntimeInner>` 只持 `Handle` clone(隔离 Drop 触发路径);
3. `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/base.rs:44-78` 的 `block_in_place` + 私有 `ETH_CALL_RUNTIME` pattern 可改为 `Handle::try_current().unwrap_or_else(|_| ETH_CALL_RUNTIME.get_or_init(...).handle().clone())` — 尽量复用外层 runtime handle,减小私有 runtime 生命周期与外层重叠时的 race 面。

三次 smoke tail 证据保存于 worktree 本地 smoke-run 目录(git-ignored,不入 doc 引用)。
