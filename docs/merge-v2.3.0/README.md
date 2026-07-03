# Merge v2.3.0 — 冲突分析总索引

## 摘要

- **155 个冲突文件**（124 UU + 26 AA + 5 AU），从 `merge-v2.3.0` 分支（HEAD `e6b7e5ba32`）`git status --porcelain` 直接采样，按 crate 目录聚合后按 12 个分组文档展开分析。
- **按 crate 目录的冲突分布**（`git status --porcelain` 口径）：

  | 目录 | 冲突数 |
  |---|---:|
  | crates/storage | 26 |
  | crates/stages | 17 |
  | crates/rpc | 13 |
  | crates/trie | 13 |
  | crates/cli | 13 |
  | crates/ethereum | 10 |
  | .github | 9 |
  | crates/node | 6 |
  | crates/engine | 6 |
  | crates/tx-pool | 6 |
  | crates/net | 4 |
  | crates/prune | 2 |
  | docs/vocs | 2 |

- **顶层最关键的 4 个分组（按风险加权排序）**：
  1. `trie-all-layers`（**高**）— DB on-disk format、GAT cursor factory 迁移、proof v2 重构三条主线交叠；`AccountsTrieV2`/`StoragesTrieV2` 的 nested trie 与上游 v2.3.0 sparse-trie / proof v2 正面冲撞。
  2. `engine-evm-execution-chainspec`（**高**）— engine tree backpressure、`Executor`/`BlockExecutor` trait、chainspec 50 Gwei base-fee floor 三大语义簇；pipe-exec-layer + grevm 并行执行 vs 上游 sparse-trie / BAL / 共享 payload cache 重写。
  3. `storage-providers-and-writers`（**高**）— `database/provider.rs` 单文件 conflict diff 极大；`UnifiedStorageWriter` / `static_file/manager.rs` 与上游 storage v2 / `BalStoreHandle` 强耦合。
  4. `storage-api-and-traits`（**高**）— gravity 私有 trait 扩展（`+ Send + Sync`、`commit_view`、`recover_block_number`、`subkey_compress_length`）与上游 `WriteStateInput`/`StateWriteConfig`/`ExecutionWitnessMode` 重构正面对抗；盲目 take-upstream 会让 `ParallelStateProvider` / RocksDB 后端无法编译。
- **顶层横切风险**：
  1. **storage v2 / RocksDB 双轨**：gravity 侧把 mdbx 替换为 RocksDB (`a1d7365bd6` #212)，引入 `RocksDBProvider`、`StorageRecoveryHelper`、`sharding-directories`；上游 v2.3.0 把 mdbx 端推进到 storage v2（`StorageSettings`、`BalStoreHandle`、`PackedStoredNibbles`、`Metadata` 表）。两条轨道在 `db-api` / `db` / `provider` / `node-builder` / `cli/commands` 全线冲撞。
  2. **`reth-engine-service` crate 上游删除**（v2.3.0 `1f3fd5da2` #22187）— gravity 仍依赖该 crate；node-builder `launch/engine.rs` 必须切到 `build_engine_orchestrator`，不能只保留 dep。
  3. **Executor / BlockExecutor trait 面变化**：gravity 自 `2dde8ca181` (#109) 起在 trait 上挂了 `transact_system_txn` / `apply_state_change` / `take_bundle` / `parallel_executor`；上游 v2.3.0 引入 `GasOutput` / `take_bal` / state-hook from `State<DB>`，二者直接冲突。
  4. **`crates/cli/commands/src/download` 模块二义**：`crates/cli/commands/src/download.rs` 与 `crates/cli/commands/src/download/mod.rs` 同时存在。Gravity 侧的 snapshot-download 特性用单文件 `download.rs`；v2.3.0 upstream 新增了 `download/` 子目录含子模块。`lib.rs` 中 `pub mod download;` 指向二义，rustfmt hook 会挂住。需要人工决策（详见 `cli-and-commands.md`）。
  5. **`crates/prune/prune/src/segments/mod.rs` 中 `mod static_file;` 悬空**：v2.3.0 upstream 已把 `segments/static_file/` 子目录整个删除，`mod.rs` 中的 `mod static_file;` 声明失去目标。同时 gravity 侧仍留有 `static_file/{headers,receipts,transactions,mod}.rs` 需要一并决策（详见 `net-prune-misc-crates.md`）。

## Baseline

| 项 | 值 |
|---|---|
| **协作 branch** | `upstream/gravity-reth-merge-v2.3.0`（remote = `Galxe/gravity-reth`） |
| **本地 branch** | `merge-v2.3.0` |
| **当前 HEAD** | `e6b7e5ba32 chore(merge): reth v2.3.0 squash — WIP team-share checkpoint` |
| **Parent commit** | `0cb1687c1c`（Galxe/gravity-reth `upstream/main` tip，已含 #373） |
| **Target upstream** | reth **v2.3.0** |
| **配套 skill** | `~/gravity/mono-grav/skills/merge-upstream-reth/`（mono-grav PR Galxe/mono-grav#88 review 中） |
| **skill 参数** | `--since 2025-11-03`（依据是 `d620fd0eeb feat: merge reth-v1.8.3 (#205)` @ 2025-11-10 前推 7 天 buffer） |

## 解决策略

12 个子文档共用以下 5 条优先级规则。"gravity-marker" 指承载 pipe-exec / grevm 并行 / nested trie / RocksDB / 50 Gwei base fee / hardfork framework / DKG / oracle relayer / system-tx / metadata-tx 等 gravity 业务语义的代码段。

1. **gravity-marker keep**：baseline 上由 gravity commit 引入、且承载 gravity 业务语义的代码段，保留 gravity 侧；上游对同一段的演进按机械合并处理。
2. **gravity-only path keep**：仅 gravity 引入的新文件 / 新模块（如 `crates/pipe-exec-layer-ext-v2`、`crates/gravity-*`、`hardfork/*` stub），上游不可能触及；冲突来自 rename / Cargo dep 重组，按 gravity 侧路径与依赖关系保留。
3. **no-gravity-touch take-upstream**：gravity 在该文件上无任何业务性 commit，冲突纯由上游重构 + git 三方算法误判触发 — 直接 take-upstream，丢弃 gravity 侧 hunk。
4. **both-touched mechanical-merge**：双方都触及但语义独立（典型：Cargo.toml feature 矩阵、`use` import 列表、新 enum variant 追加、metrics 项追加），按机械合并执行；不引入新语义。
5. **文件系统兼容优先**：涉及磁盘 on-disk 格式（trie key/value 编码、table schema）的分歧，gravity 网络已在线上跑的编码必须保留；上游新格式作为并存类型引入。

## 开放问题决策追踪

12 个分组文档末尾的「开放问题」章节均为 **决策追踪 checklist**(共 69 项),
每条两个勾选框:

- **决策**(条目本身的勾选框):勾选 = 已拍板,并在条目末尾追加
  「→ **决策**: …」记录结论与日期;未勾选 = 待决策 / 待核实。
- **冲突解决**(条目下嵌套的勾选框):勾选 = 该决策已在 worktree 落地
  (相关冲突块已按决策解掉,经实测核实);未勾选 = 待落地,注明实测状态与证据。

全局统计进度:

```bash
grep -c '^- \[ \]' docs/merge-v2.3.0/*.md         # 各文档剩余待决数
grep -c '^- \[x\]' docs/merge-v2.3.0/*.md         # 已决数
grep -c '^   - \[ \] 冲突解决' docs/merge-v2.3.0/*.md   # 待落地数
grep -c '^   - \[x\] 冲突解决' docs/merge-v2.3.0/*.md   # 已落地数
```

## 分组（共 12 个）

> 文件数列取自各子文档头部统计。复杂度按子文档自评结论汇总。

| # | Group | 文件数 | 复杂度 | 链接 |
|---|---|---|---|---|
| 1 | cli-and-commands | 14 | 高 | [`./cli-and-commands.md`](./cli-and-commands.md) |
| 2 | engine-evm-execution-chainspec | 23 | 高 | [`./engine-evm-execution-chainspec.md`](./engine-evm-execution-chainspec.md) |
| 3 | net-prune-misc-crates | 14 | 中 | [`./net-prune-misc-crates.md`](./net-prune-misc-crates.md) |
| 4 | node-builder-and-ethereum-node | 13 | 高 | [`./node-builder-and-ethereum-node.md`](./node-builder-and-ethereum-node.md) |
| 5 | rpc-eth-and-debug | 13 | 高 | [`./rpc-eth-and-debug.md`](./rpc-eth-and-debug.md) |
| 6 | stages-pipeline | 17 | 高 | [`./stages-pipeline.md`](./stages-pipeline.md) |
| 7 | storage-api-and-traits | 12 | 高 | [`./storage-api-and-traits.md`](./storage-api-and-traits.md) |
| 8 | storage-db-and-mdbx | 10 | 高 | [`./storage-db-and-mdbx.md`](./storage-db-and-mdbx.md) |
| 9 | storage-providers-and-writers | 13 | 高 | [`./storage-providers-and-writers.md`](./storage-providers-and-writers.md) |
| 10 | tests-examples-config-infra | 32 | 低 | [`./tests-examples-config-infra.md`](./tests-examples-config-infra.md) |
| 11 | transaction-pool | 12 | 中 | [`./transaction-pool.md`](./transaction-pool.md) |
| 12 | trie-all-layers | 18 | 高 | [`./trie-all-layers.md`](./trie-all-layers.md) |

分组维度与顶部按 crate 目录聚合的 155 计数不完全一一对应：`tests-examples-config-infra` 把 CI / infra / 文档 / examples 归并到一起，`net-prune-misc-crates` 把若干小 crate 集中处理，故子文档统计更大。

## 推荐解决顺序

**推荐从 `crates/stages` (17) + `crates/storage` (26) 并进起手**：stages 是最难的（真实业务冲突最集中），storage 是最多的（含 rocksdb / provider / static_file 三条 gravity 分叉线）。这两个啃完后，`crates/trie` (13) + `crates/rpc` (13) 承接 sync / eth 分叉，收尾。完整按依赖 DAG 展开如下：

1. **db-api & db-models**（`storage-db-and-mdbx` + `storage-api-and-traits` 中 db-api 部分）— 把 `commit() -> Result<(), DatabaseError>`、cursor 关联类型、`LastSafeBlock` 重命名等 db-api 基础变更落定；同时落定 gravity 私有 trait 扩展（`+ Send + Sync`、`recover_block_number`）。
2. **trie common 层**（`trie-all-layers` 中 `crates/trie/common/*`）— `PackedStoredNibbles` / `PackedAccountsTrie` / `PackedStoragesTrie` 与 nested trie 双轨在 common 层定调，下游 trie/db / trie/parallel / state-root 才能编译。
3. **storage-api**（`storage-api-and-traits` 余下部分）— `WriteStateInput`、`StateWriteConfig`、`ExecutionWitnessMode` 的入参重构；`BalProvider` / `BalStoreHandle` noop 挂回。
4. **storage providers & writers**（`storage-providers-and-writers`）— 在 db-api / storage-api 稳定后才能解决 `database/provider.rs` 大 hunk 与 `UnifiedStorageWriter` 路径。
5. **stages pipeline**（`stages-pipeline`）— 各 stage 直接依赖 storage providers + writers + trie；包含 `era.rs`（新 stage 评估是否 port）与 `merkle.rs`、`hashing_*.rs` 的并行写改造。
6. **chainspec & 共识 / EVM 基础类型**（`engine-evm-execution-chainspec` 的 chainspec 部分 + `consensus/common/src/validation.rs` + `crates/evm/evm/src/*`）— 解决 50 Gwei 基础费、`GravityHardfork` 枚举与 chainspec trait 扩展；为后续所有读 chainspec 的 crate 提供稳定签名。
7. **engine tree / EVM 执行**（`engine-evm-execution-chainspec` 余下部分）— sparse-trie / BAL / 共享 payload cache 重写；依赖 trie 与 storage providers 已落地。
8. **transaction-pool**（`transaction-pool`）— 大多数 take-upstream；`validate/eth.rs` 保留 gravity 50 Gwei + system-tx filter；依赖 chainspec 与 storage-api 已稳定。
9. **net & prune & 其他小 crate**（`net-prune-misc-crates`）— 与上面解耦，可并行但建议在 transaction-pool 之后做以便统一编译。含 `prune/segments/mod.rs` 中 `mod static_file;` 悬空处理。
10. **rpc-eth & debug**（`rpc-eth-and-debug`）— 依赖 transaction-pool / storage-api / engine；处理 `pending` tag、`safe`/`finalized`、`eth_getProof` (nested hash) 的 gravity 改造。
11. **node-builder & ethereum-node**（`node-builder-and-ethereum-node`）— 依赖以上所有 crate；落定 `launch/{common,engine}.rs`、`NodeConfig`、`GravityArgs` 接线、`RethAuthHttpMiddleware`、`build_engine_orchestrator`。
12. **cli-and-commands**（`cli-and-commands`）— 依赖 node-builder；`EnvironmentArgs::init` 新签名、`StorageRecoveryHelper.check_and_recover()`、`init_genesis` guard、stage drop/dump 子命令。含 `download.rs` vs `download/mod.rs` 二义决策。
13. **tests / examples / config / infra**（`tests-examples-config-infra`）— 等代码层全部稳定后处理；`README.md` 保留 gravity 开场叙事；workflow / docs / examples 大多 take-upstream。
14. **`Cargo.lock` 最终重生成** — 全部源码冲突收尾后 `cargo update -w` + `cargo build` 触发一次全量解析后提交。

## CHAIN-HALT / 数据丢失风险

> "风险载体" 指如果该项处理失误，最坏情况下会导致的故障类型。

| # | 风险载体 | 故障类型 | 触发条件 | 缓解 |
|---|---|---|---|---|
| 1 | `crates/storage/db-api/src/transaction.rs::commit() -> Result<()>` vs gravity `commit_view` | 数据丢失（写入丢失） | 把 gravity 的 `commit_view` / `commit() -> Result<bool>` 直接换成上游签名而不迁移调用方的 view-commit 顺序 | 必须在 storage-api 层显式保留 view-commit 路径；调用方逐个回归 |
| 2 | `crates/storage/provider/src/writer/mod.rs::UnifiedStorageWriter` vs 上游 `WriteStateInput`/`BalStoreHandle` | 数据不一致 / 重启后状态错乱 | 上游入参重构后 unified writer 路径丢链 | 三选一：移植上游 `WriteStateInput` 内部封装 unified writer / 保留 gravity 调用面但内部桥接到上游 / 完整重写 |
| 3 | `crates/stages/stages/src/stages/merkle.rs` + `crates/trie/db/src/state.rs` nested trie 接入 | CHAIN-HALT（state root mismatch） | merkle stage 在 history sync / pipe execution 两条路径上对 nested trie 的写入顺序与上游 sparse-trie 接入失误 | 保留 `9633989cdc`、`3cd18422c9`、`9acbf22633` 路径；上游 sparse-trie 与 nested trie 必须二选一，不能混链 |
| 4 | `crates/ethereum/evm/src/lib.rs` Executor trait `transact_system_txn` / `apply_state_change` | CHAIN-HALT（system tx 不上链 / 重复执行） | 上游 `GasOutput` / `take_bal` 改造覆盖 gravity 的 system-tx 接口 | 保留 `2dde8ca181`、`a077894a7d`、`6cc1001fcc`、`55ce0412ca` 的 Executor 扩展点；上游 GasOutput 走 wrapper |
| 5 | `crates/chainspec/src/spec.rs` 50 Gwei 基础费 + `GravityHardfork` | CHAIN-HALT（base fee 与节点不一致） | 上游 `amsterdam_time` / `block_access_list_hash` 字段写入覆盖 `gravity_min_base_fee` / `gravity_hardforks` | 保留 `7d0483e565`、`364b851665`、`d0666f2ab2`、`3c7634a6e1` 字段 |
| 6 | `crates/engine/tree/src/persistence.rs` 节流 + `crates/engine/tree/src/tree/mod.rs` backpressure | 节点 OOM / 出块停滞 | 上游 backpressure 重写覆盖 gravity 的 `e775fd5e72` batch-size limiting | 在新 backpressure 框架内重新加 batch-size 节流，否则 pipe-exec 高峰会撑爆内存 |
| 7 | `crates/transaction-pool/src/validate/eth.rs` 50 Gwei + system-tx filter | CHAIN-HALT（出块包含违规 tx） | 上游 `EthTransactionValidator` 重构覆盖 50 Gwei 校验与 system-tx 过滤 | keep-gravity；用 wrapper validator 接入 |
| 8 | `crates/cli/commands/src/common.rs` `StorageRecoveryHelper.check_and_recover()` + `init_genesis` guard | 数据丢失 / 创世重置 | 上游 `EnvironmentArgs::init` 签名重写覆盖启动期 storage recovery 与 init-genesis guard | 保留 `ff103f976a` (#313) 的 recovery 调用与 guard；在新签名下重新挂回 |
| 9 | `crates/storage/db/src/lib.rs` rocksdb 默认后端分发轴 | 启动失败 | 上游 mdbx 默认后端 + storage v2 改造覆盖 `a1d7365bd6` 的 RocksDB default | 保留 `a1d7365bd6` 的 default-feature；mdbx 路径降级为可选 |
| 10 | `crates/node/builder/src/launch/engine.rs` `build_engine_orchestrator` vs `EngineService` | 启动失败 | 仅删除 `reth-engine-service` dep 而代码用旧路径 / 反之 | 一次性切到 `build_engine_orchestrator`，删 `reth-engine-service` |

## Gravity 保留 commits 交叉引用

> 完整 gravity commit 列表见 `git log --oneline upstream/main`；此处仅收录跨分组高影响项。

| Commit | 标题 | 影响分组 | 保留要点 |
|---|---|---|---|
| `46c91f90fe` | `fix(eip-7702): filter intrinsic gas + Gravity acceptance tests (#343)` | transaction-pool, engine-evm-execution-chainspec | 7702 intrinsic gas 过滤；validator 与 executor 双侧改 |
| `ba7e949473` | `feat(eip-2935): serve 8191-block history via Prague-gated activation (#341)` | engine-evm-execution-chainspec | Prague gating 必须保留，否则 history 服务断 |
| `acc458846c` | `fix(rocksdb): flush batch data into storage to make sure stage is completed (#340)` | storage-db-and-mdbx, stages-pipeline | rocksdb batch flush；与上游 storage v2 双轨保留 |
| `364b851665` | `feat(fee): chainspec floor with code-driven activation schedule (#337)` | engine-evm-execution-chainspec, node-builder-and-ethereum-node | chainspec floor 字段 + 代码激活计划 |
| `7d0483e565` | `feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)` | engine-evm-execution-chainspec, transaction-pool, rpc-eth-and-debug | 50 Gwei 是 chain rule，跨 chainspec / pool / rpc |
| `ff103f976a` | `fix(unwind): commit view and set prune distance for execution unwind (#313)` | cli-and-commands, stages-pipeline | `StorageRecoveryHelper.check_and_recover()` + init-genesis guard |
| `28561abb17` | `feat(hardfork): add batch_storage_patches to HardforkUpgrades trait (#318)` | engine-evm-execution-chainspec | HardforkUpgrades trait 扩展 |
| `3c7634a6e1` | `refactor(hardfork): unified hardfork framework with stub implementations (#312)` | engine-evm-execution-chainspec | 统一 hardfork 框架；与上游 chainspec 字段合并基础 |
| `d0666f2ab2` | `feat(chainspec): add GravityHardfork enum and gravity_hardforks field (#309)` | engine-evm-execution-chainspec | `GravityHardfork` 枚举 + chainspec 字段 |
| `144fb1eea2` | `feat: add hardfork testing framework (trait + generic helpers) (#307)` | engine-evm-execution-chainspec, tests-examples-config-infra | hardfork 测试框架 |
| `ceaedd8b1a` | `fix(hardfork): read actual storage value as original_value in storage patches (#322)` | engine-evm-execution-chainspec | storage patch 原值修正 |
| `a454113910` | `feat(execute): integrate validator performance fetcher and bridge via eth call (#320)` | engine-evm-execution-chainspec, rpc-eth-and-debug | validator performance fetcher 接线 |
| `a1d7365bd6` | `feat(rocksdb): Integrating RocksDB into Reth (#212)` | storage-db-and-mdbx, storage-providers-and-writers, cli-and-commands, node-builder-and-ethereum-node | RocksDB 后端总线；mdbx 退为可选 |
| `1224ae1846` | `refactor(parallel): remove the read provider factory that supports parallel reading (#213)` | cli-and-commands, storage-providers-and-writers, stages-pipeline | 并行读 factory 删除；dump 子命令同步 |
| `c64bd613e4` | `opt(persist): use sharding rocksdb instances to optimize persist stage (#225)` | storage-db-and-mdbx, stages-pipeline | sharding rocksdb；`--db.sharding-directories` |
| `6cc1001fcc` | `feat: Split the validator transaction and execute it separately. (#229)` | engine-evm-execution-chainspec | validator tx 独立执行通路 |
| `271f1b2166` | `fix(recovery): Crash recovery trusts unverified state root (#264)` | stages-pipeline, storage-providers-and-writers | 崩溃恢复信任策略；与 #313 一起改 |
| `d33a2e7670` | `feat: implement bls verify precompile for gravity pipe execution (#254)` | engine-evm-execution-chainspec | BLS verify precompile；与 #283/#346 一起保留 |
| `605c372de6` | `feat(trie): support eth_getProof for nested hash, step 1 (#237)` | trie-all-layers, rpc-eth-and-debug | nested hash 下的 `eth_getProof` 第一步 |
| `0b57bc2340` | `opt(trie): add parallel level in nested hash (#219)` | trie-all-layers | nested hash 并行层 |
| `24f03242db` | `refactor(fmt): nighlty fmt the whole project (#220)` | tests-examples-config-infra, *跨多组* | nightly fmt 全项目，影响所有源文件的 hunk 对齐 |
| `6ddb557b5e` | `feat: Add block number tracking and oracle state fetching #245` | engine-evm-execution-chainspec | oracle 状态拉取 |
| `fd250d53d8` | `fix(rpc): set safe and finalized block when making canonical (#251)` | rpc-eth-and-debug, engine-evm-execution-chainspec | `safe`/`finalized` 写入；与 `a0d11f2288 #259` 一起 |
| `2ef67318b3` | `feat(rpc-provider): add MeteredBatchRequests(Future) (#19779) (#282)` | rpc-eth-and-debug | batched rpc metrics |
| `70157b8cf9` | `feat: add DKG transcript size validation with 100MB limit (#271)` | engine-evm-execution-chainspec | DKG transcript size guard |
| `dfa14dcdea` | `refactor: Add gravity configuration arguments (#168)` | node-builder-and-ethereum-node, cli-and-commands | `GravityArgs` + `init_gravity_config` 启动接线 |
| `e775fd5e72` | `fix: Batch size limiting for block persistence (#170)` | engine-evm-execution-chainspec, stages-pipeline | persistence batch 节流；与上游 backpressure 重写合并 |
| `2dde8ca181` | `state_root: implement cache and state write (#109)` | engine-evm-execution-chainspec, storage-providers-and-writers, trie-all-layers | Executor 扩展点起点（`transact_system_txn`/`apply_state_change`/`take_bundle`） |
| `3cd18422c9` | `use nested state root in history sync (#134)` | trie-all-layers, stages-pipeline | history sync 改走 nested state root |
| `9acbf22633` | `fix(merkle): Update merkle in trunk when history sync (#178)` | trie-all-layers, stages-pipeline | merkle trunk 修正 |

## 阅读指南

按以下顺序阅读 12 个子文档，与 §推荐解决顺序对齐：

1. 先读本 README（§摘要 + §Baseline + §解决策略 + §CHAIN-HALT 表）— 30 分钟。
2. 按依赖底层往上：
   - **底层（trait/接口/格式）**：`storage-db-and-mdbx` → `storage-api-and-traits` → `trie-all-layers`（先读 `crates/trie/common/*` 部分）→ `engine-evm-execution-chainspec`（先读 chainspec 部分）。
   - **中层（存储 / 执行 / stage）**：`storage-providers-and-writers` → `stages-pipeline` → `engine-evm-execution-chainspec`（engine tree / executor 部分）。
   - **上层（pool / rpc / node）**：`transaction-pool` → `rpc-eth-and-debug` → `net-prune-misc-crates` → `node-builder-and-ethereum-node` → `cli-and-commands`。
   - **末梢**：`tests-examples-config-infra`。
3. 每读一个子文档，回到本 README 的 §Gravity 保留 commits 交叉引用表对照 commit hash，确认正在保留的语义来源正确。
4. 收尾阶段：执行 `git status --porcelain | awk '/^(UU|AA|DU|AU|UA|DD)/'` 应为空；再 `cargo update -w && cargo check --workspace` 触发 `Cargo.lock` 重生成与编译验证；最后跑 `cargo nextest run --workspace --no-fail-fast` 留作 PR 前回归。
