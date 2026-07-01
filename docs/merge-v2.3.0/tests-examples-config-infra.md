# tests-examples-config-infra

## 分组概要

- **文件数：** 32
- **复杂度：** 低（一个例外：`README.md`，gravity 开场叙事必须保留在上游重写后的正文之上）
- **涉及模块功能：**
  - CI 编排：`.github/workflows/*.yml`、`.config/nextest.toml`、`.config/zepter.yaml`
  - 仓库治理：`.gitignore`、`deny.toml`、`typos.toml`、`README.md`、`CLAUDE.md`
  - 锁文件：`Cargo.lock`
  - 文档：`docs/vocs/docs/pages/run/faq/pruning.mdx`、`docs/vocs/docs/public/remote_exex.png`
  - 示例：`examples/bsc-p2p/tests/it/priority.rs`、`examples/custom-beacon-withdrawals/src/main.rs`、`examples/db-access/src/main.rs`
  - E2E 测试框架：`crates/e2e-test-utils/src/setup_import.rs`、`crates/e2e-test-utils/src/testsuite/actions/{mod,node_ops,produce_blocks}.rs`、`crates/e2e-test-utils/src/transaction.rs`、`crates/e2e-test-utils/tests/e2e-testsuite/main.rs`
  - Ethereum 节点 e2e：`crates/ethereum/node/tests/e2e/{dev,p2p,rpc}.rs`
  - 测试 crate：`testing/ef-tests/Cargo.toml`、`crates/ethereum/reth/Cargo.toml`
- **关键 gravity baseline 保留 commit（已通过 `git merge-base --is-ancestor … 0cb1687c1c` 验证全部在 baseline 历史里）：**
  - `46c91f90fe` — EIP-7702 acceptance tests（nextest.toml、integration.yml）
  - `9974ad0618` — 修复 unit.yml 的 CI 测试（zepter.yaml、unit.yml、produce_blocks.rs、e2e-testsuite/main.rs、rpc.rs `#[ignore]`、p2p.rs minor）
  - `99712f2834` — gravity 上禁用上游 workflow（bench/book/compact/e2e/integration/lint-actions/lint workflows 触发器收敛到 `workflow_dispatch:`）
  - `5901e7da98` — 把 unit + integration test 真的跑起来（integration.yml 加 `GRETH_DISABLE_PIPE_EXECUTION=1` 与 free-disk-space）
  - `30e93567d7` — gravity hive workflow（hive.yml 精简 test-rpc-compat job）
  - `a0d11f2288` — SYSTEM_CALLER 修复（lint.yml 的 `if: false # FIXME`、`nightly-2026-02-01` 这部分实际由 gravity 一系列 catch-up 累计落到 baseline）
  - `a1d7365bd6` — RocksDB 集成（testing/ef-tests/Cargo.toml、ethereum/reth/Cargo.toml 上 `reth-db` 不带 `mdbx` feature）
  - `fd250d53d8` — `ValidateSafeAndFinalizedBlocks` action（actions/mod.rs 重导出、actions/node_ops.rs 文件底部 ~130 LOC 新结构 + impl）
  - `6cc1001fcc` — typos.toml 中 `consts` / `Consts` 词条
  - `2d94684a9b` — deny.toml `allow-org = { github = ["Galxe"] }`
  - `671680af37` — deny.toml MPL-2.0 例外 / Galxe-aptos `allow-git` 等
  - `d620fd0eeb` — reth v1.8.3 catch-up（README.md gravity intro 实际更早，但 `d620fd0eeb` 是 baseline 上最新一次接触 README.md 的 commit）
- **不在本组文件上活动的 gravity commit：**
  - `364b851665` / `7d0483e565` 中的 50 Gwei 最小 base fee 全局 hardcode 部分**已在 `364b851665` 本身回退**（commit body 明确写 "Reverts the broken parts of commit 7d0483e565"），因此 `crates/e2e-test-utils/src/transaction.rs` 与 `crates/ethereum/node/tests/e2e/dev.rs` 两个文件上没有 gravity-side fee 改动；本组这两文件的冲突纯是 alloy / TaskManager → Runtime 接线。
  - `ba7e949473` — EIP-2935 工作触动 chainspec / hardfork 文件，不在 transaction.rs / dev.rs 的 gravity 历史里。
- **解决顺序依赖：**
  - `Cargo.lock` 必须在本组与其他组所有解决方案落地**之后**再重新生成。
  - `crates/ethereum/reth/Cargo.toml` 的 feature 标志取决于其他组对 `keccak-cache-global`、`otlp`、`portable`、`jemalloc-symbols`、`js-tracer` 在 `node-ethereum` / `cli-util` 中的传播结论（与 builder/node-core 跨组耦合）。
  - `setup_import.rs` 的 `ChainImportResult.task_manager` 字段被 gravity 多处测试引用，移除后下游调用点要在各自组里跟修。
- **CLAUDE.md 当前状态：** worktree 已经把 `CLAUDE.md` 解到 `120000` symlink 模式（指向 `AGENTS.md`），index 中也是 stage 0；但 `AGENTS.md` 工作树副本只有一行 `"AGENTS.md"` 字面量（9 字节），需要从 `v2.3.0:AGENTS.md` 恢复内容才能 commit。`CLAUDE.md~HEAD`（10942 字节常规文件）是合并工具留下的 HEAD-side 备份，要 `git rm` / 直接删。

---

## 逐文件分析

### `.config/nextest.toml`
**模块：** nextest 配置
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** 上游 commit `f60febfa6` "chore(ci): reduce default test timeout to 60s (#22212)" 把默认超时降到 60 秒，并叠加两个 override 块：`package(reth-era) and binary(it)`（10 × 2 分钟）、`package(reth-node-ethereum) and binary(e2e)`（5 × 1 分钟）。
**Gravity 侧变更：** baseline commit `46c91f90fe` (#343) 在 v1.8.3 catch-up `d620fd0eeb` 之后追加了 gravity 专属 override：`binary(gravity_pipe_test) + binary(gravity_eip2935_test) + binary(gravity_eip7702_test)`（5 分钟 × 6 = 30 分钟，`retries = 0`）。这些测试启动完整的 reth + MockConsensus 推送数百到数千区块，默认 60 秒会一律杀掉。
**影响范围：** 仅 CI 运行时长，无代码依赖。丢掉 gravity override 会让三个 gravity_* 测试 flake。
**解决方案建议：** mechanical-merge — 拼接两侧 `[[profile.default.overrides]]` 条目；保留 gravity override，叠加上游的 `reth-era` 与 `reth-node-ethereum` override。
**推理：** Gravity baseline `46c91f90fe` 是有意为之；上游叠加的两个 override 在 gravity 仓库中对应的 crate 都仍然存在。

---

### `.config/zepter.yaml`
**模块：** workspace feature 传播 linter
**冲突类型：** AA
**上游变更：** features 列表新增 `tracy`、`min-error-logs`、`min-warn-logs`、`min-info-logs`、`min-debug-logs`、`min-trace-logs`、`otlp`、`otlp-logs`、`js-tracer`、`portable`、`keccak-cache-global`、`trie-debug`、`secp256k1`（上游 commits `6c3fe54b2`、`0f7cd0fd9`、`1fbd5a95f`、`23f3f8e82`、`ab2ef9945`、`24fa984da`、`cff942ed0`）。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) 加 `--ignore-missing-propagate=reth-evm-ethereum/test-utils:grevm/test-utils`，容忍 grevm 的 test-utils 接线。
**影响范围：** Zepter 在 lint CI 中运行。删掉 gravity 行会让 `cargo zepter` 失败；丢掉上游 feature 会让传播缺失静默通过。
**解决方案建议：** mechanical-merge — features 列表取并集 + 保留 gravity 的 `--ignore-missing-propagate` 行。
**推理：** 两侧改动各自必需；feature 名称在 gravity 仓库中由各组 crate 提供，与下方 `crates/ethereum/reth/Cargo.toml` 跨组依赖一致。

---

### `.github/workflows/bench.yml`
**模块：** CI bench workflow
**冲突类型：** UU
**上游变更：** 大幅扩展（41 → 1735 行）—— 通过 Engine API 在 schelk 管理的 snapshot 上回放真实区块，依赖 `git status` 中新增的 `.github/scripts/bench-*.sh|.py|.js` 辅助脚本（depot runner + ClickHouse + Slack 接线）。
**Gravity 侧变更：** baseline commit `99712f2834` (#247) 把 trigger 收敛为只允许 `workflow_dispatch:`，其余 PR/merge_group/push 触发器被注释。Body 保留上游 v1.8.3 时期的轻量阶段。
**影响范围：** 仅 GitHub Actions。Gravity 没有 paradigm 内部的 schelk/depot/ClickHouse 基础设施。
**解决方案建议：** keep-gravity（保留 `workflow_dispatch:` 唯一触发器与精简 body）。新加的 `.github/scripts/bench-*` 文件可以留在磁盘但 workflow 不会调用。
**推理：** Gravity baseline `99712f2834` 是有意禁用以节约 runner；上游扩展依赖外部专有设施。

---

### `.github/workflows/book.yml`
**模块：** 文档站点部署
**冲突类型：** UU
**上游变更：** action SHA pin（`actions/checkout@de0fac2e... # v6.0.2`、`mozilla-actions/sccache-action@7d986dd... # v0.0.9`、`actions/configure-pages@...`、`actions/upload-pages-artifact@...`、`actions/deploy-pages@...`），上游 commit `b89288582` "ci: harden supply chain across all workflows (#23785)"；新增 `permissions: {}`；runner 切到 `depot-ubuntu-latest-8`；Bun 固定到 v1.2.23；上传路径由 `docs/vocs/docs/dist` 改为 `docs/vocs/docs/dist/public`（Vocs v2 输出布局，上游 commits `ecd117e79`、`d80418f84`、`84fb8008a`）。
**Gravity 侧变更：** baseline commit `99712f2834` 把 trigger 收敛为 `workflow_dispatch:`。
**影响范围：** 文档构建；gravity 不部署 paradigmxyz.github.io。
**解决方案建议：** mechanical-merge — 保留 gravity 的 `workflow_dispatch:` 唯一触发器；body 部分采纳上游（SHA-pinned actions、`permissions: {}`、`dist/public` 路径），这样 operator 手动触发时仍能针对 Vocs v2 构建成功。
**推理：** Gravity baseline `99712f2834` 保留触发器门控；body 部分上游对 Vocs v2 的迁移是必要的，否则手动触发会 broken。

---

### `.github/workflows/compact.yml`
**模块：** CI compact codec 测试
**冲突类型：** AA
**上游变更：** `permissions: {}` 块、runner 切到 `depot-ubuntu-latest`、SHA-pinned actions、`RUSTC_WRAPPER: sccache`、`mozilla-actions/sccache-action`。上游 commit `372802d06` "chore: remove op-reth from repository (#21532)" 删除了 `op-reth` matrix 条目。
**Gravity 侧变更：** baseline commit `99712f2834` 把 trigger 改为 `workflow_dispatch:`；baseline 上 matrix 仍带 `op-reth` matrix 条目（v1.8.3 catch-up `d620fd0eeb` 时还在；gravity 自身没有在 compact.yml 删 op-reth 的 commit）。
**影响范围：** 仅 workflow。`op-reth` 行在 gravity 仓库已无对应 crate，是死代码。
**解决方案建议：** mechanical-merge — 保留 gravity 的 `workflow_dispatch:`，**删除** `op-reth` matrix 条目（gravity 仓库无对应 crate），应用上游的 `permissions: {}` 与 action SHA pin。
**推理：** Gravity baseline `99712f2834` 保留触发器；no-OP 约束适用于 matrix；上游加固是任何项目都该采纳。

---

### `.github/workflows/e2e.yml`
**模块：** CI e2e 测试
**冲突类型：** AA
**上游变更：** action SHA pin、`permissions: {}`、depot runner、sccache wrapper（上游 commit `b89288582`、`1406a984a` "ci: pass --no-fail-fast"、`b9969c5b1` "remove rocksdb feature gate"）。
**Gravity 侧变更：** baseline commit `99712f2834` 把 trigger 改为 `workflow_dispatch:`。`5781df248c` "fix ut compilation after merge v1.8.3 (#207)" 涉及该文件但不改 trigger，是后续 catch-up 修复。
**影响范围：** 仅 CI。
**解决方案建议：** mechanical-merge — 保留 gravity 的 `workflow_dispatch:`；上游 action SHA pin 与 `permissions: {}` 块叠加。
**推理：** Gravity baseline `99712f2834` 保留触发器；上游加固叠加。

---

### `.github/workflows/hive.yml`
**模块：** Hive Ethereum 协议一致性测试
**冲突类型：** UU
**上游变更：** 大规模重写（188 → 452 行）。新增 `build-reth` + `prepare-hive` matrix job（`amsterdam`、`osaka` 变体）、可复用 `./.github/workflows/docker-test.yml`、hive 资产缓存、按模拟器分片的 `test-amsterdam` matrix；脚本从 `.github/assets/hive/*` 迁移到 `.github/scripts/hive/*`（上游 commit `8d37f76d2`、`d4ca2e268`、`101385a66`）；只在 cron schedule 触发。
**Gravity 侧变更：** baseline commit `30e93567d7` (#244) 移植了一个精简 hive，在 `main` 的 PR/push 时跑 `test-rpc-compat`，保留了旧路径 `.github/assets/hive/*` 下的脚本。
**影响范围：** 仅 CI。
**解决方案建议：** keep-gravity — 保留 gravity 精简的 `test-rpc-compat` job 与 PR/push 触发器。
**推理：** Gravity baseline `30e93567d7` 是有意的 gravity 一致性测试覆盖；上游 amsterdam+osaka 多变体 matrix 依赖 depot/cache infra，移植成本不值得。

---

### `.github/workflows/integration.yml`
**模块：** CI integration 测试
**冲突类型：** UU
**上游变更：** `RUSTC_WRAPPER: "sccache"`、depot runner（`depot-ubuntu-latest-4`）、`permissions: {}`、SHA-pinned actions、`.github/assets/install_geth.sh` → `.github/scripts/install_geth.sh` 路径迁移（上游 commit `b89288582`、`8d37f76d2`、`6b1ad1f4c`）。
**Gravity 侧变更：** `5901e7da98` (#173) 开启 CI 跑该 workflow，加 `GRETH_DISABLE_PIPE_EXECUTION: 1` env、`CARGO_INCREMENTAL: 0`、free-disk-space 步骤；`99712f2834` 收敛 trigger 到 `workflow_dispatch:`；`46c91f90fe` (#343) 把 `binary(gravity_eip7702_test)` 加入 nextest binary 列表。
**影响范围：** CI；gravity_eip7702_test 必须在 nextest 命令行；`GRETH_DISABLE_PIPE_EXECUTION=1` 关闭 pipe-exec layer，避免与 mainnet-like integration test 冲突。
**解决方案建议：** mechanical-merge — 保留 gravity 的 env (`GRETH_DISABLE_PIPE_EXECUTION=1`、`CARGO_INCREMENTAL=0`)、`workflow_dispatch:` 触发器、free-disk-space 步骤、nextest binary 列表（含 `gravity_eip7702_test`）、`--exclude "op-reth"/"reth-op"/"reth-optimism-*"` 排除；机械应用上游 `install_geth.sh` 路径 rename 与 SHA pin。
**推理：** Gravity baseline `46c91f90fe`、`5901e7da98`、`99712f2834` 全部 in-baseline；脚本 rename 是 upstream-only 的纯路径迁移。

---

### `.github/workflows/lint-actions.yml`
**模块：** actionlint
**冲突类型：** UU
**上游变更：** SHA-pinned `actions/checkout@de0fac2e... # v6.0.2`；`permissions: {}` 块 + 每 job `contents: read`（上游 commit `b89288582`、`e62cb8f82`）。
**Gravity 侧变更：** baseline commit `99712f2834` 改为 `workflow_dispatch:`。
**影响范围：** 仅 CI。
**解决方案建议：** mechanical-merge — 保留 gravity trigger 门控；叠加上游 `permissions:` 块与 SHA pin。
**推理：** Gravity baseline `99712f2834` 保留；上游 permissions 加固叠加。

---

### `.github/workflows/lint.yml`
**模块：** clippy / fmt lint
**冲突类型：** UU（15 个冲突区域 — 本组最多）
**上游变更：** runner 升级、SHA-pinned actions、`mozilla-actions/sccache-action`、`RUSTC_WRAPPER: sccache`；`372802d06` 删除 `op-reth` matrix；`5a9dd0230` 把 MSRV 升到 1.93；`b89288582` 加固；`4af4836ec` "ci: pin nightly to 2026-02-21"；`21dadb71c` "fix: update shellexpand to 3.1.2 and unpin nightly"。
**Gravity 侧变更：** baseline commit `a0d11f2288` (#259) 触动了 lint 流程；`99712f2834` 收敛 trigger 到 `workflow_dispatch:`；baseline 上 `lint.yml` 多处有 `if: false # FIXME: ...`（`clippy-binaries`、`wasm`、`crates-io-check`、`docs`、`udeps`、`grafana` 等 job 都被屏蔽），toolchain 全部 pin 到 `nightly-2026-02-01`，这些是 gravity 一系列 catch-up 累积的现状。
**影响范围：** 仅 CI。
**解决方案建议：** keep-gravity（保留所有 `if: false # FIXME` 屏蔽行、`nightly-2026-02-01` toolchain pin、`workflow_dispatch:` 触发器、`--exclude` op-reth/reth-op/reth-optimism-* 项）；可选叠加 `permissions:` 块与 SHA pin。
**推理：** Gravity baseline 的 `if: false # FIXME` 是显式 TODO 标记 — 开启会立刻阻塞 PR；`nightly-2026-02-01` pin 与 gravity 工具链兼容性绑定，不能轻动。

---

### `.github/workflows/unit.yml`
**模块：** unit-test CI
**冲突类型：** UU
**上游变更：** `RUSTC_WRAPPER: sccache`、`permissions: {}`、depot runner、SHA-pinned actions、`--no-fail-fast`（上游 `b89288582`、`1406a984a`、`0d8d48a16`、`b9969c5b1`）。`372802d06` 删 op-reth matrix 项；`9359e21f9` "enable debug assertions for statetests"。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) — 将 matrix 拆为 `partition: 1/2 + 2/2`（hash 分片），加入 `--features "asm-keccak ethereum config-from-env" --locked`、`GRETH_DISABLE_PIPE_EXECUTION: 1`、free-disk-space 步骤；`5901e7da98` 开启 CI 跑；`5781df248c` 是 catch-up 编译修复。`state:` job 上 baseline 已带 `if: false # FIXME`。
**影响范围：** 仅 CI。
**解决方案建议：** keep-gravity — 保留 partition 分片、`GRETH_DISABLE_PIPE_EXECUTION=1`、free-disk-space、partition feature 集合、`--exclude` op-reth/reth-optimism-*、`state` job 的 `if: false`；叠加上游 `permissions: {}` 与 SHA pin。
**推理：** Gravity baseline `9974ad0618` + `5901e7da98` 是有意为之的运行时成本拆分；上游 permissions 加固叠加。

---

### `.gitignore`
**模块：** 仓库治理
**冲突类型：** UU
**上游变更：** 新增 `docs/vocs/docs/pages.gen.ts`（Vocs v2 路由 typegen 输出，上游 commit `7ab758fff` "chore(docs): upgrade Vocs to v2 (#24849)"）。
**Gravity 侧变更：** baseline 上无 gravity 内容修改（最近一次 touch 是 `d620fd0eeb` 的 v1.8.3 catch-up merge）。
**影响范围：** 仅构建产物过滤。
**解决方案建议：** take-upstream — 接受 `docs/vocs/docs/pages.gen.ts` 这行新增。
**推理：** 纯叠加；Vocs v2 typegen 是上游文档系统的一部分。

---

### `CLAUDE.md~HEAD`
**模块：** AI 开发指南（HEAD-side 备份文件）
**冲突类型：** AU（worktree 顶层独立路径；上游 v2.3.0 已把 `CLAUDE.md` 改成 symlink → `AGENTS.md`）
**上游变更：** 上游 commit `52ab4223a0` "chore(meta): rename CLAUDE.md to AGENTS.md, symlink CLAUDE.md to it (#23203)" 把 CLAUDE.md 改成 symlink，把规范文本搬到 `AGENTS.md`；之后 `d5b5caa439`、`3de9259026`、`20ae9ac405` 编辑了 `AGENTS.md`。
**Gravity 侧变更：** baseline `CLAUDE.md` 内容来自 `d620fd0eeb` v1.8.3 catch-up（gravity 没有再独立编辑 `CLAUDE.md`）。
**影响范围：** worktree 当前 index 已是 `CLAUDE.md` 120000 symlink → `AGENTS.md`（stage 0），但 worktree 中的 `AGENTS.md` 只有一行字面量 `"AGENTS.md"`（9 字节，blob `e4aa901d6f`），不是 v2.3.0 的正本 — 这是合并工具留下的损坏状态。`CLAUDE.md~HEAD`（10942 字节）是 HEAD-side 内容的备份。
**解决方案建议：** take-upstream + 修复 worktree：
  - `git rm CLAUDE.md~HEAD`（HEAD-side 内容丢弃，让 symlink 胜出）
  - 修复 `AGENTS.md`：`git show v2.3.0:AGENTS.md > AGENTS.md` 然后 `git add AGENTS.md`
**推理：** gravity baseline 上 `CLAUDE.md` 没有独立内容性修改；上游 rename 胜出。损坏的 `AGENTS.md` 是合并副作用，必须在 commit 前修复。

---

### `Cargo.lock`
**模块：** 锁文件
**冲突类型：** UU
**解决：** DEFER — 在所有其他组解决方案落地后，清掉冲突标记并 `cargo update --workspace`（或 `cargo metadata --locked`）重新生成。不做逐文件分析。
**推理：** 按任务规范延后到最后一步。

---

### `README.md`
**模块：** 仓库首页
**冲突类型：** UU（8 个冲突区域）
**上游变更：** README 改写为更精简、bullet 化的 "Reth 2.0" 风格（上游 commits `5568b76d5`、`53e1ec81b`、`76bdfb30f`）；图片由 `assets/reth-prod.png` 改为 `assets/reth-2.png`；安装 URL 由 `paradigmxyz.github.io/reth/...` 改为 `reth.rs/installation/installation`；Goals 列表全部压成短 bullet；新增 "Reth 2.0 released in April 2026" 历史；Storage compatibility 段改写为 Storage V2 介绍。
**Gravity 侧变更：** baseline `d620fd0eeb` 时期 README 顶部携带整段 "Gravity Reth: The Fastest Open-Source EVM Execution Client" 开场 — ERC20 基准图（`assets/erc20-transfer-test.png`）、Grevm/Parallel Merklization/Gravity Cache/Pipeline 架构推介、docs.gravity.xyz 链接，之后是 `# Reth Original README` 分隔符接旧版上游 README。
**影响范围：** 公开面着陆页。`assets/erc20-transfer-test.png` 必须继续存在；docs.gravity.xyz 链接是 gravity 品牌承诺。
**解决方案建议：** mechanical-merge — 保留 gravity 开场（intro、ERC20 图、Grevm/Merklization/Cache/Pipeline 要点、docs.gravity.xyz 链接、`# Reth Original README` 分隔符）；分隔符之下采用上游 v2.3.0 改写后的正文（bullet 化 Goals、`assets/reth-2.png`、`reth.rs` 安装 URL、Reth 2.0 release 段、Storage V2 段）。
**推理：** Gravity baseline `d620fd0eeb` 携带的开场叙事是这个 fork 的核心 identity；上游 body 重写是 gravity README 通过 "Reth Original README" 标题主动让位的内容，保持新鲜。

---

### `deny.toml`
**模块：** cargo-deny 配置
**冲突类型：** UU
**上游变更：** 新增 ignore：`RUSTSEC-2024-0384`（sse example）、`RUSTSEC-2023-0089`（atomic-polyfill 经 test-fuzz）、`RUSTSEC-2026-0002`（lru/discv5）、`RUSTSEC-2026-0097`（rand）、`RUSTSEC-2026-0173`（proc-macro-error2，列了两次）、`RUSTSEC-2026-0118`（hickory-proto/net）。新增 `allow-git`：`DaniPopes/slotmap.git`、`sigp/discv5`。删除 MPL-2.0 exceptions（设为 `exceptions = []`）。上游 commits `f93b41249`、`38c627ce8`、`87d878a97`、`76e45117d`。
**Gravity 侧变更：** baseline commits `9974ad0618`、`671680af37`、`2d94684a9b` 累计加上 `RUSTSEC-2024-0436` (paste!)、`RUSTSEC-2024-0320` (yaml-rust 经 gravity-aptos)、`RUSTSEC-2025-0141` (bincode)；`allow-git` 加 `aptos-labs/bcs`、`Galxe/gravity-aptos`；MPL-2.0 exceptions 保留 `option-ext` + `webpki-root-certs`；`allow-org = { github = ["Galxe"] }`。
**影响范围：** `cargo deny check` 在 CI 中运行。删 gravity 条目会让 gravity-aptos 拉入的 crate 构建失败；删上游条目会让上游新 advisory 卡 CI。
**解决方案建议：** mechanical-merge — ignore 列表、`allow-git` 列表、license exceptions 取并集；保留 `allow-org = { github = ["Galxe"] }`；MPL-2.0 exceptions 保留 gravity 形态（上游 `exceptions = []` 是把 MPL-2.0 也放进了 `allow` 列表里，gravity 走的是 `exceptions` 路线，机械合并时取 gravity 的 exceptions 表）。
**推理：** 两套 ignore 对应各自传递依赖；gravity baseline 三个 commit 全部 in-baseline；`allow-org` Galxe 是 gravity 独占。

---

### `typos.toml`
**模块：** typos lint 配置
**冲突类型：** AA
**上游变更：** `extend-exclude` 加 `arena.txt`（上游 commit `792c8f255` "feat(trie): ArenaParallelSparseTrie"）；字典加 `BA`（EIP-7928 BAL）、`writeable`（上游 `082c36ebe`）；`consts` / `Consts` 上游也加（commit `f4943abf7`）。
**Gravity 侧变更：** baseline commit `6cc1001fcc` (#229) 加 `consts` / `Consts`（与上游内容重叠）。
**影响范围：** typos lint。
**解决方案建议：** mechanical-merge — 词表取并集（`consts` / `Consts` 两侧相同保留一份；加入上游 `BA`、`writeable`、`arena.txt` 排除）。
**推理：** 纯叠加；gravity baseline `6cc1001fcc` 与上游 `f4943abf7` 落到同一内容。

---

### `docs/vocs/docs/pages/run/faq/pruning.mdx`
**模块：** 文档（pruning FAQ）
**冲突类型：** AU
**上游变更：** 上游 commit `f6f623c66` "docs: promote storage mode docs (#24351)" 把文件 rename 到 `docs/vocs/docs/pages/run/storage/pruning.mdx`，旧路径不存在。
**Gravity 侧变更：** baseline 上无 gravity 修改（最近 touch 是 `d620fd0eeb` v1.8.3 catch-up）。
**影响范围：** 仅文档。
**解决方案建议：** take-upstream — `git rm docs/vocs/docs/pages/run/faq/pruning.mdx`（新路径已从上游侧存在）。
**推理：** 无 gravity 改动；rename 胜出。

---

### `docs/vocs/docs/public/remote_exex.png`
**模块：** 文档二进制资源
**冲突类型：** AU
**上游变更：** 上游 commit `ecd117e79` "fix(docs): publish Vocs v2 static output (#24789)" 把文件 rename 到 `docs/vocs/public/remote_exex.png`，blob hash 不变（`8606616e81…`）。
**Gravity 侧变更：** 无。
**影响范围：** 文档构建产物。
**解决方案建议：** take-upstream — `git rm docs/vocs/docs/public/remote_exex.png`（新路径已存在，blob 完全相同）。
**推理：** 相同 blob，纯路径迁移。

---

### `examples/bsc-p2p/tests/it/priority.rs`
**模块：** rename-detection 幻影
**冲突类型：** AU
**Gravity 侧变更：** gravity 侧 `examples/bsc-p2p/tests/it/` 目录下**只有** `main.rs` 与 `p2p.rs`（验证：`git ls-tree 0cb1687c1c examples/bsc-p2p/tests/it/`），**没有** `priority.rs`。文件在 gravity 侧从未存在。
**上游变更：** upstream v2.3.0 上 `examples/bsc-p2p/tests/it/` 同样只有 `main.rs` 与 `p2p.rs`。
**影响范围：** git rename detection 误映射产生的幻影 AU；实际上两侧路径都不存在这个文件。
**解决方案建议：** `git rm examples/bsc-p2p/tests/it/priority.rs`。
**推理：** 纯 rename-detection 副产物；两侧都无此路径，删除即可消解 AU。

---

### `examples/custom-beacon-withdrawals/src/main.rs`
**模块：** 示例 — 通过 system call 实现自定义 beacon withdrawals
**冲突类型：** AA
**上游变更：** 在 `alloy-evm` API 变动后重构，引入 `GasOutput`、`EthTxResult`、`block::StateDB`、`EvmFactory`；闭包从 `move |…|` 改为 `async move |builder, _|`；删除 `BlockExecutorFor`、`Database`、`OnStateHook`、`State<DB>`、`alloy_sol_macro::sol` 等旧 import；executor 增加 `TxType` 参数。上游 commits `fa7c66c14` "refactor: integrate state hook from State<DB> (#24654)"、`671da5588` "refactor: expose executor transaction result type (#23759)"、`8784aa45f` "chore: bump revm to v37 (EIP-8037 state gas)"。
**Gravity 侧变更：** baseline 历史中此文件最近一次 touch 是 `d620fd0eeb` (v1.8.3 catch-up)；自此没有 gravity 独立修改。
**影响范围：** 该示例在 `cargo check --workspace` 中检查；签名必须跟 gravity vendored 的 alloy-evm 接线一致。
**解决方案建议：** take-upstream。
**推理：** 自 v1.8.3 catch-up 后 gravity baseline 无独立改动；本文件是上游 API 展示，必须跟随 v2.3.0 alloy-evm API。Gravity 其他组对 alloy-evm 的升级（在 evm/builder 组）应已落地匹配类型。

---

### `examples/db-access/src/main.rs`
**模块：** 示例 — 只读 DB 访问
**冲突类型：** UU
**上游变更：** 新增 `BlockNumReader` import；`open_read_only` 签名加 `runtime: Runtime` 参数（上游 commit `68e4ff1f7` "feat: global runtime (#21934)"）；`receipts_provider_example` 重写为用 `keccak256` 显式构造 event filter，topic 参数换成 `indexed_from`、`indexed_to` Address pair（上游 `9f3949cd3`、`90e265134`）；删除 total difficulty 用法（上游 `e21048314`）。
**Gravity 侧变更：** baseline 上无 gravity 独立修改（最近 touch 是 `d620fd0eeb` 的 v1.8.3 catch-up）。
**影响范围：** 示例要对 gravity provider API 编译。
**解决方案建议：** take-upstream。
**推理：** 无 gravity 改动；签名必须跟 `crates/storage/provider/...`（其他组）一致。

---

### `testing/ef-tests/Cargo.toml`
**模块：** ef-tests harness manifest
**冲突类型：** UU
**上游变更：** `reth-primitives-traits` 加 `features = ["rayon"]`；`reth-db` 加显式 `features = ["mdbx", "test-utils", "disable-lock"]`；`revm` features 升级到含 `memory_limit`、`p256-aws-lc-rs`（上游 `b27169430` "perf(revm): enable p256-aws-lc-rs feature"、`dc8efbf9b` "feat: add --rpc.evm-memory-limit flag"）；删除 `reth-stateless` 依赖（上游 `c915841a4` "chore(stateless): Remove reth-stateless crate"）；`reth-revm` 不再带 features。
**Gravity 侧变更：** baseline commit `a1d7365bd6` (#212) "feat(rocksdb): Integrating RocksDB into Reth" 把 `reth-db` 的 `mdbx` feature 拿掉（gravity 走 mdbx + rocksdb 多后端动态选择），加上 `reth-stateless` 依赖与 `reth-revm` 的 `witness` feature。
**影响范围：** ef-tests 二进制构建；gravity 数据库后端选择 + witness 测试覆盖。
**解决方案建议：** mechanical-merge：
  - `reth-db` 采用 gravity 形态 `{ workspace = true, features = ["test-utils", "disable-lock"] }`（不带 `mdbx`）
  - `reth-revm` 保留 `features = ["std", "witness"]`
  - 保留 `reth-stateless` 依赖（除非上游 `c915841a4` 删除后 gravity vendored 也已经 drop — 跨组确认）
  - `reth-primitives-traits` 跟 gravity baseline 一致（不带 `rayon`），除非 rayon 已是 workspace 默认
  - 采纳上游 `revm` features `memory_limit` + `p256-aws-lc-rs`（性能/正确性增益，gravity 无冲突）
**推理：** Gravity baseline `a1d7365bd6` in-baseline；`reth-stateless` 是否保留取决于其他组该 crate 是否仍存在（跨组依赖）。

---

### `crates/e2e-test-utils/src/setup_import.rs`
**模块：** e2e 测试 harness — RLP import 初始化
**冲突类型：** AA
**上游变更：** `attributes_generator` 闭包参数类型从 `EthPayloadBuilderAttributes` 切到 `alloy_rpc_types_engine::PayloadAttributes`（上游 commit `cf83b198d` "refactor: remove PayloadBuilderAttributes (#23202)"）；`TaskManager::current()`/`tasks.executor()` 全部换为 `reth_tasks::Runtime::test()`（上游 `68e4ff1f7` "feat: global runtime"、`0ba685386` "refactor: dedup runtime initializations"）；`ChainImportResult` 删除 `task_manager: TaskManager` 字段；`ProviderFactory::new` 路径也跟着调整。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) 在 v1.8.3 catch-up 之上保留 `EthPayloadBuilderAttributes` 闭包签名与 `ChainImportResult.task_manager: TaskManager` 字段、`TaskManager::current()` + `tasks.executor()` 接线。本文件内 baseline 整体仍是 v1.8.3 形态。
**影响范围：** test-utils 公开 API：`ChainImportResult.task_manager` 字段被 gravity 多处测试解构引用。
**解决方案建议：** take-upstream（内部 wire 走 `Runtime::test()`），同时**临时保留** `ChainImportResult.task_manager` 字段以兼容下游调用点（若必须的话用 wrapper 字段或在跨组修复完成前作为 `Option`/`TaskManager` field 暴露）。下游调用点的机械修复在各自组处理；这里标为顺序依赖。
**推理：** 上游 `Runtime::test()` 是 v1.8.3 → v2.3.0 唯一向前兼容路线；保留字段是过渡兼容措施。

---

### `crates/e2e-test-utils/src/testsuite/actions/mod.rs`
**模块：** test action 重导出
**冲突类型：** AA
**上游变更：** 上游 commit `a047a055a` "chore: bump rust to edition 2024" 没有改 reexport 列表；冲突区无上游新导出。
**Gravity 侧变更：** baseline commit `fd250d53d8` (#251) "fix(rpc): set safe and finalized block when making canonical" 把 `ValidateSafeAndFinalizedBlocks` 加入 `pub use node_ops::{...}` 重导出。
**影响范围：** test-utils 公开 API；下游 gravity 测试引用 `ValidateSafeAndFinalizedBlocks`。
**解决方案建议：** keep-gravity — 重导出列表保留 `ValidateSafeAndFinalizedBlocks`（机械合并：上游未删除任何项）。
**推理：** Gravity baseline `fd250d53d8` in-baseline；纯叠加。

---

### `crates/e2e-test-utils/src/testsuite/actions/node_ops.rs`
**模块：** test action — node operations
**冲突类型：** AA
**上游变更：** 上游 commit `08fc0a918` "feat: eth_fillTransaction (#19199)" 在所有 `EthApiClient::<TransactionRequest, Transaction, Block, Receipt, Header>` 调用点结尾追加 `TransactionSigned` 泛型参数（新 `Tx` 泛型）；`a047a055a` "bump rust to edition 2024"。
**Gravity 侧变更：** baseline commit `fd250d53d8` (#251) 在文件末尾新增 `ValidateSafeAndFinalizedBlocks` 结构体 + `impl Action<Engine>`（baseline 中约 130 行：第 388 行 struct、397 行 impl、419 行 `impl Action`、433/443 行调用 `EthApiClient::<…>::block_by_hash` 获取 safe/finalized block）。
**影响范围：** 类型系统：所有 `EthApiClient::<…>` 调用点必须加 `TransactionSigned` 才能编译，包括 gravity 新增 `ValidateSafeAndFinalizedBlocks` impl 内部的调用点。
**解决方案建议：** mechanical-merge — 在所有 `EthApiClient::<…>` 调用点（含 gravity 新增 impl 内部）应用上游的 `TransactionSigned` 泛型添加；保留 gravity 文件末尾的结构体 + impl 块。
**推理：** Gravity baseline `fd250d53d8` 是 gravity 专属断言；上游新泛型对新 `EthApiClient` trait 签名是强制的。

---

### `crates/e2e-test-utils/src/testsuite/actions/produce_blocks.rs`
**模块：** test action — 区块生产
**冲突类型：** AA
**上游变更：** 在 `EthApiClient::<…>` 调用点加 `TransactionSigned` 泛型；新增 `use reth_ethereum_primitives::TransactionSigned`；`AssertMineBlock` 流程把 `fork_choice_updated_v2` 单一调用换为 `match v2 -> Err -> fallback to v3` switch（让同一 action 跨 cancun → prague 工作）；debug 日志格式由 `"FCU result: {:?}"` 改成结构化 `?fcu_result, "FCU v2 result"`。上游 commits `08fc0a918`、`adb4f4847`、`bfb7ab72f`、`16e79888a`。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) 仅 minor —— 保留单一 v2 路径，debug 日志为非结构化 `"FCU result: {:?}"`，无 v3 fallback。
**影响范围：** test action 行为；v2 失败时是否 fallback 到 v3 影响跨 fork 测试覆盖。
**解决方案建议：** take-upstream — 采纳上游 v2 → v3 fallback 逻辑与 `TransactionSigned` 泛型；gravity 侧日志格式是外观差异让位。
**推理：** Gravity baseline `9974ad0618` 在该文件中纯是 catch-up 后日志风格保留，无业务语义；上游 fallback 增加跨 fork 覆盖。

---

### `crates/e2e-test-utils/src/transaction.rs`
**模块：** test 辅助 — 交易构造
**冲突类型：** UU
**上游变更：** 新增 `EthereumTxEnvelope` import 与 `BlobTransactionSidecarVariant`；`alloy_network` 改用 `NetworkTransactionBuilder`（替代 `TransactionBuilder`）；`validate_sidecar` 签名拓宽为 `EthereumTxEnvelope<TxEip4844Variant<BlobTransactionSidecarVariant>>`；新增 `transfer_tx_bytes_with_nonce` helper（`max_fee_per_gas = 1000e9`，1000 gwei，清除 basefee 影响）。上游 commits `bfb7ab72f` "chore: bump alloy to 2.0.0 (#23407)"、`11d9f3807` "test(e2e): comprehensive RocksDB storage E2E tests (#21423)"、`ca26219aa` "feat: convert blobs at RPC"。
**Gravity 侧变更：** 该文件上**无** gravity-side 内容修改。`364b851665` (#337) / `7d0483e565` (#335) 中触及本文件的 tx fee 提升在 `364b851665` 本身即已回退（commit body 明确：`crates/e2e-test-utils/src/transaction.rs and crates/ethereum/node/tests/e2e/dev.rs: revert tx fee bumps`），所以 `max_priority_fee_per_gas: Some(20e9 as u128)` 与 upstream 数值一致，两侧相同，不冲突。
**影响范围：** test 类型签名 + 新 helper 被其他组测试使用。
**解决方案建议：** take-upstream — 采纳上游所有类型改动（`EthereumTxEnvelope`、`BlobTransactionSidecarVariant`、`NetworkTransactionBuilder`、`transfer_tx_bytes_with_nonce` helper、`validate_sidecar` 拓宽签名）。
**推理：** Gravity baseline 此文件无独立修改；冲突纯是 alloy API 升级。

---

### `crates/e2e-test-utils/tests/e2e-testsuite/main.rs`
**模块：** test-suite crate 的集成测试入口
**冲突类型：** AA
**上游变更：** `EthApiClient::<…>` 调用点加 `TransactionSigned` 泛型（上游 `08fc0a918`）；`PayloadAttributes` literal 加 `slot_number: None` 字段；新增 `E2ETestSetupBuilder` import 与 `test_setup_builder_with_custom_tree_config` 测试；`test_testsuite_multinode_block_production` 加 `ProduceBlocks` 后续步骤。上游 commits `01820fdaf`、`cf83b198d`、`bfb7ab72f`、`68e4ff1f7`。
**Gravity 侧变更：** baseline `9974ad0618` 在此文件中删了 3 行（无业务语义改动）。
**影响范围：** 测试要对 gravity 的 `PayloadAttributes` 形态（是否含 `slot_number`）编译。
**解决方案建议：** take-upstream。
**推理：** Gravity baseline 无业务语义改动；`slot_number` 字段与跨组 `alloy-rpc-types-engine` 设置一致即可（在该 crate 组确认）。

---

### `crates/ethereum/node/tests/e2e/dev.rs`
**模块：** dev-node e2e
**冲突类型：** UU
**上游变更：** 彻底删除 `TaskManager` 用法，切到 `reth_tasks::Runtime::test()`（上游 `68e4ff1f7`、`0ba685386`）；新增 `reth_primitives_traits::transaction::TxHashRef` import；`assert_chain_advances` 用 `TxHashRef` 处理 tx_hash（上游 `00f9bd2a9` "fix: use tx_hash for transaction identity"）；`alloy_eips::eip2718::Encodable2718` 由上游 v2.3.0 删除（已在其他文件被使用）；`FullNodePrimitives` 由上游 `936baf123` "refactor: remove FullNodePrimitives" 删掉。
**Gravity 侧变更：** 此文件上没有 gravity 独立 fee 修改。`364b851665` / `7d0483e565` 在 `dev.rs` 上触及的 tx fee 提升在 `364b851665` 本身即已回退（commit body 明确：`revert tx fee bumps and the custom_chain genesis tweak`）。gravity 侧仍是 v1.8.3 时期形态 + TaskManager 接线。
**影响范围：** 测试要对 gravity 的 task 接线与 ethereum spec 同时编译。
**解决方案建议：** take-upstream — 采纳上游 `Runtime::test()` 迁移、`TxHashRef` import、`FullNodePrimitives` 删除等。
**推理：** Gravity baseline 此文件无独立 fee 改动；纯 alloy/tasks API 升级。

---

### `crates/ethereum/node/tests/e2e/p2p.rs`
**模块：** p2p e2e 测试
**冲突类型：** UU
**上游变更：** 大量叠加 —— 新增 `can_launch_with_net_if_and_shared_discovery_port`、`setup_engine_with_connection` 等测试，覆盖 NAT/discv4-discv5 共享端口（上游 commit `06b2d3730` "fix(net): bind discovery to net-if address (#24178)"、`68e4ff1f7` "feat: global runtime"、`a43128277`）；新增 imports `TxEip1559`、`IndexedRandom`、`NetworkInfo`、`PeersInfo`、`Runtime`、`UdpSocket`、`Duration` 等。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) 仅 minor（git show 显示该文件 `+2` 行：catch-up 风格的小修），无业务内容。
**影响范围：** 上游新增测试假定 `Runtime::test()` 与 net-if 配置存在。
**解决方案建议：** take-upstream。
**推理：** Gravity baseline 此文件无业务改动；上游新增测试是有价值的覆盖。

---

### `crates/ethereum/node/tests/e2e/rpc.rs`
**模块：** rpc e2e 测试
**冲突类型：** AA
**上游变更：** 新增 imports `BuilderBlockValidationRequestV6`、`SignedBidSubmissionV6`、`AdminApiServer`、`eth_payload_attributes_amsterdam`、`NatResolver`、`PeersInfo`、`Runtime`、`NodeBuilder`、`NodeHandle`、`NetworkArgs`、`RpcServerArgs`、`NodeConfig`、`IpAddr`、`Ipv4Addr`、`Bytes` 等；新增 discv5 port advertise 测试（上游 commits `04d67cb14`、`5acc992eb`、`473f85c55`）；`setup_engine` 调用返回元组从 `(mut nodes, _tasks, wallet)` 改成 `(mut nodes, wallet)`（来自 `Runtime::test()` 迁移 `0ba685386`）。
**Gravity 侧变更：** baseline commit `9974ad0618` (#241) 给 `test_fee_history` 加了 `#[ignore = "todo fix: HashBuilder failed"]` 属性（gravity 在该测试中存在 HashBuilder 已知问题）。其它部分 baseline 与 v1.8.3 形态一致。
**影响范围：** `test_fee_history` 在 gravity 中失败 → 必须保留 ignore；上游 `setup_engine` 元组形状变化必须吃。
**解决方案建议：** mechanical-merge — 整体采纳上游内容；**保留** `#[ignore = "todo fix: HashBuilder failed"]` 在 `test_fee_history` 上方；采纳上游 `setup_engine` `(mut nodes, wallet)` 元组。
**推理：** Gravity baseline `9974ad0618` `#[ignore]` 是 gravity 已知失败的显式标记，不能掩盖；其余采纳上游。

---

### `crates/ethereum/reth/Cargo.toml`
**模块：** ethereum reth 的伞 crate（下游 import 入口）
**冲突类型：** AA
**上游变更：** `reth-db` 加 `features = ["mdbx"]`；新增多个 feature：`keccak-cache-global`（传播到 `reth-node-ethereum?`、`reth-node-core?`）、`jemalloc` / `jemalloc-prof` / `jemalloc-symbols`（传播到 `reth-cli-util?`、`reth-ethereum-cli?`、`reth-node-core?`、`reth-provider?`）、`js-tracer` 加 `reth-node-builder?/js-tracer` + `reth-node-ethereum?/js-tracer` + `reth-rpc-eth-types?/js-tracer` 传播、`otlp`（`reth-ethereum-cli?/otlp` + `reth-node-core?/otlp`）、`portable`（`reth-revm?/portable`）、`std` 加 `reth-codecs?/std` 传播、`test-utils` 加 `reth-tasks?/test-utils`。上游 commits `677d07041`、`f61098ec0`、`6f0ef914b`、`29438631b`、`ab2ef9945`、`2e5ac1ce1`、`24fa984da`。
**Gravity 侧变更：** baseline commit `a1d7365bd6` (#212) 把 `reth-db` 上的 `mdbx` feature 拿掉（多后端选择）；baseline 其余部分仍是 v1.8.3 时期的精简 feature 列表（v1.8.3 时上游还没引入 `keccak-cache-global`、`jemalloc-symbols`、`js-tracer`、`otlp`、`portable`、`reth-tasks?/test-utils`，全部由 v1.8.3 → v2.3.0 周期累计加入）。
**影响范围：** 下游 feature 门控 + 传播。跨组依赖：每个新 feature 都需要其下游目标 crate（`reth-node-ethereum`、`reth-cli-util`、`reth-rpc-eth-types`、`reth-revm`、`reth-codecs`、`reth-tasks`）也定义同名 feature；这些 crate 在其它冲突组。
**解决方案建议：** mechanical-merge：
  - `reth-db` 行不带 `mdbx`（gravity 多后端，对应 baseline `a1d7365bd6`）
  - 加上上游全部新 feature：`keccak-cache-global`、`jemalloc`、`jemalloc-prof`、`jemalloc-symbols`、`js-tracer`、`otlp`、`portable`、`reth-codecs?/std` 传播、`reth-tasks?/test-utils`
  - 每个传播 feature 在加入前必须确认下游 crate 同名 feature 存在；缺失时在 playbook 中标记，等其他组落地后再启用
**推理：** Gravity baseline `a1d7365bd6` 保留 db 后端选择；伞 crate 用途是暴露下游可能需要的所有 feature，应采纳上游 v2.3.0 后扩展的 feature 图。

---

## 组级解决 playbook

### 阶段 1 — 无条件采纳上游的琐碎项

```bash
# AU 文件 — 上游 rename 胜出，HEAD-side 路径丢弃。
git rm docs/vocs/docs/pages/run/faq/pruning.mdx
git rm docs/vocs/docs/public/remote_exex.png

# rename-detection 幻影 — 两侧路径都不存在，删除消解 AU。
git rm examples/bsc-p2p/tests/it/priority.rs

# 仅上游 API 升级，无 gravity 意图。
git checkout --theirs examples/custom-beacon-withdrawals/src/main.rs
git checkout --theirs examples/db-access/src/main.rs
git checkout --theirs crates/e2e-test-utils/src/transaction.rs
git checkout --theirs crates/e2e-test-utils/src/testsuite/actions/produce_blocks.rs
git checkout --theirs crates/e2e-test-utils/tests/e2e-testsuite/main.rs
git checkout --theirs crates/ethereum/node/tests/e2e/dev.rs
git checkout --theirs crates/ethereum/node/tests/e2e/p2p.rs
git add examples/custom-beacon-withdrawals/src/main.rs examples/db-access/src/main.rs \
        crates/e2e-test-utils/src/transaction.rs \
        crates/e2e-test-utils/src/testsuite/actions/produce_blocks.rs \
        crates/e2e-test-utils/tests/e2e-testsuite/main.rs \
        crates/ethereum/node/tests/e2e/dev.rs \
        crates/ethereum/node/tests/e2e/p2p.rs

# .gitignore — 加 `docs/vocs/docs/pages.gen.ts` 行后清掉冲突标记。
```

### 阶段 2 — CLAUDE.md 修复

```bash
# 接受上游 symlink 布局。
git rm CLAUDE.md~HEAD
# index 中 CLAUDE.md 已经是 symlink → AGENTS.md（stage 0），不动。
# 修复损坏的 AGENTS.md 工作区副本（当前只有一行字面量 "AGENTS.md"）：
git show v2.3.0:AGENTS.md > AGENTS.md
git add AGENTS.md
```

### 阶段 3 — Workflow CI 文件

针对 `.github/workflows/bench.yml`、`book.yml`、`compact.yml`、`e2e.yml`、`hive.yml`、`integration.yml`、`lint-actions.yml`、`lint.yml`、`unit.yml`：

1. 保留 gravity 触发器门控（`workflow_dispatch:` 唯一触发器，由 baseline `99712f2834` 主导；hive.yml 例外 — 由 baseline `30e93567d7` 保留 PR/push）。
2. 保留 gravity 步骤：`Free Disk Space` (jlumbroso)、`CARGO_INCREMENTAL: 0`、`GRETH_DISABLE_PIPE_EXECUTION: 1`、partition 分片（unit.yml）、`nightly-2026-02-01` toolchain pin（lint.yml）、所有 `if: false # FIXME` 屏蔽行（lint.yml 的 clippy-binaries/wasm/crates-io-check/docs/udeps、unit.yml 的 state）、`--exclude` op-reth/reth-op/reth-optimism-* 项。
3. integration.yml nextest binary 列表保留 `gravity_eip7702_test`（baseline `46c91f90fe`）。
4. compact.yml matrix 行**删除** `op-reth` 条目（no-OP 全局约束）。
5. 机械应用上游 `permissions: {}` 块与 `actions/checkout@<sha> # v6.0.2` 等 SHA pin（上游 `b89288582` 加固，对任何项目都该采纳）。
6. 机械应用 `.github/assets/` → `.github/scripts/` 脚本路径 rename（如 `install_geth.sh`、`load_images.sh`）。
7. hive.yml 完全 keep-gravity（精简 `test-rpc-compat` job），不移植上游 amsterdam/osaka 多变体 matrix。

### 阶段 4 — 配置文件（机械并集）

- `.config/nextest.toml`：保留 gravity `binary(gravity_pipe_test) + binary(gravity_eip2935_test) + binary(gravity_eip7702_test)` override；叠加上游 `package(reth-era)` 与 `package(reth-node-ethereum)` override。
- `.config/zepter.yaml`：features 列表取并集（gravity 的 `--ignore-missing-propagate=reth-evm-ethereum/test-utils:grevm/test-utils` + 上游扩展后的 feature 列表 `tracy/min-*-logs/otlp/otlp-logs/js-tracer/portable/keccak-cache-global/trie-debug/secp256k1`）。
- `deny.toml`：ignore、`allow-git`、license exceptions 取并集；保留 `allow-org = { github = ["Galxe"] }`；MPL-2.0 exceptions 保留 gravity `[option-ext, webpki-root-certs]` 形态。
- `typos.toml`：词表取并集（`consts`/`Consts` 两侧重叠去重；加入上游 `BA`、`writeable`、`arena.txt` 排除）。

### 阶段 5 — 测试 action 与重导出

- `crates/e2e-test-utils/src/testsuite/actions/mod.rs`：保留 `ValidateSafeAndFinalizedBlocks` 重导出（baseline `fd250d53d8`）。
- `crates/e2e-test-utils/src/testsuite/actions/node_ops.rs`：所有 `EthApiClient::<…>` 调用点（含 gravity 文件末尾 `ValidateSafeAndFinalizedBlocks` impl 内部的 `block_by_hash` 调用）加上游 `TransactionSigned` 泛型；保留 gravity 文件末尾约 130 行的结构体 + impl。
- `crates/ethereum/node/tests/e2e/rpc.rs`：整体采纳上游；**保留** `#[ignore = "todo fix: HashBuilder failed"]` 在 `test_fee_history` 上。

### 阶段 6 — `setup_import.rs`

采纳上游 `Runtime::test()` 接线与 `PayloadAttributes` 闭包类型。**临时保留** `ChainImportResult.task_manager` 字段以兼容 gravity 下游调用点（在跨组修复完成前的过渡），或者把字段去掉同时把下游引用 `task_manager` 字段的代码点全部跟修（依赖跨组）。

### 阶段 7 — Cargo 清单

- `testing/ef-tests/Cargo.toml`：`reth-db` 不带 `mdbx`（gravity 多后端，baseline `a1d7365bd6`）；保留 `reth-revm` 的 `["std", "witness"]`；`reth-stateless` 是否保留取决于上游 `c915841a4` 在 gravity 是否已落实（跨组确认）；采纳上游 `revm` 的 `memory_limit` + `p256-aws-lc-rs`。
- `crates/ethereum/reth/Cargo.toml`：`reth-db` 不带 `mdbx`；采纳上游全部新 feature（`keccak-cache-global`、`jemalloc`/`jemalloc-prof`/`jemalloc-symbols`、`js-tracer` 传播、`otlp`、`portable`、`reth-codecs?/std`、`reth-tasks?/test-utils`）。**跨组：** 每个传播 feature 在加入前必须确认下游 crate 同名 feature 存在。

### 阶段 8 — README.md

mechanical-merge：
- gravity 开场（intro、`assets/erc20-transfer-test.png`、Grevm/Merklization/Cache/Pipeline 要点、docs.gravity.xyz 链接、`# Reth Original README` 分隔符）保留来自 gravity baseline。
- 分隔符之下采用上游 v2.3.0 改写后的 bullet 化 Goals、`assets/reth-2.png`、`reth.rs` 安装 URL、"Reth 2.0 released in April 2026" 段、Storage V2 段。

### 阶段 9 — Cargo.lock

在本组与其他组的所有解决方案落地后：

```bash
git checkout --theirs Cargo.lock     # 或 --ours，无所谓 — 接下来要重新生成
cargo update --workspace
git add Cargo.lock
```

（如果 `cargo update` 太激进，手动清完冲突标记后用 `cargo metadata --locked`，只 commit 真正变更的 pin 行。）

---

## 开放问题

1. **AGENTS.md 工作树损坏** — `AGENTS.md` 工作树副本目前只有一行字面量 `"AGENTS.md"`（9 字节），index 也是这个 blob (`e4aa901d6f`)。这不是任何合并冲突解决的产物，必须用阶段 2 的 `git show v2.3.0:AGENTS.md > AGENTS.md` 修复后再 `git add`。Gravity baseline `CLAUDE.md` 内容是 v1.8.3 catch-up 时期的内容，没有 gravity 独立编辑过；直接取 v2.3.0 `AGENTS.md` 正本是合理结论。

2. **`crates/ethereum/reth/Cargo.toml` 跨组 feature 依赖** — 加 `keccak-cache-global`、`js-tracer`、`otlp`、`portable`、`jemalloc-symbols` 需要 `reth-node-ethereum`、`reth-cli-util`、`reth-rpc-eth-types`、`reth-revm`、`reth-node-core`、`reth-ethereum-cli` 上存在同名 feature。这些 crate 在其它冲突组。建议：等其他组的 Cargo.toml 解决后再把这些 feature 引入；若某个传播目标缺失，暂时从伞 crate 中去掉该 feature。

3. **`setup_import.rs` 中 `task_manager` 字段过渡** — `ChainImportResult { task_manager, … }` 解构调用点散落在 gravity 下游测试，移除字段后需要全部跟修。下游测试文件在其它冲突组（很可能是 `node/builder`、ethereum/node、e2e-test-utils 的下游使用方），标为跨组顺序依赖。可选过渡：把字段类型改成 `Option<TaskManager>` 并默认 `None`，让旧解构编译通过。

4. **`test_fee_history` 的 `#[ignore = "todo fix: HashBuilder failed"]`** — gravity 已知失败，理想是建一个跟踪 issue（baseline commit `9974ad0618` 没有附带 issue 链接），完成 HashBuilder 修复后能去掉。

5. **`examples/bsc-p2p/tests/it/priority.rs` AU** — 两侧路径都不存在该文件，为 git rename-detection 幻影。`rg -F 'bsc-p2p/tests/it/priority' crates/ tests/` 应无结果 → 直接 `git rm`。

6. **`docs/vocs/docs/public/remote_exex.png` AU** — 两侧内容 hash 一致（`8606616e81…`）；丢弃 HEAD-side 路径副本，接受上游的 `docs/vocs/public/remote_exex.png`。用 `rg -F 'docs/public/remote_exex.png' docs/` 确认没有 MDX 页面引用该路径。
