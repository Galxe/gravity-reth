# net-prune-misc-crates

> **Baseline anchor**：`0cb1687c1c`（gravity `main`，2026-06-09，已合入 reth v1.8.3 via `d620fd0eeb feat: merge reth-v1.8.3 (#205)`，2025-11-10）。
> **Upstream target**：reth `v2.3.0`。
> **分支**: `merge-v2.3.0`。HEAD: `e6b7e5ba32`。

## ⟲ 2026-07-05 现状核实(HEAD `a5e0201bd3`,f89d9d4e23 之后)

> 本文写于 f89d9d4e23(storage/trie/prune 整体还原 baseline)之前。按 2026-07-05
> 决策总原则(①storage 决策最高;②冲突迎合 storage;③不冲突且不破坏 gravity
> 功能则保留 v2.3.0 设计)对 14 个文件逐一实测复核,结论如下表;逐文件建议的
> 修订以「⟲」标注在对应章节,开放问题裁决见文末。cargo workspace 仍不可解析
> (缺根 deps,cargo 组),所有验证为冲突标记/diff/符号扫描级,无编译证据。

| 文件 | 冲突(实测) | 状态与建议有效性 |
|---|---|---|
| `prune/segments/mod.rs` | **0**(diff-vs-baseline = 0) | ✅ 已由 f89d9d4e23 整体还原落地,本文建议被"纯 baseline"取代(§见下) |
| `prune/segments/user/history.rs` | **0**(diff = 0) | ✅ 同上,`delete_by_key`+`seek` 保留即成事实 |
| `exex/backfill/test_utils.rs` | **0**(diff 96 行) | ✅ 零冲突侧翻至上游形态 = take-upstream 建议**已自动落地**;依赖符号全存活(实测:`LatestStateProvider::new(db)` owned 构造器 latest.rs:185、`KeccakKeyHasher` trie/common/key.rs:11、`append_blocks_with_state` 第 3 参 `HashedPostStateSorted` block_writer.rs:138-143) |
| `stages/api/pipeline/mod.rs` | 10 | 建议**被加固**:`UnifiedStorageWriter::commit/commit_unwind` 已随还原复活(writer/mod.rs:92/:110 实测);healing 建议修订(见 OQ2 裁决) |
| `stages/api/pipeline/builder.rs` | 1 | 建议维持有效 |
| `stages/api/stage.rs` | 1 | 建议维持,但理由升级:上游 test mod 绑定的 `RocksDBProvider` 全仓零定义(f89d9d4e23 删除)——port **不可行**而非"延后"(见 OQ5 裁决) |
| `net/discv4/src/lib.rs` | 10 | take-upstream 维持有效(自包含,无死符号依赖) |
| `net/nat/src/lib.rs` | 5 | take-upstream 维持有效 |
| `net/network/transactions/fetcher.rs` | 10 | take-upstream 维持有效(`FbBuildHasher` 由 workspace alloy 2.x 供给,根 Cargo.toml 已 v2.3.0 基线) |
| `net/network/tests/it/txgossip.rs` | 5 | **建议部分失效**:`MockEthProvider::with_genesis_block` 全仓零定义(storage 还原反向失效,与 transaction-pool.md 核实发现同源),v2.3.0 侧 5 处调用不可采纳,解块时剥离(⟲ 见该节) |
| `era-downloader/src/fs.rs` | 3 | take-upstream 维持有效 |
| `era-utils/tests/it/history.rs` | 3 | take-upstream 由"建议"升级为"**必须**":HEAD 侧旧路径 `reth_era::execution_types` 已是磁盘孤儿(execution_types.rs 在盘但 lib.rs 无挂载,实测;新路径 `era1/types/execution.rs:103` 在挂载树内) |
| `pipe-exec-layer-ext-v2/event-bus/Cargo.toml` | 3 | keep-gravity 维持有效;原 AU 幻象已物化为 3 个冲突块(HEAD=event-bus vs v2.3.0=payload-util rename 串扰),解块=整块取 HEAD |
| `bin/reth/Cargo.toml` | 7 | mechanical-merge 主体维持,但**新增根依赖缺口**:根 Cargo.toml 无 `reth-primitives` workspace dep(实测 0 定义,即 cargo 报错点)、无 `reth-ress-*` deps/members——而 baseline 本文件 :19/:48-49 需要它们且 `bin/reth/src/ress.rs`(零冲突,gravity 侧)是活接线 → 新增开放问题 6 |

## 分组概要

- **文件数：** 14（11 UU，2 AA，1 AU）
- **冲突分布：** 2 个 Cargo manifest（1 UU + 1 AU "rename 幻影"）、5 个 net crates（discv4、nat、tx fetcher、tx gossip 测试）、2 个 prune segments、1 个 exex 测试、2 个 era 文件、3 个 stages/api 核心文件
- **关键 gravity 保留点（baseline 内已验证）：**
  - `a1d7365bd6` `feat(rocksdb): Integrating RocksDB into Reth (#212)` — gravity-storage v1 RocksDB 接入；带来 `UnifiedStorageWriter::commit*`、`insert_historical_block` + `commit_view` 测试形态、prune cursor 绕过的雏形。
  - `3ee6ac039e` `fix(trie): fix merkle stage of history sync (#246)` — 在 `prune/segments/user/history.rs` 中把 `cursor.delete_current()` 替换为 `cursor.delete_by_key(RawKey::new(prev_key))` + `cursor.seek(...)` 恢复，是 RocksDB WriteBatch cursor 语义的关键绕过。
  - `1224ae1846` `refactor(parallel): remove the read provider factory that supports parallel reading (#213)` — 把 `PipelineBuilder<ProviderRW, ProviderRO>` / `Stage<Provider>` 系列泛型收敛为单泛型 `ProviderRW`（与上游的 `Provider` 命名分歧）。
  - `9acbf22633` `fix(merkle): Update merkle in trunk when history sync (#178)` + `3cd18422c9` `use nested state root in history sync (#134)` + `3ee6ac039e` — 共同构成 `pipeline/mod.rs` 中 stage 出错时无条件重置 `MerkleExecute` 的 gravity 专属错误恢复。
  - `7d0483e565` `feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)` — 把 `txgossip.rs` 中两个旧测试的发送方初始余额从 `100_000_000` 提到 `10u128.pow(18)` 以覆盖 1.5e16 wei 的 upfront cost。
  - `d927b5d96d` `fix: audit round 3 (#284)` + `7ea62ca632` `fix(pipe): wait PipeExecLayerEventBus ready in a loop (#228)` + `f6d831dbd2` `feat: implement onchain config (#146)` — `crates/pipe-exec-layer-ext-v2/event-bus/Cargo.toml` 的 gravity-only 演进历史。
  - `9974ad0618` `fix(test): fix CI test of unit.yml (#241)` — clippy 风格清理（`sort_by` → `sort_by_key`），baseline 内已存在（已用 `git merge-base --is-ancestor` 验证）。
- **解决顺序依赖：**
  1. 根 `Cargo.toml` workspace + `Cargo.lock`（不在本组）—— 决定 `reth-payload-util` 与 `reth-pipe-exec-layer-event-bus` 的 workspace 归属，并决定 `bin/reth` 中保留 `reth-primitives` 还是切到 `reth-primitives-traits` + `reth-ethereum-primitives`。
  2. `crates/stages/api/src/stage.rs`（test mod 决策）→ `crates/stages/api/src/pipeline/builder.rs`（泛型名）→ `crates/stages/api/src/pipeline/mod.rs`（commit 路径）。三者必须共用同一个 `ProviderRW` 泛型名与 `UnifiedStorageWriter` 抽象。
  3. `crates/prune/prune/src/segments/mod.rs` 顶层 `pub use` / import 集合 → `crates/prune/prune/src/segments/user/history.rs`（仅模块内）。
  4. `crates/net/discv4/src/lib.rs` 与 `crates/net/nat/src/lib.rs` 经由 `as_external_ip(port)` 签名耦合 —— 若采纳上游的 `NatResolver::ExternalAddr`，`Discv4Service::resolve_external_ip` 必须传入 `self.local_node_record.udp_port`。

## 逐文件分析

### `bin/reth/Cargo.toml`
**模块：** `reth` 顶层二进制 manifest。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** 多个 PR 复合 ——
- `062dd71226` chore: bump alloy to 2.0.5 (#24289)
- `fa2279ff4c` chore: default to min-trace-logs (#23851)
- `f61098ec00` fix(provider): gate rocksdb jemalloc behind feature flag (#23061)
- `b9969c5b1c` chore: remove rocksdb and edge feature gates, default to storage v2 (#22954) —— 给 `reth-db` 加上 `features = ["mdbx"]`
- `45b961c7b6` chore: deprecate reth-primitives crate (#22450) —— 删除 `reth-primitives.workspace = true`，改用 `reth-primitives-traits` + `reth-ethereum-primitives`
- `8db352dfdd` feat(trie): add `trie-debug` feature for recording sparse trie mutations (#22234)
- `1ecbb0b9d1` chore: move jemalloc, asm-keccak, min-debug-logs to default features (#22034)
- `1fbd5a95f6` feat: Support for sending logs through OTLP (#21039)
- `ab2ef99458` chore: add keccak-global (#20418) —— 把 `keccak-cache-global` 提到默认 features
- 新增 dev-deps：`alloy-node-bindings`、`alloy-provider`、`serde_json`、`tar`、`toml`、`zstd`
- 新增 `[package.metadata.deb]` 段（debian 打包元数据）
- 默认 features 扩为 `["jemalloc", "otlp", "otlp-logs", "reth-revm/portable", "js-tracer", "keccak-cache-global", "asm-keccak", "min-trace-logs"]`

**Gravity 侧变更（baseline 上）：** 自 `d620fd0eeb` (#205, v1.8.3 catch-up) 以来 `git log 0cb1687c1c -- bin/reth/Cargo.toml` 显示 **零 gravity-only commit**。Catch-up 前的 `c64bd613e4` (#225) + `a1d7365bd6` (#212) 已被 #205 的 v1.8.3 状态吸收并稳定到 baseline；baseline 上的内容是：
- 保留 `reth-primitives.workspace = true`（gravity-storage RocksDB provider 仍然 import `reth_primitives::Account` / `Receipt` 的 re-export）。
- 保留 `reth-evm.workspace = true`（gravity-ethereum-cli 在 executor builder 层接 grevm 需要）。
- 保留 `reth-tokio-util` / `reth-ress-protocol` / `reth-ress-provider`（gravity binary 使用）。
- 保留 `tokio` 的 `["sync", "macros", "time", "rt-multi-thread"]` features 与 `eyre` 顶层依赖。
- 默认 features 仍为 `["jemalloc", "reth-revm/portable"]`。

**影响范围：** 顶层 binary manifest —— `reth-primitives` 是承重的（gravity-storage rocksdb provider 直接 import），`reth-evm` 直接依赖是承重的（grevm 接线）。开启上游的 `otlp` / `js-tracer` / `keccak-cache-global` 默认 features 会向 `reth-node-core/*` 传递 feature 要求；这些在 gravity 侧的 `crates/node/core/Cargo.toml` 是否声明取决于该文件的解决方案（另一个 worker）。
**解决方案建议：** mechanical-merge。
- 保留 gravity dep 列表：`reth-primitives`、`reth-evm`、`reth-tokio-util`、`reth-ress-protocol`、`reth-ress-provider`、`eyre`。
- 采纳上游的 `reth-db = { workspace = true, features = ["mdbx"] }`（gravity 也开 MDBX，无害）。
- 默认 features 保留 gravity 的 `["jemalloc", "reth-revm/portable"]`；推迟引入 `otlp` / `js-tracer` / `keccak-cache-global` / `min-trace-logs` —— 这些要求 `crates/node/core/Cargo.toml` 配套接线，跨 worker。
- 跳过 `[package.metadata.deb]` 段（gravity 没有 .deb 发布渠道）。
- 跳过 `tar` / `zstd` / `alloy-node-bindings` 新增 dev-deps（仅供 CLI 集成测试，gravity 没有对应测试场景）。
**理由：** baseline 在此文件上完全是 v1.8.3 + gravity 接线的稳定快照（零 gravity-only commit），dep 列表分歧（保留 `reth-primitives` / 引入 `reth-evm`、`reth-ress-*`）来自更深层 gravity 模块对它们的依赖。开启上游新默认 features 需要跨文件配套，应当延后到单独的 follow-up PR。

> **⟲ 2026-07-05 补充**:本建议新增**根依赖前置**——根 Cargo.toml 当前无
> `reth-primitives` workspace dep(实测零定义,正是 cargo metadata 报错点)、
> 无 `reth-ress-{protocol,provider}` deps/members(`7490ae4ca6` 对齐时删除,
> 当时未查 bin/reth 引用),而 `bin/reth/src/ress.rs`(零冲突)是活接线。
> 裁决与回滚方案见开放问题 6;默认 features 裁决见开放问题 4。

### `crates/pipe-exec-layer-ext-v2/event-bus/Cargo.toml`
**模块：** Gravity 专属 crate `reth-pipe-exec-layer-event-bus`（在 PipeExecLayer extension 与 consensus 之间广播 `BlockProduced` / `BlockProductionRequested` 事件）。
**冲突类型：** AU —— `git ls-files -u` 只显示 stage 2（v2.3.0），无 stage 1（base）、无 stage 3（HEAD）。这是 git 的 rename detection 把上游 `crates/payload/util/Cargo.toml` 误判为 gravity event-bus 的 rename target 产生的幻象冲突（`payload/util/Cargo.toml` 本身已在 stage 0 干净落地）。
**上游变更（v1.8.3 → v2.3.0）：** 不适用 —— `crates/payload/util/Cargo.toml` 在自己路径上无冲突，事件只是 rename detection 串扰。
**Gravity 侧变更（baseline 上）：** `f6d831dbd2` (#146) 接入 onchain config → `7ea62ca632` (#228) event-bus ready 的 wait-loop → `d927b5d96d` (#284) audit round 3 dep 裁剪。Baseline 内容为 `reth-pipe-exec-layer-event-bus` package，依赖 `reth-primitives`、`reth-ethereum-primitives`、`reth-chain-state`、`alloy-primitives`、`tokio`、`tracing`。
**影响范围：** crate 必须继续以 `reth-pipe-exec-layer-event-bus` 名存在；若被误改名为 `reth-payload-util`，`gravity-reth` workspace + `gravity-cl` 会编不过（pipe-exec consensus 接线断）。
**解决方案建议：** keep-gravity —— 把 baseline 内容（即 HEAD 内容）原封写回 `crates/pipe-exec-layer-ext-v2/event-bus/Cargo.toml`，舍弃 stage-2 的上游 `reth-payload-util` 内容。`crates/payload/util/Cargo.toml` 本身已在 stage 0 干净落地，两个 crate 在各自路径上以不同名共存。
**理由：** baseline 验证（`d927b5d96d` / `7ea62ca632` / `f6d831dbd2` 全部 `git merge-base --is-ancestor` 通过）证实此 crate 是 gravity-only 路径。rename 检测是合并工具的副作用，按 keep-gravity 解决即可消解幻影 AU。

### `crates/net/discv4/src/lib.rs`
**模块：** Discv4 服务（Kademlia peer table、ping/pong、find-node、bootstrap reset）。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `e7da50a502` perf(discv4): trigger immediate lookup on first bootnode pong (#22551) —— 新增 `pending_lookup_reset` 字段 + 首个 bootnode pong 时重置 lookup interval
- `87d878a979` feat: support binding discv5 and discv4 to the same port (#23613) —— 给 `Discv4Service::new` 增加外部 `ingress_tx` / `ingress_rx` channel 参数（构造器签名扩 2 个参数），`Discv4::bind` 改为内部 `mpsc::channel(...)` 后透传
- `49fe11041a` feat(discv4): add enforce_eip868_neighbours config setting (#23503)
- `13217d551c` feat(discv4): add AddBootNode command (#23515)
- `390def905d` perf: use sort_unstable in CLI, networking, and RPC hot paths (#23364) —— `sort_by_key` → `sort_unstable_by_key`
- `c2b0f2d1e2` docs(discv4): fix misleading bootstrap doc comment (#22729)
- `c2e846093e` fix(net): use continue instead of return in buffer_hashes loop (#22337) —— 在此处带来 kbucket `entry.value()` → `entry.value_mut()` 的可变访问切换
- `c51da593d6` feat(net/p2p): support fixed external addresses with DNS resolution (#20411) —— 把 `as_external_ip()` 改为 `as_external_ip(self, port: u16)`
- 末尾增加约 100 行 `#[cfg(test)] mod tests { ... }` 块

**Gravity 侧变更（baseline 上）：** `git log 0cb1687c1c -- crates/net/discv4/src/lib.rs` 显示自 #205 catch-up 以来仅 `9974ad0618` (#241) clippy 清理（`sort_by` → `sort_by_key`，行为等价，且与上游 `sort_unstable_by_key` 在语义上一致）。文件主体保持 v1.8.3 baseline 形态：单参 `Discv4Service::new`、`as_external_ip()` 无参版、`entry.value()` 不可变访问、`if let Some(r) = ... && let Some(external_ip) = r.resolver().as_external_ip()` let-chain。
**影响范围：** discv4 可被 `reth-network` 触达。gravity 不用 P2P block sync（pipe-exec 接管），但 discv4 仍编入二进制并在 `reth-network` 测试中被运行到。`Discv4Service::new` 签名变化是内部的，唯一调用点是 `Discv4::bind`（在同一 PR 内更新）。
**解决方案建议：** take-upstream —— 全盘 v2.3.0：新的 `pending_lookup_reset` 字段、`Discv4Service::new` 的 `ingress_tx` / `ingress_rx` 参数、`as_external_ip(self.local_node_record.udp_port)` 调用点、`value_mut()` 切换、`sort_unstable_by_key`，以及末尾新增的测试块。
**理由：** baseline 在此文件上的 gravity-only 变更只有一处 clippy 风格清理（`9974ad0618`），与上游同方向；冲突全部源于上游主动重构。无承重 gravity 逻辑。

### `crates/net/nat/src/lib.rs`
**模块：** NAT resolver 枚举（`NatResolver`）与 `external_addr_with` 辅助。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `c51da593d6` feat(net/p2p): support fixed external addresses with DNS resolution (#20411) —— 新增 `NatResolver::ExternalAddr(String)` 变体；扩展 `Display` / `FromStr`（`extaddr:` 前缀）；把 `as_external_ip(self)` 改为 `as_external_ip(self, port: u16)`，对域名走 `to_socket_addrs`。
- `fc6462b5ba` fix(nat): resolve DNS for ExternalAddr in external_addr_with (#23269) —— 在 `external_addr_with` 中通过 `tokio::net::lookup_host` 增加域名解析分支。

**Gravity 侧变更（baseline 上）：** `git log 0cb1687c1c -- crates/net/nat/src/lib.rs` 显示自 #205 catch-up 以来仅 `9974ad0618` (#241) 清理（与 fs.rs 同批），文件主体与 v1.8.3 完全一致 —— 仅 `ExternalIp(IpAddr)` 变体、`as_external_ip(self) -> Option<IpAddr>` 无 port 参数。
**影响范围：** 公共 API 面 —— `NatResolver` 是 `--nat` CLI 标志接线的一部分。gravity 从 `reth-node-core` 继承 CLI；不接上游会让 gravity 无法使用 `--nat extaddr:<domain>`，且让 `crates/net/discv4/src/lib.rs` 的 `as_external_ip(port)` 调用失配。
**解决方案建议：** take-upstream。
**理由：** 与 discv4 文件解决方案耦合（`as_external_ip(port)` 必须一致）；baseline 在此处无承重 gravity 改动。

### `crates/net/network/src/transactions/fetcher.rs`
**模块：** 交易哈希拉取器（inflight/pending 哈希的 LRU 缓存 + fallback-peer 簿）。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `0363b5cce6` perf(network): use FbBuildHasher for transaction manager maps (#24497) —— 所有 `LruCache<TxHash>` / `LruMap<TxHash, ...>` / `LruCache<PeerId>` 切到 `FbBuildHasher<32>` / `FbBuildHasher<64>`，`LruMap::new(...)` → `LruMap::with_hasher(..., Default::default())`
- `283cc32396` refactor(net): use B256 collections for tx hashes (#24565) —— 引入 `alloy_primitives::map::{B256Map, B256Set, HashMap}`
- `0eeb8c5ef7` fix(net): prevent eth/68 tx request packing overflow (#23848) —— 给 `fill_request_from_hashes_pending_fetch` 增加 `bool` 返回值
- `d3846d98a8` refactor: refactor get_idle_peer_for to use Iterator::find (#21321) —— `for + if + return` 折成 `iter().find()`

**Gravity 侧变更（baseline 上）：** `git log 0cb1687c1c -- crates/net/network/src/transactions/fetcher.rs` 显示自 #205 catch-up 以来仅 `9974ad0618` (#241)；任务简报中提到的 sharded-mempool announcement filter 位于兄弟文件 `crates/net/network/src/transactions/policy.rs`（`ShardedMempoolAnnouncementFilter`），不在此文件中。
**影响范围：** 与 `transactions/mod.rs`（`TransactionsManager`）的解决方案绑定 —— `with_hasher` 构造器与 `fill_request_from_hashes_pending_fetch` 的 `bool` 返回都被 mod.rs 调用。
**解决方案建议：** take-upstream。
**理由：** baseline 在此文件上无 gravity 逻辑；`FbBuildHasher` 切换是纯性能 swap，gravity 使用的 `LruCache` trait 不变。需交叉核对兄弟文件 `transactions/mod.rs`（其他 worker）—— `with_hasher` 调用面 + 新 `bool` 返回必须一致。

### `crates/net/network/tests/it/txgossip.rs`
**模块：** tx gossip + propagation / ingress 策略的集成测试。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `56ded417e1` feat: limit handling of incoming txs to trusted peers (#19666) —— 新增 `TransactionIngressPolicy` + `TransactionsManagerConfig` import；新增一个完整 `test_tx_ingress_policy_trusted_only` 测试（约 65 行）
- `b87cde547d` feat: configurable EVM execution limits (#21088) —— 把所有 `MockEthProvider::default()` 改为 `MockEthProvider::default().with_genesis_block()`（tx 校验现在需要 chain tip）

**Gravity 侧变更（baseline 上）：** `7d0483e565` `feat(fee): enforce 50 Gwei minimum base fee for Gravity (#335)`（已 `git merge-base --is-ancestor` 验证）把 4 处 sender 余额从 `U256::from(100_000_000)` 提到 `U256::from(10u128.pow(18))`，附注释 `// Balance must cover upfront cost: 50 Gwei (GRAVITY_MIN_BASE_FEE) * 300k gas = 1.5e16 wei.`
**影响范围：** 仅测试，但是 gravity-marker。若回退到 `100_000_000`，gravity chain spec 下 sender 无法覆盖 50 Gwei × 21k gas = 1.05e15 wei 的 upfront cost，测试会失败。
**解决方案建议：** mechanical-merge：
- 采纳上游新增的 `TransactionIngressPolicy` / `TransactionsManagerConfig` import。
- 对 *所有* `MockEthProvider::default()` 加 `.with_genesis_block()`（包括 gravity 现有的 `test_tx_propagation_policy_trusted_only` / `test_tx_propagation_policy_all` / `test_4844_tx_gossip_penalization`）。
- 在 gravity 的 3 个旧测试中 **保留** `U256::from(10u128.pow(18))` 与注释。
- 原样采纳新的 `test_tx_ingress_policy_trusted_only` 测试，包括它内部的 `U256::from(100_000_000)`（该测试不依赖 gravity base fee 流转，只测 ingress accept/reject，余额不需要覆盖 upfront cost）。
**理由：** baseline 验证 `7d0483e565` 在 baseline 内 → gravity-marker 保留；新 ingress 测试与 `.with_genesis_block()` 是干净上游增量。解决后跑 `cargo test -p reth-network --tests txgossip` 验证。

> **⟲ 2026-07-05 修订(storage 还原反向失效)**:`MockEthProvider::with_genesis_block`
> **全仓零定义**(f89d9d4e23 把 provider test_utils 还原 baseline;与
> transaction-pool.md 核实中 maintain.rs 的发现同源)。本文件 v2.3.0 侧 5 处
> `.with_genesis_block()` 调用**不可采纳**——解块时按原则②剥离该调用
> (`MockEthProvider::default()` 原样),其余建议不变(gravity 3 个旧测试保
> `10u128.pow(18)` 余额;新 ingress 测试正常采纳,仅去掉 with_genesis_block)。
> 若剥离后新 ingress 测试因缺 chain tip 失败,测试侧改用 baseline
> MockEthProvider 现有 API 补 tip(落地时实测),不得反向给 provider 加方法。

### `crates/prune/prune/src/segments/mod.rs`
**模块：** Prune segment trait + static_file pruning re-export + 测试脚手架。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `058ffdc21d` feat(storage): write headers and transactions only to static files (#18681) + `3883df3e69` chore: remove db pruning of header/txs segments (#19260) + `5a9c7703d3` chore: rm `StaticFileReceipts` pruner (#19265) + `b9969c5b1c` (#22954) —— 删除 `static_file` 子模块的 `pub use Headers/Receipts/Transactions` re-export
- `9300356067` revert: "refactor(prune): remove receipts log filter segment (#19184)" (#19646) + 后续相关 PR —— 新增 `PruneProgress`、`SegmentOutputCheckpoint` 类型；扩展 import 引入 `StaticFileProviderFactory`、`reth_stages_types::StageId`、`reth_static_file_types::StaticFileSegment`
- `96c77fd8b8` feat(storage): make insert_block() operate with references (#20504) —— 测试侧 `insert_historical_block(block)` 切换为 `insert_block(&recovered)`，并把循环外的 `commit_view()` 改为 `commit()`
- 新增 `BlockWriter` 测试 import

**Gravity 侧变更（baseline 上）：** `git log 0cb1687c1c -- crates/prune/prune/src/segments/mod.rs` 显示仅 `9974ad0618` (#241) clippy 清理。Baseline 内容继承自 `a1d7365bd6` (#212) rocksdb 接入时建立的形态：
- `pub use static_file::{Headers, Receipts, Transactions};` re-export。
- 测试循环体内每个 block 一次 `provider_rw.insert_historical_block(block.try_recover()?)` + `provider_rw.commit_view()?`。

**影响范围：** `insert_historical_block` + `commit_view` 是 gravity-storage v1 RocksDB provider API（gravity rocksdb 在每个 block 边界要 flush WriteBatch 进 RocksDB —— 这是 `a1d7365bd6` (#212) + `acc458846c` (#340 `fix(rocksdb): flush batch data into storage to make sure stage is completed`) 留下的约束）。如果 gravity-storage 的 `BlockchainProvider` 没有提供 `insert_block(&recovered)` 签名，take-upstream 会编不过。
**解决方案建议：** mechanical-merge：
- 采纳上游 import 扩展（`StaticFileProviderFactory`、`StageId`、`StaticFileSegment`、`BlockWriter`、`PruneProgress`、`SegmentOutputCheckpoint`）。
- 采纳上游删除 `pub use static_file::{Headers, Receipts, Transactions}`（v2 把 static_file 段移到独立位置；下游消费者若仍 import 这些 re-export，由其他 worker 在相应文件 fixup）。
- 保留 gravity 测试体：循环内 `insert_historical_block(...) + commit_view()`（4 处重复 fixture 同样处理）。
**理由：** 顶层 import / re-export 是纯上游 surface 变动；测试 API 分歧根在 `a1d7365bd6` (#212) 与后续 `acc458846c` (#340) 共同确立的 RocksDB-per-block flush 约束。除非 gravity-storage 的 `BlockWriter::insert_block(&RecoveredBlock)` 已经接好并且不破坏 per-block flush，否则不要切到上游测试体；这是开放问题 1。解决后跑 `cargo check -p reth-prune --tests` 验证。

**关键决策（人工介入）：`mod static_file;` 悬空**
- 现状：`crates/prune/prune/src/segments/mod.rs` 中仍有 `mod static_file;` 声明，但 v2.3.0 upstream 已经把 `segments/static_file/` 子目录整个删除；gravity 侧仍留有 `segments/static_file/{headers,receipts,transactions,mod}.rs` 四个文件。声明与目标不一致会挂住 rustfmt hook。
- 决策项：
  1. 在 `mod.rs` 里删除 `mod static_file;` 声明（跟随上游）
  2. 决定要不要一并 `git rm` gravity 侧遗留的 `static_file/{headers,receipts,transactions,mod}.rs` 四个文件（推荐删除，若没有其他 crate 直接 `use reth_prune::segments::static_file::*` 的话）
- 与上面 `pub use static_file::{Headers, Receipts, Transactions}` 的删除决策捆绑：如果 mod 声明与 re-export 都删掉，需要 grep 整仓库确认无残余依赖后再落地

> **⟲ f89d9d4e23 后消解(2026-07-05 实测)**:本决策项与上文 mechanical-merge
> 建议整体**已被"纯 baseline 还原"取代**——`segments/mod.rs` 与 baseline 零
> diff、零冲突;`mod static_file;`(:3)、`pub use static_file::{..}`(:11)、
> `segments/static_file/` 四文件三者一致共存,无悬空。上游 import 扩展
> (`PruneProgress` 等)随还原一并放弃,归入 storage-v2 再合并债务
> (v2.4+,见 STORAGE-RESOLUTION-TODO)。无需任何动作。

### `crates/prune/prune/src/segments/user/history.rs`
**模块：** Per-shard 账户 / 存储 history pruning 逻辑。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** `94235d64ad` fix(pruner): prune account and storage changeset static files (#21346) —— 新增 `use itertools::Itertools;`；merge 后用普通的 `cursor.delete_current()?` + `cursor.upsert(...)` 把当前 shard 的值替换为前一个 shard 的值。
**Gravity 侧变更（baseline 上）：** 两次 gravity-only 迭代（baseline 验证均通过）：
- `a1d7365bd6` (#212) rocksdb 接入：因为"RocksDB 在写操作后不移动 cursor 指针"，在 `delete_current()` 后加 `cursor.next()?`。
- `3ee6ac039e` (#246) `fix(trie): fix merkle stage of history sync`：把 `delete_current()` 替换为 `cursor.delete_by_key(RawKey::new(prev_key))?;`（避免 RocksDB WriteBatch 下 cursor 位置不一致）；`prev()` 之后显式 `cursor.seek(RawKey::new(key.clone()))?;` 恢复 cursor。同文件其他 3 处也同步切到 `delete_by_key`。

**影响范围：** **关键** —— RocksDB cursor 在 WriteBatch 下与上游 `DbCursorRW` 契约不一致：上游 MDBX cursor 删除后可直接 `upsert`，gravity RocksDB cursor 需要显式 seek。回退到 `delete_current()` 会重新引入 PR #246 修掉的 merkle stage history-sync bug —— 这是一个 RocksDB-only 的静默数据腐蚀路径。
**解决方案建议：** keep-gravity —— 保留 `cursor.delete_by_key(RawKey::new(prev_key))?;` + 周边注释 + `cursor.seek(...)` 恢复逻辑；丢弃上游的 `use itertools::Itertools;`（gravity 此处用不到，是死重量）。
**理由：** `3ee6ac039e` (#246) 是 gravity-marker（baseline 验证通过），承重的 RocksDB 专属修复。上游 `Itertools` 在 v2.3.0 的实现里没在本文件 import 列表使用（仅 `delete_current()` + `upsert()` 路径），可安全舍弃。

### `crates/exex/exex/src/backfill/test_utils.rs`
**模块：** ExEx backfill 集成测试辅助（执行 block 并 commit 到 `ProviderFactory`）。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `936baf1234` refactor: remove `FullNodePrimitives` (#19176) —— `FullNodePrimitives` → `NodePrimitives`
- `7b2fbdcd57` chore(db): Remove Sync from DbTx (#20516) —— 触发 import 链路调整
- `121160d24f` refactor(db): use hashed state as canonical state representation (#21115) —— `LatestStateProviderRef::new(&provider)` → `LatestStateProvider::new(provider)`（拥有式）；`append_blocks_with_state` 第 3 参数从 `Default::default()` 改为 `execution_outcome.hash_state_slow::<KeccakKeyHasher>().into_sorted()`

**Gravity 侧变更：** `git log 0cb1687c1c -- crates/exex/exex/src/backfill/test_utils.rs` 显示自 #205 catch-up 以来 **零 gravity-only commit**。gravity 侧形态：`use reth_node_api::FullNodePrimitives;` + `LatestStateProviderRef::new(&provider)`（单参，无 cache 槽）+ `append_blocks_with_state(..., Default::default())`。`66ab036739` (#75) 引入的 cache 槽已被 #205 v1.8.3 catch-up 抹平。
**影响范围：** 仅 ExEx 测试 —— `LatestStateProviderRef::new(&provider)` 与 `LatestStateProvider::new(provider)` 在 gravity 代码侧已无生产路径耦合（生产用 cache-aware 入口在别处）。
**解决方案建议：** take-upstream —— 全盘 v2.3.0：`NodePrimitives` 约束、`LatestStateProvider::new(provider)`、`hashed_state` 计算。
**理由：** baseline 在此文件上零 gravity 改动，全部是 v1.8.3 → v2.3.0 上游 surface 漂移。`hashed_state` 也是上游对历史占位 `Default::default()` 的真修。需在 `crates/storage/provider/src/providers/state/latest.rs`（其他 worker）确认 `LatestStateProvider::new(provider)` 拥有式构造器已被采纳。

### `crates/era-downloader/src/fs.rs`
**模块：** 本地文件系统 ERA1 文件迭代器（`read_dir` 辅助）。
**冲突类型：** AA（HEAD 与 v2.3.0 都新增此文件 —— gravity 与上游路径独立演化）
**上游变更（v1.8.3 → v2.3.0）：**
- `8020cf4493` fix(era-downloader): align checksums with file index in fs::read_dir (#19793) —— 一次性计算 `start_index`；checksum 迭代器快进 `start_index` 步；条目 `skip_while(|(n, _)| *n < start_index)`（即便文件稀疏，checksum 位置仍对齐）。
- `ccff9a08f1` chore: fix clippy unnecessary_sort_by lint (#21385) —— `sort_by` → `sort_by_key(|(left, _)| *left)`

**Gravity 侧变更：** `9974ad0618` (#241) `fix(test): fix CI test of unit.yml` 做 clippy 化妆性整理：`entries.sort_by(|(left, _), (right, _)| left.cmp(right))` → `entries.sort_by_key(|a| a.0)`，语义无变化。gravity 侧没有 `skip_while` checksum 对齐逻辑。
**影响范围：** ERA1 历史导入 —— gravity 主网运行不会执行 ERA 导入，但 crate 仍在 workspace 构建中。
**解决方案建议：** take-upstream。
**理由：** baseline 上的 gravity 改动是化妆性的；上游的 `skip_while` + checksum 前进是稀疏 ERA1 目录的真正正确性修复。`sort_by_key(|a| a.0)` 与 `sort_by_key(|(left, _)| *left)` 语义等价 —— 取上游写法保持与上游 codebase 风格一致。

### `crates/era-utils/tests/it/history.rs`
**模块：** ERA1 import/export 集成回环测试。
**冲突类型：** AA
**上游变更（v1.8.3 → v2.3.0）：**
- `2ba17cf10b` refactor(era): move era types and file handling to new module (#19520) —— `MAX_BLOCKS_PER_ERA1` 从 `reth_era::execution_types` 迁到 `reth_era::era1::types::execution`
- `5fab66d57b` refactor(era-utils): generalize `import` over `EraBlockReader` (#24977) —— `import` 改为泛型；调用点变为 `import::<Era1, _, _, _, _, _, _>(stream, &pf, &mut hash_collector)`；新增 `reth_era_utils::Era1` 类型

**Gravity 侧变更（baseline 上）：** `9974ad0618` (#241) 在 `import` 调用上方加注释 `// should turn on proxy to visit \`https://era.ithaca.xyz/era1/checksums.txt\``。HEAD 调用形式仍是非泛型 `import(stream, &pf, &mut hash_collector)`，且 `MAX_BLOCKS_PER_ERA1` 来自旧路径 `reth_era::execution_types`。
**影响范围：** 仅测试。
**解决方案建议：** take-upstream，保留 gravity 的 proxy 注释（贴在新的 `import::<Era1, _, _, _, _, _, _>(...)` 调用上方）。把 `use reth_era::execution_types::MAX_BLOCKS_PER_ERA1;` 改为 `use reth_era::era1::types::execution::MAX_BLOCKS_PER_ERA1;`；把 `Era1` 加入 `reth_era_utils::{export, import, ExportConfig}` import。
**理由：** baseline 上的 gravity 改动仅一行有用注释；签名变化与 import 路径迁移是上游 API 要求。

### `crates/stages/api/src/pipeline/builder.rs`
**模块：** staged sync pipeline 的 `PipelineBuilder<ProviderRW>` fluent builder。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `3c39444591` fix(stages): correct tip_tx field comment in PipelineBuilder (#19655) —— `A receiver` → `A Sender` 文档修正
- `d278b75c33` chore(stages): fix naming and simplify add_stages implementation (#19923) —— `for stage in stages { self.stages.push(stage); }` 折叠成 `self.stages.extend(stages);`；`reserve_exact` → `reserve`；本地变量 `states` 改回 `stages`

**Gravity 侧变更（baseline 上）：** `1224ae1846` `refactor(parallel): remove the read provider factory that supports parallel reading (#213)` 把泛型 `PipelineBuilder<ProviderRW, ProviderRO>` 收敛到单泛型 `PipelineBuilder<ProviderRW>`。注：上游也最终收敛到单泛型，但命名为 `Provider`，gravity 选用了 `ProviderRW`。本地变量名仍是 `states`，`reserve_exact + for + push` 还是老形态。
**影响范围：** 公共类型 —— `PipelineBuilder<ProviderRW>` 被 `crates/node/builder` 与 gravity node-builder 消费方引用。
**解决方案建议：** mechanical-merge：
- 保留 gravity 泛型名 `ProviderRW`（与本组其他 stages/api 文件 + 所有 gravity stage 实现一致）。
- 采纳上游函数体的 `self.stages.extend(stages); self.stages.reserve(stages.len());` 化简（语义等价、惯用法更佳）。
- 采纳上游 doc 修正 `A Sender`。

**理由：** `1224ae1846` (#213) 是 gravity-marker，决定泛型名（baseline 验证通过）。`states → stages` 是上游纯化妆改动，取上游写法没有任何代价 —— 但如果保留 gravity 的 `states` 也可，命名一致性优先级低。

### `crates/stages/api/src/pipeline/mod.rs`
**模块：** Pipeline 驱动 —— execute / unwind 循环、错误处理、checkpoint commit。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `294e215077` fix(provider): heal finalized/safe block numbers ahead of highest header (#22995) —— unwind 时除 `last_saved_finalized_block_number` 之外，新增跟踪并持久化 `last_saved_safe_block_number`
- `4a6f9cd5c9` fix(provider): cap storage_v2 unwind history by MDBX tip (#23335) —— `database_provider_rw()` → `unwind_provider_rw()?.disable_long_read_transaction_safety()`；删除 `UnifiedStorageWriter::commit_unwind(provider_rw)?`，改为 `provider_rw.commit()?`
- `347c1325cc` fix: skip `move_to_static_files` for `storage.v2` (#23814) —— MerkleExecute checkpoint-reset 收窄为 `if stage_id == StageId::MerkleExecute { ... }`
- `12cf3d6855` fix(provider): add CommitOrder for RocksDB/MDBX unwind atomicity (#21311)
- import 列表新增 `DBProvider`、`StorageSettingsCache`；丢掉 `UnifiedStorageWriter` + `writer::` 模块
- `Pipeline::stage` 返回类型变为 `<ProviderFactory<N> as DatabaseProviderFactory>::ProviderRW`
- `block.block.number - 1` → `block.block.number.saturating_sub(1)`（溢出保护）
- 删除 `start = Instant::now()` 计时 + "Stage has executed" info! 日志块

**Gravity 侧变更（baseline 上）：**
- `1224ae1846` (#213) 裁剪 import 列表，泛型名 `ProviderRW`。
- `9acbf22633` (#178) `fix(merkle): Update merkle in trunk when history sync` + `3cd18422c9` (#134) `use nested state root in history sync` + `3ee6ac039e` (#246) 共同塑造当前 commit/unwind 路径。

Baseline 形态：
- `UnifiedStorageWriter::commit(provider_rw)?` 与 `UnifiedStorageWriter::commit_unwind(provider_rw)?`（gravity-storage v1 RocksDB writer 的原子提交 MDBX + static-files + RocksDB 抽象）。
- 任何 stage 出错都重置 MerkleExecute checkpoint（不仅是 MerkleExecute 自己 fail —— 因为 gravity 嵌套 state-root merkle stage `3cd18422c9` 的 checkpoint 格式不同）。
- 打 `"Stage has executed, and reached the target block."` info! 日志，带 `execute_duration_ms`。
- `block.block.number - 1`（无 saturating_sub）。
- `Pipeline::stage(...) -> &mut dyn Stage<DatabaseProviderRW<N>>`。

**影响范围：** **关键** —— staged sync 核心入口。`UnifiedStorageWriter` 是承重的 gravity-storage v1 抽象：替换为 `provider_rw.commit()` 会跳过 gravity 上 RocksDB 写的那一半，导致 chain-halt / 数据不一致。MerkleExecute 在所有错误上重置也是 gravity 嵌套 merkle stage 必需。`last_safe_block_number` healing 依赖 `ChainStateBlockWriter::save_safe_block_number` 接口在 gravity-storage 上的存在性。
**解决方案建议：** 主体 keep-gravity，叠加以下机械合并：
- 保留 `UnifiedStorageWriter::commit(provider_rw)?` 与 `UnifiedStorageWriter::commit_unwind(provider_rw)?`。
- 保留 `self.provider_factory.database_provider_rw()?`（不切到上游的 `unwind_provider_rw()?.disable_long_read_transaction_safety()`）。
- 保留 stage 出错无条件重置 MerkleExecute checkpoint。
- 保留 gravity 的 `execute_duration_ms` info! 日志。
- 保留 `Pipeline::stage(...) -> &mut dyn Stage<DatabaseProviderRW<N>>`。
- 采纳上游 `block.block.number.saturating_sub(1)`（零成本溢出保护）。
- **推迟** `last_saved_safe_block_number` healing —— 必须先核实 `ChainStateBlockWriter::save_safe_block_number` 在 gravity-storage 的 `DatabaseProviderRW` 上是否已实现。gravity 由 pipe-exec consensus 推进 chain tip，是否会推进 safe block 也需要核实。这是开放问题 2，建议作为后续 PR 处理。

**理由：** baseline 验证 `1224ae1846` / `9acbf22633` / `3cd18422c9` / `3ee6ac039e` 全部通过 → pipeline commit 路径上 4 个 gravity-marker。`UnifiedStorageWriter` 是 gravity-storage v1 的 atomic-commit 抽象，丢掉它等于丢掉 RocksDB 写路径。`saturating_sub` 是上游 clean 改动，可吸收。`last_safe_block_number` 需要配套 storage 接口，强行合入有 chain-halt 风险。

> **⟲ 2026-07-05 修订**:两处更新——①keep-gravity 主体**被 f89d9d4e23 加固**:
> `UnifiedStorageWriter::commit/commit_unwind` 已随 writer/mod.rs 整体还原复活
> (writer/mod.rs:92/:110 实测),上游侧 `unwind_provider_rw()` /
> `disable_long_read_transaction_safety` / `StorageSettingsCache` 反成死符号,
> 冲突块凡引用它们的一律按原则②取 HEAD;②`last_saved_safe_block_number`
> healing 的"推迟"建议**撤销**,改为解块时叠加采纳——前提已全部核实成立,
> 见开放问题 2 的裁决。

### `crates/stages/api/src/stage.rs`
**模块：** `Stage` 与 `StageExt` trait + `ExecInput::next_block_range_with_transaction_threshold`。
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：**
- `95b8a85357` feat(stages): get transaction range starting from first available block (#19662) + `ec92a839f0` refactor(stages): use named structs for ExecInput returns (#19689) + `b6e6bd35cd` refactor(stages): empty transactions range (#19753) —— 在文件末尾新增 `#[cfg(test)] mod tests` 块（约 165 行），覆盖 `next_block_range_with_transaction_threshold` 在 `ProviderFactory<MockNodeTypesWithDB>` 上的多个边界场景。
- 上游测试体内 `ProviderFactory::new(create_test_rw_db(), MAINNET.clone(), StaticFileProviderBuilder::read_write(...).build().unwrap(), RocksDBProvider::builder(create_test_rocksdb_dir().0.keep()).build().unwrap(), reth_tasks::Runtime::test())` 这种 5 参数 + RocksDB provider 构造形态本身就引用了 `reth_db::test_utils::create_test_rocksdb_dir` 与 `reth_provider::providers::RocksDBProvider` —— 实际上是 reth-2.x 的 storage-v2 二阶段开发（rocksdb 路径已合入上游 storage-v2）。
- trait 主体本身在本冲突窗口内零变更（上游 PRs 在 trait 之外）。

**Gravity 侧变更（baseline 上）：** `1224ae1846` (#213) 收敛泛型到 `ProviderRW`、`24f03242db` (#220) 一次 nightly fmt、`3cd18422c9` (#134) 嵌套 state-root 引入的 trait 注解调整。**Baseline 上没有该测试模块** —— 它是 v1.8.3 之后才在上游新增的。
**影响范围：** 仅测试。trait surface 零冲突。
**解决方案建议：** keep-gravity（删除上游新增的 test mod），同时记录 follow-up：合并稳定后核实 gravity-storage 的 `ProviderFactory::new` 参数顺序/类型是否与上游 5 参数签名兼容，再 port 测试模块进来。
**理由：** baseline 验证表明此处无 gravity 测试存在，trait 主体冲突也只是上下文行扰动。上游测试依赖 storage-v2 `RocksDBProvider::builder(...)` 形态，gravity-storage v1 RocksDB provider 不一定吻合 —— 与其在合并节点冒着改测试体的风险，不如延后到独立的 port-test PR。需要在 baseline 工作树确认 `crates/storage/provider/src/test_utils.rs` 是否暴露兼容的 `ProviderFactory::new` 入口。

> **⟲ 2026-07-05 修订**:"延后 port"升级为"**port 不可行**"——上游 test mod
> 绑定的 `RocksDBProvider` 已被 f89d9d4e23 连文件删除(全仓零定义,实测),
> 5 参 `ProviderFactory::new` 形态亦不存在。keep-gravity(删上游 test mod)
> 从"稳妥选择"变为唯一可编译选择;follow-up 改为 v2.4+ 随 storage-v2
> 再合并时重估。见开放问题 5 裁决。

## 组级解决 playbook

1. **解决顺序**（与依赖树一致;⟲ 2026-07-05 按现状核实修订）：
   1. `crates/stages/api/src/stage.rs` —— keep-gravity（删上游 test mod;⟲ follow-up 撤销,port 不可行,见 OQ5）。
   2. `crates/stages/api/src/pipeline/builder.rs` —— 保留泛型名 `ProviderRW`，采纳 `extend(stages)` + `reserve` + doc 修正。
   3. `crates/stages/api/src/pipeline/mod.rs` —— 主体 keep-gravity（`UnifiedStorageWriter::commit*` + MerkleExecute 全错误重置 + `execute_duration_ms` 日志），叠加 `saturating_sub(1)`。⟲ **改为叠加** `last_saved_safe_block_number` healing（前提已核实,见 OQ2）;凡引用 `unwind_provider_rw`/`StorageSettingsCache` 死符号的上游侧一律取 HEAD。
   4. ~~`crates/prune/prune/src/segments/mod.rs`~~ —— ⟲ **✅ 已由 f89d9d4e23 落地**(与 baseline 零 diff),无需动作。
   5. ~~`crates/prune/prune/src/segments/user/history.rs`~~ —— ⟲ **✅ 已由 f89d9d4e23 落地**,同上。
   6. `crates/net/discv4/src/lib.rs` —— take-upstream。
   7. `crates/net/nat/src/lib.rs` —— take-upstream。
   8. `crates/net/network/src/transactions/fetcher.rs` —— take-upstream；交叉核对兄弟文件 `transactions/mod.rs`（其他 worker）。
   9. `crates/net/network/tests/it/txgossip.rs` —— gravity 3 个旧测试保留 `U256::from(10u128.pow(18))` + 注释，新 ingress 测试 take-upstream;⟲ **剥离全部 5 处 `.with_genesis_block()`**(方法已死,见该节修订注)。
   10. ~~`crates/exex/exex/src/backfill/test_utils.rs`~~ —— ⟲ **✅ 已零冲突侧翻落地**(= take-upstream 结果,符号全存活),无需动作。
   11. `crates/era-downloader/src/fs.rs` —— take-upstream。
   12. `crates/era-utils/tests/it/history.rs` —— take-upstream，保留 gravity proxy 注释一行;⟲ HEAD 旧 import 路径已死(execution_types 孤儿),必须切新路径。
   13. `crates/pipe-exec-layer-ext-v2/event-bus/Cargo.toml` —— keep-gravity（3 块整块取 HEAD，消解物化的 AU 幻象）。
   14. `bin/reth/Cargo.toml` —— 按本文章节做机械合并;⟲ **前置**:根 Cargo.toml 补 ress deps/members + `reth-primitives` 等缺失 deps(OQ6,cargo 组)。

2. **批量解决后的验证命令（⟲ 当前 cargo workspace 不可解析——缺根 deps,cargo 组修复后方可执行;此前以「冲突标记归零 + rustfmt parse + 死符号扫描」为过渡验收）：**
   ```bash
   cargo check -p reth-discv4 -p reth-net-nat -p reth-network -p reth-prune -p reth-exex
   cargo check -p reth-stages-api -p reth-era-downloader -p reth-era-utils
   cargo check -p reth-pipe-exec-layer-event-bus
   cargo check -p reth --bin reth
   cargo nextest run -p reth-prune --tests
   cargo nextest run -p reth-network --tests txgossip
   ```

3. **合并落地后的 follow-up：**
   - 按 gravity 的 `ProviderFactory::new` 签名 port `crates/stages/api/src/stage.rs` 的测试模块。
   - 在确认 `ChainStateBlockWriter::save_safe_block_number` 已在 gravity-storage 接好后，给 `Pipeline::unwind_to` 加上 `last_safe_block_number` healing。
   - 若需要，把 `NatResolver::ExternalAddr` 域名解析能力写入 gravity 运维文档。
   - 评估 `bin/reth/Cargo.toml` 默认 features `otlp` / `js-tracer` / `keccak-cache-global` 的开启计划（依赖 `crates/node/core/Cargo.toml` 配套）。

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [x] 1. **`BlockWriter::insert_block(&RecoveredBlock)` 在 gravity-storage 上的接口形态** —— gravity rocksdb 的 `BlockchainProvider` 是否实现了 `insert_block(&RecoveredBlock)`，并且每个 block 后会自动 flush RocksDB WriteBatch（参见 `acc458846c` (#340)）？决定 `prune/segments/mod.rs` 测试体是否可以最终切到上游 `insert_block + commit`。
   → **决策(2026-07-05,决策总原则②/问题消解)**:保留 gravity 测试体
   (`insert_historical_block` + `commit_view`)——f89d9d4e23 整体还原已使其成为
   既成事实,"是否切上游"不再是本轮问题;上游测试体归入 storage-v2 再合并
   债务(v2.4+)。
   - [x] 冲突解决:`prune/segments/mod.rs` 冲突 0、与 baseline diff = 0
     (2026-07-05 实测;编译证据待 cargo workspace 修复后回填)。

- [x] 2. **`ChainStateBlockWriter::save_safe_block_number`** —— gravity-storage `DatabaseProviderRW` 是否实现了它？以及 gravity 的 pipe-exec consensus 是否会推进 safe block？决定 `pipeline/mod.rs` 中 `last_safe_block_number` healing 是现在合还是延后。
   → **决策(2026-07-05,决策总原则③)**:两个前提实测均成立——trait 定义
   `storage-api/src/block.rs:393` + 还原后 provider impl
   `database/provider.rs:3215`;gravity 生产路径确实推进 safe block
   (engine `persistence.rs:236` 融合版 on_save_blocks 持久化
   safe/finalized)。→ healing **现在合**:解 `pipeline/mod.rs` 10 块时在
   keep-gravity 主体(UnifiedStorageWriter 路径)上叠加采纳,纯增量、无冲突。
   - [x] 冲突解决:已落地(2026-07-06,由 stages 组随 pipeline/mod.rs 10 块一并解决)——
     safe-block 保存路径按本决策叠加采纳,紧贴其后保 gravity `UnifiedStorageWriter::commit_unwind`;
     另修复公共区 :425 死符号侧翻 `unwind_provider_rw()`→`database_provider_rw()`。
     详见 stages-pipeline.md「落地实录」。

- [x] 3. **`reth-payload-util` workspace 归属** —— 上游新增 crate `crates/payload/util` 与 gravity 的 `crates/pipe-exec-layer-ext-v2/event-bus` 必须以不同名（`reth-payload-util` vs `reth-pipe-exec-layer-event-bus`）在同一 workspace 共存。需要核查解决后的根 `Cargo.toml` workspace members 两者都在列。
   → **决策(2026-07-05,核实即裁决)**:双名共存成立——根 Cargo.toml members
   同时含 `crates/payload/util/`(:127)与
   `crates/pipe-exec-layer-ext-v2/event-bus`(:201),workspace deps 两条均在
   (:448/:485,实测)。event-bus Cargo.toml 的 AU 幻象已物化为 3 个冲突块
   (HEAD=event-bus vs v2.3.0=payload-util rename 串扰),解块=整块取 HEAD。
   - [x] 冲突解决:已落地(2026-07-06)——3 块整块取 HEAD,与 baseline blob 逐字节相等(实测);双名共存确认。

- [x] 4. **`bin/reth/Cargo.toml` 默认 features** —— 开启 `otlp` / `js-tracer` / `keccak-cache-global` / `min-trace-logs` 会传递性要求 `reth-node-core/<feature>` 声明。需要等 `crates/node/core/Cargo.toml` worker 给出结论后再决定本次合并是否一并开启。
   → **决策(2026-07-05,决策总原则 + 最小风险)**:本轮默认 features 维持
   baseline(`["jemalloc", "reth-revm/portable"]`),上游新默认 features 全部
   推迟 follow-up(与 rpc-eth-and-debug.md OQ4 failpoints 同模式)——
   node/core/Cargo.toml 未解、且传递接线属跨组;v2.4+ 或独立 PR 再评估开启。
   - [x] 冲突解决:已落地(2026-07-06)——7 块归零:default 维持 baseline、failpoints 独立(双规则),上游新 feature 定义整体不引入(引用未解块 crate 的 feature,定义即解析错);⟲ 偏差:dev-deps 保留上游全套(零冲突落盘的上游集成测试 tests/it/main.rs 实测在用,核实时漏查该文件);根依赖缺口仍前置(问题 6,cargo 组)。

- [x] 5. **stages/api `Stage` test module 的 port 可行性** —— gravity-storage 是否暴露与上游 5 参数 `ProviderFactory::new(rw_db, chain_spec, static_file_provider, rocksdb_provider, runtime)` 兼容的构造器？决定 follow-up port PR 的工作量。
   → **决策(2026-07-05,决策总原则②)**:port **不可行**——上游 test mod
   绑定的 `RocksDBProvider` 已被 f89d9d4e23 连文件删除(全仓零定义,实测),
   5 参构造器形态不存在。keep-gravity(删上游 test mod);follow-up 撤销,
   改为 v2.4+ 随 storage-v2 再合并时重估。
   - [x] 冲突解决:已落地(2026-07-06,由 stages 组解决)——1 块解毕,上游 mod tests
     整块丢弃。⟲ 勘误:该文件生产体(公共区)含上游带入的 `BlockRangeOutput`/
     `TransactionRangeOutput` 定义,实为存活自包含符号(非死符号),已按原则③保留并
     全组对齐 4 个调用点(见 stages-pipeline.md「落地实录」)。

- [x] 6. **(新增)`bin/reth` 的 ress 接线 vs 根 Cargo.toml 对齐决议** ——
   `7490ae4ca6` 按 v2.3.0 基线对齐根 Cargo.toml 时删除了 `crates/ress/*`
   members 与 `reth-ress-{protocol,provider}` 两条 workspace deps(当时判定
   依据是"上游已删",未查 bin/reth 引用);但实测 `bin/reth/src/ress.rs`
   (零冲突、gravity 侧)是**活的生产接线**(`install_ress_subprotocol`),
   baseline `bin/reth/Cargo.toml:48-49` 依赖这两条 deps,且 ress crates 文件
   全在盘、已是 gravity EBWT 形态(executed-block-split 文档核实)。
   → **决策(2026-07-05,决策总原则"不破坏 gravity 原有功能"硬条款)**:
   恢复根 Cargo.toml 的 ress members + 2 条 workspace deps = **部分回滚
   `7490ae4ca6` 的 ress 条目**(与 executed-block-split §9.1 回滚 option C
   同模式:「上游已删且无引用」前提在 bin/reth 处不成立,属当时核查遗漏)。
   连带:根 Cargo.toml 缺 `reth-primitives` workspace dep(实测零定义,即
   当前 cargo metadata 报错点)同属 cargo 组的根依赖修复批次。
   - [x] 冲突解决:已落地(2026-07-08 销记;实质早已满足)——前置由
     cargo 组 6a54e53528 兑现:根 Cargo.toml `reth-primitives`(:453)、
     `reth-ress-protocol`/`reth-ress-provider`(:498-499)、ress 2 members
     (:133-134)均实测在位(实际缺失面 7 项,非预估 ~20);
     bin/reth/Cargo.toml 7 块已随 OQ4 落地(2026-07-06)归零。本框所述
     「随后解 7 块」半句早被 OQ4 兑现,系账面漏销。连带:ress-provider
     的 `TaskSpawner` → `Runtime` API 迁移与 node/core `mod ress_args`
     挂载丢失补回已于 2026-07-08 Task #12 完成(见 executed-block 文档),
     `cargo check -p reth-ress-provider` 绿。

## ⟲ 落地实录(2026-07-06)

net ×4 + era ×2 + event-bus/bin-reth 两个 Cargo.toml 已落地(stages/api ×3
按指令留给 stages 组);收口 agent 全局验证通过。要点与偏差:

1. take-upstream 类(discv4/nat/fetcher/era ×2)与 v2.3.0 blob **hash 相等**;
   txgossip 机械合并 + 剥离全部 5 处 `.with_genesis_block()`(死方法)。
2. **bin/reth ress 接线修复(必要范围外延)**:lib.rs/main.rs 实测零冲突侧翻
   至纯 v2.3.0,baseline 的 `pub mod ress;` 与 `install_ress_subprotocol`
   调用链被静默丢弃——OQ6 前提"活接线"实际已断,已修(lib.rs 补挂载、
   main.rs 复原 baseline,调用链逐环实测)。
3. **收口补修(2026-07-06,net/network 零冲突侧翻 7 文件,原落地清单未覆盖)**:
   BAL 请求链同向剔除——eth_requests.rs(删 `BalProvider` 处理 impl,
   `GetBlockAccessLists` 派发臂改回空列表应答,eth-wire 消息层原样保留)、
   builder.rs/config.rs/testnet.rs 的 import+bounds ×7、metrics.rs 宏内
   FastInstant→std、tests/it/requests.rs 删 BAL 测试群(8 测试 + 基建,
   772→531 行)、tests/it/connect.rs 剥 `.with_genesis_block()`。全部
   parse 通过、死符号归零(实测)。
4. 越界记录:`crates/primitives/` 整目录缺失(reth_primitives 消费方:
   bin/reth、event-bus、ress ×2)与 ress 根依赖同批,归 cargo 组
   (总台账见 executed-block-split §九)。
