# trie-all-layers

## Baseline 与 worktree 锚定

- Gravity baseline commit：`0cb1687c1c`（Galxe `upstream/main` 上的稳定基线，2026-06-09）
- Baseline 已合并的 reth upstream tag：v1.8.3（`d620fd0eeb` #205, 2025-11-10）
- 目标 upstream：reth v2.3.0
- 当前分支: `merge-v2.3.0`，HEAD `e6b7e5ba32`
- 上游变更范围一律按 `v1.8.3..v2.3.0` 取
- gravity 侧变更一律按 `git log 0cb1687c1c -- <path>` 在 baseline 历史内取（v1.8.3 合并 `d620fd0eeb` 之后的 commits 视为 gravity-only 演进）

## 分组概要

- 文件数：18
- 复杂度：高（trie 层是本次合并中最重的单组，涉及 DB on-disk format、GAT cursor factory 迁移、proof v2 重构三条互相缠绕的主线）
- 涉及模块：
  - `crates/trie/common/*` — 类型 + 磁盘编码接口面（`Nibbles`、`StoredNibbles[SubKey]`、`StorageTrieEntry`、`TrieUpdates[V2]`、`TrieInput[Sorted]`）
  - `crates/trie/db/*` — `DatabaseTrieCursorFactory`、`DatabaseHashedCursorFactory`、`DatabaseStateRoot`、`DatabaseHashedPostState`，以及 gravity 的 `nested_hash` / `commitment` 模块
  - `crates/trie/parallel/*` — 并行 state-root 流水线；gravity 的 legacy `proof` vs 上游新增的 `proof_task`/`state_root_task`/`value_encoder`
  - `crates/trie/trie/*` — 顶层 crate 根（`lib.rs`）与 `verify.rs`
- baseline 历史中相关的 gravity-only commits（已用 `git merge-base --is-ancestor <hash> 0cb1687c1c` 验证全部在 baseline 内）：
  - `f1089a2ba3` (#100) — nested-trie 框架（geth 风格）
  - `2dde8ca181` (#109) — cache + state writer
  - `66ab036739` (#75)、`ac4d30767d` (#84) — account/state-root 缓存
  - `1d24757b41` (#117) — parallel OOM 修复
  - `671680af37` (#149) — `StoredNibblesSubKey` 磁盘紧凑编码 + 移除"兼容版本 trie updates"
  - `9633989cdc` (#154) — `no_std` 友好的 state_root
  - `605c372de6` (#237) — nested-hash 的 `eth_getProof`（legacy `proof` 路径）
  - `0b57bc2340` (#219) — nested hash 中的并行层
  - `9974ad0618` (#241) — CI 测试修复（commit_view 调用）
  - `25a86ae6d8` (#249) — EOA storage state-root 回退修复
  - `a1d7365bd6` (#212) — RocksDB 后端
  - `ff103f976a` (#313) — `commit_view` + prune-distance + `nested_hash.rs` 从 parallel 移到 db
- 解决顺序依赖：
  1. `common/Cargo.toml` 与 `common/src/lib.rs` 决定类型宇宙 → **最先**
  2. `common/src/nibbles.rs` + `common/src/storage.rs` 是一对**磁盘格式**决策 — 必须一致处理
  3. `common/src/updates.rs`（AA）与 `common/src/input.rs`（AA）
  4. `db/Cargo.toml`、`db/src/lib.rs`，然后 `db/src/{hashed_cursor,trie_cursor,state}.rs`
  5. `trie/trie/src/lib.rs` 然后 `trie/trie/src/verify.rs`
  6. `parallel/Cargo.toml` + `parallel/src/lib.rs`
  7. `db/tests/*` 机械跟随

## 逐文件分析

### `crates/trie/common/Cargo.toml`
**模块：** trie-common crate manifest
**冲突类型：** UU
**上游变更（v1.8.3 → v2.3.0）：** 新增可选 `arrayvec`（被 `PackedStoredNibbles[SubKey]` 编码器使用，PR #22158）；将 `alloy-eips` 提升到 dev/std/serde feature；将 `bytes`/`rayon` 从默认 std feature 集合中移除（变为可选）；新增 `reth-core` 复用（PR #23186）；删除 criterion benches（PR #22627）。
**Gravity 侧变更：** baseline 中本文件的 gravity-only commits 为 `f1089a2ba3` (#100, nested-trie 框架)、`0b57bc2340` (#219, parallel level in nested hash)；新增直接依赖 `reth-storage-errors`、`gravity-primitives`、`parking_lot`、`once_cell`、自定义 `rayon = ["dep:rayon"]` feature。`gravity-primitives` 是 `nested_trie` 模块所需。注意 `a1d7365bd6` (#212, RocksDB) 也在本文件历史中，但仅触及 features。
**影响范围：** 决定 `common/src/lib.rs` 能编译哪些类型。若删 `gravity-primitives`/`parking_lot`/`once_cell`，则 `nested_trie/`、updates.rs 中 V2 类型相关编译全崩。
**解决方案建议：** mechanical-merge。保留 gravity 的 `gravity-primitives`/`reth-storage-errors`/`parking_lot`/`once_cell`/`rayon` feature；采纳上游的可选 `arrayvec`、`alloy-eips` dev/std/serde 升级、可选化 `bytes?`。HEAD 上的 `criterion` dev-dep 与 `[[bench]] prefix_set` 应**丢弃**（上游 #22627 删了 criterion bench，baseline 上本文件历史也没有 gravity-only 的 prefix_set bench 添加 commit — HEAD 中那两行可能只是 v1.8.3 时代尾巴）。
**理由：** Gravity 的 nested-trie / V2 类型族（baseline `f1089a2ba3` #100、`671680af37` #149、`0b57bc2340` #219）依赖 gravity-primitives + parking_lot + once_cell；这些不能丢。上游 #22158 packed 类型新增的 `arrayvec` 与 dev-deps 是纯增量。

### `crates/trie/common/src/input.rs`
**模块：** `TrieInput` / 上游新增 `TrieInputSorted`
**冲突类型：** AA（baseline 历史中本文件最早出现在 `d3f302676b` v1.4.8 catch-up；merge-base 早于该 commit）
**上游变更：** 新增 `TrieInputSorted`（持有 `Arc<TrieUpdatesSorted>` + `Arc<HashedPostStateSorted>`，用于高效 multiproof 生成）；新增 `from_blocks_sorted()`；**收紧** `from_blocks` / `extend_with_blocks` 签名，从 `Option<&TrieUpdates>` → `&TrieUpdates`（调用方必须始终提供 trie updates）。相关 PR：#19068 (`be94d0d39` Merge trie changesets changes into main)、#19340 (`e58aa09f8` return sorted data from compute_trie_input)。
**Gravity 侧变更：** baseline 中本文件仅由 catch-up merge 触及（`d620fd0eeb` #205、`d3f302676b` #108、`2dde8ca181` #109）— 即 baseline 上 input.rs 的内容等同于 v1.8.3。gravity 无 input.rs 本体改动。AA 是因为 merge-base 早于该文件首次出现。
**影响范围：** 下游 `crates/storage/provider/`、`crates/engine/tree/`、`crates/chain-state/` 中的调用方今天仍持 v1.8.3 风格 `Option<&TrieUpdates>` 调用。Gravity 自身的 nested-trie 路径通过 `write_trie_updatesv2` 侧通道（不走 `TrieInput`）写入 V2 数据，所以"V1 缺失"在 gravity 语境里并非真实需求。
**解决方案建议：** take-upstream。原样采纳上游 `TrieInputSorted` + 收紧后的 `&TrieUpdates` 签名。调用方迁移时对 legacy/nested-only 区块传 `&TrieUpdates::default()`。
**理由：** baseline 上本文件无 gravity-specific 修改（仅 catch-up merge）。`TrieInputSorted` 是 v2 proof / payload-builder 流水线的必备入口。上游签名严格更通用（`&TrieUpdates::default()` 廉价且显式）。无 chain-halt / data-loss 风险。

### `crates/trie/common/src/lib.rs`
**模块：** crate 根 — 模块列表 + re-exports
**冲突类型：** UU
**上游变更：** 新增模块 `execution_witness`、`lazy`（`LazyTrieData`/`SortedTrieData`）、`target_v2`（`ChunkedMultiProofTargetsV2`/`MultiProofTargetsV2`/`ProofV2Target`）、`trie`/`trie_node_v2`、`ordered_root`、`utils`；re-export `TrieMaskIter` 与 `hashed_state::serde_bincode_compat`；删除 `doc_auto_cfg` feature flag（PR #18758 `850083dbd`）。相关 PR：#22566、#22158、#22021、#19687、#19068。
**Gravity 侧变更：** baseline 中 gravity-only commits 为 `f1089a2ba3` (#100, `pub mod nested_trie` + `pub use updates::{StorageTrieUpdatesV2, TrieUpdatesV2}`)、`2dde8ca181` (#109)、`cb2992e451` (#122, 修 nested_trie 编译 warning)。保留 `feature(doc_auto_cfg)` 因为 gravity-primitives 文档块用到。
**影响范围：** Re-export 接口面；`use reth_trie_common::TrieUpdatesV2` 的下游消费者包括 `gravity-storage` (`crates/gravity-storage/src/lib.rs:9`)、`crates/trie/db/src/nested_hash.rs`、`crates/stages/stages/src/stages/merkle.rs`。
**解决方案建议：** mechanical-merge。保留 gravity 的 `pub mod nested_trie` 与 `pub use updates::{StorageTrieUpdatesV2, TrieUpdatesV2}`；新增上游所有新模块（`execution_witness`、`lazy`、`target_v2`、`trie`、`trie_node_v2`、`ordered_root`、`utils`）；保留 `doc_auto_cfg`。
**理由：** baseline `f1089a2ba3` #100 引入 `pub mod nested_trie` 与 V2 类型 re-export；多个下游已硬编码这两个路径。上游新增模块都是纯新名字（无名称冲突）。`doc_auto_cfg` 是 baseline 上 gravity-only 加入的（`cb2992e451`），删除会让 nested_trie 文档块编译失败。

### `crates/trie/common/src/nibbles.rs`
**模块：** `StoredNibbles`、`StoredNibblesSubKey` — `AccountsTrie` / `StoragesTrie` MDBX 表的磁盘 key 编码
**冲突类型：** UU
**上游变更：** PR #22158 (`80bf5532a`) — 将 `StoredNibbles::Compact` 切换到 `arrayvec::ArrayVec<u8, 64>`（语义相同，免分配）；引入 `PackedStoredNibbles` / `PackedStoredNibblesSubKey`（33 字节定长 packed 编码）。**关键点：上游的 `StoredNibblesSubKey::Compact` impl 仍保持原始 64 字节右填充 + 1 字节长度的 65B 格式**；只有新的 `Packed*` 类型用更小的布局。PR #22314 (`8d97ab63c`) — 改用栈上 `[u8; 65]`。
**Gravity 侧变更：** baseline `671680af37` (#149) 重写 `StoredNibblesSubKey::to_compact`/`from_compact` 为变长格式 `[encoded_len:u8][packed_nibbles: encoded_len/2 bytes]`（HEAD blob 中可见：`buf.put_u8(encoded_len as u8); buf.put_slice(&pack)`）。这是**磁盘 wire-format 变更** — gravity 写入的字节与上游 65B 格式不能互读。`9974ad0618` (#241) 仅触及测试。
**影响范围：** **本组最高风险文件**。直接影响 `StoragesTrie` MDBX 表的内容。选上游 → 任何 gravity 格式写入的 Galxe 网络节点磁盘无法被新二进制读取（critical data-loss 风险）。选 gravity → 失去上游 packed 类型，但保留磁盘兼容。
**解决方案建议：** keep-gravity（对 `StoredNibblesSubKey::Compact` 函数体），并**新增**上游的 `PackedStoredNibbles` / `PackedStoredNibblesSubKey` 类型。两个类型并存，不冲突。可同时采纳上游对 `StoredNibbles::Compact`（即 account key 那一边）的 `ArrayVec` 重写（gravity 没改这部分，等价于 take-upstream）。
**理由：** baseline `671680af37` (#149) 改的是 on-disk 字节布局，无迁移路径；gravity 主网若按 v1.8.3 基线部署即在持续写入 #149 格式，硬切上游将破坏所有 storage trie 读路径。上游 PR #22158 引入的 `Packed*` 类型与 gravity #149 不字节兼容（gravity = 长度前缀 + 紧密打包；上游 packed = 打包 + 零填充到 32 + 长度后缀），不可静默互转 — 但作为**新类型并存**是安全的。

### `crates/trie/common/src/storage.rs`
**模块：** `StorageTrieEntry` — `StoragesTrie` DupSort 表的 Value 半边
**冲突类型：** UU
**上游变更：** PR #22158 (`80bf5532a`) — 将 `SubkeyContainedValue` trait 重命名为 `ValueWithSubKey`（带 `type SubKey` 关联类型 + `get_subkey()`）；`Compact::from_compact` 重写为委托 `StoredNibblesSubKey::from_compact(buf, 65)`（假定上游 65B 格式）；新增 `PackedStorageTrieEntry`。PR #19068 (`be94d0d39`)、#23186 (`677d07041`) 触及。
**Gravity 侧变更：** baseline `605c372de6` (#237, nested-hash eth_getProof)、`9974ad0618` (#241)、隐含的 `671680af37` (#149) — HEAD blob 中 `from_compact` 是变长解码：`let encoded_len = buf[0]; let pack_len = (encoded_len / 2) as usize; let mut nibbles = Nibbles::unpack(&buf[1..1 + pack_len])` — 与 nibbles.rs 配套。`impl SubkeyContainedValue { fn subkey_length() }` 返回 `Some(self.nibbles.len().div_ceil(2) + 1)`。
**影响范围：** 与 nibbles.rs 同一磁盘兼容性约束。`SubkeyContainedValue` → `ValueWithSubKey` trait 重命名跨 crate 影响 `reth-primitives-traits`（另一组）。
**解决方案建议：** keep-gravity（对 `Compact::from_compact` 函数体，变长解码，与 nibbles.rs 配对）；同时**采纳**上游 `ValueWithSubKey` trait 迁移（重命名 impl block，设 `type SubKey = StoredNibblesSubKey`，`get_subkey() -> self.nibbles.clone()`）；按上游定义新增 `PackedStorageTrieEntry`（不冲突，gravity 还没有 Packed 表）。
**理由：** baseline `671680af37` (#149)（隐含修改 StorageTrieEntry 的 from_compact 解码逻辑以匹配 nibbles.rs）— 跨文件一致性约束。trait 重命名纯 API 层，不影响磁盘字节。

### `crates/trie/common/src/updates.rs`
**模块：** `TrieUpdates`、`TrieUpdatesSorted`、`StorageTrieUpdates`，以及 gravity 的 `TrieUpdatesV2` / `StorageTrieUpdatesV2`
**冲突类型：** AA（merge-base 早于 `f1089a2ba3`；该 commit 在 gravity 侧首次加入本文件的 V2 类型，但 v1.8.3 baseline 也仅有 `TrieUpdates`，没有 V2）
**上游变更：** 新增 `with_capacity`、`extend_from_sorted`（PR #21202/#21473 `a9bd38a43e` parallelize merge_ancestors_into_overlay）；新增 `clone_into_sorted`（PR #20784 `485eb2e8d`）；`into_sorted_ref` 重构（PR #21232 `22b465dd6`）；从 sorted 数据高效构建（PR #20333 `d489f80f6`）。
**Gravity 侧变更：** baseline `f1089a2ba3` (#100) 引入 `TrieUpdatesV2` / `StorageTrieUpdatesV2`（基于 `nested_trie::Node`）；`671680af37` (#149) 细化（删除"兼容版本 trie updates"）；`cb2992e451` (#122) 编译警告修复；`2dde8ca181` (#109) cache writer。baseline 中还引入了 `drain_into_sorted` 方法（HEAD blob 可见）。
**影响范围：** `TrieUpdatesV2` 被 gravity-storage、`crates/trie/db/src/nested_hash.rs`、stages/merkle、engine/tree/recovery 消费。`extend_from_sorted` 被上游 `input.rs::from_blocks_sorted` 和新 payload builder 调用。
**解决方案建议：** mechanical-merge。保留所有 gravity V2 类型（`TrieUpdatesV2`、`StorageTrieUpdatesV2`、`StorageTrieUpdatesV2::deleted()`）与 `drain_into_sorted`；**添加**上游 `with_capacity`、`extend_from_sorted`、`clone_into_sorted`；`into_sorted_ref` 生命周期写法选能编译的即可。
**理由：** baseline `f1089a2ba3` (#100) 为 V2 类型的 ground truth；删除会让 5+ 下游编译失败。上游新增的 API 与 V2 类型在数据模型上正交，无冲突。

### `crates/trie/db/Cargo.toml`
**模块：** trie-db crate manifest
**冲突类型：** UU
**上游变更：** 新增 `reth-trie-common`、`reth-stages-types`、可选 `reth-metrics` + `metrics`（用于 changesets 模块，PR #20997 `a74cb9cbc`）；将 `reth-primitives-traits` 提升 `features = ["std"]`；将 `reth-storage-api` 提升 `features = ["db-api"]`；`test-utils` feature 加入 `reth-stages-types/test-utils`。PR #22211 (`7594e1513`)、#18882 (`eed34254f`) 触及。
**Gravity 侧变更：** baseline `ff103f976a` (#313) 是本文件唯一 gravity-only commit。引入直接依赖 `alloy-rlp`、`once_cell`、`parking_lot`、`rayon`、`reth-storage-errors`、dev-dep `rand` — 全部支撑 `nested_hash` 模块（该 PR 把 `nested_hash.rs` 从 parallel 移到 db）。`reth-storage-api = { features = ["std"] }` 是 v1.8.3 时代的 baseline。
**影响范围：** 决定 `db/src/lib.rs` 能编译什么。移除 gravity 新增 → `nested_hash.rs` 编译失败；移除上游新增 → `changesets.rs` 编译失败。
**解决方案建议：** mechanical-merge。采纳 gravity 的 `alloy-rlp`、`once_cell`、`parking_lot`、`rayon`、`reth-storage-errors`、dev-dep `rand`；采纳上游 `reth-trie-common`、`reth-stages-types`、可选 `reth-metrics`+`metrics`、`reth-primitives-traits` 升级到 std；feature 调和：用上游 `reth-storage-api = { features = ["db-api"] }` 覆盖 gravity 的 `["std"]`（`db-api` 是上游新功能门，gravity 后续无论如何要采纳）；`test-utils` 加入 `reth-stages-types/test-utils`；`serde` feature 同时保留 `rand/serde` (gravity) 与 `reth-stages-types/serde` (upstream)。
**理由：** baseline `ff103f976a` (#313) 引入的 gravity 依赖支撑下游 `nested_hash` + RocksDB 维度；上游 changesets/overlay-builder 引入需要 stages-types + metrics。

### `crates/trie/db/src/hashed_cursor.rs`
**模块：** `DatabaseHashedCursorFactory` — DB 侧的 `HashedCursorFactory` impl
**冲突类型：** UU
**上游变更：** **GAT 迁移**。泛型从 `<'a, TX>(&'a TX)` 变为 `<T>(T)`；impl block 变为 `impl<TX: DbTx> HashedCursorFactory for DatabaseHashedCursorFactory<&TX>`，带 `type AccountCursor<'a> = ... where Self: 'a;`。PR #19114 (`8eb5461da`) Add lifetime to cursors、#19588 (`573191e1d`) Allow reusing Hashed/TrieCursors。还 derive 了 `Clone`。
**Gravity 侧变更：** baseline 历史中 gravity-only commits（`66ab036739` #75、`2dde8ca181` #109）触及本文件，但是 `d620fd0eeb` (v1.8.3 merge) 之前的内容；v1.8.3 之后无 gravity-only commit。HEAD blob 上 gravity 手写了 `impl<TX> Clone for DatabaseHashedCursorFactory<'_, TX>` — 这是 v1.8.3 baseline 的 cursor factory 形式 + 一个 manual Clone impl。
**影响范围：** 这是大规模 API 变更 — 每个用 `HashedCursorFactory::AccountCursor` 的调用方都必须采纳 GAT 形式。verifier (`trie/trie/src/verify.rs`) 在 v2.3.0 中已 GAT 化。
**解决方案建议：** take-upstream。
**理由：** baseline 上 v1.8.3 合并以来本文件无 gravity-only 演进。GAT factory 是 v1.9~v2.3.0 期间引入的全生态变更（PR #19114、#19588），抗拒会迫使每个消费者重新实现 factory。手写 Clone impl 在新形式下被 `#[derive(Clone)]` 替代。

### `crates/trie/db/src/lib.rs`
**模块：** trie-db crate 根
**冲突类型：** UU
**上游变更：** 新增 `mod changesets; pub use changesets::*;`（PR #20997）；`prefix_set::PrefixSetLoader` 重命名为自由函数 `load_prefix_sets_with_provider`；`state.rs` 导出增加 `from_reverts_auto`；`storage.rs` 导出增加 `hashed_storage_from_reverts_with_provider`；`trie_cursor` 导出增加 `LegacyKeyAdapter`、`PackedKeyAdapter`、`StorageTrieEntryLike`、`TrieKeyAdapter`、`TrieTableAdapter`；新增 `with_adapter!` 宏 + re-export `PackedAccountsTrie`/`PackedStoragesTrie` 表；**删除** `mod witness; pub use witness::DatabaseTrieWitness;`（PR #22564 `b2eb061fe`）。
**Gravity 侧变更：** baseline `ff103f976a` (#313) 在本文件加入 `pub mod nested_hash;` 与 `mod commitment; pub use commitment::{MerklePatriciaTrie, StateCommitment};`。更早 `66ab036739`/`2dde8ca181` 加了 `DatabaseHashedStorage` re-export（在 v1.8.3 之前固化）。
**影响范围：** 设定整个 db crate 的模块接口面。`Packed*Adapter` 拆分是消费 `DatabaseTrieCursorFactory` 的全部下游所必需。
**解决方案建议：** mechanical-merge。保留 gravity 的 `pub mod nested_hash;` + `mod commitment;` + `pub use commitment::{MerklePatriciaTrie, StateCommitment};`；保留 `DatabaseHashedStorage` re-export；采纳上游的 `mod changesets; pub use changesets::*;`、`with_adapter!` 宏、`trie_cursor` 新 adapter 导出、`PackedAccountsTrie`/`PackedStoragesTrie` re-export；删除 `pub use witness::DatabaseTrieWitness;` 与 `mod witness;`（上游已删）。对 `prefix_set`：保留 gravity 的 `pub use prefix_set::PrefixSetLoader`（仍被 `state.rs` 中 gravity 兼容垫片需要 — 见 state.rs 小节），同时也 re-export 上游的 `load_prefix_sets_with_provider`。
**理由：** baseline `ff103f976a` (#313) 是 `nested_hash` / `commitment` 的来源，必须保留。上游 adapter / changesets / witness 移除是编译上游路径（state.rs、trie_cursor.rs）所必需。详见开放问题 #2 关于 nested-hash 路径 witness 替代品。

### `crates/trie/db/src/state.rs`
**模块：** `DatabaseStateRoot` + `DatabaseHashedPostState` trait impls — stages 流水线的入口
**冲突类型：** UU
**上游变更：** 大重构。`incremental_root_calculator` 等 trait 方法签名从 `&'a TX` 改为 `&'a (impl ChangeSetReader + StorageChangeSetReader + StorageSettingsCache + DBProvider<Tx = TX>)`（PR #23657 `344037d04`、#23667 `6377a957c`）。`DatabaseHashedPostState::from_reverts` 改为取 `impl RangeBounds<BlockNumber>` 并返回 `HashedPostStateSorted`；新增 `from_reverts_auto`。泛型链改为 `<A: TrieTableAdapter>`。
**Gravity 侧变更：** baseline 历史中本文件 gravity-only commits 仅 `66ab036739` (#75)、`ac4d30767d` (#84)、`2dde8ca181` (#109) — 全部在 `d620fd0eeb` v1.8.3 合并之前。HEAD blob 是 v1.8.3 baseline 形式：`<TX: DbTx>` + `KH: KeyHasher` 泛型 + `PrefixSetLoader::<_, KeccakKeyHasher>::new(tx).load(range)`。trait 签名仍是 `fn from_reverts<KH: KeyHasher>(tx: &TX, from: BlockNumber)`。
**影响范围：** 调用方包括 `crates/stages/stages/src/stages/merkle.rs`、`crates/storage/provider/src/providers/database/provider.rs`、以及关键的 `crates/storage/provider/src/providers/state/historical.rs:136` — 后者直接调用 `HashedPostState::from_reverts::<KeccakKeyHasher>(self.tx(), self.block_number)`（gravity 的下游 RPC eth_getBalance 历史查询路径）。这条调用属于 `HashedPostState`（在 trie crate 根）而不是 `DatabaseHashedPostState` trait 本身，但 trait 签名变更可能级联。
**解决方案建议：** take-upstream（主体采纳上游）。**必须**保留一个兼容垫片：保留 gravity `pub trait DatabaseHashedPostState<TX>` 的 `from_reverts<KH: KeyHasher>(tx: &TX, from: BlockNumber)` 方法签名（或加一个独立的 inherent fn），以维持 `crates/storage/provider/src/providers/state/historical.rs:136` 的调用面。或者按上游迁移调用点，传 provider 而非 tx。
**理由：** baseline 上本文件无 v1.8.3 之后的 gravity-only 改动（规则 4 的 take-upstream），但调用方（historical.rs，gravity 的 RPC 历史读路径）仍走 KeyHasher 泛型形式。直接砍掉会破坏 RPC（非 chain-halt，但功能性回归）。上游 PR `344037d04`、`6377a957c` 的 provider-argument 形式自洽，应作为主路径。

### `crates/trie/db/src/trie_cursor.rs`
**模块：** `DatabaseTrieCursorFactory`、`DatabaseAccountTrieCursor`、`DatabaseStorageTrieCursor`
**冲突类型：** UU
**上游变更：** PR #22158 引入 `TrieKeyAdapter` trait + `LegacyKeyAdapter` / `PackedKeyAdapter` impls；新增 `StorageTrieEntryLike`、`TrieTableAdapter`；`DatabaseTrieCursorFactory` 泛型化为 `<T, A: TrieKeyAdapter>`；cursor 类型按 adapter 参数化（同一份代码同时读 legacy 65B 表与 packed 33B 表）。同时采纳 GAT factory（与 hashed_cursor.rs 一致）。PR #21486 (`9eaa5a630`) 去掉 Sync bound。
**Gravity 侧变更：** baseline 历史中 gravity-only commits 仅 `66ab036739` (#75)、`2dde8ca181` (#109)、`9974ad0618` (#241, 仅测试) — `9974ad0618` 在 v1.8.3 之后但只动注释/格式。HEAD blob 是 v1.8.3 baseline 形式：`<'a, TX>(&'a TX)` + 手写 `impl<TX> Clone`。`TrieKeyAdapter` 体系在 baseline 中不存在。
**影响范围：** 这是 trie → MDBX 的读侧 adapter。`TrieKeyAdapter` 是让同一 factory 同时适配 `AccountsTrie`/`StoragesTrie`（legacy）与 `PackedAccountsTrie`/`PackedStoragesTrie`（v2）的关键。gravity #149 的非标 packed 编码意味着 `LegacyKeyAdapter` 分支调用 `StoredNibblesSubKey::Compact` 时产出 gravity 字节布局 — 自动正确。
**解决方案建议：** take-upstream。adapter trait + GAT factory 原样落地。磁盘格式分歧被收纳在 `StoredNibblesSubKey::Compact` 内部（见 nibbles.rs / storage.rs 决策）。
**理由：** v1.8.3 合并以来无 gravity-only 实质改动。两项变更正交：上游引入两种编码之间的类型级开关（Legacy vs Packed adapter）；gravity 自定义"legacy"分支编码到什么。两个决策同时成立。

### `crates/trie/db/tests/fuzz_in_memory_nodes.rs`
**模块：** 内存 trie cursor + hashed cursor 的 proptest 模糊测试
**冲突类型：** UU
**上游变更：** 重写为使用 `with_adapter!` 宏 + 新 adapter 泛型 + 收紧后的 `from_blocks` 签名。
**Gravity 侧变更：** baseline `9974ad0618` (#241) 在每个 state 变更后插入 `provider.tx_ref().commit_view().unwrap();`（RocksDB 序章 — 由 `a1d7365bd6` #212 引入，`ff103f976a` #313 扩展），还把每个 storage cursor 包在 `{}` block 中以确保旧 cursor drop（RocksDB iterator 不见提交后的数据）。
**影响范围：** 仅测试。
**解决方案建议：** mechanical-merge。采纳上游测试体（`with_adapter!`、新 from_blocks 签名），然后在相同插入点（`upsert` 之后）重新加回 `commit_view()` 调用；保留 gravity 的"每次迭代重建 cursor"模式（RocksDB 语义所需）。
**理由：** baseline `9974ad0618` (#241) + `a1d7365bd6` (#212) 是为 RocksDB 后端的必要 fix；上游测试改写不知 gravity 有 RocksDB 路径。

### `crates/trie/db/tests/trie.rs`
**模块：** trie 计算的集成测试（增量 vs 完整 state-root、storage roots）
**冲突类型：** UU
**上游变更：** 采纳新 adapter 形式：引入类型别名 `DbStateRoot<'a, TX, A>` / `DbStorageRoot<'a, TX, A>`，使用 `DatabaseTrieCursorFactory<&'a TX, A>`；`tx.write_storage_trie_updates_sorted(...)` 替代旧的 `write_storage_trie_updates`。
**Gravity 侧变更：** baseline `9974ad0618` (#241) — 同样的 `commit_view()` 模式；`7c34bd98bf` (#111, fix ut and integration test)。HEAD 仍用 v1.8.3 风格的 `StorageRoot::from_tx_hashed(tx.tx_ref(), hashed_address)`、`tx.write_storage_trie_updates(...)`。
**影响范围：** 仅测试。
**解决方案建议：** mechanical-merge。采纳上游 adapter 形式 + 新签名；如 rocksdb 测试失败按 #241 加回 `commit_view()`；`write_storage_trie_updates` → `write_storage_trie_updates_sorted` 跟随上游（gravity 没改 storage writer trait）。
**理由：** 同 fuzz_in_memory_nodes.rs。

### `crates/trie/db/tests/walker.rs`
**模块：** `TrieWalker` 集成测试
**冲突类型：** UU
**上游变更：** 切换到 `DatabaseTrieCursorFactory` factory 形式（用 `with_adapter!`）；import `TrieCursorFactory`、`StorageSettingsCache`。
**Gravity 侧变更：** baseline `9974ad0618` (#241) 加 `commit_view()` 调用，并在 `walk_nodes_with_common_prefix` 测试上加 `#[ignore = "deprecated"]`（gravity 在 nested-hash 假设下该测试 flaky / 不再适用）。
**影响范围：** 仅测试。
**解决方案建议：** mechanical-merge。采纳上游 factory 形式；保留 `walk_nodes_with_common_prefix` 上的 `#[ignore = "deprecated"]`；在 cursor 提交点重新加回 `commit_view()` 调用 + 提交后重建 cursor 模式。
**理由：** baseline `9974ad0618` (#241) — gravity-only 测试 hardening；上游不知。

### `crates/trie/parallel/Cargo.toml`
**模块：** parallel trie crate manifest
**冲突类型：** UU
**上游变更：** 新增 `reth-tasks`（rayon feature）、`reth-primitives-traits`（dashmap+std features）、`crossbeam-channel`、`crossbeam-utils`、可选 `reth-trie-sparse` 带 `trie-debug`、可选 `rand`；新增 `reth-chainspec`/`reth-ethereum-primitives` dev-deps；新增 `trie-debug` feature；删除 criterion bench（PR #22627 `598f228e2`）。PR #23657、#23658、#23246、#22270、#22154 等触及。
**Gravity 侧变更：** baseline `ff103f976a` (#313) — 当 `nested_hash` 从本 crate 移到 trie-db crate 后，把 `reth-db-api`/`reth-trie-db` 从本 crate 移除（实际 HEAD 仍保留以编译 `pub use reth_trie_db::nested_hash;` re-export？需复核）。baseline `f1089a2ba3` (#100) 加 `reth-trie-common`、`reth-trie-db`、`reth-trie-sparse[std]`、`tokio[rt,rt-multi-thread]`、`criterion`、dev-only `rayon`、`benches/root.rs`。baseline `1d24757b41` (#117)、`9633989cdc` (#154)、`25a86ae6d8` (#249) 触及。HEAD 上没有 `reth-primitives-traits` 作为主依赖（被 #100 移除）。
**影响范围：** 决定 parallel/src/lib.rs 与 proof_task/state_root_task 是否能编译。
**解决方案建议：** mechanical-merge。采纳上游新增（`reth-tasks`、`crossbeam-channel`、`crossbeam-utils`、`reth-primitives-traits` 带 `dashmap,std` features、可选 `reth-trie-sparse[trie-debug]`、`rand`、`trie-debug` feature、`reth-chainspec`/`reth-ethereum-primitives` dev-deps）；保留 gravity 的 `reth-trie-common`、`reth-trie-db`、`reth-trie-sparse[std]`、`tokio[rt,rt-multi-thread]`；删掉 gravity 的 `criterion` + `[[bench]] root`（上游 #22627 删了所有 criterion bench，baseline 上 `benches/root.rs` 可能已是死代码）。
**理由：** baseline 引入的 gravity 主依赖支撑 nested-hash 后台任务；上游新增支撑 proof_task / state_root_task / value_encoder 流水线。

### `crates/trie/parallel/src/lib.rs`
**模块：** parallel crate 根
**冲突类型：** UU
**上游变更：** 移除 `pub mod proof`（PR #22270 `6f9a3242e` remove legacy proof code paths and simplify to V2-only）；新增 `pub mod state_root_task`（PR #23246 `29bab063b`）；新增 `pub(crate) mod value_encoder`；删除 `doc_auto_cfg` feature flag（PR #18758）。
**Gravity 侧变更：** baseline `f1089a2ba3` (#100) 引入 `pub mod proof`；baseline `605c372de6` (#237) 让 `proof` 模块支持 nested-hash 的 `eth_getProof`；baseline `ff103f976a` (#313) 把 `nested_hash.rs` 从本 crate 物理移到 `reth-trie-db` crate，**但保留** `pub use reth_trie_db::nested_hash;` 让 `use reth_trie_parallel::nested_hash::NestedStateRoot;` 路径继续工作（已验证 baseline blob 中存在该 re-export）。
**影响范围：** 5 个下游消费 `reth_trie_parallel::nested_hash::NestedStateRoot`：`crates/stages/stages/src/stages/merkle.rs:33`、`crates/gravity-storage/src/block_view_storage/mod.rs:14`、`crates/engine/tree/src/recovery.rs:24`、`crates/storage/db-common/src/init.rs:62`、`crates/pipe-exec-layer-ext-v2/execute/tests/pipe_test.rs:20`。`proof` 模块被 nested-hash RPC 路径需要。
**解决方案建议：** mechanical-merge。保留 gravity 的 `pub mod proof;` 与 `pub use reth_trie_db::nested_hash;`；新增上游的 `pub mod state_root_task;` 与 `pub(crate) mod value_encoder;`；保留 gravity 的 `doc_auto_cfg`（被 baseline 加入，参见 lib.rs 第 8 行 HEAD blob）。
**理由：** baseline `605c372de6` (#237) 让 legacy `proof` 模块成为 gravity nested-hash 的 `eth_getProof` 必要组件；baseline `ff103f976a` (#313) 显式以 re-export 维持向后兼容路径，移除会让 5 个下游 import 失败。上游新增模块完全正交。

### `crates/trie/trie/src/lib.rs`
**模块：** 主 trie crate 根
**冲突类型：** UU
**上游变更：** 新增 `pub mod proof_v2`（PR #19687 `c57792cff`、#22021、#22922）；新增 `pub mod changesets`（PR #20997 `a74cb9cbc`）；`mock` 模块 cfg 从 `#[cfg(test)]` 放宽到 `#[cfg(any(test, feature = "test-utils"))]`；删除 `doc_auto_cfg`（PR #18758）。
**Gravity 侧变更：** baseline 历史中 gravity-only commits 仅 `f1089a2ba3` (#100)、`2dde8ca181` (#109)、`d620fd0eeb` (#205, v1.8.3 catch-up)、`d3f302676b` (#108, v1.4.8 catch-up) — 后两个是 catch-up merge，无本文件实质 gravity 改动。HEAD 保留 `doc_auto_cfg`（gravity 跨多个 trie crate 保留这个 flag 以让 `nested_trie` 文档块可编译）。
**影响范围：** 极小 — 增量模块 + cfg 放宽。
**解决方案建议：** mechanical-merge。采纳上游所有新增（`proof_v2`、`changesets`、放宽的 `mock` cfg）；保留 gravity 的 `doc_auto_cfg`。
**理由：** baseline 上 gravity 改动仅 `doc_auto_cfg`（隐含在 nested_trie 风格一致性中），其他全为 catch-up。上游新增是纯增量。

### `crates/trie/trie/src/verify.rs`
**模块：** `Verifier` — 独立 verifier，从 `HashedAccounts`/`HashedStorages` 重算 trie 校验已存的 trie 节点
**冲突类型：** AA（baseline 中本文件首次出现于 `d620fd0eeb` v1.8.3 catch-up；merge-base 早于该 commit）
**上游变更：** GAT 重构：`Verifier` 变为 `Verifier<'a, T: TrieCursorFactory, H>`，带 `trie_cursor_factory: &'a T`；cursors 是 `T::AccountTrieCursor<'a>` / `T::StorageTrieCursor<'a>`；去掉 `T: Clone` 约束。还有 5~6 处 typo 修复（PR #20894 `44a6035fa`、#21385 `ccff9a08f`、#18962 `1dfd0ff77`、#19114 `8eb5461da`）。
**Gravity 侧变更：** baseline 中本文件 gravity-only commits 仅 `9974ad0618` (#241, CI 测试修复) — 实际未触及本文件实质内容（HEAD blob 与 v1.8.3 等同）。
**影响范围：** 仅编译侧；必须与 `hashed_cursor.rs` / `trie_cursor.rs` 的 GAT 决定一致。
**解决方案建议：** take-upstream。
**理由：** baseline 上无 gravity-specific 实质修改；与上游 GAT factory 一致即可。Verifier 是调试/审计工具，无 gravity 定制。typo 修复无害。

## 分组级解决 playbook

**Phase 1 — 类型宇宙（common）：**
1. `common/Cargo.toml` — mechanical-merge 依赖（保留 gravity-primitives、parking_lot、once_cell、rayon feature；新增 arrayvec、alloy-eips；丢 criterion + prefix_set bench）
2. `common/src/lib.rs` — 合并模块列表（保留 `nested_trie` + `TrieUpdatesV2`/`StorageTrieUpdatesV2` re-export；新增 `execution_witness`/`lazy`/`target_v2`/`trie`/`trie_node_v2`/`ordered_root`/`utils`）
3. `common/src/nibbles.rs` — **keep-gravity** 的 `StoredNibblesSubKey::Compact` 函数体（保留 #149 磁盘格式），新增上游 `PackedStoredNibbles`/`PackedStoredNibblesSubKey`；`StoredNibbles::Compact` 跟随上游 ArrayVec 重写
4. `common/src/storage.rs` — keep-gravity 的变长 `Compact::from_compact`；采纳上游 `ValueWithSubKey` trait 重命名；新增 `PackedStorageTrieEntry`
5. `common/src/updates.rs` (AA) — 保留 gravity `TrieUpdatesV2` / `StorageTrieUpdatesV2` / `drain_into_sorted`；新增上游 `with_capacity`、`extend_from_sorted`、`clone_into_sorted`
6. `common/src/input.rs` (AA) — take-upstream（`TrieInputSorted` + 收紧 `&TrieUpdates` 签名）

**Phase 2 — trie crate 根：**
7. `trie/trie/src/lib.rs` — mechanical-merge（新增 `proof_v2`、`changesets`；放宽 `mock` cfg；保留 `doc_auto_cfg`）
8. `trie/trie/src/verify.rs` (AA) — take-upstream（GAT cursor 形式）

**Phase 3 — db crate（依赖 Phase 1 的 nibbles/storage 决策）：**
9. `trie/db/Cargo.toml` — mechanical-merge
10. `trie/db/src/lib.rs` — 保留 `pub mod nested_hash;` + `mod commitment;` + `MerklePatriciaTrie`/`StateCommitment` re-export；新增上游 `changesets`、`with_adapter!`、adapter re-exports、Packed 表 re-export；删 `pub use witness::DatabaseTrieWitness;`
11. `trie/db/src/hashed_cursor.rs` — take-upstream（GAT factory）
12. `trie/db/src/trie_cursor.rs` — take-upstream（TrieKeyAdapter + GAT factory）
13. `trie/db/src/state.rs` — take-upstream 主体；为 `historical.rs:136` 调用面保留 gravity 兼容垫片（`from_reverts<KH: KeyHasher>(tx: &TX, from: BlockNumber)`）或迁移调用点

**Phase 4 — parallel crate：**
14. `trie/parallel/Cargo.toml` — mechanical-merge；丢 criterion bench
15. `trie/parallel/src/lib.rs` — 保留 `pub mod proof;` + `pub use reth_trie_db::nested_hash;`；新增上游 `state_root_task` + `value_encoder`；保留 `doc_auto_cfg`

**Phase 5 — 测试修复：**
16. `trie/db/tests/walker.rs` — 上游 + 保留 `#[ignore = "deprecated"]` + 加回 `commit_view()`
17. `trie/db/tests/trie.rs` — 上游 + 加回 `commit_view()` + `write_storage_trie_updates` → `write_storage_trie_updates_sorted`
18. `trie/db/tests/fuzz_in_memory_nodes.rs` — 上游 + 加回 `commit_view()` + 每次重建 cursor 模式

**合并后跨组编译检查：**
- `crates/gravity-storage/src/block_view_storage/mod.rs:14` `use reth_trie_parallel::nested_hash::NestedStateRoot;` 必须仍可解析
- `crates/gravity-storage/src/lib.rs:9` `use reth_trie::{updates::TrieUpdatesV2, HashedPostState};` 必须仍可解析
- `crates/storage/provider/src/providers/state/historical.rs:136` `HashedPostState::from_reverts::<KeccakKeyHasher>(self.tx(), ...)` 必须仍可解析（或迁移）
- `crates/storage/provider/src/providers/database/provider.rs` 必须采纳上游 `DatabaseStateRoot::incremental_root_calculator(provider, range)` 签名
- `crates/engine/tree/src/recovery.rs:24` + `crates/stages/stages/src/stages/merkle.rs:33` + `crates/storage/db-common/src/init.rs:62` 对 `nested_hash::NestedStateRoot` 的依赖在步骤 15 之后验证

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [ ] 1. **GAT-factory 的下游传播。** Phase 3 步骤 11/12 采纳 `type AccountCursor<'a>` GAT 形式，要求 `reth_trie::hashed_cursor::HashedCursorFactory` 与 `reth_trie::trie_cursor::TrieCursorFactory`（在 `crates/trie/trie/src/`）声明 GAT 关联类型。这些文件不在本组冲突列表内，但 verify.rs (AA) 已经按 GAT 形式编写，说明上游 v2.3.0 trie crate 的 trait 也已 GAT 化 — 需在 Phase 2 步骤 7 之后实际编译验证。
   - [ ] 冲突解决:待编译验证后落地;crates/trie/trie/src/verify.rs 现存 10 处冲突块,GAT 传播需 Phase 2 步骤 7 后 cargo check(2026-07-03 实测)。

- [ ] 2. **nested-hash 路径的 witness 兼容。** 上游 PR #22564 (`b2eb061fe`) 删除了 `DatabaseTrieWitness` 与 `crates/trie/db/src/witness.rs`。gravity #237 (`605c372de6`) 在 `parallel/src/lib.rs` 保留 `pub mod proof` 用于 nested-hash 的 `eth_getProof`。删除 witness.rs 后，需 grep `crates/rpc/` 是否还有调用方仍 `use reth_trie_db::DatabaseTrieWitness;`；若有，把 `witness.rs` 作为 gravity-only 文件留下（不在本组冲突列表 — 可能本就不在 worktree 中）。
   - [ ] 冲突解决:核实部分通过、待拍板:实测 crates/trie/db/src/witness.rs 已不存在、crates/rpc/ 无 DatabaseTrieWitness 调用方;但 crates/trie/db/src/commitment.rs 仍有 2 处引用待处置(2026-07-03 实测)。

- [ ] 3. **`DatabaseHashedPostState::from_reverts` / `HashedPostState::from_reverts` 调用面。** `crates/storage/provider/src/providers/state/historical.rs:136` 调用 `HashedPostState::from_reverts::<KeccakKeyHasher>(self.tx(), self.block_number)`，这是 RPC 历史读路径（`StateProvider`）。上游签名是 `from_reverts(provider, range) -> HashedPostStateSorted`。决策点：(a) 在 state.rs 保留 gravity trait 方法作为兼容垫片；(b) 在 historical.rs 调用点迁移到 provider-argument 形式。后者更彻底，前者更小风险 — 建议先 (a) 解锁编译，再单独 PR 做 (b) 迁移。
   - [ ] 冲突解决:待决策后落地;crates/storage/provider/src/providers/state/historical.rs 现存 20 处冲突块(2026-07-03 实测)。

- [ ] 4. **`StoragesTrie` MDBX on-disk 格式锁定。** gravity #149 (`671680af37`) 改变了 `StoredNibblesSubKey` 的磁盘编码（变长 `[len][packed]` vs 上游 65B 右填充）。任何在当前 gravity main 上跑过 Galxe 网络的节点无法滚动升级到使用上游 65B 编码的二进制。本次合并保留 gravity 编码（nibbles.rs 决策为 keep-gravity）。需在 `MIGRATION.md` 或类似处记录这一锁定，并明确：上游新增的 `PackedAccountsTrie`/`PackedStoragesTrie` 表是 v2 路径，gravity 不消费这两张表（直到有迁移工具）。
   - [ ] 冲突解决:未落地:nibbles.rs 现存 4 处冲突块;实测仓库尚无 MIGRATION.md,编码锁定记录未创建(2026-07-03 实测)。

- [ ] 5. **`trie-common` 中 `gravity-primitives.workspace = true` 依赖。** 需在 workspace `Cargo.toml` 中验证 `gravity-primitives` 提供 `nested_trie::Node` 所需的类型（B256/Bytes 风格的 leaf payload）。Cargo.lock 已按 CLAUDE.md 备注解决，但 dependency tree（trie-common → gravity-primitives）应在 Phase 1 步骤 1 之后用 `cargo check -p reth-trie-common` 编译验证。
   - [ ] 冲突解决:待编译验证后落地;crates/trie/common/Cargo.toml 现存 13 处冲突块,cargo check -p reth-trie-common 需解完后跑(2026-07-03 实测)。
