# Storage 冲突解决 — 执行记录 & 跨 crate TODO

> 分支 `gravity-reth-merge-v2.3.0`。解决 storage 三组冲突(storage-db-and-mdbx / storage-api-and-traits / storage-providers-and-writers)。
> 核心原则(用户指定):**保留 gravity-reth 在 reth 上的关键优化 —— storage / state root / cache**。
> 细则:rocksdb 保证正确;mdbx 仅保留代码;不采用上游原生 rocksdb(gravity 用自己的);MPT trie 用 gravity 方案;
> BAL 保留代码(独立文件)但不接 BAL 并行(并行走 grevm);跨 crate 级联标 TODO,顺手能修则修。

## 采用的解决策略:统一 keep-gravity(storage 层完全采用 gravity 方案)

storage 层的 6 个 crate(`reth-db`/`reth-db-api`/`reth-db-models`/`reth-db-common`/`reth-storage-api`/`reth-provider`
+`reth-libmdbx`)的 **`src/` 全部还原为 gravity baseline `0cb1687c1`**,即 pre-merge 的 gravity 设计。
理由:storage/state-root/cache/nested-trie/pipe-exec/rocksdb 全是 gravity 深度改造且互相耦合的子系统,
逐 hunk 把上游 storage-v2 / 原生 rocksdb / Either / BAL 拆进来会破坏 gravity 设计且极易出错;
整体 keep-gravity 是"完全采用 gravity 方案"最忠实、最内聚、最可 review 的表达。

- **`Cargo.toml`**:按 keep-gravity 解决(`reth-db` `default=["rocksdb"]`;provider 保 `gravity-primitives`/
  `reth-evm`/`pipe_test`/直接 `dashmap`)。`db/Cargo.toml` 顺手删掉两个失效 `[[bench]]`(hash_keys/criterion,
  上游删了 bench 源文件,只留 get)。
- **上游原生 rocksdb / storage-v2 文件**(gravity 不采用)在 provider crate 已删除:
  `providers/rocksdb/`、`either_writer.rs`、`traits/rocksdb_provider.rs`、`changeset_walker.rs`、
  `providers/state/overlay.rs`、`changesets_utils/`、`providers/static_file/writer_tests.rs`、`src/init.rs`。
- **BAL 保留代码**:`storage-api/src/bal.rs`、`provider/src/bal.rs` 留在磁盘,但**不在 `lib.rs` 里 `mod` 声明**
  → 作为未编译的孤儿文件存在,满足"保留代码、不接 BAL 并行"。同理 `metadata.rs`/`macros.rs`/
  `txn_pool.rs`/`utils.rs` 等上游 storage-v2/mdbx 新文件均为未编译孤儿(baseline 的 mod 树不引用它们)。

## 跨 crate 级联 TODO(非 storage 组负责,storage 代码依赖)

1. **`reth-primitives-traits` 需恢复 `SubkeyContainedValue`**(primitives-traits/misc 组)
   - 上游 v2.3.0 重组了 primitives-traits,丢了 gravity 的
     `pub trait SubkeyContainedValue { fn subkey_length(&self) -> Option<usize>; }`。
   - storage 侧 `db-models/accounts.rs`、`db-api/models/mod.rs`、以及 `trie/common/{storage.rs,nested_trie/node.rs}`
     都 `use reth_primitives_traits::SubkeyContainedValue`。**需该组恢复 gravity 定义**,storage 才能链接。

2. **`trie/common/src/lib.rs` 的 `pub mod nested_trie;`**(trie-all-layers 组)——已顺手补回(storage 的
   `tables/mod.rs`/`models/mod.rs` 依赖 `reth_trie_common::nested_trie::{StorageNodeEntry, StoredNode}`)。
   trie 组解决时请确认与其 nested_trie / trie_node_v2 决策一致。

3. **下游 crate 对齐 gravity storage API**(rpc / engine / node / stages / cli 等各组)
   - storage 层现在是纯 gravity 设计(如 `DbTx::commit()->Result<bool>`+`commit_view`、`recover_block_number`、
     `UnifiedStorageWriter`、`StorageLocation`、`NestedStateRoot`、`TrieWriterV2`、`+Send+Sync`、
     `validator_node_only`、`check_consistency_pipe_execution`、`ChainStateKey::LastSafeBlockBlock`)。
   - 上游 v2.3.0 的 storage-v2 API(`WriteStateInput`/`StateWriteConfig`/`RocksDBProvider`/`BalStoreHandle`/
     `ExecutionWitnessMode`/`Either*`/`StorageSettings`/`Metadata` 表/`LastSafeBlock`/`commit()->Result<()>`)
     **在 storage 层不存在**。这些组解决冲突时须对齐 gravity storage API(即同样 keep-gravity),否则编译不过。

4. **`reth-provider` 的 `Cargo.toml` 依赖**:还原自 gravity baseline;若 workspace 级 cargo 解决(`200c31273c`
   "drop 9 stale workspace entries")删掉了 baseline provider 引用的某 workspace dep,需补回。用
   `cargo tree -p reth-provider` 核实。

5. **上游 init-state OOM 缓解 / 非零 genesis stage-checkpoint fix** 未 port(`db-common/init.rs` 保持 gravity,
   stage checkpoint 用 `Default::default()` = block 0)。如需支持非零 genesis,后续再评估。

## 状态

- storage 三组共 26 个带冲突标记的文件 + 9 个被上游自动合并(丢了 gravity 语义)的无标记文件,全部处理完毕。
- `grep -rlE '^<<<<<<< ' crates/storage/` = 空。
- 仅解决 storage 冲突,未追求整仓编译(按用户要求);跨 crate 依赖见上。

## 追加(第二轮):trie / prune / static-file

用户决策:trie 用 keep-gravity(nested-trie);static-file 与 prune 尽量采用上游优化(gravity 在这两块几乎未改)。

### Trie(keep-gravity baseline,与 storage 一致)
- 6 个 trie crate(common/db/parallel/trie/sparse/sparse-parallel)的 `src/` + 3 个冲突 Cargo.toml(common/db/parallel)+ db/tests 全部还原为 gravity baseline。
- 保留:nested-trie(`AccountsTrieV2`/`StoragesTrieV2`/`nested_hash`/`NestedStateRoot`)、gravity `#149` 变长 `StoredNibblesSubKey` 磁盘格式、`TrieUpdatesV2`。
- 上游 trie 引擎(proof_v2 / arena sparse / changesets / GAT cursor / state_root_task 等)= 未采纳,以孤儿文件形式留在磁盘(baseline lib.rs 不声明)。理由:gravity nested-trie 与上游 sparse-trie 互斥,GAT 迁移会级联到已还原为非-GAT 的 provider。

### Prune(优化已交付,无需 bridging)
- **`crates/prune/types` 已是上游 2.3.0**(auto-merged,无标记):`MINIMUM_DISTANCE`(Receipts/Bodies 强制留 64 块,#21520)+ `min_blocks_override`(可配置最小裁剪距离,#23082)已就位。
- `prune_target_block(tip, segment, purpose)` 公共签名 baseline 与上游**完全一致**,retain-64/min-distance 在其内部经 `segment.min_blocks()` 强制执行 → baseline `prune/prune` 调用不变即免费获得优化。
- `crates/prune/prune/src/` 还原为 gravity baseline,保留 rocksdb 专属 `delete_by_key(RawKey)`(#212/#246,rocksdb cursor 语义与 mdbx `delete_current` 不同)+ `commit_view`。
- 未采纳的上游 segment 级改动(`delete_current` mdbx 专属、`#21767` 上游原生 rocksdb 批量 prune、`PruneProgress` 富报告)——与 gravity rocksdb 无关或冲突。

### Static-file(Phase 1:桥接 baseline provider 到上游引擎)
- **引擎 crate 已是上游 2.3.0**(`nippy-jar`、`static-file/types`,auto-merged):**两阶段 commit(`#20984` sync_all/finalize)、offset/header underflow healing(`#19819`/`#18628`)、变长文件格式、`.csoff`(`#21596`)** 均在。
- **provider `static_file/{manager,writer}.rs` 桥接**:`SegmentHeader::new`、`NippyJar*`、`find_fixed_range`、`DEFAULT_BLOCKS_PER_STATIC_FILE` 等 API 全部保留;唯一编译阻塞是上游 `StaticFileSegment` 新增 3 个 storage-v2 变体(TransactionSenders/AccountChangeSets/StorageChangeSets)导致 baseline provider 的 3-arm match 非穷尽。已修:
  - 3 个 `StaticFileSegment::iter()` 循环加 gravity-segment 过滤(只保 Headers/Transactions/Receipts);
  - 4 个 match 加 wildcard(manager `_ => None` / `_ => unreachable!`;writer `_ => unreachable!` / `_ => {}`)。
- 保留 gravity `check_consistency_pipe_execution` + `has_receipt_pruning`(#253/#255 pipe-exec 恢复契约)。
- **Phase 1 交付**:nippy-jar 级引擎正确性收益(两阶段 commit、underflow healing)——provider 调 `commit()`/checker 即得。**未交付(Phase 2,已按用户决策推迟)**:变长文件*实际使用*(provider 仍传 `DEFAULT_BLOCKS_PER_STATIC_FILE` 固定 500k)、manager 级 `maybe_heal_segment`(#20508)/`StaticFileSegmentIndex` 合并(#19803)——需把 provider `static_file` 层升级到上游 + always-legacy `StorageSettings`/`EitherWriter` 垫片。

### ⚠️ 编译验证
- WIP 树仍有其它组(engine/rpc/node/cli 等)的冲突标记,**无法整仓 `cargo build`**。trie/prune 的解决是结构性还原(baseline 已知可编);static-file provider 的 7 处 bridging 编辑**需在整树可编后验证**(尤其 `matches!(&StaticFileSegment,...)` 过滤与 4 个 wildcard 的返回类型)。`metrics.rs` 两处 `StaticFileSegment::iter()` 未过滤——无 match、仅多 3 个空 metric 条目,无害。

## 追加(第三轮,rebase 后复核):孤儿清理 + Cargo.toml src/manifest 错配修复

> 触发:`git pull --rebase upstream gravity-reth-merge-v2.3.0` 后复核 static-file + prune。

### 1. 删除孤儿文件
- **`crates/prune/prune/src/segments/user/bodies.rs`** —— reth 2.3.0 的文件,squash checkpoint 带入,`user/mod.rs` **无** `mod bodies;` 声明(未编译孤儿),gravity baseline 无此文件。已 `git rm`。删除后 `crates/prune/prune/src/` 文件集与 gravity baseline **逐一致**。
- 说明:`crates/prune/prune/src/segments/static_file/{headers,receipts,transactions,mod}.rs` 之所以在 diff 里"像新增",是因为 reth 2.3.0 重构 prune 时**删掉了整个 `static_file/` 子目录**,squash checkpoint 吃了该删除,keep-gravity 还原时把它们恢复回来——是忠实还原(`headers.rs` 与 baseline 字节一致),非凭空新增。

### 2. **系统性 bug:blanket "resolve Cargo.toml to v2.3.0"(`7490ae4ca6`)在 keep-gravity crate 上留下 src/manifest 错配**
症状:src 还原为 gravity baseline,但 Cargo.toml 取了上游 2.3.0 版 → **上游删掉的、但 gravity baseline src 仍 `use` 的依赖丢失** → 编不过。已定位并修复:

| crate | 丢失且被用 | 使用点 | 修复 |
|---|---|---|---|
| `reth-prune` | `reth-chainspec`、`alloy-eips` | `builder.rs`/`set.rs`/`user/transaction_lookup.rs` | Cargo.toml 整体还原 baseline(src 100% baseline)。顺带删掉未用的 `reth-storage-api`/`reth-stages-types` |
| `reth-storage-api` | `dashmap`/`gravity-primitives`/`metrics`/`metrics-derive`/`once_cell`/`rayon`/`revm-bytecode`(共 7) | `cache.rs`(gravity `PersistBlockCache`,`lib.rs:89 mod cache` 已编译) | Cargo.toml 整体还原 baseline(编译集 100% baseline,`bal.rs`/`macros.rs`/`metadata.rs` 均为孤儿) |
| `reth-db-api` | `alloy-genesis`(无条件 use)、`parity-scale-codec`(`mod scale;` 无条件) | `models/mod.rs:8`、`scale.rs` | **surgical** 加回两项(未整体还原:baseline 的 `op` feature 引用 `reth-optimism-primitives`,该 workspace dep 已被删,整体还原会再挂)。`reth-optimism-primitives` 仅 `#[cfg(feature="op")]` 用,当前无 `op` feature → 不需要 |
| `reth-trie-sparse` | `auto_impl` | `provider.rs:8,34` `#[auto_impl::auto_impl(&)]`(`lib.rs:17 pub mod provider` 已编译) | surgical 加回(该 crate 真混合,含已编译上游 `arena/`,不可整体还原) |

- **workspace 根 `Cargo.toml`**:`parity-scale-codec` 也从 `[workspace.dependencies]` 被删,已加回 `parity-scale-codec = "3.2.1"`(gravity baseline 版本)。
- 判定方法:对每个 keep-gravity crate,取 `comm -23 baseline-deps current-deps`(baseline 有、当前无)后,`grep '\b<dep>::'` 确认是否被**已编译**(mod-declared,非孤儿、非 `#[cfg]`-off)的 src 使用。

### 3. ⚠️ 更大范围(**cargo 组负责,非本组**):workspace 根缺 ~20 个 dep 定义
- `cargo metadata` 失败于 `reth-primitives was not found in workspace.dependencies`——根 `[workspace.dependencies]` 缺失约 20 个被引用的 dep 定义:核心的 `reth-primitives`、`reth-trie-sparse-parallel`、`reth-engine-service`、`reth-ress-protocol`、`alloy-sol-macro`,以及整套 optimism/op-alloy(`op-revm`/`reth-optimism-*`/`op-alloy-*`)。这是 workspace/Cargo.lock 解决(`7490ae4ca6` + "drop 9 stale workspace entries")的**未完成 WIP**,阻塞整仓 `cargo metadata`/`build`,与本组 storage/prune/trie 无关但需 cargo 组补齐后才能编译验证上面的修复。

### 4. 校对:`net-prune-misc-crates.md` 的 `segments/mod.rs` 段与已执行的 keep-gravity 解决**不一致**
- 该段"解决方案建议"推荐 mechanical-merge:*采纳上游删除* `pub use static_file::{Headers,Receipts,Transactions}`,并在"关键决策"里推荐删 `mod static_file;` + 4 个 `static_file/*.rs` 文件。
- 但**实际执行的是 full keep-gravity**:`mod static_file;`(mod.rs:3)、re-export(mod.rs:11)、4 个文件、4 处测试体 `insert_historical_block(...) + commit_view()`(而非上游 `insert_block(&recovered)+commit()`)**全部保留**且自洽。
- 且 `pub use static_file::{...}` re-export **全仓无外部消费者**(grep 空),该段"下游消费者 fixup"与"rustfmt hook 悬空"的前提在已执行状态下不成立。
- 结论:**以该段 mod.rs 建议为准去"修"会反转 keep-gravity 解决**。同文件 `user/history.rs` 段(keep-gravity `delete_by_key`)与代码一致、正确。建议给 mod.rs 段加一条"已按 keep-gravity 解决,本建议作废"的勘误。
