# Storage API & Traits 冲突分析

## 分组概要

本组覆盖 `reth-storage-api` (storage trait 层) 与 `reth-db-api` (database 抽象层) 两个 crate 的冲突文件,共 12 个文件全部为 UU/AA。

冲突根因可归为三大类:

1. **Gravity 私有 trait 扩展**:Galxe baseline 在 storage-api 与 db-api 中加入了 `+ Send + Sync` 约束、`commit_view`、`recover_block_number`、`subkey_compress_length` 等成员,这些都是为支撑 `ParallelStateProvider`(#26/#27/#28)、cache 层(#75/#109)、并行 trie(#105/#113)、嵌套 trie(`AccountsTrieV2`/`StoragesTrieV2`,#112/#122/#134)以及 RocksDB 后端(#212)所必需的。
2. **Gravity 私有数据模型扩展**:`impl_compression_for_compact!` / `impl_compression_for_value_with_subkey!` 两个宏被搬到 `db-api/src/models/mod.rs` 里,把 storage v2 (nested trie) 的 `StorageNodeEntry`、`SubkeyContainedValue` 全部纳入。上游 v2.3.0 把这些宏搬去了 `reth-codecs`,只保留 `impl_compression_for_compact!(StoredBlockOmmers<H>, CompactU256)` 两条本地实现。
3. **上游 v2.3.0 大范围演进**:
   - `WriteStateInput` / `StateWriteConfig` 接管 `StateWriter::write_state` 入参(PR #20993、#21123、#21468、#22299)。
   - `TrieWriter` 引入 `write_trie_updates_sorted`,把所有的"先排序后写"流水线显式化(PR #21323、#22158)。
   - `StateProofProvider::witness` 增加 `ExecutionWitnessMode` 参数(PR #22289)。
   - `BalProvider` / `BalStoreHandle` 在 noop 中需要新挂(PR #23596/#23918)。
   - `StorageSettings` / `Metadata` 表(PR #19384)与 db-api 关联。
   - `PackedStoredNibbles(SubKey)` / `PackedAccountsTrie` / `PackedStoragesTrie` 给 storage v2 的 33-byte 紧凑键(PR #22158、#22379)。
   - `commit() -> Result<(), DatabaseError>` (移除了 `Result<bool>` 形式,PR #21077)。
   - `DbTx` / cursor 关联类型去掉 `Sync` 约束(PR #20516、#21486)。
   - `LastSafeBlockBlock` 拼写更正为 `LastSafeBlock`(PR #18992)。

两类冲突必然存在直接对抗:gravity 加 `Send + Sync` 是为多线程并行 I/O,upstream 删 `Sync` 是为单线程 I/O 优化。后续 merge 不能盲目 take-upstream,需要保留 gravity 侧的并发约束,**否则会让 `ParallelStateProvider` / RocksDB 后端无法编译**。

---

## 逐文件分析

### `crates/storage/storage-api/Cargo.toml`

**模块**: `reth-storage-api` crate 清单
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0,对该文件): `803839df0`、`8940f2f0d`、`8bb96ace6`、`b9969c5b1`、`08c61535d`、`5a3887148`、`40bc9d386`、`d5dc0b27e`、`c617d25c3`、`e20e56b75`。关键 v2.3.0 内容:
- `revm-database` 替代 `revm-bytecode` 作为依赖名。
- 引入 `alloy-eip7928`(执行见证特征 / BAL 相关)。
- `serde_json`(optional) + `reth-tokio-util`(optional) 作为新可选依赖,服务 `metadata` 子模块和 BAL 流。
- `db-api` feature 加 `dep:serde_json`;`std` feature 加 `serde_json?/std` + `dep:reth-tokio-util` + `alloy-eip7928/std`。
- `serde` feature 新增 `alloy-eip7928/serde`。
- `serde-bincode-compat` 去掉了 `reth-ethereum-primitives` 和 `reth-primitives-traits` 项。

**Gravity 侧变更**(baseline `0cb1687c1c` 上的 gravity-only):
- `5901e7da98` (#173): 引入 `revm-bytecode`、`dashmap`、`metrics`、`metrics-derive`、`once_cell`、`rayon` 等 cache/metrics 依赖,以及 `config-from-env` feature。
- `dfa14dcdea` (#168): 引入 `gravity-primitives` 依赖与 `config-from-env` feature 暴露。
- `66ab036739` (#75): 引入 cache 层底层依赖。

**影响范围**: 仅本 crate 编译 feature 流。后续 storage-api/lib.rs 的 `cache` 子模块 / `recover_block_number` / 并行 I/O 都依赖这些 dep。

**解决方案建议**: mechanical-merge
- 保留 gravity 私有 deps:`revm-bytecode`、`gravity-primitives`、`dashmap`、`metrics`、`metrics-derive`、`once_cell`、`rayon`。
- 新增 upstream 引入的:`alloy-eip7928`、`serde_json`(optional)、`reth-tokio-util`(optional)、以及 dev-deps `tokio`/`tokio-stream`。
- 注意 `revm-database` 与 `revm-bytecode` 是两个不同 crate,**保留两者**(gravity 用到了 `revm-bytecode` 的接口,upstream 把基础类型迁移到 `revm-database`)。
- features 段合并:`std` 同时含 `revm-bytecode/std` + `once_cell/std`(gravity)与 `serde_json?/std` + `dep:reth-tokio-util` + `alloy-eip7928/std`(upstream);`serde` 段保留 gravity 的 `dashmap/serde`、`revm-bytecode/serde`、新增 upstream 的 `alloy-eip7928/serde`;`serde-bincode-compat` 段需要核实是否 gravity 仍依赖 `reth-ethereum-primitives` / `reth-primitives-traits` 的 bincode-compat。
- `config-from-env` feature 保留。

**推理**: gravity 侧的依赖全部是为了 cache 层 + ParallelStateProvider + RocksDB 性能优化必需(见 #75/#109/#212)。如果直接 take-upstream 会把这些后端打掉。

---

### `crates/storage/storage-api/src/block_id.rs`

**模块**: `BlockNumReader` trait
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0): `7b2fbdcd5` (#20516 移除 DbTx 的 Sync bound)。**该文件几乎没有 v2.3.0 上游业务改动**。

**Gravity 侧变更**: `a1d7365bd6` (#212 RocksDB 集成) 在 `BlockNumReader` trait 上新增了 `recover_block_number()` 默认方法,作用是"返回 `StageId::Execution` 写过的最后一个块号" — 用于 RocksDB 后端在崩溃后恢复时知道哪些 batch 已落盘。

**影响范围**: RocksDB 后端的 crash-recovery 路径直接依赖该方法。MDBX 后端使用默认实现(`unimplemented!`)。

**解决方案建议**: keep-gravity
- 上游侧无业务改动,直接保留 gravity 的 `recover_block_number()` 默认方法。
- 去掉 conflict marker 即可。

**推理**: v1.8.3..v2.3.0 范围内上游对该文件的实质性 hunk 只有 `7b2fbdcd5` 中的 `Sync` bound 调整(不在本文件),conflict marker 是其它文件的连锁波及。`recover_block_number` 是 RocksDB 后端的硬依赖(#212),删除会直接破坏 crash-recovery。

---

### `crates/storage/storage-api/src/lib.rs`

**模块**: storage-api crate 入口
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `850083dbd` (#18758): 移除 `doc_auto_cfg` feature。
- `e20e56b75` (#19384): 引入 `metadata` 子模块、`MetadataProvider`/`MetadataWriter`/`StorageSettingsCache`、`StorageSettings`。
- `165a80441` (#23596 BAL 抽象): 引入相关导出。
- `815037e27` (#22379): storage v2 slot preimage。
- 其它若干 cfg 重排。
- 新增 `pub mod macros`。

**Gravity 侧变更**:
- `2dde8ca181` (#109): 引入 `mod cache; pub use cache::*;`(account & state-root cache trait)。
- `41aa4c2125` (#28) / `4a95a05490` (#27) / `377eb491b2` (#26): 引入 `ParallelStateProvider` / `STATE_PROVIDER_OPTS` 相关导出(可能通过 `cache.rs` 间接)。
- `850083dbd` 的"移除 doc_auto_cfg" 在 gravity 侧未跟进 — gravity baseline 仍是 `#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]`。

**影响范围**: storage-api 子模块拓扑。`cache` 模块被 grevm / EVM 层广泛使用。新引入的 `metadata` / `StoragePath` / `MetadataProvider` / `StorageSettingsCache` 是 v2.3.0 storage v2 的核心,后续 provider 层 / engine tree 都要消费。

**解决方案建议**: mechanical-merge
- 保留 gravity 私有的 `mod cache` 块。
- 保留 gravity 的 `docsrs` 属性写法(含 `doc_auto_cfg`)— 与 gravity `nested_trie` 文档块一致,不影响业务逻辑;take-upstream 也行,但需要确认 gravity 内部没有依赖 auto_cfg 行为的文档脚本。
- 新增 upstream 的 `metadata` 子模块导出(`#[cfg(feature = "db-api")] pub mod metadata;` 等条目)。
- 保留 `mod full; pub use full::*;`(两侧都有)。
- 新增 `pub mod macros;`。

**推理**: cache 模块是 gravity baseline 上 grevm 集成的入口,无法丢弃。metadata 模块是 upstream storage v2 的必要 plumbing。两者无冲突,可并存。

---

### `crates/storage/storage-api/src/noop.rs`

**模块**: `NoopProvider`(测试用 stub)
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `e21048314` (#19151): 移除 `header_td` / `header_td_by_number`(total difficulty 全部清除)。
- `177ad4c0b` (#19585): 移除 `transaction_block`(已被 `block_by_transaction_id` 替代)。
- `165a80441` (#23596): 新增 `BalProvider` 实现 + `bal_store: BalStoreHandle` 字段。
- `e20e56b75` (#19384): 新增 `StorageSettingsCache` 实现。
- `a05960ab0` (#22289): `StateProofProvider::witness` 加 `ExecutionWitnessMode` 参数。
- `e9598ba5a` / `058ffdc21`: 移除 / 增加 `block_by_transaction_id`、storage changeset 相关接口。
- `27fbd9a7d` (#21077): `commit() -> Result<()>` 形式,要求 noop 也提供 `commit()`。
- `f8efc7688` (#23310): 移除 changeset count APIs。
- `0eea4d76e` (#21400): 清理未用 import。
- `effa0ab4c` (#21528) / `1e734936d` (#21468): changeset 读写路径调整。
- `121160d24` (#21115): hashed state 作为规范状态表示。
- `0adbb4c9` baseline 仍是 `header(&self, _block_hash: &BlockHash)` — 引用形式;upstream 已改为按值。
- `3ab7cb98a` (#22178): 加回 `Arc` auto_impl。

**Gravity 侧变更**:
- gravity baseline 仍保留 `header_td` / `header_td_by_number` / `transaction_block` 三个老接口,因为这些 trait 在 gravity 侧未跟进 #19151/#19585 的清理。
- 保留 `BytecodeReader` import 等老 import 列表。
- `prune_modes: PruneModes::none()` 形式(upstream 改为 `PruneModes::default()`)。
- `header(&self, _block_hash: &BlockHash)` 仍为引用。

**影响范围**: 仅测试与 stub 路径,不影响主链业务。但因为 trait 签名变化(`witness` 多了 `mode` 参数),`commit()` 返回类型变了,如果 noop 不更新,所有依赖 `NoopProvider` 的 unit test 会 fail 编译。

**解决方案建议**: take-upstream
- 上游对 noop 的全部签名 / 字段调整都必须跟进 — 这些都是 trait 在主路径上的 breaking change,noop 必须同步。
- 把 gravity 保留的 `header_td*`、`transaction_block` 等老 stub 全部删除(其它文件的同步移除任务在 `block-and-header-providers.md` / `historical-and-changesets.md` 等分组中处理)。
- 新增 `bal_store: BalStoreHandle` 字段、`BalProvider` impl、`StorageSettingsCache` impl、`commit()` 默认实现、`witness(_mode: ExecutionWitnessMode)`。
- `prune_modes: PruneModes::default()` 跟随上游。

**推理**: noop 本身是 trait 接口的纯被动消费方,trait 一改它必须同改。gravity 侧没有在 noop 这一层加 chain-critical 逻辑(全部在 db-impl 层)。

---

### `crates/storage/storage-api/src/state.rs`

**模块**: `StateReader` / `StateProvider` / `HashedPostStateProvider` / `BytecodeReader` trait 定义
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `7b2fbdcd5` (#20516): 把 trait 上的 `Send + Sync` 退化为 `Send`(`DbTx` Sync 解耦)。
- `3ab7cb98a` (#22178): 加回 `Arc` auto_impl(回退一步)。
- `121160d24` (#21115): hashed state 作为规范状态表示。
- `505332271` (#20673): 文档 typo。
- `815037e27` (#22379): storage v2 slot preimage 路径。

**Gravity 侧变更**:
- 多个 baseline gravity commit (`377eb491b2`、`4a95a05490`、`41aa4c2125`、`2dde8ca181`、`5f6e2aa7ed`、`6b71a11f88`) 一直把 `StateReader`、`StateProvider`、`HashedPostStateProvider`、`BytecodeReader` 等 trait 的约束写为 `Send + Sync`,因为 `ParallelStateProvider`(#26/#27/#28)与 cache 路径(#75/#109)都需要跨线程持有。
- `StateProvider` supertrait 列表保留 `+ Send + Sync`。

**影响范围**: 极广。所有依赖这些 trait 的 provider 实现(`ParallelStateProvider`、grevm cache、并行 trie 计算)都假设 `Send + Sync`。如果跟随上游退到只有 `Send`,gravity 的并发路径直接编译失败。

**解决方案建议**: keep-gravity
- 保留 `+ Send + Sync` 在 `StateReader`、`StateProvider`、`HashedPostStateProvider`、`BytecodeReader` 上。
- 接受 upstream 的小文档 typo 修正(`"_all_ all changes" → "_all_ changes"`)。
- 其它 hashed state / slot preimage 路径无文件级冲突。

**推理**: 这是 gravity 与 upstream 的核心架构分歧。gravity 要并行执行 / 并行 I/O,必须 `Sync`。verify:`grep -r "ParallelStateProvider" crates/storage` 应该能看到强依赖。如果 take-upstream,后续要为 `ParallelStateProvider` 单独加 wrapper trait,代价远高于保留 `Send + Sync`。

---

### `crates/storage/storage-api/src/state_writer.rs`

**模块**: `StateWriter` trait
**冲突类型**: AA(两边都"新增",共同祖先里没有该文件版本)

**上游变更**(v1.8.3..v2.3.0):
- `f012b3391` (#20993 parallelize save_blocks): 引入 `WriteStateInput<'a, R>` enum,支持单 block / 多 block 两种入口。
- `80eb0d0fb` (#21123): `BlockExecutionOutput` 取代 `ExecutionOutcome` 作为 `ExecutedBlock` 内部表示。
- `1e734936d` (#21468) / `5b1010322` (#22299): 引入 `StateWriteConfig` 结构,控制是否写 receipts / storage-changeset 到 MDBX(vs static files)。
- `058ffdc21` (#18681): 静态文件路径调整。

新的 trait 签名:`write_state(impl Into<WriteStateInput<'a, Self::Receipt>>, OriginalValuesKnown, StateWriteConfig)`,`write_state_reverts(.., config: StateWriteConfig)`,`remove_state_above(block)`,`take_state_above(block)` 等。`Receipt` 上加了 `'static` bound。

**Gravity 侧变更**:
- gravity baseline `StateWriter` 签名是:`write_state_with_indices(execution_outcome, is_value_known, write_receipts_to, body_indices)` + `write_state(execution_outcome, is_value_known, write_receipts_to)` 兼容包装,`StorageLocation` 参数贯穿。
- gravity 维护 `write_state_with_indices` 是为了 RocksDB 后端在写 state 时同时下发 body indices(性能优化,#212)。

**影响范围**: `StateWriter` 是 chain-state 持久化的核心 trait,所有 provider 实现都要更新。RocksDB 后端 / MDBX 后端 / gravity 的 `DatabaseProvider::write_state` 实现都直接依赖。`StorageLocation`(决定写 db vs static file)与 upstream 的 `StateWriteConfig`(同样意图)语义重叠,需要选其一。

**解决方案建议**: needs-port + mechanical-merge
- 主体采用 upstream:`WriteStateInput` enum + `StateWriteConfig` 是 v2.3.0 的统一抽象,后续所有调用方都用这套签名。
- 但 gravity 的 `write_state_with_indices`(把 `Option<Vec<StoredBlockBodyIndices>>` 一并下发)的需求要 port 进来 — 可以作为 `StateWriteConfig` 的扩展字段(`body_indices: Option<...>`),或者保留 `write_state_with_indices` 作为附加方法,内部委派到新签名 `write_state` 后再补一次 body indices 写入。
- `StorageLocation` → `StateWriteConfig::receipts_to_db` 映射:gravity 用 `StorageLocation::Database` / `::StaticFiles` 二选一表达 receipts 落点,upstream 用 `config.receipts_to_db: bool` + 另一段 storage-changeset 配置;语义可对应。
- 同步删除 `remove_state_above` / `take_state_above` 的 `remove_receipts_from: StorageLocation` 参数,upstream 改为无参(receipts 落点已由全局 settings 决定)。

**推理**: 这是 chain-state 落盘路径,gravity 的优化(同步写 body indices)有性能价值;upstream 的 `WriteStateInput` / `StateWriteConfig` 是必须遵循的新抽象。两者可以共存:把 gravity 的 body indices 写入作为 RocksDB 后端在 `write_state` 实现内部完成,不污染公共 trait。需要在 PR 描述里说明这一拆分,否则 reviewer 容易把它当 regression。

---

### `crates/storage/storage-api/src/trie.rs`

**模块**: `StateRootProvider` / `StorageRootProvider` / `StateProofProvider` / `TrieWriter` / `StorageTrieWriter`
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `a05960ab0` (#22289 stateless witness): `StateProofProvider::witness` 增加 `mode: ExecutionWitnessMode` 参数。
- `3ab7cb98a` (#22178): 加回 `Arc` auto_impl(`#[auto_impl(&, Arc, Box)]`)。
- `da12451c9` (#21323): 引入 `write_trie_updates_sorted(&TrieUpdatesSorted)` 主签名,`write_trie_updates(TrieUpdates)` 默认实现委托给 sorted 版本(内部调 `into_sorted()`)。`StorageTrieWriter` 同步改造为 `write_storage_trie_updates_sorted`。
- `a74cb9cbc` (#20997 in-memory trie changesets): 相关 import 调整。
- `7b2fbdcd5` (#20516): trait 上 `Send + Sync` → `Send`。
- `be94d0d39` (#19068): trie changesets。

**Gravity 侧变更**:
- 保留 `Send + Sync` 在 `TrieWriter` / `StorageTrieWriter` 上(并行 trie 计算路径 #105/#113 需要)。
- `5f6e2aa7ed` (#105) / `2dde8ca181` (#109) / `3cd18422c9` (#134) / `cb2992e451` (#122): gravity 引入 `TrieWriterV2` trait(嵌套 trie 写入,nested trie 写 `TrieUpdatesV2` 类型),feature-gated 在 `db-api`。
- 保留 `write_trie_updates(&self, trie_updates: &TrieUpdates) -> ProviderResult<usize>`(by reference,不消耗 ownership);上游 PR #21323 改为 by value 然后转 sorted。
- `StorageTrieWriter::write_storage_trie_updates(iter)` 接口保持不变(by `&StorageTrieUpdates`)。
- `witness(input, target)` 仍是两参签名。

**影响范围**: 极广 — trie 写入是 state root 计算 / sync 流水线的核心。`TrieWriterV2` 是 gravity nested trie 的独有路径(`AccountsTrieV2`/`StoragesTrieV2` 表配套);上游 `write_trie_updates_sorted` 是 perf 优化(预排序减少 MDBX 写放大)。

**解决方案建议**: keep-gravity + needs-port
- 保留 `TrieWriterV2` 完整定义(gravity-only,db-api feature gated)。
- 保留 `+ Send + Sync` bound。
- 跟随上游引入 `write_trie_updates_sorted` + 把 `write_trie_updates` 改为 default 实现(签名要么取 owned 然后委派到 sorted,要么保留 gravity 的 by-ref 形式并自己实现 sorted 转换 — 选哪一种取决于 gravity provider 实现需不需要保留 `&TrieUpdates` 共享)。
- 同步给 `StorageTrieWriter` 引入 `write_storage_trie_updates_sorted`。
- `StateProofProvider::witness` 加 `_mode: ExecutionWitnessMode` 参数。

**推理**: gravity 的 nested trie 是已上线的 chain-critical 数据结构(参见 `AccountsTrieV2` 表),不能丢。upstream 的 sorted writer 是性能优化(可以暂时给个 default impl `self.write_trie_updates(trie_updates.into_unsorted())` 维持兼容,如果 sorted 路径未被 gravity 使用)。`witness` 的 `ExecutionWitnessMode` 是 stateless 客户端需求,gravity 短期不会用到 stateless,但 trait 签名必须跟进,否则下游 noop / provider 实现都编不过。

---

### `crates/storage/db-api/src/mock.rs`

**模块**: `DatabaseMock` / `TxMock` / `CursorMock`(测试 stub)
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `0569e884c` (#19302): 文档增强(`/// Mock database implementation for testing...` 等)。
- `27fbd9a7d` (#21077): `commit() -> Result<()>`。
- `f6dbf2d82` (#20964): 实现额外 dup methods。
- `815037e27` (#22379) / `4a6f9cd5c`: storage v2 / unwind 相关支持(加 `PathBuf` import 等)。

**Gravity 侧变更**:
- gravity baseline 保留 `commit(self) -> Result<bool, DatabaseError> { Ok(true) }` 原签名(未跟进 #21077)。
- 注释为简短形式(`/// Mock database used for testing with inner BTreeMap structure`)。

**影响范围**: 仅测试。但所有依赖 `DbTx::commit` 返回类型的 provider 测试 / cursor 测试都会受影响(`Result<bool>` vs `Result<()>` 不兼容)。

**解决方案建议**: take-upstream
- 全部跟随上游:`commit() -> Result<()>`、新增 doc strings、新增 `path: PathBuf` 等。
- gravity 侧无 chain-critical 逻辑在 mock,直接 take。

**推理**: 此为纯测试 stub,跟随上游 trait 签名即可。

---

### `crates/storage/db-api/src/models/mod.rs`

**模块**: 表 key/value 编解码 + `Compress`/`Decompress` 宏实现
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `677d07041` (#23186): `impl_compression_for_compact!` 从该文件搬到 `reth-codecs` 并重新导出。
- `80bf5532a` (#22158) / `8d97ab63c` (#22314): `StoredNibbles::Encoded` 从 `Vec<u8>` 改为 `arrayvec::ArrayVec<u8, 64>`;新增 `PackedStoredNibbles` / `PackedStoredNibblesSubKey` 编解码(33-byte 紧凑形式)。
- `e20e56b75` (#19384): `pub use metadata::*` + `StorageBeforeTx`、`Metadata` 表。
- `477fed7a1` (#22254): `reth-ethereum-primitives` 改用 alloy `EthereumReceipt`,移除 `Receipt<T>` 的 compact 实现。
- `da12451c9` (#21323): 移除部分 trie changeset 编解码。
- `ec9c7f8d3` (#21279): `ArrayVec` perf。
- 几乎所有 `impl_compression_for_compact!(...)` 中的具体 type list 都被 trim 到只剩 `StoredBlockOmmers<H>, CompactU256`。

**Gravity 侧变更**:
- gravity baseline 仍把 `impl_compression_for_compact!` / `impl_compression_for_value_with_subkey!`(后者是 gravity 私有,来自 `a1d7365bd6` #212 RocksDB)以及 `impl_compression_fixed_compact!` 三个宏都定义在本文件。
- 编解码列表中含 nested trie 专有 type `StorageNodeEntry`(`a1d7365bd6` 与 `2dde8ca181`)、`Receipt<T>`(gravity 未跟进 #22254)、`GenesisAccount` 等多个 type。
- `StoredNibbles::Encoded` 仍是 `Vec<u8>`(未跟进 #22158)。
- 引入 `SubkeyContainedValue` import(#212)。
- 没有 `PackedStoredNibbles*`、`metadata` 模块。

**影响范围**: 极广 — 直接决定 MDBX/RocksDB 上每个表的 key/value 字节布局。两侧的 `StoredNibbles` 编码若不一致(`Vec<u8>` vs `ArrayVec<u8, 64>`),trie 表数据 **磁盘格式不兼容**(`ArrayVec` 的 nibble 编码用 `iter().collect()` 是单 nibble 一字节;`Vec<u8>` 是 compact 形式,差异在编码长度上)。`Receipt<T>` 的 compact 实现移除也会影响 receipts 表的反序列化。

**解决方案建议**: keep-gravity + needs-port
- 保留 gravity 的 `impl_compression_for_value_with_subkey!` 宏定义(`subkey_compress_length` 路径必须有,RocksDB 子键长度要从 value 推算)。
- 保留 `impl_compression_fixed_compact!`(给 `B256`/`Address`)。
- 保留 `StorageNodeEntry` 等 nested trie 类型的 compact 注册。
- **StoredNibbles 编码**:这是 **磁盘兼容性** 决定点。如果 gravity 链上 trie 表已经用 `Vec<u8>` 形式编码,改用 `ArrayVec` 会导致旧数据无法读取 — 必须保留 `Vec<u8>` 形式(keep-gravity);如果 gravity 决定升级,需要 db migration。建议先 keep-gravity,在另一个独立 PR 中评估磁盘格式升级。
- `PackedStoredNibbles*`(33-byte 紧凑形式)可以 port 进来作为新 type(不替换老 `StoredNibbles`),给上游 storage v2 路径用。
- `Receipt<T>` 的 compact:gravity 仍依赖,**保留** `impl_compression_for_compact!(Receipt<T>)` 与 `validate_bitflag_backwards_compat!(Receipt, ...)`。
- `pub use metadata::*` / `StorageBeforeTx` 可以新增导入。

**推理**: 这是磁盘格式 chain-critical 文件。#22158 的 `ArrayVec` 升级并非纯性能优化 — 编码字节序列不同,历史数据库直接读不出来。必须先确认 gravity 侧 `AccountsTrie` / `StoragesTrie` 表的当前编码,再决定是否允许 take-upstream。`Receipt<T>` 的 compact 同理。

---

### `crates/storage/db-api/src/table.rs`

**模块**: `Compress` / `Decompress` trait 定义 + `IntoVec` 适配器
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `677d07041` (#23186): `Compress` / `Decompress` trait **整体迁移**到 `reth-codecs` crate,本文件改为 `pub use reth_codecs::{Compress, Decompress};`。
- `ec9c7f8d3` (#21279): 引入 `IntoVec` trait(给 `Vec<u8>`、`[u8; N]`、`ArrayVec<u8, N>` 提供 `into_vec()`)。
- `71c124798` (#18022): 优化 `StorageChangeSets` 导入路径。

**Gravity 侧变更**:
- 保留完整 `Compress` / `Decompress` trait 本地定义,**额外**加 `subkey_compress_length` 方法(`a1d7365bd6` #212 RocksDB 必需 — 决定 dup-sort key prefix 长度)。
- `Compress` trait bound:`Send + Sync + Sized + Debug`(gravity 保留),upstream 已迁移到 codecs。

**影响范围**: `Compress` trait 在 codebase 中被 100+ 处实现。如果 trait 定义从 db-api 移到 codecs,所有 impl 块的 `use` 语句都要更新;同时 gravity 的 `subkey_compress_length` 扩展需要保留 — 这意味着 gravity 不能简单 `pub use reth_codecs::{Compress, Decompress}`,要么:(a) 把 `subkey_compress_length` 也加到 reth-codecs(影响 codecs crate);(b) 在 db-api 保留 local trait + 加 `subkey_compress_length`,不迁移。

**解决方案建议**: keep-gravity + needs-port
- 推荐 (b):保留 gravity 的本地 `Compress`/`Decompress` 定义,**保留** `subkey_compress_length` 方法(RocksDB dup-sort 必需)。
- port upstream 的 `IntoVec` trait(并加上 `arrayvec::ArrayVec` 实现),供 packed nibbles 与未来其它 fixed-size 编码使用。
- 不要做 `pub use reth_codecs::{Compress, Decompress}` — 会丢 `subkey_compress_length` 与 `+ Send + Sync` bound,RocksDB 后端编译失败。
- 后续可以考虑把 `subkey_compress_length` 提案上游到 `reth-codecs`,然后再做迁移,但本次 merge 不动。

**推理**: trait 迁移是 upstream 的 layering 整理(把编解码从 db-api 拆出,让 codecs crate 更通用)。gravity 的 RocksDB 后端要 `subkey_compress_length` 来支持 dup-sort key 的前缀长度推算 —— 这是 RocksDB 上模拟 MDBX dup-sort 语义的关键。强行 take-upstream 会让 RocksDB 后端编译失败 / 数据存储错位。

---

### `crates/storage/db-api/src/tables/mod.rs`

**模块**: MDBX/RocksDB 表 schema 声明
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `82b5dad3` (#18992): `ChainStateKey::LastSafeBlockBlock` typo 修正为 `LastSafeBlock`。
- `e20e56b75` (#19384): 新增 `Metadata` 表(`type Key = String; type Value = Vec<u8>`)。
- `80bf5532a` (#22158): 新增 `PackedAccountsTrie` / `PackedStoragesTrie`(共享同一个 MDBX 表底层,但 type-level 用 33-byte packed key)。
- `563ae0d30` (#16660): 移除 total difficulty 支持(`HeaderTerminalDifficulties` 标记为 deprecated)。
- `da12451c9` (#21323): 清理未用 trie changeset code。
- `be94d0d39` (#19068): trie changesets。

**Gravity 侧变更**:
- gravity baseline 保留 `LastSafeBlockBlock`(未跟进 #18992 typo 修复)。
- 新增 `AccountsTrieV2`(`StoredNibbles -> StoredNode`,nested trie 主表)与 `StoragesTrieV2`(`B256 -> StorageNodeEntry, subkey StoredNibblesSubKey`,nested trie 存储表)— 来自 `233cad4c54` (#112) 与 `2dde8ca181` (#109)。
- 没有 `Metadata` 表 / `PackedAccountsTrie` / `PackedStoragesTrie` / `StorageBeforeTx`。

**影响范围**: 极广 — MDBX/RocksDB 表 schema 直接定义磁盘上数据布局。新增表(如 `AccountsTrieV2` / `Metadata`)需要 db init/migration 路径感知;`ChainStateKey` 的 enum 变体名变化会影响所有调用方匹配臂;`HeaderTerminalDifficulties` deprecated 状态调整后续 unwind / sync 流程。

**解决方案建议**: keep-gravity + needs-port + mechanical-merge
- **保留** `AccountsTrieV2` / `StoragesTrieV2` — 这是 gravity nested trie 的核心表,不能丢。
- **port** `Metadata` 表 + `pub use metadata::*` — upstream storage v2 需要,与 gravity 表无冲突。
- **port** `PackedAccountsTrie` / `PackedStoragesTrie` 作为 type-level 视图(共享 MDBX 表名)— 与 gravity v2 表名不冲突。
- `ChainStateKey`:**保留** `LastSafeBlockBlock`(避免修改 enum 变体名导致 grep `LastSafeBlock` 的所有 match 臂 / wire format 全部要改)。但同时把 upstream 的 typo 修复 cherry-pick:可以加一个 alias `pub const LastSafeBlock = LastSafeBlockBlock` 给新代码用,或者一次性把 gravity 内部所有引用都改名。这是 chain-state DB 的 key 编码 — `LastSafeBlockBlock => [1]`,改名不改编码字节就没问题,但所有源码引用都要 batch update。**建议**:一次性 rename 到 `LastSafeBlock`(典型 mechanical refactor),编码字节保持不变。
- `HeaderTerminalDifficulties` 标 deprecated 但**保留表**(gravity 链可能有历史数据)。
- nested trie 子模块 import 与 packed nibbles 子模块 import 都要并入。

**推理**: tables 是磁盘 schema 真相,gravity 的 `*V2` 表与 upstream 新表无名字冲突,可并存。`ChainStateKey` rename 是源码层面,字节编码不变。`PackedStoredNibbles` view 与 gravity 的 `StoredNibbles` 共存,storage v2 路径走 packed view、gravity nested trie 路径走旧 view。

---

### `crates/storage/db-api/src/transaction.rs`

**模块**: `DbTx` / `DbTxMut` trait 定义
**冲突类型**: UU

**上游变更**(v1.8.3..v2.3.0):
- `27fbd9a7d` (#21077): `commit() -> Result<()>`(原 `Result<bool>`,bool 信息没用)。
- `7b2fbdcd5` (#20516): `DbTx: Debug + Send`(去掉 `+ Sync`);`Cursor<T>`/`DupCursor<T>` 关联类型上的 `Sync` 也去掉。
- `9eaa5a630` (#21486): cursor 关联类型上的 `Sync` bound 清理。
- 新增 `CursorTy` / `DupCursorTy` / `CursorMutTy` / `DupCursorMutTy` 适配类型别名。

**Gravity 侧变更**:
- `9974ad0618` (#241): 给 `DbTx` 新增 `commit_view(&self) -> Result<bool, DatabaseError>` 默认方法(MDBX 默认 `unimplemented!`,RocksDB 实现把当前 batch 提交到 read view)— 服务 RocksDB 的 view-based read transaction 模型。
- `a1d7365bd6` (#212): `DbTx: Debug + Send + Sync` 保留(并行读必需)。
- `Cursor<T>: DbCursorRO<T> + Send + Sync` 保留。
- `commit(self) -> Result<bool, DatabaseError>` 保留(未跟进 #21077)。

**影响范围**: `DbTx` 是 db-api 顶层 trait,所有数据库后端(MDBX、RocksDB、Mock)都要符合。`commit` 返回类型变化是 breaking,要同步所有 impl;`Sync` bound 移除会让 gravity 的并行 I/O 路径(`ParallelStateProvider`)编译失败。

**解决方案建议**: keep-gravity + needs-port
- **保留** `+ Send + Sync` 在 `DbTx`、`Cursor<T>`、`DupCursor<T>` 上(并行 I/O 必需)— 这与 storage-api/src/state.rs 的决策一致。
- **保留** `commit_view` 默认方法(RocksDB 后端硬依赖)。
- **commit() 返回类型**:这里两难。
  - keep-gravity (`Result<bool>`):所有上游同步实现(包括 noop、mock)都要保留 `Result<bool>`,与 #21077 后的 upstream 表面 API 分歧。
  - take-upstream (`Result<()>`):gravity 的 `commit_view` 仍可保留(返回 `Result<bool>`,提交是否真触发由 RocksDB 内部判断)。
  - **推荐**:take-upstream 的 `commit() -> Result<()>`,把 commit 是否实际执行(bool)语义迁到 `commit_view` 或新增 `try_commit() -> Result<bool>`。这样 upstream 测试 / noop / mock 不用大改,gravity 的 view-based commit 通过 `commit_view` 单独管理。
- **port** `CursorTy` / `DupCursorTy` / `CursorMutTy` / `DupCursorMutTy` 类型别名(无副作用,方便后续代码引用)。

**推理**: `commit` 返回 `bool` 在 MDBX 上几乎没有信息量(commit 失败会通过 `Err` 抛出,`Ok(true)/Ok(false)` 没有真实分支)。upstream 把它清理是合理的;但 gravity 的 RocksDB 后端有 view-based commit 的需求,要单独保留接口。`Send + Sync` 是与 upstream 的核心架构分歧(参见 state.rs),坚定 keep-gravity。

---

## 开放问题

> **决策追踪 checklist**:每条两个勾选框 —「决策」勾选 = 已拍板,条目末尾「→ **决策**: …」记录结论;「冲突解决」勾选 = 该决策已在 worktree 落地(相关冲突块已按决策解掉,经实测核实)。未勾选 = 待决策 / 待落地。

- [ ] 1. **`StoredNibbles` 编码升级是否做**?gravity baseline 用 `Vec<u8>`,upstream 用 `ArrayVec<u8, 64>`。两种编码的实际字节序列不一致(`iter().collect()` 与 `to_compact` 不同 — `to_compact` 内部用 nibble pair packing),会导致 trie 表 **磁盘格式不兼容**。如果 gravity 主网已存数据,**必须保留** `Vec<u8>` 版本;否则 reorg / 重启时读不出旧 trie node。需要 storage owner 确认。
   - [ ] 冲突解决:待 storage owner 确认后落地;crates/trie/common/src/nibbles.rs 现存 4 处冲突块(2026-07-03 实测)。

- [ ] 2. **`Compress`/`Decompress` trait 迁移到 `reth-codecs` crate**:上游 #23186 已经迁移,gravity 因 `subkey_compress_length` 扩展无法直接 `pub use`。是否要把 `subkey_compress_length` 提案上游到 reth-codecs?短期内本次 merge 不动(保留 db-api 本地 trait),但长期会有 trait 分歧维护成本。
   - [ ] 冲突解决:待决策后落地(本次保留 db-api 本地 trait);crates/storage/db-api/src/table.rs(1 处)/models/mod.rs(7 处)未解(2026-07-03 实测)。

- [ ] 3. **`StateWriter::write_state_with_indices` 的 body indices 同步写**:这是 gravity RocksDB 后端的性能优化(单次 batch 写 state + body indices 减少 fsync)。upstream 用 `WriteStateInput<'a, R>` enum 重构后,需要决定 body indices 是作为 `WriteStateInput::Single { body_indices: Option<...>, ... }` 字段进入新签名,还是作为 RocksDB 后端的 `write_state` 实现内部的 batch composition(不污染公共 trait)。
   - [ ] 冲突解决:待决策后落地;crates/storage/storage-api/src/state_writer.rs 现存 6 处冲突块(2026-07-03 实测)。

- [ ] 4. **`LastSafeBlockBlock` → `LastSafeBlock` 重命名**:典型 mechanical refactor,但要在一个 commit 内一次 rename 干净,避免遗漏导致 build 半坏。需要 grep gravity 整个仓库,确认所有引用都改了(包括 sync / engine / RPC 等模块)。
   - [ ] 冲突解决:未落地:实测旧名 LastSafeBlockBlock 在 crates/storage/provider/src/providers/database/provider.rs 仍有 2 处引用,rename 未执行(2026-07-03 实测)。

- [ ] 5. **`DbTx::commit()` 返回类型 `Result<bool>` vs `Result<()>`**:本文档推荐 take-upstream 的 `Result<()>`,把 commit-触发信号迁到 `commit_view` / 新增 `try_commit` 接口。需要 db-impl 层 owner 确认 RocksDB 后端的 `commit_view` 语义能完全替代 `commit()` 的 bool 信息。
   - [ ] 冲突解决:待 db-impl owner 确认后落地;crates/storage/db-api/src/transaction.rs 现存 2 处冲突块(2026-07-03 实测)。

- [ ] 6. **`HashedPostStateProvider` / `BytecodeReader` 的 `+ Send + Sync` bound**:gravity 保留这个 bound,但 upstream 的 trait method 都是值参数,理论上不要求 `Sync`。如果 `ParallelStateProvider` 内部对 trait object 使用了 `Arc<dyn HashedPostStateProvider>`,则确实需要 `Sync`;如果是 `Box<dyn ... + Send>`,只需要 `Send`。需要 verify gravity 端的 trait object 持有方式,以确认 `Sync` bound 的真实必要性。
   - [ ] 冲突解决:待核实 trait object 持有方式后落地;storage-api 的 trie.rs(4 处)/lib.rs 等仍在冲突清单(2026-07-03 实测)。

- [ ] 7. **`Receipt<T>` 的 `impl_compression_for_compact!` 在 v2.3.0 被移除(#22254)**,因为 upstream 改用 alloy `EthereumReceipt`。gravity 是否要跟进 alloy `EthereumReceipt`?如果不跟进,gravity 必须维护私有 fork 的 `Receipt<T>` compact 实现 — 这条 fork 会越拉越大。短期建议保留 gravity `Receipt<T>` compact,长期评估迁移成本。
   - [ ] 冲突解决:待决策后落地(短期保留 gravity Receipt<T> compact);相关 codecs 文件不在当前冲突清单,属独立评估。
