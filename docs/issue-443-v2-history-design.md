# Issue #443: V2 历史状态根与证明的修复设计

## 结论与边界

目标是让 `debug_intermediateRoots`、`debug_stateRootWithUpdates` 和
`eth_getProof` 基于同一个完整区块的 V2 trie、hashed state 与 changeset 计算，
并完成 V2 multiproof。建议采用**按需创建三库快照、只在建快照时协调完整区块写入**：
每个逻辑块（或合并组）的写入协调者持独占锁，state 与 trie 写线程仍照常并行；
读者短暂持共享锁取得三个 RocksDB snapshot，随后释放锁，在固定视图上执行耗时计算。
不合并数据库、不改四阶段提交，也不为每块无条件保留三份快照。

本文最初基于本地 `41e1c20c66` 制定方案；下文“现状”表格描述的是修复前的代码。
`--storage.v2` 只决定 changeset 存在 static files 还是 RocksDB；两种布局都使用 V2 trie，
不能把该开关当作 trie 版本判断（[配置](../crates/node/core/src/args/storage.rs)）。

## 实施状态

现已接通三库 RO snapshot、完整写入范围协调、V2 root/multiproof，以及三个目标 RPC 的
V2 调用。历史查询按固定快照高度读取有界 changeset；static-file 布局在物化期间持读租约，
并校验固定高度的 canonical header。内存块按顺序叠加 hashed state；
`debug_stateRootWithUpdates` 返回显式版本为 2 的更新集。历史与内存回退按 provider 缓存，
避免逐交易 root 重复扫描同一段历史。

历史 `eth_getProof` 在同一数据库快照中核对目标 canonical hash 与 header，并将 V2 proof
的计算根与 header `stateRoot` 比较；内存目标块也校验其 header root。本地交易池构造的
`pending` header 使用占位根，因此仅对计算根自验证。写入失败后新的 snapshot/live 读事务
都会拒绝建立，直到恢复完成。

stage checkpoint 暂时不齐（包括崩溃后 Merkle trie 超前、等待同块重放）时，普通 RO
读取仍可进行，但 V2 root/proof 会拒绝混合高度。`debug_executionWitness` 仍使用旧 witness
路径，不属于本次三个 RPC 的修复范围。下述性能门槛还需要在实际节点负载下测量。

## 补充调用链复核

| 调用点 | 当前处理与边界 |
| --- | --- |
| `eth_simulateV1` | 开启 `rpc_compute_state_root_for_eth_simulate` 时，模拟区块构建器现在取 V2 root；仅用于满足旧 `BlockBuilder::finish` 返回类型的空 `TrieUpdates` 不落库。关闭该开关时沿用原有不计算 root 行为。连续模拟区块复用累计 bundle state。 |
| `debug_executionWitness`、`debug_executionWitnessByBlockHash` | 两者共用入口，生成旧 witness 前比较同一父状态的旧/V2 root。旧 trie 落后时返回明确不支持错误，避免输出无法验证的 witness；完整 V2 witness 仍未实现。RESS witness 和 invalid-block witness 也使用相同的根一致性保护。 |
| engine/execution-cache 与 engine/tree 的三个 state provider 包装 | 补齐全部五个 V2 方法委托，否则包装对象会落到 trait 默认的 `UnsupportedProvider`。 |
| engine tree 持久化 | 保持原有 V2 updates 的持久化与合并/逐块分流逻辑，不在持久化阶段新增 root 重算或 header 校验。默认 pipe-exec 提供 `TrieUpdatesV2`；非 pipe 或本地 payload 若只提供旧更新，仍需单独完成 V2 迁移，不能认为本次 RPC 修复覆盖了该路径。 |
| 远程 `rpc-provider` | 旧 `state_root_with_updates -> TrieUpdates` 已不能解码版本为 2 的 RPC 返回，现明确返回不支持；`state_root_v2` 读取并校验版本化 RPC root。远程 V2 更新集、proof 等能力仍不支持。 |

默认 Gravity pipe-exec 不经过旧 trie 的 engine validator。线上已不使用
`--gravity.disable-pipe-execution`；该非默认路径的 `payload_validator.rs` 仍依赖旧
`TrieUpdates`、内存祖先和分叉 trie 输入，不纳入本次线上验收范围。
`BasicBlockBuilder::finish` 的通用默认 root 仍使用旧接口；只有上述模拟 RPC 做了局部适配。

`crates/optimism/{rpc,consensus}` 中还有旧 storage-root 调用，但这些目录目前没有
`Cargo.toml`，不在当前 workspace 构建路径。`AlloyRethStateProvider` 等远程 provider 的
V2 能力仍为显式不支持，不应与本地 Gravity 节点的 V2 支持混为一谈。

历史 `debug_intermediateRoots` 虽缓存 changeset 扫描结果，每笔交易仍会复制回退状态并
重算 V2 root；对距持久化高度很远、交易很多的区块，需要以实际负载测量内存和延迟。

## 修复前代码的确切问题

| 环节 | 现状与后果 |
| --- | --- |
| 持久化 | [`save_blocks_per_block`](../crates/engine/tree/src/persistence.rs) 的 state 线程先后提交状态、hashed state、history，trie 线程同时提交 V2 account/storage trie；两个线程 `join` 后才调用 `persist_tip`。合并组也有中途 `commit_view` 和最终提交。完整区块不是一个 RocksDB 原子事务。 |
| 读取 | [`DatabaseEnv::tx()`](../crates/storage/db/src/implementation/rocksdb/mod.rs) 只克隆三个 DB 句柄；[`Tx::get`](../crates/storage/db/src/implementation/rocksdb/tx.rs) 和 [`Cursor`](../crates/storage/db/src/implementation/rocksdb/cursor.rs) 逐次读取实时数据。一个 provider 的多次读取可能跨过多个提交。`ConsistentProvider` 关于 DB snapshot 的注释目前不成立。 |
| 旧 trie 路径 | [`HistoricalStateProviderRef`](../crates/storage/provider/src/providers/state/historical.rs) 与 [`LatestStateProviderRef`](../crates/storage/provider/src/providers/state/latest.rs) 的 root/proof 调用旧 `StateRoot`/`Proof`；[`MemoryOverlayStateProviderRef`](../crates/chain-state/src/memory_overlay.rs) 叠加旧 `TrieInput`。Gravity 执行侧的旧 trie 更新为空，写入时旧 writer 会对空更新直接返回；持久化的是 `TrieUpdatesV2`。因此只修快照不能修这些 RPC。 |
| 历史回退 | `revert_state()`/`revert_storage()` 调用 [`HashedPostState::from_reverts`](../crates/trie/db/src/state.rs)，从 `X+1` 起无上界扫描 DB changeset。static-file 布局下普通历史账户/存储点查已按布局路由，整段 root/proof 回退却没有，因而会漏回退。 |
| V2 proof | [`NestedStateRoot::calculate()`](../crates/trie/db/src/nested_hash.rs) 可算根；`multiproof()` 在非空 target 的 storage/account 分支仍有两个 `todo!`。现有 [`Trie::get_proof()`](../crates/trie/common/src/nested_trie/trie.rs) 可提供路径节点，但没有调用与充分测试。 |
| storage-only | `NestedStateRoot::calculate()` 只迭代 `HashedPostState.accounts`，这是现有区块根计算语义，不能为了 RPC 改动。历史回退可能只有 `storages`，需在历史 provider 构造回退输入时补全账户，使 root 和 proof 收到同一份状态。 |
| RPC 类型 | `debug_stateRootWithUpdates` 返回旧 `(B256, TrieUpdates)`；V2 的 `TrieUpdatesV2` 节点类型不同，且未实现 serde，不能原样替换泛型或强行转换成旧更新。 |

Issue #443 **已观测**的是同步时 `debug_intermediateRoots` 偶发与 header root 不同、稍后重算正确；
其他 RPC 的错误属于同源代码路径推断，不把它们写成已复现事实。
`eth_getProof` 的默认 proof window 为 0，通常仅开放链头请求；放宽窗口后历史路径仍需正确。

## 必须维持的不变量

1. 一次 V2 计算有固定基准高度 `H` 和 canonical hash：同一时刻的 `state_db`、`account_db`、`storage_db`，且 `H` 对应的 state、hashed state、history 与 V2 trie 均已写完。
2. 对历史目标块 `X <= H`，只取 `X+1..=H` 的 **before** values。账户或槽若多次修改，取最早一次修改前的值；然后将本次调用附加的 `HashedPostState` 覆盖其上。目标块为内存块时，先回到内存链 anchor，再按 anchor 到目标的顺序叠加内存 hashed state。
3. V2 算法读取的 trie、hashed account/slot、changeset 和 canonical header 对应同一视图。历史 V2 读取不使用可能已前进到 `H` 之后的 `PERSIST_BLOCK_CACHE`。
4. 返回的每条 account/storage proof 必须能对所选目标块的 header `stateRoot`（或显式附加更新后的 root）独立验真；发现不一致应报错，不能返回一个看似有效的 root/proof。
5. 生产 `NestedStateRoot::calculate()` 的账户遍历、根计算与 trie updates 语义保持不变；storage-only 补全不得进入 `NestedStateRoot`。

## 一致读取方案的选择

| 方案 | 对写入和查询的影响 | 判断 |
| --- | --- | --- |
| 单库单 `WriteBatch` | 改三库分片和并行写入架构，影响最大 | 不采用。 |
| 只比对前后持久化高度 | 可能在块内多个提交之间读到同一高度；在持续同步时重试还可能饥饿 | 不满足不变量。 |
| 每块发布 `Arc<三快照>` | 写入中可立即读上一个完整块，但每块都建快照、常驻 pin RocksDB version；启动、回滚、剪枝需重新发布 | 若实测按需方案的 RPC 等待不可接受，再考虑。 |
| 按需快照 + 块级短读锁 | 只有发生读取才建快照；写方只在块边界与建快照竞争，state/trie 写线程仍并行；读者至多等待当前逻辑块/组完成 | **推荐**。控制流少，写入热路径不增加持久化或逐块 snapshot 成本。 |

共享锁不是“整次 RPC 读锁”：root/proof 可能运行一秒以上，锁仅覆盖三个快照创建及视图元信息读取。
若合并组很大，等待上界是一个合并组而非一个区块，应对该模式单独测 RPC 尾延迟。

### 协调边界

- 将协调器绑定**同一个 `DatabaseEnv`**，不要只放在某个 `ProviderFactory` 实例，否则另一个 factory 或 backfill 可绕过。写方提供一个范围守卫；非 RocksDB 数据库可沿用原行为。范围守卫由外层线程持有，允许其内部的 state/trie 子线程并行 `commit()`。审计守卫内部的只读建 Tx 调用，改为利用已有 `Tx<RW>` 读取，避免同线程重入读锁造成死锁。
- 逐块路径：从本块任何可见提交之前取得独占守卫，到两个线程 `join` 且所有提交成功后释放；失败时守卫仍由 RAII 释放，但**不能**向读者暴露半写区块，应使服务停止读或先完成恢复。合并路径包围整个 `commit_block_group`，包括中途 `commit_view`。`on_save_blocks` 最后单独更新 pipeline/finalized/safe checkpoint 的提交也需要协调。
- 回滚、trie unwind、pruner、启动恢复、运行时 backfill/pipeline 和其它可改 state/trie/history 的 writer 均需列入写侧审计。不能依据 [`backfill.rs`](../crates/engine/tree/src/backfill.rs) 的“DB write lock”注释假定 RocksDB 已实现该锁。正常 forward append 保持既有线程并发；backfill 若无法按完整区块/组划出短范围，就在它运行时明确暂停历史 root/proof RPC，而不是让读者长期等待一个跨阶段写锁或返回混合视图。
- `DatabaseEnv::tx()` 在共享锁内取得三个 snapshot 与固定 `H/hash`，随后归还只读 `Tx`；RPC 计算不再持锁。`H` 从同一视图内完成的 stage checkpoints / canonical 元数据确定，并检查 Execution、AccountHashing、IndexAccountHistory、MerkleExecute 的进度与 hash 连续性；不要用单调递增且回滚不更新的 `PERSIST_BLOCK_CACHE.persist_tip` 当视图版本，也不要用合并实时 static-file tip 的 `last_block_number()` 推断 `H`。启动时必须完成恢复后才允许建立该视图。
- 执行侧 [`BlockViewStorage`](../crates/gravity-storage/src/block_view_storage/mod.rs) 保持原来的 cache + live DB 读取：通过独立的 `database_provider_live_ro()` / `tx_live()` 建立普通只读事务。不能把固定 DB 快照与可变化、可驱逐的执行 cache 混用，否则计算期间驱逐节点后可能回落到更旧的快照。历史 RPC 仍通过普通 `database_provider_ro()` 取得固定快照，且不使用执行 cache。
- [`ConsistentProvider`](../crates/storage/provider/src/providers/consistent.rs) 先固定内存 head，再建 DB 快照的顺序保留；校验内存 anchor 的 hash 能在 DB 视图上找到且 canonical。若回滚使 anchor 失效，重取 head/view 或返回明确的 canonical 错误。检查 [`BlockchainProvider::latest()`](../crates/storage/provider/src/providers/blockchain_provider.rs) 与 `block_state_provider()` 直达路径，确保它们也取得相同语义的只读 Tx。

### RocksDB `Tx<RO>` 的局部实现

在 `rocksdb/{mod,tx,cursor}.rs` 内封装 `Arc<DB>` + snapshot 的 RAII 持有者，三个库分别一份，关联 `Tx<RO>` 的整个生命周期；**游标也持有对应 snapshot 的 `Arc`**，因为当前关联游标类型不借用 Tx，可以比 Tx 活得更久。RocksDB 0.24 的 `SnapshotWithThreadMode<'a, DB>` 借用 DB，而现有 `DbTx` 和游标要求 `'static`；先评估能否用安全的自引用封装，若必须采用与当前游标相似的受控 lifetime 扩展，需明确字段销毁顺序与安全注释：**迭代器先销毁，snapshot 后销毁，DB 最后销毁**。不要让 `ReadOptions` 或迭代器引用已释放的 snapshot。

把同一 snapshot 传给 `Tx::get/get_by_encoded_key`、DupSort 前缀查询临时迭代器、普通/dup 游标迭代器、`Cursor::point_get`；漏一个读入口就可能重新混读。`Tx<RW>` 保留现有 live/read-write 行为。对于多个逻辑 shard 指向同一 DB 路径的配置，三个句柄虽可别名，建快照仍在同一短临界区。只读 Tx 需暴露其固定 `H/hash`，或由持有它的 provider 保留此元信息，避免后续从 live static files 重新推断。

### static files 与快照的边界

RocksDB snapshot **不能**固定 static files。对 static-file changeset 布局，V2 历史计算只读取 `X+1..=H`，并在昂贵的 trie 运算开始前一次性物化本次所需 changeset 及目标 header；用快照中的 canonical hash 校验该 header。普通前进追加不会改写该闭区间；回滚可截断 rows/`.csoff` 和 header，pruner 可删整段 jar，必须分别受一个短期的 static-file 读租约约束。该租约只持续到物化结束；回滚/删 jar 取得排他租约，常规前进写入不必等待整个 proof 计算。还需测试当前 static-file reader 在并发追加时能否稳定读取旧区间；若不能，应修 reader 的追加可见性约束。

锁顺序固定为“块级共享锁 -> static-file 读租约 -> 建快照 -> 释放块级锁 -> 物化 -> 释放租约”；破坏性写入按相同顺序取得独占锁。若历史 provider 已建快照、稍后才首次计算 root，应记录 static-file 代际；代际变化时重建 provider/重放该 RPC，或明确返回视图过期/已剪枝，绝不在旧 RocksDB snapshot 上拼接截断后的 static files。一次 `debug_intermediateRoots` 应只物化/哈希历史回退一次，并复用到每笔交易的根计算。

## V2 根与 proof 的实现

1. 在 [`HistoricalStateProviderRef`](../crates/storage/provider/src/providers/state/historical.rs) 用已有 `ChangesetRangeReader`（其 [`DatabaseProvider` 实现](../crates/storage/provider/src/providers/database/provider.rs) 已按布局路由）读取 `X+1..=H`，构造回退 `HashedPostState`。保持最早 before 值、账户删除、slot 归零、storage wipe/recreate 语义；`X == H` 用空回退。`revert_storage()` 也按同一规则修复。不要复用 `NestedStateRoot::read_hashed_state`：它读取的是**当前** hashed 值，用于恢复当前 trie，不是历史 before 值。
2. 保持 [`NestedStateRoot::calculate()`](../crates/trie/db/src/nested_hash.rs) 的原有语义：只遍历 `accounts`，storage-only 输入不会改变生产根或 updates。历史 RPC 构造回退状态时，为 storage changeset 中缺少 account changeset 的地址从同一固定快照的 `HashedAccounts` 补入基准账户；随后 root 和 proof 使用同一份完整输入。此处不使用执行侧 live cache。
3. 计算历史根用 `NestedStateRoot::new(tx, None)`，从高度 `H` 的 V2 trie 施加已补全的回退与请求 overlay；`root()` 复用 `calculate()`，避免另起一套根计算语义。`multiproof()` 只增加证明收集，不扩展输入状态；proof 模式不生成更新集。为目标账户即使未被修改也取 account 路径；为其目标槽在**更新后的** storage trie 上取证明，wiped storage 使用已有空 reader 后再取证明。不存在账户/slot、空 trie 均需形成可验证的不包含证明。
4. 扩展并测试 [`Trie::get_proof`](../crates/trie/common/src/nested_trie/trie.rs) 使它能保留每个节点的 trie 路径，从而填充现有 `ProofNodes`/`MultiProof`；去掉 `HashNode` 缺节点时的 `unwrap()`，改为数据库错误。底层 `MultiProofTargets` 只有 hashed key，不能直接构造带原始 `Address` 的 `AccountProof`；建议将现有 V2 `multiproof()` 改为返回 `MultiProof`，在持有原始地址/slot 的 provider 中调用 `account_proof()` 组装，再沿用 EIP-1186 wire 格式。不要改用 [`proof_v2::ProofCalculator`](../crates/trie/trie/src/proof_v2/mod.rs)：它读取旧 `BranchNodeCompact`/hashed-leaf 结构，不是 nested V2 节点。
5. 内存目标块在 [`MemoryOverlayStateProviderRef`](../crates/chain-state/src/memory_overlay.rs) 上，从固定 DB anchor 按区块顺序叠加 `block.hashed_state`，然后交给同一 V2 算法。现有 `trie_input()` 聚合的是旧 `block.trie`，不能作为 V2 节点覆盖；也不能直接用全局 cache（可能含目标块之后的节点）。先用正确的 hashed overlay 实现，性能测试若显示重复 trie 更新成本过高，再增加按目标高度有界的 V2 节点 overlay，避免在首版引入第二套 cache 语义。

## RPC 与接口改动

保留现有生产执行依赖的 `StateRootProvider::state_root_with_updates -> TrieUpdates` 和旧 `TrieInput` 方法，避免一次修改触及 payload validation、执行缓存、witness hooks 与大量测试替身。在 [`storage-api/src/trie.rs`](../crates/storage/storage-api/src/trie.rs) 现有 object-safe trait 上增加**少量显式 V2 能力**（例如 `state_root_v2`、`state_root_with_updates_v2`、`proof_v2`、`storage_root_v2`，默认返回 `UnsupportedProvider`），由 latest、historical、memory-overlay 和必要的包装 provider 委托实现。RPC 明确调用 V2 方法，不能把旧方法误认为已迁移；后续可在生产执行不再依赖旧返回类型时统一 trait。`eth_getProof` 的请求与 EIP-1186 响应无需变化。

`debug_stateRootWithUpdates` 的**方法名和输入不变**，结果需要版本化的 wire 类型，例如 `(root, {version: 2, accountNodes, removedNodes, storageTries})`，节点以 V2 持久化编码 bytes 表示，并用稳定的 nibble-path 字符串键；明确这不是旧 `TrieUpdates` 的 `BranchNodeCompact` JSON。它是非标准 debug RPC，但返回值的版本变化仍须写进 RPC 文档/变更说明，不能返回空旧更新以伪装兼容。转换放在 RPC types 层，不给含内部缓存的 `Node` 直接派生 serde；核对编码可 round-trip 为 V2 节点。

需要同时逐一处理下列 RPC 调用点，避免留下另一条静默旧路径：[`debug.rs`](../crates/rpc/rpc/src/debug.rs) 的 `intermediate_roots`、`state_root_with_updates`、`account_at`；[`helpers/state.rs`](../crates/rpc/rpc-eth-api/src/helpers/state.rs) 的 `get_proof`、`get_account`；[`validation.rs`](../crates/rpc/rpc/src/validation.rs) 的 RPC 区块状态根校验，以及 [`cache/db.rs`](../crates/rpc/rpc-eth-types/src/cache/db.rs) 的委托。`debug_executionWitness` 仍走旧 witness 算法：它不在三项 RPC 的最小修复内，但上线前必须明确选择 V2 witness 实现或对该方法返回不支持错误，不能声称所有 proof/witness RPC 都已迁移。

### 文件级改动清单

| 位置 | 必要改动 |
| --- | --- |
| [`storage/db-api/src/database.rs`](../crates/storage/db-api/src/database.rs)、[`storage/db/src/implementation/rocksdb`](../crates/storage/db/src/implementation/rocksdb/mod.rs) | 加最小写范围协调入口；只读 Tx 和游标共用三份快照、完整高度/hash，覆盖所有读法并测试生命周期。 |
| [`engine/tree/src/persistence.rs`](../crates/engine/tree/src/persistence.rs)、[`recovery.rs`](../crates/engine/tree/src/recovery.rs)、[`backfill.rs`](../crates/engine/tree/src/backfill.rs) | 标出完整块/组、回滚与恢复的独占范围；处理失败后不可读状态；确认 pipeline 写入的可用性策略。 |
| [`storage/provider/src/providers/database/provider.rs`](../crates/storage/provider/src/providers/database/provider.rs)、[`static_file`](../crates/storage/provider/src/providers/static_file/manager.rs)、[`prune`](../crates/prune/prune/src/segments/user/account_history.rs) | 复用按布局路由的 `ChangesetRangeReader`；为 static-file 截断/删 jar 增加短期保留租约与代际检查，storage history 对称处理。 |
| [`storage/provider/src/providers/consistent.rs`](../crates/storage/provider/src/providers/consistent.rs)、[`blockchain_provider.rs`](../crates/storage/provider/src/providers/blockchain_provider.rs) | 固定内存 head/DB anchor 的顺序，校验 hash 连续性，避免 `latest()` 或直达 block-state 路径绕开一致视图。 |
| [`trie/db/src/nested_hash.rs`](../crates/trie/db/src/nested_hash.rs)、[`trie/common/src/nested_trie/trie.rs`](../crates/trie/common/src/nested_trie/trie.rs) | 保持原有根计算与输入语义；multiproof 只收集 V2 proof 节点，消除缺节点 panic。历史 provider 负责补齐 storage-only 回退输入。 |
| [`storage/storage-api/src/trie.rs`](../crates/storage/storage-api/src/trie.rs)、[`provider/state`](../crates/storage/provider/src/providers/state/historical.rs)、[`chain-state/src/memory_overlay.rs`](../crates/chain-state/src/memory_overlay.rs) | 增加窄 V2 能力及委托；latest/historical/memory 三种 provider 实现固定高度的回退、overlay、root 和 proof；同步更新 provider 委托宏及测试替身。 |
| [`rpc/rpc/src/debug.rs`](../crates/rpc/rpc/src/debug.rs)、[`rpc/rpc-eth-api/src/helpers/state.rs`](../crates/rpc/rpc-eth-api/src/helpers/state.rs)、[`rpc/rpc-api/src/debug.rs`](../crates/rpc/rpc-api/src/debug.rs) | RPC 调用 V2 能力，debug 更新集使用显式 V2 wire 类型；检查相邻 root/proof RPC 与 validation、RPC cache 委托。 |

这些是设计层面的预计修改范围；实现时可把相邻文件合并成少量提交，但不能省略写侧协调和 static-file 回退后只合入 RPC 替换。

## 建议修改顺序与验收

1. **读隔离**：`storage/db-api` 加最小协调入口（供泛型 `N::DB` 使用，非 RocksDB 默认空实现）；`storage/db/rocksdb` 实现范围守卫、三 snapshot Tx 与所有点查/游标的 snapshot 路由；`engine/tree/persistence.rs`、恢复、回滚、pruner、backfill 写入口纳入协调。先做故障注入测试：暂停四个提交点，读视图只能落在完整区块；Tx 建立后继续写多个块，点查、普通/dup 游标均保持旧值。
2. **历史回退和 V2 算法**：`storage/provider/state/{historical,latest}.rs`、`trie/db/nested_hash.rs`、`trie/common/nested_trie/trie.rs`；分别测试 DB 与 static-file changeset 布局、`X == H`、多次修改取首个 before、storage-only、删除、wipe+recreate、空树、多目标及跨 16 分区。每条 account/storage proof 都调用 `AccountProof::verify(expected_root)`；根与对应 header 对比。
3. **内存与 RPC**：`chain-state/memory_overlay.rs`、`rpc/rpc`、`rpc/rpc-eth-api`、`rpc/rpc-api` 及包装委托；测试 anchor 刚持久化、已持久化但仍在内存、持续写入时 target 横跨内存/DB 边界、回滚及剪枝。三项 RPC 的同一输入重复请求应稳定；逐交易根与独立逐交易执行结果比较，仅在没有交易后状态变更时才将最后一项直接与 block header 比较。
4. **性能门槛**：对逐块与合并组分别记录三快照创建耗时、写方等待共享锁、RPC 等待完整块、快照存活时长/并发数、RocksDB pinned version 与 compaction/磁盘增长；比较修复前后的区块持久化吞吐和 root/proof p50/p99。只允许快照建立阶段与写方互等，不允许持锁遍历 changeset 或计算 trie。若合并组导致不可接受的 RPC p99，再评估完成块后发布快照，而不是牺牲现有并发写入。

关键验收不是“RPC 不 panic”，而是：**并发持久化时每次使用可解释的完整高度 `H`，历史 root 与 header 一致，EIP-1186 proof 对该 root 验证通过，且 state/trie 写线程仍并行。**
