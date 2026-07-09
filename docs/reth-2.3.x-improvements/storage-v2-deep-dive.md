# reth v2.3.0 Storage-V2 深度解析 — 改了什么、怎么改、为什么、与 gravity-reth 对比

> 生成日期:2026-07-09。调研基准:上游 reth v2.3.0(`/Users/gx/ws/git/block/reth`,
> commit `9384bc53d8`);gravity 侧为本仓工作树。上游引用为该仓库
> `文件:行号`,gravity 侧引用为本仓路径。
>
> 本文是 [`storage.md`](./storage.md)(13 主题 port 决策速览)的**架构级
> 展开**,聚焦 storage-v2 这条主线:改了哪些、用什么方法、效果多少、
> 为什么这么改、与 gravity 自研存储栈逐维度对比、哪些可借鉴。
> port 结论以 `storage.md` 与
> [`../merge-v2.3.0/upstream-optimizations-gap.md`](../merge-v2.3.0/upstream-optimizations-gap.md) §D
> 为准,本文提供机制细节并更新两处过时口径(见 §8 注记)。

## 0. 一句话总览

storage-v2 是一次由**单个布尔开关**驱动的「热/冷数据重新分层」:

```
                     v1(全在 MDBX)                v2(按负载特征分家)
 receipts            MDBX 表                   →   static file(顺序 append + Lz4)
 tx senders          MDBX 表                   →   static file
 account/storage     MDBX dup-sort 表          →   static file(+ .csoff 偏移边车)
   changesets        (每块随机小写,写放大重灾区)
 TxHashNumbers /     MDBX 表                   →   RocksDB(LSM,3 个 CF)
   Accounts/Storages (随机键高频 upsert)
   History
 PlainState          MDBX 表(与 hashed 冗余)  →   删除(hashed state 升为规范状态)
 其余(headers、     MDBX                          MDBX(保持不变)
   bodies、hashed、
   trie branch 缓存)
```

**官方实测效果**(`docs/vocs/docs/pages/run/faq/storage-v2.mdx`,主网
block 24,396,823):Full 节点 1.46TB→1.02TB(**-30%**),Minimal
449GB→224GB(**-50%**),Archive 2.99TB→2.31TB(**-23%**)。

**状态**:v2.3.0 中 v2 已是**新库默认**(`--storage.v2` 默认 true,
#22890/#22954),同时仍标注 Experimental;已提供 `reth db migrate-v2`
原地迁移工具(#24230)。

**与 gravity 的关系一句话**:上游是「MDBX 为主库,把三类最疼的负载
外科手术式搬出去」;gravity 是「整库搬进 RocksDB + 三实例物理拆分」
(#212,自称 10x/9000 TPS)。两者都在解 MDBX copy-on-write B-tree 的
随机写放大,但目标不同——上游优化**主网节点磁盘体积**,gravity 优化
**高 TPS 链的写吞吐**。详见 §9。

---

## 1. 为什么改:MDBX 单库的三类痛点

1. **随机小写的写放大**。MDBX 是 copy-on-write B-tree:每插一条小 KV,
   沿途页全部拷贝重写。三类负载正中要害——
   - changesets:每块执行产生大量 `(block, address[, slot])` 小条目,
     往 dup-sort B-tree 随机插入;
   - `TransactionHashNumbers`:键是随机 B256,毫无局部性;
   - history 索引:每块 upsert 尾 shard,键随机。
2. **体积冗余**。`PlainAccountState/PlainStorageState` 与
   `HashedAccounts/HashedStorages` 是同一份状态的两种键序;而状态根/
   trie 计算只用 hashed 表,plain 是纯粹的「按 address 直查」副本。
   B-tree 页也没有压缩,冷数据(receipts)存在其中浪费显著。
3. **冷热不分**。receipts/senders/changesets 写后几乎只被顺序范围读
   (RPC 历史查询、unwind),却和热路径数据挤同一棵 B-tree,推高
   compact/拷贝成本。

对应三个解法:冷数据→static file(NippyJar 顺序 append + Lz4/Zstd
压缩 + mmap 读);随机写表→RocksDB(LSM 把随机写变顺序写);冗余
plain state→删除。

---

## 2. 方法一:单开关版本协商(StorageSettings + Metadata)

- **单布尔聚合**:`StorageSettings { storage_v2: bool }`
  (`db-api/src/models/metadata.rs:13-93`),配一组语义查询器
  (`receipts_in_static_files()` / `*_in_rocksdb()` /
  `use_hashed_state()`)把一个开关翻译成各子系统的路由决策。设计
  动机写在注释里:五类数据的搬迁必须**原子**,不允许「receipts 已搬、
  changesets 还在」的半迁移状态。
- **持久化与协商**:settings 以 JSON 存 `Metadata` 表
  (`tables/mod.rs:536`,key=`"storage_settings"`);启动/init_genesis
  时协商(`db-common/src/init.rs:209-253`):**老库缺 metadata 按 v1、
  库内已存 settings 永远优先于 CLI flag**(只 warn 不覆盖)——即
  flag 只对新库生效,杜绝误切换损坏数据。
- **零开销路由**:`StorageSettingsCache` trait + provider 内
  `Arc<RwLock<StorageSettings>>`(`provider.rs:198-199`),热路径判断
  只读内存锁,不碰 DB。
- **迁移工具** `reth db migrate-v2`(`cli/commands/src/db/migrate_v2.rs`)
  七阶段:changesets→SF、receipts→SF、三表→RocksDB、翻 metadata、
  清空可重算表(plain state、senders、旧 trie 表并重置
  SenderRecovery/Merkle checkpoint 让 pipeline 重建)、MDBX compact
  (`MDBX_CP_COMPACT`)、目录热交换。

---

## 3. 方法二:数据搬迁全景(五项)

### 3.1 receipts → static file
路由 `EitherWriter::receipts_destination`(`either_writer.rs:182-196`)。
纯冷数据、顺序范围读,static file 压缩 + mmap 完胜 B-tree。例外:开
receipt pruning 或 `receipts_log_filter` 时留 MDBX(尚不支持带
log-filter 的 SF 写,代码有 TODO)。

### 3.2 transaction senders → static file
每 tx 一行 20 字节 Address,由 SenderRecovery 派生、可重算,典型追加
型冷数据。段类型 `StaticFileSegment::TransactionSenders`。

### 3.3 changesets → static file(写放大重灾区的解法)
- 段 `AccountChangeSets`/`StorageChangeSets`(#18882/#20896):按块
  顺序 append,块内按 address(+slot) `par_sort_by_key` 后写入
  (`either_writer.rs:611-665`)——把 v1 的「每块对 dup B-tree 随机
  插入」变成「排序后顺序追加」。
- **变长段的定位难题**:changeset 段每块行数不定,不能像 tx/block 段
  用「块号 × 固定步长」算行偏移。解法是 **`.csoff` 偏移边车文件**
  (#21596,`static-file/types/src/changeset_offsets.rs`):定长
  16B/块 `[offset:u64][num_changes:u64]`,O(1) 随机定位任意块的行区间。

### 3.4 三张随机写表 → RocksDB
`TransactionHashNumbers` / `AccountsHistory` / `StoragesHistory`
(`rocksdb/provider.rs:293-303`,每表一个 CF)。选这三张的判据:
**键随机 + 高频 upsert + 点查/前缀读**,是 B-tree 写放大最大、LSM 最
擅长的负载;其余冷数据走 SF 而非 RocksDB,因为顺序 append 根本不需要
LSM,SF 压缩比更高。CF 级特化见 §7。

### 3.5 plain state → 删除(hashed 升为规范状态)
#21115:v2 下 `HashedAccounts/HashedStorages` 是唯一权威状态,
`PlainAccountState/PlainStorageState` 不再维护(迁移时直接清空)。
可行性:状态根/trie 路径本就只消费 hashed 表,plain 只是给「按
address 直查」的冗余副本。配套写路径优化 #22294:v2 且各类输出都已
走 SF 时,`write_state` **跳过昂贵的 `to_plain_state_and_reverts`
全量转换**,只写 bytecodes(`provider.rs:2433-2445`)。这是 Minimal
档 -50% 的最大单一来源。

---

## 4. 方法三:统一路由与写去重

- **`EitherWriter`/`EitherReader`**(`provider/src/either_writer.rs`):
  三变体 `Database(MDBX cursor) / StaticFile(writer) / RocksDB(batch)`,
  每类数据一个构造器,读 `cached_storage_settings()` 分发;不适用的
  组合返回 `UnsupportedProvider`。RocksDB 写不立即提交——batch 被
  抽出(`into_raw_rocksdb_batch`)压进 provider 的
  `pending_rocksdb_batches`,统一在 provider `commit()` 时按序提交
  (与 §5 联动)。
- **写去重**:`static_file_write_ctx` 先算出哪些数据 SF 负责
  (`provider.rs:527-553`),**取反**作为
  `StateWriteConfig{write_receipts, write_account_changesets,
  write_storage_changesets}` 传给 `write_state`——SF 已写的 MDBX 不再
  写一份。`WriteStateInput::Single/Multiple` 统一单块/多块入参
  (单块免包装成完整 ExecutionOutcome);`SaveBlocksMode::
  Full/BlocksOnly` 区分「全量写」与「只写块结构、state 后补」;
  `save_blocks` 里 SF 线程与 MDBX 主线程并行。

---

## 5. 方法四:跨存储崩溃一致性(没有分布式事务,怎么保证不坏)

三个存储(MDBX / RocksDB / static file)之间**没有原子事务**,上游用
「提交定序 + 读时夹取 + 启动自愈」三件套替代:

1. **CommitOrder**(`provider.rs:87-96`):
   - **Normal(正向)**:static file → RocksDB → **MDBX 最后**。
     MDBX 的提交定义系统权威 tip;先提交的层如果「抢跑」后崩溃,
     恢复时会被裁回 tip。
   - **Unwind(反向)**:**MDBX 最先** → RocksDB → static file。
     注释原话:先降低 MDBX checkpoint,则崩溃后 static file 只是
     「比 checkpoint 多」,可安全截断;反序则会「数据缺失需重同步」。
2. **visible_tip 读夹取**(`provider.rs:1529-1531`):读 history 时,
   RocksDB 中高于「MDBX 快照可见最高块」的条目**直接忽略**——抢跑
   写等价于没写,读者永远看到一致视图,无需 fsync 顺序保证。
3. **启动自愈** `check_consistency` 四步(`database/mod.rs:501-536` +
   `launch/common.rs:530-592`):SF 文件级 heal(NippyJar + csoff 三方
   对齐截断)→ RocksDB vs MDBX checkpoint 修复(领先则 prune、落后则
   给 unwind target)→ SF checkpoint 级检查 → chain-state 块号修复;
   各层 unwind target 取 min 后跑一条只做 unwind 的 pipeline。

---

## 6. 方法五:static-file 引擎升级

- **段分类扩展**:`is_tx_based`(Receipts/Transactions/Senders)、
  `is_block_based`(Headers)、新增 `is_change_based`(两个 changeset
  段)(`segment.rs:184-215`)。
- **`maybe_heal_segment`**(#20508,`manager.rs:1541-1565`):启动时
  逐段——只读模式仅校验,可写模式取一次 writer 即自动把数据截回最后
  committed config;changeset 段还要 header/NippyJar 行数/csoff 三方
  对齐(处理半条记录、uncommitted 残留)。
- **`StaticFileSegmentIndex`**(#19803,`manager.rs:2206-2235`):把
  「最低未过期范围(history expiry 用)/最高块/预期块范围/按 max-tx
  的可用范围」四种映射统一进单结构 + 一把 RwLock,变长段、expiry、
  一致性检查共用。

---

## 7. 方法六:RocksDB provider 的工程细节

- **引擎选型**:读写用 `OptimisticTransactionDB`(要 read-your-writes
  + 可取消后台任务);只读走 secondary 实例(可 catch-up)。
- **通用 CF**:dynamic level bytes、Lz4/底层 Zstd、write buffer 128MB
  ——注释给了数字:「128MB 相比默认 64MB 把 **p99 延迟方差降 ~80%**,
  均值吞吐几乎不变」(#21696)。共享 block cache 128MB、
  WriteBufferManager 软上限 4GiB、`max_open_files=512`(给 MDBX/SF 留
  fd 余量)。
- **tx-hash CF 特化**(`provider.rs:261-285`,反直觉但讲得通):
  - **禁 bloom filter**——查这张表的 key(块内 tx hash)**一定存在**,
    bloom 只对「查不存在的键」有用,纯浪费内存;
  - **禁压缩**——B256 是不可压缩随机哈希、值是几字节 varint,压缩
    纯烧 CPU。
- **前缀跳读在应用层**:history 迭代按原始字节比较前 20B(address)/
  52B(address+slot),越界即停,免全量 decode(`provider.rs:2139-2312`)。
- **无跨库原子性的替代**:batch 推迟到 provider commit 按 CommitOrder
  定序 + visible_tip + 启动 heal(§5);自动提交阈值 512MiB 防大灌入
  OOM,崩溃留给一致性检查兜底。

---

## 8. 效果、限制与口径更新

**效果**(有依据的量化):磁盘 Full -30% / Minimal -50% / Archive
-23%(官方 FAQ);write buffer 调优 p99 方差 -80%(代码注释);其余
为机制性收益(跳过 plain 转换、changesets 顺序化、tx-hash CF 省
CPU/内存),PR 正文无更多数字。

**限制**(上游自标):Experimental 标注与「新库默认开」并存;FAQ 称
不可回切、格式可能随版本变;receipts 的 log-filter pruning 与 SF 不
兼容(强制留 MDBX);v2 读历史表强依赖 RocksDB snapshot(缺失
panic);一致性检查算出 unwind target==0 时直接 panic。

**对本目录既有文档的两处口径更新**:
1. `storage.md`/gap 文档写作时以为 storage-v2 是可选试验线——实际
   v2.3.0 已是**新库默认**(#22890/#22954);
2. 官方 FAQ 的「只能全新同步、不可迁移」已过时,`reth db migrate-v2`
   (#24230)支持原地 v1→v2。上游对该轨道的投入承诺高于此前判断,
   **v2.4+ 再合并时 storage-v2 大概率是绕不开的默认路径**,Phase 2
   债(见 §10.2)的优先级应相应上调。

---

## 9. 与 gravity-reth 对比:两条 RocksDB 路线谁更好

### 9.1 gravity 存储栈速写(对照基准)

- **全量 RocksDB,三实例物理拆分**(#212/#225):`state_db`(绝大多数
  表)+ `account_db`(仅 `AccountsTrieV2`)+ `storage_db`(仅
  `StoragesTrieV2`),每逻辑表一个 CF(`db/src/implementation/rocksdb/
  mod.rs:250-441`);`--db.sharding-directories` 可把三库放不同磁盘。
  拆分目的:persist 阶段 state 与两棵 trie **并行提交互不锁**
  (`persistence.rs:284-293`、`tx.rs:20-45`)。
- **动机数字**:#212 commit 自述 10M 账户 + ERC-20 高负载下
  **~9,000 TPS vs MDBX ~900 TPS(10x)**。
- **dup-sort 模拟**:RocksDB 无原生 dup 表,用「主 key‖subkey」复合
  键 + `delete_range` 删整簇 + seek 后前缀比较(`tx.rs:148-198`、
  `cursor.rs:101-541`)。
- **事务模型**:`Tx` = 三个 `Arc<Mutex<WriteBatch>>`,**无
  read-your-writes**(cursor 永远读 live DB);`commit_view(&self)`
  借用式冲刷(per-write fsync,#340)让长阶段(merkle 分块/prune)
  在同一 provider 里分批落盘;`DbTx: Send+Sync` 是并行提交模型的
  硬需求(上游 MDBX 事务非 Sync,故上游去掉了该约束)。
- **崩溃一致性**:sync 写 + 阶段级 checkpoint 幂等 +
  `StorageRecoveryHelper` 启动重放(#313:以 Execution checkpoint 为
  锚,重建 hashed→重算 state root 并**与 header 校验**→重建 history
  索引)。
- **static file**:只写 Headers/Transactions/Receipts 三段
  (`writer.rs:54-56` 注释明确);changesets/senders 在主库 RocksDB;
  上游三个新段变体在 gravity **无生产者**(枚举编译在、数据不产生)。
- **状态冗余现状**:plain(执行真相,pipe 经 PersistBlockCache 按
  Address 点读)+ hashed(merklization 输入)+ nested trie 叶内嵌
  `RLP(TrieAccount)`——同一账户**三份**。

### 9.2 逐维度对比

| 维度 | 上游 storage-v2 | gravity | 判定 |
|---|---|---|---|
| 总体策略 | MDBX 为主,外科手术搬走三类最疼负载 | 整库进 LSM + 三实例拆分 | 目标不同:上游求**省盘**(主网节点),gravity 求**写吞吐**(高 TPS) |
| 随机写放大 | 只把三张表进 LSM,changesets 出库进 SF | **全部**表已在 LSM,写放大问题整体缓解 | gravity 更彻底;上游是在「不敢动 MDBX 主库」约束下的次优 |
| 磁盘体积 | -30%~-50%(删 plain + SF 压缩) | 三份状态冗余 + changesets 留在 LSM(compaction 反复搬运) | **上游明显更优** |
| 状态表示 | hashed 单份规范 | plain(执行)+ hashed(merkle)+ trie 叶(内嵌) | 上游省盘;gravity 换来**执行读免 keccak**(pipe 按 Address 点读 plain),对 TPS 目标是合理付费 |
| changesets | SF 顺序 append + csoff,O(1) 定位 | 主库 RocksDB dup 表 | 上游更优:LSM 虽吸收随机写,但 changesets 会被 compaction 反复重写;SF 一次写死 + 压缩 |
| receipts | SF | SF(persistence 只写 SF) | **平手**(gravity 早已如此) |
| tx-hash / history 表 | RocksDB + **CF 级特化**(禁 bloom/禁压缩) | 在 state 库,**所有 CF 用统一 options** | 上游的特化思想更细,直接可借鉴 |
| 跨存储一致性 | CommitOrder 定序 + visible_tip 读夹取 + 启动 heal(免 fsync 顺序依赖) | per-write fsync(#340)+ checkpoint 幂等重放 + state root 校验 | 各自自洽。gravity 持久性更强但每次 commit_view 付 fsync(merge-blocks 摊薄);上游 visible_tip 是零成本读侧技巧,更优雅 |
| read-your-writes | OptimisticTransactionDB 原生支持 | WriteBatch 无 RYW,靠 commit_view 分批冲刷补偿 | 上游省心,gravity 省事务开销;权衡而非优劣 |
| dup-sort | 剩余 dup 表留在 MDBX 原生支持 | 复合键模拟(value 里重复存 path、seek+filter 校验) | 上游省事;gravity 的模拟有真实开销(键膨胀 + 过滤) |
| 版本协商/迁移 | Metadata 表 + 老库优先协商 + migrate-v2 七阶段工具 | **无布局版本语义**(StorageSettings 已随合并出局),格式锁定靠 MIGRATION.md 文档 | 上游工程化更完备;gravity 未来改磁盘格式时会缺这套基建 |
| static file 引擎 | 变长段 + csoff + SegmentIndex + maybe_heal | baseline 引擎(固定 500k 块、无 heal/统一索引) | 上游更新;gravity 差距 = Phase 2 债 |
| 事务语义 | `commit()->Result<()>`、DbTx 去 Sync | `commit()->bool` + `commit_view` + Send+Sync | 各为自己的并发模型服务,不可互换 |

### 9.3 裁定

**没有全域赢家,且两者不在回答同一个问题。**

- 上游 storage-v2 回答的是:「**在必须保住 MDBX 主库稳定性的前提下**,
  怎么把主网节点的磁盘从 1.5TB 压到 1TB」。它的精髓是负载画像驱动的
  分家(随机写→LSM、冷顺序→SF、冗余→删),以及在没有跨库事务的
  条件下用「定序 + 读夹取 + 自愈」拼出一致性。
- gravity 回答的是:「**在确定性共识、pipe 流水线、9000 TPS 目标下**,
  怎么让写路径不成为瓶颈」。全量 LSM + 三实例并行提交 + sync 写 +
  checkpoint 重放,是围绕写吞吐和恢复确定性的设计;为执行读性能保留
  plain state 的冗余是有意付费。
- 若以 gravity 的目标函数评判:gravity 更优——上游 v2 之后 MDBX 主库
  依然承载 hashed state/trie 缓存等热写负载,写吞吐天花板仍是 B-tree;
  gravity 早已越过「把三张表搬进 LSM」这一步。
- 若以磁盘体积评判:上游明显更优——gravity 的三份状态冗余与「changesets
  留在 compaction 循环里」都有真实的空间/写放大代价,只是被 LSM 与
  硬件对冲了。

## 10. 可借鉴清单(按性价比排序)

1. **Per-CF 特化 options**(小、独立、立即可做)。gravity 三个库内
   所有 CF 共用一套 options(`create_db_options`);上游对 tx-hash 的
   「必命中查询禁 bloom、不可压缩键值禁压缩」画像式特化直接适用于
   gravity 的 `TransactionHashNumbers`,同类思路可延伸到各表(如
   trie 表 vs history 表的 block size/压缩分层差异化)。预期收益:
   内存与 CPU,量级需实测。

   > **⟲ 2026-07-09 已落地,且发现并修复一个隐性 bug**:落地时发现
   > gravity 一直用 `DB::open_cf` 打开实例,而 rust-rocksdb 的
   > `open_cf` 给每个命名 CF 的是 `Options::default()`——**所有 CF 级
   > 调优(write buffer 128MB、bloom、Lz4/Zstd 分层压缩、compaction
   > 触发器、4GB block cache)自 #212 起在数据 CF 上从未生效**,数据
   > CF 一直跑 RocksDB 默认值(64MB memtable、Snappy、无 bloom、每 CF
   > 各自 32MB 小缓存)。修复:`create_db_options` 拆分为 DB 级 +
   > `create_cf_options`(CF 级),经 `open_cf_descriptors` 逐 CF 下发;
   > 全部 CF 共享单一 block cache(`--db.block-cache-size` 语义 = 总
   > 预算);`TransactionHashNumbers` 按上游画像特化(禁 bloom/禁压缩)。
   > 内存上界由 DB 级 `db_write_buffer_size=3GB` 兜底。回归测试
   > `cf_options_apply_to_data_column_families` 断言 OPTIONS 文件中
   > 数据 CF 的实际生效参数。对既有数据目录完全兼容(options 为
   > 打开期参数,旧 SST 照常可读,新写入/compaction 渐进采用新参数)。
2. **changesets → static file**(= `STORAGE-RESOLUTION-TODO` 的
   Phase 2 债,§8 口径更新后优先级应上调)。把 changesets 挪出主
   LSM:省 compaction 反复搬运 + Lz4 压缩 + O(1) csoff 定位。前置是
   SF 引擎升级(变长段 + csoff + maybe_heal + SegmentIndex,即第 3
   项),动磁盘布局,需迁移方案,周级工程。
3. **SF 引擎加固**(可独立于第 2 项先做):`maybe_heal_segment` 的
   启动截断自愈对 gravity 现有三段(Headers/Transactions/Receipts)
   同样有价值。

   > **⟲ 2026-07-09 口径更正 + 差量落地**:核实后发现 gravity **已有**
   > 文件级 heal——eth-mode `check_consistency`(:815-820)与 pipe 的
   > `check_consistency_pipe_execution` Phase 1(gravity 自研,含
   > checkpoint 截断 Phase 2)都在启动期经 `latest_writer` 触发
   > NippyJar 截断自愈,上游 #20508 只是把同源逻辑抽成了命名方法。
   > 真实缺口仅一处:pipe 路径 **RO 模式直接 `return Ok(None)` 跳过
   > 一切校验**(上游 RO 模式会跑 `check_segment_consistency` 并在损坏
   > 时报错)。已补:RO 模式下对三个现役段逐段校验,损坏时启动即失败
   > 而非后续静默读到截断数据。变长段/csoff/SegmentIndex 维持不 port
   > (changesets 迁移工程落地前无消费方)。
4. **Metadata 版本协商 + migrate 工具模式**(为未来备)。gravity 无
   磁盘布局版本语义;若未来要做 V2 表 key 打包或 changesets 搬迁,
   照抄「Metadata 表 + 老库优先 + 分阶段迁移 + compact + 目录热交换」
   这套模式,比一次性 breaking change 稳得多。
5. **visible_tip 读夹取思想**(评估级)。gravity 三实例间靠 sync 写
   顺序 + 重放保证一致;若未来想放松 per-write fsync(降延迟),上游
   「最后提交层定义 tip、读侧夹取、启动裁齐」是现成的替代一致性模型。
6. **plain state 冗余重估**(谨慎,不建议轻动)。上游证明了
   hashed-as-canonical 可行,但 gravity 的 pipe 热路径按 Address 点读
   plain(PersistBlockCache 的 key 也是 Address),去 plain 意味着
   执行读改走 keccak(address) 或重构缓存键——省盘换执行读性能,与
   gravity 的目标函数相悖。仅当磁盘成本成为真实痛点时再评估。

**明确不借鉴**:`StorageSettings`/`EitherWriter` 双轨机器本身(gravity
无 v1/v2 双布局语义,f89d9d4e23 已裁决);provider 层 RocksDB(gravity
全库已是 RocksDB,收益重复);`WriteStateInput`/`StateWriteConfig`
入参重构(gravity 写面是 `UnifiedStorageWriter`,语义不对齐)。

---

> 相关文档:[`storage.md`](./storage.md)(13 主题 port 决策)、
> [`state-root-deep-dive.md`](./state-root-deep-dive.md) §5(nested
> trie 对比,其中「磁盘模型/写路径」维度与本文 §9 互补)、
> [`../merge-v2.3.0/STORAGE-RESOLUTION-TODO.md`](../merge-v2.3.0/STORAGE-RESOLUTION-TODO.md)
> (Phase 2 债的执行路径)。
