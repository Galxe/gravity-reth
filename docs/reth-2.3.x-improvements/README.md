# reth 2.3.x vs 1.8.x — gravity 关键优化对比(storage / state-root / cache)

> 对比范围: 上游 reth `branch-1.8.3` (4219741510) → `branch-2.3.0` (9384bc53d8),仓库 `/Users/gx/ws/git/block/reth`。
> gravity-reth 基于 1.8.3,storage/state-root/cache 三块均有自研深度优化(rocksdb 全量后端 / nested-trie / grevm+PersistBlockCache)。
> 本目录三份文档逐一梳理:baseline 之外,上游 2.3.x 还有哪些改进值得**事后选择性 port**,以及哪些因架构冲突/共识风险**不能 port**。

三份文档:
- [`storage.md`](./storage.md) — 存储层(db / db-api / provider / static-file / prune / libmdbx),13 主题。
- [`state-root.md`](./state-root.md) — state root / trie 计算(sparse-trie / proof-v2 / 写入 / cursor),12 主题。
- [`cache.md`](./cache.md) — 缓存与预热(payload processor / execution cache / prewarm / precompile / CachedReads),9 主题。

## 一句话结论

上游 2.3.x 在这三块的**"大件"全部紧耦合 reth 自己的架构**(MDBX 主库 + 原生 RocksDB 辅助 + sparse-trie + 顺序执行器 + engine-tree),而这几件事 gravity 已用**另一套自研体系**(rocksdb 全量后端 + nested-trie + grevm + PersistBlockCache)覆盖——所以主线**不建议整块 port**。真正值得拿的是从中**拆出来的、后端/引擎无关的性能与正确性改进**。

## 推荐 port 清单(高置信、正交、低风险 → 低优先)

| 优先级 | 项 | 领域 | PR | 收益 | 难度 | 改磁盘格式? |
|---|---|---|---|---|---|---|
| ★★★ | **Sorted trie writes**:`TrieUpdatesV2` 写库前按 path 排序,`AccountsTrieV2`/`StoragesTrieV2` 有序 upsert | state-root #6 | `#20784`/`#20047` | MDBX/rocksdb 写放大↓、page split↓;与 16 路 nibble 分区天然契合 | 低 | 否 |
| ★★★ | **KV 编码栈分配**(4 项,字节不变) | storage #7 | `#22089`/`#21200`/`#21279`/`#22314` | 纯分配削减,rocksdb 后端直接受益 | 低 | 否 |
| ★★ | **Receipts/Bodies 强制留 64 块** + 可配置 min prune distance | storage #10 | `#21520`/`#23082` | reorg 正确性 + 运维灵活性,后端无关 | 低 | 否 |
| ★★ | **Proof/root worker 池化 + cursor 复用**(长驻 worker 持 tx+cursor 跨块复用;cacheline / 小块降配) | state-root #4 | `#18901…`/`#23321`/`#22074` | `calculate_and_proof` 每次临时开 cursor → 池化摊薄成本 | 低-中 | 否 |
| ★★ | **static-file offset/underflow bug fix** | storage #4 | `#19819`/`#18628` | 崩溃恢复正确性,正交 | 低 | 否 |
| ★ | **Precompile cache**(⚠️ **必须**为 gravity stateful precompile 加 `#23619` 护栏:`randomness_by_height`/`mint`/`bls_pop_verify` 绝不缓存) | cache #5 | `#20502`/`#20527`/`#23619` | 重复纯计算 precompile 提速(收益需实测) | 低 | 否 |
| ★ | **过滤未变更账户**(`from_bundle_state` 判 `info==original_info`)+ 小块顺序 hash | state-root #10 | `#24432`/`#22660` | 少送 trie 的账户 | 低-中 | 否 |
| ★ | **后台并行算 receipt/tx root**(与 EVM 执行并行,root 哈希藏进执行时延) | state-root #12 | `#21131`/`#24419` | 引擎无关 drop-in;对 pipe-exec 出块/校验路径适用 | 低 | 否 |
| ★ | **`quanta` TSC 计时 + `--db.disable-metrics`** 思想引到 gravity rocksdb metric 侧 | storage #8 | `#22211`/`#24806` | 计时开销↓ | 低 | 否 |
| ★ | **`CachedReads::with_account_capacity`**(仅当走上游 basic payload builder) | cache #6 | `#25048` | 免 hashmap rehash | 极低 | 否 |
| ○ | proof-v2 target/结果类型 + witness 扁平格式**对外 API 对齐**(利于跨客户端 proof/stateless) | state-root #2/#5 | `#22270`/`#22922` | 互操作性 | 中 | 否 |
| ○ | **account key `StoredNibbles` 打包**(变长 pack,storage subkey 已 packed) | state-root #7 | (仿 `#22158` 但变长) | `AccountsTrieV2` key 字节↓ | 中 | **是(需迁移)** |

## 明确不 port(架构冲突 / 共识风险)

- **Storage v2 原生 RocksDB 双后端**(`RocksDBProvider` + `StorageSettings`/`Metadata` + `EitherReader/Writer`)—— gravity 已用 rocksdb **全量**替换 mdbx,两套架构正交(storage #1)。
- **Overlay 子系统 + in-memory trie changesets**(`OverlayBuilder`/`StateTrieOverlayManager`/`ChangesetCache`)—— 与 gravity `NestedStateRoot` **算法不等价,替换会共识分裂**(storage #5 / state-root #8)。
- **Sparse-trie 主引擎 + proof-v2 引擎**(`ArenaParallelSparseTrie`)—— 与 nested-trie **互斥,二选一**(state-root #1/#2/#3)。
- **上游 prewarming / PayloadProcessor / cross-block execution cache**(`reth-execution-cache`)—— gravity 用 **grevm + PersistBlockCache** 以另一套机制覆盖(cache #1/#2/#3/#4)。
- **BAL(EIP-7928)**—— gravity 无消费方,保留代码为孤儿(storage #11 / cache #7 / state-root #12)。

## 落地建议

1. 先做 **★★★ 两项**(sorted trie writes + KV 栈分配)——收益直接、零磁盘格式风险、与自研架构正交。
2. 再评估 **★★ 三项**(留 64 块 / worker 池化 / static-file bug fix)。
3. `#22158` packed nibbles(65→33B 定长)**明确排除**——会破坏 `AccountsTrieV2`/`StoragesTrieV2` 现有 `StoredNibbles` 磁盘格式;若要打包 account key,走变长方案 + 离线迁移(见 state-root #7)。
4. 涉及 on-disk 格式的任何 port(account key 打包)都需迁移 + 回滚预案,单独立项。

> 详细机制、PR/commit hash、涉及文件、与 gravity 现状的逐条对照,见三份子文档。
