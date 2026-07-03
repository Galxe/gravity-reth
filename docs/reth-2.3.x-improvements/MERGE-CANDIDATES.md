# reth 2.3.x 优化 — 可合并候选与落地状态

> 对比范围: 上游 reth `branch-1.8.3` (4219741510) → `branch-2.3.0` (9384bc53d8)。
> 目的: 在 keep-gravity baseline 之上,把 reth 2.3 里**真正干净、可用、非磁盘格式破坏**的优化选择性合并进来。
> 三份机制分析见同目录 `storage.md` / `state-root.md` / `cache.md`;本文是**可落地候选**的收敛清单。

## ✅ 本轮已合并(byte-identical / write-order-only,磁盘格式不变)

| 项 | PR | 文件 | 说明 | 风险 |
|---|---|---|---|---|
| **Sorted trie writes(V2 路径)** | 借鉴 `#20784`/`#20047` | `storage/provider/src/providers/database/provider.rs` `write_trie_updatesv2` | `TrieUpdatesV2` 原本按 HashMap 随机序写 `AccountsTrieV2`/`StoragesTrieV2` → 改为写前按 path 排序(account 按 `Nibbles`;storage 按 `(hashed_address)` 再按 `(len, nibbles)` = 磁盘 dup 序),cursor 近似前向移动、page split↓。**与同文件里 legacy `write_trie_updates` 已有的 sorted-merge 完全一致**。 | 低:写序改变,**写入的节点集合与 state root 不变**;无磁盘格式变更 |
| **ShardedKey/StorageShardedKey 栈分配** | `#21200` `5ef200eaad` | `storage/db-api/src/models/{sharded_key,storage_sharded_key}.rs` | `ShardedKey<Address>`→`[u8;28]`、`StorageShardedKey`→`[u8;60]`,去掉每 key 一次堆分配(gravity RocksDB `AccountsHistory`/`StoragesHistory` **每块**写路径)。泛型 `impl<T>` 特化为 `Address`(已核实其它 `ShardedKey<T>` 仅 `AsRef`,`ShardedKey<B256>` 从不直接 encode)。 | 低:字节 `[20][8]`/`[20][32][8]` 与旧 `Vec` **完全一致**;无磁盘格式变更 |

## ✅ 已交付(前几轮 / 随 merge 结构自然获得)

| 项 | PR | 交付方式 |
|---|---|---|
| Static-file 两阶段 commit + offset/header underflow healing | `#20984`/`#19819`/`#18628` | 引擎 crate(`nippy-jar`/`static-file/types`)已是上游 2.3.0;provider 已桥接(见 `STORAGE-RESOLUTION-TODO.md`)——provider 调 `commit()`/checker 即得 |
| Prune:Receipts/Bodies 强制留 64 块 + 可配置最小裁剪距离 | `#21520`/`#23082` | `crates/prune/types` 已是上游;`prune_target_block(tip,segment,purpose)` 公共签名与 baseline 一致,retain-64/min-distance 在其内部强制执行 → baseline `prune/prune` 调用不变即得 |
| BranchNodeCompact bulk memcpy | `#22089` `fc6666f6a7` | **非手工 port**:上游删了本地 `crates/storage/codecs`、改用 crates.io `reth-codecs 0.4.1`(gravity merge 分支已指向它),该 PR 是 0.4.1 里 `BranchNodeCompact::to_compact` 的唯一改动,字节不变 → 随 codecs 外部化自然获得 |

## ⏸ 建议但需决策 / 需人工(未合,含具体方法)

| 项 | PR/来源 | 位置 | 为什么没直接合 | 落地方法 |
|---|---|---|---|---|
| **过滤未变更账户** | `#24432` 思想 | `trie/common/src/hashed_state.rs` `from_bundle_state`(rayon `:48` / 非-rayon `:79`) | 共享代码(RPC/stateless/historical/pipe 都用),state root 只在谓词**精确**时不变;blast radius 大,需回归验证 | `map`→`filter_map`,谓词 `account.info == account.original_info && !account.status.was_destroyed() && account.storage.values().all(\|s\| s.present_value == s.previous_or_original_value)` → `None`。先在 mainnet-replay-pipe e2e 上验 root 逐块一致再放开(或只给 pipe caller 用独立变体) |
| **Worker cursor/tx 复用** | `#18901…`/`#23321` 思想 | `trie/db/src/nested_hash.rs:281` `NestedStateRoot::calculate_and_proof` | 每 account 开新 `StoragesTrieV2` cursor;但内部 16 路并行需 per-thread 独立 cursor,单 cursor 复用不组合 → 需上游式 `thread_local!` tx+cursor + 持久 rayon pool(架构改动) | 引入 `NestedTrieWorkerPool`(按首 nibble 分桶,每 worker 开一次 cursor、per-account re-seek,`DashMap<B256,B256>` storage-root 缓存);先落地上面两项、量测后再做 |
| **后台并行算 receipt/tx root** | `#21131` 思想 | `pipe-exec-layer-ext-v2/execute/src/lib.rs:1370` `calculate_roots`(在 `:708` 调,**先于** `:711` state_root) | 目前 tx/receipt root 在关键路径同步算,未与(通常更重的)state root 重叠 | 执行后(`:668` 后)把 tx-root+receipt-root+logs_bloom spawn 到后台 rayon,`state_root` 返回后再 join(seal 前 `:740`)——把 receipt-root 时延藏进 state-root 里。引擎侧、无格式变更、root 不变;先量测 receipt-root 占比 |
| **Precompile cache**(⚠️ 共识关键) | `#20502`/`#20527` + `#23619` | `engine/tree/src/tree/precompile_cache.rs`;wire 点在 grevm `scheduler.rs:547` | (1) 阻塞于 merge 树 **revm 29→40 分裂**(gravity precompile 用旧 `PrecompileOutput` 形状,`precompile_cache` 用 revm 40);(2) 可缓存 precompile 在 grevm crate 内构建,gravity 只控 custom precompile(恰恰是**不能**缓存的);(3) **`#23619` 护栏对 gravity 不足** | 见下方"⚠️ 安全"。方法:Option B 改 grevm fork(在 `from_static(Precompiles::new(spec))` 后、apply custom 前只 wrap 标准 `0x01–0x11`);cache map 存 `Core`,跨块持久。**先量 hit-rate 再做** |

## ⛔ 明确不合

| 项 | 原因 |
|---|---|
| **Packed nibbles `#22158` / `StoredNibbles` ArrayVec `#21279`/`#22314`** | 破坏 gravity `#149` `AccountsTrieV2`/`StoragesTrieV2` 磁盘 key 字节(链上数据不可读)。`nibbles.rs` 保 gravity HEAD 变长 `pack()` 版 |
| **quanta TSC 计时 + `--db.disable-metrics`(`#22211`/`#24806`)** | 只作用于 MDBX `metrics.rs` 的 `Instant`-timed 闭包;**gravity RocksDB 主路径用原生 property 指标(gauge),没有可加速的 clock-timed 记录** → 主路径零收益 |
| **`CachedReads::with_account_capacity`(`#25048`)** | 只服务 reth basic payload builder(出块),**gravity pipe-exec 执行路径不用 `CachedReads`** → 热路径无收益(除非 gravity 也用 basic builder 多轮出块) |
| 上游 sparse-trie / proof-v2 引擎、in-memory trie changesets、overlay、storage-v2 原生 rocksdb、prewarming/execution-cache、BAL | 与 gravity nested-trie / grevm+PersistBlockCache / 自研 rocksdb **互斥或已被覆盖**(见三份分析) |

## ⚠️ 安全 / 前置(务必注意)

1. **Precompile cache 的 `#23619` 护栏对 gravity 不足 —— 会造成共识 bug。** 已逐一核实 gravity 的 3 个 precompile:
   - **`mint`**(`…1625f5000`):直接改 journal(`load_account`+改 balance+`mark_touch`),**不走 reservoir/state-gas** → 护栏放行 → 命中缓存会**跳过铸币** → 共识 bug。
   - **`randomness_by_height`**(`…1625f5002`):随块重建、依赖块高/prev_randao,同 input 跨块出不同值 → 护栏放行 → 跨块缓存返回**陈旧值** → 共识 bug。
   - **`bls_pop_verify`**(`…1625f5001`):实为**纯函数**,可安全缓存(与"3 个 stateful"表述相反)。
   - **承重闸门必须是地址白/黑名单**:只 wrap 标准 `0x01–0x11`,**永不 wrap** 上述 3 个 `0x…1625f50xx`;`#23619` 护栏仅作 defense-in-depth。
2. **编译验证前置**:当前 merge 树是 WIP,**整仓不可编**——`Cargo.lock` 767 处冲突标记 + engine/evm/rpc/node 等**其它组** 92 个文件仍带标记(含 revm 29↔40 分裂)。storage/trie/prune **源码本组已 0 标记**;本文已合的两项(sorted writes、#21200)在整树装配、Cargo.lock 重生成后需 `cargo test -p reth-provider -p reth-db-api` 复验(尤其 #21200 的 `[u8;N]: Into<Vec<u8>>` 满足 `Encode::Encoded` bound、以及 byte-equality 断言)。

## 验证建议
- **Sorted writes**:`crates/trie/db/src/nested_hash.rs` 的 round-trip 测试 + mainnet-replay-pipe e2e 应产出**字节一致的 DB 状态与相同 root**;可加单测断言写序升序。
- **#21200**:PR 自带 round-trip 测试 + `ShardedKey::new(addr,n).encode().as_ref() == <旧 Vec 路径>` byte-equality 断言;`cargo test -p reth-db-api`。
