# reth v2.3.0 优化差距分析 — gravity-reth 缺什么、为什么、怎么补

> 生成日期:2026-07-08。对比三棵树:
>
> | 树 | 路径 | 版本 |
> |---|---|---|
> | 上游 reth v2.3.0 | `/Users/gx/ws/git/block/reth` | `9384bc53d8` |
> | 合并后 gravity-reth(本仓) | 当前 worktree | `66ea109527` |
> | 合并前 baseline | `/Users/gx/ws/git/block/gravity-reth` | `0cb1687c1c` |
>
> **判定口径**:全部结论基于符号存活实测(grep 挂载点),区分三态——
> **挂载**(在编译树内)/ **孤儿**(文件在盘、git 跟踪,但 `lib.rs`/`mod.rs`
> 未挂载,不编译)/ **不存在**(盘上无文件)。合并树 `cargo check
> --workspace --all-features` 为 0 error,故「挂载」即「编译且可用」。

## TL;DR

上游 v2.3.0 的性能演进分五个家族,gravity 的缺失状态和原因各不相同:

| 家族 | gravity 状态 | 缺失根因 | 可补性 |
|---|---|---|---|
| **bin 级 feature**(asm-keccak / keccak-cache-global 等) | default 未开 | 合并时 bin/reth default 保守保持 baseline | ✅ **几行改动,建议立即**(§A) |
| **merge 接线断链**(TreeConfig 旋钮、slow-block、share-sparse-trie) | 死配置/硬编码 | 合并副产物(非决策) | ✅ **小工程,建议尽快**(§B) |
| **sparse-trie 状态根引擎**(proof_v2 / arena / sorted API 等) | 几乎全孤儿 | 与 nested trie **架构互斥**(有意决策) | ⚠️ 仅 eth-mode 有意义,不建议(§C) |
| **storage-v2 磁盘轨道**(Packed 表 / static-file changesets 等) | 不存在或孤儿 | `f89d9d4e23` storage 整体还原 baseline(有意决策) | ⏳ v2.4+ 债,部分可独立摘取(§D) |
| **BAL / EIP-7928**(Amsterdam) | 有类型无装配 | gravity 不上 Amsterdam(业务决策) | ⏸️ 等业务决策(§F) |

另有 **mdbx 专属小优化**(§E)与**已全量吸收清单**(§G,防误判)。
建议路线图见 §H。

---

## 结构性原因:为什么会缺

理解每一项差距前,先明确四个结构性根因。后文每项都标注归属。

**根因 1 — 状态根引擎二选一(架构互斥)**。gravity 生产主路径是
pipe-exec(grevm 并行执行)+ `NestedStateRoot` 增量状态根(自研
`AccountsTrieV2`/`StoragesTrieV2` 表)+ `PersistBlockCache`。上游 v2.3.0
的状态根性能演进(proof_v2、arena 化 sparse trie、state_root_task 流水线、
sorted overlay API)全部服务于 **newPayload 的 sparse-trie 增量根流水线**
——这条路径 gravity 在 pipe 模式下**根本不经过**。两套引擎对同一批 trie
表的读写语义互斥(README §CHAIN-HALT 风险 #3:混链 = state root
mismatch),合并时裁决 nested trie 全胜,上游演进整体留作磁盘孤儿。

**根因 2 — storage 整体还原 baseline**。`f89d9d4e23`(#375)把 storage
6 crate + libmdbx 的 src 还原到合并前形态,上游 storage-v2 全家
(`StorageSettings` 版本协商、Packed 表、static-file changesets、
`EitherWriter`、provider 层原生 RocksDB)在编译树中不存在。深层原因:
gravity 已把**整个主库**搬到 RocksDB(`a1d7365bd6` #212,LSM 后端),
上游 storage-v2 是在 **mdbx 为主库**的前提下把写密集表挪进 RocksDB/静态
文件的渐进优化——两条轨道解决的是同一问题(B-tree 写放大),gravity 的
解法更激进且已在线上,上游轨道对 gravity 收益大部分**重叠**。

**根因 3 — Amsterdam/BAL 业务决策**。EIP-7928 Block Access Lists 是
Amsterdam 分叉特性(并行状态预取 + 无状态验证辅助)。gravity 不面向
以太坊 L1 共识,且 BAL 的并行预取收益与 grevm 并行执行重叠,合并时裁决
「不上 Amsterdam」:trait 面保留(`take_bal` 默认 `None`)但不装配。

**根因 4 — 合并副产物(非决策,纯遗漏/断链)**。若干项上游代码已进树,
但接线在两种解决策略(keep-gravity 骨架 vs follow-v2.3.0 服务形态)的
接缝处断掉,形成「CLI/config 在、逻辑不在」的死配置。这类**不是决策**,
是应当修复的回归——与 2026-07-08 已修的 pipe 持久化轮询断链
(`pipe_run_inner` 缺 `try_poll_persistence`)同类。

> **pipe 模式 vs eth-mode 的判读钥匙**:上游 engine/tree 的一切
> newPayload 路径优化,对 gravity 的价值取决于运行模式。生产(pipe)
> 模式走 `pipe_run_inner` + pipe-exec,不经过 `run_inner` 的
> newPayload/payload_processor;只有 `--gravity.disable-pipe-execution`
> 的 eth-mode(本地开发、RPC 只读节点、回退运维)才走上游路径。评估
> 「要不要补」时先问:这条路径 gravity 生产走不走?

---

## §A bin 级 feature 差距 — 几行改动,建议立即

### A1. `asm-keccak` + `keccak-cache-global` 未进 default ⭐ 性价比最高

> **⟲ 2026-07-09 已落地**:bin/reth default 已加 `asm-keccak` +
> `keccak-cache-global`(两 feature 均含 `alloy-primitives/*` 转发,与
> 上游 bin/reth 同形);`cargo check -p reth` 绿。语义验证走
> mainnet_replay 差分回放(见 §B 落地注记)。

- **上游是什么**:`bin/reth/Cargo.toml:85` 的 default 含 `asm-keccak`
  (keccak 汇编实现)与 `keccak-cache-global`(alloy-primitives 1.6.0 的
  进程级 keccak memoize 缓存,`bin/reth/Cargo.toml:119` 定义转发)。
  keccak 是执行热点(地址/存储槽哈希、trie key 哈希、RLP 节点哈希),
  重复输入命中缓存直接省 CPU。
- **gravity 状态**:default 仅 `["jemalloc", "reth-revm/portable"]`
  (与 baseline 一致)。`asm-keccak` feature **已定义**(bin/reth
  Cargo.toml:6)但不在 default;`keccak-cache-global` 在 bin/reth
  **未定义**,不过 crate 级插桩已随合并进树(`crates/node/core/
  Cargo.toml:83`、`crates/ethereum/node/Cargo.toml:102`,转发到
  alloy-primitives——两树 alloy-primitives 同为 1.6.0)。Makefile 的
  `FEATURES ?=` 为空,即 **release 构建默认两者都没有**。
- **缺失原因**:根因 4。合并时 bin/reth Cargo.toml 按「保 baseline
  default」解决,上游 default 扩容未跟进(net-prune 文档 OQ4 明确推迟,
  但推迟评估的对象主要是 otlp/js-tracer,asm-keccak/keccak-cache 属
  纯性能项,无运维副作用)。
- **怎么加**(半天内):
  1. bin/reth `[features]` 补 `keccak-cache-global = [
     "reth-node-core/keccak-cache-global",
     "reth-node-ethereum/keccak-cache-global" ]`(照抄上游
     bin/reth/Cargo.toml:119,gravity 侧两个转发目标已存在);
  2. default 数组加 `"asm-keccak", "keccak-cache-global"`;
  3. `cargo check` + 跑一次 pipe 金丝雀 + `mainnet_replay` 差分测试
     (keccak 缓存影响所有哈希路径,用差分 oracle 验证无语义变化)。
- **风险**:低。keccak-cache-global 有全局内存占用(有界缓存);
  asm-keccak 对目标平台有汇编要求(aarch64/x86_64 均支持)。

### A2. `min-trace-logs` / `otlp` / `js-tracer` — 运维取舍项

- **上游**:三者均在 default。`min-trace-logs` =
  `tracing/release_max_level_trace`(release 二进制保留 trace 级日志的
  **能力**,便于线上诊断,常态有轻微 filter 开销);`otlp` = OpenTelemetry
  导出;`js-tracer` = debug JS tracer。
- **gravity 状态**:`min-trace-logs` 已定义未进 default,且比上游少一条
  `reth-node-core/min-trace-logs` 转发(补齐是一行);`otlp` 在 bin 层无
  聚合 feature,但 `crates/tracing` 的 default 已含 `otlp`,通路其实在;
  `js-tracer` 走 `reth-node-ethereum = { features = ["js-tracer"] }`
  依赖行直开,**功能已可用**,只是无独立开关。
- **判定**:非性能差距,是暴露面差距。按 gravity 运维需求决定;若要
  对齐,工作量各一行。net-prune 文档 OQ4 已登记为 v2.4+ 或独立 PR。

---

## §B merge 接线断链 — 死配置修复(小工程,建议尽快)

这一类的共同点:**上游代码大部分已在树,只是接线断了**。修复不引入新
语义,属回归修复。均只影响 eth-mode 路径(pipe 模式不经过),优先级
可据此调整;但「CLI 旋钮可设而永不生效」本身是运维陷阱,建议要么接通
要么摘除,不要悬着。

### B1. TreeConfig 旋钮丢失:`disable_parallel_sparse_trie` / `max_proof_task_concurrency` ⭐ 这是 gravity 自己的旋钮被合并吃掉

> **⟲ 2026-07-09 已落地**:`TreeConfig` 恢复两字段 +
> `DEFAULT_MAX_PROOF_TASK_CONCURRENCY=256` 常量(getter/with_/new()/
> Default 全套);payload_processor 两处硬编码(构造 @126、spawn @199)
> 接回 config 读取;CLI 侧 `--engine.disable-parallel-sparse-trie`
> 去 deprecated/hide 复活接线,`--engine.max-proof-task-concurrency`
> 按 DefaultEngineValues 惯例新增。`cargo nextest -p reth-node-core
> engine` 11/11(含两个新旋钮测试)。

- **事实**:baseline 中两者是**真正的 TreeConfig 项**(config.rs:69/77,
  含 getter/setter 与 `DEFAULT_MAX_PROOF_TASK_CONCURRENCY`);合并时
  config.rs 采上游版(上游已删这两项,改用 `multiproof_chunk_size`),
  但 gravity 的 payload_processor 仍是 baseline 版,需要这两个值——
  于是被**硬编码**进 `payload_processor/mod.rs`(字段默认 false @96/126、
  `max_proof_task_concurrency = 256usize` @199)。
- **缺失原因**:根因 4(两种策略接缝:config.rs follow-v2.3.0,
  payload_processor keep-gravity)。
- **怎么修**(1 天):把两个字段加回 `TreeConfig`(照 baseline
  config.rs:69-77 形态),payload_processor/mod.rs 三处硬编码改回读
  config;若原有 CLI 透传(`--engine.*`)也一并恢复。
- **风险**:低,纯旋钮恢复。

### B2. `--engine.slow-block-threshold` 死配置

> **⟲ 2026-07-09 已落地(按台账建议摘除)**:CLI arg + TreeConfig 字段
> + DefaultEngineValues 字段/setter + 解析测试全部移除;
> `parse_duration_from_secs_or_ms`/`format_duration_as_secs_or_ms`
> import 随之清理。`ExecutionTimingStats` 按 §B4 现状保留不动。

- **事实**:CLI(node/core args/engine.rs:461)+ config 字段 + 管线
  (`with_slow_block_threshold`)都在;但上游真正消费它的三处
  (`tree/mod.rs:1999`、`:2994`、`payload_validator.rs:509`——慢块
  结构化 warn,含 execution/state-reads/state-root/commit 分项耗时与
  缓存命中率)在 gravity 树中**零命中**。设了永不触发。
- **缺失原因**:根因 4。消费点位于 payload_validator/mod.rs 的
  keep-gravity 区段,上游新增逻辑未随迁。executed-block 文档 §E 审查
  台账已把它登记为「死 CLI flag,建议摘除」。
- **怎么办**(二选一):
  - **摘**(半天):删 CLI arg + config 字段 + 管线,与台账建议一致;
  - **接**(2-3 天):照上游三处消费点移植判定与结构化日志。前置:
    分项耗时依赖 `ExecutionTimingStats`,该结构在合并树已挂载
    (chain-state `execution_stats.rs` + engine 字段),缺的是
    payload_validator 内的计时埋点。eth-mode 若用于 RPC 节点运维,
    这个诊断值回票价;纯 pipe 生产则摘。

### B3. `--engine.share-sparse-trie-with-payload-builder` 死配置

> **⟲ 2026-07-09 已落地(按台账建议摘除)**:CLI arg + TreeConfig 字段
> + DefaultEngineValues 字段/setter 移除;e2e 死测试
> `test_share_sparse_trie_with_payload_builder`(测试从未生效的特性)
> 一并移除;引擎侧孤儿 `preserved_sparse_trie.rs` 维持留盘不动。
> payload builder 侧 `trie_handle` 管线保留(恒 `None`,上游结构)。

- **事实**:CLI + config + payload builder 侧 `trie_handle` 管线都在
  (Task #12 D① 随 `03bffd095b` 复活 payload-builder 生产路径时接了
  `trie_handle: None`);但引擎侧交出 sparse trie 的模块
  `preserved_sparse_trie.rs` 是**孤儿**,`config.share_sparse_trie_with_
  payload_builder()` 在挂载代码中零调用(上游在 `mod.rs:3256` 消费)。
  引擎永远不交 trie → 出块器永远拿 `None` → 特性失效。
- **缺失原因**:根因 4 + 根因 1 边缘(该特性把**验证侧 sparse trie**
  借给**出块侧**复用,gravity eth-mode 的验证侧走的是 baseline sparse
  任务,形态不同)。台账 §E 同样登记为死 flag。
- **怎么办**:生产 pipe 模式出块不走 payload builder 的 trie 路径
  (launch/engine.rs 的 triev2 空桥接已有码内风险注释),**建议摘**
  (半天)。若未来要认真维护 eth-mode 本地出块性能,再评估挂载
  `preserved_sparse_trie.rs` + 移植 mod.rs 交接逻辑(1 周级,需先做
  §C 的整体评估)。

### B4. `ExecutionTimingStats` / `PersistedBlockSubscriptions` 半接入

- **事实**:chain-state 侧模块 + noop impl + engine 字段已挂载;
  provider(full.rs / blockchain_provider.rs)、rpc-builder、rpc/reth.rs
  端点、`payload_validator` 埋点未接。
- **缺失原因**:**这是有意裁决**(executed-block §9.3:trait + noop
  保留,rpc 端点外科摘除),不算回归;列在此处是因为它是 B2 的前置。
- **怎么办**:维持现状;若做 B2「接」路线,顺带补 payload_validator
  埋点即可,rpc 暴露面仍可不开。

### B5. 双 `CachedStateProvider` 并存

- **事实**:合并树同时存在两套执行缓存——engine/tree 自己的
  `cached_state.rs`(gravity 版,mini_moka 后端,引擎验证路径用)与
  上游新 crate `reth-execution-cache`(fixed-cache 后端,是 workspace
  member,被 payload/basic、payload/builder、ethereum/payload 依赖,
  即**出块器路径**用)。两套互不相通;上游的设计意图(顺序 payload
  处理复用父块缓存、引擎↔出块器共享)只剩一半。baseline 只有 mini_moka
  一套。
- **缺失原因**:根因 4(开放问题 #1「option C」部分回滚的残留;
  `moka-vs-mini-moka-verification.md` 有前期对比)。
- **怎么办**(评估级,2-3 天):对 gravity 生产无影响(pipe 模式有
  PersistBlockCache);eth-mode 下建议二选一统一——倾向保 gravity
  mini_moka 版并让 payload 侧也用它(改 4 个 crate 的依赖与构造),
  或反向全部切 `reth-execution-cache`(需把引擎侧 mini_moka 版迁走,
  动 keep-gravity 区段,风险高一档)。不统一也能跑,只是双份缓存
  内存与语义分裂。

---

## §C sparse-trie 状态根引擎家族 — 与 nested trie 互斥,不建议移植

**上游清单与 gravity 状态**(全部为根因 1):

| 项 | 上游位置 | 机制/收益 | gravity 状态 |
|---|---|---|---|
| proof_v2 | `trie/trie/src/proof_v2/`(4 文件,主体 132KB) | leaf-only multiproof:复用 cursor、用 `BranchNodeCompact.hashes` 跳过子树、延迟 RLP 编码 | **孤儿**(lib.rs 未挂载) |
| arena 化 SparseTrie | `trie/sparse/src/arena/`(143KB) | slotmap 节点竞技场,去 per-node Box/指针追逐,upper/lower 分层 | **孤儿** |
| ParallelSparseTrie(上游版) | `trie/sparse/src/parallel.rs`(358KB) | lower 子树并行 hash | **孤儿**(gravity 用自己的 `crates/trie/sparse-parallel` crate,baseline 产物,仍挂载) |
| lfu / lower | `trie/sparse/src/{lfu,lower}.rs` | O(1) 有界缓存;盲化子树复用分配 | **孤儿** |
| state_root_task 流水线 | `trie/parallel/src/state_root_task.rs` + `value_encoder.rs` | proof 抓取与 sparse hash 更新流水线并行 | task 壳**挂载**(gravity 版消费);value_encoder **孤儿** |
| GAT TrieCursorFactory | `trie/trie/src/trie_cursor/mod.rs:31` | cursor 借用事务、去 Send+Sync/Arc 开销 | **未采纳**(gravity 保留非 GAT + `Send + Sync`) |
| TrieInputSorted / sorted API | `trie/common/{input,updates,utils,lazy}.rs` | 预排序 + k-way 归并替代每块重排 BTree | **未采纳**(utils/lazy 为孤儿) |
| trie changesets | `trie/trie/src/changesets.rs` | 节点级旧值记录,reorg 免重算 | **孤儿** |
| Canonical witness | `common/execution_witness.rs` + mode 化 `witness.rs` | 更小的 stateless witness | **孤儿**(gravity 保 legacy 无 mode 签名) |
| receipt_root_task / ordered_root | engine payload_processor + `common/ordered_root.rs` | 流式增量 receipt 根 | **孤儿** |

- **为什么不缺也不该补**:
  1. 这些优化的服务对象是上游 newPayload 的 sparse-trie 状态根流水线。
     gravity 生产(pipe)路径的状态根由 `NestedStateRoot` 对 V2 表增量
     计算,**有自己的并行层**(`0b57bc2340` #219 nested hash 并行)和
     自己的缓存(PersistBlockCache);差分与回放测试
     (`mainnet_replay.rs`/`wipe_recreate_e2e.rs`)保障其正确性。
  2. GAT cursor 与 nested 方向**冲突**:nested 并行 worker 需要
     `Send + Sync` cursor,GAT 借用式 cursor 非 Send——采纳会拆掉
     nested 的并行。
  3. 经典回退路径(`ParallelStateRoot`/`StateRoot`)三棵树字节级几乎
     一致——v2.3 在经典路径上**没有**算法级提速,收益全在 sparse 家族,
     所以「不采纳」并没有让经典路径变慢。
- **什么条件下值得重新评估**:若 gravity 要认真运营 eth-mode(如对外
  RPC 验证节点、以太坊主网跟随节点),上游流水线在该模式下的提速是
  真实的(prewarm 并行预热 + proof_task worker 池 + arena sparse)。
  做法是**整路径切换**而非逐件移植:payload_processor 全目录采上游 +
  挂载全部孤儿 + TreeConfig 对齐,并保证 nested trie 表不被 sparse
  路径写(只读共存)。量级:2-4 周 + 完整回归。在此之前,零散移植
  单件(如只挂 proof_v2)没有消费方,无意义。
- **一个不缺的澄清**:上游 `PackedStoredNibbles`(trie 表 key 65B→33B,
  约 -49%)看似 gravity 缺失,实际 gravity 的 V2 表自 `#149` 起就用
  **变长 `[len][packed]` 打包编码**(仓库根 `MIGRATION.md` 已锁定该
  磁盘格式)——gravity 在自己的表上**已有等价甚至更紧凑的编码**。
  上游 Packed 面向 gravity 不写入的上游表,无需跟进。

---

## §D storage-v2 磁盘轨道 — v2.4+ 债,少数可独立摘取

**全家总开关**是 `StorageSettings{storage_v2}`(Metadata 表版本协商,
gravity 中为孤儿),以下各项均受其门控(根因 2):

| 项 | 上游机制/收益 | gravity 状态 | 可独立摘取? |
|---|---|---|---|
| static-file 新 segment(TransactionSenders/AccountChangeSets/StorageChangeSets) | 写密集数据从 B-tree 挪到顺序 append + 压缩 | enum 变体**已编译**(static-file/types 未还原)但无生产者,数据仍在主库;manager 的 `maybe_heal_segment`(#20508)/`StaticFileSegmentIndex`(#19803)不存在 | ⏳ Phase 2 债 |
| changeset_walker 流式 unwind | 从 static file 逐块迭代 changeset,unwind 不全量载入内存 | 不存在 | 依赖上项 |
| `EitherWriter` + provider 层原生 RocksDB | history 索引(TransactionHashNumbers/AccountsHistory/StoragesHistory)进 LSM,降 B-tree 写放大 | 不存在 | ❌ 收益与 gravity 全库 RocksDB **重叠**,不需要 |
| WriteStateInput / StateWriteConfig / SaveBlocksMode | 单块写省一次 ExecutionOutcome 构造;v2 下跳过 mdbx 重复写 | 不存在 | 接口整形为主,gravity `UnifiedStorageWriter` 已有自己的路径,收益小 |
| CommitOrder / unwind_provider_rw | 多存储崩溃一致性提交顺序 | 不存在 | gravity 已有等价纪律(`commit_view` 顺序 + `ff103f976a` #313 recovery) |
| init-state OOM 缓解(`STATE_ROOT_COMMIT_THRESHOLD=25_000`) | 大 genesis 导入分段提交防 OOM | 不存在 | ✅ **可独立摘取**(下述) |
| 非零 genesis stage checkpoint | init.rs checkpoint 用 genesis 块号而非 0 | 不存在 | ✅ 可独立摘取(小) |

- **为什么放弃**:见根因 2。补充一点量化判断:上游 static-file
  changesets 的空间/压缩收益对 gravity 仍然真实(changesets 目前在
  RocksDB 主库里),但 gravity 的 sharding RocksDB(`c64bd613e4` #225)
  已针对 persist 阶段做过写优化,收益需实测才知剩多少。
- **怎么加(如果要)**:`STORAGE-RESOLUTION-TODO.md` 已写明 Phase 2
  路径——provider static_file 层升级到上游 + 以 always-legacy 的
  `StorageSettings`/`EitherWriter` 垫片桥接,再逐 segment 打开。这是
  storage 组 v2.4+ 再合并的主体工程(周级),不建议在 v2.3 收尾期启动。
- **可立即摘的两小件**(不依赖 storage-v2 开关):
  1. **init OOM 缓解**(1-2 天):把上游 `db-common/src/init.rs:73`
     的分段提交阈值思想移植到 gravity 的 init/init-state 路径。台账
     已登记「待 state-root team 评估」——评估点是 gravity RocksDB
     init 是否有同样的脏页积压问题(LSM memtable 语义与 mdbx 脏页
     不同,可能天然不 OOM,先复现再动手)。
  2. **非零 genesis checkpoint fix**(半天):`init.rs:158`
     `Default::default()` → `StageCheckpoint::new(genesis_block_number)`,
     仅影响非零起始块场景,cherry-pick 即可。

---

## §E mdbx 专属小优化 — 低风险,收益取决于 mdbx 使用面

gravity 主库是 RocksDB,mdbx 是可选后端(eth-mode/工具链仍可用)。
以下项**技术上极易移植**,但收益面窄,按需取:

| 项 | 上游机制 | gravity 状态 | 移植成本 |
|---|---|---|---|
| `ReadTxnPool` | 无锁 `ArrayQueue(256)` 复用已 reset 的只读事务,`mdbx_txn_renew` 绕过 `lck_rdt_lock` 互斥;高并发只读(prewarm/并行 proof)下消除 read-txn-begin 竞争 | **孤儿**(`libmdbx-rs/src/txn_pool.rs` 在盘,lib.rs 未挂 `mod txn_pool`,environment.rs 未接) | 1-2 天:挂 mod + environment 接入(照上游 lib.rs:38 与 environment.rs 用法);另需处理 gravity 的 `ParallelTxRO` 与其关系 |
| db metrics prebind + quanta | 每表把全部 operation metric 句柄预绑成一个 struct(热 cursor 路径免每操作查句柄);`quanta::Instant`(TSC)替代 `std::time::Instant` 降计时开销 | 未采纳(= baseline,`FxHashMap<(&str, Operation)>` 粗粒度) | 半天-1 天,db crate 内部,照上游 `db/src/metrics.rs` |
| `drop_orphan_table` / `with_metrics_if` / `DatabaseArguments::test()` | 便利/清理项 | mdbx.rs 为孤儿,实现文件已还原 baseline | 按需,无性能意义 |

**判断**:若 eth-mode 只是开发/回退用途,这三项都可不做;若 gravity 有
mdbx-后端只读节点(高并发 RPC 读),`ReadTxnPool` 值得做——它正好命中
`prewarm`/并行读场景。

---

## §F BAL / EIP-7928 — 有类型无装配,等业务决策

- **gravity 现状**(逐层核实):
  - **挂载**:evm trait 面(`take_bal` 默认 `None`、`bump_bal_index`)、
    `alloy-eip7928 0.4.0` 依赖、net/p2p BAL client(`crates/net/p2p/
    src/lib.rs:21` `pub mod block_access_lists`)、eth-wire-types 消息;
  - **孤儿**:`storage/provider/src/bal.rs`(与上游逐字节相同、501 行,
    但 lib.rs 未挂 `mod bal`)、`storage-api/src/bal.rs`、engine
    payload_processor 的 `bal/` 与 `bal_prewarm_pool.rs`;
  - **不存在**:节点装配(`with_bal_store`/`BalStoreHandle` 构造零命中、
    `--db.balstore-cache-size` arg 缺失)、rpc/engine BAL 端点(合并时
    外科摘除,`getPayloadBodies*V2` 的 `block_access_list` 恒 `None`)。
  - 两树(含上游)都还没有 `Amsterdam` fork 枚举——上游自己也未定档,
    BAL 只是预备设施。
- **为什么**:根因 3。且 BAL 的核心收益(声明式访问清单 → 并行预取/
  并行执行调度)与 grevm 的乐观并行执行**解决同一问题**,gravity 侧
  收益存疑。
- **怎么加(若某天决定跟)**,顺序固定:
  1. 挂载 `storage-api/src/bal.rs` + `provider/src/bal.rs`(mod + re-export);
  2. `launch/common.rs` 照上游 :510-528 装配
     `BalStoreHandle::new(InMemoryBalStore::new(cache_size))` +
     `.with_bal_store(...)`,补 `--db.balstore-cache-size` arg;
  3. 恢复 rpc/engine 端点(合并时的摘除点在 rpc 组文档有清单);
  4. 执行层把 grevm/pipe 输出接 `bump_bal_index`(这是真正的设计工作,
     上游假设串行执行器构造 BAL,gravity 并行执行下 BAL 序需重定义);
  5. Amsterdam fork gate(上游定档后跟)。
  量级:装配本身 1 周内;第 4 步是开放设计问题,不定档不动。

---

## §G 已随合并全量吸收 — 防误判清单

以下 v2.3.0 演进 gravity **已经拿到**,排查差距时不要重复计入:

- **执行/引擎**:`GasOutput` / `execute_with_state_closure_always`
  (BlockExecutor);precompile cache(`map_cacheable_precompiles`
  接线,moka/LruMap 后端,三树等价);cross_block_cache_size;
  persistence 服务形态(crossbeam + `PersistenceResult`);
  **backpressure**(`should_backpressure`/`wait_for_persistence_event`,
  在 `run_inner` 生效;`pipe_run_inner` 不用它——pipe 有自己的
  `persistence_threshold=2`/`memory_block_buffer_target=0` 节流,是
  有意裁决而非缺失);era stage(td 泵线保 baseline);
  EIP-2935/7702/7825/7934 校验合并。
- **RPC**:HTTP compression(gzip/deflate/brotli/zstd +
  `--http.disable-compression`);`send_raw_transaction_sync`;
  call/estimate 的 `State<DB>` 复用与 `EvmDatabaseError` 包装;
  `Runtime` executor 全链路;`FullConsensus` 收紧。
- **网络**:eth69/eth70 协议版本;fetch/tx 广播模块与上游行数逐一
  相同(1878/3232/155);pruning pre-merge
  (`--prune.receipts.pre-merge`、`full_bodies_history_use_pre_merge`)。
- **tx-pool**:7 个配置常量与上游完全一致(含
  `MAX_NEW_PENDING_TXS_NOTIFICATIONS`、
  `DEFAULT_MAX_INFLIGHT_DELEGATED_SLOTS`);gravity 另保留自己的
  `MAX_NEW_TRANSACTIONS_PER_BATCH=1024`(上游 16,gravity 高 TPS 场景
  的有意覆盖)。
- **基建**:headers/exex 的 bincode→RLP serde;sender_recovery 的
  `spawn_os_thread`/SyncSender;CI 供应链加固 + sccache。
- **工具位面差异**(非优化,记录备查):上游删了 `reth-bench` 新增
  `bin/reth-bb`(大区块负载节点,依赖已被拒的 BAL 全链);gravity 删了
  `reth-bench` crate(残源在盘)也没引 `reth-bb`。上游
  `reth-provider/jemalloc`(转发给 provider 内嵌 rocksdb)在 gravity
  无意义——gravity 的 rocksdb 在 db 层,jemalloc 聚合已覆盖。

---

## §H 建议路线图

| 优先级 | 项 | 类型 | 量级 | 前置 |
|---|---|---|---|---|
| ~~P0~~ ✅ | A1 `asm-keccak` + `keccak-cache-global` 进 default | 纯增益 | 半天 | **已落地 2026-07-09**(见 §A1 注记) |
| ~~P0~~ ✅ | B1 TreeConfig 旋钮恢复(gravity 自有旋钮被合并吃掉) | 回归修复 | 1 天 | **已落地 2026-07-09**(见 §B1 注记) |
| ~~P0~~ ✅ | B2/B3 死 CLI flag 裁决:摘(推荐)或接 | 回归修复 | 各半天(摘) | **已按建议摘除 2026-07-09**(见 §B2/§B3 注记) |
| **P1** | D-小件:非零 genesis checkpoint fix | cherry-pick | 半天 | 无 |
| **P1** | D-小件:init OOM 阈值(先复现 RocksDB init 是否受影响) | 评估+移植 | 1-2 天 | state-root team 评估 |
| **P1** | A2 min-trace-logs 转发补齐 / otlp 开关对齐 | 运维对齐 | 各一行 | 运维拍板 |
| **P2** | E `ReadTxnPool` 挂载(仅当有 mdbx 高并发只读节点) | 移植 | 1-2 天 | 明确 mdbx 使用面 |
| **P2** | E db metrics prebind + quanta | 移植 | 1 天 | 同上 |
| **P2** | B5 双执行缓存统一 | 收敛 | 2-3 天 | eth-mode 定位拍板 |
| **P3** | C 整路径 eth-mode payload_processor 升级 | 战略 | 2-4 周 | 仅当运营 eth-mode 验证节点 |
| **P3** | D static-file changesets Phase 2 | 战略(v2.4+) | 周级 | storage-v2 再合并窗口 |
| **P3** | F BAL 装配 | 业务 | 1 周 + 设计 | Amsterdam 决策 + grevm-BAL 序设计 |

**总结一句话**:真正「白捡」的差距只有 §A(feature 开关)和 §B(合并
断链)两类,合计 3-4 人日就能收干净;§C/§D 的大头缺失是**两次架构
分叉(nested trie、全库 RocksDB)的必然代价**,gravity 在这两个方向上
已有自己的等价或更激进的优化,上游演进对 gravity 的边际收益需要以
eth-mode 定位和 v2.4+ 再合并窗口为前提重新评估,不建议零散移植。
