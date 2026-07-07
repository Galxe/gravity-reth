# ExecutedBlock 二分与 pipe-exec make-canonical 链路分析(开放问题 #2)

> 结论先行(2026-07-03 修订):**「保留二分(keep-gravity)」的决策仍然成立**——
> pipe-exec 路线的 root 必须在 seal 前急切算出、triev2 无处安放,上游 lazy/overlay
> 模型没有挂载点(第三节,未变)。**但初版「设计自洽:类型定义与全部消费方都在
> fork 自有代码内,完全不经过上游集中式 trie overlay」的论断,在 merge 后的
> worktree 不成立**:上游 overlay 机器已以零冲突文件形态整体入库并被生产路径
> (payload_validator)消费;多个零冲突文件已静默落在上游侧,keep-gravity 一落地
> 就编译不过。落地形态不是「按 keep-gravity 取边」,而是一台混合手术:
> **载荷类型 keep-gravity;服务形态(crossbeam 通道、持久化服务生命周期、引擎
> 事件循环)follow v2.3.0;外围约 30 个文件按第六节逐文件方案适配**。
> 本文第五节起为代码级实施方案,可直接照做。

> **⟲ 2026-07-03 二次修订(f89d9d4e23 之后)**:storage 组以 commit
> `f89d9d4e23`("resolve storage&cache&state root (#375)",193 文件,
> +10311/−39089)落地了与本文 §6.0/§6.1/§6.4 **方向相反**的路线——storage
> 6 crate + trie 6 crate + prune 的 `src/` **整体还原 gravity baseline**
> (static-file provider 仅 7 处桥接),上游 storage-v2 文件删除或留作磁盘
> 孤儿,并在其 `STORAGE-RESOLUTION-TODO.md` 中要求下游(engine/rpc/node)
> 同向对齐。后果:本文 storage/trie 侧方案(§6.0/§6.1/§6.4)已被其取代
> (结果等效达成,冲突全归零);**engine 侧路线随之反转**(§6.5.1 路线乙
> 地基坍塌,当初被否的路线甲成为唯一自洽路线);chain-state 侧两个子方案
> 反转(§6.2.2 改方案 B、§6.2.4 改 trim)。所有反转处以「⟲ f89d9d4e23」
> 标记,原方案压缩保留作决策史。**四个遗留未决策问题单列 §九**
> (其中 9.1/9.2 是 engine / chain-state 组按 §6.5.1 / §6.2.1 动工的前置)。

> **⟲ 2026-07-05 三次修订:§九四项已全部裁决**。用户拍板决策总原则
> (原文记档于 §九开头):storage 决策最高优先;v2.3.0 设计与之冲突则迎合
> storage,不冲突则在不破坏 gravity 功能前提下保留。裁决结果:9.1 = 选项 a
> (回滚 option C 的 engine-tree 部分)、9.2 = 路线 A(chain.rs/exex 回
> baseline 二参)、9.3 = 分拆(`PersistedBlockSubscriptions` 整链 trim /
> `ExecutionTimingStats` 保留)、9.4 = 选项 a(rpc-provider 整 crate 回
> baseline,「仅一处断点」被实测推翻)。engine / chain-state 组动工前置解除。
>
> **⟲ 2026-07-05 四次修订(落地轮)**:§六/§九方案已由六路并行落地 + 收口
> 执行完毕——本文档范围内全部文件冲突标记归零(全仓带冲突文件 117→77,
> 余者均在范围外);§八/§九落地框已按实测证据勾选,偏差以「落地实录」
> 标注;收口死符号总扫另抓 4 个断点已修(§五 25a-25c);跨组遗留见
> §九末「跨组断点台账」。编译级验收统一待 cargo 组修复 workspace 依赖后回补。

- 核实日期:2026-07-02(初版链路分析)/ 2026-07-03(代码级实施方案,六路并行
  逐冲突块核查 + 交叉裁决;同日按 `f89d9d4e23` 二次修订)/ 2026-07-05
  (§九裁决 + 9.2/9.4 补充实测)/ 2026-07-06(五组落地 + 收口:台账 4 项关闭、2 项定性、根因升级)
- 分支:`gravity-reth-merge-v2.3.0`(WIP checkpoint `f89d9d4e23`)
- 基线引用:HEAD 侧(gravity baseline)= `0cb1687c1c`;上游侧 = tag `v2.3.0`。
  对照命令:`git show 0cb1687c1c:<path>` / `git show v2.3.0:<path>`
- 行号约定:冲突块行号为当前 worktree(含冲突标记)行号,解冲突过程中会漂移;
  「块 <起始行>」指以该行 `<<<<<<<` 开头的冲突块。零冲突文件行号为当前内容行号。
- 本文是 `engine-evm-execution-chainspec.md` 开放问题 #2 的展开分析,并顺带
  给出开放问题 #5(memory_overlay)的最终方案(§6.2.2)。

---

## 一、背景:两边类型体系的分叉(修订)

**上游 v2.3.0**(#21123 / #24184 / #21133 等)把 `ExecutedBlock` /
`ExecutedBlockWithTrieUpdates` 统一为单一 `ExecutedBlock`。分歧比初版描述的大,
共**两个维度**:

1. **trie 槽位**:trie 数据并入 `ExecutedBlock`,类型为 `DeferredTrieData` /
   `ComputedTrieData`(定义在 **chain-state 自己的新文件
   `crates/chain-state/src/deferred_trie.rs`**,不在 reth-trie;reth-trie 里的
   是底层 `LazyTrieData` / `SortedTrieData`,`crates/trie/common/src/lazy.rs`),
   由 `StateTrieOverlayManager`(`crates/chain-state/src/state_trie_overlay.rs`)
   集中调度、延迟计算。
2. **execution_output 形状**:从多块 `Arc<ExecutionOutcome<N::Receipt>>`
   (字段 `.bundle` / `.receipts: Vec<Vec<R>>` / `.first_block`)换成**逐块**
   `Arc<BlockExecutionOutput<N::Receipt>>`(字段 `.state` /
   `.result.receipts: Vec<R>`)。这是 receipts 系列冲突和存储侧断点的根源,
   初版完全没提。

**gravity baseline** 保留二分,且比老上游(v1.8.x)多一个字段
(定义在 `crates/chain-state/src/in_memory.rs` 冲突块 1015 的 HEAD 侧):

```rust
pub struct ExecutedBlockWithTrieUpdates<N: NodePrimitives = EthPrimitives> {
    pub block: ExecutedBlock<N>,          // { recovered_block, execution_output: Arc<ExecutionOutcome>, hashed_state }
    pub trie: ExecutedTrieUpdates,        // 标准 v1 TrieUpdates(Present/Missing)
    pub triev2: Arc<TrieUpdatesV2>,       // gravity 独有:nested trie 紧凑序列化(#149)
}
```

关键事实:**pipe 路线上真正的 trie 载荷装在 `triev2`,标准 `trie` 字段反而填
`ExecutedTrieUpdates::empty()`**(见下节构造点)。`triev2` 连老上游都没有——
即使硬迁到上游统一模型,`ComputedTrieData` 也没有 `TrieUpdatesV2` 的槽位。

## 二、make-canonical 全链路类型流(行号 2026-07-03 复核)

pipe-exec 路线(gravity 不走 CL 的 newPayload/FCU,由 pipe-exec event bus 直接
注入 MakeCanonical)上,该类型的流转每一步:

1. **流水线 merklize 阶段 eager 算 state root**:
   `self.storage.state_root(&hashed_state)` 返回 `(state_root, trie_updates)`
   (`crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:713`;trait 定义
   `crates/gravity-storage/src/lib.rs:20`,返回 `TrieUpdatesV2`)。root 写进
   header(lib.rs:727)之后才 seal——`verify_executed_block_hash`(lib.rs:763)
   在 make-canonical **之前**,block hash 含 state_root,root 必须急切算出。
2. **构造载荷**:`ExecutedBlockWithTrieUpdates::new(recovered, outcome,
   hashed_state, ExecutedTrieUpdates::empty(), Arc::new(trie_updates))`
   (lib.rs:753)——第 4 参 `trie` 为空,第 5 参 `triev2` 携带真实 trie 数据。
3. **event bus 投递**:`make_canonical`(lib.rs:1407)发送
   `PipeExecLayerEvent::MakeCanonical`,事件载荷
   `MakeCanonicalEvent.executed_block: ExecutedBlockWithTrieUpdates<N>`
   (`crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs:41-43,61`)。
4. **engine tree 单线程事件循环消费**:`make_executed_block_canonical`
   (tree/mod.rs,位于冲突块 640 的 HEAD 侧):`tree_state.insert_executed` →
   设 forkchoice → `make_canonical(block_hash)` → `canonical_in_memory_state`
   更新(`in_memory.rs` 的 `update_chain` / `update_blocks`)→ `set_safe` /
   `set_finalized`(确定性共识,canonical 即 safe+finalized)。
5. **persistence 落库**:`persist_blocks(Vec<ExecutedBlockWithTrieUpdates<N>>)`
   → persistence 服务 `on_save_blocks` → gravity 写路径
   `save_blocks_per_block` / `save_merged_blocks`(persistence.rs:370/510,
   共同区)→ `provider_rw.write_trie_updatesv2(triev2)`
   (persistence.rs:485/599,共同区)。
   **⟲ f89d9d4e23**:初版所写 `writer/mod.rs:193` 路径经历了一次否定之否定:
   一次修订时上游 v2.3.0 已整体删除 `UnifiedStorageWriter`、该路径成孤儿;
   `f89d9d4e23` 整体还原 baseline 后 **`UnifiedStorageWriter` 复活**
   (writer/mod.rs:21,`save_blocks(Vec<EBWT>)` :136,`write_trie_updatesv2`
   调用现位于 :192,均实测)。初版描述重新成立,行号 193→192。

三、四节的「时序互斥 / 调度权互斥 / 数据槽位缺失」论证不变,此处不重复。

## 三、为何拒绝上游统一模型(不变)

1. **时序互斥**:上游 lazy 模型把 trie 计算延迟到 chain-state 集中调度;
   gravity 的 root 必须在 seal 之前、共识验证 block hash 之前就绪。
2. **调度权互斥**:gravity 流水线用 `merklize_barrier` 自驱动串行化 trie 计算
   (execute/src/lib.rs:711-719),与 `StateTrieOverlayManager` 是两套所有权模型。
3. **数据槽位缺失**:`ComputedTrieData` 无 `TrieUpdatesV2` 槽位;gravity 的
   persistence 依赖 `triev2` 走 `write_trie_updatesv2`。

## 四、结论与长期策略(修订)

- **本轮保留二分正确且必要**,理由同上,未变。
- **但初版对实施成本的估计错了一个量级**。四个实证:
  1. 上游 overlay 机器(`state_trie_overlay.rs` / `deferred_trie.rs` /
     `providers/state/overlay.rs` / v2.3.0 版 `payload_validator.rs` /
     v2.3.0 版 `tree/state.rs`)以**零冲突文件**形态整体入库,且
     payload_validator 是生产代码路径——「不经过上游 overlay」不成立。
  2. 多个零冲突文件已静默落在上游侧(`memory_overlay.rs`、`test_utils.rs`、
     `consistent.rs` 局部、`static_file/manager.rs` 局部等),keep-gravity 落地
     即编译失败;另有零冲突文件落在 gravity 侧(`ress/provider`、chain-state
     bench),**反向锁定**了类型选择。`grep '<<<<<<<'` 统计法天然看不见这两类。
  3. 三处「3-way merge 静默盲区」直接丢了 gravity 代码:trie-common `lib.rs`
     丢 `pub mod nested_trie;`(baseline :76 有);`chain-state/Cargo.toml` 丢
     `[[bench]] canonical_hashes_range` 段;tree/mod.rs 公共区残留上游已删的
     `version: EngineApiMessageVersion` 参数(两处)。
  4. 部分冲突块**不能取任何一边**:`Chain::new` 已定版三参(一次修订时点
     的事实;⟲ f89d9d4e23 后已反转回二参,见 §6.3)、
     persistence 的 metrics 字段已按 v2.3.0 解掉(commit `88a3ceca58`)、
     公共区已固化 crossbeam 通道——`to_chain_notification`、`on_save_blocks`
     等只能重写融合版。
- **长期 tech debt 判断不变**:v2.4+ 每轮上游碰 `ExecutedBlock` 的改动都要
  手工适配;是否一次性重建到 lazy 体系上,本轮不解决、不阻塞。

## 五、真实爆炸半径:全文件清单(替代初版第五节)

按「keep-gravity 落地必须动」的完整清单。**冲突数为实测
`grep -c '^<<<<<<<'`,格式「一次修订记录 → f89d9d4e23 后现状」;`0*` 表示
零冲突但必须改(盲区);✅ = `f89d9d4e23` 已解决。**

| # | 文件 | 冲突 | 处置(详见章节) |
|---|---|---|---|
| 1 | `crates/trie/common/src/updates.rs` | 17→0 | ✅ 整体还原 baseline,`TrieUpdatesV2` 在 :13(§6.0.1 ⟲) |
| 2 | `crates/trie/common/src/lib.rs` | 0*→0 | ✅ `pub mod nested_trie;` 已被补回(§6.0.2) |
| 3 | `crates/storage/storage-api/src/trie.rs` | 4→0 | ✅ 还原 baseline:`TrieWriterV2` 在 :121,witness 回无 mode 签名 :93(§6.1 ⟲) |
| 4 | `crates/storage/storage-api/src/{lib,block_id,state_writer}.rs` | 3/1/6→0 | ✅ 还原 baseline(§6.1 ⟲) |
| 5 | `crates/chain-state/src/in_memory.rs` | 39 | 逐块表;`to_chain_notification` 直接取 HEAD,不再重写;无新增访问器(§6.2.1 ⟲) |
| 6 | `crates/chain-state/src/memory_overlay.rs` | 0* | **方案 B:整文件复原 baseline,零补丁**(§6.2.2 ⟲,即开放问题 #5 终案) |
| 7 | `crates/chain-state/src/test_utils.rs` | 0* | 整文件复原 baseline(§6.2.3) |
| 8 | `crates/chain-state/Cargo.toml` | 0* | 补回 `[[bench]]` 段(§6.2.3) |
| 9 | `crates/chain-state/src/{state_trie_overlay,deferred_trie}.rs` + `lib.rs` 挂载 | 0* | **trim:删两文件 + 清 lib.rs:17-21**(§6.2.4 ⟲;deferred_trie 依赖的 sorted API 已随 #1 还原消失) |
| 10 | `crates/rpc/rpc-eth-api/src/helpers/pending_block.rs` | 0* | 尾部构造点就地适配(§6.3) |
| 11 | `crates/rpc/rpc-eth-types/src/pending_block.rs` | 0* | 3 处就地适配(§6.3) |
| 12 | `crates/evm/execution-types/src/chain.rs`、`exex/exex/src/wal/storage.rs` | 0* | **新断点(⟲)**:`LazyTrieData` 已成孤儿(lazy.rs 不在 mod 树),两文件须回 baseline(§6.3 ⟲) |
| 13 | `crates/storage/provider/src/providers/database/provider.rs` | 101→0 | ✅ 整体还原 baseline(与 baseline 仅 ~101 行 diff);`TrieWriterV2` impl :2297、`unwind_trie_state_range` 在 `remove_block_and_execution_above`(:2801)内(§6.4.1 ⟲) |
| 14 | `crates/storage/provider/src/providers/blockchain_provider.rs` | 46→0 | ✅ 与 baseline 零 diff(`git diff 0cb1687c1c HEAD --` 实测)(§6.4.2 ⟲) |
| 15 | `crates/storage/provider/src/providers/consistent.rs` | 0*→0 | ✅ 已还原,调 `executed_block_receipts()`,无需访问器(§6.4.3 ⟲ 作废) |
| 16 | `crates/storage/provider/src/providers/static_file/manager.rs` | 65→0 | ✅ 还原 baseline + 7 处桥接(§6.4.3 ⟲ 作废) |
| 17 | `crates/storage/provider/src/providers/rocksdb/` | 0*→— | ✅ 目录已删除(§6.4.3 ⟲ 作废) |
| 18 | `crates/storage/provider/src/providers/state/overlay.rs` | 0→— | ✅ 已删除;原「保留」处置作废(§6.4.4 ⟲) |
| 19 | `crates/storage/provider/src/writer/mod.rs` | 15→0 | ✅ `UnifiedStorageWriter` 复活(:21;`save_blocks` :136、`pipe_test` :172、`write_trie_updatesv2` :192)。原「删除 + 迁移 cfg」作废(§6.5.4 ⟲) |
| 20 | `crates/engine/tree/src/tree/state.rs` | 0* | **纯复原 baseline,不再嫁接 overlay 字段**(§6.5.1 ⟲) |
| 21 | `crates/engine/tree/src/tree/types.rs` | 0* | **删除**(§6.5.1 ⟲) |
| 22 | `crates/engine/tree/src/tree/payload_validator.rs` + `payload_processor/` | 0* | **整体复原 baseline(路线甲)**(§6.5.1 ⟲) |
| 23 | `crates/engine/primitives/src/event.rs` | 0* | 回退 baseline(EBWT 载荷)(§6.5.1) |
| 24 | `crates/engine/tree/src/engine.rs`、`tree/persistence_state.rs` | 0* | engine.rs 回 baseline(`InsertExecutedBlock(EBWT)`);persistence_state.rs 保上游版(§6.5.1 ⟲) |
| 25 | `crates/engine/tree/src/tree/mod.rs` | 84→**0** ✅ | 已落地(§6.5.2 ⟲ 落地实录;标记外编辑实为 5 处) |
| 25a | `crates/engine/tree/src/tree/block_buffer.rs` | 13→**0** ✅ | **⟲ 清单漏项,收口时补解**:HEAD 类型主轴(`RecoveredBlock`)+ v2.3.0 独立改进(IndexSet——公共区已定、Entry 去重插入、简化淘汰、`swap_remove`) |
| 25b | `tree/{instrumented_state,persistence_state,metrics}.rs`、`engine/tree/src/launch.rs` | 0* | **⟲ 收口死符号总扫新抓的 4 断点**:instrumented_state 侧翻双死符号→整文件复原(diff=0);persistence_state/metrics 的 `FastInstant`→std;launch.rs(v2.3.0 新文件引 ChangesetCache)摘 `pub mod launch;` 留孤儿——其唯一消费方 node/builder launch/engine.rs 按 node-builder 文档裁决解向 baseline 后本就不调用 |
| 25c | `crates/rpc/rpc-convert/{lib,transaction,block,receipt}.rs` | 0* | **⟲ 收口新发现**:三个 TryFrom* trait 失挂/被删,已重挂+加回(见 9.4 落地补记) |
| 26 | `crates/engine/tree/src/persistence.rs` | 18 | 融合版 `on_save_blocks`;**已解决区反向失效:`SaveBlocksMode`(:32/:313)、`bal_store()`(:329/:353/:884+)引用已消失符号,须一并清掉**(§6.5.3 ⟲) |
| 27 | `crates/engine/tree/src/tree/tests.rs` | 44 | **HEAD 为主轴**(随路线甲反转)(§6.5.5 ⟲) |
| 28 | `crates/ress/provider/*`、`crates/chain-state/benches/canonical_hashes_range.rs` | 0 | 已是 gravity 形态,不动(是「必须收 EBWT」的反向锁定) |
| 29 | `ethereum/node/src/node.rs`(26)、`node/builder/src/launch/engine.rs`(23) | 26/23 | 不在本文范围,解法须与 §6.5.1 路线甲同向;node/builder 侧另有 `ChangesetCache` 断点(launch/engine.rs、rpc.rs、launch/common.rs,grep 实测) |

> 初版第五节只列了 5 个文件;一次修订扩到 30 项;`f89d9d4e23` 解决了其中
> storage/trie 侧 11 项(✅),并新增 #12 断点与 #25/#26 的反向失效项。

**⚠ 磁盘孤儿文件警示(f89d9d4e23 的还原风格)**:storage 组保留了大量
"在磁盘但不在 mod 树"的上游文件供后续参考——已实测的有
`crates/trie/common/src/lazy.rs`(lib.rs 无 `mod lazy`)、
`crates/storage/storage-api/src/macros.rs`(lib.rs 无 `mod macros`,仍是
mode 版 delegate 宏)、`crates/trie/db/src/changesets.rs`(定义
`ChangesetCache` :352,lib.rs 无 `mod changesets`)。**写代码时不要因为
IDE/grep 能搜到这些符号就 import**——编译器看不见它们;engine 侧按 §6.5.1
复原后 payload_processor 目录下的 v2.3.0 文件(bal_prewarm_pool、
receipt_root_task 等)也会成为同类孤儿。

## 六、逐文件实施方案(代码级)

### 6.0 编译根:trie-common

#### 6.0.1 `crates/trie/common/src/updates.rs`(17 块)

> **⟲ f89d9d4e23 已解决,但走的不是本节方案**:storage 组整体还原 baseline
> (17→0,`TrieUpdatesV2` 现于 updates.rs:13),**「全取 v2.3.0 sorted API」
> 未发生**——`extend_from_sorted`/`clone_into_sorted`/`merge_batch` 等全仓
> 已不存在(grep 实测为空)。直接后果:依赖这批 API 的
> `chain-state/src/deferred_trie.rs` 从「可保留」变成**新断点**(§6.2.4 ⟲)。
> 下表原方案压缩保留作决策史(下轮 merge 若上游 sorted 化仍在,取舍逻辑可复用):

| 块 | 内容 | 解法 |
|---|---|---|
| 1(imports) | HEAD `nested_trie::Node` vs v2.3.0 `utils::{extend_sorted_vec, kway_merge_sorted}` | **并集**(两行都要) |
| 18 | HEAD 独有 `TrieUpdatesV2` / `StorageTrieUpdatesV2` 定义 | **HEAD** |
| 69, 131, 390, 453, 680, 864, 924, 1355, 1483, 1560 | v2.3.0 独有新增:`with_capacity`、`extend_from_sorted`×2、`clone_into_sorted`×2、`TrieUpdatesSorted` 的 `new/total_len/extend_ref_and_sort/merge_batch/merge_slice`、`From` impl、sorted 类型 bincode-compat + 测试 | **全取 v2.3.0**(`deferred_trie.rs` 硬依赖这批 sorted API,见 §6.2.4) |
| 207 | HEAD 的 `drain_into_sorted` 拆分 vs v2.3.0 收回 `into_sorted` | **v2.3.0**(全仓已无 `drain_into_sorted` 调用方,grep 实证) |
| 242, 284 | `into_sorted_ref` 签名现代化 | **v2.3.0** |
| 628 | `TrieUpdatesSorted` 字段私有化 + `new()` 带 sorted 断言 | **v2.3.0**(全仓无 `TrieUpdatesSorted {..}` 字面量、无外部裸字段访问,grep 实证;`StorageTrieUpdatesSorted` 字段保持 pub,`trie/db/src/changesets.rs:297` 等字面量不受影响) |
| 1639 | 测试模块改名 `tests`→`serde_tests` | **v2.3.0**(避免与新增 tests 模块重名) |

#### 6.0.2 `crates/trie/common/src/lib.rs`(零冲突,盲区)

> **⟲ f89d9d4e23 已解决**:`pub mod nested_trie;` 已随整体还原被补回
> (lib.rs:76 实测)。本节盲区消除。注意副作用:还原后的 lib.rs **不再挂载**
> `lazy.rs`/`utils.rs` 等上游新模块(成磁盘孤儿,见 §五警示),
> `LazyTrieData` 由此不可用 → §6.3 chain.rs 新断点。

**验收**:`cargo check -p reth-trie-common`(当前被 workspace 依赖缺失阻塞,
见 §七)。

### 6.1 storage-api(trie.rs 4 块 + 周边)

> **⟲ f89d9d4e23 已解决,走整体还原 baseline**(4/3/1/6 块全归零,实测):
> `TrieWriterV2` trait 在 trie.rs:121 ✓;但 **witness 回到 baseline 无 mode
> 签名**(trie.rs:93 唯一定义)——原「必须取 v2.3.0 mode 版」作废,理由
> (macros.rs 的 mode 版 delegate 宏)随 `mod macros` 挂载消失而失效:
> macros.rs 现为磁盘孤儿(lib.rs 无 `mod macros`,实测),编译器看不见它。
> 这直接改变了 §6.2.2 memory_overlay 的裁决(方案 B 反而零补丁)。
> `recover_block_number`、`UnifiedStorageWriter` 所需 trait 均随 baseline 在。
> 原方案(双取/取 v2.3.0)不再需要,一段话留档即可。

**验收**:`cargo check -p reth-storage-api`(被 workspace 依赖缺失阻塞,§七)。

### 6.2 chain-state

#### 6.2.1 `src/in_memory.rs`(39 块)

总原则:**类型定义与 BlockState 系保 HEAD;容器与测试基建随 v2.3.0
(`HashMap`→`B256Map`);`to_chain_notification` 重写(不能取边)**。

| 块(起始行) | 分类 | 解法 |
|---|---|---|
| 9, 28 | imports | **混合**,见下 |
| 72, 87, 207, 236, 1460, 1478, 1501, 1533, 1549, 1684, 1740 | `HashMap`→`B256Map`(全文件 10+ 处必须同侧) | **v2.3.0** |
| 282(`set_pending_block`)、302(`update_blocks` 泛型)、629/644/663/684/693/718(BlockState 定义与方法)、844/853/871(`ExecutedBlock` derive/字段/Default)、958(getters)、1015(`ExecutedTrieUpdates` + EBWT 定义)、1137/1147(`NewCanonicalChain` 字段)、1870(`sample_execution_outcome`) | 类型主干 | **HEAD**。块 644 顺带丢弃上游手写 `PartialEq`(derive 已含);块 958 丢弃上游 `trie_data()/trie_data_handle()/trie_updates()` |
| 899 | 上游 `ExecutedBlock::new`/`with_deferred_trie_data` | **HEAD(即整段删)**;上游侧调用方处置见 §6.2.3/§6.3/§6.5.1 |
| 730 | receipts 族 | **混合**,见下(HEAD 侧截断在函数中间,纯取 HEAD 得到残缺函数) |
| 1182, 1226, 1903, 1946 | `to_chain_notification` 与断言 | **⟲ 直接取 HEAD**(见下方反转记录;前提是 §6.3 chain.rs 回 baseline 二参) |
| 1263(`tip()`) | 签名 | **v2.3.0**(公共区函数体已是 `.recovered_block()`,取 HEAD 反而类型不匹配;调用方两形态兼容) |
| 1291, 1728 | test imports / 空链断言 | **v2.3.0**(imports 去掉 `LazyTrieData` 相关) |
| 1449 | Mock `witness` `_mode` 参数 | **⟲ 取 HEAD**(§6.1 反转:trait 已回无 mode 签名,trie.rs:93 实测) |

**imports 融合**(块 9/28):

```rust
use alloy_primitives::{map::B256Map, BlockNumber, TxHash, B256};
use reth_execution_types::{Chain, ExecutionOutcome};   // 不要 BlockExecutionOutput/Result
use reth_trie::{
    updates::{TrieUpdates, TrieUpdatesV2},
    HashedPostState,
};
// ⟲ 不再需要 LazyTrieData/SortedTrieData(to_chain_notification 直接取 HEAD)
// crate 内 import 删 ComputedTrieData, DeferredTrieData(本文件不再引用,
// 且两者已随 §6.2.4 trim 消失)
```

**块 730 最终代码**(之后直接衔接公共区的上游 iterator 版
`parent_state_chain`):

```rust
    pub fn executed_block_receipts(&self) -> Vec<N::Receipt> {
        let receipts = self.receipts();
        debug_assert!(receipts.len() <= 1, "Expected at most one block's worth of receipts");
        receipts.first().cloned().unwrap_or_default()
    }
```

> **⟲ f89d9d4e23**:原方案在此处新增 `executed_block_receipts_ref` 适配
> (当时 consistent.rs 零冲突调用点用 `_ref` 名)。consistent.rs 已被整体
> 还原 baseline(现调 `executed_block_receipts()`,:1070-1143 实测),
> **`_ref` 访问器不再需要**。

**⟲ f89d9d4e23:`to_chain_notification` 重写整段作废,直接取 HEAD**。
决策史:一次修订时 `chain.rs` 是零冲突的 v2.3.0 版,`Chain::new`/
`append_block` 三参(第三参 `LazyTrieData`)是硬约束,因此设计了
`lazy_trie_data`/`blocks_to_chain` 融合重写(deferred 排序、fold 语义保真,
详见本文件 git 历史 d12192ddeb 之后的首版)。`f89d9d4e23` 还原 trie-common
后 `LazyTrieData` 成孤儿(lib.rs 无 `mod lazy`,实测),chain.rs 反而成了
断点、必须回 baseline 二参(§6.3 ⟲)——HEAD 侧的二参
`Chain::new(blocks, outcome)` / `append_block(block, outcome)` 调用**原样
成立**,块 1182/1226/1903/1946 全部直接取 HEAD,公共区的上游
`blocks_to_chain` 主体(用 `trie_data_handle()`)整段删除。

**公共区盲区**(仍有效):`test_state_receipts`(~1573,无冲突标记)断言是
上游平面版,须改回 baseline 的 `assert_eq!(state.receipts(), &receipts)`。

**⟲ f89d9d4e23:原「新增兼容访问器」(`block_receipts`/`bundle_state`)
作废**——它们是为 §6.4.3 的三个零冲突断点(manager.rs/rocksdb 逐块 API
调用)设计的;storage 还原后那些调用点已不存在(§6.4.3 ⟲),不要再加。

#### 6.2.2 `src/memory_overlay.rs`(零冲突已上游化;开放问题 #5 终案)

> **⟲ f89d9d4e23 反转:终案改为方案 B——整文件复原 baseline,零补丁**:
>
> ```bash
> git checkout 0cb1687c1c -- crates/chain-state/src/memory_overlay.rs
> ```
>
> 一次修订裁决方案 A(上游版为底 + 4 hunk)所依据的三个前提已全部随
> storage 还原消失(均实测):
> 1. ~~trait `witness` 带 mode 参数~~ → 已回 baseline 无 mode 签名
>    (storage-api/src/trie.rs:93 唯一定义);baseline memory_overlay 的
>    `witness(input, target)` 原样匹配。
> 2. ~~`delegate_provider_impls!` 宏可用~~ → `reth_storage_api::macros`
>    不在 mod 树(lib.rs 无 `mod macros`,macros.rs 成磁盘孤儿);现文件
>    :286 的宏调用反而是断点。
> 3. ~~`TrieInput::from_blocks` 非 Option 签名~~ → Option 签名已回归
>    (trie/common/src/input.rs:36-38 实测
>    `(&HashedPostState, Option<&TrieUpdates>)`);baseline 的
>    `from_blocks(iter.map(|b| (b.hashed_state.as_ref(), b.trie.as_ref())))`
>    原样编译。
>
> 换句话说:**当前磁盘上的上游版文件在还原后的 workspace 里三处必炸,而
> baseline 版零补丁可用**。方案 A 的 4-hunk 设计(含手写 `trie_input()`
> 循环还原 Missing 语义)留在本文件 git 历史里作决策史——若下轮 merge
> 上游又把这三个前提立起来,方案 A 的推理可直接复用。

语义注记(两案等价,仍有效):pipe 路线 `trie = Present(empty)` → nodes 空、
state 全量;`trie_input()` 是 lazy,只在 RPC 侧
`state_root_*`/`proof`/`witness`/`storage_root` 触发,make-canonical 主链路
不碰。消费方(`in_memory.rs` 的 `state_provider()`、tree 的
`StateProviderBuilder`、`ress`、bench)在各自按本文档解完后类型自动对上。

#### 6.2.3 `src/test_utils.rs` + `Cargo.toml`(零冲突,盲区)

- `git checkout 0cb1687c1c -- crates/chain-state/src/test_utils.rs`。
  baseline = v1.8.4 + 1 行(`triev2: Default::default()`),
  `get_executed_block*` 返回 EBWT;rand API baseline 已是 `rand::rng()`,
  可直接用。上游版新增 helper 的消费方只有上游侧测试(分别在 §6.2.4/§6.5.5
  处置),复原自洽。
- `chain-state/Cargo.toml` 补回被 merge 静默丢弃的段:

```toml
[[bench]]
name = "canonical_hashes_range"
harness = false
required-features = ["test-utils"]
```

#### 6.2.4 `src/{state_trie_overlay,deferred_trie}.rs`(⟲ 反转:trim)

> **⟲ f89d9d4e23 反转:两文件删除 + `lib.rs:17-21` 挂载清理**(删
> `mod deferred_trie; pub use deferred_trie::*;` 与
> `mod state_trie_overlay; pub use state_trie_overlay::*;`)。
>
> 一次修订裁决「适配保留」的两个支柱都塌了(实测):
> 1. ~~「不 trim 因 overlay.rs → trie/parallel 依赖链」~~ →
>    `providers/state/overlay.rs` 已被 f89d9d4e23 删除(目录实测只剩
>    historical/latest/macros/mod);payload_processor 对 overlay 的引用
>    全在 `#[cfg(test)]` 区(mod.rs:1036 起,引用点 :1052/:1386),且该
>    目录按 §6.5.1 路线甲整体复原后引用随之消失。
> 2. ~~「deferred_trie.rs 可原样保留」~~ → 它依赖的
>    `clone_into_sorted`/sorted API 已随 §6.0.1 的 baseline 还原消失
>    (全仓 grep 为空),deferred_trie.rs 现在自己就是断点。
>
> 原「换型适配 + `sorted_trie_data` helper」方案见本文件 git 历史,作决策史。
> trim 后 `StateTrieOverlayManager` 的其余消费方(tree/state.rs、
> payload_validator)按 §6.5.1 路线甲复原 baseline,同步消失,无级联。

**验收**:`cargo check -p reth-chain-state` +
`cargo check -p reth-chain-state --features test-utils --benches`
(被 workspace 依赖缺失阻塞,§七)。

### 6.3 叶子文件(零冲突,定向小改)

- `crates/evm/execution-types/src/chain.rs`:**⟲ f89d9d4e23 反转:从「原样
  保留 + 三参硬约束」变为新断点,须回 baseline 二参版**。它仍是 v2.3.0 版
  (:16 `use reth_trie_common::LazyTrieData`、:43/:72/:85/:107,实测),而
  `LazyTrieData` 定义已成磁盘孤儿(trie-common lib.rs 无 `mod lazy`,实测)
  → import 必断。回 baseline 后,原「三参硬约束」作废:HEAD 侧全部二参
  `Chain::new`/`append_block` 调用(in_memory.rs ×5、blockchain_provider.rs
  ×2——后者已随 §6.4.2 还原解决)原样成立,§6.2.1 的
  `to_chain_notification` 因此直接取 HEAD。
- `exex/exex/src/wal/storage.rs`:**⟲ 随 chain.rs 回 baseline**(:192 import
  `LazyTrieData`、:302 `LazyTrieData::ready`,实测,同一断点)。
- `trie/common/src/lazy.rs`:已是磁盘孤儿,留档不动、勿引用(§五警示)。
- `crates/rpc/rpc-eth-api/src/helpers/pending_block.rs`:**勿整文件复原**
  (其余部分依赖 v2.3.0 evm API)。只改尾部构造(:435-443)+ :11 imports
  (去 `ComputedTrieData`),按 baseline :375-387 形态:

```rust
    let execution_outcome = ExecutionOutcome::new(
        db.take_bundle(),
        vec![execution_result.receipts],
        block.number(),
        vec![execution_result.requests],
    );
    Ok(ExecutedBlock {
        recovered_block: block.into(),
        execution_output: Arc::new(execution_outcome),
        hashed_state: Arc::new(hashed_state),
    })
```

- `crates/rpc/rpc-eth-types/src/pending_block.rs`:3 处就地适配——
  ① :12 imports 补 `ExecutedBlockWithTrieUpdates, ExecutedTrieUpdates`;
  ② :102 receipts 提取改
  `executed_block.execution_output.receipts.iter().flatten().cloned().collect()`;
  ③ :160-164 `From<PendingBlock> for BlockState` 改 baseline 包装
  (`ExecutedBlockWithTrieUpdates::new(.., ExecutedTrieUpdates::Missing, Default::default())`)。

### 6.4 storage/provider

#### 6.4.1 `providers/database/provider.rs`(101 块 → 0)

> **⟲ f89d9d4e23 已解决,走整体还原 baseline**(现文件与 baseline 仅
> ~101 行 diff,实测)。一次修订的方案(TYPE 8 块手术、`save_blocks`
> 拆包适配、unwind 双路径并存、`TrieWriterV2` impl 手工重组)**全部不再
> 需要**——baseline 版自带这一切:`TrieWriterV2 for DatabaseProvider` impl
> 在 :2297、`unwind_trie_state_range` 逻辑在
> `remove_block_and_execution_above`(:2801,baseline 二参签名
> `(block, remove_from: StorageLocation)`)内、`commit_view` 基建齐全
> (均实测)。原方案细节见本文件 git 历史,作决策史。
>
> **对下游的接口事实**(engine 侧解冲突要用):provider 级
> `DatabaseProvider::save_blocks` / `SaveBlocksMode` / `bal_store()`
> **均已不存在**(定义 grep 为空,实测)——落库入口回到
> `UnifiedStorageWriter::save_blocks(Vec<EBWT>)`(writer/mod.rs:136)与
> persistence 层 gravity 函数;engine 侧凡引用这三个符号的"已解决区"代码
> 都是反向失效点(§6.5.3 ⟲)。

#### 6.4.2 `providers/blockchain_provider.rs`(46 块 → 0)

> **⟲ f89d9d4e23 已解决**:与 baseline **零 diff**
> (`git diff 0cb1687c1c HEAD -- <path>` 为空,实测)。一次修订方案
> (非测试取 v2.3.0、测试取 HEAD、两处 interleave 陷阱)未被采用、不再
> 需要;上游特性(BAL store、`PersistedBlockSubscriptions`、
> `block_by_transaction_id`)随之整体放弃。原方案见 git 历史。

#### 6.4.3 零冲突断点(⟲ 作废)

> **⟲ f89d9d4e23:本节三个断点全部随还原消失,整节作废**——
> `static_file/manager.rs` 已还原 baseline + 7 处桥接(65 块归零);
> `rocksdb/provider.rs` 随目录删除;`consistent.rs` 已还原(调
> `executed_block_receipts()`,:1070-1143 实测)。§6.2.1 的
> `block_receipts`/`bundle_state` 兼容访问器因此不再需要(已同步作废)。

#### 6.4.4 `providers/state/overlay.rs`

> **⟲ f89d9d4e23:文件已删除**(目录实测只剩 historical/latest/macros/mod),
> 原「保留不动」处置作废。

**验收**:`cargo check -p reth-provider` → `cargo test -p reth-provider --no-run`
(被 workspace 依赖缺失阻塞,§七;storage 组声明 baseline 可编译,转引自
其 `STORAGE-RESOLUTION-TODO.md`,未独立验证)。

### 6.5 engine-tree

#### 6.5.1 newPayload 集群路线裁决(本轮最大的隐藏工作量)

> **⟲ f89d9d4e23 反转:路线乙地基坍塌,当初被否的路线甲(整体复原 baseline)
> 成为唯一自洽路线。engine 组按本节新版执行,勿按 git 历史里的旧版。**

**决策史**(下轮 merge 复用,勿删):一次修订时两条路线的裁决证据——
路线甲被否,因 baseline validator 调
`payload_processor.spawn(env, txs, provider_builder, consistent_view,
trie_input, config)` 并依赖 `take_trie_input()`,而当时 worktree 的
payload_processor 是纯 v2.3.0 版(`spawn` 已改
`(.., multiproof_provider_factory, config, parallel_bal_execution)`、
`take_trie_input` 已删),复原 validator 会级联复原整个 processor 子系统;
路线乙(保 v2.3.0 骨架 + 输出类型 5 处手术)因此当选,并需给 gravity
state.rs 嫁接 `state_trie_overlays` 字段(6 处)、types.rs 换字段型、
validator 内 EBWT 构造(细节见本文件 git 历史)。

**反转依据**(2026-07-03 实测,f89d9d4e23 之后):路线乙依赖的三根柱子全断——

1. `DatabaseProviderROFactory` trait **定义全仓不存在**(grep 为空);
   v2.3.0 版 payload_processor 生产区 :26(import)/:298/:390(trait bound)
   引用它 → processor 自身已编译断。
2. `OverlayBuilder`/`OverlayStateProviderFactory` **随
   `providers/state/overlay.rs` 删除而消失**(§6.4.4 ⟲);v2.3.0 版
   validator 的 `overlay_builder_for_parent`(原 :1792 一带)与 processor
   测试区(:1052/:1386)引用它们。
3. `ChangesetCache` **成磁盘孤儿**(定义在 trie/db/src/changesets.rs:352,
   但 trie/db lib.rs 无 `mod changesets`,实测);消费方 engine
   mod.rs:96 `use reth_trie_db::ChangesetCache`、node/builder 三处
   (launch/engine.rs、rpc.rs、launch/common.rs)全部断。

而当初否掉路线甲的理由(baseline validator ↔ v2.3.0 processor 签名不匹配)
在「**validator 与 processor 一起复原 baseline**」时不存在——baseline 两者
本就互相匹配。storage 组的 `STORAGE-RESOLUTION-TODO.md` 也明确要求下游
同向 keep-gravity。(为什么保留而非删除这条 gravity 运行时不用的路径:
见 §9.1 末 FAQ 注记——「死代码保编译、不保运行」。)

**落地清单(路线甲)**:

1. **`tree/payload_processor/` 整目录复原 baseline**:
   `git checkout 0cb1687c1c -- crates/engine/tree/src/tree/payload_processor`。
   注意 checkout 不会删除 v2.3.0 新增文件(bal_prewarm_pool.rs、
   receipt_root_task.rs、preserved_sparse_trie.rs、configured_sparse_trie.rs
   等)——它们会成为磁盘孤儿(不在 baseline mod.rs 树),与 storage 组
   风格一致,留档勿引用(§五警示)。
2. **`tree/payload_validator.rs` 整文件复原 baseline**(gravity 对它仅
   +1 行 delta:`triev2: Default::default()`,复原即含)。
3. **恢复 option C 删除的 `tree/cached_state.rs`**(⟲ §9.1 = 选项 a,
   2026-07-05 已裁决):
   `git checkout 0cb1687c1c -- crates/engine/tree/src/tree/cached_state.rs`;
   engine-tree Cargo.toml 被删 5 行与 mod.rs 的 `mod cached_state;` 声明随
   §6.5.2 冲突解决带回。依据:baseline validator/processor 引用它 18 处
   (§9.1 实测);`reth-engine-execution-cache` 新 crate 留作 workspace 孤儿。
4. **`tree/state.rs` 纯复原**:
   `git checkout 0cb1687c1c -- crates/engine/tree/src/tree/state.rs`。
   **不再嫁接** `state_trie_overlays` 6 处——`StateTrieOverlayManager` 已随
   §6.2.4 trim 消失,嫁接对象不存在。
5. **`tree/types.rs` 删除**(v2.3.0 新文件;`ValidationOutcome` 回到
   baseline validator 内定义):`git rm` + mod.rs 解冲突时删
   `pub mod types;` 与 `pub use types::{..}`。
6. **`engine/primitives/src/event.rs` 回 baseline**(`CanonicalBlockAdded` /
   `ForkBlockAdded` 载荷 EBWT;mod.rs 公共区 `emit_event` 传的就是 EBWT,
   回退后 mod.rs 零改动)。
7. **`engine.rs` 回 baseline**(`EngineApiRequest::InsertExecutedBlock(
   ExecutedBlockWithTrieUpdates<N>)`;上游 `BuiltPayloadExecutedBlock` 变体
   随 validator 复原失去接收方)。`tree/persistence_state.rs` **保持上游版**
   (类型无关,crossbeam `Receiver<PersistenceResult>` 与 §6.5.3 配套;
   ⟲ 决策原则 3:不冲突的 v2.3.0 设计保留)。
8. **engine mod.rs:96 断点**:删 `use reth_trie_db::ChangesetCache;`,并连带
   §6.5.2 中 changeset_cache/runtime/overlay 相关取向反转(见该节 ⟲ 注记)。

> 同向约束:`ethereum/node/src/node.rs`(26 块)与
> `node/builder/src/launch/engine.rs`(23 块)是 validator 构造方与
> InsertExecutedBlock 发送方,解冲突须与路线甲同向(基本等于贴 HEAD 侧);
> node/builder 侧另有三处 `ChangesetCache` 断点(§五 #29),不在本文展开。

#### 6.5.2 `tree/mod.rs`(84 块 = PIPE 28 + OTHER 56 + 3 处标记外编辑)

> **⟲ f89d9d4e23 重估注记**:下表若干行是在路线乙前提下裁决的,随 §6.5.1
> 反转而变(受影响行已就地标 ⟲):凡「取 v2.3.0」的理由是配合上游
> validator/processor/changeset/overlay 的,反转为贴 HEAD;新增断点
> :96 `use reth_trie_db::ChangesetCache;` 须删(§6.5.1 第 8 点)。其余行
> (通道/持久化服务形态 follow v2.3.0、pipe 三函数保 HEAD、`notify_waiters`
> 插入、3 处标记外编辑)**不受影响,仍有效**。OTHER 桶 56 块中纯上游演进
> (FCU/newPayload 重构等)的取向以「与复原后的 baseline
> validator/state.rs API 兼容」为前提重新过一遍——预期其中 newPayload
> 重构组会部分改为贴 HEAD,以 `cargo check -p reth-engine-tree` 编译驱动。
>
> **⟲ 决策原则(§九,2026-07-05)对本表的统一口径**:通道/服务形态/
> 与 storage 无关的上游重构 = 不冲突 → 按原则 3 保留 v2.3.0;凡取 v2.3.0
> 的理由绑定已消失符号(`ChangesetCache`/`FastInstant`/`SaveBlocksMode`/
> `bal_store` 等)或绑定 v2.3.0 版 validator/processor/overlay = 冲突 →
> 按原则 2 贴 HEAD/删除。逐行 ⟲ 标记即此口径的落点。

| 块 | 内容 | 解法 |
|---|---|---|
| 6(imports) | chain-state 类型 | 混合:HEAD 的 `ExecutedBlockWithTrieUpdates, ExecutedTrieUpdates` + `gravity_primitives::get_gravity_config` + v2.3.0 的 `B256Map` 等;**手工加回 `ForkchoiceStatus`**(make_executed_block_canonical 用,v2.3.0 仍导出) |
| 64(imports) | provider/trie/std | 并集:v2.3.0 全量 + HEAD 的 `DBProvider, BlockNumReader, StateRootProvider`、`reth_trie::{HashedPostState, TrieInput}`、`reth_trie_db::DatabaseHashedPostState`、`std::sync::mpsc::{RecvError, RecvTimeoutError}`(pipe 事件通道仍 std mpsc);删 HEAD 死 import(`ConsistentDbView`/`EvmState`/`StateChangeSource`/`OnStateHook`) |
| 141 | re-export | 并集:HEAD `KeccakKeyHasher` + v2.3.0 全部 |
| 164 | 常量 | 并集:`MAX_BLOCKS_TO_PERSIST` + `CHANGESET_CACHE_RETENTION_BLOCKS` |
| 185/199 | `StateProviderBuilder.overlay` | **HEAD**:`Option<Vec<ExecutedBlockWithTrieUpdates<N>>>` |
| 245/258 | `EngineApiTreeState::new`/`TreeState::new` | **⟲ 全取 HEAD**(state.rs 纯复原后仍是二参 `new(head, engine_kind)`;块 621 一带同取 HEAD,无 overlay manager 可传) |
| 445/568 | handler 字段/初始化 | **⟲ HEAD 为底**:保 `persistence_waiters: PersistenceWaiters`;v2.3.0 四件套中 `changeset_cache`(定义成孤儿)必丢,`runtime`/`execution_timing_stats`/`building_payload` 凡引用已消失符号的丢弃,编译驱动裁决 |
| 640 | spawn_new 尾 + pipe 三函数 | **并集(核心手术)**:v2.3.0 spawn 尾(`spawn_os_thread`)+ `valid_outcome`,再原样保留 HEAD 的 `try_recv_pipe_exec_event` / `on_pipe_exec_event` / `make_executed_block_canonical`。⟲ spawn 尾传参里 `changeset_cache`/`runtime` 随 445/568 行裁决削减 |
| 1321/1341 | on_new_head 回溯 | **HEAD**(old = 内层 `ExecutedBlock`,new = EBWT,与 gravity `NewCanonicalChain::Reorg` 匹配) |
| 1451 | collect_blocks_for_canonical_unwind | **HEAD**(`block_ref().block.clone()`) |
| 1471 | apply_canonical_ancestor_via_reorg / is_fork | **HEAD**(`ExecutedTrieUpdates::Missing` 包装是 gravity 语义必需);`is_fork` 若因块 4156 取 v2.3.0 而失去调用点,删或 `#[allow(dead_code)]` |
| 2062 | try_recv_engine_message / remove_blocks / persist_blocks 签名 | 混合:删 HEAD `try_recv_engine_message`(上游 `wait_for_event` 取代);`persist_blocks(&mut self, Vec<ExecutedBlockWithTrieUpdates<N>>)` + 内部 `crossbeam_channel::bounded(1)` |
| 2133/2141 | persist_blocks 体 | v2.3.0(crossbeam) |
| 2154 | advance_persistence / on_persistence_complete | **v2.3.0 结构 + 插一行**(不插则 pipe 的 WaitForPersistence 永不唤醒,共识层持久化屏障死锁):`self.persistence_state.finish(hash, number);` 之后加 `self.persistence_waiters.notify_waiters(number);`。⟲ v2.3.0 侧的 `changeset_cache.evict` 一并删(符号已消失) |
| 2912/2929 | should_persist | 混合:v2.3.0 判定 + 恢复 HEAD 的 waiters 快速路径(`if !self.persistence_waiters.is_empty() { return true }`;`building_payload`/backfill 检查保 v2.3.0;去 `const`) |
| 2944-3077 | get_canonical_blocks_to_persist | **混合签名**:`(&mut self, target: PersistTarget) -> Result<Vec<EBWT>, ..>`——`&mut` 是 HEAD trie 重算循环需要,`PersistTarget` 是 v2.3.0 `persist_until_complete` 需要;`Threshold` 臂内保 HEAD 的 gravity 批量上限逻辑(`persist_merge_blocks`/`cache_max_persist_gap`),`Head` 臂到 canonical head;HEAD 的 trie 重算 for 循环整段保留 |
| 3107-3230 | canonical_block_by_hash | **HEAD 全取**(gravity 3 字段构造;上游 changeset 重建逻辑不要),**追加** v2.3.0 新增的 `fn has_block_by_hash`(后续 v2.3.0 块依赖) |
| 3906 | reinsert_reorged_blocks | **HEAD**(`Vec<EBWT>`) |
| 4156(含 4144 一带) | insert_block_or_payload | **⟲ 取 HEAD**(路线甲:baseline validator API,无 TreeCtx 两参/ValidationOutput)。连带:1471 行注记里的 `is_fork` 恢复调用点,不再是死代码 |
| 4340 | compute_trie_input | **HEAD 整段**(依赖 `TrieInput::extend_with_blocks`/`KeccakKeyHasher`/`DatabaseHashedPostState`,v2.3.0 均在,已核) |
| 4817 | 尾部类型 | v2.3.0(空),再在公共区尾部**追加 HEAD 的 `enum PersistingKind` + impl**(上游已无此类型,`persisting_kind_for`/`compute_trie_input` 需要) |

**OTHER 桶 56 块,全取 v2.3.0**,分组:FCU 重构(1737、1829、2409、2460、
4676、4686、4700)/ newPayload 重构(980、1069、1079、3366、3445、3466、
3476、3508、3542、4016、4048、4089、4108、4118、4132、4474、4516)/
引擎消息与通道(40、404、485、505、526、541、587、599、2331、2355、2378、
2492、2505)/ backfill(2700、3608、3691)/ metrics 与日志(3834、3879、
4550、4582、4616,匹配已解决的 metrics.rs)/ 杂项 API(120、141 局部、3011、
3137、3260、3278、3292、4804、4877)。

**3 处冲突标记之外的手工编辑(盲区,grep 冲突标记发现不了)**:

1. :1734 删 `version: EngineApiMessageVersion,`(`on_forkchoice_updated`
   残留参数,上游已删,调用点全是无 version 调用);
2. :4683 同上(`process_payload_attributes`);连带 :47 import 删
   `EngineApiMessageVersion, PayloadBuilderAttributes`;
3. 块 3691 陷阱:`find_disk_reorg` 的**函数签名+前半段**(两基线逐字节相同)
   被 merge 塞进了 HEAD 侧,公共区只剩函数体后半。**纯取 v2.3.0 会把函数开头
   删掉,文件不 parse**。解法:取 v2.3.0 侧后,把 HEAD 侧中
   `fn find_disk_reorg` 起至 `while canonical.number > persisted.number {..}`
   止的段落原样接回。

#### 6.5.3 `persistence.rs`(18 块)

定盘:**载荷 keep-gravity(`Vec<EBWT>`),通道/服务生命周期 follow v2.3.0
(crossbeam + `PersistenceResult` + `ServiceGuard` + pruner;⟲ 决策原则 3:
此类与 storage 还原面无冲突的 v2.3.0 设计保留)**。公共区已
固化上游形态(tree/mod.rs:132 已 `use crate::persistence::PersistenceResult`、
persistence.rs:667 已 crossbeam、struct pending 字段已在 :98-103),纯 HEAD
解**必然编译失败**(另两个实证:metrics 字段已按 v2.3.0 解掉
——commit `88a3ceca58`;HEAD 侧引用的 `sf_provider` 定义行已被公共区吞掉)。

> **⟲ f89d9d4e23 新增一类盲区:storage 还原引发的"已解决区反向失效"**——
> 本文件已按 v2.3.0 侧解掉/公共区固化的代码,引用了 storage 还原后**已不
> 存在**的符号(定义 grep 为空,实测):`SaveBlocksMode`(:32 import、
> :313 `provider_rw.save_blocks(blocks, SaveBlocksMode::Full)`——provider 级
> `save_blocks` 本身也没了)、`bal_store()`(:329/:353、:884 起的 BAL
> 测试)。解 18 块时**一并清掉**:v2.3.0 侧 `on_save_blocks`(:296-364 一带)
> 整段被 P6 融合版取代,天然消掉 :313;`maybe_run_pruner` 里的 BAL prune 与
> P15-P18 中的 BAL 测试删除。这类失效不在任何冲突标记内,是「冲突标记归零
> ≠ 完成」的又一实证。

| 块 | 解法 |
|---|---|
| 2-37、43-74(imports + 常量/类型) | 混合:gravity `MERGE_GROUP_*` 常量 + 上游 `PersistenceResult` 都保留;import 融合要点:去 HEAD 的 `tokio oneshot`,保 `crossbeam_channel::Sender as CrossbeamSender`、`get_gravity_config`、`ExecutedBlockWithTrieUpdates`、`TrieWriterV2`、`spawn_os_thread`、`StageCheckpoint(Writer)`、`PERSIST_BLOCK_CACHE`、`set_fail_point`。⟲ 两处修正:`Instant` 用 `std::time::Instant`(原方案的 `FastInstant as Instant` 已不可用——`FastInstant` 定义随 primitives 还原消失,全仓 grep 为空,见非决策注记);`UnifiedStorageWriter` **不再删**(§6.5.4 ⟲ 已复活,共同区 HEAD 代码若引用则保留 import,编译驱动) |
| 148、163、206 | v2.3.0(`result.last_block` 提取;`maybe_run_pruner` + pending 缓存——⟲ 去 BAL prune;`remove_block_and_execution_above` + `commit`——⟲ **改二参** `remove_block_and_execution_above(n, StorageLocation::Database)`,baseline 签名 provider.rs:2801 实测) |
| 219-364(核心块) | **融合**:HEAD 的 `get_checkpoint`/`update_checkpoint` 原样 + v2.3.0 的 `maybe_run_pruner` 原样 + `on_save_blocks` 融合版(下) |
| 657、762 | `SaveBlocks(Vec<ExecutedBlockWithTrieUpdates<N>>, CrossbeamSender<PersistenceResult>)`;handle `save_blocks` 同步 |
| 681、691、722、773、788、811 | v2.3.0(`_service_guard`、spawn、doc、`ServiceGuard`) |
| 848-989(测试) | v2.3.0(HEAD 残片是与公共区 crossbeam 版重复的旧 oneshot 测试) |

**`on_save_blocks` 融合版**(生产 triev2 写入走 gravity 共同区函数,与
§6.4.1 的 provider `save_blocks` 是两条并存路径,互不重复写):

```rust
#[instrument(level = "debug", target = "engine::persistence", skip_all, fields(block_count = blocks.len()))]
fn on_save_blocks(
    &mut self,                                     // pending take 需要 &mut
    blocks: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
) -> Result<PersistenceResult, PersistenceError> {
    let first_block = blocks.first().map(|b| b.recovered_block.num_hash());
    let last_block = blocks.last().map(|b| b.recovered_block.num_hash());
    let block_count = blocks.len();

    let pending_finalized = self.pending_finalized_block.take();
    let pending_safe = self.pending_safe_block.take();

    let start_time = Instant::now();

    if let Some(last) = last_block {
        // gravity 写路径:staged per-block 提交或 merge-group 提交
        // (内部含 write_trie_updatesv2,persistence.rs:485/599 共同区)
        if get_gravity_config().persist_merge_blocks {
            self.save_merged_blocks(blocks)?;
        } else {
            self.save_blocks_per_block(blocks)?;
        }

        // pipeline 进度 + 顺延的 finalized/safe 标记共享一次提交
        let provider_rw = self.provider.database_provider_rw()?;
        provider_rw.update_pipeline_stages(last.number, false)?;
        if let Some(finalized) = pending_finalized {
            provider_rw.save_finalized_block_number(finalized.min(last.number))?;
            if finalized > last.number { self.pending_finalized_block = Some(finalized); }
        }
        if let Some(safe) = pending_safe {
            provider_rw.save_safe_block_number(safe.min(last.number))?;
            if safe > last.number { self.pending_safe_block = Some(safe); }
        }
        provider_rw.commit()?;
    }

    let elapsed = start_time.elapsed();
    self.metrics.save_blocks_batch_size.record(block_count as f64);
    self.metrics.save_blocks_duration_seconds.record(elapsed);   // 只能用 v2.3.0 字段(88a3ca 已定)
    Ok(PersistenceResult { last_block, commit_duration: Some(elapsed) })
}
```

(⟲ f89d9d4e23 定案:`bal_store()` 已随 storage 还原消失——上游
`self.provider.bal_store().flush()` 两行**不加**,`maybe_run_pruner` 的 BAL
prune 与 P15-P18 的 BAL 测试一并删,原「随 BAL 总决策」的待定项关闭。)

#### 6.5.4 `writer/mod.rs`(15 块 → 0)

> **⟲ f89d9d4e23 已解决,方向与原方案相反**:整体还原 baseline,
> `UnifiedStorageWriter` **复活**(struct :21;`save_blocks(Vec<EBWT>)`
> :136、`#[cfg(not(feature = "pipe_test"))]` :172、`write_trie_updatesv2`
> 调用 :192,均实测)。原「删除 + `pipe_test` cfg 迁移」方案作废
> (细节见 git 历史);`pipe_test` 行为原地保留,无迁移风险。
> 对调用方的提示:persistence/stages/cli/db-common 里 HEAD 侧的
> `UnifiedStorageWriter::commit(..)` 写法重新可用,这些文件解冲突时
> **贴 HEAD 侧即可**(上游侧 `provider_rw.commit()` 写法也编译,但与
> baseline 风格不一致,不取)。

#### 6.5.5 `tree/tests.rs`(44 块)

**⟲ f89d9d4e23 反转:HEAD 为主轴**(随 §6.5.1 路线甲;baseline = v1.8.4 +
gravity 测试 199 行,与复原后的 mod.rs/state.rs/payload_validator 自洽)。
v2.3.0 侧块引用的 `BasicEngineValidator`/`TreeCtx` 新形态、`ChangesetCache`、
overlay manager 在路线甲下**全部不可用**(符号已消失/将随复原消失)——原
「v2.3.0 harness 为底」方案作废(⟲ 决策原则 2:v2.3.0 harness 绑定
v2.3.0 validator API,后者按 §9.1 裁决出局,harness 随之)。gravity 专属测试
(`test_make_executed_block_canonical_sets_safe_and_finalized` 等)在 HEAD
侧原样保留。这仍是全链路里确定性最低的文件,放最后、以
`cargo test -p reth-engine-tree --no-run` 编译驱动收敛。

**验收**:`cargo check -p reth-engine-tree` →
`cargo check -p reth-pipe-exec-layer-ext-v2 -p reth-pipe-exec-layer-event-bus`
(pipe 两 crate 现零冲突,是类型回归的金丝雀)→ 全 workspace
`cargo check --workspace --all-features`。

## 七、实施顺序与验收锚点

依赖序(每步以对应 `cargo check -p` 为门禁,红了不进下一步)。
**⚠ 当前编译门禁整体被阻塞**:`cargo metadata` exit=101——workspace 根缺约
20 个 dep 定义,属 cargo 组范围(见 storage 组 `STORAGE-RESOLUTION-TODO.md`
第三轮)。在其修复前,下列步骤只能以「冲突标记归零 + 结构性核对」推进,
编译证据事后回填:

1. ~~`trie/common`~~ ✅ 已由 f89d9d4e23 完成(§6.0 ⟲)
2. ~~`storage-api`~~ ✅ 已由 f89d9d4e23 完成(§6.1 ⟲)
3. 叶子断点先行:`chain.rs` + `exex/wal/storage.rs` 回 baseline(§6.3 ⟲,
   是 in_memory.rs 取 HEAD 的前提)
4. `chain-state`:in_memory.rs(§6.2.1)→ test_utils.rs 复原 + Cargo.toml
   补 bench(§6.2.3)→ memory_overlay.rs 复原 baseline(§6.2.2 ⟲)→
   state_trie_overlay.rs + deferred_trie.rs trim + lib.rs 清挂载(§6.2.4 ⟲)
   → `cargo check -p reth-chain-state --features test-utils`
5. rpc pending_block ×2(§6.3)
6. ~~`storage/provider`~~ ✅ 已由 f89d9d4e23 完成(§6.4 ⟲,含 writer/mod.rs)
7. `engine-tree`(全部按 §6.5.1 路线甲):payload_processor 目录 +
   payload_validator + state.rs 纯复原、**cached_state.rs 恢复(§9.1=a)**、
   types.rs 删除、event.rs/engine.rs 回 baseline(§6.5.1 ⟲)→ mod.rs
   (§6.5.2,含 :96 断点与 ⟲ 行)→ persistence.rs(§6.5.3,含反向失效
   清理)→ tests.rs(§6.5.5 ⟲)
   → `cargo check -p reth-engine-tree` + pipe 两 crate + `--workspace`
8. rpc 侧收尾:`rpc-provider` 整 crate 回 baseline(§9.4=a)+ rpc-builder
   解其 49 块时按 §9.3 裁决 trim `PersistedBlockSubscriptions` 链
   (端点 + bound 随 baseline 侧)
   → `cargo check -p reth-storage-rpc-provider -p reth-rpc-builder`

四条经验教训(本轮实证,写给下轮 merge):

- **⟲ 分组解冲突会产生"反向失效"**:一个组整体还原 baseline 后,另一个组
  **已解决区**里按上游侧解掉的代码会引用已消失的符号(persistence.rs 的
  `SaveBlocksMode`/`bal_store()`,§6.5.3 ⟲),同时留下"磁盘孤儿文件"
  (在磁盘但不在 mod 树,IDE/grep 可见、编译器不可见,§五警示)。跨组解
  冲突后必须互相重扫一遍对方地盘的符号引用,且孤儿文件勿引用。

- **`grep -c '<<<<<<<'` 不是完备的工作量度量**:零冲突文件可能整体落在任一侧
  (memory_overlay/test_utils 落上游、ress 落 gravity),公共区可能残留另一侧
  已删符号(version 参数)或吞掉本侧定义(`sf_provider`、`find_disk_reorg`
  开头)。收尾验收只能以 **crate 级编译**为准。
- **冲突块不可全部机械取边**:本轮至少 5 处必须重写/融合
  (`to_chain_notification`、`on_save_blocks`、`save_blocks`、
  `should_persist`/`get_canonical_blocks_to_persist`、TrieWriterV2 impl 重组),
  另有 2 处 interleave 陷阱(blockchain_provider tests、find_disk_reorg)。
- **对非 `.rs` 文件同样要 diff**:`chain-state/Cargo.toml` 的 `[[bench]]` 段
  静默丢失,与 v2.3.0 收尾时 Cargo.toml 漏 9 项是同一模式。

## 八、落地勾选清单(开放问题 #2 的「冲突解决」侧)

决策侧(✅ 已定):保留二分 keep-gravity;实施形态 = 载荷 keep-gravity +
服务形态 follow v2.3.0 + 外围适配(本文第六节)。
**⟲ 2026-07-05:§九四项已按用户拍板的决策总原则(原文见 §九)全部裁决**
——9.1=a(回滚 option C 的 engine-tree 部分)、9.2=路线 A、
9.3=`PersistedBlockSubscriptions` 整链 trim + `ExecutionTimingStats` 留、
9.4=a(rpc-provider 整 crate 回 baseline)。路线甲的前置(9.1/9.2)已
关闭,engine / chain-state 组可动工。

落地侧(全部落地后才能勾开放问题 #2 的第二个框;勾选须附实测证据:
对应 `cargo check -p` 输出 + 该文件 `grep -c '^<<<<<<<'` 归零。
**⚠ f89d9d4e23 已勾各项的编译证据统一待补**:workspace 依赖缺失使
`cargo metadata` exit=101(cargo 组范围),修复后逐项回填
`cargo check -p` 输出):

- [x] §6.0 trie-common——⟲ f89d9d4e23 整体还原 baseline(非原方案),
      `nested_trie` 挂载已补回。证据:冲突标记归零(17→0,实测);
      编译证据待 cargo workspace 依赖修复后回填
- [x] §6.1 storage-api——⟲ 整体还原 baseline,`TrieWriterV2` 在 :121,
      witness 回无 mode 签名。证据:冲突标记归零(4/3/1/6→0,实测);
      编译证据待回填
- [x] §6.3 前置断点:chain.rs + exex/wal 回 baseline(⟲ 新增项;
      §9.2=路线 A)——与 baseline diff=0;notifications.rs 6 处
      `Chain::new` 调用点级联适配。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.2.1 in_memory.rs 39 块(⟲ to_chain_notification 直接取 HEAD,
      无新增访问器)——22 HEAD + 14 v2.3.0 + 3 融合;另修复 3 处公共区
      interleave 残局(Reorg 臂拼残、getters impl 提前闭合、测试脚手架
      夹带,文档原仅预警 1 处)与 test_state_receipts 公共区断言。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.2.2 memory_overlay.rs 整文件复原 baseline(⟲ 方案 B,零补丁;
      = 开放问题 #5)——与 baseline diff=0。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.2.3 test_utils.rs 复原(diff=0)+ Cargo.toml `[[bench]]` 补回。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.2.4 state_trie_overlay.rs + deferred_trie.rs trim(git rm)+ lib.rs 清挂载(⟲;execution_stats/notifications 按 9.3 保留)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.3 rpc pending_block ×2 就地适配(rpc-eth-api 保 v2.3.0 双参 `finish`;rpc-eth-types 3 处按方案)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.4.1 database/provider.rs——⟲ 整体还原 baseline(原 8 块手术/
      save_blocks 适配/unwind 双路径均不再需要)。证据:冲突标记归零
      (101→0)+ 与 baseline 仅 ~101 行 diff(实测);编译证据待回填
- [x] §6.4.2 blockchain_provider.rs——⟲ 与 baseline 零 diff(实测)。
      证据:冲突标记归零(46→0);编译证据待回填
- [x] §6.4.3——⟲ 作废(三断点随还原消失:consistent.rs 已还原、
      manager.rs 65→0 + 7 处桥接、rocksdb 目录已删;overlay.rs 已删)。
      无事可做,视同完成
- [x] §6.5.4 writer/mod.rs——⟲ 反向解决:`UnifiedStorageWriter` 复活
      (:21/:136/:172/:192 实测),原「删除 + cfg 迁移」作废。证据:
      冲突标记归零(15→0);编译证据待回填
- [x] §6.5.1 newPayload 集群(⟲ 路线甲)——validator/processor/state.rs/
      cached_state.rs 复原与 baseline diff=0(v2.3.0 独有 4 文件留孤儿)、
      types.rs 已删、event.rs/engine.rs 回 baseline;Cargo:root
      +`mini-moka`、engine/tree +`mini-moka`/`smallvec`。
      **落地实录偏差**:validator 两处 `Arc<dyn FullConsensus<N, Error =
      ConsensusError>>` 适配为无 Error 形态 + `validate_block_post_execution`
      调用扩为 4 参(consensus crate 未被 storage 还原,trait 已 v2.3.0 化)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.5.2 tree/mod.rs(84 块:36 HEAD + 30 v2.3.0 + 18 融合;
      4958→3696 行)——`notify_waiters` 已插(on_persistence_complete,
      紧跟 `persistence_state.finish`);**标记外编辑实为 5 处**(version
      参数 ×2、find_disk_reorg 实际是删 26 行孤儿尾巴而非接回函数头、
      purge_timing_stats 剥离 SlowBlock 发射、删公共区死函数
      try_insert_payload/try_buffer_payload 78 行);newPayload 重构组
      12 块按⟲预期贴 HEAD;`mod cached_state;` 恢复、`pub mod types` 删。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.5.3 persistence.rs(18 块,1291→796 行,融合版 on_save_blocks +
      反向失效清理)——**落地实录偏差**:P5 未按 ⟲ 注取 v2.3.0 二参,
      改取 HEAD + 补回被公共区吞掉的 `let sf_provider = ...`(理由:还原后
      writer 的 `remove_blocks_above` 内部含 static-file 截断,二参形态会把
      static-file 数据留成孤儿;现与 baseline 逐行一致);测试区另删 2 个
      死符号测试(RocksDBProviderFactory/SaveBlocksMode 全仓零命中)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §6.5.5 tree/tests.rs(44 块:41 HEAD + 3 v2.3.0;3027→1344 行,gravity 专属测试保留)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §9.3 落地(rpc 端):rpc/reth.rs 端点三件套(import/bound/handler)
      + rpc-api/reth.rs subscription 声明已外科摘除(`persisted` grep 归零;
      整文件回退会丢 429 行无关演进,按原则 3 只摘端点);chain-state 侧
      trait+noop impl 按裁决保留(自包含可编译);rpc-builder 4 处引用全在
      冲突块内(@135/@377/@850/@1071),留 rpc 组随 49 块解向 baseline。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [x] §9.4 落地:rpc-provider 整 crate 回 baseline——与 baseline diff=0
      (含手动删 v2.3.0 独有 `rpc_response.rs`);死符号反扫全绿;连带断点
      `TryFromTransactionResponse` 已按原则 2 裁决并落地 = 从 baseline 加回
      rpc-convert(见 9.4 补记)。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填
- [ ] 终验:`cargo check --workspace --all-features`(当前被 workspace
      依赖缺失阻塞)+ `grep -rl '^<<<<<<<' crates/ | wc -l` 相对基线
      只减不增 + pipe 两 crate 编译 + **跨组反向失效重扫**(§七教训 4)
    - ⟲ 2026-07-07 进度(Task #3 收尾):三处已收
        - `reth-trie-sparse` lib 12 处 `SparseTrieErrorKind::BlindedNode { path, hash }`
          → tuple `BlindedNode(path)` 迁移(canonical 定义在
          `crates/evm/execution-errors/src/trie.rs:169`,丢弃 hash 参数);顺带
          test line 2888 assert_matches 同步;`cargo check -p reth-trie-sparse
          --all-features` 绿
        - `reth-execution-types` chain.rs 3 处:`serde_bincode_compat::ExecutionOutcome<'a, N::Receipt>`
          → 去 typed generic 为 `<'a>`(bincode compat 层 struct 无 T 参数,
          只在 From impl 层解耦 RLP encode/decode);`as_repr()/from_repr()`
          → 标准 `.into()`;`cargo check -p reth-execution-types --all-features` 绿
        - `examples/custom-hardforks` chainspec.rs 补 `EthChainSpec::gravity_hardforks`
          delegate 到 `self.inner`;`cargo check -p custom-hardforks --all-features` 绿
        - 顺带:`reth-trie-sparse-parallel` trie.rs 2 处同 BlindedNode 迁移
          (跨 crate 同一 canonical form)
    - 当前 workspace check --all-features 剩 4 errors × 2 crates(不属 Task #3
      三处目标):
        - `reth-consensus-common` validation.rs:197 与 :241
          `post_merge_hardfork_fields` **函数重复定义**(E0428)——3-way merge
          未合并的双份 impl,第一版实现 EIP-7825 per-tx gas,第二版实现
          shanghai/cancun/7934 但 doc 一致 → 需选一取舍
        - `reth-storage-api` chain.rs:17 `FullNodePrimitives` 从
          `reth_primitives_traits` 找不到(E0432),连带 :46 / :79 `Primitives::Block`
          associated type 缺失(E0220);属 upstream 上游 refactor 面
    - B 阶段三 crate 复测(`cargo check -p reth-node-core -p reth-node-ethereum`)
      被 storage-api / consensus-common 传导阻塞,B 三文件本身未新增红
    - ⟲ 2026-07-07 Task #4 尾款进度:
        - **B 项 storage-api 已落**:`crates/storage/storage-api/src/chain.rs`
          按 upstream #19176(`936baf1232 refactor: remove FullNodePrimitives`)
          机械迁移 —— 五处 `FullNodePrimitives` → `NodePrimitives`(:17 import
          + :45 / :49 ChainStorageWriter bound / impl + :78 / :82
          ChainStorageReader bound / impl);`cargo check -p reth-storage-api
          --all-features` 绿。E0220 associated type `Primitives::Block`
          连坐随迁移自动消解(`NodePrimitives` trait 定义包含 `type Block`)
        - **A 项 consensus-common 待人拍板**:validation.rs 双份 impl 决策
          方案已梳理(P1 采 EIP-7825 版 / P2 采 4895+4844+7934 版 /
          P3 合并),推荐 P3(两版语义正交、upstream v1.8.3 canonical
          结构提示 EIP-7825 原应在 `validate_block_pre_execution` 而非
          `post_merge_hardfork_fields`,但当前 tree 已把它移进本函数,合并
          可回收两版全部 fork 校验)。决策落地前 workspace check 保留
          E0428 单一阻塞
        - **workspace 复扫新面**:除 A 项外浮出 1 个新错误 —
          `reth-evm` execute.rs:627 `StateBuilder::without_state_clear`
          方法未找到(E0599)。属 revm/alloy-evm 上游 API 面变化,不在
          Task #4 尾款范围,登记待后续轮次消化
    - ⟲ 2026-07-07 Task #4 A 项 P3 合并落地:
        - **P3 合并已落**:validation.rs 双份 `post_merge_hardfork_fields`
          合并为单一 impl —— 五校验全覆盖(ommers hash + EIP-4895 shanghai
          withdrawals + EIP-4844 cancun blob gas + EIP-7825 osaka per-tx
          gas + EIP-7934 osaka block-size limit);单一 caller(validation.rs
          `validate_block_pre_execution_with_tx_root`)语义等价,doc 同步
          扩一行 `EIP-7825 per-tx gas limit validation`;`grep -c '^pub fn
          post_merge_hardfork_fields'` = 1,`cargo check -p reth-consensus-common
          --all-features` 绿(3.79s),`cargo +nightly fmt` 无遗留 diff
        - **workspace 剩余尾款登记**(不属 Task #4 A/B 范围,登记待后续
          轮次消化):
          - `reth-evm` execute.rs:627 `StateBuilder::without_state_clear`
            (E0599)—— revm/alloy-evm 上游 API 面变化
          - `reth-revm` test_utils.rs:145-148 —— `reth_trie::ExecutionWitnessMode`
            未找到(E0412)+ trait `witness` 参数数 3→4 不匹配(E0050);
            trie API 面变化
          - `reth-revm` witness.rs:74 —— `BlockHashCache::keys()` 方法缺失
            (E0599),alloy-evm cache 结构变化
          - `reth-downloaders` bodies/request.rs:248 —— 使用 unstable
            `vec_deque_pop_if` feature,与当前 toolchain(nightly-2025-10-28)
            不匹配
          - `FullNodePrimitives` 4 crate 连坐(hashing_account / provider
            chain.rs / providers/mod.rs / static_file/manager.rs)—— 全
            workspace check 未触及默认 features,登记继续
    - ⟲ 2026-07-07 Task #5 下一轮尾款进度:
        - **reth-evm 已落**:execute.rs:627 `State::builder().with_bundle_update()
          .without_state_clear().build()` → 去掉 `without_state_clear()`。依据
          revm-database 15.0.2 `StateBuilder` API 已彻底移除 `without_state_clear`
          (原 EIP-158 pre-Byzantium 开关),upstream reth `crates/evm/evm/src/
          execute.rs:577` 同步为无该 flag 形态。gravity 链后 Byzantium,语义等价;
          `cargo check -p reth-evm --all-features` 绿
        - **reth-revm 已落 3 处**:
          - test_utils.rs:144-149 `witness` 4→3 参 —— 去掉 `_mode:
            reth_trie::ExecutionWitnessMode`,匹配 storage-api trie.rs:93 canonical
            trait 定义(3 参形态,已在 f89d9d4e23 baseline)
          - witness.rs:74 `statedb.block_hashes.keys().next().copied()` →
            `statedb.block_hashes.lowest().map(|(block_number, _)| block_number)`。
            依据 revm-database 15.0.2 `BlockHashCache` 从 BTreeMap 换成固定大小
            数组 + wrap-around 编码,新增 `iter()`/`lowest()` API;upstream reth
            `crates/revm/src/witness.rs:86-87` 同样迁移
          - E0412 `ExecutionWitnessMode not found` 随 :148 witness 参数缩减
            自动消解(无第三 place)。`cargo check -p reth-revm --all-features` 绿
        - **storage-provider FullNodePrimitives 连坐 3 处已落**:按 Task #4 B 项
          同模式(upstream 936baf1232 `refactor: remove FullNodePrimitives`)
          机械迁移:
          - providers/database/chain.rs:3, 9, 27, 55(import + trait bound
            + 2 impl bounds)
          - providers/mod.rs:5, 38, 47(import + 2 trait/impl bounds
            with `SignedTx: Value, Receipt: Value, BlockHeader: Value`)
          - providers/static_file/manager.rs:39, 1654, 1854(import 简化 +
            2 impl bounds)
        - **顺带 2 处 static_file/manager.rs**(允许编辑范围内的机械迁移):
          - :417 `MissingStaticFilePath(segment, path)` → `MissingStaticFilePath(path)`
            (upstream 已删 segment 参数,单 PathBuf)
          - :619 `.block_range().copied()` → `.block_range()`(`block_range()`
            现返回 `Option<SegmentRangeInclusive>` 而非 `Option<&_>`,`.copied()`
            不适用;`SegmentRangeInclusive: Copy` 直接可用)
        - **crate-local 复测**:
          - `cargo check -p reth-evm --all-features` 绿
          - `cargo check -p reth-revm --all-features` 绿
          - `cargo check -p reth-provider --all-features` 剩 14 error(见下)
        - **workspace check 复测**:`cargo check --workspace --all-features`
          仍 75 error,失败 3 crate(**均不在 Task #5 允许编辑范围**):
          - `reth-evm-ethereum`(7 err)—— alloy-evm block executor trait
            重构(StateChange{,PostBlock}Source 删除、BlockExecutor::set_state_hook
            trait 项删除、BlockExecutorFor 从 trait 变 type alias、create_executor
            lifetime 变更、commit_transaction 参数化、TxExecutionResult/Executor/
            Result/receipts 缺失)
          - `gravity-precompiles`(5 err)—— revm PrecompileError 枚举变体
            重命名(Other → other() 构造器 / OutOfGas → OutOfGas 拆分)+
            PrecompileOutput.reverted 字段删除
          - `reth-exex-types`(88 err)—— serde_bincode_compat 泛型 chain
            上 `N::BlockHeader: RlpBincode`、`N::BlockBody: RlpBincode`
            trait bound 缺失(88 处 error 同构;需要在 `Chain<'a, N>` 等
            wrapper 类型的 bound 里加 RlpBincode,或在 serde impl 加 where 子句)
        - **latent(reth-provider 单 crate --all-features 剩 14 err,workspace
          check 因上游先失败而未触达)**:
          - **可 Task #5 允许范围内消化,但涉及语义决策**:
            - static_file/manager.rs:1187 / 1189 `HighestStaticFiles.headers`
              / `HighestStaticFiles.transactions` 字段访问 —— gravity 侧
              e6b7e5ba32(v2.3.0 squash)已把 `HighestStaticFiles` 精简为仅
              `receipts` 字段(见 crates/static-file/types/src/lib.rs:39-43),
              但 manager.rs / writer.rs 未同步。writer.rs 注释仍称
              "gravity static file writer only covers Headers/Transactions/
              Receipts",与仅 receipts 的类型定义矛盾。设计决策:是恢复
              headers/transactions 字段,还是同步删掉 manager.rs 的调用?
              **本轮不自决,handoff 挂待办**
          - **不在 Task #5 允许编辑范围**:
            - providers/database/provider.rs:54 `reth_prune_types::
              MINIMUM_PRUNING_DISTANCE` 未找到(E0432)——上游删除
            - writer/mod.rs:13 `reth_storage_errors::writer` 模块未找到
              (E0432)——上游拆分
            - providers/state/historical.rs 4 处 + latest.rs 4 处 + test_utils/
              mod.rs 2 处 `DatabaseError: From<StateRootError>` trait impl
              缺失(E0277)—— reth_execution_errors::trie::StateRootError →
              reth-db DatabaseError 的转换 impl 上游已改路径,gravity 侧仍
              走老 map_err(reth_db::DatabaseError::from)
        - **未 commit 变更**(共 6 文件):
          - crates/evm/evm/src/execute.rs
          - crates/revm/src/test_utils.rs
          - crates/revm/src/witness.rs
          - crates/storage/provider/src/providers/database/chain.rs
          - crates/storage/provider/src/providers/mod.rs
          - crates/storage/provider/src/providers/static_file/manager.rs

## 九、未决策问题(f89d9d4e23 后)——⟲ 2026-07-05 已全部裁决

> **决策总原则(2026-07-05 用户拍板;本节四条裁决及 §六全部残余取向的依据)**:
>
> 1. storage 决策(f89d9d4e23 整体还原 baseline)是既成事实、**最高优先**;
> 2. reth v2.3.0 的设计与 storage 决策**冲突**的 → 迎合 storage
>    (回 baseline / keep-gravity);
> 3. **不冲突**的 → 在不破坏 gravity 原有功能的前提下保留 v2.3.0 设计。
>
> 判定方法:凡「保留 v2.3.0 设计需要改动 storage 已还原的文件/接口面
> (补挂载、补 API、补 trait impl)」即判冲突,走原则 2;冲突与否以
> **符号存活实测**为准,不凭印象。

四项均已按上述原则裁决(决策框 ☑,依据注于各条);「落地」框仍凭实测
证据勾。9.1/9.2 关闭后,§七第 3/7 步的前置解除,engine / chain-state 组
可按 §6.5.1 / §6.2.1 动工。

### 9.1 execution-cache「option C」决议与路线甲冲突

- 决策:☑ **选项 a**(2026-07-05,依据总原则 2:v2.3.0 版
  validator/processor 的三根支柱已被 storage 还原砍断(§6.5.1 反转依据,
  实测),属「与 storage 决策冲突」→ 迎合 storage,回滚 option C 的
  engine-tree 部分;`reth-engine-execution-cache` 新 crate 留作 workspace
  孤儿。这同时回答了本条末的总问题:早期 v2.3.0-为底 决议**冲突的重议**
  (option C→部分回滚),**不冲突的保留**(metrics.rs v2.3.0 版抽样兼容,
  按原则 3 不动、仅全字段复核)
- 落地:☑(2026-07-05)——cached_state.rs 与 baseline diff=0;root Cargo.toml +`mini-moka`、engine/tree Cargo.toml +`mini-moka`/`smallvec`;`mod cached_state;` 已在 mod.rs 恢复。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填

**Option C 是什么**(定义见
`docs/merge-v2.3.0/moka-vs-mini-moka-verification.md` §四,2026-07-02 拍板
执行 = commit `7df23663c8`):它的原语境**不是 execution-cache 功能取舍,
而是 moka vs mini-moka 依赖治理**的三个解法——A = `cached_state.rs` 就地
mini_moka→moka 迁移(否:改完仍是死代码);B = mini-moka 加回 workspace
(否:维护两个 LFU cache crate);**C(采纳)= 删除
`tree/cached_state.rs`,Cargo/mod.rs 冲突按 v2.3.0 侧解,全仓 cache 格局 =
上游原样**(execution cache → `reth-engine-execution-cache` 新 crate,
precompile cache → moka 0.12)。

当时删文件的三个理由均实测成立:① 该文件与上游 v1.8.3 **逐字节相同**
(gravity 零定制,`git diff v1.8.3 0cb1687c1c --` 为空,2026-07-05 复核);
② 当时**全 worktree 零调用方**(彼时 payload_validator/processor 是
v2.3.0 版,走新 crate);③ 上游已把这套机制迁进独立 crate。

**f89d9d4e23 反转击穿的是前提 ②**:路线甲要整目录复原 baseline
payload_validator + payload_processor,而 baseline 文件引用
`crate::tree::cached_state` 共 **18 处**(validator 2 处,:4 import、
:479 `CachedStateProvider::new_with_caches`;payload_processor/mod.rs
10 处;prewarm.rs 6 处,均 `git show 0cb1687c1c:...` 实测)。文件已删
(857 行,连删 engine-tree Cargo.toml 5 行 + mod.rs 的 `mod cached_state;`
声明),复原即编译断。

**「不需要 cached_state」有两层含义,被反转的只有一层**:

- **运行时层**:gravity pipe 路线不经 newPayload 执行,任何形态的
  execution cache 在 gravity 生产路径上都是死代码——**这条从未反转,
  现在仍成立**;本冲突与运行时行为无关,纯粹是「死代码保编译」路径
  (见本节末 FAQ 注记)的传递依赖。
- **编译引用层**:老模块有没有调用方,取决于保哪一版 newPayload 路径——
  两代路径各绑各的 cache 实现,`cached_state` 是被 validator 版本选择
  拖着走的行李:

| newPayload 路径版本 | 绑定的 cache 实现 |
|---|---|
| baseline(v1.8.x 形态)validator/processor | in-tree `tree/cached_state.rs`(18 处引用,实测见上) |
| v2.3.0 版 validator/processor | 新 crate `reth-engine-execution-cache`(引用老模块 0 处) |

因此反转的不是「要不要 cache」的判断,而是「保哪一版 newPayload 路径」
的前提:option C 三个理由中 ①(gravity 零定制)③(上游已迁新 crate)
现在**依然成立**,仅 ②(零调用方)随 validator 版本而变。当年决策并无
错误,是总路线反转改变了它的前提。选项:

- **a. 部分回滚 option C(☑ 2026-07-05 采纳)**:从 baseline 恢复 `cached_state.rs` 及
  Cargo.toml / mod.rs 被删行。恢复的是 gravity 从未改过的上游 v1.8.3 原文,
  机械可行、与「下游 keep-gravity 对齐」总路线一致;代价是程序性的——推翻
  已表决决议,须 engine 组重新确认。副作用:option C 采纳的
  `reth-engine-execution-cache` 新 crate 在路线甲下失去全部引用,成为
  workspace 孤儿(无害,与 §五「磁盘孤儿」同类,v2.4+ 再启用)。
- **b. 改造 baseline validator/processor 剥离 CachedStateProvider**:违背
  路线甲「整目录复原零改造」的初衷,18 处改造面,不推荐。
- **c. 把 baseline 文件的引用改指新 crate**:新 crate 也导出
  `CachedStateProvider`(execution-cache/src/cached_state.rs:97),构造器
  形态接近(`new(state, caches, metrics)` vs baseline
  `new_with_caches` 三参),但类型家族有漂移(`ExecutionCache` vs
  `ProviderCaches`、新增 `CacheFillMode`/`CacheStats`),改造面同样
  ≈18 处且引入新旧混血,不如 a 便宜,不推荐。

连带复核项:metrics.rs(`88a3ca` 已解 v2.3.0 侧)——抽样 baseline
validator 引用的两个字段 `record_state_root`(现 metrics.rs:544)与
`state_root_parallel_fallback_total`(:515)**均存在,初步兼容**;路线甲
落地时仍须全字段过一遍。本条拍板即回答总问题:「早期按 v2.3.0-为底 做的
两个 engine 侧决议(execution-cache option C、metrics v2.3.0)是否随总
路线反转重议」。

> **FAQ:既然 gravity 运行时不用,为什么不干脆删掉整条 newPayload 路径,
> 连 cache 一起省掉?** 因为 validator / processor / tree handler 与 pipe
> 路线共用同一套 engine 结构体与类型(同一个 tree handler 承载两条路径),
> 整体剜掉会让每轮 upstream merge 的结构分歧急剧扩大。本轮 fork 原则是
> 「**死代码保编译、不保运行**」——上游路径留着以 baseline 形态编译,
> 是最小分歧选法,也是 §6.5.1 路线甲的立足点。

### 9.2 Chain/LazyTrieData 集群:方向二选一 + 归属无主

- 决策:☑ **路线 A**(2026-07-05,依据总原则 2:路线 B 须往 storage 已
  还原的 trie-common `lib.rs` 补 `mod lazy;` 挂载 = 改动 storage 还原面,
  判冲突。补充实测(2026-07-05):lazy.rs 直接依赖的两个类型在 baseline
  还原后**存在**——`HashedPostStateSorted`(hashed_state.rs:462)、
  `TrieUpdatesSorted`(updates.rs:465),且 lazy.rs 自身不调用已消失的
  `clone_into_sorted`/`merge_batch` 等方法(其 use 面仅这两个类型,grep
  实测)——即路线 B 技术上大概率只差一行挂载,但这不改变「须动 storage
  还原文件」的冲突定性,仍取 A。归属:**chain-state 组**(照原建议,
  in_memory.rs 解法直接依赖本条))
- 落地:☑(2026-07-05)——chain.rs / exex wal 与 baseline diff=0;notifications.rs 6 处调用点级联适配。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填

`crates/evm/execution-types/src/chain.rs`(零冲突、落 v2.3.0 侧)**当前
就是活断点**:`Chain::new`(:69)/`append_block`(:328)三参签名 + 全文件
11 处 `LazyTrieData`,而 `trie/common/src/lazy.rs` 是磁盘孤儿(全仓无
`mod lazy` 声明,实测)。exex 侧 `exex/exex/src/wal/storage.rs`(零冲突)
同样引用(:192 import、:302 `LazyTrieData::ready`)。两条路:

- **路线 A(本文 §6.2.1/§6.3 反转版的前提;☑ 2026-07-05 采纳)**:chain.rs + exex
  `wal/storage.rs` 回 baseline 二参形态。`to_chain_notification` 直接取
  HEAD 零重写,与 storage 总路线一致。级联工作:排查 stages /
  notifications 等处的 `Chain::new` / `append_block` / `from_block`
  构造点(baseline 二参回退面)。
- **路线 B**:trie-common lib.rs 补一行 `mod lazy;`,chain.rs / exex 留
  v2.3.0。exex 零动;代价是 in_memory.rs 须用 `LazyTrieData` 重写版
  `to_chain_notification`(初版 §6.2.1 的重写代码,现已作废、需复活),
  与「下游 keep-gravity」路线相悖。

### 9.3 chain-state 上游新特性去留:`PersistedBlockSubscriptions` 与 `ExecutionTimingStats`

- 决策:☑ **分拆裁决**(2026-07-05,两特性适用不同原则):
  - `PersistedBlockSubscriptions` → **选项 b,整链 trim**(原则 2:保留它
    必须给已被 storage 还原为「与 baseline 零 diff」的
    blockchain_provider.rs 补 trait impl = 改动 storage 还原面,判冲突。
    rpc/reth.rs 端点与 rpc-builder bound 随其 49 块按 baseline 侧解;
    notifications.rs 中失去全部消费者的 trait 定义可留可删,以少 diff 为准);
  - `ExecutionTimingStats` → **选项 a,保留**(原则 3:execution_stats.rs
    是 chain-state 独立模块、仅依赖 `Duration`/`B256`、不碰 storage 还原面
    (实测独立),留作死代码不破坏 gravity 功能、下轮 merge 少 diff;若
    后续编译实测被牵连,降级为 trim 并回注此处)。
  本条已在 rpc-builder 解块前关闭,解块方向有据。
- 落地:☑(rpc 端,2026-07-05)——rpc/reth.rs + rpc-api/reth.rs 端点已外科摘除,`persisted` grep 归零;chain-state 侧 trait+noop impl 按裁决保留;**尾款**:rpc-builder 4 处引用(冲突块 @135/@377/@850/@1071)随其 49 块解向 baseline,归 rpc 组。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填

两个上游 v2.3.0 特性在 storage 还原后处境不同,决策原则应统一:

- **`PersistedBlockSubscriptions`——不是死代码,是活断点**:trait 定义
  notifications.rs:242、chain-state lib.rs:32 导出;全仓唯一 impl 只剩
  `chain-state/src/noop.rs:31`(NoopProvider);而 rpc 侧是**活消费者**——
  `rpc/rpc/src/reth.rs`(零冲突、上游侧)的 `reth_subscribe_persisted_block`
  端点(:240)+ `rpc-builder/src/lib.rs` 的 trait bound(:380/:855)。
  blockchain_provider 复原 baseline 后**不再实现**该 trait → 真实 provider
  无法满足 rpc-builder bound,node 组装必断。选项:a=给 provider 补 impl
  (打破 f89d9d4e23「与 baseline 零 diff」的状态);b(☑ 采纳)=整链 trim
  (notifications.rs 该段 + lib.rs 导出 + rpc/reth.rs 端点 + rpc-builder
  bound 一起删)。
- **`ExecutionTimingStats`**(execution_stats.rs,lib.rs:12 导出):现有
  消费者 types.rs(路线甲=删)、payload_validator.rs(路线甲=复原
  baseline)、event.rs(路线甲=回退)、mod.rs(引用全在冲突块内)——
  路线甲落地后消费者归零。选项:a(☑ 采纳)=留死代码贴上游(下轮 merge
  少 diff);b=随 baseline trim。

### 9.4 rpc-provider 断点:归属无主

- 决策:☑ **选项 a,整 crate 回 baseline**(2026-07-05,依据总原则 2 +
  下方全 crate 扫描:选项 b 的前提「仅 :1306 一处」被实测推翻——v2.3.0 侧
  与 baseline trait 面成片漂移,≥11 处、跨 ≥5 个 impl block,属「保 v2.3.0
  须适配 storage 已还原的 trait 面」的冲突)。归属:rpc 组执行,或 storage
  组补刀
- 落地:☑(2026-07-05)——整 crate 与 baseline diff=0(含手动删 v2.3.0 独有 `rpc_response.rs`);死符号反扫全绿。证据:冲突标记归零 + rustfmt parse + 死符号扫描(2026-07-05 落地);编译证据待 cargo workspace 修复后回填

**⟲ 9.4 落地补记(连带断点,收口时裁决并落地)**:复原后 rpc-provider 从 `reth_rpc_convert` 导入的三个 trait 实测**全部失挂**——不止预报的 `TryFromTransactionResponse`(v2.3.0 删除),`TryFromBlockResponse`/`TryFromReceiptResponse` 所在的 block.rs/receipt.rs 也是磁盘孤儿(文件与 baseline 逐字节一致,但 v2.3.0 侧 lib.rs 无模块声明,nested_trie 同款静默失挂)。按原则 2 落地:rpc-convert lib.rs 补 `pub mod block; pub mod receipt;` 及导出、transaction.rs 尾部从 baseline 加回 `TryFromTransactionResponse` trait + Ethereum impl(OP impl 按「no OP」惯例不带)、Cargo.toml 补 `reth-ethereum-primitives`。选 rpc-convert 而非移进 rpc-provider:贴 baseline 布局,下轮 merge 呈现为 rpc-convert 的 gravity 增量。

`crates/storage/rpc-provider` 是 workspace member(根 Cargo.toml:59、
workspace dep :372),**baseline 与 v2.3.0 都有此 crate**(`git ls-tree`
两侧均在,非上游新增);当前落 v2.3.0 侧(与 baseline diff:4 文件
+205/−126)。

**全 crate 死符号/漂移扫描(2026-07-05 实测,裁决依据)**:

| 漂移点 | 位置(rpc-provider lib.rs) | baseline trait 侧 | 错误数 |
|---|---|---|---|
| `reth_trie::ExecutionWitnessMode`(符号已消失)+ `witness` 多一参 | :1302-1306(impl :1279) | trie.rs witness 无 mode | 1 |
| `header_td`/`header_td_by_number` **缺失**(必需方法、无默认体) | HeaderProvider impl :377、:1649 | header.rs:48-51 | 4 |
| `transaction_block` 缺失 ×2 + `block_by_transaction_id` 孤儿方法 ×2(上游改名) | TransactionsProvider impl :648、:1544 | transactions.rs:53 | 4 |
| `header(&BlockHash)` 按引用 vs 按值 | :385、:1657 | header.rs:23 | 2 |

(`recover_block_number` 有默认体(block_id.rs:30-32),不构成错误。)
以上仅高危改名面抽查,30+ trait impl 未穷举——成片漂移已足以否证
选项 b(原「就地适配一处」);穷举无必要,整 crate 回退一步到位。

- **a(☑ 采纳)**=整 crate 回 baseline(与总路线一致,~205 行回退);
- b=就地适配——前提被上表推翻,实际改造面 ≥11 处且未穷举,放弃。

### 非决策注记(执行时留意,无需拍板)

- mod.rs 的 `ChangesetCache`(:96/:450/:545/:601)与 `FastInstant`(:56)
  引用**全部位于冲突块内**(按标记区间 awk 实测),不是公共区断点——
  解块时随路线甲丢弃即可;但 §6.5.3 融合版 imports 中的
  `FastInstant as Instant` 须改回 `std::time::Instant`(`FastInstant`
  定义已随 primitives 还原消失,全仓 grep 为空)。
- ress 已不在 workspace members(根 Cargo.toml 无 `crates/ress` 条目),
  不构成编译问题;`crates/ress/` 目录删不删是卫生问题。§五 #29 的
  「反向锁定」论据随之弱化,但 memory_overlay 方向已由开放问题 #5 的
  决策独立锁定,不受影响。
### 跨组断点台账(2026-07-05 落地轮汇总;本文档范围外,逐条注明归属)

| 断点 | 位置 | 归属 / 状态 |
|---|---|---|
| ~~`Chain::new` 三参调用点~~ | ✅ 全部关闭(2026-07-06 收尾轮):stages 侧 2 处随 OQ10;exex 侧 16 处(backfill/job.rs:151、wal/mod.rs ×5、notifications.rs ×8、types/notification.rs ×2)`BTreeMap::new()` → `None`(与 baseline 逐字一致;manager.rs 12 处 `Default::default()` 本就吻合);tx-pool 残留 blobstore/tracker.rs:177 由主会话同法修复 | 已关闭 |
| ~~RocksDB 死符号面~~ | ✅ 全部关闭(2026-07-06):cli ~12 文件、stages ~7 文件、e2e-test-utils(setup_import + `[[test]] rocksdb` 摘除 + 7 测试文件簇)、exex/test-utils(装配段按 baseline 逐字还原,`ProviderFactory::new` 回三参)均落地,广谱 grep 归零 | 已关闭 |
| ~~`PersistedBlockSubscriptions` 尾款~~ | ✅ rpc-builder 49 块已解,4 处随之剥离(2026-07-06 落地,实测) | 已关闭 |
| ~~`MockEthProvider::with_genesis_block`~~ | ✅ tx-pool 12 处改写完成;net/network txgossip ×5 + connect ×1 收口剥离(2026-07-06);e2e-test-utils 若有同类归 tests-infra 组 | 基本关闭 |
| ~~`builder/states.rs`/`builder/mod.rs` 零冲突侧翻引死符号~~ | ✅ 已关闭(2026-07-06,node-builder 组局部摘除 15 点,死符号 crates/node/ 归零) | 已关闭 |
| ~~`launch.rs` 与 `build_engine_orchestrator`~~ | ✅ 已关闭(2026-07-06 专项 + 主会话收口)。⟲ 前提反转实录:engine.rs 取 orchestrator 主体后 launch.rs 必须活。①挂载:lib.rs:104 补 `pub mod launch;`(merge 丢行,doc 注释孤悬为证);②咬合:launch.rs 仅一处适配——`spawn_new` 调用 11→9 实参(HEAD tree 按 baseline 形态);外部 12 crate 全在 Cargo.toml,**零增补**(v2.3.0-only 三 dep 确属 BAL 孤儿,engine-evm 原裁决无需推翻);③连带剪线(主会话):`ChangesetCache` 属 storage-v2 changeset 缓存家族——trie/db `changesets.rs` 内 7 处死符号、`BasicEngineValidator`(gravity 形态)不持有它、全仓消费方仅 engine.rs/rpc.rs/launch.rs 传递链 → 按原则②整线剪除(engine.rs 构造+3 处传参、rpc.rs re-export+trait/impl 参、launch.rs 参数,`BasicEngineValidator::new` 调用回 6 参实测吻合);`changesets.rs` 定性磁盘孤儿勿挂载;④`WaitForCaches` 补桩:v2.3.0 impl(payload_validator.rs :2235)被 merge 丢弃而 rpc.rs/launch.rs bound 存活、运行时零调用方 → 补 `CacheWaitDurations::default()` 桩 impl(gravity payload processor 无 cache 锁,零等待=baseline 行为)。4 文件 rustfmt parse 全绿,`ChangesetCache` 活代码 grep 归零 | 已关闭 |
| ~~workspace 缺 dep(阻塞一切编译证据)~~ | ✅ 已关闭(2026-07-06,cargo 组,6a54e53528):crates/primitives 恢复 baseline path crate;primitives-traits 以 crates.io 0.4.1 vendor 为底恢复 path crate + `[patch.crates-io]` 统一传递依赖 + serde-bincode-compat 整链移植;根 members +5、deps +7(实测缺失面仅 7 项,原 ~20 系旧状态);Cargo.lock 整取一侧后 cargo 自愈。**`cargo metadata` 真仓 exit=0**,check -p primitives-traits/primitives/db 通过 | 已关闭 |
| ~~`SubkeyContainedValue` 定义缺失~~ | ✅ 已关闭(同上批):定义恢复于 primitives-traits lib.rs:248 + storage.rs:46(与上游 `ValueWithSubKey` 并存,各有存活消费方),4 文件 7 处 import 可解析 | 已关闭 |
| static-file/types「缝隙」 | **收口实测定性:非断裂,系 storage 组有意桥接**——provider writer.rs 通配臂带 gravity 注释、`tables::TransactionSenders` 存活(db-api :506)、零冲突消费方(cli get.rs / without_evm.rs)按 v2.3.0 types 编译;`ChangesetOffset*` 家族零外部消费方自包含。**types 保持 v2.3.0 不回退**;changeset 段运行时能力记 v2.4+ 债务;stages 组解块时三个新 segment variant 可用 | 已定性关闭(stages 组照此解块) |
| `NodeConfig.gravity` 字段随 node/core 侧翻丢失 | 当前全仓无读取方不阻塞(cli 落地经 `init_gravity_config` 全局单例保全);node-builder 复原 launch 代码若引用须同步补字段 | node-builder 组 |
| 收口修复项(2026-07-06,已关闭) | ①metrics.rs 嫁接段 state-hook 机制二次对齐 + StateChangeSource vendored;②`WithTxEnv` 字面量 `Arc::new`;③evm execute.rs hook 机制修正;④rpc-engine-api BAL 剔除;⑤net/network 零冲突侧翻 7 文件(BAL 链 + FastInstant + with_genesis_block);⑥error/mod.rs `FeeCapBelowMinimumProtocolFeeCap` struct 形态对齐;⑦debug-client `get_block_access_list_raw` 实测为 alloy-provider 外部 trait 方法,原报断点系误报 | 已关闭(详见各组文档 ⟲ 落地实录) |
| 新增磁盘孤儿 | `rpc-eth-api/helpers/bal.rs`、cli db/{settings,state,account_storage,migrate_v2}.rs、checksum/rocksdb.rs、tx-pool 2 bench(已删) | 防误引用清单,勿再挂载 |

- 外部依赖(非本文档决策,但阻塞验收/动工):cargo 组补 workspace 约 20
  个 dep(阻塞一切编译证据);primitives-traits 组恢复
  `SubkeyContainedValue`——其还原范围顺带决定 `FastInstant` 一类符号的
  命运,engine 组动工前宜先等其结论。
