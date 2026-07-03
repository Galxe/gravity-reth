# ExecutedBlock 二分与 pipe-exec make-canonical 链路分析(开放问题 #2)

> 结论先行:**`ExecutedBlockWithTrieUpdates` 正是 pipe-exec 路线上 make-canonical
> 的载荷类型——从流水线执行、event bus、engine tree 到 persistence,整条链路每一
> 环都 typed 在这个类型上。本轮「保留二分(keep-gravity)」的决策对该路线正确且
> 必要,且设计自洽:类型定义与全部消费方都在 fork 自有代码内,完全不经过上游
> v2.3.0 新的集中式 trie overlay。** 代价是长期 tech debt:v2.4+ 起每轮上游碰
> `ExecutedBlock` 的改动都要手工适配。当前 worktree 尚有三处未闭环(见第五节),
> 决策能否落地取决于收尾时是否严格按 keep-gravity 解掉剩余冲突。

- 核实日期:2026-07-02
- 分支:`gravity-reth-merge-v2.3.0`(WIP checkpoint `32fefe844e`)
- 本文是 `engine-evm-execution-chainspec.md` 开放问题 #2 的展开分析。行号基于
  当日 worktree(mid-merge,含冲突标记文件的行号会随解冲突漂移)。

---

## 一、背景:两边类型体系的分叉

**上游 v2.3.0**(#21123 / #24184 / #21133 等)已把 `ExecutedBlock` /
`ExecutedBlockWithTrieUpdates` 统一为单一 `ExecutedBlock`,trie 数据合并进
`ComputedTrieData` / `LazyTrieData`,由 chain-state 的 `StateTrieOverlayManager`
**集中调度、延迟计算**。

**gravity baseline** 保留二分,且 gravity 版比老上游(v1.8.3)还多一个字段
(`crates/chain-state/src/in_memory.rs:1085`,当前位于冲突块 HEAD 侧):

```rust
pub struct ExecutedBlockWithTrieUpdates<N: NodePrimitives = EthPrimitives> {
    pub block: ExecutedBlock<N>,          // { recovered_block, execution_output, hashed_state }
    pub trie: ExecutedTrieUpdates,        // 标准 v1 TrieUpdates(Present/Missing)
    pub triev2: Arc<TrieUpdatesV2>,       // gravity 独有:nested trie 紧凑序列化(#149)
}
```

关键事实:**pipe 路线上真正的 trie 载荷装在 `triev2`,标准 `trie` 字段反而填
`ExecutedTrieUpdates::empty()`**(见下节构造点)。`triev2` 这个字段连老上游都
没有——即使把 gravity 硬迁到上游统一模型,`ComputedTrieData` 也没有
`TrieUpdatesV2` 的槽位,还得再 fork 一次。

## 二、make-canonical 全链路类型流

pipe-exec 路线(gravity 不走 CL 的 newPayload/FCU,由 pipe-exec event bus 直接
注入 MakeCanonical)上,该类型的流转每一步:

1. **流水线 merklize 阶段 eager 算 state root**:
   `self.storage.state_root(&hashed_state)` 返回 `(state_root, trie_updates)`
   (`crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:713`,走 GravityStorage
   自有 trie cache,返回的是 `TrieUpdatesV2`)。root 写进 header
   (lib.rs:727)之后才 seal——因为 `verify_executed_block_hash`(共识对
   block hash 的确认,lib.rs:763)发生在 make-canonical **之前**,而 block hash
   含 state_root,所以 root 必须在流水线内急切算出,不能延迟。
2. **构造载荷**:`ExecutedBlockWithTrieUpdates::new(recovered_block,
   execution_outcome, hashed_state, ExecutedTrieUpdates::empty(),
   Arc::new(trie_updates))`(lib.rs:753)——第 4 参 `trie` 为空,第 5 参
   `triev2` 携带真实 trie 数据。
3. **event bus 投递**:`make_canonical`(lib.rs:1407)发送
   `PipeExecLayerEvent::MakeCanonical(MakeCanonicalEvent { executed_block, tx })`,
   事件载荷类型即 `ExecutedBlockWithTrieUpdates<N>`
   (`crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs:43`)。
4. **engine tree 单线程事件循环消费**:`make_executed_block_canonical`
   (`crates/engine/tree/src/tree/mod.rs:730`):`tree_state.insert_executed` →
   设 forkchoice → `make_canonical(block_hash)` → `canonical_in_memory_state`
   更新(即 `in_memory.rs` 的二分类型流:`update_chain` / `update_blocks`)→
   `set_safe` / `set_finalized`(确定性共识,canonical 即 safe+finalized)。
5. **persistence 落库**:`persist_blocks(Vec<ExecutedBlockWithTrieUpdates<N>>)`
   (tree/mod.rs:2125)→ persistence 解构出 `triev2` 并调
   `write_trie_updatesv2`(`crates/engine/tree/src/persistence.rs:565-599`,
   另一路径 persistence.rs:377/485;
   `crates/storage/provider/src/writer/mod.rs:193`)。

即:**这个类型是 pipe-exec make-canonical + persistence 流水的骨架**,并且它
携带的 trie 数据是在 engine tree 之外、流水线内部提前算好的——这正是与上游
v2.3.0「插入前后由 overlay manager 调度计算」模型的结构性差异。

## 三、为何拒绝上游统一模型

1. **时序互斥**:上游 `LazyTrieData` 把 trie 计算延迟到 chain-state 集中调度;
   gravity 的 root 必须在 seal 之前、共识验证 block hash 之前就绪(见第二节
   第 1 步)。lazy 模型对 pipe 路线没有可用的挂载点。
2. **调度权互斥**:gravity 流水线用 `merklize_barrier` 自驱动串行化 trie 计算
   (lib.rs:711-719),与 `StateTrieOverlayManager` 的集中调度是两套所有权模型,
   强行叠加等于两处都要改。
3. **数据槽位缺失**:统一后的 `ComputedTrieData` 无 `TrieUpdatesV2` 槽位;
   gravity 的 persistence 依赖 `triev2` 走 `write_trie_updatesv2`。硬 port 需
   扩展上游类型,即换一个位置继续 fork。

## 四、结论与长期策略

- **本轮保留二分正确且必要**:类型(chain-state)、event bus、tree、
  persistence、writer 全在 fork 自有代码里,pipe 路线不触碰上游 overlay
  manager,root 来自 GravityStorage 自有 trie cache。上游删类型是它自己的
  重构,对 gravity 没有语义强制。
- **长期是 tech debt**:上游正持续深化集中式 trie overlay,gravity 每保留一轮
  二分,类型分歧越深、下轮 merge 冲突面越大。v2.4+ 需评估:一次性把 gravity
  trie cache 重建到 `LazyTrieData` 体系上(一次大手术,含给 `ComputedTrieData`
  加 `TrieUpdatesV2` 槽位),还是继续手工适配维护二分。本轮不解决、不阻塞。

## 五、当前 worktree 未闭环清单(2026-07-02 实测)

决策「保留二分」尚未完全落地,以下按实测冲突标记数(`grep -c '^<<<<<<<'`):

| 文件 | 冲突块数 | 说明 |
|---|---|---|
| `crates/chain-state/src/in_memory.rs` | 39 | `ExecutedTrieUpdates` / `ExecutedBlockWithTrieUpdates` 的定义本身在 1015-1127 行冲突块的 **HEAD 侧**(v2.3.0 侧是删除它们)。必须取 HEAD;且 `ExecutedBlock` 内部字段(`recovered_block` / `execution_output` / `hashed_state`)不能跟上游重组,否则 pipe-exec 构造点(lib.rs:753)全断 |
| `crates/engine/tree/src/tree/mod.rs` | 84 | make-canonical / persist_blocks 消费方所在文件,mid-merge |
| `crates/engine/tree/src/persistence.rs` | 18 | `triev2` 落库路径 |
| `crates/storage/provider/src/writer/mod.rs` | 15 | 同上 |
| `crates/chain-state/src/memory_overlay.rs` | 0 | **已是上游版**:`Cow<'a, [ExecutedBlock<N>]>`(27 行)+ `extend_from_sorted`(56-57 行,依赖上游 `ComputedTrieData` 的 sorted 字段)。一旦 `in_memory.rs` 按 gravity 侧解完会**编译不过**,须改回 `Vec<ExecutedBlockWithTrieUpdates>` + `TrieInput::from_blocks` 或做适配——对应 `engine-evm-execution-chainspec.md` 开放问题 #5 |

收尾顺序建议:先解 `in_memory.rs`(类型定义是其余文件的编译前提)→
`memory_overlay.rs` 复原/适配 → tree/mod.rs → persistence/writer,每步以
pipe-exec 两 crate(现已零冲突)能编译为回归锚点。
