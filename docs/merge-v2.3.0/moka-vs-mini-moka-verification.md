# moka vs mini-moka 核实报告(开放问题 #1)

> 结论先行:**原开放问题的前提不成立**。上游 v2.3.0 的 execution cache 根本不用
> moka——它用的是 `fixed-cache` crate(新 crate `reth-execution-cache`);moka 只
> 用于 precompile cache。而 gravity 的 `cached_state.rs` 与 upstream reth v1.8.3
> 同文件 **byte-identical(零 gravity 定制)**,且在本 worktree 中已无任何调用方。
> 正确解法既不是"切 moka"也不是"加回 mini-moka",而是**直接删除该文件、采纳
> v2.3.0 的 reth-execution-cache 结构**,mini-moka 随之整体消失。

- 核实日期:2026-07-02
- 分支:`merge-v2.3.0`(WIP checkpoint `e6b7e5ba32`)
- baseline anchor:Galxe/gravity-reth `upstream/main` tip `0cb1687c1c`
- 上游 anchor:tag `v2.3.0`(`9384bc53d8`)

---

## 一、当前状态(worktree)

### mini_moka / moka 全仓使用面(核实事项 1)

全 repo grep(`mini[-_]moka`,排除 `target/` 等非代码目录),**只有 3 个使用点,
全部在 engine/tree**:

| 位置 | 内容 | 状态 |
|---|---|---|
| `crates/engine/tree/Cargo.toml:90` | `mini-moka = { workspace = true, features = ["sync"] }` | 位于**未解决冲突块的 HEAD 侧**(89–94 行:HEAD = `mini-moka` + `smallvec`,v2.3.0 侧 = `moka`) |
| `crates/engine/tree/src/tree/cached_state.rs:4` | `use mini_moka::sync::CacheBuilder;` | 文件本身**无冲突标记**,与 baseline 完全一致 |
| `crates/engine/tree/src/tree/cached_state.rs:22` | `type Cache<K,V> = mini_moka::sync::Cache<K, V, DefaultHashBuilder>` | 同上 |

moka 使用点:

| 位置 | 内容 |
|---|---|
| 根 `Cargo.toml:604` | `moka = "0.12"`(v2.3.0 baseline,已解析,无 mini-moka 定义) |
| `crates/engine/tree/Cargo.toml:93` | `moka = { workspace = true, features = ["sync"] }`(冲突块 v2.3.0 侧) |
| `crates/engine/tree/src/tree/precompile_cache.rs` | moka + `EvictionPolicy::lru()` — **已与上游 v2.3.0 逐字节一致**(diff 验证) |

另外根 `Cargo.toml` 已含 v2.3.0 的新依赖与成员:
`fixed-cache = { version = "0.1.10", features = ["stats"] }`(603 行)、
workspace member `crates/engine/execution-cache/`(74 行)、
`reth-execution-cache = { path = ... }`(397 行)。
worktree 的 `crates/engine/execution-cache/src/cached_state.rs` 也已与上游
v2.3.0 **逐字节一致**(diff 验证,基于 `fixed_cache::Cache`)。

### baseline(`0cb1687c1c`)状态

- workspace `Cargo.toml:639`:`mini-moka = "0.10"`(workspace-level 定义);
  **没有 moka**(baseline 的 precompile_cache 时代注释虽写 "moka cache",实际走
  mini-moka 时代布局)。
- `crates/engine/tree/Cargo.toml:57`:`mini-moka = { workspace = true, features = ["sync"] }`。
- baseline **没有** `crates/engine/execution-cache/`(engine 下只有
  invalid-block-hooks / local / primitives / service / tree / util)。
- baseline `cached_state.rs` = 857 行,`mod cached_state;` 为**私有模块**(非
  `pub mod`),类型无 crate 外 re-export。

---

## 二、核实结论逐条

### 1. mini_moka 全仓使用面 → 仅 engine/tree 一处,且已是"孤岛"

见上表。关键补充:worktree 全仓 grep `cached_state::` / `ProviderCaches` /
`ExecutionCacheBuilder` 的**调用方为零**(engine/tree 的 payload_processor/mod.rs、
prewarm.rs、payload_validator.rs、bal_prewarm_pool.rs、mod.rs 均已改为
`use reth_execution_cache::{CachedStateProvider, ExecutionCache, SavedCache, ...}`)。
旧模块唯一的存活线索是 `crates/engine/tree/src/tree/mod.rs:111` 的
`mod cached_state;`,而这一行**位于未解决冲突块的 HEAD 侧**——v2.3.0 侧已将其
删除(上游把该文件整体迁到了 `reth-execution-cache` crate)。

即:一旦 mod.rs 该冲突按 v2.3.0 侧解决,`cached_state.rs` 就是死文件。

### 2. gravity 为什么用 mini-moka → 纯 fork 时点遗留,从未刻意选型

决定性证据:**gravity baseline 的 `cached_state.rs` 与 upstream reth v1.8.3 的
同路径文件 diff 为空(byte-identical,857 行)**。gravity 对该文件零定制,
mini-moka 完全继承自上游 v1.8.3 时代(上游 `c9169705e2 perf(tree): add
cross-block caching (#13769)`,2025-02,引入 mini-moka)。

git log -S 'mini-moka' 的 gravity 侧命中全部是 merge/transplant 类 commit
(`663dbf46d2 merge reth v1.2.0`、`9662084392 resolve v1.11.3 conflict markers`、
`32298763f2 reth v1.8.3 upstream base`、`8be11d82be WIP transplant`),没有任何
"为 perf/依赖精简而选 mini-moka"的独立 commit。**切走 mini-moka 无历史包袱。**

### 3. 上游演进时间线 → cached_state 从未用过 moka,直接跳到 fixed-cache

| 时间 | 上游 commit | 内容 |
|---|---|---|
| 2025-02 | `c9169705e2` #13769 | cross-block caching 引入,**mini-moka** |
| 2025-12-19 | `30162c535e` #20502 "properly share precompile cache + use moka" | **只改 precompile_cache.rs**(+prewarm 一行),moka 进 workspace;**未动 cached_state.rs** |
| 2025-12-19 | `9d4ec70f7d` "just moka"(branch `reth/alexey/execution-cache-moka`) | 上游曾实验把 cached_state 切到 moka(还试过 papaya:`31ecadcbcb`),**该分支未合入 main** |
| 2026-01-23 | `c137ed836f` #21128 "perf(engine): fixed-cache for execution cache" | cached_state.rs 从 mini-moka **直接切到 `fixed-cache`**(1039 行改动) |
| ≤ v2.2.0 (2026-04) | — | cached_state 整体迁出 engine/tree,成为新 crate `crates/engine/execution-cache`(`reth-execution-cache`) |
| v2.3.0 | `9384bc53d8` | 同结构;workspace:`moka = "0.12"`(仅 precompile)+ `fixed-cache = "0.1.10"`(execution cache);**mini-moka 彻底移除** |

要点:上游自己评估过 moka 方案(execution-cache-moka 实验分支)后**放弃**,
选择了为此场景定制的 `fixed-cache`(cache-line 对齐 bucket、epoch-based O(1)
invalidation、AnyRef,见 v2.3.0 `crates/engine/execution-cache/src/cached_state.rs`)。
所以"与上游 v2.3.0 一致"意味着 execution cache 走 fixed-cache,而不是 moka。

### 4. 上游 v2.3.0 的 cached_state.rs → 路径已变,实现已换

`git show v2.3.0:crates/engine/tree/src/tree/cached_state.rs` 报"不在 v2.3.0
中";实际位于 `crates/engine/execution-cache/src/cached_state.rs`,基于
`fixed_cache::Cache<K, V, H, EpochCacheConfig>`,并引入了 gravity 版没有的新
语义(`CacheFillMode::LookupOnly/FillOnMiss`、`CacheStats` 慢块统计、
`StatsHandler`、`PayloadExecutionCache` 等)。worktree 已把这个文件原样搬进来
(与 v2.3.0 逐字节一致),engine/tree 的 `Cargo.toml:27` 也已有
`reth-execution-cache.workspace = true`。

**gravity 版(=v1.8.3 版)与 v2.3.0 版结构差异巨大(整文件重写级),不存在
"gravity 版切个 cache 后端继续用"的低成本路径——也没有必要,因为调用方已全部
指向新 crate。**

### 5. 调用方与 re-export 审计 → 无 gravity 自有代码依赖旧模块

- baseline 中使用 `CachedStateProvider`/`SavedCache`/`ProviderCaches` 的文件只有
  4 个,全部是上游标准文件(cached_state.rs 自身 + payload_processor/mod.rs +
  prewarm.rs + payload_validator.rs),这些文件上游 v2.3.0 已统一改接
  `reth_execution_cache`,worktree 对应文件也已如此。
- baseline `mod cached_state;` 是私有模块,`git grep` baseline 全仓无 crate 外
  引用(gravity-precompiles / gravity-primitives / gravity-storage 及 bin 均不
  涉及)。
- gravity 特有的 grevm/pipe-exec 路径不经过这套 cache 类型(engine/tree 内
  grevm 仅 recovery.rs 涉及,与 cache 无关)。

---

## 三、API 对照表(mini_moka 0.10.3 vs moka 0.12.x)

> 仅作留档:因为最终建议是删文件而非迁移,此表是"假如迁移"的核对结果。
> 依据:本机 `~/.cargo/registry/src/` 下 mini-moka-0.10.3 与 moka-0.12.15 源码。

gravity `cached_state.rs` 用到的 mini_moka API 全集及 moka 0.12 等价物:

| gravity 用法(行号) | mini_moka 0.10 | moka 0.12 | 兼容性 |
|---|---|---|---|
| `CacheBuilder::new(entries)`(437/449/475/589) | `new(max_capacity: u64)` | 同签名 | 直换 |
| `.weigher(\|k,v\| -> u32)`(438/450/476) | ✔ | ✔ 同签名 | 直换 |
| `.max_capacity(u64)`(444/470/485) | ✔ | ✔ | 直换 |
| `.time_to_live(Duration)` / `.time_to_idle(Duration)`(445–446 等) | ✔ | ✔ | 直换 |
| `.build_with_hasher(DefaultHashBuilder::default())`(447/473/488/589) | ✔(第三泛型参 = hash builder) | ✔ `moka::sync::Cache<K, V, S>` 同为第三泛型参 | 直换(precompile_cache.rs:44 已是现成模板:`moka::sync::Cache<Bytes, CacheEntry<S>, DefaultHashBuilder>`) |
| `.get(&K) -> Option<V>`(118/169/319/332/598) | `get(&Q)`,V: Clone | 同 | 直换 |
| `.insert(K, V)`(126/177/334/371/407/607) | ✔ | ✔ | 直换 |
| `.invalidate(&K)`(342/384) | ✔ | ✔(读侧立即不可见,内存回收延迟) | 直换 |
| `.iter()`(347) | 产出 `EntryRef`(Deref 到 V) | 产出 `(Arc<K>, V)` 元组 | **需改写** closure:`\|e\| e.len()` → `\|(_, v)\| v.len()` |
| `.entry_count()`(563/564/612) | 近似值,需 `cache.sync()` 才准(0.10.3 源码 docs 明示) | 近似值,需 `run_pending_tasks()` 才准 | **语义相同**,见下 |

语义差异核对(任务点名的三项):

1. **entry_count 与 metrics 漂移**:mini_moka 0.10.3 的 `entry_count()` docs 与
   moka 完全同款——"call `sync()` first to get accurate numbers"。gravity/v1.8.3
   代码本来就是**不 sync 直接读**,即 metrics 本来就接受近似值;换 moka 不会
   引入新的漂移类别(moka 的 pending 队列更深、批处理更明显,漂移幅度可能略大,
   但性质相同)。cached_state.rs 63/75/87 行注释里写的 "moka caches'
   entry_count" 甚至就是上游当年从 moka 视角写的注释原文。
2. **eviction / 并发结构**:两者同为 TinyLFU 族;moka 0.12 起**已无后台线程**
   (housekeeping 改为读写路径摊销 + 可显式 `run_pending_tasks()`),与
   mini-moka 的摊销模型同构,热路径无"housekeeping 线程"顾虑。moka 每操作
   开销略高(更完整的 frequency sketch / policy 维护);上游正是嫌这类通用
   cache 在 execution 热路径太重才做了 fixed-cache(#21128)。
3. **CacheBuilder 第三泛型参**:moka 等价写法即
   `CacheBuilder::new(n).build_with_hasher(DefaultHashBuilder::default())`,
   类型别名写作 `moka::sync::Cache<K, V, DefaultHashBuilder>`——与
   precompile_cache.rs 现行写法一致。

结论:若真要迁移,只有 `iter()` 一处需要动逻辑,其余是 `mini_moka::` →
`moka::` 换名,半小时工作量。但见下节——没有迁移的必要。

---

## 四、选项对比

### 选项 A(原文档建议):cached_state.rs 就地 mini_moka → moka

- 工作量:~5 行 use/类型别名 + iter() 一处 + Cargo 冲突取 moka 侧。
- **致命问题:改完仍是死代码。** 全 worktree 没有任何调用方引用
  `crate::tree::cached_state`;`mod cached_state;` 只存在于 mod.rs 未解决冲突的
  HEAD 侧。为一个即将被删的文件做后端迁移没有意义,反而在 review 里制造
  "gravity 定制了 cache?"的假信号。

### 选项 B:把 mini-moka 加回 workspace

- 工作量:根 Cargo.toml 加 1 行 + engine/tree Cargo 冲突取 HEAD 侧。
- 风险:
  - 维护两个 LFU cache crate(moka + mini-moka),mini-moka 0.10.3 是 2024 年初
    的最后一版,上游生态已整体离开;
  - 与上游漂移扩大:上游 v2.2.0+ 的 engine/tree 结构里根本没有这个文件,保留它
    意味着 mod.rs、payload_processor 等 4+ 个文件的冲突都要往"保旧结构"方向
    解,后续每次 merge 都要重打一遍;
  - 同样是保一个无调用方的死文件。

### 选项 C(核实后的正确解法):删除旧文件,整体采纳 v2.3.0 结构

具体动作(全部是"按 v2.3.0 侧解决既有冲突",无新增开发):

1. `crates/engine/tree/Cargo.toml` 89–94 行冲突:取 v2.3.0 侧(`moka`),丢弃
   `mini-moka`;HEAD 侧的 `smallvec` 经 grep 确认 engine/tree 全 crate 无使用,
   一并丢弃(若后续其他冲突文件解出 smallvec 使用再加回)。
2. `crates/engine/tree/src/tree/mod.rs` mod 声明冲突:取 v2.3.0 侧,即删除
   `mod cached_state;`(该冲突块还含 instrumented_state/payload_processor 的
   pub 化等,按该文件既定分析走)。
3. `git rm crates/engine/tree/src/tree/cached_state.rs`(与 baseline 一致、零
   定制,无任何需要抢救的 gravity 逻辑)。
4. 其余无需动:根 Cargo.toml(moka/fixed-cache/execution-cache member)、
   `crates/engine/execution-cache/`、`precompile_cache.rs` 均已与 v2.3.0 一致。

- 风险:**几乎为零**。唯一理论风险是"gravity 未来想给 execution cache 换后端"
  ——那也应该基于 v2.3.0 的 `reth-execution-cache` crate 做,而不是基于 v1.8.3
  的旧文件。
- 附带收益:workspace 少一个 dep;`fixed-cache` 是上游为这条热路径 benchmark
  后的选择(epoch invalidation 替代了旧版整 cache invalidate),gravity 白拿
  perf 改进。

---

## 五、建议

**采用选项 C:不做 mini_moka→moka 迁移,也不加回 mini-moka;删除
`crates/engine/tree/src/tree/cached_state.rs`,Cargo/mod.rs 冲突按 v2.3.0 侧
解决。** 合并后全仓 cache 格局 = 上游原样:execution cache → `fixed-cache`
(`reth-execution-cache`),precompile cache → `moka 0.12`。

理由浓缩:

1. gravity 从未定制过这个文件(与 v1.8.3 byte-identical),mini-moka 是纯
   fork 遗留,没有任何选型主张需要保卫;
2. worktree 里它已经没有调用方——所有 caller 已指向 `reth_execution_cache`;
3. "切 moka" 的原建议基于"上游 v2.3.0 用 moka"的误判;上游在 cached_state 上
   评估过 moka(实验分支)后选择了 fixed-cache,moka 从未进入过上游的
   execution cache 主线;
4. 三个动作全部落在本来就必须解决的既有冲突块内,零额外工作量。

原分析文档 `engine-evm-execution-chainspec.md` 的开放问题 #1 及
`crates/engine/tree/Cargo.toml` 小节的 "建议切到 moka" 措辞建议据此更正。

---

## 执行记录(2026-07-02)

用户拍板选项 C,已在 worktree 执行。逐步记录:

### 1. `crates/engine/tree/Cargo.toml`(原 89–94 行冲突块)

- 冲突块 HEAD 侧 = `mini-moka` + `smallvec` 两行,v2.3.0 侧 = `moka` 一行。
  **整块取 v2.3.0 侧**(现为单行
  `moka = { workspace = true, features = ["sync"] }`)。
- 动手前复核:`rg -n 'smallvec' crates/engine/tree/src` **零命中**,smallvec
  确认全 crate 无使用,随块丢弃。
- 该冲突块内**无** gravity-only 依赖;`reth-pipe-exec-layer-event-bus` /
  `gravity-primitives` 位于另一个冲突块(35–64 行,reth deps 段)的 HEAD 侧,
  **未触碰**,留待该文件其余冲突按既定分析解决。

### 2. `crates/engine/tree/src/tree/mod.rs`(原 110–123 行冲突块)

- 块内容纯 mod 声明差异:HEAD 侧多 `mod cached_state;`,且
  `instrumented_state` / `payload_processor` 为私有;v2.3.0 侧无 cached_state
  且二者 pub 化。满足"只有 mod 声明差异"条件,**整块取 v2.3.0 侧**
  (非最小改动路径;`pub mod instrumented_state` / `pub mod payload_processor`
  的 pub 化一并采纳,与上游 v2.3.0 结构及 worktree 已就位的 caller 一致)。
- 该文件其余全部冲突块(30+ 个)**原样保留**,未触碰。

### 3. 删除 `crates/engine/tree/src/tree/cached_state.rs`

- `git rm crates/engine/tree/src/tree/cached_state.rs`,删除已进 index
  (`git status` 显示 `D `)。

### 4. 静态验证结果

- `rg 'mini_moka|mini-moka'` 全仓(排除 `target/`):**源码与
  manifest 零命中**。残留仅两类,均非代码:
  - `Cargo.lock`(2 处)— workspace 目前带冲突标记无法 parse,`cargo` 跑不
    起来,lock 只能等 merge 收尾后首次 cargo 调用自动再生;
  - `docs/merge-v2.3.0/engine-evm-execution-chainspec.md`(git-tracked 的
    团队共享分析文档,含旧的"建议切 moka"措辞)— 见下"偏差与遗留"。
- `rg 'cached_state' crates/`:**无任何模块路径引用**
  (`tree::cached_state` / `crate::tree::cached_state` / `super::cached_state`
  全零命中,含其它文件未解决冲突块的 HEAD 侧)。剩余字符串命中均无关:
  新 crate `crates/engine/execution-cache/` 自身(`lib.rs` 的
  `mod cached_state;` + 测试函数名)、rpc `validation.rs` 的 `cached_state`
  字段(CachedReads,同名不同物)、optimism/flashblocks、trie proof_v2
  局部变量名。
- `rg 'SavedCache|CachedStateMetrics' crates/engine/tree/src`:类型来源全部
  为 `reth_execution_cache` —— `payload_validator.rs:73` /
  `bal_prewarm_pool.rs:2` / `payload_processor/mod.rs:1049` 直接
  `use reth_execution_cache::...`;`payload_processor/mod.rs` 与 `prewarm.rs`
  头部经 `crate::tree::{...}` 走 `tree/mod.rs:144` 的
  `pub use reth_execution_cache::{...}` re-export。注意该 re-export 本身位于
  mod.rs 另一个**未解决冲突块的 v2.3.0 侧**(HEAD 侧只有
  `use reth_trie::KeccakKeyHasher;`,不提供这些名字)——该块按规矩未碰,
  但它后续必须往 v2.3.0 方向解,与本决策自洽。
- `git status --porcelain`:仅预期三项(`M Cargo.toml`、`M mod.rs`、
  `D cached_state.rs`)+ 先前已存在的 untracked 文件(均未 add)。

### 偏差与遗留

1. **mod.rs 走了"整块取 v2.3.0 侧"而非最小改动**(块内确实只有 mod 声明
   差异,符合预设条件),顺带采纳了 instrumented_state / payload_processor
   的 pub 化。
2. **`Cargo.lock` 仍含 mini-moka 条目**——mid-merge 无法 cargo 再生,属预期
   残留,merge 收尾编译时自动消失,无需手工编辑。
3. ~~git-tracked 的 `docs/merge-v2.3.0/engine-evm-execution-chainspec.md`
   未更新~~ — 已于同日同步:分析文档以 `docs/merge-v2.3.0/` 为准,"建议切到
   moka"措辞已更正,本报告亦迁入同目录。
