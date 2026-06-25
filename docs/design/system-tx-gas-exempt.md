# System Transactions: gas-exempt + zero-balance SYSTEM_CALLER

> **Status: Draft / skeleton.** 实现追踪 [gravity-reth#364](https://github.com/Galxe/gravity-reth/issues/364) ·
> 分析 [gravity-audit#720](https://github.com/Galxe/gravity-audit/issues/720)。
> 本文件是 hardfork 实现的设计骨架,代码尚未落地(见下方落点地图 + checklist)。

## 问题

系统交易发送账户 `SYSTEM_CALLER`（`0x00000000000000000000000000000001625f0000`）在 genesis 被预分配「哨兵级无穷大」余额（≈ 1.16×10⁵⁸ G），用来支付每块系统交易的 base fee（实测系统 tx `gas_price=baseFee`、无 tip，base fee 从该账户余额烧掉 ~0.006 G/块）。

虽然已验证假供应未流通、footgun 未触发,但带来 4 个问题:污染供应核算 / 增发 footgun（任何余额外流的 bug = 无上限增发真 G）/ 为系统 tx 从假账户烧 base fee / 软不变量。

## 生产先例

| 维度 | EIP-4788/2935 | Arbitrum `0x6a` ArbInternalTx | Gravity 现状 | 本方案 |
| --- | --- | --- | --- | --- |
| receipt / 入块可见 | 无（幽灵） | 有（实测 status=0x1） | 有 | 保留 |
| nonce 自增 | 否 | 否（恒 0，from=`0xA4B05`） | 是（≈块高） | 是（保留；可选改静态） |
| gas / fee | 免费、计量 | 全免（gasPrice/gasUsed=0） | 收 base fee（烧假余额） | 免费、计量 |
| 发送方余额 | 0 | 0 | 1.16×10⁵⁸ 哨兵 | fork 后清 0 |

费用语义与 4788/2935 一致;与 Arbitrum 最接近（真交易 + receipt + 完全免费 + 零余额）。

## 方案（必须 hardfork —— 改状态根,须全节点在同一 fork 高度原子切换）

### ① 执行层:系统 tx 免 fee/balance（保留 gas 计量）

fork 激活后,对系统 tx 的 EVM env:
- `cfg_env.disable_base_fee = true`
- `cfg_env.disable_balance_check = true`
- 系统 tx `gas_price = 0`
- `disable_nonce_check` 保持 `false`（nonce 序列照常自增）

执行 / calldata / state / receipt / gas_used 全照旧,只是不记账收费。复用仓库已有的 revm cfg 标志。

### ② 状态迁移:fork 块把 SYSTEM_CALLER 余额清 0

fork 激活块做一次确定性状态写 `SYSTEM_CALLER.balance = 0`。nonce 保留（>0 → 非空,不被 EIP-161 裁剪）。

## 代码落点地图（实现者参考）

| 改动点 | 位置 |
| --- | --- |
| `SYSTEM_CALLER` 常量 | `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/mod.rs:55` |
| 系统 tx 执行（设 cfg 标志）| `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs` → `transact_system_txn` |
| serial 后端系统 tx 路径 | `crates/ethereum/evm/src/lib.rs`（#363 在此加 `set_state_clear_flag`,同处加 fee/balance 豁免）|
| grevm 并行后端系统 tx 路径 | `crates/ethereum/evm/src/parallel_execute.rs`（**必须同 serial 一起改**，否则状态根分叉）|
| revm cfg 标志参考用法 | `crates/rpc/rpc-eth-api/src/helpers/call.rs`（`disable_base_fee` / `disable_nonce_check` 已在用）|
| fork 块清零余额 | block 执行的 `apply_pre_execution_changes` / fork hook |
| hardfork 注册 | chainspec 的 fork time（参考 `pragueTime` 接法）|

## Checklist

- [ ] serial（disable-grevm）与 grevm 两路同样改（同 #363，否则系统-tx 块状态根分叉）
- [ ] 确认无合约/逻辑依赖 SYSTEM_CALLER 余额（它是身份 `msg.sender`，非钱）
- [ ] nonce 序列保持、receipt 不变
- [ ] coinbase 不受影响（本就无 tip）
- [ ] gas 上限仍在（免费 != 不限制）
- [ ] fork 门控 + pre-fork 行为不变
- [ ] e2e（testnet）：fork 后余额恒 0、系统 tx 执行+receipt 正确、无 base fee、serial==parallel 状态根、nonce 连续、fork-transition 块确定性清零、总供应量回真实值
- [ ] staging → mainnet 走 hardfork SOP（单 PR + atomic image swap + >=24h lead）
