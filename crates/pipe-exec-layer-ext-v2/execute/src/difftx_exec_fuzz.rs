//! A×B 融合 fuzz：把 proptest 的**对抗块**生成能力接到 `difftx_exec` 的
//! serial(revm `WrapExecutor`) ⟷ grevm(`GrevmExecutor`) 执行差分预言机上。
//!
//! ## 被测不变式
//!
//! 对**任意**区块，串行 revm 顺序执行与 grevm 并行执行必须逐字节一致
//! （`BundleState.state/contracts/state_size` + 语义化 `reverts` + receipts + gas）。
//! 任何背离（`ExecDiff::diverged`）= 一次**共识 state-fork**；任一侧 `Ok` 另一侧 `Err`
//! （或两侧 panic）= **非对称停链**。二者都是真 finding。
//!
//! ## 生成策略（覆盖并行执行 hazard 类）
//!
//! 每个策略镜像并**参数化** `difftx_exec::exec_adversarial_cases` 的手搓用例：
//!
//! 1. `storage_conflict_strategy` —— 同/异存储槽 RAW/WAW：2..4 笔调用同一
//!    「读-改-写(RMW)」合约，槽下标由 calldata 决定（同槽=依赖，异槽=可并行）。
//! 2. `nonce_chain_strategy` —— 同发送者 nonce 链与其它发送者交织（顺序敏感）。
//! 3. `recipient_waw_strategy` —— 多个不同发送者向**同一收款人**转账（余额并行累加 WAW）。
//! 4. `create_strategy` —— 直接 CREATE（nonce 派生地址）+ 经工厂 CREATE2（同 salt→地址碰撞）混合。
//! 5. `eip7702_strategy` —— 多笔交易触碰**同一** 7702 委托账户（委托代码在 D 的上下文 SSTORE）。
//! 6. `selfdestruct_strategy` —— 多个 SELFDESTRUCT 合约向同一受益人转账（账户触碰 + 余额 WAW）。
//!
//! 每个生成的 `ExecCase` → `run_exec_diff` → `prop_assert!(!diverged)` 且
//! `prop_assert_eq!(serial_ok, grevm_ok)`。若 proptest 最小化出 `diverged` 反例，
//! **不 allowlist**——让测试大声失败并捕获最小反例（即一次真实的 fork/halt 发现）。
//!
//! proptest 默认确定性（不触任何 wall-clock / RNG API）。每个 `run_exec_diff` 跑**两**次
//! 整块执行且重建主网 ChainSpec（~0.5s/case 主导），故每策略 case 数按需拨盘（20..32），
//! 全套控制在 ~90s 内。纯测试期 harness。

use alloy_consensus::{TxEip1559, TxEip7702, TxLegacy};
use alloy_eips::{
    eip2930::AccessList,
    eip7702::{Authorization, SignedAuthorization},
};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use proptest::{
    collection::vec as prop_vec,
    prelude::*,
    strategy::{BoxedStrategy, Strategy, ValueTree},
    test_runner::TestRunner,
};
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use revm::state::{AccountInfo, Bytecode};
use std::collections::HashMap;

// 复用 difftx.rs 的种子类型 + difftx_exec.rs 的执行差分预言机。
use crate::{
    difftx::SeedAccount,
    difftx_exec::{run_exec_diff, ExecCase},
};

// ————————————————————————————————— 常量 ————————————————————————————————— //

/// 主网 chain id（typed 交易的 `chain_id`）。
const MAINNET_CHAIN_ID: u64 = 1;
/// 充裕余额（100 ETH）——保证「余额不是被测因素」，让块能真执行（非因余额门整块 Err）。
const HUGE_BALANCE: u128 = 100_000_000_000_000_000_000;
/// SELFDESTRUCT 合约的预置余额（用于制造受益人余额 WAW）。
const SD_BALANCE: u128 = 1_000_000_000_000;
/// 统一 base fee。
const BASE_FEE: u64 = 1;
/// 统一 gas price / `max_fee`（> `base_fee`，保证不被 fee 门整块拒绝）。
const GAS_PRICE: u128 = 10;
/// 统一区块 gas limit。
const BLOCK_GAS_LIMIT: u64 = 30_000_000;

// ————————————————————————————————— 地址工具 ————————————————————————————————— //

/// 确定性地址：`0x00..00 || b`。
const fn addr(b: u8) -> Address {
    Address::with_last_byte(b)
}

// ————————————————————————————————— 字节码生成 ————————————————————————————————— //

/// 读-改-写(RMW)合约：槽下标从 **calldata** 取（`CALLDATALOAD(0)`），`slot += 1`。
/// 汇编：`PUSH1 0; CALLDATALOAD; DUP1; SLOAD; PUSH1 1; ADD; SWAP1; SSTORE; STOP`。
/// 同槽多笔 = RAW/WAW 依赖（grevm 必须串行化，使 `slot == 命中次数`）；异槽则可并行。
fn rmw_calldata_slot_code() -> Vec<u8> {
    vec![0x60, 0x00, 0x35, 0x80, 0x54, 0x60, 0x01, 0x01, 0x90, 0x55, 0x00]
}

/// SELFDESTRUCT 合约：受益人从 **calldata** 取（`CALLDATALOAD(0)` 低 20 字节）。
/// 汇编：`PUSH1 0; CALLDATALOAD; SELFDESTRUCT`。Cancun 后对**预置**（非同 tx 创建）合约，
/// SELFDESTRUCT 只把余额转给受益人、不删账户 → 多笔向同一受益人 = 余额 WAW + 账户触碰。
fn selfdestruct_calldata_code() -> Vec<u8> {
    vec![0x60, 0x00, 0x35, 0xff]
}

/// CREATE2 工厂运行时：把「部署空 runtime 的 init code」写进内存，再用 **calldata 里的 salt**
/// 做 `CREATE2(value=0, offset=0, size=5, salt)`；`POP; STOP`。
/// 同 salt 的两笔调用 → 目标地址**碰撞** → 第二笔 CREATE2 失败返回 0，但交易本身成功。
/// 汇编：`PUSH32 <init||0..>; PUSH1 0; MSTORE; PUSH1 0; CALLDATALOAD; PUSH1 5; PUSH1 0; PUSH1 0;
/// CREATE2; POP; STOP`。
fn create2_factory_code() -> Vec<u8> {
    // 被部署的 init code：`PUSH1 0; PUSH1 0; RETURN` → 返回空 runtime。放在 32 字节字的高 5 字节
    // （MSTORE 大端写入 → 落在 memory[0..5]，正好被 offset=0/size=5 读取）。
    let mut word = [0u8; 32];
    word[0..5].copy_from_slice(&[0x60, 0x00, 0x60, 0x00, 0xf3]);
    let mut code = vec![0x7f]; // PUSH32
    code.extend_from_slice(&word);
    code.extend_from_slice(&[
        0x60, 0x00, 0x52, // PUSH1 0; MSTORE   —— memory[0..32] = word
        0x60, 0x00, 0x35, // PUSH1 0; CALLDATALOAD —— salt
        0x60, 0x05, // PUSH1 5   —— size
        0x60, 0x00, // PUSH1 0   —— offset
        0x60, 0x00, // PUSH1 0   —— value
        0xf5, // CREATE2
        0x50, // POP
        0x00, // STOP
    ]);
    code
}

/// 直接 CREATE 交易的 init code：部署空 runtime（`PUSH1 0; PUSH1 0; RETURN`）。
fn empty_deploy_init() -> Vec<u8> {
    vec![0x60, 0x00, 0x60, 0x00, 0xf3]
}

// ————————————————————————————————— calldata 编码 ————————————————————————————————— //

/// 32 字节字，末字节 = `slot`（`CALLDATALOAD(0)` 大端解读即 `slot`）。
fn slot_calldata(slot: u8) -> Bytes {
    let mut w = [0u8; 32];
    w[31] = slot;
    Bytes::from(w.to_vec())
}

/// 32 字节字，低 20 字节 = 地址（供 SELFDESTRUCT 受益人 / 通用地址参数）。
fn addr_calldata(a: Address) -> Bytes {
    let mut w = [0u8; 32];
    w[12..32].copy_from_slice(a.as_slice());
    Bytes::from(w.to_vec())
}

// ————————————————————————————————— 账户种子 ————————————————————————————————— //

/// EOA 种子（空代码）。
fn eoa(a: Address, balance: u128, nonce: u64) -> SeedAccount {
    SeedAccount {
        addr: a,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: alloy_consensus::constants::KECCAK_EMPTY,
            code: None,
        },
    }
}

/// 带真实字节码的合约账户种子（nonce=1，已部署）。
fn contract(a: Address, code_bytes: Vec<u8>, balance: u128) -> SeedAccount {
    let code = Bytecode::new_raw(Bytes::from(code_bytes));
    SeedAccount {
        addr: a,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce: 1,
            code_hash: code.hash_slow(),
            code: Some(code),
        },
    }
}

/// EIP-7702 委托账户种子：code = `0xef0100 || delegate`。
fn delegated_account(a: Address, delegate: Address, balance: u128, nonce: u64) -> SeedAccount {
    let code = Bytecode::new_eip7702(delegate);
    SeedAccount {
        addr: a,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: code.hash_slow(),
            code: Some(code),
        },
    }
}

// ————————————————————————————————— 交易构造 ————————————————————————————————— //

/// legacy CALL 交易（可带 calldata）。`chain_id`=None（pre-155，合法）与 `difftx_exec` 一致。
fn legacy_call(
    nonce: u64,
    gas_limit: u64,
    value: u128,
    to: Address,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price: GAS_PRICE,
            gas_limit,
            to: TxKind::Call(to),
            value: U256::from(value),
            input,
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// EIP-1559 CALL 交易（可带 calldata）—— 提供 typed 交易类型覆盖。
fn eip1559_call(
    nonce: u64,
    gas_limit: u64,
    value: u128,
    to: Address,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Eip1559(TxEip1559 {
            chain_id: MAINNET_CHAIN_ID,
            nonce,
            gas_limit,
            max_fee_per_gas: GAS_PRICE,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(to),
            value: U256::from(value),
            access_list: AccessList::default(),
            input,
        }),
        Signature::test_signature(),
    )
}

/// legacy CREATE 交易（nonce 派生地址）。
fn legacy_create(nonce: u64, gas_limit: u64, init: Bytes) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price: GAS_PRICE,
            gas_limit,
            to: TxKind::Create,
            value: U256::ZERO,
            input: init,
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// EIP-7702（type-0x04）交易，`to` 走其已存在的委托代码（授权项为自委托占位，
/// 委托效果由**预置的** `delegated_account` 提供）。镜像 `difftx_exec::eip7702_tx`。
fn eip7702_call(nonce: u64, gas_limit: u64, to: Address, input: Bytes) -> TransactionSigned {
    let authorization_list = vec![SignedAuthorization::new_unchecked(
        Authorization { chain_id: U256::from(MAINNET_CHAIN_ID), address: to, nonce: 0 },
        0,
        U256::from(1u64),
        U256::from(1u64),
    )];
    TransactionSigned::new_unhashed(
        Transaction::Eip7702(TxEip7702 {
            chain_id: MAINNET_CHAIN_ID,
            nonce,
            gas_limit,
            max_fee_per_gas: GAS_PRICE,
            max_priority_fee_per_gas: 0,
            to,
            value: U256::ZERO,
            access_list: AccessList::default(),
            authorization_list,
            input,
        }),
        Signature::test_signature(),
    )
}

// ————————————————————————————————— nonce 分配 ————————————————————————————————— //

/// 按区块顺序给每个发送者分配**递增** nonce（种子起始 nonce 均为 0）。
/// 同发送者的多笔自然构成有效链，且与其它发送者交织后各自仍按 nonce 升序出现。
fn assign_nonces(senders: &[Address]) -> Vec<u64> {
    let mut counts: HashMap<Address, u64> = HashMap::new();
    senders
        .iter()
        .map(|s| {
            let n = *counts.get(s).unwrap_or(&0);
            counts.insert(*s, n + 1);
            n
        })
        .collect()
}

// ————————————————————————————————— 策略 1：存储 RAW/WAW ————————————————————————————————— //

/// RMW 合约地址。
const STG_CONTRACT: Address = addr(0x1C);
/// 发送者池基址（0x11..=0x14）。
const STG_SENDER_BASE: u8 = 0x11;

/// 每笔：(发送者下标 0..4, 槽 0..3, 是否用 1559)。同槽 → 依赖；异槽 → 可并行。
fn storage_conflict_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec((0usize..4usize, 0u8..3u8, any::<bool>()), 2..=4).prop_map(|specs| {
        let senders: Vec<Address> =
            specs.iter().map(|(si, _, _)| addr(STG_SENDER_BASE + *si as u8)).collect();
        let nonces = assign_nonces(&senders);
        let txs: Vec<TransactionSigned> = specs
            .iter()
            .zip(&nonces)
            .map(|((_, slot, use_1559), n)| {
                let cd = slot_calldata(*slot);
                if *use_1559 {
                    eip1559_call(*n, 200_000, 0, STG_CONTRACT, cd)
                } else {
                    legacy_call(*n, 200_000, 0, STG_CONTRACT, cd)
                }
            })
            .collect();
        // 播种：4 个 EOA + RMW 合约。
        let mut seed: Vec<SeedAccount> =
            (0u8..4).map(|i| eoa(addr(STG_SENDER_BASE + i), HUGE_BALANCE, 0)).collect();
        seed.push(contract(STG_CONTRACT, rmw_calldata_slot_code(), 0));
        ExecCase {
            name: format!("fuzz_storage/{}tx", txs.len()),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— 策略 2：同发送者 nonce 链 ————————————————————————————————— //

/// nonce 链发送者池基址（0x21..=0x23）。
const NC_SENDER_BASE: u8 = 0x21;
/// nonce 链的独立收款人基址（每笔一个，隔离掉收款人竞争，只留 nonce 依赖）。
const NC_SINK_BASE: u8 = 0x71;

/// 2..5 笔来自 3 个发送者之一；每笔转账到**各自独立**的收款人 → 唯一 hazard 是同发送者
/// nonce 顺序（与其它发送者交织）。
fn nonce_chain_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec(0usize..3usize, 2..=5).prop_map(|sender_idxs| {
        let senders: Vec<Address> =
            sender_idxs.iter().map(|si| addr(NC_SENDER_BASE + *si as u8)).collect();
        let nonces = assign_nonces(&senders);
        let txs: Vec<TransactionSigned> = nonces
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let sink = addr(NC_SINK_BASE + i as u8);
                legacy_call(*n, 21_000, 100, sink, Bytes::new())
            })
            .collect();
        // 播种：3 个发送者 + 每笔一个收款人。
        let mut seed: Vec<SeedAccount> =
            (0u8..3).map(|i| eoa(addr(NC_SENDER_BASE + i), HUGE_BALANCE, 0)).collect();
        for i in 0..txs.len() as u8 {
            seed.push(eoa(addr(NC_SINK_BASE + i), 0, 0));
        }
        ExecCase {
            name: format!("fuzz_nonce_chain/{}tx", txs.len()),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— 策略 3：同收款人余额 WAW ————————————————————————————————— //

/// 收款人地址。
const RW_RECIPIENT: Address = addr(0x3F);
/// 发送者池基址（0x31..，每笔一个不同发送者，nonce 全 0）。
const RW_SENDER_BASE: u8 = 0x31;

/// 2..5 个**不同**发送者各转一笔到同一收款人 R → R 的余额并行累加 WAW。
fn recipient_waw_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec(1u128..=5_000u128, 2..=5).prop_map(|values| {
        let n = values.len();
        let senders: Vec<Address> = (0..n).map(|i| addr(RW_SENDER_BASE + i as u8)).collect();
        let txs: Vec<TransactionSigned> =
            values.iter().map(|v| legacy_call(0, 21_000, *v, RW_RECIPIENT, Bytes::new())).collect();
        let mut seed: Vec<SeedAccount> =
            (0..n).map(|i| eoa(addr(RW_SENDER_BASE + i as u8), HUGE_BALANCE, 0)).collect();
        seed.push(eoa(RW_RECIPIENT, 0, 0));
        ExecCase {
            name: format!("fuzz_recipient_waw/{n}tx"),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— 策略 4：CREATE / CREATE2 ————————————————————————————————— //

/// CREATE2 工厂地址。
const CR_FACTORY: Address = addr(0x4C);
/// 发送者池基址（0x41..=0x44）。
const CR_SENDER_BASE: u8 = 0x41;

/// 每笔：(kind 0=直接 CREATE / 1=经工厂 CREATE2, 发送者下标 0..4, salt 0..3)。
/// 直接 CREATE → nonce 派生地址（并行创建账户）；同 salt 的多笔工厂调用 → CREATE2 地址碰撞。
fn create_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec((0u8..2u8, 0usize..4usize, 0u8..3u8), 2..=4).prop_map(|specs| {
        let senders: Vec<Address> =
            specs.iter().map(|(_, si, _)| addr(CR_SENDER_BASE + *si as u8)).collect();
        let nonces = assign_nonces(&senders);
        let txs: Vec<TransactionSigned> = specs
            .iter()
            .zip(&nonces)
            .map(|((kind, _, salt), n)| {
                if *kind == 0 {
                    legacy_create(*n, 200_000, Bytes::from(empty_deploy_init()))
                } else {
                    legacy_call(*n, 300_000, 0, CR_FACTORY, slot_calldata(*salt))
                }
            })
            .collect();
        let mut seed: Vec<SeedAccount> =
            (0u8..4).map(|i| eoa(addr(CR_SENDER_BASE + i), HUGE_BALANCE, 0)).collect();
        seed.push(contract(CR_FACTORY, create2_factory_code(), 0));
        ExecCase {
            name: format!("fuzz_create/{}tx", txs.len()),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— 策略 5：7702 委托账户共享触碰 —————————————————————————————————
// //

/// 被委托的账户 D（code = 0xef0100 || C）。
const D7_DELEGATE: Address = addr(0x5D);
/// D 委托到的真实合约 C（RMW-by-calldata）。
const D7_TARGET: Address = addr(0x5C);
/// 发送者池基址（0x51..=0x53）。
const D7_SENDER_BASE: u8 = 0x51;

/// 2..4 笔（首笔为 type-4，其余 legacy CALL）来自 3 个发送者，**都触碰同一委托账户 D**：
/// 委托代码在 D 的上下文对 calldata 指定槽做 RMW → D 的存储 RAW/WAW（跨多笔）。
fn eip7702_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec((0usize..3usize, 0u8..2u8), 2..=4).prop_map(|specs| {
        let senders: Vec<Address> =
            specs.iter().map(|(si, _)| addr(D7_SENDER_BASE + *si as u8)).collect();
        let nonces = assign_nonces(&senders);
        let txs: Vec<TransactionSigned> = specs
            .iter()
            .zip(&nonces)
            .enumerate()
            .map(|(i, ((_, slot), n))| {
                let cd = slot_calldata(*slot);
                if i == 0 {
                    // 首笔用 type-0x04 交易走 7702 执行路径（to=D，携自委托占位授权）。
                    eip7702_call(*n, 300_000, D7_DELEGATE, cd)
                } else {
                    legacy_call(*n, 200_000, 0, D7_DELEGATE, cd)
                }
            })
            .collect();
        // 播种：3 个发送者 + 委托账户 D + 真实合约 C。
        let mut seed: Vec<SeedAccount> =
            (0u8..3).map(|i| eoa(addr(D7_SENDER_BASE + i), HUGE_BALANCE, 0)).collect();
        seed.push(delegated_account(D7_DELEGATE, D7_TARGET, 0, 0));
        seed.push(contract(D7_TARGET, rmw_calldata_slot_code(), 0));
        ExecCase {
            name: format!("fuzz_7702/{}tx", txs.len()),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— 策略 6：SELFDESTRUCT / 账户触碰
// ————————————————————————————————— //

/// 两个 SELFDESTRUCT 合约（各带余额）。
const SD_A: Address = addr(0x6A);
const SD_B: Address = addr(0x6B);
/// 两个候选受益人。
const SD_R0: Address = addr(0x6E);
const SD_R1: Address = addr(0x6F);
/// 发送者池基址（0x61..=0x63）。
const SD_SENDER_BASE: u8 = 0x61;

/// 2..4 笔：(合约选择 0/1, 受益人选择 0/1, 发送者下标 0..3)。
/// 多笔向**同一受益人** SELFDESTRUCT → 受益人余额 WAW；重复调用**同一合约** →
/// 账户触碰/余额清零交织。
fn selfdestruct_strategy() -> impl Strategy<Value = ExecCase> {
    prop_vec((any::<bool>(), any::<bool>(), 0usize..3usize), 2..=4).prop_map(|specs| {
        let senders: Vec<Address> =
            specs.iter().map(|(_, _, si)| addr(SD_SENDER_BASE + *si as u8)).collect();
        let nonces = assign_nonces(&senders);
        let txs: Vec<TransactionSigned> = specs
            .iter()
            .zip(&nonces)
            .map(|((use_b, use_r1, _), n)| {
                let target = if *use_b { SD_B } else { SD_A };
                let benef = if *use_r1 { SD_R1 } else { SD_R0 };
                legacy_call(*n, 200_000, 0, target, addr_calldata(benef))
            })
            .collect();
        // 播种：3 个发送者 + 2 个 SELFDESTRUCT 合约（带余额）+ 2 个受益人。
        let mut seed: Vec<SeedAccount> =
            (0u8..3).map(|i| eoa(addr(SD_SENDER_BASE + i), HUGE_BALANCE, 0)).collect();
        seed.push(contract(SD_A, selfdestruct_calldata_code(), SD_BALANCE));
        seed.push(contract(SD_B, selfdestruct_calldata_code(), SD_BALANCE));
        seed.push(eoa(SD_R0, 0, 0));
        seed.push(eoa(SD_R1, 0, 0));
        ExecCase {
            name: format!("fuzz_selfdestruct/{}tx", txs.len()),
            txs,
            senders,
            seed,
            base_fee: BASE_FEE,
            block_gas_limit: BLOCK_GAS_LIMIT,
        }
    })
}

// ————————————————————————————————— proptest 断言 ————————————————————————————————— //

/// 对一个生成的 `ExecCase` 施加共识不变式：无背离 + 两侧执行结果对称（同 Ok/Err）。
/// 若 proptest 最小化出反例（`diverged`），即为一次真实的 serial⟷grevm 共识 fork / 非对称停链。
fn assert_no_divergence(case: &ExecCase) -> Result<(), TestCaseError> {
    let d = run_exec_diff(case);
    prop_assert!(
        !d.diverged,
        "serial⟷grevm 背离（疑似共识 state-fork / 非对称停链）\n case={}\n divergence={:?}\n \
         serial_ok={} grevm_ok={}\n minimized ExecCase={:#?}",
        d.name,
        d.divergence,
        d.serial_ok,
        d.grevm_ok,
        case
    );
    prop_assert_eq!(
        d.serial_ok,
        d.grevm_ok,
        "单侧执行失败（可能一侧 panic/Err，另一侧成功）：{} divergence={:?}",
        d.name,
        d.divergence
    );
    Ok(())
}

// 每个策略拆成独立 `proptest!` 块以逐测试拨盘 case 数（重建 ChainSpec ~0.5s/case 主导）。
// 富 hazard 的类给 32，其余 20..24，全套 ~144 case，控制在 ~90s 内。

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// 1) 存储 RAW/WAW：同/异槽 RMW 的 serial⟷grevm 一致性。
    #[test]
    fn storage_conflict_no_divergence(case in storage_conflict_strategy()) {
        assert_no_divergence(&case)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// 2) 同发送者 nonce 链交织。
    #[test]
    fn nonce_chain_no_divergence(case in nonce_chain_strategy()) {
        assert_no_divergence(&case)?;
    }

    /// 3) 同收款人余额 WAW。
    #[test]
    fn recipient_waw_no_divergence(case in recipient_waw_strategy()) {
        assert_no_divergence(&case)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    /// 4) CREATE / CREATE2（含地址碰撞）。
    #[test]
    fn create_no_divergence(case in create_strategy()) {
        assert_no_divergence(&case)?;
    }

    /// 6) SELFDESTRUCT / 账户触碰交织。
    #[test]
    fn selfdestruct_no_divergence(case in selfdestruct_strategy()) {
        assert_no_divergence(&case)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// 5) 7702 委托账户被多笔触碰。
    #[test]
    fn eip7702_no_divergence(case in eip7702_strategy()) {
        assert_no_divergence(&case)?;
    }
}

// ————————————————————————————————— 非平凡性（反-vacuity）门 ————————————————————————————————— //

/// **证明生成器确实产出「两侧都能真执行」的块**：从每个策略确定性采样若干用例，
/// 断言其中至少有一个 `serial_ok && grevm_ok`。否则上面所有 `!diverged` 断言可能在
/// 「所有块都单侧/双侧硬失败」的退化空间里平凡通过，从而观察不到任何真背离。
#[test]
fn generator_actually_executes() {
    let mut runner = TestRunner::deterministic();
    let strategies: Vec<(&str, BoxedStrategy<ExecCase>)> = vec![
        ("storage", storage_conflict_strategy().boxed()),
        ("nonce_chain", nonce_chain_strategy().boxed()),
        ("recipient_waw", recipient_waw_strategy().boxed()),
        ("create", create_strategy().boxed()),
        ("eip7702", eip7702_strategy().boxed()),
        ("selfdestruct", selfdestruct_strategy().boxed()),
    ];

    let mut total = 0usize;
    let mut both_ok = 0usize;
    for (name, st) in &strategies {
        // 每策略采样 2 个，兼顾覆盖与速度。
        for _ in 0..2 {
            let case = st.new_tree(&mut runner).expect("strategy 采样成功").current();
            let d = run_exec_diff(&case);
            total += 1;
            if d.serial_ok && d.grevm_ok {
                both_ok += 1;
            }
            println!(
                "[non-vacuity] {name:<14} {:<28} serial_ok={} grevm_ok={} diverged={}",
                d.name, d.serial_ok, d.grevm_ok, d.diverged
            );
        }
    }

    assert!(
        both_ok > 0,
        "非平凡性失败：{total} 个采样块无一在两侧都成功执行（serial_ok && grevm_ok）——\
         生成器全在产出硬失败的块，`!diverged` 断言会平凡通过、观察不到真背离"
    );
}
