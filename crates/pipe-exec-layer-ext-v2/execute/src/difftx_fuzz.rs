//! 交易 / 交易序列 **proptest fuzz 生成器** —— 把 `crate::difftx` 里手搓的 12 个单笔用例 +
//! 1 条序列，升级成对**整个字段空间**的自动搜索。
//!
//! ## 被测不变式
//!
//! `tx_filter::filter_invalid_txs` 必须是 revm 交易级校验的**超集**：任何被 filter 放行
//! （ADMIT）却被 revm 以 `InvalidTransaction` 拒绝的交易，都是能打停真链的停链缺口
//! （见 `difftx.rs` / `tx_filter.rs` 模块头，`lib.rs:1323` 的 `.unwrap()` panic 点）。
//!
//! 本模块用 proptest 随机生成 `TransactionSigned`（tx 类型 / nonce / gas / fee / value /
//! initcode 长度（跨 EIP-3860 边界）/ chain_id / access-list / 7702 授权列表 各字段的对抗
//! 组合）+ 匹配的 `SeedAccount` 种子，喂给 `run_case` / `run_seq_case` 两侧预言机，断言：
//!
//! 1. **单笔字段级**（`fuzz_single_tx_no_filter_gap`）：`prop_assert!(!out.is_gap)` —— filter
//!    永不放行 revm 会拒绝的单笔交易，对**整个生成空间**成立。
//! 2. **多笔序列**（`fuzz_seq_no_exec_gap`）：`prop_assert!(out.exec_gaps.is_empty())` —— 未预置
//!    委托指示符的普通序列不应产生任何执行期缺口；一旦出现即为 NEW finding。
//! 3. **7702 授权列表**（`fuzz_7702_auth_list_no_exec_gap`）：用 `alloy_signer_local` 真实签名， 让
//!    `recover_authority()` 得到**受控**的 authority 地址，探测 filter 的授权-nonce 模型是否 与
//!    revm 执行一致；断言无新执行缺口（已知的委托-CREATE nonce 竞态需预置 CREATE 运行时的
//!    委托代码，本策略不构造，故预期零缺口）。
//!
//! proptest 默认确定性（不调用任何 wall-clock / 随机种子 API），发现反例会自动最小化并打印。
//! 纯测试期 harness。

use alloy_consensus::{constants::KECCAK_EMPTY, TxEip1559, TxEip7702, TxLegacy};
use alloy_eips::{
    eip2930::{AccessList, AccessListItem},
    eip7702::{Authorization, SignedAuthorization},
};
use alloy_primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use proptest::{collection::vec as prop_vec, prelude::*};
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use revm::state::{AccountInfo, Bytecode};
use revm_primitives::eip3860::MAX_INITCODE_SIZE;

use crate::difftx::{run_case, run_seq_case, DiffCase, SeedAccount, SeqCase};

/// 主网 chain id —— 与 `difftx.rs` 的测试 chainspec 对齐（filter 的 chain-id 门拒绝不等此值的
/// typed 交易）。
const MAINNET_CHAIN_ID: u64 = 1;
/// 一个错误 chain id（用于制造 `InvalidChainId`）。
const WRONG_CHAIN_ID: u64 = 999;
/// 充裕余额（1 ETH），供「余额不是被测因素」的用例。
const BIG_BALANCE: u128 = 1_000_000_000_000_000_000;
/// 单笔 fuzz 的固定发送者地址（确定性，便于复现）。
const FUZZ_SENDER: Address = Address::with_last_byte(0x11);
/// 序列 fuzz 的固定发送者集合。
const SEQ_SENDERS: [Address; 3] =
    [Address::with_last_byte(0x11), Address::with_last_byte(0x12), Address::with_last_byte(0x13)];

// —————————————————————————————— 账户 / 交易 构造辅助 —————————————————————————————— //

/// 按 `code_variant` 造账户信息：0=空码 EOA；1=非委托代码（触发 EIP-3607）；
/// 2=7702 委托指示符（Pectra 放行）。三者在两侧预言机上应当行为一致。
fn build_account(balance: u128, nonce: u64, code_variant: u8) -> AccountInfo {
    match code_variant {
        1 => {
            // 一段普通 EVM 字节码（非 0xef0100 委托指示符）。
            let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]));
            AccountInfo {
                balance: U256::from(balance),
                nonce,
                code_hash: code.hash_slow(),
                code: Some(code),
            }
        }
        2 => {
            // 指向任意目标的 7702 委托指示符 0xef0100||target。
            let code = Bytecode::new_eip7702(Address::with_last_byte(0x77));
            AccountInfo {
                balance: U256::from(balance),
                nonce,
                code_hash: code.hash_slow(),
                code: Some(code),
            }
        }
        _ => {
            AccountInfo { balance: U256::from(balance), nonce, code_hash: KECCAK_EMPTY, code: None }
        }
    }
}

/// 造一个 `len` 项的 access list（每项一个存储槽），用于覆盖 access-list 内联 gas 分支。
fn build_access_list(len: usize) -> AccessList {
    AccessList(
        (0..len)
            .map(|i| AccessListItem {
                address: Address::with_last_byte(0x30u8.wrapping_add(i as u8)),
                storage_keys: vec![B256::ZERO],
            })
            .collect(),
    )
}

/// 造 `n` 个「未签名占位」授权项（`r=s=0`，`recover_authority()` 会失败 → 两侧都跳过），
/// 用于在不牵扯真实签名的前提下驱动空/非空授权列表分支。
fn placeholder_auths(n: usize) -> Vec<SignedAuthorization> {
    (0..n)
        .map(|_| {
            SignedAuthorization::new_unchecked(
                Authorization { chain_id: U256::ZERO, address: Address::ZERO, nonce: 0 },
                0,
                U256::ZERO,
                U256::ZERO,
            )
        })
        .collect()
}

/// 按各字段造一笔 `TransactionSigned`（签名恒为 `test_signature`，与 `difftx.rs` 的 builder
/// 一致——两侧预言机都以显式 sender 绕过签名恢复，故签名内容对单笔/序列不变式无影响）。
#[allow(clippy::too_many_arguments)]
fn build_tx(
    ty: u8,
    chain_sel: u8,
    nonce: u64,
    gas_limit: u64,
    max_fee: u128,
    max_prio: u128,
    value: u128,
    to_create: bool,
    to_sel: u8,
    input_len: usize,
    input_byte: u8,
    al_len: usize,
    auth_len: usize,
) -> TransactionSigned {
    let input = Bytes::from(vec![input_byte; input_len]);
    let to_addr = Address::with_last_byte(0x20u8.wrapping_add(to_sel));
    let inner = match ty {
        // Legacy：chain_id 可为 None（pre-EIP-155，合法）/ 正确 / 错误。
        0 => {
            let chain_id = match chain_sel {
                1 => Some(WRONG_CHAIN_ID),
                2 => None,
                _ => Some(MAINNET_CHAIN_ID),
            };
            let to = if to_create { TxKind::Create } else { TxKind::Call(to_addr) };
            Transaction::Legacy(TxLegacy {
                chain_id,
                nonce,
                gas_price: max_fee,
                gas_limit,
                to,
                value: U256::from(value),
                input,
            })
        }
        // EIP-1559：typed，chain_id 恒有值（正确 / 错误）。
        1 => {
            let chain_id = if chain_sel == 1 { WRONG_CHAIN_ID } else { MAINNET_CHAIN_ID };
            let to = if to_create { TxKind::Create } else { TxKind::Call(to_addr) };
            Transaction::Eip1559(TxEip1559 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas: max_fee,
                max_priority_fee_per_gas: max_prio,
                to,
                value: U256::from(value),
                access_list: build_access_list(al_len),
                input,
            })
        }
        // EIP-7702：typed，`to` 恒为 Call（无 Create）；授权列表用占位项。
        _ => {
            let chain_id = if chain_sel == 1 { WRONG_CHAIN_ID } else { MAINNET_CHAIN_ID };
            Transaction::Eip7702(TxEip7702 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas: max_fee,
                max_priority_fee_per_gas: max_prio,
                to: to_addr,
                value: U256::from(value),
                access_list: build_access_list(al_len),
                authorization_list: placeholder_auths(auth_len),
                input,
            })
        }
    };
    TransactionSigned::new_unhashed(inner, Signature::test_signature())
}

// —————————————————————————————— 单笔字段级策略 —————————————————————————————— //

fn nonce_strat() -> impl Strategy<Value = u64> {
    prop_oneof![8 => 0u64..=8, 1 => 9u64..=1000, 1 => Just(u64::MAX)]
}
fn gas_limit_strat() -> impl Strategy<Value = u64> {
    prop_oneof![
        3 => 0u64..=25_000,
        3 => 25_000u64..=100_000,
        2 => 100_000u64..=40_000_000,
        1 => Just(u64::MAX),
    ]
}
fn fee_strat() -> impl Strategy<Value = u128> {
    prop_oneof![
        5 => 0u128..=5_000,          // 覆盖 max_fee < base_fee 边界
        2 => 900u128..=1_100,        // 贴近典型 base_fee
        1 => 1_000_000u128..=1_000_000_000,
        1 => Just(u128::MAX),         // 溢出 / 余额门
    ]
}
fn value_strat() -> impl Strategy<Value = u128> {
    prop_oneof![6 => 0u128..=2_000, 2 => 10_000u128..=1_000_000_000, 1 => Just(u128::MAX)]
}
fn input_len_strat() -> impl Strategy<Value = usize> {
    prop_oneof![
        5 => 0usize..=200,
        3 => (MAX_INITCODE_SIZE - 2)..=(MAX_INITCODE_SIZE + 2), // 跨 EIP-3860 initcode 边界
        1 => 200usize..=2_000,
    ]
}
fn base_fee_strat() -> impl Strategy<Value = u64> {
    prop_oneof![5 => 0u64..=2_000, 2 => Just(1_000u64), 1 => 1_000_000u64..=20_000_000_000]
}
fn block_gas_limit_strat() -> impl Strategy<Value = u64> {
    prop_oneof![1 => Just(0u64), 3 => 21_000u64..=100_000, 4 => 100_000u64..=30_000_000]
}
fn balance_strat() -> impl Strategy<Value = u128> {
    prop_oneof![
        3 => 0u128..=100_000,
        4 => Just(BIG_BALANCE),
        2 => 1_000_000u128..=1_000_000_000_000,
        1 => Just(u128::MAX / 2),
    ]
}

/// 单笔差分用例策略：生成 `TransactionSigned` + 匹配的发送者 `SeedAccount` + 区块参数。
fn single_case_strategy() -> impl Strategy<Value = DiffCase> {
    // 拆成两组 ≤10 元 tuple（proptest 对 tuple 的 Strategy 实现上限），再 prop_map 合成。
    let g1 = (
        0u8..=2u8,         // ty: Legacy / 1559 / 7702
        nonce_strat(),     // tx nonce
        gas_limit_strat(), // tx gas_limit
        fee_strat(),       // max_fee / gas_price
        fee_strat(),       // max_priority_fee
        value_strat(),     // value
        any::<bool>(),     // to = Create?（7702 忽略，恒 Call）
        0u8..8u8,          // to 目标选择
        input_len_strat(), // input 长度（跨 initcode 边界）
        any::<u8>(),       // input 填充字节（零/非零 calldata gas）
    );
    let g2 = (
        0u8..=2u8,               // chain_sel: 正确 / 错误 / (legacy)None
        0usize..=4,              // access-list 项数
        0usize..=3,              // 7702 授权项数（含 0 → EmptyAuthorizationList）
        base_fee_strat(),        // 区块 base fee
        block_gas_limit_strat(), // 区块 gas limit
        any::<bool>(),           // prague / shanghai
        balance_strat(),         // 发送者余额
        nonce_strat(),           // 发送者账户 nonce
        0u8..=2u8,               // 发送者代码变体：空 / 非委托 / 7702 委托
    );
    (g1, g2).prop_map(|(a, b)| {
        let (ty, nonce, gas_limit, max_fee, max_prio, value, to_create, to_sel, input_len, input_byte) =
            a;
        let (chain_sel, al_len, auth_len, base_fee, block_gas_limit, prague, balance, seed_nonce, code_variant) =
            b;
        let tx = build_tx(
            ty, chain_sel, nonce, gas_limit, max_fee, max_prio, value, to_create, to_sel,
            input_len, input_byte, al_len, auth_len,
        );
        DiffCase {
            name: format!(
                "fuzz1/ty{ty}/n{nonce}/gl{gas_limit}/cs{chain_sel}/cv{code_variant}/al{al_len}/au{auth_len}"
            ),
            tx,
            sender: FUZZ_SENDER,
            seed: vec![SeedAccount {
                addr: FUZZ_SENDER,
                info: build_account(balance, seed_nonce, code_variant),
            }],
            base_fee,
            block_gas_limit,
            prague,
        }
    })
}

// —————————————————————————————— 多笔序列策略 —————————————————————————————— //

/// 单条序列内一笔交易的生成参数。
#[derive(Debug, Clone)]
struct SeqTxSpec {
    sender_idx: u8,
    ty: u8,
    nonce: u64,
    gas_limit: u64,
    max_fee: u128,
    value: u128,
    auth_len: usize,
}

fn seq_tx_spec_strat() -> impl Strategy<Value = SeqTxSpec> {
    (
        0u8..3u8,    // 3 个固定 sender 之一
        0u8..=2u8,   // ty
        0u64..=6u64, // nonce：覆盖有效链 / 重用 / 空洞
        prop_oneof![3 => 21_000u64..=120_000, 2 => 0u64..=21_000, 1 => 500_000u64..=5_000_000],
        prop_oneof![4 => 0u128..=5_000, 1 => Just(u128::MAX)],
        prop_oneof![5 => 0u128..=1_000, 1 => 10_000u128..=1_000_000],
        0usize..=2usize, // 7702 占位授权项数
    )
        .prop_map(|(sender_idx, ty, nonce, gas_limit, max_fee, value, auth_len)| SeqTxSpec {
            sender_idx,
            ty,
            nonce,
            gas_limit,
            max_fee,
            value,
            auth_len,
        })
}

/// 多笔序列差分用例策略：2..=5 笔来自固定 sender 集合的混合类型交易，各 sender 独立播种。
fn seq_case_strategy() -> impl Strategy<Value = SeqCase> {
    (
        prop_vec(seq_tx_spec_strat(), 2..=5),
        // 3 个 sender 的起始 nonce（小范围，含链与错配）。
        (0u64..=3u64, 0u64..=3u64, 0u64..=3u64),
        base_fee_strat(),
        block_gas_limit_strat(),
        any::<bool>(),
    )
        .prop_map(|(specs, seed_nonces, base_fee, block_gas_limit, prague)| {
            let mut txs = Vec::with_capacity(specs.len());
            let mut senders = Vec::with_capacity(specs.len());
            for sp in &specs {
                let tx = build_tx(
                    sp.ty,
                    0, // chain_sel=正确：序列聚焦执行期 TOCTOU，非 chain-id 门
                    sp.nonce,
                    sp.gas_limit,
                    sp.max_fee,
                    0, // max_prio
                    sp.value,
                    false, // to = Call
                    (sp.sender_idx + 1) & 7,
                    0, // 无 calldata
                    0,
                    0, // 无 access list
                    sp.auth_len,
                );
                txs.push(tx);
                senders.push(SEQ_SENDERS[sp.sender_idx as usize]);
            }
            let seed = vec![
                SeedAccount {
                    addr: SEQ_SENDERS[0],
                    info: build_account(BIG_BALANCE, seed_nonces.0, 0),
                },
                SeedAccount {
                    addr: SEQ_SENDERS[1],
                    info: build_account(BIG_BALANCE, seed_nonces.1, 0),
                },
                SeedAccount {
                    addr: SEQ_SENDERS[2],
                    info: build_account(BIG_BALANCE, seed_nonces.2, 0),
                },
            ];
            SeqCase {
                name: format!("fuzzSeq/{}tx", specs.len()),
                txs,
                senders,
                seed,
                base_fee,
                block_gas_limit,
                prague,
            }
        })
}

// —————————————————————————————— 7702 授权列表策略（真实签名） —————————————————————————————— //

/// 从固定私钥派生的确定性签名者（key = 全零 + 末字节 idx+1，是合法非零 secp256k1 标量）。
/// 不使用 `random()`，保证 proptest 确定性。
fn auth_signer(idx: u8) -> PrivateKeySigner {
    let mut key = [0u8; 32];
    key[31] = idx + 1;
    PrivateKeySigner::from_slice(&key).expect("valid secp256k1 key")
}

/// 该签名者对应的 authority 地址（`recover_authority()` 将确定性地恢复出它）。
fn authority_addr(idx: u8) -> Address {
    auth_signer(idx).address()
}

/// 用固定私钥真实签名一个授权项，使 `recover_authority()` 得到受控地址。
fn signed_auth(idx: u8, chain_id: U256, target: Address, nonce: u64) -> SignedAuthorization {
    let signer = auth_signer(idx);
    let auth = Authorization { chain_id, address: target, nonce };
    let sig = signer.sign_hash_sync(&auth.signature_hash()).expect("sign auth");
    auth.into_signed(sig)
}

/// 单个授权项的生成参数。
#[derive(Debug, Clone)]
struct AuthSpec {
    signer_idx: u8,
    chain_sel: u8,
    nonce_kind: u8,
    target_sel: u8,
}

fn auth_spec_strat() -> impl Strategy<Value = AuthSpec> {
    (0u8..3u8, 0u8..3u8, 0u8..4u8, 0u8..4u8).prop_map(
        |(signer_idx, chain_sel, nonce_kind, target_sel)| AuthSpec {
            signer_idx,
            chain_sel,
            nonce_kind,
            target_sel,
        },
    )
}

/// 后续（follow-up）普通交易的生成参数：从某 authority 发一笔 legacy，探测 filter 的
/// 授权-nonce 模型与 revm 执行是否一致。
#[derive(Debug, Clone)]
struct FollowSpec {
    signer_idx: u8,
    nonce: u64,
}

fn follow_spec_strat() -> impl Strategy<Value = FollowSpec> {
    (0u8..3u8, 0u64..=4u64).prop_map(|(signer_idx, nonce)| FollowSpec { signer_idx, nonce })
}

/// 7702 授权列表序列策略：
/// - 1..=2 笔来自同一 sender（可能是某 authority 本身 → 自赞助）的 7702 交易，每笔携 1..=2
///   条**真实签名**的授权（chain_id = 0/正确/错配，authorization nonce = 账户 nonce/+1/空洞/MAX，
///   可重复 authority，多授权链）；
/// - 0..=2 笔来自某 authority 的后续 legacy 交易。
/// 所有 authority 账户（3 个签名者地址）与基础 sender E 都被播种（起始 nonce=0，余额充裕）。
fn seq_7702_auth_strategy() -> impl Strategy<Value = SeqCase> {
    (
        0u8..4u8, // sender_sel：0=独立 EOA E；1..3=某 authority（自赞助）
        prop_vec(prop_vec(auth_spec_strat(), 1..=2), 1..=2),
        prop_vec(follow_spec_strat(), 0..=2),
    )
        .prop_map(|(sender_sel, tx_auth_specs, follows)| {
            let base_eoa = FUZZ_SENDER; // 0x11
            let sender_addr =
                if sender_sel == 0 { base_eoa } else { authority_addr(sender_sel - 1) };

            let mut txs = Vec::new();
            let mut senders = Vec::new();

            // 7702 交易：同一 sender，nonce = 交易序号（构成有效链，最大化被放行执行的概率）。
            for (i, auths) in tx_auth_specs.iter().enumerate() {
                let authorization_list: Vec<SignedAuthorization> = auths
                    .iter()
                    .map(|a| {
                        let chain_id = match a.chain_sel {
                            0 => U256::ZERO,                   // 0 = 通配（对任意链有效）
                            1 => U256::from(MAINNET_CHAIN_ID), // 正确
                            _ => U256::from(WRONG_CHAIN_ID),   // 错配 → 两侧都应跳过
                        };
                        // authority 账户起始 nonce=0，故：0→0(匹配)，1→1(错位)，2→5(空洞)，3→MAX。
                        let auth_nonce = match a.nonce_kind {
                            0 => 0,
                            1 => 1,
                            2 => 5,
                            _ => u64::MAX,
                        };
                        let target = Address::with_last_byte(0x60u8.wrapping_add(a.target_sel));
                        signed_auth(a.signer_idx, chain_id, target, auth_nonce)
                    })
                    .collect();
                let inner = TxEip7702 {
                    chain_id: MAINNET_CHAIN_ID,
                    nonce: i as u64,
                    gas_limit: 500_000, // 足够覆盖 21k + 25k*auths 内联 + 调用
                    max_fee_per_gas: 10,
                    max_priority_fee_per_gas: 0,
                    to: Address::with_last_byte(0x20),
                    value: U256::ZERO,
                    access_list: AccessList::default(),
                    authorization_list,
                    input: Bytes::new(),
                };
                txs.push(TransactionSigned::new_unhashed(
                    Transaction::Eip7702(inner),
                    Signature::test_signature(),
                ));
                senders.push(sender_addr);
            }

            // 后续 legacy 交易：从某 authority 发出，nonce 受 fuzz 控制。
            for f in &follows {
                let tx = build_tx(
                    0, // legacy
                    0, // chain_sel 正确
                    f.nonce, 21_000, 10, // max_fee=gas_price ≥ base_fee(1)
                    0, 0, false, 0, 0, 0, 0, 0,
                );
                txs.push(tx);
                senders.push(authority_addr(f.signer_idx));
            }

            // 播种：基础 EOA + 3 个 authority（起始 nonce=0，空码，余额充裕）。
            let mut seed =
                vec![SeedAccount { addr: base_eoa, info: build_account(BIG_BALANCE, 0, 0) }];
            for idx in 0u8..3 {
                seed.push(SeedAccount {
                    addr: authority_addr(idx),
                    info: build_account(BIG_BALANCE, 0, 0),
                });
            }

            SeqCase {
                name: format!("fuzz7702/sender{sender_sel}/{}tx", txs.len()),
                txs,
                senders,
                seed,
                base_fee: 1,
                block_gas_limit: 30_000_000,
                prague: true,
            }
        })
}

// —————————————————————————————— proptest —————————————————————————————— //

// 每次 `run_case` / `run_seq_case` 都要重建一份完整主网
// chainspec（`ChainSpecBuilder::from(&*MAINNET)` ，约 0.3–0.7s/case），故 case
// 数按每个策略单独拨盘，保证整套在一分钟内跑完，同时仍充分搜索。 拆成独立 `proptest!`
// 块以便逐测试配置 case 数。

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// (1) 单笔字段级 fuzz：filter 永不放行 revm 会拒绝的交易 —— 对整个生成空间成立。
    /// 一旦 `is_gap`，即找到一个 NEW filter⊇revm 停链缺口；proptest 会自动最小化并打印。
    #[test]
    fn fuzz_single_tx_no_filter_gap(case in single_case_strategy()) {
        let out = run_case(&case);
        prop_assert!(
            !out.is_gap,
            "NEW filter-superset-revm CHAIN-HALT GAP: filter ADMITTED a tx revm REJECTS.\n\
             name={}\n revm_variant={:?}\n filter_admit={} revm_reject={}\n minimized DiffCase={:#?}",
            case.name, out.revm_variant, out.filter_admit, out.revm_reject, case
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// (2) 多笔序列 fuzz：未预置委托指示符的普通序列不应产生任何执行期缺口。
    /// 非空 `exec_gaps` 即为超出已知 7702-CREATE 类的 NEW finding —— 让断言携带细节并打印。
    #[test]
    fn fuzz_seq_no_exec_gap(case in seq_case_strategy()) {
        let out = run_seq_case(&case);
        prop_assert!(
            out.exec_gaps.is_empty(),
            "NEW seq exec gap (beyond known 7702-CREATE class): filter admitted {:?}, \
             exec gaps {:#?}\n minimized SeqCase={:#?}",
            out.filter_admitted, out.exec_gaps, case
        );
    }

    /// (3) 7702 授权列表 fuzz（真实签名）：探测 filter 的授权-nonce 模型与 revm 执行的一致性。
    /// 本策略不构造 CREATE 运行时的委托代码，故已知的委托-CREATE nonce 竞态不会出现；任何
    /// `exec_gap` 都是 filter 授权模型与 revm 分歧的 NEW finding —— 携细节打印。
    #[test]
    fn fuzz_7702_auth_list_no_exec_gap(case in seq_7702_auth_strategy()) {
        let out = run_seq_case(&case);
        prop_assert!(
            out.exec_gaps.is_empty(),
            "NEW 7702 auth-list exec gap: filter admitted {:?}, exec gaps {:#?}\n\
             minimized SeqCase={:#?}",
            out.filter_admitted, out.exec_gaps, case
        );
    }
}

// —————————————————————————————— 非平凡性（反-vacuity）门 —————————————————————————————— //

/// 证明 fuzz 空间非退化：至少存在被 filter 放行且 revm 接受的用例（`filter_admit` 可达 true），
/// 且至少存在让 revm 拒绝的用例（`revm_reject` 可达 true）。若 builder 造出的交易全被某一侧硬拒，
/// 上面的 `!is_gap` 断言会平凡通过而失去意义——本测试把这两半可达性钉死。
#[test]
fn fuzz_generators_are_non_vacuous() {
    // (a) 一笔完全合法的 1559 转账：应被两侧放行/接受。
    let happy = DiffCase {
        name: "nonvacuous_happy".into(),
        tx: build_tx(1, 0, 0, 21_000, 2_000, 100, 0, false, 0, 0, 0, 0, 0),
        sender: FUZZ_SENDER,
        seed: vec![SeedAccount { addr: FUZZ_SENDER, info: build_account(BIG_BALANCE, 0, 0) }],
        base_fee: 1_000,
        block_gas_limit: 30_000_000,
        prague: true,
    };
    let ho = run_case(&happy);
    assert!(ho.filter_admit, "builder 造的合法交易应被 filter 放行: {ho:?}");
    assert!(!ho.revm_reject, "builder 造的合法交易应被 revm 接受: {ho:?}");
    assert!(!ho.is_gap);

    // (b) 一笔 gas_price < base_fee 的交易：应让 revm 拒绝（revm_reject 可达 true）。
    let rejy = DiffCase {
        name: "nonvacuous_reject".into(),
        tx: build_tx(0, 0, 0, 21_000, 100, 0, 0, false, 0, 0, 0, 0, 0),
        sender: FUZZ_SENDER,
        seed: vec![SeedAccount { addr: FUZZ_SENDER, info: build_account(BIG_BALANCE, 0, 0) }],
        base_fee: 1_000,
        block_gas_limit: 30_000_000,
        prague: true,
    };
    let ro = run_case(&rejy);
    assert!(ro.revm_reject, "gas_price<base_fee 应让 revm 拒绝: {ro:?}");
    assert!(!ro.is_gap, "filter 也应拒绝，故非 gap: {ro:?}");

    // (c) 7702 真实签名 authority 可被恢复为受控地址（证明 stretch 策略接线正确）。
    let auth = signed_auth(0, U256::from(MAINNET_CHAIN_ID), Address::with_last_byte(0x60), 0);
    assert_eq!(
        auth.recover_authority().ok(),
        Some(authority_addr(0)),
        "真实签名授权应恢复出确定性 authority 地址"
    );
}
