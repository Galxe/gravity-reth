//! 差分测试预言机（differential oracle）—— 机械化验证不变式
//! **`tx_filter` ⊇ revm 交易级校验（tx-level validation）**。
//!
//! ## 背景
//!
//! 在 Gravity「先共识排序、后执行」的模型里，执行器无法从单笔交易的
//! `revm::InvalidTransaction` 错误中恢复（见 `lib.rs:1323` 的 `executor.execute(...).unwrap()`
//! panic 点）。因此 `tx_filter::filter_invalid_txs` 必须是 revm 交易级校验的**超集**：
//! 任何被 filter「放行（ADMIT）」但被 revm 以 `InvalidTransaction` 拒绝的交易，
//! 都是一个能打停真链的 `DoS` 缺口（chain-halt gap）。
//!
//! 本模块把两侧预言机（filter 侧 / revm 侧）喂**同一笔交易 + 同一份账户状态**，
//! 逐 revm 变体（variant）机械比对：
//! - `filter_admit`  —— filter 是否放行（下标不在返回的 invalid 集合里）。
//! - `revm_reject`   —— revm 是否以 `EVMError::Transaction(InvalidTransaction)` 拒绝。
//! - `is_gap = filter_admit && revm_reject` —— 即「放行了 revm 会拒绝的交易」= 停链缺口。
//!
//! 这是 D1 Tier-1（字段级、单笔交易）预言机；不做任何 live 集群或多笔执行序列验证。
//!
//! ## 使用
//!
//! ```text
//! cargo test -p reth-pipe-exec-layer-ext-v2 --features difftx difftx -- --nocapture
//! cargo run  -p reth-pipe-exec-layer-ext-v2 --features difftx --bin difftx
//! ```

use alloy_consensus::{
    constants::KECCAK_EMPTY, transaction::Transaction as _, Header, TxEip1559, TxEip7702, TxLegacy,
};
use alloy_eips::{
    eip7702::{Authorization, SignedAuthorization},
    Typed2718,
};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use reth_evm::{ConfigureEvm, Evm, EvmError, IntoTxEnv};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives::Recovered;
use revm::{
    database::{CacheDB, EmptyDB, State},
    state::{AccountInfo, Bytecode},
    DatabaseCommit,
};
use revm_primitives::eip3860::MAX_INITCODE_SIZE;
use std::sync::Arc;

/// 主网 chain id —— 所有测试 chainspec 都由 `MAINNET` 派生，filter 的 chain-id 门会
/// 拒绝任何 `chain_id` 不等于此值的 typed 交易。
const MAINNET_CHAIN_ID: u64 = 1;

/// 两侧预言机共用的区块时间戳 / 高度。**必须取 0**：`ChainSpecBuilder::from(&*MAINNET)`
/// 会带入主网真实的 Cancun/Prague 时间戳，而 `shanghai_activated()` 只在 ts=0 强制打开
/// Shanghai、并不移除后续 fork——若用一个很大的时间戳，Shanghai spec 会被主网真实的
/// Cancun/Prague 激活覆盖成 PRAGUE。ts=0 时：`prague_chain_spec`（Prague@0）解析出
/// `SpecId::PRAGUE`，`shanghai_chain_spec`（Cancun 未到 ts）解析出 `SpecId::SHANGHAI`。
/// 两侧必须传入相同的 (ts, number) 才能对齐 `SpecId`。
const BLOCK_TIMESTAMP: u64 = 0;
const BLOCK_NUMBER: u64 = 1;

/// 一个充裕余额（1 ETH），用于「余额不是被测因素」的用例。
const BIG_BALANCE: u128 = 1_000_000_000_000_000_000;

/// 喂给两侧预言机的种子账户（filter 侧塞进 `CacheDB`，revm 侧塞进同一个 `CacheDB`）。
#[derive(Debug, Clone)]
pub struct SeedAccount {
    /// 账户地址。
    pub addr: Address,
    /// 账户信息（余额 / nonce / 代码）。
    pub info: AccountInfo,
}

/// 一个差分用例：一笔交易 + 一份账户状态 + 区块参数。
#[derive(Debug, Clone)]
pub struct DiffCase {
    /// 用例名（通常等于目标 revm 变体名）。
    pub name: String,
    /// 被测交易。
    pub tx: TransactionSigned,
    /// 交易发送者（两侧共用，revm 侧用 `Recovered::new_unchecked` 绕过签名恢复）。
    pub sender: Address,
    /// 种子账户列表；filter 侧与 revm 侧从同一份列表 seed，保证状态一致。
    pub seed: Vec<SeedAccount>,
    /// 区块 base fee（同时喂给 filter 参数与 revm 的 `Header::base_fee_per_gas`）。
    pub base_fee: u64,
    /// 区块 gas limit（同时喂给 filter 参数与 revm 的 `Header::gas_limit`）。
    pub block_gas_limit: u64,
    /// `true` => 使用 `prague_chain_spec`；`false` => `shanghai_chain_spec`。
    pub prague: bool,
}

/// 一个差分结果。
#[derive(Debug, Clone)]
pub struct DiffOutcome {
    /// 用例名。
    pub name: String,
    /// filter 是否放行该交易。
    pub filter_admit: bool,
    /// revm 是否以 `InvalidTransaction` 拒绝该交易。
    pub revm_reject: bool,
    /// 被拒绝时内层 `InvalidTransaction` 的 `Debug` 串。
    pub revm_variant: Option<String>,
    /// 停链缺口：`filter_admit && revm_reject`。
    pub is_gap: bool,
}

/// Prague 从创世激活的 chainspec —— 需要 7702 / initcode / prague 内联 gas 规则的用例用它。
fn prague_chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
}

/// Shanghai 激活、Prague 未激活的 chainspec —— 用于 pre-Prague 的 7702 边界用例
/// （revm 会给 `Eip7702NotSupported`）。
fn shanghai_chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).shanghai_activated().build())
}

/// 运行单个差分用例：分别喂 filter 侧与 revm 侧，计算 `is_gap`。
pub fn run_case(case: &DiffCase) -> DiffOutcome {
    let chain_spec = if case.prague { prague_chain_spec() } else { shanghai_chain_spec() };

    // 以同一份 seed 构造 CacheDB（同时满足 filter 的 `DatabaseRef` 与 revm 的 `Database`）。
    let mut cache: CacheDB<EmptyDB> = CacheDB::new(EmptyDB::default());
    for s in &case.seed {
        cache.insert_account_info(s.addr, s.info.clone());
    }

    // —— filter 侧 —— //
    // `filter_invalid_txs` 返回 INVALID 下标集合；下标 0 不在其中即为「放行」。
    let invalid = crate::tx_filter::filter_invalid_txs(
        cache.clone(),
        std::slice::from_ref(&case.tx),
        std::slice::from_ref(&case.sender),
        case.base_fee,
        case.block_gas_limit,
        chain_spec.as_ref(),
        BLOCK_TIMESTAMP,
        BLOCK_NUMBER,
    );
    let filter_admit = !invalid.contains(&0);

    // —— revm 侧 —— //
    // Header 必须让 revm 解析出与 filter 相同的 SpecId，并满足 validate_env 的前置：
    // MERGE+ 需 prevrandao（由 mix_hash 默认 ZERO 提供）、CANCUN+ 需 excess_blob_gas。
    let evm_config = EthEvmConfig::new(chain_spec);
    let header = Header {
        gas_limit: case.block_gas_limit,
        base_fee_per_gas: Some(case.base_fee),
        excess_blob_gas: Some(0),
        timestamp: BLOCK_TIMESTAMP,
        number: BLOCK_NUMBER,
        ..Default::default()
    };
    let evm_env = evm_config.evm_env(&header).expect("evm_env");
    let state = State::builder().with_bundle_update().with_database(cache).build();
    let mut evm = evm_config.evm_with_env(state, evm_env);

    // 与 lib.rs:1067 完全一致的 TxEnv 构造路径。
    let tx_env = Recovered::new_unchecked(case.tx.clone(), case.sender).into_tx_env();
    let result = evm.transact_raw(tx_env);

    // 只把 `EVMError::Transaction(InvalidTransaction)` 记为拒绝信号；Database / Header /
    // Custom 错误不算交易级拒绝（不构成本预言机意义上的 gap）。
    let (revm_reject, revm_variant) = match &result {
        Err(e) => match e.as_invalid_tx_err() {
            Some(itx) => (true, Some(format!("{itx:?}"))),
            None => (false, None),
        },
        Ok(_) => (false, None),
    };

    let is_gap = filter_admit && revm_reject;
    DiffOutcome { name: case.name.clone(), filter_admit, revm_reject, revm_variant, is_gap }
}

// —————————————————————————————— 交易 / 账户 构造辅助 —————————————————————————————— //

/// 普通外部账户（EOA）种子：空代码。
fn eoa(addr: Address, balance: u128, nonce: u64) -> SeedAccount {
    SeedAccount {
        addr,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: KECCAK_EMPTY,
            code: None,
        },
    }
}

/// 带非委托代码的账户种子（用于 EIP-3607 / `RejectCallerWithCode`）。
fn coded_account(addr: Address, balance: u128, nonce: u64) -> SeedAccount {
    // 一段普通 EVM 字节码（非 0xef0100 委托指示符），触发 EIP-3607。
    let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]));
    SeedAccount {
        addr,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: code.hash_slow(),
            code: Some(code),
        },
    }
}

/// 构造 legacy 交易（`chain_id = None`，合法）。
fn legacy_tx(
    nonce: u64,
    gas_limit: u64,
    gas_price: u128,
    value: u128,
    kind: TxKind,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price,
            gas_limit,
            to: kind,
            value: U256::from(value),
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// 构造 EIP-1559 交易。
fn eip1559_tx(
    chain_id: u64,
    nonce: u64,
    gas_limit: u64,
    max_fee: u128,
    max_prio: u128,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Eip1559(TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: max_prio,
            to: TxKind::Call(Address::ZERO),
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// 构造 EIP-7702（type-0x04）交易，含 `auths` 个授权项。
fn eip7702_tx(nonce: u64, gas_limit: u64, max_fee: u128, auths: usize) -> TransactionSigned {
    let authorization_list = (0..auths)
        .map(|_| {
            SignedAuthorization::new_unchecked(
                Authorization { chain_id: U256::ZERO, address: Address::ZERO, nonce: 0 },
                0,
                U256::ZERO,
                U256::ZERO,
            )
        })
        .collect();
    TransactionSigned::new_unhashed(
        Transaction::Eip7702(TxEip7702 {
            chain_id: MAINNET_CHAIN_ID,
            nonce,
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: 0,
            authorization_list,
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// 固定发送者地址（确定性，便于复现）。
const fn sender() -> Address {
    Address::with_last_byte(0x11)
}

// —————————————————————————————— 变体矩阵 —————————————————————————————— //

/// 构造所有「字段级可造（field-level craftable）」的 revm 变体用例。
///
/// 每个 case 都是单笔孤立交易，能让 revm 返回目标 `InvalidTransaction` 变体；同时
/// filter 也应拒绝它（本 pin 下应为零 gap）。
// 逐个 push 是刻意的：每个用例带独立注释说明其触发的 revm 变体，比一个大 `vec![]` 更可读。
#[allow(clippy::vec_init_then_push)]
fn craftable_cases() -> Vec<DiffCase> {
    let s = sender();
    let mut v = Vec::new();

    // 1) GasPriceLessThanBasefee：legacy gas_price < base_fee。filter 也拒（max_fee < base_fee）。
    v.push(DiffCase {
        name: "GasPriceLessThanBasefee".into(),
        tx: legacy_tx(0, 21_000, 100, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 2) PriorityFeeGreaterThanMaxFee：1559 max_priority > max_fee。
    v.push(DiffCase {
        name: "PriorityFeeGreaterThanMaxFee".into(),
        tx: eip1559_tx(MAINNET_CHAIN_ID, 0, 21_000, 1000, 2000),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 3) NonceTooHigh：账户 nonce=0，交易 nonce=5。
    v.push(DiffCase {
        name: "NonceTooHigh".into(),
        tx: legacy_tx(5, 21_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 4) NonceTooLow：账户 nonce=5，交易 nonce=0。
    v.push(DiffCase {
        name: "NonceTooLow".into(),
        tx: legacy_tx(0, 21_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 5)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 5) LackOfFundForMaxFee：余额远小于 max_fee*gas_limit。
    v.push(DiffCase {
        name: "LackOfFundForMaxFee".into(),
        tx: legacy_tx(0, 21_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, 1000, 0)], // 1000 wei << 21_000_000 wei 上限
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 6) CreateInitCodeSizeLimit：create 交易 input 长度 > MAX_INITCODE_SIZE。
    let big_init = Bytes::from(vec![0u8; MAX_INITCODE_SIZE + 1]);
    v.push(DiffCase {
        name: "CreateInitCodeSizeLimit".into(),
        tx: TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                nonce: 0,
                gas_price: 1000,
                gas_limit: 100_000,
                to: TxKind::Create,
                input: big_init,
                ..Default::default()
            }),
            Signature::test_signature(),
        ),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 7) InvalidChainId：typed(1559) 交易带错误 chain_id。
    v.push(DiffCase {
        name: "InvalidChainId".into(),
        tx: eip1559_tx(999, 0, 21_000, 1000, 0),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 8) CallGasCostMoreThanGasLimit：内联 gas 不足（gas_limit < 21000）。
    v.push(DiffCase {
        name: "CallGasCostMoreThanGasLimit".into(),
        tx: legacy_tx(0, 20_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 9) CallerGasLimitMoreThanBlock：tx.gas_limit > 区块 gas_limit。
    v.push(DiffCase {
        name: "CallerGasLimitMoreThanBlock".into(),
        tx: legacy_tx(0, 22_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 21_000, // 区块上限低于交易 gas_limit
        prague: true,
    });

    // 10) EmptyAuthorizationList：Prague 下 7702 交易空授权列表。
    v.push(DiffCase {
        name: "EmptyAuthorizationList".into(),
        tx: eip7702_tx(0, 100_000, 1, 0),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    // 11) Eip7702NotSupported：pre-Prague（Shanghai）下的 7702 交易。
    v.push(DiffCase {
        name: "Eip7702NotSupported".into(),
        tx: eip7702_tx(0, 100_000, 1, 1),
        sender: s,
        seed: vec![eoa(s, BIG_BALANCE, 0)],
        base_fee: 1,
        block_gas_limit: 30_000_000,
        prague: false, // Shanghai
    });

    // 12) RejectCallerWithCode：发送者带非委托代码（EIP-3607）。
    v.push(DiffCase {
        name: "RejectCallerWithCode".into(),
        tx: legacy_tx(0, 21_000, 1000, 0, TxKind::Call(Address::ZERO)),
        sender: s,
        seed: vec![coded_account(s, BIG_BALANCE, 0)],
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    });

    v
}

/// 生成一条「非 live」的说明行（documented unreachable / 不可造 / filter 整型拦截）。
fn note_outcome(variant: &str, reason: &str) -> DiffOutcome {
    DiffOutcome {
        name: variant.to_string(),
        filter_admit: false,
        revm_reject: false,
        revm_variant: Some(reason.to_string()),
        is_gap: false,
    }
}

/// 说明行：本 pin 下 revm 无构造点、或无法由真实签名交易表达、或被 filter 按交易类型整体拦截。
fn note_cases() -> Vec<DiffOutcome> {
    vec![
        // 无法由真实签名交易表达：alloy 的 typed 交易恒带 chain_id，`tx.chain_id()` 永不为 None，
        // 故 revm 的 MissingChainId 从真实 TransactionSigned 不可达（filter
        // 的对应门为防御性死代码）。
        note_outcome("MissingChainId", "not-craftable: typed signed txs always carry chain_id"),
        // 文档已 pin 为 revm 本版本无构造点（unreachable，见 tx_filter.rs 模块头）。
        note_outcome(
            "AccessListNotSupported",
            "revm_unreachable: legacy env validation site removed",
        ),
        note_outcome(
            "MaxFeePerBlobGasNotSupported",
            "revm_unreachable: removed; 4844 类型被整体拒绝",
        ),
        note_outcome(
            "BlobVersionedHashesNotSupported",
            "revm_unreachable: removed; 4844 类型被整体拒绝",
        ),
        note_outcome(
            "AuthorizationListNotSupported",
            "revm_unreachable: 走 Eip7702NotSupported 路径",
        ),
        note_outcome(
            "AuthorizationListInvalidFields",
            "revm_unreachable: 移至执行期 7702 授权处理",
        ),
        note_outcome(
            "NonceOverflowInTransaction",
            "revm_unreachable: revm-handler 18.1.0 才重新引入",
        ),
        note_outcome(
            "TxGasLimitGreaterThanCap",
            "revm_unreachable: cap=u64::MAX（OSAKA 前不可触发）",
        ),
        note_outcome("Eip7873NotSupported", "revm_unreachable: OSAKA 激活"),
        note_outcome("Eip7873MissingTarget", "revm_unreachable: OSAKA 激活"),
        // 被 filter 按交易类型整体拦截（filter 拒绝所有 EIP-4844 交易，故这些变体 filter_admit
        // 恒为假）。
        note_outcome("EmptyBlobs", "filter-wholesale: is_eip4844 门整体拒绝 type-3"),
        note_outcome("TooManyBlobs", "filter-wholesale: is_eip4844 门整体拒绝 type-3"),
        note_outcome("BlobVersionNotSupported", "filter-wholesale: is_eip4844 门整体拒绝 type-3"),
        note_outcome(
            "BlobGasPriceGreaterThanMax",
            "filter-wholesale: is_eip4844 门整体拒绝 type-3",
        ),
        note_outcome("BlobCreateTransaction", "filter-wholesale: is_eip4844 门整体拒绝 type-3"),
        note_outcome("Eip4844NotSupported", "filter-wholesale: is_eip4844 门整体拒绝 type-3"),
        // 内联 gas 下限门：filter 同时校验 initial_gas 与 floor_gas，故与 CallGasCost 同源覆盖。
        note_outcome("GasFloorMoreThanGasLimit", "filter-covered: intrinsic floor_gas 门覆盖"),
        // 只在 pre-Berlin / pre-London 才可触发；Gravity 恒在 Prague，全部启用，不可达。
        note_outcome("Eip2930NotSupported", "spec-gated: Gravity 恒 Prague，EIP-2930 已启用"),
        note_outcome("Eip1559NotSupported", "spec-gated: Gravity 恒 Prague，EIP-1559 已启用"),
        // 需要 balance >= 2^128 才能与 filter 的 saturating 超集产生分歧，物理不可造。
        note_outcome(
            "OverflowPaymentInTransaction",
            "not-craftable: 需 balance>=2^128；filter saturating 超集覆盖",
        ),
    ]
}

/// 对外暴露可造用例列表（供 bin 遍历 gap 并导出复现文件）。
pub fn craftable_cases_pub() -> Vec<DiffCase> {
    craftable_cases()
}

/// 生成完整变体矩阵：所有可造用例（live 运行两侧预言机）+ 说明行。
pub fn variant_matrix() -> Vec<DiffOutcome> {
    let mut out: Vec<DiffOutcome> = craftable_cases().iter().map(run_case).collect();
    out.extend(note_cases());
    out
}

// —————————————————————————————— 复现导出（gnode send 兼容） —————————————————————————————— //

/// 为一个 gap（或任意用例）导出一个 gnode `send` 兼容的 tx.json 对象。
///
/// 字段从 `case.tx` 推导：`to` / `data` / `value` / `nonce` / `gas` / `type` /
/// `maxFeePerGas` 或 `gasPrice`，type-4 追加 `authorizationList`；`_note` 描述 revm
/// 变体与复现方式。
pub fn emit_repro(case: &DiffCase, outcome: &DiffOutcome) -> serde_json::Value {
    let tx = &case.tx;
    let to = match tx.kind() {
        TxKind::Call(addr) => serde_json::Value::String(format!("0x{}", hex::encode(addr))),
        TxKind::Create => serde_json::Value::Null,
    };
    let data = format!("0x{}", hex::encode(tx.input()));
    let value = format!("0x{:x}", tx.value());
    let nonce = format!("0x{:x}", tx.nonce());
    let gas = format!("0x{:x}", tx.gas_limit());
    let ty = tx.ty();

    let mut obj = serde_json::json!({
        "from": format!("0x{}", hex::encode(case.sender)),
        "to": to,
        "data": data,
        "value": value,
        "nonce": nonce,
        "gas": gas,
        "type": format!("0x{:x}", ty),
    });
    let map = obj.as_object_mut().unwrap();

    // 费用字段：typed（1559/7702）用 maxFeePerGas + maxPriorityFeePerGas；legacy 用 gasPrice。
    if ty == 0 {
        map.insert(
            "gasPrice".into(),
            serde_json::Value::String(format!("0x{:x}", tx.max_fee_per_gas())),
        );
    } else {
        map.insert(
            "maxFeePerGas".into(),
            serde_json::Value::String(format!("0x{:x}", tx.max_fee_per_gas())),
        );
        map.insert(
            "maxPriorityFeePerGas".into(),
            serde_json::Value::String(format!(
                "0x{:x}",
                tx.max_priority_fee_per_gas().unwrap_or_default()
            )),
        );
    }

    // type-4 授权列表。
    if let Some(auth_list) = tx.authorization_list() {
        let auths: Vec<serde_json::Value> = auth_list
            .iter()
            .map(|a| {
                serde_json::json!({
                    "chainId": format!("0x{:x}", a.chain_id()),
                    "address": format!("0x{}", hex::encode(a.address())),
                    "nonce": format!("0x{:x}", a.nonce()),
                })
            })
            .collect();
        map.insert("authorizationList".into(), serde_json::Value::Array(auths));
    }

    map.insert(
        "_note".into(),
        serde_json::Value::String(format!(
            "CHAIN-HALT GAP: filter ADMITS but revm REJECTS with {}. \
             filter_admit={}, revm_reject={}. 复现：将本对象存为 tx.json，用 `gnode send tx.json` \
             提交到本地集群，观察执行器在 lib.rs:1323 panic 停链。",
            outcome.revm_variant.as_deref().unwrap_or("<unknown>"),
            outcome.filter_admit,
            outcome.revm_reject,
        )),
    );

    obj
}

// ============================================================================
// Tier-2：多笔交易序列执行差分（execution-induced TOCTOU）
// ============================================================================
//
// Tier-1（字段级、单笔）证明 filter 是 revm 交易级校验的字段超集，找到 0 gap。
// 但已知的真链停机是一种**执行诱导的 TOCTOU**：filter 只按「区块起始状态 + 每笔 sender
// nonce +1 + 7702 授权 nonce +1」建模，无法看到执行期由 CREATE / 委托调用等造成的 nonce
// 跃迁。典型场景（tools/gnode/gnodelib/scenarios/halt_7702.py）：区块 [tx0, tx1] 中
// filter 对区块起始状态同时放行两笔，但执行 tx0 会把某账户 nonce 越过 filter 的「+1」模型，
// 使 tx1 在执行期命中 NonceTooLow → 执行器在 lib.rs:1323 panic 停链。
//
// Tier-2 在同一进程内确定性复现：整块跑 filter 得到放行集合，再用一个**逐笔提交**的 revm
// evm 顺序执行放行的交易；任一放行交易在执行期返回 `EVMError::Transaction(InvalidTransaction)`
// 即为一个执行缺口（SeqGap）—— 这正是停链。仅 `InvalidTransaction` 计入；Revert/Halt（Ok）
// 不算（链能存活）。

/// Tier-2 序列用例：一个区块内的多笔交易 + 区块起始账户状态。
#[derive(Debug, Clone)]
pub struct SeqCase {
    /// 用例名。
    pub name: String,
    /// 区块内交易（按区块顺序）。
    pub txs: Vec<TransactionSigned>,
    /// 各交易发送者（与 `txs` 一一对应）。
    pub senders: Vec<Address>,
    /// 区块起始账户状态（两侧共用）。
    pub seed: Vec<SeedAccount>,
    /// 区块 base fee。
    pub base_fee: u64,
    /// 区块 gas limit。
    pub block_gas_limit: u64,
    /// `true` => Prague spec。
    pub prague: bool,
}

/// 一个执行缺口：被 filter 放行、却在执行期被 revm 以 `InvalidTransaction` 拒绝的交易。
#[derive(Debug, Clone)]
pub struct SeqGap {
    /// 交易在区块内的下标。
    pub tx_index: usize,
    /// 该交易发送者。
    pub sender: Address,
    /// 执行期 `InvalidTransaction` 的 Debug 串。
    pub revm_variant: String,
}

/// Tier-2 序列差分结果。
#[derive(Debug, Clone)]
pub struct SeqOutcome {
    /// 用例名。
    pub name: String,
    /// filter 放行的交易下标集合（对整块 `txs` 跑 `filter_invalid_txs` 的补集）。
    pub filter_admitted: Vec<usize>,
    /// 执行期发现的缺口（放行但执行期 `InvalidTransaction`）。
    pub exec_gaps: Vec<SeqGap>,
}

/// 运行一个 Tier-2 序列用例。
///
/// 1. 整块跑 `filter_invalid_txs` 得到 `filter_admitted`（不在 invalid 集合的下标）。
/// 2. 建单个 revm evm（与 Tier-1 相同的 EthEvmConfig/Header/State 模式），对放行的交易
///    **按顺序逐笔提交执行**（`transact_commit`，状态随之演进）。
/// 3. 任一放行交易返回 `EVMError::Transaction(itx)` 即记为 `SeqGap`。
pub fn run_seq_case(case: &SeqCase) -> SeqOutcome {
    let chain_spec = if case.prague { prague_chain_spec() } else { shanghai_chain_spec() };

    let mut cache: CacheDB<EmptyDB> = CacheDB::new(EmptyDB::default());
    for s in &case.seed {
        cache.insert_account_info(s.addr, s.info.clone());
    }

    // —— filter 侧：整块一次性判定 —— //
    let invalid = crate::tx_filter::filter_invalid_txs(
        cache.clone(),
        &case.txs,
        &case.senders,
        case.base_fee,
        case.block_gas_limit,
        chain_spec.as_ref(),
        BLOCK_TIMESTAMP,
        BLOCK_NUMBER,
    );
    let filter_admitted: Vec<usize> =
        (0..case.txs.len()).filter(|i| !invalid.contains(i)).collect();

    // —— revm 侧：逐笔提交，状态演进 —— //
    let evm_config = EthEvmConfig::new(chain_spec);
    let header = Header {
        gas_limit: case.block_gas_limit,
        base_fee_per_gas: Some(case.base_fee),
        excess_blob_gas: Some(0),
        timestamp: BLOCK_TIMESTAMP,
        number: BLOCK_NUMBER,
        ..Default::default()
    };
    let evm_env = evm_config.evm_env(&header).expect("evm_env");
    let state = State::builder().with_bundle_update().with_database(cache).build();
    let mut evm = evm_config.evm_with_env(state, evm_env);

    let mut exec_gaps = Vec::new();
    for &i in &filter_admitted {
        let tx_env = Recovered::new_unchecked(case.txs[i].clone(), case.senders[i]).into_tx_env();
        // 用 transact_raw（取具体 `Self::Tx`，与 Tier-1 同路径、无 IntoTxEnv 歧义），成功后把
        // ResultAndState.state 提交回 evm 的 State，使区块内状态随每笔演进。
        // Success / Revert / Halt（Ok）都提交（链存活）；只有 `EVMError::Transaction` 是执行器
        // 无法恢复的停链信号，记为 gap。
        match evm.transact_raw(tx_env) {
            Ok(result_and_state) => {
                evm.db_mut().commit(result_and_state.state);
            }
            Err(e) => {
                if let Some(itx) = e.as_invalid_tx_err() {
                    exec_gaps.push(SeqGap {
                        tx_index: i,
                        sender: case.senders[i],
                        revm_variant: format!("{itx:?}"),
                    });
                }
                // 非交易级错误（Database/Header）不计入 gap。
            }
        }
    }

    SeqOutcome { name: case.name.clone(), filter_admitted, exec_gaps }
}

/// 已知的真链停机场景：**委托-CREATE nonce 竞态**（等价 `gnode attack 7702-halt`）。
///
/// - 合约 T：运行时字节码 `0x600060006000f05000`（PUSH1 0 ×3, CREATE, POP, STOP）。
/// - 账户 A（EOA）：其代码为 7702 委托指示符 `0xef0100 ++ T`，故调用 A 即在 A 的上下文里 执行 T
///   的运行时，其中的 CREATE 会 **凸起 A 的 nonce**。
/// - tx0：faucet F → A 的普通调用（足够 gas）。执行它 => 运行 A 的委托代码 => CREATE => A.nonce +=
///   1。
/// - tx1：来自 A、nonce = N（A 的区块起始 nonce）的普通调用。filter 按区块起始 nonce=N 放行； 但
///   tx0 执行后 A.nonce = N+1，故 tx1 执行期校验 => `NonceTooLow { tx: N, state: N+1 }`。
pub fn seq_7702_create_nonce_race() -> SeqCase {
    let f = Address::with_last_byte(0xFF); // faucet
    let a = Address::with_last_byte(0xAA); // 被委托的 EOA
    let t = Address::with_last_byte(0x77); // 携带 CREATE 运行时的合约

    // T 运行时：CREATE(0,0,0); POP; STOP。
    let t_code = Bytecode::new_raw(Bytes::from_static(&[
        0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0xf0, 0x50, 0x00,
    ]));
    // A 代码：7702 委托指示符 0xef0100 || T（Bytecode::new_eip7702 生成规范格式）。
    let a_code = Bytecode::new_eip7702(t);

    let a_nonce: u64 = 1; // A 的区块起始 nonce N
    let seed = vec![
        eoa(f, BIG_BALANCE, 0),
        SeedAccount {
            addr: a,
            info: AccountInfo {
                balance: U256::from(BIG_BALANCE),
                nonce: a_nonce,
                code_hash: a_code.hash_slow(),
                code: Some(a_code),
            },
        },
        SeedAccount {
            addr: t,
            info: AccountInfo {
                balance: U256::ZERO,
                nonce: 1, // 已部署合约
                code_hash: t_code.hash_slow(),
                code: Some(t_code),
            },
        },
    ];

    // tx0：F(nonce 0) -> A 的普通调用，给足 gas 让委托 CREATE 执行。
    let tx0 = legacy_tx(0, 500_000, 1000, 0, TxKind::Call(a));
    // tx1：A(nonce N=1) -> 任意 sink 的普通调用；filter 按区块起始 nonce 放行。
    let tx1 = legacy_tx(a_nonce, 21_000, 1000, 0, TxKind::Call(Address::ZERO));

    SeqCase {
        name: "seq_7702_create_nonce_race".into(),
        txs: vec![tx0, tx1],
        senders: vec![f, a],
        seed,
        base_fee: 1000,
        block_gas_limit: 30_000_000,
        prague: true,
    }
}

/// 所有 Tier-2 序列用例。
pub fn seq_cases() -> Vec<SeqCase> {
    vec![seq_7702_create_nonce_race()]
}

/// 为一个含执行缺口的序列用例导出 gnode 可复现的说明工件。
pub fn emit_seq_repro(case: &SeqCase, outcome: &SeqOutcome) -> serde_json::Value {
    let gaps: Vec<serde_json::Value> = outcome
        .exec_gaps
        .iter()
        .map(|g| {
            serde_json::json!({
                "tx_index": g.tx_index,
                "sender": format!("0x{}", hex::encode(g.sender)),
                "revm_variant": g.revm_variant,
            })
        })
        .collect();

    let txs: Vec<serde_json::Value> = case
        .txs
        .iter()
        .enumerate()
        .map(|(i, tx)| {
            let to = match tx.kind() {
                TxKind::Call(addr) => serde_json::Value::String(format!("0x{}", hex::encode(addr))),
                TxKind::Create => serde_json::Value::Null,
            };
            serde_json::json!({
                "index": i,
                "from": format!("0x{}", hex::encode(case.senders[i])),
                "to": to,
                "nonce": format!("0x{:x}", tx.nonce()),
                "gas": format!("0x{:x}", tx.gas_limit()),
                "type": format!("0x{:x}", tx.ty()),
            })
        })
        .collect();

    serde_json::json!({
        "name": case.name,
        "kind": "tier2-sequence-execution-gap",
        "filter_admitted": outcome.filter_admitted,
        "exec_gaps": gaps,
        "block": {
            "base_fee": case.base_fee,
            "gas_limit": case.block_gas_limit,
            "prague": case.prague,
            "txs": txs,
        },
        "_note": format!(
            "CHAIN-HALT (execution-induced TOCTOU): filter 放行交易下标 {:?}，但顺序执行时下标 {:?} \
             在执行期被 revm 以 InvalidTransaction 拒绝（见 exec_gaps）。这就是委托-CREATE nonce 竞态：\
             执行 tx0（F->A）运行 A 的 7702 委托代码触发 CREATE，把 A 的 nonce 越过 filter 的 +1 模型，\
             tx1（A, 区块起始 nonce）执行期命中 NonceTooLow，执行器在 lib.rs:1323 panic 停链。\
             复现：在本地集群上 `gnode attack 7702-halt`；或用 `gnode send --no-wait` 在同一区块内先后提交 \
             tx0、tx1（A 预装 0xef0100||T 委托、T 预装 CREATE 运行时）。",
            outcome.filter_admitted,
            outcome.exec_gaps.iter().map(|g| g.tx_index).collect::<Vec<_>>(),
        ),
    })
}

#[cfg(all(test, feature = "difftx"))]
mod tests {
    use super::*;

    /// Milestone 1：证明两侧预言机接线正确。
    /// GasPriceLessThanBasefee —— revm 拒绝、filter 也拒绝，非 gap。
    #[test]
    fn wiring_gas_price_less_than_basefee() {
        let s = sender();
        let case = DiffCase {
            name: "GasPriceLessThanBasefee".into(),
            tx: legacy_tx(0, 21_000, 100, 0, TxKind::Call(Address::ZERO)),
            sender: s,
            seed: vec![eoa(s, BIG_BALANCE, 0)],
            base_fee: 1000,
            block_gas_limit: 30_000_000,
            prague: true,
        };
        let out = run_case(&case);
        println!("wiring outcome: {out:?}");
        assert!(out.revm_reject, "revm 应拒绝 GasPriceLessThanBasefee");
        assert!(!out.filter_admit, "filter 应拒绝（max_fee < base_fee）");
        assert!(!out.is_gap);
        assert_eq!(out.revm_variant.as_deref(), Some("GasPriceLessThanBasefee"));
    }

    /// Milestone 2：每个可造用例都必须真的让 revm 拒绝（否则 crafter 有问题）。
    #[test]
    fn every_craftable_case_makes_revm_reject() {
        for case in craftable_cases() {
            let out = run_case(&case);
            println!(
                "{:<32} filter_admit={:<5} revm_reject={:<5} variant={:?}",
                out.name, out.filter_admit, out.revm_reject, out.revm_variant
            );
            assert!(
                out.revm_reject,
                "用例 {} 未能让 revm 拒绝（revm_variant={:?}）—— crafter 需修正",
                case.name, out.revm_variant
            );
        }
    }

    /// CI 门：矩阵中不允许任何 gap（filter 是 revm 可达变体的证明超集）。
    #[test]
    fn matrix_has_no_gap() {
        let matrix = variant_matrix();
        let gaps: Vec<&DiffOutcome> = matrix.iter().filter(|o| o.is_gap).collect();
        for o in &matrix {
            println!(
                "{:<32} admit={:<5} reject={:<5} gap={:<5} {}",
                o.name,
                o.filter_admit,
                o.revm_reject,
                o.is_gap,
                o.revm_variant.as_deref().unwrap_or("")
            );
        }
        assert!(
            gaps.is_empty(),
            "发现停链缺口: {:?}",
            gaps.iter().map(|g| &g.name).collect::<Vec<_>>()
        );
    }

    /// emit_repro 对每个 live 用例都能产出结构化 JSON（含 _note）。
    #[test]
    fn emit_repro_shape() {
        for case in craftable_cases() {
            let out = run_case(&case);
            let json = emit_repro(&case, &out);
            assert!(json.get("_note").is_some());
            assert!(json.get("type").is_some());
        }
    }

    /// 反-vacuity 门：证明 `matrix_has_no_gap` 的「零 gap」不是某一侧被硬编成 false 的假象。
    ///
    /// 单笔字段级下 filter 是 revm 校验的完全超集，本 pin 下不存在真实 gap 反例可作 canary，
    /// 因此用「两半可达性」证明 `is_gap = filter_admit && revm_reject` 是两个都能独立取到
    /// `true` 的合取——一旦二者在同一 case 同时为真即会被判为 gap：
    /// - (a) 合法交易 ⇒ filter 放行（`filter_admit == true`）且 revm 接受（`revm_reject ==
    ///   false`）；
    /// - (b) 至少一个可造用例 ⇒ revm 拒绝（`revm_reject == true`）。
    /// 若 `run_case` 把 `filter_admit` 恒置 false（例如接线错误），(a) 会立即失败。
    #[test]
    fn no_gap_gate_is_non_vacuous() {
        let s = sender();
        // (a) 一笔完全合法的 EIP-1559 转账：nonce 对齐、max_fee ≥ base_fee ≥ prio、余额充足、
        //     21000 intrinsic 足够、目标为空码地址。两侧都应放行/接受。
        let happy = DiffCase {
            name: "happy_valid_1559".into(),
            tx: eip1559_tx(MAINNET_CHAIN_ID, 0, 21_000, 2000, 100),
            sender: s,
            seed: vec![eoa(s, BIG_BALANCE, 0)],
            base_fee: 1000,
            block_gas_limit: 30_000_000,
            prague: true,
        };
        let ho = run_case(&happy);
        println!("happy outcome: {ho:?}");
        assert!(ho.filter_admit, "filter 应放行合法交易（证明 filter_admit 可达 true）");
        assert!(!ho.revm_reject, "revm 应接受合法交易，variant={:?}", ho.revm_variant);
        assert!(!ho.is_gap);

        // (b) 可造用例证明 revm_reject 可达 true（与 (a) 的 admit=true 构成非平凡合取）。
        let rejected = run_case(&craftable_cases()[0]);
        assert!(rejected.revm_reject, "可造用例应让 revm 拒绝，证明 revm_reject 可达 true");
    }

    /// Tier-2：已知真链停机的委托-CREATE nonce 竞态。
    ///
    /// 期望 filter 放行 tx1（下标 1），而顺序执行时 tx1 在执行期命中 NonceTooLow —— 即预言机
    /// 捕获到这个执行诱导的停链缺口。若 filter 实际已拒绝 tx1（exec_gaps 为空），则说明该路径
    /// 已被 filter 关闭；此时**不伪造通过**，而是断言经验事实并在断言信息里打印真实结果。
    #[test]
    fn seq_7702_create_nonce_race_is_a_gap() {
        let case = seq_7702_create_nonce_race();
        let out = run_seq_case(&case);
        println!(
            "SEQ outcome: name={} filter_admitted={:?} exec_gaps={:?}",
            out.name, out.filter_admitted, out.exec_gaps
        );

        // 记录经验事实：ground truth 需两种情形都能被如实报告。
        let tx1_admitted = out.filter_admitted.contains(&1);
        let nonce_gap = out
            .exec_gaps
            .iter()
            .find(|g| g.tx_index == 1 && g.revm_variant.contains("NonceTooLow"));

        assert!(
            tx1_admitted,
            "经验事实：filter 未放行 tx1（filter_admitted={:?}）—— 该路径已被 filter 关闭，不是开放 gap",
            out.filter_admitted
        );
        assert!(
            nonce_gap.is_some(),
            "经验事实：filter 放行了 tx1 但执行期未产生 NonceTooLow（exec_gaps={:?}）—— \
             需人工确认是否已闭合/或需修正复现",
            out.exec_gaps
        );
        println!("Tier-2 verdict: 已复现已知停链缺口 -> {:?}", nonce_gap.unwrap());
    }

    /// emit_seq_repro 对含 gap 的序列用例产出结构化工件（含 _note 与 exec_gaps）。
    #[test]
    fn emit_seq_repro_shape() {
        let case = seq_7702_create_nonce_race();
        let out = run_seq_case(&case);
        let json = emit_seq_repro(&case, &out);
        assert!(json.get("_note").is_some());
        assert!(json.get("exec_gaps").is_some());
        assert_eq!(json.get("kind").and_then(|v| v.as_str()), Some("tier2-sequence-execution-gap"));
    }
}
