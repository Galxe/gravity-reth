//! serial(revm) ⟷ grevm 执行差分预言机（differential oracle）。
//!
//! ## 不变式（invariant）
//!
//! **串行执行（`WrapExecutor` → `BasicBlockExecutor`，即 revm 顺序执行）
//! == grevm 并行执行（`GrevmExecutor`）**，对任意区块**逐字节**一致。
//! grevm 是 Galxe 自研的并行执行器；两侧在 state root / `BundleState` / receipts
//! 上的任何背离 = 一次**共识 state-fork**（节点对状态产生分歧 → 链分裂）。
//! 这类 bug 静态分析看不出——只有把两个后端真跑起来对比才现形。
//!
//! ## 停链预言机的推广（generalized halt oracle）
//!
//! 不止 `InvalidTransaction`：**任意**执行器级失败（一个后端命中而另一个没命中），
//! 或任一后端 panic，都是一个 finding。这里以「一侧 `Ok`、另一侧 `Err`」记为
//! `diverged=true`（并记录错误串）。
//!
//! ## 构造法（参照 `system_caller_migration.rs`）
//!
//! - 串行侧 `WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))` （见 `fresh_executor`）。
//! - grevm 侧 `GrevmExecutor::new(chain_spec, &evm_config, db)` （见 `fresh_grevm_executor`）。
//! - 两侧从**同一份** `CacheDB<EmptyDB>`（clone）seed，保证前置状态逐字节一致。
//! - `BundleState` 字段选取与迁移测试一致：比对 `state` / `contracts` / `state_size` /
//!   `reverts`，**跳过** `reverts_size`（grevm 不更新它，非共识相关）。
//!
//! ## 使用
//!
//! ```text
//! cargo test -p reth-pipe-exec-layer-ext-v2 --features difftx_exec --lib difftx_exec -- --nocapture
//! cargo run  -p reth-pipe-exec-layer-ext-v2 --features difftx_exec --bin difftx_exec
//! ```

use alloy_consensus::{constants::KECCAK_EMPTY, Header, TxEip1559, TxEip7702, TxLegacy};
use alloy_eips::{
    eip4895::Withdrawals,
    eip7685::EMPTY_REQUESTS_HASH,
    eip7702::{Authorization, SignedAuthorization},
    merge::BEACON_NONCE,
};
use alloy_primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
use reth_ethereum_primitives::{Block, BlockBody, Receipt, Transaction, TransactionSigned};
use reth_evm::{execute::BasicBlockExecutor, parallel_execute::ParallelExecutor};
use reth_evm_ethereum::{parallel_execute::GrevmExecutor, EthEvmConfig};
use reth_primitives_traits::RecoveredBlock;
use revm::{
    database::{CacheDB, EmptyDB},
    state::{AccountInfo, Bytecode},
};
use std::sync::Arc;

// 复用 difftx.rs 的 `SeedAccount`（避免两套种子类型）。
use crate::difftx::SeedAccount;

// ————————————————————————————————— 常量 ————————————————————————————————— //

/// 主网 chain id —— 所有 typed 交易的 `chain_id` 都取此值。
const MAINNET_CHAIN_ID: u64 = 1;

/// 两侧共用的区块时间戳。**取 0**：`prague_activated()` 让 Prague 从创世（ts=0）激活，
/// `evm_env` 解析出 `SpecId::PRAGUE`（与 difftx.rs 的取值理由一致）。
const BLOCK_TIMESTAMP: u64 = 0;

/// 区块高度。**必须 ≥ 1**（非创世）：EIP-4788 在创世块要求 `parent_beacon_block_root == 0`
/// 且不发系统交易；取 1 让 4788/2935 走正常 system-call 路径（两侧走同一 `SystemCaller`，
/// 若在此处背离本身就是一个 finding）。
const BLOCK_NUMBER: u64 = 1;

/// 充裕余额（100 ETH）——保证「余额不是被测因素」。
const HUGE_BALANCE: u128 = 100_000_000_000_000_000_000;

// ————————————————————————————————— 数据结构 ————————————————————————————————— //

/// 一个执行差分用例：一批交易 + 一份种子状态 + 区块参数。
#[derive(Debug, Clone)]
pub struct ExecCase {
    /// 用例名。
    pub name: String,
    /// 区块内的交易序列（顺序即区块内排序）。
    pub txs: Vec<TransactionSigned>,
    /// 每笔交易的发送者（与 `txs` 等长、下标对齐；两侧共用）。
    pub senders: Vec<Address>,
    /// 种子账户；两侧从同一份列表 seed，保证前置状态逐字节一致。
    pub seed: Vec<SeedAccount>,
    /// 区块 base fee（喂给 `Header::base_fee_per_gas`）。
    pub base_fee: u64,
    /// 区块 gas limit（喂给 `Header::gas_limit`）。
    pub block_gas_limit: u64,
}

/// 一个执行差分结果。
#[derive(Debug, Clone)]
pub struct ExecDiff {
    /// 用例名。
    pub name: String,
    /// 串行后端是否无硬错误地执行了整块。
    pub serial_ok: bool,
    /// grevm 后端是否无硬错误地执行了整块。
    pub grevm_ok: bool,
    /// 是否背离：`state` / `contracts` / `state_size` / `reverts` / `receipts` / state root
    /// 任一不一致，或一侧 `Ok` 一侧 `Err`。
    pub diverged: bool,
    /// 首个具体差异的人类可读描述（`None` = 完全一致）。
    pub divergence: Option<String>,
}

// ————————————————————————————————— chainspec ————————————————————————————————— //

/// Prague 从创世激活的 chainspec（与 difftx.rs 的 `prague_chain_spec` 同构）。
fn prague_chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
}

// ————————————————————————————————— 后端构造 ————————————————————————————————— //

/// 由一份 seed 构造 `CacheDB<EmptyDB>`（两侧 clone 同一份，保证前置状态逐字节一致）。
fn seeded_cache(seed: &[SeedAccount]) -> CacheDB<EmptyDB> {
    let mut cache: CacheDB<EmptyDB> = CacheDB::new(EmptyDB::default());
    for s in seed {
        cache.insert_account_info(s.addr, s.info.clone());
    }
    cache
}

/// 串行后端类型：镜像 `system_caller_migration.rs::fresh_executor`。
type SerialExecutor = reth_evm::parallel_execute::WrapExecutor<
    CacheDB<EmptyDB>,
    BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>,
>;

fn fresh_serial_executor(chain_spec: Arc<ChainSpec>, db: CacheDB<EmptyDB>) -> SerialExecutor {
    let evm_config = EthEvmConfig::new(chain_spec);
    reth_evm::parallel_execute::WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))
}

/// grevm 后端（镜像 `system_caller_migration.rs::fresh_grevm_executor`）。
fn fresh_grevm_executor(
    chain_spec: Arc<ChainSpec>,
    db: CacheDB<EmptyDB>,
) -> GrevmExecutor<CacheDB<EmptyDB>, EthEvmConfig, ChainSpec> {
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    GrevmExecutor::new(chain_spec, &evm_config, db)
}

// ————————————————————————————————— 区块构造 ————————————————————————————————— //

/// 由 `case.txs` + `case.senders` 构造一个 Prague-shape 的 `RecoveredBlock<Block>`。
///
/// Header 字段镜像 `lib.rs::create_block_for_executor` 的 Prague 分支：
/// `timestamp=0`、`number=1`、`base_fee_per_gas`、`gas_limit`、`mix_hash=ZERO`（prevrandao）、
/// `excess_blob_gas=Some(0)`、`blob_gas_used=Some(0)`、`parent_beacon_block_root=Some(ZERO)`
/// （EIP-4788）、`requests_hash=Some(EMPTY_REQUESTS_HASH)`（EIP-7685）、
/// `withdrawals_root=Some(EMPTY_WITHDRAWALS)`（Shanghai）。
fn build_block(case: &ExecCase) -> RecoveredBlock<Block> {
    assert_eq!(case.txs.len(), case.senders.len(), "txs 与 senders 必须等长");

    let header = Header {
        parent_hash: B256::ZERO,
        beneficiary: Address::ZERO,
        number: BLOCK_NUMBER,
        timestamp: BLOCK_TIMESTAMP,
        gas_limit: case.block_gas_limit,
        base_fee_per_gas: Some(case.base_fee),
        mix_hash: B256::ZERO, // prevrandao（MERGE+ 需要，默认 ZERO 即可）
        nonce: BEACON_NONCE.into(),
        ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
        // Shanghai：空提款
        withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
        // Cancun：blob 字段
        excess_blob_gas: Some(0),
        blob_gas_used: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        // Prague：EIP-7685 requests
        requests_hash: Some(EMPTY_REQUESTS_HASH),
        ..Default::default()
    };

    let body = BlockBody {
        transactions: case.txs.clone(),
        ommers: Vec::new(),
        withdrawals: Some(Withdrawals::default()),
    };

    // 两侧执行器都用调用方提供的 senders（不重新做签名恢复），故 `test_signature`
    // 的交易配任意 sender 均可（与 lib.rs 的 `RecoveredBlock::new_unhashed` 路径一致）。
    RecoveredBlock::new_unhashed(Block { header, body }, case.senders.clone())
}

// ————————————————————————————————— 差分核心 ————————————————————————————————— //

/// 比对两侧 `BundleState`（字段选取与 `system_caller_migration.rs` 的 U-6b/U-6c 一致：
/// `state` / `contracts` / `state_size` / `reverts`；**跳过** `reverts_size`）。返回首个差异描述。
fn diff_bundle(
    serial: &revm::database::BundleState,
    grevm: &revm::database::BundleState,
) -> Option<String> {
    if serial.state != grevm.state {
        // 定位首个不一致的账户，给出更精确的定位。
        for (addr, s_acc) in &serial.state {
            match grevm.state.get(addr) {
                Some(g_acc) if g_acc == s_acc => {}
                Some(_) => {
                    return Some(format!(
                        "BundleState.state 账户 {addr} 背离（余额/nonce/code/storage 之一不同）"
                    ))
                }
                None => return Some(format!("BundleState.state：grevm 缺少账户 {addr}")),
            }
        }
        for addr in grevm.state.keys() {
            if !serial.state.contains_key(addr) {
                return Some(format!("BundleState.state：serial 缺少账户 {addr}"));
            }
        }
        return Some("BundleState.state 背离（顺序/内容差异）".to_string());
    }
    if serial.contracts != grevm.contracts {
        return Some("BundleState.contracts 背离（部署的合约字节码集合不同）".to_string());
    }
    if serial.state_size != grevm.state_size {
        return Some(format!(
            "BundleState.state_size 背离：serial={} grevm={}",
            serial.state_size, grevm.state_size
        ));
    }
    // reverts 的**语义**比对（order-insensitive，忽略空 revert-block）。
    //
    // 经验事实（见模块尾 `notes` 测试）：即便 post-state（state/contracts/state_size）
    // 与 receipts 逐字节一致——即 state root 必然相同、无共识 fork——两后端的 `reverts`
    // 原始结构仍会因两处**纯簿记差异**而不等：
    //   (a) 串行 `BasicBlockExecutor` 会多出一个**空的**尾部 revert-block（outer len=2），
    //       grevm 只有一个（outer len=1）；空 block 无任何 revert 条目。
    //   (b) 同一 block 内账户的**排列顺序**不同（串行按执行/触碰顺序，grevm 按其收集顺序），
    //       但账户集合、`previous_status`、storage revert 内容完全一致。
    // reverts 是**回滚/unwind 元数据**，从不参与 state root 的哈希，故 (a)(b) 均非共识相关。
    // 因此这里把所有非空 block 的 `(Address → AccountRevert)` 拍平成有序 map 后比对：
    // 既容忍良性的顺序/空块差异，又能抓住**真正**的内容背离（某账户回滚到了不同的
    // previous 值 / 缺失账户 —— 那意味着两后端看到了不同的中间态）。
    if let Some(d) = diff_reverts_semantic(serial, grevm) {
        return Some(d);
    }
    None
}

/// 把 `BundleState.reverts` 拍平成 `BTreeMap<Address, Vec<AccountRevert>>`（跨所有 outer
/// block 合并、丢弃空 block），返回顺序无关的语义视图。
fn flatten_reverts(
    bs: &revm::database::BundleState,
) -> std::collections::BTreeMap<Address, Vec<revm::database::states::reverts::AccountRevert>> {
    let mut m: std::collections::BTreeMap<
        Address,
        Vec<revm::database::states::reverts::AccountRevert>,
    > = std::collections::BTreeMap::new();
    for blk in bs.reverts.iter() {
        for (addr, revert) in blk {
            m.entry(*addr).or_default().push(revert.clone());
        }
    }
    m
}

/// 顺序无关地比对两侧 reverts 的语义内容。返回首个内容差异描述（`None` = 语义一致）。
fn diff_reverts_semantic(
    serial: &revm::database::BundleState,
    grevm: &revm::database::BundleState,
) -> Option<String> {
    let s = flatten_reverts(serial);
    let g = flatten_reverts(grevm);
    if s == g {
        return None;
    }
    for (addr, s_rev) in &s {
        match g.get(addr) {
            Some(g_rev) if g_rev == s_rev => {}
            Some(_) => {
                return Some(format!(
                "reverts 内容背离：账户 {addr} 的 AccountRevert 不同（回滚到了不同的 previous 值）"
            ))
            }
            None => return Some(format!("reverts 内容背离：grevm 缺少账户 {addr} 的回滚项")),
        }
    }
    for addr in g.keys() {
        if !s.contains_key(addr) {
            return Some(format!("reverts 内容背离：serial 缺少账户 {addr} 的回滚项"));
        }
    }
    Some("reverts 语义背离（内容差异）".to_string())
}

/// 比对两侧 receipts（逐笔 `Receipt` 相等）。返回首个差异描述。
fn diff_receipts(serial: &[Receipt], grevm: &[Receipt]) -> Option<String> {
    if serial.len() != grevm.len() {
        return Some(format!("receipts 数量背离：serial={} grevm={}", serial.len(), grevm.len()));
    }
    for (i, (s, g)) in serial.iter().zip(grevm.iter()).enumerate() {
        if s != g {
            return Some(format!(
                "receipt[{i}] 背离：serial={{success={}, cumulative_gas={}, logs={}}} \
                 grevm={{success={}, cumulative_gas={}, logs={}}}",
                s.success,
                s.cumulative_gas_used,
                s.logs.len(),
                g.success,
                g.cumulative_gas_used,
                g.logs.len()
            ));
        }
    }
    None
}

/// 运行单个执行差分用例：
/// 1. 由 `case.seed` 构造两份**逐字节相同**的 `CacheDB<EmptyDB>`（clone）。
/// 2. 构造 Prague-shape 区块。
/// 3. 分别过串行 `WrapExecutor` 与 grevm `GrevmExecutor` 的 `.execute(&block)`。
/// 4. 比对 `Ok/Err`、`BundleState`、receipts；填充 `diverged` + `divergence`。
///
/// 注：**state root** 是 post-state 的确定性像（pre-image 即 `BundleState.state`），
/// 故 `BundleState.state` 逐字节相等 ⟹ state root 相等；此处不单独计算 trie root
/// （需 provider/trie 依赖），由 `BundleState.state` 比对**传递性覆盖**。
pub fn run_exec_diff(case: &ExecCase) -> ExecDiff {
    let chain_spec = prague_chain_spec();
    let block = build_block(case);

    // 两份逐字节相同的前置状态。
    let cache = seeded_cache(&case.seed);
    let mut serial = fresh_serial_executor(chain_spec.clone(), cache.clone());
    let mut grevm = fresh_grevm_executor(chain_spec, cache);

    let serial_res = serial.execute(&block);
    let grevm_res = grevm.execute(&block);

    let serial_ok = serial_res.is_ok();
    let grevm_ok = grevm_res.is_ok();

    // 组装结果的便捷闭包。
    let mk = |diverged: bool, divergence: Option<String>| ExecDiff {
        name: case.name.clone(),
        serial_ok,
        grevm_ok,
        diverged,
        divergence,
    };

    // —— 推广的停链预言机：一侧 Ok 一侧 Err（或错误串不同）即背离。 ——
    match (&serial_res, &grevm_res) {
        (Ok(so), Ok(go)) => {
            // 两侧都成功 → 逐字段比对 BundleState + receipts + gas。
            if let Some(d) = diff_bundle(&so.state, &go.state) {
                return mk(true, Some(d));
            }
            if let Some(d) = diff_receipts(&so.result.receipts, &go.result.receipts) {
                return mk(true, Some(d));
            }
            if so.result.gas_used != go.result.gas_used {
                return mk(
                    true,
                    Some(format!(
                        "block gas_used 背离：serial={} grevm={}",
                        so.result.gas_used, go.result.gas_used
                    )),
                );
            }
            mk(false, None)
        }
        (Ok(_), Err(e)) => mk(
            true,
            Some(format!("停链预言机命中：串行成功而 grevm 失败（halt gap）——grevm error: {e:?}")),
        ),
        (Err(e), Ok(_)) => mk(
            true,
            Some(format!("停链预言机命中：grevm 成功而串行失败（halt gap）——serial error: {e:?}")),
        ),
        (Err(se), Err(ge)) => {
            // 两侧都失败：若错误串不同也算背离（不同后端对同一块给出不同失败模式）。
            let ss = format!("{se:?}");
            let gs = format!("{ge:?}");
            if ss == gs {
                // 同样的错误 —— 非背离（两后端一致地拒绝了这个块）。
                mk(false, None)
            } else {
                mk(true, Some(format!("两侧均失败但错误模式不同：serial={ss} grevm={gs}")))
            }
        }
    }
}

// ————————————————————————————————— 交易/账户 构造辅助 ————————————————————————————————— //

/// EOA 种子：空代码。
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

/// 带真实字节码的合约账户种子。
fn contract(addr: Address, code_bytes: &'static [u8], balance: u128) -> SeedAccount {
    let code = Bytecode::new_raw(Bytes::from_static(code_bytes));
    SeedAccount {
        addr,
        info: AccountInfo {
            balance: U256::from(balance),
            nonce: 1,
            code_hash: code.hash_slow(),
            code: Some(code),
        },
    }
}

/// EIP-7702 委托账户种子：code = `0xef0100 || delegate`（委托指示符）。
fn delegated_account(addr: Address, delegate: Address, balance: u128, nonce: u64) -> SeedAccount {
    let code = Bytecode::new_eip7702(delegate);
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

/// legacy 转账/调用交易。
fn legacy_tx(
    nonce: u64,
    gas_limit: u64,
    gas_price: u128,
    value: u128,
    to: TxKind,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price,
            gas_limit,
            to,
            value: U256::from(value),
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// EIP-1559 转账/调用交易。
fn eip1559_tx(
    nonce: u64,
    gas_limit: u64,
    max_fee: u128,
    max_prio: u128,
    value: u128,
    to: TxKind,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Eip1559(TxEip1559 {
            chain_id: MAINNET_CHAIN_ID,
            nonce,
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: max_prio,
            to,
            value: U256::from(value),
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// EIP-7702（type-0x04）交易：调用 `to`（走其已存在的委托代码）。
/// `authorization_list` 塞一个自委托授权（其签名走 dummy，不一定 recover 成功——
/// 本用例的委托效果由**预置的** `delegated_account` 提供，type-4 执行路径本身即被覆盖）。
fn eip7702_tx(nonce: u64, gas_limit: u64, max_fee: u128, to: Address) -> TransactionSigned {
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
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: 0,
            to,
            value: U256::ZERO,
            authorization_list,
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

// 确定性地址工具。
const fn addr(b: u8) -> Address {
    Address::with_last_byte(b)
}

/// increment-slot0 合约字节码：`SLOAD(0); +1; SSTORE(0)`；STOP。
/// 汇编：PUSH1 1, PUSH1 0, SLOAD, ADD, PUSH1 0, SSTORE, STOP。
const INC_SLOT0_CODE: &[u8] = &[0x60, 0x01, 0x60, 0x00, 0x54, 0x01, 0x60, 0x00, 0x55, 0x00];

// ————————————————————————————————— 用例集 ————————————————————————————————— //

/// 正常区块基线：这些块**必须** serial==grevm（证明预言机确实两个后端都跑了、且在正常
/// 输入上一致——所以真背离会被抓住）。
#[allow(clippy::vec_init_then_push)]
pub fn exec_baseline_cases() -> Vec<ExecCase> {
    let base_fee = 1u64;
    let gas = 30_000_000u64;
    let mut v = Vec::new();

    // 1) 简单 legacy 转账：A -> B。
    {
        let a = addr(0xA1);
        let b = addr(0xB1);
        v.push(ExecCase {
            name: "baseline_simple_transfer".into(),
            txs: vec![legacy_tx(0, 21_000, 10, 1_000, TxKind::Call(b))],
            senders: vec![a],
            seed: vec![eoa(a, HUGE_BALANCE, 0), eoa(b, 0, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // 2) 两笔互不相关的转账（不同 sender、不同收款人）——可并行、必须一致。
    {
        let a = addr(0xA2);
        let b = addr(0xB2);
        let c = addr(0xC2);
        let d = addr(0xD2);
        v.push(ExecCase {
            name: "baseline_two_independent_transfers".into(),
            txs: vec![
                legacy_tx(0, 21_000, 10, 111, TxKind::Call(b)),
                legacy_tx(0, 21_000, 10, 222, TxKind::Call(d)),
            ],
            senders: vec![a, c],
            seed: vec![
                eoa(a, HUGE_BALANCE, 0),
                eoa(b, 0, 0),
                eoa(c, HUGE_BALANCE, 0),
                eoa(d, 0, 0),
            ],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // 3) EIP-1559 转账。
    {
        let a = addr(0xA3);
        let b = addr(0xB3);
        v.push(ExecCase {
            name: "baseline_eip1559_transfer".into(),
            txs: vec![eip1559_tx(0, 21_000, 100, 2, 500, TxKind::Call(b))],
            senders: vec![a],
            seed: vec![eoa(a, HUGE_BALANCE, 0), eoa(b, 0, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // 4) 合约 CREATE：init code `PUSH1 0 PUSH1 0 RETURN`（部署空合约）。
    {
        let a = addr(0xA4);
        // 6000 6000 f3 = PUSH1 0, PUSH1 0, RETURN → 返回空 runtime code。
        let init: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xf3];
        v.push(ExecCase {
            name: "baseline_create_contract".into(),
            txs: vec![TransactionSigned::new_unhashed(
                Transaction::Legacy(TxLegacy {
                    nonce: 0,
                    gas_price: 10,
                    gas_limit: 200_000,
                    to: TxKind::Create,
                    value: U256::ZERO,
                    input: Bytes::from_static(init),
                    ..Default::default()
                }),
                Signature::test_signature(),
            )],
            senders: vec![a],
            seed: vec![eoa(a, HUGE_BALANCE, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // 5) EIP-7702：type-4 交易调用一个**预置委托**账户（委托到真实合约 C）。 覆盖 type-4 执行 +
    //    委托跳转（delegation follow）路径。
    {
        let a = addr(0xA5);
        let deleg = addr(0xD5); // 已带 0xef0100||C 委托代码的账户
        let c = addr(0xC5); // 真实 increment-slot0 合约
        v.push(ExecCase {
            name: "baseline_eip7702_delegated_call".into(),
            txs: vec![eip7702_tx(0, 200_000, 100, deleg)],
            senders: vec![a],
            seed: vec![
                eoa(a, HUGE_BALANCE, 0),
                delegated_account(deleg, c, 0, 0),
                contract(c, INC_SLOT0_CODE, 0),
            ],
            base_fee,
            block_gas_limit: gas,
        });
    }

    v
}

/// 对抗性区块：面向并行执行背离的构造（RAW/WAW 冲突、同 sender nonce 依赖、
/// 同收款人余额写写、CREATE 交互）。**HUNTING** 真 fork——若全都 serial==grevm，
/// 如实报「未发现背离」，绝不编造。
#[allow(clippy::vec_init_then_push)]
pub fn exec_adversarial_cases() -> Vec<ExecCase> {
    let base_fee = 1u64;
    let gas = 30_000_000u64;
    let mut v = Vec::new();

    // A1) 同一存储槽的 RAW/WAW 冲突：两笔交易都调用同一合约 C（读-改-写 slot0）。
    //     正确的 grevm 必须探测到依赖并串行化；若不然，slot0 结果会是 1 而非 2。
    {
        let a = addr(0x1A);
        let b = addr(0x1B);
        let c = addr(0x1C);
        v.push(ExecCase {
            name: "adv_raw_waw_same_slot".into(),
            txs: vec![
                legacy_tx(0, 100_000, 10, 0, TxKind::Call(c)),
                legacy_tx(0, 100_000, 10, 0, TxKind::Call(c)),
            ],
            senders: vec![a, b],
            seed: vec![
                eoa(a, HUGE_BALANCE, 0),
                eoa(b, HUGE_BALANCE, 0),
                contract(c, INC_SLOT0_CODE, 0),
            ],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // A2) 同 sender 的 nonce 依赖：同一账户连发两笔（nonce 0、1），顺序敏感。
    {
        let a = addr(0x2A);
        let b = addr(0x2B);
        v.push(ExecCase {
            name: "adv_same_sender_nonce_chain".into(),
            txs: vec![
                legacy_tx(0, 21_000, 10, 100, TxKind::Call(b)),
                legacy_tx(1, 21_000, 10, 200, TxKind::Call(b)),
            ],
            senders: vec![a, a],
            seed: vec![eoa(a, HUGE_BALANCE, 0), eoa(b, 0, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // A3) 同收款人的余额写-写：两个不同 sender 向同一账户转账（收款人余额 WAW）。
    {
        let a = addr(0x3A);
        let b = addr(0x3B);
        let r = addr(0x3F);
        v.push(ExecCase {
            name: "adv_same_recipient_balance_waw".into(),
            txs: vec![
                legacy_tx(0, 21_000, 10, 1_000, TxKind::Call(r)),
                legacy_tx(0, 21_000, 10, 2_000, TxKind::Call(r)),
            ],
            senders: vec![a, b],
            seed: vec![eoa(a, HUGE_BALANCE, 0), eoa(b, HUGE_BALANCE, 0), eoa(r, 0, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    // A4) 三笔链式：tx0 调用 C 写 slot0；tx1 同 sender 转账；tx2 再调 C。
    //     混合 nonce 依赖 + 存储冲突，压 grevm 的依赖图。
    {
        let a = addr(0x4A);
        let b = addr(0x4B);
        let c = addr(0x4C);
        v.push(ExecCase {
            name: "adv_mixed_nonce_and_storage".into(),
            txs: vec![
                legacy_tx(0, 100_000, 10, 0, TxKind::Call(c)),
                legacy_tx(1, 21_000, 10, 50, TxKind::Call(b)),
                legacy_tx(2, 100_000, 10, 0, TxKind::Call(c)),
            ],
            senders: vec![a, a, a],
            seed: vec![eoa(a, HUGE_BALANCE, 0), eoa(b, 0, 0), contract(c, INC_SLOT0_CODE, 0)],
            base_fee,
            block_gas_limit: gas,
        });
    }

    v
}

// ————————————————————————————————— 测试 ————————————————————————————————— //

#[cfg(all(test, feature = "difftx_exec"))]
mod tests {
    use super::*;

    /// **承重的正确性测试**：每个基线块 → 两侧都 Ok 且 `!diverged`。
    /// 证明该差分预言机确实把区块过了**两个后端**、且在正常输入上一致——
    /// 因此一个真实背离**会**被抓住。
    #[test]
    fn baseline_serial_equals_grevm() {
        for case in exec_baseline_cases() {
            let d = run_exec_diff(&case);
            assert!(d.serial_ok, "[{}] 串行后端未成功执行区块：{:?}", d.name, d.divergence);
            assert!(d.grevm_ok, "[{}] grevm 后端未成功执行区块：{:?}", d.name, d.divergence);
            assert!(
                !d.diverged,
                "[{}] 基线块出现 serial⟷grevm 背离（不应发生）：{:?}",
                d.name, d.divergence
            );
            println!(
                "[baseline OK] {:<40} serial_ok={} grevm_ok={} diverged={}",
                d.name, d.serial_ok, d.grevm_ok, d.diverged
            );
        }
    }

    /// 对抗性块：跑并记录经验事实。断言的是「不允许出现 serial⟷grevm 背离」——
    /// 这才是共识安全的要求（我们在 hunting，若一致就报一致，绝不编造）。
    /// 若某天此断言失败，即抓到了真 fork。
    #[test]
    fn adversarial_no_divergence_or_report() {
        let mut any_diverged = false;
        for case in exec_adversarial_cases() {
            let d = run_exec_diff(&case);
            println!(
                "[adversarial] {:<40} serial_ok={} grevm_ok={} diverged={} {}",
                d.name,
                d.serial_ok,
                d.grevm_ok,
                d.diverged,
                d.divergence.as_deref().unwrap_or("")
            );
            if d.diverged {
                any_diverged = true;
            }
        }
        assert!(
            !any_diverged,
            "对抗集出现 serial⟷grevm 背离 —— 疑似共识 state-fork，见上方 [adversarial] 行"
        );
    }

    /// **经验事实文档（empirical finding）**：serial 与 grevm 在**原始** `BundleState.reverts`
    /// 上确实不等，但这是**良性簿记差异、非共识 fork**——精确断言其形态，防止未来把它
    /// 误判成安全问题，也防止 grevm 在 reverts *内容* 上真回归时无人发现。
    ///
    /// 以最简单的 `A -> B` 转账块为证，逐字段固定观察到的事实：
    /// 1. `state` / `contracts` / `state_size` **逐字节相等** ⟹ post-state 相同 ⟹ state root 相同。
    /// 2. receipts **逐笔相等**。
    /// 3. **原始 `reverts` 不等**：
    ///    - outer-block 数量不同：serial=2（含一个空尾块）、grevm=1；
    ///    - 首块内账户排列顺序不同（串行 `[sender, coinbase, recipient]`、 grevm `[sender,
    ///      recipient, coinbase]`），但账户集合与 `AccountRevert` 内容一致。
    /// 4. 语义化（顺序无关、忽略空块）的 `flatten_reverts` **相等** —— 证明差异纯属排列/结构。
    #[test]
    fn empirical_raw_reverts_differ_but_state_root_equivalent() {
        let case = ExecCase {
            name: "empirical_simple_transfer".into(),
            txs: vec![legacy_tx(0, 21_000, 10, 1_000, TxKind::Call(addr(0xB1)))],
            senders: vec![addr(0xA1)],
            seed: vec![eoa(addr(0xA1), HUGE_BALANCE, 0), eoa(addr(0xB1), 0, 0)],
            base_fee: 1,
            block_gas_limit: 30_000_000,
        };

        let chain_spec = prague_chain_spec();
        let block = build_block(&case);
        let cache = seeded_cache(&case.seed);
        let mut serial = fresh_serial_executor(chain_spec.clone(), cache.clone());
        let mut grevm = fresh_grevm_executor(chain_spec, cache);

        let so = serial.execute(&block).expect("serial 执行成功");
        let go = grevm.execute(&block).expect("grevm 执行成功");

        // 1) 共识相关字段逐字节相等（⟹ state root 相同）。
        assert_eq!(so.state.state, go.state.state, "post-state 必须相同（决定 state root）");
        assert_eq!(so.state.contracts, go.state.contracts, "contracts 必须相同");
        assert_eq!(so.state.state_size, go.state.state_size, "state_size 必须相同");

        // 2) receipts 逐笔相等。
        assert_eq!(so.result.receipts, go.result.receipts, "receipts 必须相同");
        assert_eq!(so.result.gas_used, go.result.gas_used, "block gas_used 必须相同");

        // 3) 原始 reverts **不等**（良性簿记差异：空尾块 + 账户排列顺序）。
        assert_ne!(
            so.state.reverts, go.state.reverts,
            "已知经验事实：serial/grevm 的原始 reverts 因空尾块+账户排列而不等；\
             若此断言失败说明 grevm 改了 reverts 结构，需复核本 finding 是否仍成立"
        );

        // 4) 语义化 reverts（顺序无关、忽略空块）**相等** —— 差异纯属排列/结构，非内容。
        assert_eq!(
            flatten_reverts(&so.state),
            flatten_reverts(&go.state),
            "reverts 的语义内容必须一致（顺序无关）；若不等则是真正的共识相关回归"
        );
    }
}
