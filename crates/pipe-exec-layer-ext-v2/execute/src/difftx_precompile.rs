//! Gravity 自定义 precompile 预言机（differential / invariant oracle）。
//!
//! ## 目标面
//!
//! - `mint_precompile.rs`（`NATIVE_MINT_PRECOMPILE_ADDR`）：只允许 `AUTHORIZED_CALLER` 调用，直接改
//!   EVM journal 余额铸币（`load_account(recipient).balance += amount`）。
//! - `randomness_precompile.rs` / `gravity_precompiles::randomness_by_height`
//!   （`RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR`）：按高度只读取随机数。
//!
//! 这些是 Gravity 自研、审计较少、且**直接改状态**的面。
//!
//! ## 不变式（invariant）
//!
//! 1. **caller 门**——非授权 caller 必须被干净拒绝（`Err`，不 panic、不铸币）；授权 caller +
//!    合法输入必须 `Ok`（真铸币）。
//! 2. **畸形输入永不 panic**——空/短/超长/错 func-id/gas 不足/amount=MAX 全部返回干净 `Err`
//!    （或合法时 `Ok`），绝不 panic（precompile panic = 全网停链）。
//! 3. **应用自定义 precompile 后 serial ⟷ grevm 执行逐字节一致**——mint 直改 journal state， 若
//!    grevm 未把该「precompile 直写」当作读写依赖追踪，后续读该余额的 tx 会看到陈旧状态 →
//!    与串行背离 = 一次共识 state-fork。
//!
//! 生产接线参照 `lib.rs:955-960`（组 `(NATIVE_MINT_PRECOMPILE_ADDR,
//! create_mint_token_precompile())`） 与 `lib.rs:1257-1260`（`executor.apply_custom_precompiles(...
//! )`）；执行 harness 参照 `difftx_exec.rs`。
//!
//! ```text
//! cargo test -p reth-pipe-exec-layer-ext-v2 --features difftx_precompile --lib difftx_precompile -- --nocapture
//! ```

use alloy_consensus::{constants::KECCAK_EMPTY, Header, TxLegacy};
use alloy_eips::{eip4895::Withdrawals, eip7685::EMPTY_REQUESTS_HASH, merge::BEACON_NONCE};
use alloy_primitives::{address, Address, Bytes, Signature, TxKind, B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
use reth_ethereum_primitives::{Block, BlockBody, Receipt, Transaction, TransactionSigned};
use reth_evm::{
    execute::BasicBlockExecutor,
    parallel_execute::{ParallelExecutor, WrapExecutor},
    precompiles::{DynPrecompile, PrecompileInput},
};
use reth_evm_ethereum::{parallel_execute::GrevmExecutor, EthEvmConfig};
use reth_primitives_traits::RecoveredBlock;
use revm::{
    database::{CacheDB, EmptyDB},
    precompile::{PrecompileError, PrecompileResult},
    state::{AccountInfo, Bytecode},
};
use std::sync::Arc;

use crate::{
    difftx::SeedAccount,
    mint_precompile::{create_mint_token_precompile, AUTHORIZED_CALLER},
    onchain_config::NATIVE_MINT_PRECOMPILE_ADDR,
};

// mint_precompile.rs 里 `FUNC_MINT` / `MINT_BASE_GAS`
// 是私有常量，这里镜像其取值（与目标面严格对齐）。
/// mint 函数 id（对应 `mint_precompile::FUNC_MINT`）。
const FUNC_MINT: u8 = 0x01;
/// mint 基础 gas（对应 `mint_precompile::MINT_BASE_GAS`）。
const MINT_BASE_GAS: u64 = 21_000;
/// mint 精确输入长度：1(func) + 20(recipient) + 32(amount)。
const MINT_INPUT_LEN: usize = 1 + 20 + 32;

// —————————————————————————————— 通用小工具 —————————————————————————————— //

/// 确定性地址：`0x0000…00{b}`。
const fn addr(b: u8) -> Address {
    Address::with_last_byte(b)
}

/// 构造 mint precompile 的合法 calldata：`[func_id | recipient(20) | amount(32-be)]`。
fn mint_calldata(recipient: Address, amount: U256) -> [u8; MINT_INPUT_LEN] {
    let mut data = [0u8; MINT_INPUT_LEN];
    data[0] = FUNC_MINT;
    data[1..21].copy_from_slice(recipient.as_slice());
    data[21..].copy_from_slice(&amount.to_be_bytes::<32>());
    data
}

// ============================================================================
// 第 1 部分：precompile 级不变式测试（直接 .call，caller 门 + 畸形输入不 panic）
// ============================================================================

#[cfg(all(test, feature = "difftx_precompile"))]
mod precompile_level {
    use super::*;
    use gravity_precompiles::randomness_by_height::{
        create_randomness_by_height_precompile, RandomnessByHeightLookup,
        RandomnessByHeightProvider, RANDOMNESS_BY_HEIGHT_INPUT_LEN,
        RANDOMNESS_BY_HEIGHT_OUTPUT_LEN,
    };
    use reth_evm::{eth::EthEvmContext, precompiles::Precompile, EvmInternals};
    use revm::{context::JournalTr, database::EmptyDB};

    /// 用给定 `caller` / `data` / `gas` 直接调用 mint precompile；`ctx` 由调用方持有，
    /// 以便调用后检查 recipient 余额是否真被改。返回结果（result 为 owned，不借用 ctx）。
    fn call_mint(
        ctx: &mut EthEvmContext<EmptyDB>,
        caller: Address,
        data: &[u8],
        gas: u64,
    ) -> PrecompileResult {
        create_mint_token_precompile().call(PrecompileInput {
            data,
            gas,
            caller,
            value: U256::ZERO,
            target_address: NATIVE_MINT_PRECOMPILE_ADDR,
            bytecode_address: NATIVE_MINT_PRECOMPILE_ADDR,
            internals: EvmInternals::new(&mut ctx.journaled_state, &ctx.block),
        })
    }

    fn fresh_ctx() -> EthEvmContext<EmptyDB> {
        EthEvmContext::new(EmptyDB::default(), Default::default())
    }

    fn is_unauthorized(res: &PrecompileResult) -> bool {
        matches!(res, Err(PrecompileError::Other(msg)) if msg.contains("Unauthorized"))
    }

    // ————————————————————————— caller 门 ————————————————————————— //

    /// 非授权 caller → 干净 `Err(Unauthorized)`，不 panic，且**未铸币**（recipient
    /// 仍不存在/零余额）。
    #[test]
    fn unauthorized_caller_is_rejected_and_mints_nothing() {
        let recipient = addr(0xCC);
        let data = mint_calldata(recipient, U256::from(1_000u64));

        // 试遍若干非授权 caller：零地址、随机地址、以及「差一位」的近似地址。
        let impostors = [
            Address::ZERO,
            addr(0x01),
            address!("0x595475934ed7d9faa7fca28341c2ce583904a44f"), // 末位 e→f
        ];
        for caller in impostors {
            assert_ne!(caller, AUTHORIZED_CALLER, "构造前提：impostor 必须≠授权地址");
            let mut ctx = fresh_ctx();
            let res = call_mint(&mut ctx, caller, &data, MINT_BASE_GAS);
            assert!(is_unauthorized(&res), "非授权 caller {caller} 应被拒：{res:?}");

            // 未铸币：recipient 账户余额必须为零（load_account 会惰性建账，但余额应为 0）。
            let bal = ctx.journaled_state.load_account(recipient).unwrap().data.info.balance;
            assert_eq!(bal, U256::ZERO, "被拒的调用不得改动 recipient 余额");
        }
    }

    /// 授权 caller + 合法输入 → `Ok`，且 recipient 余额精确 += amount，账户被 touch。
    #[test]
    fn authorized_caller_mints_exactly() {
        let recipient = addr(0xCC);
        let amount = U256::from(123_456_789u64);
        let data = mint_calldata(recipient, amount);
        let mut ctx = fresh_ctx();

        let out = call_mint(&mut ctx, AUTHORIZED_CALLER, &data, MINT_BASE_GAS)
            .expect("授权 caller + 合法输入必须 Ok");
        assert_eq!(out.gas_used, MINT_BASE_GAS, "mint 应恰好计 MINT_BASE_GAS");

        let acct = ctx.journaled_state.load_account(recipient).unwrap().data;
        assert_eq!(acct.info.balance, amount, "recipient 余额应精确等于 mint 数额");
        assert!(acct.is_touched(), "mint 后 recipient 必须被 mark_touch");
    }

    // ————————————————————————— 畸形输入永不 panic ————————————————————————— //

    /// 一批畸形/边界输入：**每个都必须返回 `Err` 而非 panic**。用授权 caller 以穿过 caller 门、
    /// 真正压到长度/func-id/amount 解析路径（gas 不足用例除外——它在最前置被拦）。
    #[test]
    fn malformed_input_never_panics() {
        let recipient = addr(0xCC);
        let good = mint_calldata(recipient, U256::from(7u64));

        // (名称, caller, data, gas) → 期望 Err。
        let mut over_long = good.to_vec();
        over_long.extend_from_slice(&[0u8; 16]); // 69 bytes，超长

        let max_amount = mint_calldata(recipient, U256::MAX); // amount 溢出 u128 → Err

        let cases: Vec<(&str, Address, Vec<u8>, u64)> = vec![
            ("empty", AUTHORIZED_CALLER, vec![], MINT_BASE_GAS),
            (
                "wrong_func_id",
                AUTHORIZED_CALLER,
                {
                    let mut d = good.to_vec();
                    d[0] = 0x02;
                    d
                },
                MINT_BASE_GAS,
            ),
            ("truncated_1byte", AUTHORIZED_CALLER, vec![FUNC_MINT], MINT_BASE_GAS),
            ("truncated_20bytes", AUTHORIZED_CALLER, good[..20].to_vec(), MINT_BASE_GAS),
            ("truncated_52bytes", AUTHORIZED_CALLER, good[..52].to_vec(), MINT_BASE_GAS),
            ("over_long", AUTHORIZED_CALLER, over_long, MINT_BASE_GAS),
            ("gas_below_base", AUTHORIZED_CALLER, good.to_vec(), MINT_BASE_GAS - 1),
            ("gas_zero", AUTHORIZED_CALLER, good.to_vec(), 0),
            ("amount_u256_max", AUTHORIZED_CALLER, max_amount.to_vec(), MINT_BASE_GAS),
            (
                "amount_zero",
                AUTHORIZED_CALLER,
                mint_calldata(recipient, U256::ZERO).to_vec(),
                MINT_BASE_GAS,
            ),
            // 非授权 + 畸形：应在 caller 门/gas 门被拒，同样不得 panic。
            ("unauth_empty", addr(0x09), vec![], MINT_BASE_GAS),
            ("unauth_garbage", addr(0x09), vec![0xff; 200], MINT_BASE_GAS),
        ];

        for (name, caller, data, gas) in cases {
            let mut ctx = fresh_ctx();
            // 若 handler panic，此处会直接 panic 使测试失败——即「不 panic」的证明。
            let res = call_mint(&mut ctx, caller, &data, gas);
            assert!(res.is_err(), "[{name}] 畸形输入应返回 Err（不 panic、不铸币）：{res:?}");
        }
    }

    // ————————————————————————— randomness precompile（provider
    // 可廉价构造）—————————————————————————

    /// 极简 mock provider：任意高度返回固定值，`Error = Infallible`（构造零成本）。
    struct MockRand {
        value: Option<B256>,
    }
    impl RandomnessByHeightProvider for MockRand {
        type Error = std::convert::Infallible;
        fn randomness_by_height(
            &self,
            _height: u64,
        ) -> Result<RandomnessByHeightLookup, Self::Error> {
            Ok(RandomnessByHeightLookup::recent(self.value))
        }
    }

    fn call_rand(precompile: &DynPrecompile, data: &[u8], gas: u64) -> PrecompileResult {
        let mut ctx = fresh_ctx();
        precompile.call(PrecompileInput {
            data,
            gas,
            caller: addr(0xAB),
            value: U256::ZERO,
            target_address: addr(0x02),
            bytecode_address: addr(0x02),
            internals: EvmInternals::new(&mut ctx.journaled_state, &ctx.block),
        })
    }

    /// randomness：合法 32 字节输入 + 充裕 gas → `Ok`，输出恰好 64 字节；
    /// 畸形长度 / gas 不足 → 干净 `Err`，全程不 panic。
    #[test]
    fn randomness_gating_and_malformed_never_panic() {
        let precompile = create_randomness_by_height_precompile(Arc::new(MockRand {
            value: Some(B256::repeat_byte(0x7a)),
        }));

        // 1) 合法：ABI 编码 uint256 height=1，gas 充裕。
        let mut height = [0u8; RANDOMNESS_BY_HEIGHT_INPUT_LEN];
        height[31] = 1;
        let ok = call_rand(&precompile, &height, 1_000_000).expect("合法输入应 Ok");
        assert_eq!(ok.bytes.len(), RANDOMNESS_BY_HEIGHT_OUTPUT_LEN, "输出应为 64 字节");

        // 2) height > u64::MAX（全 0xff）→ Ok(found=false)，仍不 panic。
        let huge = [0xffu8; RANDOMNESS_BY_HEIGHT_INPUT_LEN];
        let res = call_rand(&precompile, &huge, 1_000_000);
        assert!(res.is_ok(), "height 溢出 u64 应 Ok(found=false)：{res:?}");

        // 3) 畸形长度 / gas 不足 → Err，不 panic。
        let bad: Vec<(&str, Vec<u8>, u64)> = vec![
            ("empty", vec![], 1_000_000),
            ("short_31", vec![0u8; 31], 1_000_000),
            ("long_33", vec![0u8; 33], 1_000_000),
            ("gas_zero", height.to_vec(), 0),
            ("gas_below_recent", height.to_vec(), 1), // < RECENT_GAS(4000)
        ];
        for (name, data, gas) in bad {
            let res = call_rand(&precompile, &data, gas);
            assert!(res.is_err(), "[rand:{name}] 应 Err（不 panic）：{res:?}");
        }
    }
}

// ============================================================================
// 第 2 部分：应用自定义 precompile 后的 serial ⟷ grevm 执行差分（fork hunt）
// ============================================================================
//
// harness 镜像 `difftx_exec.rs`（seeded CacheDB → 两侧同源 clone → 各自 execute → 逐字段比对），
// 但在 execute 之前对**每一侧**调用 `apply_custom_precompiles([(mint_addr, mint())])`
// （镜像 `lib.rs:1257-1260`）。这是自定义 precompile 抵达执行器的**唯一**通道（不在 EthEvmConfig
// 里）。

/// Prague-from-genesis `chainspec`（与 `difftx_exec` 同构）。
fn prague_chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
}

/// 由 seed 构造 `CacheDB<EmptyDB>`（两侧 clone 同一份，保证前置状态逐字节一致）。
fn seeded_cache(seed: &[SeedAccount]) -> CacheDB<EmptyDB> {
    let mut cache: CacheDB<EmptyDB> = CacheDB::new(EmptyDB::default());
    for s in seed {
        cache.insert_account_info(s.addr, s.info.clone());
    }
    cache
}

type SerialExecutor =
    WrapExecutor<CacheDB<EmptyDB>, BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>>;

fn fresh_serial_executor(chain_spec: Arc<ChainSpec>, db: CacheDB<EmptyDB>) -> SerialExecutor {
    let evm_config = EthEvmConfig::new(chain_spec);
    WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))
}

fn fresh_grevm_executor(
    chain_spec: Arc<ChainSpec>,
    db: CacheDB<EmptyDB>,
) -> GrevmExecutor<CacheDB<EmptyDB>, EthEvmConfig, ChainSpec> {
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    GrevmExecutor::new(chain_spec, &evm_config, db)
}

/// 每次返回一份**新的** mint 自定义 precompile 注册表（两侧各持一份 Arc，不共享实例）。
fn mint_custom_precompiles() -> Arc<Vec<(Address, DynPrecompile)>> {
    Arc::new(vec![(NATIVE_MINT_PRECOMPILE_ADDR, create_mint_token_precompile())])
}

/// 构造 Prague-shape 区块（镜像 `difftx_exec::build_block`）。
fn build_block(txs: Vec<TransactionSigned>, senders: Vec<Address>) -> RecoveredBlock<Block> {
    assert_eq!(txs.len(), senders.len(), "txs 与 senders 必须等长");
    let header = Header {
        parent_hash: B256::ZERO,
        beneficiary: Address::ZERO,
        number: 1,
        timestamp: 0,
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        mix_hash: B256::ZERO,
        nonce: BEACON_NONCE.into(),
        ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
        withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
        excess_blob_gas: Some(0),
        blob_gas_used: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        requests_hash: Some(EMPTY_REQUESTS_HASH),
        ..Default::default()
    };
    let body = BlockBody {
        transactions: txs,
        ommers: Vec::new(),
        withdrawals: Some(Withdrawals::default()),
    };
    RecoveredBlock::new_unhashed(Block { header, body }, senders)
}

/// legacy 交易（可带 input，用于 CALL 合约传 calldata）。
fn legacy_tx(
    nonce: u64,
    gas_limit: u64,
    gas_price: u128,
    value: u128,
    to: TxKind,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price,
            gas_limit,
            to,
            value: U256::from(value),
            input,
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// EOA 种子（空代码）。
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

/// 合约种子（运行时字节码）。
fn contract(addr: Address, code_bytes: Vec<u8>, balance: u128) -> SeedAccount {
    let code = Bytecode::new_raw(Bytes::from(code_bytes));
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

/// 「授权铸币者」运行时字节码：把 tx calldata（53 字节 mint 输入）拷进内存后 `CALL` mint
/// precompile。 因该字节码部署在 `AUTHORIZED_CALLER` 地址，CALL 时 precompile 看到的 `caller ==
/// AUTHORIZED_CALLER`。
///
/// 汇编：
/// ```text
/// PUSH1 0x35 PUSH1 0x00 PUSH1 0x00 CALLDATACOPY   ; mem[0..53] = calldata[0..53]
/// PUSH1 0 PUSH1 0 PUSH1 0x35 PUSH1 0 PUSH1 0      ; retLen retOff argsLen(53) argsOff value(0)
/// PUSH20 <mint_addr> GAS CALL STOP                ; CALL(gas, mint, 0, 0, 53, 0, 0)
/// ```
fn mint_caller_bytecode() -> Vec<u8> {
    let mut code = vec![
        0x60, 0x35, 0x60, 0x00, 0x60, 0x00, 0x37, // PUSH1 53, PUSH1 0, PUSH1 0, CALLDATACOPY
        0x60, 0x00, // retLength = 0
        0x60, 0x00, // retOffset = 0
        0x60, 0x35, // argsLength = 53
        0x60, 0x00, // argsOffset = 0
        0x60, 0x00, // value = 0
        0x73, // PUSH20
    ];
    code.extend_from_slice(NATIVE_MINT_PRECOMPILE_ADDR.as_slice());
    code.extend_from_slice(&[0x5a, 0xf1, 0x00]); // GAS, CALL, STOP
    code
}

/// 单个差分结果。
struct PrecompileExecDiff {
    serial_ok: bool,
    grevm_ok: bool,
    diverged: bool,
    divergence: Option<String>,
    /// 便于断言的补充：recipient 在两侧的最终余额（若可取）。
    note: String,
}

/// 逐字段比对两侧输出（`state`/`contracts`/`state_size` + `receipts` +
/// `gas_used`）。返回首个差异描述。
fn diff_outputs(
    serial: &reth_execution_types::BlockExecutionOutput<Receipt>,
    grevm: &reth_execution_types::BlockExecutionOutput<Receipt>,
) -> Option<String> {
    if serial.state.state != grevm.state.state {
        return Some("BundleState.state 背离（余额/nonce/code/storage 之一不同）".to_string());
    }
    if serial.state.contracts != grevm.state.contracts {
        return Some("BundleState.contracts 背离".to_string());
    }
    if serial.state.state_size != grevm.state.state_size {
        return Some(format!(
            "BundleState.state_size 背离：serial={} grevm={}",
            serial.state.state_size, grevm.state.state_size
        ));
    }
    if serial.result.receipts != grevm.result.receipts {
        return Some("receipts 背离".to_string());
    }
    if serial.result.gas_used != grevm.result.gas_used {
        return Some(format!(
            "block gas_used 背离：serial={} grevm={}",
            serial.result.gas_used, grevm.result.gas_used
        ));
    }
    None
}

/// 跑一次「应用 mint precompile → 两侧 execute → 比对」的差分。
fn run_precompile_exec_diff(
    seed: Vec<SeedAccount>,
    txs: Vec<TransactionSigned>,
    senders: Vec<Address>,
    probe: Option<Address>,
) -> PrecompileExecDiff {
    let chain_spec = prague_chain_spec();
    let block = build_block(txs, senders);
    let cache = seeded_cache(&seed);

    let mut serial = fresh_serial_executor(chain_spec.clone(), cache.clone());
    let mut grevm = fresh_grevm_executor(chain_spec, cache);

    // 关键步骤：在 execute 之前，对**每一侧**注册自定义 mint precompile（镜像 lib.rs:1257-1260）。
    serial.apply_custom_precompiles(mint_custom_precompiles());
    grevm.apply_custom_precompiles(mint_custom_precompiles());

    let serial_res = serial.execute(&block);
    let grevm_res = grevm.execute(&block);

    let serial_ok = serial_res.is_ok();
    let grevm_ok = grevm_res.is_ok();

    let (diverged, divergence, note) = match (&serial_res, &grevm_res) {
        (Ok(so), Ok(go)) => {
            let note = probe
                .map(|p| {
                    let sb =
                        so.state.state.get(&p).and_then(|a| a.info.as_ref()).map(|i| i.balance);
                    let gb =
                        go.state.state.get(&p).and_then(|a| a.info.as_ref()).map(|i| i.balance);
                    format!("probe {p} 最终余额: serial={sb:?} grevm={gb:?}")
                })
                .unwrap_or_default();
            match diff_outputs(so, go) {
                Some(d) => (true, Some(d), note),
                None => (false, None, note),
            }
        }
        (Ok(_), Err(e)) => {
            (true, Some(format!("串行 Ok 而 grevm Err（halt gap）：{e:?}")), String::new())
        }
        (Err(e), Ok(_)) => {
            (true, Some(format!("grevm Ok 而串行 Err（halt gap）：{e:?}")), String::new())
        }
        (Err(se), Err(ge)) => {
            let ss = format!("{se:?}");
            let gs = format!("{ge:?}");
            if ss == gs {
                (false, None, "两侧一致失败".to_string())
            } else {
                (
                    true,
                    Some(format!("两侧均失败但错误模式不同：serial={ss} grevm={gs}")),
                    String::new(),
                )
            }
        }
    };

    PrecompileExecDiff { serial_ok, grevm_ok, diverged, divergence, note }
}

#[cfg(all(test, feature = "difftx_precompile"))]
mod exec_diff {
    use super::*;

    /// **主战场（fork hunt）**：授权铸币者合约 CALL mint precompile 直改 recipient journal 余额，
    /// 随后同块内 recipient 花掉这笔被铸的余额。预言机回答的问题：
    /// **grevm 是否把 precompile 的「直写 journal」当成 recipient 的读写依赖，
    /// 使得 tx1 看到的 recipient 余额与串行逐字节一致？** 否则即真共识 fork。
    #[test]
    fn mint_precompile_journal_write_tracked_as_dependency() {
        let funder = addr(0xA0); // tx0 sender（付 gas）
        let recipient = addr(0xCC); // 被铸币者 = tx1 sender
        let dest = addr(0xDD); // tx1 收款人
        let one_eth: u128 = 1_000_000_000_000_000_000;
        let huge: u128 = 100 * one_eth;

        // tx0：funder → AUTHORIZED_CALLER（铸币合约），input = mint(recipient, 1 ETH)。
        //      合约 CALL mint precompile → recipient 余额 += 1 ETH（直写 journal）。
        let tx0 = legacy_tx(
            0,
            300_000,
            10,
            0,
            TxKind::Call(AUTHORIZED_CALLER),
            Bytes::from(mint_calldata(recipient, U256::from(one_eth)).to_vec()),
        );
        // tx1：recipient → dest 转 1000 wei。**依赖** tx0 铸出的余额（否则余额不足付 gas）。
        let tx1 = legacy_tx(0, 21_000, 10, 1_000, TxKind::Call(dest), Bytes::new());

        let seed = vec![
            eoa(funder, huge, 0),
            contract(AUTHORIZED_CALLER, mint_caller_bytecode(), 0),
            eoa(recipient, 0, 0), // 初始零余额：tx1 完全依赖 tx0 的 precompile 铸币
            eoa(dest, 0, 0),
        ];

        let d = run_precompile_exec_diff(
            seed,
            vec![tx0, tx1],
            vec![funder, recipient],
            Some(recipient),
        );

        println!(
            "[exec_diff:authorized-mint-then-spend] serial_ok={} grevm_ok={} diverged={} {} {}",
            d.serial_ok,
            d.grevm_ok,
            d.diverged,
            d.divergence.as_deref().unwrap_or(""),
            d.note,
        );

        assert!(d.serial_ok, "串行后端应成功执行本块：{:?}", d.divergence);
        assert!(d.grevm_ok, "grevm 后端应成功执行本块：{:?}", d.divergence);
        assert!(
            !d.diverged,
            "serial⟷grevm 背离 —— 疑似共识 state-fork（precompile 直写未被 grevm 当依赖追踪？）：{:?}",
            d.divergence
        );
    }

    /// 退化路径（path B 兜底）：直接把一笔 tx 打到 mint precompile 地址（EOA sender，非授权 →
    /// precompile 返回 Err）。仍应用 precompile 并执行整块，断言 serial==grevm。
    /// 覆盖「custom precompile 已注册 + 被 tx 直接命中」的执行路径本身。
    #[test]
    fn direct_unauthorized_call_to_precompile_serial_equals_grevm() {
        let sender = addr(0xB0);
        let huge: u128 = 100_000_000_000_000_000_000;

        // EOA 直接 CALL mint precompile 地址；caller=sender≠AUTHORIZED_CALLER → precompile Err。
        let tx0 = legacy_tx(
            0,
            100_000,
            10,
            0,
            TxKind::Call(NATIVE_MINT_PRECOMPILE_ADDR),
            Bytes::from(mint_calldata(addr(0xEE), U256::from(1u64)).to_vec()),
        );

        let seed = vec![eoa(sender, huge, 0), eoa(addr(0xEE), 0, 0)];
        let d = run_precompile_exec_diff(seed, vec![tx0], vec![sender], Some(addr(0xEE)));

        println!(
            "[exec_diff:direct-unauthorized] serial_ok={} grevm_ok={} diverged={} {} {}",
            d.serial_ok,
            d.grevm_ok,
            d.diverged,
            d.divergence.as_deref().unwrap_or(""),
            d.note,
        );

        assert!(d.serial_ok, "串行应成功执行整块（tx 内部失败不影响块）：{:?}", d.divergence);
        assert!(d.grevm_ok, "grevm 应成功执行整块：{:?}", d.divergence);
        assert!(!d.diverged, "serial⟷grevm 背离：{:?}", d.divergence);
    }

    /// 基线：应用了 mint precompile，但块内不触碰它（普通 A→B 转账）。证明「注册自定义 precompile」
    /// 这一步本身不引入两侧背离——从而真背离可被归因到 precompile 交互而非注册副作用。
    #[test]
    fn applying_precompile_without_touching_it_serial_equals_grevm() {
        let a = addr(0xA7);
        let b = addr(0xB7);
        let huge: u128 = 100_000_000_000_000_000_000;
        let tx0 = legacy_tx(0, 21_000, 10, 1_000, TxKind::Call(b), Bytes::new());
        let seed = vec![eoa(a, huge, 0), eoa(b, 0, 0)];
        let d = run_precompile_exec_diff(seed, vec![tx0], vec![a], Some(b));

        println!(
            "[exec_diff:baseline-no-touch] serial_ok={} grevm_ok={} diverged={} {}",
            d.serial_ok, d.grevm_ok, d.diverged, d.note,
        );
        assert!(d.serial_ok && d.grevm_ok, "基线两侧都应成功：{:?}", d.divergence);
        assert!(!d.diverged, "基线不应背离：{:?}", d.divergence);
    }
}
