//! EIP-2935 / BLOCKHASH exec-time 预言机（differential oracle）。
//!
//! ## 目标面
//!
//! - `eip_2935.rs`：Prague 激活块部署 `HISTORY_STORAGE`,部署失败 `panic!`。
//! - `crates/gravity-storage/src/block_view_storage/mod.rs`：`BLOCKHASH` 由 `(block_number,
//!   block_id)` 环支撑;`block_hash_ref`（191-202 / 262-273 行）在请求块号 落在 `(_, max]`
//!   但**不在环内**时返回 `BlockHashError::BlockTooOld`。 warm-restart / checkpoint-resume 若只供给
//!   <256 对,窗口内最老的合法 `BLOCKHASH` 读取就会命中 `BlockTooOld`。
//!
//! ## 两条不变式
//!
//! ① **exec-time 一致性**：`BLOCKHASH` opcode 在 mined-tx 内部读到的值,
//!    串行(revm `WrapExecutor`) ⟷ grevm(`GrevmExecutor`) 必须逐字节一致,且等于喂入的
//!    规范 block hash。这里用「探针合约把 `BLOCKHASH` 结果 `SSTORE` 进自身存储、再从
//!    `BundleState` 读该 slot」的方式取 **exec-time** 值——不走 RPC / canonical-header 视图
//!    （那是错误的预言机,读的是链上视图而非执行器实际喂给 EVM 的值）。
//!
//! ② **截断窗口失败行为**：warm-restart 供给 <256 对时,最老 in-window 读取应命中
//!    `BlockTooOld`。存储层直接返回 `Err`（**不 panic**）;panic 发生在上游
//!    `lib.rs:1354 / 1411` 的 `executor.execute(&block).unwrap_or_else(|err| panic!(...))`。
//!
//! ## 可驱动性（feasibility,见模块尾测试）
//!
//! - 不变式①：**完全可驱动**——`CacheDB<EmptyDB>` 里 seed `block_hashes`,跑真实 串行 / grevm
//!   两个后端,对比探针 slot。
//! - 不变式②的**存储层** `BlockTooOld`：**完全可驱动**——用**生产结构体** `RawBlockViewProvider` +
//!   截断 `BTreeMap` 直接 `block_hash_ref`,捕获 `Err`。
//! - 不变式②的**完整 exec→panic 链**（opcode 命中 `BlockTooOld` → `host.block_hash` 返回 `None` →
//!   解释器 `FatalExternalError` halt → `executor.execute` 返回 `Err` → `lib.rs:1354` panic）：本
//!   harness 用一个**忠实复刻** `block_hash_ref` 窗口判定的 `DatabaseRef` shim 在**进程内**驱动
//!   opcode→halt→execute-`Err` 这段;但把 shim 换成 **真实**
//!   `RawBlockViewProvider`/`BlockViewProvider`（需真实 reth DB `TX` 后端）并在 warm-restart
//!   时用**截断的** `block_number_to_id` 种子驱动,需要 `pipe_test` / gnode 集群 harness——本单元
//!   harness 不具备真实 provider,故那一段**如实记为**需集群复现, 绝不伪造。

use alloy_consensus::{
    constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS, KECCAK_EMPTY},
    Header, TxLegacy,
};
use alloy_eips::{eip4895::Withdrawals, eip7685::EMPTY_REQUESTS_HASH, merge::BEACON_NONCE};
use alloy_primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};
use reth_evm::{execute::BasicBlockExecutor, parallel_execute::ParallelExecutor};
use reth_evm_ethereum::{parallel_execute::GrevmExecutor, EthEvmConfig};
use reth_primitives_traits::RecoveredBlock;
use revm::{
    database::{BundleState, CacheDB, EmptyDB},
    state::{AccountInfo, Bytecode},
};
use std::{collections::BTreeMap, sync::Arc};

// 复用 difftx.rs 的 `SeedAccount`（避免两套种子类型）。
use crate::difftx::SeedAccount;

// ————————————————————————————————— 常量 ————————————————————————————————— //

/// 区块时间戳取 0：`prague_activated()` 让 Prague 从创世（ts=0）激活,`evm_env` 解析
/// 出 `SpecId::PRAGUE`（与 `difftx_exec.rs` 取值理由一致）。
const BLOCK_TIMESTAMP: u64 = 0;

/// 充裕余额（100 ETH）——保证「余额不是被测因素」。
const HUGE_BALANCE: u128 = 100_000_000_000_000_000_000;

/// 探针合约地址。
const PROBE_ADDR: Address = Address::with_last_byte(0xBB);

/// 探针合约字节码：`SSTORE(CALLDATALOAD(0), BLOCKHASH(CALLDATALOAD(0)))`。
/// 即「读 calldata 前 32 字节当目标块号 `n`,取 `BLOCKHASH(n)`,把结果存到 slot `n`」。
///
/// 反汇编：
/// ```text
/// 60 00   PUSH1 0x00        ; calldata offset
/// 35      CALLDATALOAD      ; [n]
/// 80      DUP1              ; [n, n]
/// 40      BLOCKHASH         ; [n, hash]   (消费栈顶 n)
/// 90      SWAP1             ; [hash, n]   (栈顶=n=key)
/// 55      SSTORE            ; storage[n] = hash
/// 00      STOP
/// ```
const PROBE_CODE: &[u8] = &[0x60, 0x00, 0x35, 0x80, 0x40, 0x90, 0x55, 0x00];

// ————————————————————————————————— chainspec ————————————————————————————————— //

/// Prague 从创世激活的 chainspec（与 `difftx_exec.rs` 的 `prague_chain_spec` 同构）。
fn prague_chain_spec() -> Arc<ChainSpec> {
    Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
}

// ————————————————————————————————— 种子构造 ————————————————————————————————— //

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

/// 由一份 seed + 一份 `block_hashes`（number→hash）构造 `CacheDB<EmptyDB>`。
/// 两侧 clone 同一份,保证前置状态逐字节一致。
fn seeded_cache(seed: &[SeedAccount], block_hashes: &BTreeMap<u64, B256>) -> CacheDB<EmptyDB> {
    let mut cache: CacheDB<EmptyDB> = CacheDB::new(EmptyDB::default());
    for s in seed {
        cache.insert_account_info(s.addr, s.info.clone());
    }
    for (&number, &hash) in block_hashes {
        cache.cache.block_hashes.insert(U256::from(number), hash);
    }
    cache
}

// ————————————————————————————————— 后端构造 ————————————————————————————————— //

type SerialExecutor = reth_evm::parallel_execute::WrapExecutor<
    CacheDB<EmptyDB>,
    BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>,
>;

fn fresh_serial_executor(chain_spec: Arc<ChainSpec>, db: CacheDB<EmptyDB>) -> SerialExecutor {
    let evm_config = EthEvmConfig::new(chain_spec);
    reth_evm::parallel_execute::WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))
}

fn fresh_grevm_executor(
    chain_spec: Arc<ChainSpec>,
    db: CacheDB<EmptyDB>,
) -> GrevmExecutor<CacheDB<EmptyDB>, EthEvmConfig, ChainSpec> {
    let evm_config = EthEvmConfig::new(chain_spec.clone());
    GrevmExecutor::new(chain_spec, &evm_config, db)
}

// ————————————————————————————————— 区块 / 交易构造 ————————————————————————————————— //

/// legacy「调用探针合约」交易：`input = 32B big-endian 目标块号`。
fn probe_call_tx(nonce: u64, target: u64) -> TransactionSigned {
    let mut input = [0u8; 32];
    input[24..32].copy_from_slice(&target.to_be_bytes());
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            nonce,
            gas_price: 10,
            gas_limit: 100_000,
            to: TxKind::Call(PROBE_ADDR),
            value: U256::ZERO,
            input: Bytes::from(input.to_vec()),
            ..Default::default()
        }),
        Signature::test_signature(),
    )
}

/// 构造一个 Prague-shape 区块,块号 = `number`,含 `txs` / `senders`。
/// Header 字段镜像 `difftx_exec.rs::build_block` 的 Prague 分支。
fn build_block(
    number: u64,
    txs: Vec<TransactionSigned>,
    senders: Vec<Address>,
) -> RecoveredBlock<Block> {
    assert_eq!(txs.len(), senders.len(), "txs 与 senders 必须等长");
    let header = Header {
        parent_hash: B256::ZERO,
        beneficiary: Address::ZERO,
        number,
        timestamp: BLOCK_TIMESTAMP,
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        mix_hash: B256::ZERO,
        nonce: BEACON_NONCE.into(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
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

// ————————————————————————————————— 读 exec-time slot ————————————————————————————————— //

/// 从 `BundleState` 读探针合约 slot `n` 的 `present_value`,转成 `B256`。
/// `None` = 探针未写该 slot（即 `BLOCKHASH` 返回 0,SSTORE 写 0 被 revm 裁剪 / 未写）。
fn read_probe_slot(bundle: &BundleState, slot: u64) -> Option<B256> {
    bundle
        .state
        .get(&PROBE_ADDR)
        .and_then(|acc| acc.storage.get(&U256::from(slot)))
        .map(|s| B256::from(s.present_value.to_be_bytes::<32>()))
}

/// 确定性伪 hash（喂给 `block_hashes` 的规范值）。
fn fake_hash(number: u64) -> B256 {
    let mut b = [0u8; 32];
    b[0] = 0x9a;
    b[24..32].copy_from_slice(&number.to_be_bytes());
    B256::from(b)
}

// ————————————————————————————————— exec-time 一致性核心 ————————————————————————————————— //

/// 一次 exec-time `BLOCKHASH` 一致性观测结果（单个目标高度）。
#[derive(Debug, Clone)]
struct BlockhashObs {
    target: u64,
    serial: Option<B256>,
    grevm: Option<B256>,
    expected: Option<B256>,
}

/// 在块号 `number` 处,对每个 `targets[i]` 发一笔探针交易读 `BLOCKHASH(target)` 并存进
/// slot=target。两侧真跑串行 / grevm,返回逐目标的观测。
///
/// `expected`：喂入 `CacheDB.block_hashes` 的 number→hash（也是不变式①的期望值);
/// 未出现在 `expected` 里的 target 表示「预期 EVM 返回 0」（如 diff==0 或 >256）。
fn run_blockhash_case(
    number: u64,
    targets: &[u64],
    expected: &BTreeMap<u64, B256>,
) -> Vec<BlockhashObs> {
    // 每个 target 一个独立 sender（nonce=0）,便于 grevm 并行；探针合约一份。
    let mut seed = vec![contract(PROBE_ADDR, PROBE_CODE, 0)];
    let mut txs = Vec::new();
    let mut senders = Vec::new();
    for (i, &t) in targets.iter().enumerate() {
        let sender = Address::with_last_byte(0x40 + i as u8);
        seed.push(eoa(sender, HUGE_BALANCE, 0));
        txs.push(probe_call_tx(0, t));
        senders.push(sender);
    }

    let chain_spec = prague_chain_spec();
    let block = build_block(number, txs, senders);
    let cache = seeded_cache(&seed, expected);

    let mut serial = fresh_serial_executor(chain_spec.clone(), cache.clone());
    let mut grevm = fresh_grevm_executor(chain_spec, cache);

    let so = serial.execute(&block).expect("串行执行整块应成功（BLOCKHASH 命中已 seed 的窗口）");
    let go = grevm.execute(&block).expect("grevm 执行整块应成功");

    targets
        .iter()
        .map(|&t| BlockhashObs {
            target: t,
            serial: read_probe_slot(&so.state, t),
            grevm: read_probe_slot(&go.state, t),
            expected: expected.get(&t).copied(),
        })
        .collect()
}

// ————————————————————————————————— 测试 ————————————————————————————————— //

#[cfg(all(test, feature = "difftx_2935"))]
mod tests {
    use super::*;
    use gravity_storage::block_view_storage::{BlockHashError, RawBlockViewProvider};
    use reth_db::mock::TxMock;
    use reth_revm::DatabaseRef as RethDatabaseRef;

    /// 承重的 **exec-time 一致性** 测试（不变式①）。
    ///
    /// 在块号 `N=300` 处,对多个 in-window 高度发探针交易读 `BLOCKHASH`,断言:
    /// (a) 串行 == grevm（逐字节）;(b) 二者都 == 喂入的规范 hash。
    /// 覆盖窗口边界:`N-1`（最新可读）、`N-2`、`N-128`（中段）、`N-256`（最老 in-window）。
    ///
    /// 这证明该差分预言机确实把探针块过了**两个后端**、且 exec-time `BLOCKHASH` 一致——
    /// 因此任何 serial⟷grevm 的 `BLOCKHASH` 背离都会被抓住。
    #[test]
    fn exec_time_blockhash_serial_equals_grevm_and_expected() {
        let n: u64 = 300;
        let in_window = [n - 1, n - 2, n - 128, n - 256];
        let mut expected = BTreeMap::new();
        for &t in &in_window {
            expected.insert(t, fake_hash(t));
        }

        let obs = run_blockhash_case(n, &in_window, &expected);
        assert_eq!(obs.len(), in_window.len());
        for o in &obs {
            println!(
                "[exec-time BLOCKHASH] N={n} target={} serial={:?} grevm={:?} expected={:?}",
                o.target, o.serial, o.grevm, o.expected
            );
            // (a) serial == grevm（共识安全的核心要求）。
            assert_eq!(
                o.serial, o.grevm,
                "target={} 的 exec-time BLOCKHASH 串行⟷grevm 背离（疑似共识 fork）",
                o.target
            );
            // (b) == 喂入的规范 hash。
            assert_eq!(
                o.serial, o.expected,
                "target={} 的 exec-time BLOCKHASH 与喂入的规范 hash 不符",
                o.target
            );
            assert!(o.serial.is_some(), "target={} 应读到非零 BLOCKHASH", o.target);
        }
    }

    /// 边界 sanity（不变式①的补充）：
    /// - `BLOCKHASH(N)`（diff==0,当前块）→ revm 返回 0（`host.rs:169`）。
    /// - `BLOCKHASH(N-257)`（diff>256,超窗）→ revm 返回 0（`host.rs:183`）。
    ///
    /// 两侧都应「未写 slot / 写 0」,且串行==grevm。用于确认预言机对 revm 的
    /// BLOCKHASH 窗口语义把握正确（否则不变式①的正例也不可信）。
    #[test]
    fn exec_time_blockhash_out_of_window_is_zero_both_backends() {
        let n: u64 = 300;
        let targets = [n, n - 257]; // diff==0 与 diff>256
        let expected = BTreeMap::new(); // 期望都为 0（不 seed）

        let obs = run_blockhash_case(n, &targets, &expected);
        for o in &obs {
            println!(
                "[exec-time BLOCKHASH oob] N={n} target={} serial={:?} grevm={:?}",
                o.target, o.serial, o.grevm
            );
            assert_eq!(o.serial, o.grevm, "target={} 超窗读取串行⟷grevm 背离", o.target);
            // SSTORE 0 会被 revm 裁剪(present==original==0),故 slot 不出现或为 0。
            assert!(
                o.serial.is_none() || o.serial == Some(B256::ZERO),
                "target={} 超窗应读到 0,实得 {:?}",
                o.target,
                o.serial
            );
        }
    }

    /// **截断窗口 `BlockTooOld`（存储层,真实证据）**（不变式②）。
    ///
    /// 用**生产结构体** `RawBlockViewProvider`（`block_view_storage/mod.rs`）+ 一份**截断**
    /// 的 `block_number_to_id`（只 51 对,<256）复现 warm-restart 场景:
    /// - 环 = `{250..=300}`（51 对,模拟 checkpoint-resume 只供给最近 51 块）。
    /// - `BLOCKHASH(44)`（在合法 256 窗内:`300-256=44`,但**不在**截断环内）→ `BlockTooOld`。
    ///
    /// 存储层**直接返回 `Err`（不 panic）**;这里用 `Result` 捕获并断言 `BlockTooOld` 变体,
    /// **在真实证据上确认**了 red-team 记录的前置条件（<256 对 → 最老 in-window 读 too-old）。
    /// 生产里这条 `Err` 会沿 `host.block_hash → None → FatalExternalError halt →
    /// executor.execute 返回 Err → lib.rs:1354 panic!` 上抛（见下一测试的进程内后果链）。
    #[test]
    fn truncated_window_blocktooold_storage_level() {
        // 截断环:只 51 对（<256），模拟 warm-restart 供给不足。
        let mut window: BTreeMap<u64, B256> = BTreeMap::new();
        for number in 250u64..=300 {
            window.insert(number, fake_hash(number));
        }
        assert!(window.len() < 256, "构造前提:窗口必须 <256 对");
        let map = Arc::new(std::sync::Mutex::new(window));

        // 生产结构体,真实 block_hash_ref 路径。
        let provider = RawBlockViewProvider::new(TxMock::default(), None, map);

        // (1) 截断环内最老命中 → Ok。
        assert_eq!(
            RethDatabaseRef::block_hash_ref(&provider, 250).unwrap(),
            fake_hash(250),
            "截断环内应命中"
        );

        // (2) 合法 256 窗内、但落在截断环之下(300-256=44 < 250) → BlockTooOld。
        let oldest_in_256 = 300u64 - 256; // = 44
        let err = RethDatabaseRef::block_hash_ref(&provider, oldest_in_256)
            .expect_err("截断窗口下最老 in-256 读取必须 Err(BlockTooOld)");
        let downcast = err.downcast_other_ref::<BlockHashError>().expect("Err 应为 BlockHashError");
        println!("[truncated-window 存储层] BLOCKHASH({oldest_in_256}) => {downcast}");
        assert!(
            matches!(downcast, BlockHashError::BlockTooOld(n) if *n == oldest_in_256),
            "预期 BlockTooOld({oldest_in_256})，实得 {downcast:?}"
        );

        // (3) 对照:> max 仍是 BlockTooHigh（证明判定分支正确、not 一刀切）。
        let too_high = RethDatabaseRef::block_hash_ref(&provider, 301)
            .expect_err("超过 max 应 Err(BlockTooHigh)");
        assert!(
            matches!(
                too_high.downcast_other_ref::<BlockHashError>().unwrap(),
                BlockHashError::BlockTooHigh(301)
            ),
            "预期 BlockTooHigh(301)"
        );
    }

    /// **exec-time halt 后果链（进程内,复刻件驱动）**（不变式②的后果）。
    ///
    /// 用一个 `DatabaseRef`（`TruncatedWindowErrDb`,忠实复刻 `block_hash_ref` 的窗口判定并
    /// 在「截断环内缺失」时返回 `Err`）作为执行器后端,发一笔读 `BLOCKHASH(too_old)` 的探针
    /// 交易,断言 `executor.execute(&block)` **返回 `Err`**（即生产 `lib.rs:1354`
    /// `unwrap_or_else(|err| panic!(...))` 会 panic 的那个 `Err`）。
    ///
    /// 这证明「截断窗口 → `BlockTooOld` → opcode halt → 整块执行失败」的后果链在真实
    /// 执行器上成立。**边界**:此处用复刻 `DatabaseRef`（真实 `RawBlockViewProvider` 需真实
    /// reth DB `TX`,不在本单元 harness 内可得);把它换成真实 provider 并用截断
    /// `block_number_to_id` 种子驱动,需 `pipe_test` / gnode 集群 warm-restart——见模块头。
    #[test]
    fn truncated_window_exec_halt_returns_err() {
        use revm::database::DBErrorMarker;

        /// 复刻件:截断环内缺失 → `Err`（可触发 opcode halt）。
        #[derive(Debug)]
        struct TruncatedWindowErrDb {
            window: BTreeMap<u64, B256>,
        }

        /// 与 `BlockHashError` 同形的、可进 revm/reth 错误通道的错误。
        #[derive(Debug)]
        struct TooOld(u64);
        impl std::fmt::Display for TooOld {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "block {} too old (truncated window replica)", self.0)
            }
        }
        impl std::error::Error for TooOld {}
        impl DBErrorMarker for TooOld {}

        impl revm::DatabaseRef for TruncatedWindowErrDb {
            type Error = TooOld;
            fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
                Ok(EmptyDB::default().basic_ref(address).unwrap())
            }
            fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
                Ok(EmptyDB::default().code_by_hash_ref(code_hash).unwrap())
            }
            fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
                Ok(EmptyDB::default().storage_ref(address, index).unwrap())
            }
            /// 复刻 `block_view_storage.rs`:191-202 判定:环内命中 → Ok;缺失 → `BlockTooOld` → Err。
            fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
                match self.window.get(&number) {
                    Some(h) => Ok(*h),
                    None => Err(TooOld(number)),
                }
            }
        }

        // 截断环:只 51 对(<256)。
        let mut window: BTreeMap<u64, B256> = BTreeMap::new();
        for number in 250u64..=300 {
            window.insert(number, fake_hash(number));
        }

        // 用 CacheDB 包裹截断 DB,上层 seed 探针合约 + 一个 sender。
        let ext = TruncatedWindowErrDb { window };
        let mut cache: CacheDB<TruncatedWindowErrDb> = CacheDB::new(ext);
        cache.insert_account_info(PROBE_ADDR, contract(PROBE_ADDR, PROBE_CODE, 0).info);
        let sender = Address::with_last_byte(0x77);
        cache.insert_account_info(sender, eoa(sender, HUGE_BALANCE, 0).info);

        let chain_spec = prague_chain_spec();
        // 读 BLOCKHASH(44)：合法 256 窗内(300-256=44)但不在截断环 → too old → halt。
        let block = build_block(300, vec![probe_call_tx(0, 44)], vec![sender]);

        let evm_config = EthEvmConfig::new(chain_spec);
        let executor: BasicBlockExecutor<EthEvmConfig, CacheDB<TruncatedWindowErrDb>> =
            BasicBlockExecutor::new(evm_config, cache);

        // `Executor::execute` 消费 self（见 evm/execute.rs:66）。
        let res = reth_evm::execute::Executor::execute(executor, &block);
        println!("[truncated-window exec halt] executor.execute => is_err={}", res.is_err());
        assert!(
            res.is_err(),
            "截断窗口下读最老 in-256 BLOCKHASH,整块执行应失败(FatalExternalError)——\
             生产中即 lib.rs:1354 的 panic!"
        );
    }
}
