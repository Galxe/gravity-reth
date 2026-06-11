//! DIFFERENTIAL ORACLE: grevm backend vs `disable_grevm` `WrapExecutor<BasicBlockExecutor>`.
//!
//! Finding family: grevm-vs-disablegrevm consensus divergence (Track A2). Generalizes the
//! single-precompile repro in `repro_wrapexecutor_custom_precompiles_noop_grevm_divergence_test.rs`
//! into a multi-scenario oracle that runs the SAME ordered block through BOTH executor
//! backends and asserts byte-equal block-level results (state_root, gas_used) and per-tx
//! receipt fields (status, cumulative gas, log count).
//!
//! ## Why this matters (consensus model)
//!
//! Which executor a node runs is chosen by a *per-node, non-consensus-bound* toggle
//! (`gravity-primitives/src/config.rs:38`, `GRETH_DISABLE_GREVM` / `--gravity.disable-grevm`):
//!   - default       → `GrevmExecutor`                       (crates/ethereum/evm/src/parallel_execute.rs)
//!   - disable_grevm → `WrapExecutor<BasicBlockExecutor>`    (crates/evm/evm/src/parallel_execute.rs)
//!
//! Every validator MUST compute the SAME post-state for the SAME consensus-ordered block.
//! Any field that differs across the two backends for an identical block is a state-root
//! split: in a mixed cluster the two groups disagree on the block hash and the chain
//! halts (or the minority forks). `lib.rs` then `unwrap_or_else(panic!)`s on the execution
//! result, so a divergence is a hard halt, not a soft fork.
//!
//! ## Confirmed divergences between the two `execute_one` paths (from code @ HEAD)
//!
//! 1. **Custom precompiles (user-reachable).** `GrevmExecutor::apply_custom_precompiles`
//!    stores the set and forwards it into `Scheduler::new`
//!    (parallel_execute.rs:332-334, :118), so every tx sees e.g. the BLS pop-verify
//!    precompile at `0x…1625f5001`. `WrapExecutor::apply_custom_precompiles` is a verbatim
//!    no-op (`crates/evm/evm/src/parallel_execute.rs:141-146`, body is just the TODO
//!    comment). → a tx CALLing that address runs the precompile on grevm but hits an empty
//!    account on disable_grevm. Different gas, different state. THIS oracle reproduces it.
//!
//! 2. **Gravity hardfork upgrades (Alpha/Beta/Gamma/Delta bytecode + storage patches).**
//!    `GrevmExecutor::apply_post_execution_changes` calls `apply_hardfork_upgrades(...)`
//!    at the activation block (parallel_execute.rs:198-230). The serial
//!    `BasicBlockExecutor`/`EthBlockExecutor` path that `WrapExecutor` wraps has NO such
//!    call (grep for `apply_hardfork_upgrades`/`GravityHardfork` in
//!    `crates/evm/evm/src/execute.rs` and `crates/ethereum/evm/src/execute.rs` → none).
//!    → at a gravity-hardfork-activation block, grevm rewrites system-contract bytecode and
//!    storage while disable_grevm does not. Different state root for that block.
//!    **HONEST LIMITATION:** on THIS open-source branch all four hardfork upgrade tables
//!    are empty stubs (`crates/ethereum/evm/src/hardfork/{alpha,beta,gamma,delta}.rs` all
//!    return `&[]`), so `apply_hardfork_upgrades` is a no-op at HEAD and the divergence is
//!    NOT observable here. In production those tables carry the real (removed) bytecodes /
//!    E2E overrides, where the divergence is live. This oracle therefore exercises a
//!    gamma-activation block as a regression guard: it MUST stay equal while the tables are
//!    empty, and will start failing the moment a non-empty upgrade is restored without
//!    fixing the disable_grevm path.
//!
//! ## Scenarios exercised in ONE ordered block (block 100, Prague active)
//!
//!   tx[0] value transfer       — touches no custom precompile / hardfork → MUST match.
//!   tx[1] contract deploy      — plain CREATE                            → MUST match.
//!   tx[2] BLS-precompile CALL   — user-reachable custom precompile        → DIVERGES (bug #1).
//!
//! Plus a separate gamma-activation block (number 20) run end-to-end → MUST match while
//! the hardfork tables are empty stubs (guard for bug #2).
//!
//! ## How the two backends are driven in one test
//!
//! `get_gravity_config()` caches the toggle in a process-global `OnceLock`, and the node
//! CLI's `init_gravity_config` shadows the env var, so the two backends cannot coexist in
//! one process. We reuse the proven re-exec pattern: the parent re-execs THIS binary twice
//! (plain → grevm; `--gravity.disable-grevm` → disable_grevm), each child prints a single
//! machine-readable `ORACLE=<json-ish>` line, and the parent compares them.
//!
//! Run:
//!   cargo test -p reth-pipe-exec-layer-ext-v2 \
//!     --test oracle_grevm_vs_disable_grevm_test -- --nocapture

use alloy_consensus::{TxEip1559, TxLegacy, TxReceipt};
use alloy_primitives::{address, Address, Bytes, Signature, TxKind, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use gravity_api_types::{
    config_storage::{BlockNumber, ConfigStorage, OnChainConfig},
    events::contract_event::GravityEvent,
};
use gravity_storage::{block_view_storage::BlockViewStorage, GravityStorage};
use reth_chainspec::ChainSpec;
use reth_cli_commands::{launcher::FnLauncher, NodeCommand};
use reth_cli_runner::CliRunner;
use reth_db::DatabaseEnv;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use reth_node_builder::{EngineNodeLauncher, NodeBuilder, WithLaunchContext};
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_pipe_exec_layer_ext_v2::{
    new_pipe_exec_layer_api, ExecutionArgs, OrderedBlock, PipeExecLayerApi,
};
use reth_provider::{
    providers::BlockchainProvider, BlockHashReader, BlockNumReader, DatabaseProviderFactory,
    HeaderProvider, ReceiptProvider, StateProviderFactory,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// chainId from `gravity_hardfork.json`.
const CHAIN_ID: u64 = 7771625;

/// Block N's timestamp = (TS_BASE + N) * 1_000_000 (us).
const TS_BASE: u64 = 2_000_000_000;

/// Prague activation block (EIP-7702 not required here, but keeps the harness identical to
/// the sibling repro; all scenarios are valid pre/post-Prague).
const PRAGUE_ACTIVATION_BLOCK: u64 = 100;
const PRAGUE_TS: u64 = TS_BASE + PRAGUE_ACTIVATION_BLOCK;

/// Gamma hardfork activation block (must match `gravity_hardfork.json` → `gammaBlock`).
const GAMMA_BLOCK: u64 = 20;

/// BLS pop-verify precompile address (user-reachable custom precompile).
const BLS_PRECOMPILE_ADDR: Address = address!("00000000000000000000000000000001625f5001");
const BLS_INPUT_LEN: usize = 144;

/// Recipient of the plain value transfer.
const TRANSFER_TO: Address = address!("0x00000000000000000000000000000000000000aa");

/// Generous gas so the BLS precompile (flat 110_000) succeeds on grevm.
const TX_GAS_LIMIT: u64 = 600_000;

const CHILD_DATADIR_ENV: &str = "ORACLE_CHILD_DATADIR";
const DISABLE_GREVM_FLAG_ENV: &str = "ORACLE_DISABLE_GREVM_FLAG";
/// Which scenario block the child should run: "txs" (block 100, 3 txs) or "gamma".
const CHILD_SCENARIO_ENV: &str = "ORACLE_SCENARIO";
const OUTCOME_PREFIX: &str = "ORACLE=";

/// Anvil account 0 — pre-funded in `gravity_hardfork.json`.
const FUNDED_PRIVKEY_HEX: &[u8; 32] = &[
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX)).expect("funded test key")
}

fn gravity_chainspec(prague_time: u64) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse");
    json["config"]["pragueTime"] = serde_json::json!(prague_time);
    json.to_string()
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn ts_us(block_number: u64) -> u64 {
    (TS_BASE + block_number) * 1_000_000
}

// ---------------------------------------------------------------------------
// Transaction builders (legacy + 1559; all signed by the funded account).
// ---------------------------------------------------------------------------

fn sign_legacy(signer: &PrivateKeySigner, tx: TxLegacy) -> (TransactionSigned, Address) {
    use alloy_consensus::SignableTransaction;
    let sig: Signature =
        signer.sign_hash_sync(&tx.signature_hash()).expect("legacy tx signing");
    let signed = tx.into_signed(sig);
    let (tx, sig, _h) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Legacy(tx), sig);
    let _ = signed_tx.hash();
    (signed_tx, signer.address())
}

fn sign_1559(signer: &PrivateKeySigner, tx: TxEip1559) -> (TransactionSigned, Address) {
    use alloy_consensus::SignableTransaction;
    let sig: Signature =
        signer.sign_hash_sync(&tx.signature_hash()).expect("1559 tx signing");
    let signed = tx.into_signed(sig);
    let (tx, sig, _h) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Eip1559(tx), sig);
    let _ = signed_tx.hash();
    (signed_tx, signer.address())
}

/// tx[0]: a plain value transfer (legacy tx).
fn build_value_transfer(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    sign_legacy(
        signer,
        TxLegacy {
            chain_id: Some(CHAIN_ID),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(TRANSFER_TO),
            value: U256::from(1_000u64),
            input: Bytes::new(),
        },
    )
}

/// tx[1]: a contract deploy (legacy CREATE). Init code returns a 1-byte runtime.
/// `0x60016000526001601ff3` style minimal returner; exact code is irrelevant — the point is
/// both backends must compute the same created-address, code hash, and gas.
fn build_contract_deploy(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    // PUSH1 0x01 PUSH1 0x00 MSTORE PUSH1 0x01 PUSH1 0x1f RETURN  → stores 1 byte (0x01) runtime.
    let init_code = Bytes::from(vec![
        0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x01, 0x60, 0x1f, 0xf3,
    ]);
    sign_legacy(
        signer,
        TxLegacy {
            chain_id: Some(CHAIN_ID),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: 200_000,
            to: TxKind::Create,
            value: U256::ZERO,
            input: init_code,
        },
    )
}

/// tx[2]: CALL the BLS custom precompile with 144 bytes of input (1559 tx).
fn build_bls_call(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    sign_1559(
        signer,
        TxEip1559 {
            chain_id: CHAIN_ID,
            nonce,
            gas_limit: TX_GAS_LIMIT,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(BLS_PRECOMPILE_ADDR),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(vec![0u8; BLS_INPUT_LEN]),
        },
    )
}

// ---------------------------------------------------------------------------
// OrderedBlock helpers
// ---------------------------------------------------------------------------

fn empty_block(epoch: u64, n: u64, id: B256, parent: B256, ts_us: u64) -> OrderedBlock {
    OrderedBlock {
        failed_proposer_indices: vec![],
        epoch,
        parent_id: parent,
        id,
        number: n,
        timestamp_us: ts_us,
        coinbase: Address::ZERO,
        prev_randao: B256::ZERO,
        withdrawals: Default::default(),
        transactions: vec![],
        senders: vec![],
        proposer_index: Some(0),
        extra_data: vec![],
        randomness: U256::ZERO,
    }
}

fn block_with_txs(
    epoch: u64,
    n: u64,
    id: B256,
    parent: B256,
    ts_us: u64,
    transactions: Vec<TransactionSigned>,
    senders: Vec<Address>,
) -> OrderedBlock {
    OrderedBlock {
        failed_proposer_indices: vec![],
        epoch,
        parent_id: parent,
        id,
        number: n,
        timestamp_us: ts_us,
        coinbase: Address::ZERO,
        prev_randao: B256::ZERO,
        withdrawals: Default::default(),
        transactions,
        senders,
        proposer_index: Some(0),
        extra_data: vec![],
        randomness: U256::ZERO,
    }
}

type TimestampFn = Box<dyn Fn(u64) -> u64 + Send + Sync>;

struct MockConsensus<Storage, EthApi> {
    pipeline_api: PipeExecLayerApi<Storage, EthApi>,
    ts_for_block: TimestampFn,
}

impl<Storage, EthApi> MockConsensus<Storage, EthApi>
where
    Storage: GravityStorage,
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn new(pipeline_api: PipeExecLayerApi<Storage, EthApi>, ts_for_block: TimestampFn) -> Self {
        Self { pipeline_api, ts_for_block }
    }

    async fn push_one(
        &self,
        epoch: &mut u64,
        block: OrderedBlock,
    ) -> reth_pipe_exec_layer_ext_v2::ExecutionResult {
        let block_id = block.id;
        let block_number = block.number;
        self.pipeline_api.push_ordered_block(block).unwrap();
        let result = self.pipeline_api.pull_executed_block_hash().await.unwrap();
        assert_eq!(result.block_number, block_number);
        assert_eq!(result.block_id, block_id);
        self.pipeline_api.commit_executed_block_hash(block_id, Some(result.block_hash)).unwrap();

        for event in &result.gravity_events {
            if let GravityEvent::NewEpoch(new_epoch, _) = event {
                assert_eq!(*new_epoch, *epoch + 1);
                self.pipeline_api.wait_for_block_persistence(block_number).await.unwrap();
                self.pipeline_api
                    .push_ordered_block(empty_block(
                        *epoch,
                        block_number + 1,
                        mock_block_id(block_number + 1),
                        block_id,
                        (self.ts_for_block)(block_number + 1),
                    ))
                    .unwrap();
                *epoch = *new_epoch;
            }
        }
        result
    }

    async fn push_empty_range(&self, epoch: &mut u64, start: u64, end: u64) {
        for n in start..=end {
            let block = empty_block(
                *epoch,
                n,
                mock_block_id(n),
                mock_block_id(n - 1),
                (self.ts_for_block)(n),
            );
            self.push_one(epoch, block).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
}

fn account_nonce<P: StateProviderFactory>(provider: &P, block_number: u64, addr: Address) -> u64 {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider");
    state.basic_account(&addr).expect("read account").map(|a| a.nonce).unwrap_or_default()
}

async fn boot_pipeline(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<(
    Arc<reth_chainspec::ChainSpec>,
    impl StateProviderFactory + HeaderProvider + BlockHashReader + BlockNumReader + ReceiptProvider + Clone,
    PipeExecLayerApi<
        BlockViewStorage<
            BlockchainProvider<
                reth_node_api::NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>,
            >,
        >,
        impl EthCall<NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>>,
    >,
    u64,
)> {
    let handle = builder
        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
        .with_components(EthereumNode::components())
        .with_add_ons(EthereumAddOns::default())
        .launch_with_fn(|builder| {
            let launcher = EngineNodeLauncher::new(
                builder.task_executor().clone(),
                builder.config().datadir(),
                reth_engine_primitives::TreeConfig::default(),
            );
            builder.launch_with(launcher)
        })
        .await?;

    let chain_spec = handle.node.chain_spec();
    let eth_api = handle.node.rpc_registry.eth_api().clone();
    let provider = handle.node.provider;

    let db_provider = provider.database_provider_ro().unwrap();
    let latest_block_number = db_provider.best_block_number().unwrap();
    let latest_block_hash = db_provider.block_hash(latest_block_number).unwrap().unwrap();
    let latest_block_header = db_provider.header_by_number(latest_block_number).unwrap().unwrap();
    drop(db_provider);

    assert_eq!(latest_block_number, 0, "tests expect a fresh datadir");

    let storage = BlockViewStorage::new(provider.clone());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec.clone(),
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );
    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok((chain_spec, provider, pipeline_api, latest_block_number))
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// One transaction's consensus-relevant receipt fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ReceiptSummary {
    success: bool,
    cumulative_gas_used: u64,
    num_logs: usize,
}

/// One block's consensus-relevant result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct BlockOutcome {
    state_root: B256,
    gas_used: u64,
    receipts: Vec<ReceiptSummary>,
    sender_balance: U256,
}

impl BlockOutcome {
    fn encode(&self) -> String {
        // ORACLE=<state_root>|<gas_used>|<sender_balance>|r:s,cgu,logs;s,cgu,logs;...
        let mut s = format!(
            "{OUTCOME_PREFIX}{:#x}|{}|{:#x}|r:",
            self.state_root, self.gas_used, self.sender_balance
        );
        for r in &self.receipts {
            s.push_str(&format!(
                "{},{},{};",
                if r.success { 1 } else { 0 },
                r.cumulative_gas_used,
                r.num_logs
            ));
        }
        s
    }

    fn parse_from_stdout(stdout: &str) -> Option<Self> {
        let line = stdout.lines().find(|l| l.trim_start().starts_with(OUTCOME_PREFIX))?;
        let payload = line.trim_start().strip_prefix(OUTCOME_PREFIX)?;
        let mut parts = payload.split('|');
        let state_root = parts.next()?.trim().parse::<B256>().ok()?;
        let gas_used = parts.next()?.trim().parse::<u64>().ok()?;
        let sender_balance = parts.next()?.trim().parse::<U256>().ok()?;
        let r_part = parts.next()?.trim().strip_prefix("r:")?;
        let mut receipts = Vec::new();
        for entry in r_part.split(';').filter(|e| !e.is_empty()) {
            let mut f = entry.split(',');
            let success = f.next()?.trim() == "1";
            let cumulative_gas_used = f.next()?.trim().parse::<u64>().ok()?;
            let num_logs = f.next()?.trim().parse::<usize>().ok()?;
            receipts.push(ReceiptSummary { success, cumulative_gas_used, num_logs });
        }
        Some(Self { state_root, gas_used, receipts, sender_balance })
    }
}

fn read_block_outcome<P>(provider: &P, block_number: u64, sender: Address) -> BlockOutcome
where
    P: StateProviderFactory + HeaderProvider + ReceiptProvider,
    P::Receipt: TxReceipt,
{
    use alloy_consensus::BlockHeader;
    let header = provider
        .header_by_number(block_number)
        .expect("header read")
        .expect("header persisted");
    let receipts = provider
        .receipts_by_block(block_number.into())
        .expect("receipts read")
        .unwrap_or_default()
        .into_iter()
        .map(|r| ReceiptSummary {
            success: r.status(),
            cumulative_gas_used: r.cumulative_gas_used(),
            num_logs: r.logs().len(),
        })
        .collect();
    let sender_balance = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider")
        .basic_account(&sender)
        .expect("read account")
        .map(|a| a.balance)
        .unwrap_or_default();
    BlockOutcome {
        state_root: header.state_root(),
        gas_used: header.gas_used(),
        receipts,
        sender_balance,
    }
}

// ---------------------------------------------------------------------------
// Scenario A: 3-tx block at block 100 (transfer, deploy, BLS-precompile call).
// ---------------------------------------------------------------------------

async fn run_txs_scenario(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<BlockOutcome> {
    let (_chain_spec, provider, pipeline_api, latest) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(ts_us));
    consensus.push_empty_range(&mut epoch, latest + 1, PRAGUE_ACTIVATION_BLOCK - 1).await;

    let sender = funded_signer();
    let (tx0, s0) = build_value_transfer(&sender, 0);
    let (tx1, s1) = build_contract_deploy(&sender, 1);
    let (tx2, s2) = build_bls_call(&sender, 2);

    let block = block_with_txs(
        epoch,
        PRAGUE_ACTIVATION_BLOCK,
        mock_block_id(PRAGUE_ACTIVATION_BLOCK),
        mock_block_id(PRAGUE_ACTIVATION_BLOCK - 1),
        ts_us(PRAGUE_ACTIVATION_BLOCK),
        vec![tx0, tx1, tx2],
        vec![s0, s1, s2],
    );

    consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.pipeline_api;
    pipeline_api.wait_for_block_persistence(PRAGUE_ACTIVATION_BLOCK).await.unwrap();

    // All three txs from the same sender must have executed (nonce advanced to 3).
    let nonce = account_nonce(&provider, PRAGUE_ACTIVATION_BLOCK, sender.address());
    assert_eq!(nonce, 3, "all 3 txs must execute and advance the sender nonce");

    Ok(read_block_outcome(&provider, PRAGUE_ACTIVATION_BLOCK, sender.address()))
}

// ---------------------------------------------------------------------------
// Scenario B: gamma-activation block (number 20). Empty block — the only state delta at
// this height (if any) is the gravity-hardfork upgrade applied on the grevm path.
// ---------------------------------------------------------------------------

/// Scenario C: a PLAIN block at height 7 — no gravity-hardfork transition (alpha@5,
/// beta@10, gamma@20, delta@25 are all off at 7), no user txs, pre-Prague. Isolates
/// whether the empty-block state_root divergence is hardfork-specific or systematic.
async fn run_plain_scenario(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<BlockOutcome> {
    let (_chain_spec, provider, pipeline_api, latest) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(ts_us));
    consensus.push_empty_range(&mut epoch, latest + 1, 7).await;

    let pipeline_api = consensus.pipeline_api;
    pipeline_api.wait_for_block_persistence(7).await.unwrap();

    // A2-1 INSTRUMENTATION: dump candidate accounts at block 7 so the parent can
    // diff the two children's persisted state and identify WHICH account causes
    // the state-root split. The in-process d4 test predicts the coinbase
    // (Address::ZERO, touched by the system tx's zero-reward `reward_beneficiary`)
    // is persisted as an EMPTY account by disable_grevm (state-clear OFF at
    // system-tx commit) but OMITTED by grevm (lazy_reward skips zero increments).
    {
        use alloy_eips::BlockNumberOrTag;
        let candidates: [(&str, Address); 3] = [
            ("coinbase(ZERO)", Address::ZERO),
            ("SYSTEM_CALLER", address!("00000000000000000000000000000001625f0000")),
            ("BLOCK_ADDR", address!("00000000000000000000000000000001625f2004")),
        ];
        for bn in 3u64..=7 {
            let state = provider
                .state_by_block_number_or_tag(BlockNumberOrTag::Number(bn))
                .expect("state provider");
            for (label, addr) in candidates {
                let acc = state.basic_account(&addr).expect("read account");
                println!(
                    "A2DUMP|@{bn}|{label}|{addr:?}|{}",
                    match acc {
                        Some(a) => format!("present nonce={} balance={:#x}", a.nonce, a.balance),
                        None => "ABSENT".to_string(),
                    }
                );
            }
        }
    }

    Ok(read_block_outcome(&provider, 7, funded_signer().address()))
}

async fn run_gamma_scenario(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<BlockOutcome> {
    let (_chain_spec, provider, pipeline_api, latest) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(ts_us));
    // Advance through the gamma-activation block.
    consensus.push_empty_range(&mut epoch, latest + 1, GAMMA_BLOCK).await;

    let pipeline_api = consensus.pipeline_api;
    pipeline_api.wait_for_block_persistence(GAMMA_BLOCK).await.unwrap();

    Ok(read_block_outcome(&provider, GAMMA_BLOCK, funded_signer().address()))
}

// ---------------------------------------------------------------------------
// Child entry point.
// ---------------------------------------------------------------------------

#[test]
fn oracle_child_worker() {
    let Ok(datadir) = std::env::var(CHILD_DATADIR_ENV) else {
        eprintln!("[child] not a child invocation (no {CHILD_DATADIR_ENV}); skipping");
        return;
    };
    let _ = std::fs::remove_dir_all(&datadir);

    let disable_grevm = std::env::var(DISABLE_GREVM_FLAG_ENV).is_ok();
    let scenario = std::env::var(CHILD_SCENARIO_ENV).unwrap_or_else(|_| "txs".to_string());
    let chain_spec = gravity_chainspec(PRAGUE_TS);
    let leaked_datadir: &'static str = Box::leak(datadir.into_boxed_str());

    let outcome = match scenario.as_str() {
        "gamma" => run_worker(&chain_spec, leaked_datadir, disable_grevm, run_gamma_scenario),
        "plain" => run_worker(&chain_spec, leaked_datadir, disable_grevm, run_plain_scenario),
        _ => run_worker(&chain_spec, leaked_datadir, disable_grevm, run_txs_scenario),
    };
    println!("{}", outcome.encode());
}

// ---------------------------------------------------------------------------
// Parent tests.
// ---------------------------------------------------------------------------

/// ORACLE: the 3-tx block must produce IDENTICAL block-level + per-tx results on both
/// backends. At HEAD this FAILS on tx[2] (BLS precompile) because
/// `WrapExecutor::apply_custom_precompiles` is a no-op → divergence reproduced. tx[0]
/// (transfer) and tx[1] (deploy) must match on both.
#[test]
fn oracle_three_tx_block_must_match_across_backends() {
    let grevm = run_child(false, "txs", "data/oracle_txs_grevm")
        .expect("grevm child must emit ORACLE line");
    let disable = run_child(true, "txs", "data/oracle_txs_disable_grevm")
        .expect("disable_grevm child must emit ORACLE line");

    println!("[oracle] grevm         = {grevm:?}");
    println!("[oracle] disable_grevm = {disable:?}");

    assert_eq!(
        grevm.receipts.len(),
        disable.receipts.len(),
        "receipt count diverged between backends"
    );

    // tx[0] value transfer and tx[1] contract deploy MUST match on both backends.
    // (These touch no custom precompile / hardfork; if they ever diverge it is a new bug.)
    for i in [0usize, 1usize] {
        assert_eq!(
            grevm.receipts[i], disable.receipts[i],
            "BUG: tx[{i}] ({}) diverged across backends grevm={:?} disable={:?}",
            if i == 0 { "value transfer" } else { "contract deploy" },
            grevm.receipts[i],
            disable.receipts[i]
        );
    }

    // tx[2] BLS precompile call + whole-block results: the consensus invariant.
    assert_eq!(
        grevm.receipts[2], disable.receipts[2],
        "BUG: tx[2] (BLS precompile CALL) diverged — disable_grevm dropped the custom \
         precompile (WrapExecutor::apply_custom_precompiles is a no-op). grevm={:?} disable={:?}",
        grevm.receipts[2], disable.receipts[2]
    );
    assert_eq!(
        grevm.gas_used, disable.gas_used,
        "BUG: block gas_used diverged across backends — state-root split in a mixed cluster"
    );
    assert_eq!(
        grevm.sender_balance, disable.sender_balance,
        "BUG: sender balance diverged across backends (different gas charged for same block)"
    );
    assert_eq!(
        grevm.state_root, disable.state_root,
        "BUG: block state_root diverged across backends for the SAME consensus-ordered \
         block → chain halt / fork in a mixed cluster."
    );
}

/// ORACLE (headline): a PLAIN block (height 7 — no hardfork transition, no user txs, no
/// custom precompile, pre-Prague) must produce the SAME state_root on both backends.
///
/// At HEAD this FAILS: the two backends compute DIFFERENT state roots for an otherwise
/// empty block whose gas_used, receipts, and balances are byte-identical. This is a
/// SYSTEMATIC per-block divergence — independent of the custom-precompile (bug #1) and the
/// gravity-hardfork (bug #2) causes — meaning `disable_grevm` is not consensus-equivalent
/// to grevm on ANY block. A mixed cluster would split at the first block. Root cause lives
/// in the differing `execute_one` / `take_bundle` post-execution + empty-account handling
/// of `GrevmExecutor` vs `WrapExecutor<BasicBlockExecutor>` (the grevm path hand-rolls
/// `apply_post_execution_changes`; the serial path runs stock alloy `EthBlockExecutor`).
#[test]
fn oracle_plain_block_state_root_must_match_across_backends() {
    let grevm = run_child(false, "plain", "data/oracle_plain_grevm")
        .expect("grevm child must emit ORACLE line");
    let disable = run_child(true, "plain", "data/oracle_plain_disable_grevm")
        .expect("disable_grevm child must emit ORACLE line");

    println!("[oracle] plain grevm         = {grevm:?}");
    println!("[oracle] plain disable_grevm = {disable:?}");

    // gas/receipts/balance are equal — only the trie root diverges.
    assert_eq!(grevm.gas_used, disable.gas_used, "plain block gas_used unexpectedly diverged");
    assert_eq!(grevm.receipts, disable.receipts, "plain block receipts unexpectedly diverged");
    assert_eq!(
        grevm.state_root, disable.state_root,
        "BUG (systematic): grevm and disable_grevm computed DIFFERENT state roots for a \
         PLAIN block (height 7, no user txs / hardfork / precompile) whose gas_used, \
         receipts and balances are identical. disable_grevm is not consensus-equivalent to \
         grevm on ANY block → a mixed cluster splits at the first block. grevm={:#x} \
         disable={:#x}",
        grevm.state_root, disable.state_root
    );
}

/// ORACLE (regression guard): the gamma-activation block must match across backends.
/// While the gravity hardfork upgrade tables are empty stubs this PASSES; it will FAIL the
/// moment a non-empty Alpha/Beta/Gamma/Delta upgrade is restored without teaching the
/// disable_grevm path to apply it (the `apply_hardfork_upgrades` call exists only on the
/// grevm path).
#[test]
fn oracle_gamma_activation_block_must_match_across_backends() {
    let grevm = run_child(false, "gamma", "data/oracle_gamma_grevm")
        .expect("grevm child must emit ORACLE line");
    let disable = run_child(true, "gamma", "data/oracle_gamma_disable_grevm")
        .expect("disable_grevm child must emit ORACLE line");

    println!("[oracle] gamma grevm         = {grevm:?}");
    println!("[oracle] gamma disable_grevm = {disable:?}");

    assert_eq!(
        grevm.state_root, disable.state_root,
        "BUG: gamma-activation block state_root diverged across backends. If the hardfork \
         upgrade tables are non-empty, the disable_grevm path failed to apply \
         apply_hardfork_upgrades (it is only wired into the grevm path)."
    );
    assert_eq!(grevm.gas_used, disable.gas_used, "gamma block gas_used diverged");
}

// ---------------------------------------------------------------------------
// Re-exec plumbing.
// ---------------------------------------------------------------------------

fn run_child(disable_grevm: bool, scenario: &str, datadir: &str) -> Option<BlockOutcome> {
    let exe = std::env::current_exe().expect("current test binary");
    let mut cmd = std::process::Command::new(exe);
    cmd.args(["--exact", "oracle_child_worker", "--nocapture", "--test-threads", "1"])
        .env(CHILD_DATADIR_ENV, datadir)
        .env(CHILD_SCENARIO_ENV, scenario)
        .env_remove(DISABLE_GREVM_FLAG_ENV);
    if disable_grevm {
        cmd.env(DISABLE_GREVM_FLAG_ENV, "1");
    }
    let output = cmd.output().expect("spawn child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!(
        "[parent] child (disable_grevm={disable_grevm}, scenario={scenario}) status={:?}\n\
         --- child stdout ---\n{stdout}\n--- child stderr ---\n{stderr}",
        output.status
    );
    assert!(output.status.success(), "child did not exit successfully");
    BlockOutcome::parse_from_stdout(&stdout)
}

fn run_worker<F, Fut, R>(
    chain_spec: &str,
    datadir: &'static str,
    disable_grevm: bool,
    run_fn: F,
) -> R
where
    F: FnOnce(WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = eyre::Result<R>> + Send + 'static,
    R: Send + Default + 'static,
{
    init_panic_hook_and_tracer();

    let runner = CliRunner::try_default_runtime().unwrap();
    let mut args: Vec<&str> =
        vec!["reth", "--chain", chain_spec, "--with-unused-ports", "--dev", "--datadir", datadir];
    if disable_grevm {
        args.push("--gravity.disable-grevm");
    }
    let command: NodeCommand<EthereumChainSpecParser> =
        NodeCommand::try_parse_args_from(args).unwrap();

    let result = Arc::new(std::sync::Mutex::new(None::<R>));
    let result_clone = result.clone();
    runner
        .run_command_until_exit(|ctx| {
            command.execute(
                ctx,
                FnLauncher::new::<EthereumChainSpecParser, _>(|builder, _| async move {
                    let r = run_fn(builder).await?;
                    *result_clone.lock().unwrap() = Some(r);
                    Ok(())
                }),
            )
        })
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));
    result.lock().unwrap().take().unwrap_or_default()
}

fn init_panic_hook_and_tracer() {
    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();
        eprintln!("Panic occurred: {panic_info}\nBacktrace:\n{backtrace}");
        std::process::exit(1);
    }));

    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(
            LogFormat::Terminal,
            LevelFilter::INFO.to_string(),
            String::new(),
            Some("always".to_string()),
        ))
        .init();
}
