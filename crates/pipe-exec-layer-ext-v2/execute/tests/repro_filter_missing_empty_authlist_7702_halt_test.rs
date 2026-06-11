#![allow(missing_docs)]

//! REPRO: tx_filter admits an EIP-7702 tx with an EMPTY authorization_list,
//! revm aborts the block, and `execute_ordered_block` panics -> chain halt.
//!
//! Finding id: filter-missing-empty-authlist-7702-halt (severity: high)
//!
//! ## The bug
//!
//! `filter_invalid_txs` (crates/pipe-exec-layer-ext-v2/execute/src/tx_filter.rs)
//! is documented (tx_filter.rs:1-5) as the last-line guard that mirrors the
//! pool-side checks so that non-pool-injected (consensus-side mempool) txs
//! cannot reach grevm in a state that makes the executor panic.
//!
//! For an EIP-7702 (type 0x04) transaction the filter only checks:
//!   (a) the pre-Prague gate (tx.is_eip7702() && !PRAGUE -> reject, tx_filter.rs:90-98)
//!   (b) intrinsic gas via calculate_initial_tx_gas with the auth-list COUNT
//!       (tx_filter.rs:102-112) -> for an empty list the count is 0, so the
//!       intrinsic floor collapses to the flat 21_000 and any gas_limit >= 21_000
//!       passes
//!   (c) nonce, (d) balance.
//!
//! It NEVER checks that `authorization_list` is non-empty. revm
//! (Galxe/revm bfa048c, crates/handler/src/validation.rs:192-196) rejects a
//! Prague-active 7702 tx whose authorization_list length is 0 with
//! `InvalidTransaction::EmptyAuthorizationList`. grevm (3c09e7c,
//! scheduler.rs:540-552) turns that EVM validation error into a deterministic
//! `GrevmError` abort of the whole block (no sequential fallback for EvmError),
//! which `parallel_execute.rs:120-125` maps to
//! `BlockExecutionError::Internal(..)`. That `Err` reaches
//! `execute_ordered_block` (lib.rs:1060-1067) where
//! `executor.execute(&block).unwrap_or_else(|err| panic!(...))` panics the
//! process. Every validator executing this consensus-ordered block crashes
//! identically -> chain halt.
//!
//! ## Threat model
//!
//! An empty-auth-list 7702 tx is rejected by the reth RPC pool, but the
//! consensus-side mempool path that `tx_filter` exists to defend bypasses the
//! pool. Consensus orders the crafted tx into an `OrderedBlock`, the filter
//! lets it through, and grevm/revm abort -> panic.
//!
//! ## What this test does
//!
//! This is the empty-auth-list sibling of the existing P-12 test in
//! `gravity_eip7702_test.rs::run_p12_filter_discard`. P-12 used a LOW-GAS auth
//! tx (caught by the intrinsic-gas branch). Here the auth list is EMPTY and the
//! gas_limit is generous (200_000), so the intrinsic-gas branch does NOT fire
//! and the *only* thing that could save the node is an explicit empty-auth-list
//! guard -- which does not exist at HEAD.
//!
//! We boot the pipeline, advance to the Prague activation block, and inject an
//! `OrderedBlock` containing exactly one signed `TxEip7702` whose
//! `authorization_list == []`.
//!
//! EXPECTED (correct / post-fix) behaviour, which this test asserts:
//!   - `filter_invalid_txs` discards the empty-auth-list tx,
//!   - the block executes cleanly with no panic,
//!   - the chain keeps making progress (one more empty block commits).
//!
//! ACTUAL behaviour on buggy HEAD f39cdf39a:
//!   - the tx is NOT filtered, grevm/revm return `EmptyAuthorizationList`,
//!   - `execute_ordered_block` hits `panic!("failed to execute block ...")`,
//!   - the node's panic hook (`init_panic_hook_and_tracer`) calls
//!     `std::process::exit(1)` -> the test process aborts before the assertions
//!     are reached. The test therefore FAILS on HEAD (it never returns
//!     successfully), demonstrating the all-validator halt.

use alloy_consensus::TxEip7702;
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{address, Address, Bytes, Signature, B256, U256};
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
    HeaderProvider, StateProviderFactory,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Constants (mirror gravity_eip7702_test.rs so Prague fires at block 100)
// ---------------------------------------------------------------------------

const CHAIN_ID: u64 = 7771625;
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;
const PRAGUE_TS_BLOCK_100: u64 = P3_TS_BASE + P3_ACTIVATION_BLOCK;

fn gravity_prague_chainspec(prague_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = prague_time {
        json["config"]["pragueTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

/// Anvil account 0 — pre-funded in `gravity_hardfork.json`.
const FUNDED_PRIVKEY_HEX: &[u8; 32] = &[
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];
const FUNDED_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX))
        .expect("funded test key must parse")
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn p3_ts_us(block_number: u64) -> u64 {
    (P3_TS_BASE + block_number) * 1_000_000
}

// ---------------------------------------------------------------------------
// Tx builder — note: authorization_list is the EMPTY vec.
// ---------------------------------------------------------------------------

fn build_signed_eip7702_tx_empty_authlist(
    sender: &PrivateKeySigner,
    nonce: u64,
    gas_limit: u64,
    to: Address,
) -> (TransactionSigned, Address) {
    use alloy_consensus::SignableTransaction;

    let tx = TxEip7702 {
        chain_id: CHAIN_ID,
        nonce,
        gas_limit,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to,
        value: U256::ZERO,
        access_list: Default::default(),
        // The crux of the repro: an EMPTY authorization list. A syntactically
        // valid signed 7702 envelope with zero authorizations.
        authorization_list: Vec::<SignedAuthorization>::new(),
        input: Bytes::new(),
    };
    let sig_hash = tx.signature_hash();
    let signature: Signature = sender.sign_hash_sync(&sig_hash).expect("tx signing must succeed");
    let signed = tx.into_signed(signature);
    let (tx, sig, _hash) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Eip7702(tx), sig);
    let _ = signed_tx.hash();
    (signed_tx, sender.address())
}

// ---------------------------------------------------------------------------
// OrderedBlock helpers
// ---------------------------------------------------------------------------

fn empty_ordered_block(
    epoch: u64,
    block_number: u64,
    block_id: B256,
    parent_block_id: B256,
    timestamp_us: u64,
) -> OrderedBlock {
    OrderedBlock {
        failed_proposer_indices: vec![],
        epoch,
        parent_id: parent_block_id,
        id: block_id,
        number: block_number,
        timestamp_us,
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

fn ordered_block_with_txs(
    epoch: u64,
    block_number: u64,
    block_id: B256,
    parent_block_id: B256,
    timestamp_us: u64,
    transactions: Vec<TransactionSigned>,
    senders: Vec<Address>,
) -> OrderedBlock {
    OrderedBlock {
        failed_proposer_indices: vec![],
        epoch,
        parent_id: parent_block_id,
        id: block_id,
        number: block_number,
        timestamp_us,
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
                    .push_ordered_block(empty_ordered_block(
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
            let block = empty_ordered_block(
                *epoch,
                n,
                mock_block_id(n),
                mock_block_id(n - 1),
                (self.ts_for_block)(n),
            );
            self.push_one(epoch, block).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn into_inner(self) -> PipeExecLayerApi<Storage, EthApi> {
        self.pipeline_api
    }
}

// ---------------------------------------------------------------------------
// State assertions
// ---------------------------------------------------------------------------

fn assert_no_authority_code<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    authority: Address,
) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for absent-code check");
    let code_len = state
        .account_code(&authority)
        .expect("read authority code")
        .map(|c| c.original_bytes().len())
        .unwrap_or(0);
    assert_eq!(
        code_len, 0,
        "authority {authority:?} must hold no code at block {block_number}, got {code_len}B"
    );
}

fn read_state_root<P: HeaderProvider>(provider: &P, block_number: u64) -> B256 {
    use alloy_consensus::BlockHeader;
    provider
        .header_by_number(block_number)
        .expect("header read must succeed")
        .expect("header must be persisted")
        .state_root()
}

// ---------------------------------------------------------------------------
// Boot path (mirrors gravity_eip7702_test.rs::boot_pipeline)
// ---------------------------------------------------------------------------

async fn boot_pipeline(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<(
    Arc<reth_chainspec::ChainSpec>,
    impl StateProviderFactory + HeaderProvider + BlockHashReader + BlockNumReader + Clone,
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
// The repro scenario.
// ---------------------------------------------------------------------------

async fn run_empty_authlist_repro(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<B256> {
    let (_chain_spec, provider, pipeline_api, latest_block_number) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    consensus.push_empty_range(&mut epoch, latest_block_number + 1, P3_ACTIVATION_BLOCK - 1).await;

    // Build a Prague-active block 100 with a single TxEip7702 whose
    // authorization_list is EMPTY but whose gas_limit (200_000) is well above
    // the flat 21_000 intrinsic floor that the filter computes for 0 auths.
    let sender = funded_signer();
    let (tx, sender_addr) = build_signed_eip7702_tx_empty_authlist(
        &sender,
        /* nonce = */ 0,
        /* gas_limit = */ 200_000,
        /* to = */ FUNDED_ADDR,
    );
    assert_eq!(sender_addr, FUNDED_ADDR, "funded signer must derive to anvil[0] address");

    let block = ordered_block_with_txs(
        epoch,
        P3_ACTIVATION_BLOCK,
        mock_block_id(P3_ACTIVATION_BLOCK),
        mock_block_id(P3_ACTIVATION_BLOCK - 1),
        p3_ts_us(P3_ACTIVATION_BLOCK),
        vec![tx],
        vec![sender_addr],
    );

    // On buggy HEAD, push_one -> pull_executed_block_hash never returns a clean
    // result because execute_ordered_block panics inside the pipeline service
    // (lib.rs:1060-1067) and the panic hook process::exit(1)s. So the next line
    // is where the test aborts on HEAD. After the fix, the filter drops the tx
    // and the block executes with zero user receipts.
    let result = consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();
    println!(
        "[repro_empty_authlist] block 100 executed WITHOUT panic: {:?}",
        result.block_hash
    );

    // The empty-auth-list tx must have been discarded by the filter: no state
    // change attributable to it (sender holds no designator code, etc.).
    assert_no_authority_code(&provider, P3_ACTIVATION_BLOCK, FUNDED_ADDR);

    // Prove the chain keeps making progress past the activation block.
    let mut epoch_after = epoch;
    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    consensus
        .push_empty_range(&mut epoch_after, P3_ACTIVATION_BLOCK + 1, P3_ACTIVATION_BLOCK + 1)
        .await;
    println!("[repro_empty_authlist] chain progressed past activation block — no halt.");

    let state_root = read_state_root(&provider, P3_ACTIVATION_BLOCK);
    Ok(state_root)
}

#[test]
fn test_empty_authlist_7702_does_not_halt_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/repro_empty_authlist_7702_grevm_test",
        false,
        run_empty_authlist_repro,
    );
}

#[test]
fn test_empty_authlist_7702_does_not_halt_disable_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/repro_empty_authlist_7702_disable_grevm_test",
        true,
        run_empty_authlist_repro,
    );
}

// ---------------------------------------------------------------------------
// Test harness (mirrors gravity_eip7702_test.rs::run_pipe_e2e_test)
// ---------------------------------------------------------------------------

fn run_pipe_e2e_test<F, Fut, R>(
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
