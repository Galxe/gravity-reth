//! Gas-limit regression test for the BLS pop-verify precompile (gravity-audit#678).
//!
//! The BLS precompile `0x…1625f5001` is registered in the **user-transaction**
//! `custom_precompiles` set and charges a flat `POP_VERIFY_GAS = 110_000` for any
//! input. Before the fix, the handler returned `Ok(gas_used = 110_000)` without
//! checking the gas forwarded by the caller; the alloy-evm dispatcher then ran
//! `assert!(record_cost(110_000))`, and when the forwarded gas was below the flat
//! charge, `record_cost` returned false → assert panic → deterministic abort of
//! block execution. Any funded account could trigger a network-wide halt with a
//! single under-gassed transaction (and restart-replay re-panics).
//!
//! This test submits a transaction calling the BLS precompile with a `gas_limit`
//! above the EIP-7702 intrinsic floor (so it is not dropped by
//! `filter_invalid_txs`) but leaving less than 110_000 gas forwarded to the
//! precompile:
//!   - Before the fix: executing the block panics (the panic hook calls `process::exit(1)`,
//!     aborting the test).
//!   - After the fix: the precompile returns `OutOfGas`, the dispatcher takes the normal
//!     PrecompileOOG branch, the tx is included as a failed transaction (sender nonce advances),
//!     the block executes cleanly, and the chain keeps producing blocks.
//!
//! The harness mirrors `gravity_eip7702_test.rs` (under Cargo's
//! tests-as-binaries model each test binary is intentionally self-contained and
//! does not share a `tests/common` module).

use alloy_consensus::TxEip7702;
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
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
// Constants
// ---------------------------------------------------------------------------

/// chainId from `gravity_hardfork.json`.
const CHAIN_ID: u64 = 7771625;

/// Block N's timestamp = (P3_TS_BASE + N) * 1_000_000 (us).
/// `pragueTime = P3_TS_BASE + P3_ACTIVATION_BLOCK` activates Prague exactly at
/// block 100 (the EIP-7702 transaction type requires Prague).
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;
const PRAGUE_TS_BLOCK_100: u64 = P3_TS_BASE + P3_ACTIVATION_BLOCK;

/// BLS pop-verify precompile address (`onchain_config/mod.rs:BLS_PRECOMPILE_ADDR`).
const BLS_PRECOMPILE_ADDR: Address = address!("00000000000000000000000000000001625f5001");

/// Input length the precompile expects: pubkey(48) + pop(96) = 144 bytes. Must be
/// exactly 144 bytes to pass the handler's length check and reach the flat-charge
/// `Ok` branch (the contents need not be valid).
const BLS_INPUT_LEN: usize = 144;

/// Poison tx gas_limit: above the EIP-7702 intrinsic floor (21_000 base +
/// 25_000/auth + calldata, ~46.6k, so it is not dropped by filter_invalid_txs),
/// yet after intrinsic the gas forwarded to the precompile (~53k) is below
/// POP_VERIFY_GAS=110_000 → triggers the pre-fix panic.
const POISON_GAS_LIMIT: u64 = 100_000;

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

/// Delegation target for the authorization tuple (only there to make the 7702 tx
/// valid; irrelevant to triggering the bug).
const TARGET_ADDR: Address = address!("0x0000000000000000000000000000000000001234");

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX))
        .expect("funded test key must parse")
}

fn authority_signer(seed: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[31] = seed;
    PrivateKeySigner::from_bytes(&B256::from(bytes)).expect("authority key must parse")
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn p3_ts_us(block_number: u64) -> u64 {
    (P3_TS_BASE + block_number) * 1_000_000
}

fn sign_authorization(
    signer: &PrivateKeySigner,
    chain_id: u64,
    target: Address,
    nonce: u64,
) -> SignedAuthorization {
    let auth = Authorization { chain_id: U256::from(chain_id), address: target, nonce };
    let sig: Signature =
        signer.sign_hash_sync(&auth.signature_hash()).expect("auth signing must succeed");
    auth.into_signed(sig)
}

/// Build and sign an EIP-7702 transaction calling `to`, returning the signed tx
/// and the sender address. The 7702 tx type is used only to reuse the existing
/// harness (Prague@100); the bug is triggered by the call to `to` (the BLS
/// precompile) itself, independent of the transaction type.
fn build_signed_eip7702_tx(
    sender: &PrivateKeySigner,
    nonce: u64,
    gas_limit: u64,
    to: Address,
    input: Bytes,
    authorization_list: Vec<SignedAuthorization>,
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
        authorization_list,
        input,
    };
    let sig_hash = tx.signature_hash();
    let signature: Signature = sender.sign_hash_sync(&sig_hash).expect("tx signing must succeed");
    let signed = tx.into_signed(signature);
    let (tx, sig, _hash) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Eip7702(tx), sig);
    let _ = signed_tx.hash();
    (signed_tx, sender.address())
}

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

/// Read an account's nonce at the given block height (via StateProviderFactory,
/// which is part of boot_pipeline's returned bound).
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
// Repro + regression: an under-gassed tx calling the BLS precompile must not
// panic block execution.
// ---------------------------------------------------------------------------

async fn run_bls_low_gas(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<B256> {
    let (_chain_spec, provider, pipeline_api, latest_block_number) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    // Advance to the block just before Prague activation (block 99).
    consensus.push_empty_range(&mut epoch, latest_block_number + 1, P3_ACTIVATION_BLOCK - 1).await;

    // Inject the poison tx at activation block 100: call the BLS precompile, 144
    // bytes of input, gas_limit=100_000. The gas_limit is above the 7702 intrinsic
    // floor (so it is not filtered), but after intrinsic the gas forwarded to the
    // precompile is < 110_000 → pre-fix this panics in the alloy-evm dispatcher.
    let sender = funded_signer();
    let authority = authority_signer(0x42);
    let auth = sign_authorization(&authority, CHAIN_ID, TARGET_ADDR, 0);
    let (tx, sender_addr) = build_signed_eip7702_tx(
        &sender,
        0,
        POISON_GAS_LIMIT,
        BLS_PRECOMPILE_ADDR,
        Bytes::from(vec![0u8; BLS_INPUT_LEN]),
        vec![auth],
    );

    let block = ordered_block_with_txs(
        epoch,
        P3_ACTIVATION_BLOCK,
        mock_block_id(P3_ACTIVATION_BLOCK),
        mock_block_id(P3_ACTIVATION_BLOCK - 1),
        p3_ts_us(P3_ACTIVATION_BLOCK),
        vec![tx],
        vec![sender_addr],
    );

    // Pre-fix: the line below panics (panic hook → process::exit(1), aborting the
    // test). Post-fix: the precompile returns OutOfGas and the block executes
    // cleanly with no panic.
    let result = consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();
    println!(
        "[bls_test] ✅ block with the low-gas BLS precompile call executed without panic: {:?}",
        result.block_hash
    );

    // Positive confirmation: the poison tx actually executed (was not dropped by
    // the filter) — the sender nonce advances to 1. An OOG tx is an "included but
    // failed" tx, so the sender is charged gas and the nonce increments.
    let nonce = account_nonce(&provider, P3_ACTIVATION_BLOCK, sender_addr);
    assert_eq!(nonce, 1, "poison tx must execute as a failed tx and advance the sender nonce");

    // Prove the chain keeps producing blocks: push one more empty block.
    let mut epoch_after = epoch;
    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    consensus
        .push_empty_range(&mut epoch_after, P3_ACTIVATION_BLOCK + 1, P3_ACTIVATION_BLOCK + 1)
        .await;
    println!("[bls_test] ✅ chain advanced past the poison tx.");

    use alloy_consensus::BlockHeader;
    let state_root = provider
        .header_by_number(P3_ACTIVATION_BLOCK)
        .expect("header read")
        .expect("header persisted")
        .state_root();
    Ok(state_root)
}

#[test]
fn test_bls_precompile_low_gas_no_panic_grevm() {
    let state_root = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_bls_precompile_grevm_test",
        false,
        run_bls_low_gas,
    );
    println!("[bls_test] grevm state root = {state_root:?}");
}

#[test]
fn test_bls_precompile_low_gas_no_panic_disable_grevm() {
    let state_root = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_bls_precompile_disable_grevm_test",
        true,
        run_bls_low_gas,
    );
    println!("[bls_test] disable_grevm state root = {state_root:?}");
}

// ---------------------------------------------------------------------------
// Test harness — boots the node CLI (mirrors gravity_eip7702_test.rs).
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
