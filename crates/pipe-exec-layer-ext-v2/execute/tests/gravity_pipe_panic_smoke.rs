#![allow(missing_docs)]

//! Pipe-layer panic-smoke test — gravity-audit#710 acceptance bar.
//!
//! Constructs an `OrderedBlock` carrying one valid tx of each EIP envelope (legacy /
//! EIP-2930 / EIP-1559 / EIP-7702) alongside malformed variants of each known
//! `revm::InvalidTransaction` panic trigger (EIP-4844 empty blobs, EIP-3860 oversized
//! init code, EIP-7702 empty auth list, EIP-7702 sub-floor intrinsic gas,
//! `max_fee < base_fee`, `priority > max_fee`, and chain-id mismatch) and pushes it
//! through `PipeExecLayerApi::push_ordered_block`. The acceptance bar — per the
//! user's framing on audit#710 — is **the pipe must not panic**: any panic from
//! `lib.rs:1060` is converted to `process::exit(1)` by the test harness's panic
//! hook, so the test fails by aborting the process.
//!
//! ## Scope and limits
//!
//! The smoke test guards the executor's panic surface, not the per-variant filter
//! contract. Most malformed txs in this batch use throw-away signers (zero balance)
//! and would be rejected by the filter's "sender does not exist" branch *before*
//! reaching the gap-specific checks — which is fine for the panic-smoke claim, but
//! means this file alone does not prove "each gap check is the gate". Use the
//! per-gap unit tests in `crates/pipe-exec-layer-ext-v2/execute/src/tx_filter.rs`
//! (`test_filter_invalid_txs_*`) for that contract — they exercise the gap checks
//! in isolation against `MockDatabase`. Together: UTs pin the per-variant contract,
//! this smoke test pins the integration boundary against future regressions where
//! filter changes accidentally let a malformed tx reach grevm.
//!
//! Harness (`boot_pipeline` / `MockConsensus` / `run_pipe_e2e_test` /
//! `init_panic_hook_and_tracer`) is duplicated from `gravity_eip7702_test.rs` —
//! Cargo treats each `tests/*.rs` as a separate binary, so a sibling-module
//! refactor would couple the test binaries. Dedup is tracked as future cleanup.

use alloy_consensus::{TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy};
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
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
    HeaderProvider, StateProviderFactory,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Constants — mirrored from gravity_eip7702_test.rs (intentional duplication).
// ---------------------------------------------------------------------------

const CHAIN_ID: u64 = 7771625;
const P3_TS_BASE: u64 = 2_000_000_000;

/// Push the smoke block at block 1 (not 100) — Prague is activated at this same
/// block so the 7702 surface is reachable, but we avoid spending 99 empty-block
/// rounds boot-warming the chain just to get there.
const ACTIVATION_BLOCK: u64 = 1;
const PRAGUE_TS_ACTIVATION: u64 = P3_TS_BASE + ACTIVATION_BLOCK;

const FUNDED_PRIVKEY_HEX: &[u8; 32] = &[
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];
const TARGET_ADDR: Address = address!("0x0000000000000000000000000000000000001234");

fn gravity_prague_chainspec(prague_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = prague_time {
        json["config"]["pragueTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX))
        .expect("funded test key must parse")
}

fn authority_signer(seed: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[31] = seed;
    PrivateKeySigner::from_bytes(&B256::from(bytes)).expect("authority key must parse")
}

/// Deterministic throw-away signer for malformed-tx senders. Zero balance — the
/// filter rejects them on the "sender does not exist" branch, fine for smoke.
fn throwaway_signer(seed: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[0] = 0xaa;
    bytes[31] = seed;
    PrivateKeySigner::from_bytes(&B256::from(bytes)).expect("throwaway key must parse")
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn p3_ts_us(block_number: u64) -> u64 {
    (P3_TS_BASE + block_number) * 1_000_000
}

// ---------------------------------------------------------------------------
// Signing helpers — mirrored from gravity_eip7702_test.rs.
// ---------------------------------------------------------------------------

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

fn finalize<T>(tx: T, signer: &PrivateKeySigner) -> (TransactionSigned, Address)
where
    T: alloy_consensus::SignableTransaction<Signature>
        + alloy_consensus::transaction::RlpEcdsaEncodableTx,
    Transaction: From<T>,
{
    let sig_hash = tx.signature_hash();
    let signature: Signature = signer.sign_hash_sync(&sig_hash).expect("tx signing must succeed");
    let signed = tx.into_signed(signature);
    let (inner, sig, _hash) = signed.into_parts();
    let outer: Transaction = inner.into();
    let signed_tx = TransactionSigned::new_unhashed(outer, sig);
    let _ = signed_tx.hash();
    (signed_tx, signer.address())
}

// ---------------------------------------------------------------------------
// Per-envelope tx builders — one valid variant of each envelope, then malformed
// variants of every revm `InvalidTransaction` trigger gated by `tx_filter`.
// ---------------------------------------------------------------------------

fn build_valid_legacy(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    let tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

fn build_valid_2930(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    let tx = TxEip2930 {
        chain_id: CHAIN_ID,
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

fn build_valid_1559(signer: &PrivateKeySigner, nonce: u64) -> (TransactionSigned, Address) {
    let tx = TxEip1559 {
        chain_id: CHAIN_ID,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

fn build_valid_7702(
    signer: &PrivateKeySigner,
    nonce: u64,
    auth: SignedAuthorization,
) -> (TransactionSigned, Address) {
    let tx = TxEip7702 {
        chain_id: CHAIN_ID,
        nonce,
        gas_limit: 200_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: Address::ZERO,
        value: U256::ZERO,
        access_list: Default::default(),
        authorization_list: vec![auth],
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#696 trigger 2 — type-3 (blob) ----------
fn build_malformed_4844_empty_blobs(signer: &PrivateKeySigner) -> (TransactionSigned, Address) {
    let tx = TxEip4844 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 100_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: TARGET_ADDR,
        value: U256::ZERO,
        access_list: Default::default(),
        blob_versioned_hashes: vec![],
        max_fee_per_blob_gas: 1,
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#696 trigger 4 — EIP-3860 oversized init code ----------
fn build_malformed_oversized_initcode(signer: &PrivateKeySigner) -> (TransactionSigned, Address) {
    use revm_primitives::eip3860::MAX_INITCODE_SIZE;
    let tx = TxEip1559 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 30_000_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: TxKind::Create,
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(vec![0u8; MAX_INITCODE_SIZE + 1]),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#710 gap 3 — 7702 empty authorization_list ----------
fn build_malformed_7702_empty_auth(signer: &PrivateKeySigner) -> (TransactionSigned, Address) {
    let tx = TxEip7702 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 100_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: Address::ZERO,
        value: U256::ZERO,
        access_list: Default::default(),
        authorization_list: vec![],
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#668 — 7702 below intrinsic gas floor ----------
fn build_malformed_7702_low_gas(signer: &PrivateKeySigner) -> (TransactionSigned, Address) {
    let auth = sign_authorization(signer, CHAIN_ID, TARGET_ADDR, 0);
    let tx = TxEip7702 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 21_001, // < 21_000 + 25_000 floor
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: Address::ZERO,
        value: U256::ZERO,
        access_list: Default::default(),
        authorization_list: vec![auth],
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#710 gap 1 — max_fee < base_fee ----------
fn build_malformed_1559_max_fee_below_base(
    signer: &PrivateKeySigner,
) -> (TransactionSigned, Address) {
    // Block base_fee on a freshly-booted devchain is ~1 gwei. Setting max_fee to 1
    // wei guarantees `max_fee < base_fee` if the filter ever forwards this to revm.
    let tx = TxEip1559 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 21_000,
        max_fee_per_gas: 1,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#710 gap 2 — priority > max ----------
fn build_malformed_1559_prio_gt_max(signer: &PrivateKeySigner) -> (TransactionSigned, Address) {
    let tx = TxEip1559 {
        chain_id: CHAIN_ID,
        nonce: 0,
        gas_limit: 21_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 5_000_000_000,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------- Malformed: audit#710 gap 5 — chain_id mismatch ----------
fn build_malformed_legacy_wrong_chain_id(
    signer: &PrivateKeySigner,
) -> (TransactionSigned, Address) {
    let tx = TxLegacy {
        chain_id: Some(99), // != CHAIN_ID (7771625)
        nonce: 0,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(TARGET_ADDR),
        value: U256::ZERO,
        input: Bytes::new(),
    };
    finalize(tx, signer)
}

// ---------------------------------------------------------------------------
// OrderedBlock helpers — mirrored from gravity_eip7702_test.rs.
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

    fn into_inner(self) -> PipeExecLayerApi<Storage, EthApi> {
        self.pipeline_api
    }
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
// Boot path.
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
// The smoke test body.
// ---------------------------------------------------------------------------

async fn run_pipe_panic_smoke(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<B256> {
    let (_chain_spec, provider, pipeline_api, _) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();
    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));

    // --- Valid txs from the funded signer (sequential nonces 0..3). All four
    // envelopes are exercised so a regression in any tx-type decoding panics us.
    let funded = funded_signer();
    let authority = authority_signer(0x42);
    let auth = sign_authorization(&authority, CHAIN_ID, TARGET_ADDR, 0);
    let (legacy_tx, legacy_sender) = build_valid_legacy(&funded, 0);
    let (eip2930_tx, eip2930_sender) = build_valid_2930(&funded, 1);
    let (eip1559_tx, eip1559_sender) = build_valid_1559(&funded, 2);
    let (eip7702_tx, eip7702_sender) = build_valid_7702(&funded, 3, auth);

    // --- Malformed variants from throwaway signers (zero balance — filter
    // rejects them on the "sender does not exist" branch; that's fine for
    // smoke, the per-gap UTs prove the specific check fires).
    let mal_4844 = build_malformed_4844_empty_blobs(&throwaway_signer(0x01));
    let mal_initcode = build_malformed_oversized_initcode(&throwaway_signer(0x02));
    let mal_7702_empty = build_malformed_7702_empty_auth(&throwaway_signer(0x03));
    let mal_7702_lowgas = build_malformed_7702_low_gas(&throwaway_signer(0x04));
    let mal_low_fee = build_malformed_1559_max_fee_below_base(&throwaway_signer(0x05));
    let mal_prio_high = build_malformed_1559_prio_gt_max(&throwaway_signer(0x06));
    let mal_chain_id = build_malformed_legacy_wrong_chain_id(&throwaway_signer(0x07));

    let txs = vec![
        legacy_tx,
        eip2930_tx,
        eip1559_tx,
        eip7702_tx,
        mal_4844.0,
        mal_initcode.0,
        mal_7702_empty.0,
        mal_7702_lowgas.0,
        mal_low_fee.0,
        mal_prio_high.0,
        mal_chain_id.0,
    ];
    let senders = vec![
        legacy_sender,
        eip2930_sender,
        eip1559_sender,
        eip7702_sender,
        mal_4844.1,
        mal_initcode.1,
        mal_7702_empty.1,
        mal_7702_lowgas.1,
        mal_low_fee.1,
        mal_prio_high.1,
        mal_chain_id.1,
    ];

    let block = ordered_block_with_txs(
        epoch,
        ACTIVATION_BLOCK,
        mock_block_id(ACTIVATION_BLOCK),
        mock_block_id(ACTIVATION_BLOCK - 1),
        p3_ts_us(ACTIVATION_BLOCK),
        txs,
        senders,
    );

    // The acceptance bar: we reach here without panicking. push_one drives the
    // tx through `filter_invalid_txs` → `parallel_executor.execute(&block)` →
    // `lib.rs:1060`. Any `EVMError` that leaks past the filter into grevm
    // triggers the bare `panic!` at `lib.rs:1067`; the harness's panic hook
    // converts that to `process::exit(1)`, so the test fails by aborting.
    let result = consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(ACTIVATION_BLOCK).await.unwrap();

    println!(
        "[panic_smoke] ✅ mixed-tx batch executed without panic at block {ACTIVATION_BLOCK}: hash={:?}",
        result.block_hash
    );
    let state_root = read_state_root(&provider, ACTIVATION_BLOCK);
    println!("[panic_smoke] ✅ state_root={state_root:?}");
    Ok(state_root)
}

#[test]
fn test_pipe_panic_smoke_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_ACTIVATION)),
        "data/gravity_pipe_panic_smoke_grevm",
        false,
        run_pipe_panic_smoke,
    );
}

#[test]
fn test_pipe_panic_smoke_disable_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_ACTIVATION)),
        "data/gravity_pipe_panic_smoke_disable_grevm",
        true,
        run_pipe_panic_smoke,
    );
}

// ---------------------------------------------------------------------------
// Harness — mirrored from gravity_eip7702_test.rs.
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
