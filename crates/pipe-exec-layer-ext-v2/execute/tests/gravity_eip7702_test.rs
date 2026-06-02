#![allow(missing_docs)]

//! Integration tests for EIP-7702 (set code for EOA) on the Gravity pipe-exec-layer.
//!
//! Drives `MockConsensus + PipeExecLayerApi` through an in-memory chainspec
//! built from `gravity_hardfork.json` with `pragueTime` patched to align Prague
//! activation with block 100 (`timestamp == 2_000_000_100`) — the same scheme
//! `gravity_eip2935_test.rs` uses. Each scenario pushes the first 99 blocks as
//! empty pre-Prague blocks and then injects a *tx-bearing* `OrderedBlock` 100
//! that exercises the 7702 codepath that the audit (gravity-audit#668) flagged.
//!
//! ## Scope
//!
//! Covers the highest-value cases from
//! `_local/drafts/eip-7702/EIP-7702-acceptance-tests-2026-06-01.md`:
//!
//! - **P-12** filter_invalid_txs end-to-end: a low-gas 7702 tx injected through `OrderedBlock` is
//!   discarded by `filter_invalid_txs` instead of panicking the executor at `lib.rs:1072`. Pins the
//!   gravity-audit#668 fix at the integration boundary.
//! - **P-13** positive control: a sufficient-gas 7702 tx with one valid authorization is included;
//!   the authority's account code becomes the 23-byte EIP-7702 designator `0xef0100 || target`.
//! - **P-14** executor variant smoke: P-13 is run twice — once with `disable_grevm = true`
//!   (`WrapExecutor<BasicBlockExecutor>`) and once with `false` (`GrevmExecutor`). Both paths must
//!   apply the authorization so the authority ends up holding the 23-byte designator. The spec's
//!   *stronger* claim ("byte-identical state roots") is not asserted here because this codebase is
//!   known to produce different block-level accumulators across executor variants — see
//!   `crates/ethereum/evm/src/parallel_execute.rs:446-449`, which delegates cross-executor
//!   equivalence to unit-level diff tests rather than e2e state-root comparison. Both state roots
//!   are printed for forensic inspection, but the test passes as long as the user-observable
//!   designator state is identical.
//!
//! Cases that primarily exercise upstream pool, revm, or grevm semantics
//! (P-1/P-3 pool fork-gate, P-5/P-6 multi-auth + chain_id==0, P-7 re-delegation,
//! P-8/P-9/P-10 CALL/DELEGATECALL/STATICCALL → designator, P-15..P-18 grevm
//! BlockSTM concurrency) are out of scope for this commit — the integration
//! contract proven here is "pipe-exec-layer-v2 + grevm reach the upstream 7702
//! engine without panicking and the upstream engine then runs to completion".

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

/// chainId from `gravity_hardfork.json`. Must match the chainspec the test
/// boots under, otherwise pool/executor reject 7702 txs with ChainIdMismatch.
const CHAIN_ID: u64 = 7771625;

/// Block N is pushed with `timestamp = (P3_TS_BASE + N) * 1_000_000` (us).
/// `pragueTime = P3_TS_BASE + P3_ACTIVATION_BLOCK = 2_000_000_100`, so block
/// 99 is the last pre-Prague tip and block 100 is the activation block.
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;

/// `pragueTime` aligned to the P-3 activation block — same value the EIP-2935
/// tests use. Passed through `gravity_prague_chainspec()` so the in-memory
/// chainspec activates Prague exactly at block 100.
const PRAGUE_TS_BLOCK_100: u64 = P3_TS_BASE + P3_ACTIVATION_BLOCK;

/// Build a Gravity chainspec JSON string by loading `gravity_hardfork.json`
/// at compile time and patching `config.pragueTime`. Mirrors the helper in
/// `gravity_eip2935_test.rs` — kept inline here so the 7702 tests stay
/// self-contained (a sibling `tests/common/mod.rs` would couple the test
/// binaries via Cargo's tests-as-binaries model).
///
/// - `Some(ts)` ⇒ Prague fires at `ts` (`ForkCondition::Timestamp(ts)`).
/// - `None` ⇒ field omitted ⇒ Prague never fires (`ForkCondition::Never`).
///
/// `chain_value_parser` (`crates/ethereum/cli/src/chainspec.rs`) accepts
/// in-memory JSON for `--chain`, so no tempfile / no IO overhead.
fn gravity_prague_chainspec(prague_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = prague_time {
        json["config"]["pragueTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

/// Anvil account 0 — pre-funded in `gravity_hardfork.json` (`0x2e51 ETH`).
/// Used as the tx sender (wallet_B / relayer) in every 7702 scenario.
const FUNDED_PRIVKEY_HEX: &[u8; 32] = &[
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];
const FUNDED_ADDR: Address = address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

/// Delegation target for the authorization — an arbitrary address. The
/// 7702 designator is `0xef0100 || target`; the target's code (or lack
/// thereof) only matters if the authority is subsequently CALLed.
const TARGET_ADDR: Address = address!("0x0000000000000000000000000000000000001234");

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX))
        .expect("funded test key must parse")
}

/// Deterministic non-funded EOA — used as the EIP-7702 authority. The seed
/// makes every test run hit the same address so failures reproduce.
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

// ---------------------------------------------------------------------------
// Signing helpers
// ---------------------------------------------------------------------------

/// Sign an EIP-7702 `Authorization { chain_id, address, nonce }` and wrap it
/// in `SignedAuthorization`. Mirrors the upstream pattern from
/// `crates/ethereum/node/tests/e2e/utils.rs:99-102`.
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

/// Build a TxEip7702 and sign it with `sender`. Returns the encoded
/// `TransactionSigned` plus the recovered sender (so the caller can populate
/// `OrderedBlock.senders` without going through the recovery codepath).
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
    let (tx, sig, hash) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Eip7702(tx), sig);
    // Persist the precomputed hash so downstream consumers see the same tx hash as the signer did.
    let _ = signed_tx.hash();
    debug_assert_eq!(*signed_tx.hash(), hash);
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

    /// Push a single ordered block and synchronously wait for execution.
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
                // Push a stale-epoch heartbeat so the pipeline's epoch state advances. Mirrors
                // the gravity_eip2935_test.rs pattern.
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

    /// Push a contiguous range of empty blocks `[start, end]`.
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

fn assert_designator<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    authority: Address,
    target: Address,
) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for authority code check");
    let code = state
        .account_code(&authority)
        .expect("read authority code")
        .expect("authority must hold designator code after 7702 tx");
    let bytes = code.original_bytes();
    assert_eq!(bytes.len(), 23, "designator must be 23 bytes, got {}", bytes.len());
    assert_eq!(&bytes[..3], &[0xef, 0x01, 0x00], "designator prefix mismatch");
    assert_eq!(&bytes[3..], target.as_slice(), "designator target mismatch");
    println!(
        "[eip7702_test] ✅ authority {authority:?} has designator → {target:?} at block {block_number}"
    );
}

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
    println!(
        "[eip7702_test] ✅ authority {authority:?} carries no code at block {block_number} (discard/no-fix path)"
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
// Common boot path
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

    // Give the service a moment to come online — mirrors gravity_eip2935_test.
    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok((chain_spec, provider, pipeline_api, latest_block_number))
}

// ---------------------------------------------------------------------------
// P-13 / P-14 — happy path: designator deployment via OrderedBlock injection.
// ---------------------------------------------------------------------------

async fn run_p13_happy_path(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<B256> {
    let (_chain_spec, provider, pipeline_api, latest_block_number) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));

    // Drive the chain through the pre-Prague window, stopping at block 99.
    consensus.push_empty_range(&mut epoch, latest_block_number + 1, P3_ACTIVATION_BLOCK - 1).await;

    // Build the activation block 100 with a single TxEip7702.
    let sender = funded_signer();
    let authority = authority_signer(0x42);
    let auth = sign_authorization(&authority, CHAIN_ID, TARGET_ADDR, /* nonce = */ 0);
    let (tx, sender_addr) = build_signed_eip7702_tx(
        &sender,
        /* nonce = */ 0,
        /* gas_limit = */ 200_000,
        /* to = */ authority.address(),
        Bytes::new(),
        vec![auth],
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
    let result = consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();

    println!(
        "[eip7702_test] ✅ P-13 activation block executed: block_hash = {:?}",
        result.block_hash
    );

    assert_designator(&provider, P3_ACTIVATION_BLOCK, authority.address(), TARGET_ADDR);
    let state_root = read_state_root(&provider, P3_ACTIVATION_BLOCK);
    println!("[eip7702_test] ✅ P-13 state root at block 100 = {state_root:?}");
    Ok(state_root)
}

#[test]
fn test_p13_happy_path_grevm() {
    let state_root = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip7702_p13_grevm_test",
        false,
        run_p13_happy_path,
    );
    println!("[eip7702_test] grevm state root = {state_root:?}");
}

#[test]
fn test_p13_happy_path_disable_grevm() {
    let state_root = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip7702_p13_disable_grevm_test",
        true,
        run_p13_happy_path,
    );
    println!("[eip7702_test] disable_grevm state root = {state_root:?}");
}

// ---------------------------------------------------------------------------
// P-12 — filter_invalid_txs end-to-end: low-gas 7702 tx is discarded.
//
// Before the gravity-audit#668 fix the executor would panic at lib.rs:1072
// with `Eip7702{intrinsic gas too low}` because filter_invalid_txs forwarded
// the tx unchanged. After the fix the filter computes the 7702-aware intrinsic
// floor itself and discards the tx, so the block executes cleanly with zero
// user receipts and the chain keeps advancing.
// ---------------------------------------------------------------------------

async fn run_p12_filter_discard(
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

    // Build the activation block 100 with a TxEip7702 whose gas_limit (21_001)
    // is below the Prague intrinsic floor (21_000 base + 25_000/auth).
    let sender = funded_signer();
    let authority = authority_signer(0x12);
    let auth = sign_authorization(&authority, CHAIN_ID, TARGET_ADDR, 0);
    let (tx, sender_addr) =
        build_signed_eip7702_tx(&sender, 0, 21_001, authority.address(), Bytes::new(), vec![auth]);

    let block = ordered_block_with_txs(
        epoch,
        P3_ACTIVATION_BLOCK,
        mock_block_id(P3_ACTIVATION_BLOCK),
        mock_block_id(P3_ACTIVATION_BLOCK - 1),
        p3_ts_us(P3_ACTIVATION_BLOCK),
        vec![tx],
        vec![sender_addr],
    );
    let result = consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();
    println!("[eip7702_test] ✅ P-12 block executed (no panic): {:?}", result.block_hash);

    // Authority must NOT have designator code — the tx never made it into the
    // executor, so `apply_eip7702_auth_list` could not run on this account.
    assert_no_authority_code(&provider, P3_ACTIVATION_BLOCK, authority.address());

    // Push one more empty block to prove the chain keeps making progress.
    let mut epoch_after = epoch;
    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    consensus
        .push_empty_range(&mut epoch_after, P3_ACTIVATION_BLOCK + 1, P3_ACTIVATION_BLOCK + 1)
        .await;
    println!("[eip7702_test] ✅ P-12 chain progressed past activation block.");

    let state_root = read_state_root(&provider, P3_ACTIVATION_BLOCK);
    Ok(state_root)
}

#[test]
fn test_p12_low_gas_eip7702_discarded_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip7702_p12_grevm_test",
        false,
        run_p12_filter_discard,
    );
}

#[test]
fn test_p12_low_gas_eip7702_discarded_disable_grevm() {
    let _ = run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip7702_p12_disable_grevm_test",
        true,
        run_p12_filter_discard,
    );
}

// ---------------------------------------------------------------------------
// Test harness — single entry point that boots the node CLI.
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
