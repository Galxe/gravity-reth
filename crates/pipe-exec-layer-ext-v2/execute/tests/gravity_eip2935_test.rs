#![allow(missing_docs)]

//! Integration tests for EIP-2935 (HISTORY_STORAGE) on the Gravity pipe-exec-layer.
//!
//! Boots a single reth node from `gravity_hardfork.json` patched with a per-test
//! `pragueTime` (via `gravity_prague_chainspec()` below) and drives
//! `MockConsensus + PipeExecLayerApi` to exercise the EIP-2935 activation path
//! described in `_local/drafts/eip-2935/EIP-2935-acceptance-tests-2026-06-01.md`.

use alloy_eips::{
    eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE},
    BlockId,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types_eth::{state::EvmOverrides, TransactionRequest};
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
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Number of blocks pushed during the pre-fork (P-1) scenario.
const PRE_FORK_BLOCK_COUNT: u64 = 50;

/// P-3: pragueTime is aligned so block 100 is the activation block.
///
/// Block N is pushed with `timestamp = P3_TS_BASE + N`, so block 99's ts equals
/// `pragueTime - 1` (pre-Prague) and block 100's ts equals `pragueTime` (transitions).
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;

/// pragueTime aligned to the P-3 activation block. Used by P-3 / P-4 / P-5 /
/// P-6 / P-7' / P-8' / P-16 — every test that needs the chain to cross the
/// Prague boundary within the deterministic timestamp range.
const PRAGUE_TS_BLOCK_100: u64 = P3_TS_BASE + P3_ACTIVATION_BLOCK;

/// "Prague never fires during the test" — far-future timestamp. Used by P-1
/// to verify the deployment branch stays dormant when `pragueTime` is set
/// (`ForkCondition::Timestamp(far_future)`).
const PRAGUE_TS_FAR_FUTURE: u64 = 9_999_999_999;

/// `pragueTime = 0` boundary case. Used by P-2 to verify
/// `transitions_at_timestamp(parent_ts=0, current_ts=0)` returns false
/// (`parent_ts < pragueTime` is `0 < 0` ⇒ false), so the deployment branch
/// is unreachable from genesis without an alloc preload.
const PRAGUE_TS_GENESIS: u64 = 0;

/// Build a Gravity chainspec JSON string by loading `gravity_hardfork.json`
/// at compile time and patching `config.pragueTime`.
///
/// - `Some(ts)` ⇒ `ForkCondition::Timestamp(ts)` — Prague fires at `ts`.
/// - `None` ⇒ field omitted ⇒ `ForkCondition::Never` — Prague never fires. Used by P-15 to verify
///   the missing-field path.
///
/// Returns an in-memory JSON string. `chain_value_parser`
/// (`crates/ethereum/cli/src/chainspec.rs`) accepts in-memory JSON for
/// `--chain`, so no tempfile is needed.
fn gravity_prague_chainspec(prague_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = prague_time {
        json["config"]["pragueTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn now_us(_block_number: u64) -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
}

fn p3_ts_us(block_number: u64) -> u64 {
    (P3_TS_BASE + block_number) * 1_000_000
}

fn new_ordered_block(
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

type TimestampFn = Box<dyn Fn(u64) -> u64 + Send + Sync>;

struct MockConsensus<Storage, EthApi> {
    pipeline_api: PipeExecLayerApi<Storage, EthApi>,
    target_block_count: u64,
    ts_for_block: TimestampFn,
}

impl<Storage, EthApi> MockConsensus<Storage, EthApi>
where
    Storage: GravityStorage,
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn new(
        pipeline_api: PipeExecLayerApi<Storage, EthApi>,
        target_block_count: u64,
        ts_for_block: TimestampFn,
    ) -> Self {
        Self { pipeline_api, target_block_count, ts_for_block }
    }

    /// Drive the pipeline through `target_block_count` blocks and return the
    /// `PipeExecLayerApi` so the caller can await persistence or perform other
    /// follow-up actions (e.g. P-6 needs `wait_for_block_persistence` before
    /// reading the sealed header's `parent_hash`).
    async fn run(self, latest_block_number: u64) -> PipeExecLayerApi<Storage, EthApi> {
        let Self { pipeline_api, target_block_count, ts_for_block } = self;
        let mut epoch: u64 = pipeline_api
            .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
            .unwrap()
            .try_into()
            .unwrap();
        println!(
            "[eip2935_test] latest_block_number={latest_block_number}, epoch={epoch}, target={target_block_count}"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;

        let target_block = latest_block_number + target_block_count;
        for block_number in latest_block_number + 1..=target_block {
            let block_id = mock_block_id(block_number);
            let parent_block_id = mock_block_id(block_number - 1);
            let timestamp_us = ts_for_block(block_number);
            pipeline_api
                .push_ordered_block(new_ordered_block(
                    epoch,
                    block_number,
                    block_id,
                    parent_block_id,
                    timestamp_us,
                ))
                .unwrap();
            let result = pipeline_api.pull_executed_block_hash().await.unwrap();
            assert_eq!(result.block_number, block_number);
            assert_eq!(result.block_id, block_id);
            pipeline_api.commit_executed_block_hash(block_id, Some(result.block_hash)).unwrap();

            // Mirror hardfork test: bump epoch on NewEpoch events so the pipeline stays live.
            for event in &result.gravity_events {
                if let GravityEvent::NewEpoch(new_epoch, _) = event {
                    assert_eq!(*new_epoch, epoch + 1);
                    pipeline_api.wait_for_block_persistence(block_number).await.unwrap();
                    pipeline_api
                        .push_ordered_block(new_ordered_block(
                            epoch,
                            block_number + 1,
                            mock_block_id(block_number + 1),
                            block_id,
                            ts_for_block(block_number + 1),
                        ))
                        .unwrap();
                    epoch = *new_epoch;
                }
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        println!("[eip2935_test] ✅ Pushed {target_block_count} blocks (target={target_block}).");
        pipeline_api
    }
}

// ---------------------------------------------------------------------------
// HISTORY_STORAGE state assertions
// ---------------------------------------------------------------------------

/// Assert that HISTORY_STORAGE_ADDRESS holds **no** code at `block_number`. The
/// pre-deployment baseline: deployment branch must not have fired, and the account
/// must observe an empty code payload.
fn assert_history_storage_absent<P: StateProviderFactory>(provider: &P, block_number: u64) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for HISTORY_STORAGE check");
    let code = state.account_code(&HISTORY_STORAGE_ADDRESS).expect("read account_code");
    let len = code.as_ref().map(|c| c.original_bytes().len()).unwrap_or(0);
    assert!(
        len == 0,
        "expected HISTORY_STORAGE_ADDRESS to be empty at block {block_number}, found {len}B of code"
    );
    println!(
        "[eip2935_test] ✅ HISTORY_STORAGE absent at block {block_number} (account_code = {:?})",
        code.is_none()
    );
}

/// Assert that HISTORY_STORAGE_ADDRESS has been deployed with the spec bytecode.
fn assert_history_storage_deployed<P: StateProviderFactory>(provider: &P, block_number: u64) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for HISTORY_STORAGE check");
    let code = state
        .account_code(&HISTORY_STORAGE_ADDRESS)
        .expect("read account_code")
        .expect("HISTORY_STORAGE_ADDRESS must hold code post-activation");
    let deployed = code.original_bytes();
    assert_eq!(
        deployed.as_ref(),
        HISTORY_STORAGE_CODE.as_ref(),
        "deployed HISTORY_STORAGE bytecode mismatch at block {block_number} ({}B vs expected {}B)",
        deployed.len(),
        HISTORY_STORAGE_CODE.len()
    );
    println!(
        "[eip2935_test] ✅ HISTORY_STORAGE deployed at block {block_number} ({}B match)",
        deployed.len()
    );
}

/// Assert that the HISTORY_STORAGE_ADDRESS account's nonce equals `expected` at
/// `block_number`. Used by P-4 to confirm that deployment did not re-fire on a
/// post-activation block (re-firing would not change the nonce *value* — the diff
/// always sets nonce=1 — but the assertion documents the invariant we depend on).
fn assert_history_account_nonce<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    expected: u64,
) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for HISTORY_STORAGE account read");
    let account = state
        .basic_account(&HISTORY_STORAGE_ADDRESS)
        .expect("read basic_account")
        .expect("HISTORY_STORAGE_ADDRESS account must exist post-activation");
    assert_eq!(
        account.nonce, expected,
        "HISTORY_STORAGE nonce mismatch at block {block_number}: got {}, expected {expected}",
        account.nonce
    );
    println!("[eip2935_test] ✅ nonce = {expected} at block {block_number}");
}

/// Assert that `HISTORY_STORAGE_ADDRESS[slot]` equals `expected` at `block_number`.
fn assert_history_slot_eq<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    slot: u64,
    expected: U256,
) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for HISTORY_STORAGE slot read");
    let actual = state
        .storage(HISTORY_STORAGE_ADDRESS, B256::from(U256::from(slot)))
        .expect("read storage")
        .unwrap_or(U256::ZERO);
    assert_eq!(
        actual, expected,
        "HISTORY_STORAGE slot {slot} mismatch at block {block_number}: got {actual}, expected {expected}"
    );
    println!("[eip2935_test] ✅ slot {slot} = {expected} at block {block_number}");
}

// ---------------------------------------------------------------------------
// P-1: Pre-fork — no deployment, no slot writes
// ---------------------------------------------------------------------------

async fn run_pipe_pre_fork(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    println!("[eip2935_test] genesis latest_block_number={latest_block_number}");
    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    let consensus = MockConsensus::new(pipeline_api, PRE_FORK_BLOCK_COUNT, Box::new(now_us));
    consensus.run(latest_block_number).await;

    // pragueTime = 9999999999 is far in the future, so no block triggers
    // `transitions_at_timestamp` and the deployment branch never fires.
    let tip = latest_block_number + PRE_FORK_BLOCK_COUNT;
    assert_history_storage_absent(&provider, tip);
    assert_history_storage_absent(&provider, latest_block_number + 1);
    assert_history_storage_absent(&provider, tip / 2);

    println!(
        "[eip2935_test] ✅ pre-fork: HISTORY_STORAGE absent across all sampled blocks (deployment branch never fired)."
    );
    Ok(())
}

#[test]
fn test_p1_pre_fork_no_deployment_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_FAR_FUTURE)),
        "data/gravity_eip2935_p1_grevm_test",
        false,
        run_pipe_pre_fork,
    );
}

#[test]
fn test_p1_pre_fork_no_deployment_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_FAR_FUTURE)),
        "data/gravity_eip2935_p1_disable_grevm_test",
        true,
        run_pipe_pre_fork,
    );
}

// ---------------------------------------------------------------------------
// P-3: Activation block — deployment + first SSTORE
// ---------------------------------------------------------------------------

async fn run_pipe_p3_activation(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-3 expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    // Push exactly 100 blocks with deterministic timestamps so block 100 == activation.
    let consensus = MockConsensus::new(pipeline_api, P3_ACTIVATION_BLOCK, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    // Sanity: block 99 must still be pre-Prague (no deployment).
    assert_history_storage_absent(&provider, P3_ACTIVATION_BLOCK - 1);

    // P-3 asserts at block 100:
    //   (a) HISTORY_STORAGE code matches HISTORY_STORAGE_CODE
    //   (b) slot 99 == mock_block_id(99)  (SystemCaller wrote parent_id into ring index N-1)
    //   (c) slot 100 == 0 (next block's SSTORE has not run yet)
    assert_history_storage_deployed(&provider, P3_ACTIVATION_BLOCK);
    assert_history_slot_eq(
        &provider,
        P3_ACTIVATION_BLOCK,
        P3_ACTIVATION_BLOCK - 1,
        U256::from(P3_ACTIVATION_BLOCK - 1),
    );
    assert_history_slot_eq(&provider, P3_ACTIVATION_BLOCK, P3_ACTIVATION_BLOCK, U256::ZERO);

    println!("[eip2935_test] ✅ P-3 activation: deployment + first SSTORE verified at block {P3_ACTIVATION_BLOCK}.");
    Ok(())
}

#[test]
fn test_p3_activation_block_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p3_grevm_test",
        false,
        run_pipe_p3_activation,
    );
}

#[test]
fn test_p3_activation_block_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p3_disable_grevm_test",
        true,
        run_pipe_p3_activation,
    );
}

// ---------------------------------------------------------------------------
// P-4: T+1 does NOT re-fire the deployment branch (idempotency from
//      transitions_at_timestamp, not from code-empty check)
// ---------------------------------------------------------------------------

async fn run_pipe_p4_post_activation(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-4 expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    // Push 101 blocks — one beyond P3_ACTIVATION_BLOCK — so block 101 has
    // parent_ts == pragueTime, ensuring `transitions_at_timestamp` returns false
    // (deployment branch must NOT re-fire) while the per-block SystemCaller still
    // does write slot 100.
    let target_blocks = P3_ACTIVATION_BLOCK + 1;
    let consensus = MockConsensus::new(pipeline_api, target_blocks, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    // Sanity: P-3 properties must still hold at block 100 (no regression from the
    // extra block).
    assert_history_storage_deployed(&provider, P3_ACTIVATION_BLOCK);
    assert_history_account_nonce(&provider, P3_ACTIVATION_BLOCK, 1);

    // P-4 asserts at block 101 (= T+1):
    //   (a) Code is still HISTORY_STORAGE_CODE (deployment branch did not wipe/re-deploy).
    //   (b) Nonce is still 1 (deployment branch did not re-fire and bump-or-reset).
    //   (c) slot 99 is preserved from block 100 (not wiped by a re-Created status diff).
    //   (d) slot 100 == mock_block_id(100) (SystemCaller per-block SSTORE wrote it,
    //       which proves the post-deployment account is reachable from the system
    //       call path and that the SystemCaller's own gating is independent of the
    //       deployment branch).
    let t_plus_1 = P3_ACTIVATION_BLOCK + 1;
    assert_history_storage_deployed(&provider, t_plus_1);
    assert_history_account_nonce(&provider, t_plus_1, 1);
    assert_history_slot_eq(
        &provider,
        t_plus_1,
        P3_ACTIVATION_BLOCK - 1,
        U256::from(P3_ACTIVATION_BLOCK - 1),
    );
    assert_history_slot_eq(
        &provider,
        t_plus_1,
        P3_ACTIVATION_BLOCK,
        U256::from(P3_ACTIVATION_BLOCK),
    );

    println!("[eip2935_test] ✅ P-4 T+1 idempotent: deployment branch did NOT re-fire at block {t_plus_1}.");
    Ok(())
}

#[test]
fn test_p4_no_redeploy_at_t_plus_1_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p4_grevm_test",
        false,
        run_pipe_p4_post_activation,
    );
}

#[test]
fn test_p4_no_redeploy_at_t_plus_1_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p4_disable_grevm_test",
        true,
        run_pipe_p4_post_activation,
    );
}

// ---------------------------------------------------------------------------
// P-5: Deployment idempotency — slot history is not erased across post-activation
//      blocks, and code stays equal to the spec bytecode.
// ---------------------------------------------------------------------------

async fn run_pipe_p5_idempotency(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-5 expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    // Push two blocks past activation so the SystemCaller has written slots
    // 99 (at block 100), 100 (at block 101), and 101 (at block 102).
    let target_blocks = P3_ACTIVATION_BLOCK + 2;
    let consensus = MockConsensus::new(pipeline_api, target_blocks, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    // P-5 asserts at block 102 (= T+2):
    //   (a) code at T+2 is still HISTORY_STORAGE_CODE (deployment branch did not
    //       re-fire and overwrite or clear it on any post-activation block).
    //   (b) nonce stays at 1 across the whole post-activation segment.
    //   (c) slot 99 still holds mock_block_id(99) — the value written on the
    //       activation block was not erased by the SSTOREs of blocks 101 / 102,
    //       which target different ring indices (100 / 101 respectively).
    //   (d) slot 100 == mock_block_id(100) (block 101 wrote it).
    //   (e) slot 101 == mock_block_id(101) (block 102 wrote it).
    let t_plus_2 = P3_ACTIVATION_BLOCK + 2;
    assert_history_storage_deployed(&provider, t_plus_2);
    assert_history_account_nonce(&provider, t_plus_2, 1);
    assert_history_slot_eq(
        &provider,
        t_plus_2,
        P3_ACTIVATION_BLOCK - 1,
        U256::from(P3_ACTIVATION_BLOCK - 1),
    );
    assert_history_slot_eq(
        &provider,
        t_plus_2,
        P3_ACTIVATION_BLOCK,
        U256::from(P3_ACTIVATION_BLOCK),
    );
    assert_history_slot_eq(
        &provider,
        t_plus_2,
        P3_ACTIVATION_BLOCK + 1,
        U256::from(P3_ACTIVATION_BLOCK + 1),
    );

    println!("[eip2935_test] ✅ P-5 idempotency: slot 99/100/101 preserved + code intact at block {t_plus_2}.");
    Ok(())
}

#[test]
fn test_p5_redeployment_idempotency_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p5_grevm_test",
        false,
        run_pipe_p5_idempotency,
    );
}

#[test]
fn test_p5_redeployment_idempotency_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p5_disable_grevm_test",
        true,
        run_pipe_p5_idempotency,
    );
}

// ---------------------------------------------------------------------------
// P-15: chainspec without `pragueTime` — deployment branch must never fire,
//       regardless of how high the chain timestamps climb.
//
// P-1 covers the "pragueTime set, but to a far-future value" case; P-15 covers
// the orthogonal "pragueTime field absent entirely" case (`ForkCondition::Never`).
// Both should produce identical observable behaviour, so we re-use the same
// scenario runner — only the chainspec JSON differs. `gravity_hardfork.json`
// is the canonical no-Prague chainspec in this crate (it has no `pragueTime`
// field), so we point `--chain` at it directly instead of materialising a
// dedicated JSON copy.
// ---------------------------------------------------------------------------

#[test]
fn test_p15_chainspec_without_prague_time_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(None),
        "data/gravity_eip2935_p15_grevm_test",
        false,
        run_pipe_pre_fork,
    );
}

#[test]
fn test_p15_chainspec_without_prague_time_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(None),
        "data/gravity_eip2935_p15_disable_grevm_test",
        true,
        run_pipe_pre_fork,
    );
}

// ---------------------------------------------------------------------------
// P-2: Genesis activation edge case — `pragueTime = 0` does NOT auto-deploy
//
// The activation helper requires `parent_ts < pragueTime`. When `pragueTime`
// is set to 0 (i.e. "active from genesis"), there is no block whose parent
// timestamp is strictly less than zero — so `transitions_at_timestamp` never
// fires and the deployment branch is unreachable. This is the documented
// rationale for "pragueTime = 0 chainspecs must preload HISTORY_STORAGE in
// the genesis alloc"; this test pins the unreachable-branch behaviour so
// nobody assumes pragueTime=0 alone is enough.
// ---------------------------------------------------------------------------

#[test]
fn test_p2_genesis_activation_no_auto_deploy_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_GENESIS)),
        "data/gravity_eip2935_p2_grevm_test",
        false,
        run_pipe_pre_fork,
    );
}

#[test]
fn test_p2_genesis_activation_no_auto_deploy_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_GENESIS)),
        "data/gravity_eip2935_p2_disable_grevm_test",
        true,
        run_pipe_pre_fork,
    );
}

// ---------------------------------------------------------------------------
// P-6: Transient parent_hash carrier — header.parent_hash is overwritten with
//      the real chain hash at seal time, AFTER the SystemCaller consumed it
//      as 2935 calldata.
//
// Asserts the dual identity of `header.parent_hash` documented in the design:
//   (a) During execution: header.parent_hash == ordered_block.parent_id
//       (the consensus-supplied block_id, carried via the transient field).
//       Evidence: slot (N-1) % 8191 of HISTORY_STORAGE == mock_block_id(N-1)
//       — only possible if the SystemCaller saw the transient parent_id.
//   (b) After seal: header.parent_hash == real_chain_hash(N-1)
//       — `create_block_for_executor` set it to parent_id, then the seal
//       stage overwrote it back to the real hash for canonical persistence.
//   (c) The two values are NOT equal (mock_block_id(N-1) != real_hash(N-1)),
//       proving the override genuinely happened.
// ---------------------------------------------------------------------------

async fn run_pipe_p6_transient_parent_hash(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-6 expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    let consensus = MockConsensus::new(pipeline_api, P3_ACTIVATION_BLOCK, Box::new(p3_ts_us));
    let pipeline_api = consensus.run(latest_block_number).await;

    // Ensure both block 99 (parent) and block 100 (activation) are persisted
    // before reading their headers — the persistence channel is async and the
    // disable_grevm test (P-14) documents the race we avoid here.
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK - 1).await.unwrap();
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();

    // Confirm the EIP-2935 functional baseline (same as P-3) still holds, so
    // any failure of the transient-carrier assertion below cannot be confused
    // with a regression in the activation path itself.
    assert_history_storage_deployed(&provider, P3_ACTIVATION_BLOCK);
    assert_history_slot_eq(
        &provider,
        P3_ACTIVATION_BLOCK,
        P3_ACTIVATION_BLOCK - 1,
        U256::from(P3_ACTIVATION_BLOCK - 1),
    );

    let db_provider = provider.database_provider_ro().unwrap();
    let header_99 = db_provider
        .header_by_number(P3_ACTIVATION_BLOCK - 1)
        .unwrap()
        .expect("block 99 header must be persisted");
    let header_100 = db_provider
        .header_by_number(P3_ACTIVATION_BLOCK)
        .unwrap()
        .expect("block 100 header must be persisted");
    let real_hash_99 = db_provider
        .block_hash(P3_ACTIVATION_BLOCK - 1)
        .unwrap()
        .expect("block 99 sealed hash must be persisted");
    drop(db_provider);

    // (b) After seal: parent_hash equals the real (keccak) hash of block 99.
    assert_eq!(
        header_100.parent_hash, real_hash_99,
        "sealed block 100 parent_hash must equal block 99's real chain hash (seal-stage overwrite at lib.rs:519-521)"
    );

    // (c) The real hash must differ from the transient mock_block_id(99) —
    //     otherwise the test below would not actually prove anything. Block 99
    //     contains gravity hardfork bytecode (gammaBlock=20, deltaBlock=25) so
    //     its real keccak hash cannot collide with a left-padded block number.
    let transient_99 = mock_block_id(P3_ACTIVATION_BLOCK - 1);
    assert_ne!(
        real_hash_99, transient_99,
        "real_hash_99 must differ from mock_block_id(99) for the override assertion to be meaningful"
    );
    assert_ne!(
        header_100.parent_hash, transient_99,
        "sealed block 100 parent_hash must NOT equal the transient mock_block_id(99) — that value lives only during execution"
    );

    // Sanity: block 99 also has a coherent parent_hash chained back to its predecessor.
    assert_ne!(header_99.parent_hash, B256::ZERO, "block 99 must reference a non-zero parent");

    println!(
        "[eip2935_test] ✅ P-6 transient parent_hash:\n  real_hash(99) = {real_hash_99:?}\n  mock_block_id(99) = {transient_99:?}\n  header_100.parent_hash = {:?}",
        header_100.parent_hash
    );
    println!("[eip2935_test]   → header_100.parent_hash == real_hash(99) ✓ (seal overwrite)");
    println!(
        "[eip2935_test]   → slot 99 == mock_block_id(99) ✓ (transient carrier consumed by SystemCaller)"
    );
    Ok(())
}

#[test]
fn test_p6_transient_parent_hash_carrier_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p6_grevm_test",
        false,
        run_pipe_p6_transient_parent_hash,
    );
}

#[test]
fn test_p6_transient_parent_hash_carrier_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p6_disable_grevm_test",
        true,
        run_pipe_p6_transient_parent_hash,
    );
}

// ---------------------------------------------------------------------------
// P-16: Block T-1 (pre-Prague) — deployment branch is strictly silent and
//       SLOAD on the absent account returns ZERO without revert.
//
// Re-uses the P-3 chainspec but stops the consensus at exactly the
// pre-activation tip (block 99, with timestamp == pragueTime - 1). At that
// height `transitions_at_timestamp(99_ts, 98_ts)` returns false because the
// parent timestamp is still below pragueTime — the deployment irregular state
// change must not fire, and the account at HISTORY_STORAGE_ADDRESS must
// remain non-existent. The companion "SLOAD returns ZERO" assertion sweeps a
// few slots to confirm reads against an absent account degrade to ZERO
// instead of reverting (the underlying state provider behavior the EVM
// inherits via EmptyDB / `LoadedNotExisting`).
// ---------------------------------------------------------------------------

async fn run_pipe_p16_pre_t_silent(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-16 expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api,
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    // Push exactly 99 blocks — block 99's timestamp equals `pragueTime - 1`
    // and block 99's parent (block 98) is even further below, so on the next
    // (un-pushed) block 100 we would observe `transitions_at_timestamp(100_ts,
    // 99_ts)` flip true. Stopping one short pins the strict pre-T regime.
    let pre_t_tip = P3_ACTIVATION_BLOCK - 1;
    let consensus = MockConsensus::new(pipeline_api, pre_t_tip, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    // (a) HISTORY_STORAGE has no code at the pre-T tip.
    assert_history_storage_absent(&provider, pre_t_tip);
    // Also sample mid-chain to rule out "deployment happened then was rolled
    // back" — across the whole pre-T window the account must stay absent.
    assert_history_storage_absent(&provider, pre_t_tip / 2);
    assert_history_storage_absent(&provider, 1);

    // (b) SLOAD on the absent account returns ZERO without revert. We sample
    //     several ring-buffer indices to exercise the same code path the
    //     post-activation tests assert against.
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(pre_t_tip))
        .expect("state provider for pre-T SLOAD sweep");
    for slot in [0u64, 1, 50, 99, 8190] {
        let value = state
            .storage(HISTORY_STORAGE_ADDRESS, B256::from(U256::from(slot)))
            .expect("storage read must not error on absent account");
        assert_eq!(
            value.unwrap_or(U256::ZERO),
            U256::ZERO,
            "pre-T SLOAD on absent HISTORY_STORAGE must return ZERO (slot {slot})"
        );
    }

    // (c) The account itself reads as "does not exist" — the deployment branch
    //     never created it.
    let account =
        state.basic_account(&HISTORY_STORAGE_ADDRESS).expect("basic_account read must succeed");
    assert!(
        account.is_none(),
        "HISTORY_STORAGE_ADDRESS must not exist as an account at pre-T block {pre_t_tip}"
    );

    println!(
        "[eip2935_test] ✅ P-16 pre-T: HISTORY_STORAGE account absent + SLOAD returns ZERO across sampled slots at block {pre_t_tip}."
    );
    Ok(())
}

#[test]
fn test_p16_pre_t_no_deployment_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p16_grevm_test",
        false,
        run_pipe_p16_pre_t_silent,
    );
}

#[test]
fn test_p16_pre_t_no_deployment_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p16_disable_grevm_test",
        true,
        run_pipe_p16_pre_t_silent,
    );
}

// ---------------------------------------------------------------------------
// P-7' / P-8': eth_call directly against canonical HISTORY_STORAGE_ADDRESS
//
// The end-to-end user contract is "dApp calls eth_call to HISTORY_STORAGE
// with 32-byte calldata = block number n; gets back the corresponding block
// id, or reverts if n is out of window". These two cases exercise that
// contract through pipe deploy + RPC + EVM + bytecode — any layer breaking
// (cache/journaling timing, RPC simulation seeing stale state, ...) reds.
// No probe contract / no chainspec bypass: the canonical 0x...0935 address
// is the production EIP-2935 entry point.
//
// P-7' sample plan (block 110 with pragueTime aligned to block 100):
//   - n=99   → SystemCaller wrote slot 99 at block 100, expect mock_block_id(99)
//   - n=109  → SystemCaller wrote slot 109 at block 110, expect mock_block_id(109)
//   - n=50   → pre-T slot, never written; predicate n < N && N - n <= 8191 holds, so the read path
//     returns ZERO without revert
//   - n=0    → same warmup-miss path
//
// P-8' boundary plan (same fixture):
//   - n == N      → contract's n < block.number guard fails → revert
//   - n == N + 1  → same
//   - n == u64::MAX → far-future / overflow → same
// ---------------------------------------------------------------------------

/// Build a `TransactionRequest` for `eth_call` against the canonical
/// `HISTORY_STORAGE_ADDRESS` with `n` packed as 32-byte big-endian calldata.
fn call_history_storage_request(n: u64) -> TransactionRequest {
    let calldata = Bytes::copy_from_slice(B256::from(U256::from(n)).as_slice());
    TransactionRequest::default().to(HISTORY_STORAGE_ADDRESS).input(calldata.into())
}

async fn run_pipe_p7_warmup_eth_call(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-7' expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api.clone(),
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    // Push 110 blocks: SystemCaller fills slot indices 99..=109 (one per block
    // 100..=110), leaving slot 0..=98 / 110..=8190 as the warmup window.
    let query_block: u64 = P3_ACTIVATION_BLOCK + 10;
    let consensus = MockConsensus::new(pipeline_api, query_block, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    let query_at = Some(BlockId::Number(query_block.into()));

    // Populated post-T slots: n=99 / n=109 → expect mock_block_id(n).
    let ret = eth_api
        .call(call_history_storage_request(99), query_at, EvmOverrides::default())
        .await
        .expect("eth_call(HISTORY_STORAGE, n=99) must not revert");
    assert_eq!(
        ret.as_ref(),
        mock_block_id(99).as_slice(),
        "eth_call(n=99) must return mock_block_id(99)"
    );
    println!("[eip2935_test] ✅ P-7' eth_call(n=99) → mock_block_id(99)");

    let ret = eth_api
        .call(call_history_storage_request(109), query_at, EvmOverrides::default())
        .await
        .expect("eth_call(HISTORY_STORAGE, n=109) must not revert");
    assert_eq!(
        ret.as_ref(),
        mock_block_id(109).as_slice(),
        "eth_call(n=109) must return mock_block_id(109)"
    );
    println!("[eip2935_test] ✅ P-7' eth_call(n=109) → mock_block_id(109)");

    // Warmup-window pre-T slots: predicate n < N && N - n <= 8191 holds,
    // SLOAD on unwritten slot returns ZERO → contract returns ZERO, no revert.
    let ret = eth_api
        .call(call_history_storage_request(50), query_at, EvmOverrides::default())
        .await
        .expect("eth_call(HISTORY_STORAGE, n=50) must return ZERO, not revert");
    assert_eq!(
        ret.as_ref(),
        [0u8; 32].as_ref(),
        "eth_call(n=50) at block 110 must surface ZERO (slot never written, but in window)"
    );
    println!("[eip2935_test] ✅ P-7' eth_call(n=50) → 0x00..00 (warmup miss)");

    let ret = eth_api
        .call(call_history_storage_request(0), query_at, EvmOverrides::default())
        .await
        .expect("eth_call(HISTORY_STORAGE, n=0) must return ZERO, not revert");
    assert_eq!(ret.as_ref(), [0u8; 32].as_ref(), "eth_call(n=0) must surface ZERO");
    println!("[eip2935_test] ✅ P-7' eth_call(n=0) → 0x00..00");

    println!(
        "[eip2935_test] ✅ P-7' warmup window: eth_call direct to canonical HISTORY_STORAGE_ADDRESS surfaces correct block ids and warmup ZEROs without revert."
    );
    Ok(())
}

async fn run_pipe_p8_out_of_window_eth_call(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<()> {
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

    assert_eq!(
        latest_block_number, 0,
        "P-8' expects a fresh datadir so block numbers align with deterministic timestamps"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec,
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api.clone(),
    );

    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();

    let query_block: u64 = P3_ACTIVATION_BLOCK + 10;
    let consensus = MockConsensus::new(pipeline_api, query_block, Box::new(p3_ts_us));
    consensus.run(latest_block_number).await;

    let query_at = Some(BlockId::Number(query_block.into()));

    // n == N: contract's `n < block.number` guard fails.
    let err = eth_api
        .call(call_history_storage_request(query_block), query_at, EvmOverrides::default())
        .await
        .expect_err("eth_call(HISTORY_STORAGE, n=N) must revert");
    println!("[eip2935_test] ✅ P-8' eth_call(n=N={query_block}) reverted: {err:?}");

    // n == N + 1: clearly future.
    let err = eth_api
        .call(call_history_storage_request(query_block + 1), query_at, EvmOverrides::default())
        .await
        .expect_err("eth_call(HISTORY_STORAGE, n=N+1) must revert");
    println!("[eip2935_test] ✅ P-8' eth_call(n=N+1={}) reverted: {err:?}", query_block + 1);

    // n == u64::MAX: far-future / overflow.
    let err = eth_api
        .call(call_history_storage_request(u64::MAX), query_at, EvmOverrides::default())
        .await
        .expect_err("eth_call(HISTORY_STORAGE, n=u64::MAX) must revert");
    println!("[eip2935_test] ✅ P-8' eth_call(n=u64::MAX) reverted: {err:?}");

    println!(
        "[eip2935_test] ✅ P-8' out-of-window: eth_call direct to canonical HISTORY_STORAGE_ADDRESS reverts on n >= N (3 sampled boundaries)."
    );
    Ok(())
}

#[test]
fn test_p7_warmup_eth_call_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p7_grevm_test",
        false,
        run_pipe_p7_warmup_eth_call,
    );
}

#[test]
fn test_p7_warmup_eth_call_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p7_disable_grevm_test",
        true,
        run_pipe_p7_warmup_eth_call,
    );
}

#[test]
fn test_p8_out_of_window_eth_call_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p8_grevm_test",
        false,
        run_pipe_p8_out_of_window_eth_call,
    );
}

#[test]
fn test_p8_out_of_window_eth_call_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100)),
        "data/gravity_eip2935_p8_disable_grevm_test",
        true,
        run_pipe_p8_out_of_window_eth_call,
    );
}

// ---------------------------------------------------------------------------
// shared init + parameterization helper
//
// `run_pipe_e2e_test` is the single entry point every #[test] in this file
// dispatches through. It builds the NodeCommand CLI args, optionally
// appending `--gravity.disable-grevm`, then drives the CliRunner. This is
// how each behavior case (P-1..P-8' / P-15 / P-16) gets sampled twice — once
// on the grevm path and once on the WrapExecutor path — without duplicating
// 25 lines of CLI boilerplate per case.
// ---------------------------------------------------------------------------

fn run_pipe_e2e_test<F, Fut>(
    chain_spec: &str,
    datadir: &'static str,
    disable_grevm: bool,
    run_fn: F,
) where
    F: FnOnce(WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = eyre::Result<()>> + Send + 'static,
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

    runner
        .run_command_until_exit(|ctx| {
            command.execute(
                ctx,
                FnLauncher::new::<EthereumChainSpecParser, _>(|builder, _| async move {
                    run_fn(builder).await
                }),
            )
        })
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));
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
