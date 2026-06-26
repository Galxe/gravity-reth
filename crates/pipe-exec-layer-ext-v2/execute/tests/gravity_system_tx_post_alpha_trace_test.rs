#![allow(missing_docs)]

//! T6 / §3.1 + §3.2 — post-Alpha block-family + single-tx trace consistency.
//!
//! Pins the load-bearing claim of the system-tx gas-exempt PR: once Alpha is
//! active and SYSTEM_CALLER.balance is zero, the RPC replay path (every
//! block-family endpoint + every single-tx endpoint) must reproduce canonical
//! execution **without** failing on `GasPriceLessThanBasefee` or
//! `InsufficientFunds`. The per-tx exemption (cfg-side `disable_base_fee +
//! disable_balance_check`) must activate exactly for those persisted txs whose
//! recovered sender is SYSTEM_CALLER, leaving user txs on the standard fee
//! path.
//!
//! Acceptance matrix rows: §3.1 (block family) + §3.2 (single-tx family) —
//! must-pass.
//!
//! Coverage delta from the spec:
//!  - Block-family endpoints exercised: `trace_block`, `trace_filter`,
//!    `trace_replayBlockTransactions`, `trace_block_opcode_gas`, `trace_block_storage_access`,
//!    `debug_traceBlock`. (`ots_getContractCreator` skipped — needs an actual contract-creating tx,
//!    which is not produced by the empty-block harness.)
//!  - Single-tx endpoints exercised (target=system tx): `trace_transaction`,
//!    `trace_replayTransaction`, `trace_get`, `debug_traceTransaction`,
//!    `trace_transaction_opcode_gas`.
//!  - The "target=user tx with system-tx prelude" sub-case from §3.2 is NOT covered here —
//!    injecting signed user txs into the OrderedBlock alongside the protocol-injected system tx
//!    would expand the scaffolding by ~250 LOC and is better covered by a sibling test if/when the
//!    EIP-7702 helper from `gravity_bls_precompile_test.rs` is generalised. The current test still
//!    pins the load-bearing "system-tx segment runs gas-exempt without fee errors" half of §3.2;
//!    the user-tx-with-prelude half is documented as a follow-up.
//!
//! Location note: see `gravity_system_tx_pre_alpha_replay_test.rs` for the
//! rationale on co-locating RPC-surface tests in the pipe-exec-layer harness.

use alloy_eips::BlockId;
use alloy_primitives::{address, map::HashSet, Address, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_rpc_types_trace::{
    filter::TraceFilter,
    geth::{GethDebugTracingOptions, TraceResult},
    parity::TraceType,
};
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
    providers::BlockchainProvider, BlockHashReader, BlockNumReader, BlockReader,
    DatabaseProviderFactory, HeaderProvider, ReceiptProvider, StateProviderFactory,
    TransactionVariant,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// `alphaTime = 1` so every pushed block is post-Alpha and Alpha-active.
const ALPHA_TS_BASE: u64 = 2_000_000_000;
const ALPHA_TIME_ALWAYS: u64 = 1;
const POST_ALPHA_TIP: u64 = 10;
const SAMPLE_BLOCK: u64 = 5;

const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000");

fn gravity_alpha_chainspec(alpha_time: u64) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    json["config"]["alphaTime"] = serde_json::json!(alpha_time);
    json.to_string()
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn alpha_ts_us(block_number: u64) -> u64 {
    (ALPHA_TS_BASE + block_number) * 1_000_000
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

// ---------------------------------------------------------------------------
// MockConsensus
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Core T6 runner — block-family + single-tx trace consistency on post-Alpha
// blocks containing protocol-injected system txs.
// ---------------------------------------------------------------------------

async fn run_post_alpha_trace_consistency(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
    label: &'static str,
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
    let trace_api = handle.node.rpc_registry.trace_api();
    let debug_api = handle.node.rpc_registry.debug_api();
    let provider = handle.node.provider;

    let db_provider = provider.database_provider_ro().unwrap();
    let latest_block_number = db_provider.best_block_number().unwrap();
    let latest_block_hash = db_provider.block_hash(latest_block_number).unwrap().unwrap();
    let latest_block_header = db_provider.header_by_number(latest_block_number).unwrap().unwrap();
    drop(db_provider);

    assert_eq!(latest_block_number, 0, "[post_alpha_trace {label}] runner expects a fresh datadir");

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

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(alpha_ts_us));
    consensus.push_empty_range(&mut epoch, 1, POST_ALPHA_TIP).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(POST_ALPHA_TIP).await.unwrap();
    drop(pipeline_api);

    // Sanity: post-Alpha SYSTEM_CALLER.balance is zero (migration fired at
    // block 1). Without this, the gas-exempt levers wouldn't be load-bearing
    // and the test would devolve into a trivial happy-path check.
    let sysacc = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(POST_ALPHA_TIP))
        .expect("state provider")
        .basic_account(&SYSTEM_CALLER)
        .expect("basic_account")
        .expect("SYSTEM_CALLER must remain present post-Alpha");
    assert_eq!(
        sysacc.balance,
        U256::ZERO,
        "[post_alpha_trace {label}] post-Alpha SYSTEM_CALLER.balance must be 0 — test is vacuous otherwise"
    );

    // -----------------------------------------------------------------
    // §3.1 BLOCK-FAMILY — exercise every block-level endpoint on every
    // post-Alpha block and pin "no fee error escapes the RPC layer".
    // -----------------------------------------------------------------
    for n in 1..=POST_ALPHA_TIP {
        let block_id = BlockId::Number(n.into());
        let canonical_receipts = provider
            .receipts_by_block(alloy_eips::BlockHashOrNumber::Number(n))
            .expect("provider receipts read")
            .unwrap_or_else(|| panic!("block {n} must have persisted receipts"));
        assert!(
            !canonical_receipts.is_empty(),
            "[post_alpha_trace {label}] block {n} must contain >= 1 receipt (metadata system tx)"
        );

        // 3.1.a — trace_block
        let blk_traces = trace_api
            .trace_block(block_id)
            .await
            .unwrap_or_else(|e| panic!("[{label}] trace_block({n}) errored: {e:?}"))
            .unwrap_or_else(|| panic!("[{label}] trace_block({n}) returned None"));
        assert!(
            blk_traces.len() >= canonical_receipts.len(),
            "[post_alpha_trace {label}] trace_block({n}) returned {} traces; expected >= {} (per canonical receipts)",
            blk_traces.len(),
            canonical_receipts.len()
        );

        // 3.1.b — debug_trace_block; every entry must be Success (no
        // GasPriceLessThanBasefee / InsufficientFunds escape).
        let debug_blk = debug_api
            .debug_trace_block(block_id, GethDebugTracingOptions::default())
            .await
            .unwrap_or_else(|e| panic!("[{label}] debug_trace_block({n}) errored: {e:?}"));
        assert!(
            !debug_blk.is_empty(),
            "[post_alpha_trace {label}] debug_trace_block({n}) returned empty Vec"
        );
        for (i, entry) in debug_blk.iter().enumerate() {
            assert!(
                matches!(entry, TraceResult::Success { .. }),
                "[post_alpha_trace {label}] debug_trace_block({n})[{i}] must be Success, got {entry:?}"
            );
        }

        // 3.1.c — replay_block_transactions (parity-style)
        let mut trace_types: HashSet<TraceType> = HashSet::default();
        trace_types.insert(TraceType::Trace);
        let replay_blk = trace_api
            .replay_block_transactions(block_id, trace_types.clone())
            .await
            .unwrap_or_else(|e| panic!("[{label}] replay_block_transactions({n}) errored: {e:?}"))
            .unwrap_or_else(|| panic!("[{label}] replay_block_transactions({n}) returned None"));
        assert_eq!(
            replay_blk.len(),
            canonical_receipts.len(),
            "[post_alpha_trace {label}] replay_block_transactions({n}) entry count mismatch"
        );

        // 3.1.d — trace_block_opcode_gas (Reth extension)
        let opcode_gas = trace_api
            .trace_block_opcode_gas(block_id)
            .await
            .unwrap_or_else(|e| panic!("[{label}] trace_block_opcode_gas({n}) errored: {e:?}"))
            .unwrap_or_else(|| panic!("[{label}] trace_block_opcode_gas({n}) returned None"));
        assert!(
            !opcode_gas.transactions.is_empty(),
            "[post_alpha_trace {label}] trace_block_opcode_gas({n}) must report >= 1 tx opcode-gas record"
        );

        // 3.1.e — trace_block_storage_access (Reth extension); the inspector
        // surface is reused, so we only assert Ok+non-error here.
        let _storage_access = trace_api
            .trace_block_storage_access(block_id)
            .await
            .unwrap_or_else(|e| panic!("[{label}] trace_block_storage_access({n}) errored: {e:?}"));
    }

    // 3.1.f — trace_filter spanning the full post-Alpha range. Should not
    // error and should return at least one trace per included block.
    let filter = TraceFilter {
        from_block: Some(1),
        to_block: Some(POST_ALPHA_TIP),
        from_address: vec![],
        to_address: vec![],
        mode: Default::default(),
        after: None,
        count: None,
    };
    let filt_traces = trace_api
        .trace_filter(filter)
        .await
        .unwrap_or_else(|e| panic!("[{label}] trace_filter errored: {e:?}"));
    assert!(
        !filt_traces.is_empty(),
        "[post_alpha_trace {label}] trace_filter must return >= 1 trace across blocks 1..={POST_ALPHA_TIP}"
    );

    // -----------------------------------------------------------------
    // §3.2 SINGLE-TX FAMILY — pick the metadata system tx at SAMPLE_BLOCK
    // and exercise every single-tx endpoint. With the gas-exempt cfg-side
    // levers active, the replay path must succeed even though
    // SYSTEM_CALLER.balance == 0.
    // -----------------------------------------------------------------
    let sample_block = provider
        .recovered_block(SAMPLE_BLOCK.into(), TransactionVariant::WithHash)
        .expect("recovered_block")
        .unwrap_or_else(|| panic!("block {SAMPLE_BLOCK} must be persisted"));
    let txs = sample_block.body().transactions.as_slice();
    assert!(
        !txs.is_empty(),
        "[post_alpha_trace {label}] block {SAMPLE_BLOCK} must contain the metadata system tx"
    );
    let metadata_tx_hash: B256 = *txs[0].hash();
    println!(
        "[post_alpha_trace {label}] sample system tx at block {SAMPLE_BLOCK}: hash={metadata_tx_hash:?}"
    );

    // 3.2.a — trace_transaction
    let trace_tx = trace_api
        .trace_transaction(metadata_tx_hash)
        .await
        .unwrap_or_else(|e| panic!("[{label}] trace_transaction errored: {e:?}"))
        .unwrap_or_else(|| panic!("[{label}] trace_transaction returned None"));
    assert!(
        !trace_tx.is_empty(),
        "[post_alpha_trace {label}] trace_transaction must return >= 1 trace for the metadata system tx"
    );

    // 3.2.b — trace_replay_transaction
    let mut replay_trace_types: HashSet<TraceType> = HashSet::default();
    replay_trace_types.insert(TraceType::Trace);
    replay_trace_types.insert(TraceType::StateDiff);
    let replay_tx = trace_api
        .replay_transaction(metadata_tx_hash, replay_trace_types)
        .await
        .unwrap_or_else(|e| panic!("[{label}] replay_transaction errored: {e:?}"));
    // state_diff may be Some or None depending on whether trace types include
    // it; we requested it above. Just assert the response surface succeeded.
    let _ = replay_tx.trace;

    // 3.2.c — trace_get index 0
    let trace_get_0 = trace_api
        .trace_get(metadata_tx_hash, vec![0])
        .await
        .unwrap_or_else(|e| panic!("[{label}] trace_get errored: {e:?}"));
    assert!(
        trace_get_0.is_some(),
        "[post_alpha_trace {label}] trace_get(hash, [0]) must return Some(trace)"
    );

    // 3.2.d — debug_trace_transaction
    let debug_tx = debug_api
        .debug_trace_transaction(metadata_tx_hash, GethDebugTracingOptions::default())
        .await
        .unwrap_or_else(|e| panic!("[{label}] debug_trace_transaction errored: {e:?}"));
    let s = format!("{debug_tx:?}");
    assert!(
        !s.contains("InsufficientFunds") && !s.contains("GasPriceLessThanBasefee"),
        "[post_alpha_trace {label}] debug_trace_transaction payload must not contain fee-error markers: {s}"
    );

    // 3.2.e — trace_transaction_opcode_gas
    let txopcode = trace_api
        .trace_transaction_opcode_gas(metadata_tx_hash)
        .await
        .unwrap_or_else(|e| panic!("[{label}] trace_transaction_opcode_gas errored: {e:?}"));
    assert!(
        txopcode.is_some(),
        "[post_alpha_trace {label}] trace_transaction_opcode_gas must return Some"
    );

    println!(
        "[post_alpha_trace {label}] ✅ block-family ({POST_ALPHA_TIP} blocks × 5 endpoints + trace_filter) + single-tx (5 endpoints) all succeeded post-Alpha"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

#[test]
fn test_rpc_post_alpha_trace_consistency_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME_ALWAYS),
        "data/gravity_system_tx_post_alpha_trace_grevm",
        false,
        |b| run_post_alpha_trace_consistency(b, "grevm"),
    );
}

#[test]
fn test_rpc_post_alpha_trace_consistency_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME_ALWAYS),
        "data/gravity_system_tx_post_alpha_trace_disable_grevm",
        true,
        |b| run_post_alpha_trace_consistency(b, "disable_grevm"),
    );
}

// ---------------------------------------------------------------------------
// Shared CLI harness
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
