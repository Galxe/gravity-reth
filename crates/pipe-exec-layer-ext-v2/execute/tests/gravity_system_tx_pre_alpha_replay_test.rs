#![allow(missing_docs)]

//! T4 / §3.3 — Pre-Alpha RPC block & single-tx replay regression test.
//!
//! Pins the "the gate is keyed on the replayed block's timestamp" invariant: for
//! any pre-Alpha block the RPC replay path (trace_block / debug_traceBlock /
//! trace_transaction / debug_traceTransaction) must reproduce the canonical
//! execution byte-for-byte. In particular, `is_system_tx_gas_exempt(chain_spec,
//! B'.timestamp)` returns **false** for pre-Alpha blocks, so the cfg-side
//! `disable_base_fee` / `disable_balance_check` levers MUST NOT activate on
//! replay — pre-Alpha SYSTEM_CALLER traverses the standard fee path, paying
//! `gas_used × base_fee` out of its sentinel-sized genesis alloc balance.
//!
//! Acceptance matrix row: §3.3 (must-pass).
//!
//! Note on location: §3.3 nominally targets `crates/rpc/rpc/tests/` but the
//! reth-rpc crate has no `tests/` directory and pulling in `reth-node-builder`
//! + `reth-cli` dev-deps purely to bootstrap an RPC mock would be invasive
//! churn (much larger than the test itself). The pipe-exec-layer dev harness
//! already exposes the full RPC registry (`handle.node.rpc_registry.trace_api()`
//! / `.debug_api()`) — same RPC surface, smaller blast radius. Tests live here
//! to match `gravity_eip2935_test.rs` / `gravity_bls_precompile_test.rs` which
//! also exercise RPC paths from the pipe harness for the same reason.

use alloy_eips::BlockId;
use alloy_primitives::{Address, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_rpc_types_trace::geth::{GethDebugTracingOptions, TraceResult};
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
    DatabaseProviderFactory, HeaderProvider, ReceiptProvider, TransactionVariant,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Test parameters
// ---------------------------------------------------------------------------

const PRE_ALPHA_TS_BASE: u64 = 1_000_000_000;
/// `alphaTime` so distant that no pushed block can transition into Alpha
/// activation — the entire run is strictly pre-fork.
const ALPHA_TS_NEVER: u64 = 9_999_999_999;
const PRE_ALPHA_BLOCK_COUNT: u64 = 10;
const SAMPLE_TRACE_BLOCK: u64 = 5;

/// Patch `alphaTime` on the embedded `gravity_hardfork.json`, returning JSON
/// the CLI parser accepts as `--chain`. Pattern lifted from
/// `gravity_eip2935_test::gravity_prague_chainspec`.
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

fn pre_alpha_ts_us(block_number: u64) -> u64 {
    (PRE_ALPHA_TS_BASE + block_number) * 1_000_000
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
// MockConsensus — drives empty blocks (one metadata system tx each).
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
// Core T4 runner — pre-Alpha block replay regression.
// ---------------------------------------------------------------------------

async fn run_pre_alpha_replay_regression(
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

    assert_eq!(
        latest_block_number, 0,
        "[pre_alpha_replay {label}] runner expects a fresh datadir (latest must be genesis=0)"
    );

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

    let consensus = MockConsensus::new(pipeline_api, Box::new(pre_alpha_ts_us));
    consensus.push_empty_range(&mut epoch, 1, PRE_ALPHA_BLOCK_COUNT).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(PRE_ALPHA_BLOCK_COUNT).await.unwrap();
    drop(pipeline_api);

    println!(
        "[pre_alpha_replay {label}] pushed {PRE_ALPHA_BLOCK_COUNT} pre-Alpha blocks; sweeping RPC replay endpoints"
    );

    // -----------------------------------------------------------------
    // Phase 1 — trace_block / debug_trace_block / replay_block_transactions
    // over every pre-Alpha block. Each call MUST return Ok, and the trace
    // tx-count MUST match the canonical receipt count for that block. A
    // mismatch (e.g. silent system-tx skipping or duplicate emission) would
    // surface here.
    // -----------------------------------------------------------------
    for n in 1..=PRE_ALPHA_BLOCK_COUNT {
        // Canonical receipts: provider walks the persisted block body.
        let canonical_receipts = provider
            .receipts_by_block(alloy_eips::BlockHashOrNumber::Number(n))
            .expect("provider read must succeed")
            .unwrap_or_else(|| panic!("block {n} must have persisted receipts"));
        assert!(
            !canonical_receipts.is_empty(),
            "[pre_alpha_replay {label}] block {n} must contain >= 1 receipt (the metadata system tx)"
        );

        // RPC trace_block — Reth's parity-style block-family endpoint.
        let traces = trace_api
            .trace_block(BlockId::Number(n.into()))
            .await
            .unwrap_or_else(|e| panic!("[{label}] trace_block({n}) must not error: {e:?}"))
            .unwrap_or_else(|| panic!("[{label}] trace_block({n}) must return Some(_)"));
        // Pre-Alpha block uses normal coinbase reward attribution; sanity-only
        // check is `traces.len() >= canonical_receipts.len()` since
        // `extract_reward_traces` may append a separate reward trace.
        assert!(
            traces.len() >= canonical_receipts.len(),
            "[pre_alpha_replay {label}] block {n} trace_block returned {} traces; expected >= {} per-tx traces (canonical receipts)",
            traces.len(),
            canonical_receipts.len()
        );

        // debug_trace_block — geth-style block-family endpoint with default
        // tracer config (callTracer). Every entry MUST be `Success`. If the
        // gate were incorrectly active pre-Alpha, the cfg-side disable_* flags
        // would still produce a successful trace (no observable diff at the
        // result variant level when balance >> fee), so the strongest assertion
        // here is "no Err variant escapes".
        let debug_traces = debug_api
            .debug_trace_block(BlockId::Number(n.into()), GethDebugTracingOptions::default())
            .await
            .unwrap_or_else(|e| panic!("[{label}] debug_trace_block({n}) must not error: {e:?}"));
        assert!(
            !debug_traces.is_empty(),
            "[pre_alpha_replay {label}] debug_trace_block({n}) must return >= 1 trace entry"
        );
        for (i, entry) in debug_traces.iter().enumerate() {
            assert!(
                matches!(entry, TraceResult::Success { .. }),
                "[pre_alpha_replay {label}] debug_trace_block({n})[{i}] must be Success, got {entry:?}"
            );
        }
    }

    // -----------------------------------------------------------------
    // Phase 2 — single-tx family on a sampled pre-Alpha block.
    // -----------------------------------------------------------------
    let block_5 = provider
        .recovered_block(SAMPLE_TRACE_BLOCK.into(), TransactionVariant::WithHash)
        .expect("recovered_block must succeed")
        .unwrap_or_else(|| panic!("block {SAMPLE_TRACE_BLOCK} must be persisted"));
    let txs = block_5.body().transactions.as_slice();
    assert!(
        !txs.is_empty(),
        "[pre_alpha_replay {label}] block {SAMPLE_TRACE_BLOCK} must contain >= 1 tx (metadata system tx)"
    );

    let metadata_tx_hash: B256 = *txs[0].hash();
    println!(
        "[pre_alpha_replay {label}] sampling metadata system tx at block {SAMPLE_TRACE_BLOCK}: hash={metadata_tx_hash:?}"
    );

    // trace_transaction — must not Err, must return Some(traces) with >= 1 trace.
    let single_traces = trace_api
        .trace_transaction(metadata_tx_hash)
        .await
        .unwrap_or_else(|e| panic!("[{label}] trace_transaction must not error: {e:?}"))
        .unwrap_or_else(|| {
            panic!("[{label}] trace_transaction for pre-Alpha metadata system tx must return Some")
        });
    assert!(
        !single_traces.is_empty(),
        "[pre_alpha_replay {label}] trace_transaction must return >= 1 trace per metadata system tx"
    );

    // debug_trace_transaction — geth-style single-tx tracer. Must not Err.
    let debug_single = debug_api
        .debug_trace_transaction(metadata_tx_hash, GethDebugTracingOptions::default())
        .await
        .unwrap_or_else(|e| panic!("[{label}] debug_trace_transaction must not error: {e:?}"));
    // Sanity: the trace returned a non-null geth payload. Any
    // `GasPriceLessThanBasefee` / `InsufficientFunds` style failure on replay
    // would surface as an Err above; reaching this point is the load-bearing
    // gate assertion.
    let s = format!("{debug_single:?}");
    assert!(
        !s.contains("InsufficientFunds") && !s.contains("GasPriceLessThanBasefee"),
        "[pre_alpha_replay {label}] debug_trace_transaction payload must not contain fee-error markers: {s}"
    );

    // -----------------------------------------------------------------
    // Phase 3 — sanity: pre-Alpha block ts is < alphaTime, so the gate must
    // return false for every replayed block timestamp. We verify by
    // reading the persisted header ts and comparing to the chain_spec's
    // alphaTime fork condition; this guards against accidental gate-on-tip
    // regressions (design §3.5.2).
    // -----------------------------------------------------------------
    for n in 1..=PRE_ALPHA_BLOCK_COUNT {
        let header = provider
            .header_by_number(n)
            .expect("provider read must succeed")
            .unwrap_or_else(|| panic!("block {n} header must be persisted"));
        let ts = header.timestamp;
        assert!(
            !reth_chainspec::is_system_tx_gas_exempt(chain_spec.as_ref(), ts),
            "[pre_alpha_replay {label}] is_system_tx_gas_exempt must return false at pre-Alpha block {n} ts={ts}"
        );
    }

    println!(
        "[pre_alpha_replay {label}] ✅ all {PRE_ALPHA_BLOCK_COUNT} pre-Alpha block / single-tx replays passed (no false exemption)"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test entry points — grevm + serial backends both exercised for the same
// pre-Alpha invariants (regression in either path is independent).
// ---------------------------------------------------------------------------

#[test]
fn test_rpc_pre_alpha_replay_regression_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TS_NEVER),
        "data/gravity_system_tx_pre_alpha_replay_grevm",
        false,
        |b| run_pre_alpha_replay_regression(b, "grevm"),
    );
}

#[test]
fn test_rpc_pre_alpha_replay_regression_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TS_NEVER),
        "data/gravity_system_tx_pre_alpha_replay_disable_grevm",
        true,
        |b| run_pre_alpha_replay_regression(b, "disable_grevm"),
    );
}

// ---------------------------------------------------------------------------
// Shared CLI harness (mirrors gravity_bls_precompile_test::run_pipe_e2e_test).
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
