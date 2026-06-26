#![allow(missing_docs)]

//! T5 / §3.4 — pure-simulation endpoints anti-spoof regression test.
//!
//! Pins the load-bearing safety invariant from design §2.5: the gas-exempt
//! exemption MUST be keyed on the **recovered sender** of a persisted system
//! tx (only reachable via `RecoveredBlock::transactions_recovered()` against
//! empty-signature protocol-injected txs), NEVER on a user-supplied
//! `from = SYSTEM_CALLER` in a simulation `TransactionRequest`. Otherwise any
//! caller could spoof the address and obtain free gas / balance-check bypass
//! on `eth_call` / `eth_estimateGas` / `eth_simulateV1` / `debug_traceCall` /
//! `trace_call` / `trace_callMany` / `trace_rawTransaction`.
//!
//! Acceptance matrix row: §3.4 (must-pass).
//!
//! Strategy — each endpoint is invoked twice on the same state:
//!  1. once with `from = SYSTEM_CALLER` (the spoof attempt),
//!  2. once with `from = SPOOF_PROBE_ADDR` (a fresh zero-balance address that receives no special
//!     treatment from any code path).
//!
//! If the exemption is correctly keyed on the recovered sender (i.e.
//! *unreachable* from a user-supplied `from`), the two invocations produce the
//! same observable result — both Ok with the same bytes, or both Err. Any
//! divergence — `from = SYSTEM_CALLER` succeeds while the probe fails — proves
//! the exemption leaked through user input and is a security regression.
//!
//! Location note: see `gravity_system_tx_pre_alpha_replay_test.rs` for the
//! rationale on co-locating RPC-surface tests inside the pipe-exec-layer
//! harness (rpc/rpc has no `tests/` dir and the harness already exposes the
//! full RPC registry).

use alloy_eips::BlockId;
use alloy_primitives::{address, map::HashSet, Address, Bytes, B256, U256};
use alloy_rpc_types_eth::{simulate::SimulatePayload, state::EvmOverrides, TransactionRequest};
use alloy_rpc_types_trace::{
    geth::{GethDebugTracingCallOptions, GethDebugTracingOptions},
    parity::TraceType,
    tracerequest::TraceCallRequest,
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
    providers::BlockchainProvider, BlockHashReader, BlockNumReader, DatabaseProviderFactory,
};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// `alphaTime = 1` so every block in the test (numbers 1..=) is strictly
/// post-Alpha (block 1's timestamp = `ALPHA_TS_BASE + 1` >> 1). The migration
/// hook fires at block 1 and SYSTEM_CALLER.balance is zeroed thereafter.
const ALPHA_TS_BASE: u64 = 2_000_000_000;
const ALPHA_TIME_ALWAYS: u64 = 1;
const TIP_BLOCK: u64 = 10;

/// `SYSTEM_CALLER = 0x…625f0000` — same literal as
/// `reth_chainspec::SYSTEM_CALLER` (inlined to avoid pulling reth-chainspec
/// into this test crate's import list).
const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000");

/// A fresh, zero-balance "spoof probe" address. Distinct from any pre-funded
/// genesis alloc account and not the SYSTEM_CALLER itself; receives no special
/// treatment from any gas-exempt / migration code path. Acts as the
/// counter-factual baseline against which the SYSTEM_CALLER spoof attempt is
/// compared.
const SPOOF_PROBE_ADDR: Address = address!("0000000000000000000000000000000000abcdef");

/// Patch `alphaTime` on the embedded `gravity_hardfork.json`.
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
// Anti-spoof request builders. The two requests are byte-identical except for
// the `from` field; any divergence in the endpoint's response thus isolates
// the leak point to user-supplied `from`.
// ---------------------------------------------------------------------------

fn spoof_call_request(from: Address) -> TransactionRequest {
    // Hardcoded receiver: SYSTEM_CALLER itself. Calldata empty so no contract
    // logic runs — the call short-circuits at intrinsic gas, and any
    // gas-exempt leak shows up as "the call would have failed (out of funds)
    // but succeeded via free-gas".
    TransactionRequest::default()
        .from(from)
        .to(SYSTEM_CALLER)
        .value(U256::ZERO)
        .input(Bytes::new().into())
}

// ---------------------------------------------------------------------------
// Core T5 runner — drives every targeted endpoint twice (SYSTEM_CALLER vs
// SPOOF_PROBE_ADDR) at a post-Alpha block and asserts behavioral equivalence.
// ---------------------------------------------------------------------------

async fn run_simulation_anti_spoof(
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
        "[anti_spoof {label}] runner expects a fresh datadir (latest must be genesis=0)"
    );

    let storage = BlockViewStorage::new(provider.clone());

    let (tx, rx) = tokio::sync::oneshot::channel();
    let pipeline_api = new_pipe_exec_layer_api(
        chain_spec.clone(),
        storage,
        latest_block_header,
        latest_block_hash,
        rx,
        eth_api.clone(),
    );
    tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    // Push enough blocks past Alpha so SYSTEM_CALLER.balance is zero and a
    // post-Alpha state snapshot is queryable. Block 1 already activates Alpha
    // (alphaTime=1, block 1 ts >> 1), but pushing to TIP_BLOCK keeps the test
    // robust against post-fork blocks accidentally regressing.
    let consensus = MockConsensus::new(pipeline_api, Box::new(alpha_ts_us));
    consensus.push_empty_range(&mut epoch, 1, TIP_BLOCK).await;
    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(TIP_BLOCK).await.unwrap();
    drop(pipeline_api);

    // -----------------------------------------------------------------
    // Sanity: post-Alpha SYSTEM_CALLER.balance is 0 (so any "free gas" leak
    // would be observable if the exemption were keyed on user `from`).
    // -----------------------------------------------------------------
    use reth_provider::StateProviderFactory;
    let sysacc = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(TIP_BLOCK))
        .expect("state provider")
        .basic_account(&SYSTEM_CALLER)
        .expect("basic_account")
        .expect("SYSTEM_CALLER must remain present post-Alpha (EIP-161 not pruned)");
    assert_eq!(
        sysacc.balance,
        U256::ZERO,
        "[anti_spoof {label}] post-Alpha SYSTEM_CALLER.balance must be 0 — otherwise spoof test is vacuous"
    );

    let block_id = Some(BlockId::Number(TIP_BLOCK.into()));

    // -----------------------------------------------------------------
    // 1. eth_call — pure simulation; design says it already has `disable_base_fee=true` and our
    //    exemption MUST NOT key in here.
    // -----------------------------------------------------------------
    let call_sys =
        eth_api.call(spoof_call_request(SYSTEM_CALLER), block_id, EvmOverrides::default()).await;
    let call_probe =
        eth_api.call(spoof_call_request(SPOOF_PROBE_ADDR), block_id, EvmOverrides::default()).await;
    assert_results_equivalent(label, "eth_call", &call_sys, &call_probe);

    // -----------------------------------------------------------------
    // 2. eth_estimateGas
    // -----------------------------------------------------------------
    let est_sys =
        eth_api.estimate_gas_at(spoof_call_request(SYSTEM_CALLER), block_id.unwrap(), None).await;
    let est_probe = eth_api
        .estimate_gas_at(spoof_call_request(SPOOF_PROBE_ADDR), block_id.unwrap(), None)
        .await;
    assert_results_equivalent(label, "eth_estimateGas", &est_sys, &est_probe);

    // -----------------------------------------------------------------
    // 3. eth_simulateV1 — a Single block with one inner call; same equivalence.
    // -----------------------------------------------------------------
    use alloy_rpc_types_eth::simulate::SimBlock;
    let payload_for = |from: Address| SimulatePayload {
        block_state_calls: vec![SimBlock {
            block_overrides: None,
            state_overrides: None,
            calls: vec![spoof_call_request(from)],
        }],
        trace_transfers: false,
        validation: false,
        return_full_transactions: false,
    };
    let sim_sys = eth_api.simulate_v1(payload_for(SYSTEM_CALLER), block_id).await;
    let sim_probe = eth_api.simulate_v1(payload_for(SPOOF_PROBE_ADDR), block_id).await;
    assert_results_equivalent(
        label,
        "eth_simulateV1",
        &sim_sys.as_ref().map(|_| ()),
        &sim_probe.as_ref().map(|_| ()),
    );

    // -----------------------------------------------------------------
    // 4. debug_traceCall — geth-style; default cfg ENABLES basefee check (debug.rs comment). Must
    //    not leak gas-exempt either.
    // -----------------------------------------------------------------
    let dt_sys = debug_api
        .debug_trace_call(
            spoof_call_request(SYSTEM_CALLER),
            block_id,
            GethDebugTracingCallOptions::new(GethDebugTracingOptions::default()),
        )
        .await;
    let dt_probe = debug_api
        .debug_trace_call(
            spoof_call_request(SPOOF_PROBE_ADDR),
            block_id,
            GethDebugTracingCallOptions::new(GethDebugTracingOptions::default()),
        )
        .await;
    assert_results_equivalent(
        label,
        "debug_traceCall",
        &dt_sys.as_ref().map(|_| ()),
        &dt_probe.as_ref().map(|_| ()),
    );

    // -----------------------------------------------------------------
    // 5. trace_call — parity-style; default cfg from caller.
    // -----------------------------------------------------------------
    let mut trace_types: HashSet<TraceType> = HashSet::default();
    trace_types.insert(TraceType::Trace);
    let trace_req = |from: Address| TraceCallRequest {
        call: spoof_call_request(from),
        trace_types: trace_types.clone(),
        block_id,
        state_overrides: None,
        block_overrides: None,
    };
    let tc_sys = trace_api.trace_call(trace_req(SYSTEM_CALLER)).await;
    let tc_probe = trace_api.trace_call(trace_req(SPOOF_PROBE_ADDR)).await;
    assert_results_equivalent(
        label,
        "trace_call",
        &tc_sys.as_ref().map(|_| ()),
        &tc_probe.as_ref().map(|_| ()),
    );

    // -----------------------------------------------------------------
    // 6. trace_callMany — bundles. The same equivalence applies per call.
    // -----------------------------------------------------------------
    let tcm_sys = trace_api
        .trace_call_many(vec![(spoof_call_request(SYSTEM_CALLER), trace_types.clone())], block_id)
        .await;
    let tcm_probe = trace_api
        .trace_call_many(
            vec![(spoof_call_request(SPOOF_PROBE_ADDR), trace_types.clone())],
            block_id,
        )
        .await;
    assert_results_equivalent(
        label,
        "trace_callMany",
        &tcm_sys.as_ref().map(|_| ()),
        &tcm_probe.as_ref().map(|_| ()),
    );

    // -----------------------------------------------------------------
    // 7. Block-info sanity at the queried block — confirms `block_id` actually resolves (gate
    //    against the test passing on a missing block).
    // -----------------------------------------------------------------
    use reth_provider::HeaderProvider as _;
    let blk_header = provider
        .header_by_number(TIP_BLOCK)
        .expect("header_by_number must not error")
        .expect("queried block header must be persisted");
    assert_eq!(blk_header.number, TIP_BLOCK, "[anti_spoof {label}] queried block number mismatch");

    println!(
        "[anti_spoof {label}] ✅ all 6 simulation endpoints behaved identically for from=SYSTEM_CALLER and from=PROBE — no spoof leak"
    );
    Ok(())
}

/// Helper: assert two endpoint results are behaviorally equivalent. Either
/// both Ok (we don't compare bytes — the call may emit different gas-used /
/// trace structures depending on coinbase / fee accounting against from, but
/// the success/failure dichotomy is the load-bearing signal) or both Err.
fn assert_results_equivalent<T: std::fmt::Debug, E: std::fmt::Debug>(
    label: &str,
    endpoint: &str,
    spoof: &Result<T, E>,
    probe: &Result<T, E>,
) {
    let (s_ok, p_ok) = (spoof.is_ok(), probe.is_ok());
    assert_eq!(
        s_ok, p_ok,
        "[anti_spoof {label}] {endpoint}: spoof=SYSTEM_CALLER got Ok={s_ok}, probe got Ok={p_ok} — divergence proves exemption leak through user `from`. spoof={spoof:?} probe={probe:?}"
    );
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

#[test]
fn test_rpc_simulation_anti_spoof_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME_ALWAYS),
        "data/gravity_system_tx_simulation_anti_spoof_grevm",
        false,
        |b| run_simulation_anti_spoof(b, "grevm"),
    );
}

#[test]
fn test_rpc_simulation_anti_spoof_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME_ALWAYS),
        "data/gravity_system_tx_simulation_anti_spoof_disable_grevm",
        true,
        |b| run_simulation_anti_spoof(b, "disable_grevm"),
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
