#![allow(missing_docs)]

//! Integration test for the **Epsilon** Gravity hardfork (gravity-reth#364 / gravity-audit#720).
//!
//! Boots a single reth node from `gravity_hardfork.json` patched with a per-test `epsilonTime`,
//! drives `MockConsensus + PipeExecLayerApi` across the activation boundary, and asserts that
//! `SYSTEM_CALLER`'s sentinel balance is positive pre-fork and exactly **zero** from the activation
//! block onward, with its nonce preserved (the account survives as an identity). Run on BOTH the
//! grevm and the serial (`WrapExecutor`) backends — they must agree.

use alloy_primitives::{address, Address, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use gravity_api_types::{
    config_storage::{BlockNumber, ConfigStorage, OnChainConfig},
    events::contract_event::GravityEvent,
};
use gravity_storage::{block_view_storage::BlockViewStorage, GravityStorage};
use reth_chainspec::{ChainSpec, EthChainSpec, GravityHardfork};
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
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// SYSTEM_CALLER — the system-transaction sender, funded with a sentinel balance in genesis.
const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000");

/// Block N is pushed with `timestamp = TS_BASE + N` (seconds), so `epsilonTime = TS_BASE +
/// ACTIVATION_BLOCK` makes block `ACTIVATION_BLOCK` the activation block (its parent's ts is
/// `epsilonTime - 1`, its own ts is `epsilonTime` ⇒ `transitions_at_timestamp` fires once).
const TS_BASE: u64 = 2_000_000_000;
const ACTIVATION_BLOCK: u64 = 10;
const EPSILON_TS: u64 = TS_BASE + ACTIVATION_BLOCK;
/// Drive a few blocks past the boundary to prove the balance stays zero (gas-exempt).
const TARGET_BLOCK_COUNT: u64 = ACTIVATION_BLOCK + 4;

/// Load `gravity_hardfork.json` and patch `config.epsilonTime`. SYSTEM_CALLER is already funded
/// with the sentinel balance in that genesis, so nothing else needs preloading.
fn gravity_epsilon_chainspec(epsilon_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = epsilon_time {
        json["config"]["epsilonTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn ts_for_block(block_number: u64) -> u64 {
    (TS_BASE + block_number) * 1_000_000
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

    async fn run(self, latest_block_number: u64) {
        let Self { pipeline_api, target_block_count, ts_for_block } = self;
        let mut epoch: u64 = pipeline_api
            .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
            .unwrap()
            .try_into()
            .unwrap();

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

            // Bump epoch on NewEpoch events so the pipeline stays live (mirrors the other
            // hardfork integration tests).
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
    }
}

fn system_caller_account<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
) -> (U256, u64) {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for SYSTEM_CALLER");
    state
        .basic_account(&SYSTEM_CALLER)
        .expect("read basic_account")
        .map(|a| (a.balance, a.nonce))
        .unwrap_or((U256::ZERO, 0))
}

async fn run_pipe_epsilon(
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

    // epsilonTime parsed ⇒ Epsilon transitions exactly on ACTIVATION_BLOCK's timestamp.
    assert!(
        chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Epsilon)
            .transitions_at_timestamp(EPSILON_TS, EPSILON_TS - 1),
        "Epsilon must transition at epsilonTime={EPSILON_TS}"
    );

    let eth_api = handle.node.rpc_registry.eth_api().clone();
    let provider = handle.node.provider;

    let db_provider = provider.database_provider_ro().unwrap();
    let latest_block_number = db_provider.best_block_number().unwrap();
    let latest_block_hash = db_provider.block_hash(latest_block_number).unwrap().unwrap();
    let latest_block_header = db_provider.header_by_number(latest_block_number).unwrap().unwrap();
    drop(db_provider);

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

    let consensus = MockConsensus::new(pipeline_api, TARGET_BLOCK_COUNT, Box::new(ts_for_block));
    consensus.run(latest_block_number).await;

    let before = ACTIVATION_BLOCK - 1;
    let after = ACTIVATION_BLOCK + 4;
    let (bal_before, _) = system_caller_account(&provider, before);
    let (bal_at, nonce_at) = system_caller_account(&provider, ACTIVATION_BLOCK);
    let (bal_after, _) = system_caller_account(&provider, after);

    println!(
        "[epsilon_test] SYSTEM_CALLER balance: block {before}={bal_before}, block {ACTIVATION_BLOCK}={bal_at}, block {after}={bal_after}; nonce@{ACTIVATION_BLOCK}={nonce_at}"
    );

    assert!(
        bal_before > U256::ZERO,
        "SYSTEM_CALLER must be funded pre-fork at block {before}, got {bal_before}"
    );
    assert_eq!(
        bal_at,
        U256::ZERO,
        "SYSTEM_CALLER balance must be zeroed at Epsilon activation block {ACTIVATION_BLOCK}, got {bal_at}"
    );
    assert_eq!(
        bal_after,
        U256::ZERO,
        "SYSTEM_CALLER must stay zero post-fork at block {after}, got {bal_after}"
    );
    assert!(
        nonce_at > 0,
        "SYSTEM_CALLER nonce must be preserved (>0) at block {ACTIVATION_BLOCK} — kept as identity"
    );

    println!(
        "[epsilon_test] ✅ SYSTEM_CALLER zeroed at Epsilon activation, stayed zero post-fork, nonce preserved."
    );
    Ok(())
}

#[test]
fn test_epsilon_zeroes_system_caller_grevm() {
    run_pipe_e2e_test(
        &gravity_epsilon_chainspec(Some(EPSILON_TS)),
        "data/gravity_epsilon_grevm_test",
        false,
        run_pipe_epsilon,
    );
}

#[test]
fn test_epsilon_zeroes_system_caller_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_epsilon_chainspec(Some(EPSILON_TS)),
        "data/gravity_epsilon_disable_grevm_test",
        true,
        run_pipe_epsilon,
    );
}

// `run_pipe_e2e_test` is the single entry point each #[test] dispatches through: it builds the
// NodeCommand CLI args, optionally appending `--gravity.disable-grevm`, and drives the CliRunner —
// so each case is sampled twice, once on the grevm path and once on the WrapExecutor path. The
// datadir is wiped first so re-runs are deterministic.
fn run_pipe_e2e_test<F, Fut>(chain_spec: &str, datadir: &'static str, disable_grevm: bool, run_fn: F)
where
    F: FnOnce(WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = eyre::Result<()>> + Send + 'static,
{
    init_panic_hook_and_tracer();
    let _ = std::fs::remove_dir_all(datadir);

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
