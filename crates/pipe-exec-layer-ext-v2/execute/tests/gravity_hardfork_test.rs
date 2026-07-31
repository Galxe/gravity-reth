#![allow(missing_docs)]

//! Integration smoke for Gravity hardfork genesis parsing + pipe boot.
//!
//! Boots a single reth node from `gravity_hardfork.json`, verifies that
//! `betaTime` is parsed as a timestamp fork condition (the EIP-7702 lockdown
//! release gate), and pushes empty blocks through the MockConsensus /
//! PipeExecLayerApi pipeline.

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
use reth_ethereum_forks::Hardforks;
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

/// `betaTime` in `gravity_hardfork.json` — far-future so lockdown stays on during the
/// empty-block push; this test only verifies parse, not the Beta transition itself.
const BETA_TIME: u64 = 9_999_999_999;

/// How many blocks past genesis to push (smoke that the pipe still works).
const TARGET_BLOCKS_PAST_GENESIS: u64 = 30;

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn new_ordered_block(
    epoch: u64,
    block_number: u64,
    block_id: B256,
    parent_block_id: B256,
) -> OrderedBlock {
    OrderedBlock {
        failed_proposer_indices: vec![],
        epoch,
        parent_id: parent_block_id,
        id: block_id,
        number: block_number,
        timestamp_us: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()
            as u64,
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

struct MockConsensus<Storage, EthApi> {
    pipeline_api: PipeExecLayerApi<Storage, EthApi>,
}

impl<Storage, EthApi> MockConsensus<Storage, EthApi>
where
    Storage: GravityStorage,
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn new(pipeline_api: PipeExecLayerApi<Storage, EthApi>) -> Self {
        Self { pipeline_api }
    }

    async fn run(self, latest_block_number: u64) {
        let Self { pipeline_api } = self;
        let mut epoch: u64 = pipeline_api
            .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
            .unwrap()
            .try_into()
            .unwrap();
        println!(
            "[hardfork_test] latest_block_number={latest_block_number}, epoch={epoch}, betaTime={BETA_TIME}"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Push a short empty-block run to exercise the pipe under the current hardfork schedule.
        let target_block = latest_block_number + TARGET_BLOCKS_PAST_GENESIS;
        for block_number in latest_block_number + 1..=target_block {
            let block_id = mock_block_id(block_number);
            let parent_block_id = mock_block_id(block_number - 1);
            pipeline_api
                .push_ordered_block(new_ordered_block(
                    epoch,
                    block_number,
                    block_id,
                    parent_block_id,
                ))
                .unwrap();
            let result = pipeline_api.pull_executed_block_hash().await.unwrap();
            assert_eq!(result.block_number, block_number);
            assert_eq!(result.block_id, block_id);
            pipeline_api.commit_executed_block_hash(block_id, Some(result.block_hash)).unwrap();

            // Handle epoch transitions
            for event in &result.gravity_events {
                match event {
                    GravityEvent::NewEpoch(new_epoch, _) => {
                        assert_eq!(*new_epoch, epoch + 1);
                        pipeline_api.wait_for_block_persistence(block_number).await.unwrap();
                        let stored_epoch: u64 = pipeline_api
                            .fetch_config_bytes(
                                OnChainConfig::Epoch,
                                BlockNumber::Number(block_number),
                            )
                            .unwrap()
                            .try_into()
                            .unwrap();
                        assert_eq!(stored_epoch, *new_epoch);
                        // Push stale epoch block
                        pipeline_api
                            .push_ordered_block(new_ordered_block(
                                epoch,
                                block_number + 1,
                                mock_block_id(block_number + 1),
                                block_id,
                            ))
                            .unwrap();
                        epoch = *new_epoch;
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        println!("[hardfork_test] ✅ Pushed through block {target_block}.");
    }
}

async fn run_pipe(
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

    // Verify betaTime is parsed as a timestamp fork condition (lockdown release gate).
    assert!(
        chain_spec
            .gravity_hardforks()
            .is_fork_active_at_timestamp(GravityHardfork::Beta, BETA_TIME),
        "Beta must be active at betaTime={BETA_TIME}"
    );
    assert!(
        !chain_spec
            .gravity_hardforks()
            .is_fork_active_at_timestamp(GravityHardfork::Beta, BETA_TIME - 1),
        "Beta must be inactive at betaTime-1"
    );
    // Fail-closed check: with far-future betaTime, lockdown is still active at "now".
    assert!(
        reth_chainspec::is_eip7702_lockdown_active(chain_spec.as_ref(), 0),
        "lockdown must stay on before betaTime"
    );
    println!("[hardfork_test] ✅ ChainSpec correctly parsed betaTime={BETA_TIME}");

    let eth_api = handle.node.rpc_registry.eth_api().clone();
    let provider = handle.node.provider;

    let db_provider = provider.database_provider_ro().unwrap();
    let latest_block_number = db_provider.best_block_number().unwrap();
    let latest_block_hash = db_provider.block_hash(latest_block_number).unwrap().unwrap();
    let latest_block_header = db_provider.header_by_number(latest_block_number).unwrap().unwrap();
    drop(db_provider);

    println!("[hardfork_test] latest_block_header: {:?}", latest_block_header);
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

    // Run consensus — push blocks past hardfork boundaries
    let consensus = MockConsensus::new(pipeline_api);
    consensus.run(latest_block_number).await;

    println!("[hardfork_test] ✅ All blocks pushed successfully. Hardfork framework verified.");

    Ok(())
}

#[test]
fn test_beta_hardfork_parse_smoke() {
    std::panic::set_hook(Box::new({
        |panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            eprintln!("Panic occurred: {panic_info}\nBacktrace:\n{backtrace}");
            std::process::exit(1);
        }
    }));

    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(
            LogFormat::Terminal,
            LevelFilter::INFO.to_string(),
            "".to_string(),
            Some("always".to_string()),
        ))
        .init();

    let runner = CliRunner::try_default_runtime().unwrap();
    let command: NodeCommand<EthereumChainSpecParser> = NodeCommand::try_parse_args_from([
        "reth",
        "--chain",
        "gravity_hardfork.json",
        "--with-unused-ports",
        "--dev",
        "--datadir",
        "data/gravity_hardfork_test",
    ])
    .unwrap();

    runner
        .run_command_until_exit(|ctx| {
            command.execute(
                ctx,
                FnLauncher::new::<EthereumChainSpecParser, _>(|builder, _| async move {
                    run_pipe(builder).await
                }),
            )
        })
        .unwrap();

    // Give background threads time to exit cleanly
    std::thread::sleep(Duration::from_secs(2));
}
