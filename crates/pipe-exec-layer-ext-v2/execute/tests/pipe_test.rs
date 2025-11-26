//! Test executing ETH mainnet history blocks using the Pipe Execution Layer.

use gravity_storage::block_view_storage::BlockViewStorage;
use reth_chainspec::ChainSpec;
use reth_cli_commands::{launcher::FnLauncher, NodeCommand};
use reth_cli_runner::CliRunner;
use reth_db::DatabaseEnv;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_api::NodeTypesWithDBAdapter;
use reth_node_builder::{EngineNodeLauncher, NodeBuilder, WithLaunchContext};
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_pipe_exec_layer_ext_v2::{new_pipe_exec_layer_api, ExecutionArgs, PipeExecLayerApi};
use reth_provider::{
    providers::BlockchainProvider, writer::UnifiedStorageWriter, BlockHashReader, BlockNumReader,
    BlockReader, DatabaseProviderFactory, HeaderProvider, TransactionVariant, TrieWriterV2,
};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tracing::info;

type Provider = BlockchainProvider<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>;

struct MockConsensus<EthApi> {
    pipeline_api: PipeExecLayerApi<BlockViewStorage<Provider>, EthApi>,
    provider: Provider,
}

impl<EthApi> MockConsensus<EthApi>
where
    EthApi: Send + Sync + 'static,
{
    fn new(
        pipeline_api: PipeExecLayerApi<BlockViewStorage<Provider>, EthApi>,
        provider: Provider,
    ) -> Self {
        Self { pipeline_api, provider }
    }

    async fn run(self) {
        let Self { pipeline_api, provider } = self;
        let (start_block, target_block) = {
            let db_provider = provider.database_provider_ro().unwrap();
            let start_block = db_provider.best_block_number().unwrap() + 1;
            let target_block = db_provider.last_block_number().unwrap();
            (start_block, target_block)
        };
        println!("start_block={start_block} target_block={target_block}");

        let pipeline_api = Arc::new(pipeline_api);
        let (block_header_tx, mut block_header_rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn({
            let pipeline_api = pipeline_api.clone();
            async move {
                for block_number in start_block..=target_block {
                    let block = provider
                        .sealed_block_with_senders(
                            block_number.into(),
                            TransactionVariant::WithHash,
                        )
                        .unwrap()
                        .unwrap();
                    let block_hash = block.hash();
                    let block_header = block.header().clone();
                    if pipeline_api.push_history_block(block).is_none() {
                        break;
                    }

                    block_header_tx.send((block_header, block_hash)).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });

        let (execution_result_tx, mut execution_result_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn({
            let pipeline_api = pipeline_api.clone();
            async move {
                let mut result_buffer = BTreeMap::new();
                let mut next_block_number = start_block;
                while let Some(result) = pipeline_api.pull_executed_block_hash().await {
                    if result.block_number == next_block_number {
                        execution_result_tx.send(result).unwrap();
                        next_block_number += 1;
                        while let Some((&block_number, _)) = result_buffer.first_key_value() {
                            if block_number == next_block_number {
                                execution_result_tx
                                    .send(result_buffer.pop_first().unwrap().1)
                                    .unwrap();
                                next_block_number += 1;
                            } else {
                                break;
                            }
                        }
                    } else if next_block_number < result.block_number {
                        result_buffer.insert(result.block_number, result);
                    } else {
                        panic!("next block number ({}) cannot be greater than pulled block number ({})", next_block_number, result.block_number);
                    }
                }
            }
        });

        while let Some((header, block_hash)) = block_header_rx.recv().await {
            let result = execution_result_rx.recv().await.unwrap();
            assert_eq!(result.block_hash, block_hash, "block_hash mismatch: {header:?}");
            pipeline_api
                .commit_executed_block_hash(result.block_id, Some(result.block_hash))
                .unwrap();
        }
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
    let eth_api = handle.node.rpc_registry.eth_api().clone();

    let provider: Provider = handle.node.provider;
    let db_provider = provider.database_provider_ro().unwrap();
    let latest_block_number = db_provider.best_block_number().unwrap();
    println!("The latest_block_number is {}", latest_block_number);
    let latest_block_hash = db_provider.block_hash(latest_block_number).unwrap().unwrap();
    let latest_block_header = db_provider.header_by_number(latest_block_number).unwrap().unwrap();
    drop(db_provider);

    // write new state root
    let tx = provider.database_provider_ro().unwrap().into_tx();
    let nested_hash = NestedStateRoot::new(&tx, None);
    let hashed_state = nested_hash.read_hashed_state(None).unwrap();
    let (root_hash, trie_updates) = nested_hash.calculate(&hashed_state).unwrap();
    let trie_write = provider.database_provider_rw().unwrap();
    trie_write.write_trie_updatesv2(&trie_updates).unwrap();
    UnifiedStorageWriter::commit_unwind(trie_write)?;

    info!("Calculate and write state root={root_hash}");

    // Load block number to hash from test data

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

    let consensus = MockConsensus::new(pipeline_api, provider);
    consensus.run().await;

    Ok(())
}

#[test]
fn test() {
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
            LevelFilter::DEBUG.to_string(),
            String::new(),
            Some("always".to_string()),
        ))
        .init();

    let datadir = std::env::var("PIPE_TEST_DATADIR").expect("Failed to get PIPE_TEST_DATADIR");

    let runner = CliRunner::try_default_runtime().unwrap();
    let command: NodeCommand<EthereumChainSpecParser> = NodeCommand::try_parse_args_from([
        "reth",
        "--chain",
        "mainnet",
        "--with-unused-ports",
        "--dev",
        "--datadir",
        &datadir,
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
}
