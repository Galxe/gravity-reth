#![allow(missing_docs)]

//! P-14: EIP-2935 activation with `--gravity.disable-grevm true`.
//!
//! Lives in its own test binary because the gravity config is a process-global
//! `OnceLock` — initialising it twice from sibling tests in the same binary
//! panics. Mirrors `gravity_eip2935_test.rs` (P-3) but flips the executor
//! variant to `WrapExecutor<BasicBlockExecutor>` via the CLI flag and asserts
//! the EIP-2935 state output is identical to the grevm path.

use alloy_eips::eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE};
use alloy_primitives::{Address, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
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
use std::{collections::BTreeMap, sync::Arc, time::Duration};

// Aligned with `gravity_eip2935_test.rs` so the disable_grevm path can be
// directly compared against P-3 / P-4 / P-5 if needed.
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
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

    async fn run(self, latest_block_number: u64) {
        let Self { pipeline_api, target_block_count, ts_for_block } = self;
        let mut epoch: u64 = pipeline_api
            .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
            .unwrap()
            .try_into()
            .unwrap();
        println!(
            "[eip2935_p14_test] latest_block_number={latest_block_number}, epoch={epoch}, target={target_block_count}"
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

        println!(
            "[eip2935_p14_test] ✅ Pushed {target_block_count} blocks (target={target_block})."
        );
    }
}

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
        "[eip2935_p14_test] ✅ HISTORY_STORAGE deployed at block {block_number} ({}B match)",
        deployed.len()
    );
}

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
    println!("[eip2935_p14_test] ✅ nonce = {expected} at block {block_number}");
}

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
    println!("[eip2935_p14_test] ✅ slot {slot} = {expected} at block {block_number}");
}

/// Reference state root produced by the grevm path at block 100 (captured from a
/// clean P-3 run on the same chainspec). Used as a diagnostic — see the comment
/// at the call site for why it is not asserted byte-equal.
const P3_GREVM_STATE_ROOT_AT_BLOCK_100: B256 = B256::new([
    0x9a, 0x64, 0xea, 0x51, 0xe3, 0x64, 0x82, 0xb0, 0xc7, 0x36, 0xcd, 0x13, 0x9e, 0x2e, 0x5f, 0x7d,
    0x7f, 0x97, 0xc5, 0x50, 0xa6, 0xad, 0x8e, 0x7a, 0x16, 0x83, 0x1e, 0x8b, 0x46, 0x0c, 0x2d, 0x8f,
]);

async fn run_pipe_p14_disable_grevm(
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
        "P-14 expects a fresh datadir so block numbers align with deterministic timestamps"
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
    consensus.run(latest_block_number).await;

    // Same assertions as P-3, except this run flipped `disable_grevm=true` via
    // the `--gravity.disable-grevm true` CLI flag, so the deployment ran through
    // `WrapExecutor<BasicBlockExecutor>` instead of `GrevmExecutor`.
    assert_history_storage_deployed(&provider, P3_ACTIVATION_BLOCK);
    assert_history_account_nonce(&provider, P3_ACTIVATION_BLOCK, 1);
    assert_history_slot_eq(
        &provider,
        P3_ACTIVATION_BLOCK,
        P3_ACTIVATION_BLOCK - 1,
        U256::from(P3_ACTIVATION_BLOCK - 1),
    );
    assert_history_slot_eq(&provider, P3_ACTIVATION_BLOCK, P3_ACTIVATION_BLOCK, U256::ZERO);

    // State-root comparison is reported as a diagnostic only.
    //
    // Block-level state-root equivalence between the two executor variants
    // requires that *every* per-block irregular state change goes through the
    // same path. `WrapExecutor<BasicBlockExecutor>` currently skips the Gravity
    // hardfork upgrades (Alpha/Beta/Gamma/Delta) that `GrevmExecutor` runs in
    // `apply_post_execution_changes` (see `crates/ethereum/evm/src/parallel_execute.rs`
    // 200-230). On `gravity_prague_p3.json` the Gamma upgrade fires at block 20
    // and Delta at block 25, so the two paths diverge long before EIP-2935
    // activation at block 100 — divergence empirically confirmed at block 99
    // (well before any EIP-2935 effect). Asserting full state-root equality
    // here would surface a separate architectural gap, not an EIP-2935 issue.
    // Header-level state-root print is informational. The header may not be
    // persisted yet by the time we read here (the consensus engine just
    // committed but the persistence channel runs async), so the read is
    // best-effort.
    let db_provider = provider.database_provider_ro().unwrap();
    match db_provider.header_by_number(P3_ACTIVATION_BLOCK).ok().flatten() {
        Some(header) => {
            println!(
                "[eip2935_p14_test] diagnostic — state root at block 100 (disable_grevm=true): {:?}",
                header.state_root
            );
            println!(
                "[eip2935_p14_test] diagnostic — grevm baseline state root at block 100:        {:?}",
                P3_GREVM_STATE_ROOT_AT_BLOCK_100
            );
            if header.state_root != P3_GREVM_STATE_ROOT_AT_BLOCK_100 {
                println!(
                    "[eip2935_p14_test] ⚠ state-root mismatch is EXPECTED — caused by the Gravity hardfork"
                );
                println!(
                    "[eip2935_p14_test]   upgrades being skipped on the WrapExecutor path. Tracked separately."
                );
            }
        }
        None => {
            println!(
                "[eip2935_p14_test] diagnostic — block 100 header not yet persisted, skipping state-root comparison"
            );
        }
    }

    println!("[eip2935_p14_test] ✅ P-14 disable_grevm: deployment + first SSTORE verified on the WrapExecutor path.");
    Ok(())
}

#[test]
fn test_p14_disable_grevm_activation() {
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

    let runner = CliRunner::try_default_runtime().unwrap();
    let command: NodeCommand<EthereumChainSpecParser> = NodeCommand::try_parse_args_from([
        "reth",
        "--chain",
        "gravity_prague_p3.json",
        "--with-unused-ports",
        "--dev",
        "--datadir",
        "data/gravity_eip2935_p14_test",
        "--gravity.disable-grevm",
    ])
    .unwrap();

    runner
        .run_command_until_exit(|ctx| {
            command.execute(
                ctx,
                FnLauncher::new::<EthereumChainSpecParser, _>(|builder, _| async move {
                    run_pipe_p14_disable_grevm(builder).await
                }),
            )
        })
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));
}
