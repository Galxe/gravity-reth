#![allow(missing_docs)]

//! Integration test for BLOCKHASH / Aptos consensus `block_id` alignment.
//!
//! Boots a single Gravity reth node, drives `MockConsensus + PipeExecLayerApi`
//! through a handful of blocks, then exercises:
//!
//! 1. **EVM `BLOCKHASH(n)` via historical `eth_call`** — must return the `block_id` for `n` (or
//!    `0x0` outside the 256-block opcode window), NEVER the keccak header hash.
//! 2. **`eth_getBlockByNumber(n).hash`** — must continue to return the standard
//!    `keccak(rlp(header))`, distinct from the `block_id`.
//! 3. **`tables::BlockNumberToBlockId`** — gets populated as canonical blocks advance through
//!    `BlockViewStorage::update_canonical`.
//!
//! Mirrors the structure of `gravity_eip2935_test.rs`; deliberately runs in the
//! "Prague never fires" chainspec branch so this test exercises only the
//! BLOCKHASH alignment plumbing, not EIP-2935 deployment.

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{address, map::AddressHashMap, Address, Bytes, B256, U256};
use alloy_rpc_types_eth::{
    state::{AccountOverride, EvmOverrides, StateOverride},
    TransactionRequest,
};
use gravity_api_types::{
    config_storage::{BlockNumber, ConfigStorage, OnChainConfig},
    events::contract_event::GravityEvent,
};
use gravity_storage::{block_view_storage::BlockViewStorage, GravityStorage};
use reth_chainspec::ChainSpec;
use reth_cli_commands::{launcher::FnLauncher, NodeCommand};
use reth_cli_runner::CliRunner;
use reth_db::{tables, transaction::DbTx, DatabaseEnv};
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_builder::{EngineNodeLauncher, NodeBuilder, WithLaunchContext};
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_pipe_exec_layer_ext_v2::{
    new_pipe_exec_layer_api, ExecutionArgs, OrderedBlock, PipeExecLayerApi,
};
use reth_provider::{
    providers::BlockchainProvider, BlockHashReader, BlockNumReader, BlockNumberToBlockIdReader,
    DatabaseProviderFactory, HeaderProvider,
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

/// Number of blocks pushed through the pipe-exec layer.
const TOTAL_BLOCKS: u64 = 20;

/// Anchor for the per-block timestamp so the timestamps are deterministic and
/// remain pre-Prague (we don't want EIP-2935 deployment to interfere).
const TS_BASE: u64 = 1_700_000_000;

/// Build a Gravity chainspec with `pragueTime` deep in the future so this test
/// exercises only the BLOCKHASH alignment plumbing.
fn gravity_blockhash_alignment_chainspec() -> String {
    const PRAGUE_FAR_FUTURE: u64 = 9_999_999_999;
    let raw = include_str!("../gravity_hardfork.json");
    let mut value: serde_json::Value =
        serde_json::from_str(raw).expect("gravity_hardfork.json must parse");
    let config = value
        .get_mut("config")
        .and_then(|v| v.as_object_mut())
        .expect("gravity_hardfork.json must contain `config`");
    config.insert("pragueTime".to_string(), serde_json::Value::from(PRAGUE_FAR_FUTURE));
    serde_json::to_string(&value).expect("re-serialize patched chainspec")
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
}

fn mock_block_id(block_number: u64) -> B256 {
    // High-entropy block_id distinct from any plausible keccak header hash.
    // Pattern: 0xBL00CKID_<block_number_big_endian>_padding...
    let mut bytes = [0xAB_u8; 32];
    bytes[..8].copy_from_slice(&block_number.to_be_bytes());
    B256::from(bytes)
}

fn ts_us(block_number: u64) -> u64 {
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

    async fn run(self, latest_block_number: u64) -> PipeExecLayerApi<Storage, EthApi> {
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

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        pipeline_api
    }
}

/// EVM bytecode for the BLOCKHASH probe contract.
///
/// ```text
/// PUSH1 0          // offset 0 — read first 32 bytes of calldata as `n`
/// CALLDATALOAD     // stack: n
/// BLOCKHASH        // stack: BLOCKHASH(n)
/// PUSH1 0
/// MSTORE           // store result at memory[0..32]
/// PUSH1 0x20       // size
/// PUSH1 0          // offset
/// RETURN
/// ```
///
/// Hex: `60003540600052602060 00F3`.
const BLOCKHASH_PROBE_BYTECODE: &[u8] = &[
    0x60, 0x00, // PUSH1 0
    0x35, // CALLDATALOAD
    0x40, // BLOCKHASH
    0x60, 0x00, // PUSH1 0
    0x52, // MSTORE
    0x60, 0x20, // PUSH1 0x20
    0x60, 0x00, // PUSH1 0
    0xF3, // RETURN
];

const PROBE_ADDR: Address = address!("0xBB00bb00bb00bb00bb00bb00bb00bb00bb00bb00");

/// Build an `eth_call` request that runs `BLOCKHASH(n)` against the probe
/// contract injected via `state_overrides`. Returns 32 bytes — the value the
/// EVM observed for `BLOCKHASH(n)`.
fn blockhash_probe_request(n: u64) -> TransactionRequest {
    let calldata = Bytes::copy_from_slice(B256::from(U256::from(n)).as_slice());
    TransactionRequest::default().to(PROBE_ADDR).input(calldata.into())
}

fn state_override_with_probe() -> StateOverride {
    let mut map: AddressHashMap<AccountOverride> = AddressHashMap::default();
    map.insert(
        PROBE_ADDR,
        AccountOverride {
            code: Some(Bytes::from_static(BLOCKHASH_PROBE_BYTECODE)),
            ..Default::default()
        },
    );
    map
}

async fn run_pipe_blockhash_alignment(
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
        "BLOCKHASH alignment test expects a fresh datadir so block numbers align with deterministic timestamps"
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

    let consensus = MockConsensus::new(pipeline_api, TOTAL_BLOCKS, Box::new(ts_us));
    let pipeline_api = consensus.run(latest_block_number).await;

    // Let advance_persistence catch up: the `BlockViewStorage::update_canonical`
    // path that writes the `BlockNumberToBlockId` table runs synchronously, but
    // the engine-tree persistence task that flushes Headers/CanonicalHeaders is
    // async. We wait for the tip to be persisted before asserting on the table.
    pipeline_api.wait_for_block_persistence(TOTAL_BLOCKS).await.unwrap();
    // A small grace window — `update_canonical` opens its own RW tx and may
    // commit slightly after `wait_for_block_persistence` returns.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let now = now_us();
    println!(
        "[blockhash_alignment_test] consensus produced {TOTAL_BLOCKS} blocks (test_start_us={now})"
    );

    // -------------------------------------------------------------------------
    // (1) `BlockNumberToBlockId` table is populated for every committed block.
    // -------------------------------------------------------------------------
    let db_provider = provider.database_provider_ro().unwrap();
    for block_number in 1..=TOTAL_BLOCKS {
        let stored = db_provider
            .tx_ref()
            .get::<tables::BlockNumberToBlockId>(block_number)
            .expect("read BlockNumberToBlockId")
            .unwrap_or_else(|| {
                panic!("BlockNumberToBlockId must hold an entry for committed block {block_number}")
            });
        assert_eq!(
            stored,
            mock_block_id(block_number),
            "BlockNumberToBlockId at {block_number}: stored {stored} != mock_block_id"
        );
    }
    drop(db_provider);
    println!(
        "[blockhash_alignment_test] ✅ BlockNumberToBlockId populated for blocks 1..={TOTAL_BLOCKS}"
    );

    // -------------------------------------------------------------------------
    // (2) `BlockchainProvider::block_id_by_number` returns block_id.
    // -------------------------------------------------------------------------
    for block_number in 1..=TOTAL_BLOCKS {
        let via_provider = provider
            .block_id_by_number(block_number)
            .expect("provider.block_id_by_number")
            .unwrap_or_else(|| panic!("missing block_id for block {block_number}"));
        assert_eq!(via_provider, mock_block_id(block_number));
    }
    println!(
        "[blockhash_alignment_test] ✅ BlockchainProvider::block_id_by_number returns block_id"
    );

    // -------------------------------------------------------------------------
    // (3) `eth_getBlockByNumber(n).hash` still returns the keccak header hash
    //     (the design separates EVM-visible hash from RPC-visible hash).
    // -------------------------------------------------------------------------
    let db_provider = provider.database_provider_ro().unwrap();
    for block_number in 1..=TOTAL_BLOCKS {
        let keccak_hash = db_provider
            .block_hash(block_number)
            .expect("provider.block_hash")
            .unwrap_or_else(|| panic!("missing keccak hash for block {block_number}"));
        assert_ne!(
            keccak_hash,
            mock_block_id(block_number),
            "eth_getBlockByNumber({block_number}).hash must be keccak header hash, distinct from block_id"
        );
    }
    drop(db_provider);
    println!("[blockhash_alignment_test] ✅ eth_getBlockByNumber(n).hash != block_id (preserved)");

    // -------------------------------------------------------------------------
    // (4) Historical `eth_call` BLOCKHASH(n) returns block_id for in-window n.
    //     BLOCKHASH opcode caps at `current - 256`, so for block N we can probe
    //     anything in [N-256, N-1]. With N=20 and probe range [1, N-1], every
    //     probe is in window.
    // -------------------------------------------------------------------------
    let exec_block = TOTAL_BLOCKS; // probe @ block N == TOTAL_BLOCKS
    let probe_at = Some(BlockId::Number((exec_block).into()));
    let overrides = EvmOverrides::new(Some(state_override_with_probe()), None);

    for n in 1..exec_block {
        let ret = eth_api
            .call(blockhash_probe_request(n), probe_at, overrides.clone())
            .await
            .unwrap_or_else(|err| panic!("eth_call(BLOCKHASH({n})) failed: {err:?}"));
        let observed = B256::from_slice(ret.as_ref());
        assert_eq!(
            observed,
            mock_block_id(n),
            "eth_call(BLOCKHASH({n})) at block {exec_block}: got {observed}, expected block_id={}",
            mock_block_id(n)
        );
    }
    println!(
        "[blockhash_alignment_test] ✅ eth_call BLOCKHASH(n) returns block_id for n ∈ [1, {})",
        exec_block - 1
    );

    // -------------------------------------------------------------------------
    // (5) BLOCKHASH(n) outside the 256-block opcode window returns 0x0.
    //     The opcode itself enforces this; we pick `n = exec_block + 1` (future
    //     block) which is also guaranteed to be missing.
    // -------------------------------------------------------------------------
    let ret = eth_api
        .call(blockhash_probe_request(exec_block + 1), probe_at, overrides.clone())
        .await
        .expect("eth_call(BLOCKHASH(future)) should succeed (returns 0x0)");
    assert_eq!(
        B256::from_slice(ret.as_ref()),
        B256::ZERO,
        "BLOCKHASH(future) must return 0x0, not block_id and not keccak"
    );
    println!("[blockhash_alignment_test] ✅ BLOCKHASH(future) → 0x0");

    // -------------------------------------------------------------------------
    // (6) BLOCKHASH(current_block) returns 0x0. The opcode rejects the
    //     currently executing block.
    // -------------------------------------------------------------------------
    let ret = eth_api
        .call(blockhash_probe_request(exec_block), probe_at, overrides.clone())
        .await
        .expect("eth_call(BLOCKHASH(current)) should succeed (returns 0x0)");
    assert_eq!(
        B256::from_slice(ret.as_ref()),
        B256::ZERO,
        "BLOCKHASH(current_block) must return 0x0 by opcode contract"
    );
    println!("[blockhash_alignment_test] ✅ BLOCKHASH(current) → 0x0");

    println!(
        "[blockhash_alignment_test] ✅ All BLOCKHASH alignment invariants hold at TOTAL_BLOCKS={TOTAL_BLOCKS}."
    );
    Ok(())
}

#[test]
fn test_blockhash_alignment_grevm() {
    run_pipe_e2e_test(
        &gravity_blockhash_alignment_chainspec(),
        "data/gravity_blockhash_alignment_grevm_test",
        false,
        run_pipe_blockhash_alignment,
    );
}

#[test]
fn test_blockhash_alignment_disable_grevm() {
    run_pipe_e2e_test(
        &gravity_blockhash_alignment_chainspec(),
        "data/gravity_blockhash_alignment_disable_grevm_test",
        true,
        run_pipe_blockhash_alignment,
    );
}

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
