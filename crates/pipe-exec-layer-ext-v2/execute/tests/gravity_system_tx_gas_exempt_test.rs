#![allow(missing_docs)]

//! Integration tests for the Gravity Alpha hardfork system-tx gas-exempt + balance
//! migration bundle (acceptance matrix §2.1 / §2.2 — **must-pass**).
//!
//! Boots a single reth node from `gravity_hardfork.json` patched with a per-test
//! `alphaTime` and drives `MockConsensus + PipeExecLayerApi` across the Alpha
//! activation boundary (block T-1 → T → T+i).
//!
//! Covers two acceptance rows:
//!
//! - §2.1: fork-activation block — receipts / balance / nonce / `transactions_root` behave as
//!   documented across T-1 / T / T+i. Pre-Alpha block T-1 keeps the sentinel SYSTEM_CALLER balance;
//!   activation block T zeroes it via `system_caller_migration::apply_state_changes_for_block`;
//!   subsequent blocks keep executing 1 metadata system tx each while balance stays 0.
//!
//! - §2.2: dual-backend parity — running the same Alpha-crossing block sequence under both grevm
//!   (parallel) and `--gravity.disable-grevm` (serial `WrapExecutor`) yields **identical** state
//!   roots at every block, including T-1 (pre-fork), T (activation), and T+i (post-fork). Pinned by
//!   the shared `apply_state_change` channel + serial/grevm-symmetric `transact_system_txn`
//!   exemption.
//!
//! Naming and scaffolding mirror `gravity_eip2935_test.rs` /
//! `gravity_bls_precompile_test.rs` so the matrix row maps 1:1 to a `cargo
//! nextest` target.

use alloy_consensus::BlockHeader;
use alloy_primitives::{address, Address, B256, U256};
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
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

// ---------------------------------------------------------------------------
// Activation parameters
// ---------------------------------------------------------------------------

/// Block N's timestamp (seconds) = `ALPHA_TS_BASE + N`; `alphaTime` is aligned so
/// the predicate `transitions_at_timestamp(parent_ts = ALPHA_TS_BASE + T - 1,
/// current_ts = ALPHA_TS_BASE + T)` flips true exactly at block T.
const ALPHA_TS_BASE: u64 = 2_000_000_000;
const ALPHA_ACTIVATION_BLOCK: u64 = 100;
const ALPHA_TIME: u64 = ALPHA_TS_BASE + ALPHA_ACTIVATION_BLOCK;

/// Last block we push in the activation runner: covers T-1, T, T+1..T+5. The
/// extra post-activation blocks pin the "post-fork system txs keep executing
/// gas-exempt" half of §2.1.
const POST_ACTIVATION_TIP: u64 = ALPHA_ACTIVATION_BLOCK + 5;

/// `SYSTEM_CALLER = 0x…625f0000` — see `crates/chainspec/src/gravity.rs`. We
/// inline the constant to avoid pulling in `reth-chainspec` as a hard test dep
/// (the tests crate `reth-pipe-exec-layer-ext-v2` already re-exports the
/// address via its lib, but the constant `SYSTEM_CALLER` is the literal source
/// of truth and a regression in either path is caught by §1.4 unit tests).
const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000");

/// Patch `alphaTime` (a `ForkCondition::Timestamp(...)` field) on the embedded
/// `gravity_hardfork.json`, returning an in-memory JSON string the CLI parser
/// accepts as `--chain`. Pattern lifted verbatim from
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

fn alpha_ts_us(block_number: u64) -> u64 {
    (ALPHA_TS_BASE + block_number) * 1_000_000
}

fn _now_us(_block_number: u64) -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
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
// MockConsensus — drives the pipe layer for empty blocks. Each empty block ends
// up with exactly 1 protocol-injected system tx (the metadata `onBlockStart`
// from `execute_system_transactions`).
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
// State assertion helpers
// ---------------------------------------------------------------------------

fn system_caller_account<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
) -> Option<reth_primitives_traits::account::Account> {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider for SYSTEM_CALLER read");
    state.basic_account(&SYSTEM_CALLER).expect("read basic_account")
}

fn state_root_at<P: HeaderProvider>(provider: &P, block_number: u64) -> B256 {
    provider
        .header_by_number(block_number)
        .expect("header read must succeed")
        .expect("header must be persisted")
        .state_root()
}

// ---------------------------------------------------------------------------
// Core e2e runner — pushes blocks 1..=POST_ACTIVATION_TIP across the Alpha
// boundary, asserts the §2.1 invariants block-by-block, returns the per-block
// state-root map so the §2.2 dual-backend test can assert byte-equality.
// ---------------------------------------------------------------------------

async fn run_alpha_e2e(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
    label: &'static str,
) -> eyre::Result<BTreeMap<u64, B256>> {
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
        "alpha activation runner expects a fresh datadir so block numbers align with deterministic timestamps ({label})"
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

    println!("[alpha_e2e {label}] epoch={epoch}, pushing blocks 1..={POST_ACTIVATION_TIP}");

    let consensus = MockConsensus::new(pipeline_api, Box::new(alpha_ts_us));
    consensus.push_empty_range(&mut epoch, 1, POST_ACTIVATION_TIP).await;

    let pipeline_api = consensus.into_inner();
    pipeline_api.wait_for_block_persistence(POST_ACTIVATION_TIP).await.unwrap();
    drop(pipeline_api);

    // -------------------------------------------------------------------
    // §2.1 / pre-Alpha sanity (T-1 = block 99)
    // -------------------------------------------------------------------
    //
    // Block 99 has ts = ALPHA_TS_BASE + 99 < ALPHA_TIME, so
    // `is_system_tx_gas_exempt` returns false: the system tx is built with
    // `gas_price = base_fee`, the cfg lever is off, and SYSTEM_CALLER pays fee
    // out of the genesis sentinel balance. The sentinel is so large
    // (`0x1999…9999` ≈ 1.16×10⁵⁸) that the balance after 99 such payments is
    // still well above 1×10⁵⁷ — we only sanity-check it stayed above a
    // conservative floor, not its exact value (which depends on the exact
    // gas_used for the metadata system tx and would couple this test to
    // contract bytecode).
    let acc_t_minus_1 = system_caller_account(&provider, ALPHA_ACTIVATION_BLOCK - 1)
        .expect("SYSTEM_CALLER must exist pre-Alpha (genesis alloc populates it)");
    let pre_alpha_floor = U256::from(10).pow(U256::from(50));
    assert!(
        acc_t_minus_1.balance > pre_alpha_floor,
        "[alpha_e2e {label}] pre-Alpha SYSTEM_CALLER.balance must still be sentinel-sized at block {} (got {}, floor 10^50)",
        ALPHA_ACTIVATION_BLOCK - 1,
        acc_t_minus_1.balance,
    );
    assert_eq!(
        acc_t_minus_1.nonce,
        ALPHA_ACTIVATION_BLOCK - 1,
        "[alpha_e2e {label}] pre-Alpha SYSTEM_CALLER.nonce must equal block 99 (1 metadata tx per block, starting from genesis nonce=0)"
    );

    // -------------------------------------------------------------------
    // §2.1 / activation block (T = block 100)
    // -------------------------------------------------------------------
    //
    // The migration hook fires once on T:
    //   - balance: sentinel → 0
    //   - nonce:   preserved from pre-state (= 99), then incremented by the metadata system tx to
    //     100.
    //   - code/code_hash: unchanged (the alloc has no code; we still check the account stays
    //     non-empty under EIP-161 thanks to nonce > 0).
    let acc_t = system_caller_account(&provider, ALPHA_ACTIVATION_BLOCK).expect(
        "SYSTEM_CALLER must still exist after balance zeroing (EIP-161 not pruned, nonce>0)",
    );
    assert_eq!(
        acc_t.balance,
        U256::ZERO,
        "[alpha_e2e {label}] activation block SYSTEM_CALLER.balance must be zeroed by `system_caller_migration::apply_state_changes_for_block`",
    );
    assert_eq!(
        acc_t.nonce, ALPHA_ACTIVATION_BLOCK,
        "[alpha_e2e {label}] activation block SYSTEM_CALLER.nonce must reflect migration preservation + 1 metadata system tx at T",
    );

    // -------------------------------------------------------------------
    // §2.1 / post-activation blocks (T+1..T+5)
    // -------------------------------------------------------------------
    //
    // After T, every empty block still ships exactly 1 metadata system tx —
    // L1 (cfg-side disable_base_fee + disable_balance_check) + L2 (gas_price=0
    // construction) let SYSTEM_CALLER keep paying nothing while gas is still
    // metered. The migration hook is a no-op on these blocks
    // (transitions_at_timestamp returns false).
    for n in (ALPHA_ACTIVATION_BLOCK + 1)..=POST_ACTIVATION_TIP {
        let acc = system_caller_account(&provider, n)
            .expect("SYSTEM_CALLER must remain present across post-Alpha blocks");
        assert_eq!(
            acc.balance,
            U256::ZERO,
            "[alpha_e2e {label}] post-Alpha SYSTEM_CALLER.balance must stay 0 at block {n} (L1 + L2 levers prevent any fee charge)",
        );
        assert_eq!(
            acc.nonce, n,
            "[alpha_e2e {label}] post-Alpha SYSTEM_CALLER.nonce must monotonically increase 1-per-block at block {n}",
        );
    }

    // -------------------------------------------------------------------
    // Collect per-block state_root for §2.2 dual-backend comparison. Every
    // block in the sequence — including T-1 — is captured so a divergence
    // before activation (e.g. baseline drift unrelated to Alpha) shows up
    // distinctly from a divergence at/after T.
    // -------------------------------------------------------------------
    let mut state_roots: BTreeMap<u64, B256> = BTreeMap::new();
    for n in (ALPHA_ACTIVATION_BLOCK - 1)..=POST_ACTIVATION_TIP {
        state_roots.insert(n, state_root_at(&provider, n));
    }
    println!("[alpha_e2e {label}] per-block state_roots: {:?}", state_roots);
    Ok(state_roots)
}

// ---------------------------------------------------------------------------
// §2.1 grevm + serial — assert all per-block invariants under both backends.
// (The dual-backend equivalence below re-uses the same runner but compares
// roots without re-running assertions.)
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_system_tx_alpha_activation_grevm() {
    let state_roots = run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME),
        "data/gravity_system_tx_gas_exempt_grevm_test",
        false,
        |b| run_alpha_e2e(b, "grevm"),
    );
    assert!(!state_roots.is_empty(), "runner must return non-empty state-root map");
}

#[test]
fn test_e2e_system_tx_alpha_activation_disable_grevm() {
    let state_roots = run_pipe_e2e_test(
        &gravity_alpha_chainspec(ALPHA_TIME),
        "data/gravity_system_tx_gas_exempt_disable_grevm_test",
        true,
        |b| run_alpha_e2e(b, "disable_grevm"),
    );
    assert!(!state_roots.is_empty(), "runner must return non-empty state-root map");
}

// ---------------------------------------------------------------------------
// §2.2 dual-backend state-root equivalence across the Alpha boundary.
//
// Drives the same activation runner once under grevm and once under
// `--gravity.disable-grevm`, then asserts the per-block state-root maps match
// byte-for-byte at every block in {T-1, T, T+1..T+5}. The pre-Alpha entry
// (T-1) guards against baseline drift (anything that diverges before the fork
// must be a pre-existing serial/grevm bug, not the Alpha bundle). T and T+i
// pin both lever paths plus the migration hook.
//
// Two CLI invocations live inside one #[test] function — each `run_pipe_e2e_test`
// drives its own CliRunner against an isolated datadir, so the second
// invocation re-uses the same harness without inheriting state.
// ---------------------------------------------------------------------------

#[test]
fn test_e2e_dual_backend_state_root_equivalence_across_alpha_boundary() {
    let chain_spec = gravity_alpha_chainspec(ALPHA_TIME);

    let grevm_roots = run_pipe_e2e_test(
        &chain_spec,
        "data/gravity_system_tx_gas_exempt_dual_grevm",
        false,
        |b| run_alpha_e2e(b, "dual/grevm"),
    );
    let serial_roots = run_pipe_e2e_test(
        &chain_spec,
        "data/gravity_system_tx_gas_exempt_dual_serial",
        true,
        |b| run_alpha_e2e(b, "dual/serial"),
    );

    assert_eq!(
        grevm_roots.len(),
        serial_roots.len(),
        "dual-backend runs must produce state-root maps of equal cardinality (grevm={}, serial={})",
        grevm_roots.len(),
        serial_roots.len()
    );

    for (block, grevm_root) in &grevm_roots {
        let serial_root = serial_roots
            .get(block)
            .unwrap_or_else(|| panic!("serial run is missing state_root for block {block}"));
        assert_eq!(
            grevm_root, serial_root,
            "block {block} state_root divergence: grevm={grevm_root:?} serial={serial_root:?} — Alpha bundle (L1 cfg + L2 gas_price + migration hook) must be serial==grevm symmetric"
        );
    }

    println!(
        "[alpha_e2e dual] all {} block state-roots match across grevm + serial backends",
        grevm_roots.len()
    );
}

// ---------------------------------------------------------------------------
// Shared CLI harness — boots a single reth node with the supplied chainspec
// JSON and datadir, optionally forcing the serial executor via
// `--gravity.disable-grevm`. Returns whatever the launcher closure produced.
// Lifted from `gravity_bls_precompile_test::run_pipe_e2e_test`.
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
