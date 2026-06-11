//! Repro: `disable_grevm` path drops executor-level custom precompiles → cross-node
//! state-root divergence / consensus halt.
//! Finding id: wrapexecutor-custom-precompiles-noop-grevm-divergence (severity: high)
//!
//! ## The bug
//!
//! The pipe layer registers user-reachable executor-level custom precompiles — in
//! particular the BLS pop-verify precompile bound to
//! `BLS_PRECOMPILE_ADDR = 0x…1625f5001` — and applies them to whatever executor it
//! built, via `executor.apply_custom_precompiles(self.custom_precompiles.clone())`
//! (crates/pipe-exec-layer-ext-v2/execute/src/lib.rs, execute_ordered_block;
//! custom_precompiles built in the same file from
//! `vec![(BLS_PRECOMPILE_ADDR, create_bls_pop_verify_precompile())]`).
//!
//! Which executor gets built is chosen by a *per-node, non-consensus-bound* toggle
//! (`crates/ethereum/evm/src/lib.rs:299`; `crates/gravity-primitives/src/config.rs:38`
//! reads it from the `GRETH_DISABLE_GREVM` env var, or the `--gravity.disable-grevm`
//! CLI flag):
//!   - default              → `GrevmExecutor`
//!   - disable_grevm        → `WrapExecutor<BasicBlockExecutor>`
//!
//! On the **grevm** path `GrevmExecutor::apply_custom_precompiles` stores the set and
//! forwards it into `Scheduler::new`, so EVERY user transaction in the block sees the
//! BLS precompile (crates/ethereum/evm/src/parallel_execute.rs).
//!
//! On the **disable_grevm** path `WrapExecutor::apply_custom_precompiles` is a verbatim
//! no-op (crates/evm/evm/src/parallel_execute.rs:141-146 — body is just the comment
//! "// TODO(Ashin Gau): How does basic executor handle custom precompiles"). The wrapped
//! `BasicBlockExecutor`'s block-execution EVM never receives the custom precompiles
//! (the only `apply_precompile` sites are inside `transact_system_txn`, which only inject
//! per-system-txn precompiles, not the executor-level user set).
//!
//! ## Observable divergence
//!
//! A user tx that CALLs `BLS_PRECOMPILE_ADDR` with 144 bytes of input and ≥110_000 gas:
//!   - grevm node:         precompile runs → consumes flat `POP_VERIFY_GAS = 110_000`,
//!                         returns a 32-byte ABI bool.
//!   - disable_grevm node: address has no precompile → CALL to an empty account returns
//!                         success with empty output, consuming only the call's base cost
//!                         and refunding the rest.
//! Different `gas_used` → different gas charged to the sender → different post-state →
//! **different block state root** for the SAME consensus-ordered block. In a mixed
//! cluster (some nodes with the toggle, some without) the two groups compute different
//! state roots → make_canonical / state-root comparison disagrees → chain halts (or the
//! minority forks). A single crafted user tx is enough.
//!
//! ## Why this test re-execs itself
//!
//! `get_gravity_config()` caches the config in a process-global `OnceLock`
//! (config.rs:27/36), so grevm vs disable_grevm cannot be toggled twice in one process.
//! Worse, the node CLI calls `init_gravity_config(gravity.to_config())`
//! (crates/cli/commands/src/node.rs:182) which sets that `OnceLock` from the
//! `--gravity.disable-grevm` CLI flag, *shadowing* the `GRETH_DISABLE_GREVM` env var
//! entirely (a child with only the env var set still logs `disable_grevm: false`). So the
//! toggle MUST go through the CLI flag, and only one value can take effect per process.
//!
//! Therefore the comparison runs each path in its OWN child process: the parent test
//! re-execs THIS test binary twice (once plain → grevm; once with `--gravity.disable-grevm`
//! → disable_grevm), each child runs the `child_run_bls_call_block` worker and prints a
//! single machine-readable `REPRO_OUTCOME=<state_root>,<gas_used>,<balance>` line. The
//! parent parses both lines and asserts they are EQUAL.
//!
//! ## Observed at HEAD f39cdf39a (bug reproduced)
//!
//!   - grevm:         gas_used=264_095, state_root=0x642e52cd…04ad9e
//!   - disable_grevm: gas_used=154_095, state_root=0xd751dea9…3f929b
//!
//! The gas delta is EXACTLY `POP_VERIFY_GAS = 110_000`: the BLS precompile ran on grevm
//! and did NOT run on disable_grevm (a control tx to a plain empty address yields the same
//! 154_095 the disable_grevm path produces). Different gas → different sender balance →
//! different state root for the SAME ordered block.
//!
//!   - Buggy HEAD (f39cdf39a): the two outcomes DIFFER → assertion fails → bug reproduced.
//!   - Correct behavior (after `WrapExecutor::apply_custom_precompiles` forwards the set
//!     into `BasicBlockExecutor`): the two outcomes are identical → assertion passes.
//!
//! Harness for the worker mirrors `gravity_bls_precompile_test.rs` (each tests-as-binary
//! file is intentionally self-contained; there is no shared tests/common module).

use alloy_consensus::TxEip7702;
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
use alloy_primitives::{address, Address, Bytes, Signature, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
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
use reth_ethereum_primitives::{Transaction, TransactionSigned};
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// chainId from `gravity_hardfork.json`.
const CHAIN_ID: u64 = 7771625;

/// Block N's timestamp = (P3_TS_BASE + N) * 1_000_000 (us).
/// `pragueTime = P3_TS_BASE + P3_ACTIVATION_BLOCK` activates Prague exactly at
/// block 100 (the EIP-7702 transaction type requires Prague).
const P3_TS_BASE: u64 = 2_000_000_000;
const P3_ACTIVATION_BLOCK: u64 = 100;
const PRAGUE_TS_BLOCK_100: u64 = P3_TS_BASE + P3_ACTIVATION_BLOCK;

/// BLS pop-verify precompile address (`onchain_config/mod.rs:BLS_PRECOMPILE_ADDR`).
const BLS_PRECOMPILE_ADDR: Address = address!("00000000000000000000000000000001625f5001");

/// Input length the precompile expects: pubkey(48) + pop(96) = 144 bytes.
const BLS_INPUT_LEN: usize = 144;

/// Gas limit chosen so that, after the EIP-7702 intrinsic cost, MORE than
/// `POP_VERIFY_GAS = 110_000` is forwarded to the precompile, so the call SUCCEEDS on
/// the grevm path (charging the flat 110_000). On the disable_grevm path the same call
/// hits an empty account and succeeds with empty output, consuming far less gas. The
/// resulting gas-charge / state-root divergence is the bug under test.
const SUCCEED_GAS_LIMIT: u64 = 500_000;

/// Env var the parent sets to tell the child to pass `--gravity.disable-grevm` to the
/// node CLI. NOTE: the executor toggle MUST go through the CLI flag, not the
/// `GRETH_DISABLE_GREVM` env var: the node CLI calls `init_gravity_config(gravity.to_config())`
/// (crates/cli/commands/src/node.rs:182) which sets the process-global `OnceLock` from the
/// CLI flag, so any later `get_gravity_config()` env read is shadowed. Empirically a child
/// run with only the env var set still logs `disable_grevm: false`.
const DISABLE_GREVM_FLAG_ENV: &str = "REPRO_DISABLE_GREVM_FLAG";

/// Env var set by the parent to tell a re-exec'd child to run the worker test and which
/// datadir to use (so the two children do not share state).
const CHILD_DATADIR_ENV: &str = "REPRO_CHILD_DATADIR";

/// Prefix of the single machine-readable line the child prints for the parent to parse.
const OUTCOME_PREFIX: &str = "REPRO_OUTCOME=";

fn gravity_prague_chainspec(prague_time: Option<u64>) -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../gravity_hardfork.json"))
            .expect("gravity_hardfork.json must parse as JSON");
    if let Some(ts) = prague_time {
        json["config"]["pragueTime"] = serde_json::json!(ts);
    }
    json.to_string()
}

/// Anvil account 0 — pre-funded in `gravity_hardfork.json`.
const FUNDED_PRIVKEY_HEX: &[u8; 32] = &[
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];

/// Delegation target for the authorization tuple (only there to make the 7702 tx
/// valid; irrelevant to triggering the bug).
const TARGET_ADDR: Address = address!("0x0000000000000000000000000000000000001234");

fn funded_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::from(*FUNDED_PRIVKEY_HEX))
        .expect("funded test key must parse")
}

fn authority_signer(seed: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[31] = seed;
    PrivateKeySigner::from_bytes(&B256::from(bytes)).expect("authority key must parse")
}

fn mock_block_id(block_number: u64) -> B256 {
    B256::left_padding_from(&block_number.to_be_bytes())
}

fn p3_ts_us(block_number: u64) -> u64 {
    (P3_TS_BASE + block_number) * 1_000_000
}

fn sign_authorization(
    signer: &PrivateKeySigner,
    chain_id: u64,
    target: Address,
    nonce: u64,
) -> SignedAuthorization {
    let auth = Authorization { chain_id: U256::from(chain_id), address: target, nonce };
    let sig: Signature =
        signer.sign_hash_sync(&auth.signature_hash()).expect("auth signing must succeed");
    auth.into_signed(sig)
}

/// Build and sign an EIP-7702 transaction calling `to`, returning the signed tx and the
/// sender address. The 7702 tx type is used only to reuse the existing harness
/// (Prague@100); the bug is triggered by the CALL to `to` (the BLS precompile) itself,
/// independent of the transaction type.
fn build_signed_eip7702_tx(
    sender: &PrivateKeySigner,
    nonce: u64,
    gas_limit: u64,
    to: Address,
    input: Bytes,
    authorization_list: Vec<SignedAuthorization>,
) -> (TransactionSigned, Address) {
    use alloy_consensus::SignableTransaction;

    let tx = TxEip7702 {
        chain_id: CHAIN_ID,
        nonce,
        gas_limit,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to,
        value: U256::ZERO,
        access_list: Default::default(),
        authorization_list,
        input,
    };
    let sig_hash = tx.signature_hash();
    let signature: Signature = sender.sign_hash_sync(&sig_hash).expect("tx signing must succeed");
    let signed = tx.into_signed(signature);
    let (tx, sig, _hash) = signed.into_parts();
    let signed_tx = TransactionSigned::new_unhashed(Transaction::Eip7702(tx), sig);
    let _ = signed_tx.hash();
    (signed_tx, sender.address())
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

fn ordered_block_with_txs(
    epoch: u64,
    block_number: u64,
    block_id: B256,
    parent_block_id: B256,
    timestamp_us: u64,
    transactions: Vec<TransactionSigned>,
    senders: Vec<Address>,
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
        transactions,
        senders,
        proposer_index: Some(0),
        extra_data: vec![],
        randomness: U256::ZERO,
    }
}

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
}

/// Read an account's nonce at the given block height.
fn account_nonce<P: StateProviderFactory>(provider: &P, block_number: u64, addr: Address) -> u64 {
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("state provider");
    state.basic_account(&addr).expect("read account").map(|a| a.nonce).unwrap_or_default()
}

async fn boot_pipeline(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<(
    Arc<reth_chainspec::ChainSpec>,
    impl StateProviderFactory + HeaderProvider + BlockHashReader + BlockNumReader + Clone,
    PipeExecLayerApi<
        BlockViewStorage<
            BlockchainProvider<
                reth_node_api::NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>,
            >,
        >,
        impl EthCall<NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>>,
    >,
    u64,
)> {
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

    assert_eq!(latest_block_number, 0, "tests expect a fresh datadir");

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

    Ok((chain_spec, provider, pipeline_api, latest_block_number))
}

// ---------------------------------------------------------------------------
// Outcome of one executor path's run of the BLS-precompile-calling block.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RunOutcome {
    /// state_root of block 100 (the block containing the BLS-precompile-calling tx).
    state_root: B256,
    /// gas_used recorded in block 100's header.
    gas_used: u64,
    /// sender balance right after block 100.
    sender_balance: U256,
}

impl RunOutcome {
    fn encode(&self) -> String {
        format!(
            "{OUTCOME_PREFIX}{:#x},{},{:#x}",
            self.state_root, self.gas_used, self.sender_balance
        )
    }

    fn parse_from_stdout(stdout: &str) -> Option<Self> {
        let line = stdout.lines().find(|l| l.trim_start().starts_with(OUTCOME_PREFIX))?;
        let payload = line.trim_start().strip_prefix(OUTCOME_PREFIX)?;
        let mut it = payload.split(',');
        let state_root = it.next()?.trim().parse::<B256>().ok()?;
        let gas_used = it.next()?.trim().parse::<u64>().ok()?;
        let sender_balance = it.next()?.trim().parse::<U256>().ok()?;
        Some(Self { state_root, gas_used, sender_balance })
    }
}

/// Boots the pipeline, advances to block 99, then executes block 100 containing exactly
/// one user tx that CALLs the BLS precompile (144 bytes of input, enough gas to succeed),
/// and returns the resulting block-100 state root, gas_used, and sender balance.
async fn run_bls_call_block(
    builder: WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, ChainSpec>>,
) -> eyre::Result<RunOutcome> {
    use alloy_consensus::BlockHeader;

    let (_chain_spec, provider, pipeline_api, latest_block_number) = boot_pipeline(builder).await?;

    let mut epoch: u64 = pipeline_api
        .fetch_config_bytes(OnChainConfig::Epoch, BlockNumber::Latest)
        .unwrap()
        .try_into()
        .unwrap();

    let consensus = MockConsensus::new(pipeline_api, Box::new(p3_ts_us));
    // Advance to the block just before Prague activation (block 99).
    consensus.push_empty_range(&mut epoch, latest_block_number + 1, P3_ACTIVATION_BLOCK - 1).await;

    // Block 100: a single user tx that CALLs the BLS precompile with 144 bytes of input
    // and enough gas to SUCCEED. On grevm the precompile runs (flat 110_000 gas + 32-byte
    // output); on disable_grevm the address is empty (success, empty output, tiny gas).
    let sender = funded_signer();
    let authority = authority_signer(0x42);
    let auth = sign_authorization(&authority, CHAIN_ID, TARGET_ADDR, 0);
    let (tx, sender_addr) = build_signed_eip7702_tx(
        &sender,
        0,
        SUCCEED_GAS_LIMIT,
        BLS_PRECOMPILE_ADDR,
        Bytes::from(vec![0u8; BLS_INPUT_LEN]),
        vec![auth],
    );

    let block = ordered_block_with_txs(
        epoch,
        P3_ACTIVATION_BLOCK,
        mock_block_id(P3_ACTIVATION_BLOCK),
        mock_block_id(P3_ACTIVATION_BLOCK - 1),
        p3_ts_us(P3_ACTIVATION_BLOCK),
        vec![tx],
        vec![sender_addr],
    );

    consensus.push_one(&mut epoch, block).await;
    let pipeline_api = consensus.pipeline_api;
    pipeline_api.wait_for_block_persistence(P3_ACTIVATION_BLOCK).await.unwrap();

    // Sanity: the tx executed (nonce advanced) on BOTH paths — this is not an OOG/dropped
    // tx test; the divergence is in HOW MUCH gas the call consumes.
    let nonce = account_nonce(&provider, P3_ACTIVATION_BLOCK, sender_addr);
    assert_eq!(nonce, 1, "the BLS-calling tx must execute and advance the sender nonce");

    let header = provider
        .header_by_number(P3_ACTIVATION_BLOCK)
        .expect("header read")
        .expect("header persisted");

    let sender_balance = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(P3_ACTIVATION_BLOCK))
        .expect("state provider")
        .basic_account(&sender_addr)
        .expect("read account")
        .map(|a| a.balance)
        .unwrap_or_default();

    Ok(RunOutcome {
        state_root: header.state_root(),
        gas_used: header.gas_used(),
        sender_balance,
    })
}

// ---------------------------------------------------------------------------
// Child worker test: invoked in a fresh process by the parent. Selects grevm or
// disable_grevm purely via the GRETH_DISABLE_GREVM env var (config.rs:38) — no CLI flag,
// so we never touch the OnceLock twice in one process. Prints REPRO_OUTCOME=... .
// ---------------------------------------------------------------------------

#[test]
fn child_run_bls_call_block() {
    // Only run when invoked as a child (parent sets CHILD_DATADIR_ENV). When cargo runs
    // the whole binary normally, this test is a no-op so it never collides with the
    // parent's own re-exec and never sets the OnceLock for the parent process.
    let Ok(datadir) = std::env::var(CHILD_DATADIR_ENV) else {
        eprintln!("[child] not a child invocation (no {CHILD_DATADIR_ENV}); skipping");
        return;
    };

    // Fresh datadir per child.
    let _ = std::fs::remove_dir_all(&datadir);

    let disable_grevm = std::env::var(DISABLE_GREVM_FLAG_ENV).is_ok();
    let chain_spec = gravity_prague_chainspec(Some(PRAGUE_TS_BLOCK_100));
    let leaked_datadir: &'static str = Box::leak(datadir.into_boxed_str());

    let outcome = run_worker(&chain_spec, leaked_datadir, disable_grevm, run_bls_call_block);
    // Single machine-readable line for the parent to parse.
    println!("{}", outcome.encode());
}

// ---------------------------------------------------------------------------
// Parent test: re-exec this binary twice (grevm vs disable_grevm), compare outcomes.
// ---------------------------------------------------------------------------

/// CONSENSUS-SAFETY REPRO.
///
/// Runs the identical BLS-precompile-calling ordered block through BOTH executor paths
/// (each in its own child process) and asserts they agree. The custom-precompile set is
/// consensus-relevant: every node MUST compute the same post-state for the same ordered
/// block, regardless of the `disable_grevm` per-node toggle.
///
///   - Buggy HEAD: `WrapExecutor::apply_custom_precompiles` is a no-op, so the
///     disable_grevm child never sees the BLS precompile → different gas_used / state_root
///     → this test FAILS (the bug is reproduced).
///   - Correct: both children produce identical state_root / gas_used → this test PASSES.
#[test]
fn test_custom_precompiles_state_root_must_match_across_grevm_and_disable_grevm() {
    let grevm = run_child(false, "data/repro_wrapexec_custom_precompiles_grevm")
        .expect("grevm child must produce a REPRO_OUTCOME line");
    let disable_grevm = run_child(true, "data/repro_wrapexec_custom_precompiles_disable_grevm")
        .expect("disable_grevm child must produce a REPRO_OUTCOME line");

    println!("[repro] grevm         outcome = {grevm:?}");
    println!("[repro] disable_grevm  outcome = {disable_grevm:?}");

    // The core consensus invariant: same ordered block ⇒ same post-state on every node.
    assert_eq!(
        grevm.gas_used, disable_grevm.gas_used,
        "BUG: grevm and disable_grevm computed DIFFERENT block gas_used for the same \
         ordered block — the disable_grevm path dropped the executor-level BLS precompile \
         (WrapExecutor::apply_custom_precompiles is a no-op). grevm={} disable_grevm={}",
        grevm.gas_used, disable_grevm.gas_used
    );
    assert_eq!(
        grevm.sender_balance, disable_grevm.sender_balance,
        "BUG: grevm and disable_grevm charged the sender DIFFERENT gas for the same tx \
         (BLS precompile present on grevm, absent on disable_grevm)."
    );
    assert_eq!(
        grevm.state_root, disable_grevm.state_root,
        "BUG: grevm and disable_grevm produced DIFFERENT block state roots for the SAME \
         consensus-ordered block. In a mixed cluster this is a state-root split / chain \
         halt. Root cause: WrapExecutor::apply_custom_precompiles (disable_grevm path) is \
         a TODO no-op, so the user-reachable BLS precompile at 0x…1625f5001 is never \
         injected into BasicBlockExecutor's block-execution EVM."
    );
}

/// Re-exec THIS test binary, running only `child_run_bls_call_block`, with the given
/// executor toggle and datadir. Returns the child's parsed outcome.
fn run_child(disable_grevm: bool, datadir: &str) -> Option<RunOutcome> {
    let exe = std::env::current_exe().expect("current test binary path");
    let mut cmd = std::process::Command::new(exe);
    cmd.args([
        "--exact",
        "child_run_bls_call_block",
        "--nocapture",
        "--test-threads",
        "1",
    ])
    .env(CHILD_DATADIR_ENV, datadir)
    // The child translates this into the `--gravity.disable-grevm` CLI flag (the env-var
    // route is shadowed by the CLI-driven init_gravity_config; see DISABLE_GREVM_FLAG_ENV).
    .env_remove(DISABLE_GREVM_FLAG_ENV);
    if disable_grevm {
        cmd.env(DISABLE_GREVM_FLAG_ENV, "1");
    }

    let output = cmd.output().expect("spawn child test process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!(
        "[parent] child (disable_grevm={disable_grevm}) status={:?}\n--- child stdout ---\n{stdout}\n--- child stderr ---\n{stderr}",
        output.status
    );
    assert!(
        output.status.success(),
        "child (disable_grevm={disable_grevm}) did not exit successfully"
    );
    RunOutcome::parse_from_stdout(&stdout)
}

// ---------------------------------------------------------------------------
// Worker harness — boots the node CLI in-process (mirrors gravity_bls_precompile_test.rs)
// WITHOUT the --gravity.disable-grevm flag; the executor selection comes purely from the
// GRETH_DISABLE_GREVM env var so the OnceLock is set at most once per child process.
// ---------------------------------------------------------------------------

fn run_worker<F, Fut, R>(
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
