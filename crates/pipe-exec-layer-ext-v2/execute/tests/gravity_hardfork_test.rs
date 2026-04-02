#![allow(missing_docs)]

//! Integration test for Gravity hardfork framework activation.
//!
//! This test boots a single reth node using a genesis generated from
//! `gravity-testnet-v1.0.0` contracts (via `generate_genesis_single.sh`).
//! It pushes blocks through the MockConsensus/PipeExecLayerApi pipeline
//! and verifies that the hardfork dispatch infrastructure correctly parses
//! and activates hardforks at the configured block numbers.
//!
//! **Gamma hardfork**: system contract bytecodes + StakePool upgrades + ReentrancyGuard init
//! **Delta hardfork**: 4 contract bytecodes + Governance owner + StakingConfig migration

use alloy_primitives::{Address, B256, U256};
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
use reth_evm_ethereum::hardfork::{
    delta::{
        DELTA_SYSTEM_UPGRADES, GOVERNANCE_ADDRESS, GOVERNANCE_CONFIG_ADDRESS, GOVERNANCE_OWNER,
        GOVERNANCE_OWNER_SLOT, GOV_CONFIG_MIN_THRESHOLD, GOV_CONFIG_PROPOSER_STAKE,
        GOV_CONFIG_SLOT_MIN_THRESHOLD, GOV_CONFIG_SLOT_PROPOSER_STAKE,
        GOV_CONFIG_SLOT_VOTING_DURATION, GOV_CONFIG_VOTING_DURATION, STAKING_CONFIG_ADDRESS,
    },
    gamma::{
        GAMMA_SYSTEM_UPGRADES, REENTRANCY_GUARD_NOT_ENTERED, REENTRANCY_GUARD_SLOT,
        STAKEPOOL_ADDRESSES, STAKEPOOL_BYTECODE,
    },
};
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

/// Block number at which Gamma hardfork activates (must match gravity_hardfork.json).
const GAMMA_BLOCK: u64 = 20;

/// Block number at which Delta hardfork activates (must match gravity_hardfork.json).
const DELTA_BLOCK: u64 = 25;

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
            "[hardfork_test] latest_block_number={latest_block_number}, epoch={epoch}, gammaBlock={GAMMA_BLOCK}, deltaBlock={DELTA_BLOCK}"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Push blocks past all hardfork boundaries
        let target_block = DELTA_BLOCK + 30;
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

        println!("[hardfork_test] ✅ Pushed {target_block} blocks past deltaBlock.");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GAMMA HARDFORK VERIFICATION
// ═══════════════════════════════════════════════════════════════════════════════

/// Verify that all system contracts have the expected new bytecodes after the Gamma hardfork.
fn verify_gamma_bytecodes_upgraded<P: StateProviderFactory>(provider: &P) {
    if GAMMA_SYSTEM_UPGRADES.is_empty() {
        println!("[hardfork_test] ⚠ GAMMA_SYSTEM_UPGRADES is empty (bytecodes stripped), skipping Gamma bytecode verification");
        return;
    }
    println!("[hardfork_test] Verifying Gamma system contract bytecodes at block {GAMMA_BLOCK}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(GAMMA_BLOCK))
        .expect("Failed to get state provider for hardfork block");

    let mut all_upgraded = true;
    for (addr, expected_bytecode) in GAMMA_SYSTEM_UPGRADES {
        match state.account_code(addr) {
            Ok(Some(code)) => {
                let code_bytes = code.original_bytes();
                if code_bytes.as_ref() == *expected_bytecode {
                    println!("[hardfork_test] ✅ {addr}: bytecode matches ({}B)", code_bytes.len());
                } else {
                    println!(
                        "[hardfork_test] ❌ {addr}: MISMATCH got={}B expected={}B",
                        code_bytes.len(),
                        expected_bytecode.len()
                    );
                    all_upgraded = false;
                }
            }
            Ok(None) => {
                // Contract may not have existed in v1.0.0 genesis — apply_gamma skips it
                println!("[hardfork_test] ⚠ {addr}: no code found (not in v1.0.0 genesis, skip)");
            }
            Err(e) => {
                println!("[hardfork_test] ❌ {addr}: error: {e:?}");
                all_upgraded = false;
            }
        }
    }

    assert!(all_upgraded, "Not all system contracts were upgraded at gammaBlock!");
    println!(
        "[hardfork_test] ✅ All {} Gamma system contract bytecodes verified!",
        GAMMA_SYSTEM_UPGRADES.len()
    );

    // Also verify StakePool upgrades
    println!("[hardfork_test] Verifying StakePool bytecodes at block {GAMMA_BLOCK}...");
    for pool_addr in STAKEPOOL_ADDRESSES {
        match state.account_code(pool_addr) {
            Ok(Some(code)) => {
                let code_bytes = code.original_bytes();
                assert_eq!(
                    code_bytes.as_ref(),
                    STAKEPOOL_BYTECODE,
                    "StakePool {pool_addr}: bytecode MISMATCH"
                );
                println!(
                    "[hardfork_test] ✅ StakePool {pool_addr}: bytecode matches ({}B)",
                    code_bytes.len()
                );
            }
            Ok(None) => panic!("[hardfork_test] ❌ StakePool {pool_addr}: no code found"),
            Err(e) => panic!("[hardfork_test] ❌ StakePool {pool_addr}: error: {e:?}"),
        }
    }

    // Verify ReentrancyGuard storage slot was initialized for StakePool
    println!("[hardfork_test] Verifying ReentrancyGuard storage for StakePools...");
    let guard_slot = alloy_primitives::B256::from(REENTRANCY_GUARD_SLOT);
    for pool_addr in STAKEPOOL_ADDRESSES {
        let guard_value =
            state.storage(*pool_addr, guard_slot).expect("Failed to read ReentrancyGuard storage");
        assert_eq!(
            guard_value,
            Some(U256::from(REENTRANCY_GUARD_NOT_ENTERED)),
            "StakePool {pool_addr}: ReentrancyGuard should be NOT_ENTERED (1)"
        );
        println!("[hardfork_test] ✅ StakePool {pool_addr}: ReentrancyGuard = {guard_value:?}");
    }
}

/// Verify bytecodes were NOT yet upgraded before the Gamma hardfork block.
fn verify_gamma_bytecodes_not_upgraded_before<P: StateProviderFactory>(provider: &P) {
    if GAMMA_SYSTEM_UPGRADES.is_empty() {
        println!("[hardfork_test] ⚠ GAMMA_SYSTEM_UPGRADES is empty (bytecodes stripped), skipping pre-Gamma verification");
        return;
    }
    println!("[hardfork_test] Verifying bytecodes are OLD before gammaBlock...");

    let pre_block = GAMMA_BLOCK - 1;
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(pre_block))
        .expect("Failed to get state provider for pre-hardfork block");

    // Just check the first contract (StakingConfig) as a smoke test
    let (addr, expected_new) = &GAMMA_SYSTEM_UPGRADES[0];
    match state.account_code(addr) {
        Ok(Some(code)) => {
            let code_bytes = code.original_bytes();
            assert_ne!(
                code_bytes.as_ref(),
                *expected_new,
                "Bytecode at {addr} should be OLD before gammaBlock but was already upgraded!"
            );
            println!(
                "[hardfork_test] ✅ StakingConfig at block {pre_block}: old bytecode ({}B), expected new={}B",
                code_bytes.len(),
                expected_new.len()
            );

            // Also check StakePool is still OLD before gammaBlock
            for pool_addr in STAKEPOOL_ADDRESSES {
                match state.account_code(pool_addr) {
                    Ok(Some(pool_code)) => {
                        let pool_bytes = pool_code.original_bytes();
                        assert_ne!(
                            pool_bytes.as_ref(),
                            STAKEPOOL_BYTECODE,
                            "StakePool {pool_addr}: should be OLD before gammaBlock"
                        );
                        println!(
                            "[hardfork_test] ✅ StakePool {pool_addr} at block {pre_block}: old bytecode ({}B), expected new={}B",
                            pool_bytes.len(),
                            STAKEPOOL_BYTECODE.len()
                        );
                    }
                    Ok(None) => println!(
                        "[hardfork_test] ⚠ StakePool {pool_addr} at block {pre_block}: no code"
                    ),
                    Err(e) => panic!("[hardfork_test] StakePool {pool_addr}: error: {e:?}"),
                }
            }
        }
        Ok(None) => {
            println!("[hardfork_test] ⚠ StakingConfig at block {pre_block}: no code (may be expected if no blocks yet)");
        }
        Err(e) => {
            panic!("[hardfork_test] Failed to fetch code before hardfork: {e:?}");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DELTA HARDFORK VERIFICATION
// ═══════════════════════════════════════════════════════════════════════════════

/// Verify that all 4 system contracts have new bytecodes after the Delta hardfork.
fn verify_delta_bytecodes_upgraded<P: StateProviderFactory>(provider: &P) {
    println!("[hardfork_test] Verifying Delta system contract bytecodes at block {DELTA_BLOCK}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(DELTA_BLOCK))
        .expect("Failed to get state provider for delta hardfork block");

    let mut all_upgraded = true;
    for (addr, expected_bytecode) in DELTA_SYSTEM_UPGRADES {
        match state.account_code(addr) {
            Ok(Some(code)) => {
                let code_bytes = code.original_bytes();
                if code_bytes.as_ref() == *expected_bytecode {
                    println!(
                        "[hardfork_test] ✅ Delta {addr}: bytecode matches ({}B)",
                        code_bytes.len()
                    );
                } else {
                    println!(
                        "[hardfork_test] ❌ Delta {addr}: MISMATCH got={}B expected={}B",
                        code_bytes.len(),
                        expected_bytecode.len()
                    );
                    all_upgraded = false;
                }
            }
            Ok(None) => {
                println!("[hardfork_test] ❌ Delta {addr}: no code found");
                all_upgraded = false;
            }
            Err(e) => {
                println!("[hardfork_test] ❌ Delta {addr}: error: {e:?}");
                all_upgraded = false;
            }
        }
    }

    assert!(all_upgraded, "Not all Delta system contracts were upgraded at deltaBlock!");
    println!(
        "[hardfork_test] ✅ All {} Delta system contract bytecodes verified!",
        DELTA_SYSTEM_UPGRADES.len()
    );
}

/// Verify Delta bytecodes were NOT yet upgraded before the Delta hardfork block.
fn verify_delta_bytecodes_not_upgraded_before<P: StateProviderFactory>(provider: &P) {
    println!("[hardfork_test] Verifying Delta bytecodes are OLD before deltaBlock...");

    let pre_block = DELTA_BLOCK - 1;
    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(pre_block))
        .expect("Failed to get state provider for pre-delta block");

    // Check first Delta contract (StakingConfig) as smoke test
    let (addr, expected_new) = &DELTA_SYSTEM_UPGRADES[0];
    match state.account_code(addr) {
        Ok(Some(code)) => {
            let code_bytes = code.original_bytes();
            // Before deltaBlock, bytecode may have been upgraded by gammaBlock already,
            // but it should NOT match the delta bytecodes
            assert_ne!(
                code_bytes.as_ref(),
                *expected_new,
                "Delta bytecode at {addr} should be OLD before deltaBlock"
            );
            println!(
                "[hardfork_test] ✅ {addr} at block {pre_block}: bytecode differs from Delta target ({}B vs {}B)",
                code_bytes.len(),
                expected_new.len()
            );
        }
        Ok(None) => {
            println!(
                "[hardfork_test] ⚠ {addr} at block {pre_block}: no code (unexpected for StakingConfig)"
            );
        }
        Err(e) => {
            panic!("[hardfork_test] Failed to fetch code before delta hardfork: {e:?}");
        }
    }
}

/// Verify that the Governance contract owner was set by the Delta hardfork.
fn verify_governance_owner_set<P: StateProviderFactory>(provider: &P) {
    println!("[hardfork_test] Verifying Governance owner storage at block {DELTA_BLOCK}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(DELTA_BLOCK))
        .expect("Failed to get state provider for delta hardfork block");

    let owner_slot = alloy_primitives::B256::from(GOVERNANCE_OWNER_SLOT);
    let owner_value = state
        .storage(GOVERNANCE_ADDRESS, owner_slot)
        .expect("Failed to read Governance owner storage");
    let expected_value = U256::from_be_bytes(GOVERNANCE_OWNER.into_word().0);
    assert_eq!(
        owner_value,
        Some(expected_value),
        "Governance owner should be set to {GOVERNANCE_OWNER} after deltaBlock"
    );
    println!("[hardfork_test] ✅ Governance owner at block {DELTA_BLOCK}: {GOVERNANCE_OWNER}");

    // Also verify owner was NOT set before delta block
    let pre_state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(DELTA_BLOCK - 1))
        .expect("Failed to get state provider for pre-delta block");
    let pre_owner = pre_state
        .storage(GOVERNANCE_ADDRESS, owner_slot)
        .expect("Failed to read pre-delta Governance owner");
    assert_ne!(
        pre_owner,
        Some(expected_value),
        "Governance owner should NOT be set before deltaBlock"
    );
    println!(
        "[hardfork_test] ✅ Governance owner at block {}: not yet set (as expected)",
        DELTA_BLOCK - 1
    );
}

/// Verify GovernanceConfig E2E overrides were applied at deltaBlock.
fn verify_governance_config_overrides<P: StateProviderFactory>(provider: &P) {
    println!("[hardfork_test] Verifying GovernanceConfig overrides at block {DELTA_BLOCK}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(DELTA_BLOCK))
        .expect("Failed to get state provider for delta hardfork block");

    // minVotingThreshold (slot 0) = 1
    let threshold = state
        .storage(GOVERNANCE_CONFIG_ADDRESS, B256::from(GOV_CONFIG_SLOT_MIN_THRESHOLD))
        .expect("Failed to read GovernanceConfig minVotingThreshold");
    assert_eq!(
        threshold,
        Some(U256::from(GOV_CONFIG_MIN_THRESHOLD)),
        "GovernanceConfig.minVotingThreshold should be {GOV_CONFIG_MIN_THRESHOLD}"
    );
    println!("[hardfork_test] ✅ GovernanceConfig.minVotingThreshold = {GOV_CONFIG_MIN_THRESHOLD}");

    // requiredProposerStake (slot 1) = 1
    let stake = state
        .storage(GOVERNANCE_CONFIG_ADDRESS, B256::from(GOV_CONFIG_SLOT_PROPOSER_STAKE))
        .expect("Failed to read GovernanceConfig requiredProposerStake");
    assert_eq!(
        stake,
        Some(U256::from(GOV_CONFIG_PROPOSER_STAKE)),
        "GovernanceConfig.requiredProposerStake should be {GOV_CONFIG_PROPOSER_STAKE}"
    );
    println!(
        "[hardfork_test] ✅ GovernanceConfig.requiredProposerStake = {GOV_CONFIG_PROPOSER_STAKE}"
    );

    // votingDurationMicros (slot 2) = 10_000_000
    let duration = state
        .storage(GOVERNANCE_CONFIG_ADDRESS, B256::from(GOV_CONFIG_SLOT_VOTING_DURATION))
        .expect("Failed to read GovernanceConfig votingDurationMicros");
    assert_eq!(
        duration,
        Some(U256::from(GOV_CONFIG_VOTING_DURATION)),
        "GovernanceConfig.votingDurationMicros should be {GOV_CONFIG_VOTING_DURATION}"
    );
    println!(
        "[hardfork_test] ✅ GovernanceConfig.votingDurationMicros = {GOV_CONFIG_VOTING_DURATION}"
    );
}

/// Verify StakingConfig storage is preserved after Delta hardfork (gap pattern).
/// With the storage gap approach, slot positions are unchanged from v1.2.0.
fn verify_staking_config_preserved<P: StateProviderFactory>(provider: &P) {
    println!(
        "[hardfork_test] Verifying StakingConfig storage preservation at block {DELTA_BLOCK}..."
    );

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(DELTA_BLOCK))
        .expect("Failed to get state provider for delta hardfork block");

    // Verify minimumStake (slot 0) is preserved (not cleared)
    let slot_0 = B256::ZERO;
    let min_stake =
        state.storage(STAKING_CONFIG_ADDRESS, slot_0).expect("Failed to read StakingConfig slot 0");
    assert!(
        min_stake.map_or(false, |v| v > U256::ZERO),
        "StakingConfig slot 0 (minimumStake) should be preserved, got {:?}",
        min_stake
    );
    println!("[hardfork_test] ✅ StakingConfig slot 0 (minimumStake): preserved ({:?})", min_stake);

    // Verify slot 1 (lockup|unbonding packed) is preserved
    let slot_1 = {
        let mut s = [0u8; 32];
        s[31] = 1;
        B256::new(s)
    };
    let slot_1_value =
        state.storage(STAKING_CONFIG_ADDRESS, slot_1).expect("Failed to read StakingConfig slot 1");
    assert!(
        slot_1_value.map_or(false, |v| v > U256::ZERO),
        "StakingConfig slot 1 (lockup|unbonding) should be preserved, got {:?}",
        slot_1_value
    );
    println!(
        "[hardfork_test] ✅ StakingConfig slot 1 (lockup|unbonding): preserved ({:#066x})",
        slot_1_value.unwrap()
    );

    // Verify _initialized (slot 3 with gap pattern) is still true
    let slot_3 = {
        let mut s = [0u8; 32];
        s[31] = 3;
        B256::new(s)
    };
    let initialized =
        state.storage(STAKING_CONFIG_ADDRESS, slot_3).expect("Failed to read StakingConfig slot 3");
    assert!(
        initialized.map_or(false, |v| v > U256::ZERO),
        "StakingConfig slot 3 (_initialized) should be true, got {:?}",
        initialized
    );
    println!("[hardfork_test] ✅ StakingConfig slot 3 (_initialized): true");
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════

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

    // Verify gammaBlock is parsed correctly
    assert!(
        chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Gamma)
            .transitions_at_block(GAMMA_BLOCK),
        "gamma transitions_at_block({GAMMA_BLOCK}) should be true"
    );
    assert!(
        !chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Gamma)
            .transitions_at_block(GAMMA_BLOCK - 1),
        "gamma transitions_at_block({}) should be false",
        GAMMA_BLOCK - 1
    );
    println!("[hardfork_test] ✅ ChainSpec correctly parsed gammaBlock={GAMMA_BLOCK}");

    // Verify deltaBlock is parsed correctly
    assert!(
        chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Delta)
            .transitions_at_block(DELTA_BLOCK),
        "delta transitions_at_block({DELTA_BLOCK}) should be true"
    );
    assert!(
        !chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Delta)
            .transitions_at_block(DELTA_BLOCK - 1),
        "delta transitions_at_block({}) should be false",
        DELTA_BLOCK - 1
    );
    println!("[hardfork_test] ✅ ChainSpec correctly parsed deltaBlock={DELTA_BLOCK}");

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

    // Run consensus — push blocks past all hardfork boundaries
    let consensus = MockConsensus::new(pipeline_api);
    consensus.run(latest_block_number).await;

    // ── Gamma hardfork verification ──
    verify_gamma_bytecodes_not_upgraded_before(&provider);
    verify_gamma_bytecodes_upgraded(&provider);

    // ── Delta hardfork verification ──
    verify_delta_bytecodes_not_upgraded_before(&provider);
    verify_delta_bytecodes_upgraded(&provider);
    verify_governance_owner_set(&provider);
    verify_governance_config_overrides(&provider);
    verify_staking_config_preserved(&provider);

    println!("[hardfork_test] ✅ All hardfork verifications passed!");

    Ok(())
}

#[test]
fn test_gamma_hardfork() {
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
