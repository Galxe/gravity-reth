//! Pipe-layer injection for [`GravityHardfork::TestnetOwnerFix`].
//!
//! Runs after metadata/DKG/JWK system txs and before user txs. Constructs four
//! forced `transferOwnership` calls with `from = old_owner`, executes them via
//! `transact_system_txn` (Alpha gas levers), and returns [`SystemTxnResult`]s
//! whose `sender` is the corresponding `old_owner` (not [`SYSTEM_CALLER`]).
//!
//! One-shot gate matches Alpha / EIP-2935:
//! `transitions_at_timestamp(current_ts, parent_ts)` — fires only on the unique
//! Longevity activation block. Wrong chain / non-crossing block → empty vec.
//! Any forced-tx revert panics so the block is not committed as a success.

use crate::onchain_config::{new_system_call_txn, SystemTxnResult};
use reth_chainspec::{ChainSpec, EthChainSpec, GravityHardfork, LONGEVITY_TESTNET_CHAIN_ID};
use reth_evm::{
    execute::BlockExecutionError, parallel_execute::ParallelExecutor, EvmEnv, IntoTxEnv,
};
use reth_evm_ethereum::hardfork::testnet_owner_fix::{
    transfer_ownership_calldata, MigrationRow, MIGRATION_TABLE,
};
use reth_primitives::{EthPrimitives, Recovered};
use tracing::info;

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

/// Execute `TestnetOwnerFix` forced transfers on the Longevity activation block.
///
/// Returns an empty vec when `chain_id` is not Longevity or when
/// `parent_ts < fixTime <= current_ts` is false. Panics if any forced transfer
/// fails to execute or reverts.
pub(crate) fn execute_forced_transfers(
    executor: Executor<'_>,
    chain_spec: &ChainSpec,
    evm_env: EvmEnv,
    system_tx_gas_price: u128,
    block_number: u64,
    block_timestamp: u64,
    parent_timestamp: u64,
) -> Vec<SystemTxnResult> {
    if chain_spec.chain().id() != LONGEVITY_TESTNET_CHAIN_ID {
        return Vec::new();
    }
    if !chain_spec
        .gravity_hardforks()
        .fork(GravityHardfork::TestnetOwnerFix)
        .transitions_at_timestamp(block_timestamp, parent_timestamp)
    {
        return Vec::new();
    }

    let mut results = Vec::with_capacity(MIGRATION_TABLE.len());
    for row in &MIGRATION_TABLE {
        let result = execute_one(executor, evm_env.clone(), system_tx_gas_price, row)
            .unwrap_or_else(|e| {
                panic!(
                    "TestnetOwnerFix: forced transferOwnership failed for {} ({}) at block {block_number}: {e:?}",
                    row.label, row.stake_pool
                )
            });
        if !result.result.is_success() {
            panic!(
                "TestnetOwnerFix: transferOwnership reverted for {} ({}) at block {block_number}: {:?}",
                row.label,
                row.stake_pool,
                result.result
            );
        }
        results.push(result);
    }

    info!(
        target: "execute_ordered_block",
        number = block_number,
        timestamp = block_timestamp,
        parent_timestamp,
        pools = MIGRATION_TABLE.len(),
        "TestnetOwnerFix: injected forced transferOwnership txs"
    );
    results
}

fn execute_one(
    executor: Executor<'_>,
    evm_env: EvmEnv,
    system_tx_gas_price: u128,
    row: &MigrationRow,
) -> Result<SystemTxnResult, BlockExecutionError> {
    let nonce = executor.basic(row.old_owner)?.map(|a| a.nonce).unwrap_or(0);
    let txn = new_system_call_txn(
        row.stake_pool,
        nonce,
        system_tx_gas_price,
        transfer_ownership_calldata(row.new_owner),
    );
    let tx_env = Recovered::new_unchecked(txn.clone(), row.old_owner).into_tx_env();
    let execution_result = executor.transact_system_txn(evm_env, Vec::new(), tx_env)?;
    Ok(SystemTxnResult { result: execution_result, txn, sender: row.old_owner })
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::{ChainHardforks, ChainSpecBuilder, ForkCondition, MAINNET};
    use reth_evm::{execute::BasicBlockExecutor, parallel_execute::WrapExecutor};
    use reth_evm_ethereum::EthEvmConfig;
    use revm::database::{CacheDB, EmptyDB};
    use std::sync::Arc;

    const FIX_TS: u64 = 1_700_000_000;

    fn longevity_spec_with_fix(fix_ts: Option<u64>) -> Arc<ChainSpec> {
        let mut genesis = MAINNET.genesis.clone();
        genesis.config.chain_id = LONGEVITY_TESTNET_CHAIN_ID;
        let mut spec = ChainSpec::from(genesis);
        if let Some(ts) = fix_ts {
            spec.gravity_hardforks = ChainHardforks::from([(
                GravityHardfork::TestnetOwnerFix,
                ForkCondition::Timestamp(ts),
            )]);
        }
        Arc::new(spec)
    }

    #[test]
    fn wrong_chain_id_is_noop_even_on_crossing_block() {
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks = ChainHardforks::from([(
            GravityHardfork::TestnetOwnerFix,
            ForkCondition::Timestamp(FIX_TS),
        )]);
        let spec = Arc::new(spec);
        assert_ne!(spec.chain().id(), LONGEVITY_TESTNET_CHAIN_ID);

        let db = CacheDB::<EmptyDB>::default();
        let evm_config = EthEvmConfig::new(spec.clone());
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
        let out = execute_forced_transfers(
            &mut executor,
            spec.as_ref(),
            EvmEnv::default(),
            0,
            1,
            FIX_TS,
            FIX_TS - 1,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn unscheduled_fork_is_noop_on_longevity_chain_id() {
        let spec = longevity_spec_with_fix(None);
        assert_eq!(spec.chain().id(), LONGEVITY_TESTNET_CHAIN_ID);

        let db = CacheDB::<EmptyDB>::default();
        let evm_config = EthEvmConfig::new(spec.clone());
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
        let out = execute_forced_transfers(
            &mut executor,
            spec.as_ref(),
            EvmEnv::default(),
            0,
            1,
            FIX_TS,
            FIX_TS - 1,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn post_activation_block_is_noop_even_when_fork_active() {
        let spec = longevity_spec_with_fix(Some(FIX_TS));
        assert!(spec
            .gravity_hardforks()
            .is_fork_active_at_timestamp(GravityHardfork::TestnetOwnerFix, FIX_TS + 1));
        assert!(!spec
            .gravity_hardforks()
            .fork(GravityHardfork::TestnetOwnerFix)
            .transitions_at_timestamp(FIX_TS + 1, FIX_TS));

        let db = CacheDB::<EmptyDB>::default();
        let evm_config = EthEvmConfig::new(spec.clone());
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
        // Would have injected under the old `is_fork_active` gate.
        let out = execute_forced_transfers(
            &mut executor,
            spec.as_ref(),
            EvmEnv::default(),
            0,
            2,
            FIX_TS + 1,
            FIX_TS,
        );
        assert!(out.is_empty());
    }
}
