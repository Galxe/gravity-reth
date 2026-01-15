//! Storage recovery helper for interrupted block writes.
//!
//! This module provides utilities for recovering storage state after interrupted block writes.
//! When using RocksDB's WriteBatch model, each stage commits independently. If a block write
//! is interrupted, this helper can recover the incomplete stages by checking the stage
//! checkpoints and rebuilding the missing data.

use alloy_primitives::BlockNumber;
use gravity_primitives::get_gravity_config;
use reth_db::{
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_errors::ProviderError;
use reth_provider::{
    providers::ProviderNodeTypes, AccountExtReader, BlockNumReader, DatabaseProviderFactory,
    HashingWriter, HistoryWriter, ProviderFactory, ProviderResult, StageCheckpointWriter,
    StorageReader, TrieWriterV2,
};
use reth_stages_api::{StageCheckpoint, StageId};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use tracing::info;

/// Helper for recovering storage state after interrupted block writes.
///
/// This helper is designed to work with RocksDB's WriteBatch model, where each stage
/// commits independently. When a block write is interrupted, this helper can recover
/// the incomplete stages by checking the stage checkpoints and rebuilding the missing data.
#[derive(Debug)]
pub struct StorageRecoveryHelper<'a, N: ProviderNodeTypes> {
    factory: &'a ProviderFactory<N>,
}

impl<'a, N: ProviderNodeTypes> StorageRecoveryHelper<'a, N> {
    /// Creates a new [`StorageRecoveryHelper`] with the given provider factory.
    pub const fn new(factory: &'a ProviderFactory<N>) -> Self {
        Self { factory }
    }

    /// Check checkpoints and recover any unfinished block writes.
    ///
    /// # Recovery Logic and Correctness Guarantees
    ///
    /// This method detects and recovers from interrupted block writes by leveraging the
    /// checkpoint-based commit ordering described in `Tx::commit`. The recovery process
    /// ensures data consistency even when trie commits succeed but state commits fail.
    ///
    /// ## Detection: Checkpoint Comparison
    ///
    /// We compare two critical checkpoints:
    /// - `recover_block_number`: The execution checkpoint from state_db, indicating the last block
    ///   whose state commit completed successfully
    /// - `best_block_number`: The highest block number that has been fully executed
    ///
    /// If these don't match, it means execution progressed beyond the last successful
    /// state commit, indicating an interrupted block write.
    ///
    /// ## Why Interruption Happens: Partial Commit Failures
    ///
    /// During parallel execution (state_handle + trie_handle in persistence.rs), several
    /// failure scenarios can leave orphaned data:
    ///
    /// 1. **State commit succeeds, trie commit fails**: No orphaned data, but trie is incomplete.
    ///    Stages need to be re-executed.
    ///
    /// 2. **Trie commits succeed, state commit fails**: Account and storage trie data is written to
    ///    disk, but no checkpoint is recorded in state_db (since state_batch is committed last in
    ///    Tx::commit). This leaves orphaned trie data.
    ///
    /// 3. **Partial trie commit**: Account trie succeeds but storage trie fails (or vice versa).
    ///    State commit never happens, so no checkpoint is written. Leaves partial orphaned trie
    ///    data.
    ///
    /// ## Recovery Strategy: Stage-by-Stage Rebuild
    ///
    /// We recover by re-executing stages in dependency order, using each stage's checkpoint
    /// to determine what needs to be rebuilt:
    ///
    /// 1. **AccountHashing**: Rebuild hashed account/storage state from plain state.
    ///    - Reads changed accounts/storages from history indices
    ///    - Idempotent: re-hashing the same data produces identical hashes
    ///
    /// 2. **MerkleExecute**: Rebuild account and storage trie nodes.
    ///    - Reads hashed state from AccountHashing stage
    ///    - **Idempotent**: Writing the same trie node data overwrites orphaned data with identical
    ///      content, producing a consistent trie structure
    ///
    /// 3. **IndexAccountHistory**: Rebuild history indices for changed accounts/storages.
    ///    - Reads account/storage changes from plain state
    ///    - Idempotent: re-indexing the same changes produces identical indices
    ///
    /// ## Why This Works: Checkpoint-Driven Idempotency
    ///
    /// Each stage checks its own checkpoint (stored in state_db) to determine if it needs
    /// to re-execute. Since all checkpoints are in state_db and state_batch is committed
    /// last (see Tx::commit), the checkpoint accurately reflects which stages completed:
    ///
    /// - If a stage's checkpoint is behind `recover_block_number`, it means the stage's data commit
    ///   may have succeeded but the checkpoint write (in state_batch) failed.
    /// - Re-executing the stage is safe because all stage operations are idempotent.
    /// - Orphaned trie data from failed commits is harmlessly overwritten with identical data
    ///   during recovery.
    ///
    /// ## Correctness Guarantee
    ///
    /// After recovery completes, all stages are brought to `recover_block_number`, and
    /// the pipeline stages are updated to reflect this. The system state is consistent:
    /// - Plain state reflects execution up to `recover_block_number`
    /// - Hashed state matches plain state
    /// - Trie nodes correctly represent hashed state
    /// - History indices correctly track all state changes
    /// - All checkpoints are synchronized at `recover_block_number`
    pub fn check_and_recover(&self) -> ProviderResult<()> {
        let provider_ro = self.factory.database_provider_ro()?;
        let recover_block_number = provider_ro.recover_block_number()?;
        let best_block_number = provider_ro.best_block_number()?;
        drop(provider_ro);

        if recover_block_number == best_block_number {
            info!(target: "engine::recovery", block_number = ?recover_block_number, "No recovery needed, checkpoints are consistent");
            return Ok(());
        }

        info!(target: "engine::recovery", recover_block = ?recover_block_number, best_block = ?best_block_number, "Detected interrupted block write, starting recovery");

        // Stage 1: Recover AccountHashing
        self.recover_hashing(recover_block_number)?;

        // Stage 2: Recover MerkleExecute
        self.recover_merkle(recover_block_number)?;

        // Stage 3: Recover IndexAccountHistory
        if !get_gravity_config().validator_node_only {
            self.recover_history_indices(recover_block_number)?;
        }

        let provider_rw = self.factory.database_provider_rw()?;
        provider_rw.update_pipeline_stages(recover_block_number, false)?;
        provider_rw.commit()?;
        info!(target: "engine::recovery", recover_block_number = ?recover_block_number, "Recovery completed successfully");
        Ok(())
    }

    /// Recover AccountHashing stage if needed.
    fn recover_hashing(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw
            .tx_ref()
            .get::<tables::StageCheckpoints>(StageId::AccountHashing.to_string())
            .map_err(ProviderError::Database)?
            .unwrap_or_default();

        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering hashing state");
            // rebuild hashing account
            let lists =
                provider_rw.changed_accounts_with_range(ck.block_number + 1..=block_number)?;
            let accounts = provider_rw.basic_accounts(lists)?;
            provider_rw.insert_account_for_hashing(accounts)?;
            // rebuild hashing storage
            let lists =
                provider_rw.changed_storages_with_range(ck.block_number + 1..=block_number)?;
            let storages = provider_rw.plain_state_storages(lists)?;
            provider_rw.insert_storage_for_hashing(storages)?;

            provider_rw
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::AccountHashing.to_string(),
                    StageCheckpoint { block_number, ..ck },
                )
                .map_err(ProviderError::Database)?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "Hashing state recovered");
        }

        Ok(())
    }

    /// Recover MerkleExecute stage if needed.
    fn recover_merkle(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw
            .tx_ref()
            .get::<tables::StageCheckpoints>(StageId::MerkleExecute.to_string())
            .map_err(ProviderError::Database)?
            .unwrap_or_default();

        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering merkle state");
            let nested_state_root = NestedStateRoot::new(provider_rw.tx_ref(), None);
            let hashed_state =
                nested_state_root.read_hashed_state(Some(ck.block_number + 1..=block_number))?;
            let (_final_root, trie_updates_v2) = nested_state_root.calculate(&hashed_state)?;
            provider_rw.write_trie_updatesv2(&trie_updates_v2).map_err(ProviderError::Database)?;

            provider_rw
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::MerkleExecute.to_string(),
                    StageCheckpoint { block_number, ..ck },
                )
                .map_err(ProviderError::Database)?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "Merkle state recovered");
        }

        Ok(())
    }

    /// Recover IndexAccountHistory stage if needed.
    fn recover_history_indices(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw
            .tx_ref()
            .get::<tables::StageCheckpoints>(StageId::IndexAccountHistory.to_string())
            .map_err(ProviderError::Database)?
            .unwrap_or_default();

        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering history indices");
            provider_rw.update_history_indices(ck.block_number + 1..=block_number)?;

            provider_rw
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::IndexAccountHistory.to_string(),
                    StageCheckpoint { block_number, ..ck },
                )
                .map_err(ProviderError::Database)?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "History indices recovered");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gravity_primitives::{init_gravity_config, Config as GravityConfig};
    use reth_db::{
        tables,
        transaction::{DbTx, DbTxMut},
    };
    use reth_provider::test_utils::create_test_provider_factory;
    use reth_stages_api::{StageCheckpoint, StageId};
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn init_test_gravity_config() {
        INIT.call_once(|| {
            init_gravity_config(GravityConfig {
                disable_pipe_execution: false,
                disable_grevm: false,
                pipe_block_gas_limit: 1_000_000_000,
                cache_max_persist_gap: 64,
                cache_capacity: 2_000_000,
                report_db_metrics: false,
                trie_parallel_levels: 1,
                validator_node_only: false,
            });
        });
    }

    #[test]
    fn test_recovery_not_needed_when_checkpoints_consistent() {
        reth_tracing::init_test_tracing();
        init_test_gravity_config();
        let factory = create_test_provider_factory();

        // Set all checkpoints to block 5 (consistent state)
        {
            let provider = factory.database_provider_rw().unwrap();
            for stage_id in [
                StageId::Execution,
                StageId::AccountHashing,
                StageId::MerkleExecute,
                StageId::IndexAccountHistory,
            ] {
                provider
                    .tx_ref()
                    .put::<tables::StageCheckpoints>(
                        stage_id.to_string(),
                        StageCheckpoint { block_number: 5, stage_checkpoint: None },
                    )
                    .unwrap();
            }
            provider.commit().unwrap();
        }

        let helper = StorageRecoveryHelper::new(&factory);

        // Recovery should succeed without error (no recovery needed)
        let result = helper.check_and_recover();
        assert!(result.is_ok());
    }

    #[test]
    fn test_helper_creation() {
        let factory = create_test_provider_factory();
        let _helper = StorageRecoveryHelper::new(&factory);
        // If helper is created successfully, test passes
    }

    #[test]
    fn test_recover_hashing_when_checkpoint_behind() {
        reth_tracing::init_test_tracing();
        let factory = create_test_provider_factory();

        // Set AccountHashing checkpoint to block 3 (behind execution at 5)
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::Execution.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::AccountHashing.to_string(),
                    StageCheckpoint { block_number: 3, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::MerkleExecute.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::IndexAccountHistory.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider.commit().unwrap();
        }

        let helper = StorageRecoveryHelper::new(&factory);

        // Call recover_hashing directly (normally called by check_and_recover)
        // This should handle the case where no actual data exists gracefully
        let result = helper.recover_hashing(5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_recover_merkle_when_checkpoint_behind() {
        reth_tracing::init_test_tracing();
        init_test_gravity_config();
        let factory = create_test_provider_factory();

        // Set MerkleExecute checkpoint to block 3 (behind execution at 5)
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::Execution.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::AccountHashing.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::MerkleExecute.to_string(),
                    StageCheckpoint { block_number: 3, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::IndexAccountHistory.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider.commit().unwrap();
        }

        let helper = StorageRecoveryHelper::new(&factory);

        // Call recover_merkle directly
        let result = helper.recover_merkle(5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_recover_history_indices_when_checkpoint_behind() {
        reth_tracing::init_test_tracing();
        let factory = create_test_provider_factory();

        // Set IndexAccountHistory checkpoint to block 3 (behind execution at 5)
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::Execution.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::AccountHashing.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::MerkleExecute.to_string(),
                    StageCheckpoint { block_number: 5, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::IndexAccountHistory.to_string(),
                    StageCheckpoint { block_number: 3, stage_checkpoint: None },
                )
                .unwrap();
            provider.commit().unwrap();
        }

        let helper = StorageRecoveryHelper::new(&factory);

        // Call recover_history_indices directly
        let result = helper.recover_history_indices(5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_checkpoint_ordering_logic() {
        reth_tracing::init_test_tracing();
        let factory = create_test_provider_factory();

        // Test that checkpoints are read correctly
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::Execution.to_string(),
                    StageCheckpoint { block_number: 10, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::AccountHashing.to_string(),
                    StageCheckpoint { block_number: 8, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::MerkleExecute.to_string(),
                    StageCheckpoint { block_number: 9, stage_checkpoint: None },
                )
                .unwrap();
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(
                    StageId::IndexAccountHistory.to_string(),
                    StageCheckpoint { block_number: 7, stage_checkpoint: None },
                )
                .unwrap();
            provider.commit().unwrap();
        }

        // Verify each checkpoint is stored and retrieved correctly
        {
            let provider = factory.database_provider_ro().unwrap();
            let ck_exec = provider
                .tx_ref()
                .get::<tables::StageCheckpoints>(StageId::Execution.to_string())
                .unwrap()
                .unwrap();
            let ck_hash = provider
                .tx_ref()
                .get::<tables::StageCheckpoints>(StageId::AccountHashing.to_string())
                .unwrap()
                .unwrap();
            let ck_merkle = provider
                .tx_ref()
                .get::<tables::StageCheckpoints>(StageId::MerkleExecute.to_string())
                .unwrap()
                .unwrap();
            let ck_history = provider
                .tx_ref()
                .get::<tables::StageCheckpoints>(StageId::IndexAccountHistory.to_string())
                .unwrap()
                .unwrap();

            assert_eq!(ck_exec.block_number, 10);
            assert_eq!(ck_hash.block_number, 8);
            assert_eq!(ck_merkle.block_number, 9);
            assert_eq!(ck_history.block_number, 7);
        }

        // Verify recovery helper can be created
        let _helper = StorageRecoveryHelper::new(&factory);
    }
}
