//! Incremental recovery for interrupted persistence writes.
//!
//! The state-first writer advances Execution, hashing and history in one state batch, but keeps
//! Merkle completion behind until every database batch is durable. If state committed while a
//! trie write failed, the lagging Merkle checkpoint triggers repair from the changed range.
//! Legacy per-block writes may leave trie data ahead for replay of the same ordered block.
//! Separate hashing and history recovery supports the older per-block writer.

use alloy_consensus::BlockHeader;
use alloy_primitives::BlockNumber;
use reth_db::{
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_errors::ProviderError;
use reth_primitives_traits::GotExpected;
use reth_provider::{
    providers::ProviderNodeTypes, AccountExtReader, BlockHashReader, BlockNumReader,
    ChangesetRangeReader, DatabaseProviderFactory, HashingWriter, HeaderProvider, HistoryWriter,
    ProviderFactory, ProviderResult, StageCheckpointWriter, StorageReader, StorageSettingsCache,
    TrieWriterV2,
};
use reth_stages_api::{StageCheckpoint, StageId};
use reth_storage_errors::provider::RootMismatch;
use reth_trie_parallel::nested_hash::NestedStateRoot;
use tracing::{info, warn};

/// Helper for recovering stages that lag the durable Execution checkpoint.
///
/// Merkle can lag after an interrupted trie commit. Hashing and history advance atomically
/// with Execution in that protocol; their incremental recovery supports older datadirs.
#[derive(Debug)]
pub struct StorageRecoveryHelper<'a, N: ProviderNodeTypes> {
    factory: &'a ProviderFactory<N>,
}

impl<'a, N: ProviderNodeTypes> StorageRecoveryHelper<'a, N> {
    /// Creates a new [`StorageRecoveryHelper`] with the given provider factory.
    pub const fn new(factory: &'a ProviderFactory<N>) -> Self {
        Self { factory }
    }

    /// Recover stages whose checkpoints lag the durable Execution checkpoint.
    ///
    /// `recover_block_number` reads Execution and `best_block_number` reads Finish, both from
    /// `state_db`. A difference can indicate an interrupted legacy write. Equal checkpoints do
    /// not establish that both trie databases currently represent that same block.
    ///
    /// Recovery runs in dependency order: hashing reads changeset keys and their current plain
    /// values; Merkle reads those keys' current hashed values; history indexes changesets.
    /// Each branch only rebuilds the affected interval after its own checkpoint.
    ///
    /// The writer commits static files, then state, then both trie batches in parallel. Its
    /// state batch includes plain state, hashed state, history and Execution, but leaves Merkle
    /// at the previous height until a separate completion commit. Incremental Merkle recovery
    /// repairs the affected block/group range before advancing completion. Recovery commits
    /// trie batches before saving the Merkle checkpoint.
    ///
    /// An older per-block writer can leave trie data at N+1 while Execution remains N. Gravity
    /// consensus restarts execution and commitment from Execution + 1 with the same ordered
    /// block. Reapplying that block's complete absolute account/storage changes is idempotent,
    /// including deletes and storage wipes, even when only one trie batch previously committed.
    ///
    /// That replay happens in the execution/consensus layer, not in this helper. Temporary trie
    /// inconsistency until replay completes is accepted by this protocol; neither checkpoint
    /// normalization below nor successful return certifies a consistent cross-database trie view.
    pub fn check_and_recover(&self) -> ProviderResult<()> {
        let provider_ro = self.factory.database_provider_ro()?;
        let recover_block_number = provider_ro.recover_block_number()?;
        let best_block_number = provider_ro.best_block_number()?;
        drop(provider_ro);

        if recover_block_number != best_block_number {
            info!(target: "engine::recovery", recover_block = ?recover_block_number, best_block = ?best_block_number, "Detected interrupted block write, starting recovery");
        }

        // Older writers committed state stages separately. Preserve their dependency order.
        self.recover_hashing(recover_block_number)?;
        self.recover_merkle(recover_block_number)?;
        self.recover_history_indices(recover_block_number)?;

        let provider_rw = self.factory.database_provider_rw()?;
        // Normalize metadata after recovery; this does not roll back tries ahead of Execution.
        provider_rw.update_pipeline_stages(recover_block_number, false)?;
        provider_rw.commit()?;
        info!(target: "engine::recovery", recover_block_number = ?recover_block_number, "Recovery completed successfully");
        Ok(())
    }

    /// Rebuild hashed state when a legacy `AccountHashing` checkpoint lags Execution.
    fn recover_hashing(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw
            .tx_ref()
            .get::<tables::StageCheckpoints>(StageId::AccountHashing.to_string())
            .map_err(ProviderError::Database)?
            .unwrap_or_default();

        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering hashing state");
            let lists =
                provider_rw.changed_accounts_with_range(ck.block_number + 1..=block_number)?;
            let accounts = provider_rw.basic_accounts(lists)?;
            provider_rw.insert_account_for_hashing(accounts)?;
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

    /// Rebuild V2 tries when `MerkleExecute` lags durable Execution.
    fn recover_merkle(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw
            .tx_ref()
            .get::<tables::StageCheckpoints>(StageId::MerkleExecute.to_string())
            .map_err(ProviderError::Database)?
            .unwrap_or_default();

        // Legacy per-block writes can leave trie data ahead of Execution, with Merkle either
        // ahead or unchanged. Retain it for replay of the same ordered block.
        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering merkle state");
            let nested_state_root = NestedStateRoot::new(provider_rw.tx_ref(), None);
            let range = ck.block_number + 1..=block_number;
            let hashed_state = if provider_rw.cached_storage_settings().changesets_in_static_files {
                nested_state_root.read_hashed_state_from_changesets(
                    provider_rw.account_changesets_range(range.clone())?,
                    provider_rw.storage_changesets_range(range)?,
                )?
            } else {
                nested_state_root.read_hashed_state(Some(range))?
            };
            let (final_root, trie_updates_v2) = nested_state_root.calculate(&hashed_state)?;

            if let Some(header) = provider_rw.header_by_number(block_number)? {
                let expected_root = header.state_root();
                if final_root != expected_root {
                    let block_hash = provider_rw
                        .block_hash(block_number)?
                        .ok_or_else(|| ProviderError::HeaderNotFound(block_number.into()))?;
                    return Err(ProviderError::StateRootMismatch(Box::new(RootMismatch {
                        root: GotExpected { got: final_root, expected: expected_root },
                        block_number,
                        block_hash,
                    })));
                }
            } else {
                warn!(target: "engine::recovery",
                    checkpoint = ?ck.block_number,
                    block_number = ?block_number,
                    final_root = ?final_root,
                    "Header not found");
            }

            provider_rw.write_trie_updatesv2(&trie_updates_v2).map_err(ProviderError::Database)?;
            // Finish both trie writes before a state-first commit can record completion.
            provider_rw.tx_ref().commit_view().map_err(ProviderError::Database)?;

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

    /// Rebuild history indices when a legacy `IndexAccountHistory` checkpoint lags Execution.
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
                cache_max_persist_gap: 128,
                persist_merge_blocks: false,
                cache_capacity: 2_000_000,
                report_db_metrics: false,
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
