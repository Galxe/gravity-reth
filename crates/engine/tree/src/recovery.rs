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
    /// This method checks if there was an interrupted block write by comparing the
    /// execution checkpoint with the best block number. If they don't match, it means
    /// a block write was interrupted, and we need to recover the missing stages:
    /// 1. AccountHashing - rebuild hashed account state
    /// 2. MerkleExecute - rebuild trie nodes
    /// 3. IndexAccountHistory - rebuild history indices
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
