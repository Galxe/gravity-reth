//! Storage recovery helper for interrupted block writes.
//!
//! This module provides utilities for recovering storage state after interrupted block writes.
//! When using RocksDB's WriteBatch model, each stage commits independently. If a block write
//! is interrupted, this helper can recover the incomplete stages by checking the stage
//! checkpoints and rebuilding the missing data.

use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use alloy_primitives::BlockNumber;
use reth_db::{cursor::DbCursorRO, tables, transaction::DbTx};
use reth_errors::ProviderError;
use reth_primitives_traits::GotExpected;
use reth_provider::{
    providers::ProviderNodeTypes, AccountExtReader, BlockHashReader, BlockNumReader,
    ChangesetRangeReader, DatabaseProviderFactory, HashingWriter, HeaderProvider, HistoryWriter,
    ProviderFactory, ProviderResult, StageCheckpointReader, StageCheckpointWriter, StorageReader,
    StorageSettingsCache, TrieWriterV2,
};
use reth_stages_api::{StageCheckpoint, StageId};
use reth_storage_errors::provider::RootMismatch;
use reth_trie_parallel::nested_hash::NestedStateRoot;
use tracing::{info, warn};

/// Helper for recovering storage state after interrupted block writes.
///
/// This helper is designed to work with `RocksDB`'s `WriteBatch` model, where each stage
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
    /// The execution checkpoint determines the durable state height. Hashing and history
    /// recovery read changed keys from changesets, which are available before history indexing
    /// completes. Merkle recovery applies the current hashed values and checks the resulting
    /// root against the stored header before writing the rebuilt trie.
    ///
    /// The parallel trie writer can also finish ahead of execution. Such trie data is retained
    /// for deterministic replay of the same ordered blocks; resetting pipeline checkpoints does
    /// not roll it back. Checkpoint alignment therefore does not, by itself, certify a complete
    /// RPC view. Publication additionally respects the original replay boundary and validates
    /// the trie when reopening after interruption.
    ///
    /// All recovery reads use live RW transactions. A published read snapshot can lag the
    /// interrupted stages, and opening a normal RO transaction under the write guard would
    /// recursively acquire the snapshot barrier.
    ///
    /// Production calls this helper for pipe execution, where block bodies and the execution
    /// checkpoint commit with plain state. A body tail above execution identifies an interrupted
    /// legacy merged flush whose plain-state height cannot be inferred safely. Refuse that data
    /// before modifying checkpoints rather than starting the EVM from an unknown state height.
    pub fn check_and_recover(&self) -> ProviderResult<()> {
        let mut rpc_maintenance = self
            .factory
            .rpc_maintenance(true)
            .expect("blocking RPC maintenance always acquires a guard");
        let mut write_guard = self.factory.consistent_write();
        let provider_rw = self.factory.database_provider_rw()?;
        let recover_block_number = provider_rw.recover_block_number()?;
        let best_block_number = provider_rw.best_block_number()?;
        if let Some((body_tip, _)) =
            provider_rw.tx_ref().cursor_read::<tables::BlockBodyIndices>()?.last()? &&
            body_tip > recover_block_number
        {
            return Err(ProviderError::Database(reth_db::DatabaseError::Other(format!(
                "legacy interrupted persistence: body tip {body_tip} exceeds Execution checkpoint \
                 {recover_block_number}; plain-state height is unknown, refusing automatic recovery"
            ))));
        }
        drop(provider_rw);

        if recover_block_number != best_block_number {
            info!(target: "engine::recovery", recover_block = ?recover_block_number, best_block = ?best_block_number, "Detected interrupted block write, starting recovery");
        }

        // Stage 1: Recover AccountHashing
        self.recover_hashing(recover_block_number)?;

        // Stage 2: Recover MerkleExecute
        self.recover_merkle(recover_block_number)?;

        // Stage 3: Recover IndexAccountHistory
        self.recover_history_indices(recover_block_number)?;

        let provider_rw = self.factory.database_provider_rw()?;
        provider_rw.update_pipeline_stages(recover_block_number, false)?;
        let recovered_hash = provider_rw.block_hash(recover_block_number)?;
        provider_rw.commit()?;
        if let Some(hash) = recovered_hash {
            self.factory
                .publish_rpc_read_view(BlockNumHash { number: recover_block_number, hash })?;
            rpc_maintenance.complete();
        }
        write_guard.recovered();
        info!(target: "engine::recovery", recover_block_number = ?recover_block_number, "Recovery completed successfully");
        Ok(())
    }

    /// Recover `AccountHashing` stage if needed.
    fn recover_hashing(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw.get_stage_checkpoint(StageId::AccountHashing)?.unwrap_or_default();

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

            provider_rw.save_stage_checkpoint(
                StageId::AccountHashing,
                StageCheckpoint { block_number, ..ck },
            )?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "Hashing state recovered");
        }

        Ok(())
    }

    /// Recover `MerkleExecute` stage if needed.
    fn recover_merkle(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck = provider_rw.get_stage_checkpoint(StageId::MerkleExecute)?.unwrap_or_default();

        // Merkle can be ahead when its parallel commit wins the race with a failed state commit.
        // Gravity consensus never replaces an ordered block, so recovery will replay the same
        // block with absolute state values. Trie upserts and deletes are idempotent; retaining the
        // ahead trie lets that replay overwrite identical nodes and complete any missing trie DB.
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

            provider_rw.save_stage_checkpoint(
                StageId::MerkleExecute,
                StageCheckpoint { block_number, ..ck },
            )?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "Merkle state recovered");
        }

        Ok(())
    }

    /// Recover `IndexAccountHistory` stage if needed.
    fn recover_history_indices(&self, block_number: BlockNumber) -> ProviderResult<()> {
        let provider_rw = self.factory.database_provider_rw()?;
        let ck =
            provider_rw.get_stage_checkpoint(StageId::IndexAccountHistory)?.unwrap_or_default();

        if ck.block_number < block_number {
            info!(target: "engine::recovery", checkpoint = ?ck.block_number, block_number = ?block_number, "Recovering history indices");
            provider_rw.update_history_indices(ck.block_number + 1..=block_number)?;

            provider_rw.save_stage_checkpoint(
                StageId::IndexAccountHistory,
                StageCheckpoint { block_number, ..ck },
            )?;
            provider_rw.commit()?;
            info!(target: "engine::recovery", block_number = ?block_number, "History indices recovered");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header;
    use alloy_primitives::{keccak256, Address, B256, U256};
    use gravity_primitives::{init_gravity_config, Config as GravityConfig};
    use reth_db::{
        models::{AccountBeforeTx, BlockNumberAddress, StoredBlockBodyIndices},
        tables,
        transaction::{DbTx, DbTxMut},
    };
    use reth_primitives_traits::{Account, StorageEntry};
    use reth_provider::{test_utils::create_test_provider_factory, StateWriter};
    use reth_stages_api::{StageCheckpoint, StageId};
    use reth_trie::{HashedPostState, HashedStorage};
    use std::sync::Once;

    static INIT: Once = Once::new();

    const RECOVERY_STAGES: [StageId; 4] = [
        StageId::Execution,
        StageId::AccountHashing,
        StageId::IndexAccountHistory,
        StageId::MerkleExecute,
    ];

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
        let factory = create_test_provider_factory();
        let address = Address::with_last_byte(1);
        let deleted_address = Address::with_last_byte(2);
        let slot = B256::with_last_byte(3);
        let removed_slot = B256::with_last_byte(4);
        let current = Account { balance: U256::from(7), ..Default::default() };
        let before = Account { balance: U256::from(2), ..Default::default() };
        let provider = factory.database_provider_rw().unwrap();
        let tx = provider.tx_ref();
        tx.put::<tables::PlainAccountState>(address, current).unwrap();
        tx.put::<tables::PlainStorageState>(
            address,
            StorageEntry { key: slot, value: U256::from(9) },
        )
        .unwrap();
        for changed in [address, deleted_address] {
            tx.put::<tables::HashedAccounts>(keccak256(changed), before).unwrap();
            tx.put::<tables::AccountChangeSets>(
                1,
                AccountBeforeTx { address: changed, info: Some(before) },
            )
            .unwrap();
        }
        for changed_slot in [slot, removed_slot] {
            let previous = StorageEntry { key: changed_slot, value: U256::from(2) };
            tx.put::<tables::StorageChangeSets>(BlockNumberAddress((1, address)), previous)
                .unwrap();
            tx.put::<tables::HashedStorages>(
                keccak256(address),
                StorageEntry { key: keccak256(changed_slot), value: previous.value },
            )
            .unwrap();
        }
        write_checkpoint(tx, StageId::Execution, 1);
        provider.commit().unwrap();

        // No history index exists yet. Recovery must discover both updates and removals from
        // changesets and load their current values from plain state.
        StorageRecoveryHelper::new(&factory).recover_hashing(1).unwrap();
        let provider = factory.database_provider_ro().unwrap();
        let tx = provider.tx_ref();
        assert_eq!(tx.get::<tables::HashedAccounts>(keccak256(address)).unwrap(), Some(current));
        assert_eq!(tx.get::<tables::HashedAccounts>(keccak256(deleted_address)).unwrap(), None);
        let state = NestedStateRoot::new(tx, None).read_hashed_state(None).unwrap();
        let storage = &state.storages[&keccak256(address)].storage;
        assert_eq!(storage.len(), 1);
        assert_eq!(storage[&keccak256(slot)], U256::from(9));
        assert_eq!(
            provider.get_stage_checkpoint(StageId::AccountHashing).unwrap().unwrap().block_number,
            1
        );
        assert_eq!(tx.entries::<tables::AccountsHistory>().unwrap(), 0);
        assert_eq!(tx.entries::<tables::StoragesHistory>().unwrap(), 0);
    }

    #[test]
    fn test_recover_merkle_when_checkpoint_behind() {
        init_test_gravity_config();
        let factory = create_test_provider_factory();
        let address = Address::with_last_byte(1);
        let slot = B256::with_last_byte(2);
        let current = recovery_state(address, slot, 7, 9);
        let provider = factory.database_provider_rw().unwrap();
        let expected = NestedStateRoot::new(provider.tx_ref(), None).root(&current).unwrap();
        provider.write_hashed_state(&current.into_sorted()).unwrap();
        let tx = provider.tx_ref();
        tx.put::<tables::AccountChangeSets>(1, AccountBeforeTx { address, info: None }).unwrap();
        tx.put::<tables::StorageChangeSets>(
            BlockNumberAddress((1, address)),
            StorageEntry { key: slot, value: U256::ZERO },
        )
        .unwrap();
        let header = Header { number: 1, state_root: B256::ZERO, ..Default::default() };
        tx.put::<tables::Headers>(1, header.clone()).unwrap();
        tx.put::<tables::CanonicalHeaders>(1, header.hash_slow()).unwrap();
        provider.commit().unwrap();

        assert!(matches!(
            StorageRecoveryHelper::new(&factory).recover_merkle(1),
            Err(ProviderError::StateRootMismatch(_)),
        ));
        let provider = factory.database_provider_ro().unwrap();
        assert!(provider.get_stage_checkpoint(StageId::MerkleExecute).unwrap().is_none());
        assert_eq!(provider.tx_ref().entries::<tables::AccountsTrieV2>().unwrap(), 0);
        assert_eq!(provider.tx_ref().entries::<tables::StoragesTrieV2>().unwrap(), 0);
        drop(provider);

        let provider = factory.database_provider_rw().unwrap();
        let header = Header { number: 1, state_root: expected, ..Default::default() };
        provider.tx_ref().put::<tables::Headers>(1, header.clone()).unwrap();
        provider.tx_ref().put::<tables::CanonicalHeaders>(1, header.hash_slow()).unwrap();
        provider.commit().unwrap();
        StorageRecoveryHelper::new(&factory).recover_merkle(1).unwrap();
        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            expected
        );
        assert_eq!(
            provider.get_stage_checkpoint(StageId::MerkleExecute).unwrap().unwrap().block_number,
            1
        );
    }

    #[test]
    fn recovery_retains_ahead_trie_but_does_not_publish_it_at_execution_height() {
        init_test_gravity_config();
        let factory = create_test_provider_factory();
        let interrupted = factory.consistent_write();
        let provider = factory.database_provider_rw().unwrap();
        let (ahead_root, updates) = NestedStateRoot::new(provider.tx_ref(), None)
            .calculate(&recovery_state(Address::with_last_byte(1), B256::with_last_byte(2), 7, 9))
            .unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        let tx = provider.tx_ref();
        let header =
            Header { number: 1, state_root: reth_trie::EMPTY_ROOT_HASH, ..Default::default() };
        tx.put::<tables::Headers>(1, header.clone()).unwrap();
        tx.put::<tables::CanonicalHeaders>(1, header.hash_slow()).unwrap();
        tx.put::<tables::BlockBodyIndices>(1, StoredBlockBodyIndices::default()).unwrap();
        for stage in RECOVERY_STAGES {
            write_checkpoint(tx, stage, 1);
        }
        write_checkpoint(tx, StageId::MerkleExecute, 2);
        provider.commit().unwrap();
        drop(interrupted);
        let rpc = factory.rpc_provider();
        assert!(rpc.provider().is_err());

        StorageRecoveryHelper::new(&rpc).check_and_recover().unwrap();
        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            ahead_root
        );
        assert_eq!(
            provider.get_stage_checkpoint(StageId::MerkleExecute).unwrap().unwrap().block_number,
            1
        );
        assert!(rpc.provider().is_err());
    }

    #[test]
    fn recovery_rejects_legacy_future_plain_state_before_modifying_database() {
        init_test_gravity_config();
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_rw().unwrap();
        let tx = provider.tx_ref();
        tx.put::<tables::BlockBodyIndices>(2, StoredBlockBodyIndices::default()).unwrap();
        for stage in RECOVERY_STAGES {
            write_checkpoint(tx, stage, 1);
        }
        provider.commit().unwrap();
        let checkpoints = factory.database_provider_ro().unwrap().get_all_checkpoints().unwrap();

        let error = StorageRecoveryHelper::new(&factory).check_and_recover().unwrap_err();
        assert!(error.to_string().contains("body tip 2 exceeds Execution checkpoint 1"));
        let provider = factory.database_provider_rw().unwrap();
        assert_eq!(provider.get_all_checkpoints().unwrap(), checkpoints);
        assert!(factory.rpc_provider().provider().is_err());
    }

    fn write_checkpoint<TX: DbTxMut>(tx: &TX, stage: StageId, number: u64) {
        tx.put::<tables::StageCheckpoints>(
            stage.to_string(),
            StageCheckpoint { block_number: number, ..Default::default() },
        )
        .unwrap();
    }

    fn recovery_state(address: Address, slot: B256, balance: u64, value: u64) -> HashedPostState {
        let mut state = HashedPostState::default();
        state.accounts.insert(
            keccak256(address),
            Some(Account { balance: U256::from(balance), ..Default::default() }),
        );
        state.storages.insert(
            keccak256(address),
            HashedStorage::from_iter(false, [(keccak256(slot), U256::from(value))]),
        );
        state
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
