//! `reth db migrate-changesets` command.
//!
//! Migrates account/storage changesets from the database tables into the changeset static file
//! segments, then flips the persisted storage layout so the node reads and writes changesets
//! from the static files afterwards.
//!
//! The migration is a one-way ratchet: the layout flag is flipped only after both segments are
//! written, and the database tables are cleared last. Both crash windows recover on rerun:
//! - Crash before the flag flip: the node is still on the legacy layout. A rerun deletes any
//!   partially-written segments (they are append-only) and redoes the migration.
//! - Crash after the flag flip but before the tables are cleared: the node is on the new layout. A
//!   rerun re-runs the idempotent table clear and finishes.
//!
//! Genesis initialization writes the genesis-alloc reverts at block 0 (`insert_genesis_state`),
//! so on every stock datadir the changeset tables start at block 0. Those rows are migrated as a
//! regular block-0 changeset — not dropped in favor of an empty anchor — because the history
//! indices point at block 0 for every genesis account and the static-file read path treats a
//! missing changeset row as an error rather than as "did not exist" (see
//! `HistoricalStateProviderRef::basic_account`).

use crate::common::AccessRights;
use clap::Parser;
use reth_db_api::{
    cursor::DbCursorRO,
    models::GravityStorageSettings,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_db_common::DbTool;
use reth_provider::{
    providers::ProviderNodeTypes, writer::UnifiedStorageWriter, BlockNumReader,
    DatabaseProviderFactory, MetadataProvider, MetadataWriter, ProviderFactory,
    StaticFileProviderFactory, StaticFileWriter, StorageSettingsCache,
};
use reth_static_file_types::StaticFileSegment;
use tracing::{info, warn};

/// `reth db migrate-changesets` command.
#[derive(Debug, Parser)]
pub struct Command;

impl Command {
    /// Returns the database access rights required for the command.
    pub const fn access_rights(&self) -> AccessRights {
        AccessRights::RW
    }

    /// Execute the migration.
    pub fn execute<N: ProviderNodeTypes>(self, tool: &DbTool<N>) -> eyre::Result<()> {
        let factory = &tool.provider_factory;

        let provider = factory.database_provider_ro()?;
        let settings = provider.storage_settings()?.unwrap_or_else(GravityStorageSettings::legacy);
        let sf = factory.static_file_provider();

        // Crash-recovery: if the layout flag was already flipped, a previous run got past the
        // ratchet point but may have died before clearing the database tables. Clearing is
        // idempotent, so just (re)run it and finish.
        if settings.changesets_in_static_files {
            info!(target: "reth::cli", "Layout already flipped; ensuring database tables are cleared");
            Self::clear_database_tables(factory)?;
            return Ok(());
        }

        // Crash-recovery: a previous run wrote the segments but died before the flag flip. The
        // segments are append-only, so restart from a clean slate by deleting them, then redo.
        for segment in [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
        {
            // `delete_jar` removes both the `.jar` and its `.csoff` sidecar and re-initializes
            // the index; walk down the fixed ranges until no segment file remains.
            while let Some(block) = sf.get_highest_static_file_block(segment) {
                warn!(
                    target: "reth::cli",
                    ?segment,
                    "Partially migrated {segment:?} segment found (layout still legacy); \
                     resetting it before redoing the migration"
                );
                sf.delete_jar(segment, block)?;
            }
        }

        let tip = provider.last_block_number()?;
        // Pruned nodes whose earliest changeset is above block 1 are not supported: the static
        // file segments are append-only from genesis and gravity has no prune-aware start yet.
        // Block 0 is expected, not evidence of pruning: genesis initialization writes the
        // genesis-alloc reverts at block 0 on every chain (see `insert_genesis_state`), so stock
        // datadirs start at 0 and databases seeded without genesis state start at 1 (#391).
        eyre::ensure!(
            provider
                .tx_ref()
                .cursor_read::<tables::AccountChangeSets>()?
                .first()?
                .is_none_or(|(block, _)| block <= 1),
            "changeset history is pruned; migrating a pruned database is not yet supported"
        );
        drop(provider);

        info!(target: "reth::cli", tip, "Migrating changesets to static files");
        Self::migrate_account_changesets(tool, tip)?;
        Self::migrate_storage_changesets(tool, tip)?;

        // Ratchet point: flip the persisted layout and refresh the cache, so subsequent reads and
        // writes target the static files.
        let new_settings = GravityStorageSettings { changesets_in_static_files: true };
        let provider_rw = factory.database_provider_rw()?;
        provider_rw.write_storage_settings(new_settings)?;
        UnifiedStorageWriter::commit(provider_rw)?;
        factory.set_storage_settings_cache(new_settings);
        info!(target: "reth::cli", "Storage layout flipped to static-file changesets");

        // Clear the now-superseded database tables. Safe to rerun after a crash here.
        Self::clear_database_tables(factory)?;

        info!(target: "reth::cli", "Changeset migration complete");
        Ok(())
    }

    /// Clears the database changeset tables. Idempotent: safe to rerun after a crash.
    fn clear_database_tables<N: ProviderNodeTypes>(
        factory: &ProviderFactory<N>,
    ) -> eyre::Result<()> {
        let provider_rw = factory.database_provider_rw()?;
        provider_rw.tx_ref().clear::<tables::AccountChangeSets>()?;
        provider_rw.tx_ref().clear::<tables::StorageChangeSets>()?;
        UnifiedStorageWriter::commit(provider_rw)?;
        info!(target: "reth::cli", "Cleared database changeset tables");
        Ok(())
    }

    fn migrate_account_changesets<N: ProviderNodeTypes>(
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        let factory = &tool.provider_factory;
        let provider = factory.database_provider_ro()?;
        let sf = factory.static_file_provider();

        let mut writer = sf.latest_writer(StaticFileSegment::AccountChangeSets)?;

        // Walk the table once, grouping rows by block. The table is sorted by (block, address),
        // so rows for a block arrive contiguously. Every block from genesis up to the tip is
        // appended — even empty ones — so the offset sidecar stays aligned with block numbers.
        // The genesis-alloc reverts at block 0 are appended as a regular changeset, anchoring
        // the append chain with the same entity representation `init_genesis_with_settings`
        // writes for a fresh static-file datadir; when the table has no block-0 rows the gap
        // fill below writes the empty block-0 anchor instead (the empty-alloc degenerate case).
        let mut count = 0u64;
        let mut next_block = 0u64;
        let mut current: Vec<reth_db_api::models::AccountBeforeTx> = Vec::new();
        let mut current_block = 0u64;
        let mut cursor = provider.tx_ref().cursor_read::<tables::AccountChangeSets>()?;
        let walker = cursor.walk(None)?;
        for entry in walker {
            let (block, change) = entry?;
            if block != current_block && !current.is_empty() {
                while next_block < current_block {
                    writer.append_account_changeset(Vec::new(), next_block)?;
                    next_block += 1;
                }
                count += current.len() as u64;
                writer.append_account_changeset(std::mem::take(&mut current), current_block)?;
                next_block += 1;
            }
            current_block = block;
            current.push(change);
        }
        if !current.is_empty() {
            while next_block < current_block {
                writer.append_account_changeset(Vec::new(), next_block)?;
                next_block += 1;
            }
            count += current.len() as u64;
            writer.append_account_changeset(current, current_block)?;
            next_block += 1;
        }
        // Fill any trailing empty blocks up to the tip.
        while next_block <= tip {
            writer.append_account_changeset(Vec::new(), next_block)?;
            next_block += 1;
        }
        writer.commit()?;

        info!(target: "reth::cli", count, "AccountChangeSets migrated");
        Ok(())
    }

    fn migrate_storage_changesets<N: ProviderNodeTypes>(
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        use reth_db_api::models::StorageBeforeTx;

        let factory = &tool.provider_factory;
        let provider = factory.database_provider_ro()?;
        let sf = factory.static_file_provider();

        let mut writer = sf.latest_writer(StaticFileSegment::StorageChangeSets)?;

        // The table is keyed by (block, address) with a storage entry per dup; walk once and
        // regroup into per-block `StorageBeforeTx` rows. Block 0 (genesis-alloc storage reverts)
        // is appended as data, mirroring `migrate_account_changesets`.
        let mut count = 0u64;
        let mut next_block = 0u64;
        let mut current: Vec<StorageBeforeTx> = Vec::new();
        let mut current_block = 0u64;
        let mut cursor = provider.tx_ref().cursor_read::<tables::StorageChangeSets>()?;
        let walker = cursor.walk(None)?;
        for entry in walker {
            let (bna, storage_entry) = entry?;
            let block = bna.block_number();
            if block != current_block && !current.is_empty() {
                while next_block < current_block {
                    writer.append_storage_changeset(Vec::new(), next_block)?;
                    next_block += 1;
                }
                count += current.len() as u64;
                writer.append_storage_changeset(std::mem::take(&mut current), current_block)?;
                next_block += 1;
            }
            current_block = block;
            current.push(StorageBeforeTx {
                address: bna.address(),
                key: storage_entry.key,
                value: storage_entry.value,
            });
        }
        if !current.is_empty() {
            while next_block < current_block {
                writer.append_storage_changeset(Vec::new(), next_block)?;
                next_block += 1;
            }
            count += current.len() as u64;
            writer.append_storage_changeset(current, current_block)?;
            next_block += 1;
        }
        while next_block <= tip {
            writer.append_storage_changeset(Vec::new(), next_block)?;
            next_block += 1;
        }
        writer.commit()?;

        info!(target: "reth::cli", count, "StorageChangeSets migrated");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_genesis::{Genesis, GenesisAccount};
    use alloy_primitives::{Address, B256, U256};
    use reth_chainspec::{Chain, ChainSpec};
    use reth_db_api::{
        cursor::DbCursorRO,
        models::{AccountBeforeTx, BlockNumberAddress},
    };
    use reth_db_common::init::init_genesis;
    use reth_primitives_traits::{Account, StorageEntry};
    use reth_provider::{
        test_utils::{create_test_provider_factory, create_test_provider_factory_with_chain_spec},
        AccountReader, ChangeSetReader, DatabaseProviderFactory, HistoricalStateProviderRef,
        StorageChangeSetReader, StorageSettingsCache,
    };
    use std::{collections::BTreeMap, sync::Arc};

    #[test]
    fn migrates_changesets_and_flips_layout() {
        let factory = create_test_provider_factory();
        let addr = Address::with_last_byte(1);

        // Seed the legacy database changeset tables for blocks 1 and 3 (block 2 has none).
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            let tx = provider_rw.tx_ref();
            tx.put::<tables::AccountChangeSets>(1, AccountBeforeTx { address: addr, info: None })
                .unwrap();
            tx.put::<tables::AccountChangeSets>(
                3,
                AccountBeforeTx { address: addr, info: Some(Account::default()) },
            )
            .unwrap();
            tx.put::<tables::StorageChangeSets>(
                BlockNumberAddress((1, addr)),
                StorageEntry { key: B256::with_last_byte(7), value: U256::from(3) },
            )
            .unwrap();
            provider_rw.commit().unwrap();
        }

        // Sanity: the seed is present and readable via the legacy path before migrating.
        {
            let provider = factory.database_provider_ro().unwrap();
            let rows = provider
                .tx_ref()
                .cursor_read::<tables::AccountChangeSets>()
                .unwrap()
                .walk(None)
                .unwrap()
                .count();
            assert_eq!(rows, 2, "seed should have written 2 account changeset rows");
        }

        let tool = DbTool::new(factory.clone()).unwrap();
        Command.execute(&tool).unwrap();

        // Isolate: static files have the rows, and the settings cache is flipped.
        assert_eq!(
            factory.static_file_provider().account_block_changeset(1).unwrap().len(),
            1,
            "static file segment should hold block 1's account changeset"
        );
        // Layout flag flipped and the reader now sees the static file rows.
        let provider = factory.database_provider_ro().unwrap();
        assert!(provider.storage_settings().unwrap().unwrap().changesets_in_static_files);
        assert!(
            provider.cached_storage_settings().changesets_in_static_files,
            "provider cache should be flipped"
        );
        assert_eq!(provider.account_block_changeset(1).unwrap().len(), 1);
        assert!(provider.account_block_changeset(2).unwrap().is_empty());
        assert_eq!(provider.account_block_changeset(3).unwrap().len(), 1);
        assert_eq!(provider.storage_changeset(1).unwrap().len(), 1);

        // Database changeset tables were cleared.
        let acc_rows = provider
            .tx_ref()
            .cursor_read::<tables::AccountChangeSets>()
            .unwrap()
            .walk(None)
            .unwrap()
            .count();
        assert_eq!(acc_rows, 0, "account changeset table should be empty after migration");

        // Rerunning is a no-op.
        Command.execute(&tool).unwrap();
    }

    /// A crash after the flag flip but before the tables are cleared: rerun clears them.
    #[test]
    fn rerun_after_flag_flip_clears_tables() {
        let factory = create_test_provider_factory();
        let addr = Address::with_last_byte(1);

        // Simulate the post-flip crash state: layout is new, but the database tables still hold
        // rows (they were never cleared).
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            provider_rw
                .tx_ref()
                .put::<tables::AccountChangeSets>(1, AccountBeforeTx { address: addr, info: None })
                .unwrap();
            provider_rw
                .write_storage_settings(GravityStorageSettings { changesets_in_static_files: true })
                .unwrap();
            provider_rw.commit().unwrap();
        }

        let tool = DbTool::new(factory.clone()).unwrap();
        Command.execute(&tool).unwrap();

        let provider = factory.database_provider_ro().unwrap();
        let rows = provider
            .tx_ref()
            .cursor_read::<tables::AccountChangeSets>()
            .unwrap()
            .walk(None)
            .unwrap()
            .count();
        assert_eq!(rows, 0, "rerun after flag flip should clear the database tables");
    }

    /// A stock datadir — genesis initialization writes the alloc reverts at block 0 — must pass
    /// the preflight and migrate the block-0 rows into the static files (#391).
    #[test]
    fn migrates_stock_datadir_with_block0_genesis_reverts() {
        let address_with_balance = Address::with_last_byte(1);
        let address_with_storage = Address::with_last_byte(2);
        let storage_key = B256::with_last_byte(1);
        let chain_spec = Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([
                    (
                        address_with_balance,
                        GenesisAccount { balance: U256::from(1), ..Default::default() },
                    ),
                    (
                        address_with_storage,
                        GenesisAccount {
                            storage: Some(BTreeMap::from([(storage_key, B256::with_last_byte(9))])),
                            ..Default::default()
                        },
                    ),
                ]),
                ..Default::default()
            },
            ..Default::default()
        });
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis(&factory).unwrap();

        // Genesis wrote the alloc reverts at block 0 — the exact shape that used to trip the
        // pruned-history preflight.
        {
            let provider = factory.database_provider_ro().unwrap();
            let first = provider
                .tx_ref()
                .cursor_read::<tables::AccountChangeSets>()
                .unwrap()
                .first()
                .unwrap();
            assert_eq!(first.map(|(block, _)| block), Some(0), "genesis reverts live at block 0");
        }

        // One post-genesis change so the migration walks a multi-block history with a gap.
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            provider_rw
                .tx_ref()
                .put::<tables::AccountChangeSets>(
                    2,
                    AccountBeforeTx {
                        address: address_with_balance,
                        info: Some(Account::default()),
                    },
                )
                .unwrap();
            provider_rw.commit().unwrap();
        }

        // Legacy-path baseline: at block 0 the history index resolves to the block-0 changeset
        // and the alloc accounts read as nonexistent (pre-genesis state).
        {
            let provider = factory.database_provider_ro().unwrap();
            let state = HistoricalStateProviderRef::new(&provider, 0);
            assert_eq!(state.basic_account(&address_with_balance).unwrap(), None);
            assert_eq!(state.basic_account(&address_with_storage).unwrap(), None);
        }

        let tool = DbTool::new(factory.clone()).unwrap();
        Command.execute(&tool).unwrap();

        // Layout flipped; the genesis reverts now live in the block-0 static file entry.
        let provider = factory.database_provider_ro().unwrap();
        assert!(provider.storage_settings().unwrap().unwrap().changesets_in_static_files);
        let block0 = provider.account_block_changeset(0).unwrap();
        assert_eq!(block0.len(), 2, "block 0 should carry the genesis-alloc account reverts");
        assert!(block0.iter().all(|change| change.info.is_none()));
        assert_eq!(
            provider.storage_changeset(0).unwrap().len(),
            1,
            "block 0 should carry the genesis-alloc storage revert"
        );
        assert_eq!(provider.account_block_changeset(1).unwrap().len(), 0, "gap block is empty");
        assert_eq!(provider.account_block_changeset(2).unwrap().len(), 1);

        // Read equivalence: the same history-index lookup must keep resolving on the static-file
        // layout instead of erroring on a missing block-0 row.
        let state = HistoricalStateProviderRef::new(&provider, 0);
        assert_eq!(state.basic_account(&address_with_balance).unwrap(), None);
        assert_eq!(state.basic_account(&address_with_storage).unwrap(), None);

        // Database tables were cleared.
        let rows = provider
            .tx_ref()
            .cursor_read::<tables::AccountChangeSets>()
            .unwrap()
            .walk(None)
            .unwrap()
            .count();
        assert_eq!(rows, 0);
    }

    /// A genuinely pruned history — earliest changeset above block 1 — is still refused.
    #[test]
    fn rejects_pruned_history_starting_above_block_one() {
        let factory = create_test_provider_factory();
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            provider_rw
                .tx_ref()
                .put::<tables::AccountChangeSets>(
                    2,
                    AccountBeforeTx { address: Address::with_last_byte(1), info: None },
                )
                .unwrap();
            provider_rw.commit().unwrap();
        }

        let tool = DbTool::new(factory.clone()).unwrap();
        let err = Command.execute(&tool).unwrap_err();
        assert!(err.to_string().contains("pruned"), "unexpected error: {err}");
    }

    /// A crash before the flag flip that left partially-written segments: rerun resets them and
    /// migrates cleanly.
    #[test]
    fn rerun_with_partial_segments_resets_and_migrates() {
        let factory = create_test_provider_factory();
        let addr = Address::with_last_byte(1);

        // Seed the legacy tables.
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            provider_rw
                .tx_ref()
                .put::<tables::AccountChangeSets>(1, AccountBeforeTx { address: addr, info: None })
                .unwrap();
            provider_rw
                .tx_ref()
                .put::<tables::StorageChangeSets>(
                    BlockNumberAddress((1, addr)),
                    StorageEntry { key: B256::with_last_byte(7), value: U256::from(3) },
                )
                .unwrap();
            provider_rw.commit().unwrap();
        }

        // Simulate a partially-written account segment from a crashed run (layout still legacy).
        {
            let sf = factory.static_file_provider();
            let mut writer = sf.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
            writer.increment_block(0).unwrap();
            writer
                .append_account_changeset(vec![AccountBeforeTx { address: addr, info: None }], 1)
                .unwrap();
            writer.commit().unwrap();
            assert!(sf
                .get_highest_static_file_block(StaticFileSegment::AccountChangeSets)
                .is_some());
        }

        let tool = DbTool::new(factory.clone()).unwrap();
        Command.execute(&tool).unwrap();

        // Migration succeeded despite the partial segment.
        let provider = factory.database_provider_ro().unwrap();
        assert!(provider.storage_settings().unwrap().unwrap().changesets_in_static_files);
        assert_eq!(provider.account_block_changeset(1).unwrap().len(), 1);
        assert_eq!(provider.storage_changeset(1).unwrap().len(), 1);
    }
}
