//! `reth db migrate-changesets` command.
//!
//! Migrates account/storage changesets from the database tables into the changeset static file
//! segments, then flips the persisted storage layout so the node reads and writes changesets
//! from the static files afterwards.
//!
//! The migration is a one-way ratchet: the layout flag is flipped only after both segments are
//! written and verified, and the database tables are cleared last. A crash before the flag flip
//! leaves the node on the legacy layout (rerun the migration); a crash after it leaves the flag
//! set with the tables still present (rerun clears them).

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
    DatabaseProviderFactory, MetadataProvider, MetadataWriter, StaticFileProviderFactory,
    StaticFileWriter, StorageSettingsCache,
};
use reth_static_file_types::StaticFileSegment;
use tracing::info;

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

        // Preflight: refuse to run unless the node is on the legacy layout with empty segments.
        let provider = factory.database_provider_ro()?;
        let settings = provider.storage_settings()?.unwrap_or_else(GravityStorageSettings::legacy);
        if settings.changesets_in_static_files {
            info!(target: "reth::cli", "Already on the static-file changeset layout; nothing to do");
            return Ok(());
        }
        let sf = factory.static_file_provider();
        for segment in [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
        {
            eyre::ensure!(
                sf.get_highest_static_file_block(segment).is_none(),
                "{segment:?} static file already has data; migration target must be empty"
            );
        }

        let tip = provider.last_block_number()?;
        // Pruned nodes whose earliest changeset is above block 1 are not supported: the static
        // file segments are append-only from genesis and gravity has no prune-aware start yet.
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
        let provider_rw = factory.database_provider_rw()?;
        provider_rw.tx_ref().clear::<tables::AccountChangeSets>()?;
        provider_rw.tx_ref().clear::<tables::StorageChangeSets>()?;
        UnifiedStorageWriter::commit(provider_rw)?;
        info!(target: "reth::cli", "Cleared database changeset tables");

        info!(target: "reth::cli", "Changeset migration complete");
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
        // Anchor the append chain at genesis, matching init_genesis.
        writer.increment_block(0)?;

        // Walk the table once, grouping rows by block. The table is sorted by (block, address),
        // so rows for a block arrive contiguously. Every block up to the tip is appended — even
        // empty ones — so the offset sidecar stays aligned with block numbers.
        let mut count = 0u64;
        let mut next_block = 1u64;
        let mut current: Vec<reth_db_api::models::AccountBeforeTx> = Vec::new();
        let mut current_block = 0u64;
        let mut cursor = provider.tx_ref().cursor_read::<tables::AccountChangeSets>()?;
        let mut walker = cursor.walk(None)?;
        while let Some(entry) = walker.next() {
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
        writer.increment_block(0)?;

        // The table is keyed by (block, address) with a storage entry per dup; walk once and
        // regroup into per-block `StorageBeforeTx` rows.
        let mut count = 0u64;
        let mut next_block = 1u64;
        let mut current: Vec<StorageBeforeTx> = Vec::new();
        let mut current_block = 0u64;
        let mut cursor = provider.tx_ref().cursor_read::<tables::StorageChangeSets>()?;
        let mut walker = cursor.walk(None)?;
        while let Some(entry) = walker.next() {
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
    use alloy_primitives::{Address, B256, U256};
    use reth_db_api::{
        cursor::DbCursorRO,
        models::{AccountBeforeTx, BlockNumberAddress},
    };
    use reth_primitives_traits::{Account, StorageEntry};
    use reth_provider::{
        test_utils::create_test_provider_factory, ChangeSetReader, DatabaseProviderFactory,
        StorageChangeSetReader, StorageSettingsCache,
    };

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
}
