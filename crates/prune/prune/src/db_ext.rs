use crate::PruneLimiter;
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, RangeWalker},
    table::{DupSort, Table, TableRow},
    transaction::DbTxMut,
    DatabaseError,
};
use std::{fmt::Debug, ops::RangeBounds};
use tracing::debug;

pub(crate) trait DbTxPruneExt: DbTxMut {
    /// Prune the table for the specified key range.
    ///
    /// Returns number of rows pruned.
    fn prune_table_with_range<T: Table>(
        &self,
        keys: impl RangeBounds<T::Key> + Clone + Debug,
        limiter: &mut PruneLimiter,
        mut skip_filter: impl FnMut(&TableRow<T>) -> bool,
        mut delete_callback: impl FnMut(TableRow<T>),
    ) -> Result<(usize, bool), DatabaseError> {
        let mut cursor = self.cursor_write::<T>()?;
        let mut walker = cursor.walk_range(keys)?;

        let mut deleted_entries = 0;

        let done = loop {
            // check for time out must be done in this scope since it's not done in
            // `prune_table_with_range_step`
            if limiter.is_limit_reached() {
                debug!(
                    target: "providers::db",
                    ?limiter,
                    deleted_entries_limit = %limiter.is_deleted_entries_limit_reached(),
                    time_limit = %limiter.is_time_limit_reached(),
                    table = %T::NAME,
                    "Pruning limit reached"
                );
                break false
            }

            let done = self.prune_table_with_range_step(
                &mut walker,
                limiter,
                &mut skip_filter,
                &mut |row| {
                    deleted_entries += 1;
                    delete_callback(row);
                },
            )?;

            if done {
                break true
            }
        };

        Ok((deleted_entries, done))
    }

    /// Steps once with the given walker and prunes the entry in the table.
    ///
    /// Returns `true` if the walker is finished, `false` if it may have more data to prune.
    ///
    /// CAUTION: Pruner limits are not checked. This allows for a clean exit of a prune run that's
    /// pruning different tables concurrently, by letting them step to the same height before
    /// timing out.
    fn prune_table_with_range_step<T: Table>(
        &self,
        walker: &mut RangeWalker<'_, T, Self::CursorMut<T>>,
        limiter: &mut PruneLimiter,
        skip_filter: &mut impl FnMut(&TableRow<T>) -> bool,
        delete_callback: &mut impl FnMut(TableRow<T>),
    ) -> Result<bool, DatabaseError> {
        let Some(res) = walker.next() else { return Ok(true) };

        let row = res?;

        if !skip_filter(&row) {
            walker.delete_current()?;
            limiter.increment_deleted_entries_count();
            delete_callback(row);
        }

        Ok(false)
    }
    /// Prune duplicate entries for a single DUPSORT key.
    ///
    /// Returns the number of rows pruned and whether all duplicate entries for the key were
    /// deleted.
    #[allow(dead_code)]
    fn prune_dupsort_key_entries<T: DupSort>(
        &self,
        key: T::Key,
        limiter: &mut PruneLimiter,
    ) -> Result<(usize, bool), DatabaseError> {
        let mut cursor = self.cursor_dup_write::<T>()?;
        let mut entry = cursor.seek_exact(key)?;

        let mut deleted_entries = 0;

        while entry.is_some() && !limiter.is_limit_reached() {
            cursor.delete_current()?;
            limiter.increment_deleted_entries_count();
            deleted_entries += 1;
            entry = cursor.next_dup()?;
        }

        // an entry remaining means the loop stopped because a limit was reached
        let done = entry.is_none();
        if !done {
            debug!(
                target: "providers::db",
                ?limiter,
                deleted_entries_limit = %limiter.is_deleted_entries_limit_reached(),
                time_limit = %limiter.is_time_limit_reached(),
                table = %T::NAME,
                "Pruning limit reached"
            );
        }

        Ok((deleted_entries, done))
    }
}

impl<Tx> DbTxPruneExt for Tx where Tx: DbTxMut {}

#[cfg(test)]
mod duplicate_budget_tests {
    use super::*;
    use alloy_primitives::{B256, U256};
    use reth_db_api::{tables, transaction::DbTxMut};
    use reth_primitives_traits::StorageEntry;
    use reth_provider::DatabaseProviderFactory;
    use reth_stages::test_utils::TestStageDB;
    use std::time::Duration;
    fn storage_entry(slot_byte: u8) -> StorageEntry {
        StorageEntry { key: B256::with_last_byte(slot_byte), value: U256::from(slot_byte) }
    }

    fn insert_hashed_storages(db: &TestStageDB, rows: impl IntoIterator<Item = (u8, u8)>) {
        let provider = db.factory.database_provider_rw().unwrap();
        for (address_byte, slot_byte) in rows {
            provider
                .tx_ref()
                .put::<tables::HashedStorages>(
                    B256::with_last_byte(address_byte),
                    storage_entry(slot_byte),
                )
                .expect("insert hashed storage");
        }
        provider.commit().expect("commit");
    }

    fn hashed_storage_slots(db: &TestStageDB, address_byte: u8) -> Vec<B256> {
        db.table::<tables::HashedStorages>()
            .unwrap()
            .into_iter()
            .filter_map(|(key, entry)| {
                (key == B256::with_last_byte(address_byte)).then_some(entry.key)
            })
            .collect()
    }

    fn prune_hashed_storage_key(
        db: &TestStageDB,
        address_byte: u8,
        limiter: &mut PruneLimiter,
    ) -> (usize, bool) {
        let provider = db.factory.database_provider_rw().unwrap();
        let result = provider
            .tx_ref()
            .prune_dupsort_key_entries::<tables::HashedStorages>(
                B256::with_last_byte(address_byte),
                limiter,
            )
            .expect("prune hashed storages");
        provider.commit().expect("commit");
        result
    }

    #[test]
    fn range_pruning_counts_deletions_and_preserves_budget_for_skipped_rows() {
        let db = TestStageDB::default();
        insert_hashed_storages(&db, (0..6).map(|slot| (1, slot)));
        let address = B256::with_last_byte(1);
        let provider = db.factory.database_provider_rw().unwrap();
        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(1);
        let mut deleted_slots = Vec::new();
        let result = provider
            .tx_ref()
            .prune_table_with_range::<tables::HashedStorages>(
                address..=address,
                &mut limiter,
                |(_, entry)| entry.key < B256::with_last_byte(4),
                |(_, entry)| deleted_slots.push(entry.key),
            )
            .unwrap();
        assert_eq!(result, (1, false));
        assert_eq!(deleted_slots, vec![B256::with_last_byte(4)]);
        assert_eq!(limiter.deleted_entries_limit_left(), Some(0));
        provider.commit().unwrap();
        assert_eq!(hashed_storage_slots(&db, 1), [0, 1, 2, 3, 5].map(B256::with_last_byte));

        let provider = db.factory.database_provider_rw().unwrap();
        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(1);
        let result = provider
            .tx_ref()
            .prune_table_with_range::<tables::HashedStorages>(
                address..=address,
                &mut limiter,
                |_| true,
                |_| panic!("skipped rows must not invoke the deletion callback"),
            )
            .unwrap();
        assert_eq!(result, (0, true));
        assert_eq!(limiter.deleted_entries_limit_left(), Some(1));
    }

    #[test]
    fn prune_dupsort_key_entries_resumes_with_deleted_entries_budget() {
        let db = TestStageDB::default();
        insert_hashed_storages(&db, (0..5).map(|slot| (1, slot)));
        insert_hashed_storages(&db, (10..12).map(|slot| (2, slot)));

        let mut total_deleted = 0;

        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(2);
        let (deleted, done) = prune_hashed_storage_key(&db, 1, &mut limiter);
        total_deleted += deleted;
        assert_eq!((deleted, done), (2, false));
        assert_eq!(
            hashed_storage_slots(&db, 1),
            vec![B256::with_last_byte(2), B256::with_last_byte(3), B256::with_last_byte(4),]
        );
        assert_eq!(
            hashed_storage_slots(&db, 2),
            vec![B256::with_last_byte(10), B256::with_last_byte(11)]
        );

        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(2);
        let (deleted, done) = prune_hashed_storage_key(&db, 1, &mut limiter);
        total_deleted += deleted;
        assert_eq!((deleted, done), (2, false));
        assert_eq!(hashed_storage_slots(&db, 1), vec![B256::with_last_byte(4)]);
        assert_eq!(
            hashed_storage_slots(&db, 2),
            vec![B256::with_last_byte(10), B256::with_last_byte(11)]
        );

        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(2);
        let (deleted, done) = prune_hashed_storage_key(&db, 1, &mut limiter);
        total_deleted += deleted;
        assert_eq!((deleted, done), (1, true));
        assert!(hashed_storage_slots(&db, 1).is_empty());
        assert_eq!(
            hashed_storage_slots(&db, 2),
            vec![B256::with_last_byte(10), B256::with_last_byte(11)]
        );
        assert_eq!(total_deleted, 5);
    }

    #[test]
    fn prune_dupsort_key_entries_stops_on_time_limit() {
        let db = TestStageDB::default();
        insert_hashed_storages(&db, (0..3).map(|slot| (1, slot)));

        let mut limiter = PruneLimiter::default().set_time_limit(Duration::from_nanos(1));
        std::thread::sleep(Duration::from_millis(1));

        let (deleted, done) = prune_hashed_storage_key(&db, 1, &mut limiter);

        assert_eq!((deleted, done), (0, false));
        assert!(limiter.is_time_limit_reached());
        assert_eq!(
            hashed_storage_slots(&db, 1),
            vec![B256::with_last_byte(0), B256::with_last_byte(1), B256::with_last_byte(2)]
        );
    }

    #[test]
    fn prune_dupsort_key_entries_missing_key_is_done() {
        let db = TestStageDB::default();
        insert_hashed_storages(&db, (0..3).map(|slot| (1, slot)));

        let mut limiter = PruneLimiter::default().set_deleted_entries_limit(2);
        let result = prune_hashed_storage_key(&db, 2, &mut limiter);

        assert_eq!(result, (0, true));
        assert_eq!(
            hashed_storage_slots(&db, 1),
            vec![B256::with_last_byte(0), B256::with_last_byte(1), B256::with_last_byte(2)]
        );
    }

    #[test]
    fn prune_dupsort_key_entries_leaves_other_keys_untouched() {
        let db = TestStageDB::default();
        insert_hashed_storages(&db, (0..3).map(|slot| (1, slot)));
        insert_hashed_storages(&db, (10..13).map(|slot| (2, slot)));

        let mut limiter = PruneLimiter::default();
        let result = prune_hashed_storage_key(&db, 1, &mut limiter);

        assert_eq!(result, (3, true));
        assert!(hashed_storage_slots(&db, 1).is_empty());
        assert_eq!(
            hashed_storage_slots(&db, 2),
            vec![B256::with_last_byte(10), B256::with_last_byte(11), B256::with_last_byte(12),]
        );
    }
}
