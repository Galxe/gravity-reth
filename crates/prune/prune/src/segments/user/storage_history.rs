use crate::{
    segments::{
        user::history::{finalize_history_prune, HistoryPruneResult},
        PruneInput, Segment, SegmentOutput,
    },
    PrunerError,
};
use alloy_primitives::{Address, BlockNumber, B256};
use reth_db_api::{
    cursor::DbCursorRO,
    models::{storage_sharded_key::StorageShardedKey, BlockNumberAddress},
    tables,
    transaction::DbTxMut,
};
use reth_provider::{
    DBProvider, StaticFileProviderFactory, StaticFileSegment, StorageSettingsCache,
};
use reth_prune_types::{PruneMode, PrunePurpose, PruneSegment, SegmentOutputCheckpoint};
use rustc_hash::FxHashMap;
use tracing::{instrument, trace};

/// Number of storage history tables to prune in one step
///
/// Storage History consists of two tables: [`tables::StorageChangeSets`] and
/// [`tables::StoragesHistory`]. We want to prune them to the same block number.
const STORAGE_HISTORY_TABLES_TO_PRUNE: usize = 2;

#[derive(Debug)]
pub struct StorageHistory {
    mode: PruneMode,
}

impl StorageHistory {
    pub const fn new(mode: PruneMode) -> Self {
        Self { mode }
    }
}

impl<Provider> Segment<Provider> for StorageHistory
where
    Provider: DBProvider<Tx: DbTxMut> + StaticFileProviderFactory + StorageSettingsCache,
{
    fn segment(&self) -> PruneSegment {
        PruneSegment::StorageHistory
    }

    fn mode(&self) -> Option<PruneMode> {
        Some(self.mode)
    }

    fn purpose(&self) -> PrunePurpose {
        PrunePurpose::User
    }

    #[instrument(level = "trace", target = "pruner", skip(self, provider), ret)]
    fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput, PrunerError> {
        let range = match input.get_next_block_range() {
            Some(range) => range,
            None => {
                trace!(target: "pruner", "No storage history to prune");
                return Ok(SegmentOutput::done())
            }
        };
        let range_end = *range.end();

        // Under storage-v2 the storage changesets live in static files and the database changeset
        // table is empty, so pruning the DB table would no-op while the jars leak. Walk the static
        // files and reclaim their jars instead.
        if provider.cached_storage_settings().changesets_in_static_files {
            self.prune_static_files(provider, input, range, range_end)
        } else {
            self.prune_database(provider, input, range, range_end)
        }
    }
}

impl StorageHistory {
    /// Prunes storage history when changesets are stored in static files.
    ///
    /// Walks the changesets from static files to prune the [`tables::StoragesHistory`] index, then
    /// reclaims any changeset jar that has fallen entirely below the prune horizon.
    fn prune_static_files<Provider>(
        &self,
        provider: &Provider,
        input: PruneInput,
        range: std::ops::RangeInclusive<BlockNumber>,
        range_end: BlockNumber,
    ) -> Result<SegmentOutput, PrunerError>
    where
        Provider: DBProvider<Tx: DbTxMut> + StaticFileProviderFactory,
    {
        // Split the budget across both tables, matching the database path: the changeset walk and
        // the history-index prune each get half.
        let mut limiter = if let Some(limit) = input.limiter.deleted_entries_limit() {
            input.limiter.set_deleted_entries_limit(limit / STORAGE_HISTORY_TABLES_TO_PRUNE)
        } else {
            input.limiter
        };

        // The limiter may already be exhausted by a previous segment in the same prune run.
        if limiter.is_limit_reached() {
            return Ok(SegmentOutput::not_done(
                limiter.interrupt_reason(),
                input.previous_checkpoint.map(SegmentOutputCheckpoint::from_prune_checkpoint),
            ))
        }

        // Deleted storage changeset keys (account address + storage slot) with the highest block
        // number deleted for that key. Bounded the same way as the database path below.
        let mut highest_deleted_storages = FxHashMap::default();
        let mut last_changeset_pruned_block = None;
        let mut pruned_changesets = 0;
        let mut done = true;

        let walker = provider.static_file_provider().walk_storage_changeset_range(range);
        for result in walker {
            let (BlockNumberAddress((block_number, address)), entry) = result?;
            // The walk itself deletes nothing, so an interrupted block cannot be resumed: giving
            // up the budget inside block N reports checkpoint N-1 and the next run rereads the
            // same entries, forever. Stop on block boundaries only, overshooting the budget by at
            // most the rest of one block.
            if limiter.is_limit_reached() &&
                last_changeset_pruned_block.is_some_and(|last| last != block_number)
            {
                done = false;
                break
            }
            highest_deleted_storages.insert((address, entry.key), block_number);
            last_changeset_pruned_block = Some(block_number);
            pruned_changesets += 1;
            limiter.increment_deleted_entries_count();
        }

        // Reclaim whole jars only once the range is fully processed, so a jar straddling the
        // horizon is never removed while it still holds live blocks (`delete_segment_below_block`
        // additionally refuses to delete the highest jar).
        if done && let Some(last_block) = last_changeset_pruned_block {
            provider
                .static_file_provider()
                .delete_segment_below_block(StaticFileSegment::StorageChangeSets, last_block + 1)?;
        }
        trace!(target: "pruner", pruned = %pruned_changesets, %done, "Pruned storage history (changesets from static files)");

        let result = HistoryPruneResult {
            highest_deleted: highest_deleted_storages,
            last_pruned_block: last_changeset_pruned_block,
            pruned_count: pruned_changesets,
            done,
        };
        finalize_history_prune::<_, tables::StoragesHistory, (Address, B256), _>(
            provider,
            result,
            range_end,
            &limiter,
            |(address, storage_key), block_number| {
                StorageShardedKey::new(address, storage_key, block_number)
            },
            |a, b| a.address == b.address && a.sharded_key.key == b.sharded_key.key,
        )
        .map_err(Into::into)
    }

    /// Prunes storage history when changesets are stored in the database changeset table.
    fn prune_database<Provider>(
        &self,
        provider: &Provider,
        input: PruneInput,
        range: std::ops::RangeInclusive<BlockNumber>,
        range_end: BlockNumber,
    ) -> Result<SegmentOutput, PrunerError>
    where
        Provider: DBProvider<Tx: DbTxMut>,
    {
        let mut limiter = if let Some(limit) = input.limiter.deleted_entries_limit() {
            input.limiter.set_deleted_entries_limit(limit / STORAGE_HISTORY_TABLES_TO_PRUNE)
        } else {
            input.limiter
        };
        if limiter.is_limit_reached() {
            return Ok(SegmentOutput::not_done(
                limiter.interrupt_reason(),
                input.previous_checkpoint.map(SegmentOutputCheckpoint::from_prune_checkpoint),
            ))
        }

        // Deleted storage changeset keys (account addresses and storage slots) with the highest
        // block number deleted for that key.
        //
        // The size of this map it's limited by `prune_delete_limit * blocks_since_last_run /
        // STORAGE_HISTORY_TABLES_TO_PRUNE`, and with current default it's usually `3500 * 5
        // / 2`, so 8750 entries. Each entry is `160 bit + 256 bit + 64 bit`, so the total
        // size should be up to 0.5MB + some hashmap overhead. `blocks_since_last_run` is
        // additionally limited by the `max_reorg_depth`, so no OOM is expected here.
        let mut highest_deleted_storages = FxHashMap::default();
        let mut last_changeset_pruned_block = None;
        let mut pruned_changesets = 0;
        let mut done = true;
        let mut cursor = provider.tx_ref().cursor_write::<tables::StorageChangeSets>()?;
        let mut walker = cursor.walk_range(BlockNumberAddress::range(range))?;
        while let Some((BlockNumberAddress((block_number, address)), entry)) =
            walker.next().transpose()?
        {
            if limiter.is_limit_reached() &&
                last_changeset_pruned_block.is_some_and(|last| last != block_number)
            {
                done = false;
                break
            }

            walker.delete_current()?;
            limiter.increment_deleted_entries_count();
            pruned_changesets += 1;
            highest_deleted_storages.insert((address, entry.key), block_number);
            last_changeset_pruned_block = Some(block_number);
        }
        trace!(target: "pruner", deleted = %pruned_changesets, %done, "Pruned storage history (changesets from database)");

        let result = HistoryPruneResult {
            highest_deleted: highest_deleted_storages,
            last_pruned_block: last_changeset_pruned_block,
            pruned_count: pruned_changesets,
            done,
        };
        finalize_history_prune::<_, tables::StoragesHistory, (Address, B256), _>(
            provider,
            result,
            range_end,
            &limiter,
            |(address, storage_key), block_number| {
                StorageShardedKey::new(address, storage_key, block_number)
            },
            |a, b| a.address == b.address && a.sharded_key.key == b.sharded_key.key,
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use crate::segments::{
        user::storage_history::STORAGE_HISTORY_TABLES_TO_PRUNE, PruneInput, PruneLimiter, Segment,
        SegmentOutput, StorageHistory,
    };
    use alloy_primitives::{BlockNumber, B256};
    use assert_matches::assert_matches;
    use reth_db_api::{models::GravityStorageSettings, tables, BlockNumberList};
    use reth_provider::{
        DatabaseProviderFactory, PruneCheckpointReader, StaticFileProviderFactory,
        StaticFileSegment, StorageSettingsCache,
    };
    use reth_prune_types::{PruneCheckpoint, PruneMode, PruneProgress, PruneSegment};
    use reth_stages::test_utils::{StorageKind, TestStageDB};
    use reth_testing_utils::generators::{
        self, random_block_range, random_changeset_range, random_eoa_accounts, BlockRangeParams,
    };
    use std::{collections::BTreeMap, ops::AddAssign};

    #[test]
    fn prune() {
        let db = TestStageDB::default();
        let mut rng = generators::rng();

        let blocks = random_block_range(
            &mut rng,
            0..=5000,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..1, ..Default::default() },
        );
        db.insert_blocks(blocks.iter(), StorageKind::Database(None)).expect("insert blocks");

        let accounts = random_eoa_accounts(&mut rng, 2).into_iter().collect::<BTreeMap<_, _>>();

        let (changesets, _) = random_changeset_range(
            &mut rng,
            blocks.iter(),
            accounts.into_iter().map(|(addr, acc)| (addr, (acc, Vec::new()))),
            1..2,
            1..2,
        );
        db.insert_changesets(changesets.clone(), None).expect("insert changesets");
        db.insert_history(changesets.clone(), None).expect("insert history");

        let storage_occurrences = db.table::<tables::StoragesHistory>().unwrap().into_iter().fold(
            BTreeMap::<_, usize>::new(),
            |mut map, (key, _)| {
                map.entry((key.address, key.sharded_key.key)).or_default().add_assign(1);
                map
            },
        );
        assert!(storage_occurrences.into_iter().any(|(_, occurrences)| occurrences > 1));

        assert_eq!(
            db.table::<tables::StorageChangeSets>().unwrap().len(),
            changesets.iter().flatten().flat_map(|(_, _, entries)| entries).count()
        );

        let original_shards = db.table::<tables::StoragesHistory>().unwrap();

        let test_prune = |to_block: BlockNumber,
                          run: usize,
                          expected_result: (PruneProgress, usize)| {
            let prune_mode = PruneMode::Before(to_block);
            let deleted_entries_limit = 1000;
            let mut limiter =
                PruneLimiter::default().set_deleted_entries_limit(deleted_entries_limit);
            let input = PruneInput {
                previous_checkpoint: db
                    .factory
                    .provider()
                    .unwrap()
                    .get_prune_checkpoint(PruneSegment::StorageHistory)
                    .unwrap(),
                to_block,
                limiter: limiter.clone(),
            };
            let segment = StorageHistory::new(prune_mode);

            let provider = db.factory.database_provider_rw().unwrap();
            let result = segment.prune(&provider, input).unwrap();
            limiter.increment_deleted_entries_count_by(result.pruned);

            assert_matches!(
                result,
                SegmentOutput {progress, pruned, checkpoint: Some(_)}
                    if (progress, pruned) == expected_result
            );

            segment
                .save_checkpoint(
                    &provider,
                    result.checkpoint.unwrap().as_prune_checkpoint(prune_mode),
                )
                .unwrap();
            provider.commit().expect("commit");

            let changesets = changesets
                .iter()
                .enumerate()
                .flat_map(|(block_number, changeset)| {
                    changeset.iter().flat_map(move |(address, _, entries)| {
                        entries.iter().map(move |entry| (block_number, address, entry))
                    })
                })
                .collect::<Vec<_>>();

            #[expect(clippy::skip_while_next)]
            let pruned = changesets
                .iter()
                .enumerate()
                .skip_while(|(i, (block_number, _, _))| {
                    *i < deleted_entries_limit / STORAGE_HISTORY_TABLES_TO_PRUNE * run &&
                        *block_number <= to_block as usize
                })
                .next()
                .map(|(i, _)| i)
                .unwrap_or_default();

            let mut pruned_changesets = changesets.iter().skip(pruned.saturating_sub(1));

            let last_pruned_block_number = pruned_changesets
                .next()
                .map(|(block_number, _, _)| *block_number as BlockNumber)
                .unwrap_or(to_block);

            let pruned_changesets = pruned_changesets.fold(
                BTreeMap::<_, Vec<_>>::new(),
                |mut acc, (block_number, address, entry)| {
                    acc.entry((block_number, address)).or_default().push(entry);
                    acc
                },
            );

            assert_eq!(
                db.table::<tables::StorageChangeSets>().unwrap().len(),
                pruned_changesets.values().flatten().count()
            );

            let actual_shards = db.table::<tables::StoragesHistory>().unwrap();

            let expected_shards = original_shards
                .iter()
                .filter(|(key, _)| key.sharded_key.highest_block_number > last_pruned_block_number)
                .map(|(key, blocks)| {
                    let new_blocks =
                        blocks.iter().skip_while(|block| *block <= last_pruned_block_number);
                    (key.clone(), BlockNumberList::new_pre_sorted(new_blocks))
                })
                .collect::<Vec<_>>();

            assert_eq!(actual_shards, expected_shards);

            assert_eq!(
                db.factory
                    .provider()
                    .unwrap()
                    .get_prune_checkpoint(PruneSegment::StorageHistory)
                    .unwrap(),
                Some(PruneCheckpoint {
                    block_number: Some(last_pruned_block_number),
                    tx_number: None,
                    prune_mode
                })
            );
        };

        test_prune(
            998,
            1,
            (
                PruneProgress::HasMoreData(
                    reth_prune_types::PruneInterruptReason::DeletedEntriesLimitReached,
                ),
                500,
            ),
        );
        test_prune(998, 2, (PruneProgress::Finished, 499));
        test_prune(1200, 3, (PruneProgress::Finished, 202));
    }

    /// Exercises the static-file changeset path. With `changesets_in_static_files` enabled the
    /// changesets live in static files (the DB changeset table is empty), so pruning must walk the
    /// static files to prune the `StoragesHistory` index and to reclaim below-horizon jars.
    #[test]
    fn prune_static_files_path() {
        let db = TestStageDB::default();
        let mut rng = generators::rng();

        let blocks = random_block_range(
            &mut rng,
            0..=100,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..1, ..Default::default() },
        );
        db.insert_blocks(blocks.iter(), StorageKind::Database(None)).expect("insert blocks");

        let accounts = random_eoa_accounts(&mut rng, 2).into_iter().collect::<BTreeMap<_, _>>();
        let (changesets, _) = random_changeset_range(
            &mut rng,
            blocks.iter(),
            accounts.into_iter().map(|(addr, acc)| (addr, (acc, Vec::new()))),
            1..2,
            1..2,
        );

        // Changesets in static files, history index in the database.
        db.insert_changesets_to_static_files(changesets.clone(), None)
            .expect("insert changesets to static files");
        db.insert_history(changesets.clone(), None).expect("insert history");

        // Under this configuration the database changeset table stays empty.
        assert!(db.table::<tables::StorageChangeSets>().unwrap().is_empty());

        let count_index_blocks = || {
            db.table::<tables::StoragesHistory>()
                .unwrap()
                .iter()
                .map(|(_, blocks)| blocks.iter().count())
                .sum::<usize>()
        };
        let blocks_before = count_index_blocks();
        assert!(blocks_before > 0);

        let to_block: BlockNumber = 50;
        let prune_mode = PruneMode::Before(to_block);
        let input =
            PruneInput { previous_checkpoint: None, to_block, limiter: PruneLimiter::default() };
        let segment = StorageHistory::new(prune_mode);

        // Route to the static-file path.
        db.factory.set_storage_settings_cache(GravityStorageSettings {
            changesets_in_static_files: true,
        });

        let provider = db.factory.database_provider_rw().unwrap();
        let result = segment.prune(&provider, input).unwrap();

        assert_matches!(
            result,
            SegmentOutput { progress: PruneProgress::Finished, pruned, checkpoint: Some(_) }
                if pruned > 0
        );

        segment
            .save_checkpoint(&provider, result.checkpoint.unwrap().as_prune_checkpoint(prune_mode))
            .unwrap();
        provider.commit().expect("commit");

        // The history index no longer references pruned blocks, and it shrank.
        assert!(count_index_blocks() < blocks_before);
        for (_, blocks) in db.table::<tables::StoragesHistory>().unwrap() {
            assert!(blocks.iter().all(|b| b > to_block));
        }

        // Checkpoint advanced to the prune horizon.
        assert_eq!(
            db.factory
                .provider()
                .unwrap()
                .get_prune_checkpoint(PruneSegment::StorageHistory)
                .unwrap(),
            Some(PruneCheckpoint { block_number: Some(to_block), tx_number: None, prune_mode })
        );

        // The single changeset jar spans the tip, so it is not reclaimed, but nothing above the
        // horizon is lost: the static-file tip is preserved.
        assert_eq!(
            db.factory
                .static_file_provider()
                .get_highest_static_file_block(StaticFileSegment::StorageChangeSets),
            Some(100)
        );
    }

    /// A block holding at least a whole run's budget of changesets must not stall pruning: the
    /// walk deletes no changesets, so a checkpoint rewound below such a block would make every
    /// later run reread it and never advance.
    #[test]
    fn dense_block_advances_static_file_checkpoint() {
        use alloy_primitives::U256;
        use reth_primitives_traits::StorageEntry;

        let db = TestStageDB::default();
        let mut rng = generators::rng();

        let blocks = random_block_range(
            &mut rng,
            0..=20,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..1, ..Default::default() },
        );
        db.insert_blocks(blocks.iter(), StorageKind::Database(None)).expect("insert blocks");

        // Two storage changesets per block, so a budget of two (after table split) makes every
        // block "dense".
        const ENTRIES_PER_BLOCK: usize = 2;
        let (address, account) = random_eoa_accounts(&mut rng, 1).into_iter().next().unwrap();
        let keys = [B256::with_last_byte(1), B256::with_last_byte(2)];
        let changesets = (0..=20)
            .map(|_| {
                vec![(
                    address,
                    account,
                    keys.iter()
                        .map(|key| StorageEntry { key: *key, value: U256::from(1) })
                        .collect(),
                )]
            })
            .collect::<Vec<_>>();
        db.insert_changesets_to_static_files(changesets.clone(), None)
            .expect("insert changesets to static files");
        db.insert_history(changesets, None).expect("insert history");
        assert!(db.table::<tables::StorageChangeSets>().unwrap().is_empty());

        let to_block = 15u64;
        let prune_mode = PruneMode::Before(to_block);
        let segment = StorageHistory::new(prune_mode);

        // Start from a checkpoint in the middle so a rewind can't be masked by block 0.
        let mut checkpoint = PruneCheckpoint { block_number: Some(4), tx_number: None, prune_mode };

        db.factory.set_storage_settings_cache(GravityStorageSettings {
            changesets_in_static_files: true,
        });

        for _ in 0..3 {
            let previous = checkpoint.block_number;
            let input = PruneInput {
                previous_checkpoint: Some(checkpoint),
                to_block,
                // Halved internally by STORAGE_HISTORY_TABLES_TO_PRUNE.
                limiter: PruneLimiter::default()
                    .set_deleted_entries_limit(ENTRIES_PER_BLOCK * STORAGE_HISTORY_TABLES_TO_PRUNE),
            };

            let provider = db.factory.database_provider_rw().unwrap();
            provider.set_storage_settings_cache(GravityStorageSettings {
                changesets_in_static_files: true,
            });
            let result = segment.prune(&provider, input).unwrap();
            segment
                .save_checkpoint(
                    &provider,
                    result.checkpoint.unwrap().as_prune_checkpoint(prune_mode),
                )
                .unwrap();
            provider.commit().expect("commit");

            checkpoint = db
                .factory
                .provider()
                .unwrap()
                .get_prune_checkpoint(PruneSegment::StorageHistory)
                .unwrap()
                .unwrap();

            assert!(
                !result.progress.is_finished(),
                "the range is longer than one run's budget allows"
            );
            assert!(
                checkpoint.block_number > previous,
                "checkpoint must advance past the dense block, got {:?} after {previous:?}",
                checkpoint.block_number
            );
        }
        assert_eq!(checkpoint.block_number, Some(7), "one dense block cleared per run");

        // With enough budget the remainder of the range completes in one run.
        let input = PruneInput {
            previous_checkpoint: Some(checkpoint),
            to_block,
            limiter: PruneLimiter::default().set_deleted_entries_limit(1000),
        };
        let provider = db.factory.database_provider_rw().unwrap();
        provider.set_storage_settings_cache(GravityStorageSettings {
            changesets_in_static_files: true,
        });
        let result = segment.prune(&provider, input).unwrap();
        segment
            .save_checkpoint(&provider, result.checkpoint.unwrap().as_prune_checkpoint(prune_mode))
            .unwrap();
        provider.commit().expect("commit");

        let checkpoint = db
            .factory
            .provider()
            .unwrap()
            .get_prune_checkpoint(PruneSegment::StorageHistory)
            .unwrap()
            .unwrap();
        assert!(result.progress.is_finished());
        assert_eq!(checkpoint.block_number, Some(to_block));
    }
}
