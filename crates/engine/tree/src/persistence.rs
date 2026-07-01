use crate::metrics::PersistenceMetrics;
<<<<<<< HEAD
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use gravity_primitives::get_gravity_config;
use reth_chain_state::{ExecutedBlock, ExecutedBlockWithTrieUpdates};
use reth_db::{
    set_fail_point, tables,
    transaction::{DbTx, DbTxMut},
};
use reth_errors::ProviderError;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    providers::ProviderNodeTypes, writer::UnifiedStorageWriter, BlockHashReader, BlockWriter,
    ChainStateBlockWriter, DatabaseProviderFactory, HistoryWriter, ProviderFactory,
    StageCheckpointWriter, StateWriter, StaticFileProviderFactory, StaticFileWriter,
    StorageLocation, TrieWriter, TrieWriterV2, PERSIST_BLOCK_CACHE,
};
use reth_prune::{PrunerError, PrunerOutput, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender, StageCheckpoint, StageId};
use revm::database::OriginalValuesKnown;
=======
use alloy_eips::BlockNumHash;
use crossbeam_channel::Sender as CrossbeamSender;
use reth_chain_state::ExecutedBlock;
use reth_errors::ProviderError;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives_traits::{FastInstant as Instant, NodePrimitives};
use reth_provider::{
    providers::ProviderNodeTypes, BalProvider, BlockExecutionWriter, BlockHashReader,
    ChainStateBlockWriter, DBProvider, DatabaseProviderFactory, ProviderFactory, SaveBlocksMode,
};
use reth_prune::{PrunerError, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender};
use reth_tasks::spawn_os_thread;
>>>>>>> v2.3.0
use std::{
    sync::{
        mpsc::{Receiver, SendError, Sender},
        Arc,
    },
<<<<<<< HEAD
    thread,
    time::Instant,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, error, info};

/// When `persist_merge_blocks` is on, close the current merged group once its accumulated
/// `gas_used` crosses this threshold (a cheap proxy for transaction/receipt write volume).
const MERGE_GROUP_MAX_GAS: u64 = 1_000_000_000;
/// Also close the group once the accumulated number of changed hashed accounts crosses this
/// threshold. This bounds the in-memory write batch and the trie/state write volume by actual
/// state churn rather than a raw block count (mostly-empty catch-up blocks coalesce freely; a
/// burst of state-heavy blocks closes the group sooner).
const MERGE_GROUP_MAX_STATE: usize = 10_000;
=======
    thread::JoinHandle,
    time::Duration,
};
use thiserror::Error;
use tracing::{debug, error, instrument, warn};

/// Unified result of any persistence operation.
#[derive(Debug)]
pub struct PersistenceResult {
    /// The last block that was persisted, if any.
    pub last_block: Option<BlockNumHash>,
    /// The commit duration, only available for save-blocks operations.
    pub commit_duration: Option<Duration>,
}
>>>>>>> v2.3.0

/// Writes parts of reth's in memory tree state to the database and static files.
///
/// This is meant to be a spawned service that listens for various incoming persistence operations,
/// performing those actions on disk, and returning the result in a channel.
///
/// This should be spawned in its own thread with [`std::thread::spawn`], since this performs
/// blocking I/O operations in an endless loop.
#[derive(Debug)]
pub struct PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// The provider factory to use
    provider: ProviderFactory<N>,
    /// Incoming requests
    incoming: Receiver<PersistenceAction<N::Primitives>>,
    /// The pruner
    pruner: PrunerWithFactory<ProviderFactory<N>>,
    /// metrics
    metrics: PersistenceMetrics,
    /// Sender for sync metrics - we only submit sync metrics for persisted blocks
    sync_metrics_tx: MetricEventsSender,
    /// Pending finalized block number to be committed with the next block save.
    /// This avoids triggering a separate fsync for each finalized block update.
    pending_finalized_block: Option<u64>,
    /// Pending safe block number to be committed with the next block save.
    /// This avoids triggering a separate fsync for each safe block update.
    pending_safe_block: Option<u64>,
}

impl<N> PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// Create a new persistence service
    pub fn new(
        provider: ProviderFactory<N>,
        incoming: Receiver<PersistenceAction<N::Primitives>>,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
    ) -> Self {
        Self {
            provider,
            incoming,
            pruner,
            metrics: PersistenceMetrics::default(),
            sync_metrics_tx,
            pending_finalized_block: None,
            pending_safe_block: None,
        }
    }
}

impl<N> PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// This is the main loop, that will listen to database events and perform the requested
    /// database actions
    pub fn run(mut self) -> Result<(), PersistenceError> {
        // If the receiver errors then senders have disconnected, so the loop should then end.
        while let Ok(action) = self.incoming.recv() {
            match action {
                PersistenceAction::RemoveBlocksAbove(new_tip_num, sender) => {
                    let last_block = self.on_remove_blocks_above(new_tip_num)?;
                    // send new sync metrics based on removed blocks
                    let _ =
                        self.sync_metrics_tx.send(MetricEvent::SyncHeight { height: new_tip_num });
                    let _ = sender.send(PersistenceResult { last_block, commit_duration: None });
                }
                PersistenceAction::SaveBlocks(blocks, sender) => {
                    let result = self.on_save_blocks(blocks)?;
<<<<<<< HEAD
                    let result_number = result.map(|r| r.number);

                    // we ignore the error because the caller may or may not care about the result
=======
                    let result_number = result.last_block.map(|b| b.number);

>>>>>>> v2.3.0
                    let _ = sender.send(result);

                    if let Some(block_number) = result_number {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: block_number });
<<<<<<< HEAD

                        if self.pruner.is_pruning_needed(block_number) {
                            // We log `PrunerOutput` inside the `Pruner`
                            let _ = self.prune_before(block_number)?;
                        }
                    }
                }
                PersistenceAction::SaveFinalizedBlock(finalized_block) => {
                    let provider = self.provider.database_provider_rw()?;
                    provider.save_finalized_block_number(finalized_block)?;
                    provider.commit()?;
                }
                PersistenceAction::SaveSafeBlock(safe_block) => {
                    let provider = self.provider.database_provider_rw()?;
                    provider.save_safe_block_number(safe_block)?;
                    provider.commit()?;
=======
                        self.maybe_run_pruner(block_number)?;
                    }
                }
                PersistenceAction::SaveFinalizedBlock(finalized_block) => {
                    self.pending_finalized_block = Some(finalized_block);
                }
                PersistenceAction::SaveSafeBlock(safe_block) => {
                    self.pending_safe_block = Some(safe_block);
>>>>>>> v2.3.0
                }
            }
        }
        Ok(())
    }

    #[instrument(level = "debug", target = "engine::persistence", skip_all, fields(%new_tip_num))]
    fn on_remove_blocks_above(
        &self,
        new_tip_num: u64,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", ?new_tip_num, "Removing blocks");
        let start_time = Instant::now();
        let provider_rw = self.provider.database_provider_rw()?;

        let new_tip_hash = provider_rw.block_hash(new_tip_num)?;
<<<<<<< HEAD
        UnifiedStorageWriter::from(&provider_rw, &sf_provider).remove_blocks_above(new_tip_num)?;
        UnifiedStorageWriter::commit_unwind(provider_rw)?;
=======
        provider_rw.remove_block_and_execution_above(new_tip_num)?;
        provider_rw.commit()?;
>>>>>>> v2.3.0

        debug!(target: "engine::persistence", ?new_tip_num, ?new_tip_hash, "Removed blocks from disk");
        self.metrics.remove_blocks_above_duration_seconds.record(start_time.elapsed());
        Ok(new_tip_hash.map(|hash| BlockNumHash { hash, number: new_tip_num }))
    }

<<<<<<< HEAD
    fn get_checkpoint<TX: DbTx>(
        tx: &TX,
        stage_id: StageId,
        check_next: Option<u64>,
    ) -> Result<StageCheckpoint, ProviderError> {
        let ck = tx
            .get::<tables::StageCheckpoints>(stage_id.to_string())
            .map_err(ProviderError::Database)
            .map(Option::unwrap_or_default)?;
        if let Some(next) = check_next {
            if next == 0 {
                // for test
                assert_eq!(ck.block_number, 0);
            } else {
                assert_eq!(
                    ck.block_number + 1,
                    next,
                    "Stage {stage_id}'s checkpoint is inconsistent"
                );
            }
        }
        Ok(ck)
    }

    fn update_checkpoint<TX: DbTxMut>(
        tx: &TX,
        stage_id: StageId,
        checkpoint: StageCheckpoint,
    ) -> Result<(), ProviderError> {
        tx.put::<tables::StageCheckpoints>(stage_id.to_string(), checkpoint)
            .map_err(ProviderError::Database)
    }

    fn on_save_blocks(
        &self,
        blocks: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", first=?blocks.first().map(|b| b.recovered_block.num_hash()), last=?blocks.last().map(|b| b.recovered_block.num_hash()), "Saving range of blocks");
        let start_time = Instant::now();
        let last_block_hash_num = blocks.last().map(|block| BlockNumHash {
            hash: block.recovered_block().hash(),
            number: block.recovered_block().header().number(),
        });

        let num_blocks = blocks.len();
        if last_block_hash_num.is_some() {
            let first_block = blocks.first().unwrap().recovered_block();
            let last_block = blocks.last().unwrap().recovered_block();
            let first_number = first_block.number();
            let last_block_number = last_block.number();
            debug!(target: "provider::storage_writer", block_count = blocks.len(), "Writing blocks and execution data to storage");

            if get_gravity_config().persist_merge_blocks {
                self.save_merged_blocks(blocks)?;
            } else {
                self.save_blocks_per_block(blocks)?;
            }

            // Update pipeline progress
            let start_time = Instant::now();
            let provider_rw = self.provider.database_provider_rw()?;
            provider_rw.update_pipeline_stages(last_block_number, false)?;
            provider_rw.commit()?;
            self.metrics
                .persist_commit_duration_seconds
                .record(start_time.elapsed().as_secs_f64() / num_blocks as f64);
            debug!(target: "provider::storage_writer", range = ?first_number..=last_block_number, "Appended block data");
        }
        let elapsed = start_time.elapsed();
        self.metrics.save_blocks_duration_seconds.record(elapsed);
        self.metrics
            .save_duration_per_block_seconds
            .record(elapsed.as_secs_f64() / num_blocks as f64);
        Ok(last_block_hash_num)
=======
    #[instrument(level = "debug", target = "engine::persistence", skip_all, fields(block_count = blocks.len()))]
    fn on_save_blocks(
        &mut self,
        blocks: Vec<ExecutedBlock<N::Primitives>>,
    ) -> Result<PersistenceResult, PersistenceError> {
        let first_block = blocks.first().map(|b| b.recovered_block.num_hash());
        let last_block = blocks.last().map(|b| b.recovered_block.num_hash());
        let block_count = blocks.len();

        let pending_finalized = self.pending_finalized_block.take();
        let pending_safe = self.pending_safe_block.take();

        debug!(target: "engine::persistence", ?block_count, first=?first_block, last=?last_block, "Saving range of blocks");

        let start_time = Instant::now();

        if let Some(last) = last_block {
            let provider_rw = self.provider.database_provider_rw()?;
            provider_rw.save_blocks(blocks, SaveBlocksMode::Full)?;

            if let Some(finalized) = pending_finalized {
                provider_rw.save_finalized_block_number(finalized.min(last.number))?;
                if finalized > last.number {
                    self.pending_finalized_block = Some(finalized);
                }
            }
            if let Some(safe) = pending_safe {
                provider_rw.save_safe_block_number(safe.min(last.number))?;
                if safe > last.number {
                    self.pending_safe_block = Some(safe);
                }
            }

            provider_rw.commit()?;
            let _ = self.provider.bal_store().flush().inspect_err(|err| {
                warn!(target: "engine::persistence", last=?last_block, ?err, "Failed to flush BAL store");
            });
            debug!(target: "engine::persistence", first=?first_block, last=?last_block, "Saved range of blocks");
        }

        let elapsed = start_time.elapsed();
        self.metrics.save_blocks_batch_size.record(block_count as f64);
        self.metrics.save_blocks_duration_seconds.record(elapsed);

        Ok(PersistenceResult { last_block, commit_duration: Some(elapsed) })
    }

    fn maybe_run_pruner(&mut self, block_number: u64) -> Result<(), PersistenceError> {
        // The durable save is already committed at this point, so pruning can happen after we
        // acknowledge the save without extending the synchronous persistence wait.
        if self.pruner.is_pruning_needed(block_number) {
            debug!(target: "engine::persistence", block_num=?block_number, "Running pruner");
            let prune_start = Instant::now();
            let provider_rw = self.provider.database_provider_rw()?;
            let _ = self.pruner.run_with_provider(&provider_rw, block_number)?;
            provider_rw.commit()?;
            let pruned_bals = self
                .provider
                .bal_store()
                .prune(block_number)
                .inspect_err(|err| {
                    warn!(target: "engine::persistence", tip=?block_number, ?err, "Failed to prune BAL store");
                })
                .unwrap_or_default();
            debug!(target: "engine::persistence", tip=?block_number, pruned_bals, "Finished pruning after saving blocks");
            self.metrics.prune_before_duration_seconds.record(prune_start.elapsed());
        }

        Ok(())
>>>>>>> v2.3.0
    }

    /// Persist `blocks` one at a time, committing each block per stage (state / hashed / history /
    /// trie) before moving on. This is the durable default: a crash never loses more than the
    /// single block in flight.
    fn save_blocks_per_block(
        &self,
        blocks: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
    ) -> Result<(), PersistenceError> {
        for ExecutedBlockWithTrieUpdates {
            block: ExecutedBlock { recovered_block, execution_output, hashed_state },
            trie,
            triev2,
        } in blocks
        {
            let block_number = recovered_block.number();
            let block_hash = recovered_block.hash();
            let inner_provider = &self.provider;
            info!(target: "persistence::save_block", block_number = block_number, "Write block updates into DB");

            // Parallel execution of state and trie updates is safe because the database is
            // split into three separate RocksDB instances: state_db (for state and history),
            // account_db (for account trie), and storage_db (for storage trie). This allows
            // concurrent writes and commits across different DB instances without conflicts.
            // The `write_trie_updatesv2` implementation also parallelizes writes to account_db
            // and storage_db internally. For fault tolerance, stage checkpoints ensure
            // idempotency - each stage's checkpoint is verified before writing, guaranteeing
            // exactly-once execution even if the process crashes mid-block.
            thread::scope(|scope| -> Result<(), PersistenceError> {
                let state_handle = scope.spawn(|| -> Result<(), PersistenceError> {
                    let start = Instant::now();
                    let provider_rw = inner_provider.database_provider_rw()?;
                    let ck = Self::get_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::Execution,
                        Some(block_number),
                    )?;
                    let body_indices = provider_rw.insert_block(
                        Arc::unwrap_or_clone(recovered_block),
                        StorageLocation::Both,
                    )?;
                    set_fail_point!("persistence::after_write_state");
                    // Write state and changesets to the database.
                    // Must be written after blocks because of the receipt lookup.
                    provider_rw.write_state_with_indices(
                        &execution_output,
                        OriginalValuesKnown::No,
                        StorageLocation::StaticFiles,
                        Some(vec![body_indices]),
                    )?;
                    Self::update_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::Execution,
                        StageCheckpoint { block_number, ..ck },
                    )?;
                    provider_rw.static_file_provider().commit()?;
                    provider_rw.commit()?;
                    set_fail_point!("persistence::after_state_commit");
                    metrics::histogram!("save_blocks_time", &[("process", "write_state")])
                        .record(start.elapsed());

                    let start = Instant::now();
                    let provider_rw = inner_provider.database_provider_rw()?;
                    let ck = Self::get_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::AccountHashing,
                        Some(block_number),
                    )?;
                    // insert hashes and intermediate merkle nodes
                    provider_rw
                        .write_hashed_state(&Arc::unwrap_or_clone(hashed_state).into_sorted())?;
                    set_fail_point!("persistence::after_hashed_state");
                    Self::update_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::AccountHashing,
                        StageCheckpoint { block_number, ..ck },
                    )?;
                    provider_rw.commit()?;
                    set_fail_point!("persistence::after_hashed_state_commit");
                    metrics::histogram!("save_blocks_time", &[("process", "write_hashed_state")])
                        .record(start.elapsed());

                    let start = Instant::now();
                    let provider_rw = inner_provider.database_provider_rw()?;
                    let ck = Self::get_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::IndexAccountHistory,
                        Some(block_number),
                    )?;
                    provider_rw.update_history_indices(block_number..=block_number)?;
                    set_fail_point!("persistence::after_history_indices");
                    Self::update_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::IndexAccountHistory,
                        StageCheckpoint { block_number, ..ck },
                    )?;
                    provider_rw.commit()?;
                    set_fail_point!("persistence::after_history_commit");
                    metrics::histogram!(
                        "save_blocks_time",
                        &[("process", "update_history_indices")]
                    )
                    .record(start.elapsed());
                    Ok(())
                });
                let trie_handle = scope.spawn(|| -> Result<(), PersistenceError> {
                    let start = Instant::now();
                    let provider_rw = inner_provider.database_provider_rw()?;
                    let ck =
                        Self::get_checkpoint(provider_rw.tx_ref(), StageId::MerkleExecute, None)?;
                    if ck.block_number + 1 != block_number {
                        info!(target: "persistence::trie_update",
                            checkpoint = ck.block_number,
                            block_number = block_number,
                            "Detected interrupted trie update, but trie has idempotency");
                    }
                    provider_rw.write_trie_updates(
                        trie.as_ref().ok_or(ProviderError::MissingTrieUpdates(block_hash))?,
                    )?;
                    provider_rw
                        .write_trie_updatesv2(triev2.as_ref())
                        .map_err(ProviderError::Database)?;
                    set_fail_point!("persistence::after_trie_update");
                    Self::update_checkpoint(
                        provider_rw.tx_ref(),
                        StageId::MerkleExecute,
                        StageCheckpoint { block_number, ..ck },
                    )?;
                    provider_rw.commit()?;
                    set_fail_point!("persistence::after_trie_commit");
                    metrics::histogram!("save_blocks_time", &[("process", "write_trie_updatesv2")])
                        .record(start.elapsed());
                    Ok(())
                });
                state_handle.join().unwrap()?;
                trie_handle.join().unwrap()
            })?;
            PERSIST_BLOCK_CACHE.persist_tip(block_number);
        }
        Ok(())
    }

    /// Persist `blocks` as a sequence of merged groups, each committed once. Groups are bounded by
    /// [`MERGE_GROUP_MAX_GAS`] and [`MERGE_GROUP_MAX_STATE`] so the in-flight write batch and the
    /// crash-replay window stay bounded.
    fn save_merged_blocks(
        &self,
        blocks: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
    ) -> Result<(), PersistenceError> {
        let mut group: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>> = Vec::new();
        let mut group_gas = 0u64;
        let mut group_state = 0usize;
        for block in blocks {
            let gas = block.recovered_block().header().gas_used();
            let state = block.hashed_state().accounts.len();
            // Close the current (non-empty) group before it would cross a bound; a single block
            // that alone exceeds a bound becomes its own group.
            if !group.is_empty() &&
                (group_gas.saturating_add(gas) > MERGE_GROUP_MAX_GAS ||
                    group_state.saturating_add(state) > MERGE_GROUP_MAX_STATE)
            {
                self.commit_block_group(std::mem::take(&mut group))?;
                group_gas = 0;
                group_state = 0;
            }
            group_gas = group_gas.saturating_add(gas);
            group_state = group_state.saturating_add(state);
            group.push(block);
        }
        self.commit_block_group(group)
    }

    /// Write one contiguous group of executed blocks and commit it once.
    ///
    /// The whole group is written into a single transaction and committed together, so the
    /// per-commit fsync is paid once per group instead of once per block. The group is atomic: a
    /// crash before the commit rolls it back and recovery re-executes it idempotently from the
    /// stage checkpoints, with consensus re-supplying anything past the persisted tip. Batching the
    /// per-block `write_*` calls is safe because state/hashed/trie writes are last-writer-wins puts
    /// and receipts use the indices returned by `insert_blocks`, so none of them depends on
    /// observing earlier uncommitted writes within the transaction.
    fn commit_block_group(
        &self,
        group: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
    ) -> Result<(), PersistenceError> {
        let Some(first) = group.first() else { return Ok(()) };
        let group_first = first.recovered_block().number();
        let group_last = group.last().unwrap().recovered_block().number();
        let block_count = group.len() as u32;
        info!(target: "persistence::save_block", group_first, group_last, count = block_count, "Write merged block group into DB");
        let start = Instant::now();

        // Split the group into the per-stage artifacts each writer consumes.
        let mut recovered_blocks = Vec::with_capacity(group.len());
        let mut execution_outputs = Vec::with_capacity(group.len());
        let mut hashed_states = Vec::with_capacity(group.len());
        let mut trie_updates = Vec::with_capacity(group.len());
        for ExecutedBlockWithTrieUpdates {
            block: ExecutedBlock { recovered_block, execution_output, hashed_state },
            trie,
            triev2,
        } in group
        {
            let block_hash = recovered_block.hash();
            recovered_blocks.push(Arc::unwrap_or_clone(recovered_block));
            execution_outputs.push(execution_output);
            hashed_states.push(hashed_state);
            trie_updates.push((trie, triev2, block_hash));
        }

        let provider_rw = self.provider.database_provider_rw()?;

        // Headers / bodies / senders / tx lookups for the whole group (transaction numbers threaded
        // across the batch in memory by `insert_blocks`).
        let body_indices = provider_rw.insert_blocks(recovered_blocks, StorageLocation::Both)?;

        // Receipts, state changesets and hashed state, per block.
        for ((execution_output, hashed_state), body_index) in
            execution_outputs.into_iter().zip(hashed_states).zip(body_indices)
        {
            provider_rw.write_state_with_indices(
                &execution_output,
                OriginalValuesKnown::No,
                StorageLocation::StaticFiles,
                Some(vec![body_index]),
            )?;
            provider_rw.write_hashed_state(&Arc::unwrap_or_clone(hashed_state).into_sorted())?;
        }

        // Trie updates, per block.
        for (trie, triev2, block_hash) in &trie_updates {
            provider_rw.write_trie_updates(
                trie.as_ref().ok_or(ProviderError::MissingTrieUpdates(*block_hash))?,
            )?;
            provider_rw.write_trie_updatesv2(triev2.as_ref()).map_err(ProviderError::Database)?;
        }

        // History indices for the whole range, once.
        provider_rw.update_history_indices(group_first..=group_last)?;

        // Advance every written stage's checkpoint to the group tip, then commit the group once.
        // `MerkleExecute` passes `None` (trie writes are idempotent and may resume mid-range); the
        // rest assert checkpoint continuity from `group_first`.
        let tx = provider_rw.tx_ref();
        Self::advance_checkpoint(tx, StageId::Execution, Some(group_first), group_last)?;
        Self::advance_checkpoint(tx, StageId::AccountHashing, Some(group_first), group_last)?;
        Self::advance_checkpoint(tx, StageId::IndexAccountHistory, Some(group_first), group_last)?;
        Self::advance_checkpoint(tx, StageId::MerkleExecute, None, group_last)?;

        provider_rw.static_file_provider().commit()?;
        provider_rw.commit()?;
        PERSIST_BLOCK_CACHE.persist_tip(group_last);

        metrics::histogram!("save_blocks_time", &[("process", "merge_block")])
            .record(start.elapsed() / block_count);
        Ok(())
    }

    /// Read `stage_id`'s checkpoint (asserting continuity when `check_next` is set) and re-write it
    /// at block `to`. Lets [`commit_block_group`](Self::commit_block_group) advance every stage to
    /// the group tip within the single group commit.
    fn advance_checkpoint<TX: DbTx + DbTxMut>(
        tx: &TX,
        stage_id: StageId,
        check_next: Option<u64>,
        to: u64,
    ) -> Result<(), ProviderError> {
        let ck = Self::get_checkpoint(tx, stage_id, check_next)?;
        Self::update_checkpoint(tx, stage_id, StageCheckpoint { block_number: to, ..ck })
    }
}

/// One of the errors that can happen when using the persistence service.
#[derive(Debug, Error)]
pub enum PersistenceError {
    /// A pruner error
    #[error(transparent)]
    PrunerError(#[from] PrunerError),

    /// A provider error
    #[error(transparent)]
    ProviderError(#[from] ProviderError),
}

/// A signal to the persistence service that part of the tree state can be persisted.
#[derive(Debug)]
pub enum PersistenceAction<N: NodePrimitives = EthPrimitives> {
    /// The section of tree state that should be persisted. These blocks are expected in order of
    /// increasing block number.
    ///
    /// First, header, transaction, and receipt-related data should be written to static files.
    /// Then the execution history-related data will be written to the database.
<<<<<<< HEAD
    SaveBlocks(Vec<ExecutedBlockWithTrieUpdates<N>>, oneshot::Sender<Option<BlockNumHash>>),
=======
    SaveBlocks(Vec<ExecutedBlock<N>>, CrossbeamSender<PersistenceResult>),
>>>>>>> v2.3.0

    /// Removes block data above the given block number from the database.
    ///
    /// This will first update checkpoints from the database, then remove actual block data from
    /// static files.
    RemoveBlocksAbove(u64, CrossbeamSender<PersistenceResult>),

    /// Update the persisted finalized block on disk
    SaveFinalizedBlock(u64),

    /// Update the persisted safe block on disk
    SaveSafeBlock(u64),
}

/// A handle to the persistence service
#[derive(Debug, Clone)]
pub struct PersistenceHandle<N: NodePrimitives = EthPrimitives> {
    /// The channel used to communicate with the persistence service
    sender: Sender<PersistenceAction<N>>,
<<<<<<< HEAD
=======
    /// Guard that joins the service thread when all handles are dropped.
    /// Uses `Arc` so the handle remains `Clone`.
    _service_guard: Arc<ServiceGuard>,
>>>>>>> v2.3.0
}

impl<T: NodePrimitives> PersistenceHandle<T> {
    /// Create a new [`PersistenceHandle`] from a [`Sender<PersistenceAction>`].
<<<<<<< HEAD
    pub const fn new(sender: Sender<PersistenceAction<T>>) -> Self {
        Self { sender }
    }

    /// Create a new [`PersistenceHandle`], and spawn the persistence service.
=======
    ///
    /// This is intended for testing purposes where you want to mock the persistence service.
    /// For production use, prefer [`spawn_service`](Self::spawn_service).
    pub fn new(sender: Sender<PersistenceAction<T>>) -> Self {
        Self { sender, _service_guard: Arc::new(ServiceGuard(None)) }
    }

    /// Create a new [`PersistenceHandle`], and spawn the persistence service.
    ///
    /// The returned handle can be cloned and shared. When all clones are dropped, the service
    /// thread will be joined, ensuring graceful shutdown before resources (like `RocksDB`) are
    /// released.
>>>>>>> v2.3.0
    pub fn spawn_service<N>(
        provider_factory: ProviderFactory<N>,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
    ) -> PersistenceHandle<N::Primitives>
    where
        N: ProviderNodeTypes,
    {
        // create the initial channels
        let (db_service_tx, db_service_rx) = std::sync::mpsc::channel();

<<<<<<< HEAD
        // construct persistence handle
        let persistence_handle = PersistenceHandle::new(db_service_tx);

=======
>>>>>>> v2.3.0
        // spawn the persistence service
        let db_service =
            PersistenceService::new(provider_factory, db_service_rx, pruner, sync_metrics_tx);
        let join_handle = spawn_os_thread("persistence", || {
            if let Err(err) = db_service.run() {
                error!(target: "engine::persistence", ?err, "Persistence service failed");
            }
        });

        PersistenceHandle {
            sender: db_service_tx,
            _service_guard: Arc::new(ServiceGuard(Some(join_handle))),
        }
    }

    /// Sends a specific [`PersistenceAction`] in the contained channel. The caller is responsible
    /// for creating any channels for the given action.
    pub fn send_action(
        &self,
        action: PersistenceAction<T>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.sender.send(action)
    }

    /// Tells the persistence service to save a certain list of finalized blocks. The blocks are
    /// assumed to be ordered by block number.
    ///
    /// This returns the latest hash that has been saved, allowing removal of that block and any
    /// previous blocks from in-memory data structures. This value is returned in the receiver end
    /// of the sender argument.
    ///
    /// If there are no blocks to persist, then `None` is sent in the sender.
    pub fn save_blocks(
        &self,
<<<<<<< HEAD
        blocks: Vec<ExecutedBlockWithTrieUpdates<T>>,
        tx: oneshot::Sender<Option<BlockNumHash>>,
=======
        blocks: Vec<ExecutedBlock<T>>,
        tx: CrossbeamSender<PersistenceResult>,
>>>>>>> v2.3.0
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveBlocks(blocks, tx))
    }

<<<<<<< HEAD
    /// Persists the finalized block number on disk.
=======
    /// Queues the finalized block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
>>>>>>> v2.3.0
    pub fn save_finalized_block_number(
        &self,
        finalized_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveFinalizedBlock(finalized_block))
    }

<<<<<<< HEAD
    /// Persists the finalized block number on disk.
=======
    /// Queues the safe block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
>>>>>>> v2.3.0
    pub fn save_safe_block_number(
        &self,
        safe_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveSafeBlock(safe_block))
    }

    /// Tells the persistence service to remove blocks above a certain block number. The removed
    /// blocks are returned by the service.
    ///
    /// When the operation completes, the new tip hash is returned in the receiver end of the sender
    /// argument.
    pub fn remove_blocks_above(
        &self,
        block_num: u64,
<<<<<<< HEAD
        tx: oneshot::Sender<Option<BlockNumHash>>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::RemoveBlocksAbove(block_num, tx))
    }
=======
        tx: CrossbeamSender<PersistenceResult>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::RemoveBlocksAbove(block_num, tx))
    }
}

/// Guard that joins the persistence service thread when dropped.
///
/// This ensures graceful shutdown - the service thread completes before resources like
/// `RocksDB` are released. Stored in an `Arc` inside [`PersistenceHandle`] so the handle
/// can be cloned while sharing the same guard.
struct ServiceGuard(Option<JoinHandle<()>>);

impl std::fmt::Debug for ServiceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ServiceGuard").field(&self.0.as_ref().map(|_| "...")).finish()
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if let Some(join_handle) = self.0.take() {
            let _ = join_handle.join();
        }
    }
>>>>>>> v2.3.0
}

#[cfg(test)]
mod tests {
    use super::*;
<<<<<<< HEAD
    use alloy_primitives::B256;
    use reth_chain_state::test_utils::TestBlockBuilder;
    use reth_exex_types::FinishedExExHeight;
    use reth_provider::test_utils::create_test_provider_factory;
=======
    use alloy_eips::NumHash;
    use alloy_primitives::{keccak256, BlockHash, BlockNumber, Bytes, Sealed, B256, U256};
    use reth_chain_state::test_utils::TestBlockBuilder;
    use reth_exex_types::FinishedExExHeight;
    use reth_provider::{
        providers::{ProviderFactoryBuilder, ReadOnlyConfig},
        test_utils::{create_test_provider_factory, MockNodeTypes},
        AccountReader, BalConfig, BalNotificationStream, BalStore, BalStoreHandle,
        ChainSpecProvider, HeaderProvider, InMemoryBalStore, ProviderError, ProviderResult,
        SealedBal, StorageSettingsCache, TryIntoHistoricalStateProvider,
    };
>>>>>>> v2.3.0
    use reth_prune::Pruner;
    use reth_prune_types::PruneMode;
    use tokio::sync::mpsc::unbounded_channel;

    fn default_persistence_handle() -> PersistenceHandle<EthPrimitives> {
        let provider = create_test_provider_factory();

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);

        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, finished_exex_height_rx);

        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        PersistenceHandle::<EthPrimitives>::spawn_service(provider, pruner, sync_metrics_tx)
    }

    #[test]
    fn test_pruner_prunes_bal_store() {
        reth_tracing::init_test_tracing();

        let old_hash = B256::random();
        let retained_hash = B256::random();
        let old_bal = Bytes::from_static(b"old");
        let retained_bal = Bytes::from_static(b"retained");
        let bal_store = BalStoreHandle::new(InMemoryBalStore::new(
            BalConfig::with_in_memory_retention(PruneMode::Before(2)),
        ));

        bal_store
            .insert(
                NumHash::new(1, old_hash),
                Sealed::new_unchecked(old_bal.clone(), keccak256(&old_bal)),
            )
            .unwrap();
        bal_store
            .insert(
                NumHash::new(2, retained_hash),
                Sealed::new_unchecked(retained_bal.clone(), keccak256(&retained_bal)),
            )
            .unwrap();

        let provider = create_test_provider_factory().with_bal_store(bal_store.clone());
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 0, 0, None, finished_exex_height_rx);
        let (_db_service_tx, db_service_rx) = std::sync::mpsc::channel();
        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        let mut service = PersistenceService::new(provider, db_service_rx, pruner, sync_metrics_tx);

        service.maybe_run_pruner(2).unwrap();

        assert_eq!(
            bal_store.get_by_hashes(&[old_hash, retained_hash]).unwrap(),
            vec![None, Some(retained_bal)]
        );
    }

    #[test]
    fn test_pruner_ignores_bal_store_prune_error() {
        reth_tracing::init_test_tracing();
<<<<<<< HEAD
        let persistence_handle = default_persistence_handle();
        let block_number = 0;
        let mut test_block_builder = TestBlockBuilder::eth();
        let executed =
            test_block_builder.get_executed_block_with_number(block_number, B256::random());
        let block_hash = executed.recovered_block().hash();
=======
>>>>>>> v2.3.0

        let provider = create_test_provider_factory()
            .with_bal_store(BalStoreHandle::new(FailingPruneBalStore));
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 0, 0, None, finished_exex_height_rx);
        let (_db_service_tx, db_service_rx) = std::sync::mpsc::channel();
        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        let mut service = PersistenceService::new(provider, db_service_rx, pruner, sync_metrics_tx);

        service.maybe_run_pruner(2).unwrap();
    }

    #[derive(Debug)]
    struct FailingPruneBalStore;

<<<<<<< HEAD
        let mut test_block_builder = TestBlockBuilder::eth();
        let blocks = test_block_builder.get_executed_blocks(0..5).collect::<Vec<_>>();
        let last_hash = blocks.last().unwrap().recovered_block().hash();
        let (tx, rx) = oneshot::channel();
=======
    impl BalStore for FailingPruneBalStore {
        fn insert(&self, _num_hash: NumHash, _bal: SealedBal) -> ProviderResult<()> {
            Ok(())
        }
>>>>>>> v2.3.0

        fn prune(&self, _tip: BlockNumber) -> ProviderResult<usize> {
            Err(ProviderError::other(std::io::Error::other("BAL store prune failed")))
        }

        fn get_by_hashes(&self, block_hashes: &[BlockHash]) -> ProviderResult<Vec<Option<Bytes>>> {
            Ok(vec![None; block_hashes.len()])
        }

<<<<<<< HEAD
        let ranges = [0..1, 1..2, 2..4, 4..5];
        let mut test_block_builder = TestBlockBuilder::eth();
        for range in ranges {
            let blocks = test_block_builder.get_executed_blocks(range).collect::<Vec<_>>();
            let last_hash = blocks.last().unwrap().recovered_block().hash();
            let (tx, rx) = oneshot::channel();

            persistence_handle.save_blocks(blocks, tx).unwrap();

            let BlockNumHash { hash: actual_hash, number: _ } = rx.await.unwrap().unwrap();
            assert_eq!(last_hash, actual_hash);
=======
        fn bal_stream(&self) -> BalNotificationStream {
            BalStoreHandle::noop().bal_stream()
>>>>>>> v2.3.0
        }
    }

    #[test]
    fn test_save_blocks_empty() {
        reth_tracing::init_test_tracing();
        let handle = default_persistence_handle();

        let blocks = vec![];
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, tx).unwrap();

        let result = rx.recv().unwrap();
        assert!(result.last_block.is_none());
    }

    #[test]
    fn test_save_blocks_single_block() {
        reth_tracing::init_test_tracing();
        let handle = default_persistence_handle();
        let block_number = 0;
        let mut test_block_builder = TestBlockBuilder::eth();
        let executed =
            test_block_builder.get_executed_block_with_number(block_number, B256::random());
        let block_hash = executed.recovered_block().hash();

        let blocks = vec![executed];
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, tx).unwrap();

        let result = rx.recv_timeout(std::time::Duration::from_secs(10)).expect("test timed out");

        assert_eq!(block_hash, result.last_block.unwrap().hash);
    }

    #[test]
    fn test_save_blocks_multiple_blocks() {
        reth_tracing::init_test_tracing();
        let handle = default_persistence_handle();

        let mut test_block_builder = TestBlockBuilder::eth();
        let blocks = test_block_builder.get_executed_blocks(0..5).collect::<Vec<_>>();
        let last_hash = blocks.last().unwrap().recovered_block().hash();
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, tx).unwrap();
        let result = rx.recv().unwrap();
        assert_eq!(last_hash, result.last_block.unwrap().hash);
    }

    #[test]
    fn test_save_blocks_multiple_calls() {
        reth_tracing::init_test_tracing();
        let handle = default_persistence_handle();

        let ranges = [0..1, 1..2, 2..4, 4..5];
        let mut test_block_builder = TestBlockBuilder::eth();
        for range in ranges {
            let blocks = test_block_builder.get_executed_blocks(range).collect::<Vec<_>>();
            let last_hash = blocks.last().unwrap().recovered_block().hash();
            let (tx, rx) = crossbeam_channel::bounded(1);

            handle.save_blocks(blocks, tx).unwrap();

            let result = rx.recv().unwrap();
            assert_eq!(last_hash, result.last_block.unwrap().hash);
        }
    }

    /// Verifies that committing `save_blocks` history before running the pruner
    /// prevents the pruner from overwriting new entries.
    ///
    /// Previously, both `save_blocks` and the pruner pushed `RocksDB` batches before
    /// a single commit. Both read committed state, so the pruner didn't see the
    /// new entries and its batch overwrote them. The fix commits `save_blocks`
    /// first, then runs the pruner against committed state in a separate provider.
    #[test]
    fn test_save_blocks_then_prune_preserves_new_history() {
        use reth_db::{models::ShardedKey, tables, BlockNumberList};
        use reth_provider::RocksDBProviderFactory;

        reth_tracing::init_test_tracing();

        let provider_factory = create_test_provider_factory();
        let tracked_addr = alloy_primitives::Address::from([0xBE; 20]);

        // Phase 1: Establish baseline history for blocks 0..20.
        let rocksdb = provider_factory.rocksdb_provider();
        {
            let mut batch = rocksdb.batch();
            let initial_blocks: Vec<u64> = (0..20).collect();
            let shard = BlockNumberList::new_pre_sorted(initial_blocks.iter().copied());
            batch
                .put::<tables::AccountsHistory>(ShardedKey::new(tracked_addr, u64::MAX), &shard)
                .unwrap();
            batch.commit().unwrap();
        }

        // Phase 2: Simulate the fixed on_save_blocks flow.
        // Step 1: save_blocks appends new entries 20..25 and commits immediately.
        let mut batch1 = rocksdb.batch();
        batch1.append_account_history_shard(tracked_addr, 20..25u64).unwrap();
        batch1.commit().unwrap();

        // Step 2: Pruner runs AFTER commit, so it reads state that includes 20..25.
        // Prunes entries ≤ 14, leaving [15..25).
        let mut batch2 = rocksdb.batch();
        batch2.prune_account_history_to(tracked_addr, 14).unwrap();
        batch2.commit().unwrap();

        // Verify new entries survived pruning.
        let shards = rocksdb.account_history_shards(tracked_addr).unwrap();
        let entries: Vec<u64> = shards.iter().flat_map(|(_, list)| list.iter()).collect();
        let expected: Vec<u64> = (15..25).collect();
        assert_eq!(entries, expected, "new entries 20..25 must survive pruning");
    }

    #[test]
    fn test_read_only_consistency_across_reorg() {
        reth_tracing::init_test_tracing();

        // Allow opening the same MDBX env twice in-process
        reth_db::test_utils::enable_legacy_multiopen();

        let provider_factory = create_test_provider_factory();
        provider_factory.set_storage_settings_cache(reth_provider::StorageSettings::v2());

        // Open the secondary provider concurrently with the primary.
        let secondary = ProviderFactoryBuilder::<MockNodeTypes>::default()
            .open_read_only(
                provider_factory.chain_spec(),
                ReadOnlyConfig::from_datadir(provider_factory.db_ref().path()),
                reth_tasks::Runtime::test(),
            )
            .expect("failed to open read-only provider factory");
        secondary.set_storage_settings_cache(reth_provider::StorageSettings::v2());

        // --- Phase 1: Write blocks 0..3 via the primary ---
        let mut test_block_builder = TestBlockBuilder::eth().with_state();
        let signer = test_block_builder.signer;
        let blocks_a: Vec<_> = test_block_builder.get_executed_blocks(0..3).collect();
        let hash_a1 = blocks_a[1].recovered_block().hash();
        let hash_a2 = blocks_a[2].recovered_block().hash();

        // Compute expected signer state after each block from tx counts.
        let single_cost = TestBlockBuilder::<EthPrimitives>::single_tx_cost();
        let initial_balance = U256::from(10).pow(U256::from(18));
        let txs_in_block0 = blocks_a[0].recovered_block().body().transactions.len() as u64;
        let txs_in_block1 = blocks_a[1].recovered_block().body().transactions.len() as u64;

        let balance_after_block0 = initial_balance - single_cost * U256::from(txs_in_block0);
        let nonce_after_block0 = txs_in_block0;
        let balance_after_block1 = balance_after_block0 - single_cost * U256::from(txs_in_block1);
        let nonce_after_block1 = nonce_after_block0 + txs_in_block1;

        {
            let provider_rw = provider_factory.database_provider_rw().unwrap();
            provider_rw.save_blocks(blocks_a, SaveBlocksMode::Full).unwrap();
            provider_rw.commit().unwrap();
        }

        // Secondary catches up and sees all 3 blocks.
        // Hold this provider (and its MDBX RO tx) across the reorg to test snapshot isolation.
        let pre_reorg_provider = secondary.provider().unwrap();
        assert_eq!(
            pre_reorg_provider.sealed_header(2).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a2),
            "secondary must see block 2 after initial append"
        );

        // Check the primary can read its own historical state.
        {
            let primary_state_at_1 = provider_factory.history_by_block_number(1).unwrap();
            let primary_account = primary_state_at_1.basic_account(&signer).unwrap();
            assert!(primary_account.is_some(), "primary: signer must exist at block 1");
        }

        // Verify historical state at block 1 is accessible via changesets on the secondary.
        {
            let state_at_1 = secondary.history_by_block_number(1).unwrap();
            let account_at_1 = state_at_1.basic_account(&signer).unwrap();
            assert!(account_at_1.is_some(), "signer account must exist at block 1");
            let account_at_1 = account_at_1.unwrap();
            assert_eq!(account_at_1.balance, balance_after_block1, "signer balance at block 1");
            assert_eq!(account_at_1.nonce, nonce_after_block1, "signer nonce at block 1");
        }

        // --- Phase 2: Reorg — remove block 2 and append a different block 2 ---
        // Build the reorg block before starting the commit so we can write it in the
        // same thread after the unwind.
        let block_b2 = test_block_builder.get_executed_block_with_number(2, hash_a1);
        let hash_b2 = block_b2.recovered_block().hash();
        let txs_in_block_b2 = block_b2.recovered_block().body().transactions.len() as u64;
        assert_ne!(hash_a2, hash_b2, "reorg block must differ");

        // Expected signer state after the reorged block 2.
        let balance_after_reorg_block2 =
            balance_after_block1 - single_cost * U256::from(txs_in_block_b2);
        let nonce_after_reorg_block2 = nonce_after_block1 + txs_in_block_b2;

        // Spawn the reorg on a background thread because `commit_unwind` calls
        // `wait_for_pre_commit_readers()` which blocks until the secondary's held
        // RO tx is dropped.
        //
        // We want to keep provider factory around, otherwise it's gonna drop mdbx env before the
        // reorg thread is on
        #[expect(clippy::redundant_clone)]
        let pf = provider_factory.clone();
        let reorg_handle = std::thread::spawn(move || {
            let provider_rw = pf.database_provider_rw().unwrap();
            provider_rw.remove_block_and_execution_above(1).unwrap();
            provider_rw.commit().unwrap();

            let provider_rw = pf.database_provider_rw().unwrap();
            provider_rw.save_blocks(vec![block_b2], SaveBlocksMode::Full).unwrap();
            provider_rw.commit().unwrap();
        });

        // Give the reorg thread time to start and block on wait_for_pre_commit_readers.
        std::thread::sleep(std::time::Duration::from_millis(100));

        // The pre-reorg provider still holds its MDBX snapshot — it must still see
        // the OLD block 2 from before the reorg.
        assert_eq!(
            pre_reorg_provider.sealed_header(2).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a2),
            "pre-reorg provider must still see the original block 2"
        );
        assert_eq!(
            pre_reorg_provider.sealed_header(1).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a1),
            "pre-reorg provider must still see block 1"
        );

        // The held RO tx must still be able to read historical state at block 1 via
        // changesets, even though the reorg thread is about to rewrite block 2's data.
        // Consuming pre_reorg_provider here also unblocks the reorg commit.
        let state_at_1 = pre_reorg_provider.try_into_history_at_block(1).unwrap();
        let account = state_at_1.basic_account(&signer).unwrap();
        assert!(
            account.is_some(),
            "pre-reorg RO tx must still read signer at block 1 during reorg"
        );
        let account = account.unwrap();
        assert_eq!(
            account.balance, balance_after_block1,
            "pre-reorg RO tx: signer balance at block 1 during reorg"
        );
        assert_eq!(
            account.nonce, nonce_after_block1,
            "pre-reorg RO tx: signer nonce at block 1 during reorg"
        );
        drop(state_at_1);
        reorg_handle.join().expect("reorg thread panicked");

        // A new provider catches up and sees the reorged chain.
        let obs_header = secondary.provider().unwrap().sealed_header(2).unwrap();
        assert_eq!(
            obs_header.as_ref().map(|h| h.hash()),
            Some(hash_b2),
            "secondary must see the reorged block 2, not the old one"
        );

        // Block 1 should still be the original.
        let obs_header = secondary.provider().unwrap().sealed_header(1).unwrap();
        assert_eq!(
            obs_header.as_ref().map(|h| h.hash()),
            Some(hash_a1),
            "secondary must still see block 1"
        );

        // Verify historical state at block 1 is still accessible after the reorg.
        let state_at_1 = secondary.history_by_block_number(1).unwrap();
        let account_at_1 = state_at_1.basic_account(&signer).unwrap();
        assert!(account_at_1.is_some(), "signer account must exist at block 1 after reorg");
        let account_at_1 = account_at_1.unwrap();
        assert_eq!(
            account_at_1.balance, balance_after_block1,
            "signer balance at block 1 must survive reorg"
        );
        assert_eq!(
            account_at_1.nonce, nonce_after_block1,
            "signer nonce at block 1 must survive reorg"
        );

        // Verify the latest state (at block 2) reflects the reorged execution.
        let state_at_2 = secondary.history_by_block_number(2).unwrap();
        let account_at_2 = state_at_2.basic_account(&signer).unwrap();
        assert!(account_at_2.is_some(), "signer account must exist at block 2 after reorg");
        let account_at_2 = account_at_2.unwrap();
        assert_eq!(
            account_at_2.balance, balance_after_reorg_block2,
            "signer balance at block 2 must reflect reorged execution"
        );
        assert_eq!(
            account_at_2.nonce, nonce_after_reorg_block2,
            "signer nonce at block 2 must reflect reorged execution"
        );
    }
}
