use crate::metrics::PersistenceMetrics;
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use crossbeam_channel::Sender as CrossbeamSender;
use gravity_primitives::get_gravity_config;
use reth_chain_state::{ExecutedBlock, ExecutedBlockWithTrieUpdates};
use reth_db::{
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_errors::ProviderError;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    providers::ProviderNodeTypes, writer::UnifiedStorageWriter, BlockHashReader, BlockWriter,
    ChainStateBlockWriter, DatabaseProvider, DatabaseProviderFactory, ProviderFactory,
    StageCheckpointWriter, StateWriter, StaticFileProviderFactory, StaticFileWriter,
    StorageLocation, TrieWriter, TrieWriterV2, PERSIST_BLOCK_CACHE,
};
use reth_prune::{PrunerError, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender, StageCheckpoint, StageId};
use reth_tasks::spawn_os_thread;
use revm::database::OriginalValuesKnown;
use std::{
    sync::{
        mpsc::{Receiver, RecvTimeoutError, SendError, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use thiserror::Error;
use tracing::{debug, error, info, instrument};

/// When `persist_merge_blocks` is on, close the current merged group once its accumulated
/// `gas_used` crosses this threshold (a cheap proxy for transaction/receipt write volume).
const MERGE_GROUP_MAX_GAS: u64 = 1_000_000_000;
/// Also close the group once the accumulated number of changed hashed accounts crosses this
/// threshold. This bounds the in-memory write batch and the trie/state write volume by actual
/// state churn rather than a raw block count (mostly-empty catch-up blocks coalesce freely; a
/// burst of state-heavy blocks closes the group sooner).
const MERGE_GROUP_MAX_STATE: usize = 10_000;

/// Unified result of any persistence operation.
#[derive(Debug)]
pub struct PersistenceResult {
    /// The last block that was persisted, if any.
    pub last_block: Option<BlockNumHash>,
    /// The commit duration, only available for save-blocks operations.
    pub commit_duration: Option<Duration>,
}

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
        if let Err(err) = self.provider.on_prune_commit() {
            tracing::warn!(target: "engine::persistence", %err, "History file reclamation deferred");
        }
        let mut last_collection = Instant::now();
        loop {
            // Retry even when messages keep arriving without advancing the prune checkpoint.
            if last_collection.elapsed() >= Duration::from_secs(1) {
                if let Err(err) = self.provider.on_prune_commit() {
                    tracing::warn!(target: "engine::persistence", %err, "History file reclamation deferred");
                }
                last_collection = Instant::now();
            }
            let action = match self.incoming.recv_timeout(Duration::from_secs(1)) {
                Ok(action) => action,
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => continue,
            };
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
                    let result_number = result.last_block.map(|b| b.number);

                    let _ = sender.send(result);

                    if let Some(block_number) = result_number {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: block_number });
                        self.maybe_run_pruner(block_number)?;
                    }
                }
                PersistenceAction::SaveFinalizedBlock(finalized_block) => {
                    self.pending_finalized_block = Some(finalized_block);
                }
                PersistenceAction::SaveSafeBlock(safe_block) => {
                    self.pending_safe_block = Some(safe_block);
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
        let sf_provider = self.provider.static_file_provider();
        let _maintenance = sf_provider.begin_maintenance();
        let provider_rw = self.provider.database_provider_rw()?;

        let new_tip_hash = provider_rw.block_hash(new_tip_num)?;
        UnifiedStorageWriter::from(&provider_rw, &sf_provider).remove_blocks_above(new_tip_num)?;
        UnifiedStorageWriter::commit_unwind(provider_rw)?;

        debug!(target: "engine::persistence", ?new_tip_num, ?new_tip_hash, "Removed blocks from disk");
        self.metrics.remove_blocks_above_duration_seconds.record(start_time.elapsed());
        Ok(new_tip_hash.map(|hash| BlockNumHash { hash, number: new_tip_num }))
    }

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

    #[instrument(level = "debug", target = "engine::persistence", skip_all, fields(block_count = blocks.len()))]
    fn on_save_blocks(
        &mut self,
        blocks: Vec<ExecutedBlockWithTrieUpdates<N::Primitives>>,
    ) -> Result<PersistenceResult, PersistenceError> {
        let first_block = blocks.first().map(|b| b.recovered_block.num_hash());
        let last_block = blocks.last().map(|b| b.recovered_block.num_hash());
        let block_count = blocks.len();

        debug!(target: "engine::persistence", ?block_count, first=?first_block, last=?last_block, "Saving range of blocks");

        let start_time = Instant::now();

        if let Some(last) = last_block {
            if get_gravity_config().persist_merge_blocks {
                self.save_merged_blocks(blocks)?;
            } else {
                self.save_blocks_per_block(blocks)?;
            }

            // Safe/finalized are independent metadata; block completion checkpoints are already
            // durable. An empty save must retain pending markers for the next non-empty save.
            if self.pending_finalized_block.is_some() || self.pending_safe_block.is_some() {
                let provider_rw = self.provider.database_provider_rw()?;
                if let Some(finalized) = self.pending_finalized_block {
                    provider_rw.save_finalized_block_number(finalized.min(last.number))?;
                }
                if let Some(safe) = self.pending_safe_block {
                    provider_rw.save_safe_block_number(safe.min(last.number))?;
                }
                provider_rw.commit()?;
                self.pending_finalized_block =
                    self.pending_finalized_block.filter(|number| *number > last.number);
                self.pending_safe_block =
                    self.pending_safe_block.filter(|number| *number > last.number);
            }
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
            if let Err(err) = self.provider.on_prune_commit() {
                tracing::warn!(target: "engine::persistence", %err, "History file reclamation deferred");
            }
            debug!(target: "engine::persistence", tip=?block_number, "Finished pruning after saving blocks");
            self.metrics.prune_before_duration_seconds.record(prune_start.elapsed());
        }

        Ok(())
    }

    /// Persist each block's main data atomically per database, then record trie completion.
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
            let start = Instant::now();
            let block_number = recovered_block.number();
            let block_hash = recovered_block.hash();
            info!(target: "persistence::save_block", block_number, "Write block updates into DB");
            let provider_rw = self.provider.database_provider_rw()?;

            for stage in [StageId::Execution, StageId::AccountHashing, StageId::IndexAccountHistory]
            {
                Self::get_checkpoint(provider_rw.tx_ref(), stage, Some(block_number))?;
            }

            let body_indices = provider_rw
                .insert_block(Arc::unwrap_or_clone(recovered_block), StorageLocation::Both)?;
            // History consumes the keys actually written to changesets, including slots discovered
            // by a storage wipe. A RocksDB cursor cannot read this transaction's pending writes.
            provider_rw.write_state_with_indices_and_history(
                &execution_output,
                OriginalValuesKnown::No,
                StorageLocation::StaticFiles,
                Some(vec![body_indices]),
            )?;
            provider_rw.write_hashed_state(&Arc::unwrap_or_clone(hashed_state).into_sorted())?;
            provider_rw.write_trie_updates(
                trie.as_ref().ok_or(ProviderError::MissingTrieUpdates(block_hash))?,
            )?;
            provider_rw.write_trie_updatesv2(triev2.as_ref()).map_err(ProviderError::Database)?;

            Self::commit_block(provider_rw, block_number)?;
            metrics::histogram!("save_blocks_time", &[("process", "atomic_block")])
                .record(start.elapsed());
        }
        Ok(())
    }

    /// Persist `blocks` as a sequence of merged groups. Groups are bounded by
    /// [`MERGE_GROUP_MAX_GAS`] and [`MERGE_GROUP_MAX_STATE`] to bound the in-flight write batch.
    /// A block that wipes storage starts a new group, so its changeset scan sees committed slots.
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
            // Wipe reverts include untouched slots read from plain storage. Commit earlier blocks
            // as a complete group before this scan, rather than flushing partial block data.
            let wipes_storage = block
                .execution_outcome()
                .bundle
                .reverts
                .iter()
                .flatten()
                .any(|(_, revert)| revert.wipe_storage);
            if !group.is_empty() &&
                (wipes_storage ||
                    group_gas.saturating_add(gas) > MERGE_GROUP_MAX_GAS ||
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

    /// Write one contiguous group of executed blocks with amortized commits.
    ///
    /// History inputs are collected while writing the group's changesets, and each history key is
    /// appended once. No block's state becomes visible before its other `state_db` data is
    /// complete.
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
        for stage in [StageId::Execution, StageId::AccountHashing, StageId::IndexAccountHistory] {
            Self::get_checkpoint(provider_rw.tx_ref(), stage, Some(group_first))?;
        }

        // Headers / bodies / senders / tx lookups for the whole group (transaction numbers threaded
        // across the batch in memory by `insert_blocks`).
        let body_indices = provider_rw.insert_blocks(recovered_blocks, StorageLocation::Both)?;

        // Combine execution results so repeated history keys are appended once for the full
        // range. Only the first block can wipe storage, whose old slots are already committed.
        let mut execution_outputs = execution_outputs.into_iter().map(Arc::unwrap_or_clone);
        let mut execution_output = execution_outputs.next().expect("group is non-empty");
        for next in execution_outputs {
            execution_output.extend(next);
        }
        provider_rw.write_state_with_indices_and_history(
            &execution_output,
            OriginalValuesKnown::No,
            StorageLocation::StaticFiles,
            Some(body_indices),
        )?;
        for hashed_state in hashed_states {
            provider_rw.write_hashed_state(&Arc::unwrap_or_clone(hashed_state).into_sorted())?;
        }

        // Trie updates, per block.
        for (trie, triev2, block_hash) in &trie_updates {
            provider_rw.write_trie_updates(
                trie.as_ref().ok_or(ProviderError::MissingTrieUpdates(*block_hash))?,
            )?;
            provider_rw.write_trie_updatesv2(triev2.as_ref()).map_err(ProviderError::Database)?;
        }

        Self::commit_block(provider_rw, group_last)?;

        metrics::histogram!("save_blocks_time", &[("process", "merge_block")])
            .record(start.elapsed() / block_count);
        Ok(())
    }

    /// Commit state before the tries, then record Merkle completion after both trie writes.
    fn commit_block<TX: DbTx + DbTxMut + 'static>(
        provider_rw: DatabaseProvider<TX, N>,
        block_number: u64,
    ) -> Result<(), PersistenceError> {
        for stage in [
            StageId::Execution,
            StageId::AccountHashing,
            StageId::IndexAccountHistory,
            StageId::Finish,
        ] {
            let checkpoint = Self::get_checkpoint(provider_rw.tx_ref(), stage, None)?;
            provider_rw
                .save_stage_checkpoint(stage, StageCheckpoint { block_number, ..checkpoint })?;
        }
        provider_rw.static_file_provider().commit()?;
        // The old Merkle checkpoint makes recovery repair the entire block/group if either
        // trie write fails after state has committed.
        provider_rw.tx_ref().commit_view().map_err(ProviderError::Database)?;
        provider_rw.update_pipeline_stages(block_number, false)?;
        provider_rw.commit()?;
        PERSIST_BLOCK_CACHE.persist_tip(block_number);
        Ok(())
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
    SaveBlocks(Vec<ExecutedBlockWithTrieUpdates<N>>, CrossbeamSender<PersistenceResult>),

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
    /// Guard that joins the service thread when all handles are dropped.
    /// Uses `Arc` so the handle remains `Clone`.
    _service_guard: Arc<ServiceGuard>,
}

impl<T: NodePrimitives> PersistenceHandle<T> {
    /// Create a new [`PersistenceHandle`] from a [`Sender<PersistenceAction>`].
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
        blocks: Vec<ExecutedBlockWithTrieUpdates<T>>,
        tx: CrossbeamSender<PersistenceResult>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveBlocks(blocks, tx))
    }

    /// Queues the finalized block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
    pub fn save_finalized_block_number(
        &self,
        finalized_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveFinalizedBlock(finalized_block))
    }

    /// Queues the safe block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header;
    use alloy_primitives::{keccak256, Address, B256, U256};
    use reth_chain_state::{test_utils::TestBlockBuilder, ExecutedTrieUpdates};
    #[cfg(feature = "failpoints")]
    use reth_db::cursor::DbCursorRO;
    use reth_db::{
        cursor::DbDupCursorRO,
        models::{storage_sharded_key::StorageShardedKey, GravityStorageSettings, ShardedKey},
    };
    use reth_ethereum_primitives::Block;
    use reth_execution_types::ExecutionOutcome;
    use reth_exex_types::FinishedExExHeight;
    use reth_primitives_traits::{Account, RecoveredBlock, StorageEntry};
    use reth_provider::{
        test_utils::{create_test_provider_factory, MockNodeTypesWithDB},
        ChangesetRangeReader, StorageChangeSetReader, StorageSettingsCache,
    };
    use reth_prune::Pruner;
    use reth_trie::{HashedPostState, KeccakKeyHasher};
    use reth_trie_parallel::nested_hash::NestedStateRoot;
    use revm::database::states::{
        reverts::Reverts, AccountRevert, AccountStatus, BundleAccount, BundleState,
    };
    use tokio::sync::mpsc::unbounded_channel;

    fn persistence_service(
        provider: ProviderFactory<MockNodeTypesWithDB>,
    ) -> PersistenceService<MockNodeTypesWithDB> {
        let (_tx, rx) = tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner = Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, rx);
        PersistenceService::new(
            provider,
            std::sync::mpsc::channel().1,
            pruner,
            unbounded_channel().0,
        )
    }

    #[test]
    fn history_collection_progresses_while_persistence_messages_keep_arriving() {
        use reth_db::models::AccountBeforeTx;
        use reth_provider::{providers::StaticFileProvider, ChainSpecProvider, StaticFileSegment};

        let factory = create_test_provider_factory();
        let sf = StaticFileProvider::read_write(factory.static_file_provider().path())
            .unwrap()
            .with_custom_blocks_per_file(5);
        {
            let mut writer = sf.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
            for block in 0..=12 {
                writer
                    .append_account_changeset(
                        vec![AccountBeforeTx { address: Address::ZERO, info: None }],
                        block,
                    )
                    .unwrap();
            }
            writer.commit().unwrap();
        }
        let provider =
            ProviderFactory::new(factory.db_ref().clone(), factory.chain_spec(), sf.clone());
        provider.set_storage_settings_cache(GravityStorageSettings {
            changesets_in_static_files: true,
        });
        let mut service = persistence_service(provider);
        let (sender, receiver) = std::sync::mpsc::channel();
        service.incoming = receiver;
        let old = sf.read_lease().unwrap();
        sf.retire_history_after_commit(Some(10), None);
        let worker = std::thread::spawn(move || service.run());
        let (completed, completion) = crossbeam_channel::bounded(1);
        sender.send(PersistenceAction::SaveBlocks(vec![], completed)).unwrap();
        completion.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(old);

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) &&
            sf.get_lowest_static_file_block(StaticFileSegment::AccountChangeSets) == Some(4)
        {
            sender.send(PersistenceAction::SaveSafeBlock(12)).unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(sender);
        worker.join().unwrap().unwrap();
        assert_eq!(sf.get_lowest_static_file_block(StaticFileSegment::AccountChangeSets), Some(12));
    }

    fn state_block(
        provider: &ProviderFactory<MockNodeTypesWithDB>,
        outcome: ExecutionOutcome,
    ) -> ExecutedBlockWithTrieUpdates {
        let hashed = HashedPostState::from_bundle_state::<KeccakKeyHasher>(outcome.bundle.state());
        let provider_ro = provider.database_provider_ro().unwrap();
        let (state_root, trie) =
            NestedStateRoot::new(provider_ro.tx_ref(), None).calculate(&hashed).unwrap();
        let block = Block {
            header: Header { number: outcome.first_block(), state_root, ..Default::default() },
            body: Default::default(),
        };
        ExecutedBlockWithTrieUpdates::new(
            Arc::new(RecoveredBlock::new_unhashed(block, vec![])),
            Arc::new(outcome),
            Arc::new(hashed),
            ExecutedTrieUpdates::empty(),
            Arc::new(trie),
        )
    }

    fn account_update(
        number: u64,
        address: Address,
        previous: Option<Account>,
        value: u64,
    ) -> ExecutionOutcome {
        let slot = B256::with_last_byte(1);
        let old_value = if previous.is_some() { U256::from(value - 1) } else { U256::ZERO };
        let account = Account { nonce: number, balance: U256::from(value), ..Default::default() };
        let state = std::iter::once((
            address,
            (
                previous,
                Some(account),
                std::iter::once((slot, (old_value, U256::from(value)))).collect(),
            ),
        ))
        .collect();
        let account_reverts = std::iter::once((
            address,
            (Some(previous), vec![StorageEntry { key: slot, value: old_value }]),
        ))
        .collect();
        ExecutionOutcome::new_init(
            state,
            std::iter::once((number, account_reverts)).collect(),
            [],
            vec![vec![]],
            number,
            vec![Default::default()],
        )
    }

    fn empty_outcome() -> ExecutionOutcome {
        ExecutionOutcome::new(
            BundleState { reverts: Reverts::new(vec![vec![]]), ..Default::default() },
            vec![vec![]],
            0,
            vec![Default::default()],
        )
    }

    #[cfg(feature = "failpoints")]
    fn assert_state_boundary<TX: DbTx>(
        tx: &TX,
        address: Address,
        state_committed: bool,
        completion_committed: bool,
    ) {
        let complete = state_committed;
        let account = Account { nonce: 1, balance: U256::from(7), ..Default::default() };
        assert_eq!(
            tx.get::<tables::PlainAccountState>(address).unwrap(),
            complete.then_some(account)
        );
        assert_eq!(
            tx.get::<tables::HashedAccounts>(keccak256(address)).unwrap(),
            complete.then_some(account)
        );
        let slot = B256::with_last_byte(1);
        let plain = tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .unwrap()
            .seek_by_key_subkey(address, slot)
            .unwrap();
        let hashed = tx
            .cursor_dup_read::<tables::HashedStorages>()
            .unwrap()
            .seek_by_key_subkey(keccak256(address), keccak256(slot))
            .unwrap();
        assert_eq!(plain, complete.then_some(StorageEntry { key: slot, value: U256::from(7) }));
        assert_eq!(
            hashed,
            complete.then_some(StorageEntry { key: keccak256(slot), value: U256::from(7) })
        );
        let account_history = tx.get::<tables::AccountsHistory>(ShardedKey::last(address)).unwrap();
        let storage_history =
            tx.get::<tables::StoragesHistory>(StorageShardedKey::last(address, slot)).unwrap();
        assert_eq!(
            account_history.map(|indices| indices.iter().collect::<Vec<_>>()),
            complete.then_some(vec![1])
        );
        assert_eq!(
            storage_history.map(|indices| indices.iter().collect::<Vec<_>>()),
            complete.then_some(vec![1])
        );
        assert_eq!(tx.get::<tables::BlockBodyIndices>(1).unwrap().is_some(), complete);
        for stage in StageId::ALL {
            assert_eq!(
                tx.get::<tables::StageCheckpoints>(stage.to_string())
                    .unwrap()
                    .unwrap()
                    .block_number,
                u64::from(
                    if matches!(
                        stage,
                        StageId::Execution |
                            StageId::AccountHashing |
                            StageId::IndexAccountHistory |
                            StageId::Finish
                    ) {
                        state_committed
                    } else {
                        completion_committed
                    }
                ),
                "{stage:?}"
            );
        }
    }

    #[cfg(feature = "failpoints")]
    fn reopen_provider(
        provider: ProviderFactory<MockNodeTypesWithDB>,
        changesets_in_static_files: bool,
    ) -> ProviderFactory<MockNodeTypesWithDB> {
        use reth_chainspec::MAINNET;
        use reth_db::{test_utils::TempDatabase, ClientVersion, DatabaseArguments};
        use reth_provider::providers::StaticFileProvider;

        let db_path = provider.db_ref().path().to_path_buf();
        let static_path = provider.static_file_provider().path().to_path_buf();
        // Keep the datadir but close all three RocksDB instances before reopening it.
        drop(Arc::try_unwrap(provider.into_db()).unwrap().into_inner_db());
        let db =
            reth_db::open_db(&db_path, DatabaseArguments::new(ClientVersion::default())).unwrap();
        let provider = ProviderFactory::<MockNodeTypesWithDB>::new(
            Arc::new(TempDatabase::new(db, db_path)),
            MAINNET.clone(),
            StaticFileProvider::read_write(static_path).unwrap(),
        );
        provider.set_storage_settings_cache(GravityStorageSettings { changesets_in_static_files });
        provider
    }

    #[cfg(feature = "failpoints")]
    fn assert_persisted_trie<TX: DbTx>(
        tx: &TX,
        address: Address,
        block: &ExecutedBlockWithTrieUpdates,
    ) {
        use reth_trie::{
            nested_trie::{StorageNodeEntry, Trie},
            Nibbles, StoredNibblesSubKey,
        };
        use reth_trie_db::nested_hash::StorageTrieReader;

        let hashed_address = keccak256(address);
        let expected = &block.triev2.storage_tries[&hashed_address].storage_nodes;
        let mut cursor = tx.cursor_dup_read::<tables::StoragesTrieV2>().unwrap();
        let stored = cursor
            .walk_dup(Some(hashed_address), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(stored.len(), expected.len());
        for (path, node) in expected {
            let path = StoredNibblesSubKey(*path);
            assert_eq!(
                cursor.get_by_key_subkey(hashed_address, path.clone()).unwrap(),
                Some(StorageNodeEntry::create(&path, node))
            );
        }
        let reader = StorageTrieReader::new(cursor, hashed_address, None);
        let mut trie = Trie::new(reader, false).unwrap();
        assert_eq!(trie.hash(), expected[&Nibbles::new()].hash());
        assert_eq!(
            NestedStateRoot::new(tx, None).calculate(&Default::default()).unwrap().0,
            block.recovered_block().state_root()
        );
    }

    fn exercise_state_history(changesets_in_static_files: bool, merged: bool) {
        PERSIST_BLOCK_CACHE.reset();
        let provider = create_test_provider_factory();
        provider.set_storage_settings_cache(GravityStorageSettings { changesets_in_static_files });
        let service = persistence_service(provider.clone());
        let persist = |blocks| {
            if merged {
                service.save_merged_blocks(blocks)
            } else {
                service.save_blocks_per_block(blocks)
            }
            .unwrap();
        };
        persist(vec![state_block(&provider, empty_outcome())]);
        let address = Address::with_last_byte(1);
        let first_account = Account { nonce: 1, balance: U256::from(7), ..Default::default() };
        let blocks = vec![
            state_block(&provider, account_update(1, address, None, 7)),
            state_block(&provider, account_update(2, address, Some(first_account), 8)),
        ];
        let root = blocks[1].recovered_block().state_root();
        persist(blocks);

        let provider_ro = provider.database_provider_ro().unwrap();
        let tx = provider_ro.tx_ref();
        let slot = B256::with_last_byte(1);
        let account = Account { nonce: 2, balance: U256::from(8), ..Default::default() };
        assert_eq!(tx.get::<tables::PlainAccountState>(address).unwrap(), Some(account));
        assert_eq!(tx.get::<tables::HashedAccounts>(keccak256(address)).unwrap(), Some(account));
        assert_eq!(
            tx.cursor_dup_read::<tables::PlainStorageState>()
                .unwrap()
                .seek_by_key_subkey(address, slot)
                .unwrap()
                .unwrap()
                .value,
            U256::from(8)
        );
        assert_eq!(
            tx.cursor_dup_read::<tables::HashedStorages>()
                .unwrap()
                .seek_by_key_subkey(keccak256(address), keccak256(slot))
                .unwrap()
                .unwrap()
                .value,
            U256::from(8)
        );
        assert_eq!(NestedStateRoot::new(tx, None).calculate(&Default::default()).unwrap().0, root);
        for stage in StageId::ALL {
            assert_eq!(
                tx.get::<tables::StageCheckpoints>(stage.to_string())
                    .unwrap()
                    .unwrap()
                    .block_number,
                2
            );
        }
        drop(provider_ro);

        let mut bundle = BundleState::default();
        bundle.state.insert(
            address,
            BundleAccount::new(
                Some(account.into()),
                None,
                Default::default(),
                AccountStatus::Destroyed,
            ),
        );
        bundle.reverts = Reverts::new(vec![vec![(
            address,
            AccountRevert {
                account: revm::database::states::reverts::AccountInfoRevert::RevertTo(
                    account.into(),
                ),
                wipe_storage: true,
                ..Default::default()
            },
        )]]);
        let wipe = ExecutionOutcome::new(bundle, vec![vec![]], 3, vec![Default::default()]);
        let wipe = state_block(&provider, wipe);
        let root = wipe.recovered_block().state_root();
        persist(vec![wipe]);
        let provider_ro = provider.database_provider_ro().unwrap();
        let tx = provider_ro.tx_ref();
        assert!(tx.get::<tables::PlainAccountState>(address).unwrap().is_none());
        assert!(tx.get::<tables::HashedAccounts>(keccak256(address)).unwrap().is_none());
        assert!(tx
            .cursor_dup_read::<tables::PlainStorageState>()
            .unwrap()
            .seek_by_key_subkey(address, slot)
            .unwrap()
            .is_none());
        assert!(tx
            .cursor_dup_read::<tables::HashedStorages>()
            .unwrap()
            .seek_by_key_subkey(keccak256(address), keccak256(slot))
            .unwrap()
            .is_none());
        assert_eq!(
            provider_ro.storage_changeset(3).unwrap()[0].1,
            StorageEntry { key: slot, value: U256::from(8) }
        );
        assert_eq!(provider_ro.account_changesets_range(3..=3).unwrap()[0].1.info, Some(account));
        assert_eq!(
            tx.get::<tables::AccountsHistory>(ShardedKey::last(address))
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            tx.get::<tables::StoragesHistory>(StorageShardedKey::last(address, slot))
                .unwrap()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(NestedStateRoot::new(tx, None).calculate(&Default::default()).unwrap().0, root);
    }

    #[test]
    fn per_block_persistence_keeps_state_history_and_wipes_complete() {
        for static_changesets in [false, true] {
            exercise_state_history(static_changesets, false);
        }
    }

    #[test]
    #[ignore = "local persistence timing and serialized batch measurement"]
    fn measure_atomic_persistence() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().unwrap();
        for merged in [false, true] {
            PERSIST_BLOCK_CACHE.reset();
            let provider = create_test_provider_factory();
            let service = persistence_service(provider.clone());
            service.save_blocks_per_block(vec![state_block(&provider, empty_outcome())]).unwrap();
            let mut previous = None;
            let blocks = (1..=32)
                .map(|number| {
                    let value = number + 6;
                    let outcome =
                        account_update(number, Address::with_last_byte(1), previous, value);
                    previous = Some(Account {
                        nonce: number,
                        balance: U256::from(value),
                        ..Default::default()
                    });
                    state_block(&provider, outcome)
                })
                .collect();
            snapshotter.snapshot();
            let start = Instant::now();
            if merged {
                service.save_merged_blocks(blocks)
            } else {
                service.save_blocks_per_block(blocks)
            }
            .unwrap();
            let elapsed = start.elapsed();
            println!(
                "merged={merged}, 32 blocks, persistence={elapsed:?}, blocks/s={:.1}",
                32.0 / elapsed.as_secs_f64()
            );
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                match (key.key().name(), value) {
                    ("reth_rocksdb_sync_commits_total", DebugValue::Counter(count)) => {
                        let database = key
                            .key()
                            .labels()
                            .find(|label| label.key() == "database")
                            .unwrap()
                            .value();
                        let commits = if merged { 1 } else { 32 };
                        assert_eq!(count, if database == "state" { 2 * commits } else { commits });
                        println!("{:?}: {count} sync commits", key.key());
                    }
                    ("reth_rocksdb_commit_batch_size_bytes", DebugValue::Histogram(values)) => {
                        println!(
                            "{:?}: maximum serialized batch {:?} bytes",
                            key.key(),
                            values.into_iter().max().unwrap()
                        );
                    }
                    (
                        "reth_rocksdb_sync_commit_duration_seconds",
                        DebugValue::Histogram(values),
                    ) => {
                        let total = values.iter().map(|value| value.into_inner()).sum::<f64>();
                        println!(
                            "{:?}: total sync time {total:.6}s, mean {:.6}s, max {:?}s",
                            key.key(),
                            total / values.len() as f64,
                            values.into_iter().max().unwrap()
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn empty_save_keeps_deferred_finality() {
        let mut service = persistence_service(create_test_provider_factory());
        service.pending_finalized_block = Some(10);
        service.pending_safe_block = Some(11);
        assert!(service.on_save_blocks(Vec::new()).unwrap().last_block.is_none());
        assert_eq!(service.pending_finalized_block, Some(10));
        assert_eq!(service.pending_safe_block, Some(11));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn interrupted_commits_reopen_and_recover_the_ordered_block() {
        use crate::recovery::StorageRecoveryHelper;
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let _scenario = fail::FailScenario::setup();
        for static_changesets in [false, true] {
            for (failpoint, storage_committed, state_committed) in [
                ("db::commit::before_storage_trie", false, true),
                ("db::commit::before_state", false, false),
            ] {
                PERSIST_BLOCK_CACHE.reset();
                let provider = create_test_provider_factory();
                provider.set_storage_settings_cache(GravityStorageSettings {
                    changesets_in_static_files: static_changesets,
                });
                let service = persistence_service(provider.clone());
                service
                    .save_blocks_per_block(vec![state_block(&provider, empty_outcome())])
                    .unwrap();
                let address = Address::with_last_byte(1);
                let block = state_block(&provider, account_update(1, address, None, 7));
                fail::cfg(failpoint, "panic").unwrap();
                let interrupted = catch_unwind(AssertUnwindSafe(|| {
                    service.save_blocks_per_block(vec![block.clone()]).unwrap();
                }));
                fail::remove(failpoint);
                assert!(interrupted.is_err(), "{failpoint}");

                let provider_ro = provider.database_provider_ro().unwrap();
                assert_state_boundary(provider_ro.tx_ref(), address, state_committed, false);
                assert_eq!(
                    provider_ro
                        .tx_ref()
                        .get::<tables::AccountsTrieV2>(Default::default())
                        .unwrap()
                        .is_some(),
                    state_committed
                );
                assert_eq!(
                    provider_ro
                        .tx_ref()
                        .cursor_dup_read::<tables::StoragesTrieV2>()
                        .unwrap()
                        .seek_exact(keccak256(address))
                        .unwrap()
                        .is_some(),
                    storage_committed
                );
                drop(provider_ro);
                drop(service);

                let provider = reopen_provider(provider, static_changesets);
                let provider_ro = provider.database_provider_ro().unwrap();
                assert_state_boundary(provider_ro.tx_ref(), address, state_committed, false);
                provider.static_file_provider().check_consistency(&provider_ro, false).unwrap();
                drop(provider_ro);
                StorageRecoveryHelper::new(&provider).check_and_recover().unwrap();
                let provider_ro = provider.database_provider_ro().unwrap();
                assert_state_boundary(
                    provider_ro.tx_ref(),
                    address,
                    state_committed,
                    state_committed,
                );
                if state_committed {
                    assert_persisted_trie(provider_ro.tx_ref(), address, &block);
                }
                drop(provider_ro);
                if !state_committed {
                    let replay = state_block(&provider, account_update(1, address, None, 7));
                    assert_eq!(
                        replay.recovered_block().state_root(),
                        block.recovered_block().state_root()
                    );
                    persistence_service(provider.clone())
                        .save_blocks_per_block(vec![replay])
                        .unwrap();
                }
                let provider_ro = provider.database_provider_ro().unwrap();
                assert_state_boundary(provider_ro.tx_ref(), address, true, true);
                assert_eq!(provider_ro.account_changesets_range(1..=1).unwrap().len(), 1);
                assert_eq!(provider_ro.storage_changeset(1).unwrap().len(), 1);
                assert_persisted_trie(provider_ro.tx_ref(), address, &block);
            }
        }
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn merged_commit_recovers_after_interrupted_trie_write() {
        use crate::recovery::StorageRecoveryHelper;
        use reth_provider::StageCheckpointReader;
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let _scenario = fail::FailScenario::setup();
        for static_changesets in [false, true] {
            PERSIST_BLOCK_CACHE.reset();
            let reference = create_test_provider_factory();
            let reference_service = persistence_service(reference.clone());
            reference_service
                .save_blocks_per_block(vec![state_block(&reference, empty_outcome())])
                .unwrap();
            let blocks: Vec<_> = (1..=2)
                .map(|number| {
                    let block = state_block(
                        &reference,
                        account_update(number, Address::with_last_byte(number as u8), None, 7),
                    );
                    reference_service.save_blocks_per_block(vec![block.clone()]).unwrap();
                    block
                })
                .collect();
            let expected = blocks.last().unwrap().recovered_block().state_root();

            PERSIST_BLOCK_CACHE.reset();
            let provider = create_test_provider_factory();
            provider.set_storage_settings_cache(GravityStorageSettings {
                changesets_in_static_files: static_changesets,
            });
            let service = persistence_service(provider.clone());
            service.save_blocks_per_block(vec![state_block(&provider, empty_outcome())]).unwrap();
            fail::cfg("db::commit::before_storage_trie", "panic").unwrap();
            assert!(catch_unwind(AssertUnwindSafe(|| {
                service.save_merged_blocks(blocks).unwrap();
            }))
            .is_err());
            drop(service);

            let provider = reopen_provider(provider, static_changesets);
            let provider_ro = provider.database_provider_ro().unwrap();
            provider.static_file_provider().check_consistency(&provider_ro, false).unwrap();
            for (stage, number) in [(StageId::Execution, 2), (StageId::MerkleExecute, 0)] {
                assert_eq!(
                    provider_ro.get_stage_checkpoint(stage).unwrap().unwrap().block_number,
                    number
                );
            }
            drop(provider_ro);
            // Interrupt recovery too: it must not advance Merkle before storage is durable.
            assert!(catch_unwind(AssertUnwindSafe(|| {
                StorageRecoveryHelper::new(&provider).check_and_recover().unwrap();
            }))
            .is_err());
            fail::remove("db::commit::before_storage_trie");

            let provider = reopen_provider(provider, static_changesets);
            assert_eq!(
                provider
                    .database_provider_ro()
                    .unwrap()
                    .get_stage_checkpoint(StageId::MerkleExecute)
                    .unwrap()
                    .unwrap()
                    .block_number,
                0
            );
            StorageRecoveryHelper::new(&provider).check_and_recover().unwrap();
            let provider_ro = provider.database_provider_ro().unwrap();
            assert_eq!(
                NestedStateRoot::new(provider_ro.tx_ref(), None)
                    .calculate(&Default::default())
                    .unwrap()
                    .0,
                expected
            );
            assert_eq!(
                provider_ro
                    .get_stage_checkpoint(StageId::MerkleExecute)
                    .unwrap()
                    .unwrap()
                    .block_number,
                2
            );
        }
    }

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
    fn merged_persistence_keeps_state_history_and_wipes_complete() {
        for static_changesets in [false, true] {
            exercise_state_history(static_changesets, true);
        }
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
}
