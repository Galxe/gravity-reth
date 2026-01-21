use crate::metrics::PersistenceMetrics;
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
    StorageLocation, TrieWriterV2, PERSIST_BLOCK_CACHE,
};
use reth_prune::{PrunerError, PrunerOutput, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender, StageCheckpoint, StageId};
use revm::database::OriginalValuesKnown;
use std::{
    sync::{
        mpsc::{Receiver, SendError, Sender},
        Arc,
    },
    thread,
    time::Instant,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, error, info};

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
        Self { provider, incoming, pruner, metrics: PersistenceMetrics::default(), sync_metrics_tx }
    }

    /// Prunes block data before the given block hash according to the configured prune
    /// configuration.
    fn prune_before(&mut self, block_num: u64) -> Result<PrunerOutput, PrunerError> {
        debug!(target: "engine::persistence", ?block_num, "Running pruner");
        let start_time = Instant::now();
        // TODO: doing this properly depends on pruner segment changes
        let result = self.pruner.run(block_num);
        self.metrics.prune_before_duration_seconds.record(start_time.elapsed());
        result
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
                    let result = self.on_remove_blocks_above(new_tip_num)?;
                    // send new sync metrics based on removed blocks
                    let _ =
                        self.sync_metrics_tx.send(MetricEvent::SyncHeight { height: new_tip_num });
                    // we ignore the error because the caller may or may not care about the result
                    let _ = sender.send(result);
                }
                PersistenceAction::SaveBlocks(blocks, sender) => {
                    let result = self.on_save_blocks(blocks)?;
                    let result_number = result.map(|r| r.number);

                    // we ignore the error because the caller may or may not care about the result
                    let _ = sender.send(result);

                    if let Some(block_number) = result_number {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: block_number });

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
                }
            }
        }
        Ok(())
    }

    fn on_remove_blocks_above(
        &self,
        new_tip_num: u64,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", ?new_tip_num, "Removing blocks");
        let start_time = Instant::now();
        let provider_rw = self.provider.database_provider_rw()?;
        let sf_provider = self.provider.static_file_provider();

        let new_tip_hash = provider_rw.block_hash(new_tip_num)?;
        UnifiedStorageWriter::from(&provider_rw, &sf_provider).remove_blocks_above(new_tip_num)?;
        UnifiedStorageWriter::commit_unwind(provider_rw)?;

        debug!(target: "engine::persistence", ?new_tip_num, ?new_tip_hash, "Removed blocks from disk");
        self.metrics.remove_blocks_above_duration_seconds.record(start_time.elapsed());
        Ok(new_tip_hash.map(|hash| BlockNumHash { hash, number: new_tip_num }))
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

            for ExecutedBlockWithTrieUpdates {
                block: ExecutedBlock { recovered_block, execution_output, hashed_state },
                trie: _,
                triev2,
            } in blocks
            {
                let block_number = recovered_block.number();
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
                        let static_file_provider = inner_provider.static_file_provider();
                        let ck = provider_rw
                            .tx_ref()
                            .get::<tables::StageCheckpoints>(StageId::Execution.to_string())
                            .map_err(ProviderError::Database)?
                            .unwrap_or_default();
                        assert_eq!(ck.block_number + 1, block_number);
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
                        provider_rw
                            .tx_ref()
                            .put::<tables::StageCheckpoints>(
                                StageId::Execution.to_string(),
                                StageCheckpoint { block_number, ..ck },
                            )
                            .map_err(ProviderError::Database)?;
                        static_file_provider.commit()?;
                        provider_rw.commit()?;
                        set_fail_point!("persistence::after_state_commit");
                        metrics::histogram!("save_blocks_time", &[("process", "write_state")])
                            .record(start.elapsed());

                        let start = Instant::now();
                        let provider_rw = inner_provider.database_provider_rw()?;
                        let ck = provider_rw
                            .tx_ref()
                            .get::<tables::StageCheckpoints>(StageId::AccountHashing.to_string())
                            .map_err(ProviderError::Database)?
                            .unwrap_or_default();
                        assert_eq!(ck.block_number + 1, block_number);
                        // insert hashes and intermediate merkle nodes
                        provider_rw.write_hashed_state(
                            &Arc::unwrap_or_clone(hashed_state).into_sorted(),
                        )?;
                        set_fail_point!("persistence::after_hashed_state");
                        provider_rw
                            .tx_ref()
                            .put::<tables::StageCheckpoints>(
                                StageId::AccountHashing.to_string(),
                                StageCheckpoint { block_number, ..ck },
                            )
                            .map_err(ProviderError::Database)?;
                        provider_rw.commit()?;
                        set_fail_point!("persistence::after_hashed_state_commit");
                        metrics::histogram!(
                            "save_blocks_time",
                            &[("process", "write_hashed_state")]
                        )
                        .record(start.elapsed());

                        if !get_gravity_config().validator_node_only {
                            let start = Instant::now();
                            let provider_rw = inner_provider.database_provider_rw()?;
                            let ck = provider_rw
                                .tx_ref()
                                .get::<tables::StageCheckpoints>(
                                    StageId::IndexAccountHistory.to_string(),
                                )
                                .map_err(ProviderError::Database)?
                                .unwrap_or_default();
                            assert_eq!(ck.block_number + 1, block_number);
                            provider_rw.update_history_indices(block_number..=block_number)?;
                            set_fail_point!("persistence::after_history_indices");
                            provider_rw
                                .tx_ref()
                                .put::<tables::StageCheckpoints>(
                                    StageId::IndexAccountHistory.to_string(),
                                    StageCheckpoint { block_number, ..ck },
                                )
                                .map_err(ProviderError::Database)?;
                            provider_rw.commit()?;
                            set_fail_point!("persistence::after_history_commit");
                            metrics::histogram!(
                                "save_blocks_time",
                                &[("process", "update_history_indices")]
                            )
                            .record(start.elapsed());
                        }
                        Ok(())
                    });
                    let trie_handle = scope.spawn(|| -> Result<(), PersistenceError> {
                        let start = Instant::now();
                        let provider_rw = inner_provider.database_provider_rw()?;
                        let ck = provider_rw
                            .tx_ref()
                            .get::<tables::StageCheckpoints>(StageId::MerkleExecute.to_string())
                            .map_err(ProviderError::Database)?
                            .unwrap_or_default();
                        if ck.block_number + 1 != block_number {
                            info!(target: "persistence::trie_update",
                                checkpoint = ck.block_number,
                                block_number = block_number,
                                "Detected interrupted trie update, but trie has idempotency");
                        }
                        provider_rw
                            .write_trie_updatesv2(triev2.as_ref())
                            .map_err(ProviderError::Database)?;
                        set_fail_point!("persistence::after_trie_update");
                        provider_rw
                            .tx_ref()
                            .put::<tables::StageCheckpoints>(
                                StageId::MerkleExecute.to_string(),
                                StageCheckpoint { block_number, ..ck },
                            )
                            .map_err(ProviderError::Database)?;
                        provider_rw.commit()?;
                        set_fail_point!("persistence::after_trie_commit");
                        metrics::histogram!(
                            "save_blocks_time",
                            &[("process", "write_trie_updatesv2")]
                        )
                        .record(start.elapsed());
                        Ok(())
                    });
                    state_handle.join().unwrap()?;
                    trie_handle.join().unwrap()
                })?;
                PERSIST_BLOCK_CACHE.persist_tip(block_number);
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
    SaveBlocks(Vec<ExecutedBlockWithTrieUpdates<N>>, oneshot::Sender<Option<BlockNumHash>>),

    /// Removes block data above the given block number from the database.
    ///
    /// This will first update checkpoints from the database, then remove actual block data from
    /// static files.
    RemoveBlocksAbove(u64, oneshot::Sender<Option<BlockNumHash>>),

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
}

impl<T: NodePrimitives> PersistenceHandle<T> {
    /// Create a new [`PersistenceHandle`] from a [`Sender<PersistenceAction>`].
    pub const fn new(sender: Sender<PersistenceAction<T>>) -> Self {
        Self { sender }
    }

    /// Create a new [`PersistenceHandle`], and spawn the persistence service.
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

        // construct persistence handle
        let persistence_handle = PersistenceHandle::new(db_service_tx);

        // spawn the persistence service
        let db_service =
            PersistenceService::new(provider_factory, db_service_rx, pruner, sync_metrics_tx);
        std::thread::Builder::new()
            .name("Persistence Service".to_string())
            .spawn(|| {
                if let Err(err) = db_service.run() {
                    error!(target: "engine::persistence", ?err, "Persistence service failed");
                }
            })
            .unwrap();

        persistence_handle
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
        tx: oneshot::Sender<Option<BlockNumHash>>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveBlocks(blocks, tx))
    }

    /// Persists the finalized block number on disk.
    pub fn save_finalized_block_number(
        &self,
        finalized_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveFinalizedBlock(finalized_block))
    }

    /// Persists the finalized block number on disk.
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
        tx: oneshot::Sender<Option<BlockNumHash>>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::RemoveBlocksAbove(block_num, tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use reth_chain_state::test_utils::TestBlockBuilder;
    use reth_exex_types::FinishedExExHeight;
    use reth_provider::test_utils::create_test_provider_factory;
    use reth_prune::Pruner;
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

    #[tokio::test]
    async fn test_save_blocks_empty() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let blocks = vec![];
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();

        let hash = rx.await.unwrap();
        assert_eq!(hash, None);
    }

    #[tokio::test]
    async fn test_save_blocks_single_block() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();
        let block_number = 0;
        let mut test_block_builder = TestBlockBuilder::eth();
        let executed =
            test_block_builder.get_executed_block_with_number(block_number, B256::random());
        let block_hash = executed.recovered_block().hash();

        let blocks = vec![executed];
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();

        let BlockNumHash { hash: actual_hash, number: _ } =
            tokio::time::timeout(std::time::Duration::from_secs(10), rx)
                .await
                .expect("test timed out")
                .expect("channel closed unexpectedly")
                .expect("no hash returned");

        assert_eq!(block_hash, actual_hash);
    }

    #[tokio::test]
    async fn test_save_blocks_multiple_blocks() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let mut test_block_builder = TestBlockBuilder::eth();
        let blocks = test_block_builder.get_executed_blocks(0..5).collect::<Vec<_>>();
        let last_hash = blocks.last().unwrap().recovered_block().hash();
        let (tx, rx) = oneshot::channel();

        persistence_handle.save_blocks(blocks, tx).unwrap();
        let BlockNumHash { hash: actual_hash, number: _ } = rx.await.unwrap().unwrap();
        assert_eq!(last_hash, actual_hash);
    }

    #[tokio::test]
    async fn test_save_blocks_multiple_calls() {
        reth_tracing::init_test_tracing();
        let persistence_handle = default_persistence_handle();

        let ranges = [0..1, 1..2, 2..4, 4..5];
        let mut test_block_builder = TestBlockBuilder::eth();
        for range in ranges {
            let blocks = test_block_builder.get_executed_blocks(range).collect::<Vec<_>>();
            let last_hash = blocks.last().unwrap().recovered_block().hash();
            let (tx, rx) = oneshot::channel();

            persistence_handle.save_blocks(blocks, tx).unwrap();

            let BlockNumHash { hash: actual_hash, number: _ } = rx.await.unwrap().unwrap();
            assert_eq!(last_hash, actual_hash);
        }
    }
}
