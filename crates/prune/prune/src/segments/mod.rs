mod receipts;
mod set;
mod static_file;
mod user;

use crate::{PruneLimiter, PrunerError};
use alloy_primitives::{BlockNumber, TxNumber};
use reth_provider::{
    errors::provider::ProviderResult, BlockReader, PruneCheckpointWriter, StaticFileProviderFactory,
};
use reth_prune_types::{
    PruneCheckpoint, PruneMode, PrunePurpose, PruneSegment, PruneSegmentError, SegmentOutput,
    SegmentOutputCheckpoint,
};
use reth_static_file_types::StaticFileSegment;
pub use set::SegmentSet;
pub use static_file::{
    Headers as StaticFileHeaders, Receipts as StaticFileReceipts,
    Transactions as StaticFileTransactions,
};
use std::{fmt::Debug, ops::RangeInclusive};
pub use user::{
    AccountHistory, Bodies, Receipts as UserReceipts, ReceiptsByLogs, SenderRecovery,
    StorageHistory, TransactionLookup,
};

/// A segment represents a pruning of some portion of the data.
///
/// Segments are called from [`Pruner`](crate::Pruner) with the following lifecycle:
/// 1. Call [`Segment::prune`] with `delete_limit` of [`PruneInput`].
/// 2. If [`Segment::prune`] returned a [`Some`] in `checkpoint` of [`SegmentOutput`], call
///    [`Segment::save_checkpoint`].
/// 3. Subtract `pruned` of [`SegmentOutput`] from `delete_limit` of next [`PruneInput`].
pub trait Segment<Provider>: Debug + Send + Sync {
    /// Segment of data that's pruned.
    fn segment(&self) -> PruneSegment;

    /// Prune mode with which the segment was initialized.
    fn mode(&self) -> Option<PruneMode>;

    /// Purpose of the segment.
    fn purpose(&self) -> PrunePurpose;

    /// Resolves the current target, including retention constraints imposed by other segments.
    fn target_block(
        &self,
        tip: BlockNumber,
    ) -> Result<Option<(BlockNumber, PruneMode)>, PruneSegmentError> {
        self.mode()
            .map(|mode| mode.prune_target_block(tip, self.segment(), self.purpose()))
            .transpose()
            .map(Option::flatten)
    }

    /// Prune data for [`Self::segment`] using the provided input.
    fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput, PrunerError>;

    /// Save checkpoint for [`Self::segment`] to the database.
    fn save_checkpoint(
        &self,
        provider: &Provider,
        checkpoint: PruneCheckpoint,
    ) -> ProviderResult<()>
    where
        Provider: PruneCheckpointWriter,
    {
        provider.save_prune_checkpoint(self.segment(), checkpoint)
    }
}

/// Segment pruning input, see [`Segment::prune`].
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub struct PruneInput {
    pub(crate) previous_checkpoint: Option<PruneCheckpoint>,
    /// Target block up to which the pruning needs to be done, inclusive.
    pub(crate) to_block: BlockNumber,
    /// Limits pruning of a segment.
    pub(crate) limiter: PruneLimiter,
}

impl PruneInput {
    /// Get next inclusive tx number range to prune according to the checkpoint and `to_block` block
    /// number.
    ///
    /// To get the range start:
    /// 1. If checkpoint exists, get next block body and return its first tx number.
    /// 2. If checkpoint doesn't exist, return 0.
    ///
    /// To get the range end: get last tx number for `to_block`.
    pub(crate) fn get_next_tx_num_range<Provider: BlockReader>(
        &self,
        provider: &Provider,
    ) -> ProviderResult<Option<RangeInclusive<TxNumber>>> {
        let from_tx_number = match self.previous_checkpoint {
            Some(checkpoint) => match (checkpoint.tx_number, checkpoint.block_number) {
                (Some(tx_number), _) => tx_number + 1,
                // An empty prefix has a block checkpoint but no transaction checkpoint.
                (None, Some(block)) => provider
                    .block_body_indices(block)?
                    .map(|body| body.next_tx_num())
                    .unwrap_or_default(),
                (None, None) => 0,
            },
            None => 0,
        };

        let to_tx_number = match provider.block_body_indices(self.to_block)? {
            Some(body) => {
                let last_tx = body.last_tx_num();
                if last_tx + body.tx_count() == 0 {
                    // Prevents a scenario where the pruner correctly starts at a finalized block,
                    // but the first transaction (tx_num = 0) only appears on a non-finalized one.
                    // Should only happen on a test/hive scenario.
                    return Ok(None)
                }
                last_tx
            }
            None => return Ok(None),
        };

        let range = from_tx_number..=to_tx_number;
        if range.is_empty() {
            return Ok(None)
        }

        Ok(Some(range))
    }

    /// Get next inclusive block range to prune according to the checkpoint, `to_block` block
    /// number and `limit`.
    ///
    /// To get the range start (`from_block`):
    /// 1. If checkpoint exists, use next block.
    /// 2. If checkpoint doesn't exist, use block 0.
    ///
    /// To get the range end: use block `to_block`.
    pub(crate) fn get_next_block_range(&self) -> Option<RangeInclusive<BlockNumber>> {
        let from_block = self.get_start_next_block_range();
        let range = from_block..=self.to_block;
        if range.is_empty() {
            return None
        }

        Some(range)
    }

    /// Returns the start of the next block range.
    ///
    /// 1. If checkpoint exists, use next block.
    /// 2. If checkpoint doesn't exist, use block 0.
    pub(crate) fn get_start_next_block_range(&self) -> u64 {
        self.previous_checkpoint
            .and_then(|checkpoint| checkpoint.block_number)
            // Checkpoint exists, prune from the next block after the highest pruned one
            .map(|block_number| block_number + 1)
            // No checkpoint exists, prune from genesis
            .unwrap_or(0)
    }
}

/// Removes complete jars below the target, retaining the boundary jar.
/// Transactions retain their active jar. Legacy receipt files have no writer after switching
/// to receipt pruning, so their final jar can also be removed once it is entirely expired.
/// File ranges are the source of progress, so deletion is resumable even if the DB checkpoint
/// commit was interrupted. A single jar is the minimum unit of work and may exceed the row budget.
pub(crate) fn prune_static_files<Provider: StaticFileProviderFactory>(
    provider: &Provider,
    input: PruneInput,
    segment: StaticFileSegment,
) -> Result<SegmentOutput, PrunerError> {
    let files = provider.static_file_provider();
    let mut limiter = input.limiter;
    let mut output = SegmentOutput::done();
    output.checkpoint =
        input.previous_checkpoint.map(SegmentOutputCheckpoint::from_prune_checkpoint);
    loop {
        let Some(end) = files.get_lowest_static_file_block(segment) else { break };
        if end > input.to_block ||
            (!segment.is_receipts() && Some(end) == files.get_highest_static_file_block(segment))
        {
            break
        }
        if limiter.is_limit_reached() {
            output.progress = limiter.progress(false);
            break
        }
        let header = {
            let jar = files
                .get_segment_provider(segment, || Some(files.find_fixed_range(segment, end)), None)?
                .ok_or(PrunerError::InconsistentData("Static file to prune is missing"))?;
            jar.user_header().clone()
        };
        files.delete_jar(segment, end)?;
        let pruned = header.tx_range().map_or(0, |range| range.len() as usize);
        limiter.increment_deleted_entries_count_by(pruned);
        output.pruned += pruned;
        output.checkpoint = Some(SegmentOutputCheckpoint {
            block_number: Some(end),
            tx_number: header
                .tx_range()
                .map(|range| range.end())
                .or_else(|| output.checkpoint.and_then(|checkpoint| checkpoint.tx_number)),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use reth_provider::{
        providers::BlockchainProvider,
        test_utils::{create_test_provider_factory, MockEthProvider},
    };
    use reth_testing_utils::generators::{self, random_block_range, BlockRangeParams};

    #[test]
    fn test_prune_input_get_next_tx_num_range_no_to_block() {
        let input = PruneInput {
            previous_checkpoint: None,
            to_block: 10,
            limiter: PruneLimiter::default(),
        };

        // Default provider with no block corresponding to block 10
        let provider = MockEthProvider::default();

        // No block body for block 10, expected None
        let range = input.get_next_tx_num_range(&provider).expect("Expected range");
        assert!(range.is_none());
    }

    #[test]
    fn test_prune_input_get_next_tx_num_range_no_tx() {
        let input = PruneInput {
            previous_checkpoint: None,
            to_block: 10,
            limiter: PruneLimiter::default(),
        };

        let mut rng = generators::rng();
        let factory = create_test_provider_factory();

        // Generate 10 random blocks with no transactions
        let blocks = random_block_range(
            &mut rng,
            0..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..1, ..Default::default() },
        );

        // Insert the blocks into the database
        let provider_rw = factory.provider_rw().expect("failed to get provider_rw");
        for block in &blocks {
            provider_rw
                .insert_historical_block(
                    block.clone().try_recover().expect("failed to seal block with senders"),
                )
                .expect("failed to insert block");
            provider_rw.commit_view().unwrap();
        }

        // Create a new provider
        let provider = BlockchainProvider::new(factory).unwrap();

        // Since there are no transactions, expected None
        let range = input.get_next_tx_num_range(&provider).expect("Expected range");
        assert!(range.is_none());
    }

    #[test]
    fn test_prune_input_get_next_tx_num_range_valid() {
        // Create a new prune input
        let input = PruneInput {
            previous_checkpoint: None,
            to_block: 10,
            limiter: PruneLimiter::default(),
        };

        let mut rng = generators::rng();
        let factory = create_test_provider_factory();

        // Generate 10 random blocks with some transactions
        let blocks = random_block_range(
            &mut rng,
            0..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..5, ..Default::default() },
        );

        // Insert the blocks into the database
        let provider_rw = factory.provider_rw().expect("failed to get provider_rw");
        for block in &blocks {
            provider_rw
                .insert_historical_block(
                    block.clone().try_recover().expect("failed to seal block with senders"),
                )
                .expect("failed to insert block");
            provider_rw.commit_view().unwrap();
        }

        // Create a new provider
        let provider = BlockchainProvider::new(factory).unwrap();

        // Get the next tx number range
        let range = input.get_next_tx_num_range(&provider).expect("Expected range").unwrap();

        // Calculate the total number of transactions
        let num_txs = blocks.iter().map(|block| block.transaction_count() as u64).sum::<u64>();

        assert_eq!(range, 0..=num_txs - 1);
    }

    #[test]
    fn test_prune_input_get_next_tx_checkpoint_without_tx_number() {
        // Create a prune input with a previous checkpoint without a tx number (unexpected)
        let input = PruneInput {
            previous_checkpoint: Some(PruneCheckpoint {
                block_number: Some(5),
                tx_number: None,
                prune_mode: PruneMode::Full,
            }),
            to_block: 10,
            limiter: PruneLimiter::default(),
        };

        let mut rng = generators::rng();
        let factory = create_test_provider_factory();

        // Generate 10 random blocks
        let blocks = random_block_range(
            &mut rng,
            0..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..5, ..Default::default() },
        );

        // Insert the blocks into the database
        let provider_rw = factory.provider_rw().expect("failed to get provider_rw");
        for block in &blocks {
            provider_rw
                .insert_historical_block(
                    block.clone().try_recover().expect("failed to seal block with senders"),
                )
                .expect("failed to insert block");
            provider_rw.commit_view().unwrap();
        }

        // Create a new provider
        let provider = BlockchainProvider::new(factory).unwrap();

        // Fetch the range and check if it is correct
        let range = input.get_next_tx_num_range(&provider).expect("Expected range").unwrap();

        // Calculate the total number of transactions
        let num_txs = blocks.iter().map(|block| block.transaction_count() as u64).sum::<u64>();

        assert_eq!(range, 0..=num_txs - 1,);
    }

    #[test]
    fn test_prune_input_get_next_tx_empty_range() {
        // Create a new provider via factory
        let mut rng = generators::rng();
        let factory = create_test_provider_factory();

        // Generate 10 random blocks
        let blocks = random_block_range(
            &mut rng,
            0..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 0..5, ..Default::default() },
        );

        // Insert the blocks into the database
        let provider_rw = factory.provider_rw().expect("failed to get provider_rw");
        for block in &blocks {
            provider_rw
                .insert_historical_block(
                    block.clone().try_recover().expect("failed to seal block with senders"),
                )
                .expect("failed to insert block");
            provider_rw.commit_view().unwrap();
        }

        // Create a new provider
        let provider = BlockchainProvider::new(factory).unwrap();

        // Get the last tx number
        // Calculate the total number of transactions
        let num_txs = blocks.iter().map(|block| block.transaction_count() as u64).sum::<u64>();
        let max_range = num_txs - 1;

        // Create a prune input with a previous checkpoint that is the last tx number
        let input = PruneInput {
            previous_checkpoint: Some(PruneCheckpoint {
                block_number: Some(5),
                tx_number: Some(max_range),
                prune_mode: PruneMode::Full,
            }),
            to_block: 10,
            limiter: PruneLimiter::default(),
        };

        // We expect an empty range since the previous checkpoint is the last tx number
        let range = input.get_next_tx_num_range(&provider).expect("Expected range");
        assert!(range.is_none());
    }
}
