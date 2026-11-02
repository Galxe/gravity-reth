use crate::{
    segments::{self, PruneInput, Segment},
    PrunerError,
};
use alloy_primitives::BlockNumber;
use reth_provider::{BlockReader, PruneCheckpointReader, StaticFileProviderFactory};
use reth_prune_types::{
    PruneInterruptReason, PruneMode, PrunePurpose, PruneSegment, PruneSegmentError, SegmentOutput,
    SegmentOutputCheckpoint,
};
use reth_static_file_types::StaticFileSegment;
use tracing::{debug, instrument};

/// Segment responsible for pruning transactions in static files.
///
/// This segment is controlled by the `bodies_history` configuration.
#[derive(Debug)]
pub struct Bodies {
    mode: PruneMode,
    /// Transaction lookup prune mode. Used to determine if we need to wait for tx lookup pruning
    /// before deleting transaction bodies.
    tx_lookup_mode: Option<PruneMode>,
}

impl Bodies {
    /// Creates a new [`Bodies`] segment with the given prune mode and optional transaction lookup
    /// prune mode for coordination.
    pub const fn new(mode: PruneMode, tx_lookup_mode: Option<PruneMode>) -> Self {
        Self { mode, tx_lookup_mode }
    }

    /// Returns the next best block that bodies can prune up to considering the transaction lookup
    /// pruning configuration (if any) and progress.
    ///
    /// Returns `None` if there's no block available to prune (e.g., waiting on `tx_lookup`).
    fn next_bodies_prune_target<Provider>(
        &self,
        provider: &Provider,
        input: &PruneInput,
    ) -> Result<Option<BlockNumber>, PrunerError>
    where
        Provider: PruneCheckpointReader,
    {
        let Some(tx_lookup_mode) = self.tx_lookup_mode else { return Ok(Some(input.to_block)) };

        let tx_lookup_checkpoint = provider
            .get_prune_checkpoint(PruneSegment::TransactionLookup)?
            .and_then(|cp| cp.block_number);

        // Determine the safe prune target, if any.
        // tx_lookup's next_pruned_block tells us what block it will prune next.
        // - None: tx_lookup will never prune more blocks (e.g. Before(N) reached its target), so
        //   bodies can prune freely
        // - Some(next) > to_block: tx_lookup is ahead of our target, so we're safe to prune
        //   to_block
        // - Some(next) <= to_block: tx_lookup still needs to prune blocks we want to delete, so we
        //   must wait and only prune up to (next - 1) to preserve tx data it needs
        let to_block = match tx_lookup_mode.next_pruned_block(tx_lookup_checkpoint) {
            None => Some(input.to_block),
            Some(tx_lookup_next) if tx_lookup_next > input.to_block => Some(input.to_block),
            Some(tx_lookup_next) => {
                // We can only prune bodies up to the block BEFORE tx_lookup's next target.
                // tx_lookup_next is the next block tx_lookup will prune, meaning it still needs
                // to read transactions from that block. We must preserve those transactions,
                // so bodies can only safely delete up to (tx_lookup_next - 1).
                let Some(safe) = tx_lookup_next.checked_sub(1) else {
                    return Ok(None);
                };

                if input.previous_checkpoint.is_some_and(|cp| cp.block_number.unwrap_or(0) >= safe)
                {
                    // we have pruned what we can
                    return Ok(None)
                }

                debug!(
                    target: "pruner",
                    to_block = input.to_block,
                    safe,
                    "Bodies pruning limited by tx_lookup progress"
                );
                Some(safe)
            }
        };

        Ok(to_block)
    }
}

impl<Provider> Segment<Provider> for Bodies
where
    Provider: StaticFileProviderFactory + BlockReader + PruneCheckpointReader,
{
    fn segment(&self) -> PruneSegment {
        PruneSegment::Bodies
    }

    fn mode(&self) -> Option<PruneMode> {
        Some(self.mode)
    }

    fn purpose(&self) -> PrunePurpose {
        PrunePurpose::User
    }

    fn target_block(
        &self,
        tip: BlockNumber,
    ) -> Result<Option<(BlockNumber, PruneMode)>, PruneSegmentError> {
        let Some((mut target, mode)) =
            self.mode.prune_target_block(tip, PruneSegment::Bodies, PrunePurpose::User)?
        else {
            return Ok(None)
        };
        if let Some(lookup_mode) = self.tx_lookup_mode {
            match lookup_mode {
                PruneMode::Distance(_) | PruneMode::Full => {
                    let Some((lookup_target, _)) = lookup_mode.prune_target_block(
                        tip,
                        PruneSegment::TransactionLookup,
                        PrunePurpose::User,
                    )?
                    else {
                        return Ok(None)
                    };
                    // Future lookup pruning still needs bodies within its retention window.
                    target = target.min(lookup_target);
                }
                PruneMode::Before(block) if block > tip => return Ok(None),
                PruneMode::Before(_) => {}
            }
        }
        Ok(Some((target, mode)))
    }

    #[instrument(
        name = "Bodies::prune",
        target = "pruner",
        skip(self, provider),
        ret(level = "trace")
    )]
    fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput, PrunerError> {
        let Some(to_block) = self.next_bodies_prune_target(provider, &input)? else {
            debug!(
                to_block = input.to_block,
                "Transaction lookup still has work to be done up to target block"
            );
            return Ok(SegmentOutput::not_done(
                PruneInterruptReason::WaitingOnSegment(PruneSegment::TransactionLookup),
                input.previous_checkpoint.map(SegmentOutputCheckpoint::from_prune_checkpoint),
            ));
        };

        let waiting = to_block < input.to_block;
        let adjusted_input = PruneInput { to_block, ..input };
        let mut output = segments::prune_static_files(
            provider,
            adjusted_input,
            StaticFileSegment::Transactions,
        )?;
        if waiting {
            output.progress =
                output.progress.combine(reth_prune_types::PruneProgress::HasMoreData(
                    PruneInterruptReason::WaitingOnSegment(PruneSegment::TransactionLookup),
                ));
        }
        Ok(output)
    }
}
