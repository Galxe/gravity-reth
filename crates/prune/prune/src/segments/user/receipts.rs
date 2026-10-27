use crate::{
    segments::{PruneInput, Segment},
    PrunerError,
};
use reth_db_api::{table::Value, transaction::DbTxMut};
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    errors::provider::ProviderResult, BlockReader, DBProvider, NodePrimitivesProvider,
    PruneCheckpointWriter, StaticFileProviderFactory, TransactionsProvider,
};
use reth_prune_types::{PruneCheckpoint, PruneMode, PrunePurpose, PruneSegment, SegmentOutput};
use reth_static_file_types::StaticFileSegment;
use tracing::instrument;

#[derive(Debug)]
pub struct Receipts {
    mode: PruneMode,
}

impl Receipts {
    pub const fn new(mode: PruneMode) -> Self {
        Self { mode }
    }
}

impl<Provider> Segment<Provider> for Receipts
where
    Provider: DBProvider<Tx: DbTxMut>
        + StaticFileProviderFactory
        + PruneCheckpointWriter
        + TransactionsProvider
        + BlockReader
        + NodePrimitivesProvider<Primitives: NodePrimitives<Receipt: Value>>,
{
    fn segment(&self) -> PruneSegment {
        PruneSegment::Receipts
    }

    fn mode(&self) -> Option<PruneMode> {
        Some(self.mode)
    }

    fn purpose(&self) -> PrunePurpose {
        PrunePurpose::User
    }

    #[instrument(level = "trace", target = "pruner", skip(self, provider), ret)]
    fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput, PrunerError> {
        // The shared Receipts checkpoint tracks DB cleanup, including archive migration.
        // Always inspect the files independently, even while that checkpoint is ahead of the
        // user's target. Never overwrite it with the lower file-deletion boundary.
        let files_output = crate::segments::prune_static_files(
            provider,
            PruneInput {
                previous_checkpoint: None,
                to_block: input.to_block,
                limiter: input.limiter.clone(),
            },
            StaticFileSegment::Receipts,
        )?;
        let mut input = input;
        input.limiter.increment_deleted_entries_count_by(files_output.pruned);
        if input.limiter.is_limit_reached() {
            return Ok(SegmentOutput {
                progress: input.limiter.progress(false),
                pruned: files_output.pruned,
                checkpoint: None,
            })
        }
        let mut output = crate::segments::receipts::prune(provider, input)?;
        output.progress = output.progress.combine(files_output.progress);
        output.pruned += files_output.pruned;
        Ok(output)
    }

    fn save_checkpoint(
        &self,
        provider: &Provider,
        checkpoint: PruneCheckpoint,
    ) -> ProviderResult<()> {
        crate::segments::receipts::save_checkpoint(provider, checkpoint)
    }
}
