use clap::{Parser, Subcommand};
use reth_db_common::DbTool;
use reth_provider::{providers::ProviderNodeTypes, StaticFileProviderFactory};
use reth_static_file_types::StaticFileSegment;
use std::path::PathBuf;
use tracing::warn;

/// The arguments for the `reth db static-file-header` command
#[derive(Parser, Debug)]
pub struct Command {
    #[command(subcommand)]
    source: Source,
}

/// Source for locating the static file
#[derive(Subcommand, Debug)]
enum Source {
    /// Query by segment and block number
    Block {
        /// Static file segment
        #[arg(value_enum)]
        segment: StaticFileSegment,
        /// Block number to query
        block: u64,
    },
    /// Query by path to static file
    Path {
        /// Path to the static file
        path: PathBuf,
    },
}

impl Command {
    /// Execute `db static-file-header` command
    pub fn execute<N: ProviderNodeTypes>(self, tool: &DbTool<N>) -> eyre::Result<()> {
        let static_file_provider = tool.provider_factory.static_file_provider();
        let db_provider = tool.provider_factory.provider()?;
        // NOTE(gravity): check_consistency takes an explicit `has_receipt_pruning` flag; derive it
        // from the provider's prune modes.
        if let Err(err) = static_file_provider
            .check_consistency(&db_provider, db_provider.prune_modes_ref().has_receipts_pruning())
        {
            warn!("Error checking consistency of static files: {err}");
        }

        // Get the provider based on the source
        let provider = match self.source {
            Source::Path { path } => {
                // NOTE(gravity): no `get_segment_provider_for_path`; parse the segment from the
                // file name and resolve the jar through `get_segment_provider`.
                let segment = path
                    .file_name()
                    .and_then(|name| StaticFileSegment::parse_filename(&name.to_string_lossy()))
                    .map(|(segment, _)| segment)
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "Could not find static file segment for path: {}",
                            path.display()
                        )
                    })?;
                static_file_provider
                    .get_segment_provider(segment, || None, Some(&path))?
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "Could not find static file segment for path: {}",
                            path.display()
                        )
                    })?
            }
            Source::Block { segment, block } => {
                static_file_provider.get_segment_provider_from_block(segment, block, None)?
            }
        };

        let header = provider.user_header();

        println!("Segment: {}", header.segment());
        println!("Expected Block Range: {}", header.expected_block_range());
        println!("Block Range: {:?}", header.block_range());
        println!("Transaction Range: {:?}", header.tx_range());

        Ok(())
    }
}
