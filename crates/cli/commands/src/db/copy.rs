use clap::Parser;
use reth_db::mdbx;
use std::path::PathBuf;

/// Copies the MDBX database to a new location.
///
/// Equivalent to the standalone `mdbx_copy` tool but bundled into reth.
#[derive(Parser, Debug)]
pub struct Command {
    /// Destination path for the database copy.
    dest: PathBuf,

    /// Compact the database while copying (reclaims free space).
    #[arg(short, long)]
    compact: bool,

    /// Force dynamic size for the destination database.
    #[arg(short = 'd', long)]
    force_dynamic_size: bool,

    /// Throttle to avoid MVCC pressure on writers.
    #[arg(short = 'p', long)]
    throttle_mvcc: bool,
}

impl Command {
    /// Execute `db copy` command
    pub fn execute(self, _db: &mdbx::DatabaseEnv) -> eyre::Result<()> {
        // This gravity fork uses RocksDB as the database backend.
        // MDBX-specific copy (mdbx_env_copy) is not supported for RocksDB databases.
        // Use filesystem-level copy tools (cp -r, rsync) to copy the RocksDB directory instead.
        eyre::bail!(
            "The `db copy` command is not supported for the RocksDB backend used by this node. \
             To copy the database, stop the node and use a filesystem copy tool such as \
             `cp -r` or `rsync` to copy the database directory to {:?}.",
            self.dest
        )
    }
}
