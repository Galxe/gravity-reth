use crate::{
    common::CliNodeTypes,
    db::get::{maybe_json_value_parser, table_key},
};
use alloy_primitives::map::foldhash::fast::FixedState;
use clap::Parser;
use reth_chainspec::EthereumHardforks;
use reth_db::DatabaseEnv;
use reth_db_api::{
    cursor::DbCursorRO,
    table::{Decode, Table},
    transaction::DbTx,
    DatabaseError, RawKey, RawTable, TableViewer, Tables,
};
use reth_db_common::DbTool;
use reth_node_builder::{NodeTypesWithDB, NodeTypesWithDBAdapter};
use reth_provider::{providers::ProviderNodeTypes, DBProvider};
use std::{
    hash::{BuildHasher, Hasher},
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{info, warn};

#[derive(Parser, Debug)]
/// The arguments for the `reth db checksum` command
pub struct Command {
    /// The table name
    table: Tables,

    /// The start of the range to checksum.
    #[arg(long, value_parser = maybe_json_value_parser)]
    start_key: Option<String>,

    /// The end of the range to checksum.
    #[arg(long, value_parser = maybe_json_value_parser)]
    end_key: Option<String>,

    /// The maximum number of records that are queried and used to compute the
    /// checksum.
    #[arg(long)]
    limit: Option<usize>,
}

impl Command {
    /// Execute `db checksum` command
    pub fn execute<N: CliNodeTypes<ChainSpec: EthereumHardforks>>(
        self,
        tool: &DbTool<NodeTypesWithDBAdapter<N, Arc<DatabaseEnv>>>,
    ) -> eyre::Result<()> {
        warn!("This command should be run without the node running!");
        self.table.view(&ChecksumViewer {
            tool,
            start_key: self.start_key,
            end_key: self.end_key,
            limit: self.limit,
        })?;
        Ok(())
    }
}

pub(crate) struct ChecksumViewer<'a, N: NodeTypesWithDB> {
    tool: &'a DbTool<N>,
    start_key: Option<String>,
    end_key: Option<String>,
    limit: Option<usize>,
}

impl<N: NodeTypesWithDB> ChecksumViewer<'_, N> {
    pub(crate) const fn new(tool: &'_ DbTool<N>) -> ChecksumViewer<'_, N> {
        ChecksumViewer { tool, start_key: None, end_key: None, limit: None }
    }
}

impl<N: ProviderNodeTypes> TableViewer<(u64, Duration)> for ChecksumViewer<'_, N> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> Result<(u64, Duration), Self::Error> {
        let provider =
            self.tool.provider_factory.provider()?.disable_long_read_transaction_safety();
        let tx = provider.tx_ref();
        log_checksum_start(&self.start_key, &self.end_key, self.limit);

        let mut cursor = tx.cursor_read::<RawTable<T>>()?;
        let parse_key = |key: &str| table_key::<T>(key).map(RawKey::new);
        let start = self.start_key.as_deref().map(parse_key).transpose()?;
        let end = self.end_key.as_deref().map(parse_key).transpose()?;
        let bound = |key: Option<RawKey<T::Key>>| key.map_or(Bound::Unbounded, Bound::Included);
        let mut entries = cursor
            .walk_range((bound(start), bound(end)))?
            .map(|entry| entry.map(|(k, v)| (k.into_key(), v.into_value())));

        checksum_entries(
            &mut entries,
            self.limit.unwrap_or(usize::MAX),
            &|key| Ok(serde_json::to_string(&T::Key::decode(key)?).unwrap_or_default()),
            T::NAME,
        )
    }
}

fn log_checksum_start(start: &Option<String>, end: &Option<String>, limit: Option<usize>) {
    info!("Start computing checksum, start={:?}, end={:?}, limit={:?}", start, end, limit);
}

/// Hashes the raw key-value entries of a table and logs the result.
///
/// `key_json` serializes a raw key to JSON, as it can only be decoded with the table's key type.
fn checksum_entries(
    entries: &mut dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), DatabaseError>>,
    limit: usize,
    key_json: &dyn Fn(&[u8]) -> eyre::Result<String>,
    table: &str,
) -> eyre::Result<(u64, Duration)> {
    let start_time = Instant::now();
    let mut hasher = FixedState::with_seed(u64::from_be_bytes(*b"RETHRETH")).build_hasher();
    let mut total = 0;

    let mut enumerate_start_key = None;
    let mut enumerate_end_key = None;
    for (index, entry) in entries.enumerate() {
        let (k, v) = entry?;

        if index.is_multiple_of(100_000) {
            info!("Hashed {index} entries.");
        }

        hasher.write(&k);
        hasher.write(&v);

        if enumerate_start_key.is_none() {
            enumerate_start_key = Some(k.clone());
        }
        enumerate_end_key = Some(k);

        total = index + 1;
        if total >= limit {
            break;
        }
    }

    info!("Hashed {total} entries.");
    if let (Some(s), Some(e)) = (enumerate_start_key, enumerate_end_key) {
        info!("start-key: {}", key_json(&s)?);
        info!("end-key: {}", key_json(&e)?);
    }

    let checksum = hasher.finish();
    let elapsed = start_time.elapsed();

    info!("Checksum for table `{}`: {:#x} (elapsed: {:?})", table, checksum, elapsed);

    Ok((checksum, elapsed))
}
