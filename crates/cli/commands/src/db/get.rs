use alloy_primitives::{hex, BlockHash};
use clap::Parser;
use reth_db::{
    static_file::{
        ColumnSelectorOne, ColumnSelectorTwo, HeaderWithHashMask, ReceiptMask, TransactionMask,
    },
    RawDupSort,
};
use reth_db_api::{
    cursor::DbCursorRO,
    database::Database,
    table::{Compress, Decompress, DupSort, Table},
    tables,
    transaction::DbTx,
    RawKey, RawTable, TableViewer,
};
use reth_db_common::DbTool;
use reth_node_api::{HeaderTy, ReceiptTy, TxTy};
use reth_node_builder::NodeTypesWithDB;
use reth_provider::{providers::ProviderNodeTypes, StaticFileProviderFactory};
use reth_static_file_types::StaticFileSegment;
use tracing::error;

/// The arguments for the `reth db get` command
#[derive(Parser, Debug)]
pub struct Command {
    #[command(subcommand)]
    subcommand: Subcommand,
}

#[derive(clap::Subcommand, Debug)]
enum Subcommand {
    /// Gets the content of a database table for the given key
    Mdbx {
        table: tables::Tables,

        /// The key to get content for
        #[arg(value_parser = maybe_json_value_parser)]
        key: String,

        /// The subkey to get content for
        #[arg(value_parser = maybe_json_value_parser)]
        subkey: Option<String>,

        /// Optional end key for range query (exclusive upper bound)
        #[arg(value_parser = maybe_json_value_parser)]
        end_key: Option<String>,

        /// Optional end subkey for range query (exclusive upper bound)
        #[arg(value_parser = maybe_json_value_parser)]
        end_subkey: Option<String>,

        /// Output bytes instead of human-readable decoded value
        #[arg(long)]
        raw: bool,
    },
    /// Gets the content of a static file segment for the given key
    StaticFile {
        segment: StaticFileSegment,

        /// The key to get content for
        #[arg(value_parser = maybe_json_value_parser)]
        key: String,

        /// The subkey to get content for, for example address in changeset
        #[arg(value_parser = maybe_json_value_parser)]
        subkey: Option<String>,

        /// Output bytes instead of human-readable decoded value
        #[arg(long)]
        raw: bool,
    },
}

impl Command {
    /// Execute `db get` command
    pub fn execute<N: ProviderNodeTypes>(self, tool: &DbTool<N>) -> eyre::Result<()> {
        match self.subcommand {
            Subcommand::Mdbx { table, key, subkey, end_key, end_subkey, raw } => {
                table.view(&GetValueViewer { tool, key, subkey, end_key, end_subkey, raw })?
            }
            Subcommand::StaticFile { segment, key, subkey: _, raw } => {
                let (key, mask): (u64, _) = match segment {
                    StaticFileSegment::Headers => (
                        table_key::<tables::Headers>(&key)?,
                        <HeaderWithHashMask<HeaderTy<N>>>::MASK,
                    ),
                    StaticFileSegment::Transactions => {
                        (table_key::<tables::Transactions>(&key)?, <TransactionMask<TxTy<N>>>::MASK)
                    }
                    StaticFileSegment::Receipts => {
                        (table_key::<tables::Receipts>(&key)?, <ReceiptMask<ReceiptTy<N>>>::MASK)
                    }
                    // NOTE(gravity): this storage backend keeps transaction senders and
                    // changesets in the database, not in static files.
                    StaticFileSegment::TransactionSenders |
                    StaticFileSegment::AccountChangeSets |
                    StaticFileSegment::StorageChangeSets => {
                        eyre::bail!(
                            "{segment} static file segment is not supported by this storage backend"
                        )
                    }
                };

                let content = tool.provider_factory.static_file_provider().find_static_file(
                    segment,
                    |provider| {
                        let mut cursor = provider.cursor()?;
                        cursor.get(key.into(), mask).map(|result| {
                            result.map(|vec| {
                                vec.iter().map(|slice| slice.to_vec()).collect::<Vec<_>>()
                            })
                        })
                    },
                )?;

                match content {
                    Some(content) => {
                        if raw {
                            println!("{}", hex::encode_prefixed(&content[0]));
                        } else {
                            match segment {
                                StaticFileSegment::Headers => {
                                    let header = HeaderTy::<N>::decompress(content[0].as_slice())?;
                                    let block_hash = BlockHash::decompress(content[1].as_slice())?;
                                    println!(
                                        "Header\n{}\n\nBlockHash\n{}",
                                        serde_json::to_string_pretty(&header)?,
                                        serde_json::to_string_pretty(&block_hash)?
                                    );
                                }
                                StaticFileSegment::Transactions => {
                                    let transaction = TxTy::<N>::decompress(content[0].as_slice())?;
                                    println!("{}", serde_json::to_string_pretty(&transaction)?);
                                }
                                StaticFileSegment::Receipts => {
                                    let receipt =
                                        ReceiptTy::<N>::decompress(content[0].as_slice())?;
                                    println!("{}", serde_json::to_string_pretty(&receipt)?);
                                }
                                StaticFileSegment::TransactionSenders |
                                StaticFileSegment::AccountChangeSets |
                                StaticFileSegment::StorageChangeSets => {
                                    unreachable!(
                                        "unsupported segments are special cased before this match"
                                    )
                                }
                            }
                        }
                    }
                    None => {
                        error!(target: "reth::cli", "No content for the given table key.");
                    }
                };
            }
        }

        Ok(())
    }
}

/// Get an instance of key for given table
pub(crate) fn table_key<T: Table>(key: &str) -> Result<T::Key, eyre::Error> {
    serde_json::from_str(key).map_err(|e| eyre::eyre!(e))
}

/// Get an instance of subkey for given dupsort table
fn table_subkey<T: DupSort>(subkey: &str) -> Result<T::SubKey, eyre::Error> {
    serde_json::from_str(subkey).map_err(|e| eyre::eyre!(e))
}

struct GetValueViewer<'a, N: NodeTypesWithDB> {
    tool: &'a DbTool<N>,
    key: String,
    subkey: Option<String>,
    end_key: Option<String>,
    end_subkey: Option<String>,
    raw: bool,
}

impl<N: ProviderNodeTypes> TableViewer<()> for GetValueViewer<'_, N> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> Result<(), Self::Error> {
        let key = table_key::<T>(&self.key)?;

        // A non-dupsort table cannot have subkeys. The `subkey` arg becomes the `end_key`. First we
        // check that `end_key` and `end_subkey` weren't previously given, as that wouldn't be
        // valid.
        if self.end_key.is_some() || self.end_subkey.is_some() {
            return Err(eyre::eyre!("Only END_KEY can be given for non-DUPSORT tables"));
        }

        let end_key = self.subkey.clone();

        // Check if we're doing a range query
        if let Some(ref end_key_str) = end_key {
            let end_key = table_key::<T>(end_key_str)?;

            // Use walk_range to iterate over the range
            let tx = self.tool.provider_factory.db_ref().tx()?;
            let mut cursor = tx.cursor_read::<T>()?;
            for result in cursor.walk_range(key..end_key)? {
                let (k, v) = result?;
                print_json(&row_json::<T>(k, v, self.raw))?;
            }
            tx.commit()?;
        } else {
            // Single key lookup
            let content = if self.raw {
                self.tool
                    .get::<RawTable<T>>(RawKey::from(key))?
                    .map(|content| hex::encode_prefixed(content.raw_value()))
            } else {
                self.tool.get::<T>(key)?.as_ref().map(serde_json::to_string_pretty).transpose()?
            };

            print_content(content, "key");
        }

        Ok(())
    }

    // NOTE(gravity): the upstream range-query variant requires a `ValueWithSubKey` bound that
    // this backend's `TableViewer::view_dupsort` does not carry; only single key/subkey lookups
    // are supported for dupsort tables.
    fn view_dupsort<T: DupSort>(&self) -> Result<(), Self::Error> {
        // get a key for given table
        let key = table_key::<T>(&self.key)?;

        if self.end_key.is_some() || self.end_subkey.is_some() {
            return Err(eyre::eyre!("Range queries are not supported for DUPSORT tables"));
        }

        // process dupsort table
        let subkey = self
            .subkey
            .as_deref()
            .ok_or_else(|| eyre::eyre!("a subkey is required for DUPSORT lookups"))?;
        let subkey = table_subkey::<T>(subkey)?;

        let content = if self.raw {
            self.tool
                .get_dup::<RawDupSort<T>>(RawKey::from(key), RawKey::from(subkey))?
                .map(|content| hex::encode_prefixed(content.raw_value()))
        } else {
            self.tool
                .get_dup::<T>(key, subkey)?
                .as_ref()
                .map(serde_json::to_string_pretty)
                .transpose()?
        };

        print_content(content, "subkey");
        Ok(())
    }
}

/// Builds the JSON object printed for a key-value pair in a range query.
fn row_json<T: Table>(key: T::Key, value: T::Value, raw: bool) -> serde_json::Value {
    if raw {
        row_object(
            hex::encode_prefixed(RawKey::from(key).raw_key()).into(),
            hex::encode_prefixed(value.compress().as_ref()).into(),
        )
    } else {
        row_object(serde_json::to_value(&key).unwrap(), serde_json::to_value(&value).unwrap())
    }
}

fn row_object(key: serde_json::Value, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "key": key, "val": value })
}

fn print_json(value: &serde_json::Value) -> eyre::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Prints the looked up content, or logs that nothing exists for the given `key` or `subkey`.
fn print_content(content: Option<String>, missing: &str) {
    match content {
        Some(content) => println!("{content}"),
        None => error!(target: "reth::cli", "No content for the given table {missing}."),
    }
}

/// Map the user input value to json
pub(crate) fn maybe_json_value_parser(value: &str) -> Result<String, eyre::Error> {
    if serde_json::from_str::<serde::de::IgnoredAny>(value).is_ok() {
        Ok(value.to_string())
    } else {
        serde_json::to_string(&value).map_err(|e| eyre::eyre!(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, B256};
    use clap::{Args, Parser};
    use reth_db_api::{
        models::{storage_sharded_key::StorageShardedKey, ShardedKey},
        AccountsHistory, HashedAccounts, Headers, StageCheckpoints, StoragesHistory,
    };
    use std::str::FromStr;

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn parse_numeric_key_args() {
        assert_eq!(table_key::<Headers>("123").unwrap(), 123);
        assert_eq!(
            table_key::<HashedAccounts>(
                "\"0x0ac361fe774b78f8fc4e86c1916930d150865c3fc2e21dca2e58833557608bac\""
            )
            .unwrap(),
            B256::from_str("0x0ac361fe774b78f8fc4e86c1916930d150865c3fc2e21dca2e58833557608bac")
                .unwrap()
        );
    }

    #[test]
    fn parse_string_key_args() {
        assert_eq!(
            table_key::<StageCheckpoints>("\"MerkleExecution\"").unwrap(),
            "MerkleExecution"
        );
    }

    #[test]
    fn parse_json_key_args() {
        assert_eq!(
            table_key::<StoragesHistory>(r#"{ "address": "0x01957911244e546ce519fbac6f798958fafadb41", "sharded_key": { "key": "0x0000000000000000000000000000000000000000000000000000000000000003", "highest_block_number": 18446744073709551615 } }"#).unwrap(),
            StorageShardedKey::new(
                address!("0x01957911244e546ce519fbac6f798958fafadb41"),
                B256::from_str(
                    "0x0000000000000000000000000000000000000000000000000000000000000003"
                )
                .unwrap(),
                18446744073709551615
            )
        );
    }

    #[test]
    fn parse_json_key_for_account_history() {
        assert_eq!(
            table_key::<AccountsHistory>(r#"{ "key": "0x4448e1273fd5a8bfdb9ed111e96889c960eee145", "highest_block_number": 18446744073709551615 }"#).unwrap(),
            ShardedKey::new(
                address!("0x4448e1273fd5a8bfdb9ed111e96889c960eee145"),
                18446744073709551615
            )
        );
    }
}
