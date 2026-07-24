//! Sharded key
use crate::{
    table::{Decode, Encode},
    DatabaseError,
};
use alloy_primitives::{Address, BlockNumber};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// Number of indices in one shard.
pub const NUM_OF_INDICES_IN_SHARD: usize = 2_000;

/// Sometimes data can be too big to be saved for a single key. This helps out by dividing the data
/// into different shards. Example:
///
/// `Address | 200` -> data is from block 0 to 200.
///
/// `Address | 300` -> data is from block 201 to 300.
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct ShardedKey<T> {
    /// The key for this type.
    pub key: T,
    /// Highest block number to which `value` is related to.
    pub highest_block_number: BlockNumber,
}

impl<T> AsRef<Self> for ShardedKey<T> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<T> ShardedKey<T> {
    /// Creates a new `ShardedKey<T>`.
    pub const fn new(key: T, highest_block_number: BlockNumber) -> Self {
        Self { key, highest_block_number }
    }

    /// Creates a new key with the highest block number set to maximum.
    /// This is useful when we want to search the last value for a given key.
    pub const fn last(key: T) -> Self {
        Self { key, highest_block_number: u64::MAX }
    }
}

/// Number of bytes in an encoded [`ShardedKey<Address>`]: 20-byte address + 8-byte BE block.
const SHARDED_KEY_ADDRESS_BYTES_SIZE: usize = 20 + std::mem::size_of::<BlockNumber>();

// Stack-allocated codec specialized for the only encoded shape, `ShardedKey<Address>` (the
// `AccountsHistory` table key), avoiding a per-key heap allocation on the RocksDB history-index
// write path (#21200). Bytes are identical to the previous `Vec<u8>` encoding:
// `[20-byte address][8-byte big-endian block]`. All other `ShardedKey<T>` uses are `AsRef`-only
// (`ShardedKey<B256>` is never encoded directly — `StorageShardedKey` encodes its fields itself).
impl Encode for ShardedKey<Address> {
    type Encoded = [u8; SHARDED_KEY_ADDRESS_BYTES_SIZE];

    fn encode(self) -> Self::Encoded {
        let mut buf = [0u8; SHARDED_KEY_ADDRESS_BYTES_SIZE];
        buf[..20].copy_from_slice(self.key.as_slice());
        buf[20..].copy_from_slice(&self.highest_block_number.to_be_bytes());
        buf
    }
}

impl Decode for ShardedKey<Address> {
    fn decode(value: &[u8]) -> Result<Self, DatabaseError> {
        if value.len() != SHARDED_KEY_ADDRESS_BYTES_SIZE {
            return Err(DatabaseError::Decode)
        }
        let key = Address::from_slice(&value[..20]);
        let highest_block_number =
            u64::from_be_bytes(value[20..].try_into().map_err(|_| DatabaseError::Decode)?);
        Ok(Self::new(key, highest_block_number))
    }
}
