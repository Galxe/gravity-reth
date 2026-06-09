//! Gravity: subkey-length trait for DupSort table values.
//!
//! In v2.2.0 the `reth-primitives-traits` crate is pulled from crates.io, so
//! Gravity's `SubkeyContainedValue` extension can't live there. It is relocated
//! here (the lowest path crate shared by `reth-db-api` and the nested-trie
//! types) and implemented for the foreign `StorageEntry` via the orphan rule
//! (the trait is local to this crate).

use reth_primitives_traits::StorageEntry;

/// A DupSort table value whose compressed form embeds a subkey prefix.
pub trait SubkeyContainedValue {
    /// Byte length of the compressed subkey prefix, or `None` if absent.
    fn subkey_length(&self) -> Option<usize>;
}

impl SubkeyContainedValue for StorageEntry {
    fn subkey_length(&self) -> Option<usize> {
        Some(32)
    }
}
