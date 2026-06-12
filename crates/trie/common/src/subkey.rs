//! Gravity: subkey-length trait for DupSort table values.
//!
//! In v2.2.0 `reth-primitives-traits` is pulled from crates.io, so Gravity's
//! `SubkeyContainedValue` extension can't live there anymore. It is relocated
//! here (the lowest path crate shared by the storage crates and the
//! nested-trie types). The impl for the foreign `StorageEntry` is legal via
//! the orphan rule because the trait is local to this crate.

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
