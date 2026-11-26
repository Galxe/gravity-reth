//! An integration of `reth-trie` with `reth-db`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

/// Nested state root implementation using V2 trie tables.
pub mod nested_hash;

mod commitment;
mod hashed_cursor;
mod prefix_set;
mod proof;
mod state;
mod storage;
mod trie_cursor;
mod witness;

pub use commitment::{MerklePatriciaTrie, StateCommitment};
pub use hashed_cursor::{
    DatabaseHashedAccountCursor, DatabaseHashedCursorFactory, DatabaseHashedStorageCursor,
};
pub use prefix_set::PrefixSetLoader;
pub use proof::{DatabaseProof, DatabaseStorageProof};
pub use state::{DatabaseHashedPostState, DatabaseStateRoot};
pub use storage::{DatabaseHashedStorage, DatabaseStorageRoot};
pub use trie_cursor::{
    DatabaseAccountTrieCursor, DatabaseStorageTrieCursor, DatabaseTrieCursorFactory,
};
pub use witness::DatabaseTrieWitness;

/// Opt-in legacy trie verification over a shared immutable database snapshot.
#[cfg(not(target_family = "wasm"))]
pub mod verify;
#[cfg(not(target_family = "wasm"))]
pub use verify::LegacyDatabaseVerifier;

/// Opt-in legacy on-demand trie revert computation and cache.
pub mod changesets;
