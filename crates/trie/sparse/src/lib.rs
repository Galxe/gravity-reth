//! The implementation of sparse MPT.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod state;
pub use state::*;

mod trie;
pub use trie::*;

mod traits;
pub use traits::*;

/// In-memory arena sparse trie implementation.
#[cfg(feature = "std")]
pub mod arena;
#[cfg(feature = "std")]
pub use arena::{ArenaParallelSparseTrie, ArenaParallelismThresholds};

/// Operations specific to the retained arena implementation.
#[cfg(feature = "std")]
pub mod arena_traits;
#[cfg(feature = "std")]
pub use arena_traits::{LeafUpdate, SparseTrie as ArenaTrieInterface};

#[cfg(feature = "trie-debug")]
pub mod debug_recorder;
#[cfg(feature = "trie-debug")]
use serde_json as _;

pub mod provider;

#[cfg(feature = "metrics")]
mod metrics;

/// Re-export sparse trie error types.
pub mod errors {
    pub use reth_execution_errors::{
        SparseStateTrieError, SparseStateTrieErrorKind, SparseStateTrieResult, SparseTrieError,
        SparseTrieErrorKind, SparseTrieResult,
    };
}
