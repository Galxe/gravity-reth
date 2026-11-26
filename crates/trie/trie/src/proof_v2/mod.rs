//! Proof calculation version 2 using leaf data and cached branch hashes.

mod calculator;
pub use calculator::*;

mod multiproof_calculator;
pub use multiproof_calculator::MultiProofCalculator;

/// Read-only cursor factory adapters for reusable proof calculations.
pub mod cursor;
pub use cursor::{
    ProofCursorFactory, RebindableHashedStorageCursor, ResettableHashedCursor,
    ResettableTrieCursor, TrieStorageCursor,
};

mod value;
pub use value::*;

mod node;

mod target;
pub(crate) use target::*;

/// Target to use with the `tracing` crate.
static TRACE_TARGET: &str = "trie::proof_v2";
