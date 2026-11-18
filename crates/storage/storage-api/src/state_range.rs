//! Read-only, hash-native state ranges for an explicitly injected SNAP backend.

use alloc::{boxed::Box, vec::Vec};
use alloy_primitives::{Bytes, B256, U256};
use reth_primitives_traits::Account;
use reth_storage_errors::provider::ProviderResult;

/// A type that can iterate over consecutive hashed accounts and storage slots, and generate
/// boundary proofs for them, for serving `snap/2` (EIP-8189) `GetAccountRange`/`GetStorageRanges`
/// requests. Hash-native throughout, unlike [`crate::StorageRootProvider`].
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait StateRangeProvider {
    /// Returns consecutive accounts starting at `start`, with hash and byte soft limits.
    ///
    /// The first item reaching or passing `limit`, or exceeding `response_bytes`, may be
    /// included. `RangeEnd` must report the actual stopping condition so the caller knows
    /// whether a boundary proof is required.
    fn account_range(
        &self,
        start: B256,
        limit: B256,
        response_bytes: usize,
    ) -> RangeResult<(B256, Account)>;

    /// Returns the storage root for `hashed_address` without needing its address preimage.
    fn storage_root_by_hash(&self, hashed_address: B256) -> ProviderResult<B256>;

    /// Same as [`Self::account_range`], but for the storage slots of `hashed_address`.
    ///
    /// Returns `None` if `hashed_address` isn't present in the account trie at the pinned state
    /// root, distinct from an account that is present but has no storage.
    fn storage_range(
        &self,
        hashed_address: B256,
        start: B256,
        limit: B256,
        response_bytes: usize,
    ) -> StorageRangeResult;

    /// Returns an account-trie boundary proof for the already-hashed `keys`.
    fn account_range_proof(&self, keys: &[B256]) -> ProviderResult<Vec<Bytes>>;

    /// Same as [`Self::account_range_proof`], but for the storage trie of `hashed_address`.
    fn storage_range_proof(
        &self,
        hashed_address: B256,
        keys: &[B256],
    ) -> ProviderResult<Vec<Bytes>>;
}

/// A type that resolves retained state roots into reusable state range views.
///
/// The returned view must pin one consistent, verified state for its entire lifetime. Resolving
/// the root, iterating ranges and building boundary proofs must refer to that same state. A root
/// that is unavailable or not verified must return `None`, never a view over a different root.
/// This interface does not prescribe a database layout or a trie implementation.
#[auto_impl::auto_impl(&, Arc)]
pub trait StateRangeProviderFactory {
    /// Returns a view pinned to `state_root`, or `None` if that root is not retained.
    fn state_range_provider(&self, state_root: B256) -> ProviderResult<Option<StateRangeView>>;
}

/// A range query's items and why the range ended where it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeResponse<T> {
    /// The items found within the requested range, in ascending key order.
    pub items: Vec<T>,
    /// Why `items` doesn't necessarily continue past its last entry.
    pub end: RangeEnd,
}

/// Why a range query stopped before the caller-requested `limit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeEnd {
    /// The cursor ran out of entries: `items` covers everything from `start` onward.
    Exhausted,
    /// The last returned item's key reached or passed the requested `limit`.
    HashLimit,
    /// `response_bytes` was exceeded before `limit` was reached.
    ByteLimit,
}

/// Result of a [`StateRangeProvider`] range query.
pub type RangeResult<T> = ProviderResult<RangeResponse<T>>;

/// Result of a [`StateRangeProvider::storage_range`] query: `None` if the account itself isn't
/// present in the trie at the pinned state root.
pub type StorageRangeResult = ProviderResult<Option<RangeResponse<(B256, U256)>>>;

/// A reusable state range view resolved for a specific state root.
pub type StateRangeView = Box<dyn StateRangeProvider + Send + 'static>;
