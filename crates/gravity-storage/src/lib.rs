//! Storage for pipeline execution

/// Block view for pipeline execution
pub mod block_view_storage;

use alloy_primitives::B256;
use reth_evm::ParallelDatabase;
use reth_provider::ProviderResult;
use reth_trie::{updates::TrieUpdatesV2, HashedPostState};

/// Gravity storage for pipeline execution
pub trait GravityStorage: Send + Sync + 'static {
    /// parallel database to support concurrent read
    type StateView: ParallelDatabase;

    /// get state view for execute
    fn get_state_view(&self) -> ProviderResult<Self::StateView>;

    /// calculate state root
    fn state_root(&self, hashed_state: &HashedPostState) -> ProviderResult<(B256, TrieUpdatesV2)>;

    /// Insert the mapping from `block_number` to `block_id`
    fn insert_block_id(&self, block_number: u64, block_id: B256);

    /// Get the `block_id` by `block_number`
    fn get_block_id(&self, block_number: u64) -> Option<B256>;

    /// Update canonical to `block_number` and reclaim the intermediate result cache
    fn update_canonical(&self, block_number: u64, block_hash: B256);

    /// Get the block timestamp by block number
    ///
    /// Returns the timestamp in seconds, or 0 if the block is not found.
    fn get_block_timestamp(&self, block_number: u64) -> u64;
}
