pub mod block_view_storage;

use alloy_primitives::B256;
use reth_evm::ParallelDatabase;
use reth_provider::{ProviderResult, StateProviderOptions};
use reth_trie::{
    updates::{TrieUpdates, TrieUpdatesV2},
    HashedPostState,
};

pub trait GravityStorage: Send + Sync + 'static {
    type StateView: ParallelDatabase;

    /// get state view for execute
    fn get_state_view(&self, opts: StateProviderOptions) -> ProviderResult<Self::StateView>;

    /// calculate state root
    fn state_root(
        &self,
        hashed_state: &HashedPostState,
        compatible: bool,
    ) -> ProviderResult<(B256, TrieUpdatesV2, Option<TrieUpdates>)>;

    /// Insert the mapping from block_number to block_id
    fn insert_block_id(&self, block_number: u64, block_id: B256);

    /// Get the block_id by block_number
    fn get_block_id(&self, block_number: u64) -> Option<B256>;

    /// Update canonical to block_number and reclaim the intermediate result cache
    fn update_canonical(&self, block_number: u64, block_hash: B256);
}
