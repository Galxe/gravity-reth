pub mod block_view_storage;

use std::sync::Arc;

use alloy_primitives::B256;
use reth_evm::ParallelDatabase;
use reth_storage_api::errors::provider::ProviderError;
use reth_trie::{updates::TrieUpdates, HashedPostState};
use revm::db::BundleState;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GravityStorageError {
    // block number too new
    TooNew(u64),
    StateProviderError((B256, ProviderError)),
}

// 实现错误显示
impl std::fmt::Display for GravityStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GravityStorageError::TooNew(new) => {
                write!(f, "The block number {} is too new", new)
            }
            GravityStorageError::StateProviderError((block_hash, error)) => {
                write!(
                    f,
                    "Failed to get state provider. block_hash={}, error={}",
                    block_hash, error
                )
            }
        }
    }
}

pub trait GravityStorage: Send + Sync + 'static {
    type StateView: ParallelDatabase;

    /// get state view for execute
    fn get_state_view(
        &self,
        block_number: u64,
    ) -> Result<(B256, Self::StateView), GravityStorageError>;

    /// Insert the mapping from block_number to block_id
    fn insert_block_id(&self, block_number: u64, block_id: B256);

    /// Get the block_id by block_number
    fn get_block_id(&self, block_number: u64) -> Option<B256>;

    /// Insert the mapping from block_number to bundle_state
    fn insert_bundle_state(&self, block_number: u64, bundle_state: &BundleState);

    /// Update canonical to block_number and reclaim the intermediate result cache
    fn update_canonical(&self, block_number: u64, block_hash: B256);

    /// calculate state root by block_number
    fn state_root_with_updates(
        &self,
        block_number: u64,
    ) -> Result<(B256, Arc<HashedPostState>, Arc<TrieUpdates>), GravityStorageError>;
}
