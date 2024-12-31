pub mod block_view_storage;

use std::sync::Arc;

use async_trait::async_trait;
use reth_primitives::B256;
use reth_revm::DatabaseRef;
use reth_storage_api::errors::provider::ProviderError;
use reth_trie::updates::TrieUpdates;
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

#[async_trait]
pub trait GravityStorage: Send + Sync + 'static {
    type StateView: DatabaseRef<Error = ProviderError>;

    async fn get_state_view(
        &self,
        block_number: u64,
    ) -> Result<(B256, Self::StateView), GravityStorageError>;

    async fn commit_state(&self, block_id: B256, block_number: u64, bundle_state: &BundleState);

    async fn insert_block_hash(&self, block_number: u64, block_hash: B256);

    async fn block_hash_by_number(&self, block_number: u64) -> Result<B256, GravityStorageError>;

    async fn update_canonical(&self, block_number: u64); // gc

    async fn state_root_with_updates(
        &self,
        block_number: u64,
        bundle_state: &BundleState,
    ) -> Result<(B256, TrieUpdates), GravityStorageError>;
}
