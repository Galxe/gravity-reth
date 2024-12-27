pub mod block_view_storage;

use std::sync::Arc;

use async_trait::async_trait;
use reth_primitives::B256;
use reth_revm::DatabaseRef;
use reth_storage_api::errors::provider::ProviderError;
use reth_trie::updates::TrieUpdates;
use revm::db::BundleState;

#[async_trait]
pub trait GravityStorage: Send + Sync + 'static {
    async fn get_state_view(
        &self,
        block_number: u64,
    ) -> (B256, Arc<dyn DatabaseRef<Error = ProviderError>>);

    async fn commit_state(&self, block_id: B256, block_number: u64, bundle_state: &BundleState);

    async fn insert_block_hash(&self, block_number: u64, block_hash: B256);

    async fn block_hash_by_number(&self, block_number: u64) -> B256;

    async fn update_canonical(&self, block_number: u64); // gc

    async fn state_root_with_updates(
        &self,
        block_number: u64,
        bundle_state: &BundleState,
    ) -> (B256, TrieUpdates);
}
