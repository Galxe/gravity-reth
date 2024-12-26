pub mod block_view_storage;

use std::sync::Arc;

use async_trait::async_trait;
use reth_primitives::B256;
use reth_storage_api::errors::provider::ProviderError;
use reth_trie::updates::TrieUpdates;
use revm::db::BundleState;
use reth_revm::Database;

#[async_trait]
pub trait GravityStorage : Send + Sync {
    async fn get_state_view(&self, block_number: u64) -> (B256, Arc<dyn Database<Error = ProviderError>>);

    async fn commit_state(&mut self, block_id: B256, block_number: u64, bundle_state: BundleState);
    
    async fn update_block_hash(&mut self, block_number: u64, block_hash: B256);

    async fn get_block_hash_by_block_number(&self, block_number:u64) -> B256;

    async fn update_canonical(&mut self, block_number: u64); // gc

    async fn state_root_with_updates(&self, block_number: u64, bundle_state: BundleState) -> (B256, TrieUpdates);
}

