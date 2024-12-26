use async_trait::async_trait;
use reth_payload_builder::database::CachedReads;
use reth_primitives::{revm_primitives::Bytecode, Address, B256, U256};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{errors::provider::ProviderError, StateProviderBox, StateProviderFactory};
use revm::{db::BundleState, primitives::AccountInfo, Database, DatabaseRef};
use tokio::{sync::Mutex, time::{sleep, Duration}};
use std::{
    collections::BTreeMap,
    sync::Arc,
};

use crate::GravityStorage;
use tracing::debug;

pub struct BlockViewStorage<Client> {
    inner: Mutex<BlockViewStorageInner<Client>>,
}

pub struct BlockViewStorageInner<Client> {
    client: Client,
    state_provider_info: (B256, u64), // (block_hash, block_number),
    block_number_to_view: BTreeMap<u64, Arc<CachedReads>>,
    block_number_to_hash: BTreeMap<u64, B256>,
    block_number_to_id: BTreeMap<u64, B256>,
}


async fn get_state_provider<Client: StateProviderFactory>(
    client: &Client,
    block_hash: B256,
) -> StateProviderBox {
    loop {
        let state_provider = client.state_by_block_hash(block_hash);
        
        match state_provider {
            Ok(state_provider) => break state_provider,
            Err(ProviderError::BlockHashNotFound(_)) => {
                // if the parent block is not found, we need to wait for it to be available before
                // we can proceed
                debug!(target: "payload_builder",
                    block_hash=%block_hash,
                    "block not found, waiting for it to be available"
                );
                sleep(Duration::from_millis(100)).await;
            }
            // FIXME(nekomoto): handle error
            Err(err) => {
                panic!(
                    "failed to get state provider
                    (block_hash={:?}): {err}",
                    block_hash,
                )
            }
        }
    }
}

impl<Client: StateProviderFactory> BlockViewStorage<Client> {
    fn new(client: Client, block_number: u64, block_hash: B256) -> Self {
        Self {
            inner: Mutex::new(BlockViewStorageInner::new(client, block_number, block_hash)),
        }
    }
}

impl<Client: StateProviderFactory> BlockViewStorageInner<Client> {
    fn new(client: Client, block_number: u64, block_hash: B256) -> Self {
        Self {
            client: client,
            state_provider_info: (block_hash, block_number),
            block_number_to_view: BTreeMap::new(),
            block_number_to_hash: BTreeMap::new(),
            block_number_to_id: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl<Client: StateProviderFactory> GravityStorage for BlockViewStorage<Client> {
    async fn get_state_view(&self, target_block_number: u64) -> (B256, Arc<dyn Database<Error = ProviderError>>) {
        loop {
            {
                let storage = self.inner.lock().await;
                match storage.block_number_to_view.get(&target_block_number) {
                    Some(_) => break,
                    None => {},
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        let storage = self.inner.lock().await;
        let mut block_views = vec![];
        storage.block_number_to_view.iter().rev().for_each(|(block_number, block_view)| {
            let block_number = *block_number;
            if storage.state_provider_info.1 < block_number && block_number <= target_block_number {
                block_views.push(block_view.clone());
            }
        });
        let block_id = *storage.block_number_to_id.get(&target_block_number).unwrap();
        (block_id, Arc::new(BlockViewProvider::new(block_views, get_state_provider(&storage.client, storage.state_provider_info.0).await)))
    }

    async fn commit_state(&mut self, block_id: B256, block_number: u64, bundle_state: BundleState) {
        let mut cached = CachedReads::default();
        for (addr, acc) in bundle_state.state().iter().map(|(a, acc)| (*a, acc)) {
            if let Some(info) = acc.info.clone() {
                // we want pre cache existing accounts and their storage
                // this only includes changed accounts and storage but is better than nothing
                let storage =
                    acc.storage.iter().map(|(key, slot)| (*key, slot.present_value)).collect();
                cached.insert_account(addr, info, storage);
            }
        }
        let mut storage = self.inner.lock().await;
        storage.block_number_to_view.insert(block_number, Arc::new(cached));
        storage.block_number_to_id.insert(block_number, block_id);
    }

    async fn update_block_hash(&mut self, block_number: u64, block_hash: B256) {
        let mut storage = self.inner.lock().await;
        storage.block_number_to_hash.insert(block_number, block_hash);
    }

    async fn get_block_hash_by_block_number(&self, block_number: u64) -> B256 {
        loop {
            {
                let storage = self.inner.lock().await;
                match storage.block_number_to_hash.get(&block_number) {
                    Some(block_hash) => break *block_hash,
                    None => {},
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn update_canonical(&mut self, block_number: u64) {
        let mut storage = self.inner.lock().await;
        assert!(block_number > storage.state_provider_info.1);
        let gc_block_number = storage.state_provider_info.1;
        let block_hash = *storage.block_number_to_hash.get(&block_number).unwrap();
        storage.state_provider_info = (block_hash, block_number);
        storage.block_number_to_view.remove(&gc_block_number);
        storage.block_number_to_hash.remove(&gc_block_number);
        storage.block_number_to_id.remove(&gc_block_number);
    }
}

pub struct BlockViewProvider {
    block_views: Vec<Arc<CachedReads>>,
    db: StateProviderDatabase<StateProviderBox>,
}

impl BlockViewProvider {
    fn new(block_views: Vec<Arc<CachedReads>>, state_provider: StateProviderBox) -> Self {
        Self {
            block_views,
            db: StateProviderDatabase::new(state_provider),
        }
    }
}

impl Database for BlockViewProvider {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        for block_view in &self.block_views {
            if let Some(account) = block_view.accounts.get(&address) {
                return Ok(account.info.clone());
            }
        }
        Ok(self.db.basic_ref(address)?)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        for block_view in &self.block_views {
            if let Some(bytecode) = block_view.contracts.get(&code_hash) {
                return Ok(bytecode.clone());
            }
        }
        Ok(self.db.code_by_hash_ref(code_hash)?)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        for block_view in &self.block_views {
            if let Some(acc_entry) = block_view.accounts.get(&address) {
                if let Some(value) = acc_entry.storage.get(&index) {
                    return Ok(*value);
                }
            }
        }
        Ok(self.db.storage_ref(address, index)?)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        for block_view in &self.block_views {
            if let Some(hash) = block_view.block_hashes.get(&number) {
                return Ok(*hash);
            }
        }
        Ok(self.db.block_hash_ref(number)?)
    }
}
