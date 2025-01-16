use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::{Address, BlockNumber, Bytes, B256};
use reth_db::Database;
use reth_primitives::{Account, Bytecode, StorageKey, StorageValue};
use reth_storage_api::{
    AccountReader, BlockHashReader, StateProofProvider, StateProvider, StateRootProvider,
    StorageRootProvider, TryIntoHistoricalStateProvider,
};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof, TrieInput,
};

use crate::{providers::ProviderNodeTypes, LatestStateProvider, StaticFileProviderFactory};

use super::ProviderFactory;
use flume as mpmc;
use tokio::sync::oneshot;

enum StateProviderTask {
    Storage(Address, StorageKey, oneshot::Sender<ProviderResult<Option<StorageValue>>>),
    BytecodeByHash(B256, oneshot::Sender<ProviderResult<Option<Bytecode>>>),
    BasicAccount(Address, oneshot::Sender<ProviderResult<Option<Account>>>),
    BlockHash(u64, oneshot::Sender<ProviderResult<Option<B256>>>),
}

impl StateProviderTask {
    fn process(self, state_provider: &dyn StateProvider) {
        match self {
            Self::Storage(address, key, tx) => {
                let result = tokio::task::block_in_place(|| state_provider.storage(address, key));
                let _ = tx.send(result);
            }
            Self::BytecodeByHash(code_hash, tx) => {
                let result =
                    tokio::task::block_in_place(|| state_provider.bytecode_by_hash(code_hash));
                let _ = tx.send(result);
            }
            Self::BasicAccount(address, tx) => {
                let result = tokio::task::block_in_place(|| state_provider.basic_account(address));
                let _ = tx.send(result);
            }
            Self::BlockHash(block_number, tx) => {
                let result =
                    tokio::task::block_in_place(|| state_provider.block_hash(block_number));
                let _ = tx.send(result);
            }
        }
    }
}

pub(super) struct ParallelStateProvider {
    task_tx: mpmc::Sender<StateProviderTask>,
}

impl ParallelStateProvider {
    pub(super) fn try_new<N>(
        db: &ProviderFactory<N>,
        block_number: u64,
        parallel: usize,
    ) -> ProviderResult<Self>
    where
        N: ProviderNodeTypes,
    {
        assert!(parallel > 1, "parallel must be greater than 1");

        let (task_tx, task_rx) = mpmc::unbounded::<StateProviderTask>();

        for _ in 0..parallel {
            let state_provider = db.provider()?.try_into_history_at_block(block_number)?;
            let task_rx = task_rx.clone();
            // TODO: use individual tokio runtime
            tokio::spawn(async move {
                while let Ok(task) = task_rx.recv_async().await {
                    task.process(state_provider.as_ref());
                }
            });
        }

        Ok(Self { task_tx })
    }

    pub(super) fn try_new_latest<N>(
        db: &ProviderFactory<N>,
        parallel: usize,
    ) -> ProviderResult<Self>
    where
        N: ProviderNodeTypes,
    {
        assert!(parallel > 1, "parallel must be greater than 1");

        let (task_tx, task_rx) = mpmc::unbounded::<StateProviderTask>();

        for _ in 0..parallel {
            let state_provider =
                Box::new(LatestStateProvider::new(db.db_ref().tx()?, db.static_file_provider()));
            let task_rx = task_rx.clone();
            // TODO: use individual tokio runtime
            tokio::spawn(async move {
                while let Ok(task) = task_rx.recv_async().await {
                    task.process(state_provider.as_ref());
                }
            });
        }

        Ok(Self { task_tx })
    }
}

impl StateProvider for ParallelStateProvider {
    fn storage(&self, address: Address, key: StorageKey) -> ProviderResult<Option<StorageValue>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.task_tx.send(StateProviderTask::Storage(address, key, tx));
        tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
    }

    fn bytecode_by_hash(&self, code_hash: B256) -> ProviderResult<Option<Bytecode>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.task_tx.send(StateProviderTask::BytecodeByHash(code_hash, tx));
        tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
    }
}

#[allow(unused)]
impl BlockHashReader for ParallelStateProvider {
    fn block_hash(&self, block_number: u64) -> ProviderResult<Option<B256>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.task_tx.send(StateProviderTask::BlockHash(block_number, tx));
        tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        todo!()
    }
}

impl AccountReader for ParallelStateProvider {
    fn basic_account(&self, address: Address) -> ProviderResult<Option<Account>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.task_tx.send(StateProviderTask::BasicAccount(address, tx));
        tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
    }
}

#[allow(unused)]
impl StateRootProvider for ParallelStateProvider {
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256> {
        todo!()
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        todo!()
    }

    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        todo!()
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        todo!()
    }

    fn state_root_with_updates_v2(
        &self,
        state: HashedPostState,
        hashed_state_vec: Vec<Arc<HashedPostState>>,
        trie_updates_vec: Vec<Arc<TrieUpdates>>,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        todo!()
    }
}

#[allow(unused)]
impl StorageRootProvider for ParallelStateProvider {
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        todo!()
    }
}

#[allow(unused)]
impl StateProofProvider for ParallelStateProvider {
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        todo!()
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: HashMap<B256, HashSet<B256>>,
    ) -> ProviderResult<MultiProof> {
        todo!()
    }

    fn witness(
        &self,
        input: TrieInput,
        target: HashedPostState,
    ) -> ProviderResult<HashMap<B256, Bytes>> {
        todo!()
    }
}
