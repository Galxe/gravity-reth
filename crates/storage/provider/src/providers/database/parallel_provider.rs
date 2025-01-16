use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use alloy_primitives::{Address, BlockNumber, Bytes, B256};
use reth_primitives::{Account, Bytecode, StorageKey, StorageValue};
use reth_storage_api::{
    AccountReader, BlockHashReader, StateProofProvider, StateProvider, StateProviderBox,
    StateRootProvider, StorageRootProvider,
};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof, TrieInput,
};

use flume as mpmc;
use paste::paste;
use tokio::sync::oneshot;

enum StateProviderTask {
    Storage(Address, StorageKey, oneshot::Sender<ProviderResult<Option<StorageValue>>>),
    BytecodeByHash(B256, oneshot::Sender<ProviderResult<Option<Bytecode>>>),
    BasicAccount(Address, oneshot::Sender<ProviderResult<Option<Account>>>),
    BlockHash(u64, oneshot::Sender<ProviderResult<Option<B256>>>),
}

macro_rules! provider_fn {
    {$func_name:ident ($($param_name:ident : $param_type:ty),*) -> $return_type:ty} => {
        fn $func_name(&self, $($param_name: $param_type),*) -> ProviderResult<$return_type> {
            if self
                .provider_busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let result = self.provider.$func_name($($param_name),*);
                self.provider_busy.store(false, Ordering::Release);
                return result;
            }

            let (tx, rx) = oneshot::channel();
            let _ = self.task_tx.send(paste! {StateProviderTask::[< $func_name:camel >]}($($param_name),* , tx));
            tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
        }
    };
}

impl StateProviderTask {
    fn process(self, state_provider: &dyn StateProvider) {
        macro_rules! match_task {
            {$($task: ident ($($param:ident),*)), *} => {
                paste! {
                    match self {
                        $(Self::$task($($param),*, tx) => {
                            let _ = tx.send(tokio::task::block_in_place(|| state_provider.[< $task:snake >]($($param),*)));
                        }),*
                    }
                }
            };
        }

        match_task! {Storage(address, key), BytecodeByHash(code_hash), BasicAccount(address), BlockHash(block_number)}
    }
}

pub(super) struct ParallelStateProvider {
    task_tx: mpmc::Sender<StateProviderTask>,
    provider: Box<dyn StateProvider>,
    provider_busy: AtomicBool,
}

impl ParallelStateProvider {
    pub(super) fn try_new<F>(provider_factory: F, parallel: usize) -> ProviderResult<Self>
    where
        F: Fn() -> ProviderResult<StateProviderBox>,
    {
        assert!(parallel > 1, "parallel must be greater than 1");

        let (task_tx, task_rx) = mpmc::unbounded::<StateProviderTask>();

        let provider = provider_factory()?;

        for _ in 0..parallel - 1 {
            let state_provider = provider_factory()?;
            let task_rx = task_rx.clone();
            // TODO: use individual tokio runtime
            tokio::spawn(async move {
                while let Ok(task) = task_rx.recv_async().await {
                    task.process(state_provider.as_ref());
                }
            });
        }

        Ok(Self { task_tx, provider, provider_busy: AtomicBool::new(false) })
    }
}

impl StateProvider for ParallelStateProvider {
    provider_fn! {storage(address: Address, key: StorageKey) -> Option<StorageValue>}

    provider_fn! {bytecode_by_hash(code_hash: B256) -> Option<Bytecode>}
}

#[allow(unused)]
impl BlockHashReader for ParallelStateProvider {
    provider_fn! {block_hash(block_number: u64) -> Option<B256>}

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        todo!()
    }
}

impl AccountReader for ParallelStateProvider {
    provider_fn! {basic_account(address: Address) -> Option<Account>}
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
