#![allow(unused_variables)]

use alloy_primitives::{Address, BlockNumber, Bytes, StorageKey, StorageValue, B256};
use reth_primitives_traits::{Account, Bytecode};
use reth_storage_api::{
    AccountReader, BlockHashReader, HashedPostStateProvider, StateProofProvider, StateProvider,
    StateProviderBox, StateRootProvider, StorageRootProvider,
};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof,
    MultiProofTargets, StorageMultiProof, StorageProof, TrieInput,
};
use revm_database::BundleState;
use std::sync::atomic::{AtomicBool, Ordering};

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
    {$func_name:ident ($($param_name:ident : & $param_type:ty),*) -> $return_type:ty} => {
        fn $func_name(&self, $($param_name: & $param_type),*) -> ProviderResult<$return_type> {
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
            let _ = self.task_tx.send(paste! {StateProviderTask::[< $func_name:camel >]}($($param_name),* .clone(), tx));
            tokio::task::block_in_place(|| rx.blocking_recv().unwrap())
        }
    };
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
        match self {
            Self::Storage(address, key, tx) => {
                let _ =
                    tx.send(tokio::task::block_in_place(|| state_provider.storage(address, key)));
            }
            Self::BytecodeByHash(code_hash, tx) => {
                let _ = tx.send(tokio::task::block_in_place(|| {
                    state_provider.bytecode_by_hash(&code_hash)
                }));
            }
            Self::BasicAccount(address, tx) => {
                let _ =
                    tx.send(tokio::task::block_in_place(|| state_provider.basic_account(&address)));
            }
            Self::BlockHash(block_number, tx) => {
                let _ = tx
                    .send(tokio::task::block_in_place(|| state_provider.block_hash(block_number)));
            }
        }
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

    provider_fn! {bytecode_by_hash(code_hash: &B256) -> Option<Bytecode>}
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
    provider_fn! {basic_account(address: &Address) -> Option<Account>}
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

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageProof> {
        todo!()
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
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
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        todo!()
    }

    /// Get trie witness for provided state.
    fn witness(&self, input: TrieInput, target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        todo!()
    }
}

impl HashedPostStateProvider for ParallelStateProvider {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        todo!()
    }
}
