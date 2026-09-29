//! Helper types to workaround 'higher-ranked lifetime error'
//! <https://github.com/rust-lang/rust/issues/100013> in default implementation of
//! `reth_rpc_eth_api::helpers::Call`.

use alloy_primitives::{Address, B256, U256};
use reth_errors::ProviderResult;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    BytecodeReader, HashedPostStateProvider, StateProvider, StateProviderBox, StateRootProvider,
};
use reth_trie::{HashedStorage, MultiProofTargets};
use revm::database::{BundleState, State};

/// Helper alias type for the state's [`State`]
pub type StateCacheDb = State<StateProviderDatabase<StateProviderTraitObjWrapper>>;

/// Hack to get around 'higher-ranked lifetime error', see
/// <https://github.com/rust-lang/rust/issues/100013>
///
/// Apparently, when dealing with our RPC code, compiler is struggling to prove lifetimes around
/// [`StateProvider`] trait objects. This type is a workaround which should help the compiler to
/// understand that there are no lifetimes involved.
#[expect(missing_debug_implementations)]
pub struct StateProviderTraitObjWrapper(pub StateProviderBox, bool);

impl StateProviderTraitObjWrapper {
    /// Wraps a state provider for RPC execution.
    pub fn new(provider: StateProviderBox) -> Self {
        Self(provider, false)
    }

    /// Uses the V2 root in simulated blocks; their legacy trie updates are never persisted.
    pub const fn for_v2_simulation(mut self) -> Self {
        self.1 = true;
        self
    }
}

impl StateRootProvider for StateProviderTraitObjWrapper {
    fn state_root(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<B256> {
        self.0.state_root(hashed_state)
    }

    fn state_root_v2(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<B256> {
        self.0.state_root_v2(hashed_state)
    }

    fn state_root_from_nodes(
        &self,
        input: reth_trie::TrieInput,
    ) -> reth_errors::ProviderResult<B256> {
        self.0.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<(B256, reth_trie::updates::TrieUpdates)> {
        if self.1 {
            v2_simulation_root_with_updates(&self.0, hashed_state)
        } else {
            self.0.state_root_with_updates(hashed_state)
        }
    }

    fn state_root_with_updates_v2(
        &self,
        hashed_state: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<(B256, reth_trie::updates::TrieUpdatesV2)> {
        self.0.state_root_with_updates_v2(hashed_state)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: reth_trie::TrieInput,
    ) -> reth_errors::ProviderResult<(B256, reth_trie::updates::TrieUpdates)> {
        self.0.state_root_from_nodes_with_updates(input)
    }
}

impl reth_storage_api::StorageRootProvider for StateProviderTraitObjWrapper {
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        self.0.storage_root(address, hashed_storage)
    }

    fn storage_root_v2(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        self.0.storage_root_v2(address, hashed_storage)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        self.0.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageMultiProof> {
        self.0.storage_multiproof(address, slots, hashed_storage)
    }
}

impl reth_storage_api::StateProofProvider for StateProviderTraitObjWrapper {
    fn proof(
        &self,
        input: reth_trie::TrieInput,
        address: Address,
        slots: &[B256],
    ) -> reth_errors::ProviderResult<reth_trie::AccountProof> {
        self.0.proof(input, address, slots)
    }

    fn proof_v2(
        &self,
        input: reth_trie::TrieInput,
        address: Address,
        slots: &[B256],
    ) -> reth_errors::ProviderResult<reth_trie::AccountProof> {
        self.0.proof_v2(input, address, slots)
    }

    fn multiproof(
        &self,
        input: reth_trie::TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<reth_trie::MultiProof> {
        self.0.multiproof(input, targets)
    }

    fn multiproof_v2(
        &self,
        input: reth_trie::TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<reth_trie::MultiProof> {
        self.0.multiproof_v2(input, targets)
    }

    fn witness(
        &self,
        input: reth_trie::TrieInput,
        target: reth_trie::HashedPostState,
    ) -> reth_errors::ProviderResult<Vec<alloy_primitives::Bytes>> {
        self.0.witness(input, target)
    }
}

impl reth_storage_api::AccountReader for StateProviderTraitObjWrapper {
    fn basic_account(
        &self,
        address: &Address,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Account>> {
        self.0.basic_account(address)
    }
}

impl reth_storage_api::BlockHashReader for StateProviderTraitObjWrapper {
    fn block_hash(
        &self,
        block_number: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Option<B256>> {
        self.0.block_hash(block_number)
    }

    fn convert_block_hash(
        &self,
        hash_or_number: alloy_rpc_types_eth::BlockHashOrNumber,
    ) -> reth_errors::ProviderResult<Option<B256>> {
        self.0.convert_block_hash(hash_or_number)
    }

    fn canonical_hashes_range(
        &self,
        start: alloy_primitives::BlockNumber,
        end: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Vec<B256>> {
        self.0.canonical_hashes_range(start, end)
    }
}

impl HashedPostStateProvider for StateProviderTraitObjWrapper {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> reth_trie::HashedPostState {
        self.0.hashed_post_state(bundle_state)
    }
}

impl StateProvider for StateProviderTraitObjWrapper {
    fn storage(
        &self,
        account: Address,
        storage_key: alloy_primitives::StorageKey,
    ) -> reth_errors::ProviderResult<Option<alloy_primitives::StorageValue>> {
        self.0.storage(account, storage_key)
    }

    fn account_code(
        &self,
        addr: &Address,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        self.0.account_code(addr)
    }

    fn account_balance(&self, addr: &Address) -> reth_errors::ProviderResult<Option<U256>> {
        self.0.account_balance(addr)
    }

    fn account_nonce(&self, addr: &Address) -> reth_errors::ProviderResult<Option<u64>> {
        self.0.account_nonce(addr)
    }
}

impl BytecodeReader for StateProviderTraitObjWrapper {
    fn bytecode_by_hash(
        &self,
        code_hash: &B256,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        self.0.bytecode_by_hash(code_hash)
    }
}

fn v2_simulation_root_with_updates(
    provider: &impl StateRootProvider,
    hashed_state: reth_trie::HashedPostState,
) -> ProviderResult<(B256, reth_trie::updates::TrieUpdates)> {
    provider.state_root_v2(hashed_state).map(|root| (root, Default::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_errors::ProviderError;
    use reth_storage_api::noop::NoopProvider;
    use reth_trie::{updates::TrieUpdates, HashedPostState, TrieInput};

    struct DistinctRoots(B256);

    impl StateRootProvider for DistinctRoots {
        fn state_root(&self, _state: HashedPostState) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }

        fn state_root_v2(&self, _state: HashedPostState) -> ProviderResult<B256> {
            Ok(self.0)
        }

        fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }

        fn state_root_with_updates(
            &self,
            _state: HashedPostState,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            Ok((B256::ZERO, TrieUpdates::default()))
        }

        fn state_root_from_nodes_with_updates(
            &self,
            _input: TrieInput,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            Ok((B256::ZERO, TrieUpdates::default()))
        }
    }

    #[test]
    fn simulation_root_uses_v2_without_persisted_updates() {
        let expected = B256::repeat_byte(0x42);
        let provider = DistinctRoots(expected);
        let (root, updates) =
            v2_simulation_root_with_updates(&provider, HashedPostState::default()).unwrap();
        assert_eq!(root, expected);
        assert!(updates.is_empty());

        let legacy = StateProviderTraitObjWrapper::new(Box::new(NoopProvider::default()));
        assert!(legacy.state_root_with_updates(HashedPostState::default()).is_ok());
        let simulation = legacy.for_v2_simulation();
        assert!(matches!(
            simulation.state_root_with_updates(HashedPostState::default()),
            Err(ProviderError::UnsupportedProvider)
        ));
    }
}
