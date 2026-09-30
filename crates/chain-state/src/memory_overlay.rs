use super::ExecutedBlockWithTrieUpdates;
use alloy_consensus::BlockHeader;
use alloy_primitives::{keccak256, Address, BlockNumber, Bytes, StorageKey, StorageValue, B256};
use reth_errors::{ProviderError, ProviderResult};
use reth_primitives_traits::{Account, Bytecode, NodePrimitives};
use reth_storage_api::{
    AccountReader, BlockHashReader, BytecodeReader, HashedPostStateProvider, StateProofProvider,
    StateProvider, StateRootProvider, StorageRootProvider,
};
use reth_trie::{
    updates::{TrieUpdates, TrieUpdatesV2},
    AccountProof, HashedPostState, HashedStorage, MultiProof, MultiProofTargets, StorageMultiProof,
    TrieInput,
};
use revm_database::BundleState;
use std::sync::OnceLock;

/// A state provider that stores references to in-memory blocks along with their state as well as a
/// reference of the historical state provider for fallback lookups.
#[expect(missing_debug_implementations)]
pub struct MemoryOverlayStateProviderRef<
    'a,
    N: NodePrimitives = reth_ethereum_primitives::EthPrimitives,
> {
    /// Historical state provider for state lookups that are not found in memory blocks.
    pub(crate) historical: Box<dyn StateProvider + 'a>,
    /// The collection of executed parent blocks. Expected order is newest to oldest.
    pub(crate) in_memory: Vec<ExecutedBlockWithTrieUpdates<N>>,
    /// V2 calls reuse the same ordered in-memory overlay for this provider.
    hashed_state_v2: OnceLock<HashedPostState>,
    /// Locally built pending blocks do not have a computed state root in their header.
    verify_proof_header_root: bool,
}

/// A state provider that stores references to in-memory blocks along with their state as well as
/// the historical state provider for fallback lookups.
pub type MemoryOverlayStateProvider<N> = MemoryOverlayStateProviderRef<'static, N>;

impl<'a, N: NodePrimitives> MemoryOverlayStateProviderRef<'a, N> {
    /// Create new memory overlay state provider.
    ///
    /// ## Arguments
    ///
    /// - `in_memory` - the collection of executed ancestor blocks in reverse.
    /// - `historical` - a historical state provider for the latest ancestor block stored in the
    ///   database.
    pub fn new(
        historical: Box<dyn StateProvider + 'a>,
        in_memory: Vec<ExecutedBlockWithTrieUpdates<N>>,
    ) -> Self {
        Self {
            historical,
            in_memory,
            hashed_state_v2: OnceLock::new(),
            verify_proof_header_root: true,
        }
    }

    /// Skip header-root verification for a locally built pending block without a computed root.
    pub const fn without_proof_header_root_check(mut self) -> Self {
        self.verify_proof_header_root = false;
        self
    }

    /// Turn this state provider into a state provider
    pub fn boxed(self) -> Box<dyn StateProvider + 'a> {
        Box::new(self)
    }

    fn hashed_state_v2(&self) -> &HashedPostState {
        self.hashed_state_v2.get_or_init(|| {
            let mut state = HashedPostState::default();
            for block in self.in_memory.iter().rev() {
                state.extend_ref(block.hashed_state.as_ref());
            }
            state
        })
    }
}

impl<N: NodePrimitives> BlockHashReader for MemoryOverlayStateProviderRef<'_, N> {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        for block in &self.in_memory {
            if block.recovered_block().number() == number {
                return Ok(Some(block.recovered_block().hash()));
            }
        }

        self.historical.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        let range = start..end;
        let mut earliest_block_number = None;
        let mut in_memory_hashes = Vec::with_capacity(range.size_hint().0);

        // iterate in ascending order (oldest to newest = low to high)
        for block in &self.in_memory {
            let block_num = block.recovered_block().number();
            if range.contains(&block_num) {
                in_memory_hashes.push(block.recovered_block().hash());
                earliest_block_number = Some(block_num);
            }
        }

        // `self.in_memory` stores executed blocks in ascending order (oldest to newest).
        // However, `in_memory_hashes` should be constructed in descending order (newest to oldest),
        // so we reverse the vector after collecting the hashes.
        in_memory_hashes.reverse();

        let mut hashes =
            self.historical.canonical_hashes_range(start, earliest_block_number.unwrap_or(end))?;
        hashes.append(&mut in_memory_hashes);
        Ok(hashes)
    }
}

impl<N: NodePrimitives> AccountReader for MemoryOverlayStateProviderRef<'_, N> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        for block in &self.in_memory {
            if let Some(account) = block.execution_output.account(address) {
                return Ok(account);
            }
        }

        self.historical.basic_account(address)
    }
}

impl<N: NodePrimitives> StateRootProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        let mut merged = self.hashed_state_v2().clone();
        merged.extend(state);
        self.historical.state_root(merged)
    }

    fn state_root_with_updates_v2(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdatesV2)> {
        let mut merged = self.hashed_state_v2().clone();
        merged.extend(state);
        self.historical.state_root_with_updates_v2(merged)
    }

    fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn state_root_with_updates(
        &self,
        _state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        _input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives> StorageRootProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        let mut merged =
            self.hashed_state_v2().storages.get(&keccak256(address)).cloned().unwrap_or_default();
        merged.extend(&storage);
        self.historical.storage_root(address, merged)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        let mut hashed_storage =
            self.hashed_state_v2().storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed_storage.extend(&storage);
        self.historical.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        let mut hashed_storage =
            self.hashed_state_v2().storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed_storage.extend(&storage);
        self.historical.storage_multiproof(address, slots, hashed_storage)
    }
}

impl<N: NodePrimitives> StateProofProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        if !input.nodes.is_empty() {
            return Err(ProviderError::UnsupportedProvider)
        }
        let verify_header_root = self.verify_proof_header_root && input.state.is_empty();
        let mut merged = self.hashed_state_v2().clone();
        merged.extend(input.state);
        let proof = self.historical.proof(TrieInput::from_state(merged), address, slots)?;
        if verify_header_root && let Some(target) = self.in_memory.first() {
            proof
                .verify(target.recovered_block().header().state_root())
                .map_err(ProviderError::other)?;
        }
        Ok(proof)
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        if !input.nodes.is_empty() {
            return Err(ProviderError::UnsupportedProvider)
        }
        let mut merged = self.hashed_state_v2().clone();
        merged.extend(input.state);
        self.historical.multiproof(TrieInput::from_state(merged), targets)
    }

    fn witness(&self, _input: TrieInput, _target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives> HashedPostStateProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.historical.hashed_post_state(bundle_state)
    }
}

impl<N: NodePrimitives> StateProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn storage(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        for block in &self.in_memory {
            if let Some(value) = block.execution_output.storage(&address, storage_key.into()) {
                return Ok(Some(value));
            }
        }

        self.historical.storage(address, storage_key)
    }
}

impl<N: NodePrimitives> BytecodeReader for MemoryOverlayStateProviderRef<'_, N> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        for block in &self.in_memory {
            if let Some(contract) = block.execution_output.bytecode(code_hash) {
                return Ok(Some(contract));
            }
        }

        self.historical.bytecode_by_hash(code_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestBlockBuilder;
    use alloy_primitives::U256;
    use reth_trie::{StorageProof, EMPTY_ROOT_HASH};
    use std::sync::Arc;

    struct AssertingHistoricalProvider {
        expected: HashedPostState,
    }

    impl StateProvider for AssertingHistoricalProvider {
        fn storage(
            &self,
            _address: Address,
            _storage_key: StorageKey,
        ) -> ProviderResult<Option<StorageValue>> {
            unreachable!()
        }
    }

    impl BytecodeReader for AssertingHistoricalProvider {
        fn bytecode_by_hash(&self, _code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
            unreachable!()
        }
    }

    impl BlockHashReader for AssertingHistoricalProvider {
        fn block_hash(&self, _number: BlockNumber) -> ProviderResult<Option<B256>> {
            unreachable!()
        }

        fn canonical_hashes_range(
            &self,
            _start: BlockNumber,
            _end: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            unreachable!()
        }
    }

    impl AccountReader for AssertingHistoricalProvider {
        fn basic_account(&self, _address: &Address) -> ProviderResult<Option<Account>> {
            unreachable!()
        }
    }

    impl StateRootProvider for AssertingHistoricalProvider {
        fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
            assert_eq!(state, self.expected);
            Ok(B256::repeat_byte(0xab))
        }

        fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
            unreachable!()
        }

        fn state_root_with_updates(
            &self,
            _state: HashedPostState,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            unreachable!()
        }

        fn state_root_from_nodes_with_updates(
            &self,
            _input: TrieInput,
        ) -> ProviderResult<(B256, TrieUpdates)> {
            unreachable!()
        }
    }

    impl StorageRootProvider for AssertingHistoricalProvider {
        fn storage_root(&self, _address: Address, _storage: HashedStorage) -> ProviderResult<B256> {
            unreachable!()
        }

        fn storage_proof(
            &self,
            address: Address,
            slot: B256,
            storage: HashedStorage,
        ) -> ProviderResult<StorageProof> {
            assert_eq!(&storage, &self.expected.storages[&keccak256(address)]);
            Ok(StorageProof::new(slot))
        }

        fn storage_multiproof(
            &self,
            address: Address,
            _slots: &[B256],
            storage: HashedStorage,
        ) -> ProviderResult<StorageMultiProof> {
            assert_eq!(&storage, &self.expected.storages[&keccak256(address)]);
            Ok(StorageMultiProof::empty())
        }
    }

    impl StateProofProvider for AssertingHistoricalProvider {
        fn proof(
            &self,
            input: TrieInput,
            address: Address,
            _slots: &[B256],
        ) -> ProviderResult<AccountProof> {
            assert_eq!(input.state, self.expected);
            Ok(AccountProof::new(address))
        }

        fn multiproof(
            &self,
            _input: TrieInput,
            _targets: MultiProofTargets,
        ) -> ProviderResult<MultiProof> {
            unreachable!()
        }

        fn witness(
            &self,
            _input: TrieInput,
            _target: HashedPostState,
        ) -> ProviderResult<Vec<Bytes>> {
            unreachable!()
        }
    }

    impl HashedPostStateProvider for AssertingHistoricalProvider {
        fn hashed_post_state(&self, _bundle_state: &BundleState) -> HashedPostState {
            unreachable!()
        }
    }

    #[test]
    fn v2_root_and_proof_merge_memory_blocks_before_request_overlay() {
        let address = Address::repeat_byte(0x11);
        let other_address = Address::repeat_byte(0x22);
        let hashed_address = keccak256(address);
        let old_slot = B256::repeat_byte(0x33);
        let new_slot = B256::repeat_byte(0x44);

        let mut oldest = HashedPostState::default();
        oldest.accounts.insert(hashed_address, Some(Account { nonce: 1, ..Default::default() }));
        oldest
            .accounts
            .insert(keccak256(other_address), Some(Account { nonce: 10, ..Default::default() }));
        oldest.storages.insert(
            hashed_address,
            HashedStorage::from_iter(false, [(old_slot, U256::from(1)), (new_slot, U256::from(2))]),
        );

        let mut newest = HashedPostState::default();
        newest.accounts.insert(hashed_address, Some(Account { nonce: 2, ..Default::default() }));
        newest
            .storages
            .insert(hashed_address, HashedStorage::from_iter(false, [(new_slot, U256::from(3))]));

        let mut request = HashedPostState::default();
        request.accounts.insert(hashed_address, Some(Account { nonce: 3, ..Default::default() }));
        request
            .storages
            .insert(hashed_address, HashedStorage::from_iter(false, [(old_slot, U256::from(4))]));

        let mut expected = oldest.clone();
        expected.extend(newest.clone());
        expected.extend(request.clone());
        let request_storage = request.storages[&hashed_address].clone();

        let mut builder: TestBlockBuilder<reth_ethereum_primitives::EthPrimitives> =
            TestBlockBuilder::default();
        let mut older_block = builder.get_executed_block_with_number(1, B256::ZERO);
        older_block.hashed_state = Arc::new(oldest);
        let mut newer_block =
            builder.get_executed_block_with_number(2, older_block.recovered_block().hash());
        newer_block.hashed_state = Arc::new(newest);

        let provider = MemoryOverlayStateProviderRef::new(
            Box::new(AssertingHistoricalProvider { expected }),
            vec![newer_block, older_block],
        );
        assert_eq!(provider.state_root(request.clone()).unwrap(), B256::repeat_byte(0xab));
        assert_eq!(
            provider.proof(TrieInput::from_state(request), address, &[old_slot]).unwrap().address,
            address
        );
        assert_eq!(
            provider.storage_proof(address, old_slot, request_storage.clone()).unwrap().key,
            old_slot
        );
        assert_eq!(
            provider.storage_multiproof(address, &[old_slot], request_storage).unwrap().root,
            EMPTY_ROOT_HASH
        );
        assert!(matches!(
            provider.state_root_with_updates(HashedPostState::default()),
            Err(ProviderError::UnsupportedProvider)
        ));
    }

    #[test]
    fn v2_memory_proof_checks_header_unless_pending_or_overlaid() {
        let address = Address::repeat_byte(0x11);
        let mut builder: TestBlockBuilder<reth_ethereum_primitives::EthPrimitives> =
            TestBlockBuilder::default();
        let block = builder.get_executed_block_with_number(1, B256::ZERO);
        assert_ne!(block.recovered_block().header().state_root(), EMPTY_ROOT_HASH);
        AccountProof::new(address).verify(EMPTY_ROOT_HASH).unwrap();

        let provider = MemoryOverlayStateProviderRef::new(
            Box::new(AssertingHistoricalProvider { expected: HashedPostState::default() }),
            vec![block.clone()],
        );
        assert!(provider.proof(TrieInput::default(), address, &[]).is_err());
        assert_eq!(
            provider
                .without_proof_header_root_check()
                .proof(TrieInput::default(), address, &[])
                .unwrap()
                .address,
            address
        );

        let mut overlay = HashedPostState::default();
        overlay.accounts.insert(keccak256(address), Some(Account::default()));
        let provider = MemoryOverlayStateProviderRef::new(
            Box::new(AssertingHistoricalProvider { expected: overlay.clone() }),
            vec![block],
        );
        assert_eq!(
            provider.proof(TrieInput::from_state(overlay), address, &[]).unwrap().address,
            address
        );

        let mut legacy_input = TrieInput::default();
        legacy_input.nodes.removed_nodes.insert(Default::default());
        assert!(matches!(
            provider.proof(legacy_input.clone(), address, &[]),
            Err(ProviderError::UnsupportedProvider)
        ));
        assert!(matches!(
            provider.multiproof(legacy_input, MultiProofTargets::account(keccak256(address))),
            Err(ProviderError::UnsupportedProvider)
        ));
    }
}
