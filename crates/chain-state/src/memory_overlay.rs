use super::ExecutedBlockWithTrieUpdates;
use alloy_consensus::BlockHeader;
use alloy_primitives::{
    keccak256, map::HashMap, Address, BlockNumber, Bytes, StorageKey, StorageValue, B256,
};
use reth_errors::ProviderResult;
use reth_primitives_traits::{Account, Bytecode, NodePrimitives};
use reth_storage_api::{
    AccountReader, BlockHashReader, BlockNumberToBlockIdReader, BytecodeReader,
    HashedPostStateProvider, StateProofProvider, StateProvider, StateRootProvider,
    StorageRootProvider,
};
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof,
    MultiProofTargets, StorageMultiProof, TrieInput,
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
    /// Frozen snapshot of `(block_number, block_id)` for blocks that are canonical
    /// in-memory but not yet persisted to `tables::BlockNumberToBlockId`.
    ///
    /// Populated by [`Self::with_block_ids`] from
    /// `CanonicalInMemoryState::snapshot_block_ids_for`. Used by
    /// `BlockNumberToBlockIdReader::block_id_by_number` to close the race window
    /// between `MakeCanonical` and `advance_persistence`.
    ///
    /// **An empty map re-opens that race window.** `Self::new` leaves this empty so
    /// existing call sites stay source-compatible, but callers in any path where EVM
    /// `BLOCKHASH(n)` may target an in-memory ancestor MUST chain
    /// [`Self::with_block_ids`] with a snapshot from `CanonicalInMemoryState`.
    /// `HashMap` (rather than `BTreeMap`) is used here because lookups are point-only
    /// for the overlay's lifetime — no range eviction is needed in this short-lived
    /// snapshot view.
    pub(crate) block_ids: HashMap<BlockNumber, B256>,
    /// Lazy-loaded in-memory trie data.
    pub(crate) trie_input: OnceLock<TrieInput>,
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
    ///
    /// The `block_ids` overlay is empty; callers that need to close the in-memory race
    /// window (see [`Self::with_block_ids`]) should chain the builder method.
    pub fn new(
        historical: Box<dyn StateProvider + 'a>,
        in_memory: Vec<ExecutedBlockWithTrieUpdates<N>>,
    ) -> Self {
        Self { historical, in_memory, block_ids: HashMap::default(), trie_input: OnceLock::new() }
    }

    /// Attach a frozen `(block_number, block_id)` snapshot to this overlay.
    ///
    /// Callers should pass the result of
    /// `CanonicalInMemoryState::snapshot_block_ids_for(...)` covering the in-memory
    /// window so that `block_id_by_number` can serve EVM `BLOCKHASH(n)` for blocks
    /// that are canonical in-memory but not yet persisted to
    /// `tables::BlockNumberToBlockId`. The snapshot is frozen at construction time;
    /// the overlay's lifetime is one `eth_call` / trace invocation, so we
    /// deliberately don't hold a lock or Arc back to `CanonicalInMemoryState`.
    pub fn with_block_ids(mut self, block_ids: HashMap<BlockNumber, B256>) -> Self {
        self.block_ids = block_ids;
        self
    }

    /// Turn this state provider into a state provider
    pub fn boxed(self) -> Box<dyn StateProvider + 'a> {
        Box::new(self)
    }

    /// Return lazy-loaded trie state aggregated from in-memory blocks.
    fn trie_input(&self) -> &TrieInput {
        self.trie_input.get_or_init(|| {
            TrieInput::from_blocks(
                self.in_memory
                    .iter()
                    .rev()
                    .map(|block| (block.hashed_state.as_ref(), block.trie.as_ref())),
            )
        })
    }
}

impl<N: NodePrimitives> BlockNumberToBlockIdReader for MemoryOverlayStateProviderRef<'_, N> {
    fn block_id_by_number(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        // In-memory window first: when the overlay was built with a `block_ids`
        // snapshot, blocks that are canonical in-memory but not yet persisted
        // resolve here. Note this is the only place where EVM BLOCKHASH semantics
        // diverge from `BlockHashReader::block_hash`: `block_hash` continues to
        // return the keccak header hash from in_memory sealed blocks, while
        // `block_id_by_number` returns the Aptos consensus `block_id`.
        if let Some(id) = self.block_ids.get(&number) {
            return Ok(Some(*id));
        }
        // Fall back to historical (`tables::BlockNumberToBlockId`).
        self.historical.block_id_by_number(number)
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
        self.state_root_from_nodes(TrieInput::from_state(state))
    }

    fn state_root_from_nodes(&self, mut input: TrieInput) -> ProviderResult<B256> {
        input.prepend_self(self.trie_input().clone());
        self.historical.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.state_root_from_nodes_with_updates(TrieInput::from_state(state))
    }

    fn state_root_from_nodes_with_updates(
        &self,
        mut input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        input.prepend_self(self.trie_input().clone());
        self.historical.state_root_from_nodes_with_updates(input)
    }
}

impl<N: NodePrimitives> StorageRootProvider for MemoryOverlayStateProviderRef<'_, N> {
    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        let state = &self.trie_input().state;
        let mut hashed_storage =
            state.storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed_storage.extend(&storage);
        self.historical.storage_root(address, hashed_storage)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        let state = &self.trie_input().state;
        let mut hashed_storage =
            state.storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed_storage.extend(&storage);
        self.historical.storage_proof(address, slot, hashed_storage)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        let state = &self.trie_input().state;
        let mut hashed_storage =
            state.storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed_storage.extend(&storage);
        self.historical.storage_multiproof(address, slots, hashed_storage)
    }
}

impl<N: NodePrimitives> StateProofProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn proof(
        &self,
        mut input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        input.prepend_self(self.trie_input().clone());
        self.historical.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        mut input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        input.prepend_self(self.trie_input().clone());
        self.historical.multiproof(input, targets)
    }

    fn witness(&self, mut input: TrieInput, target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        input.prepend_self(self.trie_input().clone());
        self.historical.witness(input, target)
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
    use alloy_primitives::{StorageValue, U256};
    use reth_ethereum_primitives::EthPrimitives;
    use reth_primitives_traits::{Account, Bytecode};
    use reth_trie::{
        updates::TrieUpdates, AccountProof, HashedStorage, MultiProof, MultiProofTargets,
        StorageMultiProof, StorageProof, TrieInput,
    };

    /// Historical mock with a parameterizable `block_id_by_number` response.
    ///
    /// Everything else returns `None`/default — only the methods exercised by the
    /// overlay's `block_id_by_number` and `block_hash` paths matter for these tests.
    struct HistoricalMock {
        block_ids: HashMap<BlockNumber, B256>,
    }

    impl HistoricalMock {
        fn new() -> Self {
            Self { block_ids: HashMap::default() }
        }

        fn with_block_id(mut self, number: BlockNumber, id: B256) -> Self {
            self.block_ids.insert(number, id);
            self
        }
    }

    impl StateProvider for HistoricalMock {
        fn storage(
            &self,
            _address: Address,
            _storage_key: StorageKey,
        ) -> ProviderResult<Option<StorageValue>> {
            Ok(None)
        }
    }

    impl BytecodeReader for HistoricalMock {
        fn bytecode_by_hash(&self, _code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
            Ok(None)
        }
    }

    impl BlockHashReader for HistoricalMock {
        fn block_hash(&self, _number: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(None)
        }

        fn canonical_hashes_range(
            &self,
            _start: BlockNumber,
            _end: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            Ok(vec![])
        }
    }

    impl BlockNumberToBlockIdReader for HistoricalMock {
        fn block_id_by_number(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(self.block_ids.get(&number).copied())
        }
    }

    impl AccountReader for HistoricalMock {
        fn basic_account(&self, _address: &Address) -> ProviderResult<Option<Account>> {
            Ok(None)
        }
    }

    impl StateRootProvider for HistoricalMock {
        fn state_root(&self, _hashed_state: HashedPostState) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }
        fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }
        fn state_root_with_updates(
            &self,
            _hashed_state: HashedPostState,
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

    impl HashedPostStateProvider for HistoricalMock {
        fn hashed_post_state(&self, _bundle_state: &BundleState) -> HashedPostState {
            HashedPostState::default()
        }
    }

    impl StorageRootProvider for HistoricalMock {
        fn storage_root(
            &self,
            _address: Address,
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<B256> {
            Ok(B256::ZERO)
        }
        fn storage_proof(
            &self,
            _address: Address,
            slot: B256,
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<StorageProof> {
            Ok(StorageProof::new(slot))
        }
        fn storage_multiproof(
            &self,
            _address: Address,
            _slots: &[B256],
            _hashed_storage: HashedStorage,
        ) -> ProviderResult<StorageMultiProof> {
            Ok(StorageMultiProof::empty())
        }
    }

    impl StateProofProvider for HistoricalMock {
        fn proof(
            &self,
            _input: TrieInput,
            _address: Address,
            _slots: &[B256],
        ) -> ProviderResult<AccountProof> {
            Ok(AccountProof::new(Address::ZERO))
        }
        fn multiproof(
            &self,
            _input: TrieInput,
            _targets: MultiProofTargets,
        ) -> ProviderResult<MultiProof> {
            Ok(MultiProof::default())
        }
        fn witness(
            &self,
            _input: TrieInput,
            _target: HashedPostState,
        ) -> ProviderResult<Vec<Bytes>> {
            Ok(Vec::default())
        }
    }

    /// Constructs an overlay over a single in-memory block at `number` with the given
    /// `block_ids` snapshot and `historical` fallback.
    fn build_overlay(
        historical: HistoricalMock,
        number: BlockNumber,
        block_ids: HashMap<BlockNumber, B256>,
    ) -> (MemoryOverlayStateProvider<EthPrimitives>, B256) {
        let mut builder: TestBlockBuilder<EthPrimitives> = TestBlockBuilder::default();
        let block = builder.get_executed_block_with_number(number, B256::random());
        let sealed_hash = block.recovered_block().hash();
        let overlay = MemoryOverlayStateProvider::new(Box::new(historical), vec![block])
            .with_block_ids(block_ids);
        (overlay, sealed_hash)
    }

    fn block_id(byte: u8) -> B256 {
        B256::from(U256::from(byte))
    }

    /// T1: historical returns `None`. With empty `block_ids` snapshot, `block_id_by_number`
    /// returns `None` — the `in_memory` sealed-block hash must NOT leak through as a `block_id`.
    /// With a populated snapshot, it returns the snapshot value.
    #[test]
    fn t1_in_memory_only_block_no_keccak_leak() {
        const N: BlockNumber = 7;

        // empty snapshot → None (in_memory's sealed_block.hash() must not surface here)
        let (overlay_empty, sealed_hash) =
            build_overlay(HistoricalMock::new(), N, HashMap::default());
        let observed = overlay_empty.block_id_by_number(N).unwrap();
        assert_eq!(
            observed, None,
            "block_id_by_number must not return the in_memory sealed_block.hash() when no \
             block_id snapshot is present"
        );
        // sanity: that sealed hash is what `block_hash` would surface, and it's not what we want
        // here.
        assert_eq!(overlay_empty.block_hash(N).unwrap(), Some(sealed_hash));

        // populated snapshot → Some(0xBB)
        let bb = block_id(0xBB);
        let mut snapshot = HashMap::default();
        snapshot.insert(N, bb);
        let (overlay_filled, _) = build_overlay(HistoricalMock::new(), N, snapshot);
        assert_eq!(overlay_filled.block_id_by_number(N).unwrap(), Some(bb));
    }

    /// T2: priority rule. Historical has `Some(0xAA)` for N, in-memory snapshot has
    /// `Some(0xBB)`. The in-memory snapshot must win. In production the two should
    /// never both be populated for the same N — the engine tree evicts the in-memory
    /// entry only after the row lands in `tables::BlockNumberToBlockId`, so an entry
    /// migrates from in-memory → historical. This test pins the priority rule so the
    /// race-window invariant (in-memory is the more-current view) holds even if the
    /// eviction order ever slips.
    #[test]
    fn t2_in_memory_snapshot_overrides_historical() {
        const N: BlockNumber = 11;
        let aa = block_id(0xAA);
        let bb = block_id(0xBB);

        let historical = HistoricalMock::new().with_block_id(N, aa);
        let mut snapshot = HashMap::default();
        snapshot.insert(N, bb);
        let (overlay, _) = build_overlay(historical, N, snapshot);

        assert_eq!(
            overlay.block_id_by_number(N).unwrap(),
            Some(bb),
            "in-memory snapshot must take priority over historical"
        );
    }

    /// T3: `block_hash` (returns keccak header hash from `in_memory` `sealed_block`) and
    /// `block_id_by_number` (returns Aptos consensus `block_id` from snapshot) diverge on the
    /// same overlay — verifies the two trait methods are semantically distinct.
    #[test]
    fn t3_block_hash_and_block_id_diverge_on_same_overlay() {
        const N: BlockNumber = 13;
        let bb = block_id(0xBB);

        let mut snapshot = HashMap::default();
        snapshot.insert(N, bb);
        let (overlay, sealed_hash) = build_overlay(HistoricalMock::new(), N, snapshot);

        let block_hash_observed = overlay.block_hash(N).unwrap();
        let block_id_observed = overlay.block_id_by_number(N).unwrap();

        assert_eq!(
            block_hash_observed,
            Some(sealed_hash),
            "block_hash should keep returning sealed_block.hash() (keccak header hash)"
        );
        assert_eq!(
            block_id_observed,
            Some(bb),
            "block_id_by_number should return the Aptos consensus block_id from the snapshot"
        );
        assert_ne!(
            block_hash_observed, block_id_observed,
            "BLOCKHASH (block_id) and RPC block hash (keccak) must NOT collapse to the same value"
        );
    }
}
