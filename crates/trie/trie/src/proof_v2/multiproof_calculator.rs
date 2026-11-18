//! Reusable multiproof generation over a fixed state snapshot.

use super::{
    DeferredValueEncoder, LeafValueEncoder, ProofCalculator, RebindableHashedStorageCursor,
    ResettableHashedCursor, ResettableTrieCursor, StorageProofCalculator, TrieStorageCursor,
};
use crate::{
    hashed_cursor::HashedCursorFactory,
    prefix_set::{PrefixSet, TriePrefixSets},
    trie_cursor::TrieCursorFactory,
    DecodedMultiProofV2, MultiProofTargetsV2,
};
use alloy_primitives::{map::B256Map, B256, U256};
use alloy_rlp::Encodable;
use parking_lot::Mutex;
use reth_execution_errors::StateProofError;
use reth_primitives_traits::Account;
use std::{fmt, sync::Arc};

/// Generates account and storage multiproofs for one immutable state snapshot.
///
/// The calculator owns its cursor factories and may be reused for any number of requests. It stays
/// usable after an error, but must be dropped after a panic during a request. The factories must
/// keep describing the same state for the lifetime of the calculator. Cursor factories control
/// whether their cursors are instrumented.
///
/// Pass factories whose cursors implement the local fallible reset/rebind contracts, such as
/// [`super::ProofCursorFactory`] over a fixed database snapshot or immutable in-memory data.
/// Reopening cursors on a live database does not provide a stable read view.
pub struct MultiProofCalculator<T, H>
where
    T: TrieCursorFactory,
    H: HashedCursorFactory,
    T::AccountTrieCursor: ResettableTrieCursor,
    T::StorageTrieCursor: TrieStorageCursor,
    H::AccountCursor: ResettableHashedCursor,
    H::StorageCursor: RebindableHashedStorageCursor,
{
    account_calculator: Option<AccountCalculatorOf<T, H>>,
    storage_calculator: Option<Arc<Mutex<StorageCalculatorOf<T, H>>>>,
    prefix_sets: TriePrefixSets,
    trie_cursor_factory: T,
    hashed_cursor_factory: H,
}

impl<T, H> MultiProofCalculator<T, H>
where
    T: TrieCursorFactory,
    H: HashedCursorFactory,
    T::AccountTrieCursor: ResettableTrieCursor,
    T::StorageTrieCursor: TrieStorageCursor,
    H::AccountCursor: ResettableHashedCursor,
    H::StorageCursor: RebindableHashedStorageCursor,
{
    /// Creates a calculator for the supplied factories and frozen changed-key prefixes.
    ///
    /// Prefixes invalidate cached branch hashes for keys whose leaf values have changed, and apply
    /// to every request served by this calculator.
    pub const fn new(
        trie_cursor_factory: T,
        hashed_cursor_factory: H,
        prefix_sets: TriePrefixSets,
    ) -> Self {
        Self {
            account_calculator: None,
            storage_calculator: None,
            prefix_sets,
            trie_cursor_factory,
            hashed_cursor_factory,
        }
    }

    /// Generates account and storage proofs for `targets`.
    ///
    /// An explicit storage address with no slot targets requests its root node. Empty targets
    /// return an empty proof.
    pub fn multiproof(
        &mut self,
        targets: MultiProofTargetsV2,
    ) -> Result<DecodedMultiProofV2, StateProofError> {
        if targets.is_empty() {
            return Ok(DecodedMultiProofV2::default())
        }
        let MultiProofTargetsV2 { mut account_targets, storage_targets } = targets;
        let mut proof = DecodedMultiProofV2::default();

        // Open the storage calculator, which account proofs also use for storage roots.
        let storage_calculator = match &mut self.storage_calculator {
            Some(storage_calculator) => storage_calculator,
            slot @ None => {
                // Storage cursors are retargeted to each requested account before use.
                let trie_cursor = self.trie_cursor_factory.storage_trie_cursor(B256::ZERO)?;
                let hashed_cursor = self.hashed_cursor_factory.hashed_storage_cursor(B256::ZERO)?;
                // Prefixes are taken only after the fallible cursor opens, so a failed
                // initialization can be retried.
                let prefix_sets = std::mem::take(&mut self.prefix_sets.storage_prefix_sets);
                slot.insert(Arc::new(Mutex::new(PrefixSetStorageCalculator {
                    calculator: StorageProofCalculator::new_storage(trie_cursor, hashed_cursor),
                    prefix_sets,
                    hashed_address: None,
                })))
            }
        };

        // Calculate account proofs, computing storage roots inline.
        if !account_targets.is_empty() {
            let account_calculator = match &mut self.account_calculator {
                Some(account_calculator) => account_calculator,
                slot @ None => {
                    let trie_cursor = self.trie_cursor_factory.account_trie_cursor()?;
                    let hashed_cursor = self.hashed_cursor_factory.hashed_account_cursor()?;
                    let prefix_set = std::mem::take(&mut self.prefix_sets.account_prefix_set);
                    slot.insert(
                        ProofCalculator::new(trie_cursor, hashed_cursor)
                            .with_prefix_set(prefix_set),
                    )
                }
            };
            let mut encoder =
                AccountValueEncoder { storage_calculator: storage_calculator.clone() };
            proof.account_proofs = account_calculator.proof(&mut encoder, &mut account_targets)?;
        }

        // Calculate storage proofs, or root nodes for accounts without slot targets.
        if !storage_targets.is_empty() {
            let mut storage_calculator = storage_calculator.lock();
            for (hashed_address, mut targets) in storage_targets {
                let calculator = storage_calculator.calculator_for(hashed_address);
                let nodes = if targets.is_empty() {
                    vec![calculator.storage_root_node(hashed_address)?]
                } else {
                    calculator.storage_proof(hashed_address, &mut targets)?
                };
                proof.storage_proofs.insert(hashed_address, nodes);
            }
        }
        Ok(proof)
    }
}

impl<T, H> fmt::Debug for MultiProofCalculator<T, H>
where
    T: TrieCursorFactory,
    H: HashedCursorFactory,
    T::AccountTrieCursor: ResettableTrieCursor,
    T::StorageTrieCursor: TrieStorageCursor,
    H::AccountCursor: ResettableHashedCursor,
    H::StorageCursor: RebindableHashedStorageCursor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiProofCalculator")
            .field("account_initialized", &self.account_calculator.is_some())
            .field("storage_initialized", &self.storage_calculator.is_some())
            .finish_non_exhaustive()
    }
}

/// Account calculator opened from the owned cursor factories.
type AccountCalculatorOf<T, H> = ProofCalculator<
    <T as TrieCursorFactory>::AccountTrieCursor,
    <H as HashedCursorFactory>::AccountCursor,
    AccountValueEncoder<
        <T as TrieCursorFactory>::StorageTrieCursor,
        <H as HashedCursorFactory>::StorageCursor,
    >,
>;

/// Storage calculator opened from the owned cursor factories.
type StorageCalculatorOf<T, H> = PrefixSetStorageCalculator<
    <T as TrieCursorFactory>::StorageTrieCursor,
    <H as HashedCursorFactory>::StorageCursor,
>;

/// Storage proof calculator that applies each account's changed-slot prefixes.
///
/// All storage calculations go through this type so the prefixes always match the account being
/// calculated.
struct PrefixSetStorageCalculator<TC, HC> {
    calculator: StorageProofCalculator<TC, HC>,
    /// Changed-slot prefixes keyed by hashed address.
    prefix_sets: B256Map<PrefixSet>,
    /// Account whose prefixes are currently applied to `calculator`.
    hashed_address: Option<B256>,
}

impl<TC: TrieStorageCursor, HC: RebindableHashedStorageCursor<Value = U256>>
    PrefixSetStorageCalculator<TC, HC>
{
    fn calculator_for(&mut self, hashed_address: B256) -> &mut StorageProofCalculator<TC, HC> {
        if self.hashed_address != Some(hashed_address) {
            self.calculator
                .set_prefix_set(self.prefix_sets.get(&hashed_address).cloned().unwrap_or_default());
            self.hashed_address = Some(hashed_address);
        }
        &mut self.calculator
    }
}

/// Account value encoder that computes storage roots with a shared storage calculator.
struct AccountValueEncoder<TC, HC> {
    // Arc<Mutex<_>> keeps the calculator Send whenever its cursors are. The mutex never poisons,
    // which is why the calculator must not be reused after a panic.
    storage_calculator: Arc<Mutex<PrefixSetStorageCalculator<TC, HC>>>,
}

impl<TC: TrieStorageCursor, HC: RebindableHashedStorageCursor<Value = U256>> LeafValueEncoder
    for AccountValueEncoder<TC, HC>
{
    type Value = Account;
    type DeferredEncoder = AccountDeferredValueEncoder<TC, HC>;

    fn deferred_encoder(
        &mut self,
        hashed_address: B256,
        account: Account,
    ) -> Self::DeferredEncoder {
        AccountDeferredValueEncoder {
            storage_calculator: self.storage_calculator.clone(),
            hashed_address,
            account,
        }
    }
}

/// Deferred account encoder that computes the account's storage root when encoded.
struct AccountDeferredValueEncoder<TC, HC> {
    storage_calculator: Arc<Mutex<PrefixSetStorageCalculator<TC, HC>>>,
    hashed_address: B256,
    account: Account,
}

impl<TC: TrieStorageCursor, HC: RebindableHashedStorageCursor<Value = U256>> DeferredValueEncoder
    for AccountDeferredValueEncoder<TC, HC>
{
    fn encode(self, buf: &mut Vec<u8>) -> Result<(), StateProofError> {
        let mut storage = self.storage_calculator.lock();
        let calculator = storage.calculator_for(self.hashed_address);
        let root_node = calculator.storage_root_node(self.hashed_address)?;
        let root = calculator
            .compute_root_hash(core::slice::from_ref(&root_node))?
            .expect("storage_root_node returns a node at the root path");
        self.account.into_trie_account(root).encode(buf);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hashed_cursor::mock::MockHashedCursorFactory,
        prefix_set::PrefixSetMut,
        proof_v2::ProofCursorFactory,
        test_utils::{self, TrieTestHarness},
        trie_cursor::{mock::MockTrieCursorFactory, noop::NoopTrieCursorFactory},
        StateRoot,
    };
    use alloy_primitives::{keccak256, Address};
    use reth_storage_errors::db::DatabaseError;
    use reth_trie_common::{Nibbles, ProofTrieNodeV2, ProofV2Target};
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    type TestCalculator = MultiProofCalculator<
        ProofCursorFactory<ControlledTrieFactory>,
        ProofCursorFactory<ControlledHashedFactory>,
    >;

    struct Fixture {
        accounts: BTreeMap<Address, Account>,
        storage: BTreeMap<Address, BTreeMap<B256, U256>>,
        trie: MockTrieCursorFactory,
        hashed: MockHashedCursorFactory,
        account_prefixes: Vec<Nibbles>,
        storage_prefixes: B256Map<Vec<Nibbles>>,
    }

    impl Fixture {
        fn new() -> Self {
            let a = Address::with_last_byte(1);
            let b = Address::with_last_byte(2);
            let deleted = Address::with_last_byte(3);
            let mut accounts = (1u8..=32)
                .map(|i| {
                    (
                        Address::with_last_byte(i),
                        Account { nonce: i as u64, balance: U256::from(i), bytecode_hash: None },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let mut storage = accounts
                .keys()
                .copied()
                .map(|address| (address, BTreeMap::new()))
                .collect::<BTreeMap<_, _>>();
            for address in [a, b] {
                storage.insert(
                    address,
                    (0u64..64).map(|i| (B256::from(U256::from(i)), U256::from(i + 1))).collect(),
                );
            }
            let before_hashed = Self::hashed_factory(&accounts, &storage);
            let (_, before_updates) =
                StateRoot::new(NoopTrieCursorFactory, before_hashed).root_with_updates().unwrap();
            let mut storage_nodes = B256Map::default();
            for (&address, slots) in &storage {
                let harness = TrieTestHarness::new(
                    slots.iter().map(|(slot, value)| (keccak256(slot), *value)).collect(),
                );
                storage_nodes.insert(
                    keccak256(address),
                    harness.storage_trie_updates().storage_nodes.clone().into_iter().collect(),
                );
            }
            // The reusable storage cursor is first opened at ZERO. Missing addresses are explicit
            // empty fixtures, rather than a fallback that conceals a factory error.
            storage_nodes.insert(B256::ZERO, BTreeMap::new());
            storage_nodes.insert(keccak256(Self::missing()), BTreeMap::new());
            let trie = MockTrieCursorFactory::new(
                before_updates.account_nodes.into_iter().collect(),
                storage_nodes,
            );

            accounts.get_mut(&a).unwrap().balance = U256::from(900);
            accounts.remove(&deleted);
            let mut storage_prefixes = B256Map::default();
            for (address, slot, value) in [(a, Self::slot(0), 901u64), (b, Self::slot(1), 902)] {
                storage.get_mut(&address).unwrap().insert(slot, U256::from(value));
                storage_prefixes.insert(keccak256(address), vec![Nibbles::unpack(keccak256(slot))]);
            }
            let hashed = Self::hashed_factory(&accounts, &storage);
            Self {
                accounts,
                storage,
                trie,
                hashed,
                account_prefixes: [a, b, deleted]
                    .map(|address| Nibbles::unpack(keccak256(address)))
                    .to_vec(),
                storage_prefixes,
            }
        }

        fn slot(number: u64) -> B256 {
            B256::from(U256::from(number))
        }
        fn missing() -> Address {
            Address::repeat_byte(0xff)
        }

        fn hashed_factory(
            accounts: &BTreeMap<Address, Account>,
            storage: &BTreeMap<Address, BTreeMap<B256, U256>>,
        ) -> MockHashedCursorFactory {
            let accounts =
                accounts.iter().map(|(address, account)| (keccak256(address), *account)).collect();
            let mut slots = storage
                .iter()
                .map(|(address, slots)| {
                    (
                        keccak256(address),
                        slots.iter().map(|(slot, value)| (keccak256(slot), *value)).collect(),
                    )
                })
                .collect::<B256Map<_>>();
            slots.insert(B256::ZERO, BTreeMap::new());
            slots.insert(keccak256(Self::missing()), BTreeMap::new());
            MockHashedCursorFactory::new(accounts, slots)
        }

        fn prefixes(&self) -> TriePrefixSets {
            TriePrefixSets {
                account_prefix_set: PrefixSetMut::from(self.account_prefixes.iter().copied())
                    .freeze(),
                storage_prefix_sets: self
                    .storage_prefixes
                    .iter()
                    .map(|(address, prefixes)| {
                        (*address, PrefixSetMut::from(prefixes.iter().copied()).freeze())
                    })
                    .collect(),
                ..Default::default()
            }
        }

        fn calculator(&self, control: Arc<Control>) -> TestCalculator {
            MultiProofCalculator::new(
                ProofCursorFactory::new(ControlledTrieFactory {
                    inner: self.trie.clone(),
                    control: control.clone(),
                }),
                ProofCursorFactory::new(ControlledHashedFactory {
                    inner: self.hashed.clone(),
                    control,
                }),
                self.prefixes(),
            )
        }

        fn expected_root(&self) -> B256 {
            test_utils::state_root(self.accounts.iter().map(|(address, account)| {
                (
                    *address,
                    (
                        *account,
                        self.storage
                            .get(address)
                            .unwrap()
                            .iter()
                            .map(|(key, value)| (*key, *value)),
                    ),
                )
            }))
        }

        fn expected_storage_root(&self, address: Address) -> B256 {
            test_utils::storage_root(
                self.storage
                    .get(&address)
                    .into_iter()
                    .flat_map(|slots| slots.iter())
                    .map(|(slot, value)| (*slot, *value)),
            )
        }

        fn root_targets() -> MultiProofTargetsV2 {
            MultiProofTargetsV2 {
                account_targets: vec![ProofV2Target::new(B256::ZERO)],
                ..Default::default()
            }
        }

        fn mixed_targets() -> MultiProofTargetsV2 {
            MultiProofTargetsV2 {
                account_targets: [
                    Address::with_last_byte(1),
                    Address::with_last_byte(2),
                    Address::with_last_byte(3),
                    Self::missing(),
                ]
                .map(|address| ProofV2Target::new(keccak256(address)))
                .to_vec(),
                storage_targets: B256Map::from_iter([
                    (
                        keccak256(Address::with_last_byte(1)),
                        [Self::slot(0), Self::slot(1), Self::slot(999)]
                            .map(|slot| ProofV2Target::new(keccak256(slot)))
                            .to_vec(),
                    ),
                    (keccak256(Address::with_last_byte(2)), Vec::new()),
                    (keccak256(Self::missing()), Vec::new()),
                ]),
            }
        }
    }

    fn proof_root(nodes: &[ProofTrieNodeV2]) -> B256 {
        let root =
            nodes.iter().find(|node| node.path.is_empty()).expect("full proof contains root");
        keccak256(alloy_rlp::encode(&root.node))
    }

    #[derive(Default)]
    struct Control {
        calls: AtomicUsize,
        fail_on: AtomicUsize,
    }

    impl Control {
        fn open(&self) -> Result<(), DatabaseError> {
            let number = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            if self.fail_on.load(Ordering::Relaxed) == number {
                return Err(DatabaseError::Other(format!("factory open {number} failed")))
            }
            Ok(())
        }
        fn fail_next(&self) -> usize {
            let next = self.calls.load(Ordering::Relaxed) + 1;
            self.fail_on.store(next, Ordering::Relaxed);
            next
        }
    }

    #[derive(Clone)]
    struct ControlledTrieFactory {
        inner: MockTrieCursorFactory,
        control: Arc<Control>,
    }
    impl TrieCursorFactory for ControlledTrieFactory {
        type AccountTrieCursor = <MockTrieCursorFactory as TrieCursorFactory>::AccountTrieCursor;
        type StorageTrieCursor = <MockTrieCursorFactory as TrieCursorFactory>::StorageTrieCursor;
        fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
            self.control.open()?;
            self.inner.account_trie_cursor()
        }
        fn storage_trie_cursor(
            &self,
            address: B256,
        ) -> Result<Self::StorageTrieCursor, DatabaseError> {
            self.control.open()?;
            self.inner.storage_trie_cursor(address)
        }
    }

    #[derive(Clone)]
    struct ControlledHashedFactory {
        inner: MockHashedCursorFactory,
        control: Arc<Control>,
    }
    impl HashedCursorFactory for ControlledHashedFactory {
        type AccountCursor = <MockHashedCursorFactory as HashedCursorFactory>::AccountCursor;
        type StorageCursor = <MockHashedCursorFactory as HashedCursorFactory>::StorageCursor;
        fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
            self.control.open()?;
            self.inner.hashed_account_cursor()
        }
        fn hashed_storage_cursor(
            &self,
            address: B256,
        ) -> Result<Self::StorageCursor, DatabaseError> {
            self.control.open()?;
            self.inner.hashed_storage_cursor(address)
        }
    }

    fn assert_factory_error<T: fmt::Debug>(result: Result<T, StateProofError>, open: usize) {
        match result {
            Err(StateProofError::Database(DatabaseError::Other(message))) => {
                assert_eq!(message, format!("factory open {open} failed"))
            }
            other => panic!("expected original factory initialization error, got {other:?}"),
        }
    }

    #[test]
    fn mixed_multiproofs_match_independent_state_and_storage_oracles() {
        let fixture = Fixture::new();
        let mut calculator = fixture.calculator(Default::default());
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&calculator);
        for _ in 0..2 {
            let proof = calculator.multiproof(Fixture::mixed_targets()).unwrap();
            let root = fixture.expected_root();
            assert_eq!(proof_root(&proof.account_proofs), root);
            for address in [
                Address::with_last_byte(1),
                Address::with_last_byte(2),
                Address::with_last_byte(3),
                Fixture::missing(),
            ] {
                let slots = if address == Address::with_last_byte(1) {
                    vec![Fixture::slot(0), Fixture::slot(1), Fixture::slot(999)]
                } else {
                    Vec::new()
                };
                let account_proof = proof.account_proof(address, &slots).unwrap();
                assert_eq!(account_proof.info, fixture.accounts.get(&address).copied());
                account_proof.verify(root).unwrap();
            }
            for address in
                [Address::with_last_byte(1), Address::with_last_byte(2), Fixture::missing()]
            {
                assert_eq!(
                    proof_root(&proof.storage_proofs[&keccak256(address)]),
                    fixture.expected_storage_root(address)
                );
            }
        }
    }

    #[test]
    fn repeated_address_switches_keep_matching_dirty_prefixes() {
        let fixture = Fixture::new();
        let mut calculator = fixture.calculator(Default::default());
        for address in [
            Address::with_last_byte(1),
            Address::with_last_byte(2),
            Fixture::missing(),
            Address::with_last_byte(1),
        ] {
            let proof = calculator
                .multiproof(MultiProofTargetsV2 {
                    storage_targets: B256Map::from_iter([(keccak256(address), Vec::new())]),
                    ..Default::default()
                })
                .unwrap();
            assert!(proof.account_proofs.is_empty());
            assert_eq!(
                proof_root(&proof.storage_proofs[&keccak256(address)]),
                fixture.expected_storage_root(address)
            );
        }
        let proof = calculator.multiproof(Fixture::root_targets()).unwrap();
        assert!(proof.storage_proofs.is_empty());
        assert_eq!(proof_root(&proof.account_proofs), fixture.expected_root());
    }

    #[test]
    fn empty_targets_do_not_open_factories() {
        let fixture = Fixture::new();
        let control = Arc::new(Control::default());
        control.fail_on.store(1, Ordering::Relaxed);
        let mut calculator = fixture.calculator(control.clone());
        assert!(calculator.multiproof(Default::default()).unwrap().is_empty());
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn failed_lazy_opens_are_retryable_without_losing_prefixes() {
        let fixture = Fixture::new();
        // First/second storage open, then first/second account open.
        for fail_on in 1..=4 {
            let control = Arc::new(Control::default());
            control.fail_on.store(fail_on, Ordering::Relaxed);
            let mut calculator = fixture.calculator(control.clone());
            assert_factory_error(calculator.multiproof(Fixture::root_targets()), fail_on);
            control.fail_on.store(0, Ordering::Relaxed);
            let proof = calculator.multiproof(Fixture::root_targets()).unwrap();
            assert_eq!(proof_root(&proof.account_proofs), fixture.expected_root());
        }
    }

    #[test]
    fn failures_after_success_preserve_reset_and_rebind_errors() {
        let fixture = Fixture::new();
        let control = Arc::new(Control::default());
        let mut calculator = fixture.calculator(control.clone());
        assert_eq!(
            proof_root(&calculator.multiproof(Fixture::root_targets()).unwrap().account_proofs),
            fixture.expected_root()
        );
        let fail_on = control.fail_next();
        assert_factory_error(calculator.multiproof(Fixture::root_targets()), fail_on);
        control.fail_on.store(0, Ordering::Relaxed);
        assert_eq!(
            proof_root(&calculator.multiproof(Fixture::root_targets()).unwrap().account_proofs),
            fixture.expected_root()
        );

        let address = Address::with_last_byte(2);
        let targets = || MultiProofTargetsV2 {
            storage_targets: B256Map::from_iter([(keccak256(address), Vec::new())]),
            ..Default::default()
        };
        let fail_on = control.fail_next();
        assert_factory_error(calculator.multiproof(targets()), fail_on);
        control.fail_on.store(0, Ordering::Relaxed);
        let proof = calculator.multiproof(targets()).unwrap();
        assert_eq!(
            proof_root(&proof.storage_proofs[&keccak256(address)]),
            fixture.expected_storage_root(address)
        );
    }
}
