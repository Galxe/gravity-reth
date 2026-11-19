use super::*;
use crate::{
    hashed_cursor::{mock::MockHashedCursorFactory, HashedCursorFactory},
    proof_v2::ProofCursorFactory,
    test_utils::{state_root_prehashed, TrieTestHarness},
    trie_cursor::{mock::MockTrieCursorFactory, noop::NoopTrieCursorFactory, TrieCursorFactory},
    BranchNodeCompact, StateRoot,
};
use alloy_primitives::{keccak256, map::B256Map};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier,
    },
    time::Duration,
};

type TestVerifier = Verifier<
    <MockTrieCursorFactory as TrieCursorFactory>::AccountTrieCursor,
    <MockHashedCursorFactory as HashedCursorFactory>::AccountCursor,
>;
type StorageHook = Arc<dyn Fn(B256) -> Result<(), DatabaseError> + Send + Sync>;

struct Fixture {
    accounts: BTreeMap<B256, Account>,
    slots: B256Map<BTreeMap<B256, U256>>,
    account_nodes: BTreeMap<Nibbles, BranchNodeCompact>,
    storage_nodes: B256Map<BTreeMap<Nibbles, BranchNodeCompact>>,
}

impl Fixture {
    fn new() -> Self {
        let accounts = (1u64..=32)
            .map(|i| {
                (
                    keccak256(i.to_be_bytes()),
                    Account { nonce: i, balance: U256::from(i), bytecode_hash: None },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut slots =
            accounts.keys().map(|address| (*address, BTreeMap::new())).collect::<B256Map<_>>();
        for address in accounts.keys().take(2) {
            slots.insert(
                *address,
                (1u64..=64).map(|i| (keccak256(i.to_be_bytes()), U256::from(i))).collect(),
            );
        }
        slots.insert(B256::ZERO, BTreeMap::new());
        let hashed = MockHashedCursorFactory::new(accounts.clone(), slots.clone());
        let (root, updates) =
            StateRoot::new(NoopTrieCursorFactory, hashed).root_with_updates().unwrap();
        assert_eq!(
            root,
            state_root_prehashed(
                accounts
                    .iter()
                    .map(|(address, account)| (*address, (*account, slots[address].clone())))
            )
        );
        let storage_nodes = slots
            .iter()
            .map(|(address, storage)| {
                let harness = TrieTestHarness::new(storage.clone());
                (
                    *address,
                    harness.storage_trie_updates().storage_nodes.clone().into_iter().collect(),
                )
            })
            .collect();
        Self {
            accounts,
            slots,
            account_nodes: updates.account_nodes.into_iter().collect(),
            storage_nodes,
        }
    }

    fn trie(&self) -> MockTrieCursorFactory {
        MockTrieCursorFactory::new(self.account_nodes.clone(), self.storage_nodes.clone())
    }
    fn hashed(&self) -> MockHashedCursorFactory {
        MockHashedCursorFactory::new(self.accounts.clone(), self.slots.clone())
    }
    fn jobs(&self) -> Vec<Result<StorageJob, DatabaseError>> {
        let mut addresses = self
            .slots
            .keys()
            .chain(self.storage_nodes.keys())
            .copied()
            .filter(|address| *address != B256::ZERO)
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        addresses
            .into_iter()
            .map(|address| {
                Ok(StorageJob {
                    address,
                    root_required: self.slots.get(&address).is_some_and(|slots| !slots.is_empty()),
                })
            })
            .collect()
    }
    fn verifier(
        &self,
        hook: StorageHook,
        jobs: Vec<Result<StorageJob, DatabaseError>>,
        lookahead: usize,
        drops: Arc<AtomicUsize>,
    ) -> TestVerifier {
        let trie = ProofCursorFactory::new(self.trie());
        let hashed = ProofCursorFactory::new(HookedFactory { inner: self.hashed(), hook });
        let workers = (0..2)
            .map(|_| StorageWorker {
                trie: trie.storage_trie_cursor(B256::ZERO).unwrap(),
                hashed: hashed.hashed_storage_cursor(B256::ZERO).unwrap(),
                tx: DropGuard(drops.clone()),
            })
            .collect();
        Verifier::new(
            self.trie().account_trie_cursor().unwrap(),
            self.hashed().hashed_account_cursor().unwrap(),
            jobs.into_iter(),
            workers,
            lookahead,
        )
        .unwrap()
    }
}

#[derive(Clone)]
struct HookedFactory {
    inner: MockHashedCursorFactory,
    hook: StorageHook,
}
impl std::fmt::Debug for HookedFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookedFactory").finish_non_exhaustive()
    }
}
impl HashedCursorFactory for HookedFactory {
    type AccountCursor = <MockHashedCursorFactory as HashedCursorFactory>::AccountCursor;
    type StorageCursor = <MockHashedCursorFactory as HashedCursorFactory>::StorageCursor;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        self.inner.hashed_account_cursor()
    }
    fn hashed_storage_cursor(&self, address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        if address != B256::ZERO {
            (self.hook)(address)?;
        }
        self.inner.hashed_storage_cursor(address)
    }
}
#[derive(Debug)]
struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn no_hook() -> StorageHook {
    Arc::new(|_| Ok(()))
}
fn findings(iter: impl Iterator<Item = Result<Output, StateRootError>>) -> Vec<Output> {
    iter.map(Result::unwrap).filter(|output| !matches!(output, Output::Progress(_))).collect()
}
fn assert_same_findings(mut actual: Vec<Output>, mut expected: Vec<Output>) {
    // Findings are unordered across producers; compare the complete multiset, including duplicates.
    actual.sort_by_key(|item| format!("{item:?}"));
    expected.sort_by_key(|item| format!("{item:?}"));
    assert_eq!(actual, expected);
}
fn assert_original_error(result: Result<Output, StateRootError>, expected: &str) {
    match result {
        Err(StateRootError::Database(DatabaseError::Other(message))) => {
            assert_eq!(message, expected)
        }
        other => panic!("expected original database error {expected:?}, got {other:?}"),
    }
}

#[test]
fn parallel_findings_match_serial_for_clean_and_corrupted_tries() {
    let mut fixture = Fixture::new();
    assert!(findings(fixture.verifier(no_hook(), fixture.jobs(), 1, Default::default())).is_empty());
    let paths = fixture.account_nodes.keys().copied().take(2).collect::<Vec<_>>();
    assert_eq!(paths.len(), 2);
    fixture.account_nodes.remove(&paths[0]);
    fixture.account_nodes.get_mut(&paths[1]).unwrap().root_hash = Some(B256::repeat_byte(0xee));
    let extra = Nibbles::from_nibbles([15; 12]);
    fixture.account_nodes.insert(extra, BranchNodeCompact::default());
    let address = *fixture.accounts.keys().next().unwrap();
    let storage = fixture.storage_nodes.get_mut(&address).unwrap();
    let paths = storage.keys().copied().take(2).collect::<Vec<_>>();
    assert_eq!(paths.len(), 2);
    storage.remove(&paths[0]);
    storage.get_mut(&paths[1]).unwrap().root_hash = Some(B256::repeat_byte(0xdd));
    storage.insert(extra, BranchNodeCompact::default());
    let serial = findings(crate::verify::Verifier::new(fixture.trie(), fixture.hashed()).unwrap());
    assert!(serial.iter().any(|finding| matches!(finding, Output::AccountMissing(..))));
    assert!(serial.iter().any(|finding| matches!(finding, Output::AccountWrong { .. })));
    assert!(serial.iter().any(|finding| matches!(finding, Output::StorageMissing(..))));
    assert!(serial.iter().any(|finding| matches!(finding, Output::StorageWrong { .. })));
    for lookahead in [1, 2, 64] {
        assert_same_findings(
            findings(fixture.verifier(no_hook(), fixture.jobs(), lookahead, Default::default())),
            serial.clone(),
        );
    }
}

#[test]
fn orphan_storage_is_verified_without_creating_account_leaves() {
    let mut fixture = Fixture::new();
    let hashed_orphan = B256::repeat_byte(0xee);
    let trie_only_orphan = B256::repeat_byte(0xff);
    assert!(!fixture.accounts.contains_key(&hashed_orphan));
    let slots = BTreeMap::from([(B256::ZERO, U256::ONE), (B256::repeat_byte(0x80), U256::from(2))]);
    fixture.storage_nodes.insert(
        hashed_orphan,
        TrieTestHarness::new(slots.clone())
            .storage_trie_updates()
            .storage_nodes
            .clone()
            .into_iter()
            .collect(),
    );
    fixture.slots.insert(hashed_orphan, slots);
    fixture.slots.insert(trie_only_orphan, BTreeMap::new());
    let path = Nibbles::from_nibbles([7]);
    let node = BranchNodeCompact::default();
    fixture.storage_nodes.insert(trie_only_orphan, BTreeMap::from([(path, node.clone())]));
    for lookahead in [1, 4] {
        assert_eq!(
            findings(fixture.verifier(no_hook(), fixture.jobs(), lookahead, Default::default())),
            vec![Output::StorageExtra(trie_only_orphan, path, node.clone())]
        );
    }
}

#[test]
fn two_storage_workers_execute_concurrently() {
    let fixture = Fixture::new();
    let addresses = fixture.accounts.keys().take(2).copied().collect::<Vec<_>>();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(2);
    let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(0);
    let hook: StorageHook = Arc::new(move |address| {
        if addresses.contains(&address) {
            entered_tx.send(address).unwrap();
            let _ = release_rx.recv();
        }
        Ok(())
    });
    let verifier = fixture.verifier(hook, fixture.jobs(), 64, Default::default());
    // A sequential worker cannot enter both jobs before either is released.
    let first = entered_rx.recv_timeout(Duration::from_secs(10));
    let second = entered_rx.recv_timeout(Duration::from_secs(10));
    // Release even on timeout so a failing assertion cannot strand the verifier's Drop join.
    drop(release_tx);
    assert_ne!(first.unwrap(), second.unwrap());
    // The hook sees channel disconnection as the release signal.
    assert!(findings(verifier).is_empty());
}

#[test]
fn worker_and_scheduler_errors_keep_payload_and_stop_iteration() {
    let fixture = Fixture::new();
    let address = *fixture.accounts.keys().next().unwrap();
    let hook: StorageHook = Arc::new(move |selected| {
        if selected == address {
            Err(DatabaseError::Other("storage rebind failed".into()))
        } else {
            Ok(())
        }
    });
    let mut verifier = fixture.verifier(hook, fixture.jobs(), 1, Default::default());
    let error = verifier.find(|item| item.is_err()).expect("worker failure must be yielded");
    assert_original_error(error, "storage rebind failed");
    assert!(verifier.next().is_none());
    let mut jobs = fixture.jobs();
    jobs.insert(0, Err(DatabaseError::Other("address scan failed".into())));
    let mut verifier = fixture.verifier(no_hook(), jobs, 1, Default::default());
    assert_original_error(verifier.next().unwrap(), "address scan failed");
    assert!(verifier.next().is_none());
}

#[test]
fn dropping_verifier_joins_workers_with_more_findings_than_channel_capacity() {
    let mut fixture = Fixture::new();
    let orphan = B256::repeat_byte(0xff);
    fixture.slots.insert(orphan, BTreeMap::new());
    let stale = (0..(FINDINGS_CAPACITY + 128))
        .map(|index| (Nibbles::unpack(B256::from(U256::from(index))), BranchNodeCompact::default()))
        .collect();
    fixture.storage_nodes.insert(orphan, stale);
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut verifier = fixture.verifier(no_hook(), fixture.jobs(), 1, dropped.clone());
    assert!(verifier
        .any(|item| matches!(item, Ok(Output::StorageExtra(address, ..)) if address == orphan)));
    drop(verifier);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        2,
        "all worker-owned resources must be dropped before return"
    );
}

#[test]
fn cancellation_unblocks_a_saturated_findings_sender() {
    let (events, receiver) = crossbeam_channel::bounded(1);
    events.send(Ok(Output::Progress(Nibbles::default()))).unwrap();
    let (cancel, cancel_rx) = crossbeam_channel::bounded(0);
    let context = WorkerContext::new(events, cancel_rx);
    let barrier = Arc::new(Barrier::new(2));
    let entered = barrier.clone();
    let sender = thread::spawn(move || {
        entered.wait();
        context.report_one(Output::Progress(Nibbles::default()))
    });
    barrier.wait();
    drop(cancel);
    assert!(!sender.join().unwrap());
    assert!(receiver.recv().unwrap().is_ok());
    assert!(receiver.recv().is_err());
}
