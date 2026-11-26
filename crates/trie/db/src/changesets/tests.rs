use super::*;
use alloy_primitives::{map::B256Map, U256};
use reth_chainspec::ChainInfo;
use reth_db_api::models::AccountBeforeTx;
use reth_primitives_traits::Account;
use reth_storage_api::BlockHashReader;
use reth_trie::{
    hashed_cursor::mock::MockHashedCursorFactory,
    test_utils::state_root,
    trie_cursor::{mock::MockTrieCursorFactory, noop::NoopTrieCursorFactory},
    BranchNodeCompact, Nibbles,
};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Default)]
struct Snapshot {
    accounts: BTreeMap<Address, Account>,
    slots: BTreeMap<Address, BTreeMap<B256, U256>>,
}
#[derive(Clone)]
struct History {
    accounts: Vec<(BlockNumber, AccountBeforeTx)>,
    slots: Vec<(BlockNumberAddress, StorageEntry)>,
    hashes: BTreeMap<BlockNumber, B256>,
    reads: Arc<AtomicUsize>,
}
impl ChangesetRangeReader for History {
    fn account_changesets_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<Vec<(BlockNumber, AccountBeforeTx)>, ProviderError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(self.accounts.iter().filter(|(block, _)| range.contains(block)).cloned().collect())
    }
    fn storage_changesets_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<Vec<(BlockNumberAddress, StorageEntry)>, ProviderError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .slots
            .iter()
            .filter(|(key, _)| range.contains(&key.block_number()))
            .copied()
            .collect())
    }
}
impl BlockHashReader for History {
    fn block_hash(&self, number: BlockNumber) -> Result<Option<B256>, ProviderError> {
        Ok(self.hashes.get(&number).copied())
    }
    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> Result<Vec<B256>, ProviderError> {
        Ok(self.hashes.range(start..end).map(|(_, hash)| *hash).collect())
    }
}
impl BlockNumReader for History {
    fn chain_info(&self) -> Result<ChainInfo, ProviderError> {
        let (&number, &hash) = self.hashes.last_key_value().unwrap();
        Ok(ChainInfo { best_hash: hash, best_number: number })
    }
    fn best_block_number(&self) -> Result<BlockNumber, ProviderError> {
        Ok(*self.hashes.last_key_value().unwrap().0)
    }
    fn last_block_number(&self) -> Result<BlockNumber, ProviderError> {
        self.best_block_number()
    }
    fn block_number(&self, hash: B256) -> Result<Option<BlockNumber>, ProviderError> {
        Ok(self
            .hashes
            .iter()
            .find_map(|(number, candidate)| (*candidate == hash).then_some(*number)))
    }
}

#[derive(Clone)]
struct Factories {
    trie: MockTrieCursorFactory,
    hashed: MockHashedCursorFactory,
}
impl TrieCursorFactory for Factories {
    type AccountTrieCursor = <MockTrieCursorFactory as TrieCursorFactory>::AccountTrieCursor;
    type StorageTrieCursor = <MockTrieCursorFactory as TrieCursorFactory>::StorageTrieCursor;
    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        self.trie.account_trie_cursor()
    }
    fn storage_trie_cursor(&self, address: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
        self.trie.storage_trie_cursor(address)
    }
}
impl HashedCursorFactory for Factories {
    type AccountCursor = <MockHashedCursorFactory as HashedCursorFactory>::AccountCursor;
    type StorageCursor = <MockHashedCursorFactory as HashedCursorFactory>::StorageCursor;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        self.hashed.hashed_account_cursor()
    }
    fn hashed_storage_cursor(&self, address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        self.hashed.hashed_storage_cursor(address)
    }
}

struct Fixture {
    states: Vec<Snapshot>,
    history: History,
    nodes: Vec<TrieUpdatesSorted>,
    forward: Vec<(u64, Arc<TrieUpdatesSorted>)>,
    factories: Factories,
}
impl Fixture {
    fn new() -> Self {
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let c = Address::repeat_byte(0xaa);
        let mut state = Snapshot {
            accounts: (1u8..=32)
                .map(|i| {
                    (
                        Address::with_last_byte(i),
                        Account { nonce: i as u64, balance: U256::from(i), bytecode_hash: None },
                    )
                })
                .collect(),
            slots: BTreeMap::from([
                (a, (0..64).map(|i| (Self::slot(i), U256::from(i + 1))).collect()),
                (b, (0..16).map(|i| (Self::slot(i), U256::from(i + 1))).collect()),
            ]),
        };
        let mut states = vec![state.clone()];
        state.accounts.get_mut(&a).unwrap().balance += U256::from(10);
        for i in 64..80 {
            state.slots.get_mut(&a).unwrap().insert(Self::slot(i), U256::from(i + 1));
        }
        state.accounts.insert(c, Account::default());
        state.slots.insert(c, (80..96).map(|i| (Self::slot(i), U256::from(i + 1))).collect());
        states.push(state.clone());
        for i in 64..80 {
            state.slots.get_mut(&a).unwrap().remove(&Self::slot(i));
        }
        state.accounts.remove(&c);
        state.slots.remove(&c);
        state.accounts.get_mut(&b).unwrap().balance += U256::ONE;
        states.push(state.clone());
        state.accounts.remove(&a);
        state.slots.remove(&a);
        state.slots.get_mut(&b).unwrap().insert(Self::slot(2), U256::from(500));
        states.push(state.clone());
        state.accounts.insert(a, Account { nonce: 900, ..Default::default() });
        state.slots.insert(a, (100..116).map(|i| (Self::slot(i), U256::from(i + 1))).collect());
        state.slots.get_mut(&b).unwrap().insert(Self::slot(99), U256::ONE);
        states.push(state.clone());
        state.slots.get_mut(&b).unwrap().remove(&Self::slot(99));
        state.slots.get_mut(&a).unwrap().insert(Self::slot(200), U256::ONE);
        states.push(state);
        let addresses = states
            .iter()
            .flat_map(|state| state.accounts.keys().chain(state.slots.keys()))
            .copied()
            .collect::<BTreeSet<_>>();
        let mut accounts = Vec::new();
        let mut slots = Vec::new();
        for (offset, pair) in states.windows(2).enumerate() {
            let block = offset as u64 + 1;
            let before = &pair[0];
            let after = &pair[1];
            for address in &addresses {
                if before.accounts.get(address) != after.accounts.get(address) {
                    accounts.push((
                        block,
                        AccountBeforeTx {
                            address: *address,
                            info: before.accounts.get(address).copied(),
                        },
                    ));
                }
                let keys = before
                    .slots
                    .get(address)
                    .into_iter()
                    .flat_map(|slots| slots.keys())
                    .chain(after.slots.get(address).into_iter().flat_map(|slots| slots.keys()))
                    .copied()
                    .collect::<BTreeSet<_>>();
                for key in keys {
                    let old = before
                        .slots
                        .get(address)
                        .and_then(|slots| slots.get(&key))
                        .copied()
                        .unwrap_or_default();
                    let new = after
                        .slots
                        .get(address)
                        .and_then(|slots| slots.get(&key))
                        .copied()
                        .unwrap_or_default();
                    if old != new {
                        slots.push((
                            BlockNumberAddress((block, *address)),
                            StorageEntry { key, value: old },
                        ));
                    }
                }
            }
        }
        let history = History {
            accounts,
            slots,
            hashes: (0u64..=5).map(|block| (block, keccak256(block.to_be_bytes()))).collect(),
            reads: Default::default(),
        };
        let mut nodes = Vec::new();
        let mut hashed_views = Vec::new();
        for snapshot in &states {
            let mut storage = addresses
                .iter()
                .map(|address| {
                    (
                        keccak256(address),
                        snapshot
                            .slots
                            .get(address)
                            .into_iter()
                            .flat_map(|slots| slots.iter())
                            .map(|(slot, value)| (keccak256(slot), *value))
                            .collect(),
                    )
                })
                .collect::<B256Map<_>>();
            storage.insert(B256::ZERO, BTreeMap::new());
            let hashed = MockHashedCursorFactory::new(
                snapshot
                    .accounts
                    .iter()
                    .map(|(address, account)| (keccak256(address), *account))
                    .collect(),
                storage,
            );
            let (root, updates) = StateRoot::new(NoopTrieCursorFactory::default(), hashed.clone())
                .root_with_updates()
                .unwrap();
            assert_eq!(root, Self::expected_root(snapshot));
            nodes.push(apply_nodes(&updates.into_sorted(), &TrieUpdatesSorted::default()));
            hashed_views.push(hashed);
        }
        // Original forward updates are independently derived from complete adjacent node maps.
        let mut forward: Vec<(u64, Arc<TrieUpdatesSorted>)> = nodes
            .windows(2)
            .enumerate()
            .map(|(offset, pair)| (offset as u64 + 1, Arc::new(node_diff(&pair[0], &pair[1]))))
            .collect();
        for (offset, pair) in states.windows(2).enumerate() {
            for address in
                pair[0].accounts.keys().filter(|address| !pair[1].accounts.contains_key(*address))
            {
                forward[offset].1 = Arc::new({
                    let mut update = forward[offset].1.as_ref().clone();
                    update.storage_tries.entry(keccak256(address)).or_default().is_deleted = true;
                    update
                });
            }
        }
        let tip = nodes.last().unwrap();
        let mut storages = addresses
            .iter()
            .map(|address| (keccak256(address), BTreeMap::new()))
            .collect::<B256Map<_>>();
        storages.insert(B256::ZERO, BTreeMap::new());
        for (address, storage) in &tip.storage_tries {
            storages.insert(
                *address,
                storage
                    .storage_nodes
                    .iter()
                    .map(|(path, node)| (*path, node.clone().unwrap()))
                    .collect(),
            );
        }
        let factories = Factories {
            trie: MockTrieCursorFactory::new(
                tip.account_nodes
                    .iter()
                    .map(|(path, node)| (*path, node.clone().unwrap()))
                    .collect(),
                storages,
            ),
            hashed: hashed_views.pop().unwrap(),
        };
        Self { states, history, nodes, forward, factories }
    }
    fn slot(number: u64) -> B256 {
        B256::from(U256::from(number))
    }
    fn expected_root(snapshot: &Snapshot) -> B256 {
        state_root(snapshot.accounts.iter().map(|(address, account)| {
            (*address, (*account, snapshot.slots.get(address).cloned().unwrap_or_default()))
        }))
    }
}

fn node_diff(before: &TrieUpdatesSorted, after: &TrieUpdatesSorted) -> TrieUpdatesSorted {
    let old_accounts = before.account_nodes.iter().cloned().collect::<BTreeMap<_, _>>();
    let new_accounts = after.account_nodes.iter().cloned().collect::<BTreeMap<_, _>>();
    let account_nodes = old_accounts
        .keys()
        .chain(new_accounts.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|path| old_accounts.get(path) != new_accounts.get(path))
        .map(|path| (path, new_accounts.get(&path).cloned().flatten()))
        .collect();
    let mut storage_tries = B256Map::default();
    for address in before
        .storage_tries
        .keys()
        .chain(after.storage_tries.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let old = before
            .storage_tries
            .get(&address)
            .map(|storage| storage.storage_nodes.iter().cloned().collect::<BTreeMap<_, _>>())
            .unwrap_or_default();
        let new = after
            .storage_tries
            .get(&address)
            .map(|storage| storage.storage_nodes.iter().cloned().collect::<BTreeMap<_, _>>())
            .unwrap_or_default();
        let nodes = old
            .keys()
            .chain(new.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|path| old.get(path) != new.get(path))
            .map(|path| (path, new.get(&path).cloned().flatten()))
            .collect::<Vec<_>>();
        if !nodes.is_empty() {
            storage_tries.insert(
                address,
                reth_trie::updates::StorageTrieUpdatesSorted {
                    is_deleted: false,
                    storage_nodes: nodes,
                },
            );
        }
    }
    TrieUpdatesSorted::new(account_nodes, storage_tries)
}
fn apply_nodes(base: &TrieUpdatesSorted, updates: &TrieUpdatesSorted) -> TrieUpdatesSorted {
    let mut accounts = base
        .account_nodes
        .iter()
        .filter_map(|(path, node)| node.clone().map(|node| (*path, node)))
        .collect::<BTreeMap<_, _>>();
    for (path, node) in &updates.account_nodes {
        if let Some(node) = node {
            accounts.insert(*path, node.clone());
        } else {
            accounts.remove(path);
        }
    }
    let mut storages = base
        .storage_tries
        .iter()
        .map(|(address, storage)| {
            (
                *address,
                storage
                    .storage_nodes
                    .iter()
                    .filter_map(|(path, node)| node.clone().map(|node| (*path, node)))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (address, storage) in &updates.storage_tries {
        let nodes = storages.entry(*address).or_default();
        if storage.is_deleted {
            nodes.clear();
        }
        for (path, node) in &storage.storage_nodes {
            if let Some(node) = node {
                nodes.insert(*path, node.clone());
            } else {
                nodes.remove(path);
            }
        }
    }
    TrieUpdatesSorted::new(
        accounts.into_iter().map(|(path, node)| (path, Some(node))).collect(),
        storages
            .into_iter()
            .filter(|(_, nodes)| !nodes.is_empty())
            .map(|(address, nodes)| {
                (
                    address,
                    reth_trie::updates::StorageTrieUpdatesSorted {
                        is_deleted: false,
                        storage_nodes: nodes
                            .into_iter()
                            .map(|(path, node)| (path, Some(node)))
                            .collect(),
                    },
                )
            })
            .collect(),
    )
}

impl Fixture {
    fn view_at(&self, index: usize) -> Factories {
        let state = &self.states[index];
        let nodes = &self.nodes[index];
        let addresses = self
            .states
            .iter()
            .flat_map(|state| state.accounts.keys().chain(state.slots.keys()))
            .copied()
            .collect::<BTreeSet<_>>();
        let mut hashed_storage = B256Map::from_iter([(B256::ZERO, BTreeMap::new())]);
        let mut trie_storage = B256Map::from_iter([(B256::ZERO, BTreeMap::new())]);
        for address in addresses {
            let hashed = keccak256(address);
            hashed_storage.insert(
                hashed,
                state
                    .slots
                    .get(&address)
                    .into_iter()
                    .flat_map(|slots| slots.iter())
                    .map(|(slot, value)| (keccak256(slot), *value))
                    .collect(),
            );
            trie_storage.insert(
                hashed,
                nodes
                    .storage_tries
                    .get(&hashed)
                    .into_iter()
                    .flat_map(|storage| storage.storage_nodes.iter())
                    .map(|(path, node)| (*path, node.clone().unwrap()))
                    .collect(),
            );
        }
        Factories {
            trie: MockTrieCursorFactory::new(
                nodes
                    .account_nodes
                    .iter()
                    .map(|(path, node)| (*path, node.clone().unwrap()))
                    .collect(),
                trie_storage,
            ),
            hashed: MockHashedCursorFactory::new(
                state
                    .accounts
                    .iter()
                    .map(|(address, account)| (keccak256(address), *account))
                    .collect(),
                hashed_storage,
            ),
        }
    }
}

#[test]
fn aggregate_range_matches_pre_range_cache_for_wipes_recreation_and_missing_forward_blocks() {
    let fixture = Fixture::new();
    let partial =
        fixture.forward.iter().filter(|(block, _)| block % 2 != 0).cloned().collect::<Vec<_>>();
    for forward in [&[][..], fixture.forward.as_slice(), partial.as_slice()] {
        for start in 1u64..=5 {
            for end in start..=5 {
                let changesets = compute_range_trie_changesets(
                    &fixture.history,
                    &fixture.factories,
                    forward,
                    start..=end,
                    5,
                )
                .unwrap();
                assert_eq!(
                    apply_nodes(&fixture.nodes[end as usize], &changesets),
                    fixture.nodes[start as usize - 1],
                    "range {start}..={end}, forward {:?}",
                    forward.iter().map(|(number, _)| *number).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn persisted_range_retains_transient_nodes_without_executed_blocks() {
    let fixture = Fixture::new();
    let address = keccak256(Address::with_last_byte(1));
    let before = fixture.nodes[0].storage_tries[&address]
        .storage_nodes
        .iter()
        .map(|(path, _)| *path)
        .collect::<BTreeSet<_>>();
    let after = fixture.nodes[2].storage_tries[&address]
        .storage_nodes
        .iter()
        .map(|(path, _)| *path)
        .collect::<BTreeSet<_>>();
    let transient = fixture.nodes[1].storage_tries[&address]
        .storage_nodes
        .iter()
        .map(|(path, _)| *path)
        .filter(|path| !before.contains(path) && !after.contains(path))
        .collect::<Vec<_>>();
    assert!(!transient.is_empty(), "fixture must create then remove real legacy branch paths");
    for forward in [&[][..], &fixture.forward[..2]] {
        let changesets =
            compute_range_trie_changesets(&fixture.history, &fixture.factories, forward, 1..=2, 5)
                .unwrap();
        let storage = &changesets.storage_tries[&address];
        for path in &transient {
            assert!(
                storage.storage_nodes.contains(&(*path, None)),
                "transient {path:?} must be explicitly reverted"
            );
        }
    }
}

#[test]
fn calculated_historical_range_excludes_tail_reverts() {
    let fixture = Fixture::new();
    let post_range = fixture.view_at(2);
    for forward in [&[][..], fixture.forward.as_slice()] {
        let historical =
            compute_range_trie_changesets(&fixture.history, &fixture.factories, forward, 1..=2, 5)
                .unwrap();
        let at_end =
            compute_range_trie_changesets(&fixture.history, &post_range, forward, 1..=2, 2)
                .unwrap();
        assert_eq!(
            historical, at_end,
            "later storage wipe/recreation paths must not leak into requested range"
        );
    }
}

#[test]
fn range_bounds_and_forward_order_are_checked_without_overflow() {
    let fixture = Fixture::new();
    let empty = compute_range_trie_changesets(
        &fixture.history,
        &fixture.factories,
        &[],
        RangeInclusive::new(5, 4),
        5,
    )
    .unwrap();
    assert!(empty.is_empty());
    assert!(matches!(
        compute_range_trie_changesets(&fixture.history, &fixture.factories, &[], 1..=6, 5),
        Err(ProviderError::InsufficientChangesets { requested: 6, .. })
    ));
    for forward in [
        vec![fixture.forward[1].clone(), fixture.forward[0].clone()],
        vec![fixture.forward[0].clone(), fixture.forward[0].clone()],
    ] {
        assert!(compute_range_trie_changesets(
            &fixture.history,
            &fixture.factories,
            &forward,
            1..=2,
            5
        )
        .is_err());
    }
    let address = Address::with_last_byte(1);
    let history = History {
        accounts: vec![(u64::MAX, AccountBeforeTx { address, info: None })],
        slots: vec![(
            BlockNumberAddress((u64::MAX, address)),
            StorageEntry { key: B256::ZERO, value: U256::ONE },
        )],
        hashes: BTreeMap::from([(u64::MAX, B256::ZERO)]),
        reads: Default::default(),
    };
    let state = load_legacy_state_reverts(&history, u64::MAX..=u64::MAX).unwrap();
    assert!(state.accounts.destroyed_accounts.contains(&keccak256(address)));
    assert_eq!(
        state.storages[&keccak256(address)].non_zero_valued_slots,
        vec![(keccak256(B256::ZERO), U256::ONE)]
    );
    // A max-height no-op block must terminate at that height, without end+1 or start-1 overflow.
    let no_changes = History { accounts: Vec::new(), slots: Vec::new(), ..history };
    let max_result = compute_range_trie_changesets(
        &no_changes,
        &fixture.factories,
        &[],
        u64::MAX..=u64::MAX,
        u64::MAX,
    )
    .unwrap();
    assert_eq!(apply_nodes(&fixture.nodes[5], &max_result), fixture.nodes[5]);
}

#[test]
fn cached_range_merge_keeps_oldest_revert_values_and_reuses_exact_result() {
    let fixture = Fixture::new();
    let cache = ChangesetCache::new();
    let path = Nibbles::from_nibbles([1, 2]);
    let older = BranchNodeCompact::new(0b0001, 0, 0, vec![], None);
    let newer = BranchNodeCompact::new(0b0010, 0, 0, vec![], None);
    for (block, node) in [(1, older.clone()), (2, newer)] {
        cache.inner.write().insert(
            ChangesetRangeKey::new(block, block, fixture.history.hashes[&block]),
            Arc::new(TrieUpdatesSorted::new(vec![(path, Some(node))], B256Map::default())),
        );
    }
    let reads = fixture.history.reads.load(Ordering::Relaxed);
    let result =
        cache.get_or_compute_range(&fixture.history, &fixture.factories, &[], 1..=2).unwrap();
    assert_eq!(result.account_nodes, vec![(path, Some(older))]);
    assert_eq!(fixture.history.reads.load(Ordering::Relaxed), reads);
    let hit = cache.get_or_compute_range(&fixture.history, &fixture.factories, &[], 1..=2).unwrap();
    assert!(Arc::ptr_eq(&result, &hit));
}

#[test]
fn cache_computation_matches_range_oracle_and_chain_hash_changes_force_a_miss() {
    let fixture = Fixture::new();
    let cache = ChangesetCache::new();
    let result = cache
        .get_or_compute_range(&fixture.history, &fixture.factories, &fixture.forward, 1..=4)
        .unwrap();
    assert_eq!(apply_nodes(&fixture.nodes[4], &result), fixture.nodes[0]);
    let hit = cache.get_or_compute_range(&fixture.history, &fixture.factories, &[], 1..=4).unwrap();
    assert!(Arc::ptr_eq(&result, &hit));
    let mut alternate = fixture.history.clone();
    alternate.hashes.insert(4, B256::repeat_byte(0xfa));
    let reads = alternate.reads.load(Ordering::Relaxed);
    let miss = cache
        .get_or_compute_range(&alternate, &fixture.factories, &fixture.forward, 1..=4)
        .unwrap();
    assert!(!Arc::ptr_eq(&result, &miss));
    assert!(alternate.reads.load(Ordering::Relaxed) > reads);
    assert_eq!(result, miss);
    let block = cache.get_or_compute(&fixture.history, &fixture.factories, 3).unwrap();
    assert_eq!(cache.get(fixture.history.hashes[&3], 3).unwrap(), block);
    assert_eq!(apply_nodes(&fixture.nodes[3], &block), fixture.nodes[2]);
}

fn create_test_changesets() -> Arc<TrieUpdatesSorted> {
    Arc::new(TrieUpdatesSorted::default())
}
fn insert_test_changesets(
    cache: &mut ChangesetCacheInner,
    hash: B256,
    number: u64,
    value: Arc<TrieUpdatesSorted>,
) {
    cache.insert(ChangesetRangeKey::new(number, number, hash), value);
}
fn get_test_changesets(
    cache: &ChangesetCacheInner,
    hash: B256,
    number: u64,
) -> Option<Arc<TrieUpdatesSorted>> {
    cache.get(&ChangesetRangeKey::new(number, number, hash))
}

#[test]
fn test_insert_and_retrieve_single_entry() {
    let mut cache = ChangesetCacheInner::default();
    let hash = B256::random();
    let changesets = create_test_changesets();

    insert_test_changesets(&mut cache, hash, 100, Arc::clone(&changesets));

    // Should be able to retrieve it
    let retrieved = get_test_changesets(&cache, hash, 100);
    assert!(retrieved.is_some());
    assert_eq!(cache.entries.len(), 1);
}

#[test]
fn test_insert_multiple_entries() {
    let mut cache = ChangesetCacheInner::default();

    // Insert 10 blocks
    let mut hashes = Vec::new();
    for i in 0..10 {
        let hash = B256::random();
        insert_test_changesets(&mut cache, hash, 100 + i, create_test_changesets());
        hashes.push((100 + i, hash));
    }

    // Should be able to retrieve all
    assert_eq!(cache.entries.len(), 10);
    for (block_number, hash) in hashes {
        assert!(get_test_changesets(&cache, hash, block_number).is_some());
    }
}

#[test]
fn test_eviction_when_explicitly_called() {
    let mut cache = ChangesetCacheInner::default();

    // Insert 15 blocks (0-14)
    let mut hashes = Vec::new();
    for i in 0..15 {
        let hash = B256::random();
        insert_test_changesets(&mut cache, hash, i, create_test_changesets());
        hashes.push((i, hash));
    }

    // All blocks should be present (no automatic eviction)
    assert_eq!(cache.entries.len(), 15);

    // Explicitly evict blocks < 4
    cache.evict(4);

    // Blocks 0-3 should be evicted
    assert_eq!(cache.entries.len(), 11); // blocks 4-14 = 11 blocks

    // Verify blocks 0-3 are evicted
    for i in 0..4 {
        assert!(
            get_test_changesets(&cache, hashes[i as usize].1, i).is_none(),
            "Block {} should be evicted",
            i
        );
    }

    // Verify blocks 4-14 are still present
    for i in 4..15 {
        assert!(
            get_test_changesets(&cache, hashes[i as usize].1, i).is_some(),
            "Block {} should be present",
            i
        );
    }
}

#[test]
fn test_eviction_with_persistence_watermark() {
    let mut cache = ChangesetCacheInner::default();

    // Insert blocks 100-165
    let mut hashes = HashMap::new();
    for i in 100..=165 {
        let hash = B256::random();
        insert_test_changesets(&mut cache, hash, i, create_test_changesets());
        hashes.insert(i, hash);
    }

    // All blocks should be present (no automatic eviction)
    assert_eq!(cache.entries.len(), 66);

    // Simulate persistence up to block 164, with 64-block retention window
    // Eviction threshold = 164 - 64 = 100
    cache.evict(100);

    // Blocks 100-165 should remain (66 blocks)
    assert_eq!(cache.entries.len(), 66);

    // Simulate persistence up to block 165
    // Eviction threshold = 165 - 64 = 101
    cache.evict(101);

    // Blocks 101-165 should remain (65 blocks)
    assert_eq!(cache.entries.len(), 65);
    assert!(get_test_changesets(&cache, hashes[&100], 100).is_none());
    assert!(get_test_changesets(&cache, hashes[&101], 101).is_some());
}

#[test]
fn test_out_of_order_inserts_with_explicit_eviction() {
    let mut cache = ChangesetCacheInner::default();

    // Insert blocks in random order
    let hash_10 = B256::random();
    insert_test_changesets(&mut cache, hash_10, 10, create_test_changesets());

    let hash_5 = B256::random();
    insert_test_changesets(&mut cache, hash_5, 5, create_test_changesets());

    let hash_15 = B256::random();
    insert_test_changesets(&mut cache, hash_15, 15, create_test_changesets());

    let hash_3 = B256::random();
    insert_test_changesets(&mut cache, hash_3, 3, create_test_changesets());

    // All blocks should be present (no automatic eviction)
    assert_eq!(cache.entries.len(), 4);

    // Explicitly evict blocks < 5
    cache.evict(5);

    assert!(get_test_changesets(&cache, hash_3, 3).is_none(), "Block 3 should be evicted");
    assert!(get_test_changesets(&cache, hash_5, 5).is_some(), "Block 5 should be present");
    assert!(get_test_changesets(&cache, hash_10, 10).is_some(), "Block 10 should be present");
    assert!(get_test_changesets(&cache, hash_15, 15).is_some(), "Block 15 should be present");
}

#[test]
fn test_multiple_blocks_same_number() {
    let mut cache = ChangesetCacheInner::default();

    // Insert multiple blocks with same number (side chains)
    let hash_1a = B256::random();
    let hash_1b = B256::random();
    insert_test_changesets(&mut cache, hash_1a, 100, create_test_changesets());
    insert_test_changesets(&mut cache, hash_1b, 100, create_test_changesets());

    // Both should be retrievable
    assert!(get_test_changesets(&cache, hash_1a, 100).is_some());
    assert!(get_test_changesets(&cache, hash_1b, 100).is_some());
    assert_eq!(cache.entries.len(), 2);
}

#[test]
fn test_ranges_with_same_numbers_and_different_end_hashes_are_distinct() {
    let mut cache = ChangesetCacheInner::default();
    let path = Nibbles::from_nibbles_unchecked([0x01]);
    let hash_a = B256::with_last_byte(1);
    let hash_b = B256::with_last_byte(2);
    let key_a = ChangesetRangeKey::new(10, 20, hash_a);
    let key_b = ChangesetRangeKey::new(10, 20, hash_b);
    let changesets_a = Arc::new(TrieUpdatesSorted::new(
        vec![(path, Some(BranchNodeCompact::new(0b0001, 0, 0, vec![], None)))],
        B256Map::default(),
    ));
    let changesets_b = Arc::new(TrieUpdatesSorted::new(
        vec![(path, Some(BranchNodeCompact::new(0b0010, 0, 0, vec![], None)))],
        B256Map::default(),
    ));

    cache.insert(key_a, Arc::clone(&changesets_a));
    cache.insert(key_b, Arc::clone(&changesets_b));

    assert_eq!(cache.entries.len(), 2);
    assert_eq!(cache.get(&key_a).unwrap().account_nodes_ref(), changesets_a.account_nodes_ref());
    assert_eq!(cache.get(&key_b).unwrap().account_nodes_ref(), changesets_b.account_nodes_ref());

    cache.evict(11);
    assert!(cache.get(&key_a).is_none());
    assert!(cache.get(&key_b).is_none());
}

#[test]
fn test_eviction_removes_all_side_chains() {
    let mut cache = ChangesetCacheInner::default();

    // Insert multiple blocks at same height (side chains)
    let hash_10a = B256::random();
    let hash_10b = B256::random();
    let hash_10c = B256::random();
    insert_test_changesets(&mut cache, hash_10a, 10, create_test_changesets());
    insert_test_changesets(&mut cache, hash_10b, 10, create_test_changesets());
    insert_test_changesets(&mut cache, hash_10c, 10, create_test_changesets());

    let hash_20 = B256::random();
    insert_test_changesets(&mut cache, hash_20, 20, create_test_changesets());

    assert_eq!(cache.entries.len(), 4);

    // Evict blocks < 15 - should remove all three side chains at height 10
    cache.evict(15);

    assert_eq!(cache.entries.len(), 1);
    assert!(get_test_changesets(&cache, hash_10a, 10).is_none());
    assert!(get_test_changesets(&cache, hash_10b, 10).is_none());
    assert!(get_test_changesets(&cache, hash_10c, 10).is_none());
    assert!(get_test_changesets(&cache, hash_20, 20).is_some());
}

#[derive(Clone, Copy)]
struct DbView<'a, TX>(&'a TX);
impl<TX: DbTx> TrieCursorFactory for DbView<'_, TX> {
    type AccountTrieCursor = crate::DatabaseAccountTrieCursor<TX::Cursor<tables::AccountsTrie>>;
    type StorageTrieCursor = crate::DatabaseStorageTrieCursor<TX::DupCursor<tables::StoragesTrie>>;
    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        crate::DatabaseTrieCursorFactory::new(self.0).account_trie_cursor()
    }
    fn storage_trie_cursor(&self, address: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
        crate::DatabaseTrieCursorFactory::new(self.0).storage_trie_cursor(address)
    }
}
impl<TX: DbTx> HashedCursorFactory for DbView<'_, TX> {
    type AccountCursor = crate::DatabaseHashedAccountCursor<TX::Cursor<tables::HashedAccounts>>;
    type StorageCursor = crate::DatabaseHashedStorageCursor<TX::DupCursor<tables::HashedStorages>>;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        crate::DatabaseHashedCursorFactory::new(self.0).hashed_account_cursor()
    }
    fn hashed_storage_cursor(&self, address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        crate::DatabaseHashedCursorFactory::new(self.0).hashed_storage_cursor(address)
    }
}

#[test]
fn actual_v1_snapshot_range_reverts_ignore_later_commits_and_support_max_block() {
    use reth_db_api::{database::Database, transaction::DbTxMut};
    use reth_trie::{StorageTrieEntry, StoredNibbles, StoredNibblesSubKey};
    let fixture = Fixture::new();
    let temp = reth_db::test_utils::create_test_rw_db();
    let db = temp.db();
    let max_address = Address::repeat_byte(0xff);
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        for (block, account) in &fixture.history.accounts {
            tx.put::<tables::AccountChangeSets>(*block, account.clone()).unwrap();
        }
        for (key, storage) in &fixture.history.slots {
            tx.put::<tables::StorageChangeSets>(*key, *storage).unwrap();
        }
        tx.put::<tables::AccountChangeSets>(
            u64::MAX,
            AccountBeforeTx { address: max_address, info: None },
        )
        .unwrap();
        tx.put::<tables::StorageChangeSets>(
            BlockNumberAddress((u64::MAX, max_address)),
            StorageEntry { key: B256::ZERO, value: U256::ONE },
        )
        .unwrap();
        for (address, account) in &fixture.states[5].accounts {
            tx.put::<tables::HashedAccounts>(keccak256(address), *account).unwrap();
        }
        for (address, slots) in &fixture.states[5].slots {
            for (slot, value) in slots {
                tx.put::<tables::HashedStorages>(
                    keccak256(address),
                    StorageEntry { key: keccak256(slot), value: *value },
                )
                .unwrap();
            }
        }
        for (path, node) in &fixture.nodes[5].account_nodes {
            tx.put::<tables::AccountsTrie>(StoredNibbles(*path), node.clone().unwrap()).unwrap();
        }
        for (address, storage) in &fixture.nodes[5].storage_tries {
            for (path, node) in &storage.storage_nodes {
                tx.put::<tables::StoragesTrie>(
                    *address,
                    StorageTrieEntry {
                        nibbles: StoredNibblesSubKey(*path),
                        node: node.clone().unwrap(),
                    },
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
    });
    assert!(LegacyChangesetReader::new(&db.tx().unwrap()).is_err());
    let captured = db.snapshot(true).unwrap().unwrap();
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        tx.put::<tables::HashedAccounts>(
            keccak256(Address::with_last_byte(1)),
            Account { balance: U256::from(99999), ..Default::default() },
        )
        .unwrap();
        tx.put::<tables::AccountChangeSets>(
            1,
            AccountBeforeTx { address: Address::with_last_byte(1), info: None },
        )
        .unwrap();
        tx.commit().unwrap();
    });
    // Both actual database factories and the V1 loader open on the captured pre-write version.
    let tx = db.tx_with_snapshot(Some(captured)).unwrap();
    let reader = LegacyChangesetReader::new(&tx).unwrap();
    assert_eq!(
        load_legacy_state_reverts(&reader, 1..=5).unwrap(),
        load_legacy_state_reverts(&fixture.history, 1..=5).unwrap()
    );
    let result =
        compute_range_trie_changesets(&reader, &DbView(&tx), &fixture.forward, 1..=4, 5).unwrap();
    assert_eq!(apply_nodes(&fixture.nodes[4], &result), fixture.nodes[0]);
    let max = load_legacy_state_reverts(&reader, u64::MAX..=u64::MAX).unwrap();
    assert!(max.accounts.destroyed_accounts.contains(&keccak256(max_address)));
    assert_eq!(
        max.storages[&keccak256(max_address)].non_zero_valued_slots,
        vec![(keccak256(B256::ZERO), U256::ONE)]
    );
}

#[test]
fn v1_reader_preserves_asymmetric_prune_bounds_on_the_captured_transaction() {
    use reth_db_api::{database::Database, transaction::DbTxMut};
    use reth_prune_types::{PruneCheckpoint, PruneMode};
    let temp = reth_db::test_utils::create_test_rw_db();
    let db = temp.db();
    let checkpoint = |block| PruneCheckpoint {
        block_number: Some(block),
        tx_number: None,
        prune_mode: PruneMode::Full,
    };
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        tx.put::<tables::PruneCheckpoints>(PruneSegment::AccountHistory, checkpoint(2)).unwrap();
        tx.put::<tables::PruneCheckpoints>(PruneSegment::StorageHistory, checkpoint(4)).unwrap();
        tx.commit().unwrap();
    });
    let captured = db.snapshot(true).unwrap().unwrap();
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        tx.put::<tables::PruneCheckpoints>(PruneSegment::AccountHistory, checkpoint(10)).unwrap();
        tx.put::<tables::PruneCheckpoints>(PruneSegment::StorageHistory, checkpoint(u64::MAX))
            .unwrap();
        tx.commit().unwrap();
    });
    let tx = db.tx_with_snapshot(Some(captured)).unwrap();
    let reader = LegacyChangesetReader::new(&tx).unwrap();
    assert!(
        matches!(reader.account_changesets_range(2..=5), Err(ProviderError::InsufficientChangesets { requested: 2, available }) if available == (3..=u64::MAX))
    );
    assert!(reader.account_changesets_range(3..=5).unwrap().is_empty());
    assert!(
        matches!(reader.storage_changesets_range(4..=5), Err(ProviderError::InsufficientChangesets { requested: 4, available }) if available == (5..=u64::MAX))
    );
    assert!(reader.storage_changesets_range(5..=5).unwrap().is_empty());
    assert!(reader.account_changesets_range(RangeInclusive::new(9, 8)).unwrap().is_empty());
    let current = db.tx_with_snapshot(db.snapshot(true).unwrap()).unwrap();
    let current_reader = LegacyChangesetReader::new(&current).unwrap();
    assert!(
        matches!(current_reader.storage_changesets_range(u64::MAX..=u64::MAX), Err(ProviderError::InsufficientChangesets { requested: u64::MAX, available }) if available.is_empty())
    );
}

#[test]
fn source_errors_and_missing_canonical_hashes_are_not_cached_as_empty_reverts() {
    struct FailingReader;
    impl ChangesetRangeReader for FailingReader {
        fn account_changesets_range(
            &self,
            _range: RangeInclusive<u64>,
        ) -> Result<Vec<(u64, AccountBeforeTx)>, ProviderError> {
            Err(DatabaseError::Other("account changeset read failed".into()).into())
        }
        fn storage_changesets_range(
            &self,
            _range: RangeInclusive<u64>,
        ) -> Result<Vec<(BlockNumberAddress, StorageEntry)>, ProviderError> {
            unreachable!("must stop on the first reader error")
        }
    }
    let fixture = Fixture::new();
    assert!(
        matches!(compute_range_trie_changesets(&FailingReader, &fixture.factories, &[], 1..=2, 5), Err(ProviderError::Database(DatabaseError::Other(message))) if message == "account changeset read failed")
    );
    let mut missing = fixture.history.clone();
    missing.hashes.remove(&4);
    let cache = ChangesetCache::new();
    assert!(cache.get_or_compute_range(&missing, &fixture.factories, &[], 1..=4).is_err());
    assert!(cache.inner.read().entries.is_empty());
}
