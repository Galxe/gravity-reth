#![allow(clippy::type_complexity)]

use core::ops::RangeInclusive;

use alloy_primitives::{
    keccak256,
    map::{hash_map, B256Map, HashMap},
    BlockNumber, Bytes, B256, U256,
};
use alloy_rlp::{encode_fixed_size, Decodable};
use once_cell::sync::OnceCell;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    models::{AccountBeforeTx, BlockNumberAddress},
    tables,
    transaction::DbTx,
};

use parking_lot::Mutex;
use reth_primitives_traits::{Account, StorageEntry};
use reth_storage_api::PersistBlockCache;
use reth_storage_errors::{db::DatabaseError, provider::ProviderResult};
use reth_trie::{
    nested_trie::{Node, Trie, TrieReader, MIN_PARALLEL_NODES},
    updates::TrieUpdatesV2,
    HashedPostState, HashedStorage, MultiProof, MultiProofTargets, Nibbles, StorageMultiProof,
    StorageTrieUpdatesV2, StoredNibbles, StoredNibblesSubKey, TrieAccount, TrieNode,
};

/// Storage trie node reader
#[allow(missing_debug_implementations)]
pub struct StorageTrieReader<C> {
    hashed_address: B256,
    cursor: C,
    cache: Option<PersistBlockCache>,
}

impl<C> StorageTrieReader<C> {
    /// Create a new `StorageTrieReader`
    pub const fn new(cursor: C, hashed_address: B256, cache: Option<PersistBlockCache>) -> Self {
        Self { cursor, hashed_address, cache }
    }
}

impl<C> TrieReader for StorageTrieReader<C>
where
    C: DbCursorRO<tables::StoragesTrieV2> + DbDupCursorRO<tables::StoragesTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        if let Some(cache) = &self.cache {
            // A cache hit is authoritative: it is either a live node or a tombstone
            // (`Some(None)`), both of which shadow the DB so a removed node is never re-read
            // from a not-yet-pruned DB.
            if let Some(value) = cache.trie_storage(&self.hashed_address, path) {
                return Ok(value);
            }
        }
        let path = StoredNibblesSubKey(*path);
        Ok(self
            .cursor
            .get_by_key_subkey(self.hashed_address, path.clone())?
            .filter(|e| e.path == path)
            .map(|e| e.node.into()))
    }
}

/// Storage trie node reader that can be forced to behave as an empty trie.
///
/// When an account is wiped (self-destructed and possibly recreated in the same block) the
/// storage trie must be rebuilt from scratch: any on-disk or cached node belongs to the
/// destroyed incarnation and must be ignored. [`MaybeEmptyStorageReader::Empty`] returns `None`
/// for every path so the trie starts empty and only the freshly written slots are inserted.
#[allow(missing_debug_implementations)]
pub enum MaybeEmptyStorageReader<C> {
    /// Always reads nothing (used for wiped storage tries).
    Empty,
    /// Reads from the backing storage trie (on-disk + cache).
    Storage(StorageTrieReader<C>),
}

impl<C> TrieReader for MaybeEmptyStorageReader<C>
where
    C: DbCursorRO<tables::StoragesTrieV2> + DbDupCursorRO<tables::StoragesTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        match self {
            Self::Empty => Ok(None),
            Self::Storage(reader) => reader.read(path),
        }
    }
}

/// Account trie node reader
#[allow(missing_debug_implementations)]
pub struct AccountTrieReader<C>(C, Option<PersistBlockCache>);

impl<C> AccountTrieReader<C> {
    /// Create a new `AccountTrieReader`
    pub const fn new(cursor: C, cache: Option<PersistBlockCache>) -> Self {
        Self(cursor, cache)
    }
}

impl<C> TrieReader for AccountTrieReader<C>
where
    C: DbCursorRO<tables::AccountsTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        if let Some(cache) = &self.1 {
            // A cache hit is authoritative (live node or tombstone); both shadow the DB.
            if let Some(value) = cache.trie_account(path) {
                return Ok(value);
            }
        }
        Ok(self.0.get(StoredNibbles(*path))?.map(|(_, value)| value.into()))
    }
}

/// Root hash for nested trie
#[derive(Debug)]
pub struct NestedStateRoot<'tx, Tx>
where
    Tx: DbTx,
{
    tx: &'tx Tx,
    cache: Option<PersistBlockCache>,
}

impl<'tx, Tx> NestedStateRoot<'tx, Tx>
where
    Tx: DbTx,
{
    /// Create a new `NestedStateRoot`
    pub const fn new(tx: &'tx Tx, cache: Option<PersistBlockCache>) -> Self {
        Self { tx, cache }
    }

    /// Compatible with origin `BranchNodeCompact` trie
    /// Like [`Self::read_hashed_state`] for a block range, but with the changesets supplied
    /// by the caller — required when changesets live in static files rather than the database
    /// tables this function walks.
    pub fn read_hashed_state_from_changesets(
        &self,
        account_changesets: impl IntoIterator<Item = (BlockNumber, AccountBeforeTx)>,
        storage_changesets: impl IntoIterator<Item = (BlockNumberAddress, StorageEntry)>,
    ) -> ProviderResult<HashedPostState> {
        let mut accounts = HashMap::default();
        let mut storages: B256Map<HashedStorage> = HashMap::default();
        let tx = self.tx;

        let mut account_hashed_state_cursor = tx.cursor_read::<tables::HashedAccounts>()?;
        for (_, AccountBeforeTx { address, .. }) in account_changesets {
            let hashed_address = keccak256(address);
            if let hash_map::Entry::Vacant(e) = accounts.entry(hashed_address) {
                let account = account_hashed_state_cursor.seek_exact(hashed_address)?;
                e.insert(account.map(|a| a.1));
            }
        }

        let mut storage_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
        for (BlockNumberAddress((_, address)), entry) in storage_changesets {
            let hashed_address = keccak256(address);
            if let hash_map::Entry::Vacant(e) = accounts.entry(hashed_address) {
                let account = account_hashed_state_cursor.seek_exact(hashed_address)?;
                e.insert(account.map(|a| a.1));
            }
            let hashed_slot = keccak256(entry.key);
            let slot_value = storage_cursor
                .get_by_key_subkey(hashed_address, hashed_slot)?
                .map(|s| s.value)
                .unwrap_or(U256::ZERO);
            storages.entry(hashed_address).or_default().storage.insert(hashed_slot, slot_value);
        }

        Ok(HashedPostState { accounts, storages })
    }

    /// Reads the hashed post-state for the given block range by walking the account and
    /// storage changeset tables. When `range` is `None`, reads the full hashed state from
    /// the `HashedAccounts`/`HashedStorages` tables instead.
    pub fn read_hashed_state(
        &self,
        range: Option<RangeInclusive<BlockNumber>>,
    ) -> ProviderResult<HashedPostState> {
        let mut accounts = HashMap::default();
        let mut storages: B256Map<HashedStorage> = HashMap::default();
        let tx = self.tx;
        if let Some(range) = range {
            // Walk account changeset and insert account prefixes.
            let mut account_changeset_cursor = tx.cursor_read::<tables::AccountChangeSets>()?;
            let mut account_hashed_state_cursor = tx.cursor_read::<tables::HashedAccounts>()?;
            for account_entry in account_changeset_cursor.walk_range(range.clone())? {
                let (_, AccountBeforeTx { address, .. }) = account_entry?;
                let hashed_address = keccak256(address);
                if let hash_map::Entry::Vacant(e) = accounts.entry(hashed_address) {
                    let account = account_hashed_state_cursor.seek_exact(hashed_address)?;
                    e.insert(account.map(|a| a.1));
                }
            }

            // Walk storage changeset and insert storage prefixes as well as account prefixes if
            // missing from the account prefix set.
            let mut storage_changeset_cursor = tx.cursor_dup_read::<tables::StorageChangeSets>()?;
            let mut storage_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
            let storage_range = BlockNumberAddress::range(range);
            for storage_entry in storage_changeset_cursor.walk_range(storage_range)? {
                let (BlockNumberAddress((_, address)), entry) = storage_entry?;
                let hashed_address = keccak256(address);
                if let hash_map::Entry::Vacant(e) = accounts.entry(hashed_address) {
                    let account = account_hashed_state_cursor.seek_exact(hashed_address)?;
                    e.insert(account.map(|a| a.1));
                }
                let hashed_slot = keccak256(entry.key);
                let slot_value = storage_cursor
                    .get_by_key_subkey(hashed_address, hashed_slot)?
                    .map(|s| s.value)
                    .unwrap_or(U256::ZERO);
                storages.entry(hashed_address).or_default().storage.insert(hashed_slot, slot_value);
            }
        } else {
            let mut account_cursor = tx.cursor_read::<tables::HashedAccounts>()?;
            let account_walker = account_cursor.walk(None)?;
            for account in account_walker {
                let (hashed_address, account) = account?;
                accounts.insert(hashed_address, Some(account));
            }

            let mut storage_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
            let storage_walker = storage_cursor.walk(None)?;
            for storage in storage_walker {
                let (hashed_address, entry) = storage?;
                storages.entry(hashed_address).or_default().storage.insert(entry.key, entry.value);
            }
        }

        Ok(HashedPostState { accounts, storages })
    }
}

impl<'tx, Tx> NestedStateRoot<'tx, Tx>
where
    Tx: DbTx,
{
    /// Generate a V2 multiproof over a consistent base trie plus the supplied hashed overlay.
    /// The caller must keep the base trie, hashed state, and historical reverts at one height.
    /// Storage updates require a corresponding entry in `accounts` to change the root.
    /// Branch masks are omitted, as in the default legacy multiproof mode; the returned nodes
    /// support account and storage proof extraction, including absence proofs.
    pub fn multiproof(
        &self,
        reverted_state: &HashedPostState,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        self.multiproof_with_root(reverted_state, targets).map(|(_, proof)| proof)
    }

    /// Calculate the V2 state root and multiproof in one trie traversal.
    pub fn multiproof_with_root(
        &self,
        reverted_state: &HashedPostState,
        targets: MultiProofTargets,
    ) -> ProviderResult<(B256, MultiProof)> {
        self.calculate_and_proof(reverted_state, targets).map(|(root, _, proof)| (root, proof))
    }

    /// Calculate only the V2 state root after applying the hashed-state overlay.
    pub fn root(&self, hashed_state: &HashedPostState) -> ProviderResult<B256> {
        self.calculate(hashed_state).map(|r| r.0)
    }

    /// Calculate the root hash of nested trie.
    /// Storage updates without a corresponding account entry are ignored.
    pub fn calculate(
        &self,
        hashed_state: &HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdatesV2)> {
        self.calculate_and_proof(hashed_state, Default::default()).map(|r| (r.0, r.1))
    }

    /// Calculate a V2 storage root after applying a hashed storage overlay.
    pub fn storage_root(
        &self,
        hashed_address: B256,
        storage: &HashedStorage,
    ) -> ProviderResult<B256> {
        self.storage_proof(hashed_address, storage, &[]).map(|proof| proof.root)
    }

    fn calculate_and_proof(
        &self,
        hashed_state: &HashedPostState,
        targets: MultiProofTargets,
    ) -> ProviderResult<(B256, TrieUpdatesV2, MultiProof)> {
        let need_update = targets.is_empty();
        let trie_update = Mutex::new(TrieUpdatesV2::default());
        let updated_account_nodes: [Mutex<Vec<(Nibbles, Option<Node>)>>; 16] = Default::default();
        let mut partitioned_accounts: [Vec<(&B256, &Option<Account>)>; 16] = Default::default();
        let HashedPostState { accounts: hashed_accounts, storages: hashed_storages } = hashed_state;

        for (hashed_address, account) in hashed_accounts {
            let index = (hashed_address[0] >> 4) as usize;
            partitioned_accounts[index].push((hashed_address, account));
        }
        let abort = OnceCell::new();
        rayon::scope(|scope| {
            for partition in partitioned_accounts {
                if partition.is_empty() {
                    continue;
                }
                scope.spawn(|_| {
                    let wrap = || -> ProviderResult<()> {
                        let index = (partition[0].0[0] >> 4) as usize;
                        let mut updated_account_nodes = updated_account_nodes[index].lock();
                        for (hashed_address, account) in partition {
                            let hashed_address = *hashed_address;
                            let account = *account;
                            let path = Nibbles::unpack(hashed_address);
                            let deleted_storage = || {
                                if need_update {
                                    trie_update
                                        .lock()
                                        .storage_tries
                                        .insert(hashed_address, StorageTrieUpdatesV2::deleted());
                                }
                            };
                            if let Some(account) = account {
                                let storage = hashed_storages.get(&hashed_address);
                                // If the account was wiped (self-destructed, and possibly
                                // recreated in the same block) the storage trie must be rebuilt
                                // from scratch: the on-disk / cached nodes belong to the
                                // destroyed incarnation and must not be read. We build the trie
                                // with an empty reader and flag the update as `is_deleted` so the
                                // writer drops all previous nodes before applying the rebuilt
                                // ones. Note: the recreated slots are still applied below, which
                                // is exactly what was dropped before this fix.
                                let wiped = storage.is_some_and(|storage| storage.wiped);

                                let mut updated_storage_nodes: [Vec<(Nibbles, Option<Node>)>; 16] =
                                    Default::default();
                                // only make the large storage trie parallel
                                let parallel = storage.is_some_and(|storage| {
                                    storage.storage.len() > MIN_PARALLEL_NODES
                                });
                                if let Some(storage) = storage {
                                    for (hashed_slot, value) in &storage.storage {
                                        let nibbles = Nibbles::unpack(*hashed_slot);
                                        let index = nibbles.get_unchecked(0) as usize;
                                        let value = if value.is_zero() {
                                            None
                                        } else {
                                            let value = encode_fixed_size(value);
                                            Some(Node::ValueNode(value.to_vec()))
                                        };
                                        updated_storage_nodes[index].push((nibbles, value));
                                    }
                                }

                                let create_reader = || {
                                    if wiped {
                                        Ok(MaybeEmptyStorageReader::Empty)
                                    } else {
                                        let cursor =
                                            self.tx.cursor_dup_read::<tables::StoragesTrieV2>()?;
                                        Ok(MaybeEmptyStorageReader::Storage(
                                            StorageTrieReader::new(
                                                cursor,
                                                hashed_address,
                                                self.cache.clone(),
                                            ),
                                        ))
                                    }
                                };
                                let trie_reader = create_reader()?;
                                let mut storage_trie = Trie::new(trie_reader, parallel)?;
                                storage_trie
                                    .parallel_update(updated_storage_nodes, create_reader)?;
                                let account = account.into_trie_account(storage_trie.hash());
                                updated_account_nodes.push((
                                    path,
                                    Some(Node::ValueNode(alloy_rlp::encode(account))),
                                ));

                                if need_update {
                                    let trie_output = storage_trie.take_output();
                                    if wiped {
                                        // Always emit the deletion marker so the previous
                                        // storage trie is wiped, carrying the rebuilt nodes
                                        // (which are empty iff the recreated storage is empty).
                                        assert!(trie_update
                                            .lock()
                                            .storage_tries
                                            .insert(
                                                hashed_address,
                                                StorageTrieUpdatesV2 {
                                                    is_deleted: true,
                                                    storage_nodes: trie_output.update_nodes,
                                                    removed_nodes: trie_output.removed_nodes,
                                                }
                                            )
                                            .is_none());
                                    } else if !trie_output.is_empty() {
                                        assert!(trie_update
                                            .lock()
                                            .storage_tries
                                            .insert(
                                                hashed_address,
                                                StorageTrieUpdatesV2 {
                                                    is_deleted: false,
                                                    storage_nodes: trie_output.update_nodes,
                                                    removed_nodes: trie_output.removed_nodes,
                                                }
                                            )
                                            .is_none());
                                    }
                                }
                            } else {
                                updated_account_nodes.push((path, None));
                                deleted_storage();
                            }
                        }
                        Ok(())
                    };
                    if let Err(e) = wrap() {
                        abort.get_or_init(|| e);
                    }
                });
            }
        });
        if let Some(abort) = abort.into_inner() {
            return Err(abort);
        }

        let updated_account_nodes = updated_account_nodes.map(|u| u.into_inner());
        let mut trie_update = trie_update.into_inner();
        let mut proofs = MultiProof::default();
        let create_reader = || {
            let cursor = self.tx.cursor_read::<tables::AccountsTrieV2>()?;
            Ok(AccountTrieReader(cursor, self.cache.clone()))
        };
        let cursor = self.tx.cursor_read::<tables::AccountsTrieV2>()?;
        let mut account_trie = Trie::new(AccountTrieReader(cursor, self.cache.clone()), true)?;
        account_trie.parallel_update(updated_account_nodes, create_reader)?;

        let root_hash = account_trie.hash();
        for (hashed_address, slots) in targets {
            let account_proof =
                account_trie.get_proof_with_paths(Nibbles::unpack(hashed_address))?;
            let account = account_from_proof(&account_proof, hashed_address)?;
            for (path, node) in account_proof {
                proofs.account_subtree.insert(path, Bytes::from(alloy_rlp::encode(node)));
            }
            let storage_proof = if let Some(account) = account {
                let empty_storage = HashedStorage::default();
                let proof = self.storage_proof(
                    hashed_address,
                    hashed_storages.get(&hashed_address).unwrap_or(&empty_storage),
                    &slots.into_iter().collect::<Vec<_>>(),
                )?;
                if proof.root != account.storage_root {
                    return Err(DatabaseError::Other(format!(
                        "V2 storage proof root for {hashed_address} does not match account leaf"
                    ))
                    .into());
                }
                proof
            } else {
                StorageMultiProof::empty()
            };
            proofs.storages.insert(hashed_address, storage_proof);
        }
        if need_update {
            let output = account_trie.take_output();
            trie_update.account_nodes = output.update_nodes;
            trie_update.removed_nodes = output.removed_nodes;
        }

        Ok((root_hash, trie_update, proofs))
    }

    fn storage_proof(
        &self,
        hashed_address: B256,
        storage: &HashedStorage,
        slots: &[B256],
    ) -> ProviderResult<StorageMultiProof> {
        let create_reader = || {
            if storage.wiped {
                Ok(MaybeEmptyStorageReader::Empty)
            } else {
                let cursor = self.tx.cursor_dup_read::<tables::StoragesTrieV2>()?;
                Ok(MaybeEmptyStorageReader::Storage(StorageTrieReader::new(
                    cursor,
                    hashed_address,
                    self.cache.clone(),
                )))
            }
        };
        let mut trie = Trie::new(create_reader()?, storage.storage.len() > MIN_PARALLEL_NODES)?;
        let mut updated_nodes: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
        for (hashed_slot, value) in &storage.storage {
            let path = Nibbles::unpack(*hashed_slot);
            let node =
                (!value.is_zero()).then(|| Node::ValueNode(encode_fixed_size(value).to_vec()));
            updated_nodes[path.get_unchecked(0) as usize].push((path, node));
        }
        trie.parallel_update(updated_nodes, create_reader)?;
        let root = trie.hash();
        let mut proof = StorageMultiProof {
            root,
            subtree: Default::default(),
            branch_node_hash_masks: Default::default(),
            branch_node_tree_masks: Default::default(),
        };
        for slot in slots {
            for (path, node) in trie.get_proof_with_paths(Nibbles::unpack(*slot))? {
                proof.subtree.insert(path, Bytes::from(alloy_rlp::encode(node)));
            }
        }
        Ok(proof)
    }
}

fn account_from_proof(
    proof: &[(Nibbles, TrieNode)],
    hashed_address: B256,
) -> ProviderResult<Option<TrieAccount>> {
    let Some((prefix, TrieNode::Leaf(leaf))) = proof.last() else {
        return Ok(None);
    };
    let mut path = *prefix;
    path.extend(&leaf.key);
    if path != Nibbles::unpack(hashed_address) {
        return Ok(None);
    }
    let account = TrieAccount::decode(&mut &leaf.value[..])
        .map_err(|err| DatabaseError::Other(format!("invalid trie account: {err}")))?;
    Ok(Some(account))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::Entry,
        sync::{mpsc, Arc, Mutex},
    };

    use super::*;
    use alloy_primitives::{keccak256, map::HashMap, Address, Bytes, U256};
    use alloy_rlp::encode_fixed_size;
    use rand::Rng;
    use reth_primitives_traits::Account;
    use reth_provider::{
        test_utils::create_test_provider_factory, DatabaseProviderFactory, StateWriter,
        TrieWriterV2,
    };
    use reth_trie::{
        nested_trie::{Node, NodeFlag, Trie, TrieReader},
        test_utils, HashedPostState, HashedStorage, EMPTY_ROOT_HASH,
    };

    #[derive(Default)]
    struct InmemoryTrieDB {
        account_trie: Arc<Mutex<HashMap<Nibbles, Bytes>>>,
        storage_trie: Arc<Mutex<HashMap<B256, HashMap<Nibbles, Bytes>>>>,
    }
    struct InmemoryAccountTrieReader(Arc<InmemoryTrieDB>);
    struct InmemoryStorageTrieReader(Arc<InmemoryTrieDB>, B256);

    impl TrieReader for InmemoryAccountTrieReader {
        fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
            Ok(self.0.account_trie.lock().unwrap().get(path).map(|v| v.clone().into()))
        }
    }

    impl TrieReader for InmemoryStorageTrieReader {
        fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
            Ok(self
                .0
                .storage_trie
                .lock()
                .unwrap()
                .get(&self.1)
                .and_then(|storage| storage.get(path))
                .map(|v| v.clone().into()))
        }
    }

    impl TrieWriterV2 for InmemoryTrieDB {
        fn write_trie_updatesv2(&self, input: &TrieUpdatesV2) -> Result<usize, DatabaseError> {
            let mut account_trie = self.account_trie.lock().unwrap();
            let mut storage_trie = self.storage_trie.lock().unwrap();
            let mut num_update = 0;

            for path in &input.removed_nodes {
                if account_trie.remove(path).is_some() {
                    num_update += 1;
                }
            }
            for (path, node) in input.account_nodes.clone() {
                account_trie.insert(path, node.into());
                num_update += 1;
            }

            for (hashed_address, storage_trie_update) in &input.storage_tries {
                // Mirror the production writers: a deletion wipes the previous storage trie, but
                // the rebuilt nodes carried in the same update must still be applied afterwards
                // (the self-destruct + recreate case).
                if storage_trie_update.is_deleted &&
                    let Some(destruct_account) = storage_trie.remove(hashed_address)
                {
                    num_update += destruct_account.len();
                }
                let remove_storage = if let Some(storage) = storage_trie.get_mut(hashed_address) {
                    for path in &storage_trie_update.removed_nodes {
                        if storage.remove(path).is_some() {
                            num_update += 1;
                        }
                    }
                    storage.is_empty()
                } else {
                    false
                };
                if remove_storage {
                    storage_trie.remove(hashed_address);
                }
                if !storage_trie_update.storage_nodes.is_empty() {
                    let storage = storage_trie.entry(*hashed_address).or_default();
                    for (path, node) in storage_trie_update.storage_nodes.clone() {
                        storage.insert(path, node.into());
                        num_update += 1;
                    }
                }
            }

            Ok(num_update)
        }
    }

    fn calculate(
        state: HashMap<Address, (Account, HashMap<B256, U256>)>,
        db: Arc<InmemoryTrieDB>,
        is_insert: bool,
    ) -> (B256, TrieUpdatesV2) {
        let (tx, rx) = mpsc::channel();
        let num_task = state.len();
        for (address, (account, storage)) in state {
            let db = db.clone();
            let tx = tx.clone();
            rayon::spawn_fifo(move || {
                let hashed_address = keccak256(address);
                let create_reader = || Ok(InmemoryStorageTrieReader(db.clone(), hashed_address));
                let storage_reader = InmemoryStorageTrieReader(db.clone(), hashed_address);
                let mut storage_trie = Trie::new(storage_reader, true).unwrap();
                let mut batches: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
                for (hashed_slot, value) in
                    storage.into_iter().map(|(k, v)| (keccak256(k), encode_fixed_size(&v)))
                {
                    let nibbles = Nibbles::unpack(hashed_slot);
                    let index = nibbles.get_unchecked(0) as usize;
                    batches[index]
                        .push((nibbles, is_insert.then_some(Node::ValueNode(value.to_vec()))));
                }
                // parallel update
                storage_trie.parallel_update(batches, create_reader).unwrap();
                let storage_root = storage_trie.hash();
                let account = account.into_trie_account(storage_root);
                let _ = tx.send((
                    hashed_address,
                    alloy_rlp::encode(account),
                    storage_trie.take_output(),
                ));
            });
        }

        // parallel insert
        let mut trie_updates = TrieUpdatesV2::default();
        let mut batches: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
        let create_reader = || Ok(InmemoryAccountTrieReader(db.clone()));
        for _ in 0..num_task {
            let (hashed_address, rlp_account, trie_output) =
                rx.recv().expect("Failed to receive storage trie");
            let storage_trie_update = StorageTrieUpdatesV2 {
                is_deleted: false,
                storage_nodes: trie_output.update_nodes,
                removed_nodes: trie_output.removed_nodes,
            };
            let nibbles = Nibbles::unpack(hashed_address);
            let index = nibbles.get_unchecked(0) as usize;
            batches[index].push((nibbles, is_insert.then_some(Node::ValueNode(rlp_account))));
            assert!(trie_updates
                .storage_tries
                .insert(hashed_address, storage_trie_update)
                .is_none());
        }
        let account_reader = InmemoryAccountTrieReader(db.clone());
        let mut account_trie = Trie::new(account_reader, true).unwrap();

        // parallel update
        account_trie.parallel_update(batches, create_reader).unwrap();

        let root_hash = account_trie.hash();
        let output = account_trie.take_output();
        trie_updates.account_nodes = output.update_nodes;
        trie_updates.removed_nodes = output.removed_nodes;
        (root_hash, trie_updates)
    }

    fn random_state() -> HashMap<Address, (Account, HashMap<B256, U256>)> {
        let mut rng = rand::rng();
        (0..10)
            .map(|_| {
                let address = Address::random();
                let mut account =
                    Account { balance: U256::from(rng.random::<u64>()), ..Default::default() };
                let mut storage = HashMap::<B256, U256>::default();
                let has_storage = rng.random_bool(0.7);
                if has_storage {
                    account.bytecode_hash = Some(B256::from(U256::from(rng.random::<u64>())));
                    for _ in 0..10000 {
                        storage.insert(
                            B256::from(U256::from(rng.random::<u64>())),
                            U256::from(rng.random::<u64>()),
                        );
                    }
                }
                (address, (account, storage))
            })
            .collect::<HashMap<_, _>>()
    }

    fn merge_state(
        mut state1: HashMap<Address, (Account, HashMap<B256, U256>)>,
        state2: HashMap<Address, (Account, HashMap<B256, U256>)>,
    ) -> HashMap<Address, (Account, HashMap<B256, U256>)> {
        for (address, (account, storage)) in state2 {
            match state1.entry(address) {
                Entry::Occupied(mut entry) => {
                    let origin = entry.get_mut();
                    origin.0 = account;
                    origin.1.extend(storage);
                }
                Entry::Vacant(entry) => {
                    entry.insert((account, storage));
                }
            }
        }
        state1
    }

    #[test]
    fn nested_state_root() {
        // create random state
        let state1 = random_state();
        let db = Arc::new(InmemoryTrieDB::default());

        let (state_root1, trie_input1) = calculate(state1.clone(), db.clone(), true);
        // compare state root
        assert_eq!(state_root1, test_utils::state_root(state1.clone()));

        // write into db
        let _ = db.write_trie_updatesv2(&trie_input1).unwrap();
        let state2 = random_state();
        let (state_root2, trie_input2) = calculate(state2.clone(), db.clone(), true);
        let state_merged = merge_state(state1.clone(), state2.clone());
        let _ = db.write_trie_updatesv2(&trie_input2).unwrap();

        // compare state root
        assert_eq!(state_root2, test_utils::state_root(state_merged.clone()));
        let (state_root_merged, ..) =
            calculate(state_merged.clone(), Arc::new(InmemoryTrieDB::default()), true);
        assert_eq!(state_root2, state_root_merged);

        // test delete
        if state_merged.len() == state1.len() + state2.len() {
            let (delete_root1, delete_input1) = calculate(state2, db.clone(), false);
            assert_eq!(delete_root1, state_root1);
            let _ = db.write_trie_updatesv2(&delete_input1).unwrap();
            let (delete_root2, delete_input2) = calculate(state1, db.clone(), false);
            // has deleted all data, so the state root is EMPTY_ROOT_HASH
            assert_eq!(delete_root2, EMPTY_ROOT_HASH);
            let _ = db.write_trie_updatesv2(&delete_input2).unwrap();
            assert!(db.account_trie.lock().unwrap().is_empty());
            assert!(db.storage_trie.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn nested_hash_calculate() {
        let state = random_state();
        // test parallel root hash
        let factory = create_test_provider_factory();
        let mut hashed_state = HashedPostState::default();
        for (address, (account, storage)) in state.clone() {
            let hashed_address = keccak256(address);
            hashed_state.accounts.insert(hashed_address, Some(account));
            let mut hashed_storage = HashedStorage::default();
            for (slot, value) in storage {
                hashed_storage.storage.insert(keccak256(slot), value);
            }
            hashed_state.storages.insert(hashed_address, hashed_storage);
        }

        let provider = factory.database_provider_ro().unwrap();
        let tx = provider.tx_ref();
        let (parallel_root_hash, ..) =
            NestedStateRoot::new(tx, None).calculate(&hashed_state).unwrap();
        assert_eq!(parallel_root_hash, test_utils::state_root(state))
    }

    #[test]
    fn v2_multiproof_verifies_account_storage_overlay_and_absence() {
        let factory = create_test_provider_factory();
        let address = Address::random();
        let other = Address::random();
        let missing = Address::random();
        let hashed_address = keccak256(address);
        let slot = B256::from(U256::from(1u64));
        let other_slot = B256::from(U256::from(2u64));
        let missing_slot = B256::from(U256::from(3u64));
        let account = Account { balance: U256::from(42u64), ..Default::default() };
        let storage_with_slot = |value| {
            let mut storage = HashedStorage::new(false);
            storage.storage.insert(keccak256(slot), value);
            storage
        };

        let mut initial = HashedPostState::default();
        initial.accounts.insert(hashed_address, Some(account));
        initial.accounts.insert(keccak256(other), Some(Account::default()));
        let mut initial_storage = storage_with_slot(U256::from(11u64));
        initial_storage.storage.insert(keccak256(other_slot), U256::from(22u64));
        initial.storages.insert(hashed_address, initial_storage);
        let provider = factory.provider_rw().unwrap();
        let (_, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.write_hashed_state(&initial.clone().into_sorted()).unwrap();
        provider.commit().unwrap();

        let mut overlay = HashedPostState::default();
        overlay.storages.insert(hashed_address, storage_with_slot(U256::from(99u64)));
        let provider = factory.database_provider_ro().unwrap();
        let nested = NestedStateRoot::new(provider.tx_ref(), None);
        let (unchanged_root, unchanged_updates) = nested.calculate(&overlay).unwrap();
        let (base_root, base_updates) = nested.calculate(&HashedPostState::default()).unwrap();
        assert_eq!((unchanged_root, unchanged_updates), (base_root, base_updates));
        assert_eq!(nested.root(&overlay).unwrap(), unchanged_root);
        let err = nested
            .multiproof(
                &overlay,
                MultiProofTargets::account_with_slots(hashed_address, [keccak256(slot)]),
            )
            .unwrap_err();
        assert!(err.to_string().contains("does not match account leaf"));
        overlay.accounts.insert(hashed_address, Some(account));
        let root = nested.root(&overlay).unwrap();
        let proof = nested
            .multiproof(
                &overlay,
                MultiProofTargets::from_iter([
                    (
                        hashed_address,
                        [keccak256(slot), keccak256(other_slot), keccak256(missing_slot)]
                            .into_iter()
                            .collect(),
                    ),
                    (keccak256(missing), std::iter::once(keccak256(slot)).collect()),
                ]),
            )
            .unwrap();
        assert!(proof.branch_node_hash_masks.is_empty());
        assert!(proof.branch_node_tree_masks.is_empty());
        let existing = proof.account_proof(address, &[slot, other_slot, missing_slot]).unwrap();
        assert_eq!(existing.info, Some(account));
        assert_eq!(existing.storage_proofs[0].value, U256::from(99u64));
        assert_eq!(existing.storage_proofs[1].value, U256::from(22u64));
        assert_eq!(existing.storage_proofs[2].value, U256::ZERO);
        existing.verify(root).unwrap();

        let absent = proof.account_proof(missing, &[slot]).unwrap();
        assert!(absent.info.is_none());
        absent.verify(root).unwrap();

        assert_eq!(
            nested.storage_root(hashed_address, &overlay.storages[&hashed_address]).unwrap(),
            existing.storage_root
        );

        let mut wiped = HashedPostState::default();
        wiped.accounts.insert(hashed_address, Some(account));
        let mut new_storage = storage_with_slot(U256::from(5u64));
        new_storage.wiped = true;
        wiped.storages.insert(hashed_address, new_storage);
        let wiped_root = nested.root(&wiped).unwrap();
        let wiped_proof = nested
            .multiproof(
                &wiped,
                MultiProofTargets::account_with_slots(
                    hashed_address,
                    [keccak256(slot), keccak256(other_slot)],
                ),
            )
            .unwrap()
            .account_proof(address, &[slot, other_slot])
            .unwrap();
        assert_eq!(wiped_proof.storage_proofs[0].value, U256::from(5u64));
        assert_eq!(wiped_proof.storage_proofs[1].value, U256::ZERO);
        wiped_proof.verify(wiped_root).unwrap();
    }

    #[test]
    fn v2_multiproof_empty_trie_and_orphan_storage() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_ro().unwrap();
        let nested = NestedStateRoot::new(provider.tx_ref(), None);
        let address = Address::random();
        let slot = B256::from(U256::from(7u64));
        let proof = nested
            .multiproof(
                &HashedPostState::default(),
                MultiProofTargets::account_with_slots(keccak256(address), [keccak256(slot)]),
            )
            .unwrap()
            .account_proof(address, &[slot])
            .unwrap();
        proof.verify(EMPTY_ROOT_HASH).unwrap();

        let mut storage = HashedStorage::new(false);
        storage.storage.insert(keccak256(slot), U256::from(1u64));
        let orphan = HashedPostState::from_hashed_storage(keccak256(address), storage);
        assert_eq!(nested.root(&orphan).unwrap(), EMPTY_ROOT_HASH);
        nested
            .multiproof(
                &orphan,
                MultiProofTargets::account_with_slots(keccak256(address), [keccak256(slot)]),
            )
            .unwrap()
            .account_proof(address, &[slot])
            .unwrap()
            .verify(EMPTY_ROOT_HASH)
            .unwrap();
    }

    #[test]
    fn v2_multiproof_uses_unpersisted_trie_cache() {
        let factory = create_test_provider_factory();
        let cache = PersistBlockCache::new();
        let address = Address::random();
        let hashed_address = keccak256(address);
        let first_slot = B256::from(U256::from(1u64));
        let second_slot = B256::from(U256::from(2u64));
        let account = Account { balance: U256::from(42u64), ..Default::default() };

        let mut initial = HashedPostState::default();
        initial.accounts.insert(hashed_address, Some(account));
        let mut initial_storage = HashedStorage::new(false);
        initial_storage.storage.insert(keccak256(first_slot), U256::from(11u64));
        initial.storages.insert(hashed_address, initial_storage);

        let provider = factory.database_provider_ro().unwrap();
        let (_, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial).unwrap();
        cache.write_trie_updates(&updates, 1);

        let mut storage = HashedStorage::new(false);
        storage.storage.insert(keccak256(second_slot), U256::from(22u64));
        let mut overlay = HashedPostState::from_hashed_storage(hashed_address, storage);
        overlay.accounts.insert(hashed_address, Some(account));
        let nested = NestedStateRoot::new(provider.tx_ref(), Some(cache));
        let root = nested.root(&overlay).unwrap();
        let proof = nested
            .multiproof(
                &overlay,
                MultiProofTargets::account_with_slots(
                    hashed_address,
                    [keccak256(first_slot), keccak256(second_slot)],
                ),
            )
            .unwrap()
            .account_proof(address, &[first_slot, second_slot])
            .unwrap();

        let mut expected_storage = HashMap::<B256, U256>::default();
        expected_storage.insert(first_slot, U256::from(11u64));
        expected_storage.insert(second_slot, U256::from(22u64));
        let expected = HashMap::<Address, (Account, HashMap<B256, U256>)>::from_iter([(
            address,
            (account, expected_storage),
        )]);
        assert_eq!(root, test_utils::state_root(expected));
        assert_eq!(proof.info, Some(account));
        proof.verify(root).unwrap();
    }

    #[test]
    fn v2_multiproof_ignores_stale_hashed_account() {
        let factory = create_test_provider_factory();
        let cache = PersistBlockCache::new();
        let address = Address::random();
        let hashed_address = keccak256(address);
        let slot = B256::from(U256::from(1u64));
        let old_account = Account { balance: U256::from(1u64), ..Default::default() };
        let new_account = Account { balance: U256::from(2u64), ..Default::default() };

        let mut initial = HashedPostState::default();
        initial.accounts.insert(hashed_address, Some(old_account));
        let provider = factory.provider_rw().unwrap();
        let (_, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.write_hashed_state(&initial.into_sorted()).unwrap();
        provider.commit().unwrap();

        let mut pending = HashedPostState::default();
        pending.accounts.insert(hashed_address, Some(new_account));
        let provider = factory.database_provider_ro().unwrap();
        let (_, updates) = NestedStateRoot::new(provider.tx_ref(), Some(cache.clone()))
            .calculate(&pending)
            .unwrap();
        cache.write_trie_updates(&updates, 2);

        let overlay = HashedPostState::default();
        let nested = NestedStateRoot::new(provider.tx_ref(), Some(cache));
        let root = nested.root(&overlay).unwrap();
        let proof = nested
            .multiproof(
                &overlay,
                MultiProofTargets::account_with_slots(hashed_address, [keccak256(slot)]),
            )
            .unwrap()
            .account_proof(address, &[slot])
            .unwrap();

        let expected = HashMap::<Address, (Account, HashMap<B256, U256>)>::from_iter([(
            address,
            (new_account, HashMap::default()),
        )]);
        assert_eq!(root, test_utils::state_root(expected));
        assert_eq!(proof.info, Some(new_account));
        assert_eq!(proof.storage_proofs[0].value, U256::ZERO);
        proof.verify(root).unwrap();
    }

    #[test]
    fn v2_multiproof_does_not_resurrect_deleted_account() {
        let factory = create_test_provider_factory();
        let cache = PersistBlockCache::new();
        let address = Address::random();
        let hashed_address = keccak256(address);
        let account = Account { balance: U256::from(1u64), ..Default::default() };

        let mut initial = HashedPostState::default();
        initial.accounts.insert(hashed_address, Some(account));
        let provider = factory.provider_rw().unwrap();
        let (_, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.write_hashed_state(&initial.into_sorted()).unwrap();
        provider.commit().unwrap();

        let mut deleted = HashedPostState::default();
        deleted.accounts.insert(hashed_address, None);
        let provider = factory.database_provider_ro().unwrap();
        let (_, updates) = NestedStateRoot::new(provider.tx_ref(), Some(cache.clone()))
            .calculate(&deleted)
            .unwrap();
        cache.write_trie_updates(&updates, 2);

        let mut storage = HashedStorage::new(false);
        storage.storage.insert(keccak256(B256::ZERO), U256::from(1u64));
        let overlay = HashedPostState::from_hashed_storage(hashed_address, storage);
        let nested = NestedStateRoot::new(provider.tx_ref(), Some(cache));
        assert_eq!(
            nested.root(&overlay).unwrap(),
            nested.root(&HashedPostState::default()).unwrap()
        );
        let root = nested.root(&overlay).unwrap();
        let proof = nested
            .multiproof(&overlay, MultiProofTargets::account(hashed_address))
            .unwrap()
            .account_proof(address, &[])
            .unwrap();
        assert!(proof.info.is_none());
        proof.verify(root).unwrap();
    }

    #[test]
    fn nested_state_root_wipe_and_recreate() {
        // Regression test for the self-destruct + recreate bug (Galxe/gravity-audit#715):
        // when an account's storage is wiped and recreated in the same block, the recreated
        // slots must survive instead of collapsing the storage root to `EMPTY_ROOT_HASH`.
        let factory = create_test_provider_factory();

        let address = Address::random();
        let hashed_address = keccak256(address);
        let account = Account { balance: U256::from(42u64), ..Default::default() };

        // Round 1: account with an initial storage set, persisted into the trie tables.
        let mut storage1 = HashMap::<B256, U256>::default();
        storage1.insert(B256::from(U256::from(1u64)), U256::from(111u64));
        storage1.insert(B256::from(U256::from(2u64)), U256::from(222u64));

        let mut hashed_state1 = HashedPostState::default();
        hashed_state1.accounts.insert(hashed_address, Some(account));
        let mut hashed_storage1 = HashedStorage::new(false);
        for (slot, value) in &storage1 {
            hashed_storage1.storage.insert(keccak256(slot), *value);
        }
        hashed_state1.storages.insert(hashed_address, hashed_storage1);

        let provider_rw = factory.provider_rw().unwrap();
        let (_root1, updates1) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&hashed_state1).unwrap();
        provider_rw.write_trie_updatesv2(&updates1).unwrap();
        provider_rw.commit().unwrap();

        // Round 2: wipe the account and recreate it with a brand new storage set.
        let mut storage2 = HashMap::<B256, U256>::default();
        storage2.insert(B256::from(U256::from(3u64)), U256::from(333u64));
        storage2.insert(B256::from(U256::from(4u64)), U256::from(444u64));

        let mut hashed_state2 = HashedPostState::default();
        hashed_state2.accounts.insert(hashed_address, Some(account));
        let mut hashed_storage2 = HashedStorage::new(true); // wiped
        for (slot, value) in &storage2 {
            hashed_storage2.storage.insert(keccak256(slot), *value);
        }
        hashed_state2.storages.insert(hashed_address, hashed_storage2);

        let provider_rw = factory.provider_rw().unwrap();
        let (root2, updates2) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&hashed_state2).unwrap();

        // The storage update must wipe the old trie while still carrying the rebuilt nodes.
        let storage_update = updates2.storage_tries.get(&hashed_address).unwrap();
        assert!(storage_update.is_deleted);
        assert!(!storage_update.storage_nodes.is_empty());

        // The recreated storage must be reflected in the root: it has to match an independent
        // computation of `{account -> storage2}`. Before the fix the root would have collapsed
        // to the account with empty storage.
        let mut reference = HashMap::<Address, (Account, HashMap<B256, U256>)>::default();
        reference.insert(address, (account, storage2.clone()));
        assert_eq!(root2, test_utils::state_root(reference));

        // Persisting round 2 and reading the storage back from the on-disk trie yields the same
        // root, proving the wiped nodes were replaced rather than merged with the stale ones.
        provider_rw.write_trie_updatesv2(&updates2).unwrap();
        provider_rw.commit().unwrap();

        let provider_ro = factory.database_provider_ro().unwrap();
        let mut hashed_state3 = HashedPostState::default();
        hashed_state3.accounts.insert(hashed_address, Some(account));
        let (root3, _updates3) =
            NestedStateRoot::new(provider_ro.tx_ref(), None).calculate(&hashed_state3).unwrap();
        assert_eq!(root3, root2);
    }

    fn single_account_state(
        hashed_address: B256,
        account: Account,
        wiped: bool,
        storage: &HashMap<B256, U256>,
    ) -> HashedPostState {
        let mut hashed_state = HashedPostState::default();
        hashed_state.accounts.insert(hashed_address, Some(account));
        let mut hashed_storage = HashedStorage::new(wiped);
        for (slot, value) in storage {
            hashed_storage.storage.insert(keccak256(*slot), *value);
        }
        hashed_state.storages.insert(hashed_address, hashed_storage);
        hashed_state
    }

    #[test]
    fn nested_state_root_wipe_and_recreate_multi_account() {
        // A wipe + recreate of one account must not disturb the other accounts, and the new
        // storage of the recreated account must be reflected in the root.
        let factory = create_test_provider_factory();

        let accounts: Vec<(Address, Account, HashMap<B256, U256>)> = (0..3u64)
            .map(|i| {
                let address = Address::random();
                let account = Account { balance: U256::from(100 + i), ..Default::default() };
                let mut storage = HashMap::<B256, U256>::default();
                for j in 0..4u64 {
                    storage.insert(B256::from(U256::from(i * 10 + j + 1)), U256::from((j + 1) * 7));
                }
                (address, account, storage)
            })
            .collect();

        // Round 1: persist all three accounts.
        let mut state1 = HashedPostState::default();
        for (address, account, storage) in &accounts {
            let hashed_address = keccak256(address);
            state1.accounts.insert(hashed_address, Some(*account));
            let mut hs = HashedStorage::new(false);
            for (slot, value) in storage {
                hs.storage.insert(keccak256(*slot), *value);
            }
            state1.storages.insert(hashed_address, hs);
        }
        let provider_rw = factory.provider_rw().unwrap();
        let (_root1, updates1) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&state1).unwrap();
        provider_rw.write_trie_updatesv2(&updates1).unwrap();
        provider_rw.commit().unwrap();

        // Round 2: wipe + recreate accounts[0] with brand new storage; leave the others alone.
        let (wiped_addr, wiped_acc, _old) = accounts[0].clone();
        let mut new_storage = HashMap::<B256, U256>::default();
        new_storage.insert(B256::from(U256::from(999u64)), U256::from(12345u64));
        new_storage.insert(B256::from(U256::from(1000u64)), U256::from(67890u64));
        let state2 = single_account_state(keccak256(wiped_addr), wiped_acc, true, &new_storage);

        let provider_rw = factory.provider_rw().unwrap();
        let (root2, updates2) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&state2).unwrap();
        provider_rw.write_trie_updatesv2(&updates2).unwrap();
        provider_rw.commit().unwrap();

        // Reference: accounts[0] now holds only `new_storage`; the others keep their original.
        let mut reference = HashMap::<Address, (Account, HashMap<B256, U256>)>::default();
        reference.insert(wiped_addr, (wiped_acc, new_storage));
        for (address, account, storage) in accounts.iter().skip(1) {
            reference.insert(*address, (*account, storage.clone()));
        }
        assert_eq!(root2, test_utils::state_root(reference));
    }

    #[test]
    fn cache_shadows_stale_db_after_wipe() {
        // End-to-end reproduction of the persistence-lag staleness (Galxe/gravity-audit#715):
        // after an account is wiped, a later read must observe the wiped (empty) storage through
        // the cache tombstone instead of resurrecting the destroyed incarnation from a DB that
        // has not yet pruned it.
        let factory = create_test_provider_factory();
        let cache = PersistBlockCache::new();

        let address = Address::random();
        let hashed_address = keccak256(address);
        let account = Account { balance: U256::from(7u64), ..Default::default() };

        // Round 1: persist A with storage S1 into BOTH the DB and the cache.
        let mut s1 = HashMap::<B256, U256>::default();
        s1.insert(B256::from(U256::from(1u64)), U256::from(111u64));
        s1.insert(B256::from(U256::from(2u64)), U256::from(222u64));
        let state1 = single_account_state(hashed_address, account, false, &s1);

        let provider_rw = factory.provider_rw().unwrap();
        let (_root1, updates1) = NestedStateRoot::new(provider_rw.tx_ref(), Some(cache.clone()))
            .calculate(&state1)
            .unwrap();
        provider_rw.write_trie_updatesv2(&updates1).unwrap();
        cache.write_trie_updates(&updates1, 1);
        provider_rw.commit().unwrap();

        // Round 2: wipe A (no recreated storage). Update the CACHE only and deliberately do NOT
        // commit to the DB, simulating the async-persistence lag the cache exists to bridge.
        let empty = HashMap::<B256, U256>::default();
        let state2 = single_account_state(hashed_address, account, true, &empty);
        let provider_rw = factory.provider_rw().unwrap();
        let (_root2, updates2) = NestedStateRoot::new(provider_rw.tx_ref(), Some(cache.clone()))
            .calculate(&state2)
            .unwrap();
        cache.write_trie_updates(&updates2, 2);
        drop(provider_rw); // discard without committing -> the DB still holds S1.

        // Round 3: touch A again (balance-only change) and recompute through cache + stale DB.
        // The storage root must be `EMPTY_ROOT_HASH`, taken from the cache tombstone, not the
        // stale S1 root still sitting in the DB.
        let account3 = Account { balance: U256::from(8u64), ..Default::default() };
        let mut state3 = HashedPostState::default();
        state3.accounts.insert(hashed_address, Some(account3));

        let provider_ro = factory.database_provider_ro().unwrap();
        let (root3, _u3) =
            NestedStateRoot::new(provider_ro.tx_ref(), Some(cache)).calculate(&state3).unwrap();

        let mut reference = HashMap::<Address, (Account, HashMap<B256, U256>)>::default();
        reference.insert(address, (account3, HashMap::default()));
        assert_eq!(root3, test_utils::state_root(reference));
    }

    #[test]
    fn nested_state_root_wipe_to_empty() {
        // An account wiped and recreated with EMPTY storage in the same block: the storage root
        // must collapse to `EMPTY_ROOT_HASH` and the on-disk storage trie must be cleared, while
        // the account itself is retained.
        let factory = create_test_provider_factory();
        let address = Address::random();
        let hashed_address = keccak256(address);
        let account = Account { balance: U256::from(5u64), ..Default::default() };

        // Round 1: account with storage, persisted.
        let mut s1 = HashMap::<B256, U256>::default();
        s1.insert(B256::from(U256::from(1u64)), U256::from(11u64));
        s1.insert(B256::from(U256::from(2u64)), U256::from(22u64));
        let state1 = single_account_state(hashed_address, account, false, &s1);
        let provider_rw = factory.provider_rw().unwrap();
        let (_r1, updates1) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&state1).unwrap();
        provider_rw.write_trie_updatesv2(&updates1).unwrap();
        provider_rw.commit().unwrap();

        // Round 2: wipe + recreate with no storage.
        let empty = HashMap::<B256, U256>::default();
        let state2 = single_account_state(hashed_address, account, true, &empty);
        let provider_rw = factory.provider_rw().unwrap();
        let (root2, updates2) =
            NestedStateRoot::new(provider_rw.tx_ref(), None).calculate(&state2).unwrap();

        // The storage update wipes the trie and carries no rebuilt nodes.
        let su = updates2.storage_tries.get(&hashed_address).unwrap();
        assert!(su.is_deleted);
        assert!(su.storage_nodes.is_empty());

        // Root matches the account with empty storage.
        let mut reference = HashMap::<Address, (Account, HashMap<B256, U256>)>::default();
        reference.insert(address, (account, HashMap::default()));
        assert_eq!(root2, test_utils::state_root(reference));

        // Persisting and reading back from the on-disk (now-wiped) trie yields the same root.
        provider_rw.write_trie_updatesv2(&updates2).unwrap();
        provider_rw.commit().unwrap();
        let provider_ro = factory.database_provider_ro().unwrap();
        let mut state3 = HashedPostState::default();
        state3.accounts.insert(hashed_address, Some(account));
        let (root3, _u3) =
            NestedStateRoot::new(provider_ro.tx_ref(), None).calculate(&state3).unwrap();
        assert_eq!(root3, root2);
    }

    #[test]
    fn write_trie_updatesv2_wipe_clears_old_storage_nodes() {
        // `write_trie_updatesv2` must, on `is_deleted`, drop ALL previous storage-trie nodes from
        // the DB before writing the rebuilt ones.
        let factory = create_test_provider_factory();
        let address = keccak256(Address::random());
        let root = Nibbles::new();
        let old_path = Nibbles::from_nibbles_unchecked([0x1]);
        let leaf = |v: u64| Node::ShortNode {
            key: Nibbles::from_nibbles_unchecked([0x9]),
            value: Box::new(Node::ValueNode(v.to_be_bytes().to_vec())),
            flags: NodeFlag::new(None),
        };
        let read_node = |path: &Nibbles| {
            let provider = factory.database_provider_ro().unwrap();
            let cursor = provider.tx_ref().cursor_dup_read::<tables::StoragesTrieV2>().unwrap();
            StorageTrieReader::new(cursor, address, None).read(path).unwrap()
        };

        // Round 1: write a root + an extra node.
        let mut u1 = TrieUpdatesV2::default();
        let mut s1 = StorageTrieUpdatesV2::default();
        s1.storage_nodes.insert(root, leaf(1));
        s1.storage_nodes.insert(old_path, leaf(2));
        u1.storage_tries.insert(address, s1);
        let provider_rw = factory.provider_rw().unwrap();
        provider_rw.write_trie_updatesv2(&u1).unwrap();
        provider_rw.commit().unwrap();
        assert!(read_node(&root).is_some());
        assert!(read_node(&old_path).is_some());

        // Round 2: wipe + recreate with only a new root.
        let mut u2 = TrieUpdatesV2::default();
        let mut s2 = StorageTrieUpdatesV2 { is_deleted: true, ..Default::default() };
        s2.storage_nodes.insert(root, leaf(3));
        u2.storage_tries.insert(address, s2);
        let provider_rw = factory.provider_rw().unwrap();
        provider_rw.write_trie_updatesv2(&u2).unwrap();
        provider_rw.commit().unwrap();

        // The old node is gone; only the rebuilt root remains.
        assert_eq!(read_node(&root), Some(leaf(3)));
        assert!(read_node(&old_path).is_none());
    }
}
