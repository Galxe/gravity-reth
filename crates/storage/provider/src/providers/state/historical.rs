use crate::{
    providers::state::{latest::complete_storage_accounts, macros::delegate_provider_impls},
    AccountReader, BlockHashReader, HashedPostStateProvider, HeaderProvider, ProviderError,
    StateProvider, StateRootProvider, StaticFileProviderFactory,
};
use alloy_consensus::BlockHeader;
use alloy_primitives::{
    keccak256,
    map::{hash_map, B256Map, HashMap},
    Address, BlockNumber, Bytes, StorageKey, StorageValue, B256,
};
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    models::{storage_sharded_key::StorageShardedKey, ShardedKey},
    table::Table,
    tables,
    transaction::DbTx,
    BlockNumberList,
};
use reth_primitives_traits::{Account, Bytecode, GotExpected};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{
    BlockNumReader, BytecodeReader, ChangeSetReader, ChangesetRangeReader, DBProvider,
    StateProofProvider, StorageChangeSetReader, StorageRootProvider, StorageSettingsCache,
};
use reth_storage_errors::provider::{ProviderResult, RootMismatch};
use reth_trie::{
    updates::{TrieUpdates, TrieUpdatesV2},
    AccountProof, HashedPostState, HashedStorage, KeccakKeyHasher, MultiProof, MultiProofTargets,
    StorageMultiProof, TrieInput,
};
use reth_trie_db::nested_hash::NestedStateRoot;
use std::{fmt::Debug, sync::OnceLock};

/// State provider for a given block number which takes a tx reference.
///
/// Historical state provider accesses the state at the start of the provided block number.
/// It means that all changes made in the provided block number are not included.
///
/// Historical state provider reads the following tables:
/// - [`tables::AccountsHistory`]
/// - [`tables::Bytecodes`]
/// - [`tables::StoragesHistory`]
/// - [`tables::AccountChangeSets`]
/// - [`tables::StorageChangeSets`]
#[derive(Debug)]
pub struct HistoricalStateProviderRef<'b, Provider> {
    /// Database provider
    provider: &'b Provider,
    /// Block number is main index for the history state of accounts and storages.
    block_number: BlockNumber,
    /// Lowest blocks at which different parts of the state are available.
    lowest_available_blocks: LowestAvailableBlocks,
    /// Shared only by the owned provider, so repeated root calls reuse the same historical base.
    revert_cache: Option<&'b OnceLock<HashedPostState>>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum HistoryInfo {
    NotYetWritten,
    InChangeset(u64),
    InPlainState,
    MaybeInPlainState,
}

impl<'b, Provider: DBProvider + BlockNumReader> HistoricalStateProviderRef<'b, Provider> {
    /// Create new `StateProvider` for historical block number
    pub fn new(provider: &'b Provider, block_number: BlockNumber) -> Self {
        Self {
            provider,
            block_number,
            lowest_available_blocks: Default::default(),
            revert_cache: None,
        }
    }

    /// Create new `StateProvider` for historical block number and lowest block numbers at which
    /// account & storage histories are available.
    pub const fn new_with_lowest_available_blocks(
        provider: &'b Provider,
        block_number: BlockNumber,
        lowest_available_blocks: LowestAvailableBlocks,
    ) -> Self {
        Self { provider, block_number, lowest_available_blocks, revert_cache: None }
    }

    /// Lookup an account in the `AccountsHistory` table
    pub fn account_history_lookup(&self, address: Address) -> ProviderResult<HistoryInfo> {
        if !self.lowest_available_blocks.is_account_history_available(self.block_number) {
            return Err(ProviderError::StateAtBlockPruned(self.block_number))
        }

        // history key to search IntegerList of block number changesets.
        let history_key = ShardedKey::new(address, self.block_number);
        self.history_info::<tables::AccountsHistory, _>(
            history_key,
            |key| key.key == address,
            self.lowest_available_blocks.account_history_block_number,
        )
    }

    /// Lookup a storage key in the `StoragesHistory` table
    pub fn storage_history_lookup(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<HistoryInfo> {
        if !self.lowest_available_blocks.is_storage_history_available(self.block_number) {
            return Err(ProviderError::StateAtBlockPruned(self.block_number))
        }

        // history key to search IntegerList of block number changesets.
        let history_key = StorageShardedKey::new(address, storage_key, self.block_number);
        self.history_info::<tables::StoragesHistory, _>(
            history_key,
            |key| key.address == address && key.sharded_key.key == storage_key,
            self.lowest_available_blocks.storage_history_block_number,
        )
    }

    /// Reconstructs the target state from the first before-value for each key after the target.
    fn revert_state_v2(&self) -> ProviderResult<HashedPostState>
    where
        Provider: ChangesetRangeReader + StorageSettingsCache + StaticFileProviderFactory,
    {
        if let Some(reverted) = self.revert_cache.and_then(OnceLock::get) {
            return Ok(reverted.clone())
        }

        let reverted = self.load_revert_state_v2()?;
        if let Some(cache) = self.revert_cache {
            let _ = cache.set(reverted.clone());
        }
        Ok(reverted)
    }

    fn snapshot_canonical_hash(&self, height: BlockNumber) -> ProviderResult<B256>
    where
        Provider: StaticFileProviderFactory,
    {
        let snapshot_height = match super::latest::snapshot_block_number(self.tx())? {
            Some(height) => height,
            None => self.provider.last_block_number()?,
        };
        if height > snapshot_height {
            return Err(ProviderError::HeaderNotFound(height.into()))
        }
        if let Some(hash) = self.tx().get::<tables::CanonicalHeaders>(height)? {
            return Ok(hash)
        }
        // Header migration removes the canonical-height index but retains the reverse index,
        // which anchors the static header to this transaction's view.
        if let Some(hash) = self.provider.static_file_provider().block_hash(height)? &&
            self.tx().get::<tables::HeaderNumbers>(hash)? == Some(height)
        {
            return Ok(hash)
        }
        Err(ProviderError::HeaderNotFound(height.into()))
    }

    fn load_revert_state_v2(&self) -> ProviderResult<HashedPostState>
    where
        Provider: ChangesetRangeReader + StorageSettingsCache + StaticFileProviderFactory,
    {
        if !self.lowest_available_blocks.is_account_history_available(self.block_number) ||
            !self.lowest_available_blocks.is_storage_history_available(self.block_number)
        {
            return Err(ProviderError::StateAtBlockPruned(self.block_number))
        }

        let height = match super::latest::snapshot_block_number(self.tx())? {
            Some(height) => height,
            None => self.provider.last_block_number()?,
        };
        if self.block_number > height.saturating_add(1) {
            return Err(ProviderError::BlockNotExecuted {
                requested: self.block_number - 1,
                executed: height,
            })
        }
        let static_files = self.provider.static_file_provider();
        let _history_guard = self
            .provider
            .cached_storage_settings()
            .changesets_in_static_files
            .then(|| static_files.history_read_guard());
        if _history_guard.is_some() {
            let snapshot_hash = self.snapshot_canonical_hash(height)?;
            if static_files.block_hash(height)? != Some(snapshot_hash) {
                return Err(ProviderError::other(std::io::Error::other(
                    "static-file headers no longer match the state snapshot",
                )))
            }
        }
        if self.block_number > height {
            return Ok(HashedPostState::default())
        }
        if _history_guard.is_some() {
            for segment in
                [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
            {
                if static_files
                    .get_highest_static_file_block(segment)
                    .is_none_or(|last| last < height)
                {
                    return Err(ProviderError::MissingStaticFileBlock(segment, height))
                }
                let first_jar_end = static_files.get_lowest_static_file_block(segment);
                if first_jar_end.is_some_and(|first| {
                    static_files.find_fixed_range(self.block_number).end() < first
                }) {
                    return Err(ProviderError::StateAtBlockPruned(self.block_number))
                }
            }
        }
        let mut accounts = HashMap::default();
        for (_, before) in self.provider.account_changesets_range(self.block_number..=height)? {
            accounts.entry(keccak256(before.address)).or_insert(before.info);
        }

        let mut storages: B256Map<HashedStorage> = HashMap::default();
        for (block_address, before) in
            self.provider.storage_changesets_range(self.block_number..=height)?
        {
            let hashed_address = keccak256(block_address.address());
            if let hash_map::Entry::Vacant(entry) = accounts.entry(hashed_address) {
                // An unchanged account has no changeset; the snapshot's hashed account matches it.
                entry.insert(self.tx().get::<tables::HashedAccounts>(hashed_address)?);
            }
            storages
                .entry(hashed_address)
                .or_default()
                .storage
                .entry(keccak256(before.key))
                .or_insert(before.value);
        }

        Ok(HashedPostState { accounts, storages })
    }

    fn history_info<T, K>(
        &self,
        key: K,
        key_filter: impl Fn(&K) -> bool,
        lowest_available_block_number: Option<BlockNumber>,
    ) -> ProviderResult<HistoryInfo>
    where
        T: Table<Key = K, Value = BlockNumberList>,
    {
        let mut cursor = self.tx().cursor_read::<T>()?;

        // Lookup the history chunk in the history index. If they key does not appear in the
        // index, the first chunk for the next key will be returned so we filter out chunks that
        // have a different key.
        if let Some(chunk) = cursor.seek(key)?.filter(|(key, _)| key_filter(key)).map(|x| x.1 .0) {
            // Get the rank of the first entry before or equal to our block.
            let mut rank = chunk.rank(self.block_number);

            // Adjust the rank, so that we have the rank of the first entry strictly before our
            // block (not equal to it).
            if rank.checked_sub(1).and_then(|rank| chunk.select(rank)) == Some(self.block_number) {
                rank -= 1
            };

            let block_number = chunk.select(rank);

            // If our block is before the first entry in the index chunk and this first entry
            // doesn't equal to our block, it might be before the first write ever. To check, we
            // look at the previous entry and check if the key is the same.
            // This check is worth it, the `cursor.prev()` check is rarely triggered (the if will
            // short-circuit) and when it passes we save a full seek into the changeset/plain state
            // table.
            if rank == 0 &&
                block_number != Some(self.block_number) &&
                !cursor.prev()?.is_some_and(|(key, _)| key_filter(&key))
            {
                if let (Some(_), Some(block_number)) = (lowest_available_block_number, block_number)
                {
                    // The key may have been written, but due to pruning we may not have changesets
                    // and history, so we need to make a changeset lookup.
                    Ok(HistoryInfo::InChangeset(block_number))
                } else {
                    // The key is written to, but only after our block.
                    Ok(HistoryInfo::NotYetWritten)
                }
            } else if let Some(block_number) = block_number {
                // The chunk contains an entry for a write after our block, return it.
                Ok(HistoryInfo::InChangeset(block_number))
            } else {
                // The chunk does not contain an entry for a write after our block. This can only
                // happen if this is the last chunk and so we need to look in the plain state.
                Ok(HistoryInfo::InPlainState)
            }
        } else if lowest_available_block_number.is_some() {
            // The key may have been written, but due to pruning we may not have changesets and
            // history, so we need to make a plain state lookup.
            Ok(HistoryInfo::MaybeInPlainState)
        } else {
            // The key has not been written to at all.
            Ok(HistoryInfo::NotYetWritten)
        }
    }

    /// Set the lowest block number at which the account history is available.
    pub const fn with_lowest_available_account_history_block_number(
        mut self,
        block_number: BlockNumber,
    ) -> Self {
        self.lowest_available_blocks.account_history_block_number = Some(block_number);
        self
    }

    /// Set the lowest block number at which the storage history is available.
    pub const fn with_lowest_available_storage_history_block_number(
        mut self,
        block_number: BlockNumber,
    ) -> Self {
        self.lowest_available_blocks.storage_history_block_number = Some(block_number);
        self
    }
}

impl<Provider: DBProvider + BlockNumReader> HistoricalStateProviderRef<'_, Provider> {
    fn tx(&self) -> &Provider::Tx {
        self.provider.tx_ref()
    }
}

impl<
        Provider: DBProvider
            + BlockNumReader
            + StorageSettingsCache
            + ChangeSetReader
            + StorageChangeSetReader,
    > AccountReader for HistoricalStateProviderRef<'_, Provider>
{
    /// Get basic account information.
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        match self.account_history_lookup(*address)? {
            HistoryInfo::NotYetWritten => Ok(None),
            HistoryInfo::InChangeset(changeset_block_number) => {
                if self.provider.cached_storage_settings().changesets_in_static_files {
                    return Ok(self
                        .provider
                        .account_block_changeset(changeset_block_number)?
                        .into_iter()
                        .find(|change| change.address == *address)
                        .ok_or(ProviderError::AccountChangesetNotFound {
                            block_number: changeset_block_number,
                            address: *address,
                        })?
                        .info)
                }
                Ok(self
                    .tx()
                    .cursor_dup_read::<tables::AccountChangeSets>()?
                    .get_by_key_subkey(changeset_block_number, *address)?
                    .ok_or(ProviderError::AccountChangesetNotFound {
                        block_number: changeset_block_number,
                        address: *address,
                    })?
                    .info)
            }
            HistoryInfo::InPlainState | HistoryInfo::MaybeInPlainState => {
                Ok(self.tx().get_by_encoded_key::<tables::PlainAccountState>(address)?)
            }
        }
    }
}

impl<Provider: DBProvider + BlockNumReader + BlockHashReader> BlockHashReader
    for HistoricalStateProviderRef<'_, Provider>
{
    /// Get block hash by number.
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        self.provider.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.provider.canonical_hashes_range(start, end)
    }
}

impl<
        Provider: DBProvider
            + BlockNumReader
            + ChangesetRangeReader
            + StorageSettingsCache
            + StaticFileProviderFactory,
    > StateRootProvider for HistoricalStateProviderRef<'_, Provider>
{
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256> {
        let mut reverted = self.revert_state_v2()?;
        reverted.extend(hashed_state);
        NestedStateRoot::new(self.tx(), None).root(&complete_storage_accounts(self.tx(), reverted)?)
    }

    fn state_root_with_updates_v2(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdatesV2)> {
        let mut reverted = self.revert_state_v2()?;
        reverted.extend(hashed_state);
        NestedStateRoot::new(self.tx(), None)
            .calculate(&complete_storage_accounts(self.tx(), reverted)?)
    }

    fn state_root_from_nodes(&self, _input: TrieInput) -> ProviderResult<B256> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn state_root_with_updates(
        &self,
        _hashed_state: HashedPostState,
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

impl<
        Provider: DBProvider
            + BlockNumReader
            + ChangesetRangeReader
            + StorageSettingsCache
            + StaticFileProviderFactory,
    > StorageRootProvider for HistoricalStateProviderRef<'_, Provider>
{
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        let mut reverted = self.revert_state_v2()?;
        let hash = keccak256(address);
        let mut storage = reverted.storages.remove(&hash).unwrap_or_default();
        storage.extend(&hashed_storage);
        NestedStateRoot::new(self.tx(), None).storage_root(hash, &storage)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        self.storage_multiproof(address, &[slot], hashed_storage)?
            .storage_proof(slot)
            .map_err(ProviderError::Rlp)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        let hashed_address = keccak256(address);
        let mut reverted = self.revert_state_v2()?;
        if !hashed_storage.is_empty() {
            reverted.storages.entry(hashed_address).or_default().extend(&hashed_storage);
        }
        let reverted = complete_storage_accounts(self.tx(), reverted)?;
        let targets = MultiProofTargets::account_with_slots(
            hashed_address,
            slots.iter().copied().map(keccak256),
        );
        let mut proof = NestedStateRoot::new(self.tx(), None).multiproof(&reverted, targets)?;
        Ok(proof.storages.remove(&hashed_address).unwrap_or_else(StorageMultiProof::empty))
    }
}

impl<
        Provider: DBProvider
            + BlockNumReader
            + ChangesetRangeReader
            + StorageSettingsCache
            + StaticFileProviderFactory
            + HeaderProvider,
    > StateProofProvider for HistoricalStateProviderRef<'_, Provider>
{
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        if !input.nodes.is_empty() {
            return Err(ProviderError::UnsupportedProvider)
        }
        let has_overlay = !input.state.is_empty();
        let mut reverted = self.revert_state_v2()?;
        reverted.extend(input.state);
        let reverted = complete_storage_accounts(self.tx(), reverted)?;
        let canonical_root = if has_overlay {
            None
        } else {
            let target = self
                .block_number
                .checked_sub(1)
                .ok_or_else(|| ProviderError::HeaderNotFound(self.block_number.into()))?;
            let static_files = self.provider.static_file_provider();
            let _history_guard = static_files.history_read_guard();
            let canonical_hash = self.snapshot_canonical_hash(target)?;
            let header = self
                .provider
                .sealed_header(target)?
                .ok_or_else(|| ProviderError::HeaderNotFound(target.into()))?;
            if header.hash() != canonical_hash {
                return Err(ProviderError::other(std::io::Error::other(
                    "proof header no longer matches the state snapshot",
                )))
            }
            Some((target, canonical_hash, header.header().state_root()))
        };
        let targets = MultiProofTargets::account_with_slots(
            keccak256(address),
            slots.iter().copied().map(keccak256),
        );
        let (root, multiproof) =
            NestedStateRoot::new(self.tx(), None).multiproof_with_root(&reverted, targets)?;
        if let Some((block_number, block_hash, expected)) = canonical_root &&
            root != expected
        {
            return Err(ProviderError::StateRootMismatch(Box::new(RootMismatch {
                root: GotExpected { got: root, expected },
                block_number,
                block_hash,
            })))
        }
        let proof = multiproof.account_proof(address, slots).map_err(ProviderError::Rlp)?;
        proof.verify(root).map_err(ProviderError::other)?;
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
        let mut reverted = self.revert_state_v2()?;
        reverted.extend(input.state);
        NestedStateRoot::new(self.tx(), None)
            .multiproof(&complete_storage_accounts(self.tx(), reverted)?, targets)
    }

    fn witness(&self, _input: TrieInput, _target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<Provider: Sync> HashedPostStateProvider for HistoricalStateProviderRef<'_, Provider> {
    fn hashed_post_state(&self, bundle_state: &revm_database::BundleState) -> HashedPostState {
        HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle_state.state())
    }
}

impl<
        Provider: DBProvider
            + BlockNumReader
            + BlockHashReader
            + StorageSettingsCache
            + ChangeSetReader
            + StorageChangeSetReader
            + ChangesetRangeReader
            + StaticFileProviderFactory
            + HeaderProvider,
    > StateProvider for HistoricalStateProviderRef<'_, Provider>
{
    /// Get storage.
    fn storage(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        match self.storage_history_lookup(address, storage_key)? {
            HistoryInfo::NotYetWritten => Ok(None),
            HistoryInfo::InChangeset(changeset_block_number) => {
                if self.provider.cached_storage_settings().changesets_in_static_files {
                    return Ok(Some(
                        self.provider
                            .storage_changeset(changeset_block_number)?
                            .into_iter()
                            .find(|(bna, entry)| {
                                bna.address() == address && entry.key == storage_key
                            })
                            .ok_or_else(|| ProviderError::StorageChangesetNotFound {
                                block_number: changeset_block_number,
                                address,
                                storage_key: Box::new(storage_key),
                            })?
                            .1
                            .value,
                    ))
                }
                Ok(Some(
                    self.tx()
                        .cursor_dup_read::<tables::StorageChangeSets>()?
                        .get_by_key_subkey((changeset_block_number, address).into(), storage_key)?
                        .ok_or_else(|| ProviderError::StorageChangesetNotFound {
                            block_number: changeset_block_number,
                            address,
                            storage_key: Box::new(storage_key),
                        })?
                        .value,
                ))
            }
            HistoryInfo::InPlainState | HistoryInfo::MaybeInPlainState => Ok(self
                .tx()
                .cursor_dup_read::<tables::PlainStorageState>()?
                .get_by_key_subkey(address, storage_key)?
                .map(|entry| entry.value)
                .or(Some(StorageValue::ZERO))),
        }
    }
}

impl<Provider: DBProvider + BlockNumReader> BytecodeReader
    for HistoricalStateProviderRef<'_, Provider>
{
    /// Get account code by its hash
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        self.tx().get_by_encoded_key::<tables::Bytecodes>(code_hash).map_err(Into::into)
    }
}

/// State provider for a given block number.
/// For more detailed description, see [`HistoricalStateProviderRef`].
#[derive(Debug)]
pub struct HistoricalStateProvider<Provider> {
    /// Database provider.
    provider: Provider,
    /// State at the block number is the main indexer of the state.
    block_number: BlockNumber,
    /// Lowest blocks at which different parts of the state are available.
    lowest_available_blocks: LowestAvailableBlocks,
    revert_cache: OnceLock<HashedPostState>,
}

impl<Provider: DBProvider + BlockNumReader> HistoricalStateProvider<Provider> {
    /// Create new `StateProvider` for historical block number
    pub fn new(provider: Provider, block_number: BlockNumber) -> Self {
        Self {
            provider,
            block_number,
            lowest_available_blocks: Default::default(),
            revert_cache: OnceLock::new(),
        }
    }

    /// Set the lowest block number at which the account history is available.
    pub fn with_lowest_available_account_history_block_number(
        mut self,
        block_number: BlockNumber,
    ) -> Self {
        self.lowest_available_blocks.account_history_block_number = Some(block_number);
        self.revert_cache = OnceLock::new();
        self
    }

    /// Set the lowest block number at which the storage history is available.
    pub fn with_lowest_available_storage_history_block_number(
        mut self,
        block_number: BlockNumber,
    ) -> Self {
        self.lowest_available_blocks.storage_history_block_number = Some(block_number);
        self.revert_cache = OnceLock::new();
        self
    }

    /// Returns a new provider that takes the `TX` as reference
    #[inline(always)]
    const fn as_ref(&self) -> HistoricalStateProviderRef<'_, Provider> {
        HistoricalStateProviderRef {
            provider: &self.provider,
            block_number: self.block_number,
            lowest_available_blocks: self.lowest_available_blocks,
            revert_cache: Some(&self.revert_cache),
        }
    }
}

// Delegates all provider impls to [HistoricalStateProviderRef]
delegate_provider_impls!(HistoricalStateProvider<Provider> where [Provider: DBProvider + BlockNumReader + BlockHashReader + StorageSettingsCache + ChangeSetReader + StorageChangeSetReader + ChangesetRangeReader + StaticFileProviderFactory + HeaderProvider ]);

/// Lowest blocks at which different parts of the state are available.
/// They may be [Some] if pruning is enabled.
#[derive(Clone, Copy, Debug, Default)]
pub struct LowestAvailableBlocks {
    /// Lowest block number at which the account history is available. It may not be available if
    /// [`reth_prune_types::PruneSegment::AccountHistory`] was pruned.
    /// [`Option::None`] means all history is available.
    pub account_history_block_number: Option<BlockNumber>,
    /// Lowest block number at which the storage history is available. It may not be available if
    /// [`reth_prune_types::PruneSegment::StorageHistory`] was pruned.
    /// [`Option::None`] means all history is available.
    pub storage_history_block_number: Option<BlockNumber>,
}

impl LowestAvailableBlocks {
    /// Check if account history is available at the provided block number, i.e. lowest available
    /// block number for account history is less than or equal to the provided block number.
    pub fn is_account_history_available(&self, at: BlockNumber) -> bool {
        self.account_history_block_number.map(|block_number| block_number <= at).unwrap_or(true)
    }

    /// Check if storage history is available at the provided block number, i.e. lowest available
    /// block number for storage history is less than or equal to the provided block number.
    pub fn is_storage_history_available(&self, at: BlockNumber) -> bool {
        self.storage_history_block_number.map(|block_number| block_number <= at).unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        providers::state::historical::{HistoryInfo, LowestAvailableBlocks},
        test_utils::create_test_provider_factory,
        AccountReader, HistoricalStateProvider, HistoricalStateProviderRef, StateProvider,
        StateRootProvider, StaticFileProviderFactory, StaticFileWriter, TrieWriterV2,
    };
    use alloy_consensus::Header;
    use alloy_primitives::{address, b256, keccak256, Address, B256, U256};
    use reth_db_api::{
        models::{
            storage_sharded_key::StorageShardedKey, AccountBeforeTx, GravityStorageSettings,
            ShardedKey, StorageBeforeTx,
        },
        tables,
        transaction::{DbTx, DbTxMut},
        BlockNumberList,
    };
    use reth_primitives_traits::{Account, StorageEntry};
    use reth_stages_types::{StageCheckpoint, StageId};
    use reth_static_file_types::StaticFileSegment;
    use reth_storage_api::{
        BlockHashReader, BlockNumReader, ChangeSetReader, ChangesetRangeReader, DBProvider,
        DatabaseProviderFactory, HeaderProvider, StateProofProvider, StateWriter,
        StorageChangeSetReader, StorageSettingsCache,
    };
    use reth_storage_errors::provider::ProviderError;
    use reth_trie::{HashedPostState, HashedStorage, TrieInput, EMPTY_ROOT_HASH};
    use reth_trie_db::nested_hash::NestedStateRoot;

    const ADDRESS: Address = address!("0x0000000000000000000000000000000000000001");
    const HIGHER_ADDRESS: Address = address!("0x0000000000000000000000000000000000000005");
    const STORAGE: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");

    const fn assert_state_provider<T: StateProvider>() {}
    #[expect(dead_code)]
    const fn assert_historical_state_provider<
        T: DBProvider
            + BlockNumReader
            + BlockHashReader
            + StorageSettingsCache
            + ChangeSetReader
            + StorageChangeSetReader
            + ChangesetRangeReader
            + StaticFileProviderFactory
            + HeaderProvider,
    >() {
        assert_state_provider::<HistoricalStateProvider<T>>();
    }

    fn set_complete_height<T: DbTxMut>(tx: &T, height: u64) {
        for stage in [
            StageId::Execution,
            StageId::AccountHashing,
            StageId::IndexAccountHistory,
            StageId::MerkleExecute,
        ] {
            tx.put::<tables::StageCheckpoints>(
                stage.to_string(),
                StageCheckpoint { block_number: height, ..Default::default() },
            )
            .unwrap();
        }
    }

    fn write_canonical_header<T: DbTxMut>(tx: &T, height: u64, state_root: B256) {
        let header = Header { number: height, state_root, ..Default::default() };
        let hash = header.hash_slow();
        tx.put::<tables::Headers<Header>>(height, header).unwrap();
        tx.put::<tables::CanonicalHeaders>(height, hash).unwrap();
    }

    #[test]
    fn history_provider_get_account() {
        let factory = create_test_provider_factory();
        let tx = factory.provider_rw().unwrap().into_tx();

        tx.put::<tables::AccountsHistory>(
            ShardedKey { key: ADDRESS, highest_block_number: 7 },
            BlockNumberList::new([1, 3, 7]).unwrap(),
        )
        .unwrap();
        tx.put::<tables::AccountsHistory>(
            ShardedKey { key: ADDRESS, highest_block_number: u64::MAX },
            BlockNumberList::new([10, 15]).unwrap(),
        )
        .unwrap();
        tx.put::<tables::AccountsHistory>(
            ShardedKey { key: HIGHER_ADDRESS, highest_block_number: u64::MAX },
            BlockNumberList::new([4]).unwrap(),
        )
        .unwrap();

        let acc_plain = Account { nonce: 100, balance: U256::ZERO, bytecode_hash: None };
        let acc_at15 = Account { nonce: 15, balance: U256::ZERO, bytecode_hash: None };
        let acc_at10 = Account { nonce: 10, balance: U256::ZERO, bytecode_hash: None };
        let acc_at7 = Account { nonce: 7, balance: U256::ZERO, bytecode_hash: None };
        let acc_at3 = Account { nonce: 3, balance: U256::ZERO, bytecode_hash: None };

        let higher_acc_plain = Account { nonce: 4, balance: U256::ZERO, bytecode_hash: None };

        // setup
        tx.put::<tables::AccountChangeSets>(1, AccountBeforeTx { address: ADDRESS, info: None })
            .unwrap();
        tx.put::<tables::AccountChangeSets>(
            3,
            AccountBeforeTx { address: ADDRESS, info: Some(acc_at3) },
        )
        .unwrap();
        tx.put::<tables::AccountChangeSets>(
            4,
            AccountBeforeTx { address: HIGHER_ADDRESS, info: None },
        )
        .unwrap();
        tx.put::<tables::AccountChangeSets>(
            7,
            AccountBeforeTx { address: ADDRESS, info: Some(acc_at7) },
        )
        .unwrap();
        tx.put::<tables::AccountChangeSets>(
            10,
            AccountBeforeTx { address: ADDRESS, info: Some(acc_at10) },
        )
        .unwrap();
        tx.put::<tables::AccountChangeSets>(
            15,
            AccountBeforeTx { address: ADDRESS, info: Some(acc_at15) },
        )
        .unwrap();

        // setup plain state
        tx.put::<tables::PlainAccountState>(ADDRESS, acc_plain).unwrap();
        tx.put::<tables::PlainAccountState>(HIGHER_ADDRESS, higher_acc_plain).unwrap();
        tx.commit().unwrap();

        let db = factory.provider().unwrap();

        // run
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 1).basic_account(&ADDRESS),
            Ok(None)
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 2).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at3
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 3).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at3
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 4).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at7
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 7).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at7
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 9).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at10
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 10).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at10
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 11).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_at15
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 16).basic_account(&ADDRESS),
            Ok(Some(acc)) if acc == acc_plain
        ));

        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 1).basic_account(&HIGHER_ADDRESS),
            Ok(None)
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 1000).basic_account(&HIGHER_ADDRESS),
            Ok(Some(acc)) if acc == higher_acc_plain
        ));
    }

    #[test]
    fn history_provider_get_storage() {
        let factory = create_test_provider_factory();
        let tx = factory.provider_rw().unwrap().into_tx();

        tx.put::<tables::StoragesHistory>(
            StorageShardedKey {
                address: ADDRESS,
                sharded_key: ShardedKey { key: STORAGE, highest_block_number: 7 },
            },
            BlockNumberList::new([3, 7]).unwrap(),
        )
        .unwrap();
        tx.put::<tables::StoragesHistory>(
            StorageShardedKey {
                address: ADDRESS,
                sharded_key: ShardedKey { key: STORAGE, highest_block_number: u64::MAX },
            },
            BlockNumberList::new([10, 15]).unwrap(),
        )
        .unwrap();
        tx.put::<tables::StoragesHistory>(
            StorageShardedKey {
                address: HIGHER_ADDRESS,
                sharded_key: ShardedKey { key: STORAGE, highest_block_number: u64::MAX },
            },
            BlockNumberList::new([4]).unwrap(),
        )
        .unwrap();

        let higher_entry_plain = StorageEntry { key: STORAGE, value: U256::from(1000) };
        let higher_entry_at4 = StorageEntry { key: STORAGE, value: U256::from(0) };
        let entry_plain = StorageEntry { key: STORAGE, value: U256::from(100) };
        let entry_at15 = StorageEntry { key: STORAGE, value: U256::from(15) };
        let entry_at10 = StorageEntry { key: STORAGE, value: U256::from(10) };
        let entry_at7 = StorageEntry { key: STORAGE, value: U256::from(7) };
        let entry_at3 = StorageEntry { key: STORAGE, value: U256::from(0) };

        // setup
        tx.put::<tables::StorageChangeSets>((3, ADDRESS).into(), entry_at3).unwrap();
        tx.put::<tables::StorageChangeSets>((4, HIGHER_ADDRESS).into(), higher_entry_at4).unwrap();
        tx.put::<tables::StorageChangeSets>((7, ADDRESS).into(), entry_at7).unwrap();
        tx.put::<tables::StorageChangeSets>((10, ADDRESS).into(), entry_at10).unwrap();
        tx.put::<tables::StorageChangeSets>((15, ADDRESS).into(), entry_at15).unwrap();

        // setup plain state
        tx.put::<tables::PlainStorageState>(ADDRESS, entry_plain).unwrap();
        tx.put::<tables::PlainStorageState>(HIGHER_ADDRESS, higher_entry_plain).unwrap();
        tx.commit().unwrap();

        let db = factory.provider().unwrap();

        // run
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 0).storage(ADDRESS, STORAGE),
            Ok(None)
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 3).storage(ADDRESS, STORAGE),
            Ok(Some(U256::ZERO))
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 4).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_at7.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 7).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_at7.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 9).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_at10.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 10).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_at10.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 11).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_at15.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 16).storage(ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == entry_plain.value
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 1).storage(HIGHER_ADDRESS, STORAGE),
            Ok(None)
        ));
        assert!(matches!(
            HistoricalStateProviderRef::new(&db, 1000).storage(HIGHER_ADDRESS, STORAGE),
            Ok(Some(expected_value)) if expected_value == higher_entry_plain.value
        ));
    }

    #[test]
    fn history_provider_unavailable() {
        let factory = create_test_provider_factory();
        let db = factory.database_provider_rw().unwrap();

        // provider block_number < lowest available block number,
        // i.e. state at provider block is pruned
        let provider = HistoricalStateProviderRef::new_with_lowest_available_blocks(
            &db,
            2,
            LowestAvailableBlocks {
                account_history_block_number: Some(3),
                storage_history_block_number: Some(3),
            },
        );
        assert!(matches!(
            provider.account_history_lookup(ADDRESS),
            Err(ProviderError::StateAtBlockPruned(number)) if number == provider.block_number
        ));
        assert!(matches!(
            provider.storage_history_lookup(ADDRESS, STORAGE),
            Err(ProviderError::StateAtBlockPruned(number)) if number == provider.block_number
        ));

        // provider block_number == lowest available block number,
        // i.e. state at provider block is available
        let provider = HistoricalStateProviderRef::new_with_lowest_available_blocks(
            &db,
            2,
            LowestAvailableBlocks {
                account_history_block_number: Some(2),
                storage_history_block_number: Some(2),
            },
        );
        assert!(matches!(
            provider.account_history_lookup(ADDRESS),
            Ok(HistoryInfo::MaybeInPlainState)
        ));
        assert!(matches!(
            provider.storage_history_lookup(ADDRESS, STORAGE),
            Ok(HistoryInfo::MaybeInPlainState)
        ));

        // provider block_number == lowest available block number,
        // i.e. state at provider block is available
        let provider = HistoricalStateProviderRef::new_with_lowest_available_blocks(
            &db,
            2,
            LowestAvailableBlocks {
                account_history_block_number: Some(1),
                storage_history_block_number: Some(1),
            },
        );
        assert!(matches!(
            provider.account_history_lookup(ADDRESS),
            Ok(HistoryInfo::MaybeInPlainState)
        ));
        assert!(matches!(
            provider.storage_history_lookup(ADDRESS, STORAGE),
            Ok(HistoryInfo::MaybeInPlainState)
        ));
    }

    #[test]
    fn history_v2_storage_only_revert_keeps_account_in_root_and_proof() {
        for static_changesets in [false, true] {
            let factory = create_test_provider_factory();
            factory.set_storage_settings_cache(GravityStorageSettings {
                changesets_in_static_files: static_changesets,
            });
            let hashed_address = keccak256(ADDRESS);
            let hashed_slot = keccak256(STORAGE);
            let account = Account { nonce: 7, balance: U256::from(42), ..Default::default() };
            let changed_hash = keccak256(HIGHER_ADDRESS);
            let first = Account { nonce: 1, ..Default::default() };

            let mut latest = HashedPostState::default();
            latest.accounts.insert(hashed_address, Some(account));
            latest.accounts.insert(changed_hash, Some(Account { nonce: 3, ..Default::default() }));
            latest
                .storages
                .entry(hashed_address)
                .or_default()
                .storage
                .insert(hashed_slot, U256::from(20));
            let provider = factory.provider_rw().unwrap();
            let (latest_root, updates) =
                NestedStateRoot::new(provider.tx_ref(), None).calculate(&latest).unwrap();
            provider.write_trie_updatesv2(&updates).unwrap();
            provider.write_hashed_state(&latest.into_sorted()).unwrap();
            let mut expected = HashedPostState::default();
            expected.accounts.insert(hashed_address, Some(account));
            expected.accounts.insert(changed_hash, Some(first));
            expected
                .storages
                .entry(hashed_address)
                .or_default()
                .storage
                .insert(hashed_slot, U256::from(10));
            let expected_root =
                NestedStateRoot::new(provider.tx_ref(), None).root(&expected).unwrap();
            let static_files = factory.static_file_provider();
            if static_changesets {
                let mut headers = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
                for number in 0..=3 {
                    let header = Header {
                        number,
                        state_root: if number == 1 { expected_root } else { latest_root },
                        ..Default::default()
                    };
                    let hash = header.hash_slow();
                    headers.append_header(&header, U256::ZERO, &hash).unwrap();
                    provider.tx_ref().put::<tables::CanonicalHeaders>(number, hash).unwrap();
                }
                headers.commit().unwrap();
            } else {
                provider
                    .tx_ref()
                    .put::<tables::CanonicalHeaders>(3, B256::with_last_byte(3))
                    .unwrap();
            }
            set_complete_height(provider.tx_ref(), 3);
            let mut changeset_writers = static_changesets.then(|| {
                let mut accounts =
                    static_files.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
                let mut storages =
                    static_files.latest_writer(StaticFileSegment::StorageChangeSets).unwrap();
                for writer in [&mut accounts, &mut storages] {
                    writer.increment_block(0).unwrap();
                    writer.increment_block(1).unwrap();
                }
                (accounts, storages)
            });
            for (height, before_value) in [(2, 10), (3, 15)] {
                let account_before = AccountBeforeTx {
                    address: HIGHER_ADDRESS,
                    info: Some(Account { nonce: height - 1, ..Default::default() }),
                };
                let storage_before = StorageBeforeTx {
                    address: ADDRESS,
                    key: STORAGE,
                    value: U256::from(before_value),
                };
                if let Some((accounts, storages)) = &mut changeset_writers {
                    accounts.append_account_changeset(vec![account_before], height).unwrap();
                    storages.append_storage_changeset(vec![storage_before], height).unwrap();
                } else {
                    provider
                        .tx_ref()
                        .put::<tables::AccountChangeSets>(height, account_before)
                        .unwrap();
                    provider
                        .tx_ref()
                        .put::<tables::StorageChangeSets>(
                            (height, ADDRESS).into(),
                            StorageEntry { key: STORAGE, value: storage_before.value },
                        )
                        .unwrap();
                }
            }
            if let Some((mut accounts, mut storages)) = changeset_writers {
                accounts.commit().unwrap();
                storages.commit().unwrap();
            }
            provider.commit().unwrap();

            let provider = factory.provider().unwrap();
            let historical = HistoricalStateProviderRef::new(&provider, 2);
            let reverted = historical.revert_state_v2().unwrap();
            assert_eq!(reverted.accounts[&hashed_address], Some(account));
            assert_eq!(reverted.accounts[&changed_hash], Some(first));
            assert_eq!(reverted.storages[&hashed_address].storage[&hashed_slot], U256::from(10));

            assert_ne!(expected_root, latest_root);
            assert_eq!(historical.state_root(HashedPostState::default()).unwrap(), expected_root);

            drop(provider);
            let provider = factory.provider_rw().unwrap();
            write_canonical_header(provider.tx_ref(), 1, expected_root);
            provider.commit().unwrap();
            let provider = factory.provider().unwrap();
            let historical = HistoricalStateProviderRef::new(&provider, 2);

            let proof = historical.proof(TrieInput::default(), ADDRESS, &[STORAGE]).unwrap();
            assert_eq!(proof.info, Some(account));
            assert_eq!(proof.storage_proofs[0].value, U256::from(10));
            proof.verify(expected_root).unwrap();

            let latest = crate::LatestStateProviderRef::new(&provider);
            let tip = HistoricalStateProviderRef::new(&provider, 4);
            for wiped in [false, true] {
                let mut storage_only = HashedPostState::default();
                let mut storage = HashedStorage::new(wiped);
                if !wiped {
                    storage.storage.insert(hashed_slot, U256::from(40));
                }
                storage_only.storages.insert(hashed_address, storage);
                let mut expected = storage_only.clone();
                expected.accounts.insert(hashed_address, Some(account));
                let expected_root =
                    NestedStateRoot::new(provider.tx_ref(), None).root(&expected).unwrap();
                for state in [&tip as &dyn StateRootProvider, &latest] {
                    assert_eq!(state.state_root(storage_only.clone()).unwrap(), expected_root);
                    let (root, updates) =
                        state.state_root_with_updates_v2(storage_only.clone()).unwrap();
                    assert_eq!(root, expected_root);
                    assert_ne!(root, latest_root);
                    assert!(!updates.account_nodes.is_empty());
                    let storage = &updates.storage_tries[&hashed_address];
                    assert_eq!(storage.is_deleted, wiped);
                    assert!(wiped || !storage.storage_nodes.is_empty());
                }
            }
        }
    }

    #[test]
    fn history_v2_static_headers_require_snapshot_hash_anchor() {
        let factory = create_test_provider_factory();
        factory.set_storage_settings_cache(GravityStorageSettings {
            changesets_in_static_files: true,
        });
        let static_files = factory.static_file_provider();
        let writer = factory.provider_rw().unwrap();
        let mut hashes = Vec::new();
        {
            let mut headers = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
            for number in 0..=2 {
                let header = Header {
                    number,
                    parent_hash: hashes.last().copied().unwrap_or_default(),
                    state_root: EMPTY_ROOT_HASH,
                    ..Default::default()
                };
                let hash = header.hash_slow();
                headers.append_header(&header, U256::ZERO, &hash).unwrap();
                writer.tx_ref().put::<tables::HeaderNumbers>(hash, number).unwrap();
                hashes.push(hash);
            }
            headers.commit().unwrap();
        }
        set_complete_height(writer.tx_ref(), 1);
        writer.commit().unwrap();

        // Header migration leaves only static headers and the snapshot's reverse hash index.
        let provider = factory.provider().unwrap();
        assert!(provider.tx_ref().get::<tables::CanonicalHeaders>(1).unwrap().is_none());
        let historical = HistoricalStateProviderRef::new(&provider, 2);
        historical
            .proof(TrieInput::default(), ADDRESS, &[STORAGE])
            .unwrap()
            .verify(EMPTY_ROOT_HASH)
            .unwrap();
        // A known static header beyond the complete snapshot height is still unavailable.
        assert!(matches!(
            historical.snapshot_canonical_hash(2),
            Err(ProviderError::HeaderNotFound(_))
        ));

        for anchor_height in [Some(2), None] {
            let writer = factory.provider_rw().unwrap();
            if let Some(height) = anchor_height {
                writer.tx_ref().put::<tables::HeaderNumbers>(hashes[1], height).unwrap();
            } else {
                writer.tx_ref().delete::<tables::HeaderNumbers>(hashes[1], None).unwrap();
            }
            writer.commit().unwrap();
            let changed = factory.provider().unwrap();
            assert!(matches!(
                HistoricalStateProviderRef::new(&changed, 2).proof(
                    TrieInput::default(),
                    ADDRESS,
                    &[STORAGE],
                ),
                Err(ProviderError::HeaderNotFound(_))
            ));
        }
        // A retained snapshot still uses the valid anchor from before those commits.
        historical
            .proof(TrieInput::default(), ADDRESS, &[STORAGE])
            .unwrap()
            .verify(EMPTY_ROOT_HASH)
            .unwrap();
    }
}
