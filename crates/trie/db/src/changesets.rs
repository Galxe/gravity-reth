//! Database-backed trie changeset computation utilities.
//!
//! This module reconstructs trie changesets from database state. The resulting changesets contain
//! the old trie node values needed to revert a block or contiguous range of blocks.

use alloy_primitives::{keccak256, Address, BlockNumber, B256};
use parking_lot::RwLock;
use reth_db_api::{
    cursor::DbCursorRO,
    models::{AccountBeforeTx, BlockNumberAddress},
    tables,
    transaction::DbTx,
    DatabaseError,
};
use reth_primitives_traits::StorageEntry;
use reth_prune_types::PruneSegment;
use reth_storage_api::{BlockNumReader, ChangesetRangeReader};
use reth_storage_errors::provider::ProviderError;
use reth_trie::{
    hashed_cursor::{HashedCursorFactory, HashedPostStateCursorFactory},
    proof_v2::cursor::HashedFactoryRef,
    trie_cursor::{InMemoryTrieCursorFactory, TrieCursorFactory},
    updates::TrieUpdatesSorted,
    HashedPostState, HashedPostStateSorted, HashedStorage, StateRoot, TrieInputSorted,
};
use std::{
    collections::{BTreeMap, HashMap},
    iter::once,
    ops::RangeInclusive,
    sync::Arc,
};
use tracing::debug;

/// Computes trie changesets for a block.
///
/// For block `N`, this reconstructs the trie as it existed after `N`, then calculates the trie
/// updates needed to restore the state before `N`.
///
/// # Errors
///
/// Returns an error if the block exceeds the database tip, database access fails, or state root
/// computation fails. The complete-history and shared immutable-view requirements of
/// [`compute_range_trie_changesets`] also apply.
pub fn compute_block_trie_changesets<Provider, StateTrieProvider>(
    provider: &Provider,
    state_trie_provider: &StateTrieProvider,
    block_number: BlockNumber,
) -> Result<TrieUpdatesSorted, ProviderError>
where
    Provider: ChangesetRangeReader + BlockNumReader,
    StateTrieProvider: TrieCursorFactory + HashedCursorFactory,
{
    let db_tip_block = provider.best_block_number()?;
    compute_range_trie_changesets(
        provider,
        state_trie_provider,
        &[],
        block_number..=block_number,
        db_tip_block,
    )
}

/// Computes aggregate trie changesets for an inclusive block range.
///
/// Returns the value before `range.start()` of every trie node touched by a block in `range`,
/// including nodes created and removed within it.
///
/// `provider` must return COMPLETE legacy changesets with plain addresses and storage slots;
/// missing rows cannot be distinguished from unchanged state by this generic API. Availability
/// and prune bounds must be enforced by the reader or caller. It and
/// `state_trie_provider` must share one immutable view at `db_tip_block`, including canonical
/// block hashes when used with [`ChangesetCache`]. The caller must coordinate snapshot capture
/// with all relevant writers. This function neither captures a snapshot nor opens live factories.
/// `state_trie_provider` must expose the complete LEGACY trie and hashed state; Gravity nested
/// tables have different codecs and must not be passed as legacy trie nodes.
///
/// `forward_updates` contains original executed-block trie updates on that same chain, sorted by
/// strictly increasing block number. Consecutive available blocks are reverted together, preserving
/// forward paths omitted by the aggregate calculation. Missing blocks are reverted individually so
/// transient nodes are retained.
///
/// # Errors
///
/// Returns an error if the range exceeds `db_tip_block`, database access fails, or state root
/// computation fails.
pub fn compute_range_trie_changesets<Provider, StateTrieProvider>(
    provider: &Provider,
    state_trie_provider: &StateTrieProvider,
    forward_updates: &[(BlockNumber, Arc<TrieUpdatesSorted>)],
    range: RangeInclusive<BlockNumber>,
    db_tip_block: BlockNumber,
) -> Result<TrieUpdatesSorted, ProviderError>
where
    Provider: ChangesetRangeReader,
    StateTrieProvider: TrieCursorFactory + HashedCursorFactory,
{
    validate_forward_updates(forward_updates)?;

    if range.is_empty() {
        return Ok(TrieUpdatesSorted::default())
    }

    let start_block = *range.start();
    let end_block = *range.end();

    if end_block > db_tip_block {
        return Err(ProviderError::InsufficientChangesets {
            requested: end_block,
            available: 0..=db_tip_block,
        })
    }

    debug!(
        target: "trie::changesets",
        start_block,
        end_block,
        db_tip_block,
        "Computing range trie changesets from database state"
    );

    let mut overlay = TrieInputSorted::default();
    // Rewind later blocks to reconstruct the trie at the end of the requested range.
    if end_block < db_tip_block {
        rewind_trie_range(
            provider,
            state_trie_provider,
            forward_updates,
            (end_block + 1)..=db_tip_block,
            &mut overlay,
            &mut None,
        )?;
    }

    let mut reverts = Some(TrieUpdatesSorted::default());
    rewind_trie_range(
        provider,
        state_trie_provider,
        forward_updates,
        range,
        &mut overlay,
        &mut reverts,
    )?;
    let reverts = reverts.expect("revert collection is enabled");

    debug!(
        target: "trie::changesets",
        start_block,
        end_block,
        num_account_nodes = reverts.account_nodes_ref().len(),
        num_storage_tries = reverts.storage_tries_ref().len(),
        "Computed range trie changesets successfully"
    );

    Ok(reverts)
}

/// Rewinds the current state and trie views, accumulating changesets when `reverts` is `Some`.
fn rewind_trie_range<Provider, StateTrieProvider>(
    provider: &Provider,
    state_trie_provider: &StateTrieProvider,
    forward_updates: &[(BlockNumber, Arc<TrieUpdatesSorted>)],
    blocks: RangeInclusive<BlockNumber>,
    overlay: &mut TrieInputSorted,
    reverts: &mut Option<TrieUpdatesSorted>,
) -> Result<(), ProviderError>
where
    Provider: ChangesetRangeReader,
    StateTrieProvider: TrieCursorFactory + HashedCursorFactory,
{
    let mut end = *blocks.end();
    while end >= *blocks.start() {
        let mut start = end;
        let mut segment_range = 0..0;

        // Select updates of consecutive blocks iterating backwards from `end`.
        // Without the original updates, reverting one block at a time preserves nodes that
        // appear and disappear inside the range. An endpoint-only calculation would omit them.
        if let Ok(index) = forward_updates.binary_search_by_key(&end, |(block, _)| *block) {
            segment_range = index..index + 1;
            while segment_range.start > 0 && start > *blocks.start() {
                if forward_updates[segment_range.start - 1].0 != start - 1 {
                    break
                }
                segment_range.start -= 1;
                start -= 1;
            }
        }

        // Collect reverts for the segment and use them to generate the trie reverts for the
        // segment.
        let segment_state_reverts = load_legacy_state_reverts(provider, start..=end)?;
        let prefixes = segment_state_reverts.construct_prefix_sets().freeze();
        Arc::make_mut(&mut overlay.state).extend_ref_and_sort(&segment_state_reverts);

        let segment_trie_reverts = StateRoot::new(
            InMemoryTrieCursorFactory::new(state_trie_provider, overlay.nodes.as_ref()),
            HashedPostStateCursorFactory::new(
                HashedFactoryRef::new(state_trie_provider),
                overlay.state.as_ref(),
            ),
        )
        .with_prefix_sets(prefixes)
        .root_with_updates()
        .map_err(ProviderError::other)?
        .1
        .into_sorted();

        // A path missing from the calculated reverts has the same value at both segment ends, so
        // its newest forward update is also its value before the segment. Merging the forward
        // updates keeps nodes that appear and disappear inside the segment, e.g. a node created
        // in block 6 and deleted in block 8 when reverting 6..=8.
        let segment_trie_forward =
            forward_updates[segment_range].iter().rev().map(|(_, updates)| updates.as_ref());
        let segment_trie_reverts =
            TrieUpdatesSorted::merge_iter(once(&segment_trie_reverts).chain(segment_trie_forward));

        Arc::make_mut(&mut overlay.nodes).extend_ref_and_sort(&segment_trie_reverts);
        if let Some(reverts) = reverts {
            reverts.extend_ref_and_sort(&segment_trie_reverts);
        }

        if start == *blocks.start() {
            break
        }
        end = start - 1;
    }

    Ok(())
}

/// Reads V1 plaintext account/storage changesets through one already fixed transaction.
///
/// The caller must use this transaction's exact snapshot for the trie/hashed-state factories and
/// tip metadata. This adapter reads only legacy changeset tables, never static-file V2 changesets.
/// Corresponding history prune checkpoints are checked on that same snapshot. An absent checkpoint
/// does not establish that imported or externally modified history is complete; callers must ensure
/// complete canonical history beyond the known pruning bounds.
#[derive(Debug, Clone, Copy)]
pub struct LegacyChangesetReader<'a, TX>(&'a TX);

impl<'a, TX: DbTx> LegacyChangesetReader<'a, TX> {
    /// Requires fixed changeset and prune-checkpoint views; a live transaction is rejected.
    pub fn new(tx: &'a TX) -> Result<Self, DatabaseError> {
        if !tx.is_fixed_snapshot::<tables::AccountChangeSets>() ||
            !tx.is_fixed_snapshot::<tables::StorageChangeSets>() ||
            !tx.is_fixed_snapshot::<tables::PruneCheckpoints>()
        {
            return Err(DatabaseError::Other(
                "legacy changeset reads require a fixed snapshot".into(),
            ));
        }
        Ok(Self(tx))
    }

    fn check_available(
        &self,
        segment: PruneSegment,
        start: BlockNumber,
    ) -> Result<(), ProviderError> {
        if let Some(pruned) = self
            .0
            .get::<tables::PruneCheckpoints>(segment)?
            .and_then(|checkpoint| checkpoint.block_number) &&
            start <= pruned
        {
            let available = pruned
                .checked_add(1)
                .map(|first| first..=u64::MAX)
                .unwrap_or(RangeInclusive::new(u64::MAX, 0));
            return Err(ProviderError::InsufficientChangesets { requested: start, available });
        }
        Ok(())
    }
}
impl<TX: DbTx> ChangesetRangeReader for LegacyChangesetReader<'_, TX> {
    fn account_changesets_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<Vec<(BlockNumber, AccountBeforeTx)>, ProviderError> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        self.check_available(PruneSegment::AccountHistory, *range.start())?;
        self.0
            .cursor_read::<tables::AccountChangeSets>()?
            .walk_range(range)?
            .map(|entry| entry.map_err(Into::into))
            .collect()
    }
    fn storage_changesets_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<Vec<(BlockNumberAddress, StorageEntry)>, ProviderError> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        self.check_available(PruneSegment::StorageHistory, *range.start())?;
        let start = BlockNumberAddress((*range.start(), Address::ZERO));
        let end = BlockNumberAddress((*range.end(), Address::repeat_byte(0xff)));
        self.0
            .cursor_read::<tables::StorageChangeSets>()?
            .walk_range(start..=end)?
            .map(|entry| entry.map_err(Into::into))
            .collect()
    }
}

/// Returns the oldest pre-change value of every account and slot in an inclusive V1 range.
///
/// Both endpoints, including `u64::MAX`, are represented without converting to an exclusive end.
/// Plain addresses and slots are hashed exactly once. Rows from earlier blocks take precedence.
pub fn load_legacy_state_reverts(
    provider: &impl ChangesetRangeReader,
    range: RangeInclusive<BlockNumber>,
) -> Result<HashedPostStateSorted, ProviderError> {
    if range.is_empty() {
        return Ok(HashedPostStateSorted::default());
    }
    let mut accounts = provider.account_changesets_range(range.clone())?;
    accounts.sort_by_key(|(block, _)| *block);
    let mut storage_rows = provider.storage_changesets_range(range)?;
    storage_rows.sort_by_key(|(key, _)| key.block_number());
    let mut state = HashedPostState::default();
    for (_, before) in accounts {
        state.accounts.entry(keccak256(before.address)).or_insert(before.info);
    }
    for (key, storage) in storage_rows {
        let address = keccak256(key.address());
        state
            .storages
            .entry(address)
            .or_insert_with(|| HashedStorage::new(false))
            .storage
            .entry(keccak256(storage.key))
            .or_insert(storage.value);
    }
    Ok(state.into_sorted())
}

fn validate_forward_updates(
    forward: &[(BlockNumber, Arc<TrieUpdatesSorted>)],
) -> Result<(), ProviderError> {
    if !forward.is_sorted_by(|(a, _), (b, _)| a < b) {
        return Err(ProviderError::other(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "forward updates must have strictly increasing block numbers",
        )));
    }
    Ok(())
}

/// On-demand cache of legacy trie reverts keyed by range and canonical end block hash.
///
/// All metadata, changesets, factories and supplied forward updates must describe one immutable
/// chain view. End hashes distinguish forks at equal heights. Entries have no automatic capacity
/// bound: callers must invoke [`Self::evict`] at their chosen persistence watermark. This cache is
/// opt-in; it is not connected to Gravity's production persistence or nested trie cache.
#[derive(Clone, Debug, Default)]
pub struct ChangesetCache {
    inner: Arc<RwLock<ChangesetCacheInner>>,
}
impl ChangesetCache {
    /// Creates an empty cache with explicit eviction.
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns a cached per-block revert on the specified chain.
    pub fn get(
        &self,
        block_hash: B256,
        block_number: BlockNumber,
    ) -> Option<Arc<TrieUpdatesSorted>> {
        self.inner.read().get(&ChangesetRangeKey::new(block_number, block_number, block_hash))
    }
    /// Removes all ranges starting strictly below `up_to_block`, including alternate forks.
    pub fn evict(&self, up_to_block: BlockNumber) {
        self.inner.write().evict(up_to_block);
    }
    /// Computes and caches one block using the same fixed view as `state_trie_provider`.
    pub fn get_or_compute<P, S>(
        &self,
        provider: &P,
        state_trie_provider: &S,
        block_number: BlockNumber,
    ) -> Result<Arc<TrieUpdatesSorted>, ProviderError>
    where
        P: ChangesetRangeReader + BlockNumReader,
        S: TrieCursorFactory + HashedCursorFactory,
    {
        self.get_or_compute_range(provider, state_trie_provider, &[], block_number..=block_number)
    }
    /// Returns complete reverts for a range, calculating missing paths from the supplied fixed
    /// view.
    ///
    /// Cached individual block reverts are merged with OLDEST before-values taking precedence.
    /// Ordinary forward trie overlays instead use NEWEST values. `forward_updates` must belong to
    /// the same chain and be strictly increasing; missing blocks are reconstructed individually.
    pub fn get_or_compute_range<P, S>(
        &self,
        provider: &P,
        state_trie_provider: &S,
        forward_updates: &[(BlockNumber, Arc<TrieUpdatesSorted>)],
        range: RangeInclusive<BlockNumber>,
    ) -> Result<Arc<TrieUpdatesSorted>, ProviderError>
    where
        P: ChangesetRangeReader + BlockNumReader,
        S: TrieCursorFactory + HashedCursorFactory,
    {
        validate_forward_updates(forward_updates)?;
        if range.is_empty() {
            return Ok(Arc::new(TrieUpdatesSorted::default()));
        }
        let tip = provider.best_block_number()?;
        let start = *range.start();
        let end = *range.end();
        if end > tip {
            return Err(ProviderError::InsufficientChangesets {
                requested: end,
                available: 0..=tip,
            });
        }
        let hash = |number| {
            provider.block_hash(number)?.ok_or_else(|| {
                ProviderError::other(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("block hash not found for block number {number}"),
                ))
            })
        };
        let end_hash = hash(end)?;
        let key = ChangesetRangeKey::new(start, end, end_hash);
        if let Some(cached) = self.inner.read().get(&key) {
            return Ok(cached);
        }
        let mut cached_blocks = Vec::new();
        let mut all_cached = true;
        for number in range.clone().rev() {
            let block_hash = if number == end { end_hash } else { hash(number)? };
            if let Some(block) = self.get(block_hash, number) {
                cached_blocks.push(block);
            } else {
                all_cached = false;
                break;
            }
        }
        let result = if all_cached {
            // merge_slice gives earlier inputs priority; reverts need the oldest before-value.
            cached_blocks.reverse();
            TrieUpdatesSorted::merge_slice(&cached_blocks)
        } else {
            compute_range_trie_changesets(
                provider,
                state_trie_provider,
                forward_updates,
                range,
                tip,
            )?
        };
        let result = Arc::new(result);
        self.inner.write().insert(key, result.clone());
        Ok(result)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ChangesetRangeKey {
    start_block: BlockNumber,
    end_block: BlockNumber,
    end_block_hash: B256,
}
impl ChangesetRangeKey {
    const fn new(start_block: BlockNumber, end_block: BlockNumber, end_block_hash: B256) -> Self {
        Self { start_block, end_block, end_block_hash }
    }
}
#[derive(Debug, Default)]
struct ChangesetCacheInner {
    entries: HashMap<ChangesetRangeKey, Arc<TrieUpdatesSorted>>,
    range_starts: BTreeMap<BlockNumber, Vec<ChangesetRangeKey>>,
}
impl ChangesetCacheInner {
    fn get(&self, key: &ChangesetRangeKey) -> Option<Arc<TrieUpdatesSorted>> {
        self.entries.get(key).cloned()
    }
    fn insert(&mut self, key: ChangesetRangeKey, changesets: Arc<TrieUpdatesSorted>) {
        if self.entries.insert(key, changesets).is_none() {
            self.range_starts.entry(key.start_block).or_default().push(key);
        }
    }
    fn evict(&mut self, up_to_block: BlockNumber) {
        let starts =
            self.range_starts.range(..up_to_block).map(|(number, _)| *number).collect::<Vec<_>>();
        for start in starts {
            if let Some(keys) = self.range_starts.remove(&start) {
                for key in keys {
                    self.entries.remove(&key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
