use crate::{
    providers::{
        state::latest::LatestStateProvider, static_file::StaticFileWriter, StaticFileProvider,
        StaticFileProviderRWRefMut,
    },
    to_range,
    traits::{BlockSource, ReceiptProvider},
    BlockHashReader, BlockNumReader, BlockReader, ChainSpecProvider, DatabaseProviderFactory,
    HashedPostStateProvider, HeaderProvider, HeaderSyncGapProvider, ProviderError,
    ProviderReadView, PruneCheckpointReader, StageCheckpointReader, StateProviderBox,
    StaticFileProviderFactory, TransactionVariant, TransactionsProvider,
};
use alloy_consensus::transaction::TransactionMeta;
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{Address, BlockHash, BlockNumber, TxHash, TxNumber, B256, U256};
use core::fmt;
use parking_lot::RwLock;
use reth_chainspec::ChainInfo;
use reth_db::{init_db, DatabaseArguments, DatabaseEnv};
use reth_db_api::{
    database::{Database, DatabaseSnapshot},
    models::{GravityStorageSettings, StoredBlockBodyIndices},
    tables,
    transaction::DbTx,
};
use reth_errors::{RethError, RethResult};
use reth_node_types::{
    BlockTy, HeaderTy, NodeTypes, NodeTypesWithDB, NodeTypesWithDBAdapter, ReceiptTy, TxTy,
};
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_prune_types::{PruneCheckpoint, PruneModes, PruneSegment};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{
    metadata_keys, BlockBodyIndicesProvider, NodePrimitivesProvider, StorageSettingsCache,
    TryIntoHistoricalStateProvider,
};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::HashedPostState;
use revm_database::BundleState;
use std::{
    ops::{RangeBounds, RangeInclusive},
    path::Path,
    sync::Arc,
};

use tracing::trace;

mod provider;
pub use provider::{DatabaseProvider, DatabaseProviderRO, DatabaseProviderRW};

use super::ProviderNodeTypes;
use reth_trie::KeccakKeyHasher;

mod builder;
pub use builder::{ProviderFactoryBuilder, ReadOnlyConfig};

mod metrics;

mod chain;
pub use chain::*;

/// A common provider that fetches data from a database or static file.
///
/// This provider implements most provider or provider factory traits.
pub struct ProviderFactory<N: NodeTypesWithDB> {
    /// Database instance
    db: N::DB,
    /// Chain spec
    chain_spec: Arc<N::ChainSpec>,
    /// Static File Provider
    static_file_provider: StaticFileProvider<N::Primitives>,
    /// Optional pruning configuration
    prune_modes: PruneModes,
    /// The node storage handler.
    storage: Arc<N::Storage>,
    /// Cached storage layout settings, loaded from the metadata table at construction.
    storage_settings: Arc<RwLock<GravityStorageSettings>>,
    read_mode: ReadMode,
}

/// Execution providers read live data; RPC providers capture a view on demand.
#[derive(Clone, Debug)]
enum ReadMode {
    Live,
    Rpc,
    Captured(Arc<ReadView>),
}

/// The database snapshot and height shared by one state computation.
#[derive(Debug)]
pub(crate) struct ReadView {
    db: Option<Arc<dyn DatabaseSnapshot>>,
    consistent: bool,
    pub(crate) bounds: ReadBounds,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadBounds {
    pub(crate) block_number: BlockNumber,
    pub(crate) next_tx_num: TxNumber,
}

impl<N: NodeTypes> ProviderFactory<NodeTypesWithDBAdapter<N, Arc<DatabaseEnv>>> {
    /// Instantiates the builder for this type
    pub fn builder() -> ProviderFactoryBuilder<N> {
        ProviderFactoryBuilder::default()
    }
}

/// Loads the persisted storage layout settings, falling back to the legacy layout when the
/// metadata entry is missing, unreadable, or the database can't be opened yet (a fresh datadir
/// is initialized by `init_genesis`, which also refreshes this cache).
fn load_storage_settings<DB: Database>(db: &DB) -> GravityStorageSettings {
    db.tx()
        .ok()
        .and_then(|tx| {
            tx.get::<tables::Metadata>(metadata_keys::GRAVITY_STORAGE_SETTINGS.to_string())
                .ok()
                .flatten()
        })
        .and_then(|bytes| GravityStorageSettings::from_metadata_bytes(&bytes))
        .unwrap_or_else(GravityStorageSettings::legacy)
}

impl<N: NodeTypesWithDB> ProviderFactory<N> {
    /// Create new database provider factory.
    pub fn new(
        db: N::DB,
        chain_spec: Arc<N::ChainSpec>,
        static_file_provider: StaticFileProvider<N::Primitives>,
    ) -> Self {
        let storage_settings = Arc::new(RwLock::new(load_storage_settings(&db)));
        Self {
            db,
            chain_spec,
            static_file_provider,
            prune_modes: PruneModes::none(),
            storage: Default::default(),
            storage_settings,
            read_mode: ReadMode::Live,
        }
    }

    /// Enables metrics on the static file provider.
    pub fn with_static_files_metrics(mut self) -> Self {
        self.static_file_provider = self.static_file_provider.with_metrics();
        self
    }

    /// Sets the pruning configuration for an existing [`ProviderFactory`].
    pub fn with_prune_modes(mut self, prune_modes: PruneModes) -> Self {
        self.prune_modes = prune_modes;
        self
    }

    /// Returns reference to the underlying database.
    pub const fn db_ref(&self) -> &N::DB {
        &self.db
    }

    #[cfg(any(test, feature = "test-utils"))]
    /// Consumes Self and returns DB
    pub fn into_db(self) -> N::DB {
        self.db
    }
}

impl<N: NodeTypesWithDB<DB = Arc<DatabaseEnv>>> ProviderFactory<N> {
    /// Create new database provider by passing a path. [`ProviderFactory`] will own the database
    /// instance.
    pub fn new_with_database_path<P: AsRef<Path>>(
        path: P,
        chain_spec: Arc<N::ChainSpec>,
        args: DatabaseArguments,
        static_file_provider: StaticFileProvider<N::Primitives>,
    ) -> RethResult<Self> {
        let db = Arc::new(init_db(path, args).map_err(RethError::msg)?);
        let storage_settings = Arc::new(RwLock::new(load_storage_settings(&db)));
        Ok(Self {
            db,
            chain_spec,
            static_file_provider,
            prune_modes: PruneModes::none(),
            storage: Default::default(),
            storage_settings,
            read_mode: ReadMode::Live,
        })
    }
}

impl<N: ProviderNodeTypes> ProviderFactory<N> {
    /// Returns a provider with a created `DbTx` inside, which allows fetching data from the
    /// database using different types of providers. Example: [`HeaderProvider`]
    /// [`BlockHashReader`]. This may fail if the inner read database transaction fails to open.
    ///
    /// This sets the [`PruneModes`] to [`None`], because they should only be relevant for writing
    /// data.
    #[track_caller]
    pub fn provider(&self) -> ProviderResult<DatabaseProviderRO<N::DB, N>> {
        if matches!(self.read_mode, ReadMode::Rpc) {
            return self.capture_view_provider(false).map(|(_, provider)| provider)
        }
        let tx = match self.read_view() {
            Some(view) => self.db.tx_with_snapshot(view.db.clone())?,
            None => self.db.tx()?,
        };
        let mut provider = DatabaseProvider::new(
            tx,
            self.chain_spec.clone(),
            self.static_file_provider.clone(),
            self.prune_modes.clone(),
            self.storage.clone(),
            self.storage_settings.clone(),
        );
        provider.read_view = self.read_view().cloned();
        Ok(provider)
    }

    pub(crate) const fn uses_state_reads(&self) -> bool {
        !matches!(self.read_mode, ReadMode::Live)
    }

    const fn read_view(&self) -> Option<&Arc<ReadView>> {
        match &self.read_mode {
            ReadMode::Captured(view) => Some(view),
            ReadMode::Live | ReadMode::Rpc => None,
        }
    }

    pub(crate) fn has_consistent_view(&self) -> bool {
        self.read_view().is_some_and(|view| view.consistent)
    }

    pub(crate) fn capture_view(&self, consistent: bool) -> ProviderResult<Self> {
        if self.read_view().is_some() && (!consistent || self.has_consistent_view()) {
            return Ok(self.clone())
        }
        self.capture_view_provider(consistent).map(|(factory, _)| factory)
    }

    /// Returns the transaction used to capture bounds so the caller can continue reading it.
    pub(crate) fn capture_view_provider(
        &self,
        consistent: bool,
    ) -> ProviderResult<(Self, DatabaseProviderRO<N::DB, N>)> {
        if self.read_view().is_some() && (!consistent || self.has_consistent_view()) {
            return Ok((self.clone(), self.provider()?))
        }
        let snapshot = self.db.snapshot(consistent)?;
        let mut provider: DatabaseProvider<_, N> = DatabaseProvider::new(
            self.db.tx_with_snapshot(snapshot.clone())?,
            self.chain_spec.clone(),
            self.static_file_provider.clone(),
            self.prune_modes.clone(),
            self.storage.clone(),
            self.storage_settings.clone(),
        );
        let block_number =
            provider.get_stage_checkpoint(StageId::Execution)?.unwrap_or_default().block_number;
        // Gravity persistence commits state atomically at Execution. A failed trie commit can
        // leave Merkle behind after releasing the write lock, so cross-store reads must check it.
        if consistent &&
            provider
                .get_stage_checkpoint(StageId::MerkleExecute)?
                .unwrap_or_default()
                .block_number !=
                block_number
        {
            return Err(ProviderError::other(std::io::Error::other(
                "state and Merkle checkpoints do not match",
            )))
        }
        let next_tx_num = provider
            .block_body_indices(block_number)?
            .ok_or(ProviderError::BlockBodyIndicesNotFound(block_number))?
            .next_tx_num();
        let mut factory = self.clone();
        factory.read_mode = ReadMode::Captured(Arc::new(ReadView {
            db: snapshot,
            consistent,
            bounds: ReadBounds { block_number, next_tx_num },
        }));
        provider.read_view = factory.read_view().cloned();
        Ok((factory, provider))
    }

    /// Returns a provider with a created `DbTxMut` inside, which allows fetching and updating
    /// data from the database using different types of providers. Example: [`HeaderProvider`]
    /// [`BlockHashReader`].  This may fail if the inner read/write database transaction fails to
    /// open.
    #[track_caller]
    pub fn provider_rw(&self) -> ProviderResult<DatabaseProviderRW<N::DB, N>> {
        Ok(DatabaseProviderRW(DatabaseProvider::new_rw(
            self.db.tx_mut()?,
            self.chain_spec.clone(),
            self.static_file_provider.clone(),
            self.prune_modes.clone(),
            self.storage.clone(),
            self.storage_settings.clone(),
        )))
    }

    /// State provider for latest block
    #[track_caller]
    pub fn latest(&self) -> ProviderResult<StateProviderBox> {
        trace!(target: "providers::db", "Returning latest state provider");
        Ok(Box::new(LatestStateProvider::new(self.database_provider_ro()?)))
    }

    /// Storage provider for state at that given block
    pub fn history_by_block_number(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<StateProviderBox> {
        let state_provider = self.provider()?.try_into_history_at_block(block_number)?;
        trace!(target: "providers::db", ?block_number, "Returning historical state provider for block number");
        Ok(state_provider)
    }

    /// Storage provider for state at that given block hash
    pub fn history_by_block_hash(&self, block_hash: BlockHash) -> ProviderResult<StateProviderBox> {
        let provider = self.provider()?;

        let block_number = provider
            .block_number(block_hash)?
            .ok_or(ProviderError::BlockHashNotFound(block_hash))?;

        let state_provider = provider.try_into_history_at_block(block_number)?;
        trace!(target: "providers::db", ?block_number, %block_hash, "Returning historical state provider for block hash");
        Ok(state_provider)
    }
}

impl<N: NodeTypesWithDB> NodePrimitivesProvider for ProviderFactory<N> {
    type Primitives = N::Primitives;
}

impl<N: ProviderNodeTypes> ProviderReadView for ProviderFactory<N> {
    fn view(&self) -> ProviderResult<Self> {
        self.capture_view(false)
    }

    fn consistent_view(&self) -> ProviderResult<Self> {
        self.capture_view(true)
    }

    fn rpc_provider(&self) -> Self {
        let mut factory = self.clone();
        if matches!(factory.read_mode, ReadMode::Live) {
            factory.read_mode = ReadMode::Rpc;
        }
        factory
    }
}

impl<N: ProviderNodeTypes> DatabaseProviderFactory for ProviderFactory<N> {
    type DB = N::DB;
    type Provider = DatabaseProvider<<N::DB as Database>::TX, N>;
    type ProviderRW = DatabaseProvider<<N::DB as Database>::TXMut, N>;

    fn database_provider_ro(&self) -> ProviderResult<Self::Provider> {
        self.provider()
    }

    fn database_provider_rw(&self) -> ProviderResult<Self::ProviderRW> {
        self.provider_rw().map(|provider| provider.0)
    }
}

impl<N: NodeTypesWithDB> StaticFileProviderFactory for ProviderFactory<N> {
    /// Returns static file provider
    fn static_file_provider(&self) -> StaticFileProvider<Self::Primitives> {
        self.static_file_provider.clone()
    }

    fn get_static_file_writer(
        &self,
        block: BlockNumber,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>> {
        self.static_file_provider.get_writer(block, segment)
    }
}

impl<N: ProviderNodeTypes> HeaderSyncGapProvider for ProviderFactory<N> {
    type Header = HeaderTy<N>;
    fn local_tip_header(
        &self,
        highest_uninterrupted_block: BlockNumber,
    ) -> ProviderResult<SealedHeader<Self::Header>> {
        self.provider()?.local_tip_header(highest_uninterrupted_block)
    }
}

impl<N: ProviderNodeTypes> HeaderProvider for ProviderFactory<N> {
    type Header = HeaderTy<N>;

    fn header(&self, block_hash: &BlockHash) -> ProviderResult<Option<Self::Header>> {
        self.provider()?.header(block_hash)
    }

    fn header_by_number(&self, num: BlockNumber) -> ProviderResult<Option<Self::Header>> {
        if self.uses_state_reads() {
            return self.provider()?.header_by_number(num)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Headers,
            num,
            |static_file| static_file.header_by_number(num),
            || self.provider()?.header_by_number(num),
        )
    }

    fn header_td(&self, hash: &BlockHash) -> ProviderResult<Option<U256>> {
        self.provider()?.header_td(hash)
    }

    fn header_td_by_number(&self, number: BlockNumber) -> ProviderResult<Option<U256>> {
        self.provider()?.header_td_by_number(number)
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Self::Header>> {
        if self.uses_state_reads() {
            return self.provider()?.headers_range(range)
        }
        self.static_file_provider.get_range_with_static_file_or_database(
            StaticFileSegment::Headers,
            to_range(range),
            |static_file, range, _| static_file.headers_range(range),
            |range, _| self.provider()?.headers_range(range),
            |_| true,
        )
    }

    fn sealed_header(
        &self,
        number: BlockNumber,
    ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        if self.uses_state_reads() {
            return self.provider()?.sealed_header(number)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Headers,
            number,
            |static_file| static_file.sealed_header(number),
            || self.provider()?.sealed_header(number),
        )
    }

    fn sealed_headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        self.sealed_headers_while(range, |_| true)
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<BlockNumber>,
        predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        if self.uses_state_reads() {
            return self.provider()?.sealed_headers_while(range, predicate)
        }
        self.static_file_provider.get_range_with_static_file_or_database(
            StaticFileSegment::Headers,
            to_range(range),
            |static_file, range, predicate| static_file.sealed_headers_while(range, predicate),
            |range, predicate| self.provider()?.sealed_headers_while(range, predicate),
            predicate,
        )
    }
}

impl<N: ProviderNodeTypes> BlockHashReader for ProviderFactory<N> {
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        if self.uses_state_reads() {
            return self.provider()?.block_hash(number)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Headers,
            number,
            |static_file| static_file.block_hash(number),
            || self.provider()?.block_hash(number),
        )
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        if self.uses_state_reads() {
            return self.provider()?.canonical_hashes_range(start, end)
        }
        self.static_file_provider.get_range_with_static_file_or_database(
            StaticFileSegment::Headers,
            start..end,
            |static_file, range, _| static_file.canonical_hashes_range(range.start, range.end),
            |range, _| self.provider()?.canonical_hashes_range(range.start, range.end),
            |_| true,
        )
    }
}

impl<N: ProviderNodeTypes> BlockNumReader for ProviderFactory<N> {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        self.provider()?.chain_info()
    }

    fn best_block_number(&self) -> ProviderResult<BlockNumber> {
        self.provider()?.best_block_number()
    }

    fn last_block_number(&self) -> ProviderResult<BlockNumber> {
        self.provider()?.last_block_number()
    }

    fn recover_block_number(&self) -> ProviderResult<BlockNumber> {
        self.provider()?.recover_block_number()
    }

    fn earliest_block_number(&self) -> ProviderResult<BlockNumber> {
        // earliest history height tracks the lowest block number that has __not__ been expired, in
        // other words, the first/earliest available block.
        Ok(self.static_file_provider.earliest_history_height())
    }

    fn block_number(&self, hash: B256) -> ProviderResult<Option<BlockNumber>> {
        self.provider()?.block_number(hash)
    }
}

impl<N: ProviderNodeTypes> BlockReader for ProviderFactory<N> {
    type Block = BlockTy<N>;

    fn find_block_by_hash(
        &self,
        hash: B256,
        source: BlockSource,
    ) -> ProviderResult<Option<Self::Block>> {
        self.provider()?.find_block_by_hash(hash, source)
    }

    fn block(&self, id: BlockHashOrNumber) -> ProviderResult<Option<Self::Block>> {
        self.provider()?.block(id)
    }

    fn pending_block(&self) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        self.provider()?.pending_block()
    }

    fn pending_block_and_receipts(
        &self,
    ) -> ProviderResult<Option<(RecoveredBlock<Self::Block>, Vec<Self::Receipt>)>> {
        self.provider()?.pending_block_and_receipts()
    }

    fn recovered_block(
        &self,
        id: BlockHashOrNumber,
        transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        self.provider()?.recovered_block(id, transaction_kind)
    }

    fn sealed_block_with_senders(
        &self,
        id: BlockHashOrNumber,
        transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        self.provider()?.sealed_block_with_senders(id, transaction_kind)
    }

    fn block_range(&self, range: RangeInclusive<BlockNumber>) -> ProviderResult<Vec<Self::Block>> {
        self.provider()?.block_range(range)
    }

    fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        self.provider()?.block_with_senders_range(range)
    }

    fn recovered_block_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        self.provider()?.recovered_block_range(range)
    }
}

impl<N: ProviderNodeTypes> TransactionsProvider for ProviderFactory<N> {
    type Transaction = TxTy<N>;

    fn transaction_id(&self, tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
        self.provider()?.transaction_id(tx_hash)
    }

    fn transaction_by_id(&self, id: TxNumber) -> ProviderResult<Option<Self::Transaction>> {
        if self.uses_state_reads() {
            return self.provider()?.transaction_by_id(id)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Transactions,
            id,
            |static_file| static_file.transaction_by_id(id),
            || self.provider()?.transaction_by_id(id),
        )
    }

    fn transaction_by_id_unhashed(
        &self,
        id: TxNumber,
    ) -> ProviderResult<Option<Self::Transaction>> {
        if self.uses_state_reads() {
            return self.provider()?.transaction_by_id_unhashed(id)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Transactions,
            id,
            |static_file| static_file.transaction_by_id_unhashed(id),
            || self.provider()?.transaction_by_id_unhashed(id),
        )
    }

    fn transaction_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Transaction>> {
        self.provider()?.transaction_by_hash(hash)
    }

    fn transaction_by_hash_with_meta(
        &self,
        tx_hash: TxHash,
    ) -> ProviderResult<Option<(Self::Transaction, TransactionMeta)>> {
        self.provider()?.transaction_by_hash_with_meta(tx_hash)
    }

    fn transaction_block(&self, id: TxNumber) -> ProviderResult<Option<BlockNumber>> {
        self.provider()?.transaction_block(id)
    }

    fn transactions_by_block(
        &self,
        id: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Transaction>>> {
        self.provider()?.transactions_by_block(id)
    }

    fn transactions_by_block_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Transaction>>> {
        self.provider()?.transactions_by_block_range(range)
    }

    fn transactions_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Transaction>> {
        self.provider()?.transactions_by_tx_range(range)
    }

    fn senders_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Address>> {
        self.provider()?.senders_by_tx_range(range)
    }

    fn transaction_sender(&self, id: TxNumber) -> ProviderResult<Option<Address>> {
        self.provider()?.transaction_sender(id)
    }
}

impl<N: ProviderNodeTypes> ReceiptProvider for ProviderFactory<N> {
    type Receipt = ReceiptTy<N>;
    fn receipt(&self, id: TxNumber) -> ProviderResult<Option<Self::Receipt>> {
        if self.uses_state_reads() {
            return self.provider()?.receipt(id)
        }
        self.static_file_provider.get_with_static_file_or_database(
            StaticFileSegment::Receipts,
            id,
            |static_file| static_file.receipt(id),
            || self.provider()?.receipt(id),
        )
    }

    fn receipt_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Receipt>> {
        self.provider()?.receipt_by_hash(hash)
    }

    fn receipts_by_block(
        &self,
        block: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Receipt>>> {
        self.provider()?.receipts_by_block(block)
    }

    fn receipts_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Receipt>> {
        if self.uses_state_reads() {
            return self.provider()?.receipts_by_tx_range(range)
        }
        self.static_file_provider.get_range_with_static_file_or_database(
            StaticFileSegment::Receipts,
            to_range(range),
            |static_file, range, _| static_file.receipts_by_tx_range(range),
            |range, _| self.provider()?.receipts_by_tx_range(range),
            |_| true,
        )
    }

    fn receipts_by_block_range(
        &self,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Receipt>>> {
        self.provider()?.receipts_by_block_range(block_range)
    }
}

impl<N: ProviderNodeTypes> BlockBodyIndicesProvider for ProviderFactory<N> {
    fn block_body_indices(
        &self,
        number: BlockNumber,
    ) -> ProviderResult<Option<StoredBlockBodyIndices>> {
        self.provider()?.block_body_indices(number)
    }

    fn block_body_indices_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<StoredBlockBodyIndices>> {
        self.provider()?.block_body_indices_range(range)
    }
}

impl<N: ProviderNodeTypes> StageCheckpointReader for ProviderFactory<N> {
    fn get_stage_checkpoint(&self, id: StageId) -> ProviderResult<Option<StageCheckpoint>> {
        self.provider()?.get_stage_checkpoint(id)
    }

    fn get_stage_checkpoint_progress(&self, id: StageId) -> ProviderResult<Option<Vec<u8>>> {
        self.provider()?.get_stage_checkpoint_progress(id)
    }
    fn get_all_checkpoints(&self) -> ProviderResult<Vec<(String, StageCheckpoint)>> {
        self.provider()?.get_all_checkpoints()
    }
}

impl<N: NodeTypesWithDB> ChainSpecProvider for ProviderFactory<N> {
    type ChainSpec = N::ChainSpec;

    fn chain_spec(&self) -> Arc<N::ChainSpec> {
        self.chain_spec.clone()
    }
}

impl<N: ProviderNodeTypes> PruneCheckpointReader for ProviderFactory<N> {
    fn get_prune_checkpoint(
        &self,
        segment: PruneSegment,
    ) -> ProviderResult<Option<PruneCheckpoint>> {
        self.provider()?.get_prune_checkpoint(segment)
    }

    fn get_prune_checkpoints(&self) -> ProviderResult<Vec<(PruneSegment, PruneCheckpoint)>> {
        self.provider()?.get_prune_checkpoints()
    }
}

impl<N: ProviderNodeTypes> HashedPostStateProvider for ProviderFactory<N> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle_state.state())
    }
}

impl<N> fmt::Debug for ProviderFactory<N>
where
    N: NodeTypesWithDB<DB: fmt::Debug, ChainSpec: fmt::Debug, Storage: fmt::Debug>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            db,
            chain_spec,
            static_file_provider,
            prune_modes,
            storage,
            storage_settings: _,
            ..
        } = self;
        f.debug_struct("ProviderFactory")
            .field("db", &db)
            .field("chain_spec", &chain_spec)
            .field("static_file_provider", &static_file_provider)
            .field("prune_modes", &prune_modes)
            .field("storage", &storage)
            .finish()
    }
}

impl<N: NodeTypesWithDB> Clone for ProviderFactory<N> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            chain_spec: self.chain_spec.clone(),
            static_file_provider: self.static_file_provider.clone(),
            prune_modes: self.prune_modes.clone(),
            storage: self.storage.clone(),
            storage_settings: self.storage_settings.clone(),
            read_mode: self.read_mode.clone(),
        }
    }
}

impl<N: NodeTypesWithDB> StorageSettingsCache for ProviderFactory<N> {
    fn cached_storage_settings(&self) -> GravityStorageSettings {
        *self.storage_settings.read()
    }

    fn set_storage_settings_cache(&self, settings: GravityStorageSettings) {
        *self.storage_settings.write() = settings;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        providers::{StaticFileProvider, StaticFileWriter},
        test_utils::{blocks::TEST_BLOCK, create_test_provider_factory, MockNodeTypesWithDB},
        BlockHashReader, BlockNumReader, BlockWriter, DBProvider, HeaderSyncGapProvider,
        StorageLocation, TransactionsProvider,
    };
    use alloy_primitives::{TxNumber, B256, U256};
    use assert_matches::assert_matches;
    use reth_chainspec::ChainSpecBuilder;
    use reth_db::{
        test_utils::{create_test_static_files_dir, ERROR_TEMPDIR},
        DatabaseArguments,
    };
    use reth_db_api::tables;
    use reth_primitives_traits::SignerRecoverable;
    use reth_prune_types::{PruneMode, PruneModes};
    use reth_storage_errors::provider::ProviderError;
    use reth_testing_utils::generators::{self, random_block, random_header, BlockParams};
    use std::{ops::RangeInclusive, sync::Arc};

    #[test]
    fn common_history_provider() {
        let factory = create_test_provider_factory();
        let _ = factory.latest();
    }

    #[test]
    fn default_chain_info() {
        let factory = create_test_provider_factory();
        let provider = factory.provider().unwrap();

        let chain_info = provider.chain_info().expect("should be ok");
        assert_eq!(chain_info.best_number, 0);
        assert_eq!(chain_info.best_hash, B256::ZERO);
    }

    #[test]
    fn provider_flow() {
        let factory = create_test_provider_factory();
        let provider = factory.provider().unwrap();
        provider.block_hash(0).unwrap();
        let provider_rw = factory.provider_rw().unwrap();
        provider_rw.block_hash(0).unwrap();
        provider.block_hash(0).unwrap();
    }

    #[test]
    fn state_read_factory_hides_static_file_tail_before_state_commit() -> eyre::Result<()> {
        use crate::StageCheckpointWriter;
        use reth_ethereum_primitives::Receipt;

        for initial_tx_count in [0, 1] {
            let factory = create_test_provider_factory();
            let mut rng = generators::rng();
            let first = random_block(
                &mut rng,
                0,
                BlockParams { tx_count: Some(initial_tx_count), ..Default::default() },
            );
            let provider = factory.provider_rw()?;
            provider.insert_block(first.clone().try_recover()?, StorageLocation::StaticFiles)?;
            provider.update_pipeline_stages(0, false)?;
            let files = factory.static_file_provider();
            {
                let mut writer = files.latest_writer(StaticFileSegment::Receipts)?;
                writer.increment_block(0)?;
                for tx in 0..u64::from(initial_tx_count) {
                    writer.append_receipt(tx, &Receipt::default())?;
                }
            }
            files.commit()?;
            provider.commit()?;
            let rpc = Arc::new(Box::new(factory.clone())).rpc_provider();
            let captured = rpc.view()?;
            assert!(rpc.consistent_view()?.has_consistent_view());
            assert!(captured.provider()?.ensure_trie_read_allowed().is_err());

            let next =
                random_block(&mut rng, 1, BlockParams { tx_count: Some(1), ..Default::default() });
            let provider = factory.provider_rw()?;
            provider.insert_block(next.try_recover()?, StorageLocation::StaticFiles)?;
            let next_tx = u64::from(initial_tx_count);
            {
                let mut writer = files.latest_writer(StaticFileSegment::Receipts)?;
                writer.increment_block(1)?;
                writer.append_receipt(next_tx, &Receipt::default())?;
            }
            // Persistence publishes the file tail before its atomic state batch.
            files.commit()?;
            drop(provider);

            assert!(factory.header_by_number(1)?.is_some());
            assert!(factory.block_hash(1)?.is_some());
            assert!(factory.transaction_by_id(next_tx)?.is_some());
            assert!(factory.receipt(next_tx)?.is_some());

            assert!(captured.header_by_number(1)?.is_none());
            assert_eq!(captured.headers_range(0..=1)?.len(), 1);
            assert_eq!(captured.block_hash(0)?, Some(first.hash()));
            assert!(captured.block_hash(1)?.is_none());
            assert!(captured.transaction_by_id(next_tx)?.is_none());
            assert!(captured.receipt(next_tx)?.is_none());
            assert!(factory.rpc_provider().header_by_number(1)?.is_none());
        }
        Ok(())
    }

    #[test]
    fn provider_factory_with_database_path() {
        let chain_spec = ChainSpecBuilder::mainnet().build();
        let (_static_dir, static_dir_path) = create_test_static_files_dir();
        let factory = ProviderFactory::<MockNodeTypesWithDB<DatabaseEnv>>::new_with_database_path(
            tempfile::TempDir::new().expect(ERROR_TEMPDIR).keep(),
            Arc::new(chain_spec),
            DatabaseArguments::new(Default::default()),
            StaticFileProvider::read_write(static_dir_path).unwrap(),
        )
        .unwrap();

        let provider = factory.provider().unwrap();
        provider.block_hash(0).unwrap();
        let provider_rw = factory.provider_rw().unwrap();
        provider_rw.block_hash(0).unwrap();
        provider.block_hash(0).unwrap();
    }

    #[test]
    fn insert_block_with_prune_modes() {
        let block = TEST_BLOCK.clone();

        {
            let factory = create_test_provider_factory();
            let provider = factory.provider_rw().unwrap();
            assert_matches!(
                provider
                    .insert_block(block.clone().try_recover().unwrap(), StorageLocation::Database),
                Ok(_)
            );
            provider.commit_view().unwrap();
            assert_matches!(
                provider.transaction_sender(0), Ok(Some(sender))
                if sender == block.body().transactions[0].recover_signer().unwrap()
            );
            assert_matches!(
                provider.transaction_id(*block.body().transactions[0].tx_hash()),
                Ok(Some(0))
            );
        }

        {
            let factory = create_test_provider_factory();
            let prune_modes = PruneModes {
                sender_recovery: Some(PruneMode::Full),
                transaction_lookup: Some(PruneMode::Full),
                ..PruneModes::none()
            };
            let provider = factory.with_prune_modes(prune_modes).provider_rw().unwrap();
            assert_matches!(
                provider
                    .insert_block(block.clone().try_recover().unwrap(), StorageLocation::Database),
                Ok(_)
            );
            provider.commit_view().unwrap();
            assert_matches!(provider.transaction_sender(0), Ok(None));
            assert_matches!(
                provider.transaction_id(*block.body().transactions[0].tx_hash()),
                Ok(None)
            );
        }
    }

    #[test]
    fn gravity_system_senders_after_pruning() {
        use crate::{BlockReader, StaticFileProviderFactory, TransactionVariant};
        use alloy_consensus::TxLegacy;
        use alloy_primitives::{Address, Signature};
        use reth_chainspec::SYSTEM_CALLER;
        use reth_db_api::transaction::DbTxMut;
        use reth_ethereum_primitives::{Transaction, TransactionSigned};
        use reth_primitives_traits::RecoveredBlock;

        let mut block = TEST_BLOCK.clone().into_block();
        block.header.number = 0;
        let user_tx = block.body.transactions[0].clone();
        let user_sender = user_tx.recover_signer_unchecked().unwrap();
        let system_tx = TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy::default()),
            Signature::new(U256::ZERO, U256::ZERO, false),
        );
        assert!(system_tx.recover_signer_unchecked().is_err());
        block.body.transactions = vec![system_tx.clone(), user_tx, system_tx];

        for (location, retained) in [
            (StorageLocation::Database, false),
            (StorageLocation::Database, true),
            (StorageLocation::StaticFiles, false),
            (StorageLocation::StaticFiles, true),
        ] {
            let factory = create_test_provider_factory().with_prune_modes(PruneModes {
                sender_recovery: Some(PruneMode::Full),
                ..PruneModes::none()
            });
            let provider = factory.provider_rw().unwrap();
            let mut expected = vec![SYSTEM_CALLER, user_sender, SYSTEM_CALLER];
            provider
                .insert_block(
                    RecoveredBlock::new_unhashed(block.clone(), expected.clone()),
                    location,
                )
                .unwrap();
            provider.commit_view().unwrap();
            provider.static_file_provider().commit().unwrap();
            assert!(provider.senders_by_tx_range(0..3).unwrap().is_empty());
            if location.static_files() {
                let static_files = provider.static_file_provider();
                let jar = static_files
                    .get_segment_provider_from_transaction(StaticFileSegment::Transactions, 0, None)
                    .unwrap();
                assert_eq!(static_files.senders_by_tx_range(0..3).unwrap(), expected);
                assert_eq!(jar.senders_by_tx_range(0..3).unwrap(), expected);
                for (id, sender) in expected.iter().enumerate() {
                    assert_eq!(static_files.transaction_sender(id as u64).unwrap(), Some(*sender));
                    assert_eq!(jar.transaction_sender(id as u64).unwrap(), Some(*sender));
                }
                assert_eq!(static_files.transaction_sender(3).unwrap(), None);
                assert_eq!(jar.transaction_sender(3).unwrap(), None);
            }
            if retained {
                // A retained entry in the middle must remain aligned with its transaction ID.
                expected[1] = Address::repeat_byte(0x42);
                provider.tx_ref().put::<tables::TransactionSenders>(1, expected[1]).unwrap();
                provider.commit_view().unwrap();
            }
            let by_number =
                provider.recovered_block(0.into(), TransactionVariant::WithHash).unwrap().unwrap();
            let by_hash = provider
                .sealed_block_with_senders(by_number.hash().into(), TransactionVariant::WithHash)
                .unwrap()
                .unwrap();
            let range = provider.block_with_senders_range(0..=0).unwrap();
            let sealed_range = provider.recovered_block_range(0..=0).unwrap();
            for recovered in [&by_number, &by_hash, &range[0], &sealed_range[0]] {
                assert_eq!(recovered.senders(), expected);
                assert_eq!(recovered.body().transactions, block.body.transactions);
            }
        }
    }

    #[test]
    fn take_block_transaction_range_recover_senders() {
        let factory = create_test_provider_factory();

        let mut rng = generators::rng();
        let block =
            random_block(&mut rng, 0, BlockParams { tx_count: Some(3), ..Default::default() });

        let tx_ranges: Vec<RangeInclusive<TxNumber>> = vec![0..=0, 1..=1, 2..=2, 0..=1, 1..=2];
        for range in tx_ranges {
            let provider = factory.provider_rw().unwrap();

            assert_matches!(
                provider
                    .insert_block(block.clone().try_recover().unwrap(), StorageLocation::Database),
                Ok(_)
            );
            provider.commit_view().unwrap();

            let senders = provider.take::<tables::TransactionSenders>(range.clone()).unwrap();
            assert_eq!(
                senders,
                range
                    .clone()
                    .map(|tx_number| (
                        tx_number,
                        block.body().transactions[tx_number as usize].recover_signer().unwrap()
                    ))
                    .collect::<Vec<_>>()
            );

            // todo fix: Why is empty
            // let db_senders = provider.senders_by_tx_range(range);
            // assert!(matches!(db_senders, Ok(ref v) if v.is_empty()));
        }
    }

    #[test]
    fn header_sync_gap_lookup() {
        let factory = create_test_provider_factory();
        let provider = factory.provider_rw().unwrap();

        let mut rng = generators::rng();

        // Genesis
        let checkpoint = 0;
        let head = random_header(&mut rng, 0, None);

        // Empty database
        assert_matches!(
            provider.local_tip_header(checkpoint),
            Err(ProviderError::HeaderNotFound(block_number))
                if block_number.as_number().unwrap() == checkpoint
        );

        // Checkpoint and no gap
        let static_file_provider = provider.static_file_provider();
        let mut static_file_writer =
            static_file_provider.latest_writer(StaticFileSegment::Headers).unwrap();
        static_file_writer.append_header(head.header(), U256::ZERO, &head.hash()).unwrap();
        static_file_writer.commit().unwrap();
        drop(static_file_writer);

        let local_head = provider.local_tip_header(checkpoint).unwrap();

        assert_eq!(local_head, head);
    }
}
