//! Implementation of the [`jsonrpsee`] generated [`EthApiServer`](crate::EthApi) trait
//! Handles RPC requests for the `eth_` namespace.

use std::{sync::Arc, time::Duration};

use crate::{eth::helpers::types::EthRpcConverter, EthApiBuilder};
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumberOrTag;
use alloy_network::Ethereum;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_client::RpcClient;
use derive_more::Deref;
use reth_chainspec::{ChainSpec, ChainSpecProvider};
use reth_evm_ethereum::EthEvmConfig;
use reth_network_api::noop::NoopNetwork;
use reth_rpc_convert::{RpcConvert, RpcConverter};
use reth_rpc_eth_api::{
    helpers::{pending_block::PendingEnvBuilder, spec::SignersForRpc, SpawnBlocking},
    node::{RpcNodeCoreAdapter, RpcNodeCoreExt},
    EthApiTypes, RpcNodeCore,
};
use reth_rpc_eth_types::{
    builder::config::PendingBlockKind, receipt::EthReceiptConverter, EthApiError, EthApiSettings,
    EthStateCache, FeeHistoryCache, GasCap, GasPriceOracle, PendingBlock,
};
use reth_storage_api::{noop::NoopProvider, BlockReaderIdExt, ProviderHeader};
use reth_tasks::{
    pool::{BlockingTaskGuard, BlockingTaskPool},
    Runtime,
};
use reth_transaction_pool::{
    blobstore::BlobSidecarConverter, noop::NoopTransactionPool, AddedTransactionOutcome,
    BatchTxProcessor, BatchTxRequest, TransactionPool,
};
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};

const DEFAULT_BROADCAST_CAPACITY: usize = 2000;

/// `Eth` API implementation.
///
/// This type provides the functionality for handling `eth_` related requests.
/// These are implemented two-fold: Core functionality is implemented as
/// [`EthApiSpec`](reth_rpc_eth_api::helpers::EthApiSpec) trait. Additionally, the required server
/// implementations (e.g. [`EthApiServer`](reth_rpc_eth_api::EthApiServer)) are implemented
/// separately in submodules. The rpc handler implementation can then delegate to the main impls.
/// This way [`EthApi`] is not limited to [`jsonrpsee`] and can be used standalone or in other
/// network handlers (for example ipc).
///
/// ## Trait requirements
///
/// While this type requires various unrestricted generic components, trait bounds are enforced when
/// additional traits are implemented for this type.
#[derive(Deref)]
pub struct EthApi<N: RpcNodeCore, Rpc: RpcConvert> {
    /// All nested fields bundled together.
    #[deref]
    pub(super) inner: Arc<EthApiInner<N, Rpc>>,
    /// Selects live execution reads, live RPC reads, or a captured request context.
    pub(super) read_context: EthApiReadContext<N>,
}

impl<N, Rpc> Clone for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), read_context: self.read_context.clone() }
    }
}

impl<N: RpcNodeCore, Rpc: RpcConvert> EthApi<N, Rpc> {
    /// Returns the provider selected for this handler.
    pub fn provider(&self) -> &N::Provider {
        match &self.read_context {
            EthApiReadContext::Node => self.inner.components.provider(),
            EthApiReadContext::LiveRpc => self.inner.provider(),
            EthApiReadContext::View { provider, .. } => provider,
        }
    }

    /// EVM configuration paired with the selected provider.
    pub fn evm_config(&self) -> &N::Evm {
        match &self.read_context {
            EthApiReadContext::Node | EthApiReadContext::LiveRpc => self.inner.evm_config(),
            EthApiReadContext::View { evm, .. } => evm,
        }
    }
}

/// Keeps the selected provider and auxiliary EVM reads in the same request context.
#[derive(Clone)]
pub(super) enum EthApiReadContext<N: RpcNodeCore> {
    /// Internal execution and on-chain configuration reads use the live node provider.
    Node,
    /// RPC requests capture their view when they begin reading related state.
    LiveRpc,
    /// A captured provider paired with the EVM configuration that reads from it.
    View { provider: N::Provider, evm: N::Evm },
}

impl
    EthApi<
        RpcNodeCoreAdapter<NoopProvider, NoopTransactionPool, NoopNetwork, EthEvmConfig>,
        EthRpcConverter<ChainSpec>,
    >
{
    /// Convenience fn to obtain a new [`EthApiBuilder`] instance with mandatory components.
    ///
    /// Creating an [`EthApi`] requires a few mandatory components:
    ///  - provider: The type responsible for fetching requested data from disk.
    ///  - transaction pool: To interact with the pool, submitting new transactions (e.g.
    ///    `eth_sendRawTransactions`).
    ///  - network: required to handle requests related to network state (e.g. `eth_syncing`).
    ///  - evm config: Knows how create a new EVM instance to transact,estimate,call,trace.
    ///
    /// # Create an instance with noop ethereum implementations
    ///
    /// ```no_run
    /// use alloy_network::Ethereum;
    /// use reth_evm_ethereum::EthEvmConfig;
    /// use reth_network_api::noop::NoopNetwork;
    /// use reth_provider::noop::NoopProvider;
    /// use reth_rpc::EthApi;
    /// use reth_transaction_pool::noop::NoopTransactionPool;
    /// let eth_api = EthApi::builder(
    ///     NoopProvider::default(),
    ///     NoopTransactionPool::default(),
    ///     NoopNetwork::default(),
    ///     EthEvmConfig::mainnet(),
    /// )
    /// .build();
    /// ```
    #[expect(clippy::type_complexity)]
    pub fn builder<Provider, Pool, Network, EvmConfig, ChainSpec>(
        provider: Provider,
        pool: Pool,
        network: Network,
        evm_config: EvmConfig,
    ) -> EthApiBuilder<
        RpcNodeCoreAdapter<Provider, Pool, Network, EvmConfig>,
        RpcConverter<Ethereum, EvmConfig, EthReceiptConverter<ChainSpec>>,
    >
    where
        RpcNodeCoreAdapter<Provider, Pool, Network, EvmConfig>:
            RpcNodeCore<Provider: ChainSpecProvider<ChainSpec = ChainSpec>, Evm = EvmConfig>,
    {
        EthApiBuilder::new(provider, pool, network, evm_config)
    }
}

impl<N, Rpc> EthApiTypes for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert<Error = EthApiError>,
{
    type Error = EthApiError;
    type NetworkTypes = Rpc::Network;
    type RpcConvert = Rpc;

    fn eth_api_settings(&self) -> &EthApiSettings {
        self.inner.eth_api_settings()
    }

    fn converter(&self) -> &Self::RpcConvert {
        &self.converter
    }
}

impl<N, Rpc> RpcNodeCore for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    type Primitives = N::Primitives;
    type Provider = N::Provider;
    type Pool = N::Pool;
    type Evm = N::Evm;
    type Network = N::Network;

    fn pool(&self) -> &Self::Pool {
        self.inner.pool()
    }

    fn evm_config(&self) -> &Self::Evm {
        self.evm_config()
    }

    fn network(&self) -> &Self::Network {
        self.inner.network()
    }

    fn provider(&self) -> &Self::Provider {
        self.provider()
    }
}

impl<N, Rpc> RpcNodeCoreExt for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    #[inline]
    fn cache(&self) -> &EthStateCache<N::Primitives> {
        self.inner.cache()
    }
}

impl<N, Rpc> std::fmt::Debug for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthApi").finish_non_exhaustive()
    }
}

impl<N, Rpc> SpawnBlocking for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert<Error = EthApiError>,
{
    #[inline]
    fn io_task_spawner(&self) -> &Runtime {
        self.inner.task_spawner()
    }

    #[inline]
    fn tracing_task_pool(&self) -> &BlockingTaskPool {
        self.inner.blocking_task_pool()
    }

    #[inline]
    fn tracing_task_guard(&self) -> &BlockingTaskGuard {
        self.inner.blocking_task_guard()
    }

    #[inline]
    fn blocking_io_task_guard(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        self.inner.blocking_io_request_semaphore()
    }
}

/// Container type `EthApi`
#[expect(missing_debug_implementations)]
pub struct EthApiInner<N: RpcNodeCore, Rpc: RpcConvert> {
    /// The components of the node.
    components: N,
    /// Provider used by RPC reads, independently of execution's node provider.
    provider: N::Provider,
    /// All configured Signers
    signers: SignersForRpc<N::Provider, Rpc::Network>,
    /// The async cache frontend for eth related data
    eth_cache: EthStateCache<N::Primitives>,
    /// The async gas oracle frontend for gas price suggestions
    gas_oracle: GasPriceOracle<N::Provider>,
    /// Settings shared by the `eth` RPC helpers.
    settings: EthApiSettings,
    /// The block number at which the node started
    starting_block: U256,
    /// The type that can spawn tasks which would otherwise block.
    task_spawner: Runtime,
    /// Cached pending block if any
    pending_block: Mutex<Option<PendingBlock<N::Primitives>>>,
    /// A pool dedicated to CPU heavy blocking tasks.
    blocking_task_pool: BlockingTaskPool,
    /// Cache for block fees history
    fee_history_cache: FeeHistoryCache<ProviderHeader<N::Provider>>,

    /// Guard for getproof calls
    blocking_task_guard: BlockingTaskGuard,

    /// Semaphore to limit concurrent blocking IO requests (`eth_call`, `eth_estimateGas`, etc.)
    blocking_io_request_semaphore: Arc<Semaphore>,

    /// Transaction broadcast channel
    raw_tx_sender: broadcast::Sender<Bytes>,

    /// Raw transaction forwarder
    raw_tx_forwarder: Option<RpcClient>,

    /// Converter for RPC types.
    converter: Rpc,

    /// Builder for pending block environment.
    next_env_builder: Box<dyn PendingEnvBuilder<N::Evm>>,

    /// Transaction batch sender for batching tx insertions
    tx_batch_sender:
        mpsc::UnboundedSender<BatchTxRequest<<N::Pool as TransactionPool>::Transaction>>,

    /// Blob sidecar converter
    blob_sidecar_converter: BlobSidecarConverter,
}

impl<N, Rpc> EthApiInner<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    /// Creates a new, shareable instance using the default tokio task spawner.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        components: N,
        eth_cache: EthStateCache<N::Primitives>,
        gas_oracle: GasPriceOracle<N::Provider>,
        gas_cap: impl Into<GasCap>,
        max_simulate_blocks: u64,
        compute_state_root_for_eth_simulate: bool,
        eth_proof_window: u64,
        blocking_task_pool: BlockingTaskPool,
        fee_history_cache: FeeHistoryCache<ProviderHeader<N::Provider>>,
        task_spawner: Runtime,
        proof_permits: usize,
        converter: Rpc,
        next_env: impl PendingEnvBuilder<N::Evm>,
        max_batch_size: usize,
        max_blocking_io_requests: usize,
        pending_block_kind: PendingBlockKind,
        raw_tx_forwarder: Option<RpcClient>,
        send_raw_transaction_sync_timeout: Duration,
        evm_memory_limit: u64,
        force_blob_sidecar_upcasting: bool,
    ) -> Self {
        let gas_cap: GasCap = gas_cap.into();
        Self::new_with_settings(
            components,
            eth_cache,
            gas_oracle,
            EthApiSettings {
                proof_permits,
                max_batch_size,
                max_blocking_io_requests,
                gas_cap: gas_cap.into(),
                max_simulate_blocks,
                compute_state_root_for_eth_simulate,
                eth_proof_window,
                pending_block_kind,
                send_raw_transaction_sync_timeout,
                evm_memory_limit,
                force_blob_sidecar_upcasting,
                sender_recovery_cache: None,
            },
            blocking_task_pool,
            fee_history_cache,
            task_spawner,
            converter,
            next_env,
            raw_tx_forwarder,
        )
    }

    /// Creates the API with one shared set of initialization and runtime settings.
    #[expect(clippy::too_many_arguments)]
    pub fn new_with_settings(
        components: N,
        eth_cache: EthStateCache<N::Primitives>,
        gas_oracle: GasPriceOracle<N::Provider>,
        settings: EthApiSettings,
        blocking_task_pool: BlockingTaskPool,
        fee_history_cache: FeeHistoryCache<ProviderHeader<N::Provider>>,
        task_spawner: Runtime,
        converter: Rpc,
        next_env: impl PendingEnvBuilder<N::Evm>,
        raw_tx_forwarder: Option<RpcClient>,
    ) -> Self {
        let signers = parking_lot::RwLock::new(Default::default());
        let provider = components.provider().clone();
        // get the block number of the latest block
        let starting_block = U256::from(
            provider
                .header_by_number_or_tag(BlockNumberOrTag::Latest)
                .ok()
                .flatten()
                .map(|header| header.number())
                .unwrap_or_default(),
        );

        let (raw_tx_sender, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

        // Create tx pool insertion batcher
        let (processor, tx_batch_sender) =
            BatchTxProcessor::new(components.pool().clone(), settings.max_batch_size);
        task_spawner.spawn_critical_task("tx-batcher", processor);

        Self {
            components,
            provider,
            signers,
            eth_cache,
            gas_oracle,
            starting_block,
            task_spawner,
            pending_block: Default::default(),
            blocking_task_pool,
            fee_history_cache,
            blocking_task_guard: BlockingTaskGuard::new(settings.proof_permits),
            blocking_io_request_semaphore: Arc::new(Semaphore::new(
                settings.max_blocking_io_requests,
            )),
            settings,
            raw_tx_sender,
            raw_tx_forwarder,
            converter,
            next_env_builder: Box::new(next_env),
            tx_batch_sender,
            blob_sidecar_converter: BlobSidecarConverter::new(),
        }
    }

    /// Overrides the provider without replacing the node's execution components.
    pub(crate) fn with_provider(mut self, provider: N::Provider) -> Self {
        self.provider = provider;
        self
    }
}

impl<N, Rpc> EthApiInner<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert,
{
    /// Returns a handle to data on disk.
    #[inline]
    pub const fn provider(&self) -> &N::Provider {
        &self.provider
    }

    /// Returns a handle to the transaction response builder.
    #[inline]
    pub const fn converter(&self) -> &Rpc {
        &self.converter
    }

    /// Returns a handle to data in memory.
    #[inline]
    pub const fn cache(&self) -> &EthStateCache<N::Primitives> {
        &self.eth_cache
    }

    /// Returns a handle to the pending block.
    #[inline]
    pub const fn pending_block(&self) -> &Mutex<Option<PendingBlock<N::Primitives>>> {
        &self.pending_block
    }

    /// Returns a type that knows how to build a [`reth_evm::ConfigureEvm::NextBlockEnvCtx`] for a
    /// pending block.
    #[inline]
    pub const fn pending_env_builder(&self) -> &dyn PendingEnvBuilder<N::Evm> {
        &*self.next_env_builder
    }

    /// Returns a handle to the task spawner.
    #[inline]
    pub const fn task_spawner(&self) -> &Runtime {
        &self.task_spawner
    }

    /// Returns a handle to the blocking thread pool.
    ///
    /// This is intended for tasks that are CPU bound.
    #[inline]
    pub const fn blocking_task_pool(&self) -> &BlockingTaskPool {
        &self.blocking_task_pool
    }

    /// Returns a handle to the EVM config.
    #[inline]
    pub fn evm_config(&self) -> &N::Evm {
        self.components.evm_config()
    }

    /// Returns a handle to the transaction pool.
    #[inline]
    pub fn pool(&self) -> &N::Pool {
        self.components.pool()
    }

    /// Returns the settings shared by the `eth` RPC helpers.
    #[inline]
    pub const fn eth_api_settings(&self) -> &EthApiSettings {
        &self.settings
    }

    /// Returns the gas cap.
    #[inline]
    pub const fn gas_cap(&self) -> u64 {
        self.settings.gas_cap
    }

    /// Returns the `max_simulate_blocks`.
    #[inline]
    pub const fn max_simulate_blocks(&self) -> u64 {
        self.settings.max_simulate_blocks
    }

    /// Returns whether state roots are computed for `eth_simulateV1`.
    #[inline]
    pub const fn compute_state_root_for_eth_simulate(&self) -> bool {
        self.settings.compute_state_root_for_eth_simulate
    }

    /// Returns a handle to the gas oracle.
    #[inline]
    pub const fn gas_oracle(&self) -> &GasPriceOracle<N::Provider> {
        &self.gas_oracle
    }

    /// Returns a handle to the fee history cache.
    #[inline]
    pub const fn fee_history_cache(&self) -> &FeeHistoryCache<ProviderHeader<N::Provider>> {
        &self.fee_history_cache
    }

    /// Returns a handle to the signers.
    #[inline]
    pub const fn signers(&self) -> &SignersForRpc<N::Provider, Rpc::Network> {
        &self.signers
    }

    /// Returns the starting block.
    #[inline]
    pub const fn starting_block(&self) -> U256 {
        self.starting_block
    }

    /// Returns the inner `Network`
    #[inline]
    pub fn network(&self) -> &N::Network {
        self.components.network()
    }

    /// The maximum number of blocks into the past for generating state proofs.
    #[inline]
    pub const fn eth_proof_window(&self) -> u64 {
        self.settings.eth_proof_window
    }

    /// Returns reference to [`BlockingTaskGuard`].
    #[inline]
    pub const fn blocking_task_guard(&self) -> &BlockingTaskGuard {
        &self.blocking_task_guard
    }

    /// Returns [`broadcast::Receiver`] of new raw transactions
    #[inline]
    pub fn subscribe_to_raw_transactions(&self) -> broadcast::Receiver<Bytes> {
        self.raw_tx_sender.subscribe()
    }

    /// Broadcasts raw transaction if there are active subscribers.
    #[inline]
    pub fn broadcast_raw_transaction(&self, raw_tx: Bytes) {
        let _ = self.raw_tx_sender.send(raw_tx);
    }

    /// Returns the transaction batch sender
    #[inline]
    pub const fn tx_batch_sender(
        &self,
    ) -> &mpsc::UnboundedSender<BatchTxRequest<<N::Pool as TransactionPool>::Transaction>> {
        &self.tx_batch_sender
    }

    /// Adds an _unvalidated_ transaction into the pool via the transaction batch sender.
    #[inline]
    pub async fn add_pool_transaction(
        &self,
        origin: reth_transaction_pool::TransactionOrigin,
        transaction: <N::Pool as TransactionPool>::Transaction,
    ) -> Result<AddedTransactionOutcome, EthApiError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let request = reth_transaction_pool::BatchTxRequest::new(origin, transaction, response_tx);

        self.tx_batch_sender()
            .send(request)
            .map_err(|_| reth_rpc_eth_types::EthApiError::BatchTxSendError)?;

        Ok(response_rx.await??)
    }

    /// Returns the pending block kind
    #[inline]
    pub const fn pending_block_kind(&self) -> PendingBlockKind {
        self.settings.pending_block_kind
    }

    /// Returns a handle to the raw transaction forwarder.
    #[inline]
    pub const fn raw_tx_forwarder(&self) -> Option<&RpcClient> {
        self.raw_tx_forwarder.as_ref()
    }

    /// Returns the timeout duration for `send_raw_transaction_sync` RPC method.
    #[inline]
    pub const fn send_raw_transaction_sync_timeout(&self) -> Duration {
        self.settings.send_raw_transaction_sync_timeout
    }

    /// Returns a handle to the blob sidecar converter.
    #[inline]
    pub const fn blob_sidecar_converter(&self) -> &BlobSidecarConverter {
        &self.blob_sidecar_converter
    }

    /// Returns the EVM memory limit.
    #[inline]
    pub const fn evm_memory_limit(&self) -> u64 {
        self.settings.evm_memory_limit
    }

    /// Returns a reference to the blocking IO request semaphore.
    #[inline]
    pub const fn blocking_io_request_semaphore(&self) -> &Arc<Semaphore> {
        &self.blocking_io_request_semaphore
    }

    /// Returns whether to force upcasting EIP-4844 blob sidecars to EIP-7594 format.
    #[inline]
    pub const fn force_blob_sidecar_upcasting(&self) -> bool {
        self.settings.force_blob_sidecar_upcasting
    }
}

#[cfg(test)]
mod tests {
    use crate::{eth::helpers::types::EthRpcConverter, EthApi, EthApiBuilder};
    use alloy_consensus::{Block, BlockBody, Header};
    use alloy_eips::{BlockId, BlockNumberOrTag};
    use alloy_network::TransactionBuilder;
    use alloy_primitives::{Address, Bytes, Signature, B256, U256, U64};
    use alloy_rpc_types::FeeHistory;
    use alloy_rpc_types_eth::{
        state::{AccountOverride, EvmOverrides, StateOverride},
        Bundle, TransactionRequest,
    };
    use alloy_rpc_types_trace::{parity::TraceType, tracerequest::TraceCallRequest};
    use jsonrpsee_types::error::INVALID_PARAMS_CODE;
    use rand::Rng;
    use reth_chain_state::CanonStateSubscriptions;
    use reth_chainspec::{ChainSpec, ChainSpecBuilder, ChainSpecProvider, EthChainSpec};
    use reth_ethereum_primitives::TransactionSigned;
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_primitives_traits::{Block as _, SealedHeader, SignerRecoverable};
    use reth_provider::{
        providers::BlockchainProvider,
        test_utils::{
            create_test_provider_factory, ExtendedAccount, MockEthProvider, NoopProvider,
        },
        PruneCheckpointReader, StageCheckpointReader,
    };
    use reth_rpc_eth_api::{
        helpers::{EthCall, EthTransactions, SpawnBlocking},
        node::RpcNodeCoreAdapter,
        EthApiServer,
    };
    use reth_rpc_eth_types::{EthConfig, RpcInvalidTransactionError};
    use reth_storage_api::{BlockReaderIdExt, ProviderReadView, StateProviderFactory};
    use reth_tasks::cancel::is_cancelled;
    use reth_testing_utils::generators;
    use reth_transaction_pool::test_utils::{testing_pool, TestPool};
    use std::time::{Duration, Instant};

    type FakeEthApi<P = MockEthProvider> = EthApi<
        RpcNodeCoreAdapter<P, TestPool, NoopNetwork, EthEvmConfig>,
        EthRpcConverter<ChainSpec>,
    >;

    fn build_test_eth_api<
        P: BlockReaderIdExt<
                Block = reth_ethereum_primitives::Block,
                Receipt = reth_ethereum_primitives::Receipt,
                Header = alloy_consensus::Header,
                Transaction = reth_ethereum_primitives::TransactionSigned,
            > + ChainSpecProvider<ChainSpec = ChainSpec>
            + StateProviderFactory
            + ProviderReadView
            + CanonStateSubscriptions<Primitives = reth_ethereum_primitives::EthPrimitives>
            + StageCheckpointReader
            + PruneCheckpointReader
            + Unpin
            + Clone
            + 'static,
    >(
        provider: P,
    ) -> FakeEthApi<P> {
        EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .build()
    }

    #[test]
    fn shared_settings_preserve_builder_limits() {
        let api = EthApiBuilder::new(
            MockEthProvider::default(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::mainnet(),
        )
        .gas_cap(123_456u64.into())
        .max_simulate_blocks(7)
        .eth_proof_window(19)
        .proof_permits(3)
        .max_batch_size(5)
        .max_blocking_io_requests(11)
        .evm_memory_limit(4096)
        .send_raw_transaction_sync_timeout(Duration::from_secs(17))
        .build();
        let settings = api.eth_api_settings();
        assert_eq!(settings.gas_cap, 123_456);
        assert_eq!(settings.max_simulate_blocks, 7);
        assert_eq!(settings.eth_proof_window, 19);
        assert_eq!(settings.proof_permits, 3);
        assert_eq!(settings.max_batch_size, 5);
        assert_eq!(settings.max_blocking_io_requests, 11);
        assert_eq!(settings.evm_memory_limit, 4096);
        assert_eq!(settings.send_raw_transaction_sync_timeout, Duration::from_secs(17));
        assert!(settings.sender_recovery_cache.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ordinary_rpc_does_not_wait_for_consistent_view() {
        use reth_db_api::database::Database;
        use reth_rpc_eth_api::helpers::EthState;
        use std::{sync::mpsc, time::Duration};

        use reth_storage_api::{BlockWriter, StageCheckpointWriter, StorageLocation};
        let factory = create_test_provider_factory();
        let genesis = reth_ethereum_primitives::Block::default().seal_slow();
        let writer = factory.provider_rw().unwrap();
        writer
            .insert_block(genesis.clone().try_recover().unwrap(), StorageLocation::Database)
            .unwrap();
        writer.update_pipeline_stages(0, false).unwrap();
        writer.commit().unwrap();
        let provider =
            BlockchainProvider::with_latest(factory.clone(), genesis.clone_sealed_header())
                .unwrap();
        let eth_api = build_test_eth_api(provider.rpc_provider());

        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let commit = std::thread::spawn(move || {
            factory.db_ref().with_write_lock(|| {
                locked_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            });
        });
        locked_rx.await.unwrap();
        let trie_api = eth_api.clone();
        let address = Address::repeat_byte(0x71);
        let mut trie_query = tokio::spawn(async move {
            let (account, proof) = tokio::join!(
                EthState::get_account(&trie_api, address, BlockNumberOrTag::Latest.into()),
                EthState::get_proof(&trie_api, address, Vec::new(), None).unwrap(),
            );
            assert!(account.unwrap().is_none());
            let proof = proof.unwrap();
            assert_eq!(proof.balance, U256::ZERO);
            assert_eq!(proof.nonce, 0);
            assert_eq!(proof.storage_hash, B256::ZERO);
        });

        let balance = tokio::time::timeout(
            Duration::from_secs(5),
            EthState::balance(&eth_api, address, None),
        )
        .await
        .expect("ordinary RPC must not wait for a consistent view")
        .unwrap();
        assert_eq!(balance, U256::ZERO);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut trie_query).await.is_err());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), trie_query).await.unwrap().unwrap();
        commit.join().unwrap();
    }

    #[tokio::test]
    async fn multi_proof_matches_individual_nested_proofs_and_preserves_duplicates() {
        use alloy_primitives::keccak256;
        use reth_primitives_traits::Account;
        use reth_rpc_eth_api::helpers::EthState;
        use reth_storage_api::{
            BlockWriter, StageCheckpointWriter, StateWriter, StorageLocation, TrieWriterV2,
        };
        use reth_trie_common::{HashedPostState, HashedStorage};
        use reth_trie_db::nested_hash::NestedStateRoot;

        let address = Address::repeat_byte(0x31);
        let absent = Address::repeat_byte(0x32);
        let slot = B256::with_last_byte(7);
        let factory = create_test_provider_factory();
        let writer = factory.provider_rw().unwrap();
        let mut state = HashedPostState::default();
        state.accounts.insert(
            keccak256(address),
            Some(Account { balance: U256::from(42), nonce: 3, ..Default::default() }),
        );
        state.storages.insert(
            keccak256(address),
            HashedStorage {
                wiped: false,
                storage: std::iter::once((keccak256(slot), U256::from(11))).collect(),
            },
        );
        let (root, updates) =
            NestedStateRoot::new(writer.tx_ref(), None).calculate(&state).unwrap();
        writer.write_hashed_state(&state.into_sorted()).unwrap();
        writer.write_trie_updatesv2(&updates).unwrap();
        let genesis = reth_ethereum_primitives::Block {
            header: Header { state_root: root, ..Default::default() },
            body: Default::default(),
        }
        .seal_slow();
        writer
            .insert_block(genesis.clone().try_recover().unwrap(), StorageLocation::Database)
            .unwrap();
        writer.update_pipeline_stages(0, false).unwrap();
        writer.commit().unwrap();
        let provider =
            BlockchainProvider::with_latest(factory, genesis.clone_sealed_header()).unwrap();
        let api = build_test_eth_api(provider.rpc_provider());
        let targets = vec![(address, vec![slot, slot]), (absent, vec![slot]), (address, vec![])];
        let proofs = EthState::get_multi_proof(&api, targets.clone(), None).unwrap().await.unwrap();
        assert_eq!(proofs.len(), targets.len());
        for ((address, slots), proof) in targets.into_iter().zip(proofs.iter()) {
            let individual = EthState::get_proof(
                &api,
                address,
                slots.into_iter().map(Into::into).collect(),
                None,
            )
            .unwrap()
            .await
            .unwrap();
            assert_eq!(*proof, individual);
        }
        assert_eq!(proofs[0].balance, U256::from(42));
        assert_eq!(proofs[0].storage_proof.len(), 2);
        assert_eq!(proofs[0].storage_proof[0].value, U256::from(11));
        assert_eq!(proofs[1].storage_hash, B256::ZERO);
        let oversized =
            vec![(address, vec![]); reth_rpc_server_types::constants::DEFAULT_MAX_PROOF_SLOTS + 1];
        assert!(EthState::get_multi_proof(&api, oversized, None).is_err());
    }

    #[tokio::test]
    async fn simulate_bal_hash_resets_each_block_across_amsterdam() {
        use alloy_rpc_types_eth::simulate::{SimBlock, SimulatePayload};
        let provider = MockEthProvider::default().with_chain_spec(
            ChainSpecBuilder::mainnet().osaka_activated().with_amsterdam_at(24).build(),
        );
        let header = Header {
            number: 0,
            timestamp: 0,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        };
        provider.add_block(header.hash_slow(), Block { header, body: BlockBody::default() });
        let api = build_test_eth_api(provider);
        let from = Address::with_last_byte(0x41);
        let contract = Address::with_last_byte(0x42);
        let state_overrides = StateOverride::from_iter([
            (from, AccountOverride::default().with_nonce(0)),
            (
                contract,
                AccountOverride::default().with_code(alloy_primitives::bytes!("5f545f5260205ff3")),
            ),
        ]);
        let call_block = SimBlock::default()
            .with_state_overrides(state_overrides)
            .call(TransactionRequest::default().with_from(from).with_to(contract));
        for trace_transfers in [false, true] {
            let mut payload = SimulatePayload::default()
                .extend(SimBlock::default())
                .extend(call_block.clone())
                .extend(call_block.clone())
                .extend(SimBlock::default());
            payload.trace_transfers = trace_transfers;
            let result =
                EthCall::simulate_v1(&api, payload, Some(BlockId::latest())).await.unwrap();
            assert_eq!(result.len(), 4);
            assert_eq!(result[0].inner.header.block_access_list_hash, None);
            let bal_hash =
                result[1].inner.header.block_access_list_hash.expect("Amsterdam BAL hash");
            assert!(result[1].calls[0].status);
            assert!(result[2].calls[0].status);
            assert_eq!(result[2].inner.header.block_access_list_hash, Some(bal_hash));
            let empty_hash =
                result[3].inner.header.block_access_list_hash.expect("empty block BAL hash");
            assert_ne!(bal_hash, empty_hash);
            for pair in result.windows(2) {
                assert_eq!(pair[1].inner.header.parent_hash, pair[0].inner.header.hash);
            }
            for block in &result {
                assert_eq!(block.inner.header.hash, block.inner.header.inner.hash_slow());
            }
        }
    }

    #[tokio::test]
    async fn simulate_transfer_logs_across_amsterdam() {
        use alloy_rpc_types_eth::simulate::{SimBlock, SimulatePayload, SimulatedBlock};
        let provider = MockEthProvider::default().with_chain_spec(
            ChainSpecBuilder::mainnet().osaka_activated().with_amsterdam_at(24).build(),
        );
        let header = Header {
            number: 0,
            timestamp: 0,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        };
        provider.add_block(header.hash_slow(), Block { header, body: BlockBody::default() });
        let api = build_test_eth_api(provider);
        let from = Address::with_last_byte(0x41);
        let to = Address::with_last_byte(0x42);
        let reverting = Address::with_last_byte(0x43);
        let call_block = SimBlock::default()
            .with_state_overrides(StateOverride::from_iter([
                (from, AccountOverride::default().with_balance(U256::from(100)).with_nonce(0)),
                (
                    reverting,
                    AccountOverride::default().with_code(alloy_primitives::bytes!("5f5ffd")),
                ),
            ]))
            .call(TransactionRequest::default().from(from).to(to).value(U256::from(1)))
            .call(TransactionRequest::default().from(from).to(reverting).value(U256::from(1)));
        let mut untraced: Option<Vec<SimulatedBlock>> = None;
        for trace_transfers in [false, true] {
            let mut payload = SimulatePayload::default()
                .extend(call_block.clone())
                .extend(call_block.clone())
                .extend(call_block.clone());
            payload.trace_transfers = trace_transfers;
            let result =
                EthCall::simulate_v1(&api, payload, Some(BlockId::latest())).await.unwrap();
            assert_eq!(result.len(), 3);
            for (index, block) in result.iter().enumerate() {
                assert_eq!(block.inner.header.timestamp, (index as u64 + 1) * 12);
                assert!(block.calls[0].status);
                let logs = &block.calls[0].logs;
                if index == 0 && !trace_transfers {
                    assert!(logs.is_empty());
                } else {
                    assert_eq!(logs.len(), 1);
                    let emitter = if index == 0 {
                        Address::repeat_byte(0xee)
                    } else {
                        revm::primitives::eip7708::ETH_TRANSFER_LOG_ADDRESS
                    };
                    assert_eq!(logs[0].address(), emitter);
                }
                assert!(!block.calls[1].status);
                assert!(block.calls[1].logs.is_empty());
                if index > 0 {
                    assert!(!block.inner.header.logs_bloom.is_zero());
                    if let Some(untraced) = &untraced {
                        assert_eq!(logs[0].inner, untraced[index].calls[0].logs[0].inner);
                        assert_eq!(
                            block.inner.header.logs_bloom,
                            untraced[index].inner.header.logs_bloom
                        );
                        assert_eq!(
                            block.inner.header.receipts_root,
                            untraced[index].inner.header.receipts_root
                        );
                    }
                }
            }
            untraced = Some(result);
        }
    }

    #[test]
    fn access_list_completes_with_one_blocking_worker() {
        use reth_rpc_eth_api::helpers::EthCall;
        use std::time::Duration;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let provider = MockEthProvider::default();
            let block = Block {
                header: Header { number: 12_244_000, gas_limit: 30_000_000, ..Default::default() },
                body: BlockBody::default(),
            };
            let hash = SealedHeader::seal_slow(block.header.clone()).hash();
            provider.add_block(hash, block);
            let eth_api = build_test_eth_api(provider);
            tokio::time::timeout(
                Duration::from_secs(5),
                EthCall::create_access_list_at(
                    &eth_api,
                    TransactionRequest {
                        to: Some(Address::repeat_byte(0x22).into()),
                        gas: Some(100_000),
                        gas_price: Some(0),
                        ..Default::default()
                    },
                    Some(hash.into()),
                    None,
                ),
            )
            .await
        });
        // A deadlocked request must not prevent the runtime from shutting down after the timeout.
        runtime.shutdown_background();
        let response = result.expect("access-list request exhausted the blocking pool").unwrap();
        assert!(response.error.is_none());
        assert_eq!(response.gas_used, U256::from(21_000));
    }

    // Function to prepare the EthApi with mock data
    fn prepare_eth_api(
        newest_block: u64,
        mut oldest_block: Option<B256>,
        block_count: u64,
        mock_provider: MockEthProvider,
    ) -> (FakeEthApi, Vec<u128>, Vec<f64>) {
        let mut rng = generators::rng();

        // Build mock data
        let mut gas_used_ratios = Vec::with_capacity(block_count as usize);
        let mut base_fees_per_gas = Vec::with_capacity(block_count as usize);
        let mut last_header = None;
        let mut parent_hash = B256::default();

        for i in (0..block_count).rev() {
            let hash = rng.random();
            // Note: Generates saner values to avoid invalid overflows later
            let gas_limit = rng.random::<u32>() as u64;
            let base_fee_per_gas: Option<u64> =
                rng.random::<bool>().then(|| rng.random::<u32>() as u64);
            let gas_used = rng.random::<u32>() as u64;

            let header = Header {
                number: newest_block - i,
                gas_limit,
                gas_used,
                base_fee_per_gas,
                parent_hash,
                ..Default::default()
            };
            last_header = Some(header.clone());
            parent_hash = hash;

            const TOTAL_TRANSACTIONS: usize = 100;
            let mut transactions = Vec::with_capacity(TOTAL_TRANSACTIONS);
            for _ in 0..TOTAL_TRANSACTIONS {
                let random_fee: u128 = rng.random();

                if let Some(base_fee_per_gas) = header.base_fee_per_gas {
                    let transaction = TransactionSigned::new_unhashed(
                        reth_ethereum_primitives::Transaction::Eip1559(
                            alloy_consensus::TxEip1559 {
                                max_priority_fee_per_gas: random_fee,
                                max_fee_per_gas: random_fee + base_fee_per_gas as u128,
                                ..Default::default()
                            },
                        ),
                        Signature::test_signature(),
                    );

                    transactions.push(transaction);
                } else {
                    let transaction = TransactionSigned::new_unhashed(
                        reth_ethereum_primitives::Transaction::Legacy(Default::default()),
                        Signature::test_signature(),
                    );

                    transactions.push(transaction);
                }
            }

            mock_provider.add_block(
                hash,
                Block {
                    header: header.clone(),
                    body: BlockBody { transactions, ..Default::default() },
                },
            );
            mock_provider.add_header(hash, header);

            oldest_block.get_or_insert(hash);
            gas_used_ratios.push(gas_used as f64 / gas_limit as f64);
            base_fees_per_gas.push(base_fee_per_gas.map(|fee| fee as u128).unwrap_or_default());
        }

        // Add final base fee (for the next block outside of the request)
        let last_header = last_header.unwrap();
        let spec = mock_provider.chain_spec();
        base_fees_per_gas.push(
            spec.next_block_base_fee(&last_header, last_header.timestamp).unwrap_or_default()
                as u128,
        );

        let eth_api = build_test_eth_api(mock_provider);

        (eth_api, base_fees_per_gas, gas_used_ratios)
    }

    /// Invalid block range
    #[tokio::test]
    async fn test_fee_history_empty() {
        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::fee_history(
            &build_test_eth_api(NoopProvider::default()),
            U64::from(1),
            BlockNumberOrTag::Latest,
            None,
        )
        .await;
        assert!(response.is_err());
        let error_object = response.unwrap_err();
        assert_eq!(error_object.code(), INVALID_PARAMS_CODE);
    }

    #[tokio::test]
    /// Invalid block range (request is before genesis)
    async fn test_fee_history_invalid_block_range_before_genesis() {
        let block_count = 10;
        let newest_block = 1337;
        let oldest_block = None;

        let (eth_api, _, _) =
            prepare_eth_api(newest_block, oldest_block, block_count, MockEthProvider::default());

        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::fee_history(
            &eth_api,
            U64::from(newest_block + 1),
            newest_block.into(),
            Some(vec![10.0]),
        )
        .await;

        assert!(response.is_err());
        let error_object = response.unwrap_err();
        assert_eq!(error_object.code(), INVALID_PARAMS_CODE);
    }

    #[tokio::test]
    /// Invalid block range (request is in the future)
    async fn test_fee_history_invalid_block_range_in_future() {
        let block_count = 10;
        let newest_block = 1337;
        let oldest_block = None;

        let (eth_api, _, _) =
            prepare_eth_api(newest_block, oldest_block, block_count, MockEthProvider::default());

        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::fee_history(
            &eth_api,
            U64::from(1),
            (newest_block + 1000).into(),
            Some(vec![10.0]),
        )
        .await;

        assert!(response.is_err());
        let error_object = response.unwrap_err();
        assert_eq!(error_object.code(), INVALID_PARAMS_CODE);
    }

    #[tokio::test]
    async fn test_call_many_maps_provider_block_lookup_error_with_eth_api_conversion() {
        let eth_api = build_test_eth_api(MockEthProvider::default());
        let bundles = vec![Bundle {
            transactions: vec![TransactionRequest::default()],
            block_override: None,
        }];

        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::call_many(
            &eth_api, bundles, None, None,
        )
        .await;

        let err = response.expect_err("call_many should fail when latest block lookup errors");
        let message = err.message().to_ascii_lowercase();
        assert!(
            message.contains("block not found"),
            "best block lookup should map via EthApiError::from(ProviderError): {message}"
        );
        assert!(
            !message.contains("best block does not exist"),
            "provider implementation detail should not leak from converted error: {message}"
        );
    }

    #[tokio::test]
    async fn test_call_many_keeps_header_not_found_when_block_hash_absent() {
        let eth_api = build_test_eth_api(NoopProvider::default());
        let bundles = vec![Bundle {
            transactions: vec![TransactionRequest::default()],
            block_override: None,
        }];

        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::call_many(
            &eth_api, bundles, None, None,
        )
        .await;

        let err =
            response.expect_err("call_many should fail when latest block hash is unavailable");
        let message = err.message().to_ascii_lowercase();
        assert!(
            message.contains("block not found"),
            "missing block hash should still map to block-not-found: {message}"
        );
    }

    #[tokio::test]
    async fn call_many_methods_limit_calls_and_share_gas_cap() {
        use alloy_rpc_types_trace::parity::TraceType;
        use reth_rpc_eth_types::EthConfig;
        use reth_rpc_server_types::constants::DEFAULT_MAX_CALL_MANY_CALLS;
        use reth_tasks::{pool::BlockingTaskGuard, Runtime};
        let provider = MockEthProvider::default();
        let header = Header { number: 1, gas_limit: 30_000_000, ..Default::default() };
        provider.add_block(header.hash_slow(), Block { header, body: BlockBody::default() });
        // Loops until it runs out of gas.
        let looping = Address::with_last_byte(0x43);
        provider.add_account(
            looping,
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(alloy_primitives::bytes!("5b600056")),
        );
        // A transfer to an empty account uses 21_000 gas, so a third one only gets 8_000 and a
        // loop after one transfer runs out of the 29_000 left.
        let api = build_test_eth_api_with_gas_cap(provider, 50_000);
        let trace_api =
            crate::TraceApi::new(api.clone(), BlockingTaskGuard::new(1), EthConfig::default());
        let debug_api = crate::DebugApi::new(
            api.clone(),
            BlockingTaskGuard::new(1),
            &Runtime::test(),
            futures::stream::empty(),
        );
        let transfer = TransactionRequest::default().to(Address::with_last_byte(0x42));
        let transfers = |calls| vec![transfer.clone(); calls];
        let transfer_and_loop = vec![transfer.clone(), TransactionRequest::default().to(looping)];
        let bundles = |calls: Vec<TransactionRequest>| {
            vec![Bundle { transactions: calls, block_override: None }]
        };
        let trace_calls = |calls: Vec<TransactionRequest>| {
            calls
                .into_iter()
                .map(|call| (call, std::iter::once(TraceType::Trace).collect()))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            EthCall::call_many(&api, bundles(transfers(2)), None, None).await.unwrap()[0].len(),
            2
        );
        assert_eq!(
            trace_api.trace_call_many(trace_calls(transfers(2)), None).await.unwrap().len(),
            2
        );
        assert_eq!(
            debug_api.debug_trace_call_many(bundles(transfers(2)), None, None).await.unwrap()[0]
                .len(),
            2
        );

        let gas_cap_err = "the request's calls exceed the RPC gas cap of 50000";
        for (calls, tx_index) in [(transfers(3), 2), (transfer_and_loop, 1)] {
            assert_eq!(
                EthCall::call_many(&api, bundles(calls.clone()), None, None)
                    .await
                    .unwrap_err()
                    .to_string(),
                format!("call_many error in bundle 0 and transaction {tx_index}: {gas_cap_err}")
            );
            assert_eq!(
                trace_api
                    .trace_call_many(trace_calls(calls.clone()), None)
                    .await
                    .unwrap_err()
                    .to_string(),
                gas_cap_err
            );
            assert_eq!(
                debug_api
                    .debug_trace_call_many(bundles(calls), None, None)
                    .await
                    .unwrap_err()
                    .to_string(),
                gas_cap_err
            );
        }

        let too_many = transfers(DEFAULT_MAX_CALL_MANY_CALLS + 1);
        let limit_err = "call count 1025 exceeds limit 1024";
        assert_eq!(
            EthCall::call_many(&api, bundles(too_many.clone()), None, None)
                .await
                .unwrap_err()
                .to_string(),
            limit_err
        );
        assert_eq!(
            trace_api
                .trace_call_many(trace_calls(too_many.clone()), None)
                .await
                .unwrap_err()
                .to_string(),
            limit_err
        );
        assert_eq!(
            debug_api
                .debug_trace_call_many(bundles(too_many), None, None)
                .await
                .unwrap_err()
                .to_string(),
            limit_err
        );
    }

    #[tokio::test]
    /// Requesting no block should result in a default response
    async fn test_fee_history_no_block_requested() {
        let block_count = 10;
        let newest_block = 1337;
        let oldest_block = None;

        let (eth_api, _, _) =
            prepare_eth_api(newest_block, oldest_block, block_count, MockEthProvider::default());

        let response = <EthApi<_, _> as EthApiServer<_, _, _, _, _, _>>::fee_history(
            &eth_api,
            U64::from(0),
            newest_block.into(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            response,
            FeeHistory::default(),
            "none: requesting no block should yield a default response"
        );
    }

    #[tokio::test]
    /// Requesting a single block should return 1 block (+ base fee for the next block over)
    async fn test_fee_history_single_block() {
        let block_count = 10;
        let newest_block = 1337;
        let oldest_block = None;

        let (eth_api, base_fees_per_gas, gas_used_ratios) =
            prepare_eth_api(newest_block, oldest_block, block_count, MockEthProvider::default());

        let fee_history =
            eth_api.fee_history(U64::from(1), newest_block.into(), None).await.unwrap();
        assert_eq!(
            fee_history.base_fee_per_gas,
            &base_fees_per_gas[base_fees_per_gas.len() - 2..],
            "one: base fee per gas is incorrect"
        );
        assert_eq!(
            fee_history.base_fee_per_gas.len(),
            2,
            "one: should return base fee of the next block as well"
        );
        assert_eq!(
            &fee_history.gas_used_ratio,
            &gas_used_ratios[gas_used_ratios.len() - 1..],
            "one: gas used ratio is incorrect"
        );
        assert_eq!(fee_history.oldest_block, newest_block, "one: oldest block is incorrect");
        assert!(
            fee_history.reward.is_none(),
            "one: no percentiles were requested, so there should be no rewards result"
        );
    }

    /// Requesting all blocks should be ok
    #[tokio::test]
    async fn test_fee_history_all_blocks() {
        let block_count = 10;
        let newest_block = 1337;
        let oldest_block = None;

        let (eth_api, base_fees_per_gas, gas_used_ratios) =
            prepare_eth_api(newest_block, oldest_block, block_count, MockEthProvider::default());

        let fee_history =
            eth_api.fee_history(U64::from(block_count), newest_block.into(), None).await.unwrap();

        assert_eq!(
            &fee_history.base_fee_per_gas, &base_fees_per_gas,
            "all: base fee per gas is incorrect"
        );
        assert_eq!(
            fee_history.base_fee_per_gas.len() as u64,
            block_count + 1,
            "all: should return base fee of the next block as well"
        );
        assert_eq!(
            &fee_history.gas_used_ratio, &gas_used_ratios,
            "all: gas used ratio is incorrect"
        );
        assert_eq!(
            fee_history.oldest_block,
            newest_block - block_count + 1,
            "all: oldest block is incorrect"
        );
        assert!(
            fee_history.reward.is_none(),
            "all: no percentiles were requested, so there should be no rewards result"
        );
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_task_observes_dropped_request() {
        let api = build_test_eth_api(MockEthProvider::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();

        let request = api.spawn_blocking_io(move |_| {
            let _ = started_tx.send(());
            let deadline = Instant::now() + Duration::from_secs(5);
            while !is_cancelled() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            let _ = cancelled_tx.send(is_cancelled());
            Ok(())
        });
        // drop the request while its blocking task is running
        tokio::select! {
            _ = request => panic!("blocking task completed before the request was dropped"),
            _ = started_rx => {}
        }

        assert!(cancelled_rx.await.unwrap());
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn async_blocking_task_observes_dropped_request() {
        let api = build_test_eth_api(MockEthProvider::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();

        let request = api.spawn_blocking_io_fut(move |_| async move {
            tokio::task::yield_now().await;
            let _ = started_tx.send(());
            let deadline = Instant::now() + Duration::from_secs(5);
            while !is_cancelled() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            let _ = cancelled_tx.send(is_cancelled());
            Ok(())
        });
        // drop the request while its blocking task is running
        tokio::select! {
            _ = request => panic!("blocking task completed before the request was dropped"),
            _ = started_rx => {}
        }

        assert!(cancelled_rx.await.unwrap());
    }
    #[tokio::test]
    async fn test_transaction_by_sender_and_nonce_without_sender_transaction_returns_none() {
        let provider = MockEthProvider::default();
        let sender = Address::random();
        provider.add_account(sender, ExtendedAccount::new(1, U256::ZERO));

        let block_hash = B256::random();
        provider.add_block(
            block_hash,
            Block {
                header: Header { number: 1, ..Default::default() },
                body: BlockBody::default(),
            },
        );

        let api = build_test_eth_api(provider);
        let transaction =
            EthTransactions::get_transaction_by_sender_and_nonce(&api, sender, 0, false)
                .await
                .expect("existing block without a sender transaction should not be a header error");

        assert!(transaction.is_none());
    }

    #[tokio::test]
    async fn test_transaction_by_sender_and_nonce_missing_block_returns_error() {
        use reth_rpc_eth_types::EthApiError;

        let provider = MockEthProvider::default();
        let sender = Address::random();
        provider.add_account(sender, ExtendedAccount::new(1, U256::ZERO));
        provider.add_header(B256::random(), Header { number: 1, ..Default::default() });

        let api = build_test_eth_api(provider);
        let error = EthTransactions::get_transaction_by_sender_and_nonce(&api, sender, 0, false)
            .await
            .unwrap_err();

        assert!(matches!(error, EthApiError::HeaderNotFound(id) if id == 1.into()));
    }

    #[tokio::test]
    async fn test_transaction_by_sender_and_nonce_returns_matching_transaction() {
        let provider = MockEthProvider::default();
        let tx = TransactionSigned::new_unhashed(
            reth_ethereum_primitives::Transaction::Legacy(Default::default()),
            Signature::test_signature(),
        );
        let sender = tx.recover_signer().unwrap();
        provider.add_account(sender, ExtendedAccount::new(1, U256::ZERO));
        let block = Block {
            header: Header { number: 1, ..Default::default() },
            body: BlockBody { transactions: vec![tx], ..Default::default() },
        };
        let block_hash = block.header.hash_slow();
        provider.add_block(block_hash, block);

        let api = build_test_eth_api(provider);
        let transaction =
            EthTransactions::get_transaction_by_sender_and_nonce(&api, sender, 0, false)
                .await
                .unwrap()
                .expect("matching transaction should be returned");

        assert_eq!(transaction.block_hash, Some(block_hash));
        assert_eq!(transaction.block_number, Some(1));
        assert_eq!(transaction.transaction_index, Some(0));
        assert_eq!(transaction.inner.signer(), sender);
    }
    fn build_test_eth_api_with_gas_cap(provider: MockEthProvider, gas_cap: u64) -> FakeEthApi {
        EthApiBuilder::new(
            provider.clone(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::new(provider.chain_spec()),
        )
        .gas_cap(gas_cap.into())
        .build()
    }

    #[tokio::test]
    async fn estimate_gas_respects_rpc_gas_cap() {
        const LOW_GAS_CAP: u64 = 50_000;
        const HIGH_GAS_CAP: u64 = 200_000;
        // Revert unless GAS > 60_000. Retrying above the RPC cap would succeed and
        // incorrectly turn the revert into an out-of-gas error.
        let code = Bytes::from_static(&[
            0x5a, 0x61, 0xea, 0x60, 0x10, 0x60, 0x0d, 0x57, 0x60, 0x00, 0x60, 0x00, 0xfd, 0x5b,
            0x00,
        ]);
        let sender = Address::repeat_byte(0x11);
        let contract = Address::repeat_byte(0xaa);
        let mut state_override = StateOverride::default();
        state_override.insert(contract, AccountOverride { code: Some(code), ..Default::default() });
        let overrides = EvmOverrides::state(Some(state_override));
        let at = BlockId::Number(BlockNumberOrTag::Latest);

        for chain_spec in [
            ChainSpecBuilder::mainnet().cancun_activated().build(),
            ChainSpecBuilder::mainnet().osaka_activated().build(),
            ChainSpecBuilder::mainnet().amsterdam_activated().build(),
        ] {
            let provider = MockEthProvider::default().with_chain_spec(chain_spec);
            provider.add_account(sender, ExtendedAccount::new(0, U256::MAX));
            provider.add_block(
                B256::repeat_byte(0x42),
                Block {
                    header: Header {
                        number: 1,
                        gas_limit: 30_000_000,
                        excess_blob_gas: Some(0),
                        ..Default::default()
                    },
                    body: BlockBody::default(),
                },
            );

            for gas in [None, Some(1_000_000)] {
                let request = TransactionRequest {
                    gas,
                    ..TransactionRequest::default().with_from(sender).with_to(contract)
                };
                let capped = build_test_eth_api_with_gas_cap(provider.clone(), LOW_GAS_CAP);
                let err = EthCall::estimate_gas_at(&capped, request.clone(), at, overrides.clone())
                    .await
                    .expect_err("estimation above the RPC gas cap must fail");
                assert!(
                    matches!(
                        err.as_invalid_transaction(),
                        Some(RpcInvalidTransactionError::Revert(_))
                    ),
                    "{err}"
                );

                for gas_cap in [HIGH_GAS_CAP, 0] {
                    let relaxed = build_test_eth_api_with_gas_cap(provider.clone(), gas_cap);
                    let estimated =
                        EthCall::estimate_gas_at(&relaxed, request.clone(), at, overrides.clone())
                            .await
                            .expect("same call fits under a higher or unlimited RPC gas cap");
                    assert!(estimated > U256::from(LOW_GAS_CAP), "{estimated}");
                    assert!(estimated <= U256::from(HIGH_GAS_CAP), "{estimated}");
                }
            }
        }
    }

    #[tokio::test]
    async fn estimate_gas_transfer_respects_rpc_gas_cap() {
        let provider = MockEthProvider::default();
        provider.add_block(
            B256::repeat_byte(0x42),
            Block {
                header: Header { number: 1, gas_limit: 30_000_000, ..Default::default() },
                body: BlockBody::default(),
            },
        );
        let request = TransactionRequest::default().with_to(Address::repeat_byte(0xaa));
        let at = BlockId::Number(BlockNumberOrTag::Latest);

        let capped = build_test_eth_api_with_gas_cap(provider.clone(), 20_999);
        let err = EthCall::estimate_gas_at(&capped, request.clone(), at, EvmOverrides::default())
            .await
            .expect_err("the transfer shortcut must respect the RPC gas cap");
        assert!(
            matches!(
                err.as_invalid_transaction(),
                Some(RpcInvalidTransactionError::GasRequiredExceedsAllowance { gas_limit: 20_999 })
            ),
            "{err}"
        );

        let sufficient = build_test_eth_api_with_gas_cap(provider, 21_000);
        let estimated = EthCall::estimate_gas_at(&sufficient, request, at, EvmOverrides::default())
            .await
            .unwrap();
        assert_eq!(estimated, U256::from(21_000));
    }
    #[tokio::test]
    async fn call_allowance_respects_rpc_gas_cap() {
        use reth_tasks::pool::BlockingTaskGuard;

        let sender = Address::repeat_byte(0x11);
        let contract = Address::repeat_byte(0xaa);
        // Return the gas remaining after intrinsic gas and the GAS opcode.
        let code = Bytes::from_static(&[0x5a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);

        // RPC cap, supplied gas, price, balance after value, block limit, expected budget.
        for (gas_cap, gas, price, balance, block_limit, budget) in [
            (50_000, None, 1, 100_000, 100_000, 50_000),
            (50_000, None, 0, 100_000, 100_000, 50_000),
            (50_000, None, 1, 40_000, 100_000, 40_000),
            (50_000, None, 1, 100_000, 40_000, 40_000),
            (50_000, Some(70_000), 1, 100_000, 100_000, 50_000),
            (50_000, Some(30_000), 1, 100_000, 100_000, 30_000),
            (50_000, Some(70_000), 1, 100_000, 40_000, 50_000),
            (0, None, 1, 100_000, 100_000, 100_000),
            (0, Some(70_000), 1, 100_000, 40_000, 70_000),
        ] {
            let provider = MockEthProvider::default()
                .with_chain_spec(ChainSpecBuilder::mainnet().cancun_activated().build());
            provider.add_block(
                B256::repeat_byte(0x42),
                Block {
                    header: Header {
                        number: 1,
                        gas_limit: block_limit,
                        excess_blob_gas: Some(0),
                        ..Default::default()
                    },
                    body: BlockBody::default(),
                },
            );
            let api = build_test_eth_api_with_gas_cap(provider, gas_cap);
            let trace_api =
                crate::TraceApi::new(api.clone(), BlockingTaskGuard::new(1), EthConfig::default());
            let state_override = StateOverride::from_iter([
                (
                    sender,
                    AccountOverride {
                        balance: Some(U256::from(balance + 1_000)),
                        ..Default::default()
                    },
                ),
                (contract, AccountOverride { code: Some(code.clone()), ..Default::default() }),
            ]);

            for dynamic_fee in [false, true] {
                let request = TransactionRequest {
                    gas,
                    gas_price: (!dynamic_fee).then_some(price),
                    max_fee_per_gas: dynamic_fee.then_some(price),
                    max_priority_fee_per_gas: dynamic_fee.then_some(price),
                    value: Some(U256::from(1_000)),
                    ..TransactionRequest::default().with_from(sender).with_to(contract)
                };
                let output = EthCall::call(
                    &api,
                    request.clone(),
                    Some(BlockId::latest()),
                    EvmOverrides::state(Some(state_override.clone())),
                )
                .await
                .unwrap();
                let traces = trace_api
                    .trace_call(TraceCallRequest {
                        call: request,
                        trace_types: std::iter::once(TraceType::Trace).collect(),
                        block_id: Some(BlockId::latest()),
                        state_overrides: Some(state_override.clone()),
                        block_overrides: None,
                    })
                    .await
                    .unwrap();

                assert_eq!(U256::from_be_slice(&output), U256::from(budget - 21_002));
                assert_eq!(traces.output, output);
            }
        }
    }

    #[tokio::test]
    async fn blob_call_allowance_reserves_maximum_fee() {
        use alloy_eips::eip4844::DATA_GAS_PER_BLOB;
        use reth_evm::ConfigureEvm;
        use reth_rpc_eth_api::helpers::Call;
        use revm::{database::CacheDB, state::AccountInfo};
        let sender = Address::repeat_byte(0x11);
        let provider = MockEthProvider::default()
            .with_chain_spec(ChainSpecBuilder::mainnet().cancun_activated().build());
        let api = build_test_eth_api(provider);
        let env = api.evm_config().evm_env(&Header::default()).unwrap();
        let allowance = |balance, value| {
            let mut db = CacheDB::<revm::database::EmptyDB>::default();
            db.insert_account_info(sender, AccountInfo { balance, ..Default::default() });
            let request = TransactionRequest {
                max_fee_per_gas: Some(10),
                max_priority_fee_per_gas: Some(10),
                max_fee_per_blob_gas: Some(3),
                blob_versioned_hashes: Some(vec![B256::repeat_byte(1)]),
                value: Some(value),
                ..TransactionRequest::default()
                    .with_from(sender)
                    .with_to(Address::repeat_byte(0xaa))
            };
            let tx = api.create_txn_env(&env, request, &mut db).unwrap();
            api.caller_gas_allowance(&mut db, &env, &tx)
        };
        let value = U256::from(1_000);
        let blob_fee = U256::from(DATA_GAS_PER_BLOB * 3);

        assert_eq!(
            allowance(U256::from(1_000_000), value).unwrap(),
            (1_000_000 - 1_000 - DATA_GAS_PER_BLOB * 3) / 10
        );
        for (balance, value) in [(value + blob_fee - U256::ONE, value), (U256::MAX, U256::MAX)] {
            let err = allowance(balance, value).unwrap_err();
            assert!(
                matches!(
                    err.as_invalid_transaction(),
                    Some(RpcInvalidTransactionError::InsufficientFunds { cost, balance: actual })
                        if *cost == value.saturating_add(blob_fee) && *actual == balance
                ),
                "{err}"
            );
        }
    }
    #[tokio::test]
    async fn bal_replay_unknown_and_pending_match_eth_and_debug_semantics() {
        use reth_rpc_api::DebugApiServer;
        use reth_rpc_eth_api::helpers::bal::GetBlockAccessList;
        use reth_rpc_eth_types::EthApiError;
        use reth_tasks::{pool::BlockingTaskGuard, Runtime};

        let api = build_test_eth_api(MockEthProvider::default());
        let runtime = Runtime::test();
        let debug = crate::DebugApi::new(
            api.clone(),
            BlockingTaskGuard::new(1),
            &runtime,
            futures::stream::empty(),
        );
        for block_id in [BlockId::hash(B256::repeat_byte(0x91)), BlockId::pending()] {
            assert_eq!(api.get_block_access_list(block_id).await.unwrap(), None);
            let expected: jsonrpsee::types::ErrorObjectOwned =
                EthApiError::HeaderNotFound(block_id).into();
            assert_eq!(debug.raw_block_access_list(block_id).await.unwrap_err(), expected);
        }
        assert_eq!(api.get_raw_block_access_list(BlockId::pending()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn bal_replay_raw_and_decoded_include_executed_storage_write() {
        use alloy_consensus::{transaction::SignerRecoverable, TxEip1559};
        use alloy_primitives::TxKind;
        use reth_ethereum_primitives::Transaction;
        use reth_rpc_api::DebugApiServer;
        use reth_rpc_eth_api::helpers::bal::GetBlockAccessList;
        use reth_tasks::{pool::BlockingTaskGuard, Runtime};
        use reth_testing_utils::generators::{generate_key, sign_tx_with_key_pair};

        let contract = Address::repeat_byte(0x42);
        let mut rng = generators::rng();
        let tx = sign_tx_with_key_pair(
            generate_key(&mut rng),
            Transaction::Eip1559(TxEip1559 {
                chain_id: 1,
                nonce: 0,
                gas_limit: 100_000,
                max_fee_per_gas: 1,
                to: TxKind::Call(contract),
                ..Default::default()
            }),
        );
        let sender = tx.recover_signer().unwrap();
        let provider = MockEthProvider::default()
            .with_chain_spec(ChainSpecBuilder::mainnet().cancun_activated().build());
        provider.add_account(sender, ExtendedAccount::new(0, U256::from(1_000_000)));
        // PUSH1 0x2a, PUSH1 0x07, SSTORE, STOP: slot 7 changes from zero to 42.
        provider.add_account(
            contract,
            ExtendedAccount::new(1, U256::ZERO)
                .with_bytecode(Bytes::from_static(&[0x60, 0x2a, 0x60, 0x07, 0x55, 0x00])),
        );
        let parent = Header {
            number: 0,
            gas_limit: 1_000_000,
            base_fee_per_gas: Some(0),
            ..Default::default()
        };
        let parent_hash = parent.hash_slow();
        provider.add_block(parent_hash, Block { header: parent, body: BlockBody::default() });
        let header = Header {
            parent_hash,
            number: 1,
            timestamp: 12,
            gas_limit: 1_000_000,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        };
        let hash = header.hash_slow();
        provider.add_block(
            hash,
            Block {
                header,
                body: BlockBody {
                    transactions: vec![tx],
                    withdrawals: Some(Default::default()),
                    ..Default::default()
                },
            },
        );
        // This fixture supplies parent state and block data, with no stored or precomputed BAL.
        let api = build_test_eth_api(provider);
        let runtime = Runtime::test();
        let debug = crate::DebugApi::new(
            api.clone(),
            BlockingTaskGuard::new(1),
            &runtime,
            futures::stream::empty(),
        );
        let at = BlockId::hash(hash);
        let raw = debug.raw_block_access_list(at).await.unwrap();
        let bal = api.get_block_access_list(at).await.unwrap().expect("known block is replayed");
        assert_eq!(raw.as_ref(), alloy_rlp::encode(&bal));
        let changed = bal
            .iter()
            .find(|account| account.address == contract)
            .expect("the contract touched by SSTORE is recorded");
        let slot = changed
            .storage_changes
            .iter()
            .find(|change| change.slot == U256::from(7))
            .expect("executing the transaction records slot 7");
        assert_eq!(slot.changes.len(), 1);
        assert_eq!(slot.changes[0].block_access_index.get(), 1);
        assert_eq!(slot.changes[0].new_value, U256::from(42));
        let sender_changes = bal
            .iter()
            .find(|account| account.address == sender)
            .expect("the transaction sender is recorded");
        assert_eq!(sender_changes.nonce_changes.last().unwrap().new_nonce, 1);
    }
}
