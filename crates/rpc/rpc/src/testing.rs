//! Implementation of the `testing` namespace.
//!
//! This exposes `testing_buildBlockV1`, intended for non-production/debug use.
//!
//! # Enabling the testing namespace
//!
//! The `testing_` namespace is disabled by default for security reasons.
//! To enable it, add `testing` to the `--http.api` flag when starting the node:
//!
//! ```sh
//! reth node --http --http.api eth,testing
//! ```
//!
//! **Warning:** This namespace allows building arbitrary blocks. Never expose it
//! on public-facing RPC endpoints without proper authentication.

use alloy_consensus::Transaction;
use alloy_eips::{eip1559::calculate_block_gas_limit, eip2718::Decodable2718};
use alloy_evm::{Evm, RecoveredTx};
use alloy_primitives::{map::HashSet, Address, Bytes, B256, U256};
use alloy_rlp::Encodable;
use alloy_rpc_types_engine::{
    BlobsBundleV2, ExecutionData, ExecutionPayloadEnvelopeV5, ExecutionPayloadEnvelopeV6,
    ExecutionPayloadSidecar, ExecutionPayloadV3, ExecutionPayloadV4, ForkchoiceState,
    PayloadAttributes, PraguePayloadFields,
};
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_consensus_common::validation::MAX_RLP_BLOCK_SIZE;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_errors::RethError;
use reth_ethereum_engine_primitives::{EthBuiltPayload, EthPayloadTypes};
use reth_evm::{execute::BlockBuilder, ConfigureEvmForRead, NextBlockEnvAttributes};
use reth_payload_primitives::{BuiltPayload, PayloadTypes};
use reth_primitives_traits::{
    transaction::{recover::try_recover_signers, signed::RecoveryError},
    AlloyBlockHeader as BlockTrait, Block as _, HeaderTy, ReceiptTy, TxTy,
};
use reth_revm::{
    database::StateProviderDatabase,
    db::{bal::BalState, State},
};
use reth_rpc_api::{TestingApiServer, TestingBuildBlockRequestV1, TestingBuildBlockResponseV1};
use reth_rpc_eth_api::{helpers::Call, FromEthApiError};
use reth_rpc_eth_types::EthApiError;
use reth_storage_api::{BlockReader, BlockReaderIdExt, HeaderProvider};
use reth_transaction_pool::{BestTransactionsAttributes, PoolTransaction, TransactionPool};
use revm::context::Block;
use revm_primitives::map::DefaultHashBuilder;
use std::sync::Arc;
use tracing::debug;

/// Testing API handler.
#[derive(Debug, Clone)]
pub struct TestingApi<Eth, Evm, Payload: PayloadTypes = EthPayloadTypes> {
    // The real node leaves this unset: legacy validation does not return Nested V2 updates.
    // Retain the typed legacy implementation without allowing it to persist incomplete trie data.
    engine_handle: Option<ConsensusEngineHandle<Payload>>,
    eth_api: Eth,
    evm_config: Evm,
    /// Desired gas limit to move toward while respecting the consensus gas limit bounds.
    desired_gas_limit: u64,
    /// If true, skip invalid transactions instead of failing.
    skip_invalid_transactions: bool,
    /// If set, override the block gas limit in `testing_buildBlockV1`.
    gas_limit_override: Option<u64>,
}

impl<Eth, Evm> TestingApi<Eth, Evm> {
    /// Create a new testing API handler.
    pub const fn new(eth_api: Eth, evm_config: Evm, desired_gas_limit: u64) -> Self {
        Self {
            engine_handle: None,
            eth_api,
            evm_config,
            desired_gas_limit,
            skip_invalid_transactions: false,
            gas_limit_override: None,
        }
    }
}

impl<Eth, Evm, Payload: PayloadTypes> TestingApi<Eth, Evm, Payload> {
    /// Enable skipping invalid transactions instead of failing.
    /// When a transaction fails, all subsequent transactions from the same sender are also
    /// skipped.
    pub const fn with_skip_invalid_transactions(mut self) -> Self {
        self.skip_invalid_transactions = true;
        self
    }

    /// Override the gas limit used by `testing_buildBlockV1`.
    pub const fn with_gas_limit_override(mut self, gas_limit: u64) -> Self {
        self.gas_limit_override = Some(gas_limit);
        self
    }
}

impl<Eth, Evm, Payload> TestingApi<Eth, Evm, Payload>
where
    Payload: PayloadTypes<
        ExecutionData = ExecutionData,
        BuiltPayload: BuiltPayload<Primitives = Evm::Primitives>,
    >,
    Eth: Call<
        Provider: BlockReader<
            Header = HeaderTy<Evm::Primitives>,
            Receipt = ReceiptTy<Evm::Primitives>,
        > + ChainSpecProvider<ChainSpec: EthereumHardforks>,
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Evm::Primitives>>>,
    >,
    Evm: ConfigureEvmForRead<NextBlockEnvCtx = NextBlockEnvAttributes> + 'static,
{
    async fn build_payload_v1(
        &self,
        request: TestingBuildBlockRequestV1,
        skip_invalid_transactions: bool,
        use_pool_transactions: bool,
    ) -> Result<EthBuiltPayload<Evm::Primitives>, Eth::Error> {
        let eth_api = self.eth_api.consistent_view().await?;
        let evm_config = self.evm_config.with_read_provider(eth_api.provider().clone());
        let desired_gas_limit = self.desired_gas_limit;
        let gas_limit_override = self.gas_limit_override;
        eth_api
            .spawn_with_state_at_block(request.parent_block_hash, move |eth_api, state| {
                let state = state.database.0;
                let mut db = State::builder()
                    .with_bundle_update()
                    .with_database(StateProviderDatabase::new(&state))
                    .build();
                let parent = eth_api
                    .provider()
                    .sealed_header_by_hash(request.parent_block_hash)?
                    .ok_or_else(|| {
                    EthApiError::HeaderNotFound(request.parent_block_hash.into())
                })?;

                let chain_spec = eth_api.provider().chain_spec();
                let is_osaka =
                    chain_spec.is_osaka_active_at_timestamp(request.payload_attributes.timestamp);

                if chain_spec.is_amsterdam_active_at_timestamp(request.payload_attributes.timestamp)
                {
                    db.bal_state = BalState::new().with_bal_builder();
                }
                let withdrawals = request.payload_attributes.withdrawals.clone();
                let withdrawals_rlp_length = withdrawals.as_ref().map(|w| w.length()).unwrap_or(0);

                let env_attrs = NextBlockEnvAttributes {
                    timestamp: request.payload_attributes.timestamp,
                    suggested_fee_recipient: request.payload_attributes.suggested_fee_recipient,
                    prev_randao: request.payload_attributes.prev_randao,
                    gas_limit: gas_limit_override.unwrap_or_else(|| {
                        calculate_block_gas_limit(
                            parent.gas_limit(),
                            request
                                .payload_attributes
                                .target_gas_limit
                                .unwrap_or(desired_gas_limit),
                        )
                    }),
                    parent_beacon_block_root: request.payload_attributes.parent_beacon_block_root,
                    withdrawals: withdrawals.map(Into::into),
                    extra_data: request.extra_data.unwrap_or_default(),
                    slot_number: request.payload_attributes.slot_number,
                };

                let mut builder = evm_config
                    .builder_for_next_block(&mut db, &parent, env_attrs)
                    .map_err(RethError::other)
                    .map_err(Eth::Error::from_eth_err)?;
                builder.apply_pre_execution_changes().map_err(Eth::Error::from_eth_err)?;

                let mut total_fees = U256::ZERO;
                let base_fee = builder.evm_mut().block().basefee();

                let mut invalid_senders: HashSet<Address, DefaultHashBuilder> = HashSet::default();
                let mut block_transactions_rlp_length = 0usize;

                // An omitted transaction argument uses the pool; an explicit empty list stays
                // empty.
                let recovered_txs = if use_pool_transactions {
                    let mut best_txs = eth_api.pool().best_transactions_with_attributes(
                        BestTransactionsAttributes::new(
                            base_fee,
                            builder
                                .evm_mut()
                                .block()
                                .blob_gasprice()
                                .map(|gasprice| gasprice as u64),
                        ),
                    );
                    best_txs.no_updates();
                    // Blob sidecars aren't attached to the response, so including blob
                    // transactions would return a payload whose blobs bundle lacks their data.
                    best_txs.skip_blobs();
                    best_txs.map(|tx| tx.to_consensus()).collect()
                } else {
                    // Decode and recover all transactions in parallel
                    try_recover_signers(&request.transactions, |tx| {
                        TxTy::<Evm::Primitives>::decode_2718_exact(tx.as_ref())
                            .map_err(RecoveryError::from_source)
                    })
                    .or(Err(EthApiError::InvalidTransactionSignature))?
                };
                let allow_skip_invalid_transactions =
                    skip_invalid_transactions || use_pool_transactions;

                for (idx, tx) in recovered_txs.into_iter().enumerate() {
                    let signer = tx.signer();
                    if allow_skip_invalid_transactions && invalid_senders.contains(&signer) {
                        continue;
                    }

                    // EIP-7934: Check estimated block size before adding transaction
                    let tx_rlp_len = tx.tx().length();
                    if is_osaka {
                        // 1KB overhead for block header
                        let estimated_block_size = block_transactions_rlp_length +
                            tx_rlp_len +
                            withdrawals_rlp_length +
                            1024;
                        if estimated_block_size > MAX_RLP_BLOCK_SIZE {
                            if allow_skip_invalid_transactions {
                                debug!(
                                    target: "rpc::testing",
                                    tx_idx = idx,
                                    ?signer,
                                    estimated_block_size,
                                    max_size = MAX_RLP_BLOCK_SIZE,
                                    "Skipping transaction: would exceed block size limit"
                                );
                                invalid_senders.insert(signer);
                                continue;
                            }
                            return Err(Eth::Error::from_eth_err(EthApiError::InvalidParams(
                                format!(
                                    "transaction at index {} would exceed max block size: {} > {}",
                                    idx, estimated_block_size, MAX_RLP_BLOCK_SIZE
                                ),
                            )));
                        }
                    }

                    let tip = tx.effective_tip_per_gas(base_fee).unwrap_or_default();
                    let gas_used = match builder.execute_transaction(tx) {
                        Ok(gas_used) => gas_used.tx_gas_used(),
                        Err(err) => {
                            if allow_skip_invalid_transactions {
                                debug!(
                                    target: "rpc::testing",
                                    tx_idx = idx,
                                    ?signer,
                                    error = ?err,
                                    "Skipping invalid transaction"
                                );
                                invalid_senders.insert(signer);
                                continue;
                            }
                            debug!(
                                target: "rpc::testing",
                                tx_idx = idx,
                                ?signer,
                                error = ?err,
                                "Transaction execution failed"
                            );
                            return Err(Eth::Error::from_eth_err(err));
                        }
                    };

                    block_transactions_rlp_length += tx_rlp_len;
                    total_fees += U256::from(tip) * U256::from(gas_used);
                }
                let outcome = builder.finish(&state, None).map_err(Eth::Error::from_eth_err)?;

                let has_requests = outcome.block.requests_hash().is_some();
                let requests = has_requests.then_some(outcome.execution_result.requests);

                let block_access_list =
                    outcome.block_access_list.map(|bal| alloy_rlp::encode(&bal).into());
                Ok(EthBuiltPayload::new(
                    Arc::new(outcome.block),
                    total_fees,
                    requests,
                    block_access_list,
                ))
            })
            .await
    }
    /// Builds a payload response using a concrete request, preserving the existing local helper.
    pub async fn build_block_v1(
        &self,
        request: TestingBuildBlockRequestV1,
    ) -> Result<TestingBuildBlockResponseV1, Eth::Error> {
        let use_pool_transactions = request.transactions.is_empty();
        self.build_response_v1(request, use_pool_transactions).await
    }

    async fn build_response_v1(
        &self,
        request: TestingBuildBlockRequestV1,
        use_pool_transactions: bool,
    ) -> Result<TestingBuildBlockResponseV1, Eth::Error> {
        let payload = self
            .build_payload_v1(request, self.skip_invalid_transactions, use_pool_transactions)
            .await?;
        let fees = payload.fees();
        let requests = payload.requests().unwrap_or_default();
        let bal = payload.block_access_list().cloned();
        let block = Arc::unwrap_or_clone(payload.into_block_arc());
        let hash = block.hash();
        let block = block.into_block().into_ethereum_block();
        Ok(match bal {
            Some(bal) => TestingBuildBlockResponseV1::V6(ExecutionPayloadEnvelopeV6 {
                execution_payload: ExecutionPayloadV4::from_block_unchecked_with_bal(
                    hash, &block, bal,
                ),
                block_value: fees,
                blobs_bundle: BlobsBundleV2::empty(),
                should_override_builder: false,
                execution_requests: requests,
            }),
            None => TestingBuildBlockResponseV1::V5(ExecutionPayloadEnvelopeV5 {
                execution_payload: ExecutionPayloadV3::from_block_unchecked(hash, &block),
                block_value: fees,
                blobs_bundle: BlobsBundleV2::empty(),
                should_override_builder: false,
                execution_requests: requests,
            }),
        })
    }

    async fn commit_block_v1(
        &self,
        payload_attributes: PayloadAttributes,
        transactions: Option<Vec<Bytes>>,
        extra_data: Option<Bytes>,
    ) -> Result<B256, Eth::Error> {
        let engine_handle = self.engine_handle.as_ref().ok_or_else(|| EthApiError::Unsupported("testing_commitBlockV1 requires verified nested state roots and Nested V2 persistence; unsupported by the current Gravity engine"))?;
        let parent = self
            .eth_api
            .provider()
            .latest_header()
            .map_err(EthApiError::from)?
            .ok_or_else(|| EthApiError::HeaderNotFound(alloy_eips::BlockId::latest()))?;
        let safe_block_hash = self
            .eth_api
            .provider()
            .safe_header()
            .map_err(EthApiError::from)?
            .map(|header| header.hash())
            .unwrap_or_else(|| parent.hash());
        let finalized_block_hash = self
            .eth_api
            .provider()
            .finalized_header()
            .map_err(EthApiError::from)?
            .map(|header| header.hash())
            .unwrap_or_else(|| parent.hash());

        let use_pool_transactions = transactions.is_none();
        let payload = self
            .build_payload_v1(
                TestingBuildBlockRequestV1 {
                    parent_block_hash: parent.hash(),
                    payload_attributes,
                    transactions: transactions.unwrap_or_default(),
                    extra_data,
                },
                false,
                use_pool_transactions,
            )
            .await?;

        let block_hash = payload.block().hash();
        let requests = payload.requests();
        let bal = payload.block_access_list().cloned();
        let block = Arc::unwrap_or_clone(payload.into_block_arc()).into_sealed_block();
        let execution_data = Payload::block_to_payload(block, bal);
        let execution_data = match (requests, execution_data.sidecar.cancun()) {
            (Some(requests), Some(cancun)) => ExecutionData::new(
                execution_data.payload,
                ExecutionPayloadSidecar::v4(cancun.clone(), PraguePayloadFields::new(requests)),
            ),
            _ => execution_data,
        };
        let status = engine_handle
            .new_payload(execution_data)
            .await
            .map_err(RethError::other)
            .map_err(Eth::Error::from_eth_err)?;
        if !status.is_valid() {
            return Err(Eth::Error::from_eth_err(EthApiError::InvalidParams(format!(
                "new payload returned non-valid status: {:?}",
                status.status
            ))));
        }

        let fcu = engine_handle
            .fork_choice_updated(
                ForkchoiceState {
                    head_block_hash: block_hash,
                    safe_block_hash,
                    finalized_block_hash,
                },
                None,
            )
            .await
            .map_err(RethError::other)
            .map_err(Eth::Error::from_eth_err)?;
        if !fcu.is_valid() {
            return Err(Eth::Error::from_eth_err(EthApiError::InvalidParams(format!(
                "forkchoice update returned non-valid status: {:?}",
                fcu.payload_status.status
            ))));
        }

        Ok(block_hash)
    }
}

#[async_trait]
impl<Eth, Evm, Payload> TestingApiServer for TestingApi<Eth, Evm, Payload>
where
    Payload: PayloadTypes<
        ExecutionData = ExecutionData,
        BuiltPayload: BuiltPayload<Primitives = Evm::Primitives>,
    >,
    Eth: Call<
        Provider: BlockReader<
            Header = HeaderTy<Evm::Primitives>,
            Receipt = ReceiptTy<Evm::Primitives>,
        > + ChainSpecProvider<ChainSpec: EthereumHardforks>,
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Evm::Primitives>>>,
    >,
    Evm: ConfigureEvmForRead<NextBlockEnvCtx = NextBlockEnvAttributes> + 'static,
{
    /// Handles `testing_buildBlockV1` by gating concurrency via a semaphore and offloading heavy
    /// work to the blocking pool to avoid stalling the async runtime.
    async fn build_block_v1(
        &self,
        parent_block_hash: B256,
        payload_attributes: PayloadAttributes,
        transactions: Option<Vec<Bytes>>,
        extra_data: Option<Bytes>,
    ) -> RpcResult<TestingBuildBlockResponseV1> {
        let use_pool_transactions = transactions.is_none();
        let request = TestingBuildBlockRequestV1 {
            parent_block_hash,
            payload_attributes,
            transactions: transactions.unwrap_or_default(),
            extra_data,
        };
        self.build_response_v1(request, use_pool_transactions).await.map_err(Into::into)
    }

    async fn commit_block_v1(
        &self,
        payload_attributes: PayloadAttributes,
        transactions: Option<Vec<Bytes>>,
        extra_data: Option<Bytes>,
    ) -> RpcResult<B256> {
        self.commit_block_v1(payload_attributes, transactions, extra_data).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{eth::helpers::types::EthRpcConverter, EthApi, EthApiBuilder};
    use alloy_consensus::Header;
    use reth_chainspec::{ChainSpec, ChainSpecBuilder};
    use reth_ethereum_primitives::{Block, BlockBody};
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_rpc_eth_api::node::RpcNodeCoreAdapter;
    use reth_transaction_pool::{
        test_utils::{testing_pool, MockTransaction, TestPool},
        TransactionOrigin,
    };
    use serde_json::{json, Value};

    type Handler = TestingApi<
        EthApi<
            RpcNodeCoreAdapter<MockEthProvider, TestPool, NoopNetwork, EthEvmConfig>,
            EthRpcConverter<ChainSpec>,
        >,
        EthEvmConfig,
    >;

    fn fixture() -> (MockEthProvider, B256, PayloadAttributes) {
        let provider = MockEthProvider::default().with_chain_spec(
            ChainSpecBuilder::mainnet().osaka_activated().with_amsterdam_at(24).build(),
        );
        let header = Header {
            gas_limit: 1_000_000,
            base_fee_per_gas: Some(0),
            timestamp: 0,
            ..Default::default()
        };
        let hash = header.hash_slow();
        provider.add_block(hash, Block { header, body: BlockBody::default() });
        let attributes = PayloadAttributes {
            timestamp: 12,
            withdrawals: Some(vec![]),
            parent_beacon_block_root: Some(B256::ZERO),
            target_gas_limit: Some(999_900),
            ..Default::default()
        };
        (provider, hash, attributes)
    }

    fn handler(provider: MockEthProvider, pool: TestPool) -> Handler {
        let evm = EthEvmConfig::new(provider.chain_spec());
        let api = EthApiBuilder::new(provider, pool, NoopNetwork::default(), evm.clone()).build();
        TestingApi::new(api, evm, 1_000_000)
    }

    async fn request(module: &jsonrpsee::RpcModule<Handler>, method: &str, params: Value) -> Value {
        let (response, _) = module
            .raw_json_request(
                &json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}).to_string(),
                1,
            )
            .await
            .unwrap();
        serde_json::from_str(response.get()).unwrap()
    }

    #[tokio::test]
    async fn positional_testing_build_distinguishes_empty_and_omitted_transactions() {
        let (provider, parent, attributes) = fixture();
        let pool = testing_pool();
        let tx = MockTransaction::eip1559().with_gas_limit(21_000);
        provider.add_account(tx.sender(), ExtendedAccount::new(0, U256::from(1_000_000)));
        pool.add_transaction(TransactionOrigin::Local, tx).await.unwrap();
        let module = handler(provider, pool).into_rpc();
        let empty = request(&module, "testing_buildBlockV1", json!([parent, attributes, []])).await;
        assert!(empty.get("error").is_none(), "{empty}");
        assert_eq!(
            empty["result"]["executionPayload"]["transactions"].as_array().unwrap().len(),
            0
        );
        assert_eq!(empty["result"]["executionPayload"]["gasLimit"], format!("{:#x}", 999_900));
        let pooled = request(&module, "testing_buildBlockV1", json!([parent, attributes])).await;
        assert!(pooled.get("error").is_none(), "{pooled}");
        assert_eq!(
            pooled["result"]["executionPayload"]["transactions"].as_array().unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn testing_build_returns_amsterdam_fields_and_honors_explicit_gas_override() {
        let (provider, parent, mut attributes) = fixture();
        attributes.timestamp = 24;
        attributes.slot_number = Some(2);
        let module = handler(provider, testing_pool()).with_gas_limit_override(900_000).into_rpc();
        let response =
            request(&module, "testing_buildBlockV1", json!([parent, attributes, []])).await;
        assert!(response.get("error").is_none(), "{response}");
        let payload = &response["result"]["executionPayload"];
        assert_eq!(payload["gasLimit"], format!("{:#x}", 900_000));
        assert_eq!(payload["slotNumber"], "0x2");
        assert!(payload.get("blockAccessList").is_some());
        assert!(matches!(
            serde_json::from_value::<TestingBuildBlockResponseV1>(response["result"].clone())
                .unwrap(),
            TestingBuildBlockResponseV1::V6(_)
        ));
    }

    #[tokio::test]
    async fn testing_commit_without_verified_nested_legacy_engine_is_unsupported() {
        let (provider, _, attributes) = fixture();
        let module = handler(provider, testing_pool()).into_rpc();
        let response = request(&module, "testing_commitBlockV1", json!([attributes, []])).await;
        assert!(
            response["error"]["message"].as_str().unwrap().contains("nested state roots"),
            "{response}"
        );
    }
}
