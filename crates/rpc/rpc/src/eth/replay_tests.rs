//! RPC replay must preserve changes made by the chain's block executor after EVM execution.

use crate::{DebugApi, EthApiBuilder};
use alloy_consensus::{transaction::SignerRecoverable, BlockHeader, Header, TxLegacy};
use alloy_evm::{
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        ExecutableTx, GasOutput, StateDB,
    },
    eth::EthTxResult,
    EvmFactory,
};
use alloy_primitives::{Address, Bytes, TxKind, B256, U256};
use reth_chainspec::ChainSpecBuilder;
use reth_ethereum_primitives::{Block, BlockBody, EthPrimitives, Transaction};
use reth_evm::{
    ConfigureEvm, ConfigureEvmForRead, EvmEnvFor, ExecutionCtxFor, NextBlockEnvAttributes,
};
use reth_evm_ethereum::{EthBlockAssembler, EthEvmConfig};
use reth_network_api::noop::NoopNetwork;
use reth_primitives_traits::{Block as _, SealedBlock, SealedHeader};
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_rpc_eth_api::helpers::{Call, Trace};
use reth_rpc_eth_types::StateCacheDb;
use reth_tasks::{pool::BlockingTaskGuard, Runtime};
use reth_testing_utils::generators::{self, generate_key, sign_tx_with_key_pair};
use reth_transaction_pool::test_utils::testing_pool;
use revm::{state::EvmState, Database, Inspector};
use std::convert::Infallible;

const CREDIT: Address = Address::repeat_byte(0x79);
type BaseFactory = <EthEvmConfig as ConfigureEvm>::BlockExecutorFactory;

trait SettlementOutput {
    fn changes_mut(&mut self) -> &mut EvmState;
}
impl<H, T> SettlementOutput for EthTxResult<H, T> {
    fn changes_mut(&mut self) -> &mut EvmState {
        &mut self.result.state
    }
}

struct SettlementExecutor<E>(E);
impl<E> BlockExecutor for SettlementExecutor<E>
where
    E: BlockExecutor,
    E::Result: SettlementOutput,
{
    type Transaction = E::Transaction;
    type Receipt = E::Receipt;
    type Evm = E::Evm;
    type Result = E::Result;
    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        self.0.apply_pre_execution_changes()
    }
    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        // ExecutableTx only depends on the same Transaction and Evm associated types.
        let mut output = self.0.execute_transaction_without_commit(tx)?;
        let account = output.changes_mut().entry(CREDIT).or_default();
        account.info.balance = U256::from(7);
        account.mark_touch();
        Ok(output)
    }
    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        self.0.commit_transaction(output)
    }
    fn finish(
        self,
    ) -> Result<(Self::Evm, BlockExecutionResult<Self::Receipt>), BlockExecutionError> {
        self.0.finish()
    }
    fn evm_mut(&mut self) -> &mut Self::Evm {
        self.0.evm_mut()
    }
    fn evm(&self) -> &Self::Evm {
        self.0.evm()
    }
    fn receipts(&self) -> &[Self::Receipt] {
        self.0.receipts()
    }
}

#[derive(Clone, Debug)]
struct SettlementFactory(BaseFactory);
impl BlockExecutorFactory for SettlementFactory {
    type EvmFactory = <BaseFactory as BlockExecutorFactory>::EvmFactory;
    type TxExecutionResult = <BaseFactory as BlockExecutorFactory>::TxExecutionResult;
    type ExecutionCtx<'a> = <BaseFactory as BlockExecutorFactory>::ExecutionCtx<'a>;
    type Transaction = <BaseFactory as BlockExecutorFactory>::Transaction;
    type Receipt = <BaseFactory as BlockExecutorFactory>::Receipt;
    type Executor<'a, DB: StateDB, I: Inspector<<Self::EvmFactory as EvmFactory>::Context<DB>>> =
        SettlementExecutor<<BaseFactory as BlockExecutorFactory>::Executor<'a, DB, I>>;
    fn evm_factory(&self) -> &Self::EvmFactory {
        self.0.evm_factory()
    }
    fn create_executor<'a, DB, I>(
        &'a self,
        evm: <Self::EvmFactory as EvmFactory>::Evm<DB, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a, DB, I>
    where
        DB: StateDB,
        I: Inspector<<Self::EvmFactory as EvmFactory>::Context<DB>>,
    {
        SettlementExecutor(self.0.create_executor(evm, ctx))
    }
}

#[derive(Clone, Debug)]
struct SettlementConfig {
    inner: EthEvmConfig,
    factory: SettlementFactory,
}
impl ConfigureEvmForRead for SettlementConfig {}
impl ConfigureEvm for SettlementConfig {
    type Primitives = EthPrimitives;
    type Error = Infallible;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = SettlementFactory;
    type BlockAssembler = EthBlockAssembler;
    fn block_executor_factory(&self) -> &SettlementFactory {
        &self.factory
    }
    fn block_assembler(&self) -> &EthBlockAssembler {
        self.inner.block_assembler()
    }
    fn evm_env(&self, header: &Header) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env(header)
    }
    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.next_evm_env(parent, attributes)
    }
    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_block(block)
    }
    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<ExecutionCtxFor<'_, Self>, Self::Error> {
        self.inner.context_for_next_block(parent, attributes)
    }
    fn parallel_executor<'a, DB: reth_evm::ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<
        dyn reth_evm::parallel_execute::ParallelExecutor<
                Primitives = Self::Primitives,
                Error = BlockExecutionError,
            > + 'a,
    > {
        self.inner.parallel_executor(db)
    }
}

fn fixture() -> (MockEthProvider, reth_primitives_traits::RecoveredBlock<Block>, Address) {
    let mut rng = generators::rng();
    let key = generate_key(&mut rng);
    let transactions = (0..2)
        .map(|nonce| {
            sign_tx_with_key_pair(
                key,
                Transaction::Legacy(TxLegacy {
                    chain_id: Some(1),
                    nonce,
                    gas_price: 1,
                    gas_limit: 21_000,
                    to: TxKind::Call(Address::repeat_byte(0x42)),
                    value: U256::ONE,
                    input: Bytes::new(),
                }),
            )
        })
        .collect::<Vec<_>>();
    let sender = transactions[0].recover_signer().unwrap();
    let provider = MockEthProvider::default()
        .with_chain_spec(ChainSpecBuilder::mainnet().cancun_activated().build());
    provider.add_account(sender, ExtendedAccount::new(0, U256::from(1_000_000)));
    let parent = Header { gas_limit: 1_000_000, base_fee_per_gas: Some(0), ..Default::default() };
    let parent_hash = parent.hash_slow();
    provider.add_block(parent_hash, Block { header: parent, body: BlockBody::default() });
    let block = Block {
        header: Header {
            parent_hash,
            number: 1,
            timestamp: 12,
            gas_limit: 1_000_000,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        },
        body: BlockBody {
            transactions,
            withdrawals: Some(Default::default()),
            ..Default::default()
        },
    }
    .try_into_recovered()
    .unwrap();
    provider.add_block(block.hash(), block.clone().into_block());
    (provider, block, sender)
}

#[tokio::test]
async fn executor_replay_preserves_settlement_and_target_pre_state() {
    use reth_chainspec::ChainSpecProvider;
    use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
    let (provider, block, sender) = fixture();
    let inner = EthEvmConfig::new(provider.chain_spec());
    let config = SettlementConfig {
        factory: SettlementFactory(inner.block_executor_factory().clone()),
        inner,
    };
    let api = EthApiBuilder::new(provider, testing_pool(), NoopNetwork::default(), config).build();
    api.spawn_with_state_at_block(block.parent_hash(), move |api, mut db: StateCacheDb| {
        let (_, result) = api.inspect_transaction_in_block(
            &block,
            &mut db,
            TracingInspector::new(TracingInspectorConfig::default()),
            1,
        )?;
        assert_eq!(
            result.state[&CREDIT].info.balance,
            U256::from(7),
            "target executor settlement must reach the tracer"
        );
        assert_eq!(result.state[&sender].info.nonce, 2);
        assert_eq!(db.basic(sender).unwrap().unwrap().nonce, 1, "the target remains uncommitted");
        assert_eq!(
            db.basic(CREDIT).unwrap().unwrap().balance,
            U256::from(7),
            "predecessor settlement must be committed"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn execution_witness_accepts_all_block_identifiers() {
    use reth_rpc_api::DebugApiServer;
    let (provider, block, _) = fixture();
    use reth_chainspec::ChainSpecProvider;
    let config = EthEvmConfig::new(provider.chain_spec());
    let api = EthApiBuilder::new(provider, testing_pool(), NoopNetwork::default(), config).build();
    let runtime = Runtime::test();
    let debug = DebugApi::new(api, BlockingTaskGuard::new(1), &runtime, futures::stream::empty());
    for id in [
        alloy_eips::BlockId::number(1),
        alloy_eips::BlockId::hash(block.hash()),
        alloy_eips::BlockId::hash_canonical(block.hash()),
    ] {
        let error = DebugApiServer::debug_execution_witness(&debug, id).await.unwrap_err();
        assert_ne!(error.code(), jsonrpsee::types::error::INVALID_PARAMS_CODE);
        assert!(error.message().contains("unsupported"), "{error}");
    }
}

#[tokio::test]
async fn intermediate_roots_replays_cached_bad_block_until_first_invalid_transaction() {
    use reth_chainspec::ChainSpecProvider;
    let (provider, block, _) = fixture();
    let mut bad_block = block.into_block();
    // Each transfer fits individually, but the second cannot fit after the first uses 21,000 gas.
    bad_block.header.gas_limit = 31_000;
    let bad_block = bad_block.try_into_recovered().unwrap();
    let hash = bad_block.hash();
    let config = EthEvmConfig::new(provider.chain_spec());
    let api = EthApiBuilder::new(provider, testing_pool(), NoopNetwork::default(), config).build();
    let runtime = Runtime::test();
    let debug = DebugApi::new(api, BlockingTaskGuard::new(1), &runtime, futures::stream::empty());
    debug.inner.bad_block_store.insert(bad_block, "second transaction exceeds block gas".into());
    assert_eq!(debug.intermediate_roots(hash).await.unwrap(), vec![B256::ZERO]);
}

#[tokio::test]
async fn commit_condition_can_observe_executor_state_without_committing() {
    use alloy_evm::block::{CommitChanges, TxResult};
    use reth_chainspec::ChainSpecProvider;
    let (provider, block, sender) = fixture();
    let inner = EthEvmConfig::new(provider.chain_spec());
    let config = SettlementConfig {
        factory: SettlementFactory(inner.block_executor_factory().clone()),
        inner,
    };
    let api = EthApiBuilder::new(provider, testing_pool(), NoopNetwork::default(), config).build();
    api.spawn_with_state_at_block(block.parent_hash(), move |api, mut db: StateCacheDb| {
        let config = api.evm_config().chain_block_mode();
        let mut executor = config.executor_for_block(&mut db, block.sealed_block()).unwrap();
        executor.apply_pre_execution_changes().unwrap();
        let result = executor
            .execute_transaction_with_commit_condition(
                block.transactions_recovered().next().unwrap(),
                |output| {
                    assert_eq!(output.result().state[&CREDIT].info.balance, U256::from(7));
                    assert_eq!(output.result().state[&sender].info.nonce, 1);
                    CommitChanges::No
                },
            )
            .unwrap();
        assert_eq!(result, None);
        drop(executor);
        assert_eq!(db.basic(sender).unwrap().unwrap().nonce, 0);
        assert_eq!(db.basic(CREDIT).unwrap(), None);
        Ok(())
    })
    .await
    .unwrap();
}
