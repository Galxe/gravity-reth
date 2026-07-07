use crate::EthEvmConfig;
use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};
use alloy_consensus::{Header, TxType};
use alloy_eips::eip7685::Requests;
use alloy_evm::{
    block::{GasOutput, StateDB},
    eth::EthTxResult,
    precompiles::PrecompilesMap,
};
use alloy_primitives::Bytes;
use alloy_rpc_types_engine::ExecutionData;
use core::marker::PhantomData;
use parking_lot::Mutex;
use reth_ethereum_primitives::{Receipt, TransactionSigned};
use reth_evm::{
    block::{BlockExecutionError, BlockExecutor, BlockExecutorFactory, ExecutableTx},
    eth::{EthBlockExecutionCtx, EthEvmContext},
    parallel_execute::ParallelExecutor,
    ConfigureEngineEvm, ConfigureEvm, EthEvm, EthEvmFactory, EvmEnvFor, EvmFactory,
    ExecutableTxIterator, ExecutionCtxFor, ParallelDatabase,
};
use reth_execution_types::{BlockExecutionResult, ExecutionOutcome};
use reth_primitives_traits::{BlockTy, SealedBlock, SealedHeader};
use revm::{
    context::result::{
        ExecutionResult, HaltReason, Output, ResultAndState, ResultGas, SuccessReason,
    },
    Inspector,
};

/// A helper type alias for mocked block executor provider.
pub type MockExecutorProvider = MockEvmConfig;

/// A block executor provider that returns mocked execution results.
#[derive(Clone, Debug)]
pub struct MockEvmConfig {
    inner: EthEvmConfig,
    exec_results: Arc<Mutex<Vec<ExecutionOutcome>>>,
}

impl Default for MockEvmConfig {
    fn default() -> Self {
        Self { inner: EthEvmConfig::mainnet(), exec_results: Default::default() }
    }
}

impl MockEvmConfig {
    /// Extend the mocked execution results
    pub fn extend(&self, results: impl IntoIterator<Item = impl Into<ExecutionOutcome>>) {
        self.exec_results.lock().extend(results.into_iter().map(Into::into));
    }
}

impl BlockExecutorFactory for MockEvmConfig {
    type EvmFactory = EthEvmFactory;
    type ExecutionCtx<'a> = EthBlockExecutionCtx<'a>;
    type Receipt = Receipt;
    type Transaction = TransactionSigned;
    // Mock transactions are not backed by a real EVM run, so we reuse the canonical
    // Ethereum per-transaction result type but leave `result` / `blob_gas_used` at their
    // defaults from `execute_transaction_without_commit`.
    type TxExecutionResult = EthTxResult<HaltReason, TxType>;
    type Executor<'a, DB: StateDB, I: Inspector<EthEvmContext<DB>>> = MockExecutor<'a, DB, I>;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EthEvm<DB, I, PrecompilesMap>,
        _ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a, DB, I>
    where
        DB: StateDB,
        I: Inspector<<Self::EvmFactory as EvmFactory>::Context<DB>>,
    {
        MockExecutor { result: self.exec_results.lock().pop().unwrap(), evm, _phantom: PhantomData }
    }
}

/// Mock executor that returns a fixed execution result.
///
/// The `'a` lifetime carries no data of its own: it exists purely to satisfy the
/// `BlockExecutor` GAT introduced in alloy-evm 0.36 (`type Executor<'a, DB, I>`), where
/// the executor is tied to the factory that created it.
#[derive(derive_more::Debug)]
pub struct MockExecutor<'a, DB: StateDB, I: Inspector<EthEvmContext<DB>>> {
    result: ExecutionOutcome,
    evm: EthEvm<DB, I, PrecompilesMap>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, DB: StateDB, I: Inspector<EthEvmContext<DB>>> BlockExecutor for MockExecutor<'a, DB, I> {
    type Evm = EthEvm<DB, I, PrecompilesMap>;
    type Transaction = TransactionSigned;
    type Receipt = Receipt;
    type Result = EthTxResult<HaltReason, TxType>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        _tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        Ok(EthTxResult {
            result: ResultAndState::new(
                ExecutionResult::Success {
                    reason: SuccessReason::Return,
                    gas: ResultGas::default(),
                    logs: vec![],
                    output: Output::Call(Bytes::from(vec![])),
                },
                Default::default(),
            ),
            blob_gas_used: 0,
            tx_type: TxType::Legacy,
        })
    }

    fn commit_transaction(&mut self, _output: Self::Result) -> GasOutput {
        GasOutput::new(0)
    }

    fn finish(
        self,
    ) -> Result<(Self::Evm, BlockExecutionResult<Self::Receipt>), BlockExecutionError> {
        let Self { result, evm, .. } = self;
        let ExecutionOutcome { bundle: _, receipts, requests, first_block: _ } = result;
        let result = BlockExecutionResult {
            receipts: receipts.into_iter().flatten().collect(),
            requests: requests.into_iter().fold(Requests::default(), |mut reqs, req| {
                reqs.extend(req);
                reqs
            }),
            gas_used: 0,
            blob_gas_used: 0,
        };
        // Note: alloy-evm 0.36 no longer requires re-attaching a `BundleState` onto the
        // executor's DB; downstream test consumers only observe `ExecutionOutcome` shape and
        // never inspect the mock DB directly. Preserving the pre-0.36 `bundle_state = bundle`
        // line would require constraining `DB` to `State<InnerDB>`, breaking the trait's
        // generic `DB: StateDB` contract.
        Ok((evm, result))
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &[]
    }
}

impl ConfigureEvm for MockEvmConfig {
    type BlockAssembler = <EthEvmConfig as ConfigureEvm>::BlockAssembler;
    type BlockExecutorFactory = Self;
    type Error = <EthEvmConfig as ConfigureEvm>::Error;
    type NextBlockEnvCtx = <EthEvmConfig as ConfigureEvm>::NextBlockEnvCtx;
    type Primitives = <EthEvmConfig as ConfigureEvm>::Primitives;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        self
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
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
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> Result<reth_evm::ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_block(block)
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<reth_evm::ExecutionCtxFor<'_, Self>, Self::Error> {
        self.inner.context_for_next_block(parent, attributes)
    }

    fn parallel_executor<'a, DB: ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<dyn ParallelExecutor<Primitives = Self::Primitives, Error = BlockExecutionError> + 'a>
    {
        self.inner.parallel_executor(db)
    }
}

impl ConfigureEngineEvm<ExecutionData> for MockEvmConfig {
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_payload(payload)
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        self.inner.tx_iterator_for_payload(payload)
    }
}
