//! EVM configuration of a Gravity node.
//!
//! [`GravityEvmConfig`] reproduces Gravity's on-chain execution in every re-execution a node
//! performs outside the pipe: RPC replay of committed blocks, simulation, pending blocks. The
//! rules live in two layers:
//!
//! - [`GravityEvm`] applies the per-transaction rules, so every path that creates an EVM gets them;
//! - [`GravityBlockExecutor`] applies the block-level steps. Endpoints that replay a committed
//!   block declare it with [`ConfigureEvmForRead::chain_block_mode`].
//!
//! The pipe does not use this configuration: it executes committed blocks with grevm through its
//! own [`EthEvmConfig`], which stays plain Ethereum.

mod delegated_create;
mod epoch_block;
mod evm;
mod executor;
mod randomness;
mod reader;
#[cfg(test)]
pub(crate) mod test_utils;

pub use evm::{GravityEvm, GravityEvmFactory};
pub use executor::{
    ChainBlock, GravityBlockExecutionCtx, GravityBlockExecutor, GravityBlockExecutorFactory,
};
pub use randomness::HeaderRandomnessProvider;
pub use reader::{GravityChainReader, ProviderChainReader};

use crate::{EthBlockAssembler, EthEvmConfig, RethReceiptBuilder};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::Header;
use alloy_evm::{eth::EthBlockExecutorFactory, precompiles::DynPrecompile, Database};
use alloy_primitives::Address;
use core::convert::Infallible;
use epoch_block::BodyFacts;
use reth_chainspec::ChainSpec;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::{
    execute::{BlockAssembler, BlockAssemblerInput, BlockExecutionError},
    parallel_execute::ParallelExecutor,
    ConfigureEvm, ConfigureEvmForRead, EvmEnv, NextBlockEnvAttributes, ParallelDatabase,
};
use reth_primitives_traits::{SealedBlock, SealedHeader};
use revm::{
    context::{
        result::{ExecutionResult, HaltReason},
        TxEnv,
    },
    database::State,
};
#[cfg(feature = "std")]
use {
    alloy_eips::Decodable2718,
    alloy_primitives::Bytes,
    alloy_rpc_types_engine::ExecutionData,
    reth_evm::{ConfigureEngineEvm, EvmEnvFor, ExecutableTxIterator, ExecutionCtxFor},
    reth_primitives_traits::{SignedTransaction, TxTy},
    reth_storage_errors::any::AnyError,
};

/// EVM configuration of a Gravity node; see the [module docs](self).
#[derive(Debug, Clone)]
pub struct GravityEvmConfig {
    /// Plain Ethereum configuration for everything the Gravity rules do not change: EVM
    /// environments (with Gravity's transaction gas cap) and the pipe's grevm entry points.
    inner: EthEvmConfig,
    executor_factory: GravityBlockExecutorFactory,
    block_assembler: GravityBlockAssembler,
    /// See [`ConfigureEvmForRead::chain_block_mode`].
    chain_block_mode: bool,
}

impl GravityEvmConfig {
    /// Creates the configuration for `chain_spec`, reading the canonical chain through `reader`.
    pub fn new(chain_spec: Arc<ChainSpec>, reader: Arc<dyn GravityChainReader>) -> Self {
        Self {
            inner: EthEvmConfig::new(chain_spec.clone()),
            executor_factory: GravityBlockExecutorFactory::new(chain_spec.clone(), reader),
            block_assembler: GravityBlockAssembler(EthBlockAssembler::new(chain_spec)),
            chain_block_mode: false,
        }
    }

    /// Returns the chain spec.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.inner.chain_spec()
    }
}

impl ConfigureEvmForRead for GravityEvmConfig {
    fn chain_block_mode(&self) -> Self {
        Self { chain_block_mode: true, ..self.clone() }
    }

    fn with_read_provider<P>(&self, provider: P) -> Self
    where
        P: reth_storage_api::HeaderProvider<Header = Header>
            + reth_storage_api::ReceiptProvider<Receipt = reth_ethereum_primitives::Receipt>
            + reth_storage_api::BlockHashReader
            + reth_storage_api::StateProviderFactory
            + Send
            + Sync
            + 'static,
    {
        let mut config = self.clone();
        config.executor_factory = GravityBlockExecutorFactory::new(
            self.chain_spec().clone(),
            Arc::new(ProviderChainReader(provider)),
        );
        config
    }
}

impl ConfigureEvm for GravityEvmConfig {
    type Primitives = EthPrimitives;
    type Error = Infallible;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = GravityBlockExecutorFactory;
    type BlockAssembler = GravityBlockAssembler;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv, Self::Error> {
        self.inner.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv, Self::Error> {
        self.inner.next_evm_env(parent, attributes)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<GravityBlockExecutionCtx<'a>, Self::Error> {
        let chain_block = self.chain_block_mode.then(|| {
            ChainBlock::new(block.hash(), BodyFacts::of(self.chain_spec().as_ref(), block))
        });
        Ok(GravityBlockExecutionCtx { inner: self.inner.context_for_block(block)?, chain_block })
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<GravityBlockExecutionCtx<'_>, Self::Error> {
        Ok(GravityBlockExecutionCtx {
            inner: self.inner.context_for_next_block(parent, attributes)?,
            chain_block: None,
        })
    }

    fn transact_system_txn<DB: Database>(
        &self,
        db: &mut State<DB>,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, BlockExecutionError> {
        self.inner.transact_system_txn(db, evm_env, precompiles, tx_env)
    }

    fn parallel_executor<'a, DB: ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<dyn ParallelExecutor<Primitives = Self::Primitives, Error = BlockExecutionError> + 'a>
    {
        self.inner.parallel_executor(db)
    }
}

#[cfg(feature = "std")]
impl ConfigureEngineEvm<ExecutionData> for GravityEvmConfig {
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        Ok(GravityBlockExecutionCtx {
            inner: self.inner.context_for_payload(payload)?,
            chain_block: None,
        })
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        let txs = payload.payload.transactions().clone();
        let convert = |tx: Bytes| {
            let tx =
                TxTy::<Self::Primitives>::decode_2718_exact(tx.as_ref()).map_err(AnyError::new)?;
            let signer = tx.try_recover().map_err(AnyError::new)?;
            Ok::<_, AnyError>(tx.with_signer(signer))
        };

        Ok((txs, convert))
    }
}

/// Assembles blocks for [`GravityEvmConfig`] with the Ethereum [`EthBlockAssembler`].
///
/// The Ethereum assembler requires the Ethereum execution context; this unwraps it from
/// [`GravityBlockExecutionCtx`].
#[derive(Debug, Clone)]
pub struct GravityBlockAssembler(EthBlockAssembler);

impl BlockAssembler<GravityBlockExecutorFactory> for GravityBlockAssembler {
    type Block = Block;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, GravityBlockExecutorFactory>,
    ) -> Result<Block, BlockExecutionError> {
        let input = BlockAssemblerInput::<
            EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, GravityEvmFactory>,
        >::new(
            input.evm_env,
            input.execution_ctx.inner,
            input.parent,
            input.transactions,
            input.output,
            input.bundle_state,
            input.state_provider,
            input.state_root,
            input.block_access_list_hash,
        );
        self.0.assemble_block(input, None, None, None)
    }
}
