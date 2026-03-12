//! Traits for parallel execution of EVM blocks.

use core::marker::PhantomData;
use std::sync::Arc;

use crate::execute::Executor;
use alloy_evm::{precompiles::DynPrecompile, Database, EvmEnv};
use alloy_primitives::Address;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
use revm::{
    context::TxEnv,
    context_interface::result::{ExecutionResult, HaltReason},
    database::BundleState,
};

/// The `ParallelExecutor` trait defines the interface for executing EVM blocks in parallel.
pub trait ParallelExecutor {
    /// The primitive types used by the executor.
    type Primitives: NodePrimitives;
    /// The error type returned by the executor.
    type Error;

    /// Executes a single block and returns [`BlockExecutionResult`], without the state changes.
    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>;

    /// Takes the `BundleState` changeset from the State, replacing it with an empty one.
    fn take_bundle(&mut self) -> BundleState;

    /// The size hint of the batch's tracked state size.
    ///
    /// This is used to optimize DB commits depending on the size of the state.
    fn size_hint(&self) -> usize;

    /// Consumes the type and executes the block.
    ///
    /// # Note
    /// Execution happens without any validation of the output.
    ///
    /// # Returns
    /// The output of the block execution.
    fn execute(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        let result = self.execute_one(block)?;
        Ok(BlockExecutionOutput { state: self.take_bundle(), result })
    }

    /// Executes a single system transaction on the executor's own internal state and commits
    /// the resulting state changes immediately.
    ///
    /// The EVM is constructed internally using the executor's `ParallelState` as the DB,
    /// so there is only ONE source of truth. State changes are committed immediately after
    /// execution. The next call will see updated nonces, balances, and storage without any
    /// external bridging.
    ///
    /// `precompiles` allows callers to inject custom precompiles (e.g. mint, BLS) for this
    /// specific transaction, in addition to any executor-level custom precompiles.
    fn transact_system_txn(
        &mut self,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error>;

    /// Applies custom precompiled contracts to the executor.
    ///
    /// These precompiles will be available during transaction execution alongside
    /// the standard Ethereum precompiles. This is a no-op by default.
    fn apply_custom_precompiles(&mut self, custom_precompiles: Arc<Vec<(Address, DynPrecompile)>>);
}

/// Wraps a [`Executor`] to provide a [`ParallelExecutor`] implementation.
#[derive(Debug)]
pub struct WrapExecutor<DB: Database, T: Executor<DB>>(pub T, PhantomData<DB>);

impl<DB: Database, T: Executor<DB>> WrapExecutor<DB, T> {
    /// Creates a new `WrapExecutor` from the given executor.
    pub const fn new(executor: T) -> Self {
        Self(executor, PhantomData)
    }
}

impl<DB: Database, T: Executor<DB>> ParallelExecutor for WrapExecutor<DB, T> {
    type Primitives = T::Primitives;
    type Error = T::Error;

    #[inline]
    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        self.0.execute_one(block)
    }

    #[inline]
    fn take_bundle(&mut self) -> BundleState {
        self.0.take_bundle()
    }

    #[inline]
    fn size_hint(&self) -> usize {
        self.0.size_hint()
    }

    #[inline]
    fn transact_system_txn(
        &mut self,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
        self.0.transact_system_txn(evm_env, precompiles, tx_env)
    }

    #[inline]
    fn apply_custom_precompiles(
        &mut self,
        _custom_precompiles: Arc<Vec<(Address, DynPrecompile)>>,
    ) {
        // TODO(Ashin Gau): How does basic executor handle custom precompiles
    }
}
