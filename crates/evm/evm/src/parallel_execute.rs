//! Traits for parallel execution of EVM blocks.

use core::marker::PhantomData;

use crate::execute::Executor;
use alloy_evm::Database;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
use revm::database::BundleState;

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
}
