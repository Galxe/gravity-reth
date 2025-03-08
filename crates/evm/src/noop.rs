//! A no operation block executor implementation.

use reth_execution_errors::BlockExecutionError;
use reth_execution_types::BlockExecutionResult;
use reth_primitives::{NodePrimitives, RecoveredBlock};

use crate::{
    execute::{BlockExecutorProvider, Executor},
    system_calls::OnStateHook,
    Database, DatabaseEnum, ParallelDatabase, State,
};

const UNAVAILABLE_FOR_NOOP: &str = "execution unavailable for noop";

/// A [`BlockExecutorProvider`] implementation that does nothing.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct NoopBlockExecutorProvider<P>(core::marker::PhantomData<P>);

impl<P: NodePrimitives> BlockExecutorProvider for NoopBlockExecutorProvider<P> {
    type Primitives = P;

    type Executor<'db> = Self;

    fn executor<'db, DB, PDB>(&self, _: DatabaseEnum<DB, PDB>) -> Self::Executor<'db>
    where
        DB: Database,
        PDB: ParallelDatabase,
    {
        Self::default()
    }
}

impl<'db, P: NodePrimitives> Executor<'db> for NoopBlockExecutorProvider<P> {
    type Primitives = P;
    type Error = BlockExecutionError;

    fn execute_one(
        &mut self,
        _block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        Err(BlockExecutionError::msg(UNAVAILABLE_FOR_NOOP))
    }

    fn execute_one_with_state_hook<F>(
        &mut self,
        _block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        _state_hook: F,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    where
        F: OnStateHook + 'static,
    {
        Err(BlockExecutionError::msg(UNAVAILABLE_FOR_NOOP))
    }

    fn into_state(self) -> Box<dyn State + 'db> {
        unreachable!()
    }

    fn size_hint(&self) -> usize {
        0
    }
}
