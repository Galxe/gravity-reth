//! Helper type that represents one of two possible executor types

use crate::{execute::Executor, Database, OnStateHook};

use alloy_evm::{precompiles::DynPrecompile, EvmEnv};
use alloy_primitives::Address;
pub use futures_util::future::Either;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
use revm::{
    context::{
        result::{ExecutionResult, HaltReason},
        TxEnv,
    },
    database::BundleState,
};

impl<A, B, DB> Executor<DB> for Either<A, B>
where
    A: Executor<DB>,
    B: Executor<DB, Primitives = A::Primitives, Error = A::Error>,
    DB: Database,
{
    type Primitives = A::Primitives;
    type Error = A::Error;

    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        match self {
            Self::Left(a) => a.execute_one(block),
            Self::Right(b) => b.execute_one(block),
        }
    }

    fn execute_one_with_state_hook<F>(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        state_hook: F,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    where
        F: OnStateHook + 'static,
    {
        match self {
            Self::Left(a) => a.execute_one_with_state_hook(block, state_hook),
            Self::Right(b) => b.execute_one_with_state_hook(block, state_hook),
        }
    }

    fn execute(
        self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        match self {
            Self::Left(a) => a.execute(block),
            Self::Right(b) => b.execute(block),
        }
    }

    fn execute_with_state_closure<F>(
        self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        state: F,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    where
        F: FnMut(&revm::database::State<DB>),
    {
        match self {
            Self::Left(a) => a.execute_with_state_closure(block, state),
            Self::Right(b) => b.execute_with_state_closure(block, state),
        }
    }

    fn into_state(self) -> revm::database::State<DB> {
        match self {
            Self::Left(a) => a.into_state(),
            Self::Right(b) => b.into_state(),
        }
    }

    fn take_bundle(&mut self) -> BundleState {
        match self {
            Self::Left(a) => a.take_bundle(),
            Self::Right(b) => b.take_bundle(),
        }
    }

    fn size_hint(&self) -> usize {
        match self {
            Self::Left(a) => a.size_hint(),
            Self::Right(b) => b.size_hint(),
        }
    }

    fn transact_system_txn(
        &mut self,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
        match self {
            Self::Left(a) => a.transact_system_txn(evm_env, precompiles, tx_env),
            Self::Right(b) => b.transact_system_txn(evm_env, precompiles, tx_env),
        }
    }
}
