//! Loads a pending block from database. Helper trait for `eth_` call and trace RPC methods.

use super::{Call, LoadBlock, LoadState, LoadTransaction};
use crate::FromEvmError;
use alloy_consensus::{transaction::TxHashRef, BlockHeader};
use alloy_primitives::B256;
use alloy_rpc_types_eth::{BlockId, TransactionInfo};
use futures::Future;
use reth_errors::ProviderError;
use reth_evm::{
    ConfigureEvm, Database, Evm, EvmEnvFor, EvmFactory, EvmFor, HaltReasonFor, InspectorFor,
    TxEnvFor,
};
use reth_primitives_traits::{BlockBody, Recovered, RecoveredBlock};
use reth_revm::{
    database::StateProviderDatabase,
    db::{bal::EvmDatabaseError, State},
};
use reth_rpc_eth_types::{cache::db::StateCacheDb, EthApiError};
use reth_storage_api::{ProviderBlock, ProviderTx};
use reth_tasks::cancel::is_cancelled;
use revm::{
    context::{
        result::{ExecutionResult, ResultAndState},
        Block,
    },
    state::EvmState,
    DatabaseCommit,
};
use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
use std::sync::Arc;

// ============================================================================
// `GravityTracingCtx` — local callback ctx for the handwritten block-family
// tracing loop. Mirrors the 5 pub fields of `alloy_evm::tracing::TracingCtx`
// without carrying the upstream `fused_inspector` / `was_fused` snapshot pair:
//
// the handwritten loop re-seeds the inspector slot via `inspector_setup()`
// after every tx — `TracingInspector::new(config)` is byte-equivalent to a
// cloned `fused_inspector` snapshot, so the per-tx reset is unconditional
// and works whether the closure took the inspector or not — `was_fused`
// bookkeeping is unnecessary.
// ============================================================================

/// Container type for context exposed during block-family tracing.
#[derive(Debug)]
pub struct GravityTracingCtx<'a, T, E: Evm> {
    /// The transaction that was just executed.
    pub tx: T,
    /// Result of transaction execution.
    pub result: ExecutionResult<E::HaltReason>,
    /// State changes after transaction.
    pub state: &'a EvmState,
    /// Inspector state after transaction.
    pub inspector: &'a mut E::Inspector,
    /// Database used when executing the transaction, _before_ committing the state changes.
    pub db: &'a mut E::DB,
}

impl<'a, T, E> GravityTracingCtx<'a, T, E>
where
    E: Evm,
    E::Inspector: Default,
{
    /// Takes the inspector out of the ctx, leaving a default-constructed
    /// replacement behind for the surrounding loop to re-seed via
    /// `inspector_setup()`.
    ///
    /// Closures that consume the inspector (e.g. `.into_parity_builder()`)
    /// call this; closures that read traces in place use `ctx.inspector`
    /// directly.
    pub fn take_inspector(&mut self) -> E::Inspector {
        core::mem::take(self.inspector)
    }
}

/// Executes CPU heavy tasks.
pub trait Trace: LoadState<Error: FromEvmError<Self::Evm>> + Call {
    /// Executes the [`TxEnvFor`] with [`reth_evm::EvmEnv`] against the given [Database] without
    /// committing state changes.
    fn inspect<DB, I>(
        &self,
        db: DB,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
        inspector: I,
    ) -> Result<ResultAndState<HaltReasonFor<Self::Evm>>, Self::Error>
    where
        DB: Database<Error = EvmDatabaseError<ProviderError>>,
        I: InspectorFor<Self::Evm, DB>,
    {
        let mut evm = self.evm_config().evm_with_env_and_inspector(db, evm_env, inspector);
        evm.transact(tx_env).map_err(Self::Error::from_evm_err)
    }

    /// Executes the transaction on top of the given [`BlockId`] with a tracer configured by the
    /// config.
    ///
    /// The callback is then called with the [`TracingInspector`] and the [`ResultAndState`] after
    /// the configured [`reth_evm::EvmEnv`] was inspected.
    ///
    /// Caution: this is blocking
    fn trace_at<F, R>(
        &self,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
        config: TracingInspectorConfig,
        at: BlockId,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        R: Send + 'static,
        F: FnOnce(
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
    {
        self.with_state_at_block(at, move |this, state| {
            let mut db = State::builder().with_database(StateProviderDatabase::new(state)).build();
            let mut inspector = TracingInspector::new(config);
            let res = this.inspect(&mut db, evm_env, tx_env, &mut inspector)?;
            f(inspector, res)
        })
    }

    /// Same as [`trace_at`](Self::trace_at) but also provides the used database to the callback.
    ///
    /// Executes the transaction on top of the given [`BlockId`] with a tracer configured by the
    /// config.
    ///
    /// The callback is then called with the [`TracingInspector`] and the [`ResultAndState`] after
    /// the configured [`reth_evm::EvmEnv`] was inspected.
    fn spawn_trace_at_with_state<F, R>(
        &self,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
        config: TracingInspectorConfig,
        at: BlockId,
        f: F,
    ) -> impl Future<Output = Result<R, Self::Error>> + Send
    where
        F: FnOnce(
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.spawn_with_state_at_block(at, move |this, mut db| {
            let mut inspector = TracingInspector::new(config);
            let res = this.inspect(&mut db, evm_env, tx_env, &mut inspector)?;
            f(inspector, res, db)
        })
    }

    /// Retrieves the transaction if it exists and returns its trace.
    ///
    /// Before the transaction is traced, all previous transaction in the block are applied to the
    /// state by executing them first.
    /// The callback `f` is invoked with the [`ResultAndState`] after the transaction was executed
    /// and the database that points to the beginning of the transaction.
    ///
    /// Note: Implementers should use a threadpool where blocking is allowed, such as
    /// [`BlockingTaskPool`](reth_tasks::pool::BlockingTaskPool).
    fn spawn_trace_transaction_in_block<F, R>(
        &self,
        hash: B256,
        config: TracingInspectorConfig,
        f: F,
    ) -> impl Future<Output = Result<Option<R>, Self::Error>> + Send
    where
        Self: LoadTransaction,
        F: FnOnce(
                TransactionInfo,
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.spawn_trace_transaction_in_block_with_inspector(hash, TracingInspector::new(config), f)
    }

    /// Retrieves the transaction if it exists and returns its trace.
    ///
    /// Before the transaction is traced, all previous transaction in the block are applied to the
    /// state by executing them first.
    /// The callback `f` is invoked with the [`ResultAndState`] after the transaction was executed
    /// and the database that points to the beginning of the transaction.
    ///
    /// Note: Implementers should use a threadpool where blocking is allowed, such as
    /// [`BlockingTaskPool`](reth_tasks::pool::BlockingTaskPool).
    fn spawn_trace_transaction_in_block_with_inspector<Insp, F, R>(
        &self,
        hash: B256,
        mut inspector: Insp,
        f: F,
    ) -> impl Future<Output = Result<Option<R>, Self::Error>> + Send
    where
        Self: LoadTransaction,
        F: FnOnce(
                TransactionInfo,
                Insp,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Insp: for<'a> InspectorFor<Self::Evm, &'a mut StateCacheDb> + Send + 'static,
        R: Send + 'static,
    {
        async move {
            let this = self.view().await?;
            let (transaction, block) = match this.transaction_and_block(hash).await? {
                None => return Ok(None),
                Some(res) => res,
            };
            let (tx, tx_info) = transaction.split();

            let evm_env = this.evm_env_for_header(block.sealed_block().sealed_header())?;

            // we need to get the state of the parent block because we're essentially replaying the
            // block the transaction is included in
            let parent_block = block.parent_hash();

            this.spawn_with_state_at_block(parent_block, move |this, mut db| {
                let block_txs = block.transactions_recovered();

                this.apply_pre_execution_changes(&block, &mut db)?;

                // replay all transactions prior to the targeted transaction
                this.replay_transactions_until(&mut db, evm_env.clone(), block_txs, *tx.tx_hash())?;

                let tx_env = this.evm_config().tx_env(tx);
                let res = this.inspect(&mut db, evm_env, tx_env, &mut inspector)?;
                f(tx_info, inspector, res, db)
            })
            .await
            .map(Some)
        }
    }

    /// Executes all transactions of a block up to a given index.
    ///
    /// If a `highest_index` is given, this will only execute the first `highest_index`
    /// transactions, in other words, it will stop executing transactions after the
    /// `highest_index`th transaction. If `highest_index` is `None`, all transactions
    /// are executed.
    fn trace_block_until<F, R>(
        &self,
        block_id: BlockId,
        block: Option<Arc<RecoveredBlock<ProviderBlock<Self::Provider>>>>,
        highest_index: Option<u64>,
        config: TracingInspectorConfig,
        f: F,
    ) -> impl Future<Output = Result<Option<Vec<R>>, Self::Error>> + Send
    where
        Self: LoadBlock,
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, &mut StateCacheDb, TracingInspector>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.trace_block_until_with_inspector(
            block_id,
            block,
            highest_index,
            move || TracingInspector::new(config),
            f,
        )
    }

    /// Executes all transactions of a block.
    ///
    /// If a `highest_index` is given, this will only execute the first `highest_index`
    /// transactions, in other words, it will stop executing transactions after the
    /// `highest_index`th transaction.
    ///
    /// Note: This expect tx index to be 0-indexed, so the first transaction is at index 0.
    ///
    /// This accepts a `inspector_setup` closure that returns the inspector to be used for tracing
    /// the transactions.
    fn trace_block_until_with_inspector<Setup, Insp, F, R>(
        &self,
        block_id: BlockId,
        block: Option<Arc<RecoveredBlock<ProviderBlock<Self::Provider>>>>,
        highest_index: Option<u64>,
        mut inspector_setup: Setup,
        f: F,
    ) -> impl Future<Output = Result<Option<Vec<R>>, Self::Error>> + Send
    where
        Self: LoadBlock,
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, &mut StateCacheDb, Insp>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Setup: FnMut() -> Insp + Send + 'static,
        Insp: for<'a> InspectorFor<Self::Evm, &'a mut StateCacheDb>,
        R: Send + 'static,
    {
        async move {
            let this = self.view().await?;
            let block =
                if block.is_some() { block } else { this.recovered_block(block_id).await? };

            let Some(block) = block else { return Ok(None) };
            let evm_env = this.evm_env_for_header(block.sealed_block().sealed_header())?;

            if block.body().transactions().is_empty() {
                // nothing to trace
                return Ok(Some(Vec::new()))
            }

            // replay all transactions of the block
            // we need to get the state of the parent block because we're replaying this block
            // on top of its parent block's state
            this.spawn_with_state_at_block(block.parent_hash(), move |this, mut db| {
                let block_hash = block.hash();

                let block_number = evm_env.block_env.number().saturating_to();
                let block_timestamp = evm_env.block_env.timestamp().saturating_to();
                let base_fee = evm_env.block_env.basefee();

                this.apply_pre_execution_changes(&block, &mut db)?;

                // prepare transactions, we do everything upfront to reduce time spent with open
                // state
                let max_transactions = highest_index.map_or_else(
                    || block.body().transaction_count(),
                    |highest| {
                        // we need + 1 because the index is 0-based
                        highest as usize + 1
                    },
                );

                let mut idx = 0u64;
                let mut txs_iter = block.transactions_recovered().take(max_transactions).peekable();
                let mut current_evm = this.evm_config().evm_factory().create_evm_with_inspector(
                    &mut db,
                    evm_env,
                    inspector_setup(),
                );
                let mut results: Vec<R> = Vec::with_capacity(max_transactions);

                while let Some(tx) = txs_iter.next() {
                    if is_cancelled() {
                        return Err(EthApiError::InternalEthError.into())
                    }
                    let tx_hash = *tx.tx_hash();
                    let ResultAndState { result, state, .. } = match current_evm.transact(tx) {
                        Ok(r) => r,
                        Err(e) => return Err(Self::Error::from_evm_err(e)),
                    };

                    let (db_ref, inspector_ref, _) = current_evm.components_mut();
                    let tx_info = TransactionInfo {
                        hash: Some(tx_hash),
                        index: Some(idx),
                        block_hash: Some(block_hash),
                        block_number: Some(block_number),
                        block_timestamp: Some(block_timestamp),
                        base_fee: Some(base_fee),
                    };
                    idx += 1;

                    let ctx = GravityTracingCtx {
                        tx,
                        result,
                        state: &state,
                        inspector: inspector_ref,
                        db: db_ref,
                    };
                    let output = f(tx_info, ctx)?;
                    results.push(output);

                    // Match `TxTracer::try_trace_many` default (`skip_last_commit
                    // = true`): commit only when there's a follow-up tx.
                    let has_more = txs_iter.peek().is_some();
                    if has_more {
                        db_ref.commit(state);
                    }

                    // Per-tx fuse: re-seed the inspector slot via
                    // `inspector_setup()`. This is byte-equivalent to cloning a
                    // fused snapshot (`TracingInspector::new(config)` ==
                    // `fused_inspector.clone()`) and works whether the closure
                    // took the inspector or not — no `was_fused` bookkeeping
                    // needed.
                    let _ = core::mem::replace(current_evm.inspector_mut(), inspector_setup());
                }

                Ok(Some(results))
            })
            .await
        }
    }

    /// Executes all transactions of a block and returns a list of callback results invoked for each
    /// transaction in the block.
    ///
    /// This
    /// 1. fetches all transactions of the block
    /// 2. configures the EVM env
    /// 3. loops over all transactions and executes them
    /// 4. calls the callback with the transaction info, the execution result, the changed state
    ///    _after_ the transaction [`StateProviderDatabase`] and the database that points to the
    ///    state right _before_ the transaction.
    fn trace_block_with<F, R>(
        &self,
        block_id: BlockId,
        block: Option<Arc<RecoveredBlock<ProviderBlock<Self::Provider>>>>,
        config: TracingInspectorConfig,
        f: F,
    ) -> impl Future<Output = Result<Option<Vec<R>>, Self::Error>> + Send
    where
        Self: LoadBlock,
        // This is the callback that's invoked for each transaction with the inspector, the result,
        // state and db
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, &mut StateCacheDb, TracingInspector>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.trace_block_until(block_id, block, None, config, f)
    }

    /// Executes all transactions of a block and returns a list of callback results invoked for each
    /// transaction in the block.
    ///
    /// This
    /// 1. fetches all transactions of the block
    /// 2. configures the EVM env
    /// 3. loops over all transactions and executes them
    /// 4. calls the callback with the transaction info, the execution result, the changed state
    ///    _after_ the transaction `EvmState` and the database that points to the state right
    ///    _before_ the transaction, in other words the state the transaction was executed on:
    ///    `changed_state = tx(cached_state)`
    ///
    /// This accepts a `inspector_setup` closure that returns the inspector to be used for tracing
    /// a transaction. This is invoked for each transaction.
    fn trace_block_inspector<Setup, Insp, F, R>(
        &self,
        block_id: BlockId,
        block: Option<Arc<RecoveredBlock<ProviderBlock<Self::Provider>>>>,
        insp_setup: Setup,
        f: F,
    ) -> impl Future<Output = Result<Option<Vec<R>>, Self::Error>> + Send
    where
        Self: LoadBlock,
        // This is the callback that's invoked for each transaction with the inspector, the result,
        // state and db
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, &mut StateCacheDb, Insp>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Setup: FnMut() -> Insp + Send + 'static,
        Insp: for<'a> InspectorFor<Self::Evm, &'a mut StateCacheDb>,
        R: Send + 'static,
    {
        self.trace_block_until_with_inspector(block_id, block, None, insp_setup, f)
    }
}
