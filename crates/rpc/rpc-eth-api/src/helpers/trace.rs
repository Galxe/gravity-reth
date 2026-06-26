//! Loads a pending block from database. Helper trait for `eth_` call and trace RPC methods.

use super::{Call, LoadBlock, LoadPendingBlock, LoadState, LoadTransaction};
use crate::FromEvmError;
use alloy_consensus::{transaction::TxHashRef, BlockHeader};
use alloy_primitives::B256;
use alloy_rpc_types_eth::{BlockId, TransactionInfo};
use futures::Future;
use reth_chainspec::{is_system_tx_gas_exempt, ChainSpecProvider, SYSTEM_CALLER};
use reth_errors::ProviderError;
use reth_evm::{
    system_calls::SystemCaller, ConfigureEvm, Database, Evm, EvmEnvFor, EvmFactory, EvmFor,
    HaltReasonFor, InspectorFor, TxEnvFor,
};
use reth_primitives_traits::{BlockBody, Recovered, RecoveredBlock};
use reth_revm::{database::StateProviderDatabase, db::CacheDB};
use reth_rpc_eth_types::{
    cache::db::{StateCacheDb, StateCacheDbRefMutWrapper, StateProviderTraitObjWrapper},
    EthApiError,
};
use reth_storage_api::{ProviderBlock, ProviderTx};
use revm::{
    context::result::{ExecutionResult, ResultAndState},
    state::EvmState,
    DatabaseCommit,
};
use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
use std::sync::Arc;

// ============================================================================
// `GravityTracingCtx` — local mirror of `alloy_evm::tracing::TracingCtx` whose
// `fused_inspector` / `was_fused` fields are pub so the handwritten loop in
// `trace_block_until_with_inspector` can construct it. See system-tx
// gas-exempt design §3.5.4 / R-A2 verify trail for the choice of (a) over
// (b) (the alternative — fork alloy-evm to pub-ify those two fields).
//
// Field shape, lifetimes and `take_inspector` semantics are byte-equivalent to
// `TracingCtx`; the only diff is that the last two fields carry `pub`.
// ============================================================================

/// Container type for context exposed during block-family tracing, mirroring
/// `alloy_evm::tracing::TracingCtx` with all fields publicly constructible.
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
    /// Fused inspector snapshot — `take_inspector` resets `inspector` to a clone of this.
    pub fused_inspector: &'a E::Inspector,
    /// Set by `take_inspector` so the surrounding loop can skip the default fuse step.
    pub was_fused: &'a mut bool,
}

impl<'a, T, E: Evm<Inspector: Clone>> GravityTracingCtx<'a, T, E> {
    /// Fuses the inspector and returns the current inspector state.
    ///
    /// Byte-equivalent to `alloy_evm::tracing::TracingCtx::take_inspector`.
    pub fn take_inspector(&mut self) -> E::Inspector {
        *self.was_fused = true;
        core::mem::replace(self.inspector, self.fused_inspector.clone())
    }
}

/// Executes CPU heavy tasks.
pub trait Trace: LoadState<Error: FromEvmError<Self::Evm>> {
    /// Executes the [`TxEnvFor`] with [`EvmEnvFor`] against the given [Database] without committing
    /// state changes.
    fn inspect<DB, I>(
        &self,
        db: DB,
        evm_env: EvmEnvFor<Self::Evm>,
        tx_env: TxEnvFor<Self::Evm>,
        inspector: I,
    ) -> Result<ResultAndState<HaltReasonFor<Self::Evm>>, Self::Error>
    where
        Self: Call,
        DB: Database<Error = ProviderError>,
        I: InspectorFor<Self::Evm, DB>,
    {
        let block_number = evm_env.block_env.number;
        let block_timestamp = evm_env.block_env.timestamp;
        let current_randomness = evm_env.block_env.prevrandao;
        let mut evm = self.evm_config().evm_with_env_and_inspector(db, evm_env, inspector);
        self.register_custom_precompiles(
            &mut evm,
            block_number,
            block_timestamp,
            current_randomness,
        );
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
        Self: Call,
        R: Send + 'static,
        F: FnOnce(
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
    {
        self.with_state_at_block(at, move |this, state| {
            let mut db = CacheDB::new(StateProviderDatabase::new(state));
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
        Self: LoadPendingBlock + Call,
        F: FnOnce(
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb<'_>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let this = self.clone();
        self.spawn_with_state_at_block(at, move |state| {
            let mut db = CacheDB::new(StateProviderDatabase::new(state));
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
        Self: LoadPendingBlock + LoadTransaction + Call,
        F: FnOnce(
                TransactionInfo,
                TracingInspector,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb<'_>,
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
        Self: LoadPendingBlock + LoadTransaction + Call,
        F: FnOnce(
                TransactionInfo,
                Insp,
                ResultAndState<HaltReasonFor<Self::Evm>>,
                StateCacheDb<'_>,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Insp:
            for<'a, 'b> InspectorFor<Self::Evm, StateCacheDbRefMutWrapper<'a, 'b>> + Send + 'static,
        R: Send + 'static,
    {
        async move {
            let (transaction, block) = match self.transaction_and_block(hash).await? {
                None => return Ok(None),
                Some(res) => res,
            };
            let (tx, tx_info) = transaction.split();

            let (evm_env, _) = self.evm_env_at(block.hash().into()).await?;

            // we need to get the state of the parent block because we're essentially replaying the
            // block the transaction is included in
            let parent_block = block.parent_hash();

            let this = self.clone();
            self.spawn_with_state_at_block(parent_block.into(), move |state| {
                let mut db = CacheDB::new(StateProviderDatabase::new(state));
                let block_txs = block.transactions_recovered();

                this.apply_pre_execution_changes(&block, &mut db, &evm_env)?;

                // replay all transactions prior to the targeted transaction
                this.replay_transactions_until(&mut db, evm_env.clone(), block_txs, *tx.tx_hash())?;

                // Gravity Alpha (system-tx gas-exempt) single-tx-family wiring —
                // if the *target* tx itself is system-sender, toggle disables on
                // the `evm_env` we pass to `inspect`. Gate keys off the replayed
                // block's timestamp (same predicate as the block family and the
                // pre-target replay loop in `replay_transactions_until`).
                let exempt_fork_active = is_system_tx_gas_exempt(
                    this.provider().chain_spec().as_ref(),
                    evm_env.block_env.timestamp.saturating_to::<u64>(),
                );
                let mut target_evm_env = evm_env;
                if exempt_fork_active && tx.signer() == SYSTEM_CALLER {
                    target_evm_env.cfg_env.disable_base_fee = true;
                    target_evm_env.cfg_env.disable_balance_check = true;
                }

                let tx_env = this.evm_config().tx_env(tx);
                let res = this.inspect(
                    StateCacheDbRefMutWrapper(&mut db),
                    target_evm_env,
                    tx_env,
                    &mut inspector,
                )?;
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
        Self: LoadBlock + Call,
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, StateCacheDbRefMutWrapper<'_, '_>, TracingInspector>,
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
        Self: LoadBlock + Call,
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, StateCacheDbRefMutWrapper<'_, '_>, Insp>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Setup: FnMut() -> Insp + Send + 'static,
        Insp: Clone + for<'a, 'b> InspectorFor<Self::Evm, StateCacheDbRefMutWrapper<'a, 'b>>,
        R: Send + 'static,
    {
        async move {
            let block = async {
                if block.is_some() {
                    return Ok(block)
                }
                self.recovered_block(block_id).await
            };

            let ((evm_env, _), block) = futures::try_join!(self.evm_env_at(block_id), block)?;

            let Some(block) = block else { return Ok(None) };

            if block.body().transactions().is_empty() {
                // nothing to trace
                return Ok(Some(Vec::new()))
            }

            // replay all transactions of the block
            self.spawn_blocking_io_fut(move |this| async move {
                // we need to get the state of the parent block because we're replaying this block
                // on top of its parent block's state
                let state_at = block.parent_hash();
                let block_hash = block.hash();

                let block_number = evm_env.block_env.number.saturating_to();
                let base_fee = evm_env.block_env.basefee;

                // now get the state
                let state = this.state_at_block_id(state_at.into()).await?;
                let mut db =
                    CacheDB::new(StateProviderDatabase::new(StateProviderTraitObjWrapper(&state)));

                this.apply_pre_execution_changes(&block, &mut db, &evm_env)?;

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

                let evm_block_number = evm_env.block_env.number;
                let evm_block_timestamp = evm_env.block_env.timestamp;
                let current_randomness = evm_env.block_env.prevrandao;

                // Gravity Alpha (system-tx gas-exempt) RPC block-family wiring,
                // sketch A1 — see system-tx gas-exempt design §3.5.4 + route doc
                // `_local/drafts/system-tx-gas-exempt/rpc-block-family-routes-A-vs-B-extend.md`.
                //
                // System transactions (sender == SYSTEM_CALLER) are positionally
                // pinned at the front of the block by the pipe layer
                // (`metadata_txn.rs:120/:185`). Run them under a cfg with
                // `disable_base_fee = true` + `disable_balance_check = true` to
                // match the canonical execution path, then `finish()` the EVM
                // once, flip the cfg back, and rebuild for the user-tx tail.
                //
                // The fork gate keys off the replayed block's timestamp
                // (not the node tip) so pre-Alpha blocks under archive replay
                // see their historical, non-zero SYSTEM_CALLER balance and don't
                // need the disables.
                let exempt_fork_active = is_system_tx_gas_exempt(
                    this.provider().chain_spec().as_ref(),
                    evm_block_timestamp.saturating_to::<u64>(),
                );

                // Classify the first transaction to pick the initial cfg.
                // If the block is empty post-take we already returned above.
                let mut txs_iter = block.transactions_recovered().take(max_transactions).peekable();
                let first_kind_system_exempt = exempt_fork_active &&
                    txs_iter.peek().map(|tx| tx.signer() == SYSTEM_CALLER).unwrap_or(false);

                // Build the initial EVM with cfg pre-toggled per the first tx's kind.
                let mut initial_env = evm_env;
                initial_env.cfg_env.disable_base_fee = first_kind_system_exempt;
                initial_env.cfg_env.disable_balance_check = first_kind_system_exempt;
                let mut current_evm = this.evm_config().evm_factory().create_evm_with_inspector(
                    StateCacheDbRefMutWrapper(&mut db),
                    initial_env,
                    inspector_setup(),
                );
                this.register_custom_precompiles(
                    &mut current_evm,
                    evm_block_number,
                    evm_block_timestamp,
                    current_randomness,
                );
                let mut current_kind_system_exempt = first_kind_system_exempt;

                // Snapshot a fused inspector base for the in-loop fuse pattern —
                // mirrors `alloy_evm::tracing::TxTracer::new`'s initial snapshot
                // so per-tx tracing semantics match canonical (each tx's hook
                // receives a fresh inspector unless it `take_inspector`-ed).
                let mut fused_inspector: Insp = current_evm.inspector().clone();

                let mut results: Vec<R> = Vec::with_capacity(max_transactions);

                while let Some(tx) = txs_iter.next() {
                    // Per-tx classification + EVM rebuild on transition.
                    let tx_is_system_exempt = exempt_fork_active && tx.signer() == SYSTEM_CALLER;
                    if tx_is_system_exempt != current_kind_system_exempt {
                        let (db_taken, mut env_taken) = current_evm.finish();
                        env_taken.cfg_env.disable_base_fee = tx_is_system_exempt;
                        env_taken.cfg_env.disable_balance_check = tx_is_system_exempt;
                        current_evm = this.evm_config().evm_factory().create_evm_with_inspector(
                            db_taken,
                            env_taken,
                            inspector_setup(),
                        );
                        this.register_custom_precompiles(
                            &mut current_evm,
                            evm_block_number,
                            evm_block_timestamp,
                            current_randomness,
                        );
                        // Refresh the fused base so subsequent fuses go back to
                        // a fresh inspector consistent with the new EVM.
                        fused_inspector = current_evm.inspector().clone();
                        current_kind_system_exempt = tx_is_system_exempt;
                    }

                    let tx_hash = *tx.tx_hash();
                    let ResultAndState { result, state, .. } =
                        match current_evm.transact(tx.clone()) {
                            Ok(r) => r,
                            Err(e) => return Err(Self::Error::from_evm_err(e)),
                        };

                    let (db_ref, inspector_ref, _) = current_evm.components_mut();
                    let mut was_fused = false;
                    let tx_info = TransactionInfo {
                        hash: Some(tx_hash),
                        index: Some(idx),
                        block_hash: Some(block_hash),
                        block_number: Some(block_number),
                        base_fee: Some(base_fee),
                    };
                    idx += 1;

                    let ctx = GravityTracingCtx {
                        tx,
                        result,
                        state: &state,
                        inspector: inspector_ref,
                        db: db_ref,
                        fused_inspector: &fused_inspector,
                        was_fused: &mut was_fused,
                    };
                    let output = f(tx_info, ctx)?;
                    results.push(output);

                    // Match `TxTracer::try_trace_many` default (`skip_last_commit
                    // = true`): commit only when there's a follow-up tx.
                    let has_more = txs_iter.peek().is_some();
                    if has_more {
                        db_ref.commit(state);
                    }

                    // Fuse the inspector for the next tx unless the hook already
                    // took ownership via `take_inspector` (which sets `was_fused`).
                    if !was_fused {
                        let _ = core::mem::replace(
                            current_evm.inspector_mut(),
                            fused_inspector.clone(),
                        );
                    }
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
        Self: LoadBlock + Call,
        // This is the callback that's invoked for each transaction with the inspector, the result,
        // state and db
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, StateCacheDbRefMutWrapper<'_, '_>, TracingInspector>,
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
        Self: LoadBlock + Call,
        // This is the callback that's invoked for each transaction with the inspector, the result,
        // state and db
        F: Fn(
                TransactionInfo,
                GravityTracingCtx<
                    '_,
                    Recovered<&ProviderTx<Self::Provider>>,
                    EvmFor<Self::Evm, StateCacheDbRefMutWrapper<'_, '_>, Insp>,
                >,
            ) -> Result<R, Self::Error>
            + Send
            + 'static,
        Setup: FnMut() -> Insp + Send + 'static,
        Insp: Clone + for<'a, 'b> InspectorFor<Self::Evm, StateCacheDbRefMutWrapper<'a, 'b>>,
        R: Send + 'static,
    {
        self.trace_block_until_with_inspector(block_id, block, None, insp_setup, f)
    }

    /// Applies chain-specific state transitions required before executing a block.
    ///
    /// Note: This should only be called when tracing an entire block vs individual transactions.
    /// When tracing transaction on top of an already committed block state, those transitions are
    /// already applied.
    fn apply_pre_execution_changes<DB: Send + Database + DatabaseCommit>(
        &self,
        block: &RecoveredBlock<ProviderBlock<Self::Provider>>,
        db: &mut DB,
        evm_env: &EvmEnvFor<Self::Evm>,
    ) -> Result<(), Self::Error> {
        let mut system_caller = SystemCaller::new(self.provider().chain_spec());

        // apply relevant system calls
        let mut evm = self.evm_config().evm_with_env(db, evm_env.clone());
        system_caller.apply_pre_execution_changes(block.header(), &mut evm).map_err(|err| {
            EthApiError::EvmCustom(format!("failed to apply 4788 system call {err}"))
        })?;

        Ok(())
    }
}
