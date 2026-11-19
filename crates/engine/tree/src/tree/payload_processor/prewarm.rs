//! Caching and prewarming related functionality.
//!
//! Prewarming executes transactions in parallel before the actual block execution
//! to populate the execution cache with state that will likely be accessed during
//! block processing.
//!
//! ## How Prewarming Works
//!
//! 1. Incoming transactions are split into two streams: one for prewarming (executed in parallel)
//!    and one for actual execution (executed sequentially)
//! 2. Prewarming tasks execute transactions in parallel using shared caches
//! 3. When actual block execution happens, it benefits from the warmed cache

use crate::tree::{
    cached_state::{CachedStateProvider, SavedCache},
    payload_processor::{
        executor::WorkloadExecutor, multiproof::MultiProofMessage,
        ExecutionCache as PayloadExecutionCache,
    },
    precompile_cache::{CachedPrecompile, PrecompileCacheMap},
    ExecutionEnv, StateProviderBuilder,
};
use alloy_consensus::transaction::TxHashRef;
use alloy_evm::Database;
use alloy_primitives::{keccak256, map::B256Set, B256};
use metrics::{Counter, Gauge, Histogram};
use reth_evm::{execute::ExecutableTxFor, ConfigureEvm, Evm, EvmFor, RecoveredTx, SpecFor};
use reth_metrics::Metrics;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{BlockReader, StateProviderFactory, StateReader};
use reth_revm::{database::StateProviderDatabase, db::BundleState, state::EvmState};
use reth_trie::MultiProofTargets;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, channel, Receiver, Sender},
        Arc,
    },
    time::Instant,
};
use tracing::{debug, trace};

/// A task that is responsible for caching and prewarming the cache by executing transactions
/// individually in parallel.
///
/// Note: This task runs until cancelled externally.
pub(super) struct PrewarmCacheTask<N, P, Evm>
where
    N: NodePrimitives,
    Evm: ConfigureEvm<Primitives = N>,
{
    /// The executor used to spawn execution tasks.
    executor: WorkloadExecutor,
    /// Shared execution cache.
    execution_cache: PayloadExecutionCache,
    /// Context provided to execution tasks
    ctx: PrewarmContext<N, P, Evm>,
    /// How many transactions should be executed in parallel
    max_concurrency: usize,
    /// Sender to emit evm state outcome messages, if any.
    to_multi_proof: Option<Sender<MultiProofMessage>>,
    /// Receiver for events produced by tx execution
    actions_rx: Receiver<PrewarmTaskEvent>,
}

impl<N, P, Evm> PrewarmCacheTask<N, P, Evm>
where
    N: NodePrimitives,
    P: BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    Evm: ConfigureEvm<Primitives = N> + 'static,
{
    /// Initializes the task with the given transactions pending execution
    pub(super) fn new(
        executor: WorkloadExecutor,
        execution_cache: PayloadExecutionCache,
        ctx: PrewarmContext<N, P, Evm>,
        to_multi_proof: Option<Sender<MultiProofMessage>>,
    ) -> (Self, Sender<PrewarmTaskEvent>) {
        let (actions_tx, actions_rx) = channel();
        (
            Self {
                executor,
                execution_cache,
                ctx,
                max_concurrency: 64,
                to_multi_proof,
                actions_rx,
            },
            actions_tx,
        )
    }

    /// Spawns all pending transactions as blocking tasks by first chunking them.
    fn spawn_all(
        &self,
        pending: mpsc::Receiver<impl ExecutableTxFor<Evm> + Send + 'static>,
        actions_tx: Sender<PrewarmTaskEvent>,
    ) {
        let executor = self.executor.clone();
        let ctx = self.ctx.clone();
        let max_concurrency = self.max_concurrency;

        self.executor.spawn_blocking(move || {
            let mut handles = Vec::with_capacity(max_concurrency);
            let (done_tx, done_rx) = mpsc::channel();
            let mut executing = 0;
            while let Ok(executable) = pending.recv() {
                let task_idx = executing % max_concurrency;

                if handles.len() <= task_idx {
                    let (tx, rx) = mpsc::channel();
                    let sender = actions_tx.clone();
                    let ctx = ctx.clone();
                    let done_tx = done_tx.clone();

                    executor.spawn_blocking(move || {
                        ctx.transact_batch(rx, sender, done_tx);
                    });

                    handles.push(tx);
                }

                let _ = handles[task_idx].send(executable);

                executing += 1;
            }

            // drop handle and wait for all tasks to finish and drop theirs
            drop(done_tx);
            drop(handles);
            while done_rx.recv().is_ok() {}

            let _ = actions_tx
                .send(PrewarmTaskEvent::FinishedTxExecution { executed_transactions: executing });
        });
    }

    /// If configured and the tx returned proof targets, emit the targets the transaction produced
    fn send_multi_proof_targets(&self, targets: Option<MultiProofTargets>) {
        if let Some((proof_targets, to_multi_proof)) = targets.zip(self.to_multi_proof.as_ref()) {
            let _ = to_multi_proof.send(MultiProofMessage::PrefetchProofs(proof_targets));
        }
    }

    /// This method calls `ExecutionCache::update_with_guard` which requires exclusive access.
    /// It should only be called after ensuring that:
    /// 1. All prewarming tasks have completed execution
    /// 2. No other concurrent operations are accessing the cache
    ///
    /// Saves the warmed caches back into the shared slot after prewarming completes.
    ///
    /// This consumes the `SavedCache` held by the task, which releases its usage guard and allows
    /// the new, warmed cache to be inserted.
    ///
    /// This method is called from `run()` only after all execution tasks are complete.
    fn save_cache(self, state: BundleState) {
        let start = Instant::now();

        let Self { execution_cache, ctx: PrewarmContext { env, metrics, saved_cache, .. }, .. } =
            self;
        let hash = env.hash;

        let (previous, rejected) = execution_cache.update_with_guard(|cached| {
            let reused = cached.as_ref().is_some_and(|previous| previous.shares_cache_with(&saved_cache));
            // Consume the prewarmer's checkout before publishing the new cache.
            let (caches, cache_metrics) = saved_cache.split();
            let new_cache = SavedCache::new(hash, caches, cache_metrics);

            // No reader may observe a partially updated cache.
            if new_cache.cache().insert_state(&state).is_err() {
                debug!(target: "engine::caching", "cleared execution cache on update error");
                return (cached.take(), Some(new_cache));
            }

            new_cache.update_metrics();
            debug!(target: "engine::caching", parent_hash=?new_cache.executed_block_hash(), "Updated execution cache");
            let previous = cached.replace(new_cache);
            if reused {
                // The replacement still owns these allocations. Release the extra checkout
                // before unlocking; this cannot free the shared Moka contents.
                drop(previous);
                (None, None)
            } else {
                (previous, None)
            }
        });
        // Destruction of an obsolete or rejected allocation may be expensive.
        drop((previous, rejected));

        metrics.cache_saving_duration.set(start.elapsed().as_secs_f64());
    }

    /// Executes the task.
    ///
    /// This will execute the transactions until all transactions have been processed or the task
    /// was cancelled.
    pub(super) fn run(
        self,
        pending: mpsc::Receiver<impl ExecutableTxFor<Evm> + Send + 'static>,
        actions_tx: Sender<PrewarmTaskEvent>,
    ) {
        // spawn execution tasks.
        self.spawn_all(pending, actions_tx);

        let mut final_block_output = None;
        let mut finished_execution = false;
        while let Ok(event) = self.actions_rx.recv() {
            match event {
                PrewarmTaskEvent::TerminateTransactionExecution => {
                    // stop tx processing
                    self.ctx.terminate_execution.store(true, Ordering::Relaxed);
                }
                PrewarmTaskEvent::Outcome { proof_targets } => {
                    // completed executing a set of transactions
                    self.send_multi_proof_targets(proof_targets);
                }
                PrewarmTaskEvent::Terminate { block_output } => {
                    trace!(target: "engine::tree::prewarm", "Received termination signal");
                    // A dropped handle may send teardown without first sending a stop event.
                    self.ctx.terminate_execution.store(true, Ordering::Relaxed);
                    final_block_output = Some(block_output);

                    if finished_execution {
                        // all tasks are done, we can exit, which will save caches and exit
                        break
                    }
                }
                PrewarmTaskEvent::FinishedTxExecution { executed_transactions } => {
                    trace!(target: "engine::tree::prewarm", "Finished prewarm execution signal");
                    self.ctx.metrics.transactions.set(executed_transactions as f64);
                    self.ctx.metrics.transactions_histogram.record(executed_transactions as f64);

                    finished_execution = true;

                    if final_block_output.is_some() {
                        // all tasks are done, we can exit, which will save caches and exit
                        break
                    }
                }
            }
        }

        trace!(target: "engine::tree::prewarm", "Completed prewarm execution");

        // save caches and finish
        if let Some(Some(state)) = final_block_output {
            self.save_cache(state);
        }
    }
}

/// Context required by tx execution tasks.
#[derive(Debug, Clone)]
pub(super) struct PrewarmContext<N, P, Evm>
where
    N: NodePrimitives,
    Evm: ConfigureEvm<Primitives = N>,
{
    pub(super) env: ExecutionEnv<Evm>,
    pub(super) evm_config: Evm,
    pub(super) saved_cache: SavedCache,
    /// Provider to obtain the state
    pub(super) provider: StateProviderBuilder<N, P>,
    pub(super) metrics: PrewarmMetrics,
    /// An atomic bool that tells prewarm tasks to not start any more execution.
    pub(super) terminate_execution: Arc<AtomicBool>,
    pub(super) precompile_cache_disabled: bool,
    pub(super) precompile_cache_map: PrecompileCacheMap<SpecFor<Evm>>,
}

impl<N, P, Evm> PrewarmContext<N, P, Evm>
where
    N: NodePrimitives,
    P: BlockReader + StateProviderFactory + StateReader + Clone + 'static,
    Evm: ConfigureEvm<Primitives = N> + 'static,
{
    /// Splits this context into an evm, an evm config, metrics, and the atomic bool for terminating
    /// execution.
    fn evm_for_ctx(self) -> Option<(EvmFor<Evm, impl Database>, PrewarmMetrics, Arc<AtomicBool>)> {
        let Self {
            env,
            evm_config,
            saved_cache,
            provider,
            metrics,
            terminate_execution,
            precompile_cache_disabled,
            precompile_cache_map,
        } = self;

        let state_provider = match provider.build() {
            Ok(provider) => provider,
            Err(err) => {
                trace!(
                    target: "engine::tree",
                    %err,
                    "Failed to build state provider in prewarm thread"
                );
                return None
            }
        };

        // Use the caches to create a new provider with caching
        let caches = saved_cache.cache().clone();
        let cache_metrics = saved_cache.metrics().clone();
        let state_provider =
            CachedStateProvider::new_with_caches(state_provider, caches, cache_metrics);

        let state_provider = StateProviderDatabase::new(state_provider);

        let mut evm_env = env.evm_env;

        // we must disable the nonce check so that we can execute the transaction even if the nonce
        // doesn't match what's on chain.
        evm_env.cfg_env.disable_nonce_check = true;

        // create a new executor and disable nonce checks in the env
        let spec_id = *evm_env.spec_id();
        let mut evm = evm_config.evm_with_env(state_provider, evm_env);

        if !precompile_cache_disabled {
            // Only cache pure precompiles to avoid issues with stateful precompiles
            evm.precompiles_mut().map_cacheable_precompiles(|address, precompile| {
                CachedPrecompile::wrap(
                    precompile,
                    precompile_cache_map.cache_for_address(*address),
                    spec_id,
                    None, // No metrics for prewarm
                )
            });
        }

        Some((evm, metrics, terminate_execution))
    }

    /// Accepts an [`mpsc::Receiver`] of transactions and a handle to prewarm task. Executes
    /// transactions and streams [`PrewarmTaskEvent::Outcome`] messages for each transaction.
    ///
    /// Returns `None` if executing the transactions failed to a non Revert error.
    /// Returns the touched+modified state of the transaction.
    ///
    /// Note: There are no ordering guarantees; this does not reflect the state produced by
    /// sequential execution.
    fn transact_batch(
        self,
        txs: mpsc::Receiver<impl ExecutableTxFor<Evm>>,
        sender: Sender<PrewarmTaskEvent>,
        done_tx: Sender<()>,
    ) {
        let Some((mut evm, metrics, terminate_execution)) = self.evm_for_ctx() else { return };

        while let Ok(tx) = txs.recv() {
            // If the task was cancelled, stop execution, send an empty result to notify the task,
            // and exit.
            if terminate_execution.load(Ordering::Relaxed) {
                let _ = sender.send(PrewarmTaskEvent::Outcome { proof_targets: None });
                break
            }

            // create the tx env
            let start = Instant::now();
            let (tx_env, tx) = tx.into_parts();
            let res = match evm.transact(tx_env) {
                Ok(res) => res,
                Err(err) => {
                    trace!(
                        target: "engine::tree::prewarm",
                        %err,
                        tx_hash=%tx.tx().tx_hash(),
                        sender=%tx.signer(),
                        "Error when executing prewarm transaction",
                    );
                    // Track transaction execution errors
                    metrics.transaction_errors.increment(1);
                    // skip error because we can ignore these errors and continue with the next tx
                    continue
                }
            };
            metrics.execution_duration.record(start.elapsed());

            let (targets, storage_targets) = multiproof_targets_from_state(res.state);
            metrics.prefetch_storage_targets.record(storage_targets as f64);
            metrics.total_runtime.record(start.elapsed());

            let _ = sender.send(PrewarmTaskEvent::Outcome { proof_targets: Some(targets) });
        }

        // send a message to the main task to flag that we're done
        let _ = done_tx.send(());
    }
}

/// Returns a set of [`MultiProofTargets`] and the total amount of storage targets, based on the
/// given state.
fn multiproof_targets_from_state(state: EvmState) -> (MultiProofTargets, usize) {
    let mut targets = MultiProofTargets::with_capacity(state.len());
    let mut storage_targets = 0;
    for (addr, account) in state {
        // if the account was not touched, or if the account was selfdestructed, do not
        // fetch proofs for it
        //
        // Since selfdestruct can only happen in the same transaction, we can skip
        // prefetching proofs for selfdestructed accounts
        //
        // See: https://eips.ethereum.org/EIPS/eip-6780
        if !account.is_touched() || account.is_selfdestructed() {
            continue
        }

        let mut storage_set =
            B256Set::with_capacity_and_hasher(account.storage.len(), Default::default());
        for (key, slot) in account.storage {
            // do nothing if unchanged
            if !slot.is_changed() {
                continue
            }

            storage_set.insert(keccak256(B256::new(key.to_be_bytes())));
        }

        storage_targets += storage_set.len();
        targets.insert(keccak256(addr), storage_set);
    }

    (targets, storage_targets)
}

/// The events the pre-warm task can handle.
pub(super) enum PrewarmTaskEvent {
    /// Stops further worker transaction execution while allowing the task to save its cache.
    TerminateTransactionExecution,
    /// Stops workers, optionally saves the final cache, and exits. This also handles teardown
    /// without a preceding `TerminateTransactionExecution`.
    Terminate {
        /// The final block state output.
        block_output: Option<BundleState>,
    },
    /// The outcome of a pre-warm task
    Outcome {
        /// The prepared proof targets based on the evm state outcome
        proof_targets: Option<MultiProofTargets>,
    },
    /// Finished executing all transactions
    FinishedTxExecution {
        /// Number of transactions executed
        executed_transactions: usize,
    },
}

/// Metrics for transactions prewarming.
#[derive(Metrics, Clone)]
#[metrics(scope = "sync.prewarm")]
pub(crate) struct PrewarmMetrics {
    /// The number of transactions to prewarm
    pub(crate) transactions: Gauge,
    /// A histogram of the number of transactions to prewarm
    pub(crate) transactions_histogram: Histogram,
    /// A histogram of duration per transaction prewarming
    pub(crate) total_runtime: Histogram,
    /// A histogram of EVM execution duration per transaction prewarming
    pub(crate) execution_duration: Histogram,
    /// A histogram for prefetch targets per transaction prewarming
    pub(crate) prefetch_storage_targets: Histogram,
    /// A histogram of duration for cache saving
    pub(crate) cache_saving_duration: Gauge,
    /// Counter for transaction execution errors during prewarming
    pub(crate) transaction_errors: Counter,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{
        cached_state::{CachedStateMetrics, ExecutionCacheBuilder, SlotStatus},
        payload_processor::ExecutionCache,
    };
    use alloy_primitives::{Address, U256};
    use reth_chainspec::ChainSpec;
    use reth_ethereum_primitives::EthPrimitives;
    use reth_evm_ethereum::EthEvmConfig;
    use reth_provider::test_utils::MockEthProvider;
    use std::sync::{atomic::AtomicBool, Weak};

    fn saved_cache(hash: B256) -> SavedCache {
        SavedCache::new(
            hash,
            ExecutionCacheBuilder::default().build_caches(1_000_000),
            CachedStateMetrics::zeroed(),
        )
    }

    fn save(slot: &ExecutionCache, saved: SavedCache, state: BundleState, duration: Gauge) {
        let ctx = PrewarmContext {
            env: ExecutionEnv { hash: B256::repeat_byte(2), ..Default::default() },
            evm_config: EthEvmConfig::new(Arc::new(ChainSpec::default())),
            saved_cache: saved,
            provider: StateProviderBuilder::<EthPrimitives, _>::new(
                MockEthProvider::default(),
                B256::ZERO,
                None,
            ),
            metrics: PrewarmMetrics { cache_saving_duration: duration, ..Default::default() },
            terminate_execution: Arc::new(AtomicBool::new(false)),
            precompile_cache_disabled: true,
            precompile_cache_map: PrecompileCacheMap::default(),
        };
        let (task, _) = PrewarmCacheTask::new(
            WorkloadExecutor::with_num_cpu_threads(1),
            slot.clone(),
            ctx,
            None,
        );
        task.save_cache(state);
    }

    struct SaveObserver {
        cache: ExecutionCache,
        observed: Arc<AtomicBool>,
    }

    impl metrics::GaugeFn for SaveObserver {
        fn increment(&self, _: f64) {}
        fn decrement(&self, _: f64) {}
        fn set(&self, _: f64) {
            let saved = self
                .cache
                .get_cache_for(B256::repeat_byte(2))
                .expect("cache must be reusable before save_cache returns");
            assert_eq!(
                saved.cache().get_storage(&Address::ZERO, &B256::ZERO),
                SlotStatus::Value(U256::from(7))
            );
            self.observed.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn save_cache_releases_checkout_before_duration_metric() {
        let slot = ExecutionCache::default();
        let saved = saved_cache(B256::repeat_byte(1));
        saved.cache().insert_storage(Address::ZERO, B256::ZERO, Some(U256::from(7)));
        slot.update_with_guard(|cached| *cached = Some(saved.clone()));
        let observed = Arc::new(AtomicBool::new(false));
        let observer = Arc::new(SaveObserver { cache: slot.clone(), observed: observed.clone() });
        save(&slot, saved, BundleState::default(), Gauge::from_arc(observer));
        assert!(observed.load(Ordering::Relaxed));
    }

    struct CacheDropProbe {
        slot: Weak<parking_lot::RwLock<Option<SavedCache>>>,
        result: Sender<bool>,
    }

    impl AsRef<[u8]> for CacheDropProbe {
        fn as_ref(&self) -> &[u8] {
            &[0xef, 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        }
    }

    impl Drop for CacheDropProbe {
        fn drop(&mut self) {
            let unlocked = self.slot.upgrade().is_some_and(|slot| slot.try_read().is_some());
            let _ = self.result.send(unlocked);
        }
    }

    fn observe_drop(saved: &SavedCache, slot: &ExecutionCache) -> Receiver<bool> {
        let (tx, rx) = channel();
        let bytes = alloy_primitives::bytes::Bytes::from_owner(CacheDropProbe {
            slot: Arc::downgrade(&slot.inner),
            result: tx,
        });
        let code = revm::bytecode::Bytecode::new_eip7702_raw(bytes.into()).unwrap();
        saved
            .cache()
            .insert_code_for_test(B256::repeat_byte(3), reth_primitives_traits::Bytecode(code));
        rx
    }

    #[test]
    fn save_cache_drops_replaced_allocation_after_unlock() {
        let slot = ExecutionCache::default();
        let previous = saved_cache(B256::repeat_byte(1));
        let dropped = observe_drop(&previous, &slot);
        slot.update_with_guard(|cached| *cached = Some(previous));
        save(&slot, saved_cache(B256::ZERO), BundleState::default(), Gauge::noop());
        assert!(dropped.recv_timeout(std::time::Duration::from_secs(5)).unwrap());
        assert!(slot.get_cache_for(B256::repeat_byte(2)).is_some());
    }

    #[test]
    fn save_cache_drops_rejected_allocations_after_unlock() {
        use reth_revm::db::{AccountStatus, BundleAccount};
        for shared in [false, true] {
            let slot = ExecutionCache::default();
            let previous = saved_cache(B256::repeat_byte(1));
            let previous_drop = observe_drop(&previous, &slot);
            let saved = if shared { previous.clone() } else { saved_cache(B256::ZERO) };
            let rejected_drop = (!shared).then(|| observe_drop(&saved, &slot));
            slot.update_with_guard(|cached| *cached = Some(previous));
            // A modified, non-destroyed account without current info is an invalid cache update.
            let mut state = BundleState::default();
            state.state.insert(
                Address::ZERO,
                BundleAccount {
                    info: None,
                    original_info: None,
                    storage: Default::default(),
                    status: AccountStatus::Changed,
                },
            );
            save(&slot, saved, state, Gauge::noop());
            assert!(previous_drop.recv_timeout(std::time::Duration::from_secs(5)).unwrap());
            if let Some(rejected_drop) = rejected_drop {
                assert!(rejected_drop.recv_timeout(std::time::Duration::from_secs(5)).unwrap());
            }
            assert!(slot.get_cache_for(B256::repeat_byte(2)).is_none());
        }
    }
}
