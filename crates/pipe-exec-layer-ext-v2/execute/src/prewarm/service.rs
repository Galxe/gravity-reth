//! Prewarm service for MPT node prewarming.

use super::metrics::PrewarmMetrics;
use alloy_primitives::Address;
use gravity_primitives::PrewarmConfig;
use grevm::PrewarmTask;
use revm::state::EvmState;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tracing::info;

/// Prewarm statistics result.
struct PrewarmStats {
    pub accounts: usize,
    pub slots: usize,
}

/// Prewarm service that processes prewarm tasks asynchronously.
#[derive(Debug)]
pub struct PrewarmService {
    /// Prewarm task receiver.
    receiver: mpsc::UnboundedReceiver<PrewarmTask>,
    /// Shutdown signal receiver.
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    /// Prewarm configuration.
    config: PrewarmConfig,
    /// Prewarm metrics.
    pub metrics: PrewarmMetrics,
    /// Current block number being processed.
    current_block: Arc<AtomicU64>,
    /// Set of accounts already prewarmed in current block.
    prewarmed_accounts: Arc<RwLock<HashSet<Address>>>,
}

impl PrewarmService {
    /// Creates a new `PrewarmService`.
    pub fn new(
        receiver: mpsc::UnboundedReceiver<PrewarmTask>,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        config: PrewarmConfig,
    ) -> Self {
        Self {
            receiver,
            shutdown_rx,
            config,
            metrics: PrewarmMetrics::default(),
            current_block: Arc::new(AtomicU64::new(0)),
            prewarmed_accounts: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Runs the prewarm service (should be spawned in a tokio task).
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                task = self.receiver.recv() => {
                    match task {
                        Some(task) => self.process_task(task).await,
                        None => break,
                    }
                }
                _ = &mut self.shutdown_rx => {
                    info!(target: "PrewarmService", "Shutdown signal received, exiting");
                    break;
                }
            }
        }
    }

    async fn process_task(&mut self, task: PrewarmTask) {
        // Update current block number metric
        self.metrics.current_block_number.set(task.block_number as f64);

        // Check if task is expired
        let current = self.current_block.load(Ordering::Acquire);
        if task.block_number < current {
            self.metrics.prewarm_skipped_expired_total.increment(1);
            return;
        }

        // Clear cache if new block
        if task.block_number > current {
            self.prewarmed_accounts.write().await.clear();
            self.current_block.store(task.block_number, Ordering::Release);
        }

        // Execute prewarm and record metrics
        let start = Instant::now();
        if let Ok(stats) =
            Self::execute_prewarm(&self.config, &self.prewarmed_accounts, &task.evm_state).await
        {
            self.metrics.prewarm_txs_total.increment(1);
            self.metrics.prewarm_accounts_total.increment(stats.accounts as u64);
            self.metrics.prewarm_slots_total.increment(stats.slots as u64);
            self.metrics.prewarm_duration.record(start.elapsed());
        }
    }

    /// Executes the prewarm operation for the given result and state.
    async fn execute_prewarm(
        config: &PrewarmConfig,
        prewarmed_accounts: &RwLock<HashSet<Address>>,
        evm_state: &EvmState,
    ) -> Result<PrewarmStats, Box<dyn std::error::Error>> {
        let mut prewarmed_accounts_local = HashSet::new();
        let mut prewarmed_slots = 0usize;

        // Iterate over state changes
        for (address, account) in evm_state.iter() {
            // Check if already prewarmed
            {
                let prewarmed = prewarmed_accounts.read().await;
                if prewarmed.contains(address) {
                    continue;
                }
            }

            // Check if only prewarming contracts
            if config.contracts_only && !Self::is_contract(account) {
                continue;
            }

            // Prewarm account
            // Note: The actual MPT prewarm logic would be implemented here
            // For now, we just track what would be prewarmed
            prewarmed_accounts_local.insert(*address);

            // Prewarm storage if threshold is met
            if account.storage.len() >= config.storage_threshold {
                prewarmed_slots += account.storage.len();
                // Note: Storage prewarm would happen here
            }
        }

        // Batch update prewarmed accounts
        let accounts_count = prewarmed_accounts_local.len();
        if !prewarmed_accounts_local.is_empty() {
            let mut prewarmed = prewarmed_accounts.write().await;
            prewarmed.extend(prewarmed_accounts_local);
        }

        Ok(PrewarmStats { accounts: accounts_count, slots: prewarmed_slots })
    }

    fn is_contract(account: &revm::state::Account) -> bool {
        account.info.code_hash != revm_primitives::KECCAK_EMPTY
    }
}
