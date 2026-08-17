//! Parallel EVM executor using Grevm

use crate::RethReceiptBuilder;
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::BlockHeader;
use alloy_eips::{eip4895::Withdrawal, eip7685::Requests};
use alloy_evm::{
    block::{calc, SystemCaller},
    eth::{dao_fork, eip6110, spec::EthExecutorSpec, EthBlockExecutorFactory},
    precompiles::DynPrecompile,
    EvmEnv,
};
use alloy_primitives::{map::HashMap, Address};
use gravity_primitives::get_gravity_config;
use grevm::{
    DynParallelPrecompile, GrevmConfig, ParallelBundleState, ParallelState, Scheduler,
    TxExecutionOutcome,
};
use reth_chainspec::{
    EthChainSpec, EthereumHardfork, EthereumHardforks, GravityHardfork, Hardforks,
};
use reth_ethereum_primitives::{Block, EthPrimitives, Receipt};
use reth_evm::{
    execute::{
        BlockExecutionError, BlockValidationError, ExecuteOutput, InternalBlockExecutionError,
    },
    parallel_execute::ParallelExecutor,
    ConfigureEvm, Evm, ParallelDatabase,
};
use reth_execution_types::BlockExecutionResult;
use reth_primitives_traits::{BlockBody, NodePrimitives, RecoveredBlock, SignedTransaction};
use revm::{
    context::{
        result::{ExecutionResult, HaltReason},
        TxEnv,
    },
    database::{
        states::bundle_state::BundleRetention, BundleState, TransitionState, WrapDatabaseRef,
    },
    state::{AccountInfo, EvmState},
    Database, DatabaseCommit,
};

pub use crate::skipped_transaction::{
    GravityTxSkipReason, GravityTxSkippedEvent, GRAVITY_TX_SKIPPED_LOG_TOPIC0,
    GRAVITY_TX_SKIPPED_LOG_VERSION,
};
pub use reth_chainspec::GRAVITY_TX_SKIPPED_LOG_ADDRESS;

/// EVM executor using Grevm that executes blocks in parallel.
#[derive(Debug)]
pub struct GrevmExecutor<DB, EvmConfig, ChainSpec> {
    /// The chainspec
    chain_spec: Arc<ChainSpec>,
    /// How to create an EVM.
    evm_config: EvmConfig,
    /// Current state for block execution.
    state: Option<ParallelState<DB>>,
    /// System caller for executing system calls.
    system_caller: SystemCaller<Arc<ChainSpec>>,
    /// Capability-restricted custom precompiles available to user transactions.
    custom_precompiles: Option<Arc<Vec<(Address, DynParallelPrecompile)>>>,
    /// Block-scoped grevm execution policy.
    grevm_config: GrevmConfig,
}

impl<DB, EvmConfig, ChainSpec> GrevmExecutor<DB, EvmConfig, ChainSpec>
where
    EvmConfig: Clone
        + ConfigureEvm<
            Primitives = EthPrimitives,
            BlockExecutorFactory = EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>>,
        >,
    DB: ParallelDatabase,
    ChainSpec: EthExecutorSpec + EthChainSpec + Hardforks + 'static,
{
    /// Creates a new [`GrevmExecutor`]
    pub fn new(chain_spec: Arc<ChainSpec>, evm_config: &EvmConfig, db: DB) -> Self {
        Self::new_with_runtime_config(chain_spec, evm_config, db, GrevmConfig::from_env())
    }

    /// Creates a [`GrevmExecutor`] that executes user transactions sequentially.
    pub fn new_sequential(chain_spec: Arc<ChainSpec>, evm_config: &EvmConfig, db: DB) -> Self {
        let mut config = GrevmConfig::from_env();
        config.force_sequential = true;
        Self::new_with_runtime_config(chain_spec, evm_config, db, config)
    }

    /// Creates a [`GrevmExecutor`] with an explicit block execution policy.
    pub fn new_with_runtime_config(
        chain_spec: Arc<ChainSpec>,
        evm_config: &EvmConfig,
        db: DB,
        grevm_config: GrevmConfig,
    ) -> Self {
        let system_caller = SystemCaller::new(chain_spec.clone());
        let report_db_metrics = get_gravity_config().report_db_metrics;
        Self {
            state: Some(ParallelState::new(db, true, report_db_metrics)),
            chain_spec,
            evm_config: evm_config.clone(),
            system_caller,
            custom_precompiles: None,
            grevm_config,
        }
    }

    fn apply_pre_execution_changes(
        &mut self,
        block: &RecoveredBlock<Block>,
    ) -> Result<(), BlockExecutionError> {
        // revm v40+ dropped pre-EIP-161 support: both the DB layer and grevm's
        // `ParallelState` always apply post-Spurious-Dragon commit semantics, so the
        // caller-side `set_state_clear_flag` toggle has been removed.
        let state = self.state.as_mut().unwrap();
        let mut evm =
            self.evm_config.evm_for_block(WrapDatabaseRef(state), block.header()).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(Box::new(e)))
            })?;
        self.system_caller.apply_pre_execution_changes(block.header(), &mut evm)
    }

    fn execute_transactions(
        &mut self,
        block: &RecoveredBlock<Block>,
    ) -> Result<ExecuteOutput<Receipt>, BlockExecutionError> {
        let evm_env = self.evm_config.evm_env(block.header()).map_err(|e| {
            BlockExecutionError::Internal(InternalBlockExecutionError::Other(Box::new(e)))
        })?;

        let mut txs = Vec::with_capacity(block.transaction_count());
        for tx in block.transactions_recovered() {
            txs.push(self.evm_config.tx_env(tx));
        }

        let txs = Arc::new(txs);
        let state = self.state.take().unwrap();

        let (results, state) = {
            let EvmEnv { cfg_env, block_env } = evm_env;
            let executor = Scheduler::new_with_runtime_config(
                cfg_env,
                block_env,
                txs,
                state,
                self.custom_precompiles.clone(),
                self.grevm_config.clone(),
            );
            executor.execute().map_err(|e| {
                // `e.txid` is grevm's per-tx index; for block-level errors it can be a
                // sentinel or out-of-bounds value. Use a saturating lookup so the error
                // path itself cannot panic (closes gravity-audit#696 trigger 4 fallout —
                // a `.unwrap()` here would re-panic on the very `EVMError` that the
                // filter is supposed to keep out, masking the original diagnostics).
                let hash = block
                    .transactions_with_sender()
                    .nth(e.txid)
                    .map(|(_, tx)| tx.recalculate_hash())
                    .unwrap_or_default();
                BlockExecutionError::Internal(InternalBlockExecutionError::EVM {
                    hash,
                    error: Box::new(e.error),
                })
            })?;
            executor.take_result_and_state()
        };

        self.state = Some(state);

        if results.len() != block.transaction_count() {
            return Err(BlockExecutionError::msg(alloc::format!(
                "grevm outcome count mismatch: got {}, expected {}",
                results.len(),
                block.transaction_count()
            )))
        }

        let mut receipts = Vec::with_capacity(results.len());
        let mut cumulative_gas_used = 0;
        for (tx_index, (outcome, transaction)) in
            results.into_iter().zip(block.body().transactions()).enumerate()
        {
            let tx_type = transaction.tx_type();
            match outcome {
                TxExecutionOutcome::Executed(result) => {
                    cumulative_gas_used += result.tx_gas_used();
                    receipts.push(Receipt {
                        tx_type,
                        success: result.is_success(),
                        cumulative_gas_used,
                        logs: result.into_logs(),
                    });
                }
                TxExecutionOutcome::Skipped(error) => {
                    let tx_hash = transaction.recalculate_hash();
                    let reason = GravityTxSkipReason::from_invalid_transaction(&error);
                    tracing::error!(
                        target: "reth::evm::skipped_transaction",
                        block_number = block.number,
                        tx_index,
                        ?tx_hash,
                        ?reason,
                        ?error,
                        "included transaction skipped during final execution",
                    );
                    receipts.push(GravityTxSkippedEvent::receipt(
                        tx_type,
                        cumulative_gas_used,
                        reason,
                    ));
                }
            }
        }
        Ok(ExecuteOutput { receipts, gas_used: cumulative_gas_used })
    }

    fn apply_post_execution_changes(
        &mut self,
        block: &RecoveredBlock<Block>,
        receipts: &[Receipt],
    ) -> Result<Requests, BlockExecutionError> {
        let requests = if self.chain_spec.is_prague_active_at_timestamp(block.timestamp) {
            // Collect all EIP-6110 deposits
            let deposit_requests =
                eip6110::parse_deposits_from_receipts(&self.chain_spec, receipts)?;

            let mut requests = Requests::default();

            if !deposit_requests.is_empty() {
                requests.push_request_with_type(eip6110::DEPOSIT_REQUEST_TYPE, deposit_requests);
            }

            let mut evm = self
                .evm_config
                .evm_for_block(WrapDatabaseRef(self.state.as_mut().unwrap()), block.header())
                .map_err(|e| {
                    BlockExecutionError::Internal(InternalBlockExecutionError::Other(Box::new(e)))
                })?;
            requests.extend(self.system_caller.apply_post_execution_changes(&mut evm)?);
            requests
        } else {
            Requests::default()
        };

        // Standard post-block coinbase + withdrawal increments — identical to the serial
        // (`disable-grevm`) / Ethereum history-sync path, so the two backends stay equivalent.
        // For Gravity this resolves to an empty map (no coinbase change): genesis sets
        // `terminalTotalDifficulty`, so Paris is active from block 0 and `calc::base_block_reward`
        // returns `None` — no PoW reward is minted (the deflationary model funds rewards from gas
        // fees alone) — and Gravity blocks carry no withdrawals.
        // INVARIANT: a Gravity genesis MUST set `terminalTotalDifficulty`; without it Paris is
        // inactive, `base_block_reward` returns `Some(2 ETH)`, and this would inflate the coinbase
        // every block (and fork against the serial path).
        let mut balance_increments = post_block_balance_increments(&self.chain_spec, block);
        let state = self.state.as_mut().unwrap();

        // Irregular state change at Ethereum DAO hardfork
        if self.chain_spec.fork(EthereumHardfork::Dao).transitions_at_block(block.number()) {
            // drain balances from hardcoded addresses.
            let drained_balance: u128 = state
                .drain_balances(dao_fork::DAO_HARDFORK_ACCOUNTS)
                .map_err(|_| BlockValidationError::IncrementBalanceFailed)?
                .into_iter()
                .sum();

            // return balance to DAO beneficiary.
            *balance_increments.entry(dao_fork::DAO_HARDFORK_BENEFICIARY).or_default() +=
                drained_balance;
        }
        // increment balances
        state
            .increment_balances(balance_increments)
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;

        // Gravity hardforks are end-of-block state transitions. The activation
        // block executes against the old runtime and commits the new runtime in
        // its post-state, so the upgraded contracts become observable at H+1.
        if self
            .chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::OracleV1)
            .transitions_at_block(block.number())
        {
            crate::hardfork::oracle_v1::apply_state_changes(self, block.number())?;
        }

        // Note: `SystemCaller::try_on_state_with` (source-aware `OnStateHook` notification)
        // was removed from alloy-evm in 0.36 together with `StateChangeSource` /
        // `StateChangePostBlockSource`. Follow upstream: skip the balance-increment state
        // notification here; the engine-tree payload processor derives the resulting delta
        // from the final bundle instead of relying on an in-executor hook.

        Ok(requests)
    }
}

impl<DB, EvmConfig, ChainSpec> ParallelExecutor for GrevmExecutor<DB, EvmConfig, ChainSpec>
where
    EvmConfig: ConfigureEvm<
        Primitives = EthPrimitives,
        BlockExecutorFactory = EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>>,
    >,
    DB: ParallelDatabase,
    ChainSpec: EthExecutorSpec + EthChainSpec + Hardforks + 'static,
{
    type Error = BlockExecutionError;
    type Primitives = EvmConfig::Primitives;

    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        self.apply_pre_execution_changes(block)?;
        let ExecuteOutput { receipts, gas_used } = if block.transaction_count() == 0 {
            ExecuteOutput { receipts: Vec::new(), gas_used: 0 }
        } else {
            self.execute_transactions(block)?
        };
        let requests = self.apply_post_execution_changes(block, &receipts)?;
        // `blob_gas_used` was added to `BlockExecutionResult` in reth v2.3.0 to track
        // per-block blob gas separately from execution gas. The Grevm parallel path does
        // not track blob gas separately (the sum is derived from receipts by the caller),
        // so we report 0 here and mirror upstream's `EthBlockExecutor::finish` shape.
        Ok(BlockExecutionResult { receipts, gas_used, requests, blob_gas_used: 0 })
    }

    fn take_bundle(&mut self) -> BundleState {
        let state_mut = self.state.as_mut().unwrap();
        if let Some(transition_state) =
            state_mut.transition_state.as_mut().map(TransitionState::take)
        {
            state_mut.bundle_state.parallel_apply_transitions_and_create_reverts(
                transition_state,
                BundleRetention::Reverts,
            );
        }
        state_mut.take_bundle()
    }

    fn size_hint(&self) -> usize {
        self.state.as_ref().unwrap().bundle_size_hint()
    }

    fn transact_system_txn(
        &mut self,
        mut evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
        // Gravity Alpha hardfork: gas-exempt the `SYSTEM_CALLER`-sourced system
        // transactions on the L1 (cfg-side) lever. MUST stay byte-identical with
        // the serial twin in `EthEvmConfig::transact_system_txn` — any drift
        // between serial / grevm here forks state root on system-tx blocks.
        let block_ts: u64 = evm_env.block_env.timestamp.saturating_to();
        if crate::is_system_tx_gas_exempt(self.chain_spec.as_ref(), block_ts) {
            evm_env.cfg_env.disable_base_fee = true;
            evm_env.cfg_env.disable_balance_check = true;
            // `disable_nonce_check` deliberately left `false` — SYSTEM_CALLER's
            // nonce sequence is part of the protocol contract.
        }

        let state = self.state.as_mut().unwrap();
        // Phase 1: execute with WrapDatabaseRef(state).
        let (execution_result, evm_state) = {
            let mut evm = self.evm_config.evm_with_env(&mut *state, evm_env);
            // Inject per-transaction system precompiles (mint, BLS, etc.)
            for (addr, precompile) in precompiles {
                evm.precompiles_mut().apply_precompile(&addr, move |_| Some(precompile));
            }
            let result = evm.transact_raw(tx_env).map_err(|e| {
                BlockExecutionError::msg(alloc::format!("system txn execution failed: {e:?}"))
            })?;
            (result.result, result.state)
        };

        // Phase 2: commit the state changes directly into the executor's ParallelState.
        state.commit(evm_state);
        Ok(execution_result)
    }

    fn apply_state_change(&mut self, state_diff: EvmState) -> Result<(), Self::Error> {
        let state = self.state.as_mut().unwrap();
        // Grevm's `ParallelState::commit` panics with "All accounts should be present
        // inside cache" if a touched address has never been loaded. Irregular state
        // changes (e.g. EIP-2935 HISTORY_STORAGE deployment at the Prague activation
        // block) introduce brand-new accounts that no prior transaction has read.
        // Pre-load each touched address via `basic` so the cache holds at least a
        // `LoadedNotExisting` entry before commit's `get_account_mut` runs.
        for addr in state_diff.keys().copied() {
            state.basic(addr).map_err(|e| {
                BlockExecutionError::msg(alloc::format!("apply_state_change preload {addr}: {e:?}"))
            })?;
        }
        state.commit(state_diff);
        Ok(())
    }

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.state
            .as_mut()
            .unwrap()
            .basic(address)
            .map_err(|e| BlockExecutionError::msg(alloc::format!("basic {address}: {e:?}")))
    }

    fn apply_custom_precompiles(
        &mut self,
        custom_precompiles: Arc<Vec<(Address, DynParallelPrecompile)>>,
    ) {
        self.custom_precompiles = Some(custom_precompiles);
    }
}

/// Standard Ethereum post-block balance increments: `PoW` block + ommer rewards (only pre-Paris,
/// i.e. when `base_block_reward` is `Some`) plus Shanghai withdrawals. Intentionally carries **no**
/// Gravity-specific gating: Gravity zeroes block rewards by having Paris active from genesis (see
/// the call site in `apply_post_execution_changes`), so this naturally returns an empty map for it.
#[inline]
fn post_block_balance_increments<ChainSpec, Block>(
    chain_spec: &ChainSpec,
    block: &RecoveredBlock<Block>,
) -> HashMap<Address, u128>
where
    ChainSpec: EthereumHardforks + EthChainSpec,
    Block: reth_primitives_traits::Block,
{
    let mut balance_increments = HashMap::default();

    // Add block rewards if they are enabled.
    if let Some(base_block_reward) = calc::base_block_reward(chain_spec, block.header().number()) {
        // Ommer rewards
        if let Some(ommers) = block.body().ommers() {
            for ommer in ommers {
                *balance_increments.entry(ommer.beneficiary()).or_default() +=
                    calc::ommer_reward(base_block_reward, block.header().number(), ommer.number());
            }
        }

        // Full block reward
        *balance_increments.entry(block.header().beneficiary()).or_default() += calc::block_reward(
            base_block_reward,
            block.body().ommers().map(|s| s.len()).unwrap_or(0),
        );
    }

    // Gravity ordered blocks cannot carry proposer-controlled withdrawals: the Gravity SDK
    // consensus block has no withdrawals field and every honest node's reth adapter constructs an
    // empty list. A Byzantine node that modifies its local adapter would compute a different block
    // hash, while commit votes bind the execution result, so that result cannot join an honest
    // quorum. Keep the standard withdrawal processing here because this generic executor is also
    // used to replay and validate post-Shanghai Ethereum mainnet history.
    insert_post_block_withdrawals_balance_increments(
        chain_spec,
        block.header().timestamp(),
        block.body().withdrawals().as_ref().map(|w| w.as_slice()),
        &mut balance_increments,
    );

    balance_increments
}

#[inline]
fn insert_post_block_withdrawals_balance_increments(
    spec: impl EthereumHardforks,
    block_timestamp: u64,
    withdrawals: Option<&[Withdrawal]>,
    balance_increments: &mut HashMap<Address, u128>,
) {
    // Process withdrawals
    if spec.is_shanghai_active_at_timestamp(block_timestamp) &&
        let Some(withdrawals) = withdrawals
    {
        for withdrawal in withdrawals {
            if withdrawal.amount > 0 {
                *balance_increments.entry(withdrawal.address).or_default() +=
                    withdrawal.amount_wei().to::<u128>();
            }
        }
    }
}

// `balance_increment_state` was removed alongside the `SystemCaller::try_on_state_with`
// hook call: the source-aware `OnStateHook` mechanism was dropped in alloy-evm 0.36, so
// gravity no longer needs to synthesize an `EvmState` delta for balance increments here.
// The engine-tree payload processor derives the resulting delta from the final bundle
// after `finish` runs, matching upstream's approach.

#[cfg(test)]
mod tests {
    //! Unit tests for the `apply_state_change` trait method on both
    //! `WrapExecutor<BasicBlockExecutor>` (revm backend) and `GrevmExecutor`
    //! (grevm backend). These pin the contract that pipe-layer EIP-2935
    //! deployment relies on:
    //!
    //! - U-1 / U-2: a first-touch HISTORY_STORAGE deployment diff lands in the bundle with
    //!   `nonce=1, balance=0, code_hash=keccak(HISTORY_STORAGE_CODE)`, no storage prefill, with
    //!   identical bundle contents across both impls (this is the unit-level proof of
    //!   `disable_grevm` equivalence — far cheaper than e2e state-root comparisons).
    //! - U-3: after `apply_state_change`, a subsequent `execute(&block)` runs the EIP-2935 system
    //!   call against the just-deployed code and writes slot `(N-1) % 8191` == `parent_hash`. Pins
    //!   the F9 regression boundary (pre-load-then-commit timing).
    //! - U-4: empty diff is a no-op, does not panic.
    //! - U-5: repeated `apply_state_change` accumulates (revm `state.commit` semantics) rather than
    //!   replacing.

    use super::*;
    use crate::{
        hardfork::oracle_v1::{
            NATIVE_ORACLE_ADDRESS, NATIVE_ORACLE_POST_FORK_CODE_HASH,
            NATIVE_ORACLE_PRE_FORK_CODE_HASH, ORACLE_TASK_CONFIG_ADDRESS,
            ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH,
        },
        is_system_tx_gas_exempt, EthEvmConfig, SYSTEM_CALLER,
    };
    use alloc::sync::Arc;
    use alloy_consensus::{constants::KECCAK_EMPTY, Header};
    use alloy_eips::{
        eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE},
        eip7685::EMPTY_REQUESTS_HASH,
    };
    use alloy_primitives::{address, keccak256, Bytes, B256, U256};
    use alloy_sol_types::{sol, SolCall};
    use core::sync::atomic::{AtomicUsize, Ordering};
    use reth_chainspec::{
        ChainHardforks, ChainSpec, ChainSpecBuilder, ForkCondition, GravityHardfork, MAINNET,
    };
    use reth_ethereum_primitives::Block;
    use reth_evm::{execute::BasicBlockExecutor, parallel_execute::WrapExecutor};
    use reth_primitives_traits::RecoveredBlock;
    use revm::{
        bytecode::Bytecode,
        context::TxEnv,
        database::{CacheDB, EmptyDB},
        precompile::{PrecompileId, PrecompileOutput},
        primitives::TxKind,
        state::{Account, AccountInfo, AccountStatus},
    };

    fn prague_chainspec() -> Arc<ChainSpec> {
        Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .shanghai_activated()
                .cancun_activated()
                .prague_activated()
                .build(),
        )
    }

    /// Prague-activated chainspec with Gravity Alpha at the given timestamp.
    ///
    /// Used by U-6 to flip the `is_system_tx_gas_exempt` gate on/off via the same
    /// `ChainHardforks` channel that production chainspecs use, so the test exercises
    /// the actual predicate rather than monkey-patching `cfg_env` directly.
    fn alpha_active_chainspec(alpha_time: u64) -> Arc<ChainSpec> {
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks =
            ChainHardforks::from([(GravityHardfork::Alpha, ForkCondition::Timestamp(alpha_time))]);
        Arc::new(spec)
    }

    const ORACLE_V1_ACTIVATION_BLOCK: u64 = 100;
    const ORACLE_SOURCE_TYPE_BLOCKCHAIN: u32 = 0;
    const ORACLE_SOURCE_ID: u64 = 1;
    const LEGACY_SOURCE_POSITION: u128 = 19_876_543;
    const POST_FORK_SOURCE_POSITION: u128 = LEGACY_SOURCE_POSITION + 1;
    const NATIVE_ORACLE_PRE_FORK_RUNTIME_HEX: &str =
        include_str!("hardfork/bytecodes/oracle_v1/NativeOracle.pre.hex");
    const ALWAYS_TRUE_CALLBACK: Address = address!("000000000000000000000000000000000000c0de");
    const ALWAYS_TRUE_CALLBACK_RUNTIME: &[u8] =
        &[0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];

    sol! {
        function recordBatch(
            uint32 sourceType,
            uint256 sourceId,
            uint128[] nonces,
            uint256[] blockNumbers,
            bytes[] payloads,
            uint256[] callbackGasLimits
        ) external;
    }

    fn oracle_v1_chainspec() -> Arc<ChainSpec> {
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        // OracleV1 activates after Alpha in production, so zero-balance system
        // transactions at the boundary use the already-active gas exemption.
        spec.gravity_hardforks = ChainHardforks::from([
            (GravityHardfork::OracleV1, ForkCondition::Block(ORACLE_V1_ACTIVATION_BLOCK)),
            (GravityHardfork::Alpha, ForkCondition::Timestamp(0)),
        ]);
        Arc::new(spec)
    }

    fn oracle_v1_seeded_db() -> CacheDB<EmptyDB> {
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(
            NATIVE_ORACLE_ADDRESS,
            AccountInfo {
                balance: U256::from(11),
                nonce: 7,
                code_hash: NATIVE_ORACLE_PRE_FORK_CODE_HASH,
                ..Default::default()
            },
        );
        db.insert_account_info(
            ORACLE_TASK_CONFIG_ADDRESS,
            AccountInfo {
                balance: U256::from(22),
                nonce: 9,
                code_hash: ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH,
                ..Default::default()
            },
        );
        db
    }

    fn solidity_mapping_slot(key: U256, slot: U256) -> U256 {
        let mut preimage = [0u8; 64];
        preimage[..32].copy_from_slice(&key.to_be_bytes::<32>());
        preimage[32..].copy_from_slice(&slot.to_be_bytes::<32>());
        U256::from_be_bytes(keccak256(preimage).0)
    }

    fn solidity_nested_mapping_slot(first_key: U256, second_key: U256, slot: u64) -> U256 {
        let outer = solidity_mapping_slot(first_key, U256::from(slot));
        solidity_mapping_slot(second_key, outer)
    }

    fn legacy_nonce_slot() -> U256 {
        solidity_nested_mapping_slot(
            U256::from(ORACLE_SOURCE_TYPE_BLOCKCHAIN),
            U256::from(ORACLE_SOURCE_ID),
            1,
        )
    }

    fn legacy_record_block_number_slot(nonce: u128) -> U256 {
        let source_records = solidity_nested_mapping_slot(
            U256::from(ORACLE_SOURCE_TYPE_BLOCKCHAIN),
            U256::from(ORACLE_SOURCE_ID),
            0,
        );
        solidity_mapping_slot(U256::from(nonce), source_records) + U256::from(1)
    }

    fn source_progress_slot() -> U256 {
        solidity_nested_mapping_slot(
            U256::from(ORACLE_SOURCE_TYPE_BLOCKCHAIN),
            U256::from(ORACLE_SOURCE_ID),
            5,
        )
    }

    fn oracle_v1_legacy_handoff_db() -> CacheDB<EmptyDB> {
        let mut db = CacheDB::new(EmptyDB::default());
        let pre_fork_runtime =
            alloy_primitives::hex::decode(NATIVE_ORACLE_PRE_FORK_RUNTIME_HEX.trim())
                .expect("pre-fork NativeOracle runtime fixture must be valid hex");
        assert_eq!(keccak256(&pre_fork_runtime), NATIVE_ORACLE_PRE_FORK_CODE_HASH);

        db.insert_account_info(
            NATIVE_ORACLE_ADDRESS,
            AccountInfo {
                code_hash: NATIVE_ORACLE_PRE_FORK_CODE_HASH,
                code: Some(Bytecode::new_raw(Bytes::from(pre_fork_runtime))),
                ..Default::default()
            },
        );
        db.insert_account_info(
            ORACLE_TASK_CONFIG_ADDRESS,
            AccountInfo { code_hash: ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH, ..Default::default() },
        );
        db.insert_account_info(
            SYSTEM_CALLER,
            AccountInfo { balance: U256::ZERO, code_hash: KECCAK_EMPTY, ..Default::default() },
        );

        let callback_code = Bytes::from_static(ALWAYS_TRUE_CALLBACK_RUNTIME);
        let callback_code_hash = keccak256(&callback_code);
        db.insert_account_info(
            ALWAYS_TRUE_CALLBACK,
            AccountInfo {
                code_hash: callback_code_hash,
                code: Some(Bytecode::new_raw(callback_code)),
                ..Default::default()
            },
        );

        let default_callback_slot =
            solidity_mapping_slot(U256::from(ORACLE_SOURCE_TYPE_BLOCKCHAIN), U256::from(2));
        db.insert_account_storage(
            NATIVE_ORACLE_ADDRESS,
            default_callback_slot,
            U256::from_be_slice(ALWAYS_TRUE_CALLBACK.as_slice()),
        )
        .expect("callback storage fixture must be writable");
        db
    }

    fn oracle_record_batch_tx_env(
        system_nonce: u64,
        oracle_nonce: u128,
        source_position: u128,
        chain_id: u64,
    ) -> TxEnv {
        let call = recordBatchCall {
            sourceType: ORACLE_SOURCE_TYPE_BLOCKCHAIN,
            sourceId: U256::from(ORACLE_SOURCE_ID),
            nonces: vec![oracle_nonce],
            blockNumbers: vec![U256::from(source_position)],
            payloads: vec![Bytes::from_static(b"oracle-v1-boundary")],
            callbackGasLimits: vec![U256::from(100_000)],
        };
        TxEnv {
            caller: SYSTEM_CALLER,
            gas_limit: 2_000_000,
            gas_price: 0,
            kind: TxKind::Call(NATIVE_ORACLE_ADDRESS),
            value: U256::ZERO,
            data: call.abi_encode().into(),
            nonce: system_nonce,
            chain_id: Some(chain_id),
            ..TxEnv::default()
        }
    }

    /// Mirrors the alloc shape that `eip_2935::apply_state_changes_for_block`
    /// produces via `deploy_contract` in the pipe layer: nonce=1, balance=0,
    /// code = HISTORY_STORAGE_CODE, no storage prefill, `Created | Touched`.
    fn build_history_storage_deployment_diff() -> EvmState {
        let code = HISTORY_STORAGE_CODE.clone();
        let code_hash = keccak256(code.as_ref());
        let info = AccountInfo {
            nonce: 1,
            balance: U256::ZERO,
            code_hash,
            code: Some(Bytecode::new_raw(code)),
            ..Default::default()
        };
        let mut state_diff = EvmState::default();
        let mut account = Account::default();
        account.info = info;
        account.status = AccountStatus::Created | AccountStatus::Touched;
        state_diff.insert(HISTORY_STORAGE_ADDRESS, account);
        state_diff
    }

    fn prague_block(number: u64, parent_hash: B256) -> RecoveredBlock<Block> {
        let header = Header {
            parent_hash,
            timestamp: 1,
            number,
            gas_limit: 30_000_000,
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Header::default()
        };
        RecoveredBlock::new_unhashed(Block { header, body: Default::default() }, vec![])
    }

    fn execute_oracle_v1_block(number: u64, force_sequential: bool) -> BundleState {
        let chain_spec = oracle_v1_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let db = oracle_v1_seeded_db();
        let mut executor = if force_sequential {
            GrevmExecutor::new_sequential(chain_spec, &evm_config, db)
        } else {
            GrevmExecutor::new(chain_spec, &evm_config, db)
        };
        executor
            .execute(&prague_block(number, B256::from([0xA5; 32])))
            .expect("OracleV1 boundary block execution must succeed")
            .state
    }

    #[test]
    fn oracle_v1_is_a_grevm_post_execution_transition() {
        for block_number in [ORACLE_V1_ACTIVATION_BLOCK - 1, ORACLE_V1_ACTIVATION_BLOCK + 1] {
            let bundle = execute_oracle_v1_block(block_number, false);
            assert!(
                !bundle.state.contains_key(&NATIVE_ORACLE_ADDRESS) &&
                    !bundle.state.contains_key(&ORACLE_TASK_CONFIG_ADDRESS),
                "OracleV1 must not change oracle accounts outside its transition block"
            );
        }

        let parallel = execute_oracle_v1_block(ORACLE_V1_ACTIVATION_BLOCK, false);
        let sequential = execute_oracle_v1_block(ORACLE_V1_ACTIVATION_BLOCK, true);

        assert_eq!(parallel.state, sequential.state, "parallel/sequential state drift");
        assert_eq!(parallel.contracts, sequential.contracts, "parallel/sequential code drift");
        assert_eq!(parallel.reverts, sequential.reverts, "parallel/sequential revert drift");

        let native = parallel
            .state
            .get(&NATIVE_ORACLE_ADDRESS)
            .and_then(|account| account.info.as_ref())
            .expect("NativeOracle must be upgraded in the activation block post-state");
        assert_eq!(native.code_hash, NATIVE_ORACLE_POST_FORK_CODE_HASH);
        assert_eq!(native.balance, U256::from(11));
        assert_eq!(native.nonce, 7);

        let task_config = parallel
            .state
            .get(&ORACLE_TASK_CONFIG_ADDRESS)
            .and_then(|account| account.info.as_ref())
            .expect("OracleTaskConfig must be upgraded in the activation block post-state");
        assert_eq!(task_config.code_hash, ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH);
        assert_eq!(task_config.balance, U256::from(22));
        assert_eq!(task_config.nonce, 9);

        assert!(parallel.contracts.contains_key(&NATIVE_ORACLE_POST_FORK_CODE_HASH));
        assert!(parallel.contracts.contains_key(&ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH));
    }

    fn execute_oracle_v1_legacy_handoff(force_sequential: bool) -> (BundleState, BundleState) {
        let chain_spec = oracle_v1_chainspec();
        let chain_id = chain_spec.chain().id();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let mut executor = if force_sequential {
            GrevmExecutor::new_sequential(chain_spec, &evm_config, oracle_v1_legacy_handoff_db())
        } else {
            GrevmExecutor::new(chain_spec, &evm_config, oracle_v1_legacy_handoff_db())
        };

        let activation_block = prague_block(ORACLE_V1_ACTIVATION_BLOCK, B256::from([0xB0; 32]));
        let activation_env = evm_config
            .evm_env(activation_block.header())
            .expect("activation EVM environment must build");
        let legacy_result = executor
            .transact_system_txn(
                activation_env,
                Vec::new(),
                oracle_record_batch_tx_env(0, 1, LEGACY_SOURCE_POSITION, chain_id),
            )
            .expect("legacy Oracle system transaction must execute");
        assert!(legacy_result.is_success(), "legacy Oracle system transaction reverted");

        executor
            .execute_one(&activation_block)
            .expect("activation block post-execution migration must succeed");
        let activation_bundle = executor.take_bundle();

        let post_fork_block = prague_block(ORACLE_V1_ACTIVATION_BLOCK + 1, B256::from([0xB1; 32]));
        let post_fork_env = evm_config
            .evm_env(post_fork_block.header())
            .expect("post-fork EVM environment must build");
        let post_fork_result = executor
            .transact_system_txn(
                post_fork_env,
                Vec::new(),
                oracle_record_batch_tx_env(1, 2, POST_FORK_SOURCE_POSITION, chain_id),
            )
            .expect("post-fork Oracle system transaction must execute");
        assert!(
            post_fork_result.is_success(),
            "post-fork Oracle transaction must resume from the legacy nonce"
        );
        executor.execute_one(&post_fork_block).expect("H+1 execution must succeed");
        (activation_bundle, executor.take_bundle())
    }

    #[test]
    fn oracle_v1_preserves_legacy_oracle_tx_and_resumes_at_h_plus_one() {
        let (parallel_h, parallel_h_plus_one) = execute_oracle_v1_legacy_handoff(false);
        let (sequential_h, sequential_h_plus_one) = execute_oracle_v1_legacy_handoff(true);

        assert_eq!(parallel_h.state, sequential_h.state, "H parallel/sequential state drift");
        assert_eq!(
            parallel_h.contracts, sequential_h.contracts,
            "H parallel/sequential code drift"
        );
        assert_eq!(parallel_h.reverts, sequential_h.reverts, "H parallel/sequential revert drift");
        assert_eq!(
            parallel_h_plus_one.state, sequential_h_plus_one.state,
            "H+1 parallel/sequential state drift"
        );
        assert_eq!(
            parallel_h_plus_one.contracts, sequential_h_plus_one.contracts,
            "H+1 parallel/sequential code drift"
        );
        assert_eq!(
            parallel_h_plus_one.reverts, sequential_h_plus_one.reverts,
            "H+1 parallel/sequential revert drift"
        );

        let native_at_h = parallel_h
            .state
            .get(&NATIVE_ORACLE_ADDRESS)
            .expect("NativeOracle must be present in the H bundle");
        let native_info = native_at_h.info.as_ref().expect("NativeOracle must retain account info");
        assert_eq!(native_info.code_hash, NATIVE_ORACLE_POST_FORK_CODE_HASH);

        assert_eq!(
            native_at_h
                .storage
                .get(&legacy_nonce_slot())
                .expect("H legacy nonce write must survive migration")
                .present_value,
            U256::from(1)
        );
        assert_eq!(
            native_at_h
                .storage
                .get(&legacy_record_block_number_slot(1))
                .expect("H legacy source position must survive migration")
                .present_value,
            U256::from(LEGACY_SOURCE_POSITION)
        );

        let expected_progress = (U256::from(POST_FORK_SOURCE_POSITION) << 128) | U256::from(2);
        let native_at_h_plus_one = parallel_h_plus_one
            .state
            .get(&NATIVE_ORACLE_ADDRESS)
            .expect("NativeOracle must be present in the H+1 bundle");
        assert_eq!(
            native_at_h_plus_one
                .storage
                .get(&source_progress_slot())
                .expect("H+1 must write the post-fork source progress slot")
                .present_value,
            expected_progress
        );
    }

    // --- U-1: WrapExecutor (revm path) -----------------------------------

    #[test]
    fn u1_wrap_executor_apply_state_change_injects_history_storage() {
        let chain_spec = prague_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec);
        let db = CacheDB::new(EmptyDB::default());
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));

        executor
            .apply_state_change(build_history_storage_deployment_diff())
            .expect("apply_state_change must succeed for HISTORY_STORAGE deployment diff");

        let bundle = executor.take_bundle();
        let acc = bundle
            .state
            .get(&HISTORY_STORAGE_ADDRESS)
            .expect("HISTORY_STORAGE_ADDRESS must be present in bundle after apply_state_change");
        let info =
            acc.info.as_ref().expect("HISTORY_STORAGE bundle account must carry account info");

        let code_hash = keccak256(HISTORY_STORAGE_CODE.as_ref());
        assert_eq!(info.nonce, 1, "deployed nonce must be 1 (mainnet alloc shape)");
        assert_eq!(info.balance, U256::ZERO, "deployed balance must be 0");
        assert_eq!(info.code_hash, code_hash, "code hash must match HISTORY_STORAGE_CODE");
        assert!(
            bundle.contracts.contains_key(&code_hash),
            "bundle.contracts must include HISTORY_STORAGE bytecode"
        );
        assert!(acc.storage.is_empty(), "EIP-2935 storage must not be prefilled");
    }

    // --- U-2: GrevmExecutor (grevm path) — bundle byte-equal to U-1 -----

    #[test]
    fn u2_grevm_executor_apply_state_change_matches_wrap_executor() {
        let chain_spec = prague_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let db = EmptyDB::default();
        let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

        executor
            .apply_state_change(build_history_storage_deployment_diff())
            .expect("apply_state_change must succeed on the grevm path");

        let bundle = executor.take_bundle();
        let acc = bundle
            .state
            .get(&HISTORY_STORAGE_ADDRESS)
            .expect("HISTORY_STORAGE_ADDRESS must be present in grevm bundle");
        let info = acc.info.as_ref().expect("grevm bundle account info must be present");

        let code_hash = keccak256(HISTORY_STORAGE_CODE.as_ref());
        assert_eq!(info.nonce, 1, "grevm path must produce identical nonce to revm path");
        assert_eq!(info.balance, U256::ZERO, "grevm path must produce identical balance");
        assert_eq!(info.code_hash, code_hash, "grevm path must produce identical code_hash");
        assert!(
            bundle.contracts.contains_key(&code_hash),
            "grevm bundle.contracts must include HISTORY_STORAGE bytecode"
        );
        assert!(acc.storage.is_empty(), "grevm storage must not be prefilled either");
    }

    // --- U-3: deployment ↔ pre-execution system call timing -------------

    #[test]
    fn u3_grevm_apply_state_change_visible_to_system_call() {
        let chain_spec = prague_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let db = EmptyDB::default();
        let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

        executor.apply_state_change(build_history_storage_deployment_diff()).unwrap();

        // Construct a Prague-compliant block at number 100. The pre-execution
        // system call hits HISTORY_STORAGE with calldata = parent_hash and
        // writes slot (number - 1) % HISTORY_SERVE_WINDOW = 99.
        let parent_hash = B256::from([0xA9; 32]);
        let block = prague_block(100, parent_hash);

        // `execute` internally takes the bundle and returns it via output.state,
        // so we must read the deployment + system-call effects from there.
        let output = executor.execute(&block).expect("post-deploy execute must succeed");
        let bundle = output.state;

        let acc = bundle
            .state
            .get(&HISTORY_STORAGE_ADDRESS)
            .expect("HISTORY_STORAGE must be in bundle output after execute");
        let slot_99 = acc
            .storage
            .get(&U256::from(99u64))
            .expect("slot 99 must be written by the EIP-2935 system call");
        assert_eq!(
            slot_99.present_value,
            U256::from_be_bytes(parent_hash.0),
            "slot 99 must hold the block's parent_hash after pre-execution system call"
        );
    }

    // --- U-4: empty diff is a no-op ---------------------------------------

    #[test]
    fn u4_wrap_executor_apply_state_change_empty_diff_is_noop() {
        let chain_spec = prague_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec);
        let db = CacheDB::new(EmptyDB::default());
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));

        executor.apply_state_change(EvmState::default()).expect("empty diff must not error");

        let bundle = executor.take_bundle();
        assert!(
            bundle.state.is_empty(),
            "empty diff must leave bundle empty (no spurious account injection)"
        );
        assert!(bundle.contracts.is_empty(), "empty diff must leave bundle.contracts empty");
    }

    // --- U-5: repeated apply_state_change accumulates -----------------------

    #[test]
    fn u5_grevm_apply_state_change_accumulates_across_calls() {
        let chain_spec = prague_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let db = EmptyDB::default();
        let mut executor = GrevmExecutor::new(chain_spec, &evm_config, db);

        // First call deploys HISTORY_STORAGE with nonce=1, balance=0, code set.
        executor.apply_state_change(build_history_storage_deployment_diff()).unwrap();

        // Second call mutates only nonce + balance on the same address — no
        // `code` field. revm's `state.commit` semantics preserve previously
        // committed code if the new diff doesn't supply one.
        let code_hash = keccak256(HISTORY_STORAGE_CODE.as_ref());
        let bumped_info = AccountInfo {
            nonce: 2,
            balance: U256::from(100u64),
            code_hash,
            code: None,
            ..Default::default()
        };
        let mut second_diff = EvmState::default();
        let mut bumped_account = Account::default();
        bumped_account.info = bumped_info;
        bumped_account.status = AccountStatus::Touched;
        second_diff.insert(HISTORY_STORAGE_ADDRESS, bumped_account);
        executor.apply_state_change(second_diff).unwrap();

        let bundle = executor.take_bundle();
        let acc = bundle
            .state
            .get(&HISTORY_STORAGE_ADDRESS)
            .expect("HISTORY_STORAGE_ADDRESS must still be present after second commit");
        let info = acc.info.as_ref().expect("info present");
        assert_eq!(info.nonce, 2, "nonce must reflect the second diff (cumulative commit)");
        assert_eq!(info.balance, U256::from(100u64), "balance must reflect the second diff");
        assert_eq!(
            info.code_hash, code_hash,
            "code_hash must still match HISTORY_STORAGE bytecode after second commit"
        );
    }

    // ====================================================================
    // Gravity Alpha system-tx gas-exempt unit tests (acceptance §1.1)
    // ====================================================================
    //
    // These pin the "承重墙" (load-bearing wall) invariant from
    // `_local/drafts/system-tx-gas-exempt/acceptance-tests-2026-06-26.md` §1.1:
    // when the Alpha gate is active, the serial (`WrapExecutor` /
    // `EthEvmConfig::transact_system_txn`) and grevm
    // (`GrevmExecutor::transact_system_txn`) backends MUST produce
    // byte-identical bundles for the same input system transactions. Any
    // drift here forks state root on system-tx blocks.

    /// Build a synthetic `EvmEnv` rooted at `timestamp` against a
    /// Prague-active chainspec, with a non-zero `gas_limit` and a basefee
    /// large enough that a system tx with `gas_price = 0` would normally be
    /// rejected (i.e. enforces that the Alpha gate is doing its job).
    fn alpha_block_header(timestamp: u64) -> Header {
        Header {
            parent_hash: B256::ZERO,
            timestamp,
            number: 1,
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1_000_000_000),
            ..Header::default()
        }
    }

    /// Build a metadata-shaped system tx (`gas_price = 0`, `value = 0`,
    /// `SYSTEM_CALLER` sender, intrinsic-only payload).
    fn system_tx_env(nonce: u64, chain_id: u64) -> TxEnv {
        TxEnv {
            caller: SYSTEM_CALLER,
            gas_limit: 1_000_000,
            gas_price: 0,
            kind: TxKind::Call(SYSTEM_CALLER),
            value: U256::ZERO,
            data: Bytes::new(),
            nonce,
            chain_id: Some(chain_id),
            ..TxEnv::default()
        }
    }

    /// Pre-seed a `CacheDB<EmptyDB>` with a `SYSTEM_CALLER` account, mirroring
    /// the post-Alpha-migration state (balance=0, nonce=N, code=empty). Both
    /// backends start from byte-identical DBs so any divergence in
    /// `take_bundle()` comes from the `transact_system_txn` path itself,
    /// which is what U-6 is designed to detect.
    fn seeded_db(balance: U256, nonce: u64) -> CacheDB<EmptyDB> {
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(
            SYSTEM_CALLER,
            AccountInfo { balance, nonce, code_hash: KECCAK_EMPTY, code: None, account_id: None },
        );
        db
    }

    // --- U-6: serial (`WrapExecutor`) == grevm (`GrevmExecutor`) bundle ---

    /// Sketch §1.1 "承重墙": run the same metadata + validator system tx
    /// sequence through both backends with the Alpha gate active. Assert the
    /// resulting `BundleState` is byte-equal between serial and grevm. Any
    /// drift = state-root fork on system-tx blocks (PR #363-class regression).
    #[test]
    fn u6_test_system_tx_gas_exempt_bundle_equivalence() {
        let chain_spec = alpha_active_chainspec(1);
        let chain_id = chain_spec.chain().id();
        assert!(
            is_system_tx_gas_exempt(chain_spec.as_ref(), 1),
            "test fixture sanity: Alpha must be active at ts=1"
        );

        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let header = alpha_block_header(1);
        let evm_env = evm_config.evm_env(&header).expect("evm_env must build");

        // metadata + validator system txs, sender = SYSTEM_CALLER, gas_price=0
        let tx_m = system_tx_env(0, chain_id);
        let tx_v = system_tx_env(1, chain_id);

        // Serial path — WrapExecutor wraps the same EthEvmConfig
        // `transact_system_txn` impl that the pipe-exec-layer takes when
        // `--gravity.disable-grevm` is set.
        let mut serial = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config.clone(),
            seeded_db(U256::ZERO, 0),
        ));
        serial
            .transact_system_txn(evm_env.clone(), Vec::new(), tx_m.clone())
            .expect("serial metadata tx must succeed under Alpha gate");
        serial
            .transact_system_txn(evm_env.clone(), Vec::new(), tx_v.clone())
            .expect("serial validator tx must succeed under Alpha gate");
        let bundle_serial = serial.take_bundle();

        // Grevm path — `GrevmExecutor::transact_system_txn`, the twin impl
        // that MUST stay byte-identical with the serial path.
        let mut grevm =
            GrevmExecutor::new(chain_spec.clone(), &evm_config, seeded_db(U256::ZERO, 0));
        grevm
            .transact_system_txn(evm_env.clone(), Vec::new(), tx_m)
            .expect("grevm metadata tx must succeed under Alpha gate");
        grevm
            .transact_system_txn(evm_env, Vec::new(), tx_v)
            .expect("grevm validator tx must succeed under Alpha gate");
        let bundle_grevm = grevm.take_bundle();

        // Assert load-bearing equality field by field. The matrix says "byte
        // equal" — that's true for everything that affects state root:
        //   - `state`: address → BundleAccount (info, original_info, storage, status)
        //   - `contracts`: code_hash → Bytecode (changes to code map)
        //   - `state_size`: memory size hint (matches between backends)
        //   - `reverts`: revert content (block-level rollback ability)
        //
        // We deliberately skip `reverts_size` comparison: grevm's
        // `parallel_apply_transitions_and_create_reverts` updates
        // `state_size` but does NOT update `reverts_size` (see
        // `grevm/src/storage.rs::parallel_apply_transitions_and_create_reverts`),
        // while revm's serial `apply_transitions_and_create_reverts` does.
        // The discrepancy is purely a memory-accounting field; the actual
        // `reverts` content (the load-bearing data) is identical, so state
        // root is unaffected. Flagged for follow-up grevm fix; not a
        // consensus issue today.
        assert_eq!(bundle_serial.state, bundle_grevm.state, "state map drift");
        assert_eq!(bundle_serial.contracts, bundle_grevm.contracts, "contracts drift");
        assert_eq!(bundle_serial.state_size, bundle_grevm.state_size, "state_size drift");
        assert_eq!(bundle_serial.reverts, bundle_grevm.reverts, "reverts content drift");

        // Sanity: the SYSTEM_CALLER account is in the bundle, balance still
        // zero (no fee paid), and nonce bumped by 2 (one per system tx) on
        // both backends.
        let serial_acc = bundle_serial
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be present after two system txs");
        let info = serial_acc.info.as_ref().expect("SYSTEM_CALLER info present");
        assert_eq!(info.balance, U256::ZERO, "SYSTEM_CALLER balance must stay zero (gas-exempt)");
        assert_eq!(info.nonce, 2, "SYSTEM_CALLER nonce must reflect both system txs");
    }

    /// Executor-level custom precompiles are scoped to Grevm's user-transaction scheduler.
    /// System transactions only see the Alloy precompiles explicitly supplied for that call.
    #[test]
    fn executor_custom_precompile_does_not_leak_into_system_transactions() {
        let chain_spec = alpha_active_chainspec(1);
        let chain_id = chain_spec.chain().id();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let evm_env = evm_config
            .evm_env(&alpha_block_header(1))
            .expect("system transaction EVM environment must build");
        let precompile_address = Address::with_last_byte(0xfe);

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_precompile = calls.clone();
        let custom_precompile = DynParallelPrecompile::new(
            PrecompileId::custom("system-precompile-isolation-sentinel"),
            move |input| {
                calls_in_precompile.fetch_add(1, Ordering::SeqCst);
                Ok(PrecompileOutput::new(0, Bytes::new(), input.reservoir()))
            },
        );

        let mut executor = GrevmExecutor::new(chain_spec, &evm_config, seeded_db(U256::ZERO, 0));
        executor.apply_custom_precompiles(Arc::new(vec![(
            precompile_address,
            custom_precompile.clone(),
        )]));

        let mut implicit_tx = system_tx_env(0, chain_id);
        implicit_tx.kind = TxKind::Call(precompile_address);
        executor
            .transact_system_txn(evm_env.clone(), Vec::new(), implicit_tx)
            .expect("system transaction without explicit precompile must succeed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "executor-level custom precompile must not be visible to system transactions"
        );

        let mut explicit_tx = system_tx_env(1, chain_id);
        explicit_tx.kind = TxKind::Call(precompile_address);
        executor
            .transact_system_txn(
                evm_env,
                vec![(precompile_address, custom_precompile.to_alloy())],
                explicit_tx,
            )
            .expect("system transaction with explicit precompile must succeed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "explicit system precompile must execute exactly once"
        );
    }

    // --- U-7: fee归零 + 余额不动 + coinbase 不收 tip (matrix §1.2) ---

    /// `u7`: under Alpha gate, SYSTEM_CALLER balance is preserved bit-for-bit
    /// across a system tx (no fee debit) and coinbase does NOT receive any
    /// tip (gas_price == 0). Both invariants together constitute the
    /// "gas-exempt without breaking gas metering" property — gas_used is
    /// still observable in the execution result.
    ///
    /// Picks an obviously non-zero sentinel for SYSTEM_CALLER's pre-state so
    /// any silent debit would change the post-tx balance.
    #[test]
    fn u7_test_system_tx_fee_zero_balance_unchanged() {
        let chain_spec = alpha_active_chainspec(1);
        let chain_id = chain_spec.chain().id();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let header = alpha_block_header(1);
        let evm_env = evm_config.evm_env(&header).expect("evm_env must build");

        // Sentinel pre-balance: 10^18 (1 G). Any silent fee debit would make
        // the post-balance < 10^18; the gas-exempt design must keep it at
        // exactly 10^18.
        let sentinel = U256::from(1_000_000_000_000_000_000_u128);

        let mut serial =
            WrapExecutor::new(BasicBlockExecutor::new(evm_config.clone(), seeded_db(sentinel, 0)));
        let result = serial
            .transact_system_txn(evm_env.clone(), Vec::new(), system_tx_env(0, chain_id))
            .expect("system tx must succeed under Alpha gate");

        // Gas IS still metered — the gate only zeros fee accounting, not gas.
        assert!(
            result.is_success(),
            "system tx execution must succeed (no insufficient-funds / GasPriceLessThanBasefee)"
        );
        assert!(
            result.tx_gas_used() > 0,
            "gas_used must stay positive — gas metering is not bypassed"
        );

        let bundle = serial.take_bundle();
        let acc = bundle
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be in bundle after system tx");
        let info = acc.info.as_ref().expect("info present");
        assert_eq!(
            info.balance, sentinel,
            "SYSTEM_CALLER balance must stay at sentinel — no fee was debited"
        );
        assert_eq!(info.nonce, 1, "nonce must bump by 1 (protocol contract still enforced)");

        // Coinbase tip: gas_price == 0  ⇒  no tip flows to beneficiary. The
        // EvmEnv's beneficiary is `header.beneficiary()` which we left as
        // Address::ZERO. Verify it's not in the bundle with a positive
        // balance.
        assert!(
            bundle
                .state
                .get(&Address::ZERO)
                .and_then(|a| a.info.as_ref())
                .is_none_or(|i| i.balance == U256::ZERO),
            "coinbase (Address::ZERO) must not receive tip when gas_price == 0"
        );
    }

    /// `u7b`: gas_used is invariant across pre-Alpha (gate inactive,
    /// gas_price = base_fee) and post-Alpha (gate active, gas_price = 0)
    /// runs of the same system tx. The L1+L2 gas-exempt levers must change
    /// **only** fee accounting, never gas metering — receipts, gas_used,
    /// state writes, calldata effects must all remain identical.
    #[test]
    fn u7b_test_system_tx_gas_used_equivalent_to_pre_fork() {
        // Pre-Alpha chainspec: Alpha = Timestamp(100), block_ts = 1  ⇒ gate inactive.
        let chain_spec_pre = alpha_active_chainspec(100);
        let evm_config_pre = EthEvmConfig::new(chain_spec_pre.clone());
        let chain_id = chain_spec_pre.chain().id();
        let header_pre = alpha_block_header(1);
        let evm_env_pre = evm_config_pre.evm_env(&header_pre).expect("evm_env pre");
        assert!(
            !is_system_tx_gas_exempt(chain_spec_pre.as_ref(), 1),
            "test fixture sanity: Alpha must NOT be active at ts=1 with Alpha=100"
        );

        // Pre-Alpha tx uses base_fee gas_price (production fallback path).
        let basefee = evm_env_pre.block_env.basefee as u128;
        let tx_pre = TxEnv {
            caller: SYSTEM_CALLER,
            gas_limit: 1_000_000,
            gas_price: basefee,
            kind: TxKind::Call(SYSTEM_CALLER),
            value: U256::ZERO,
            data: Bytes::new(),
            nonce: 0,
            chain_id: Some(chain_id),
            ..TxEnv::default()
        };

        // Need a large enough balance to cover base_fee × gas_limit for the
        // pre-fork run; pick a generous one so the test isn't fragile.
        let funded = U256::from(u128::MAX);
        let mut pre_executor = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config_pre.clone(),
            seeded_db(funded, 0),
        ));
        let result_pre = pre_executor
            .transact_system_txn(evm_env_pre, Vec::new(), tx_pre)
            .expect("pre-Alpha system tx must succeed (production fallback path)");
        let gas_used_pre = result_pre.tx_gas_used();
        assert!(result_pre.is_success(), "pre-Alpha system tx must succeed");

        // Post-Alpha chainspec: Alpha = Timestamp(1), block_ts = 1 ⇒ gate active.
        let chain_spec_post = alpha_active_chainspec(1);
        let evm_config_post = EthEvmConfig::new(chain_spec_post.clone());
        let header_post = alpha_block_header(1);
        let evm_env_post = evm_config_post.evm_env(&header_post).expect("evm_env post");
        assert!(
            is_system_tx_gas_exempt(chain_spec_post.as_ref(), 1),
            "test fixture sanity: Alpha must be active at ts=1 with Alpha=1"
        );

        let tx_post = system_tx_env(0, chain_id);
        let mut post_executor = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config_post.clone(),
            seeded_db(U256::ZERO, 0),
        ));
        let result_post = post_executor
            .transact_system_txn(evm_env_post, Vec::new(), tx_post)
            .expect("post-Alpha system tx must succeed (gas-exempt path)");
        let gas_used_post = result_post.tx_gas_used();
        assert!(result_post.is_success(), "post-Alpha system tx must succeed");

        // The core invariant: gas metering doesn't change across the fork.
        assert_eq!(
            gas_used_pre, gas_used_post,
            "system tx gas_used must be invariant across Alpha boundary (lever changes accounting, not metering)"
        );
    }

    // --- U-8: `disable_balance_check` 防御性 verify (matrix §1.3) ---

    /// `u8`: `disable_balance_check = true` must be a no-op (zero
    /// collateral) when `gas_price = 0` and `disable_base_fee = true`.
    /// Verifies the "second belt" — even without `disable_balance_check`,
    /// the upfront cost is 0 × gas_limit = 0, so the balance check is
    /// trivially satisfied. Test exercises both cfg variants and asserts
    /// byte-equal bundles, pinning that the extra flag has no state-diff
    /// side-effect today (R5 verify).
    #[test]
    fn u8_test_disable_balance_check_zero_collateral() {
        let chain_spec = alpha_active_chainspec(1);
        let chain_id = chain_spec.chain().id();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let header = alpha_block_header(1);
        let evm_env_base = evm_config.evm_env(&header).expect("evm_env must build");

        // Variant A: both flags set (production default for system tx under
        // Alpha gate). The serial `transact_system_txn` sets both.
        let mut exec_a = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config.clone(),
            seeded_db(U256::ZERO, 0),
        ));
        exec_a
            .transact_system_txn(evm_env_base.clone(), Vec::new(), system_tx_env(0, chain_id))
            .expect("variant A (both flags set) must succeed");
        let bundle_a = exec_a.take_bundle();

        // Variant B: disable_base_fee=true, disable_balance_check=false.
        // Build a fresh evm_env, then bypass the gate (i.e. emulate "what
        // happens if `disable_balance_check` ever gets reverted") by hand-
        // crafting the cfg. We do this by simulating a pre-Alpha chainspec
        // (gate inactive at this ts) and overriding the cfg fields ourselves
        // after calling evm_env.
        let chain_spec_b = alpha_active_chainspec(u64::MAX); // gate never fires
        let evm_config_b = EthEvmConfig::new(chain_spec_b.clone());
        let mut evm_env_b = evm_config_b.evm_env(&header).expect("evm_env B");
        evm_env_b.cfg_env.disable_base_fee = true;
        // explicitly DO NOT set disable_balance_check
        assert!(
            !evm_env_b.cfg_env.disable_balance_check,
            "control case: leave disable_balance_check off"
        );
        assert!(
            !is_system_tx_gas_exempt(chain_spec_b.as_ref(), 1),
            "control case: gate must be inactive so `transact_system_txn` does not auto-flip the flags"
        );

        let mut exec_b = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config_b.clone(),
            seeded_db(U256::ZERO, 0),
        ));
        exec_b
            .transact_system_txn(evm_env_b, Vec::new(), system_tx_env(0, chain_id))
            .expect("variant B (only disable_base_fee) must still succeed — upfront cost is 0×gas_limit = 0");
        let bundle_b = exec_b.take_bundle();

        // Bundles must match: `disable_balance_check` is a no-op when the
        // upfront cost is already zero (R5 verify long-term defence —
        // protects against a future revm prepay-path change).
        assert_eq!(
            bundle_a.state, bundle_b.state,
            "state map drift between disable_balance_check on/off"
        );
        assert_eq!(
            bundle_a.contracts, bundle_b.contracts,
            "contracts drift between disable_balance_check on/off"
        );
        assert_eq!(
            bundle_a.state_size, bundle_b.state_size,
            "state_size drift between disable_balance_check on/off"
        );
    }

    // --- U-6d: pre-Alpha baseline symmetry (gate OFF) --------------------

    /// `u6d`: even when the Alpha gate is inactive, serial (`WrapExecutor` →
    /// `EthEvmConfig::transact_system_txn`) and grevm
    /// (`GrevmExecutor::transact_system_txn`) must produce byte-identical
    /// bundles for the same batch of pre-Alpha-shaped system txs (i.e.
    /// `gas_price = basefee`, production fallback path). Pins that
    /// backend byte-equivalence comes from shared underlying revm/grevm
    /// semantics, not from the gate short-circuiting both backends onto the
    /// same fast path.
    ///
    /// Dual to U-6 (gate ON) and complementary to U-7b: U-7b compares
    /// `gas_used` across the fork on a single backend; U-6d compares
    /// bundles across backends on the pre-Alpha side of the fork.
    #[test]
    fn u6d_test_pre_alpha_baseline_symmetry() {
        // Alpha never fires: pushed out to u64::MAX so `is_system_tx_gas_exempt`
        // is guaranteed off at the block ts we synthesize below.
        let chain_spec = alpha_active_chainspec(u64::MAX);
        let chain_id = chain_spec.chain().id();
        assert!(
            !is_system_tx_gas_exempt(chain_spec.as_ref(), 1),
            "test fixture sanity: Alpha must be inactive at ts=1 with Alpha=u64::MAX"
        );

        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let header = alpha_block_header(1);
        let evm_env = evm_config.evm_env(&header).expect("evm_env must build");

        // With the gate off, `gas_price = 0` would trip `GasPriceLessThanBasefee`.
        // Use the block's basefee (production fallback path — matches U-7b's
        // `tx_pre`) so the tx clears revm's fee-check without further cfg
        // manipulation, keeping the comparison isolated to backend semantics.
        let basefee = evm_env.block_env.basefee as u128;
        let build_tx = |nonce: u64| TxEnv {
            caller: SYSTEM_CALLER,
            gas_limit: 1_000_000,
            gas_price: basefee,
            kind: TxKind::Call(SYSTEM_CALLER),
            value: U256::ZERO,
            data: Bytes::new(),
            nonce,
            chain_id: Some(chain_id),
            ..TxEnv::default()
        };
        let tx0 = build_tx(0);
        let tx1 = build_tx(1);

        // Fund SYSTEM_CALLER generously so `basefee × gas_limit × 2` clears
        // the balance check trivially — the test isn't measuring balance
        // arithmetic, only backend equivalence.
        let funded = U256::from(u128::MAX);

        let mut serial =
            WrapExecutor::new(BasicBlockExecutor::new(evm_config.clone(), seeded_db(funded, 0)));
        serial
            .transact_system_txn(evm_env.clone(), Vec::new(), tx0.clone())
            .expect("serial pre-Alpha tx0 must succeed (production fallback path)");
        serial
            .transact_system_txn(evm_env.clone(), Vec::new(), tx1.clone())
            .expect("serial pre-Alpha tx1 must succeed (production fallback path)");
        let bundle_serial = serial.take_bundle();

        let mut grevm = GrevmExecutor::new(chain_spec.clone(), &evm_config, seeded_db(funded, 0));
        grevm
            .transact_system_txn(evm_env.clone(), Vec::new(), tx0)
            .expect("grevm pre-Alpha tx0 must succeed (production fallback path)");
        grevm
            .transact_system_txn(evm_env, Vec::new(), tx1)
            .expect("grevm pre-Alpha tx1 must succeed (production fallback path)");
        let bundle_grevm = grevm.take_bundle();

        // Load-bearing byte-equivalence (same fields as U-6; see U-6 comment
        // for the `reverts_size` skip rationale).
        assert_eq!(bundle_serial.state, bundle_grevm.state, "pre-Alpha: state map drift");
        assert_eq!(bundle_serial.contracts, bundle_grevm.contracts, "pre-Alpha: contracts drift");
        assert_eq!(
            bundle_serial.state_size, bundle_grevm.state_size,
            "pre-Alpha: state_size drift"
        );
        assert_eq!(bundle_serial.reverts, bundle_grevm.reverts, "pre-Alpha: reverts content drift");

        // Sanity: SYSTEM_CALLER balance strictly decreased (fee was actually
        // debited) — proves the gate stayed off and we didn't accidentally
        // exercise the gas-exempt path.
        let serial_info = bundle_serial
            .state
            .get(&SYSTEM_CALLER)
            .and_then(|a| a.info.as_ref())
            .expect("SYSTEM_CALLER must be in serial bundle after two pre-Alpha txs");
        assert!(
            serial_info.balance < funded,
            "SYSTEM_CALLER balance must be debited under the pre-Alpha (gate-off) path"
        );
        assert_eq!(serial_info.nonce, 2, "nonce must bump by 2 (one per system tx)");

        // NB: we deliberately do NOT assert coinbase received a positive
        // balance. With `gas_price = basefee` (production fallback shape),
        // the EIP-1559 split is `priority_fee = gas_price - basefee = 0`
        // — the entire fee is burned and coinbase receives nothing. The
        // `SYSTEM_CALLER.balance < funded` check above is sufficient to
        // prove the pre-Alpha fee path was exercised (fee was debited);
        // asserting coinbase balance would only re-derive EIP-1559 math
        // and is not the invariant U-6d cares about.
    }
}
