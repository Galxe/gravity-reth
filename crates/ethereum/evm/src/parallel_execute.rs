//! Parallel EVM executor using Grevm

use crate::RethReceiptBuilder;
use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::BlockHeader;
use alloy_eips::{eip4895::Withdrawal, eip7685::Requests};
use alloy_evm::{
    block::{calc, StateChangePostBlockSource, StateChangeSource, SystemCaller},
    eth::{dao_fork, eip6110, spec::EthExecutorSpec, EthBlockExecutorFactory},
    precompiles::DynPrecompile,
    EvmEnv,
};
use alloy_primitives::{map::HashMap, Address};
use gravity_primitives::get_gravity_config;
use grevm::{ParallelBundleState, ParallelState, Scheduler};
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
    state::{Account, AccountStatus, EvmState},
    Database, DatabaseCommit,
};

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
    /// Custom precompiled contracts to inject into the EVM.
    custom_precompiles: Option<Arc<Vec<(Address, DynPrecompile)>>>,
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
        let system_caller = SystemCaller::new(chain_spec.clone());
        let report_db_metrics = get_gravity_config().report_db_metrics;
        Self {
            state: Some(ParallelState::new(db, true, report_db_metrics)),
            chain_spec,
            evm_config: evm_config.clone(),
            system_caller,
            custom_precompiles: None,
        }
    }

    fn apply_pre_execution_changes(
        &mut self,
        block: &RecoveredBlock<Block>,
    ) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag = self.chain_spec.is_spurious_dragon_active_at_block(block.number);
        let state = self.state.as_mut().unwrap();
        state.set_state_clear_flag(state_clear_flag);
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
            let executor = Scheduler::new(
                cfg_env,
                block_env,
                txs,
                state,
                false,
                self.custom_precompiles.clone(),
            );
            executor.parallel_execute(None).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::EVM {
                    hash: block
                        .transactions_with_sender()
                        .nth(e.txid)
                        .unwrap()
                        .1
                        .recalculate_hash(),
                    error: Box::new(e.error),
                })
            })?;
            executor.take_result_and_state()
        };

        self.state = Some(state);

        let mut receipts = Vec::with_capacity(results.len());
        let mut cumulative_gas_used = 0;
        for (result, tx_type) in
            results.into_iter().zip(block.body().transactions().map(|tx| tx.tx_type()))
        {
            cumulative_gas_used += result.gas_used();
            receipts.push(Receipt {
                tx_type,
                success: result.is_success(),
                cumulative_gas_used,
                logs: result.into_logs(),
            });
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

        // Gravity chain uses a deflationary model where rewards come solely from gas fees,
        // so PoW block rewards (coinbase increments) are disabled to prevent inflation.
        let mut balance_increments = HashMap::default();
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
        // Gravity hardforks: apply bytecode upgrades and storage patches
        {
            use crate::hardfork::{
                alpha::AlphaHardfork, beta::BetaHardfork, common::apply_hardfork_upgrades,
            };

            let hf = self.chain_spec.gravity_hardforks();
            if hf.fork(GravityHardfork::Alpha).transitions_at_block(block.number()) {
                apply_hardfork_upgrades(&AlphaHardfork, state)?;
            }
            if hf.fork(GravityHardfork::Beta).transitions_at_block(block.number()) {
                apply_hardfork_upgrades(&BetaHardfork, state)?;
            }
        }

        // increment balances
        state
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;

        {
            use crate::hardfork::{
                common::apply_hardfork_upgrades, delta::DeltaHardfork, gamma::GammaHardfork,
            };

            let hf = self.chain_spec.gravity_hardforks();
            if hf.fork(GravityHardfork::Gamma).transitions_at_block(block.number()) {
                apply_hardfork_upgrades(&GammaHardfork, state)?;
            }
            if hf.fork(GravityHardfork::Delta).transitions_at_block(block.number()) {
                apply_hardfork_upgrades(&DeltaHardfork, state)?;
            }
        }

        // call state hook with changes due to balance increments.
        self.system_caller.try_on_state_with(|| {
            balance_increment_state(&balance_increments, state).map(|state| {
                (
                    StateChangeSource::PostBlock(StateChangePostBlockSource::BalanceIncrements),
                    Cow::Owned(state),
                )
            })
        })?;

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
        Ok(BlockExecutionResult { receipts, gas_used, requests })
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
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
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

    fn apply_custom_precompiles(&mut self, custom_precompiles: Arc<Vec<(Address, DynPrecompile)>>) {
        self.custom_precompiles = Some(custom_precompiles);
    }
}

#[allow(dead_code)]
#[inline]
fn post_block_balance_increments<ChainSpec, Block>(
    chain_spec: &ChainSpec,
    block: &RecoveredBlock<Block>,
) -> HashMap<Address, u128>
where
    ChainSpec: EthereumHardforks + EthChainSpec,
    Block: reth_primitives_traits::Block,
{
    // After Alpha hardfork, skip all post-block balance increments
    // (disables PoW block rewards and DAO fork irregularities)
    if chain_spec
        .gravity_hardforks()
        .is_fork_active_at_block(GravityHardfork::Alpha, block.header().number())
    {
        return HashMap::default();
    }

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

    // process withdrawals
    insert_post_block_withdrawals_balance_increments(
        chain_spec,
        block.header().timestamp(),
        block.body().withdrawals().as_ref().map(|w| w.as_slice()),
        &mut balance_increments,
    );

    balance_increments
}

#[allow(dead_code)]
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

fn balance_increment_state<DB: ParallelDatabase>(
    balance_increments: &HashMap<Address, u128>,
    state: &ParallelState<DB>,
) -> Result<EvmState, BlockExecutionError> {
    let load_account = |address: &Address| -> Result<(Address, Account), BlockExecutionError> {
        let info = state
            .cache
            .accounts
            .get(address)
            .and_then(|account| account.value().account.clone())
            .ok_or_else(|| {
                BlockExecutionError::msg("could not load account for balance increment")
            })?;

        Ok((
            *address,
            Account {
                info,
                storage: Default::default(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            },
        ))
    };

    balance_increments
        .iter()
        .filter(|&(_, &balance)| balance != 0)
        .map(|(addr, _)| load_account(addr))
        .collect::<Result<EvmState, _>>()
}

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
    use crate::EthEvmConfig;
    use alloc::sync::Arc;
    use alloy_consensus::Header;
    use alloy_eips::{
        eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE},
        eip7685::EMPTY_REQUESTS_HASH,
    };
    use alloy_primitives::{keccak256, B256, U256};
    use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
    use reth_ethereum_primitives::Block;
    use reth_evm::{execute::BasicBlockExecutor, parallel_execute::WrapExecutor};
    use reth_primitives_traits::RecoveredBlock;
    use revm::{
        bytecode::Bytecode,
        database::{CacheDB, EmptyDB},
        state::AccountInfo,
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
        };
        let mut state_diff = EvmState::default();
        state_diff.insert(
            HISTORY_STORAGE_ADDRESS,
            Account {
                info,
                storage: Default::default(),
                status: AccountStatus::Created | AccountStatus::Touched,
                transaction_id: 0,
            },
        );
        state_diff
    }

    fn prague_block(number: u64, parent_hash: B256) -> RecoveredBlock<Block> {
        let header = Header {
            parent_hash,
            timestamp: 1,
            number,
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Header::default()
        };
        RecoveredBlock::new_unhashed(Block { header, body: Default::default() }, vec![])
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
        let bumped_info =
            AccountInfo { nonce: 2, balance: U256::from(100u64), code_hash, code: None };
        let mut second_diff = EvmState::default();
        second_diff.insert(
            HISTORY_STORAGE_ADDRESS,
            Account {
                info: bumped_info,
                storage: Default::default(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            },
        );
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

    // --- D-1: coinbase-touch bundle divergence (grevm lazy_reward vs serial revm) ---
    //
    // Serial revm credits the beneficiary INSIDE every tx via
    // `reward_beneficiary` -> `journal.balance_incr(coinbase, reward)`, which
    // *unconditionally* marks the coinbase as touched even when `reward == 0`
    // (revm journal/inner.rs balance_incr touches regardless of amount).
    //
    // Grevm runs with `lazy_reward = true` (scheduler.rs sets cfg.lazy_reward),
    // so `reward_beneficiary` is skipped (revm post_execution.rs:70). The fee is
    // deferred and applied via `StateAsyncCommit::commit` ->
    // `ParallelState::increment_balances(vec![(coinbase, lazy_reward)])`
    // (async_commit.rs:189), and `increment_balances` SKIPS zero balances
    // (parallel_state.rs:554). So a zero-priority-fee tx produces NO coinbase
    // transition in grevm.
    //
    // Consequence: the two backends emit DIFFERENT BundleState account sets for
    // the same committed state. `HashedPostState::from_bundle_state`
    // (crates/trie/common/src/hashed_state.rs:48) is hashed from whatever set
    // each backend emits, and the bundle is also persisted as plain-state +
    // reverts/changesets. This test PROVES the divergence by running both
    // backends on the same block and diffing the bundles.
    use reth_primitives_traits::SignerRecoverable;

    fn sign_legacy_tx(
        secret: B256,
        chain_id: u64,
        nonce: u64,
        gas_price: u128,
        to: Address,
        value: U256,
    ) -> reth_ethereum_primitives::TransactionSigned {
        sign_legacy_tx_gas(secret, chain_id, nonce, gas_price, to, value, 21_000)
    }

    fn sign_legacy_tx_gas(
        secret: B256,
        chain_id: u64,
        nonce: u64,
        gas_price: u128,
        to: Address,
        value: U256,
        gas_limit: u64,
    ) -> reth_ethereum_primitives::TransactionSigned {
        use alloy_consensus::transaction::SignableTransaction;
        use alloy_consensus::TxLegacy;
        use alloy_primitives::TxKind;
        let tx = TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price,
            gas_limit,
            to: TxKind::Call(to),
            value,
            input: Default::default(),
        };
        let tx = reth_ethereum_primitives::Transaction::Legacy(tx);
        let sig = reth_primitives_traits::crypto::secp256k1::sign_message(
            secret,
            tx.signature_hash(),
        )
        .unwrap();
        tx.into_signed(sig).into()
    }

    #[test]
    fn d1_grevm_vs_serial_coinbase_bundle_divergence_zero_priority_fee() {
        use alloy_consensus::constants::ETH_TO_WEI;
        use reth_chainspec::ChainSpecBuilder;
        use secp256k1::SecretKey;

        // Paris (post-Merge) active => no PoW block reward in EITHER backend
        // (calc::base_block_reward returns None after Paris), so the ONLY
        // remaining coinbase delta is the per-tx priority fee. With a legacy tx
        // priced exactly at base_fee, coinbase_gas_price = gas_price - basefee
        // = 0 => reward == 0. This isolates the coinbase-touch asymmetry from the
        // (intentional) Gravity block-reward disabling.
        //
        // RESULT (differential, both backends): at zero reward neither backend
        // emits a coinbase bundle entry; at nonzero reward both emit identical
        // entries (whether coinbase pre-exists or is freshly created). The
        // hypothesised coinbase-touch divergence does NOT reproduce.
        let chain_spec: Arc<ChainSpec> =
            Arc::new(ChainSpecBuilder::from(&*MAINNET).paris_activated().build());
        let chain_id = chain_spec.chain.id();

        let secret = SecretKey::from_slice(&[0x11u8; 32]).unwrap();
        let secret_b256 = B256::from_slice(&secret.secret_bytes());
        let coinbase = Address::from([0xC0; 20]);
        let recipient = Address::from([0xDD; 20]);

        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let priority_test_gas_price = std::env::var("D1_GAS_PRICE_GWEI")
            .ok()
            .and_then(|s| s.parse::<u128>().ok())
            .map(|g| g * 1_000_000_000)
            .unwrap_or(base_fee as u128); // default: gas_price == base_fee => zero priority fee
        let tx = sign_legacy_tx(
            secret_b256,
            chain_id,
            0,
            priority_test_gas_price,
            recipient,
            U256::from(1u64),
        );
        let sender = tx.recover_signer().expect("recover sender");

        let header = Header {
            number: 1,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(base_fee),
            beneficiary: coinbase,
            timestamp: 1,
            ..Header::default()
        };
        let body = reth_ethereum_primitives::BlockBody {
            transactions: vec![tx],
            ommers: vec![],
            withdrawals: None,
        };
        let block = Block { header, body };
        let recovered = RecoveredBlock::new_unhashed(block, vec![sender]);

        // Prestate: fund the sender. The coinbase prestate is selectable:
        //  - default: pre-existing NON-empty account (nonce=7, balance=123)
        //  - D1_COINBASE_EMPTY=1: coinbase absent from DB (LoadedNotExisting),
        //    so a nonzero reward CREATES it.
        let coinbase_empty = std::env::var("D1_COINBASE_EMPTY").is_ok();
        let prestate = |db: &mut CacheDB<EmptyDB>| {
            db.insert_account_info(
                sender,
                AccountInfo {
                    balance: U256::from(10u128 * ETH_TO_WEI),
                    nonce: 0,
                    code_hash: revm::primitives::KECCAK_EMPTY,
                    code: None,
                },
            );
            if !coinbase_empty {
                db.insert_account_info(
                    coinbase,
                    AccountInfo {
                        balance: U256::from(123u64),
                        nonce: 7,
                        code_hash: revm::primitives::KECCAK_EMPTY,
                        code: None,
                    },
                );
            }
        };

        // --- serial revm path (disable_grevm equivalent) ---
        let serial_bundle = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
            ParallelExecutor::execute(&mut executor, &recovered).expect("serial execute").state
        };

        // --- grevm path ---
        let grevm_bundle = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut executor = GrevmExecutor::new(chain_spec.clone(), &evm_config, db);
            // GrevmExecutor::execute takes the bundle internally
            <GrevmExecutor<_, _, _> as ParallelExecutor>::execute(&mut executor, &recovered)
                .expect("grevm execute")
                .state
        };

        let serial_coinbase = serial_bundle.state.get(&coinbase);
        let grevm_coinbase = grevm_bundle.state.get(&coinbase);

        eprintln!("serial coinbase bundle entry: {serial_coinbase:?}");
        eprintln!("grevm  coinbase bundle entry: {grevm_coinbase:?}");
        eprintln!("serial state addrs: {:?}", serial_bundle.state.keys().collect::<Vec<_>>());
        eprintln!("grevm  state addrs: {:?}", grevm_bundle.state.keys().collect::<Vec<_>>());
        let sbal = |b: &BundleState, a: &Address| {
            b.state.get(a).and_then(|x| x.info.as_ref()).map(|i| i.balance)
        };
        eprintln!("sender  serial={:?} grevm={:?}", sbal(&serial_bundle, &sender), sbal(&grevm_bundle, &sender));
        eprintln!("recip   serial={:?} grevm={:?}", sbal(&serial_bundle, &recipient), sbal(&grevm_bundle, &recipient));
        eprintln!("coinbase serial={:?} grevm={:?} (grevm None => stays at DB value 123)", sbal(&serial_bundle, &coinbase), sbal(&grevm_bundle, &coinbase));

        // The whole point: prove whether the bundle account SETS match.
        // If they diverge, the two backends persist different plain-state /
        // reverts, and feed different inputs to HashedPostState::from_bundle_state.
        let serial_addrs: std::collections::BTreeSet<_> = serial_bundle.state.keys().collect();
        let grevm_addrs: std::collections::BTreeSet<_> = grevm_bundle.state.keys().collect();
        assert_eq!(
            serial_addrs, grevm_addrs,
            "BUNDLE ACCOUNT SET DIVERGENCE between serial revm and grevm \
             (coinbase touch-by-zero-reward asymmetry)"
        );
    }

    /// Run a single-tx block on both backends and return (serial_bundle, grevm_bundle).
    fn run_both_backends(
        chain_spec: Arc<ChainSpec>,
        recovered: &RecoveredBlock<Block>,
        prestate: impl Fn(&mut CacheDB<EmptyDB>),
    ) -> (BundleState, BundleState) {
        let serial = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut ex = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
            ParallelExecutor::execute(&mut ex, recovered).expect("serial").state
        };
        let grevm = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut ex = GrevmExecutor::new(chain_spec.clone(), &evm_config, db);
            <GrevmExecutor<_, _, _> as ParallelExecutor>::execute(&mut ex, recovered)
                .expect("grevm")
                .state
        };
        (serial, grevm)
    }

    /// D-2: EIP-161 "touch empty existing account" class. A value=0 transfer to an
    /// account that pre-exists in the DB as EMPTY (balance=0,nonce=0,no code) must
    /// DELETE it from the trie (state clear). `from_bundle_state` keys a
    /// destroyed/None-info account as a trie deletion (hashed_state.rs:147). This
    /// pins that grevm's `touch_empty_eip161` transition (parallel_state.rs:128)
    /// matches serial revm's `CacheAccount` exactly: both must emit
    /// `info = None` for the touched-empty recipient.
    #[test]
    fn d2_grevm_vs_serial_eip161_touch_empty_existing_account() {
        use alloy_consensus::constants::ETH_TO_WEI;
        use reth_chainspec::ChainSpecBuilder;
        use secp256k1::SecretKey;

        let chain_spec: Arc<ChainSpec> =
            Arc::new(ChainSpecBuilder::from(&*MAINNET).paris_activated().build());
        let chain_id = chain_spec.chain.id();

        let secret = SecretKey::from_slice(&[0x22u8; 32]).unwrap();
        let secret_b256 = B256::from_slice(&secret.secret_bytes());
        let empty_recipient = Address::from([0xEE; 20]);
        let coinbase = Address::from([0xC0; 20]);

        let base_fee: u64 = 1_000_000_000;
        // value=0 transfer to an existing-but-empty account => pure EIP-161 touch.
        let tx = sign_legacy_tx(secret_b256, chain_id, 0, base_fee as u128, empty_recipient, U256::ZERO);
        let sender = tx.recover_signer().expect("recover");

        let header = Header {
            number: 1,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(base_fee),
            beneficiary: coinbase,
            timestamp: 1,
            ..Header::default()
        };
        let body =
            reth_ethereum_primitives::BlockBody { transactions: vec![tx], ommers: vec![], withdrawals: None };
        let recovered = RecoveredBlock::new_unhashed(Block { header, body }, vec![sender]);

        let prestate = |db: &mut CacheDB<EmptyDB>| {
            db.insert_account_info(
                sender,
                AccountInfo {
                    balance: U256::from(10u128 * ETH_TO_WEI),
                    nonce: 0,
                    code_hash: revm::primitives::KECCAK_EMPTY,
                    code: None,
                },
            );
            // Pre-existing EMPTY account (all-default). revm classifies this as
            // LoadedEmptyEIP161 on load.
            db.insert_account_info(empty_recipient, AccountInfo::default());
        };

        let (serial, grevm) = run_both_backends(chain_spec, &recovered, prestate);

        let s = serial.state.get(&empty_recipient);
        let g = grevm.state.get(&empty_recipient);
        eprintln!("d2 serial empty_recipient: {s:?}");
        eprintln!("d2 grevm  empty_recipient: {g:?}");

        // info must be None (destroyed) in BOTH, or absent in BOTH.
        let s_info_none = s.map(|a| a.info.is_none());
        let g_info_none = g.map(|a| a.info.is_none());
        assert_eq!(
            s_info_none, g_info_none,
            "EIP-161 touched-empty recipient diverges between backends: serial={s_info_none:?} grevm={g_info_none:?}"
        );

        let serial_addrs: std::collections::BTreeSet<_> = serial.state.keys().collect();
        let grevm_addrs: std::collections::BTreeSet<_> = grevm.state.keys().collect();
        assert_eq!(serial_addrs, grevm_addrs, "d2 bundle account set divergence");
    }

    /// D-3: self-destruct class. A pre-deployed contract whose only action is
    /// SELFDESTRUCT(beneficiary). Calling it triggers grevm's
    /// `AbortReason::SelfDestructed` -> `fallback_sequential` (scheduler.rs:564).
    /// Compares the resulting bundle for the self-destructed contract, the
    /// beneficiary, sender and coinbase against serial revm.
    #[test]
    fn d3_grevm_vs_serial_selfdestruct() {
        use alloy_consensus::constants::ETH_TO_WEI;
        use reth_chainspec::ChainSpecBuilder;
        use revm::bytecode::Bytecode;
        use secp256k1::SecretKey;

        // Use a pre-Cancun spec (paris) so SELFDESTRUCT actually destroys the
        // account (EIP-6780 restricts it to same-tx-created contracts in Cancun+).
        let chain_spec: Arc<ChainSpec> =
            Arc::new(ChainSpecBuilder::from(&*MAINNET).paris_activated().build());
        let chain_id = chain_spec.chain.id();

        let secret = SecretKey::from_slice(&[0x33u8; 32]).unwrap();
        let secret_b256 = B256::from_slice(&secret.secret_bytes());
        let sd_contract = Address::from([0x5D; 20]);
        let sd_beneficiary = Address::from([0xBE; 20]);
        let coinbase = Address::from([0xC0; 20]);

        // PUSH20 <beneficiary> SELFDESTRUCT
        let mut code = vec![0x73u8];
        code.extend_from_slice(sd_beneficiary.as_slice());
        code.push(0xFF);
        let code = revm::primitives::Bytes::from(code);
        let bytecode = Bytecode::new_raw(code.clone());
        let code_hash = keccak256(&code);

        let base_fee: u64 = 1_000_000_000;
        // gas limit must cover CALL into a contract that SELFDESTRUCTs.
        let tx = sign_legacy_tx_gas(
            secret_b256,
            chain_id,
            0,
            base_fee as u128,
            sd_contract,
            U256::ZERO,
            100_000,
        );
        let sender = SignerRecoverable::recover_signer(&tx).expect("recover");

        let header = Header {
            number: 1,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(base_fee),
            beneficiary: coinbase,
            timestamp: 1,
            ..Header::default()
        };
        let body =
            reth_ethereum_primitives::BlockBody { transactions: vec![tx], ommers: vec![], withdrawals: None };
        let recovered = RecoveredBlock::new_unhashed(Block { header, body }, vec![sender]);

        let prestate = |db: &mut CacheDB<EmptyDB>| {
            db.insert_account_info(
                sender,
                AccountInfo {
                    balance: U256::from(10u128 * ETH_TO_WEI),
                    nonce: 0,
                    code_hash: revm::primitives::KECCAK_EMPTY,
                    code: None,
                },
            );
            db.insert_account_info(
                sd_contract,
                AccountInfo {
                    balance: U256::from(7_000_000_000_000_000_000u128), // 7 ETH
                    nonce: 1,
                    code_hash,
                    code: Some(bytecode.clone()),
                },
            );
        };

        let (serial, grevm) = run_both_backends(chain_spec, &recovered, prestate);

        for (label, addr) in
            [("sd_contract", sd_contract), ("sd_beneficiary", sd_beneficiary), ("sender", sender), ("coinbase", coinbase)]
        {
            let s = serial.state.get(&addr);
            let g = grevm.state.get(&addr);
            eprintln!("d3 {label} serial={:?}", s.map(|a| (a.info.as_ref().map(|i| i.balance), &a.status)));
            eprintln!("d3 {label} grevm ={:?}", g.map(|a| (a.info.as_ref().map(|i| i.balance), &a.status)));
            let s_bal = s.and_then(|a| a.info.as_ref()).map(|i| i.balance);
            let g_bal = g.and_then(|a| a.info.as_ref()).map(|i| i.balance);
            let s_none = s.map(|a| a.info.is_none());
            let g_none = g.map(|a| a.info.is_none());
            assert_eq!(s_bal, g_bal, "d3 {label} balance divergence");
            assert_eq!(s_none, g_none, "d3 {label} info-None (destroyed) divergence");
        }

        let serial_addrs: std::collections::BTreeSet<_> = serial.state.keys().collect();
        let grevm_addrs: std::collections::BTreeSet<_> = grevm.state.keys().collect();
        assert_eq!(serial_addrs, grevm_addrs, "d3 bundle account set divergence (self-destruct)");
    }

    // --- D-4: system metadata transaction + plain (zero-user-tx) block --------
    //
    // FAITHFUL IN-PROCESS REPRO of the A2 oracle's "plain block" (height 7, no
    // user txs). The A2 two-process oracle reported that a plain block yields
    // DIFFERENT persisted state_roots across grevm vs disable_grevm. The d1/d2/d3
    // tests only exercised ordinary USER txs (which all match). The A2 plain block
    // contains NO user txs — only the SYSTEM metadata transaction (onBlockStart,
    // sent by SYSTEM_CALLER to BLOCK_ADDR) followed by a zero-tx block body. That
    // system-tx path is precisely what d1/d2/d3 never covered.
    //
    // This test drives, IN ONE PROCESS, the SAME sequence the pipe-exec
    // `execute_ordered_block` runs for a plain block (lib.rs):
    //   1. executor.transact_system_txn(evm_env, [], metadata_tx_env)   // onBlockStart
    //   2. executor.execute(&zero_tx_block)                              // empty body
    //   3. take_bundle() (called inside execute)                        // combined bundle
    // through BOTH GrevmExecutor (default) and WrapExecutor<BasicBlockExecutor>
    // (= --gravity.disable-grevm), against an IDENTICAL prestate, and diffs the
    // resulting BundleState.state account maps.
    //
    // The metadata system tx is modeled as SYSTEM_CALLER calling a contract at
    // BLOCK_ADDR whose runtime does SSTORE(slot0, CALLER) — i.e. it touches its
    // own storage AND credits/charges SYSTEM_CALLER (nonce + gas), exactly the
    // shape of account/storage transitions onBlockStart produces. EIP-2935 is a
    // no-op pre-Prague (paris spec, height 7), matching the A2 plain-block height.
    //
    // VERDICT MECHANISM: `HashedPostState::from_bundle_state` (used by lib.rs to
    // feed the state-root computation) is a PURE function of `bundle.state`.
    // Therefore byte-identical bundle.state across the two backends ⟹ identical
    // hashed state ⟹ identical state_root contribution from this block. Conversely,
    // an account set that differs across backends ⟹ different state_root.
    //
    // VERDICT: **REAL bug.** This test FAILS (asserts an equivalence that does NOT
    // hold) — it is a repro. At the production metadata-tx shape (gas_price ==
    // base_fee, i.e. ZERO priority fee), the serial (disable_grevm) path's revm
    // `reward_beneficiary` UNCONDITIONALLY touches the coinbase, and because
    // `BasicBlockExecutor`'s `State` is built `without_state_clear()`, that
    // touched-empty coinbase is NOT pruned and lands in the bundle. The grevm path
    // emits no coinbase transition. → different bundle account sets → different
    // state_root. The two-process A2 oracle confirms the SAME divergence on the
    // PERSISTED root (grevm 0xb063… vs disable_grevm 0xc717… at height 7), and the
    // coinbase (Address::ZERO) diverges by ~2 ETH per block in production. Full
    // analysis: reviews/focus-2026-06-11-round3/A2-1-VERDICT.md. Setting
    // D4_PRIORITY_FEE_GWEI=2 makes this PASS (nonzero reward is credited by both),
    // confirming the trigger is the zero-coinbase-reward system-tx path.
    const SYSTEM_CALLER_ADDR: Address =
        alloy_primitives::address!("00000000000000000000000000000001625f0000");
    const BLOCK_ADDR_SYS: Address =
        alloy_primitives::address!("00000000000000000000000000000001625f2004");

    // CONFIRMED REPRO for A2-1 (system-tx path coinbase divergence). FAILS on HEAD by design:
    // on the `transact_system_txn` path the serial WrapExecutor<BasicBlockExecutor> (built
    // `without_state_clear()`) retains/credits the touched coinbase while grevm (state-clear +
    // lazy_reward) prunes/burns it -> divergent bundle -> divergent state_root every block.
    // d1/d2/d3 (USER-tx path) pass because that path clears state; only the system-tx path d4
    // exercises diverges. #[ignore] keeps `unit.yml` green (reth-evm-ethereum lib is not excluded
    // there); run via the bug-repro-gate / `--ignored`. Un-ignore once both backends agree on the
    // system-tx coinbase touch/credit + state-clear semantics.
    #[test]
    #[ignore = "repro of A2-1 (system-tx coinbase divergence between grevm and disable_grevm); \
                FAILS until both backends agree on coinbase touch/credit semantics. Un-ignore when fixed."]
    fn d4_grevm_vs_serial_system_tx_plain_block() {
        use reth_chainspec::ChainSpecBuilder;

        let chain_spec: Arc<ChainSpec> =
            Arc::new(ChainSpecBuilder::from(&*MAINNET).paris_activated().build());

        // Runtime code for BLOCK_ADDR: CALLER PUSH1 0x00 SSTORE STOP
        //   33 (CALLER) 60 00 (PUSH1 0) 55 (SSTORE) 00 (STOP)
        let block_runtime = revm::primitives::Bytes::from(vec![0x33u8, 0x60, 0x00, 0x55, 0x00]);
        let block_code = Bytecode::new_raw(block_runtime.clone());
        let block_code_hash = keccak256(&block_runtime);

        let base_fee: u64 = 1_000_000_000;
        let coinbase = Address::from([0xC0; 20]);

        // Identical prestate for both backends: SYSTEM_CALLER funded (nonce 0),
        // BLOCK_ADDR carries the SSTORE runtime. This mirrors a genesis where the
        // system contracts are deployed and SYSTEM_CALLER exists.
        let prestate = |db: &mut CacheDB<EmptyDB>| {
            db.insert_account_info(
                SYSTEM_CALLER_ADDR,
                AccountInfo {
                    balance: U256::from(1_000_000_000_000_000_000u128), // 1 ETH
                    nonce: 0,
                    code_hash: revm::primitives::KECCAK_EMPTY,
                    code: None,
                },
            );
            db.insert_account_info(
                BLOCK_ADDR_SYS,
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash: block_code_hash,
                    code: Some(block_code.clone()),
                },
            );
        };

        // Plain block: height 7, zero user txs, paris (pre-Prague => EIP-2935 no-op).
        let header = Header {
            number: 7,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(base_fee),
            beneficiary: coinbase,
            timestamp: 7,
            ..Header::default()
        };
        let block = Block { header: header.clone(), body: Default::default() };
        let recovered = RecoveredBlock::new_unhashed(block, vec![]);

        // The system metadata tx: SYSTEM_CALLER -> BLOCK_ADDR (onBlockStart shape).
        // Built as a TxEnv directly (the pipe path uses
        // `Recovered::new_unchecked(metadata_txn, SYSTEM_CALLER).into_tx_env()`;
        // here we construct the equivalent TxEnv by hand so the test needs no
        // pipe-exec dependency).
        // FAITHFUL PRODUCTION SHAPE: the real metadata (onBlockStart) system tx is
        // built with `gas_price = base_fee` (lib.rs:706 / construct_metadata_txn),
        // i.e. ZERO priority fee. On the serial (disable_grevm) path, revm's
        // `reward_beneficiary` still UNCONDITIONALLY touches the coinbase (balance
        // increment of 0 marks it touched), and because `BasicBlockExecutor::new`
        // builds its `State` with `without_state_clear()`, that touched-empty
        // coinbase is NOT pruned at system-tx commit time — it lands in the bundle
        // as an empty account. The grevm path runs the scheduler with
        // `lazy_reward`, deferring the fee to `increment_balances`, which SKIPS zero
        // increments (parallel_state.rs), so grevm emits NO coinbase transition.
        // Result: the two backends emit DIFFERENT bundle account sets for the SAME
        // plain block → different HashedPostState → different state_root.
        let priority_fee: u128 = std::env::var("D4_PRIORITY_FEE_GWEI")
            .ok()
            .and_then(|s| s.parse::<u128>().ok())
            .map(|g| g * 1_000_000_000)
            .unwrap_or(0); // default: zero priority fee == production metadata tx
        let build_sys_tx_env = || TxEnv {
            caller: SYSTEM_CALLER_ADDR,
            gas_limit: 200_000,
            gas_price: base_fee as u128 + priority_fee,
            kind: revm::primitives::TxKind::Call(BLOCK_ADDR_SYS),
            value: U256::ZERO,
            data: revm::primitives::Bytes::new(),
            nonce: 0,
            chain_id: Some(chain_spec.chain.id()),
            gas_priority_fee: Some(priority_fee),
            ..Default::default()
        };

        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let evm_env = evm_config.evm_env(&header).expect("evm_env");

        // --- serial revm path (disable_grevm equivalent) ---
        let (serial_bundle, serial_sys_ok) = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut ex = WrapExecutor::new(BasicBlockExecutor::new(evm_config, db));
            let r = ParallelExecutor::transact_system_txn(
                &mut ex,
                evm_env.clone(),
                Vec::new(),
                build_sys_tx_env(),
            )
            .expect("serial system tx");
            let out = ParallelExecutor::execute(&mut ex, &recovered).expect("serial execute");
            (out.state, r.is_success())
        };

        // --- grevm path (default) ---
        let (grevm_bundle, grevm_sys_ok) = {
            let mut db = CacheDB::new(EmptyDB::default());
            prestate(&mut db);
            let evm_config = EthEvmConfig::new(chain_spec.clone());
            let mut ex = GrevmExecutor::new(chain_spec.clone(), &evm_config, db);
            let r = <GrevmExecutor<_, _, _> as ParallelExecutor>::transact_system_txn(
                &mut ex,
                evm_env.clone(),
                Vec::new(),
                build_sys_tx_env(),
            )
            .expect("grevm system tx");
            let out = <GrevmExecutor<_, _, _> as ParallelExecutor>::execute(&mut ex, &recovered)
                .expect("grevm execute");
            (out.state, r.is_success())
        };

        eprintln!("d4 serial system tx success = {serial_sys_ok}");
        eprintln!("d4 grevm  system tx success = {grevm_sys_ok}");
        assert!(serial_sys_ok, "serial system tx must succeed (SSTORE onBlockStart shape)");
        assert!(grevm_sys_ok, "grevm system tx must succeed (SSTORE onBlockStart shape)");

        // Sorted account-by-account diff of the two bundle.state maps.
        let serial_addrs: std::collections::BTreeSet<_> = serial_bundle.state.keys().collect();
        let grevm_addrs: std::collections::BTreeSet<_> = grevm_bundle.state.keys().collect();

        eprintln!("d4 serial state addrs: {serial_addrs:?}");
        eprintln!("d4 grevm  state addrs: {grevm_addrs:?}");

        // Dump the first diverging account (if any) for the verdict.
        let all: std::collections::BTreeSet<_> =
            serial_addrs.union(&grevm_addrs).copied().collect();
        let mut first_diff: Option<Address> = None;
        for addr in &all {
            let s = serial_bundle.state.get(*addr);
            let g = grevm_bundle.state.get(*addr);
            let s_info = s.and_then(|a| a.info.as_ref()).map(|i| (i.nonce, i.balance, i.code_hash));
            let g_info = g.and_then(|a| a.info.as_ref()).map(|i| (i.nonce, i.balance, i.code_hash));
            let s_storage: std::collections::BTreeMap<_, _> = s
                .map(|a| a.storage.iter().map(|(k, v)| (*k, v.present_value)).collect())
                .unwrap_or_default();
            let g_storage: std::collections::BTreeMap<_, _> = g
                .map(|a| a.storage.iter().map(|(k, v)| (*k, v.present_value)).collect())
                .unwrap_or_default();
            let s_status = s.map(|a| a.status);
            let g_status = g.map(|a| a.status);
            if s_info != g_info || s_storage != g_storage || s_status != g_status {
                eprintln!("d4 DIVERGENCE at {addr:?}:");
                eprintln!("   serial info={s_info:?} status={s_status:?} storage={s_storage:?}");
                eprintln!("   grevm  info={g_info:?} status={g_status:?} storage={g_storage:?}");
                if first_diff.is_none() {
                    first_diff = Some(**addr);
                }
            }
        }

        // Account SET must match.
        assert_eq!(
            serial_addrs, grevm_addrs,
            "D4 BUNDLE ACCOUNT SET DIVERGENCE (system metadata tx + plain block): \
             serial={serial_addrs:?} grevm={grevm_addrs:?}"
        );

        // Full account-by-account equality (info + storage + status). Bundle
        // equality ⟹ HashedPostState::from_bundle_state equality ⟹ identical
        // state_root contribution for this block.
        assert!(
            first_diff.is_none(),
            "D4 BUNDLE DIVERGENCE between serial revm and grevm for the system \
             metadata tx + plain block — first diverging account: {first_diff:?}. \
             This would make A2-1 a REAL single-block consensus bug."
        );

        // Belt-and-suspenders: assert the entire bundle.state map is equal.
        for addr in &all {
            let s = serial_bundle.state.get(*addr);
            let g = grevm_bundle.state.get(*addr);
            assert_eq!(
                s.and_then(|a| a.info.as_ref()).map(|i| (i.nonce, i.balance, i.code_hash)),
                g.and_then(|a| a.info.as_ref()).map(|i| (i.nonce, i.balance, i.code_hash)),
                "d4 account info diverges at {addr:?}"
            );
            let s_storage: std::collections::BTreeMap<_, _> = s
                .map(|a| a.storage.iter().map(|(k, v)| (*k, v.present_value)).collect())
                .unwrap_or_default();
            let g_storage: std::collections::BTreeMap<_, _> = g
                .map(|a| a.storage.iter().map(|(k, v)| (*k, v.present_value)).collect())
                .unwrap_or_default();
            assert_eq!(s_storage, g_storage, "d4 account storage diverges at {addr:?}");
        }
    }
}
