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
            if hf.fork(GravityHardfork::Alpha).active_at_timestamp(block.timestamp) {
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
        .is_fork_active_at_timestamp(GravityHardfork::Alpha, block.header().timestamp())
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
}
