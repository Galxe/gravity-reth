//use crate::debug_ext::DEBUG_EXT;
use crate::{
    dao_fork::{DAO_HARDFORK_ACCOUNTS, DAO_HARDFORK_BENEFICIARY},
    debug_ext::DEBUG_EXT,
};
use alloy_consensus::BlockHeader;
use alloy_eips::{eip6110, eip7685::Requests};
use alloy_primitives::{
    map::{DefaultHashBuilder, HashMap},
    Address,
};
use reth_chainspec::{ChainSpec, EthereumHardfork, EthereumHardforks};
use reth_consensus::ConsensusError;
use reth_ethereum_consensus::validate_block_post_execution;
use reth_evm::{
    execute::{BlockExecutionError, BlockExecutionStrategy, BlockValidationError, ExecuteOutput},
    state_change::post_block_balance_increments,
    system_calls::{OnStateHook, SystemCaller},
    ConfigureEvm, Evm, ParallelDatabase,
};
use reth_grevm::{ParallelState, Scheduler};
use reth_primitives::{EthPrimitives, Receipt, RecoveredBlock};
use reth_primitives_traits::SignedTransaction;
use revm::{
    db::{states::State, WrapDatabaseRef},
    DatabaseCommit,
};
use revm_primitives::{Account, AccountStatus, EvmState};
use std::sync::Arc;

/// Grevm Block execution strategy for Ethereum.
#[allow(missing_debug_implementations)]
pub struct GrevmExecutionStrategy<DB, EvmConfig>
where
    EvmConfig: Clone,
{
    /// The chainspec
    chain_spec: Arc<ChainSpec>,
    /// How to create an EVM.
    evm_config: EvmConfig,
    /// Current state for block execution.
    state: Option<ParallelState<DB>>,
    /// Utility to call system smart contracts.
    system_caller: SystemCaller<EvmConfig, ChainSpec>,
}

impl<DB, EvmConfig> GrevmExecutionStrategy<DB, EvmConfig>
where
    EvmConfig: Clone,
    DB: ParallelDatabase,
{
    /// Creates a new [`EthExecutionStrategy`]
    pub fn new(db: DB, chain_spec: Arc<ChainSpec>, evm_config: EvmConfig) -> Self {
        let system_caller = SystemCaller::new(evm_config.clone(), chain_spec.clone());
        Self {
            state: Some(ParallelState::new(db, true, DEBUG_EXT.update_db_metrics)),
            chain_spec,
            evm_config,
            system_caller,
        }
    }
}

impl<'db, DB, EvmConfig> BlockExecutionStrategy<'db> for GrevmExecutionStrategy<DB, EvmConfig>
where
    DB: ParallelDatabase + 'db,
    EvmConfig: ConfigureEvm<
        Header = alloy_consensus::Header,
        Transaction = reth_primitives::TransactionSigned,
    >,
{
    type Error = BlockExecutionError;
    type Primitives = EthPrimitives;

    fn apply_pre_execution_changes(
        &mut self,
        block: &RecoveredBlock<reth_primitives::Block>,
    ) -> Result<(), Self::Error> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag = self.chain_spec.is_spurious_dragon_active_at_block(block.number());
        let state = self.state.as_mut().unwrap();
        state.set_state_clear_flag(state_clear_flag);

        let mut evm = self.evm_config.evm_for_block(WrapDatabaseRef(state), block.header());

        self.system_caller.apply_pre_execution_changes(block.header(), &mut evm)?;

        Ok(())
    }

    fn execute_transactions(
        &mut self,
        block: &RecoveredBlock<reth_primitives::Block>,
    ) -> Result<ExecuteOutput<Receipt>, Self::Error> {
        let evm_env = self.evm_config.evm_env(block.header());

        let mut txs = Vec::with_capacity(block.transaction_count());
        for (sender, tx) in block.transactions_with_sender() {
            txs.push(self.evm_config.tx_env(tx, *sender).into());
        }

        let spec_id = evm_env.spec.into();
        let env = revm_primitives::Env {
            cfg: evm_env.cfg_env,
            block: evm_env.block_env,
            ..Default::default()
        };
        let txs = Arc::new(txs);
        let state = self.state.take().unwrap();

        let (results, state) = if DEBUG_EXT.compare_with_seq_exec {
            let seq_state = {
                let mut seq_state = State::builder()
                    .with_database_ref(&state.database)
                    .with_bundle_update()
                    .build();
                seq_state.cache = state.cache.as_cache_state();
                seq_state.block_hashes.extend(
                    state.block_hashes.iter().map(|entry| (*entry.key(), entry.value().clone())),
                );
                let mut evm = self.evm_config.evm_for_block(&mut seq_state, block.header());
                for (sender, tx) in block.transactions_with_sender() {
                    let result_and_state =
                        evm.transact(self.evm_config.tx_env(tx, *sender)).map_err(move |err| {
                            // Ensure hash is calculated for error log, if not already done
                            BlockValidationError::EVM {
                                hash: tx.recalculate_hash(),
                                error: Box::new(err),
                            }
                        })?;
                    evm.db_mut().commit(result_and_state.state);
                }
                drop(evm);
                (seq_state.transition_state, seq_state.cache, seq_state.block_hashes)
            };

            let dump_block = DEBUG_EXT
                .dump_block_number
                .map_or(false, |dump_block_number| block.number == dump_block_number);
            if dump_block {
                crate::debug_ext::dump_block_env(
                    &revm_primitives::EnvWithHandlerCfg::new_with_spec_id(
                        Box::new(env.clone()),
                        spec_id,
                    ),
                    &txs.as_ref(),
                    &seq_state.1,
                    &seq_state.0.as_ref().unwrap(),
                    &seq_state.2,
                )
                .unwrap();
            }

            let executor =
                Scheduler::new(spec_id, env.clone(), txs.clone(), state, DEBUG_EXT.with_hints);
            let output = executor.parallel_execute(None).map_err(|e| BlockValidationError::EVM {
                hash: block.transactions_with_sender().nth(e.txid).unwrap().1.recalculate_hash(),
                error: Box::new(e.error),
            });
            let (results, parallel_state) = executor.take_result_and_state();

            let should_dump = DEBUG_EXT
                .dump_block_number
                .map_or(false, |dump_block_number| block.number == dump_block_number);

            if output.is_err() ||
                !crate::debug_ext::compare_transition_state(
                    seq_state.0.as_ref().unwrap(),
                    parallel_state.transition_state.as_ref().unwrap(),
                )
            {
                crate::debug_ext::dump_transitions(
                    block.number,
                    seq_state.0.as_ref().unwrap(),
                    "seq_transitions.json",
                )
                .unwrap();
                crate::debug_ext::dump_transitions(
                    block.number,
                    parallel_state.transition_state.as_ref().unwrap(),
                    "parallel_transitions.json",
                )
                .unwrap();

                if !dump_block {
                    // Block has been dumped already, no need to dump it again
                    crate::debug_ext::dump_block_env(
                        &revm_primitives::EnvWithHandlerCfg::new_with_spec_id(
                            Box::new(env),
                            spec_id,
                        ),
                        &txs.as_ref(),
                        &seq_state.1,
                        &seq_state.0.as_ref().unwrap(),
                        &seq_state.2,
                    )
                    .unwrap();
                }

                panic!("Transition state mismatch, block number: {}", block.number);
            }

            output?;

            (results, parallel_state)
        } else {
            let executor = Scheduler::new(spec_id, env, txs, state, DEBUG_EXT.with_hints);
            executor.parallel_execute(None).map_err(|e| BlockValidationError::EVM {
                hash: block.transactions_with_sender().nth(e.txid).unwrap().1.recalculate_hash(),
                error: Box::new(e.error),
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
        block: &RecoveredBlock<reth_primitives::Block>,
        receipts: &[Receipt],
    ) -> Result<Requests, Self::Error> {
        let mut evm = self
            .evm_config
            .evm_for_block(WrapDatabaseRef(self.state.as_mut().unwrap()), block.header());

        let requests = if self.chain_spec.is_prague_active_at_timestamp(block.timestamp) {
            // Collect all EIP-6110 deposits
            let deposit_requests =
                crate::eip6110::parse_deposits_from_receipts(&self.chain_spec, receipts)?;

            let mut requests = Requests::default();

            if !deposit_requests.is_empty() {
                requests.push_request_with_type(eip6110::DEPOSIT_REQUEST_TYPE, deposit_requests);
            }

            requests.extend(self.system_caller.apply_post_execution_changes(&mut evm)?);
            requests
        } else {
            Requests::default()
        };
        drop(evm);

        let mut balance_increments = post_block_balance_increments(&self.chain_spec, block);
        let state = self.state.as_mut().unwrap();

        // Irregular state change at Ethereum DAO hardfork
        if self.chain_spec.fork(EthereumHardfork::Dao).transitions_at_block(block.number()) {
            // drain balances from hardcoded addresses.
            let drained_balance: u128 = state
                .drain_balances(DAO_HARDFORK_ACCOUNTS)
                .map_err(|_| BlockValidationError::IncrementBalanceFailed)?
                .into_iter()
                .sum();

            // return balance to DAO beneficiary.
            *balance_increments.entry(DAO_HARDFORK_BENEFICIARY).or_default() += drained_balance;
        }
        // increment balances
        state
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;
        // call state hook with changes due to balance increments.
        let balance_state = balance_increment_state(&balance_increments, state)?;
        self.system_caller.on_state(&balance_state);

        Ok(requests)
    }

    fn state_ref(&self) -> &dyn reth_evm::State {
        self.state.as_ref().unwrap()
    }

    fn state_mut(&mut self) -> &mut dyn reth_evm::State {
        self.state.as_mut().unwrap()
    }

    fn into_state(mut self: Box<Self>) -> Box<dyn reth_evm::State + 'db> {
        Box::new(self.state.take().unwrap())
    }

    fn with_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        todo!("Implement state hook")
    }

    fn validate_block_post_execution(
        &self,
        block: &RecoveredBlock<reth_primitives::Block>,
        receipts: &[Receipt],
        requests: &Requests,
    ) -> Result<(), ConsensusError> {
        validate_block_post_execution(block, &self.chain_spec.clone(), receipts, requests)
    }
}

fn balance_increment_state<DB>(
    balance_increments: &HashMap<Address, u128, DefaultHashBuilder>,
    state: &ParallelState<DB>,
) -> Result<EvmState, BlockExecutionError>
where
    DB: ParallelDatabase,
{
    let mut load_account = |address: &Address| -> Result<(Address, Account), BlockExecutionError> {
        let info = state
            .cache
            .accounts
            .get(address)
            .and_then(|account| account.value().account.clone())
            .ok_or(BlockExecutionError::msg("could not load account for balance increment"))?;

        Ok((
            *address,
            Account { info, storage: Default::default(), status: AccountStatus::Touched },
        ))
    };

    balance_increments
        .iter()
        .filter(|(_, &balance)| balance != 0)
        .map(|(addr, _)| load_account(addr))
        .collect::<Result<EvmState, _>>()
}
