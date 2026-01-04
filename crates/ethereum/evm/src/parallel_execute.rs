//! Parallel EVM executor using Grevm

use crate::{RethReceiptBuilder, MintEvmFactory};
use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::BlockHeader;
use alloy_eips::{eip4895::Withdrawal, eip7685::Requests};
use alloy_evm::{
    block::{calc, StateChangePostBlockSource, StateChangeSource, SystemCaller},
    eth::{dao_fork, eip6110, spec::EthExecutorSpec, EthBlockExecutorFactory},
    EvmEnv,
};
use alloy_primitives::{map::HashMap, map::HashSet, Address, B256};
use gravity_primitives::get_gravity_config;
use grevm::{ParallelBundleState, ParallelState, Scheduler};
use parking_lot::Mutex;
use reth_chainspec::{EthChainSpec, EthereumHardfork, EthereumHardforks, Hardforks};
use reth_ethereum_primitives::{Block, EthPrimitives, Receipt};
use reth_evm::{
    execute::{
        BlockExecutionError, BlockValidationError, ExecuteOutput, InternalBlockExecutionError,
    },
    parallel_execute::ParallelExecutor,
    ConfigureEvm, ParallelDatabase,
};
use reth_execution_types::BlockExecutionResult;
use reth_primitives_traits::{BlockBody, NodePrimitives, RecoveredBlock, SignedTransaction};
use revm::{
    database::{
        states::bundle_state::BundleRetention, BundleState, TransitionState, WrapDatabaseRef,
    },
    state::{Account, AccountStatus, EvmState},
    DatabaseCommit,
};
use tracing::info;

/// Mint 请求结构
/// 用于在预编译合约和执行层之间传递 mint 请求
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MintRequest {
    /// 请求唯一标识（用于去重）
    pub request_id: B256,
    /// 接收地址
    pub recipient: Address,
    /// 要 mint 的数量
    pub amount: u128,
}

/// Mint 状态队列
/// 
/// 用于在预编译合约和执行层之间传递 mint 请求
/// 预编译合约将请求加入队列，在区块执行后的 `apply_post_execution_changes` 中
/// 从队列取出所有请求，合并到 `balance_increments` 中，通过 `increment_balances` 统一处理
#[derive(Debug, Clone)]
pub struct MintStateQueue {
    /// 内部的 mint 请求队列
    queue: Arc<Mutex<Vec<MintRequest>>>,
}

impl MintStateQueue {
    /// 创建新的 MintStateQueue
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 添加一个 mint 请求到队列
    pub fn push(&self, request: MintRequest) {
        self.queue.lock().push(request);
    }

    /// 获取并清空队列中的所有请求
    /// 
    /// 这个方法由 Executor 在 `apply_post_execution_changes` 时调用
    /// 返回所有待处理的 mint 请求，并清空队列
    pub fn drain(&self) -> Vec<MintRequest> {
        self.queue.lock().drain(..).collect()
    }

    /// 获取队列的共享引用（用于传递给预编译合约）
    pub fn as_shared(&self) -> Arc<Mutex<Vec<MintRequest>>> {
        self.queue.clone()
    }

    /// 检查队列是否为空
    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    /// 获取队列中的请求数量
    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }
}

impl Default for MintStateQueue {
    fn default() -> Self {
        Self::new()
    }
}

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
    /// Mint 请求队列（用于预编译合约 mint_token）
    mint_queue: MintStateQueue,
}

impl<DB, EvmConfig, ChainSpec> GrevmExecutor<DB, EvmConfig, ChainSpec>
where
    EvmConfig: Clone
        + ConfigureEvm<
            Primitives = EthPrimitives,
            BlockExecutorFactory = EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, MintEvmFactory>,
        >,
    DB: ParallelDatabase,
    ChainSpec: EthExecutorSpec + EthChainSpec + Hardforks + 'static,
{
    /// Creates a new [`GrevmExecutor`] - 使用全局 mint_queue
    pub fn new(chain_spec: Arc<ChainSpec>, evm_config: &EvmConfig, db: DB) -> Self {
        let system_caller = SystemCaller::new(chain_spec.clone());
        let report_db_metrics = get_gravity_config().report_db_metrics;
        Self {
            state: Some(ParallelState::new(db, true, report_db_metrics)),
            chain_spec,
            evm_config: evm_config.clone(),
            system_caller,
            mint_queue: crate::mint_evm_factory::global_mint_queue().clone(),
        }
    }

    /// Creates a new [`GrevmExecutor`] with a custom mint queue
    /// @deprecated 使用 new() 即可，它会自动使用全局队列
    pub fn new_with_mint_queue(
        chain_spec: Arc<ChainSpec>,
        evm_config: &EvmConfig,
        db: DB,
        _mint_queue: MintStateQueue,
    ) -> Self {
        // 忽略传入的 mint_queue，始终使用全局队列
        Self::new(chain_spec, evm_config, db)
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
            info!("lightman0104 block_number: {:?}, tx: {:?}", block.number(), tx);
            txs.push(self.evm_config.tx_env(tx));
        }

        let txs = Arc::new(txs);
        let state = self.state.take().unwrap();

        let (results, state) = {
            info!("lightman0104 block_number: {:?}, execute txs len: {:?}", block.number(), txs.len());
            let EvmEnv { cfg_env, block_env } = evm_env;
            let executor = Scheduler::new(cfg_env, block_env, txs, state, false, None);
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
            info!("lightman0104 block_number: {:?}, result: {:?}", block.number(), result);
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

        let mut balance_increments = post_block_balance_increments(&self.chain_spec, block);
        
        // 从预编译合约队列中收集 mint 请求
        // 使用 HashSet 基于 request_id 去重，自动过滤 Grevm 并行执行产生的重复请求
        let all_mint_requests = self.mint_queue.drain();
        let unique_requests: HashSet<MintRequest> = all_mint_requests.into_iter().collect();
        
        if !unique_requests.is_empty() {
            info!(
                target: "evm::executor",
                count = unique_requests.len(),
                block_number = block.number(),
                "Applying unique mint requests (deduplicated by request_id)"
            );
            
            for mint_request in unique_requests {
                info!(
                    target: "evm::executor",
                    request_id = ?mint_request.request_id,
                    recipient = ?mint_request.recipient,
                    amount = mint_request.amount,
                    "Adding mint balance increment"
                );
                *balance_increments.entry(mint_request.recipient).or_default() += mint_request.amount;
            }
        }
        
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
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;
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
        BlockExecutorFactory = EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, MintEvmFactory>,
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
        let state_mut =
            self.state.as_mut().expect("state should be set before calling merge_transitions");
        if let Some(transition_state) =
            state_mut.transition_state.as_mut().map(TransitionState::take)
        {
            state_mut.bundle_state.parallel_apply_transitions_and_create_reverts(
                transition_state,
                BundleRetention::Reverts,
            );
        }
        Ok(BlockExecutionResult { receipts, gas_used, requests })
    }

    fn take_bundle(&mut self) -> BundleState {
        self.state.as_mut().expect("state should be set before calling take_bundle").take_bundle()
    }

    fn size_hint(&self) -> usize {
        self.state
            .as_ref()
            .expect("state should be set before calling size_hint")
            .bundle_size_hint()
    }

    fn commit_changes(&mut self, changes: EvmState) {
        // Load all accounts in the changes map to ensure they are cached before committing.
        let state = self.state.as_mut().expect("state should be set before calling commit_changes");
        for address in changes.keys() {
            state.load_mut_cache_account(*address).unwrap();
        }
        state.commit(changes);
    }
}

#[inline]
fn post_block_balance_increments<ChainSpec, Block>(
    chain_spec: &ChainSpec,
    block: &RecoveredBlock<Block>,
) -> HashMap<Address, u128>
where
    ChainSpec: EthereumHardforks,
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

    // process withdrawals
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
    if spec.is_shanghai_active_at_timestamp(block_timestamp) {
        if let Some(withdrawals) = withdrawals {
            for withdrawal in withdrawals {
                if withdrawal.amount > 0 {
                    *balance_increments.entry(withdrawal.address).or_default() +=
                        withdrawal.amount_wei().to::<u128>();
                }
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
