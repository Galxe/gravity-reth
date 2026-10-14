//! Block-level Gravity execution steps.

use super::{
    epoch_block::{active_validator_index, changes_epoch, on_block_start_txn, BodyFacts},
    GravityChainReader, GravityEvm, GravityEvmFactory,
};
use crate::{
    hardfork::{alpha, common::DbHardforkState, eip_2935, gamma, oracle_task_install},
    RethReceiptBuilder,
};
use alloc::{format, sync::Arc};
use alloy_consensus::TxType;
use alloy_evm::{
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        ExecutableTx, GasOutput, StateDB,
    },
    eth::{
        EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthEvmContext, EthTxResult,
    },
    Evm, EvmEnv, FromRecoveredTx,
};
use alloy_primitives::B256;
use alloy_sol_types::SolCall;
use reth_chainspec::{
    gravity_system_contracts::{
        getActiveValidatorsCall, NIL_PROPOSER_INDEX, TIMESTAMP_ADDR, VALIDATOR_MANAGER_ADDR,
    },
    ChainSpec, EthChainSpec, EthereumHardforks, GravityHardfork, SYSTEM_CALLER,
};
use reth_ethereum_primitives::{Receipt, TransactionSigned};
use revm::{
    context::{result::HaltReason, Block as _, BlockEnv, TxEnv},
    context_interface::result::ResultAndState,
    primitives::hardfork::SpecId,
    Database, DatabaseCommit, Inspector,
};

/// Execution context of a Gravity block.
#[derive(Debug, Clone)]
pub struct GravityBlockExecutionCtx<'a> {
    /// The Ethereum execution context.
    pub inner: EthBlockExecutionCtx<'a>,
    /// Set when the block is a committed Gravity block replayed as it was executed on chain;
    /// `None` for any other block (simulation, caller-provided block, pending block, engine
    /// payload), which gets only Ethereum's block-level steps.
    pub chain_block: Option<ChainBlock>,
}

/// A committed Gravity block, as far as its block-level execution steps need to know.
///
/// Computed when the context is built, because the executor sees no block body before
/// executing the first transaction.
#[derive(Debug, Clone, Copy)]
pub struct ChainBlock {
    pub(crate) block_hash: B256,
    pub(crate) facts: BodyFacts,
}

impl ChainBlock {
    pub(crate) const fn new(block_hash: B256, facts: BodyFacts) -> Self {
        Self { block_hash, facts }
    }
}

/// Creates [`GravityBlockExecutor`]s.
#[derive(Debug, Clone)]
pub struct GravityBlockExecutorFactory {
    inner: EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, GravityEvmFactory>,
    reader: Arc<dyn GravityChainReader>,
}

impl GravityBlockExecutorFactory {
    pub(crate) fn new(chain_spec: Arc<ChainSpec>, reader: Arc<dyn GravityChainReader>) -> Self {
        let evm_factory = GravityEvmFactory::new(chain_spec.clone(), reader.clone());
        Self {
            inner: EthBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                evm_factory,
            ),
            reader,
        }
    }

    /// Returns the chain spec.
    pub const fn spec(&self) -> &Arc<ChainSpec> {
        self.inner.spec()
    }
}

impl BlockExecutorFactory for GravityBlockExecutorFactory {
    type EvmFactory = GravityEvmFactory;
    type TxExecutionResult = EthTxResult<HaltReason, TxType>;
    type ExecutionCtx<'a> = GravityBlockExecutionCtx<'a>;
    type Transaction = TransactionSigned;
    type Receipt = Receipt;
    type Executor<'a, DB: StateDB, I: Inspector<EthEvmContext<DB>>> =
        GravityBlockExecutor<'a, GravityEvm<DB, I>>;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: GravityEvm<DB, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a, DB, I>
    where
        DB: StateDB,
        I: Inspector<EthEvmContext<DB>>,
    {
        GravityBlockExecutor {
            inner: EthBlockExecutor::new(
                evm,
                ctx.inner,
                self.inner.spec(),
                self.inner.receipt_builder(),
            ),
            chain_block: ctx.chain_block,
            reader: self.reader.as_ref(),
            epoch_change: false,
            parent_timestamp: None,
        }
    }
}

/// Executes a Gravity block: [`EthBlockExecutor`] plus Gravity's block-level steps.
///
/// Transactions run through [`GravityEvm`], which carries the per-transaction rules. The
/// block-level steps depend on what the block is:
///
/// - **A committed chain block** ([`GravityBlockExecutionCtx::chain_block`]) gets every step the
///   pipe applied to it, including the one-shot hardfork changes: the Prague `HISTORY_STORAGE`
///   deployment, the Alpha `SYSTEM_CALLER` balance reset, the Gamma migration, and the epoch-change
///   handling.
/// - **A set of user transactions** only gets the steps of the forks active at its timestamp: the
///   EIP-2935/EIP-4788 system calls and the Ethereum post-execution changes. It is not chain
///   history, so there is no one-shot change to reproduce.
#[derive(Debug)]
pub struct GravityBlockExecutor<'a, E> {
    inner: EthBlockExecutor<'a, E, &'a Arc<ChainSpec>, &'a RethReceiptBuilder>,
    chain_block: Option<ChainBlock>,
    reader: &'a dyn GravityChainReader,
    /// Whether the chain block changed the epoch. The pipe runs no block-level step on such a
    /// block besides the one-shot hardfork changes.
    epoch_change: bool,
    parent_timestamp: Option<u64>,
}

impl<E> GravityBlockExecutor<'_, E>
where
    E: Evm<DB: StateDB, Tx = TxEnv, HaltReason = HaltReason, Spec = SpecId, BlockEnv = BlockEnv>,
{
    fn block_number(&self) -> u64 {
        self.inner.evm.block().number().saturating_to()
    }

    fn block_timestamp(&self) -> u64 {
        self.inner.evm.block().timestamp().saturating_to()
    }

    /// Applies the Prague and Alpha activation-block changes, in the pipe's order.
    fn apply_activation_changes(&mut self) -> Result<(), BlockExecutionError> {
        let (number, timestamp) = (self.block_number(), self.block_timestamp());
        let chain_spec = self.inner.spec.as_ref();
        // A block before both forks cannot activate either; skip reading the parent header.
        if !chain_spec.is_prague_active_at_timestamp(timestamp) &&
            !chain_spec
                .gravity_hardforks()
                .is_fork_active_at_timestamp(GravityHardfork::Alpha, timestamp)
        {
            return Ok(())
        }

        let parent_hash = self.inner.ctx.parent_hash;
        let parent_timestamp = self
            .reader
            .header_timestamp(parent_hash)
            .map_err(BlockExecutionError::other)?
            .ok_or_else(|| {
                BlockExecutionError::msg(format!(
                    "parent header {parent_hash} of block {number} not found"
                ))
            })?;
        let mut state = DbHardforkState(self.inner.evm.db_mut());
        eip_2935::apply_state_changes_for_block(
            &mut state,
            chain_spec,
            timestamp,
            parent_timestamp,
            number,
        )?;
        alpha::apply_state_changes_for_block(
            &mut state,
            chain_spec,
            timestamp,
            parent_timestamp,
            number,
        )
    }

    /// Re-executes the `onBlockStart` a pre-Alpha DKG epoch-change block executed but left out
    /// of its body, rebuilt from chain state. It has no receipt and takes no transaction index.
    fn replay_omitted_on_block_start(
        &mut self,
        block_hash: B256,
    ) -> Result<(), BlockExecutionError> {
        let evm = &mut self.inner.evm;

        // Step 1: rebuild the `onBlockStart` arguments from chain state.
        let beneficiary = evm.block().beneficiary();
        let proposer_index = if beneficiary.is_zero() {
            // gravity-sdk leaves the beneficiary zero for a NIL block.
            NIL_PROPOSER_INDEX
        } else {
            // The active set only changes in `finishTransition`, so the parent state still maps
            // the proposer index to the beneficiary. The view call is not committed.
            let active_validators = evm
                .transact_system_call(
                    SYSTEM_CALLER,
                    VALIDATOR_MANAGER_ADDR,
                    getActiveValidatorsCall {}.abi_encode().into(),
                )
                .map_err(BlockExecutionError::other)?
                .result;
            active_validator_index(beneficiary, &active_validators)?
        };
        // `Timestamp.microseconds` is that contract's only state variable (slot 0), and nothing
        // after `onBlockStart` in an epoch-change block writes it.
        let written = self
            .reader
            .storage_after_block(block_hash, TIMESTAMP_ADDR, B256::ZERO)
            .map_err(BlockExecutionError::other)?
            .unwrap_or_default();
        let timestamp_micros = u64::try_from(written).map_err(|_| {
            BlockExecutionError::msg(format!("Timestamp slot 0 holds non-u64 {written}"))
        })?;

        // Step 2: execute it the way the pipe did, taking the nonce right before the body's
        // `finishTransition`, and commit it.
        let evm = &mut self.inner.evm;
        let nonce = evm
            .db_mut()
            .basic(SYSTEM_CALLER)
            .map_err(BlockExecutionError::other)?
            .map(|account| account.nonce)
            .unwrap_or_default();
        let tx = on_block_start_txn(nonce, evm.block().basefee(), proposer_index, timestamp_micros);
        let ResultAndState { state, .. } = evm
            .transact_raw(TxEnv::from_recovered_tx(&tx, SYSTEM_CALLER))
            .map_err(|err| BlockExecutionError::evm(err, *tx.hash()))?;
        evm.db_mut().commit(state);
        Ok(())
    }
}

impl<'a, E> BlockExecutor for GravityBlockExecutor<'a, E>
where
    E: Evm<DB: StateDB, Tx = TxEnv, HaltReason = HaltReason, Spec = SpecId, BlockEnv = BlockEnv>,
{
    type Transaction = TransactionSigned;
    type Receipt = Receipt;
    type Evm = E;
    type Result = EthTxResult<HaltReason, TxType>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        let Some(chain_block) = self.chain_block else {
            return self.inner.apply_pre_execution_changes()
        };

        if reth_chainspec::oracle_task_install_config(self.inner.spec.as_ref())
            .map_err(BlockExecutionError::msg)?
            .is_some()
        {
            let parent_hash = self.inner.ctx.parent_hash;
            self.parent_timestamp = Some(
                self.reader
                    .header_timestamp(parent_hash)
                    .map_err(BlockExecutionError::other)?
                    .ok_or_else(|| {
                        BlockExecutionError::msg(format!("parent header {parent_hash} not found"))
                    })?,
            );
        }

        // Replaying chain history is only defined for blocks the chain committed.
        let number = self.block_number();
        let canonical = self.reader.canonical_hash(number).map_err(BlockExecutionError::other)?;
        if canonical != Some(chain_block.block_hash) {
            return Err(BlockExecutionError::msg(format!(
                "only committed blocks can be replayed: block {number} ({}) is not canonical",
                chain_block.block_hash
            )))
        }

        // Step 1: one-shot hardfork changes, before any transaction like the pipe.
        self.apply_activation_changes()?;

        // Step 2: an epoch change drops the block's user transactions, so only a block of
        // system transactions can be one; the receipts tell.
        if chain_block.facts.only_system_txs {
            let receipts = self
                .reader
                .receipts_by_block_hash(chain_block.block_hash)
                .map_err(BlockExecutionError::other)?
                .ok_or_else(|| {
                    BlockExecutionError::msg(format!(
                        "receipts of block {number} ({}) are unavailable",
                        chain_block.block_hash
                    ))
                })?;
            self.epoch_change = changes_epoch(&receipts);
        }

        // Step 3: the pipe returns before the EIP-2935/EIP-4788 system calls on an epoch change.
        if self.epoch_change {
            if chain_block.facts.is_pre_alpha_dkg_epoch_block {
                self.replay_omitted_on_block_start(chain_block.block_hash)?;
            }
            return Ok(())
        }

        // Step 4: the system calls. The pipe feeds EIP-2935 the parent's consensus block id
        // instead of its hash. The sealed header records that id only as
        // `parent_beacon_block_root`, by Gravity's header contract (see the pipe's
        // `create_block_for_executor`).
        //
        // Canonical execution runs these calls after the block's system transactions, replay
        // before them: the system transactions neither read nor write the EIP-2935 and EIP-4788
        // contracts, so the resulting state is the same.
        let parent_hash = self.inner.ctx.parent_hash;
        if let Some(parent_id) = self.inner.ctx.parent_beacon_block_root {
            self.inner.ctx.parent_hash = parent_id;
        }
        let result = self.inner.apply_pre_execution_changes();
        self.inner.ctx.parent_hash = parent_hash;
        result
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        self.inner.execute_transaction_without_commit(tx)
    }

    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        self.inner.commit_transaction(output)
    }

    fn finish(self) -> Result<(Self::Evm, BlockExecutionResult<Receipt>), BlockExecutionError> {
        let (number, timestamp) = (self.block_number(), self.block_timestamp());
        let chain_spec = self.inner.spec.clone();
        let parent_timestamp = self.parent_timestamp;
        let env = EvmEnv {
            cfg_env: self.inner.evm.cfg_env().clone(),
            block_env: self.inner.evm.block().clone(),
        };

        if self.epoch_change {
            let mut inner = self.inner;
            oracle_task_install::apply_state_changes_for_block(
                inner.evm.db_mut(),
                chain_spec.as_ref(),
                env,
                parent_timestamp,
                number,
                "rpc-committed-replay",
            )?;
            return Ok((
                inner.evm,
                BlockExecutionResult {
                    receipts: inner.receipts,
                    requests: Default::default(),
                    gas_used: inner.cumulative_tx_gas_used,
                    blob_gas_used: inner.blob_gas_used,
                },
            ))
        }

        let is_chain_block = self.chain_block.is_some();
        let (mut evm, result) = self.inner.finish()?;
        // The Gamma migration runs after the Ethereum post-execution changes, like grevm's.
        if is_chain_block &&
            chain_spec
                .gravity_hardforks()
                .is_fork_active_at_timestamp(GravityHardfork::Gamma, timestamp)
        {
            gamma::apply_state_changes_for_block(
                &mut DbHardforkState(evm.db_mut()),
                chain_spec.as_ref(),
                number,
                timestamp,
            )?;
        }
        if is_chain_block {
            oracle_task_install::apply_state_changes_for_block(
                evm.db_mut(),
                chain_spec.as_ref(),
                env,
                parent_timestamp,
                number,
                "rpc-committed-replay",
            )?;
        }
        Ok((evm, result))
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        self.inner.evm_mut()
    }

    fn evm(&self) -> &Self::Evm {
        self.inner.evm()
    }

    fn receipts(&self) -> &[Self::Receipt] {
        self.inner.receipts()
    }
}

#[cfg(test)]
mod oracle_task_install_tests {
    use super::*;
    use crate::hardfork::oracle_task_install::tests::{assert_installed, env, seeded_db, spec};
    use alloy_evm::EvmFactory;
    use alloy_primitives::{Address, Bytes, Log, LogData, U256};
    use alloy_sol_types::SolEvent;
    use reth_chainspec::gravity_system_contracts::{NewEpochEvent, RECONFIGURATION_ADDR};
    use reth_storage_errors::provider::ProviderResult;

    #[derive(Debug)]
    struct Reader {
        epoch: bool,
        parent: Option<u64>,
    }
    const HASH: B256 = B256::repeat_byte(7);
    impl GravityChainReader for Reader {
        fn mix_hash_by_number(&self, _: u64) -> ProviderResult<Option<B256>> {
            Ok(None)
        }
        fn canonical_hash(&self, _: u64) -> ProviderResult<Option<B256>> {
            Ok(Some(HASH))
        }
        fn header_timestamp(&self, _: B256) -> ProviderResult<Option<u64>> {
            Ok(self.parent)
        }
        fn receipts_by_block_hash(
            &self,
            _: B256,
        ) -> ProviderResult<Option<alloc::vec::Vec<Receipt>>> {
            let logs = if self.epoch {
                vec![Log {
                    address: RECONFIGURATION_ADDR,
                    data: LogData::new_unchecked(vec![NewEpochEvent::SIGNATURE_HASH], Bytes::new()),
                }]
            } else {
                vec![]
            };
            Ok(Some(vec![Receipt {
                tx_type: TxType::Legacy,
                success: true,
                cumulative_gas_used: 0,
                logs,
            }]))
        }
        fn storage_after_block(
            &self,
            _: B256,
            _: Address,
            _: B256,
        ) -> ProviderResult<Option<U256>> {
            Ok(None)
        }
    }

    fn ctx(chain: bool) -> GravityBlockExecutionCtx<'static> {
        GravityBlockExecutionCtx {
            inner: EthBlockExecutionCtx {
                parent_hash: B256::repeat_byte(6),
                parent_beacon_block_root: Some(B256::ZERO),
                ommers: &[],
                withdrawals: None,
                extra_data: Bytes::new(),
                tx_count_hint: Some(0),
                slot_number: None,
            },
            chain_block: chain.then_some(ChainBlock::new(
                HASH,
                BodyFacts { only_system_txs: true, is_pre_alpha_dkg_epoch_block: false },
            )),
        }
    }

    #[test]
    fn oracle_task_install_rpc_committed_normal_and_epoch_finish_install_identically() {
        for epoch in [false, true] {
            let factory = GravityBlockExecutorFactory::new(
                spec(),
                Arc::new(Reader { epoch, parent: Some(99) }),
            );
            let evm = factory.evm_factory().create_evm(seeded_db(), env(100));
            let mut executor = factory.create_executor(evm, ctx(true));
            executor.apply_pre_execution_changes().unwrap();
            assert_eq!(executor.epoch_change, epoch, "exercise the actual early-return branch");
            let (mut evm, result) = executor.finish().unwrap();
            assert_installed(evm.db_mut(), 100);
            assert_eq!(result.gas_used, 0);
            assert!(result.receipts.is_empty(), "installer creates no receipt");
            let following = GravityBlockExecutorFactory::new(
                spec(),
                Arc::new(Reader { epoch: false, parent: Some(100) }),
            );
            let next_evm = following.evm_factory().create_evm(evm.db_mut().clone(), env(101));
            let mut next_executor = following.create_executor(next_evm, ctx(true));
            next_executor.apply_pre_execution_changes().unwrap();
            let (mut next_evm, _) = next_executor.finish().unwrap();
            assert_installed(next_evm.db_mut(), 100);
        }
    }

    #[test]
    fn oracle_task_install_rpc_missing_parent_and_malformed_config_reject_before_mutation() {
        let factory = GravityBlockExecutorFactory::new(
            spec(),
            Arc::new(Reader { epoch: false, parent: None }),
        );
        let evm = factory.evm_factory().create_evm(seeded_db(), env(100));
        let mut executor = factory.create_executor(evm, ctx(true));
        assert!(executor.apply_pre_execution_changes().is_err());
        assert_eq!(executor.evm_mut().db_mut().basic(SYSTEM_CALLER).unwrap(), None);
        let mut invalid = (*spec()).clone();
        invalid
            .genesis
            .config
            .extra_fields
            .insert("oracleTaskInstallTime".into(), serde_json::json!("100"));
        let factory = GravityBlockExecutorFactory::new(
            Arc::new(invalid),
            Arc::new(Reader { epoch: false, parent: Some(99) }),
        );
        let evm = factory.evm_factory().create_evm(seeded_db(), env(100));
        let mut executor = factory.create_executor(evm, ctx(true));
        assert!(executor.apply_pre_execution_changes().is_err());
        assert_eq!(executor.evm_mut().db_mut().basic(SYSTEM_CALLER).unwrap(), None);
    }

    #[test]
    fn oracle_task_install_rpc_simulation_does_not_install() {
        let factory = GravityBlockExecutorFactory::new(
            spec(),
            Arc::new(Reader { epoch: false, parent: None }),
        );
        let evm = factory.evm_factory().create_evm(seeded_db(), env(100));
        let mut executor = factory.create_executor(evm, ctx(false));
        executor.apply_pre_execution_changes().unwrap();
        let (mut evm, _) = executor.finish().unwrap();
        // NativeOracle default mapping slot2 remains zero after simulation.
        use alloy_primitives::keccak256;
        use alloy_sol_types::SolValue;
        let slot = U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
        assert_eq!(
            evm.db_mut().storage(crate::hardfork::gamma::NATIVE_ORACLE_ADDRESS, slot).unwrap(),
            U256::ZERO
        );
    }
}
