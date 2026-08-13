//! `OracleV1` testnet hardfork state transition.
//!
//! At the configured `oracleV1Block`, this hook replaces the runtime bytecode
//! of `NativeOracle` and `OracleTaskConfig`. It preserves each account's
//! balance, nonce, account id, and complete storage trie.
//!
//! Safety invariant: do not wire this migration only through the legacy Grevm
//! `ethereum/evm/hardfork/common.rs` helper. That helper mutates Grevm state
//! directly, so the `disable-grevm` serial path would observe different state.
//! This module constructs one canonical [`EvmState`] and submits it through
//! [`ParallelExecutor::apply_state_change`], which is shared by serial and
//! Grevm execution.

use alloy_primitives::{address, b256, Address, Bytes, B256};
use reth_chainspec::{ChainSpec, EthChainSpec, GravityHardfork};
use reth_evm::{execute::BlockExecutionError, parallel_execute::ParallelExecutor};
use reth_primitives::EthPrimitives;
use revm::{
    bytecode::Bytecode,
    state::{Account, AccountInfo, AccountStatus, EvmState},
};
use tracing::info;

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

pub(crate) const ORACLE_V1_CONTRACTS_COMMIT: &str = "3bbc0b71bbccbeec89c706312fb2636723b594fa";

pub(crate) const NATIVE_ORACLE_ADDRESS: Address =
    address!("00000000000000000000000000000001625f4000");
pub(crate) const ORACLE_TASK_CONFIG_ADDRESS: Address =
    address!("00000000000000000000000000000001625f1009");

pub(crate) const NATIVE_ORACLE_PRE_FORK_CODE_HASH: B256 =
    b256!("30dd3888ce26735c0d6c5a036b48a1de668dd5506efa7588ce450f976da28255");
pub(crate) const ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH: B256 =
    b256!("74127baf705119810746598b2695ff5fa38f94bd778f0edae46799ffd3606bda");

pub(crate) const NATIVE_ORACLE_POST_FORK_CODE_HASH: B256 =
    b256!("981087ccdaa0b7843960782e99b078ccdd3820b331f86ce337d9750c5565d984");
pub(crate) const ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH: B256 =
    b256!("a21bf93e6123b0104b9ea851b8154fb342a5b576c22c71f15f851e266faa9f7f");

const NATIVE_ORACLE_RUNTIME: &[u8] =
    include_bytes!("hardfork/bytecodes/oracle_v1/NativeOracle.bin");
const ORACLE_TASK_CONFIG_RUNTIME: &[u8] =
    include_bytes!("hardfork/bytecodes/oracle_v1/OracleTaskConfig.bin");

#[derive(Clone, Copy)]
struct CodeUpgrade {
    name: &'static str,
    address: Address,
    pre_fork_hash: B256,
    post_fork_hash: B256,
    runtime: &'static [u8],
}

const UPGRADES: [CodeUpgrade; 2] = [
    CodeUpgrade {
        name: "NativeOracle",
        address: NATIVE_ORACLE_ADDRESS,
        pre_fork_hash: NATIVE_ORACLE_PRE_FORK_CODE_HASH,
        post_fork_hash: NATIVE_ORACLE_POST_FORK_CODE_HASH,
        runtime: NATIVE_ORACLE_RUNTIME,
    },
    CodeUpgrade {
        name: "OracleTaskConfig",
        address: ORACLE_TASK_CONFIG_ADDRESS,
        pre_fork_hash: ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH,
        post_fork_hash: ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH,
        runtime: ORACLE_TASK_CONFIG_RUNTIME,
    },
];

/// Apply the `OracleV1` code-only migration at the configured activation block.
///
/// Both accounts must be on the exact pre-fork codehash. A fully post-fork
/// state is accepted as an idempotent replay, while missing, unknown, or
/// partially upgraded state fails closed before any diff is committed.
pub(crate) fn apply_state_changes_for_block(
    executor: Executor<'_>,
    chain_spec: &ChainSpec,
    block_number: u64,
) -> Result<(), BlockExecutionError> {
    if !chain_spec
        .gravity_hardforks()
        .fork(GravityHardfork::OracleV1)
        .transitions_at_block(block_number)
    {
        return Ok(())
    }

    let mut previous_accounts = Vec::with_capacity(UPGRADES.len());
    let mut pre_fork_count = 0;
    let mut post_fork_count = 0;

    for upgrade in UPGRADES {
        let info = executor.basic(upgrade.address)?.ok_or_else(|| {
            BlockExecutionError::msg(format!(
                "OracleV1: {} account is missing at {}",
                upgrade.name, upgrade.address
            ))
        })?;

        if info.code_hash == upgrade.pre_fork_hash {
            pre_fork_count += 1;
        } else if info.code_hash == upgrade.post_fork_hash {
            post_fork_count += 1;
        } else {
            return Err(BlockExecutionError::msg(format!(
                "OracleV1: {} has unexpected pre-state codehash {}; expected {}",
                upgrade.name, info.code_hash, upgrade.pre_fork_hash
            )))
        }
        previous_accounts.push(info);
    }

    if post_fork_count == UPGRADES.len() {
        return Ok(())
    }
    if pre_fork_count != UPGRADES.len() {
        return Err(BlockExecutionError::msg(
            "OracleV1: partial oracle system-contract upgrade detected",
        ))
    }

    let mut state_diff = EvmState::default();
    for (upgrade, previous) in UPGRADES.into_iter().zip(previous_accounts) {
        let new_info = AccountInfo {
            code_hash: upgrade.post_fork_hash,
            code: Some(Bytecode::new_raw(Bytes::from_static(upgrade.runtime))),
            ..previous
        };

        let mut account = Account::from(new_info);
        account.status = AccountStatus::Touched;
        state_diff.insert(upgrade.address, account);
    }

    executor.apply_state_change(state_diff)?;
    info!(
        target: "execute_ordered_block",
        block_number,
        contracts_commit = ORACLE_V1_CONTRACTS_COMMIT,
        native_oracle_code_hash = ?NATIVE_ORACLE_POST_FORK_CODE_HASH,
        oracle_task_config_code_hash = ?ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH,
        "applied OracleV1 oracle system-contract migration"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{keccak256, U256};
    use reth_chainspec::{ChainHardforks, ChainSpecBuilder, ForkCondition, MAINNET};
    use reth_evm::{
        execute::BasicBlockExecutor,
        parallel_execute::{ParallelExecutor, WrapExecutor},
    };
    use reth_evm_ethereum::{parallel_execute::GrevmExecutor, EthEvmConfig};
    use revm::{
        database::{CacheDB, EmptyDB},
        state::AccountId,
    };
    use std::sync::Arc;

    const ACTIVATION_BLOCK: u64 = 100;

    type SerialExecutor =
        WrapExecutor<CacheDB<EmptyDB>, BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>>;
    type ParallelGrevmExecutor =
        GrevmExecutor<CacheDB<EmptyDB>, EthEvmConfig, reth_chainspec::ChainSpec>;

    fn oracle_v1_chainspec() -> Arc<ChainSpec> {
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks = ChainHardforks::from([(
            GravityHardfork::OracleV1,
            ForkCondition::Block(ACTIVATION_BLOCK),
        )]);
        Arc::new(spec)
    }

    fn account(code_hash: B256, balance: u64, nonce: u64) -> AccountInfo {
        AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash,
            code: None,
            account_id: AccountId::new(nonce as usize),
        }
    }

    fn seed_db(native_hash: B256, task_config_hash: B256) -> CacheDB<EmptyDB> {
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(NATIVE_ORACLE_ADDRESS, account(native_hash, 11, 7));
        db.insert_account_info(ORACLE_TASK_CONFIG_ADDRESS, account(task_config_hash, 22, 9));
        db
    }

    fn serial_executor(chain_spec: Arc<ChainSpec>, db: CacheDB<EmptyDB>) -> SerialExecutor {
        let evm_config = EthEvmConfig::new(chain_spec);
        WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))
    }

    fn grevm_executor(chain_spec: Arc<ChainSpec>, db: CacheDB<EmptyDB>) -> ParallelGrevmExecutor {
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        GrevmExecutor::new(chain_spec, &evm_config, db)
    }

    #[test]
    fn embedded_runtime_hashes_match_contract_manifest() {
        assert_eq!(keccak256(NATIVE_ORACLE_RUNTIME), NATIVE_ORACLE_POST_FORK_CODE_HASH);
        assert_eq!(keccak256(ORACLE_TASK_CONFIG_RUNTIME), ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH);
    }

    #[test]
    fn activation_preserves_account_fields_and_does_not_patch_storage() {
        let chain_spec = oracle_v1_chainspec();
        let db = seed_db(NATIVE_ORACLE_PRE_FORK_CODE_HASH, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH);
        let mut executor = serial_executor(chain_spec.clone(), db);

        apply_state_changes_for_block(&mut executor, &chain_spec, ACTIVATION_BLOCK).unwrap();
        let bundle = executor.take_bundle();

        let native = bundle.state.get(&NATIVE_ORACLE_ADDRESS).unwrap();
        let native_info = native.info.as_ref().unwrap();
        assert_eq!(native_info.balance, U256::from(11));
        assert_eq!(native_info.nonce, 7);
        assert_eq!(native_info.account_id, AccountId::new(7));
        assert_eq!(native_info.code_hash, NATIVE_ORACLE_POST_FORK_CODE_HASH);
        assert!(native.storage.is_empty());

        let task_config = bundle.state.get(&ORACLE_TASK_CONFIG_ADDRESS).unwrap();
        let task_config_info = task_config.info.as_ref().unwrap();
        assert_eq!(task_config_info.balance, U256::from(22));
        assert_eq!(task_config_info.nonce, 9);
        assert_eq!(task_config_info.account_id, AccountId::new(9));
        assert_eq!(task_config_info.code_hash, ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH);
        assert!(task_config.storage.is_empty());
    }

    #[test]
    fn serial_and_grevm_apply_identical_state_diff() {
        let chain_spec = oracle_v1_chainspec();
        let db = seed_db(NATIVE_ORACLE_PRE_FORK_CODE_HASH, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH);
        let mut serial = serial_executor(chain_spec.clone(), db.clone());
        let mut grevm = grevm_executor(chain_spec.clone(), db);

        apply_state_changes_for_block(&mut serial, &chain_spec, ACTIVATION_BLOCK).unwrap();
        apply_state_changes_for_block(&mut grevm, &chain_spec, ACTIVATION_BLOCK).unwrap();

        let serial_bundle = serial.take_bundle();
        let grevm_bundle = grevm.take_bundle();
        assert_eq!(serial_bundle.state, grevm_bundle.state);
        assert_eq!(serial_bundle.contracts, grevm_bundle.contracts);
        assert_eq!(serial_bundle.reverts, grevm_bundle.reverts);
        assert_eq!(serial_bundle.state_size, grevm_bundle.state_size);
    }

    #[test]
    fn pre_and_post_activation_blocks_are_noops() {
        let chain_spec = oracle_v1_chainspec();
        for block_number in [ACTIVATION_BLOCK - 1, ACTIVATION_BLOCK + 1] {
            let db =
                seed_db(NATIVE_ORACLE_PRE_FORK_CODE_HASH, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH);
            let mut executor = serial_executor(chain_spec.clone(), db);
            apply_state_changes_for_block(&mut executor, &chain_spec, block_number).unwrap();
            assert!(executor.take_bundle().state.is_empty());
        }
    }

    #[test]
    fn fully_upgraded_state_is_idempotent() {
        let chain_spec = oracle_v1_chainspec();
        let db = seed_db(NATIVE_ORACLE_POST_FORK_CODE_HASH, ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH);
        let mut executor = serial_executor(chain_spec.clone(), db);

        apply_state_changes_for_block(&mut executor, &chain_spec, ACTIVATION_BLOCK).unwrap();
        assert!(executor.take_bundle().state.is_empty());
    }

    #[test]
    fn unknown_or_partial_pre_state_fails_before_commit() {
        let chain_spec = oracle_v1_chainspec();
        let cases = [
            (B256::ZERO, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH),
            (NATIVE_ORACLE_POST_FORK_CODE_HASH, ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH),
        ];

        for (native_hash, task_config_hash) in cases {
            let db = seed_db(native_hash, task_config_hash);
            let mut executor = serial_executor(chain_spec.clone(), db);
            assert!(apply_state_changes_for_block(&mut executor, &chain_spec, ACTIVATION_BLOCK)
                .is_err());
            assert!(executor.take_bundle().state.is_empty());
        }
    }

    #[test]
    fn missing_account_fails_before_commit() {
        let chain_spec = oracle_v1_chainspec();
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(
            NATIVE_ORACLE_ADDRESS,
            account(NATIVE_ORACLE_PRE_FORK_CODE_HASH, 11, 7),
        );
        let mut executor = serial_executor(chain_spec.clone(), db);

        assert!(
            apply_state_changes_for_block(&mut executor, &chain_spec, ACTIVATION_BLOCK).is_err()
        );
        assert!(executor.take_bundle().state.is_empty());
    }
}
