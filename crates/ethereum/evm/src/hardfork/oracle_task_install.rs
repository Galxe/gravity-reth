//! Atomic, timestamp-crossing Receiver, Oracle runtime and bridge task installation.

use super::gamma::{
    NATIVE_ORACLE_ADDRESS, NATIVE_ORACLE_POST_FORK_CODE_HASH, ORACLE_TASK_CONFIG_ADDRESS,
    ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH,
};
use alloc::{format, vec::Vec};
use alloy_evm::{eth::EthEvmBuilder, Evm, EvmEnv};
use alloy_primitives::{address, b256, keccak256, Address, Bytes, B256, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use reth_chainspec::{oracle_task_install_config, EthChainSpec, OracleTaskInstallConfig};
use reth_evm::execute::BlockExecutionError;
use revm::{
    bytecode::Bytecode,
    state::{Account, AccountInfo, AccountStatus, EvmState},
    Database, DatabaseCommit,
};

/// Receiver authorized by the native-mint precompile.
pub const BRIDGE_RECEIVER: Address = address!("595475934ed7d9faa7fca28341c2ce583904a44e");
const GOVERNANCE: Address = address!("00000000000000000000000000000001625f3000");

// Contracts 174123adf25395d0490210a3fad46e6018e10366, solc 0.8.30,
// optimizer 200, via IR, Prague, metadata bytecode hash disabled.
pub(crate) const NATIVE_ORACLE_CALLBACK_CODE_HASH: B256 =
    b256!("13c995f14f5add575e656480be4c734a76bf1b4500097e8258f214c65e174a74");
const NATIVE_ORACLE_CALLBACK_RUNTIME: &[u8] =
    include_bytes!("bytecodes/oracle_task_install/NativeOracle.bin");
const BRIDGE_RECEIVER_TEMPLATE: &[u8] =
    include_bytes!("bytecodes/oracle_task_install/GBridgeReceiver.bin");
// Frozen solc immutableReferences, independently bound to the public getters.
const RECEIVER_BRIDGE_OFFSETS: [usize; 2] = [84, 832];
const RECEIVER_SOURCE_OFFSETS: [usize; 2] = [153, 790];

/// Construct the frozen Receiver runtime with its two constructor immutables.
/// The installer requires the resulting hash to equal `receiverCodeHash`.
fn bridge_receiver_runtime(source_id: U256, trusted_bridge: Address) -> Bytes {
    let mut runtime = BRIDGE_RECEIVER_TEMPLATE.to_vec();
    let bridge_word = trusted_bridge.into_word();
    for offset in RECEIVER_BRIDGE_OFFSETS {
        runtime[offset..offset + 32].copy_from_slice(bridge_word.as_slice());
    }
    for offset in RECEIVER_SOURCE_OFFSETS {
        runtime[offset..offset + 32].copy_from_slice(&source_id.to_be_bytes::<32>());
    }
    runtime.into()
}

sol! {
    struct OracleTask { bytes config; uint64 updatedAt; }
    struct SourceProgress { uint128 latestNonce; uint128 latestPosition; }
    struct DataRecord { uint64 recordedAt; uint256 blockNumber; bytes data; }
    function getSourceProgress(uint32 sourceType, uint256 sourceId) external view returns (SourceProgress);
    function getRecord(uint32 sourceType, uint256 sourceId, uint128 nonce) external view returns (DataRecord);
    function getSourceTypes() external view returns (uint32[]);
    function getSourceIds(uint32 sourceType) external view returns (uint256[]);
    function getTaskNames(uint32 sourceType, uint256 sourceId) external view returns (bytes32[]);
    function getTask(uint32 sourceType, uint256 sourceId, bytes32 taskName) external view returns (OracleTask);
    function setTask(uint32 sourceType, uint256 sourceId, bytes32 taskName, bytes config) external;
    function getDefaultCallback(uint32 sourceType) external view returns (address);
    function setDefaultCallback(uint32 sourceType, address callback) external;
    function trustedBridge() external view returns (address);
    function trustedSourceId() external view returns (uint256);
}

/// Validate before protocol or user transactions mutate the current state.
pub fn validate_config<S: EthChainSpec + ?Sized>(spec: &S) -> Result<(), BlockExecutionError> {
    oracle_task_install_config(spec).map(|_| ()).map_err(BlockExecutionError::msg)
}

/// Apply the crossing to the current post-block state, committing exactly once.
///
/// Calls use revm's fixed system-call gas budget, no normal transaction validation,
/// nonce increment, fees, receipt, or header gas. No custom mint precompile is installed.
pub fn apply_state_changes_for_block<DB, S>(
    db: &mut DB,
    spec: &S,
    env: EvmEnv,
    parent_timestamp: Option<u64>,
    block_number: u64,
    execution_path: &'static str,
) -> Result<(), BlockExecutionError>
where
    DB: Database + DatabaseCommit,
    S: EthChainSpec + ?Sized,
{
    let Some(config) = oracle_task_install_config(spec).map_err(BlockExecutionError::msg)? else {
        return Ok(())
    };
    let timestamp: u64 = env.block_env.timestamp.saturating_to();
    if timestamp < config.timestamp {
        return Ok(())
    }
    let parent = parent_timestamp.ok_or_else(|| failure("missing parent timestamp"))?;
    if parent >= config.timestamp {
        return Ok(())
    }
    let mut overlay = InstallationOverlay { base: db, changes: EvmState::default() };
    let native = overlay
        .basic(NATIVE_ORACLE_ADDRESS)
        .map_err(BlockExecutionError::other)?
        .ok_or_else(|| failure("NativeOracle account is missing"))?;
    let upgraded = native.code_hash == NATIVE_ORACLE_CALLBACK_CODE_HASH;
    if !upgraded && native.code_hash != NATIVE_ORACLE_POST_FORK_CODE_HASH {
        return Err(failure("unexpected NativeOracle runtime hash"))
    }
    check_code(&mut overlay, ORACLE_TASK_CONFIG_ADDRESS, ORACLE_TASK_CONFIG_POST_FORK_CODE_HASH)?;
    let receiver_installed = ensure_receiver(&mut overlay, &config)?;
    if query(&mut overlay, &env, BRIDGE_RECEIVER, trustedBridgeCall {})? != config.trusted_bridge ||
        query(&mut overlay, &env, BRIDGE_RECEIVER, trustedSourceIdCall {})? != config.source_id
    {
        return Err(failure("Receiver trust does not match installation inputs"))
    }

    let before = configuration(&mut overlay, &env, &config)?;
    let matching = before.matches(&config, timestamp);
    if upgraded && matching && !receiver_installed {
        tracing::info!(target: "reth::evm::hardfork", block_number, block_timestamp = timestamp,
            action = "idempotent", execution_path, "oracle_task_install_applied");
        return Ok(())
    }
    if upgraded || (!matching && !before.is_empty()) {
        return Err(failure("partial or conflicting bridge configuration"))
    }
    let mut replacement = Account::from(AccountInfo {
        code_hash: NATIVE_ORACLE_CALLBACK_CODE_HASH,
        code: Some(Bytecode::new_raw(Bytes::from_static(NATIVE_ORACLE_CALLBACK_RUNTIME))),
        ..native
    });
    replacement.mark_touch();
    overlay.commit(core::iter::once((NATIVE_ORACLE_ADDRESS, replacement)).collect());
    check_code(&mut overlay, NATIVE_ORACLE_ADDRESS, NATIVE_ORACLE_CALLBACK_CODE_HASH)?;
    if !matching {
        mutate(
            &mut overlay,
            &env,
            ORACLE_TASK_CONFIG_ADDRESS,
            setTaskCall {
                sourceType: 0,
                sourceId: config.source_id,
                taskName: config.task_name,
                config: config.task_config.clone(),
            },
        )?;
        mutate(
            &mut overlay,
            &env,
            NATIVE_ORACLE_ADDRESS,
            setDefaultCallbackCall { sourceType: 0, callback: BRIDGE_RECEIVER },
        )?;
    }
    let after = configuration(&mut overlay, &env, &config)?;
    check_code(&mut overlay, NATIVE_ORACLE_ADDRESS, NATIVE_ORACLE_CALLBACK_CODE_HASH)?;
    let expected_updated_at = if matching { before.updated_at } else { timestamp };
    if !after.matches(&config, timestamp) ||
        after.updated_at != expected_updated_at ||
        after.other_source_types() != before.other_source_types()
    {
        return Err(failure("installation postconditions failed"))
    }
    let changes = overlay.changes;
    db.commit(changes);
    tracing::info!(target: "reth::evm::hardfork", block_number, block_timestamp = timestamp,
        source_id = ?config.source_id, task_name = ?config.task_name,
        action = "installed", execution_path, "oracle_task_install_applied");
    Ok(())
}

fn failure(detail: &str) -> BlockExecutionError {
    BlockExecutionError::msg(format!("OracleTaskInstall: {detail}"))
}

/// Stage only an absent or unused empty-code account. A prefunding transfer must
/// not prevent activation, and existing account fields/storage must survive.
fn ensure_receiver<DB: Database>(
    overlay: &mut InstallationOverlay<'_, DB>,
    config: &OracleTaskInstallConfig,
) -> Result<bool, BlockExecutionError> {
    let runtime = bridge_receiver_runtime(config.source_id, config.trusted_bridge);
    if keccak256(&runtime) != config.receiver_code_hash {
        return Err(failure("Receiver runtime hash does not match installation inputs"))
    }
    let previous = overlay.basic(BRIDGE_RECEIVER).map_err(BlockExecutionError::other)?;
    if let Some(info) = &previous {
        if info.code_hash == config.receiver_code_hash {
            return Ok(false)
        }
        if info.nonce != 0 || (info.code_hash != B256::ZERO && info.code_hash != keccak256([])) {
            return Err(failure("Receiver account conflicts with installation inputs"))
        }
    }
    let created = previous.is_none();
    let mut receiver = Account::from(AccountInfo {
        nonce: 1,
        code_hash: config.receiver_code_hash,
        code: Some(Bytecode::new_raw(runtime)),
        ..previous.unwrap_or_default()
    });
    receiver.mark_touch();
    if created {
        receiver.status |= AccountStatus::Created;
    }
    overlay.commit(core::iter::once((BRIDGE_RECEIVER, receiver)).collect());
    Ok(true)
}

fn check_code<DB: Database>(
    db: &mut DB,
    address: Address,
    expected: B256,
) -> Result<(), BlockExecutionError> {
    let info = db
        .basic(address)
        .map_err(BlockExecutionError::other)?
        .ok_or_else(|| failure("required account is missing"))?;
    if info.code_hash != expected {
        return Err(failure(&format!("unexpected runtime hash at {address}")))
    }
    Ok(())
}

/// Reads pass through to the execution cache; runtime and setter diffs commit together.
struct InstallationOverlay<'a, DB> {
    base: &'a mut DB,
    changes: EvmState,
}

impl<DB> core::fmt::Debug for InstallationOverlay<'_, DB> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InstallationOverlay")
            .field("changes", &self.changes)
            .finish_non_exhaustive()
    }
}

impl<DB: Database> Database for InstallationOverlay<'_, DB> {
    type Error = DB::Error;
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(account) = self.changes.get(&address) {
            return Ok(Some(account.info.clone()))
        }
        self.base.basic(address)
    }
    fn code_by_hash(&mut self, hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(code) = self.changes.values().find_map(|account| {
            if account.info.code_hash == hash {
                account.info.code.clone()
            } else {
                None
            }
        }) {
            return Ok(code)
        }
        self.base.code_by_hash(hash)
    }
    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(slot) =
            self.changes.get(&address).and_then(|account| account.storage.get(&index))
        {
            return Ok(slot.present_value)
        }
        self.base.storage(address, index)
    }
    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.base.block_hash(number)
    }
}

impl<DB> DatabaseCommit for InstallationOverlay<'_, DB> {
    fn commit(&mut self, state: EvmState) {
        for (address, mut account) in state {
            if !account.is_touched() {
                continue
            }
            account.storage.retain(|_, slot| slot.is_changed());
            if let Some(previous) = self.changes.get_mut(&address) {
                previous.info = account.info;
                for (key, slot) in account.storage {
                    if let Some(original) = previous.storage.get_mut(&key) {
                        // The second call's original value is the first call's post-state.
                        // Keep the base original value for the single final commit.
                        original.present_value = slot.present_value;
                    } else {
                        previous.storage.insert(key, slot);
                    }
                }
            } else {
                self.changes.insert(address, account);
            }
        }
    }
}

fn system_call<DB: Database, C: SolCall>(
    db: &mut InstallationOverlay<'_, DB>,
    env: &EvmEnv,
    address: Address,
    call: C,
) -> Result<(Bytes, EvmState), BlockExecutionError> {
    let mut evm = EthEvmBuilder::new(db, env.clone()).build();
    let result = evm
        .transact_system_call(GOVERNANCE, address, call.abi_encode().into())
        .map_err(BlockExecutionError::other)?;
    if !result.result.is_success() {
        return Err(failure(&format!("system call 0x{:02x?} failed", C::SELECTOR)))
    }
    let output = result.result.output().cloned().unwrap_or_default();
    Ok((output, result.state))
}

fn query<DB: Database, C: SolCall>(
    db: &mut InstallationOverlay<'_, DB>,
    env: &EvmEnv,
    address: Address,
    call: C,
) -> Result<C::Return, BlockExecutionError> {
    let (output, _) = system_call(db, env, address, call)?;
    C::abi_decode_returns_validate(&output).map_err(BlockExecutionError::other)
}

fn mutate<DB: Database, C: SolCall>(
    db: &mut InstallationOverlay<'_, DB>,
    env: &EvmEnv,
    address: Address,
    call: C,
) -> Result<(), BlockExecutionError> {
    let (_, state) = system_call(db, env, address, call)?;
    db.commit(state);
    Ok(())
}

struct Configuration {
    source_types: Vec<u32>,
    source_ids: Vec<U256>,
    names: Vec<B256>,
    config: Bytes,
    updated_at: u64,
    default_callback: Address,
    specialized_callback: U256,
}

impl Configuration {
    fn other_source_types(&self) -> Vec<u32> {
        self.source_types.iter().copied().filter(|source| *source != 0).collect()
    }
    fn is_empty(&self) -> bool {
        !self.source_types.contains(&0) &&
            self.source_ids.is_empty() &&
            self.names.is_empty() &&
            self.config.is_empty() &&
            self.updated_at == 0 &&
            self.default_callback.is_zero() &&
            self.specialized_callback.is_zero()
    }
    fn matches(&self, expected: &OracleTaskInstallConfig, timestamp: u64) -> bool {
        self.source_types.iter().filter(|source| **source == 0).count() == 1 &&
            self.source_ids == [expected.source_id] &&
            self.names == [expected.task_name] &&
            self.config == expected.task_config &&
            self.updated_at > 0 &&
            self.updated_at <= timestamp &&
            self.default_callback == BRIDGE_RECEIVER &&
            self.specialized_callback.is_zero()
    }
}

fn configuration<DB: Database>(
    db: &mut InstallationOverlay<'_, DB>,
    env: &EvmEnv,
    config: &OracleTaskInstallConfig,
) -> Result<Configuration, BlockExecutionError> {
    let task = query(
        db,
        env,
        ORACLE_TASK_CONFIG_ADDRESS,
        getTaskCall { sourceType: 0, sourceId: config.source_id, taskName: config.task_name },
    )?;
    // Frozen Gamma layout: NativeOracle._callbacks is mapping slot 3.
    // getCallback resolves specialized OR default, so it cannot detect a redundant override.
    let outer = keccak256((U256::ZERO, U256::from(3)).abi_encode());
    let slot = U256::from_be_bytes(keccak256((config.source_id, outer).abi_encode()).0);
    Ok(Configuration {
        source_types: query(db, env, ORACLE_TASK_CONFIG_ADDRESS, getSourceTypesCall {})?,
        source_ids: query(db, env, ORACLE_TASK_CONFIG_ADDRESS, getSourceIdsCall { sourceType: 0 })?,
        names: query(
            db,
            env,
            ORACLE_TASK_CONFIG_ADDRESS,
            getTaskNamesCall { sourceType: 0, sourceId: config.source_id },
        )?,
        config: task.config,
        updated_at: task.updatedAt,
        default_callback: query(
            db,
            env,
            NATIVE_ORACLE_ADDRESS,
            getDefaultCallbackCall { sourceType: 0 },
        )?,
        specialized_callback: db
            .storage(NATIVE_ORACLE_ADDRESS, slot)
            .map_err(BlockExecutionError::other)?,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{parallel_execute::GrevmExecutor, EthEvmConfig};
    use alloy_genesis::Genesis;
    use reth_chainspec::{Chain, ChainSpec, ChainSpecBuilder, LONGEVITY_TESTNET_CHAIN_ID, MAINNET};
    use reth_evm::parallel_execute::ParallelExecutor;
    use revm::{
        context::CfgEnv,
        database::{CacheDB, EmptyDB},
        primitives::hardfork::SpecId,
        state::{AccountId, EvmStorageSlot, TransactionId},
    };
    use std::{io, sync::Arc};

    const T: u64 = 100;
    const BRIDGE: Address = address!("0000000000000000000000000000000000001234");
    const COINBASE: Address = address!("0000000000000000000000000000000000002345");
    type DB = CacheDB<EmptyDB>;

    #[test]
    fn oracle_task_install_runtime_matches_pr119_manifest() {
        assert_eq!(keccak256(NATIVE_ORACLE_CALLBACK_RUNTIME), NATIVE_ORACLE_CALLBACK_CODE_HASH);
        assert_ne!(NATIVE_ORACLE_CALLBACK_CODE_HASH, NATIVE_ORACLE_POST_FORK_CODE_HASH);
    }

    fn install<DB: Database + DatabaseCommit>(
        db: &mut DB,
        spec: &ChainSpec,
        env: EvmEnv,
        parent: Option<u64>,
        number: u64,
    ) -> Result<(), BlockExecutionError> {
        apply_state_changes_for_block(db, spec, env, parent, number, "unit-test")
    }

    fn receiver_code() -> Bytes {
        bridge_receiver_runtime(U256::from(123), BRIDGE)
    }

    pub(crate) fn spec() -> Arc<ChainSpec> {
        let mut genesis = Genesis::default();
        genesis.config.chain_id = LONGEVITY_TESTNET_CHAIN_ID;
        genesis.config.extra_fields.insert("gammaTime".into(), serde_json::json!(10));
        genesis.config.extra_fields.insert("oracleTaskInstallTime".into(), serde_json::json!(T));
        genesis.config.extra_fields.insert("oracleTaskInstall".into(), serde_json::json!({
            "sourceId":"0x7b", "taskName":format!("{:#x}", B256::repeat_byte(1)),
            "taskConfig":"0x616263", "receiverCodeHash":format!("{:#x}", keccak256(receiver_code())),
            "trustedBridge":format!("{BRIDGE:#x}")
        }));
        Arc::new(
            ChainSpecBuilder::from(&*MAINNET)
                .chain(Chain::from(LONGEVITY_TESTNET_CHAIN_ID))
                .genesis(genesis)
                .shanghai_activated()
                .cancun_activated()
                .prague_activated()
                .build(),
        )
    }

    pub(crate) fn env(timestamp: u64) -> EvmEnv {
        let mut env: EvmEnv = EvmEnv {
            cfg_env: CfgEnv::new_with_spec(SpecId::PRAGUE)
                .with_chain_id(LONGEVITY_TESTNET_CHAIN_ID),
            ..EvmEnv::default()
        };
        env.block_env.timestamp = U256::from(timestamp);
        env.block_env.beneficiary = COINBASE;
        env
    }

    fn insert_code(db: &mut DB, address: Address, code: Bytes) {
        db.insert_account_info(
            address,
            AccountInfo {
                code_hash: keccak256(&code),
                code: Some(Bytecode::new_raw(code)),
                nonce: 1,
                ..Default::default()
            },
        );
    }

    pub(crate) fn seeded_db() -> DB {
        let mut db = DB::new(EmptyDB::default());
        insert_code(
            &mut db,
            NATIVE_ORACLE_ADDRESS,
            Bytes::from_static(include_bytes!("bytecodes/gamma/NativeOracle.bin")),
        );
        insert_code(
            &mut db,
            ORACLE_TASK_CONFIG_ADDRESS,
            Bytes::from_static(include_bytes!("bytecodes/gamma/OracleTaskConfig.bin")),
        );
        insert_code(&mut db, GOVERNANCE, Bytes::from_static(&[0]));
        let mut governance = db.basic(GOVERNANCE).unwrap().unwrap();
        governance.nonce = 19;
        governance.balance = U256::from(444);
        db.insert_account_info(GOVERNANCE, governance);
        db.insert_account_info(
            COINBASE,
            AccountInfo { balance: U256::from(55), nonce: 7, ..Default::default() },
        );
        db
    }

    fn snapshot(db: &mut DB) -> Configuration {
        let spec = spec();
        let config = oracle_task_install_config(spec.as_ref()).unwrap().unwrap();
        configuration(
            &mut InstallationOverlay { base: db, changes: EvmState::default() },
            &env(T),
            &config,
        )
        .unwrap()
    }

    pub(crate) fn assert_installed(db: &mut DB, timestamp: u64) {
        assert_eq!(
            db.basic(BRIDGE_RECEIVER).unwrap().unwrap().code_hash,
            keccak256(receiver_code())
        );
        assert_eq!(
            db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
            NATIVE_ORACLE_CALLBACK_CODE_HASH
        );
        let config = oracle_task_install_config(spec().as_ref()).unwrap().unwrap();
        let state = snapshot(db);
        assert!(state.matches(&config, timestamp));
        assert_eq!(state.updated_at, timestamp);
        assert_eq!(state.source_ids, vec![U256::from(123)]);
        assert_eq!(state.names, vec![B256::repeat_byte(1)]);
        assert_eq!(state.config.as_ref(), b"abc");
    }

    #[test]
    fn oracle_task_install_receiver_immutables_and_prefunded_account_are_preserved() {
        for offsets in [RECEIVER_BRIDGE_OFFSETS, RECEIVER_SOURCE_OFFSETS] {
            for offset in offsets {
                assert_eq!(&BRIDGE_RECEIVER_TEMPLATE[offset..offset + 32], &[0u8; 32]);
            }
        }
        let spec = spec();
        let mut db = seeded_db();
        let original = AccountInfo {
            balance: U256::from(321),
            account_id: AccountId::new(51),
            ..Default::default()
        };
        db.insert_account_info(BRIDGE_RECEIVER, original.clone());
        db.insert_account_storage(BRIDGE_RECEIVER, U256::from(8), U256::from(99)).unwrap();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        assert_installed(&mut db, T);
        let receiver = db.basic(BRIDGE_RECEIVER).unwrap().unwrap();
        assert_eq!(receiver.balance, original.balance);
        assert_eq!(receiver.account_id, original.account_id);
        assert_eq!(receiver.nonce, 1);
        assert_eq!(db.storage(BRIDGE_RECEIVER, U256::from(8)).unwrap(), U256::from(99));
        let config = oracle_task_install_config(spec.as_ref()).unwrap().unwrap();
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        assert_eq!(
            query(&mut overlay, &env(T), BRIDGE_RECEIVER, trustedBridgeCall {}).unwrap(),
            BRIDGE
        );
        assert_eq!(
            query(&mut overlay, &env(T), BRIDGE_RECEIVER, trustedSourceIdCall {}).unwrap(),
            config.source_id
        );
    }

    #[test]
    fn oracle_task_install_matching_receiver_is_unchanged() {
        let spec = spec();
        let mut db = seeded_db();
        insert_code(&mut db, BRIDGE_RECEIVER, receiver_code());
        let mut receiver = db.basic(BRIDGE_RECEIVER).unwrap().unwrap();
        receiver.balance = U256::from(765);
        receiver.nonce = 9;
        receiver.account_id = AccountId::new(27);
        db.insert_account_info(BRIDGE_RECEIVER, receiver.clone());
        db.insert_account_storage(BRIDGE_RECEIVER, U256::from(8), U256::from(99)).unwrap();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap().unwrap(), receiver);
        assert_eq!(db.storage(BRIDGE_RECEIVER, U256::from(8)).unwrap(), U256::from(99));
        install(&mut db, spec.as_ref(), env(T + 1), Some(T - 1), 73).unwrap();
        assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap().unwrap(), receiver);
    }

    #[test]
    fn oracle_task_install_conflicting_receiver_and_partial_install_fail_without_commit() {
        let spec = spec();
        for nonce in [1, 2] {
            let mut db = seeded_db();
            db.insert_account_info(BRIDGE_RECEIVER, AccountInfo { nonce, ..Default::default() });
            let before = db.basic(BRIDGE_RECEIVER).unwrap();
            assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
            assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap(), before);
            assert_eq!(
                db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
                NATIVE_ORACLE_POST_FORK_CODE_HASH
            );
            assert!(snapshot(&mut db).is_empty());
        }
        let mut db = seeded_db();
        insert_code(&mut db, BRIDGE_RECEIVER, Bytes::from_static(&[0]));
        let before = db.basic(BRIDGE_RECEIVER).unwrap();
        assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
        assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap(), before);
        assert!(snapshot(&mut db).is_empty());
        let mut db = seeded_db();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        db.cache.accounts.remove(&BRIDGE_RECEIVER);
        assert!(install(&mut db, spec.as_ref(), env(T + 1), Some(T - 1), 73).is_err());
        assert!(db.basic(BRIDGE_RECEIVER).unwrap().is_none());
    }

    #[test]
    fn oracle_task_install_actual_setters_discovery_and_caller_preservation() {
        let spec = spec();
        let mut db = seeded_db();
        let caller = db.basic(GOVERNANCE).unwrap();
        let coinbase = db.basic(COINBASE).unwrap();
        let mut original_native = db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap();
        original_native.balance = U256::from(313);
        original_native.nonce = 27;
        original_native.account_id = AccountId::new(29);
        db.insert_account_info(NATIVE_ORACLE_ADDRESS, original_native.clone());
        // Independently compute the pinned nested mappings: legacy nonce slot1,
        // record slot0 (+1 is blockNumber), and packed SourceProgress slot5.
        let nested = |root: u64| {
            let source_type = keccak256((U256::ZERO, U256::from(root)).abi_encode());
            U256::from_be_bytes(keccak256((U256::from(123), source_type).abi_encode()).0)
        };
        let record = U256::from_be_bytes(keccak256((U256::from(3), nested(0)).abi_encode()).0);
        let sentinels = [
            (nested(1), U256::from(3)),
            (record, U256::from(9)),
            (record + U256::from(1), U256::from(77)),
            (nested(5), U256::from(4) | (U256::from(88) << 128)),
        ];
        for (slot, value) in sentinels {
            db.insert_account_storage(NATIVE_ORACLE_ADDRESS, slot, value).unwrap();
        }
        // Install an unrelated JWK task/default before the bridge boundary.
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        mutate(
            &mut overlay,
            &env(T - 10),
            ORACLE_TASK_CONFIG_ADDRESS,
            setTaskCall {
                sourceType: 1,
                sourceId: U256::from(99),
                taskName: B256::repeat_byte(9),
                config: Bytes::from_static(b"jwk"),
            },
        )
        .unwrap();
        mutate(
            &mut overlay,
            &env(T - 10),
            NATIVE_ORACLE_ADDRESS,
            setDefaultCallbackCall { sourceType: 1, callback: BRIDGE_RECEIVER },
        )
        .unwrap();
        let changes = overlay.changes;
        db.commit(changes);
        install(&mut db, spec.as_ref(), env(T + 3), Some(T - 2), 73).unwrap();
        let config = oracle_task_install_config(spec.as_ref()).unwrap().unwrap();
        let installed = snapshot(&mut db);
        assert!(installed.matches(&config, T + 3));
        assert_eq!(installed.updated_at, T + 3);
        assert_eq!(db.basic(GOVERNANCE).unwrap(), caller);
        assert_eq!(db.basic(COINBASE).unwrap(), coinbase);
        let native = db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap();
        assert_eq!(native.code_hash, NATIVE_ORACLE_CALLBACK_CODE_HASH);
        assert_eq!(native.balance, original_native.balance);
        assert_eq!(native.nonce, original_native.nonce);
        assert_eq!(native.account_id, original_native.account_id);
        for (slot, value) in sentinels {
            assert_eq!(db.storage(NATIVE_ORACLE_ADDRESS, slot).unwrap(), value);
        }
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        let progress = query(
            &mut overlay,
            &env(T + 3),
            NATIVE_ORACLE_ADDRESS,
            getSourceProgressCall { sourceType: 0, sourceId: U256::from(123) },
        )
        .unwrap();
        assert_eq!((progress.latestNonce, progress.latestPosition), (4, 88));
        let record = query(
            &mut overlay,
            &env(T + 3),
            NATIVE_ORACLE_ADDRESS,
            getRecordCall { sourceType: 0, sourceId: U256::from(123), nonce: 3 },
        )
        .unwrap();
        assert_eq!((record.recordedAt, record.blockNumber), (9, U256::from(77)));
        assert!(record.data.is_empty());
        let jwk = query(
            &mut overlay,
            &env(T + 3),
            ORACLE_TASK_CONFIG_ADDRESS,
            getTaskCall { sourceType: 1, sourceId: U256::from(99), taskName: B256::repeat_byte(9) },
        )
        .unwrap();
        assert_eq!(jwk.config.as_ref(), b"jwk");
        assert_eq!(jwk.updatedAt, T - 10);
        assert_eq!(
            query(
                &mut overlay,
                &env(T + 3),
                NATIVE_ORACLE_ADDRESS,
                getDefaultCallbackCall { sourceType: 1 }
            )
            .unwrap(),
            BRIDGE_RECEIVER
        );
        // Exact matching governance state is not rewritten at the crossing.
        install(&mut db, spec.as_ref(), env(T + 8), Some(T - 1), 74).unwrap();
        assert_eq!(snapshot(&mut db).updated_at, T + 3);
        // H+1 does not overwrite a subsequent governance edit.
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        mutate(
            &mut overlay,
            &env(T + 9),
            ORACLE_TASK_CONFIG_ADDRESS,
            setTaskCall {
                sourceType: 0,
                sourceId: config.source_id,
                taskName: config.task_name,
                config: Bytes::from_static(b"edited"),
            },
        )
        .unwrap();
        let changes = overlay.changes;
        db.commit(changes);
        install(&mut db, spec.as_ref(), env(T + 10), Some(T + 9), 75).unwrap();
        assert_eq!(snapshot(&mut db).config.as_ref(), b"edited");
    }

    #[test]
    fn oracle_task_install_crossing_and_missing_context_fail_before_commit() {
        let spec = spec();
        let mut db = seeded_db();
        install(&mut db, spec.as_ref(), env(T - 1), Some(T - 2), 72).unwrap();
        assert_eq!(
            db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
            NATIVE_ORACLE_POST_FORK_CODE_HASH
        );
        assert!(snapshot(&mut db).is_empty());
        assert!(install(&mut db, spec.as_ref(), env(T), None, 73).is_err());
        assert!(snapshot(&mut db).is_empty());
        install(&mut db, spec.as_ref(), env(T), Some(T), 73).unwrap();
        assert!(snapshot(&mut db).is_empty());
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        assert_eq!(snapshot(&mut db).updated_at, T);
        assert_installed(&mut db, T);
    }

    #[test]
    fn oracle_task_install_matching_old_configuration_upgrades_without_rewriting_timestamp() {
        let spec = spec();
        let mut db = seeded_db();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        // Characterize a governance-installed matching configuration on Gamma code.
        insert_code(
            &mut db,
            NATIVE_ORACLE_ADDRESS,
            Bytes::from_static(include_bytes!("bytecodes/gamma/NativeOracle.bin")),
        );
        install(&mut db, spec.as_ref(), env(T + 3), Some(T - 1), 73).unwrap();
        assert_eq!(snapshot(&mut db).updated_at, T);
        assert_installed(&mut db, T);
        let before = db.basic(NATIVE_ORACLE_ADDRESS).unwrap();
        install(&mut db, spec.as_ref(), env(T + 4), Some(T - 1), 73).unwrap();
        assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), before);
        assert_eq!(snapshot(&mut db).updated_at, T);
    }

    #[test]
    fn oracle_task_install_unknown_or_unconfigured_successor_runtime_rejects() {
        let spec = spec();
        for code in [Bytes::from_static(&[0]), Bytes::from_static(NATIVE_ORACLE_CALLBACK_RUNTIME)] {
            let mut db = seeded_db();
            insert_code(&mut db, NATIVE_ORACLE_ADDRESS, code);
            let before = db.basic(NATIVE_ORACLE_ADDRESS).unwrap();
            assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
            assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), before);
            // Task configuration was never committed even on an unexpected runtime.
            let mut task_overlay =
                InstallationOverlay { base: &mut db, changes: EvmState::default() };
            assert!(query(
                &mut task_overlay,
                &env(T),
                ORACLE_TASK_CONFIG_ADDRESS,
                getSourceTypesCall {}
            )
            .unwrap()
            .is_empty());
        }
    }

    #[test]
    fn oracle_task_install_overlay_resolves_uncommitted_runtime_by_hash() {
        let mut db = seeded_db();
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        let previous = overlay.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap();
        let mut replacement = Account::from(AccountInfo {
            code_hash: NATIVE_ORACLE_CALLBACK_CODE_HASH,
            code: Some(Bytecode::new_raw(Bytes::from_static(NATIVE_ORACLE_CALLBACK_RUNTIME))),
            ..previous
        });
        replacement.mark_touch();
        overlay.commit(core::iter::once((NATIVE_ORACLE_ADDRESS, replacement)).collect());
        assert_eq!(
            overlay.code_by_hash(NATIVE_ORACLE_CALLBACK_CODE_HASH).unwrap().original_bytes(),
            Bytes::from_static(NATIVE_ORACLE_CALLBACK_RUNTIME)
        );
        assert_eq!(
            overlay.base.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
            NATIVE_ORACLE_POST_FORK_CODE_HASH
        );
    }

    #[test]
    fn oracle_task_install_gamma_successor_is_scoped_and_preserves_h_plus_one_code() {
        use crate::hardfork::{common::DbHardforkState, gamma};
        let spec = spec();
        let mut db = seeded_db();
        gamma::apply_state_changes_for_block(
            &mut DbHardforkState(&mut db),
            spec.as_ref(),
            72,
            T - 1,
        )
        .unwrap();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        let upgraded = db.basic(NATIVE_ORACLE_ADDRESS).unwrap();
        gamma::apply_state_changes_for_block(
            &mut DbHardforkState(&mut db),
            spec.as_ref(),
            74,
            T + 1,
        )
        .unwrap();
        assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), upgraded);
        let mut disabled = (*spec).clone();
        disabled.genesis.config.extra_fields.remove("oracleTaskInstall");
        disabled.genesis.config.extra_fields.remove("oracleTaskInstallTime");
        let mut other_chain = (*spec).clone();
        other_chain.chain = Chain::from(1625);
        for (scope, timestamp) in
            [(spec.as_ref(), T - 1), (&disabled, T + 1), (&other_chain, T + 1)]
        {
            assert!(gamma::apply_state_changes_for_block(
                &mut DbHardforkState(&mut db),
                scope,
                74,
                timestamp
            )
            .is_err());
            assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), upgraded);
        }
        let mut wrong_task = seeded_db();
        insert_code(
            &mut wrong_task,
            NATIVE_ORACLE_ADDRESS,
            Bytes::from_static(NATIVE_ORACLE_CALLBACK_RUNTIME),
        );
        insert_code(&mut wrong_task, ORACLE_TASK_CONFIG_ADDRESS, Bytes::from_static(&[0]));
        assert!(gamma::apply_state_changes_for_block(
            &mut DbHardforkState(&mut wrong_task),
            spec.as_ref(),
            74,
            T + 1
        )
        .is_err());
    }

    #[test]
    fn oracle_task_install_partial_and_redundant_specialized_callback_reject() {
        let spec = spec();
        let config = oracle_task_install_config(spec.as_ref()).unwrap().unwrap();
        let mut db = seeded_db();
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        mutate(
            &mut overlay,
            &env(T - 1),
            ORACLE_TASK_CONFIG_ADDRESS,
            setTaskCall {
                sourceType: 0,
                sourceId: config.source_id,
                taskName: config.task_name,
                config: config.task_config.clone(),
            },
        )
        .unwrap();
        let changes = overlay.changes;
        db.commit(changes);
        assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
        assert!(snapshot(&mut db).default_callback.is_zero());
        let mut db = seeded_db();
        install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
        let outer = keccak256((U256::ZERO, U256::from(3)).abi_encode());
        let slot = U256::from_be_bytes(keccak256((config.source_id, outer).abi_encode()).0);
        db.insert_account_storage(
            NATIVE_ORACLE_ADDRESS,
            slot,
            U256::from_be_slice(BRIDGE_RECEIVER.as_slice()),
        )
        .unwrap();
        assert!(install(&mut db, spec.as_ref(), env(T + 1), Some(T - 1), 73).is_err());
    }

    #[test]
    fn oracle_task_install_other_bridge_source_and_receiver_code_or_trust_reject() {
        let spec = spec();
        let mut db = seeded_db();
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        mutate(
            &mut overlay,
            &env(T - 1),
            ORACLE_TASK_CONFIG_ADDRESS,
            setTaskCall {
                sourceType: 0,
                sourceId: U256::from(124),
                taskName: B256::repeat_byte(8),
                config: Bytes::from_static(b"other"),
            },
        )
        .unwrap();
        let changes = overlay.changes;
        db.commit(changes);
        assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
        assert!(snapshot(&mut db).default_callback.is_zero());
        for (key, value) in [
            ("sourceId", serde_json::json!("0x7c")),
            ("trustedBridge", serde_json::json!("0x0000000000000000000000000000000000009999")),
            ("receiverCodeHash", serde_json::json!(format!("{:#x}", B256::repeat_byte(3)))),
        ] {
            let mut wrong = (*spec).clone();
            wrong
                .genesis
                .config
                .extra_fields
                .get_mut("oracleTaskInstall")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(key.into(), value);
            let mut db = seeded_db();
            assert!(install(&mut db, &wrong, env(T), Some(T - 1), 73).is_err(), "{key}");
            assert!(db.basic(BRIDGE_RECEIVER).unwrap().is_none());
            assert!(snapshot(&mut db).is_empty());
        }
    }

    #[test]
    fn oracle_task_install_reads_current_grevm_cache_and_rejects_malformed_before_mutation() {
        let spec = spec();
        let config = EthEvmConfig::new(spec.clone());
        let mut executor = GrevmExecutor::new(spec.clone(), &config, seeded_db());
        let mut cached = Account::from(executor.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap());
        cached.mark_touch();
        let default_slot =
            U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
        cached.storage.insert(
            default_slot,
            EvmStorageSlot::new_changed(U256::ZERO, U256::from(999), TransactionId::default()),
        );
        executor
            .apply_state_change(core::iter::once((NATIVE_ORACLE_ADDRESS, cached)).collect())
            .unwrap();
        assert!(executor.apply_oracle_task_install(env(T), T - 1, 73).is_err());
        let bundle = executor.take_bundle();
        assert!(
            !bundle.state.contains_key(&ORACLE_TASK_CONFIG_ADDRESS),
            "reading prestate would incorrectly install task"
        );
        assert_eq!(
            bundle.state[&NATIVE_ORACLE_ADDRESS].storage[&default_slot].present_value,
            U256::from(999)
        );
        let mut wrong = (*spec).clone();
        wrong
            .genesis
            .config
            .extra_fields
            .insert("oracleTaskInstallTime".into(), serde_json::json!("100"));
        let wrong = Arc::new(wrong);
        let evm_config = EthEvmConfig::new(wrong.clone());
        let mut executor = GrevmExecutor::new(wrong, &evm_config, seeded_db());
        let block = reth_ethereum_primitives::Block {
            header: alloy_consensus::Header {
                number: 73,
                timestamp: T,
                gas_limit: 30_000_000,
                parent_beacon_block_root: Some(B256::ZERO),
                excess_blob_gas: Some(0),
                blob_gas_used: Some(0),
                ..Default::default()
            },
            body: Default::default(),
        };
        let block = reth_primitives_traits::RecoveredBlock::new_unhashed(block, Vec::new());
        assert!(executor.execute_one_with_parent_timestamp(&block, T - 1).is_err());
        assert!(executor.take_bundle().state.is_empty());
    }

    struct FailSecondSetter {
        inner: DB,
        default_reads: usize,
        commits: usize,
    }
    impl Database for FailSecondSetter {
        type Error = alloy_evm::ErasedError;
        fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(self.inner.basic(address).unwrap())
        }
        fn code_by_hash(&mut self, hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(self.inner.code_by_hash(hash).unwrap())
        }
        fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
            let default =
                U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
            if address == NATIVE_ORACLE_ADDRESS && index == default {
                self.default_reads += 1;
                if self.default_reads == 2 {
                    return Err(alloy_evm::ErasedError::new(io::Error::other(
                        "second setter injected storage failure",
                    )))
                }
            }
            Ok(self.inner.storage(address, index).unwrap())
        }
        fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
            Ok(self.inner.block_hash(number).unwrap())
        }
    }
    impl DatabaseCommit for FailSecondSetter {
        fn commit(&mut self, changes: EvmState) {
            self.commits += 1;
            self.inner.commit(changes);
        }
    }

    #[test]
    fn oracle_task_install_second_setter_failure_discards_successful_first_setter() {
        let spec = spec();
        let mut db = FailSecondSetter { inner: seeded_db(), default_reads: 0, commits: 0 };
        let native = db.inner.basic(NATIVE_ORACLE_ADDRESS).unwrap();
        let error = install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap_err();
        assert!(format!("{error}").contains("second setter injected"));
        assert_eq!(db.default_reads, 2);
        assert_eq!(db.commits, 0, "no base commit even though setTask succeeded");
        assert!(db.inner.basic(BRIDGE_RECEIVER).unwrap().is_none());
        assert_eq!(db.inner.basic(NATIVE_ORACLE_ADDRESS).unwrap(), native);
        assert_eq!(
            db.inner.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
            NATIVE_ORACLE_POST_FORK_CODE_HASH
        );
        assert!(snapshot(&mut db.inner).is_empty());
    }

    #[test]
    fn oracle_task_install_overlay_merges_original_values_and_later_account_reads() {
        let mut db = seeded_db();
        let mut overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
        let info = overlay.basic(ORACLE_TASK_CONFIG_ADDRESS).unwrap().unwrap();
        for (slot, original, present) in [(1, 0, 7), (2, 0, 8), (1, 7, 9)] {
            let mut account = Account::from(info.clone());
            account.mark_touch();
            account.storage.insert(
                U256::from(slot),
                EvmStorageSlot::new_changed(
                    U256::from(original),
                    U256::from(present),
                    TransactionId::default(),
                ),
            );
            overlay.commit(core::iter::once((ORACLE_TASK_CONFIG_ADDRESS, account)).collect());
        }
        assert_eq!(
            overlay.storage(ORACLE_TASK_CONFIG_ADDRESS, U256::from(1)).unwrap(),
            U256::from(9)
        );
        let changes = &overlay.changes[&ORACLE_TASK_CONFIG_ADDRESS].storage;
        assert_eq!(changes[&U256::from(1)].original_value, U256::ZERO);
        assert_eq!(changes[&U256::from(2)].present_value, U256::from(8));
    }

    #[test]
    fn oracle_task_install_grevm_parallel_sequential_and_missing_parent() {
        let spec = spec();
        let config = EthEvmConfig::new(spec.clone());
        let mut executor = GrevmExecutor::new(spec.clone(), &config, seeded_db());
        let block = reth_ethereum_primitives::Block {
            header: alloy_consensus::Header {
                number: 73,
                timestamp: T,
                gas_limit: 30_000_000,
                parent_beacon_block_root: Some(B256::ZERO),
                excess_blob_gas: Some(0),
                blob_gas_used: Some(0),
                ..Default::default()
            },
            body: Default::default(),
        };
        let block = reth_primitives_traits::RecoveredBlock::new_unhashed(block, Vec::new());
        assert!(executor.execute_one(&block).is_err());
        assert!(executor.take_bundle().state.is_empty());
        // Execute actual normal blocks through both constructors, including their
        // post hooks and accounting, rather than comparing direct hook calls only.
        let mut normal_states = Vec::new();
        for sequential in [false, true] {
            let mut executor = if sequential {
                GrevmExecutor::new_sequential(spec.clone(), &config, seeded_db())
            } else {
                GrevmExecutor::new(spec.clone(), &config, seeded_db())
            };
            let result = executor.execute_one_with_parent_timestamp(&block, T - 1).unwrap();
            assert!(result.receipts.is_empty());
            assert_eq!(result.gas_used, 0);
            assert_eq!(result.blob_gas_used, 0);
            let next = reth_ethereum_primitives::Block {
                header: alloy_consensus::Header {
                    number: 74,
                    timestamp: T + 1,
                    gas_limit: 30_000_000,
                    parent_beacon_block_root: Some(B256::ZERO),
                    excess_blob_gas: Some(0),
                    blob_gas_used: Some(0),
                    ..Default::default()
                },
                body: Default::default(),
            };
            let next = reth_primitives_traits::RecoveredBlock::new_unhashed(next, Vec::new());
            executor.execute_one_with_parent_timestamp(&next, T).unwrap();
            let state = executor.take_bundle().state;
            assert!(state.contains_key(&ORACLE_TASK_CONFIG_ADDRESS));
            assert!(state.contains_key(&NATIVE_ORACLE_ADDRESS));
            assert_eq!(
                state[&BRIDGE_RECEIVER].info.as_ref().unwrap().code_hash,
                keccak256(receiver_code())
            );
            assert_eq!(state[&BRIDGE_RECEIVER].info.as_ref().unwrap().nonce, 1);
            assert_eq!(
                state[&NATIVE_ORACLE_ADDRESS].info.as_ref().unwrap().code_hash,
                NATIVE_ORACLE_CALLBACK_CODE_HASH
            );
            normal_states.push(state);
        }
        assert_eq!(normal_states[0], normal_states[1]);
    }
}
