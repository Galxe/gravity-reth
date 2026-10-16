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
    // An epoch block may be the first block to cross both fork timestamps.
    // Stage the prerequisite here so every installation path sees Gamma code,
    // and a later installation failure commits neither migration.
    super::gamma::apply_state_changes_for_block(
        &mut super::common::DbHardforkState(&mut overlay),
        spec,
        block_number,
        timestamp,
    )?;
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
pub(crate) mod tests;
