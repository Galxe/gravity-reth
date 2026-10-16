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
        cfg_env: CfgEnv::new_with_spec(SpecId::PRAGUE).with_chain_id(LONGEVITY_TESTNET_CHAIN_ID),
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
        Bytes::from_static(include_bytes!("../bytecodes/gamma/NativeOracle.bin")),
    );
    insert_code(
        &mut db,
        ORACLE_TASK_CONFIG_ADDRESS,
        Bytes::from_static(include_bytes!("../bytecodes/gamma/OracleTaskConfig.bin")),
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

pub(crate) fn pre_gamma_db() -> DB {
    let mut db = seeded_db();
    for (address, hash) in [
        (NATIVE_ORACLE_ADDRESS, super::super::gamma::NATIVE_ORACLE_PRE_FORK_CODE_HASH),
        (ORACLE_TASK_CONFIG_ADDRESS, super::super::gamma::ORACLE_TASK_CONFIG_PRE_FORK_CODE_HASH),
    ] {
        let mut info = db.basic(address).unwrap().unwrap();
        // Gamma authenticates the old hash and replaces code without executing it.
        info.code_hash = hash;
        info.code = None;
        db.insert_account_info(address, info);
    }
    db
}

#[test]
fn oracle_task_install_same_block_gamma_rolls_back_on_receiver_conflict() {
    let mut db = pre_gamma_db();
    insert_code(&mut db, BRIDGE_RECEIVER, Bytes::from_static(&[0]));
    let native = db.basic(NATIVE_ORACLE_ADDRESS).unwrap();
    let tasks = db.basic(ORACLE_TASK_CONFIG_ADDRESS).unwrap();
    assert!(install(&mut db, spec().as_ref(), env(T), Some(9), 73).is_err());
    assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), native);
    assert_eq!(db.basic(ORACLE_TASK_CONFIG_ADDRESS).unwrap(), tasks);
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
    assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap().unwrap().code_hash, keccak256(receiver_code()));
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
    let mut db = seeded_db();
    db.insert_account_info(BRIDGE_RECEIVER, AccountInfo { nonce: 1, ..Default::default() });
    let before = db.basic(BRIDGE_RECEIVER).unwrap();
    assert!(install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).is_err());
    assert_eq!(db.basic(BRIDGE_RECEIVER).unwrap(), before);
    assert_eq!(
        db.basic(NATIVE_ORACLE_ADDRESS).unwrap().unwrap().code_hash,
        NATIVE_ORACLE_POST_FORK_CODE_HASH
    );
    assert!(snapshot(&mut db).is_empty());
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
    // The unrelated callback must already have code while Receiver is absent.
    // Reuse the existing STOP contract; no JWK callback is executed here.
    mutate(
        &mut overlay,
        &env(T - 10),
        NATIVE_ORACLE_ADDRESS,
        setDefaultCallbackCall { sourceType: 1, callback: GOVERNANCE },
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
        GOVERNANCE
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
        Bytes::from_static(include_bytes!("../bytecodes/gamma/NativeOracle.bin")),
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
        let mut task_overlay = InstallationOverlay { base: &mut db, changes: EvmState::default() };
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
    gamma::apply_state_changes_for_block(&mut DbHardforkState(&mut db), spec.as_ref(), 72, T - 1)
        .unwrap();
    install(&mut db, spec.as_ref(), env(T), Some(T - 1), 73).unwrap();
    let upgraded = db.basic(NATIVE_ORACLE_ADDRESS).unwrap();
    gamma::apply_state_changes_for_block(&mut DbHardforkState(&mut db), spec.as_ref(), 74, T + 1)
        .unwrap();
    assert_eq!(db.basic(NATIVE_ORACLE_ADDRESS).unwrap(), upgraded);
    let mut disabled = (*spec).clone();
    disabled.genesis.config.extra_fields.remove("oracleTaskInstall");
    disabled.genesis.config.extra_fields.remove("oracleTaskInstallTime");
    let mut other_chain = (*spec).clone();
    other_chain.chain = Chain::from(1625);
    for (scope, timestamp) in [(spec.as_ref(), T - 1), (&disabled, T + 1), (&other_chain, T + 1)] {
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
    let default_slot = U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
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
        let default = U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
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
    assert_eq!(overlay.storage(ORACLE_TASK_CONFIG_ADDRESS, U256::from(1)).unwrap(), U256::from(9));
    let changes = &overlay.changes[&ORACLE_TASK_CONFIG_ADDRESS].storage;
    assert_eq!(changes[&U256::from(1)].original_value, U256::ZERO);
    assert_eq!(changes[&U256::from(2)].present_value, U256::from(8));
}

#[test]
fn oracle_task_install_grevm_empty_block_boundary_and_missing_parent() {
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
    // Empty blocks exercise the installation post hook and accounting, not
    // parallel or sequential transaction scheduling.
    let mut executor = GrevmExecutor::new(spec.clone(), &config, seeded_db());
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
}
