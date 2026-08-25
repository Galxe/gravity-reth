//! `TestnetOwnerFix` hardfork — forced `Ownable2Step` `transferOwnership` migration.
//!
//! Longevity Testnet (`chain_id == 7771625`) genesis `StakePool`s used Aptos-era
//! identity material as `owner`. Those addresses look like EOAs but have no
//! recoverable secp256k1 private key, so `onlyOwner` admin paths are stuck.
//!
//! On the first block with `timestamp >= testnetOwnerFixTime`, the pipe layer
//! injects four synthetic top-level txs (one per genesis pool) with
//! `from = old_owner`, calling `transferOwnership(new_owner)`. Gas reuses the
//! existing Alpha system-tx levers (`gas_price = 0` + `transact_system_txn`
//! basefee/balance disable). The txs are written into the block body with
//! `TransactionSenders = old_owner`.
//!
//! This module owns the hardcoded migration table, calldata encoding, and
//! fail-closed / idempotent prechecks. It intentionally contains **no** RPC
//! debug/trace sender or basefee special-casing.

use alloc::{format, string::String};
use alloy_primitives::{address, b256, Address, Bytes, B256, U256};
use alloy_sol_types::{sol, SolCall};

/// Longevity Testnet `StakePool` runtime codehash (genesis + live RPC verified
/// 2026-08-25). All four genesis pools share this bytecode.
pub const STAKEPOOL_CODE_HASH: B256 =
    b256!("77e0b0dcaa8422c64dd50f39f1c450698fa2ee51c24fb3979e0c0bff59aadfd0");

/// Ownable `_owner` storage slot on the deployed Longevity Testnet `StakePool`
/// runtime (classic sequential layout — slot 0 — verified on-chain).
pub const OWNER_SLOT: U256 = U256::ZERO;

/// `Ownable2Step` `_pendingOwner` storage slot (slot 1).
pub const PENDING_OWNER_SLOT: U256 = U256::from_limbs([1, 0, 0, 0]);

sol! {
    /// `Ownable2Step.transferOwnership(address newOwner)`
    function transferOwnership(address newOwner);
}

/// One genesis `StakePool` ownership migration row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationRow {
    /// Human label (node1 / node2 / node3 / node5).
    pub label: &'static str,
    /// `StakePool` contract address.
    pub stake_pool: Address,
    /// Current (unrecoverable) owner EOA.
    pub old_owner: Address,
    /// Ceremony-generated replacement EOA (address only; no private key here).
    pub new_owner: Address,
}

/// Hardcoded Longevity Testnet migration table (node1 → node2 → node3 → node5).
///
/// New owner addresses come from the Phase 0 ceremony
/// (`new-owners.addresses.yaml`); private keys never enter this binary.
pub const MIGRATION_TABLE: [MigrationRow; 4] = [
    MigrationRow {
        label: "node1",
        stake_pool: address!("743d93845745e01a23f9afbb990bbc7c87aae6c8"),
        old_owner: address!("CE128222Bd84D67672f863424a03D114CD1253C5"),
        new_owner: address!("c7536c625758b3072c43eab8e8880c1ae8cb4cf9"),
    },
    MigrationRow {
        label: "node2",
        stake_pool: address!("419ad62f796a0f3971bd1212f208942c3c435b99"),
        old_owner: address!("78F595Fb25D03a742338Fb32AcfD544BdC63D814"),
        new_owner: address!("f0de80e6df293be1b81d106afd5ae5430079e9d2"),
    },
    MigrationRow {
        label: "node3",
        stake_pool: address!("93e5acbcdd50767f7fd19ab4a2efc259d9a8bdd1"),
        old_owner: address!("891299fE364088ead65ABa911ea17DD5d968Cd81"),
        new_owner: address!("2c26bab4ebcc88fb0ad580652938a27e692037f0"),
    },
    MigrationRow {
        label: "node5",
        stake_pool: address!("298136ce84d442d2c0c594f5734a20afc60de244"),
        old_owner: address!("B99AA922Eb5CaE399b79ADC87621E72f66d5A976"),
        new_owner: address!("3e1b5fab188ddc208c547dd689b0f6b4864b4127"),
    },
];

/// Observed on-chain snapshot for one migration row (precheck input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// Account `code_hash` (must match [`STAKEPOOL_CODE_HASH`] pre-migration).
    pub code_hash: B256,
    /// Current `owner()` / Ownable slot 0.
    pub owner: Address,
    /// Current `pendingOwner()` / `Ownable2Step` slot 1.
    pub pending_owner: Address,
}

/// Result of evaluating the full migration table against live state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrecheckDecision {
    /// All four pools still need `transferOwnership` injection.
    Apply,
    /// All four pools already have `pendingOwner == new` or `owner == new`.
    AlreadyMigrated,
}

/// Decode an address stored in a 32-byte word (right-aligned).
#[inline]
pub fn address_from_word(word: U256) -> Address {
    Address::from_word(B256::from(word))
}

/// ABI-encode `transferOwnership(new_owner)`.
#[inline]
pub fn transfer_ownership_calldata(new_owner: Address) -> Bytes {
    Bytes::from(transferOwnershipCall { newOwner: new_owner }.abi_encode())
}

/// Fail-closed / idempotent precheck over the four genesis pools.
///
/// Rules:
/// - If every row is already migrated (`pendingOwner == new` **or** `owner == new`), return
///   [`PrecheckDecision::AlreadyMigrated`].
/// - Else every row must have `code_hash == STAKEPOOL_CODE_HASH`, `owner == old_owner`, and
///   `pendingOwner == 0`; otherwise return `Err`.
pub fn precheck_migration(snapshots: &[PoolSnapshot; 4]) -> Result<PrecheckDecision, String> {
    debug_assert_eq!(snapshots.len(), MIGRATION_TABLE.len());

    let mut migrated = 0usize;
    for (row, snap) in MIGRATION_TABLE.iter().zip(snapshots.iter()) {
        if snap.pending_owner == row.new_owner || snap.owner == row.new_owner {
            migrated += 1;
        }
    }
    if migrated == MIGRATION_TABLE.len() {
        return Ok(PrecheckDecision::AlreadyMigrated);
    }
    if migrated != 0 {
        return Err(format!(
            "TestnetOwnerFix: partial migration detected ({migrated}/{} pools already migrated)",
            MIGRATION_TABLE.len()
        ));
    }

    for (row, snap) in MIGRATION_TABLE.iter().zip(snapshots.iter()) {
        if snap.code_hash != STAKEPOOL_CODE_HASH {
            return Err(format!(
                "TestnetOwnerFix: {} StakePool {} has unexpected codehash {}; expected {}",
                row.label, row.stake_pool, snap.code_hash, STAKEPOOL_CODE_HASH
            ));
        }
        if snap.owner != row.old_owner {
            return Err(format!(
                "TestnetOwnerFix: {} StakePool {} owner is {}; expected old_owner {}",
                row.label, row.stake_pool, snap.owner, row.old_owner
            ));
        }
        if snap.pending_owner != Address::ZERO {
            return Err(format!(
                "TestnetOwnerFix: {} StakePool {} pendingOwner is {}; expected zero",
                row.label, row.stake_pool, snap.pending_owner
            ));
        }
    }

    Ok(PrecheckDecision::Apply)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_snapshots() -> [PoolSnapshot; 4] {
        [
            PoolSnapshot {
                code_hash: STAKEPOOL_CODE_HASH,
                owner: MIGRATION_TABLE[0].old_owner,
                pending_owner: Address::ZERO,
            },
            PoolSnapshot {
                code_hash: STAKEPOOL_CODE_HASH,
                owner: MIGRATION_TABLE[1].old_owner,
                pending_owner: Address::ZERO,
            },
            PoolSnapshot {
                code_hash: STAKEPOOL_CODE_HASH,
                owner: MIGRATION_TABLE[2].old_owner,
                pending_owner: Address::ZERO,
            },
            PoolSnapshot {
                code_hash: STAKEPOOL_CODE_HASH,
                owner: MIGRATION_TABLE[3].old_owner,
                pending_owner: Address::ZERO,
            },
        ]
    }

    #[test]
    fn precheck_apply_when_all_pre_migration() {
        assert_eq!(precheck_migration(&base_snapshots()).unwrap(), PrecheckDecision::Apply);
    }

    #[test]
    fn precheck_idempotent_when_all_pending_new() {
        let mut snaps = base_snapshots();
        for (snap, row) in snaps.iter_mut().zip(MIGRATION_TABLE.iter()) {
            snap.pending_owner = row.new_owner;
        }
        assert_eq!(precheck_migration(&snaps).unwrap(), PrecheckDecision::AlreadyMigrated);
    }

    #[test]
    fn precheck_idempotent_when_all_owner_new() {
        let mut snaps = base_snapshots();
        for (snap, row) in snaps.iter_mut().zip(MIGRATION_TABLE.iter()) {
            snap.owner = row.new_owner;
        }
        assert_eq!(precheck_migration(&snaps).unwrap(), PrecheckDecision::AlreadyMigrated);
    }

    #[test]
    fn precheck_rejects_wrong_chain_codehash() {
        let mut snaps = base_snapshots();
        snaps[0].code_hash =
            b256!("1111111111111111111111111111111111111111111111111111111111111111");
        assert!(precheck_migration(&snaps).unwrap_err().contains("unexpected codehash"));
    }

    #[test]
    fn precheck_rejects_partial_migration() {
        let mut snaps = base_snapshots();
        snaps[0].pending_owner = MIGRATION_TABLE[0].new_owner;
        assert!(precheck_migration(&snaps).unwrap_err().contains("partial migration"));
    }

    #[test]
    fn precheck_rejects_nonzero_pending() {
        let mut snaps = base_snapshots();
        snaps[2].pending_owner = address!("00000000000000000000000000000000000000aa");
        assert!(precheck_migration(&snaps).unwrap_err().contains("pendingOwner"));
    }

    #[test]
    fn transfer_ownership_selector_matches_oz() {
        let data = transfer_ownership_calldata(MIGRATION_TABLE[0].new_owner);
        assert_eq!(&data[..4], &[0xf2, 0xfd, 0xe3, 0x8b]);
        assert_eq!(data.len(), 4 + 32);
    }

    #[test]
    fn address_from_word_right_aligned() {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(MIGRATION_TABLE[0].old_owner.as_slice());
        let word = U256::from_be_bytes(bytes);
        assert_eq!(address_from_word(word), MIGRATION_TABLE[0].old_owner);
    }

    /// Longevity Testnet `StakePool` runtime (genesis / live codehash verified).
    const STAKEPOOL_RUNTIME: &[u8] = include_bytes!("bytecodes/testnet_owner_fix/StakePool.bin");

    fn word_from_address(addr: Address) -> U256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(addr.as_slice());
        U256::from_be_bytes(bytes)
    }

    /// Execute one forced `transferOwnership` against real `StakePool` runtime and
    /// assert `pendingOwner == new_owner` while `owner` stays `old_owner`.
    #[test]
    fn forced_transfer_sets_pending_owner_on_stakepool_runtime() {
        use crate::EthEvmConfig;
        use alloy_consensus::Header;
        use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
        use alloy_primitives::{TxKind, B256};
        use reth_chainspec::{
            ChainHardforks, ChainSpecBuilder, ForkCondition, GravityHardfork, MAINNET,
        };
        use reth_evm::{
            execute::BasicBlockExecutor,
            parallel_execute::{ParallelExecutor, WrapExecutor},
            ConfigureEvm,
        };
        use revm::{
            bytecode::Bytecode,
            context::TxEnv,
            database::{CacheDB, EmptyDB},
            state::AccountInfo,
        };
        use std::sync::Arc;

        assert_eq!(alloy_primitives::keccak256(STAKEPOOL_RUNTIME), STAKEPOOL_CODE_HASH);

        let row = MIGRATION_TABLE[0];
        let mut db = CacheDB::<EmptyDB>::default();
        db.insert_account_info(
            row.stake_pool,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 1,
                code_hash: STAKEPOOL_CODE_HASH,
                code: Some(Bytecode::new_raw(Bytes::from_static(STAKEPOOL_RUNTIME))),
                ..Default::default()
            },
        );
        db.insert_account_storage(row.stake_pool, OWNER_SLOT, word_from_address(row.old_owner))
            .unwrap();
        db.insert_account_storage(row.stake_pool, PENDING_OWNER_SLOT, U256::ZERO).unwrap();
        // old_owner EOA present with nonce 0 / zero balance — Alpha gas exempt covers fee.
        db.insert_account_info(
            row.old_owner,
            AccountInfo { nonce: 0, balance: U256::ZERO, ..Default::default() },
        );

        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks =
            ChainHardforks::from([(GravityHardfork::Alpha, ForkCondition::Timestamp(0))]);
        let chain_id = spec.chain().id();
        let evm_config = EthEvmConfig::new(Arc::new(spec));
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(evm_config.clone(), db));

        let header = Header {
            timestamp: 1,
            number: 1,
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1_000_000_000),
            ..Header::default()
        };
        let evm_env = evm_config.evm_env(&header).expect("evm_env");

        let tx_env = TxEnv {
            caller: row.old_owner,
            gas_limit: 30_000_000,
            gas_price: 0,
            kind: TxKind::Call(row.stake_pool),
            value: U256::ZERO,
            data: transfer_ownership_calldata(row.new_owner),
            nonce: 0,
            chain_id: Some(chain_id),
            ..TxEnv::default()
        };

        let result = executor
            .transact_system_txn(evm_env, Vec::new(), tx_env)
            .expect("forced transferOwnership must execute");
        assert!(result.is_success(), "transferOwnership reverted: {result:?}");

        let pending = executor.storage(row.stake_pool, PENDING_OWNER_SLOT).unwrap();
        let owner = executor.storage(row.stake_pool, OWNER_SLOT).unwrap();
        assert_eq!(address_from_word(pending), row.new_owner);
        assert_eq!(address_from_word(owner), row.old_owner);

        let old_info = executor.basic(row.old_owner).unwrap().unwrap();
        assert_eq!(old_info.nonce, 1, "old_owner nonce must bump");
        assert_eq!(old_info.balance, U256::ZERO, "gas-exempt must preserve zero balance");
    }
}
