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
//! This module owns the hardcoded migration table and calldata encoding.
//! There is no slot precheck: any forced-tx revert panics at execution time.
//! It intentionally contains **no** RPC debug/trace sender or basefee
//! special-casing.

use alloy_primitives::{address, Address, Bytes};
use alloy_sol_types::{sol, SolCall};

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

/// ABI-encode `transferOwnership(new_owner)`.
#[inline]
pub fn transfer_ownership_calldata(new_owner: Address) -> Bytes {
    Bytes::from(transferOwnershipCall { newOwner: new_owner }.abi_encode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_ownership_selector_matches_oz() {
        let data = transfer_ownership_calldata(MIGRATION_TABLE[0].new_owner);
        assert_eq!(&data[..4], &[0xf2, 0xfd, 0xe3, 0x8b]);
        assert_eq!(data.len(), 4 + 32);
    }
}
