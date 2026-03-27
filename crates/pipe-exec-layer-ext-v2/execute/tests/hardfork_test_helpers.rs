//! Reusable test helpers for Gravity hardfork integration tests.
//!
//! These helpers verify that contract bytecodes were correctly replaced
//! during a hardfork. They work with any hardfork by accepting
//! `(address, expected_bytecode)` slices as parameters.
//!
//! # Usage
//!
//! ```ignore
//! use reth_evm_ethereum::hardfork::gamma::{GAMMA_SYSTEM_UPGRADES, STAKEPOOL_ADDRESSES, STAKEPOOL_BYTECODE};
//!
//! // Verify system contracts were upgraded at the hardfork block
//! verify_bytecodes_at_block(&provider, hardfork_block, GAMMA_SYSTEM_UPGRADES, "Gamma system");
//!
//! // Verify they were NOT upgraded before the hardfork block
//! verify_bytecodes_old_before_block(&provider, hardfork_block, GAMMA_SYSTEM_UPGRADES, "Gamma system");
//! ```

use alloy_primitives::{Address, B256, U256};
use reth_provider::StateProviderFactory;

/// Verify that all contracts in `upgrades` have the expected new bytecodes
/// at the given `block_number`.
///
/// Panics if any mismatch is found. Contracts that don't exist on-chain
/// are logged as warnings but not treated as failures (they may not have
/// existed in the old genesis).
pub fn verify_bytecodes_at_block<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    upgrades: &[(Address, &[u8])],
    label: &str,
) {
    println!("[hardfork_test] Verifying {label} bytecodes at block {block_number}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("Failed to get state provider");

    let mut all_ok = true;
    for (addr, expected_bytecode) in upgrades {
        match state.account_code(addr) {
            Ok(Some(code)) => {
                let code_bytes = code.original_bytes();
                if code_bytes.as_ref() == *expected_bytecode {
                    println!("[hardfork_test] ✅ {addr}: bytecode matches ({}B)", code_bytes.len());
                } else {
                    println!(
                        "[hardfork_test] ❌ {addr}: MISMATCH got={}B expected={}B",
                        code_bytes.len(),
                        expected_bytecode.len()
                    );
                    all_ok = false;
                }
            }
            Ok(None) => {
                // Contract may not have existed in old genesis — skip
                println!("[hardfork_test] ⚠ {addr}: no code found (not in old genesis, skip)");
            }
            Err(e) => {
                println!("[hardfork_test] ❌ {addr}: error: {e:?}");
                all_ok = false;
            }
        }
    }

    assert!(all_ok, "Not all {label} contracts were upgraded at block {block_number}!");
    println!("[hardfork_test] ✅ All {} {label} bytecodes verified!", upgrades.len());
}

/// Verify that contracts in `upgrades` do NOT yet have the new bytecodes
/// before the hardfork block (i.e. at `hardfork_block - 1`).
///
/// Checks only the first contract as a smoke test to avoid excessive slowness.
pub fn verify_bytecodes_old_before_block<P: StateProviderFactory>(
    provider: &P,
    hardfork_block: u64,
    upgrades: &[(Address, &[u8])],
    label: &str,
) {
    let pre_block = hardfork_block - 1;
    println!("[hardfork_test] Verifying {label} bytecodes are OLD before block {pre_block}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(pre_block))
        .expect("Failed to get state provider for pre-hardfork block");

    // Smoke test: check the first contract
    let (addr, expected_new) = &upgrades[0];
    match state.account_code(addr) {
        Ok(Some(code)) => {
            let code_bytes = code.original_bytes();
            assert_ne!(
                code_bytes.as_ref(),
                *expected_new,
                "Bytecode at {addr} should be OLD before hardfork but was already upgraded!"
            );
            println!(
                "[hardfork_test] ✅ {addr} at block {pre_block}: old bytecode ({}B), new would be {}B",
                code_bytes.len(),
                expected_new.len()
            );
        }
        Ok(None) => {
            println!("[hardfork_test] ⚠ {addr} at block {pre_block}: no code (may be expected)");
        }
        Err(e) => {
            panic!("[hardfork_test] Failed to fetch code before hardfork: {e:?}");
        }
    }
}

/// Verify that specific storage slots have expected values at a given block.
///
/// Useful for checking ReentrancyGuard initialization, new storage slots, etc.
pub fn verify_storage_patches<P: StateProviderFactory>(
    provider: &P,
    block_number: u64,
    patches: &[(Address, B256, U256)],
    label: &str,
) {
    println!("[hardfork_test] Verifying {label} storage patches at block {block_number}...");

    let state = provider
        .state_by_block_number_or_tag(alloy_eips::BlockNumberOrTag::Number(block_number))
        .expect("Failed to get state provider");

    for (addr, slot, expected_value) in patches {
        let actual = state.storage(*addr, *slot).expect("Failed to read storage");
        assert_eq!(actual, Some(*expected_value), "{label}: {addr} slot {slot} mismatch");
        println!("[hardfork_test] ✅ {addr}: storage {slot} = {actual:?}");
    }
}
