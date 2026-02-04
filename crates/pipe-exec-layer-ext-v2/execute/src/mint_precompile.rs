//! Mint Token Precompile Contract
//!
//! This precompile allows authorized callers (JWK Manager) to mint tokens
//! directly to specified recipient addresses.

use alloy_primitives::{address, map::HashMap, Address, Bytes, U256};
use grevm::ParallelState;
use parking_lot::Mutex;
use reth_evm::{
    precompiles::{DynPrecompile, PrecompileInput},
    ParallelDatabase,
};
use revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use std::sync::Arc;
use tracing::{info, warn};

/// Authorized caller address (JWK Manager at 0x2018)
///
/// Only this address is allowed to call the mint precompile.
pub const AUTHORIZED_CALLER: Address = address!("0x595475934ed7d9faa7fca28341c2ce583904a44e");

/// Function ID for mint operation
const FUNC_MINT: u8 = 0x01;

/// Base gas cost for mint operation
const MINT_BASE_GAS: u64 = 21000;

/// Creates a mint token precompile contract instance with state access.
///
/// The precompile contract allows authorized callers to submit mint requests
/// and directly modifies the recipient's balance in the state.
///
/// # Arguments
///
/// * `state` - Shared ParallelState wrapped in `Arc<Mutex<>>` for thread-safe access
///
/// # Returns
///
/// A dynamic precompile that can be registered with the EVM
pub fn create_mint_token_precompile<DB: ParallelDatabase + Send + Sync + 'static>(
    state: Arc<Mutex<ParallelState<DB>>>,
) -> DynPrecompile {
    let precompile_id = PrecompileId::custom("mint_token");

    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        mint_token_handler(input, state.clone())
    })
        .into()
}

/// Mint Token handler function
///
/// # Security
///
/// - Only JWK Manager (0x2018) is allowed to call this precompile
/// - Calls from other addresses will be rejected with an error
///
/// # Parameter format (53 bytes)
///
/// | Offset | Size | Description |
/// |--------|------|-------------|
/// | 0      | 1    | Function ID (0x01) |
/// | 1      | 20   | Recipient address |
/// | 21     | 32   | Amount (U256, big-endian) |
///
/// # Errors
///
/// - `Unauthorized caller` - Caller is not the authorized JWK Manager
/// - `Invalid input length` - Input data is less than 53 bytes
/// - `Invalid function ID` - Function ID is not 0x01
/// - `Invalid or zero amount` - Amount is zero or exceeds u128::MAX
fn mint_token_handler<DB: ParallelDatabase + Send + Sync>(
    input: PrecompileInput<'_>,
    state: Arc<Mutex<ParallelState<DB>>>,
) -> PrecompileResult {
    // 1. Validate caller address
    if input.caller != AUTHORIZED_CALLER {
        warn!(
            target: "evm::precompile::mint_token",
            caller = ?input.caller,
            authorized = ?AUTHORIZED_CALLER,
            "Unauthorized caller"
        );
        return Err(PrecompileError::Other("Unauthorized caller".into()));
    }

    // 2. Parameter length check (1 + 20 + 32 = 53 bytes)
    const EXPECTED_LEN: usize = 1 + 20 + 32;
    if input.data.len() < EXPECTED_LEN {
        warn!(
            target: "evm::precompile::mint_token",
            input_len = input.data.len(),
            expected = EXPECTED_LEN,
            "Invalid input length"
        );
        return Err(PrecompileError::Other(
            format!("Invalid input length: {}, expected {}", input.data.len(), EXPECTED_LEN).into(),
        ));
    }

    // 3. Parse and validate function ID
    if input.data[0] != FUNC_MINT {
        warn!(
            target: "evm::precompile::mint_token",
            func_id = input.data[0],
            expected = FUNC_MINT,
            "Invalid function ID"
        );
        return Err(PrecompileError::Other(
            format!("Invalid function ID: {:#x}, expected {:#x}", input.data[0], FUNC_MINT).into(),
        ));
    }

    // 4. Parse recipient address (bytes 1-20)
    let recipient = Address::from_slice(&input.data[1..21]);

    // 5. Parse amount (bytes 21-52)
    let amount_u256 = U256::from_be_slice(&input.data[21..53]);
    let amount: u128 = amount_u256.try_into().map_err(|_| {
        warn!(
            target: "evm::precompile::mint_token",
            ?recipient,
            amount = ?amount_u256,
            "Amount exceeds u128::MAX"
        );
        PrecompileError::Other("Amount exceeds u128::MAX".into())
    })?;

    if amount == 0 {
        warn!(target: "evm::precompile::mint_token", ?recipient, "Zero amount");
        return Err(PrecompileError::Other("Zero amount not allowed".into()));
    }

    // 6. Execute mint operation
    let mut state_guard = state.lock();
    if let Err(e) = state_guard.increment_balances(HashMap::from([(recipient, amount)])) {
        warn!(
            target: "evm::precompile::mint_token",
            ?recipient,
            amount,
            error = ?e,
            "Failed to increment balance"
        );
        return Err(PrecompileError::Other("Failed to mint tokens".into()));
    }
    drop(state_guard);

    info!(
        target: "evm::precompile::mint_token",
        ?recipient,
        amount,
        "Minted tokens successfully"
    );

    Ok(PrecompileOutput { gas_used: MINT_BASE_GAS, bytes: Bytes::new(), reverted: false })
}
