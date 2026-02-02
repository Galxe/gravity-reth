//! Mint Token Precompile Contract
//!
//! This precompile allows authorized callers to mint native tokens
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
use tracing::{debug, info, warn};

/// Authorized caller addresses
///
/// These addresses are allowed to call the mint precompile:
/// - JWK Manager (0x2018) - legacy support
/// - GBridgeReceiver - deployed via genesis, address from validator_genesis.json
const AUTHORIZED_CALLERS: [Address; 1] = [
    address!("0x595475934ed7d9faa7fca28341c2ce583904a44e"),    // GBridgeReceiver (genesis)
];

/// Function selector for mint(address,uint256) - standard Solidity ABI
const MINT_SELECTOR: [u8; 4] = [0x40, 0xc1, 0x0f, 0x19];

/// Legacy function ID for mint operation (custom format)
const FUNC_MINT_LEGACY: u8 = 0x01;

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
    info!(
        target: "evm::precompile::mint_token",
        "Creating mint token precompile"
    );
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
/// - Only authorized addresses can call this precompile
/// - Calls from other addresses will be rejected with an error
///
/// # Supported formats
///
/// ## Standard ABI format (68 bytes) - mint(address,uint256)
/// | Offset | Size | Description |
/// |--------|------|-------------|
/// | 0      | 4    | Function selector (0x40c10f19) |
/// | 4      | 32   | Recipient address (padded) |
/// | 36     | 32   | Amount (U256) |
///
/// ## Legacy custom format (53 bytes)
/// | Offset | Size | Description |
/// |--------|------|-------------|
/// | 0      | 1    | Function ID (0x01) |
/// | 1      | 20   | Recipient address |
/// | 21     | 32   | Amount (U256) |
///
/// # Errors
///
/// - `Unauthorized caller` - Caller is not in the authorized list
/// - `Invalid input length` - Input data is too short
/// - `Invalid function selector` - Unrecognized function
/// - `Zero amount` - Amount is zero
fn mint_token_handler<DB: ParallelDatabase + Send + Sync>(
    input: PrecompileInput<'_>,
    state: Arc<Mutex<ParallelState<DB>>>,
) -> PrecompileResult {
    // Log immediately when precompile is called
    info!(
        target: "evm::precompile::mint_token",
        caller = ?input.caller,
        data_len = input.data.len(),
        "PRECOMPILE CALLED - mint_token_handler entry"
    );
    
    // 1. Validate caller address
    if !AUTHORIZED_CALLERS.contains(&input.caller) {
        warn!(
            target: "evm::precompile::mint_token",
            caller = ?input.caller,
            authorized = ?AUTHORIZED_CALLERS,
            "Unauthorized caller"
        );
        return Err(PrecompileError::Other("Unauthorized caller".into()));
    }

    info!(
        target: "evm::precompile::mint_token",
        caller = ?input.caller,
        data_len = input.data.len(),
        data_hex = %hex::encode(&input.data),
        "Mint precompile called"
    );

    // 2. Parse input based on format
    let (recipient, amount) = if input.data.len() >= 68 && input.data[0..4] == MINT_SELECTOR {
        // Standard ABI format: selector(4) + recipient(32, last 20 bytes) + amount(32)
        let recipient = Address::from_slice(&input.data[16..36]); // Skip 4 bytes selector + 12 bytes padding
        let amount_u256 = U256::from_be_slice(&input.data[36..68]);
        info!(
            target: "evm::precompile::mint_token",
            ?recipient,
            amount = ?amount_u256,
            "Parsed standard ABI format"
        );
        (recipient, amount_u256)
    } else if input.data.len() >= 53 && input.data[0] == FUNC_MINT_LEGACY {
        // Legacy custom format: funcId(1) + recipient(20) + amount(32)
        let recipient = Address::from_slice(&input.data[1..21]);
        let amount_u256 = U256::from_be_slice(&input.data[21..53]);
        info!(
            target: "evm::precompile::mint_token",
            ?recipient,
            amount = ?amount_u256,
            "Parsed legacy format"
        );
        (recipient, amount_u256)
    } else {
        warn!(
            target: "evm::precompile::mint_token",
            data_len = input.data.len(),
            first_bytes = ?&input.data.get(0..4),
            "Invalid input format"
        );
        return Err(PrecompileError::Other(
            format!(
                "Invalid input: len={}, expected ABI (68+ bytes with selector 0x40c10f19) or legacy (53+ bytes with 0x01)",
                input.data.len()
            ).into(),
        ));
    };

    // 3. Validate amount
    let amount: u128 = amount.try_into().map_err(|_| {
        warn!(
            target: "evm::precompile::mint_token",
            ?recipient,
            amount = ?amount,
            "Amount exceeds u128::MAX"
        );
        PrecompileError::Other("Amount exceeds u128::MAX".into())
    })?;

    if amount == 0 {
        warn!(target: "evm::precompile::mint_token", ?recipient, "Zero amount");
        return Err(PrecompileError::Other("Zero amount not allowed".into()));
    }

    // 4. Execute mint operation
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
