//! Mint Token Precompile Contract
//!
//! This precompile allows authorized callers (JWK Manager) to mint tokens
//! directly to specified recipient addresses by modifying the EVM journal state
//! in-place via `EvmInternals`.

use alloy_primitives::{address, Address, Bytes, U256};
use reth_evm::precompiles::{DynPrecompile, PrecompileInput};
use revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use tracing::{info, warn};

/// Authorized caller address (JWK Manager at 0x2018)
///
/// Only this address is allowed to call the mint precompile.
pub const AUTHORIZED_CALLER: Address = address!("0x595475934ed7d9faa7fca28341c2ce583904a44e");

/// Function ID for mint operation
const FUNC_MINT: u8 = 0x01;

/// Base gas cost for mint operation
const MINT_BASE_GAS: u64 = 21000;

/// Creates a mint token precompile contract instance.
///
/// The precompile directly modifies the recipient account's balance inside the
/// EVM journal (via [`EvmInternals::load_account`]) during execution.
///
/// # Returns
///
/// A dynamic precompile that can be registered with the EVM.
pub fn create_mint_token_precompile() -> DynPrecompile {
    let precompile_id = PrecompileId::custom("mint_token");

    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        mint_token_handler(input)
    })
        .into()
}

/// Mint Token handler function
///
/// Directly modifies the recipient's balance in the EVM journal state, ensuring
/// the change is visible to subsequent opcodes within the same block and is
/// committed atomically with the rest of the transaction's state changes.
///
/// # Security
///
/// - Only JWK Manager (AUTHORIZED_CALLER) is allowed to call this precompile
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
/// - `Balance overflow` - Adding amount would overflow the recipient's balance
/// - `Failed to load account` - Journal failed to load the recipient account
fn mint_token_handler(mut input: PrecompileInput<'_>) -> PrecompileResult {
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
    if input.data.len() != EXPECTED_LEN {
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

    // 6. Load recipient account from EVM journal and directly increment its balance. `load_account`
    //    either returns the cached journal entry or fetches from the underlying DB and inserts it
    //    into the journal cache, always returning a `&mut Account`.
    let state_load = input.internals_mut().load_account(recipient).map_err(|e| {
        PrecompileError::Other(format!("Failed to load account {recipient}: {e}").into())
    })?;
    let account = state_load.data;
    let new_balance = account
        .info
        .balance
        .checked_add(U256::from(amount))
        .ok_or_else(|| PrecompileError::Other("Balance overflow".into()))?;
    account.info.balance = new_balance;
    // Mark the account as touched so revm commits it to state on transaction end.
    account.mark_touch();

    info!(
        target: "evm::precompile::mint_token",
        ?recipient,
        amount,
        new_balance = ?new_balance,
        "Minted tokens directly into journal state"
    );

    Ok(PrecompileOutput { gas_used: MINT_BASE_GAS, bytes: Bytes::new(), reverted: false })
}
