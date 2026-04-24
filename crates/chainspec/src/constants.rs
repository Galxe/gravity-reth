use crate::spec::DepositContract;
use alloy_eips::eip6110::MAINNET_DEPOSIT_CONTRACT_ADDRESS;
use alloy_primitives::b256;

/// Gas per transaction not creating a contract.
pub const MIN_TRANSACTION_GAS: u64 = 21_000u64;

/// Gravity protocol minimum base fee per gas (50 Gwei).
///
/// Acts as both the floor for EIP-1559 base fee updates and the initial base fee at
/// genesis. The EIP-1559 recurrence is clamped at this value, so base fee never drops
/// below 50 Gwei.
pub const GRAVITY_MIN_BASE_FEE: u64 = 50_000_000_000;

/// Mainnet prune delete limit.
pub const MAINNET_PRUNE_DELETE_LIMIT: usize = 20000;

/// Deposit contract address: `0x00000000219ab540356cbb839cbe05303d7705fa`
pub(crate) const MAINNET_DEPOSIT_CONTRACT: DepositContract = DepositContract::new(
    MAINNET_DEPOSIT_CONTRACT_ADDRESS,
    11052984,
    b256!("0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"),
);
