//! Historical randomness lookup precompile.
//!
//! This read-only precompile returns the canonical post-Merge randomness value
//! stored in a block header's `mix_hash` / `prev_randao` field.

use alloy_primitives::{address, Address, Bytes, B256, U256};
use reth_evm::precompiles::{DynPrecompile, PrecompileInput};
use revm::precompile::{PrecompileHalt, PrecompileId, PrecompileOutput, PrecompileResult};
use std::{fmt, sync::Arc};
use tracing::warn;

/// Address of Gravity's historical randomness lookup precompile.
pub const RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR: Address =
    address!("00000000000000000000000000000001625f5002");

/// Precompile input length: one ABI-encoded `uint256 blockNumber`.
pub const RANDOMNESS_BY_HEIGHT_INPUT_LEN: usize = 32;
/// Precompile output length: ABI-encoded `(uint256 found, bytes32 randomness)`.
pub const RANDOMNESS_BY_HEIGHT_OUTPUT_LEN: usize = 64;
/// Number of recent ancestor blocks charged at the cheaper recent-window price.
///
/// Gravity targets roughly 3 blocks per second, so 86,400 blocks is about 8 hours.
pub const RANDOMNESS_BY_HEIGHT_RECENT_WINDOW: u64 = 86_400;
/// Gas charged when the lookup is bounded to the recent block window.
///
/// The provider is responsible for only using this tier for current/future misses or recent
/// heights relative to the executing block.
pub const RANDOMNESS_BY_HEIGHT_RECENT_GAS: u64 = 4_000;
/// Gas charged by an older historical lookup.
///
/// Older heights may hit header storage/static files, so they are priced above the cheap recent
/// window to avoid making arbitrary historical scans as cheap as a context opcode.
pub const RANDOMNESS_BY_HEIGHT_LOOKUP_GAS: u64 = 20_000;

/// Gas/window policy for the randomness-by-height precompile.
///
/// These values are intentionally carried as a policy instead of being read directly from constants
/// at every call site. The defaults preserve the current Alpha behavior, while Gravity-specific
/// execution/RPC code can select a different policy by block height when a future hardfork changes
/// the pricing or recent-window size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RandomnessByHeightGasPolicy {
    /// Number of recent ancestor blocks charged at the cheaper recent-window price.
    pub recent_window: u64,
    /// Gas charged for current/future misses or recent-window lookups.
    pub recent_gas: u64,
    /// Gas charged for older historical lookups.
    pub lookup_gas: u64,
}

impl RandomnessByHeightGasPolicy {
    /// Default gas/window policy used by the Alpha implementation.
    pub const DEFAULT: Self = Self {
        recent_window: RANDOMNESS_BY_HEIGHT_RECENT_WINDOW,
        recent_gas: RANDOMNESS_BY_HEIGHT_RECENT_GAS,
        lookup_gas: RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
    };

    /// Creates a storage-backed lookup result using this policy.
    pub const fn storage(self, value: Option<B256>) -> RandomnessByHeightLookup {
        RandomnessByHeightLookup { value, gas_used: self.lookup_gas }
    }

    /// Creates a recent-window lookup result using this policy.
    pub const fn recent(self, value: Option<B256>) -> RandomnessByHeightLookup {
        RandomnessByHeightLookup { value, gas_used: self.recent_gas }
    }
}

/// Selects the randomness-by-height gas/window policy for `block_number`.
///
/// This currently returns the Alpha defaults for every post-Alpha block. Keep this as the shared
/// Gravity extension point for future hardforks that need different pricing or a different
/// recent-window size.
pub const fn randomness_by_height_gas_policy_at_block<ChainSpec: ?Sized>(
    _chain_spec: &ChainSpec,
    _block_number: u64,
) -> RandomnessByHeightGasPolicy {
    RandomnessByHeightGasPolicy::DEFAULT
}

/// Result returned by a randomness provider, including the gas cost for the chosen lookup path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RandomnessByHeightLookup {
    /// Block header `mix_hash` / `prev_randao`, if the block is known.
    pub value: Option<B256>,
    /// Gas charged for this lookup path.
    pub gas_used: u64,
}

impl RandomnessByHeightLookup {
    /// Creates a storage-backed lookup result.
    pub const fn storage(value: Option<B256>) -> Self {
        RandomnessByHeightGasPolicy::DEFAULT.storage(value)
    }

    /// Creates a recent-window lookup result.
    pub const fn recent(value: Option<B256>) -> Self {
        RandomnessByHeightGasPolicy::DEFAULT.recent(value)
    }
}

/// Read-only provider for canonical randomness values by block height.
pub trait RandomnessByHeightProvider {
    /// Provider lookup error.
    type Error: fmt::Display;

    /// Return `header.mix_hash` / `prev_randao` for `height` if the block is known, with gas.
    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error>;
}

/// Creates the historical randomness lookup precompile.
///
/// Input is a raw ABI word (`uint256 blockNumber`).
///
/// Output is ABI-word encoded as `(uint256 found, bytes32 randomness)`:
/// - bytes `[0..32]`: `found`, encoded as `0` or `1`.
/// - bytes `[32..64]`: the block header `mix_hash` / `prev_randao` value, or zero if not found.
///
/// A direct `cast call` therefore prints 64 bytes. For example, the leading
/// `0x000...001` word means `found = 1`; the following 32-byte word is the randomness value.
///
/// Heights before Alpha activation can have a header `mix_hash`, but callers must not treat those
/// values as Gravity protocol randomness.
pub fn create_randomness_by_height_precompile<Provider>(provider: Arc<Provider>) -> DynPrecompile
where
    Provider: RandomnessByHeightProvider + Send + Sync + 'static,
{
    let precompile_id = PrecompileId::custom("randomness_by_height");

    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        randomness_by_height_handler(input.data, input.gas, provider.as_ref())
    })
        .into()
}

/// Executes a randomness lookup with the precompile's complete gas semantics.
///
/// The provider selects the gas tier together with the lookup result, so the lookup must run before
/// the forwarded-gas check. This preserves the behavior of the Alloy precompile while allowing
/// other EVM adapters to share the same data-dependent gas handling.
pub fn randomness_by_height_handler<Provider>(
    data: &[u8],
    gas: u64,
    provider: &Provider,
) -> PrecompileResult
where
    Provider: RandomnessByHeightProvider + ?Sized,
{
    // Gas guard (mirrors `bls_precompile`): explicitly enforce the precompile's gas and
    // return OutOfGas when the call forwarded less than `gas_used`, instead of letting the
    // precompile dispatcher underflow-panic on `record_cost`. Without this, any user
    // transaction that calls this precompile with insufficient forwarded gas panics every
    // node — a network-wide consensus halt. The lookup itself is cheap, so charging after
    // computing the (data-dependent) gas tier is fine.
    let output = randomness_by_height_handler_raw(data, provider)?;
    if output.gas_used > gas {
        return Ok(PrecompileOutput::halt(PrecompileHalt::OutOfGas, 0));
    }
    Ok(output)
}

/// Core lookup logic separated from `PrecompileInput` for unit tests and RPC reuse.
pub fn randomness_by_height_handler_raw<Provider>(
    data: &[u8],
    provider: &Provider,
) -> PrecompileResult
where
    Provider: RandomnessByHeightProvider + ?Sized,
{
    if data.len() != RANDOMNESS_BY_HEIGHT_INPUT_LEN {
        warn!(
            target: "evm::precompile::randomness_by_height",
            input_len = data.len(),
            expected = RANDOMNESS_BY_HEIGHT_INPUT_LEN,
            "invalid input length"
        );
        return Ok(PrecompileOutput::halt(
            PrecompileHalt::other(format!(
                "expected exactly {RANDOMNESS_BY_HEIGHT_INPUT_LEN} bytes, got {}",
                data.len()
            )),
            0,
        ));
    }

    let height = U256::from_be_slice(data);
    if height > U256::from(u64::MAX) {
        return Ok(PrecompileOutput::new(
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
            encode_randomness_by_height_result(false, B256::ZERO),
            0,
        ));
    }

    let lookup = match provider.randomness_by_height(height.to::<u64>()) {
        Ok(lookup) => lookup,
        Err(err) => {
            return Ok(PrecompileOutput::halt(
                PrecompileHalt::other(format!("randomness lookup failed: {err}")),
                0,
            ));
        }
    };
    let bytes = match lookup.value {
        Some(value) => encode_randomness_by_height_result(true, value),
        None => encode_randomness_by_height_result(false, B256::ZERO),
    };
    Ok(PrecompileOutput::new(lookup.gas_used, bytes, 0))
}

/// Encodes `(uint256 found, bytes32 randomness)`.
pub fn encode_randomness_by_height_result(found: bool, randomness: B256) -> Bytes {
    let mut output = [0u8; RANDOMNESS_BY_HEIGHT_OUTPUT_LEN];
    if found {
        output[31] = 1;
    }
    output[32..].copy_from_slice(randomness.as_slice());
    Bytes::copy_from_slice(&output)
}

#[cfg(test)]
mod tests {
    use super::{
        randomness_by_height_handler, randomness_by_height_handler_raw,
        RandomnessByHeightGasPolicy, RandomnessByHeightLookup, RandomnessByHeightProvider,
        RANDOMNESS_BY_HEIGHT_LOOKUP_GAS, RANDOMNESS_BY_HEIGHT_RECENT_GAS,
    };
    use alloy_primitives::{B256, U256};
    use revm::precompile::{PrecompileHalt, PrecompileStatus};
    use std::{collections::BTreeMap, convert::Infallible};

    #[derive(Default)]
    struct MockRandomnessProvider {
        values: BTreeMap<u64, B256>,
    }

    #[derive(Default)]
    struct RecentMockRandomnessProvider {
        values: BTreeMap<u64, B256>,
    }

    impl RandomnessByHeightProvider for RecentMockRandomnessProvider {
        type Error = Infallible;

        fn randomness_by_height(
            &self,
            height: u64,
        ) -> Result<RandomnessByHeightLookup, Self::Error> {
            Ok(RandomnessByHeightLookup::recent(self.values.get(&height).copied()))
        }
    }

    impl RandomnessByHeightProvider for MockRandomnessProvider {
        type Error = Infallible;

        fn randomness_by_height(
            &self,
            height: u64,
        ) -> Result<RandomnessByHeightLookup, Self::Error> {
            Ok(RandomnessByHeightLookup::storage(self.values.get(&height).copied()))
        }
    }

    fn encode_height(height: U256) -> [u8; 32] {
        height.to_be_bytes()
    }

    #[test]
    fn returns_found_randomness() {
        let randomness = B256::repeat_byte(0xaa);
        let provider = MockRandomnessProvider { values: BTreeMap::from([(10, randomness)]) };

        let result = randomness_by_height_handler_raw(&encode_height(U256::from(10)), &provider)
            .expect("lookup succeeds");

        assert!(result.is_success());
        assert_eq!(result.gas_used, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS);
        assert_eq!(result.bytes[31], 1);
        assert_eq!(&result.bytes[32..64], randomness.as_slice());
    }

    #[test]
    fn missing_height_returns_not_found() {
        let provider = MockRandomnessProvider::default();

        let result = randomness_by_height_handler_raw(&encode_height(U256::from(10)), &provider)
            .expect("lookup succeeds");

        assert!(result.is_success());
        assert_eq!(result.bytes[31], 0);
        assert_eq!(&result.bytes[32..64], B256::ZERO.as_slice());
    }

    #[test]
    fn returns_provider_selected_recent_gas() {
        let randomness = B256::repeat_byte(0xbb);
        let provider = RecentMockRandomnessProvider { values: BTreeMap::from([(10, randomness)]) };

        let result = randomness_by_height_handler_raw(&encode_height(U256::from(10)), &provider)
            .expect("lookup succeeds");

        assert!(result.is_success());
        assert_eq!(result.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);
        assert_eq!(result.bytes[31], 1);
        assert_eq!(&result.bytes[32..64], randomness.as_slice());
    }

    #[test]
    fn lookup_can_use_custom_gas_policy() {
        let randomness = B256::repeat_byte(0xcc);
        let policy =
            RandomnessByHeightGasPolicy { recent_window: 8, recent_gas: 123, lookup_gas: 456 };

        assert_eq!(policy.recent(Some(randomness)).gas_used, 123);
        assert_eq!(policy.storage(Some(randomness)).gas_used, 456);
    }

    #[test]
    fn height_over_u64_returns_not_found() {
        let provider = MockRandomnessProvider::default();

        let result = randomness_by_height_handler_raw(
            &encode_height(U256::from(u64::MAX) + U256::from(1)),
            &provider,
        )
        .expect("lookup succeeds");

        assert!(result.is_success());
        // Oversized heights never reach the provider and retain the historical-lookup price.
        assert_eq!(result.gas_used, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS);
        assert_eq!(result.bytes[31], 0);
        assert_eq!(&result.bytes[32..64], B256::ZERO.as_slice());
    }

    #[test]
    fn invalid_input_length_errors() {
        let provider = MockRandomnessProvider::default();
        let output =
            randomness_by_height_handler_raw(&[1, 2, 3], &provider).expect("halt is not fatal");
        assert!(output.is_halt());
    }

    #[test]
    fn gas_aware_handler_enforces_provider_selected_tier() {
        let height = encode_height(U256::from(10));
        let lookup_provider = MockRandomnessProvider::default();
        let lookup_oog = randomness_by_height_handler(
            &height,
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS - 1,
            &lookup_provider,
        )
        .unwrap();
        assert!(matches!(lookup_oog.status, PrecompileStatus::Halt(PrecompileHalt::OutOfGas)));
        assert_eq!(lookup_oog.gas_used, 0);

        let lookup_success = randomness_by_height_handler(
            &height,
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
            &lookup_provider,
        )
        .unwrap();
        assert!(lookup_success.is_success());
        assert_eq!(lookup_success.gas_used, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS);

        let recent_provider = RecentMockRandomnessProvider::default();
        let recent_success = randomness_by_height_handler(
            &height,
            RANDOMNESS_BY_HEIGHT_RECENT_GAS,
            &recent_provider,
        )
        .unwrap();
        assert!(recent_success.is_success());
        assert_eq!(recent_success.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);

        let recent_oog = randomness_by_height_handler(
            &height,
            RANDOMNESS_BY_HEIGHT_RECENT_GAS - 1,
            &recent_provider,
        )
        .unwrap();
        assert!(matches!(recent_oog.status, PrecompileStatus::Halt(PrecompileHalt::OutOfGas)));
        assert_eq!(recent_oog.gas_used, 0);
    }
}
