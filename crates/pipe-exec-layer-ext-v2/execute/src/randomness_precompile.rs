//! Historical randomness lookup precompile.
//!
//! This read-only precompile returns the canonical post-Merge randomness value
//! stored in a block header's `mix_hash` / `prev_randao` field.

use alloy_primitives::{Bytes, B256, U256};
use gravity_storage::GravityStorage;
use reth_evm::precompiles::{DynPrecompile, PrecompileInput};
use reth_provider::ProviderResult;
use revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use std::sync::Arc;
use tracing::warn;

const INPUT_LEN: usize = 32;
const OUTPUT_LEN: usize = 64;
const LOOKUP_GAS: u64 = 2_000;

/// Read-only provider for canonical randomness values by block height.
pub trait RandomnessByHeightProvider: Send + Sync + 'static {
    /// Return `header.mix_hash` / `prev_randao` for `height` if the block is known.
    fn randomness_by_height(&self, height: u64) -> ProviderResult<Option<B256>>;
}

impl<S> RandomnessByHeightProvider for S
where
    S: GravityStorage,
{
    fn randomness_by_height(&self, height: u64) -> ProviderResult<Option<B256>> {
        GravityStorage::randomness_by_height(self, height)
    }
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
pub fn create_randomness_by_height_precompile(
    provider: Arc<dyn RandomnessByHeightProvider>,
) -> DynPrecompile {
    let precompile_id = PrecompileId::custom("randomness_by_height");

    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        randomness_by_height_handler_raw(input.data, provider.as_ref())
    })
        .into()
}

/// Core lookup logic separated from `PrecompileInput` for unit tests.
pub fn randomness_by_height_handler_raw(
    data: &[u8],
    provider: &dyn RandomnessByHeightProvider,
) -> PrecompileResult {
    if data.len() != INPUT_LEN {
        warn!(
            target: "evm::precompile::randomness_by_height",
            input_len = data.len(),
            expected = INPUT_LEN,
            "invalid input length"
        );
        return Err(PrecompileError::Other(
            format!("expected exactly {INPUT_LEN} bytes, got {}", data.len()).into(),
        ));
    }

    let height = U256::from_be_slice(data);
    if height > U256::from(u64::MAX) {
        return Ok(PrecompileOutput {
            gas_used: LOOKUP_GAS,
            bytes: encode_result(false, B256::ZERO),
            reverted: false,
        });
    }

    let randomness = provider
        .randomness_by_height(height.to::<u64>())
        .map_err(|err| PrecompileError::Other(format!("randomness lookup failed: {err}").into()))?;
    Ok(PrecompileOutput {
        gas_used: LOOKUP_GAS,
        bytes: match randomness {
            Some(value) => encode_result(true, value),
            None => encode_result(false, B256::ZERO),
        },
        reverted: false,
    })
}

fn encode_result(found: bool, randomness: B256) -> Bytes {
    let mut output = [0u8; OUTPUT_LEN];
    if found {
        output[31] = 1;
    }
    output[32..].copy_from_slice(randomness.as_slice());
    Bytes::copy_from_slice(&output)
}

#[cfg(test)]
mod tests {
    use super::{randomness_by_height_handler_raw, RandomnessByHeightProvider};
    use alloy_primitives::{B256, U256};
    use reth_provider::ProviderResult;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct MockRandomnessProvider {
        values: BTreeMap<u64, B256>,
    }

    impl RandomnessByHeightProvider for MockRandomnessProvider {
        fn randomness_by_height(&self, height: u64) -> ProviderResult<Option<B256>> {
            Ok(self.values.get(&height).copied())
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

        assert!(!result.reverted);
        assert_eq!(result.bytes[31], 1);
        assert_eq!(&result.bytes[32..64], randomness.as_slice());
    }

    #[test]
    fn missing_height_returns_not_found() {
        let provider = MockRandomnessProvider::default();

        let result = randomness_by_height_handler_raw(&encode_height(U256::from(10)), &provider)
            .expect("lookup succeeds");

        assert!(!result.reverted);
        assert_eq!(result.bytes[31], 0);
        assert_eq!(&result.bytes[32..64], B256::ZERO.as_slice());
    }

    #[test]
    fn height_over_u64_returns_not_found() {
        let provider = MockRandomnessProvider::default();

        let result = randomness_by_height_handler_raw(
            &encode_height(U256::from(u64::MAX) + U256::from(1)),
            &provider,
        )
        .expect("lookup succeeds");

        assert!(!result.reverted);
        assert_eq!(result.bytes[31], 0);
        assert_eq!(&result.bytes[32..64], B256::ZERO.as_slice());
    }

    #[test]
    fn invalid_input_length_errors() {
        let provider = MockRandomnessProvider::default();
        assert!(randomness_by_height_handler_raw(&[1, 2, 3], &provider).is_err());
    }
}
