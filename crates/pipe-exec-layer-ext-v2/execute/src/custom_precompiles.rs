//! Capability-restricted adapters for precompiles used by grevm user transactions.

use gravity_precompiles::{
    bls_pop_verify::bls_pop_verify_handler,
    randomness_by_height::{randomness_by_height_handler, RandomnessByHeightProvider},
};
use grevm::DynParallelPrecompile;
use revm::precompile::PrecompileId;
use std::sync::Arc;

/// Creates the capability-restricted BLS proof-of-possession precompile.
pub(crate) fn create_bls_pop_verify_precompile() -> DynParallelPrecompile {
    DynParallelPrecompile::new(PrecompileId::custom("bls_pop_verify"), |input| {
        bls_pop_verify_handler(input.data(), input.gas()).map_err(Into::into)
    })
}

/// Creates a capability-restricted historical-randomness precompile.
///
/// `provider` must be immutable for the lifetime of the executing block and return deterministic
/// results: grevm may invoke this closure concurrently and may retry the same transaction after a
/// speculative conflict.
/// [`ExecutionRandomnessProvider`](super::randomness_precompile::ExecutionRandomnessProvider)
/// satisfies this by capturing the current/parent block values and using a read-only canonical
/// storage fallback.
pub(crate) fn create_randomness_by_height_precompile<Provider>(
    provider: Arc<Provider>,
) -> DynParallelPrecompile
where
    Provider: RandomnessByHeightProvider + Send + Sync + 'static,
{
    DynParallelPrecompile::new(PrecompileId::custom("randomness_by_height"), move |input| {
        randomness_by_height_handler(input.data(), input.gas(), provider.as_ref())
            .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use gravity_precompiles::{
        bls_pop_verify::POP_VERIFY_GAS,
        randomness_by_height::{
            RandomnessByHeightLookup, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
            RANDOMNESS_BY_HEIGHT_RECENT_GAS,
        },
    };
    use hex_literal::hex;
    use reth_evm::{
        eth::EthEvmContext,
        precompiles::{Precompile, PrecompileInput},
        EvmInternals,
    };
    use revm::{
        database::EmptyDB,
        precompile::{PrecompileHalt, PrecompileOutput, PrecompileResult, PrecompileStatus},
    };

    const PRECOMPILE_ADDRESS: Address = Address::with_last_byte(0xfe);

    // Deterministically generated from secret-key material `[42; 32]` using the protocol's PoP
    // domain separation tag. Keeping this as a fixture avoids adding `blst` to this crate solely
    // for adapter tests.
    const VALID_BLS_POP_INPUT: [u8; 144] = hex!(
        "8ae7e5822ba97ab07877ea318e747499da648b27302414f9d0b9bb7e3646d248"
        "be90c9fdaddfdb93485a6e9334f01093"
        "b16db5b947dda6c513b24b8724b659996826bfb69a8914f1b295e39572f40923"
        "e08150a0bdd12d0ee920e9a1e33acf81192230e9f074e350555315a427264246"
        "ab03b99601738c4179746e73913388b68285a854e85be32b1539ec925dd3d7fe"
    );

    #[derive(Clone, Copy)]
    struct FixedRandomnessProvider {
        lookup: Result<RandomnessByHeightLookup, &'static str>,
    }

    impl RandomnessByHeightProvider for FixedRandomnessProvider {
        type Error = &'static str;

        fn randomness_by_height(
            &self,
            _height: u64,
        ) -> Result<RandomnessByHeightLookup, Self::Error> {
            self.lookup
        }
    }

    fn call_adapter(precompile: &DynParallelPrecompile, data: &[u8], gas: u64) -> PrecompileResult {
        let mut ctx = EthEvmContext::new(EmptyDB::default(), Default::default());
        precompile.to_alloy().call(PrecompileInput {
            data,
            gas,
            reservoir: 0,
            caller: Address::ZERO,
            value: U256::ZERO,
            target_address: PRECOMPILE_ADDRESS,
            bytecode_address: PRECOMPILE_ADDRESS,
            is_static: false,
            internals: EvmInternals::from_context(&mut ctx),
        })
    }

    fn assert_adapter_matches_handler(
        precompile: &DynParallelPrecompile,
        data: &[u8],
        gas: u64,
        expected: PrecompileResult,
    ) -> PrecompileOutput {
        let actual = call_adapter(precompile, data, gas);
        assert_eq!(actual, expected);
        actual.expect("tested handler result must be non-fatal")
    }

    #[test]
    fn bls_adapter_preserves_low_gas_and_exact_gas_success() {
        let precompile = create_bls_pop_verify_precompile();

        let out_of_gas = assert_adapter_matches_handler(
            &precompile,
            &VALID_BLS_POP_INPUT,
            POP_VERIFY_GAS - 1,
            bls_pop_verify_handler(&VALID_BLS_POP_INPUT, POP_VERIFY_GAS - 1),
        );
        assert_eq!(out_of_gas.status, PrecompileStatus::Halt(PrecompileHalt::OutOfGas));
        assert_eq!(out_of_gas.gas_used, 0);

        let success = assert_adapter_matches_handler(
            &precompile,
            &VALID_BLS_POP_INPUT,
            POP_VERIFY_GAS,
            bls_pop_verify_handler(&VALID_BLS_POP_INPUT, POP_VERIFY_GAS),
        );
        assert!(success.is_success());
        assert_eq!(success.gas_used, POP_VERIFY_GAS);
        assert_eq!(success.bytes.len(), 32);
        assert_eq!(success.bytes[31], 1);
    }

    #[test]
    fn randomness_adapter_preserves_historical_gas_boundaries_and_output() {
        let randomness = B256::repeat_byte(0xa5);
        let provider = Arc::new(FixedRandomnessProvider {
            lookup: Ok(RandomnessByHeightLookup::storage(Some(randomness))),
        });
        let precompile = create_randomness_by_height_precompile(provider.clone());
        let input = U256::from(7).to_be_bytes::<32>();

        let success = assert_adapter_matches_handler(
            &precompile,
            &input,
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
            randomness_by_height_handler(
                &input,
                RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
                provider.as_ref(),
            ),
        );
        assert!(success.is_success());
        assert_eq!(success.gas_used, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS);
        assert_eq!(success.bytes[31], 1);
        assert_eq!(&success.bytes[32..], randomness.as_slice());

        let out_of_gas = assert_adapter_matches_handler(
            &precompile,
            &input,
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS - 1,
            randomness_by_height_handler(
                &input,
                RANDOMNESS_BY_HEIGHT_LOOKUP_GAS - 1,
                provider.as_ref(),
            ),
        );
        assert_eq!(out_of_gas.status, PrecompileStatus::Halt(PrecompileHalt::OutOfGas));
        assert_eq!(out_of_gas.gas_used, 0);
    }

    #[test]
    fn randomness_adapter_preserves_recent_gas_boundaries_and_invalid_input() {
        let randomness = B256::repeat_byte(0x5a);
        let provider = Arc::new(FixedRandomnessProvider {
            lookup: Ok(RandomnessByHeightLookup::recent(Some(randomness))),
        });
        let precompile = create_randomness_by_height_precompile(provider.clone());
        let input = U256::from(8).to_be_bytes::<32>();

        let success = assert_adapter_matches_handler(
            &precompile,
            &input,
            RANDOMNESS_BY_HEIGHT_RECENT_GAS,
            randomness_by_height_handler(
                &input,
                RANDOMNESS_BY_HEIGHT_RECENT_GAS,
                provider.as_ref(),
            ),
        );
        assert!(success.is_success());
        assert_eq!(success.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);
        assert_eq!(success.bytes[31], 1);
        assert_eq!(&success.bytes[32..], randomness.as_slice());

        let out_of_gas = assert_adapter_matches_handler(
            &precompile,
            &input,
            RANDOMNESS_BY_HEIGHT_RECENT_GAS - 1,
            randomness_by_height_handler(
                &input,
                RANDOMNESS_BY_HEIGHT_RECENT_GAS - 1,
                provider.as_ref(),
            ),
        );
        assert_eq!(out_of_gas.status, PrecompileStatus::Halt(PrecompileHalt::OutOfGas));
        assert_eq!(out_of_gas.gas_used, 0);

        let invalid = assert_adapter_matches_handler(
            &precompile,
            &[0xff],
            RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
            randomness_by_height_handler(
                &[0xff],
                RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
                provider.as_ref(),
            ),
        );
        assert!(matches!(invalid.status, PrecompileStatus::Halt(PrecompileHalt::Other(_))));
        assert_eq!(invalid.gas_used, 0);
    }
}
