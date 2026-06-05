//! Storage adapter for Gravity's historical randomness lookup precompile.

use alloy_primitives::B256;
use gravity_precompiles::randomness_by_height::{
    RandomnessByHeightLookup, RandomnessByHeightProvider,
};
use gravity_storage::GravityStorage;
use reth_provider::ProviderError;
use std::sync::Arc;

pub use gravity_precompiles::randomness_by_height::{
    create_randomness_by_height_precompile, encode_randomness_by_height_result,
    randomness_by_height_handler_raw, RANDOMNESS_BY_HEIGHT_INPUT_LEN,
    RANDOMNESS_BY_HEIGHT_LOOKUP_GAS, RANDOMNESS_BY_HEIGHT_OUTPUT_LEN,
    RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR, RANDOMNESS_BY_HEIGHT_RECENT_GAS,
    RANDOMNESS_BY_HEIGHT_RECENT_WINDOW,
};

/// Adapter that exposes [`GravityStorage::randomness_by_height`] to the shared precompile handler.
#[derive(Debug)]
pub struct GravityStorageRandomnessProvider<Storage> {
    storage: Arc<Storage>,
}

impl<Storage> GravityStorageRandomnessProvider<Storage> {
    /// Creates a new randomness provider backed by Gravity storage.
    pub const fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl<Storage> RandomnessByHeightProvider for GravityStorageRandomnessProvider<Storage>
where
    Storage: GravityStorage,
{
    type Error = ProviderError;

    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error> {
        GravityStorage::randomness_by_height(self.storage.as_ref(), height)
            .map(RandomnessByHeightLookup::storage)
    }
}

/// Randomness provider used while executing a live ordered block.
///
/// The current block is not canonical or persisted while its user transactions are executing, so
/// `block.number -> header.mix_hash` must come from the execution context. The parent header is
/// also supplied explicitly to avoid depending on persistence timing. Older heights fall back to
/// the canonical storage provider.
#[derive(Debug)]
pub struct ExecutionRandomnessProvider<Fallback> {
    fallback: Fallback,
    current_number: u64,
    current_randomness: B256,
    parent_number: u64,
    parent_randomness: Option<B256>,
}

impl<Fallback> ExecutionRandomnessProvider<Fallback> {
    /// Creates a live execution randomness provider.
    pub const fn new(
        fallback: Fallback,
        current_number: u64,
        current_randomness: B256,
        parent_number: u64,
        parent_randomness: Option<B256>,
    ) -> Self {
        Self { fallback, current_number, current_randomness, parent_number, parent_randomness }
    }
}

impl<Fallback> RandomnessByHeightProvider for ExecutionRandomnessProvider<Fallback>
where
    Fallback: RandomnessByHeightProvider,
{
    type Error = Fallback::Error;

    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error> {
        if height == self.current_number {
            return Ok(RandomnessByHeightLookup::recent(Some(self.current_randomness)));
        }

        if height == self.parent_number {
            return Ok(RandomnessByHeightLookup::recent(self.parent_randomness));
        }

        if height > self.current_number {
            return Ok(RandomnessByHeightLookup::recent(None));
        }

        if self.current_number - height <= RANDOMNESS_BY_HEIGHT_RECENT_WINDOW {
            return self
                .fallback
                .randomness_by_height(height)
                .map(|lookup| RandomnessByHeightLookup::recent(lookup.value));
        }

        self.fallback.randomness_by_height(height)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionRandomnessProvider, RandomnessByHeightLookup, RandomnessByHeightProvider,
        RANDOMNESS_BY_HEIGHT_LOOKUP_GAS, RANDOMNESS_BY_HEIGHT_RECENT_GAS,
        RANDOMNESS_BY_HEIGHT_RECENT_WINDOW,
    };
    use alloy_primitives::B256;
    use std::{collections::BTreeMap, convert::Infallible};

    const CURRENT_NUMBER: u64 = 100_000;
    const PARENT_NUMBER: u64 = CURRENT_NUMBER - 1;

    #[derive(Default)]
    struct MockFallback {
        values: BTreeMap<u64, B256>,
    }

    impl RandomnessByHeightProvider for MockFallback {
        type Error = Infallible;

        fn randomness_by_height(
            &self,
            height: u64,
        ) -> Result<RandomnessByHeightLookup, Self::Error> {
            Ok(RandomnessByHeightLookup::storage(self.values.get(&height).copied()))
        }
    }

    fn provider() -> ExecutionRandomnessProvider<MockFallback> {
        ExecutionRandomnessProvider::new(
            MockFallback {
                values: BTreeMap::from([
                    (CURRENT_NUMBER - RANDOMNESS_BY_HEIGHT_RECENT_WINDOW, B256::repeat_byte(0x44)),
                    (
                        CURRENT_NUMBER - RANDOMNESS_BY_HEIGHT_RECENT_WINDOW - 1,
                        B256::repeat_byte(0x43),
                    ),
                ]),
            },
            CURRENT_NUMBER,
            B256::repeat_byte(0xaa),
            PARENT_NUMBER,
            Some(B256::repeat_byte(0xbb)),
        )
    }

    #[test]
    fn current_and_parent_use_recent_gas() {
        let provider = provider();

        let current = provider.randomness_by_height(CURRENT_NUMBER).expect("current lookup");
        assert_eq!(current.value, Some(B256::repeat_byte(0xaa)));
        assert_eq!(current.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);

        let parent = provider.randomness_by_height(PARENT_NUMBER).expect("parent lookup");
        assert_eq!(parent.value, Some(B256::repeat_byte(0xbb)));
        assert_eq!(parent.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);
    }

    #[test]
    fn future_height_returns_recent_not_found_without_fallback() {
        let provider = provider();

        let lookup = provider.randomness_by_height(CURRENT_NUMBER + 1).expect("future lookup");
        assert_eq!(lookup.value, None);
        assert_eq!(lookup.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);
    }

    #[test]
    fn recent_storage_lookup_uses_recent_gas() {
        let provider = provider();
        let recent_height = CURRENT_NUMBER - RANDOMNESS_BY_HEIGHT_RECENT_WINDOW;

        let lookup = provider.randomness_by_height(recent_height).expect("recent lookup");
        assert_eq!(lookup.value, Some(B256::repeat_byte(0x44)));
        assert_eq!(lookup.gas_used, RANDOMNESS_BY_HEIGHT_RECENT_GAS);
    }

    #[test]
    fn older_storage_lookup_uses_lookup_gas() {
        let provider = provider();
        let older_height = CURRENT_NUMBER - RANDOMNESS_BY_HEIGHT_RECENT_WINDOW - 1;

        let lookup = provider.randomness_by_height(older_height).expect("older lookup");
        assert_eq!(lookup.value, Some(B256::repeat_byte(0x43)));
        assert_eq!(lookup.gas_used, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS);
    }
}
