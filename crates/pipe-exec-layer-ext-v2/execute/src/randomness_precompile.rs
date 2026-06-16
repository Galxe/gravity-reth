//! Storage adapter for Gravity's historical randomness lookup precompile.

use alloy_primitives::B256;
use gravity_precompiles::randomness_by_height::{
    RandomnessByHeightGasPolicy, RandomnessByHeightLookup, RandomnessByHeightProvider,
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
    gas_policy: RandomnessByHeightGasPolicy,
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
        Self::new_with_gas_policy(
            fallback,
            current_number,
            current_randomness,
            parent_number,
            parent_randomness,
            RandomnessByHeightGasPolicy::DEFAULT,
        )
    }

    /// Creates a live execution randomness provider with an explicit gas/window policy.
    pub const fn new_with_gas_policy(
        fallback: Fallback,
        current_number: u64,
        current_randomness: B256,
        parent_number: u64,
        parent_randomness: Option<B256>,
        gas_policy: RandomnessByHeightGasPolicy,
    ) -> Self {
        Self {
            fallback,
            current_number,
            current_randomness,
            parent_number,
            parent_randomness,
            gas_policy,
        }
    }
}

impl<Fallback> RandomnessByHeightProvider for ExecutionRandomnessProvider<Fallback>
where
    Fallback: RandomnessByHeightProvider,
{
    type Error = Fallback::Error;

    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error> {
        if height == self.current_number {
            return Ok(self.gas_policy.recent(Some(self.current_randomness)));
        }

        if height == self.parent_number {
            return Ok(self.gas_policy.recent(self.parent_randomness));
        }

        if height > self.current_number {
            return Ok(self.gas_policy.recent(None));
        }

        if self.current_number - height <= self.gas_policy.recent_window {
            return self
                .fallback
                .randomness_by_height(height)
                .map(|lookup| self.gas_policy.recent(lookup.value));
        }

        self.fallback
            .randomness_by_height(height)
            .map(|lookup| self.gas_policy.storage(lookup.value))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionRandomnessProvider, RandomnessByHeightGasPolicy, RandomnessByHeightLookup,
        RandomnessByHeightProvider, RANDOMNESS_BY_HEIGHT_LOOKUP_GAS,
        RANDOMNESS_BY_HEIGHT_RECENT_GAS, RANDOMNESS_BY_HEIGHT_RECENT_WINDOW,
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

    #[test]
    fn explicit_gas_policy_controls_window_and_prices() {
        let policy =
            RandomnessByHeightGasPolicy { recent_window: 2, recent_gas: 77, lookup_gas: 99 };
        let provider = ExecutionRandomnessProvider::new_with_gas_policy(
            MockFallback {
                values: BTreeMap::from([
                    (CURRENT_NUMBER - 2, B256::repeat_byte(0x22)),
                    (CURRENT_NUMBER - 3, B256::repeat_byte(0x21)),
                ]),
            },
            CURRENT_NUMBER,
            B256::repeat_byte(0xaa),
            PARENT_NUMBER,
            Some(B256::repeat_byte(0xbb)),
            policy,
        );

        let recent = provider.randomness_by_height(CURRENT_NUMBER - 2).expect("recent lookup");
        assert_eq!(recent.value, Some(B256::repeat_byte(0x22)));
        assert_eq!(recent.gas_used, 77);

        let older = provider.randomness_by_height(CURRENT_NUMBER - 3).expect("older lookup");
        assert_eq!(older.value, Some(B256::repeat_byte(0x21)));
        assert_eq!(older.gas_used, 99);
    }
}
