//! Storage adapter for Gravity's historical randomness lookup precompile.

use alloy_primitives::B256;
use gravity_precompiles::randomness_by_height::RandomnessByHeightProvider;
use gravity_storage::GravityStorage;
use reth_provider::ProviderError;
use std::sync::Arc;

pub use gravity_precompiles::randomness_by_height::{
    create_randomness_by_height_precompile, encode_randomness_by_height_result,
    randomness_by_height_handler_raw, RANDOMNESS_BY_HEIGHT_INPUT_LEN,
    RANDOMNESS_BY_HEIGHT_LOOKUP_GAS, RANDOMNESS_BY_HEIGHT_OUTPUT_LEN,
    RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR,
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

    fn randomness_by_height(&self, height: u64) -> Result<Option<B256>, Self::Error> {
        GravityStorage::randomness_by_height(self.storage.as_ref(), height)
    }
}
