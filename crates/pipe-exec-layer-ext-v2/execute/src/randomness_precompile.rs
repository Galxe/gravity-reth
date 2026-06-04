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

    fn randomness_by_height(&self, height: u64) -> Result<Option<B256>, Self::Error> {
        if height == self.current_number {
            return Ok(Some(self.current_randomness));
        }

        if height == self.parent_number {
            return Ok(self.parent_randomness);
        }

        self.fallback.randomness_by_height(height)
    }
}
