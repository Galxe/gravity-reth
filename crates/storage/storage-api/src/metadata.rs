//! Metadata provider traits for node-local storage layout settings.

use alloc::vec::Vec;
use reth_db_api::models::GravityStorageSettings;
use reth_storage_errors::provider::ProviderResult;

/// Metadata keys.
pub mod keys {
    /// Persisted storage layout settings for this node.
    pub const GRAVITY_STORAGE_SETTINGS: &str = "gravity_storage_settings";
}

/// Client trait for reading node metadata from the database.
#[auto_impl::auto_impl(&, Arc)]
pub trait MetadataProvider: Send {
    /// Get a metadata value by key.
    fn get_metadata(&self, key: &str) -> ProviderResult<Option<Vec<u8>>>;

    /// Get the persisted storage layout settings.
    ///
    /// Returns `None` when the entry is missing or can't be deserialized — callers treat both
    /// as [`GravityStorageSettings::legacy`] so a database that predates the settings (or a
    /// metadata schema change) keeps working without migration.
    fn storage_settings(&self) -> ProviderResult<Option<GravityStorageSettings>> {
        Ok(self
            .get_metadata(keys::GRAVITY_STORAGE_SETTINGS)?
            .and_then(|bytes| GravityStorageSettings::from_metadata_bytes(&bytes)))
    }
}

/// Client trait for writing node metadata to the database.
#[auto_impl::auto_impl(&, Arc)]
pub trait MetadataWriter: Send {
    /// Write a metadata value by key.
    fn write_metadata(&self, key: &str, value: Vec<u8>) -> ProviderResult<()>;

    /// Persist the storage layout settings.
    ///
    /// Only `init_genesis` should call this for a fresh database: existing databases keep the
    /// settings persisted in their metadata, and CLI flags must never override them.
    fn write_storage_settings(&self, settings: GravityStorageSettings) -> ProviderResult<()> {
        self.write_metadata(keys::GRAVITY_STORAGE_SETTINGS, settings.to_metadata_bytes())
    }
}
