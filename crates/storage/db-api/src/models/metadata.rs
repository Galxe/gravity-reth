//! Node-local storage layout metadata.

use serde::{Deserialize, Serialize};

/// Persisted storage layout settings for this node.
///
/// The layout is decided once per datadir: `init_genesis` persists the settings for a fresh
/// database, and existing databases always keep the settings stored in their [`Metadata`]
/// table — a missing entry means the legacy layout that predates this struct. CLI flags must
/// never override persisted settings, so a binary upgrade can not silently reinterpret data
/// written under another layout.
///
/// Serialized as JSON so that unknown fields from newer binaries are tolerated and missing
/// fields from older entries fall back to their legacy default.
///
/// [`Metadata`]: crate::tables::Metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GravityStorageSettings {
    /// Whether account/storage changesets live in static files instead of the state `RocksDB`.
    #[serde(default)]
    pub changesets_in_static_files: bool,
}

impl GravityStorageSettings {
    /// Layout of databases created before settings were persisted.
    pub const fn legacy() -> Self {
        Self { changesets_in_static_files: false }
    }

    /// Default layout written for freshly initialized databases.
    ///
    /// Still the legacy layout: the static-file changeset layout is opt-in per fresh datadir
    /// (`--storage.v2`, wired through `init_genesis_with_settings`). Flipping this default is
    /// a product decision that would switch every new datadir over.
    pub const fn current() -> Self {
        Self::legacy()
    }

    /// Encodes the settings for storage in the metadata table.
    pub fn to_metadata_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("struct of plain fields serializes infallibly")
    }

    /// Decodes settings from the metadata table.
    ///
    /// Returns `None` when the bytes can't be deserialized (e.g. the schema changed) so that
    /// callers can fall back to [`Self::legacy`] and tooling like `db` commands keeps working
    /// across metadata schema changes.
    pub fn from_metadata_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_roundtrip() {
        let settings = GravityStorageSettings { changesets_in_static_files: true };
        let bytes = settings.to_metadata_bytes();
        assert_eq!(GravityStorageSettings::from_metadata_bytes(&bytes), Some(settings));
    }

    #[test]
    fn tolerates_schema_evolution() {
        // Entry written before the field existed: falls back to the field's default.
        assert_eq!(
            GravityStorageSettings::from_metadata_bytes(b"{}"),
            Some(GravityStorageSettings::legacy())
        );
        // Entry written by a newer binary with extra fields: unknown fields are ignored.
        assert_eq!(
            GravityStorageSettings::from_metadata_bytes(
                br#"{"changesets_in_static_files":true,"future_field":42}"#
            ),
            Some(GravityStorageSettings { changesets_in_static_files: true })
        );
        // Garbage decodes to `None` rather than an error.
        assert_eq!(GravityStorageSettings::from_metadata_bytes(b"not json"), None);
    }
}
