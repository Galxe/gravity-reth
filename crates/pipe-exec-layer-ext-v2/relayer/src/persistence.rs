//! Relayer State Persistence
//!
//! Persists nonce and block cursor state to disk for fast restart.

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use tracing::{debug, info, warn};

/// State file name within datadir
const STATE_FILE_NAME: &str = "relayer_state.json";

/// Current schema version
const SCHEMA_VERSION: u32 = 1;

/// Persisted state for a single data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceState {
    /// Source type (0 = BLOCKCHAIN)
    pub source_type: u32,
    /// Source ID (chain ID for blockchain sources)
    pub source_id: u64,
    /// Last nonce we fetched and returned
    pub last_nonce: u128,
    /// Block number where last_nonce was emitted
    pub last_nonce_block: u64,
    /// Current polling cursor (block number)
    pub cursor_block: u64,
    /// ISO 8601 timestamp of last update
    pub updated_at: String,
    /// Block hash at the last confirmed processed block, used for reorg detection (GRETH-012).
    /// If this hash changes on the next poll, a chain reorganization has occurred.
    #[serde(default)]
    pub last_confirmed_block_hash: Option<String>,

    /// keccak256 checksum over the core state fields (GRETH-017).
    ///
    /// Detects accidental file corruption. Computed on every write and verified
    /// on load. Note: a determined attacker with write access could recompute this
    /// checksum; HMAC with a node-specific key would provide stronger guarantees.
    #[serde(default)]
    pub checksum: Option<String>,
}

impl SourceState {
    /// Compute the integrity checksum for this entry.
    ///
    /// Covers all mutable state fields so that any accidental modification
    /// (bit flip, partial write, truncation) is detected on next load.
    fn compute_checksum(&self) -> String {
        let data = format!(
            "v1:{}:{}:{}:{}:{}:{}:{}",
            self.source_type,
            self.source_id,
            self.last_nonce,
            self.last_nonce_block,
            self.cursor_block,
            self.updated_at,
            self.last_confirmed_block_hash.as_deref().unwrap_or(""),
        );
        let hash = keccak256(data.as_bytes());
        format!("{hash:x}")
    }

    /// Verify the stored checksum matches a freshly computed one.
    ///
    /// Returns `Ok(())` when no checksum is stored (backward compatibility)
    /// or when the checksum matches. Returns an error on mismatch.
    pub fn verify_checksum(&self) -> Result<()> {
        if let Some(stored) = &self.checksum {
            let expected = self.compute_checksum();
            if stored != &expected {
                anyhow::bail!(
                    "State integrity check failed for source {}:{}: \
                     checksum mismatch (expected {}, got {}). \
                     The state file may be corrupted.",
                    self.source_type,
                    self.source_id,
                    expected,
                    stored
                );
            }
        }
        Ok(())
    }
}

/// Root structure for persisted relayer state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelayerState {
    /// Schema version for forward compatibility
    pub version: u32,
    /// Map from URI to source state
    pub sources: HashMap<String, SourceState>,
}

impl RelayerState {
    /// Create a new empty state
    pub fn new() -> Self {
        Self { version: SCHEMA_VERSION, sources: HashMap::new() }
    }

    /// Load state from a file
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read state file: {}", path.display()))?;

        let state: Self = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse state file: {}", path.display()))?;

        if state.version != SCHEMA_VERSION {
            warn!(
                "State file version {} differs from current version {}",
                state.version, SCHEMA_VERSION
            );
        }

        // GRETH-017: verify per-source checksums to detect file corruption.
        for (uri, source) in &state.sources {
            if let Err(e) = source.verify_checksum() {
                warn!(
                    target: "relayer::persistence",
                    uri = %uri,
                    error = %e,
                    "State file integrity check failed — treating as corrupted and refusing to use"
                );
                anyhow::bail!(
                    "State file integrity check failed for '{}': {}",
                    path.display(),
                    e
                );
            }
        }

        debug!("Loaded relayer state with {} sources from {}", state.sources.len(), path.display());

        Ok(state)
    }

    /// Save state to a file
    pub fn save(&self, path: &Path) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        let content = serde_json::to_string_pretty(self).context("Failed to serialize state")?;

        // Write to temp file first, then rename for atomicity
        let temp_path = path.with_extension("json.tmp");
        fs::write(&temp_path, &content)
            .with_context(|| format!("Failed to write temp file: {}", temp_path.display()))?;

        fs::rename(&temp_path, path).with_context(|| {
            format!("Failed to rename {} to {}", temp_path.display(), path.display())
        })?;

        debug!("Saved relayer state to {}", path.display());
        Ok(())
    }

    /// Get state for a URI
    pub fn get(&self, uri: &str) -> Option<&SourceState> {
        self.sources.get(uri)
    }

    /// Update state for a URI
    pub fn update(
        &mut self,
        uri: &str,
        source_type: u32,
        source_id: u64,
        last_nonce: u128,
        last_nonce_block: u64,
        cursor_block: u64,
    ) {
        let now = chrono::Utc::now().to_rfc3339();
        let mut state = SourceState {
            source_type,
            source_id,
            last_nonce,
            last_nonce_block,
            cursor_block,
            updated_at: now,
            last_confirmed_block_hash: None,
            checksum: None,
        };
        // GRETH-017: compute and store integrity checksum on every write.
        state.checksum = Some(state.compute_checksum());
        self.sources.insert(uri.to_string(), state);
    }
}

/// Helper to get state file path from datadir
pub fn state_file_path(datadir: &Path) -> PathBuf {
    datadir.join(STATE_FILE_NAME)
}

/// Load state from datadir, returning None if file doesn't exist
pub fn load_state_if_exists(datadir: &Path) -> Option<RelayerState> {
    let path = state_file_path(datadir);
    if !path.exists() {
        info!("No existing relayer state at {}", path.display());
        return None;
    }

    match RelayerState::load(&path) {
        Ok(state) => {
            info!("Loaded existing relayer state with {} sources", state.sources.len());
            Some(state)
        }
        Err(e) => {
            warn!("Failed to load relayer state: {}", e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("relayer_state.json");

        let mut state = RelayerState::new();
        state.update("gravity://0/31337/events", 0, 31337, 42, 12340, 12345);

        state.save(&path).unwrap();
        assert!(path.exists());

        let loaded = RelayerState::load(&path).unwrap();
        assert_eq!(loaded.version, SCHEMA_VERSION);
        assert_eq!(loaded.sources.len(), 1);

        let source = loaded.get("gravity://0/31337/events").unwrap();
        assert_eq!(source.last_nonce, 42);
        assert_eq!(source.cursor_block, 12345);
        // Checksum must be present and valid after round-trip
        assert!(source.checksum.is_some(), "checksum should be set after update()");
        source.verify_checksum().expect("checksum should be valid after round-trip");
    }

    #[test]
    fn test_corrupted_state_fails_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("relayer_state.json");

        let mut state = RelayerState::new();
        state.update("gravity://0/31337/events", 0, 31337, 42, 12340, 12345);
        state.save(&path).unwrap();

        // Tamper with the file by changing last_nonce without updating the checksum.
        // to_string_pretty uses "last_nonce": 42 (space after colon).
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("42"), "state file must contain the nonce value");
        // Replace the nonce value (handles both compact and pretty JSON)
        let tampered = content
            .replace("\"last_nonce\": 42", "\"last_nonce\": 999")
            .replace("\"last_nonce\":42", "\"last_nonce\":999");
        assert!(tampered.contains("999"), "tamper must have taken effect");
        std::fs::write(&path, tampered).unwrap();

        // Load should fail due to checksum mismatch
        let result = RelayerState::load(&path);
        assert!(result.is_err(), "loading a tampered state file should fail");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("integrity check failed"), "error should mention integrity: {err}");
    }
}
