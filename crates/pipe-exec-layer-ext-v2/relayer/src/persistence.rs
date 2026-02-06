//! Relayer State Persistence
//!
//! Persists nonce and block cursor state to disk for fast restart.

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
        self.sources.insert(
            uri.to_string(),
            SourceState {
                source_type,
                source_id,
                last_nonce,
                last_nonce_block,
                cursor_block,
                updated_at: now,
            },
        );
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
    }
}
