//! Oracle URI Parser
//!
//! Parses extended gravity:// URIs that contain all oracle task configuration.
//!
//! ## URI Format
//!
//! ```text
//! gravity://<source_type>/<source_id>/<task_type>?<params>
//! ```
//!
//! ### Examples
//! - Blockchain events: `gravity://0/1/events?portal=0x283fC6...&fromBlock=9565280`

use alloy_primitives::Address;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use url::Url;

/// Parsed oracle task from URI
#[derive(Debug, Clone)]
pub struct ParsedOracleTask {
    /// Original URI string
    pub uri: String,

    /// Source type (0=BLOCKCHAIN)
    pub source_type: u32,

    /// Source identifier (chain ID, etc.)
    pub source_id: u64,

    /// Task type ("events", etc.)
    pub task_type: String,

    /// Query parameters
    pub params: HashMap<String, String>,
}

impl ParsedOracleTask {
    /// Get portal/contract address (for blockchain events)
    /// Accepts either 'contract' or 'portal' parameter name
    pub fn portal_address(&self) -> Result<Address> {
        let addr_str = self
            .params
            .get("contract")
            .or_else(|| self.params.get("portal"))
            .ok_or_else(|| anyhow!("Missing 'contract' or 'portal' parameter in URI"))?;

        addr_str.parse::<Address>().map_err(|e| anyhow!("Invalid contract address: {}", e))
    }

    /// Get fromBlock parameter
    pub fn from_block(&self) -> u64 {
        self.params.get("fromBlock").and_then(|s| s.parse().ok()).unwrap_or(0)
    }

    /// Check if this is a blockchain source
    pub fn is_blockchain(&self) -> bool {
        self.source_type == 0
    }
}

/// Parse a gravity:// URI into task configuration
///
/// # Examples
///
/// ```ignore
/// let task = parse_oracle_uri("gravity://0/1/events?portal=0x283fC6799867BF96bF862a05BDade3EE89132027&fromBlock=100")?;
/// assert_eq!(task.source_type, 0);
/// assert_eq!(task.source_id, 1);
/// assert_eq!(task.task_type, "events");
/// ```
pub fn parse_oracle_uri(uri: &str) -> Result<ParsedOracleTask> {
    // Validate scheme
    if !uri.starts_with("gravity://") {
        return Err(anyhow!("URI must start with 'gravity://'"));
    }

    // Extract the part after gravity://
    let rest = &uri[10..]; // len("gravity://") = 10

    // Find the first '/' to separate source_type from the rest
    let first_slash =
        rest.find('/').ok_or_else(|| anyhow!("URI must have path after source_type"))?;

    // Parse source_type (the part before first /)
    let source_type: u32 =
        rest[..first_slash].parse().map_err(|e| anyhow!("Invalid source_type: {}", e))?;

    // Now parse the rest using URL crate with a dummy host
    // Format the URL as http://dummy/<rest_of_path>
    let rest_after_source_type = &rest[first_slash..]; // includes leading /
    let url_str = format!("http://dummy{}", rest_after_source_type);
    let url = Url::parse(&url_str).map_err(|e| anyhow!("Failed to parse URI: {}", e))?;

    // Parse path segments: /<source_id>/<task_type>
    let path_segments: Vec<&str> = url.path_segments().map(|s| s.collect()).unwrap_or_default();

    if path_segments.is_empty() {
        return Err(anyhow!("URI must have at least source_id in path"));
    }

    // Parse source_id (first path segment)
    let source_id: u64 =
        path_segments[0].parse().map_err(|e| anyhow!("Invalid source_id: {}", e))?;

    // Parse task_type (second path segment, default to "events")
    let task_type = path_segments.get(1).unwrap_or(&"events").to_string();

    // Parse query parameters
    let params: HashMap<String, String> = url.query_pairs().into_owned().collect();

    Ok(ParsedOracleTask { uri: uri.to_string(), source_type, source_id, task_type, params })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_blockchain_uri() {
        let uri = "gravity://0/1/events?portal=0x283fC6799867BF96bF862a05BDade3EE89132027&fromBlock=9565280";
        let task = parse_oracle_uri(uri).unwrap();

        assert_eq!(task.source_type, 0);
        assert_eq!(task.source_id, 1);
        assert_eq!(task.task_type, "events");
        assert_eq!(task.from_block(), 9565280);
        assert!(task.portal_address().is_ok());
    }

    #[test]
    fn test_invalid_scheme() {
        let uri = "http://0/1/events";
        assert!(parse_oracle_uri(uri).is_err());
    }
}
