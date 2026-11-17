use clap::Args;
use reth_rpc_server_types::constants::cache::{
    DEFAULT_BAL_CACHE_MAX_LEN, DEFAULT_BLOCK_CACHE_MAX_BYTES, DEFAULT_BLOCK_CACHE_MAX_LEN,
    DEFAULT_CACHE_IDLE_TIMEOUT, DEFAULT_CONCURRENT_DB_REQUESTS, DEFAULT_HEADER_CACHE_MAX_LEN,
    DEFAULT_MAX_CACHED_TX_HASHES, DEFAULT_RECEIPT_CACHE_MAX_BYTES, DEFAULT_RECEIPT_CACHE_MAX_LEN,
};
use std::time::Duration;

/// Parameters to configure RPC state cache.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "RPC State Cache")]
pub struct RpcStateCacheArgs {
    /// Maximum estimated block payload bytes. Zero disables caching.
    #[arg(long = "rpc-cache.max-blocks-bytes", value_parser = parse_cache_bytes, default_value_t = DEFAULT_BLOCK_CACHE_MAX_BYTES)]
    pub max_blocks_bytes: usize,
    /// Maximum estimated receipt payload bytes. Zero disables caching.
    #[arg(long = "rpc-cache.max-receipts-bytes", value_parser = parse_cache_bytes, default_value_t = DEFAULT_RECEIPT_CACHE_MAX_BYTES)]
    pub max_receipts_bytes: usize,
    /// Evict unused payloads after this duration. Zero disables expiration.
    #[arg(long = "rpc-cache.idle-timeout", value_parser = humantime::parse_duration, default_value = "1h")]
    pub idle_timeout: Duration,
    /// Max number of blocks in cache.
    #[arg(
        long = "rpc-cache.max-blocks",
        default_value_t = DEFAULT_BLOCK_CACHE_MAX_LEN,
    )]
    pub max_blocks: u32,

    /// Max number receipts in cache.
    #[arg(
        long = "rpc-cache.max-receipts",
        default_value_t = DEFAULT_RECEIPT_CACHE_MAX_LEN,
    )]
    pub max_receipts: u32,

    /// Max number of headers in cache.
    #[arg(
        long = "rpc-cache.max-headers",
        alias = "rpc-cache.max-envs",
        default_value_t = DEFAULT_HEADER_CACHE_MAX_LEN,
    )]
    pub max_headers: u32,

    /// Max number of revm block access lists in cache.
    #[arg(
        long = "rpc-cache.max-bals",
        default_value_t = DEFAULT_BAL_CACHE_MAX_LEN,
    )]
    pub max_bals: u32,

    /// Max number of concurrent database requests.
    #[arg(
        long = "rpc-cache.max-concurrent-db-requests",
        default_value_t = DEFAULT_CONCURRENT_DB_REQUESTS,
    )]
    pub max_concurrent_db_requests: usize,

    /// Maximum number of transaction hashes to cache for transaction lookups.
    #[arg(
        long = "rpc-cache.max-cached-tx-hashes",
        default_value_t = DEFAULT_MAX_CACHED_TX_HASHES,
    )]
    pub max_cached_tx_hashes: u32,
}

impl RpcStateCacheArgs {
    /// Sets the Cache sizes to zero, effectively disabling caching.
    pub const fn set_zero_lengths(&mut self) {
        self.max_blocks_bytes = 0;
        self.max_receipts_bytes = 0;
        self.idle_timeout = Duration::ZERO;
        self.max_blocks = 0;
        self.max_receipts = 0;
        self.max_headers = 0;
        self.max_bals = 0;
    }
}

impl Default for RpcStateCacheArgs {
    fn default() -> Self {
        Self {
            max_blocks_bytes: DEFAULT_BLOCK_CACHE_MAX_BYTES,
            max_receipts_bytes: DEFAULT_RECEIPT_CACHE_MAX_BYTES,
            idle_timeout: DEFAULT_CACHE_IDLE_TIMEOUT,
            max_blocks: DEFAULT_BLOCK_CACHE_MAX_LEN,
            max_receipts: DEFAULT_RECEIPT_CACHE_MAX_LEN,
            max_headers: DEFAULT_HEADER_CACHE_MAX_LEN,
            max_bals: DEFAULT_BAL_CACHE_MAX_LEN,
            max_concurrent_db_requests: DEFAULT_CONCURRENT_DB_REQUESTS,
            max_cached_tx_hashes: DEFAULT_MAX_CACHED_TX_HASHES,
        }
    }
}

fn parse_cache_bytes(input: &str) -> Result<usize, String> {
    let input = input.trim();
    let split = input.find(|c: char| !c.is_ascii_digit()).unwrap_or(input.len());
    let value: usize = input[..split].parse().map_err(|_| "invalid byte count".to_owned())?;
    let multiplier = match input[split..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1usize,
        "KB" => 1024,
        "MB" => 1024usize.pow(2),
        "GB" => 1024usize.pow(3),
        "TB" => 1024usize.pow(4),
        _ => return Err("expected bytes or B, KB, MB, GB, TB".to_owned()),
    };
    value.checked_mul(multiplier).ok_or_else(|| "byte count overflow".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[derive(Parser)]
    struct Command {
        #[command(flatten)]
        args: RpcStateCacheArgs,
    }
    #[test]
    fn cache_cli_defaults_and_limits() {
        assert_eq!(Command::parse_from(["reth"]).args, RpcStateCacheArgs::default());
        let mut args = Command::parse_from([
            "reth",
            "--rpc-cache.max-blocks-bytes",
            "500 MB",
            "--rpc-cache.max-receipts-bytes",
            "0",
            "--rpc-cache.idle-timeout",
            "500ms",
        ])
        .args;
        assert_eq!(args.max_blocks_bytes, 500 * 1024 * 1024);
        assert_eq!(args.max_receipts_bytes, 0);
        assert_eq!(args.idle_timeout, Duration::from_millis(500));
        args.set_zero_lengths();
        assert_eq!(
            (args.max_blocks_bytes, args.max_receipts_bytes, args.idle_timeout),
            (0, 0, Duration::ZERO)
        );
    }
    #[test]
    fn cache_byte_parser_checks_units_and_overflow() {
        for value in ["-1", "1.5GB", "1XB", &format!("{}GB", usize::MAX)] {
            assert!(parse_cache_bytes(value).is_err());
        }
        assert_eq!(parse_cache_bytes(&usize::MAX.to_string()), Ok(usize::MAX));
    }
}
