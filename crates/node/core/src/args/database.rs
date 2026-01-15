//! clap [Args](clap::Args) for database configuration

use std::{fmt, str::FromStr};

use crate::version::default_client_version;
use clap::{
    builder::{PossibleValue, TypedValueParser},
    error::ErrorKind,
    Arg, Args, Command, Error,
};
use reth_db::{ClientVersion, DatabaseArguments, ShardingDirectories};
use reth_storage_errors::db::LogLevel;

/// Parameters for database configuration (RocksDB)
#[derive(Debug, Args, PartialEq, Eq, Default, Clone, Copy)]
#[command(next_help_heading = "Database")]
pub struct DatabaseArgs {
    /// Database logging level. Levels higher than "notice" require a debug build.
    #[arg(long = "db.log-level", value_parser = LogLevelValueParser::default())]
    pub log_level: Option<LogLevel>,
    /// Block cache size in bytes (e.g., 8GB, 4096MB).
    /// This is the LRU cache for uncompressed blocks. Higher values improve read performance.
    /// Default: 8GB
    #[arg(long = "db.block-cache-size", value_parser = parse_byte_size)]
    pub block_cache_size: Option<usize>,
    /// Write buffer size in bytes (e.g., 256MB, 512MB).
    /// Larger values improve write performance but use more memory.
    /// Default: 256MB
    #[arg(long = "db.write-buffer-size", value_parser = parse_byte_size)]
    pub write_buffer_size: Option<usize>,
    /// Maximum number of background jobs for compaction and flush.
    /// Higher values increase parallelism but use more CPU.
    /// Default: 14
    #[arg(long = "db.max-background-jobs")]
    pub max_background_jobs: Option<i32>,
    /// Maximum number of open files.
    /// Lower values if you hit OS file descriptor limits.
    /// Default: 10000
    #[arg(long = "db.max-open-files")]
    pub max_open_files: Option<i32>,
    /// Maximum number of write buffers (memtables).
    /// More buffers allow absorbing write spikes but use more memory.
    /// Default: 6
    #[arg(long = "db.max-write-buffer-number")]
    pub max_write_buffer_number: Option<i32>,
    /// Compaction readahead size in bytes (e.g., 4MB, 8MB).
    /// Larger values improve compaction performance with sequential I/O.
    /// Default: 4MB
    #[arg(long = "db.compaction-readahead-size", value_parser = parse_byte_size)]
    pub compaction_readahead_size: Option<usize>,
    /// Number of L0 files to trigger compaction.
    /// Lower values reduce read amplification but increase write amplification.
    /// Default: 4
    #[arg(long = "db.level0-file-num-compaction-trigger")]
    pub level0_file_num_compaction_trigger: Option<i32>,
    /// Maximum bytes for level 1 (e.g., 512MB, 1GB).
    /// This controls the target size for L1, affecting LSM tree structure.
    /// Default: 512MB
    #[arg(long = "db.max-bytes-for-level-base", value_parser = parse_byte_size_u64)]
    pub max_bytes_for_level_base: Option<u64>,
    /// Bytes to write before background sync (e.g., 4MB, 8MB).
    /// Larger values reduce I/O overhead but may affect durability.
    /// Default: 4MB
    #[arg(long = "db.bytes-per-sync", value_parser = parse_byte_size_u64)]
    pub bytes_per_sync: Option<u64>,
    /// Semicolon separated RocksDB sharding directories. Accepts 2 or 3 paths.
    /// Always creates 3 databases (state, account_trie, storage_trie).
    /// - 0 paths (default): Creates subdirs under --datadir: state/, account_trie/, storage_trie/
    /// - 2 paths: First dir contains state + account_trie; second dir contains storage_trie
    /// - 3 paths: First dir is state, second is account_trie, third is storage_trie
    #[arg(long = "db.sharding-directories", value_parser = parse_sharding_directories)]
    pub sharding_directories: Option<ShardingDirectories>,
}

impl DatabaseArgs {
    /// Returns default database arguments with configured log level and client version.
    pub fn database_args(&self) -> DatabaseArguments {
        self.get_database_args(default_client_version())
    }

    /// Returns the database arguments with configured parameters and client version.
    pub fn get_database_args(&self, client_version: ClientVersion) -> DatabaseArguments {
        DatabaseArguments::new(client_version)
            .with_log_level(self.log_level)
            .with_block_cache_size(self.block_cache_size)
            .with_write_buffer_size(self.write_buffer_size)
            .with_max_background_jobs(self.max_background_jobs)
            .with_max_open_files(self.max_open_files)
            .with_max_write_buffer_number(self.max_write_buffer_number)
            .with_compaction_readahead_size(self.compaction_readahead_size)
            .with_level0_file_num_compaction_trigger(self.level0_file_num_compaction_trigger)
            .with_max_bytes_for_level_base(self.max_bytes_for_level_base)
            .with_bytes_per_sync(self.bytes_per_sync)
            .with_sharding_directories(self.sharding_directories)
    }
}

/// clap value parser for [`LogLevel`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
struct LogLevelValueParser;

impl TypedValueParser for LogLevelValueParser {
    type Value = LogLevel;

    fn parse_ref(
        &self,
        _cmd: &Command,
        arg: Option<&Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, Error> {
        let val =
            value.to_str().ok_or_else(|| Error::raw(ErrorKind::InvalidUtf8, "Invalid UTF-8"))?;

        val.parse::<LogLevel>().map_err(|err| {
            let arg = arg.map(|a| a.to_string()).unwrap_or_else(|| "...".to_owned());
            let possible_values = LogLevel::value_variants()
                .iter()
                .map(|v| format!("- {:?}: {}", v, v.help_message()))
                .collect::<Vec<_>>()
                .join("\n");
            let msg = format!(
                "Invalid value '{val}' for {arg}: {err}.\n    Possible values:\n{possible_values}"
            );
            clap::Error::raw(clap::error::ErrorKind::InvalidValue, msg)
        })
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        let values = LogLevel::value_variants()
            .iter()
            .map(|v| PossibleValue::new(v.variant_name()).help(v.help_message()));
        Some(Box::new(values))
    }
}

/// Size in bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub usize);

impl From<ByteSize> for usize {
    fn from(s: ByteSize) -> Self {
        s.0
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_uppercase();
        let parts: Vec<&str> = s.split_whitespace().collect();

        let (num_str, unit) = match parts.len() {
            1 => {
                let (num, unit) =
                    s.split_at(s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len()));
                (num, unit)
            }
            2 => (parts[0], parts[1]),
            _ => {
                return Err("Invalid format. Use '<number><unit>' or '<number> <unit>'.".to_string())
            }
        };

        let num: usize = num_str.parse().map_err(|_| "Invalid number".to_string())?;

        let multiplier = match unit {
            "B" | "" => 1, // Assume bytes if no unit is specified
            "KB" => 1024,
            "MB" => 1024 * 1024,
            "GB" => 1024 * 1024 * 1024,
            "TB" => 1024 * 1024 * 1024 * 1024,
            _ => return Err(format!("Invalid unit: {unit}. Use B, KB, MB, GB, or TB.")),
        };

        Ok(Self(num * multiplier))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const KB: usize = 1024;
        const MB: usize = KB * 1024;
        const GB: usize = MB * 1024;
        const TB: usize = GB * 1024;

        let (size, unit) = if self.0 >= TB {
            (self.0 as f64 / TB as f64, "TB")
        } else if self.0 >= GB {
            (self.0 as f64 / GB as f64, "GB")
        } else if self.0 >= MB {
            (self.0 as f64 / MB as f64, "MB")
        } else if self.0 >= KB {
            (self.0 as f64 / KB as f64, "KB")
        } else {
            (self.0 as f64, "B")
        };

        write!(f, "{size:.2}{unit}")
    }
}

/// Value parser function that supports various formats.
fn parse_byte_size(s: &str) -> Result<usize, String> {
    s.parse::<ByteSize>().map(Into::into)
}

/// Value parser function for u64 byte sizes.
fn parse_byte_size_u64(s: &str) -> Result<u64, String> {
    s.parse::<ByteSize>().map(|b| b.0 as u64)
}

/// Value parser for sharding directories (semicolon separated).
fn parse_sharding_directories(raw: &str) -> Result<ShardingDirectories, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Sharding directories cannot be empty".to_string());
    }
    // Leak once to obtain a static str; acceptable because values are process-scoped config.
    Ok(Box::leak(trimmed.to_owned().into_boxed_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // Constants for byte sizes
    const KILOBYTE: usize = 1024;
    const MEGABYTE: usize = KILOBYTE * 1024;
    const GIGABYTE: usize = MEGABYTE * 1024;
    const TERABYTE: usize = GIGABYTE * 1024;

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn test_default_database_args() {
        let default_args = DatabaseArgs::default();
        let args = CommandParser::<DatabaseArgs>::parse_from(["reth"]).args;
        assert_eq!(args, default_args);
    }

    #[test]
    fn test_command_parser_with_valid_block_cache_size() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "4294967296",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(GIGABYTE * 4));
    }

    #[test]
    fn test_command_parser_with_invalid_block_cache_size() {
        let result = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "invalid",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_parser_with_valid_write_buffer_size() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.write-buffer-size",
            "536870912",
        ])
        .unwrap();
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 512));
    }

    #[test]
    fn test_command_parser_with_invalid_write_buffer_size() {
        let result = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.write-buffer-size",
            "invalid",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_parser_with_rocksdb_params_from_str() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "16GB",
            "--db.write-buffer-size",
            "512MB",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(GIGABYTE * 16));
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 512));

        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "4096MB",
            "--db.write-buffer-size",
            "256MB",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(MEGABYTE * 4096));
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 256));

        // with spaces
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "8 GB",
            "--db.write-buffer-size",
            "128 MB",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(GIGABYTE * 8));
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 128));

        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "8589934592",
            "--db.write-buffer-size",
            "268435456",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(GIGABYTE * 8));
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 256));
    }

    #[test]
    fn test_command_parser_with_background_jobs_and_open_files() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.max-background-jobs",
            "16",
            "--db.max-open-files",
            "1000",
        ])
        .unwrap();
        assert_eq!(cmd.args.max_background_jobs, Some(16));
        assert_eq!(cmd.args.max_open_files, Some(1000));

        // Test unlimited open files (use = to avoid -1 being parsed as a flag)
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from(["reth", "--db.max-open-files=-1"])
            .unwrap();
        assert_eq!(cmd.args.max_open_files, Some(-1));
    }

    #[test]
    fn test_command_parser_byte_size_invalid_unit() {
        let result = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.write-buffer-size",
            "1 PB",
        ]);
        assert!(result.is_err());

        let result =
            CommandParser::<DatabaseArgs>::try_parse_from(["reth", "--db.block-cache-size", "2PB"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_possible_values() {
        // Initialize the LogLevelValueParser
        let parser = LogLevelValueParser;

        // Call the possible_values method
        let possible_values: Vec<PossibleValue> = parser.possible_values().unwrap().collect();

        // Expected possible values
        let expected_values = vec![
            PossibleValue::new("fatal")
                .help("Enables logging for critical conditions, i.e. assertion failures"),
            PossibleValue::new("error").help("Enables logging for error conditions"),
            PossibleValue::new("warn").help("Enables logging for warning conditions"),
            PossibleValue::new("notice")
                .help("Enables logging for normal but significant condition"),
            PossibleValue::new("verbose").help("Enables logging for verbose informational"),
            PossibleValue::new("debug").help("Enables logging for debug-level messages"),
            PossibleValue::new("trace").help("Enables logging for trace debug-level messages"),
            PossibleValue::new("extra").help("Enables logging for extra debug-level messages"),
        ];

        // Check that the possible values match the expected values
        assert_eq!(possible_values.len(), expected_values.len());
        for (actual, expected) in possible_values.iter().zip(expected_values.iter()) {
            assert_eq!(actual.get_name(), expected.get_name());
            assert_eq!(actual.get_help(), expected.get_help());
        }
    }

    #[test]
    fn test_command_parser_with_valid_log_level() {
        let cmd =
            CommandParser::<DatabaseArgs>::try_parse_from(["reth", "--db.log-level", "Debug"])
                .unwrap();
        assert_eq!(cmd.args.log_level, Some(LogLevel::Debug));
    }

    #[test]
    fn test_command_parser_with_invalid_log_level() {
        let result =
            CommandParser::<DatabaseArgs>::try_parse_from(["reth", "--db.log-level", "invalid"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_parser_without_log_level() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from(["reth"]).unwrap();
        assert_eq!(cmd.args.log_level, None);
    }

    #[test]
    fn test_command_parser_with_write_buffer_number() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.max-write-buffer-number",
            "8",
        ])
        .unwrap();
        assert_eq!(cmd.args.max_write_buffer_number, Some(8));
    }

    #[test]
    fn test_command_parser_with_compaction_readahead() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.compaction-readahead-size",
            "8MB",
        ])
        .unwrap();
        assert_eq!(cmd.args.compaction_readahead_size, Some(MEGABYTE * 8));
    }

    #[test]
    fn test_command_parser_with_level0_trigger() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.level0-file-num-compaction-trigger",
            "6",
        ])
        .unwrap();
        assert_eq!(cmd.args.level0_file_num_compaction_trigger, Some(6));
    }

    #[test]
    fn test_command_parser_with_max_bytes_level_base() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.max-bytes-for-level-base",
            "1GB",
        ])
        .unwrap();
        assert_eq!(cmd.args.max_bytes_for_level_base, Some((GIGABYTE * 1) as u64));
    }

    #[test]
    fn test_command_parser_with_bytes_per_sync() {
        let cmd =
            CommandParser::<DatabaseArgs>::try_parse_from(["reth", "--db.bytes-per-sync", "8MB"])
                .unwrap();
        assert_eq!(cmd.args.bytes_per_sync, Some((MEGABYTE * 8) as u64));
    }

    #[test]
    fn test_command_parser_with_all_advanced_options() {
        let cmd = CommandParser::<DatabaseArgs>::try_parse_from([
            "reth",
            "--db.block-cache-size",
            "16GB",
            "--db.write-buffer-size",
            "512MB",
            "--db.max-background-jobs",
            "16",
            "--db.max-open-files=-1",
            "--db.max-write-buffer-number",
            "8",
            "--db.compaction-readahead-size",
            "8MB",
            "--db.level0-file-num-compaction-trigger",
            "6",
            "--db.max-bytes-for-level-base",
            "1GB",
            "--db.bytes-per-sync",
            "8MB",
        ])
        .unwrap();
        assert_eq!(cmd.args.block_cache_size, Some(GIGABYTE * 16));
        assert_eq!(cmd.args.write_buffer_size, Some(MEGABYTE * 512));
        assert_eq!(cmd.args.max_background_jobs, Some(16));
        assert_eq!(cmd.args.max_open_files, Some(-1));
        assert_eq!(cmd.args.max_write_buffer_number, Some(8));
        assert_eq!(cmd.args.compaction_readahead_size, Some(MEGABYTE * 8));
        assert_eq!(cmd.args.level0_file_num_compaction_trigger, Some(6));
        assert_eq!(cmd.args.max_bytes_for_level_base, Some((GIGABYTE * 1) as u64));
        assert_eq!(cmd.args.bytes_per_sync, Some((MEGABYTE * 8) as u64));
    }
}
