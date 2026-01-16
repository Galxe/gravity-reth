//! RocksDB implementation for the database.

use crate::{DatabaseError, TableSet};
use metrics::Label;
use reth_db_api::{
    database_metrics::DatabaseMetrics, models::ClientVersion, table::Table, tables, Tables,
};
use reth_storage_errors::db::{DatabaseErrorInfo, LogLevel};
use reth_tracing::tracing::info;
use rocksdb::{BlockBasedOptions, Cache, Options, DB};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) mod cursor;
pub(crate) mod tx;

/// Database environment kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEnvKind {
    /// Read-write database.
    RW,
    /// Read-only database.
    RO,
}

impl DatabaseEnvKind {
    /// Returns `true` if the database is read-write.
    pub const fn is_rw(self) -> bool {
        matches!(self, Self::RW)
    }
}

/// Copyable sharding directories payload.
///
/// Kept as a `'static` str; callers are responsible for providing a leaked string when parsing
/// CLI/config input.
pub type ShardingDirectories = &'static str;

/// Database arguments for RocksDB.
#[derive(Debug, Clone)]
pub struct DatabaseArguments {
    /// Client version - used for version tracking.
    pub client_version: ClientVersion,
    /// Log level - currently not used in RocksDB implementation.
    pub log_level: Option<LogLevel>,
    /// Block cache size in bytes (default: 8GB).
    /// This is the LRU cache for uncompressed blocks, critical for read performance.
    pub block_cache_size: Option<usize>,
    /// Write buffer size in bytes (default: 256MB).
    /// Larger values improve write performance but use more memory.
    pub write_buffer_size: Option<usize>,
    /// Maximum number of background jobs (default: 14).
    /// Controls parallelism for compaction and flush operations.
    pub max_background_jobs: Option<i32>,
    /// Maximum number of open files (default: -1, unlimited).
    /// Set to a lower value if you hit OS file descriptor limits.
    pub max_open_files: Option<i32>,
    /// Maximum number of write buffers (default: 6).
    /// More buffers allow absorbing write spikes but use more memory.
    pub max_write_buffer_number: Option<i32>,
    /// Compaction readahead size in bytes (default: 4MB).
    /// Larger values improve compaction performance with sequential I/O.
    pub compaction_readahead_size: Option<usize>,
    /// Number of L0 files to trigger compaction (default: 4).
    /// Lower values reduce read amplification but increase write amplification.
    pub level0_file_num_compaction_trigger: Option<i32>,
    /// Maximum bytes for level 1 (default: 512MB).
    /// This is the target size for L1, affects LSM tree structure.
    pub max_bytes_for_level_base: Option<u64>,
    /// Bytes to write before background sync (default: 4MB).
    /// Larger values reduce I/O overhead but may affect durability guarantees.
    pub bytes_per_sync: Option<u64>,
    /// Semicolon separated RocksDB sharding directories.
    /// If set, must contain 2 or 3 paths. See `DatabaseEnv::open` for routing rules.
    pub sharding_directories: Option<ShardingDirectories>,
}

impl DatabaseArguments {
    /// Default block cache size: 4GB
    pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 4 * 1024 * 1024 * 1024;
    /// Default write buffer size: 128MB
    pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 128 * 1024 * 1024;
    /// Default max background jobs: 14
    pub const DEFAULT_MAX_BACKGROUND_JOBS: i32 = 14;
    /// Default max open files: 10000
    pub const DEFAULT_MAX_OPEN_FILES: i32 = 10000;
    /// Default max write buffer number: 6
    pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 6;
    /// Default compaction readahead size: 4MB
    pub const DEFAULT_COMPACTION_READAHEAD_SIZE: usize = 4 * 1024 * 1024;
    /// Default L0 file num compaction trigger: 4
    pub const DEFAULT_LEVEL0_FILE_NUM_COMPACTION_TRIGGER: i32 = 4;
    /// Default max bytes for level base: 512MB
    pub const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * 1024 * 1024;
    /// Default bytes per sync: 4MB
    pub const DEFAULT_BYTES_PER_SYNC: u64 = 4 * 1024 * 1024;

    /// Creates a new instance of [`DatabaseArguments`].
    pub fn new(client_version: ClientVersion) -> Self {
        Self {
            client_version,
            log_level: None,
            block_cache_size: None,
            write_buffer_size: None,
            max_background_jobs: None,
            max_open_files: None,
            max_write_buffer_number: None,
            compaction_readahead_size: None,
            level0_file_num_compaction_trigger: None,
            max_bytes_for_level_base: None,
            bytes_per_sync: None,
            sharding_directories: None,
        }
    }

    /// Set the log level.
    pub fn with_log_level(mut self, log_level: Option<LogLevel>) -> Self {
        self.log_level = log_level;
        self
    }

    /// Set the block cache size in bytes.
    pub fn with_block_cache_size(mut self, size: Option<usize>) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Set the write buffer size in bytes.
    pub fn with_write_buffer_size(mut self, size: Option<usize>) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Set the maximum number of background jobs.
    pub fn with_max_background_jobs(mut self, jobs: Option<i32>) -> Self {
        self.max_background_jobs = jobs;
        self
    }

    /// Set the maximum number of open files.
    pub fn with_max_open_files(mut self, files: Option<i32>) -> Self {
        self.max_open_files = files;
        self
    }

    /// Set the maximum number of write buffers.
    pub fn with_max_write_buffer_number(mut self, num: Option<i32>) -> Self {
        self.max_write_buffer_number = num;
        self
    }

    /// Set the compaction readahead size in bytes.
    pub fn with_compaction_readahead_size(mut self, size: Option<usize>) -> Self {
        self.compaction_readahead_size = size;
        self
    }

    /// Set the L0 file num compaction trigger.
    pub fn with_level0_file_num_compaction_trigger(mut self, num: Option<i32>) -> Self {
        self.level0_file_num_compaction_trigger = num;
        self
    }

    /// Set the maximum bytes for level base.
    pub fn with_max_bytes_for_level_base(mut self, bytes: Option<u64>) -> Self {
        self.max_bytes_for_level_base = bytes;
        self
    }

    /// Set the bytes per sync.
    pub fn with_bytes_per_sync(mut self, bytes: Option<u64>) -> Self {
        self.bytes_per_sync = bytes;
        self
    }

    /// Set sharding directories for RocksDB (semicolon separated paths).
    pub fn with_sharding_directories(mut self, dirs: Option<ShardingDirectories>) -> Self {
        self.sharding_directories = dirs;
        self
    }

    /// Get the client version.
    pub fn client_version(&self) -> &ClientVersion {
        &self.client_version
    }

    /// Get the block cache size, using default if not set.
    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size.unwrap_or(Self::DEFAULT_BLOCK_CACHE_SIZE)
    }

    /// Get the write buffer size, using default if not set.
    pub fn write_buffer_size(&self) -> usize {
        self.write_buffer_size.unwrap_or(Self::DEFAULT_WRITE_BUFFER_SIZE)
    }

    /// Get the max background jobs, using default if not set.
    pub fn max_background_jobs(&self) -> i32 {
        self.max_background_jobs.unwrap_or(Self::DEFAULT_MAX_BACKGROUND_JOBS)
    }

    /// Get the max open files, using default if not set.
    pub fn max_open_files(&self) -> i32 {
        self.max_open_files.unwrap_or(Self::DEFAULT_MAX_OPEN_FILES)
    }

    /// Get the max write buffer number, using default if not set.
    pub fn max_write_buffer_number(&self) -> i32 {
        self.max_write_buffer_number.unwrap_or(Self::DEFAULT_MAX_WRITE_BUFFER_NUMBER)
    }

    /// Get the compaction readahead size, using default if not set.
    pub fn compaction_readahead_size(&self) -> usize {
        self.compaction_readahead_size.unwrap_or(Self::DEFAULT_COMPACTION_READAHEAD_SIZE)
    }

    /// Get the L0 file num compaction trigger, using default if not set.
    pub fn level0_file_num_compaction_trigger(&self) -> i32 {
        self.level0_file_num_compaction_trigger
            .unwrap_or(Self::DEFAULT_LEVEL0_FILE_NUM_COMPACTION_TRIGGER)
    }

    /// Get the max bytes for level base, using default if not set.
    pub fn max_bytes_for_level_base(&self) -> u64 {
        self.max_bytes_for_level_base.unwrap_or(Self::DEFAULT_MAX_BYTES_FOR_LEVEL_BASE)
    }

    /// Get the bytes per sync, using default if not set.
    pub fn bytes_per_sync(&self) -> u64 {
        self.bytes_per_sync.unwrap_or(Self::DEFAULT_BYTES_PER_SYNC)
    }

    /// Get sharding directories if configured.
    pub fn sharding_directories(&self) -> Option<&str> {
        self.sharding_directories
    }
}

impl Default for DatabaseArguments {
    fn default() -> Self {
        Self::new(ClientVersion::default())
    }
}

/// RocksDB database environment.
#[derive(Debug)]
pub struct DatabaseEnv {
    /// RocksDB database for state and history tables.
    pub(crate) state_db: Arc<DB>,
    /// RocksDB database for account trie tables.
    pub(crate) account_db: Arc<DB>,
    /// RocksDB database for storage trie tables.
    pub(crate) storage_db: Arc<DB>,
    /// Database environment kind (read-only or read-write).
    kind: DatabaseEnvKind,
}

impl DatabaseEnv {
    /// Opens the database at the specified path.
    pub fn open(
        path: &Path,
        kind: DatabaseEnvKind,
        args: DatabaseArguments,
    ) -> Result<Self, DatabaseError> {
        // Determine sharding layout (always 3 databases)
        let (state_path, account_path, storage_path) =
            Self::resolve_shard_paths(path, args.sharding_directories())?;
        info!(target: "database::open", state_path = ?state_path, account_path = ?account_path, storage_path = ?storage_path , "Generate RocksDB instance path");

        // Configure RocksDB options (shared across the 3-db architecture)
        let opts = Self::create_db_options(&args);

        // Assign tables to their target databases
        let tables_by_path =
            Self::assign_tables_to_shards(&state_path, &account_path, &storage_path);
        info!(target: "database::open", tables_by_path = ?tables_by_path, "Assign tables into RocksDB instance");

        // Open one RocksDB per unique path
        let dbs = Self::open_rocksdb_instances(&opts, tables_by_path)?;

        let state_db = dbs.get(&state_path).cloned().expect("state DB handle missing");
        let account_db = dbs.get(&account_path).cloned().expect("account DB handle missing");
        let storage_db = dbs.get(&storage_path).cloned().expect("storage DB handle missing");

        Ok(Self { state_db, account_db, storage_db, kind })
    }

    /// Resolve shard paths based on configuration.
    /// Always returns 3 distinct (or aliased) paths for state, account_trie, storage_trie.
    fn resolve_shard_paths(
        base_path: &Path,
        sharding_config: Option<&str>,
    ) -> Result<(PathBuf, PathBuf, PathBuf), DatabaseError> {
        match sharding_config {
            Some(raw) => {
                let parts: Vec<_> =
                    raw.split(';').map(str::trim).filter(|p| !p.is_empty()).collect();
                match parts.len() {
                    2 => {
                        // First dir: state + account; Second dir: storage
                        let dir1 = PathBuf::from(parts[0]);
                        let dir2 = PathBuf::from(parts[1]);
                        Ok((
                            dir1.join("state"),
                            dir1.join("account_trie"),
                            dir2.join("storage_trie"),
                        ))
                    }
                    3 => {
                        // First: state; Second: account; Third: storage
                        let dir1 = PathBuf::from(parts[0]);
                        let dir2 = PathBuf::from(parts[1]);
                        let dir3 = PathBuf::from(parts[2]);
                        Ok((dir1.clone(), dir2.clone(), dir3.clone()))
                    }
                    other => Err(DatabaseError::Other(format!(
                        "db.sharding-directories expects 2 or 3 ';' separated paths, got {}",
                        other
                    ))),
                }
            }
            None => {
                // Default: create 3 subdirectories under base path
                Ok((
                    base_path.join("state"),
                    base_path.join("account_trie"),
                    base_path.join("storage_trie"),
                ))
            }
        }
    }

    /// Create optimized RocksDB options for 3-db architecture.
    fn create_db_options(args: &DatabaseArguments) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // === Parallelism Configuration ===
        opts.set_max_background_jobs(args.max_background_jobs());
        let parallelism = (args.max_background_jobs() + 2).max(4) as i32;
        opts.increase_parallelism(parallelism);
        opts.set_max_open_files(args.max_open_files());

        // === Memory Configuration ===
        let write_buffer_size = args.write_buffer_size();
        opts.set_write_buffer_size(write_buffer_size);
        opts.set_max_write_buffer_number(args.max_write_buffer_number());
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_db_write_buffer_size(3 * 1024 * 1024 * 1024);

        // === Block Cache Configuration (per DB) ===
        let block_cache_size = args.block_cache_size();
        let cache = Cache::new_lru_cache(block_cache_size);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_block_size(32 * 1024);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);

        // === Compaction Configuration ===
        opts.set_level_compaction_dynamic_level_bytes(true);
        let l0_trigger = args.level0_file_num_compaction_trigger();
        opts.set_level_zero_file_num_compaction_trigger(l0_trigger);
        opts.set_level_zero_slowdown_writes_trigger(l0_trigger * 7);
        opts.set_level_zero_stop_writes_trigger(l0_trigger * 12);
        opts.set_soft_pending_compaction_bytes_limit(64 * 1024 * 1024 * 1024); // 64GB
        opts.set_hard_pending_compaction_bytes_limit(256 * 1024 * 1024 * 1024); // 256GB
        opts.set_target_file_size_base(256 * 1024 * 1024);
        opts.set_target_file_size_multiplier(2);
        opts.set_max_bytes_for_level_base(args.max_bytes_for_level_base());
        opts.set_max_bytes_for_level_multiplier(10.0);
        opts.set_max_compaction_bytes(2 * 1024 * 1024 * 1024);

        // === Write Configuration ===
        opts.set_enable_pipelined_write(true);
        opts.set_max_total_wal_size(1 * 1024 * 1024 * 1024); // 1GB max WAL per DB
        opts.set_wal_bytes_per_sync(4 * 1024 * 1024);

        // === Compression Configuration ===
        opts.set_compression_per_level(&[
            rocksdb::DBCompressionType::Lz4,
            rocksdb::DBCompressionType::Lz4,
            rocksdb::DBCompressionType::Zstd,
            rocksdb::DBCompressionType::Zstd,
            rocksdb::DBCompressionType::Zstd,
            rocksdb::DBCompressionType::Zstd,
            rocksdb::DBCompressionType::Zstd,
        ]);

        // === I/O Optimization ===
        opts.set_bytes_per_sync(args.bytes_per_sync());
        opts.set_compaction_readahead_size(args.compaction_readahead_size());

        opts
    }

    /// Assign tables to their target shard databases.
    fn assign_tables_to_shards(
        state_path: &Path,
        account_trie_path: &Path,
        storage_trie_path: &Path,
    ) -> HashMap<PathBuf, Vec<String>> {
        let mut tables_by_path: HashMap<PathBuf, Vec<String>> = HashMap::new();
        for table in Tables::tables() {
            let name = table.name().to_string();
            let target = match name.as_str() {
                tables::AccountsTrieV2::NAME => account_trie_path,
                tables::StoragesTrieV2::NAME => storage_trie_path,
                _ => state_path,
            };
            tables_by_path.entry(target.to_path_buf()).or_default().push(name);
        }
        tables_by_path
    }

    /// Open RocksDB instances for all unique shard paths.
    fn open_rocksdb_instances(
        opts: &Options,
        tables_by_path: HashMap<PathBuf, Vec<String>>,
    ) -> Result<HashMap<PathBuf, Arc<DB>>, DatabaseError> {
        let mut dbs: HashMap<PathBuf, Arc<DB>> = HashMap::new();
        for (db_path, cf_names) in tables_by_path.iter() {
            let db = DB::open_cf(opts, db_path, cf_names).map_err(|e| {
                DatabaseError::Other(format!(
                    "Failed to open RocksDB at {}: {}",
                    db_path.display(),
                    e
                ))
            })?;
            info!("Success open RocksDb at {:?}", db_path);
            dbs.insert(db_path.clone(), Arc::new(db));
        }
        Ok(dbs)
    }

    /// Returns `true` if the database is read-only.
    pub fn is_read_only(&self) -> bool {
        matches!(self.kind, DatabaseEnvKind::RO)
    }

    /// Creates tables for the given table set.
    pub fn create_tables(&self) -> Result<(), DatabaseError> {
        self.create_tables_for::<Tables>()
    }

    /// Creates tables for the given table set.
    pub fn create_tables_for<TS: TableSet>(&self) -> Result<(), DatabaseError> {
        Ok(())
    }

    /// Records the client version in the database.
    pub fn record_client_version(&self, version: ClientVersion) -> Result<(), DatabaseError> {
        use reth_db_api::{
            cursor::{DbCursorRO, DbCursorRW},
            database::Database,
            tables,
            transaction::{DbTx, DbTxMut},
        };
        use std::time::{SystemTime, UNIX_EPOCH};

        if version.is_empty() {
            return Ok(());
        }

        let tx = self.tx_mut()?;
        let mut version_cursor = tx.cursor_write::<tables::VersionHistory>()?;

        let last_version = version_cursor.last()?.map(|(_, v)| v);
        if Some(&version) != last_version.as_ref() {
            version_cursor.upsert(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
                &version,
            )?;
            tx.commit()?;
        }

        Ok(())
    }
}

impl DatabaseMetrics for DatabaseEnv {
    fn histogram_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        let mut metrics = Vec::new();

        // 1. rocksdb.actual-delayed-write-rate
        // Current write rate limit in bytes/sec when write stall occurs
        // Threshold: 0 = healthy, >0 = write stall active
        // Action: If sustained >0, writes are being throttled
        // - Check num_files_at_level0 and estimate_pending_compaction_bytes
        // - Increase max_background_jobs (currently 14)
        // - Increase delayed_write_rate limit (currently 64MB/s)
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.actual-delayed-write-rate") {
            if let Ok(rate) = value.parse::<u64>() {
                metrics.push(("rocksdb.actual_delayed_write_rate", rate as f64, vec![]));
            }
        }

        // 2. rocksdb.is-write-stopped
        // Whether writes are completely stopped (0=no, 1=yes)
        // Threshold: 0 = healthy, 1 = CRITICAL
        // Action: If 1, writes are blocked waiting for flush/compaction
        // - Immediate: Check if L0 files >= 50 (stop trigger)
        // - Or pending_compaction_bytes >= 512GB (hard limit)
        // - Increase level_zero_stop_writes_trigger above 50
        // - Increase hard_pending_compaction_bytes_limit above 512GB
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.is-write-stopped") {
            if let Ok(stopped) = value.parse::<u64>() {
                metrics.push(("rocksdb.is_write_stopped", stopped as f64, vec![]));
            }
        }

        // 3. rocksdb.num-immutable-mem-table
        // Number of immutable memtables not yet flushed to disk
        // Threshold: 0-2 = healthy, 3-4 = warning, >=5 = critical (approaching
        // max_write_buffer_number=6) Action: If >=4, flush is falling behind
        // - Increase max_background_jobs for more flush threads
        // - Reduce write_buffer_size (currently 256MB) to flush more frequently
        // - Increase max_write_buffer_number above 6 for more buffer
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.num-immutable-mem-table") {
            if let Ok(num) = value.parse::<u64>() {
                metrics.push(("rocksdb.num_immutable_mem_table", num as f64, vec![]));
            }
        }

        // 4. rocksdb.mem-table-flush-pending
        // Whether a memtable flush is pending (0=no, 1=yes)
        // Threshold: 0 = healthy, 1 = flush in progress or queued
        // Action: If sustained at 1 with high num_immutable_mem_table
        // - Same actions as num_immutable_mem_table
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.mem-table-flush-pending") {
            if let Ok(pending) = value.parse::<u64>() {
                metrics.push(("rocksdb.mem_table_flush_pending", pending as f64, vec![]));
            }
        }

        // 5. rocksdb.num-files-at-level0
        // Number of SST files at Level 0 (most critical metric for write stalls)
        // Threshold: 0-20 = healthy, 21-29 = warning, 30-49 = slowdown active, >=50 = writes
        // stopped Action based on range:
        // - 21-29: Monitor, compaction catching up
        // - 30-49: Write slowdown active
        //   - Increase level_zero_slowdown_writes_trigger above 30
        //   - Increase max_background_jobs for more compaction threads
        //   - Decrease level_zero_file_num_compaction_trigger below 4 for earlier compaction
        // - >=50: Writes stopped, immediate action needed
        //   - Increase level_zero_stop_writes_trigger above 50
        //   - Reduce write throughput temporarily
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.num-files-at-level0") {
            if let Ok(num) = value.parse::<u64>() {
                metrics.push(("rocksdb.num_files_at_level0", num as f64, vec![]));
            }
        }

        // 6. rocksdb.estimate-pending-compaction-bytes
        // Estimated bytes needing compaction to reach target level sizes
        // Threshold: 0-64GB = healthy, 64-128GB = warning, 128-512GB = slowdown, >=512GB = stopped
        // Action based on range:
        // - 64-128GB: Compaction falling behind
        //   - Increase max_background_jobs
        //   - Increase max_compaction_bytes above 2GB for larger compactions
        // - 128-512GB: Soft limit reached, writes slowing
        //   - Increase soft_pending_compaction_bytes_limit above 128GB
        // - >=512GB: Hard limit, writes stopped
        //   - Increase hard_pending_compaction_bytes_limit above 512GB
        //   - Add more CPU cores to max_background_jobs
        if let Ok(Some(value)) =
            self.state_db.property_value("rocksdb.estimate-pending-compaction-bytes")
        {
            if let Ok(bytes) = value.parse::<u64>() {
                metrics.push(("rocksdb.estimate_pending_compaction_bytes", bytes as f64, vec![]));
            }
        }

        // 7. rocksdb.cur-size-all-mem-tables
        // Total memory used by all memtables (active and immutable)
        // Threshold: Expect up to db_write_buffer_size (8GB)
        // Action: If approaching 8GB with high num_immutable_mem_table
        // - Flush is bottlenecked, same actions as num_immutable_mem_table
        // - May increase db_write_buffer_size if memory available
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.cur-size-all-mem-tables") {
            if let Ok(size) = value.parse::<u64>() {
                metrics.push(("rocksdb.cur_size_all_mem_tables", size as f64, vec![]));
            }
        }

        // 8. rocksdb.num-running-compactions
        // Number of compaction threads currently active
        // Threshold: 0-14 = normal (max_background_jobs=14), >14 should not happen
        // Action: If consistently at max (14) with pending_compaction_bytes growing
        // - Increase max_background_jobs above 14 (if CPU available)
        // - Check disk I/O is not saturated (30000 IOPS available)
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.num-running-compactions") {
            if let Ok(num) = value.parse::<u64>() {
                metrics.push(("rocksdb.num_running_compactions", num as f64, vec![]));
            }
        }

        // 9. rocksdb.num-running-flushes
        // Number of memtable flush operations currently active
        // Threshold: 0-4 = normal, >4 with high num_immutable_mem_table = bottleneck
        // Action: If sustained high with growing num_immutable_mem_table
        // - Check disk write bandwidth (may be saturated)
        // - Reduce write_buffer_size (256MB) for faster individual flushes
        // - Increase max_background_jobs for more flush threads
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.num-running-flushes") {
            if let Ok(num) = value.parse::<u64>() {
                metrics.push(("rocksdb.num_running_flushes", num as f64, vec![]));
            }
        }

        // 10. rocksdb.compaction-pending
        // Whether any compaction is pending (0=no, 1=yes)
        // Threshold: 0-1 = normal (1 expected under load)
        // Action: If 1 with high estimate_pending_compaction_bytes
        // - See estimate_pending_compaction_bytes actions
        // - Indicates compaction scheduler is active
        if let Ok(Some(value)) = self.state_db.property_value("rocksdb.compaction-pending") {
            if let Ok(pending) = value.parse::<u64>() {
                metrics.push(("rocksdb.compaction_pending", pending as f64, vec![]));
            }
        }

        metrics
    }

    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        let mut metrics = Vec::new();
        for table in Tables::ALL.iter().map(Tables::name) {
            let db = match table {
                tables::AccountsTrieV2::NAME => &self.account_db,
                tables::StoragesTrieV2::NAME => &self.storage_db,
                _ => &self.state_db,
            };
            if let Some(cf_handle) = db.cf_handle(table) {
                if let Ok(Some(value)) =
                    db.property_value_cf(cf_handle, "rocksdb.estimate-num-keys")
                {
                    if let Ok(entries) = value.parse::<usize>() {
                        metrics.push((
                            "db.table_entries",
                            entries as f64,
                            vec![Label::new("table", table)],
                        ));
                    }
                }
            }
        }
        metrics
    }
}

// Implement Database trait for RocksDB
impl reth_db_api::database::Database for DatabaseEnv {
    type TX = tx::Tx<tx::RO>;
    type TXMut = tx::Tx<tx::RW>;

    fn tx(&self) -> Result<Self::TX, crate::DatabaseError> {
        Ok(tx::Tx::new(self.state_db.clone(), self.account_db.clone(), self.storage_db.clone()))
    }

    fn tx_mut(&self) -> Result<Self::TXMut, crate::DatabaseError> {
        Ok(tx::Tx::new(self.state_db.clone(), self.account_db.clone(), self.storage_db.clone()))
    }
}

/// Convert RocksDB error to DatabaseErrorInfo
fn to_error_info(e: rocksdb::Error) -> DatabaseErrorInfo {
    DatabaseErrorInfo { message: e.to_string().into(), code: -1 }
}

/// Create a read error from RocksDB error
fn read_error(e: rocksdb::Error) -> DatabaseError {
    DatabaseError::Read(to_error_info(e))
}

/// Helper function to get column family handle with proper error handling
fn get_cf_handle<T: Table>(db: &DB) -> Result<&rocksdb::ColumnFamily, DatabaseError> {
    db.cf_handle(T::NAME).ok_or_else(|| {
        DatabaseError::Open(DatabaseErrorInfo {
            message: format!("Column family '{}' not found", T::NAME).into(),
            code: -1,
        })
    })
}
