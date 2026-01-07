//! RocksDB implementation for the database.

use crate::{DatabaseError, TableSet};
use metrics::Label;
use reth_db_api::{database_metrics::DatabaseMetrics, models::ClientVersion, table::Table, Tables};
use reth_storage_errors::db::{DatabaseErrorInfo, LogLevel};
use rocksdb::{BlockBasedOptions, Cache, Options, DB};
use std::{path::Path, sync::Arc};

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
}

impl DatabaseArguments {
    /// Default block cache size: 8GB
    pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 8 * 1024 * 1024 * 1024;
    /// Default write buffer size: 256MB
    pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024;
    /// Default max background jobs: 14
    pub const DEFAULT_MAX_BACKGROUND_JOBS: i32 = 14;
    /// Default max open files: unlimited (-1)
    pub const DEFAULT_MAX_OPEN_FILES: i32 = -1;
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
}

impl Default for DatabaseArguments {
    fn default() -> Self {
        Self::new(ClientVersion::default())
    }
}

/// RocksDB database environment.
#[derive(Debug)]
pub struct DatabaseEnv {
    /// Inner RocksDB database.
    pub(crate) inner: Arc<DB>,
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
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // === Parallelism Configuration ===
        // Configure background jobs for compaction and flush
        opts.set_max_background_jobs(args.max_background_jobs());

        // Increase parallelism for multi-threaded compactions
        let parallelism = (args.max_background_jobs() + 2).max(4) as i32;
        opts.increase_parallelism(parallelism);

        // Set max open files limit
        opts.set_max_open_files(args.max_open_files());

        // === Memory Configuration ===
        // Write buffer size per memtable
        let write_buffer_size = args.write_buffer_size();
        opts.set_write_buffer_size(write_buffer_size);

        // Configure max write buffers (memtables) before blocking writes
        // More buffers allow absorbing write spikes but use more memory
        opts.set_max_write_buffer_number(args.max_write_buffer_number());

        // Min write buffers to merge before flush (reduce write amplification)
        opts.set_min_write_buffer_number_to_merge(2);

        // Total memtable size across all column families (8GB)
        // Helps absorb write spikes
        opts.set_db_write_buffer_size(8 * 1024 * 1024 * 1024);

        // === Block Cache Configuration ===
        // Shared block cache for all column families (configurable, default 8GB)
        // Critical for read performance with frequent random reads
        let block_cache_size = args.block_cache_size();
        let cache = Cache::new_lru_cache(block_cache_size);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_block_size(32 * 1024); // 32KB blocks (good for random reads)
        block_opts.set_cache_index_and_filter_blocks(true); // Cache index/filters
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true); // Keep L0 indexes in cache

        // Enable bloom filters for faster point lookups
        block_opts.set_bloom_filter(10.0, false); // 10 bits per key

        opts.set_block_based_table_factory(&block_opts);

        // === Compaction Configuration ===
        // Level-based compaction with higher thresholds to delay write stalls
        opts.set_level_compaction_dynamic_level_bytes(true);

        // L0 file triggers - configurable to balance read/write amplification
        let l0_trigger = args.level0_file_num_compaction_trigger();
        opts.set_level_zero_file_num_compaction_trigger(l0_trigger);
        // Slowdown and stop triggers scale with compaction trigger
        opts.set_level_zero_slowdown_writes_trigger(l0_trigger * 7); // 7x trigger
        opts.set_level_zero_stop_writes_trigger(l0_trigger * 12); // 12x trigger

        // Pending compaction bytes limits - increased for high-throughput
        // Soft limit triggers slowdown, hard limit stops writes
        opts.set_soft_pending_compaction_bytes_limit(128 * 1024 * 1024 * 1024); // 128GB
        opts.set_hard_pending_compaction_bytes_limit(512 * 1024 * 1024 * 1024); // 512GB

        // Target file size for L1 (256MB, doubles per level)
        opts.set_target_file_size_base(256 * 1024 * 1024);
        opts.set_target_file_size_multiplier(2);

        // L1 size - configurable to control LSM tree structure
        opts.set_max_bytes_for_level_base(args.max_bytes_for_level_base());
        opts.set_max_bytes_for_level_multiplier(10.0);

        // Maximum compaction bytes at once (2GB)
        opts.set_max_compaction_bytes(2 * 1024 * 1024 * 1024);

        // === Write Configuration ===
        // Note: set_delayed_write_rate not available in rocksdb-rs 0.22
        // The default delayed_write_rate will be used when write stall occurs

        // Enable pipelined writes for better concurrency
        opts.set_enable_pipelined_write(true);

        // WAL configuration
        opts.set_max_total_wal_size(2 * 1024 * 1024 * 1024); // 2GB max WAL size
        opts.set_wal_bytes_per_sync(4 * 1024 * 1024); // Sync WAL every 4MB

        // === Compression Configuration ===
        // Use LZ4 for L0-L1 (fast), Zstd for L2+ (better compression)
        opts.set_compression_per_level(&[
            rocksdb::DBCompressionType::Lz4,  // L0
            rocksdb::DBCompressionType::Lz4,  // L1
            rocksdb::DBCompressionType::Zstd, // L2
            rocksdb::DBCompressionType::Zstd, // L3
            rocksdb::DBCompressionType::Zstd, // L4
            rocksdb::DBCompressionType::Zstd, // L5
            rocksdb::DBCompressionType::Zstd, // L6
        ]);

        // === I/O Optimization ===
        // Optimize for SSD with high IOPS
        opts.set_bytes_per_sync(args.bytes_per_sync()); // Configurable background sync
        opts.set_compaction_readahead_size(args.compaction_readahead_size()); // Configurable compaction readahead

        // Get all required table names
        let required_tables: Vec<String> = Tables::tables().map(|t| t.name().to_string()).collect();
        let db = DB::open_cf(&opts, path, &required_tables)
            .map_err(|e| DatabaseError::Other(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self { inner: Arc::new(db), kind })
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
            return Ok(())
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.actual-delayed-write-rate") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.is-write-stopped") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.num-immutable-mem-table") {
            if let Ok(num) = value.parse::<u64>() {
                metrics.push(("rocksdb.num_immutable_mem_table", num as f64, vec![]));
            }
        }

        // 4. rocksdb.mem-table-flush-pending
        // Whether a memtable flush is pending (0=no, 1=yes)
        // Threshold: 0 = healthy, 1 = flush in progress or queued
        // Action: If sustained at 1 with high num_immutable_mem_table
        // - Same actions as num_immutable_mem_table
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.mem-table-flush-pending") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.num-files-at-level0") {
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
            self.inner.property_value("rocksdb.estimate-pending-compaction-bytes")
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.cur-size-all-mem-tables") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.num-running-compactions") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.num-running-flushes") {
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
        if let Ok(Some(value)) = self.inner.property_value("rocksdb.compaction-pending") {
            if let Ok(pending) = value.parse::<u64>() {
                metrics.push(("rocksdb.compaction_pending", pending as f64, vec![]));
            }
        }

        metrics
    }

    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        let mut metrics = Vec::new();
        for table in Tables::ALL.iter().map(Tables::name) {
            if let Some(cf_handle) = self.inner.cf_handle(table) {
                if let Ok(Some(value)) =
                    self.inner.property_value_cf(cf_handle, "rocksdb.estimate-num-keys")
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
        Ok(tx::Tx::new(self.inner.clone()))
    }

    fn tx_mut(&self) -> Result<Self::TXMut, crate::DatabaseError> {
        Ok(tx::Tx::new(self.inner.clone()))
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
