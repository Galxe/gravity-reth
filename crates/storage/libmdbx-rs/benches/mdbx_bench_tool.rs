#![allow(missing_docs)]

use rand::{rngs::StdRng, Rng, SeedableRng};
use reth_libmdbx::{Environment, WriteFlags};
use std::{
    env, fs,
    path::Path,
    time::{Duration, Instant},
};
use tempfile::{tempdir, TempDir};

/// Test mode
#[derive(Debug, Clone, PartialEq)]
pub enum TestMode {
    Write,
    Read,
}

/// Configuration parameters
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Number of data entries per batch
    pub batch_size: usize,
    /// Size of key in bytes
    pub key_size: usize,
    /// Size of value in bytes
    pub value_size: usize,
    /// Target number of batches to write
    pub target_batches: usize,
    /// Specified storage directory (None means use temporary directory)
    pub storage_dir: Option<String>,
    /// Whether to sort keys before writing
    pub sort_keys: bool,
    /// Test mode：write | read
    pub test_mode: TestMode,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            batch_size: 50000,
            key_size: 64,
            value_size: 200,
            target_batches: 100,
            storage_dir: None,
            sort_keys: false,
            test_mode: TestMode::Write,
        }
    }
}

impl BenchConfig {
    /// Create configuration from command line arguments
    pub fn from_args() -> Self {
        let mut config = Self::default();
        let args: Vec<String> = env::args().collect();

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--batch-size" => {
                    if i + 1 < args.len() {
                        if let Ok(size) = args[i + 1].parse() {
                            config.batch_size = size;
                        }
                        i += 1;
                    }
                }
                "--key-size" => {
                    if i + 1 < args.len() {
                        if let Ok(size) = args[i + 1].parse() {
                            config.key_size = size;
                        }
                        i += 1;
                    }
                }
                "--value-size" => {
                    if i + 1 < args.len() {
                        if let Ok(size) = args[i + 1].parse() {
                            config.value_size = size;
                        }
                        i += 1;
                    }
                }
                "--target-batches" => {
                    if i + 1 < args.len() {
                        if let Ok(batches) = args[i + 1].parse() {
                            config.target_batches = batches;
                        }
                        i += 1;
                    }
                }
                "--storage-dir" => {
                    if i + 1 < args.len() {
                        config.storage_dir = Some(args[i + 1].clone());
                        i += 1;
                    }
                }
                "--sort-keys" => {
                    config.sort_keys = true;
                }
                "--test-mode" => {
                    if i + 1 < args.len() {
                        match args[i + 1].as_str() {
                            "write" => config.test_mode = TestMode::Write,
                            "read" => config.test_mode = TestMode::Read,
                            _ => {
                                eprintln!(
                                    "Error: Invalid test mode '{}', supported modes: write, read",
                                    args[i + 1]
                                );
                                std::process::exit(1);
                            }
                        }
                        i += 1;
                    }
                }
                "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                "--test" | "--bench" => {
                    // Ignore arguments passed by criterion
                }
                _ => {
                    eprintln!("Unknown argument: {}", args[i]);
                    print_help();
                    std::process::exit(1);
                }
            }
            i += 1;
        }

        config
    }
}

/// Print help information
fn print_help() {
    println!("MDBX Write Performance Benchmark Tool");
    println!("Usage: cargo bench -p reth-libmdbx --bench mdbx_bench_tool -- [options]");
    println!();
    println!("Options:");
    println!("  --batch-size SIZE     Number of data entries per batch (default: 50000)");
    println!("  --key-size SIZE       Size of key in bytes (default: 64)");
    println!("  --value-size SIZE     Size of value in bytes (default: 200)");
    println!("  --target-batches COUNT  Target number of batches to write (default: 100)");
    println!(
        "  --storage-dir PATH     Specified storage directory (default: use temporary directory)"
    );
    println!("  --sort-keys            Sort keys before writing");
    println!("  --test-mode MODE       Test mode: write|read (default: write)");
    println!("  --help                 Show this help information");
}

/// Generate random key using shared RNG (more efficient)
fn generate_key_with_rng(rng: &mut StdRng, size: usize) -> Vec<u8> {
    let mut key = vec![0u8; size];
    rng.fill(&mut key[..]);
    key
}

/// Generate key list needed for read test
fn generate_read_keys(config: &BenchConfig, batch_idx: u64) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(config.batch_size);
    let mut rng = StdRng::seed_from_u64(batch_idx * 100000);

    for _ in 0..config.batch_size {
        let key = generate_key_with_rng(&mut rng, config.key_size);
        keys.push(key);
    }

    keys
}

/// Execute a batch of read operations
fn read_batch(
    env: &Environment,
    keys: &Vec<Vec<u8>>,
) -> Result<(Duration, usize), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let txn = env.begin_ro_txn()?;
    let db = txn.open_db(None)?;

    let mut found_count = 0;
    for key in keys {
        if let Ok(_) = txn.get::<Vec<u8>>(db.dbi(), key) {
            found_count += 1;
        }
    }

    Ok((start.elapsed(), found_count))
}

/// Generate value of specified size
fn generate_value(index: usize, size: usize) -> Vec<u8> {
    let value_str = format!("value_{:0width$}", index, width = size.saturating_sub(7));
    let mut value = value_str.into_bytes();
    value.resize(size, b'1');
    value
}

/// Get database size (number of entries)
fn get_database_size(env: &Environment) -> u64 {
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    // Use db_stat() method to quickly get entry count
    txn.db_stat(&db).map(|stat| stat.entries() as u64).unwrap_or(0)
}

/// Execute a batch of write operations
fn write_batch(
    env: &Environment,
    start_index: usize,
    config: &BenchConfig,
) -> Result<(Duration, Duration), Box<dyn std::error::Error>> {
    let mut data = Vec::with_capacity(config.batch_size);

    // Create an RNG for the entire batch, using start_index as seed
    let mut rng = StdRng::seed_from_u64(start_index as u64);

    for i in 0..config.batch_size {
        let key = generate_key_with_rng(&mut rng, config.key_size);
        let value = generate_value(start_index + i, config.value_size);
        data.push((key, value));
    }

    // If sorting is enabled, sort the keys
    if config.sort_keys {
        data.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let start = Instant::now();

    let txn = env.begin_rw_txn()?;
    let db = txn.open_db(None)?;
    for (key, value) in data.iter() {
        txn.put(db.dbi(), key, value, WriteFlags::empty())?;
    }

    let commit_start = Instant::now();
    txn.commit()?;

    Ok((start.elapsed(), commit_start.elapsed()))
}

/// Setup database environment
fn setup_environment(
    config: &BenchConfig,
) -> Result<(Option<TempDir>, Environment), Box<dyn std::error::Error>> {
    let temp_dir;
    let db_path;

    if let Some(path) = &config.storage_dir {
        temp_dir = None;
        db_path = Path::new(path);

        // Ensure directory exists
        if !db_path.exists() {
            fs::create_dir_all(db_path)?;
        }

        println!("Using specified directory: {:?}", db_path);
    } else {
        temp_dir = Some(tempdir()?);
        db_path = temp_dir.as_ref().unwrap().path();
        println!("Using temporary directory: {:?}", db_path);
    }

    let env = Environment::builder().open(db_path)?;
    Ok((temp_dir, env))
}

/// Execute write test: observe write time changes as database size increases
fn run_write_benchmark(
    env: &Environment,
    config: &BenchConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting write test, target writing {} batches...", config.target_batches);
    println!(
        "Each batch {} entries, Key size: {} bytes, Value size: {} bytes",
        config.batch_size, config.key_size, config.value_size
    );

    for batch in 0..config.target_batches {
        let data_count = get_database_size(env);
        let duration = write_batch(env, data_count as usize, config)?;

        println!(
            "Batch {}: Duration {:?}, commit duration {:?}, DB entries: {}",
            batch, duration.0, duration.1, data_count
        );
    }

    println!("Write test completed, total written {} entries", get_database_size(env));
    Ok(())
}

/// Execute read test
fn run_read_benchmark(
    env: &Environment,
    config: &BenchConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_size = get_database_size(env);
    if db_size == 0 {
        println!("Error: Database is empty, cannot perform read test. Please run write test first to generate data.");
        return Ok(());
    }

    println!("Starting read test, database currently has {} entries", db_size);
    println!(
        "Each batch reads {} entries, Key size: {} bytes, Value size: {} bytes",
        config.batch_size, config.key_size, config.value_size
    );

    // Execute read test
    for batch_idx in 0..config.target_batches {
        let batch_keys = generate_read_keys(&config, batch_idx as u64);
        let (read_duration, found_count) = read_batch(env, &batch_keys)?;

        println!(
            "Batch {}: Duration {:?}, found {}/{} entries, DB entries: {}, per entry: {:?}",
            batch_idx,
            read_duration,
            found_count,
            batch_keys.len(),
            db_size,
            read_duration / batch_keys.len() as u32
        );
    }

    println!("Read test completed");
    Ok(())
}

/// Main function
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig::from_args();

    println!("MDBX Performance Benchmark Tool");
    println!("Configuration: {:?}\n", config);

    // Setup database environment
    let (_temp_dir, env) = setup_environment(&config)?;

    // Execute different tests based on test mode
    match config.test_mode {
        TestMode::Write => run_write_benchmark(&env, &config),
        TestMode::Read => run_read_benchmark(&env, &config),
    }
}
