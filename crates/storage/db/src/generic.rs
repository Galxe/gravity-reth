//! Generic database environment that supports multiple backends.

// Re-export the unified database functions and types
pub use crate::database::{create_db, init_db, open_db, open_db_read_only};

// Always use RocksDB implementation
pub use crate::implementation::rocksdb::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
