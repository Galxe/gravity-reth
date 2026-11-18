//! EVM database types used by the RPC state cache.

use reth_revm::database::StateProviderDatabase;
use reth_storage_api::StateProviderBox;
use revm::database::State;

/// The state used by RPC execution.
pub type StateCacheDb = State<StateProviderDatabase<StateProviderBox>>;
