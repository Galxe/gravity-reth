//! Common types in gravity-reth.

mod config;
pub use config::{
    get_gravity_config, init_gravity_config, Config, EIP7702_LOCKDOWN, PIPE_BLOCK_GAS_LIMIT,
};
