//! Common types in gravity-reth.

mod config;
pub use config::{
    get_global_prewarm_sender, get_gravity_config, init_gravity_config, set_global_prewarm_sender,
    Config, PrewarmConfig,
};
