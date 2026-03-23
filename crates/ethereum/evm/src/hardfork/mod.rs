//! Gravity-specific hardfork state changes.
//!
//! Each hardfork (alpha, beta, gamma, ...) should be added as a submodule
//! on the corresponding release branch. The `common` module provides shared
//! traits and types that all hardfork modules implement.

pub mod alpha;
pub mod beta;
pub mod common;
pub mod delta;
pub mod gamma;
