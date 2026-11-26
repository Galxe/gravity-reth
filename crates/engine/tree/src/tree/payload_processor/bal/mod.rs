//! BAL-driven parallel block execution.
//!
//! This is an opt-in library helper. Gravity's engine and SDK pipeline do not select this path.
//! Workers execute against a supplied decoded BAL; the caller owns fork activation and block
//! validation. The ordered commit loop follows the pinned serial executor's block-gas admission.
//! It returns the rebuilt BAL for the caller to validate against the block header.

mod ordered_outputs;
mod worker;

pub mod error;
pub mod execute;

pub use error::BalExecutionError;
pub use execute::execute_block;
