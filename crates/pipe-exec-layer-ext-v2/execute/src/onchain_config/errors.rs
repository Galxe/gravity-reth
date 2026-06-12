//! System transaction error types and decoding
//!
//! This module provides error types and decoding logic for system transactions:
//! - Metadata transactions (onBlockStart)
//! - DKG transactions (finishTransition)
//! - JWK/Oracle transactions (NativeOracle.record)

use alloy_primitives::Bytes;
use alloy_sol_macro::sol;
use alloy_sol_types::SolError;
use revm::context_interface::result::HaltReason;
use std::fmt;
use tracing::{error, warn};

// ============================================================================
// Error Definitions from Solidity Contracts
// ============================================================================

sol! {
    // -------------------- SystemAccessControl Errors --------------------
    /// @notice Caller is not authorized for the operation
    error Unauthorized();

    // -------------------- Timestamp Errors --------------------
    /// @notice Timestamp must advance for normal blocks
    error TimestampMustAdvance(uint64 proposed, uint64 current);

    /// @notice Timestamp must equal current for NIL blocks
    error TimestampMustEqual(uint64 proposed, uint64 current);

    // -------------------- Validator Errors --------------------
    /// @notice Validator index out of bounds
    error ValidatorIndexOutOfBounds(uint64 index, uint64 total);

    // -------------------- Reconfiguration Errors --------------------
    /// @notice Reconfiguration is already in progress
    error ReconfigurationInProgress();

    /// @notice No reconfiguration in progress
    error ReconfigurationNotInProgress();

    /// @notice Reconfiguration contract has not been initialized
    error ReconfigurationNotInitialized();

    // -------------------- DKG Errors --------------------
    /// @notice DKG session is already in progress
    error DKGInProgress();

    /// @notice No DKG session is in progress
    error DKGNotInProgress();

    /// @notice DKG contract has not been initialized
    error DKGNotInitialized();

    // -------------------- NativeOracle Errors --------------------
    /// @notice Nonce must be strictly increasing for each source
    error NonceNotIncreasing(uint32 sourceType, uint256 sourceId, uint128 currentNonce, uint128 providedNonce);

    /// @notice Batch arrays have mismatched lengths
    error OracleBatchArrayLengthMismatch(uint256 noncesLength, uint256 payloadsLength, uint256 gasLimitsLength);

    // -------------------- JWKManager Errors (for reference, callback failures don't revert main tx) --------------------
    /// @notice JWK version must be strictly increasing
    error JWKVersionNotIncreasing(bytes issuer, uint64 currentVersion, uint64 providedVersion);
}

// ============================================================================
// Error Types
// ============================================================================

/// Severity of system transaction error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Fatal error - should halt block production
    Fatal,
    /// Recoverable error - can skip this transaction and continue
    Recoverable,
}

/// Decoded system transaction error
#[derive(Debug, Clone)]
pub struct SystemTxnError {
    /// Name of the error (from Solidity)
    pub name: String,
    /// Human-readable details
    pub details: String,
    /// Error severity
    pub severity: ErrorSeverity,
}

impl fmt::Display for SystemTxnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}: {}", self.severity, self.name, self.details)
    }
}

/// Result of decoding a system transaction execution result
#[derive(Debug)]
pub enum SystemTxnExecutionResult {
    /// Execution succeeded
    Success,
    /// Execution reverted with a known error
    KnownError(SystemTxnError),
    /// Execution reverted with an unknown error
    UnknownRevert { output: Bytes },
    /// Execution halted (e.g., out of gas)
    Halt { reason: HaltReason },
}

// ============================================================================
// Error Decoding
// ============================================================================

/// Decode a revert output into a known error type
///
/// Uses 4-byte selector matching for O(1) lookup, then decodes only the matched error.
pub fn decode_revert_error(output: &Bytes) -> Option<SystemTxnError> {
    // Need at least 4 bytes for selector
    if output.len() < 4 {
        return None;
    }

    // Extract 4-byte selector
    let selector: [u8; 4] = output[..4].try_into().ok()?;

    // Match selector and decode
    match selector {
        // -------------------- Fatal Errors --------------------
        s if s == Unauthorized::SELECTOR => Some(SystemTxnError {
            name: "Unauthorized".into(),
            details: "Caller is not authorized (should be SYSTEM_CALLER)".into(),
            severity: ErrorSeverity::Fatal,
        }),

        s if s == TimestampMustAdvance::SELECTOR => {
            let err = TimestampMustAdvance::abi_decode(output).ok()?;
            Some(SystemTxnError {
                name: "TimestampMustAdvance".into(),
                details: format!(
                    "Timestamp must increase: proposed={} <= current={}",
                    err.proposed, err.current
                ),
                severity: ErrorSeverity::Fatal,
            })
        }

        s if s == TimestampMustEqual::SELECTOR => {
            let err = TimestampMustEqual::abi_decode(output).ok()?;
            Some(SystemTxnError {
                name: "TimestampMustEqual".into(),
                details: format!(
                    "NIL block timestamp mismatch: proposed={} != current={}",
                    err.proposed, err.current
                ),
                severity: ErrorSeverity::Fatal,
            })
        }

        s if s == ValidatorIndexOutOfBounds::SELECTOR => {
            let err = ValidatorIndexOutOfBounds::abi_decode(output).ok()?;
            Some(SystemTxnError {
                name: "ValidatorIndexOutOfBounds".into(),
                details: format!("Proposer index {} >= total validators {}", err.index, err.total),
                severity: ErrorSeverity::Fatal,
            })
        }

        s if s == ReconfigurationNotInitialized::SELECTOR => Some(SystemTxnError {
            name: "ReconfigurationNotInitialized".into(),
            details: "Reconfiguration contract not initialized".into(),
            severity: ErrorSeverity::Fatal,
        }),

        s if s == DKGNotInitialized::SELECTOR => Some(SystemTxnError {
            name: "DKGNotInitialized".into(),
            details: "DKG contract not initialized".into(),
            severity: ErrorSeverity::Fatal,
        }),

        s if s == OracleBatchArrayLengthMismatch::SELECTOR => {
            let err = OracleBatchArrayLengthMismatch::abi_decode(output).ok()?;
            Some(SystemTxnError {
                name: "OracleBatchArrayLengthMismatch".into(),
                details: format!(
                    "Array length mismatch: nonces={}, payloads={}, gasLimits={}",
                    err.noncesLength, err.payloadsLength, err.gasLimitsLength
                ),
                severity: ErrorSeverity::Fatal,
            })
        }

        // -------------------- Recoverable Errors --------------------
        s if s == ReconfigurationNotInProgress::SELECTOR => Some(SystemTxnError {
            name: "ReconfigurationNotInProgress".into(),
            details: "No epoch transition in progress (possibly already completed)".into(),
            severity: ErrorSeverity::Recoverable,
        }),

        s if s == ReconfigurationInProgress::SELECTOR => Some(SystemTxnError {
            name: "ReconfigurationInProgress".into(),
            details: "Epoch transition already in progress".into(),
            severity: ErrorSeverity::Recoverable,
        }),

        s if s == DKGNotInProgress::SELECTOR => Some(SystemTxnError {
            name: "DKGNotInProgress".into(),
            details: "No DKG session in progress (possibly already finished)".into(),
            severity: ErrorSeverity::Recoverable,
        }),

        s if s == DKGInProgress::SELECTOR => Some(SystemTxnError {
            name: "DKGInProgress".into(),
            details: "DKG session already in progress".into(),
            severity: ErrorSeverity::Recoverable,
        }),

        s if s == NonceNotIncreasing::SELECTOR => {
            let err = NonceNotIncreasing::abi_decode(output).ok()?;
            Some(SystemTxnError {
                name: "NonceNotIncreasing".into(),
                details: format!(
                    "Oracle nonce not increasing: sourceType={}, sourceId={}, current={}, provided={}",
                    err.sourceType, err.sourceId, err.currentNonce, err.providedNonce
                ),
                severity: ErrorSeverity::Recoverable,
            })
        }

        // Unknown selector
        _ => None,
    }
}

/// Analyze a system transaction execution result
pub fn analyze_execution_result(
    result: &revm::context_interface::result::ExecutionResult,
) -> SystemTxnExecutionResult {
    use revm::context_interface::result::ExecutionResult;

    match result {
        ExecutionResult::Success { .. } => SystemTxnExecutionResult::Success,
        ExecutionResult::Revert { output, .. } => {
            if let Some(error) = decode_revert_error(output) {
                SystemTxnExecutionResult::KnownError(error)
            } else {
                SystemTxnExecutionResult::UnknownRevert { output: output.clone() }
            }
        }
        ExecutionResult::Halt { reason, .. } => {
            SystemTxnExecutionResult::Halt { reason: reason.clone() }
        }
    }
}

/// Log an execution error with appropriate severity
///
/// This is the simplified public API for logging system transaction errors.
/// Call this when `result.is_success()` returns false.
pub fn log_execution_error(result: &revm::context_interface::result::ExecutionResult) {
    use revm::context_interface::result::ExecutionResult;

    match result {
        ExecutionResult::Success { .. } => {
            // No error to log
        }
        ExecutionResult::Revert { output, .. } => {
            if let Some(err) = decode_revert_error(output) {
                match err.severity {
                    ErrorSeverity::Fatal => {
                        error!("[FATAL] System transaction failed: {} - {}", err.name, err.details);
                    }
                    ErrorSeverity::Recoverable => {
                        warn!(
                            "[RECOVERABLE] System transaction failed: {} - {}",
                            err.name, err.details
                        );
                    }
                }
            } else {
                error!("System transaction reverted with unknown error: 0x{}", hex::encode(output));
            }
        }
        ExecutionResult::Halt { reason, .. } => {
            error!("System transaction halted: {:?}", reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolError;

    #[test]
    fn test_decode_timestamp_must_advance() {
        let error = TimestampMustAdvance { proposed: 1000, current: 2000 };
        let encoded = error.abi_encode();
        let result = decode_revert_error(&encoded.into());

        assert!(result.is_some());
        let err = result.unwrap();
        assert_eq!(err.name, "TimestampMustAdvance");
        assert_eq!(err.severity, ErrorSeverity::Fatal);
        assert!(err.details.contains("1000"));
        assert!(err.details.contains("2000"));
    }

    #[test]
    fn test_decode_reconfiguration_not_in_progress() {
        let error = ReconfigurationNotInProgress {};
        let encoded = error.abi_encode();
        let result = decode_revert_error(&encoded.into());

        assert!(result.is_some());
        let err = result.unwrap();
        assert_eq!(err.name, "ReconfigurationNotInProgress");
        assert_eq!(err.severity, ErrorSeverity::Recoverable);
    }

    #[test]
    fn test_decode_nonce_not_increasing() {
        let error = NonceNotIncreasing {
            sourceType: 1,
            sourceId: alloy_primitives::U256::from(42),
            currentNonce: 10,
            providedNonce: 5,
        };
        let encoded = error.abi_encode();
        let result = decode_revert_error(&encoded.into());

        assert!(result.is_some());
        let err = result.unwrap();
        assert_eq!(err.name, "NonceNotIncreasing");
        assert_eq!(err.severity, ErrorSeverity::Recoverable);
    }

    #[test]
    fn test_decode_unknown_error() {
        // Random bytes that don't match any known error
        let unknown = Bytes::from(vec![0x12, 0x34, 0x56, 0x78, 0xAB, 0xCD]);
        let result = decode_revert_error(&unknown);
        assert!(result.is_none());
    }
}
