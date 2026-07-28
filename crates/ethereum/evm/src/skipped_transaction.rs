//! Protocol encoding for transactions skipped during Gravity block execution.

use alloy_consensus::TxType;
use alloy_primitives::{Bytes, Log, B256};
use alloy_sol_types::{sol, SolEvent};
use grevm::InvalidTransaction;
use reth_chainspec::GRAVITY_TX_SKIPPED_LOG_ADDRESS;
use reth_ethereum_primitives::Receipt;

sol! {
    /// Emitted in the synthetic receipt of an included transaction that was invalid at final
    /// execution state and therefore applied as a no-op.
    event GravityTxSkipped(uint16 indexed version, uint16 indexed reason, bytes encodedError);
}

/// Current version of the [`GravityTxSkipped`] event encoding.
pub const GRAVITY_TX_SKIPPED_LOG_VERSION: u16 = 1;

/// `keccak256("GravityTxSkipped(uint16,uint16,bytes)")`, derived from the Solidity declaration.
pub const GRAVITY_TX_SKIPPED_LOG_TOPIC0: B256 = GravityTxSkipped::SIGNATURE_HASH;

macro_rules! define_skip_reasons {
    ($($variant:ident = $code:literal => $pattern:pat),+ $(,)?) => {
        /// The zero-based wire tag for a skipped [`InvalidTransaction`].
        ///
        /// Variants intentionally follow the declaration order of revm's [`InvalidTransaction`].
        /// A revm upgrade that changes that enum must update this table and the log version.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u16)]
        pub enum GravityTxSkipReason {
            $(
                #[doc = concat!("Corresponds to [`InvalidTransaction::", stringify!($variant), "`].")]
                $variant = $code,
            )+
        }

        impl GravityTxSkipReason {
            /// Returns the wire tag for `error`.
            pub const fn from_invalid_transaction(error: &InvalidTransaction) -> Self {
                match error {
                    $($pattern => Self::$variant,)+
                }
            }

            /// Parses a wire tag.
            pub const fn from_code(code: u16) -> Option<Self> {
                match code {
                    $($code => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

define_skip_reasons! {
    PriorityFeeGreaterThanMaxFee = 0 => InvalidTransaction::PriorityFeeGreaterThanMaxFee,
    GasPriceLessThanBasefee = 1 => InvalidTransaction::GasPriceLessThanBasefee,
    CallerGasLimitMoreThanBlock = 2 => InvalidTransaction::CallerGasLimitMoreThanBlock,
    CallGasCostMoreThanGasLimit = 3 => InvalidTransaction::CallGasCostMoreThanGasLimit { .. },
    GasFloorMoreThanGasLimit = 4 => InvalidTransaction::GasFloorMoreThanGasLimit { .. },
    RejectCallerWithCode = 5 => InvalidTransaction::RejectCallerWithCode,
    LackOfFundForMaxFee = 6 => InvalidTransaction::LackOfFundForMaxFee { .. },
    OverflowPaymentInTransaction = 7 => InvalidTransaction::OverflowPaymentInTransaction,
    NonceOverflowInTransaction = 8 => InvalidTransaction::NonceOverflowInTransaction,
    NonceTooHigh = 9 => InvalidTransaction::NonceTooHigh { .. },
    NonceTooLow = 10 => InvalidTransaction::NonceTooLow { .. },
    CreateInitCodeSizeLimit = 11 => InvalidTransaction::CreateInitCodeSizeLimit,
    InvalidChainId = 12 => InvalidTransaction::InvalidChainId,
    MissingChainId = 13 => InvalidTransaction::MissingChainId,
    TxGasLimitGreaterThanCap = 14 => InvalidTransaction::TxGasLimitGreaterThanCap { .. },
    AccessListNotSupported = 15 => InvalidTransaction::AccessListNotSupported,
    MaxFeePerBlobGasNotSupported = 16 => InvalidTransaction::MaxFeePerBlobGasNotSupported,
    BlobVersionedHashesNotSupported = 17 => InvalidTransaction::BlobVersionedHashesNotSupported,
    BlobGasPriceGreaterThanMax = 18 => InvalidTransaction::BlobGasPriceGreaterThanMax { .. },
    EmptyBlobs = 19 => InvalidTransaction::EmptyBlobs,
    BlobCreateTransaction = 20 => InvalidTransaction::BlobCreateTransaction,
    TooManyBlobs = 21 => InvalidTransaction::TooManyBlobs { .. },
    BlobVersionNotSupported = 22 => InvalidTransaction::BlobVersionNotSupported,
    AuthorizationListNotSupported = 23 => InvalidTransaction::AuthorizationListNotSupported,
    AuthorizationListInvalidFields = 24 => InvalidTransaction::AuthorizationListInvalidFields,
    EmptyAuthorizationList = 25 => InvalidTransaction::EmptyAuthorizationList,
    Eip2930NotSupported = 26 => InvalidTransaction::Eip2930NotSupported,
    Eip1559NotSupported = 27 => InvalidTransaction::Eip1559NotSupported,
    Eip4844NotSupported = 28 => InvalidTransaction::Eip4844NotSupported,
    Eip7702NotSupported = 29 => InvalidTransaction::Eip7702NotSupported,
    Eip7873NotSupported = 30 => InvalidTransaction::Eip7873NotSupported,
    Eip7873MissingTarget = 31 => InvalidTransaction::Eip7873MissingTarget,
    Str = 32 => InvalidTransaction::Str(_),
}

/// Encoder and decoder for Gravity's synthetic skipped-transaction receipt.
#[derive(Debug, Clone, Copy, Default)]
pub struct GravityTxSkippedEvent;

impl GravityTxSkippedEvent {
    /// Creates a protocol log containing the complete [`InvalidTransaction`].
    ///
    /// Postcard encodes the enum variant as its zero-based declaration index and serializes every
    /// field carried by that variant.
    pub fn encode(error: &InvalidTransaction) -> Result<Log, postcard::Error> {
        let reason = GravityTxSkipReason::from_invalid_transaction(error);
        let encoded_error = postcard::to_allocvec(error)?;
        Ok(Log {
            address: GRAVITY_TX_SKIPPED_LOG_ADDRESS,
            data: GravityTxSkipped {
                version: GRAVITY_TX_SKIPPED_LOG_VERSION,
                reason: reason as u16,
                encodedError: Bytes::from(encoded_error),
            }
            .encode_log_data(),
        })
    }

    /// Parses a protocol log and reconstructs the complete [`InvalidTransaction`].
    ///
    /// A wrong emitter, ABI shape, version, tag, payload, or tag/payload mismatch is rejected.
    pub fn decode(log: &Log) -> Option<InvalidTransaction> {
        if log.address != GRAVITY_TX_SKIPPED_LOG_ADDRESS {
            return None
        }

        let event = GravityTxSkipped::decode_log(log).ok()?;
        if event.version != GRAVITY_TX_SKIPPED_LOG_VERSION {
            return None
        }

        let reason = GravityTxSkipReason::from_code(event.reason)?;
        let error = postcard::from_bytes::<InvalidTransaction>(&event.encodedError).ok()?;
        if GravityTxSkipReason::from_invalid_transaction(&error) != reason {
            return None
        }
        Some(error)
    }

    /// Builds the zero-gas failed receipt for a skipped transaction.
    pub fn receipt(
        tx_type: TxType,
        cumulative_gas_used: u64,
        error: &InvalidTransaction,
    ) -> Result<Receipt, postcard::Error> {
        Ok(Receipt {
            tx_type,
            success: false,
            cumulative_gas_used,
            logs: alloc::vec![Self::encode(error)?],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{borrow::Cow, boxed::Box, vec, vec::Vec};
    use alloy_primitives::{keccak256, Address, LogData, U256};

    #[test]
    fn topic_is_derived_from_the_documented_solidity_signature() {
        assert_eq!(
            GRAVITY_TX_SKIPPED_LOG_TOPIC0,
            keccak256("GravityTxSkipped(uint16,uint16,bytes)")
        );
    }

    #[test]
    fn all_invalid_transactions_round_trip_with_declaration_order_tags() {
        let errors = all_invalid_transactions();
        for (expected_tag, error) in errors.into_iter().enumerate() {
            let log = GravityTxSkippedEvent::encode(&error).unwrap();
            let event = GravityTxSkipped::decode_log(&log).unwrap();
            assert_eq!(event.reason, expected_tag as u16);
            assert_eq!(event.encodedError.first().copied(), Some(expected_tag as u8));
            assert_eq!(GravityTxSkippedEvent::decode(&log), Some(error));
        }
    }

    #[test]
    fn event_decode_rejects_non_protocol_logs_and_mismatched_tags() {
        let error = InvalidTransaction::NonceTooLow { tx: 0, state: 1 };
        let log = GravityTxSkippedEvent::encode(&error).unwrap();
        let mut wrong_emitter = log;
        wrong_emitter.address = Address::ZERO;
        assert_eq!(GravityTxSkippedEvent::decode(&wrong_emitter), None);

        let unknown_reason = Log {
            address: GRAVITY_TX_SKIPPED_LOG_ADDRESS,
            data: GravityTxSkipped {
                version: GRAVITY_TX_SKIPPED_LOG_VERSION,
                reason: u16::MAX,
                encodedError: Bytes::from(postcard::to_allocvec(&error).unwrap()),
            }
            .encode_log_data(),
        };
        assert_eq!(GravityTxSkippedEvent::decode(&unknown_reason), None);

        let mismatched_reason = Log {
            address: GRAVITY_TX_SKIPPED_LOG_ADDRESS,
            data: GravityTxSkipped {
                version: GRAVITY_TX_SKIPPED_LOG_VERSION,
                reason: GravityTxSkipReason::NonceTooHigh as u16,
                encodedError: Bytes::from(postcard::to_allocvec(&error).unwrap()),
            }
            .encode_log_data(),
        };
        assert_eq!(GravityTxSkippedEvent::decode(&mismatched_reason), None);

        let malformed = Log {
            address: GRAVITY_TX_SKIPPED_LOG_ADDRESS,
            data: LogData::new_unchecked(
                alloc::vec![GRAVITY_TX_SKIPPED_LOG_TOPIC0],
                Default::default(),
            ),
        };
        assert_eq!(GravityTxSkippedEvent::decode(&malformed), None);
    }

    fn all_invalid_transactions() -> Vec<InvalidTransaction> {
        vec![
            InvalidTransaction::PriorityFeeGreaterThanMaxFee,
            InvalidTransaction::GasPriceLessThanBasefee,
            InvalidTransaction::CallerGasLimitMoreThanBlock,
            InvalidTransaction::CallGasCostMoreThanGasLimit {
                initial_gas: 21_001,
                gas_limit: 21_000,
            },
            InvalidTransaction::GasFloorMoreThanGasLimit { gas_floor: 30_000, gas_limit: 21_000 },
            InvalidTransaction::RejectCallerWithCode,
            InvalidTransaction::LackOfFundForMaxFee {
                fee: Box::new(U256::from(100)),
                balance: Box::new(U256::from(99)),
            },
            InvalidTransaction::OverflowPaymentInTransaction,
            InvalidTransaction::NonceOverflowInTransaction,
            InvalidTransaction::NonceTooHigh { tx: 2, state: 1 },
            InvalidTransaction::NonceTooLow { tx: 0, state: 1 },
            InvalidTransaction::CreateInitCodeSizeLimit,
            InvalidTransaction::InvalidChainId,
            InvalidTransaction::MissingChainId,
            InvalidTransaction::TxGasLimitGreaterThanCap { gas_limit: 31_000, cap: 30_000 },
            InvalidTransaction::AccessListNotSupported,
            InvalidTransaction::MaxFeePerBlobGasNotSupported,
            InvalidTransaction::BlobVersionedHashesNotSupported,
            InvalidTransaction::BlobGasPriceGreaterThanMax {
                block_blob_gas_price: 2,
                tx_max_fee_per_blob_gas: 1,
            },
            InvalidTransaction::EmptyBlobs,
            InvalidTransaction::BlobCreateTransaction,
            InvalidTransaction::TooManyBlobs { max: 6, have: 7 },
            InvalidTransaction::BlobVersionNotSupported,
            InvalidTransaction::AuthorizationListNotSupported,
            InvalidTransaction::AuthorizationListInvalidFields,
            InvalidTransaction::EmptyAuthorizationList,
            InvalidTransaction::Eip2930NotSupported,
            InvalidTransaction::Eip1559NotSupported,
            InvalidTransaction::Eip4844NotSupported,
            InvalidTransaction::Eip7702NotSupported,
            InvalidTransaction::Eip7873NotSupported,
            InvalidTransaction::Eip7873MissingTarget,
            InvalidTransaction::Str(Cow::Borrowed("custom invalid transaction")),
        ]
    }
}
