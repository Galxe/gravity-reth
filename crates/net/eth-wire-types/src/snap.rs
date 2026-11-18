//! Implements Ethereum SNAP message types.
//! Snap protocol runs on top of `RLPx`
//! facilitating the exchange of Ethereum state snapshots between peers
//! Reference: [Ethereum Snapshot Protocol](https://github.com/ethereum/devp2p/blob/master/caps/snap.md#protocol-messages)
//!
//! This module currently includes snap/1 plus preparatory snap/2 message definitions.

use crate::BlockAccessLists;
use alloc::vec::Vec;
use alloy_primitives::{Bytes, B256, KECCAK256_EMPTY, U256};
use alloy_rlp::{BufMut, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use alloy_trie::{TrieAccount, EMPTY_ROOT_HASH};
use reth_codecs_derive::add_arbitrary_tests;

/// Supported SNAP protocol versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(u8)]
pub enum SnapVersion {
    /// The original snapshot protocol.
    #[default]
    V1 = 1,
    /// BAL-based healing as proposed by EIP-8189.
    V2 = 2,
}

impl SnapVersion {
    /// Returns the message slot length; use `supports_message_id` to check valid IDs.
    pub const fn message_count(self) -> u8 {
        match self {
            Self::V1 => 8,
            Self::V2 => 10,
        }
    }

    /// Returns the highest supported message id for this version.
    pub const fn max_message_id(self) -> u8 {
        self.message_count() - 1
    }

    /// Returns `true` if `id` is a valid message id for this snap version.
    ///
    /// `snap/2` drops trie nodes (`0x06`/`0x07`) and adds BAL (`0x08`/`0x09`), so
    /// validity is not a contiguous range.
    pub const fn supports_message_id(self, id: u8) -> bool {
        match self {
            // snap/1: 0x00..=0x07 (GetAccountRange..=TrieNodes). BAL is invalid.
            Self::V1 => id <= SnapMessageId::TrieNodes as u8,
            // snap/2: 0x00..=0x05 plus BAL (0x08/0x09). TrieNodes (0x06/0x07) removed.
            Self::V2 => {
                id <= SnapMessageId::ByteCodes as u8 ||
                    id == SnapMessageId::GetBlockAccessLists as u8 ||
                    id == SnapMessageId::BlockAccessLists as u8
            }
        }
    }
}

/// Message IDs for the snap sync protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapMessageId {
    /// Requests of an unknown number of accounts from a given account trie.
    GetAccountRange = 0x00,
    /// Response with the number of consecutive accounts and the Merkle proofs for the entire
    /// range.
    AccountRange = 0x01,
    /// Requests for the storage slots of multiple accounts' storage tries.
    GetStorageRanges = 0x02,
    /// Response for the number of consecutive storage slots for the requested account.
    StorageRanges = 0x03,
    /// Request of the number of contract byte-codes by hash.
    GetByteCodes = 0x04,
    /// Response for the number of requested contract codes.
    ByteCodes = 0x05,
    /// Request of the number of state (either account or storage) Merkle trie nodes by path.
    ///
    /// Only valid for `snap/1`. Replaced by BAL-based healing in `snap/2`.
    GetTrieNodes = 0x06,
    /// Response for the number of requested state trie nodes.
    ///
    /// Only valid for `snap/1`. Replaced by BAL-based healing in `snap/2`.
    TrieNodes = 0x07,
    /// Request BALs for a list of block hashes.
    ///
    /// Only valid for `snap/2`.
    GetBlockAccessLists = 0x08,
    /// Response containing BALs for the requested block hashes.
    ///
    /// Only valid for `snap/2`.
    BlockAccessLists = 0x09,
}

impl SnapMessageId {
    /// Returns the message id of the response paired with this request, or `None` if this id is
    /// itself a response.
    pub const fn response(self) -> Option<Self> {
        match self {
            Self::GetAccountRange => Some(Self::AccountRange),
            Self::GetStorageRanges => Some(Self::StorageRanges),
            Self::GetByteCodes => Some(Self::ByteCodes),
            Self::GetTrieNodes => Some(Self::TrieNodes),
            Self::GetBlockAccessLists => Some(Self::BlockAccessLists),
            Self::AccountRange |
            Self::StorageRanges |
            Self::ByteCodes |
            Self::TrieNodes |
            Self::BlockAccessLists => None,
        }
    }
}

/// Request for a range of accounts from the state trie.
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#getaccountrange-0x00
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct GetAccountRangeMessage {
    /// Request ID to match up responses with
    pub request_id: u64,
    /// Root hash of the account trie to serve
    pub root_hash: B256,
    /// Account hash of the first to retrieve
    pub starting_hash: B256,
    /// Account hash after which to stop serving data
    pub limit_hash: B256,
    /// Soft limit at which to stop returning data
    pub response_bytes: u64,
}

/// Account data in the response.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct AccountData {
    /// Hash of the account address (trie path)
    pub hash: B256,
    /// Account body in slim format
    pub body: Bytes,
}

impl AccountData {
    /// Encodes the account fields in the shared slim format.
    pub fn from_trie_account(hash: B256, account: &TrieAccount) -> Self {
        Self { hash, body: alloy_rlp::encode(SlimAccountBody::from(account)).into() }
    }

    /// Restores the full trie account committed to by a range proof.
    pub fn trie_account(&self) -> alloy_rlp::Result<TrieAccount> {
        Ok(alloy_rlp::decode_exact::<SlimAccountBody>(&self.body)?.0)
    }
}

/// Response containing a number of consecutive accounts and the Merkle proofs for the entire range.
// http://github.com/ethereum/devp2p/blob/master/caps/snap.md#accountrange-0x01
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct AccountRangeMessage {
    /// ID of the request this is a response for
    pub request_id: u64,
    /// List of consecutive accounts from the trie
    pub accounts: Vec<AccountData>,
    /// List of trie nodes proving the account range
    pub proof: Vec<Bytes>,
}

/// Request for the storage slots of multiple accounts' storage tries.
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#getstorageranges-0x02
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct GetStorageRangesMessage {
    /// Request ID to match up responses with
    pub request_id: u64,
    /// Root hash of the account trie to serve
    pub root_hash: B256,
    /// Account hashes of the storage tries to serve
    pub account_hashes: Vec<B256>,
    /// Storage slot hash of the first to retrieve
    pub starting_hash: B256,
    /// Storage slot hash after which to stop serving
    pub limit_hash: B256,
    /// Soft limit at which to stop returning data
    pub response_bytes: u64,
}

/// Storage slot data in the response.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct StorageData {
    /// Hash of the storage slot key (trie path)
    pub hash: B256,
    /// Data content of the slot
    pub data: Bytes,
}

impl StorageData {
    /// Encodes the value exactly as the storage trie leaf commits to it.
    pub fn from_value(hash: B256, value: U256) -> Self {
        Self { hash, data: alloy_rlp::encode(value).into() }
    }

    /// Decodes the slot value, rejecting trailing bytes.
    pub fn value(&self) -> alloy_rlp::Result<U256> {
        alloy_rlp::decode_exact(&self.data)
    }
}

/// Response containing a number of consecutive storage slots for the requested account
/// and optionally the merkle proofs for the last range (boundary proofs) if it only partially
/// covers the storage trie.
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#storageranges-0x03
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct StorageRangesMessage {
    /// ID of the request this is a response for
    pub request_id: u64,
    /// List of list of consecutive slots from the trie (one list per account)
    pub slots: Vec<Vec<StorageData>>,
    /// List of trie nodes proving the slot range (if partial)
    pub proof: Vec<Bytes>,
}

/// Request to get a number of requested contract codes.
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#getbytecodes-0x04
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct GetByteCodesMessage {
    /// Request ID to match up responses with
    pub request_id: u64,
    /// Code hashes to retrieve the code for
    pub hashes: Vec<B256>,
    /// Soft limit at which to stop returning data (in bytes)
    pub response_bytes: u64,
}

/// Response containing a number of requested contract codes.
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#bytecodes-0x05
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct ByteCodesMessage {
    /// ID of the request this is a response for
    pub request_id: u64,
    /// The requested bytecodes in order
    pub codes: Vec<Bytes>,
}

/// Path in the trie for an account and its storage
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct TriePath {
    /// Path in the account trie
    pub account_path: Bytes,
    /// Paths in the storage trie
    pub slot_paths: Vec<Bytes>,
}

/// Request a number of state (either account or storage) Merkle trie nodes by path
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#gettrienodes-0x06
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct GetTrieNodesMessage {
    /// Request ID to match up responses with
    pub request_id: u64,
    /// Root hash of the account trie to serve
    pub root_hash: B256,
    /// Trie paths to retrieve the nodes for, grouped by account
    pub paths: Vec<TriePath>,
    /// Soft limit at which to stop returning data (in bytes)
    pub response_bytes: u64,
}

/// Response containing a number of requested state trie nodes
// https://github.com/ethereum/devp2p/blob/master/caps/snap.md#trienodes-0x07
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct TrieNodesMessage {
    /// ID of the request this is a response for
    pub request_id: u64,
    /// The requested trie nodes in order
    pub nodes: Vec<Bytes>,
}

/// Request BALs for the given block hashes.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct GetBlockAccessListsMessage {
    /// Request ID to match up responses with.
    pub request_id: u64,
    /// Block hashes to retrieve BALs for.
    pub block_hashes: Vec<B256>,
    /// Soft limit at which to stop returning data (in bytes).
    pub response_bytes: u64,
}

/// Response containing one BAL per requested block hash.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[add_arbitrary_tests(rlp)]
pub struct BlockAccessListsMessage {
    /// ID of the request this is a response for.
    pub request_id: u64,
    /// Raw BAL payloads in request order.
    pub block_access_lists: BlockAccessLists,
}

/// Represents all types of messages in the snap sync protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapProtocolMessage {
    /// Request for an account range - see [`GetAccountRangeMessage`]
    GetAccountRange(GetAccountRangeMessage),
    /// Response with accounts and proofs - see [`AccountRangeMessage`]
    AccountRange(AccountRangeMessage),
    /// Request for storage slots - see [`GetStorageRangesMessage`]
    GetStorageRanges(GetStorageRangesMessage),
    /// Response with storage slots - see [`StorageRangesMessage`]
    StorageRanges(StorageRangesMessage),
    /// Request for contract bytecodes - see [`GetByteCodesMessage`]
    GetByteCodes(GetByteCodesMessage),
    /// Response with contract codes - see [`ByteCodesMessage`]
    ByteCodes(ByteCodesMessage),
    /// Request for trie nodes - see [`GetTrieNodesMessage`]
    ///
    /// Only valid for `snap/1`. Replaced by BAL-based healing in `snap/2`.
    GetTrieNodes(GetTrieNodesMessage),
    /// Response with trie nodes - see [`TrieNodesMessage`]
    ///
    /// Only valid for `snap/1`. Replaced by BAL-based healing in `snap/2`.
    TrieNodes(TrieNodesMessage),
    /// Request for block access lists - see [`GetBlockAccessListsMessage`]
    ///
    /// Only valid for `snap/2`.
    GetBlockAccessLists(GetBlockAccessListsMessage),
    /// Response with block access lists - see [`BlockAccessListsMessage`]
    ///
    /// Only valid for `snap/2`.
    BlockAccessLists(BlockAccessListsMessage),
}

/// Error decoding an inbound `snap` protocol message from its framed bytes, or matching a response
/// to its request.
#[derive(thiserror::Error, Debug)]
pub enum SnapProtocolError {
    /// The payload was empty and carried no message id.
    #[error("empty snap message")]
    Empty,
    /// The message id is not valid for the negotiated snap version (e.g. the removed trie-node
    /// messages `0x06`/`0x07` under snap/2).
    #[error("message id {0:#x} is invalid for snap/{1:?}")]
    UnsupportedMessageId(u8, SnapVersion),
    /// Decoding the RLP message body failed.
    #[error("RLP error: {0}")]
    Rlp(#[from] alloy_rlp::Error),
    /// The response answers no request sent on this connection.
    ///
    /// The response is rejected before its body is decoded.
    #[error("received unsolicited {message_id:?} response with request id {request_id}")]
    UnsolicitedResponse {
        /// The message id of the response.
        message_id: SnapMessageId,
        /// The request id the response carried.
        request_id: u64,
    },
    /// A list in the response is longer than its request allows: more entries than requested, or
    /// more range proof nodes than two boundary paths can hold.
    ///
    /// The response is rejected before its body is decoded.
    #[error(
        "{message_id:?} response with request id {request_id} holds a list longer than the {max_items} items its request allows"
    )]
    ResponseExceedsRequest {
        /// The message id of the response.
        message_id: SnapMessageId,
        /// The request id the response carried.
        request_id: u64,
        /// The longest the list may be.
        max_items: usize,
    },
}

impl SnapProtocolMessage {
    /// Returns the protocol message ID for this message type.
    ///
    /// The message ID is used in the `RLPx` protocol to identify different types of messages.
    pub const fn message_id(&self) -> SnapMessageId {
        match self {
            Self::GetAccountRange(_) => SnapMessageId::GetAccountRange,
            Self::AccountRange(_) => SnapMessageId::AccountRange,
            Self::GetStorageRanges(_) => SnapMessageId::GetStorageRanges,
            Self::StorageRanges(_) => SnapMessageId::StorageRanges,
            Self::GetByteCodes(_) => SnapMessageId::GetByteCodes,
            Self::ByteCodes(_) => SnapMessageId::ByteCodes,
            Self::GetTrieNodes(_) => SnapMessageId::GetTrieNodes,
            Self::TrieNodes(_) => SnapMessageId::TrieNodes,
            Self::GetBlockAccessLists(_) => SnapMessageId::GetBlockAccessLists,
            Self::BlockAccessLists(_) => SnapMessageId::BlockAccessLists,
        }
    }

    /// Returns the `request_id` used to correlate this message with its request/response pair.
    pub const fn request_id(&self) -> u64 {
        match self {
            Self::GetAccountRange(m) => m.request_id,
            Self::AccountRange(m) => m.request_id,
            Self::GetStorageRanges(m) => m.request_id,
            Self::StorageRanges(m) => m.request_id,
            Self::GetByteCodes(m) => m.request_id,
            Self::ByteCodes(m) => m.request_id,
            Self::GetBlockAccessLists(m) => m.request_id,
            Self::BlockAccessLists(m) => m.request_id,
            Self::GetTrieNodes(m) => m.request_id,
            Self::TrieNodes(m) => m.request_id,
        }
    }

    /// Returns `true` if this is a response message (as opposed to a request).
    pub const fn is_response(&self) -> bool {
        matches!(
            self,
            Self::AccountRange(_) |
                Self::StorageRanges(_) |
                Self::ByteCodes(_) |
                Self::BlockAccessLists(_) |
                Self::TrieNodes(_)
        )
    }

    /// Overwrites the `request_id`, e.g. so a session can assign a connection-unique id before
    /// sending a request.
    pub const fn set_request_id(&mut self, request_id: u64) {
        match self {
            Self::GetAccountRange(m) => m.request_id = request_id,
            Self::AccountRange(m) => m.request_id = request_id,
            Self::GetStorageRanges(m) => m.request_id = request_id,
            Self::StorageRanges(m) => m.request_id = request_id,
            Self::GetByteCodes(m) => m.request_id = request_id,
            Self::ByteCodes(m) => m.request_id = request_id,
            Self::GetBlockAccessLists(m) => m.request_id = request_id,
            Self::BlockAccessLists(m) => m.request_id = request_id,
            Self::GetTrieNodes(m) => m.request_id = request_id,
            Self::TrieNodes(m) => m.request_id = request_id,
        }
    }

    /// Encode the message to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::new();
        // Add message ID as first byte
        buf.push(self.message_id() as u8);

        // Encode the message body based on its type
        match self {
            Self::GetAccountRange(msg) => msg.encode(&mut buf),
            Self::AccountRange(msg) => msg.encode(&mut buf),
            Self::GetStorageRanges(msg) => msg.encode(&mut buf),
            Self::StorageRanges(msg) => msg.encode(&mut buf),
            Self::GetByteCodes(msg) => msg.encode(&mut buf),
            Self::ByteCodes(msg) => msg.encode(&mut buf),
            Self::GetTrieNodes(msg) => msg.encode(&mut buf),
            Self::TrieNodes(msg) => msg.encode(&mut buf),
            Self::GetBlockAccessLists(msg) => msg.encode(&mut buf),
            Self::BlockAccessLists(msg) => msg.encode(&mut buf),
        }

        Bytes::from(buf)
    }

    /// Decodes a SNAP protocol message from its message ID and RLP-encoded body.
    pub fn decode(message_id: u8, buf: &mut &[u8]) -> Result<Self, alloy_rlp::Error> {
        // Decoding protocol message variants based on message ID
        macro_rules! decode_snap_message_variant {
            ($message_id:expr, $buf:expr, $id:expr, $variant:ident, $msg_type:ty) => {
                if $message_id == $id as u8 {
                    return Ok(Self::$variant(<$msg_type>::decode($buf)?));
                }
            };
        }

        // Try to decode each message type based on the message ID
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::GetAccountRange,
            GetAccountRange,
            GetAccountRangeMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::AccountRange,
            AccountRange,
            AccountRangeMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::GetStorageRanges,
            GetStorageRanges,
            GetStorageRangesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::StorageRanges,
            StorageRanges,
            StorageRangesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::GetByteCodes,
            GetByteCodes,
            GetByteCodesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::ByteCodes,
            ByteCodes,
            ByteCodesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::GetTrieNodes,
            GetTrieNodes,
            GetTrieNodesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::TrieNodes,
            TrieNodes,
            TrieNodesMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::GetBlockAccessLists,
            GetBlockAccessLists,
            GetBlockAccessListsMessage
        );
        decode_snap_message_variant!(
            message_id,
            buf,
            SnapMessageId::BlockAccessLists,
            BlockAccessLists,
            BlockAccessListsMessage
        );

        Err(alloy_rlp::Error::Custom("Unknown message ID"))
    }

    /// Encodes a framed message using the negotiated snap version.
    ///
    /// Legacy APIs retain byte-string account bodies. Snap/2 carries the shared slim account
    /// body as a nested list, so its adapter validates the bytes before sending them.
    pub fn encode_versioned(&self, version: SnapVersion) -> Result<Bytes, SnapProtocolError> {
        if !version.supports_message_id(self.message_id() as u8) {
            return Err(SnapProtocolError::UnsupportedMessageId(self.message_id() as u8, version));
        }
        if let (SnapVersion::V2, Self::AccountRange(message)) = (version, self) {
            let accounts = message
                .accounts
                .iter()
                .map(|account| {
                    Ok(AccountDataV2 {
                        hash: account.hash,
                        body: alloy_rlp::decode_exact::<SlimAccountBody>(&account.body)?,
                    })
                })
                .collect::<alloy_rlp::Result<Vec<_>>>()?;
            let mut bytes = Vec::new();
            bytes.push(SnapMessageId::AccountRange as u8);
            AccountRangeMessageV2 {
                request_id: message.request_id,
                accounts,
                proof: message.proof.clone(),
            }
            .encode(&mut bytes);
            Ok(bytes.into())
        } else {
            Ok(self.encode())
        }
    }

    /// Decodes a framed snap message (`[id, body..]`), validating the id against `version`.
    ///
    /// Empty payload, invalid id, and malformed body are reported as distinct
    /// [`SnapProtocolError`] variants.
    pub fn decode_versioned(version: SnapVersion, bytes: &[u8]) -> Result<Self, SnapProtocolError> {
        let (&id, mut body) = bytes.split_first().ok_or(SnapProtocolError::Empty)?;
        if !version.supports_message_id(id) {
            return Err(SnapProtocolError::UnsupportedMessageId(id, version));
        }
        let msg = if version == SnapVersion::V2 && id == SnapMessageId::AccountRange as u8 {
            let message = AccountRangeMessageV2::decode(&mut body)?;
            Self::AccountRange(AccountRangeMessage {
                request_id: message.request_id,
                accounts: message
                    .accounts
                    .into_iter()
                    .map(|account| AccountData {
                        hash: account.hash,
                        body: alloy_rlp::encode(account.body).into(),
                    })
                    .collect(),
                proof: message.proof,
            })
        } else {
            Self::decode(id, &mut body)?
        };
        if !body.is_empty() {
            return Err(SnapProtocolError::Rlp(alloy_rlp::Error::UnexpectedLength));
        }
        Ok(msg)
    }
}

/// A trie account encoded with default storage and code hashes replaced by empty byte strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlimAccountBody(TrieAccount);

impl SlimAccountBody {
    fn as_rlp(&self) -> SlimAccountBodyRef<'_> {
        SlimAccountBodyRef {
            nonce: self.0.nonce,
            balance: self.0.balance,
            storage_root: SlimAccountBodyRef::shorten(&self.0.storage_root, EMPTY_ROOT_HASH),
            code_hash: SlimAccountBodyRef::shorten(&self.0.code_hash, KECCAK256_EMPTY),
        }
    }

    /// Restores a dropped field to `empty`, rejecting any length the encoding never produces.
    fn restore(value: &[u8], empty: B256) -> alloy_rlp::Result<B256> {
        match value {
            [] => Ok(empty),
            _ => B256::try_from(value).map_err(|_| alloy_rlp::Error::UnexpectedLength),
        }
    }
}

impl From<&TrieAccount> for SlimAccountBody {
    fn from(account: &TrieAccount) -> Self {
        Self(*account)
    }
}

impl Encodable for SlimAccountBody {
    fn encode(&self, out: &mut dyn BufMut) {
        self.as_rlp().encode(out);
    }

    fn length(&self) -> usize {
        self.as_rlp().length()
    }
}

impl Decodable for SlimAccountBody {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let nonce = u64::decode(&mut payload)?;
        let balance = U256::decode(&mut payload)?;
        let storage_root =
            Self::restore(Header::decode_bytes(&mut payload, false)?, EMPTY_ROOT_HASH)?;
        let code_hash = Self::restore(Header::decode_bytes(&mut payload, false)?, KECCAK256_EMPTY)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength)
        }
        Ok(Self(TrieAccount { nonce, balance, storage_root, code_hash }))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for SlimAccountBody {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let storage_root = if u.arbitrary()? { u.arbitrary()? } else { EMPTY_ROOT_HASH };
        let code_hash = if u.arbitrary()? { u.arbitrary()? } else { KECCAK256_EMPTY };
        Ok(Self(TrieAccount {
            nonce: u.arbitrary()?,
            balance: u.arbitrary()?,
            storage_root,
            code_hash,
        }))
    }
}

/// Borrowed encode twin of [`SlimAccountBody`].
#[derive(RlpEncodable)]
struct SlimAccountBodyRef<'a> {
    /// The account's nonce.
    nonce: u64,
    /// The account's balance.
    balance: U256,
    /// Empty when the account has no storage.
    storage_root: &'a [u8],
    /// Empty when the account has no code.
    code_hash: &'a [u8],
}

impl<'a> SlimAccountBodyRef<'a> {
    /// Drops a field that holds its empty default, which is what makes the encoding slim.
    fn shorten(value: &'a B256, empty: B256) -> &'a [u8] {
        if *value == empty {
            &[]
        } else {
            value.as_slice()
        }
    }
}

// Version-specific adapters keep snap/1's public byte-string body codec unchanged.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
struct AccountDataV2 {
    hash: B256,
    body: SlimAccountBody,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
struct AccountRangeMessageV2 {
    request_id: u64,
    accounts: Vec<AccountDataV2>,
    proof: Vec<Bytes>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // Helper function to create a B256 from a u64 for testing
    fn b256_from_u64(value: u64) -> B256 {
        B256::left_padding_from(&value.to_be_bytes())
    }

    // Helper function to test roundtrip encoding/decoding
    fn test_roundtrip(original: SnapProtocolMessage) {
        let encoded = original.encode();

        // Verify the first byte matches the expected message ID
        assert_eq!(encoded[0], original.message_id() as u8);

        let mut buf = &encoded[1..];
        let decoded = SnapProtocolMessage::decode(encoded[0], &mut buf).unwrap();

        // Verify the match
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_all_message_roundtrips() {
        assert_eq!(SnapVersion::V1.message_count(), 8);
        assert_eq!(SnapVersion::V2.message_count(), 10);

        test_roundtrip(SnapProtocolMessage::GetAccountRange(GetAccountRangeMessage {
            request_id: 42,
            root_hash: b256_from_u64(123),
            starting_hash: b256_from_u64(456),
            limit_hash: b256_from_u64(789),
            response_bytes: 1024,
        }));

        test_roundtrip(SnapProtocolMessage::AccountRange(AccountRangeMessage {
            request_id: 42,
            accounts: vec![AccountData {
                hash: b256_from_u64(123),
                body: Bytes::from(vec![1, 2, 3]),
            }],
            proof: vec![Bytes::from(vec![4, 5, 6])],
        }));

        test_roundtrip(SnapProtocolMessage::GetStorageRanges(GetStorageRangesMessage {
            request_id: 42,
            root_hash: b256_from_u64(123),
            account_hashes: vec![b256_from_u64(456)],
            starting_hash: b256_from_u64(789),
            limit_hash: b256_from_u64(101112),
            response_bytes: 2048,
        }));

        test_roundtrip(SnapProtocolMessage::StorageRanges(StorageRangesMessage {
            request_id: 42,
            slots: vec![vec![StorageData {
                hash: b256_from_u64(123),
                data: Bytes::from(vec![1, 2, 3]),
            }]],
            proof: vec![Bytes::from(vec![4, 5, 6])],
        }));

        test_roundtrip(SnapProtocolMessage::GetByteCodes(GetByteCodesMessage {
            request_id: 42,
            hashes: vec![b256_from_u64(123)],
            response_bytes: 1024,
        }));

        test_roundtrip(SnapProtocolMessage::ByteCodes(ByteCodesMessage {
            request_id: 42,
            codes: vec![Bytes::from(vec![1, 2, 3])],
        }));

        test_roundtrip(SnapProtocolMessage::GetTrieNodes(GetTrieNodesMessage {
            request_id: 42,
            root_hash: b256_from_u64(123),
            paths: vec![TriePath {
                account_path: Bytes::from(vec![1, 2, 3]),
                slot_paths: vec![Bytes::from(vec![4, 5, 6])],
            }],
            response_bytes: 1024,
        }));

        test_roundtrip(SnapProtocolMessage::TrieNodes(TrieNodesMessage {
            request_id: 42,
            nodes: vec![Bytes::from(vec![1, 2, 3])],
        }));

        test_roundtrip(SnapProtocolMessage::GetBlockAccessLists(GetBlockAccessListsMessage {
            request_id: 42,
            block_hashes: vec![b256_from_u64(123), b256_from_u64(456)],
            response_bytes: 4096,
        }));

        test_roundtrip(SnapProtocolMessage::BlockAccessLists(BlockAccessListsMessage {
            request_id: 42,
            block_access_lists: BlockAccessLists(vec![
                Some(Bytes::from_static(&[alloy_rlp::EMPTY_LIST_CODE])),
                Some(Bytes::from_static(&[0xc1, alloy_rlp::EMPTY_LIST_CODE])),
            ]),
        }));
    }

    #[test]
    fn test_unknown_message_id() {
        // Create some random data
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let mut buf = data.as_ref();

        // Try to decode with an invalid message ID
        let result = SnapProtocolMessage::decode(255, &mut buf);

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.to_string(), "Unknown message ID");
        }
    }

    #[test]
    fn test_snap_v1_message_validity() {
        let v1 = SnapVersion::V1;
        // 0x00..=0x07 valid, including trie nodes.
        for id in 0x00..=0x07 {
            assert!(v1.supports_message_id(id), "snap/1 should accept {id:#x}");
        }
        // BAL is snap/2 only.
        assert!(!v1.supports_message_id(SnapMessageId::GetBlockAccessLists as u8));
        assert!(!v1.supports_message_id(SnapMessageId::BlockAccessLists as u8));
        assert!(!v1.supports_message_id(0x0a));
        assert!(!v1.supports_message_id(0xff));
    }

    #[test]
    fn test_snap_v2_message_validity() {
        let v2 = SnapVersion::V2;
        // 0x00..=0x05 valid.
        for id in 0x00..=0x05 {
            assert!(v2.supports_message_id(id), "snap/2 should accept {id:#x}");
        }
        // Trie nodes removed in snap/2.
        assert!(!v2.supports_message_id(SnapMessageId::GetTrieNodes as u8));
        assert!(!v2.supports_message_id(SnapMessageId::TrieNodes as u8));
        // BAL added in snap/2.
        assert!(v2.supports_message_id(SnapMessageId::GetBlockAccessLists as u8));
        assert!(v2.supports_message_id(SnapMessageId::BlockAccessLists as u8));
        assert!(!v2.supports_message_id(0x0a));
        assert!(!v2.supports_message_id(0xff));
    }

    #[test_case(
        SnapProtocolMessage::GetAccountRange(GetAccountRangeMessage {
            request_id: 1, root_hash: B256::ZERO, starting_hash: B256::ZERO,
            limit_hash: B256::ZERO, response_bytes: 0,
        }), 1, false ; "get_account_range is a request"
    )]
    #[test_case(
        SnapProtocolMessage::AccountRange(AccountRangeMessage {
            request_id: 2, accounts: vec![], proof: vec![],
        }), 2, true ; "account_range is a response"
    )]
    #[test_case(
        SnapProtocolMessage::GetStorageRanges(GetStorageRangesMessage {
            request_id: 3, root_hash: B256::ZERO, account_hashes: vec![],
            starting_hash: B256::ZERO, limit_hash: B256::ZERO, response_bytes: 0,
        }), 3, false ; "get_storage_ranges is a request"
    )]
    #[test_case(
        SnapProtocolMessage::StorageRanges(StorageRangesMessage {
            request_id: 4, slots: vec![], proof: vec![],
        }), 4, true ; "storage_ranges is a response"
    )]
    #[test_case(
        SnapProtocolMessage::GetByteCodes(GetByteCodesMessage {
            request_id: 5, hashes: vec![], response_bytes: 0,
        }), 5, false ; "get_byte_codes is a request"
    )]
    #[test_case(
        SnapProtocolMessage::ByteCodes(ByteCodesMessage { request_id: 6, codes: vec![] }),
        6, true ; "byte_codes is a response"
    )]
    #[test_case(
        SnapProtocolMessage::GetBlockAccessLists(GetBlockAccessListsMessage {
            request_id: 7, block_hashes: vec![], response_bytes: 0,
        }), 7, false ; "get_block_access_lists is a request"
    )]
    #[test_case(
        SnapProtocolMessage::BlockAccessLists(BlockAccessListsMessage {
            request_id: 8, block_access_lists: BlockAccessLists(vec![]),
        }), 8, true ; "block_access_lists is a response"
    )]
    fn request_id_and_is_response(msg: SnapProtocolMessage, expected_id: u64, is_response: bool) {
        assert_eq!(msg.request_id(), expected_id);
        assert_eq!(msg.is_response(), is_response);
    }

    #[test_case(
        SnapProtocolMessage::GetAccountRange(GetAccountRangeMessage {
            request_id: 1, root_hash: B256::ZERO, starting_hash: B256::ZERO,
            limit_hash: B256::ZERO, response_bytes: 0,
        }) ; "get_account_range"
    )]
    #[test_case(
        SnapProtocolMessage::AccountRange(AccountRangeMessage {
            request_id: 1, accounts: vec![], proof: vec![],
        }) ; "account_range"
    )]
    #[test_case(
        SnapProtocolMessage::GetStorageRanges(GetStorageRangesMessage {
            request_id: 1, root_hash: B256::ZERO, account_hashes: vec![],
            starting_hash: B256::ZERO, limit_hash: B256::ZERO, response_bytes: 0,
        }) ; "get_storage_ranges"
    )]
    #[test_case(
        SnapProtocolMessage::StorageRanges(StorageRangesMessage {
            request_id: 1, slots: vec![], proof: vec![],
        }) ; "storage_ranges"
    )]
    #[test_case(
        SnapProtocolMessage::GetByteCodes(GetByteCodesMessage {
            request_id: 1, hashes: vec![], response_bytes: 0,
        }) ; "get_byte_codes"
    )]
    #[test_case(
        SnapProtocolMessage::ByteCodes(ByteCodesMessage { request_id: 1, codes: vec![] }) ;
        "byte_codes"
    )]
    #[test_case(
        SnapProtocolMessage::GetBlockAccessLists(GetBlockAccessListsMessage {
            request_id: 1, block_hashes: vec![], response_bytes: 0,
        }) ; "get_block_access_lists"
    )]
    #[test_case(
        SnapProtocolMessage::BlockAccessLists(BlockAccessListsMessage {
            request_id: 1, block_access_lists: BlockAccessLists(vec![]),
        }) ; "block_access_lists"
    )]
    fn per_variant_request_id_and_round_trip(mut msg: SnapProtocolMessage) {
        // set_request_id overwrites the id for every variant.
        msg.set_request_id(42);
        assert_eq!(msg.request_id(), 42);

        // decode_versioned round-trips every valid snap/2 id.
        let decoded =
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &msg.encode()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_versioned_rejects_empty() {
        // An empty payload carries no message id and is distinct from an invalid id.
        assert!(matches!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &[]),
            Err(SnapProtocolError::Empty)
        ));
    }

    #[test]
    fn decode_versioned_rejects_trie_node_ids_in_v2() {
        // snap/2 (EIP-8189) removes trie nodes (`0x06`/`0x07`); decoding must reject them as an
        // unsupported id rather than a malformed body.
        for id in [0x06u8, 0x07] {
            assert!(matches!(
                SnapProtocolMessage::decode_versioned(SnapVersion::V2, &[id]),
                Err(SnapProtocolError::UnsupportedMessageId(got, SnapVersion::V2)) if got == id
            ));
        }
    }

    #[test]
    fn decode_versioned_reports_malformed_body() {
        // A valid id (GetBlockAccessLists, 0x08) with a non-decodable body is an RLP error, not an
        // unsupported id.
        assert!(matches!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &[0x08, 0xff]),
            Err(SnapProtocolError::Rlp(_))
        ));
    }

    #[test]
    fn decode_versioned_rejects_trailing_bytes() {
        // A valid framed message with junk appended after the RLP body must be rejected rather
        // than silently decoded.
        let original = SnapProtocolMessage::GetBlockAccessLists(GetBlockAccessListsMessage {
            request_id: 7,
            block_hashes: vec![b256_from_u64(1)],
            response_bytes: 1024,
        });
        let mut framed = original.encode().to_vec();
        framed.push(0xff);
        assert!(matches!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &framed),
            Err(SnapProtocolError::Rlp(alloy_rlp::Error::UnexpectedLength))
        ));
    }
    #[test]
    fn legacy_trie_node_helpers_preserve_snap_v1() {
        let mut request = SnapProtocolMessage::GetTrieNodes(GetTrieNodesMessage {
            request_id: 1,
            root_hash: B256::ZERO,
            paths: Vec::new(),
            response_bytes: 1024,
        });
        request.set_request_id(7);
        assert_eq!(request.request_id(), 7);
        assert!(!request.is_response());
        assert_eq!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V1, &request.encode()).unwrap(),
            request
        );
        assert!(matches!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &request.encode()),
            Err(SnapProtocolError::UnsupportedMessageId(_, SnapVersion::V2))
        ));
        let mut response =
            SnapProtocolMessage::TrieNodes(TrieNodesMessage { request_id: 1, nodes: Vec::new() });
        response.set_request_id(7);
        assert_eq!(response.request_id(), 7);
        assert!(response.is_response());
        assert_eq!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V1, &response.encode()).unwrap(),
            response
        );
    }
    impl AccountDataV2 {
        fn from_trie_account(hash: B256, account: &TrieAccount) -> Self {
            Self { hash, body: account.into() }
        }

        fn trie_account(&self) -> TrieAccount {
            self.body.0
        }

        fn into_trie_entry(self) -> (B256, TrieAccount) {
            (self.hash, self.body.0)
        }
    }

    fn trie_account(storage_root: B256, code_hash: B256) -> TrieAccount {
        TrieAccount { nonce: 7, balance: U256::from(42), storage_root, code_hash }
    }

    #[test]
    fn account_range_matches_nested_account_wire_encoding() {
        // [request_id, [[hash, [nonce, balance, storage_root, code_hash]]], proof]
        let wire = alloy_primitives::hex!(
            "ea01e7e6a00101010101010101010101010101010101010101010101010101010101010101c4072a8080c0"
        );
        let message = AccountRangeMessageV2 {
            request_id: 1,
            accounts: vec![AccountDataV2::from_trie_account(
                B256::repeat_byte(1),
                &trie_account(EMPTY_ROOT_HASH, KECCAK256_EMPTY),
            )],
            proof: vec![],
        };

        assert_eq!(alloy_rlp::encode(&message), wire);
        assert_eq!(alloy_rlp::decode_exact::<AccountRangeMessageV2>(&wire).unwrap(), message);
    }

    #[test]
    fn account_data_rejects_byte_string_wrapped_body() {
        let wire = alloy_primitives::hex!(
            "e7a0010101010101010101010101010101010101010101010101010101010101010185c4072a8080"
        );
        assert!(alloy_rlp::decode_exact::<AccountDataV2>(&wire).is_err());
    }

    #[test]
    fn account_data_consumes_only_its_own_list() {
        let account = AccountDataV2::from_trie_account(
            B256::repeat_byte(1),
            &trie_account(B256::repeat_byte(2), B256::repeat_byte(3)),
        );
        let mut wire = alloy_rlp::encode(&account);
        assert_eq!(wire.len(), account.length());
        wire.push(0x80);
        let mut input = wire.as_slice();
        assert_eq!(AccountDataV2::decode(&mut input).unwrap(), account);
        assert_eq!(input, &[0x80]);
    }

    #[test]
    fn account_data_rejects_missing_truncated_and_extra_body_items() {
        for wire in [
            "e1a00101010101010101010101010101010101010101010101010101010101010101",
            "e4a00101010101010101010101010101010101010101010101010101010101010101c4072a",
            "e7a00101010101010101010101010101010101010101010101010101010101010101c4072a808080",
        ] {
            let bytes = alloy_primitives::hex::decode(wire).unwrap();
            assert!(alloy_rlp::decode_exact::<AccountDataV2>(&bytes).is_err());
        }
    }

    #[test]
    fn slim_body_elides_empty_storage_and_code() {
        let account = trie_account(EMPTY_ROOT_HASH, KECCAK256_EMPTY);
        let hash = B256::repeat_byte(1);
        let encoded = AccountDataV2::from_trie_account(hash, &account);

        let body = alloy_rlp::encode(encoded.body);
        assert_eq!(body, alloy_primitives::hex!("c4072a8080"));
        assert_eq!(alloy_rlp::decode_exact::<SlimAccountBody>(&body).unwrap(), encoded.body);
        assert_eq!(encoded.trie_account(), account);
        assert_eq!(encoded.into_trie_entry(), (hash, account));
    }

    #[test]
    fn slim_body_keeps_non_default_storage_and_code() {
        let account = trie_account(B256::repeat_byte(2), B256::repeat_byte(3));
        let encoded = AccountDataV2::from_trie_account(B256::repeat_byte(1), &account);

        let body = alloy_rlp::encode(encoded.body);
        assert_eq!(body, alloy_rlp::encode(account));
        assert_eq!(alloy_rlp::decode_exact::<SlimAccountBody>(&body).unwrap(), encoded.body);
        assert_eq!(encoded.trie_account(), account);
    }

    #[test_case(1)]
    #[test_case(16)]
    #[test_case(31)]
    #[test_case(33)]
    fn slim_body_rejects_invalid_hash_lengths(len: usize) {
        let invalid = vec![0xaa; len];
        for (storage_root, code_hash) in
            [(invalid.as_slice(), &[][..]), (&[][..], invalid.as_slice())]
        {
            let body = alloy_rlp::encode(SlimAccountBodyRef {
                nonce: 7,
                balance: U256::from(42),
                storage_root,
                code_hash,
            });
            assert_eq!(
                alloy_rlp::decode_exact::<SlimAccountBody>(&body),
                Err(alloy_rlp::Error::UnexpectedLength)
            );
        }
    }

    #[test_case("c0")]
    #[test_case("c3072a80")]
    #[test_case("c5072a808080")]
    #[test_case("c4072a808000")]
    fn slim_body_rejects_missing_and_extra_fields(wire: &str) {
        let bytes = alloy_primitives::hex::decode(wire).unwrap();
        assert!(alloy_rlp::decode_exact::<SlimAccountBody>(&bytes).is_err());
    }

    #[test]
    fn storage_data_carries_the_trie_leaf_encoding() {
        let value = U256::from(1234);
        let slot = StorageData::from_value(B256::repeat_byte(4), value);

        // Clients verify range proofs against the RLP-encoded trie leaf, so the wire bytes must be
        // exactly that rather than a fixed-width word.
        assert_eq!(slot.data.as_ref(), alloy_rlp::encode(value));
        assert_eq!(slot.value().unwrap(), value);
    }

    #[test]
    fn storage_data_rejects_trailing_bytes() {
        let mut slot = StorageData::from_value(B256::repeat_byte(4), U256::from(1));
        slot.data = [slot.data.as_ref(), &[0x00]].concat().into();

        assert!(slot.value().is_err());
    }
    #[test]
    fn versioned_account_range_matches_final_v2_and_preserves_legacy_v1() {
        let message = SnapProtocolMessage::AccountRange(AccountRangeMessage {
            request_id: 1,
            accounts: vec![AccountData::from_trie_account(
                B256::repeat_byte(1),
                &trie_account(EMPTY_ROOT_HASH, KECCAK256_EMPTY),
            )],
            proof: vec![],
        });
        let v2 = alloy_primitives::hex!(
            "01ea01e7e6a00101010101010101010101010101010101010101010101010101010101010101c4072a8080c0"
        );
        let legacy = alloy_primitives::hex!(
            "01eb01e8e7a0010101010101010101010101010101010101010101010101010101010101010185c4072a8080c0"
        );
        assert_eq!(message.encode_versioned(SnapVersion::V2).unwrap().as_ref(), v2);
        assert_eq!(SnapProtocolMessage::decode_versioned(SnapVersion::V2, &v2).unwrap(), message);
        assert_eq!(message.encode_versioned(SnapVersion::V1).unwrap().as_ref(), legacy);
        assert_eq!(message.encode().as_ref(), legacy);
        assert_eq!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V1, &legacy).unwrap(),
            message
        );
        assert!(SnapProtocolMessage::decode_versioned(SnapVersion::V2, &legacy).is_err());
        let SnapProtocolMessage::AccountRange(decoded) =
            SnapProtocolMessage::decode_versioned(SnapVersion::V2, &v2).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            decoded.accounts[0].trie_account().unwrap(),
            trie_account(EMPTY_ROOT_HASH, KECCAK256_EMPTY)
        );
    }

    #[test]
    fn v2_account_adapter_rejects_invalid_legacy_body_bytes() {
        let message = SnapProtocolMessage::AccountRange(AccountRangeMessage {
            request_id: 1,
            accounts: vec![AccountData { hash: B256::ZERO, body: Bytes::from_static(&[0xc0]) }],
            proof: vec![],
        });
        assert!(message.encode_versioned(SnapVersion::V2).is_err());
        // Old snap/1 opaque bodies retain their public encoding/decoding behavior.
        assert_eq!(
            SnapProtocolMessage::decode_versioned(SnapVersion::V1, &message.encode()).unwrap(),
            message
        );
    }
}
