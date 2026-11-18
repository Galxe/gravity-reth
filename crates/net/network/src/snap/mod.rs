//! Standalone SNAP/2 response generation for an explicitly injected backend.
//!
//! This module does not register protocols, advertise serving capability, or connect a backend to
//! a running node. Callers must run potentially expensive range and proof reads off async tasks.

use crate::eth_requests::{MAX_BLOCK_ACCESS_LISTS_SERVE, SOFT_RESPONSE_LIMIT};
use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Bytes, B256};
use alloy_trie::TrieAccount;
use reth_eth_wire_types::{
    snap::{
        AccountData, AccountRangeMessage, BlockAccessListsMessage, ByteCodesMessage,
        GetAccountRangeMessage, GetStorageRangesMessage, SnapProtocolMessage, StorageData,
        StorageRangesMessage,
    },
    BlockAccessLists,
};
use reth_network_p2p::snap::client::SnapResponse;
use reth_storage_api::{
    errors::provider::ProviderError, BalProvider, BytecodeReader, GetBlockAccessListLimit,
    RangeEnd, RangeResponse, StateRangeProviderFactory,
};
use tokio::sync::oneshot;

/// A read-only SNAP/2 responder backed by real, caller-supplied state and BAL readers.
///
/// It is independent of the network session and manager. The provider must resolve each retained
/// root to one pinned state range view; no default provider or empty fallback is installed here.
#[derive(Debug)]
pub struct SnapRequestHandler<C> {
    provider: C,
}

impl<C> SnapRequestHandler<C> {
    /// Creates a responder with the given backend.
    pub const fn new(provider: C) -> Self {
        Self { provider }
    }

    /// Returns the injected backend.
    pub const fn provider(&self) -> &C {
        &self.provider
    }
}

impl<C> SnapRequestHandler<C>
where
    C: BalProvider + BytecodeReader + StateRangeProviderFactory,
{
    /// Answers a native SNAP/2 request without starting a network or writing state.
    ///
    /// Provider failures and unavailable roots are explicit errors. In particular they are not
    /// confused with a legitimately exhausted empty range. Response-shaped messages and legacy
    /// trie-node requests are rejected. Limits are soft and include the item crossing the budget.
    pub fn serve(&self, request: SnapProtocolMessage) -> Result<SnapResponse, SnapRequestError> {
        match request {
            SnapProtocolMessage::GetAccountRange(request) => {
                self.account_range(request).map(SnapResponse::AccountRange)
            }
            SnapProtocolMessage::GetStorageRanges(request) => {
                self.storage_ranges(request).map(SnapResponse::StorageRanges)
            }
            SnapProtocolMessage::GetByteCodes(request) => {
                let codes =
                    self.byte_codes(&request.hashes, response_budget(request.response_bytes))?;
                Ok(SnapResponse::ByteCodes(ByteCodesMessage {
                    request_id: request.request_id,
                    codes,
                }))
            }
            SnapProtocolMessage::GetBlockAccessLists(mut request) => {
                request.block_hashes.truncate(MAX_BLOCK_ACCESS_LISTS_SERVE);
                let block_access_lists = self.provider.bal_store().get_by_hashes_with_limit(
                    &request.block_hashes,
                    GetBlockAccessListLimit::ResponseSizeSoftLimit(response_budget(
                        request.response_bytes,
                    )),
                )?;
                Ok(SnapResponse::BlockAccessLists(BlockAccessListsMessage {
                    request_id: request.request_id,
                    block_access_lists: BlockAccessLists(block_access_lists),
                }))
            }
            _ => Err(SnapRequestError::UnsupportedRequest),
        }
    }

    /// Routes a request result to its consumer. Dropping the consumer discards the result.
    pub fn on_request(
        &self,
        request: SnapProtocolMessage,
        response: oneshot::Sender<Result<SnapResponse, SnapRequestError>>,
    ) {
        let _ = response.send(self.serve(request));
    }

    fn byte_codes(
        &self,
        hashes: &[B256],
        response_bytes: usize,
    ) -> Result<Vec<Bytes>, SnapRequestError> {
        let mut codes = Vec::new();
        let mut total_bytes = 0usize;
        for hash in hashes.iter().take(MAX_BYTE_CODES_SERVE) {
            let bytes = if *hash == KECCAK_EMPTY {
                Bytes::new()
            } else {
                match self.provider.bytecode_by_hash(hash)? {
                    Some(code) => code.original_bytes(),
                    None => continue,
                }
            };
            total_bytes = total_bytes.saturating_add(bytes.len());
            codes.push(bytes);
            if total_bytes > response_bytes {
                break
            }
        }
        Ok(codes)
    }

    fn account_range(
        &self,
        request: GetAccountRangeMessage,
    ) -> Result<AccountRangeMessage, SnapRequestError> {
        let state = self
            .provider
            .state_range_provider(request.root_hash)?
            .ok_or(SnapRequestError::StateRootUnavailable(request.root_hash))?;
        let RangeResponse { items: accounts, .. } = state.account_range(
            request.starting_hash,
            request.limit_hash,
            response_budget(request.response_bytes),
        )?;
        let keys = boundary_proof_keys(request.starting_hash, accounts.last());
        let mut data = Vec::with_capacity(accounts.len());
        for (hash, account) in accounts {
            let storage_root = state.storage_root_by_hash(hash)?;
            data.push(AccountData::from_trie_account(
                hash,
                &TrieAccount {
                    nonce: account.nonce,
                    balance: account.balance,
                    storage_root,
                    code_hash: account.bytecode_hash.unwrap_or(KECCAK_EMPTY),
                },
            ));
        }
        let proof = state.account_range_proof(&keys)?;
        Ok(AccountRangeMessage { request_id: request.request_id, accounts: data, proof })
    }

    fn storage_ranges(
        &self,
        request: GetStorageRangesMessage,
    ) -> Result<StorageRangesMessage, SnapRequestError> {
        let state = self
            .provider
            .state_range_provider(request.root_hash)?
            .ok_or(SnapRequestError::StateRootUnavailable(request.root_hash))?;
        let mut slots = Vec::new();
        let mut proof = Vec::new();
        let mut remaining_bytes = response_budget(request.response_bytes);
        for (index, &address) in
            request.account_hashes.iter().take(MAX_STORAGE_RANGE_ACCOUNTS_SERVE).enumerate()
        {
            if remaining_bytes == 0 {
                break
            }
            let origin = if index == 0 { request.starting_hash } else { B256::ZERO };
            let limit = if index == 0 { request.limit_hash } else { B256::repeat_byte(0xff) };
            let RangeResponse { items, end } = state
                .storage_range(address, origin, limit, remaining_bytes)?
                .ok_or(SnapRequestError::AccountUnavailable(address))?;
            remaining_bytes = remaining_bytes.saturating_sub(items.len().saturating_mul(64));
            let keys = boundary_proof_keys(origin, items.last());
            let needs_proof = origin != B256::ZERO || end != RangeEnd::Exhausted;
            slots.push(
                items
                    .into_iter()
                    .map(|(hash, value)| StorageData {
                        hash,
                        data: alloy_rlp::encode(value).into(),
                    })
                    .collect(),
            );
            if needs_proof {
                proof = state.storage_range_proof(address, &keys)?;
                break
            }
        }
        Ok(StorageRangesMessage { request_id: request.request_id, slots, proof })
    }
}

/// Errors kept distinct from a successful empty SNAP range.
#[derive(Debug, thiserror::Error)]
pub enum SnapRequestError {
    /// A response or a legacy trie-node message was submitted to the native responder.
    #[error("unsupported SNAP/2 request")]
    UnsupportedRequest,
    /// The requested root is not retained or verified by the backend.
    #[error("SNAP state root {0} is unavailable")]
    StateRootUnavailable(B256),
    /// An account is absent at the requested state root.
    #[error("SNAP storage account {0} is unavailable")]
    AccountUnavailable(B256),
    /// A backend read failed; no partial response is returned.
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

/// Maximum bytecode lookups per request.
pub const MAX_BYTE_CODES_SERVE: usize = 1024;
/// Maximum storage-account lookups per request.
pub const MAX_STORAGE_RANGE_ACCOUNTS_SERVE: usize = 1024;

fn response_budget(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX).min(SOFT_RESPONSE_LIMIT)
}

fn boundary_proof_keys<T>(origin: B256, last: Option<&(B256, T)>) -> Vec<B256> {
    match last {
        Some((last, _)) => vec![origin, *last],
        None => vec![origin],
    }
}

#[cfg(test)]
mod tests;
