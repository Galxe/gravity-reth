use super::*;
use alloy_consensus::constants::EMPTY_ROOT_HASH;
use alloy_eips::NumHash;
use alloy_primitives::{keccak256, Sealed, U256};
use alloy_trie::{
    nodes::RlpNode,
    proof::{verify_proof, ProofRetainer},
    HashBuilder, Nibbles,
};
use reth_eth_wire_types::snap::{GetBlockAccessListsMessage, GetByteCodesMessage, SnapVersion};
use reth_primitives_traits::{Account, Bytecode};
use reth_provider::{BalConfig, InMemoryBalStore};
use reth_storage_api::{
    BalStoreHandle, RangeResult, StateRangeProvider, StateRangeView, StorageRangeResult,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

/// A real immutable fixture state. Range reads, roots and proofs share these exact leaves.
#[derive(Debug, Default)]
pub(super) struct FixtureState {
    accounts: BTreeMap<B256, Account>,
    storage: BTreeMap<B256, BTreeMap<B256, U256>>,
}

impl FixtureState {
    fn storage_leaves(&self, address: B256) -> Vec<(B256, Vec<u8>)> {
        self.storage
            .get(&address)
            .into_iter()
            .flat_map(|slots| slots.iter())
            .map(|(key, value)| (*key, alloy_rlp::encode(value)))
            .collect()
    }

    fn storage_root(&self, address: B256) -> B256 {
        trie(&self.storage_leaves(address), &[]).0
    }

    fn account_leaves(&self) -> Vec<(B256, Vec<u8>)> {
        self.accounts
            .iter()
            .map(|(hash, account)| {
                (
                    *hash,
                    alloy_rlp::encode(TrieAccount {
                        nonce: account.nonce,
                        balance: account.balance,
                        storage_root: self.storage_root(*hash),
                        code_hash: account.bytecode_hash.unwrap_or(KECCAK_EMPTY),
                    }),
                )
            })
            .collect()
    }
}

type StorageRead = (B256, B256, B256, usize);

#[derive(Debug)]
pub(super) struct Fixture {
    state: Arc<FixtureState>,
    root: B256,
    codes: BTreeMap<B256, Bytecode>,
    bals: BalStoreHandle,
    pub(super) resolutions: AtomicUsize,
    code_reads: AtomicUsize,
    storage_reads: Arc<Mutex<Vec<StorageRead>>>,
    fail_reads: bool,
    fail_proof: bool,
    fail_storage_root: bool,
}

impl Fixture {
    pub(super) fn new(state: FixtureState) -> Self {
        let root = trie(&state.account_leaves(), &[]).0;
        Self {
            state: Arc::new(state),
            root,
            codes: Default::default(),
            bals: BalStoreHandle::new(InMemoryBalStore::new(BalConfig::unbounded())),
            resolutions: Default::default(),
            code_reads: Default::default(),
            storage_reads: Default::default(),
            fail_reads: false,
            fail_proof: false,
            fail_storage_root: false,
        }
    }

    pub(super) fn account_request(&self) -> GetAccountRangeMessage {
        GetAccountRangeMessage {
            request_id: 1,
            root_hash: self.root,
            starting_hash: B256::ZERO,
            limit_hash: B256::repeat_byte(0xff),
            response_bytes: SOFT_RESPONSE_LIMIT as u64,
        }
    }

    fn storage_request(&self, account_hashes: Vec<B256>) -> GetStorageRangesMessage {
        GetStorageRangesMessage {
            request_id: 2,
            root_hash: self.root,
            account_hashes,
            starting_hash: B256::ZERO,
            limit_hash: B256::repeat_byte(0xff),
            response_bytes: SOFT_RESPONSE_LIMIT as u64,
        }
    }
}

impl BalProvider for Fixture {
    fn bal_store(&self) -> &BalStoreHandle {
        &self.bals
    }
}

impl BytecodeReader for Fixture {
    fn bytecode_by_hash(&self, hash: &B256) -> Result<Option<Bytecode>, ProviderError> {
        self.code_reads.fetch_add(1, Ordering::Relaxed);
        if self.fail_reads {
            return Err(read_error())
        }
        Ok(self.codes.get(hash).cloned())
    }
}

impl StateRangeProviderFactory for Fixture {
    fn state_range_provider(&self, root: B256) -> Result<Option<StateRangeView>, ProviderError> {
        self.resolutions.fetch_add(1, Ordering::Relaxed);
        if self.fail_reads {
            return Err(read_error())
        }
        Ok((root == self.root).then(|| {
            Box::new(PinnedFixture {
                state: self.state.clone(),
                storage_reads: self.storage_reads.clone(),
                fail_proof: self.fail_proof,
                fail_storage_root: self.fail_storage_root,
            }) as StateRangeView
        }))
    }
}

struct PinnedFixture {
    state: Arc<FixtureState>,
    storage_reads: Arc<Mutex<Vec<StorageRead>>>,
    fail_proof: bool,
    fail_storage_root: bool,
}

impl StateRangeProvider for PinnedFixture {
    fn account_range(
        &self,
        start: B256,
        limit: B256,
        bytes: usize,
    ) -> RangeResult<(B256, Account)> {
        Ok(range(
            self.state.accounts.range(start..).map(|(key, value)| (*key, *value)),
            limit,
            bytes,
            128,
        ))
    }

    fn storage_root_by_hash(&self, address: B256) -> Result<B256, ProviderError> {
        if self.fail_storage_root || !self.state.accounts.contains_key(&address) {
            return Err(read_error())
        }
        Ok(self.state.storage_root(address))
    }

    fn storage_range(
        &self,
        address: B256,
        start: B256,
        limit: B256,
        bytes: usize,
    ) -> StorageRangeResult {
        self.storage_reads.lock().unwrap().push((address, start, limit, bytes));
        if !self.state.accounts.contains_key(&address) {
            return Ok(None)
        }
        Ok(Some(range(
            self.state
                .storage
                .get(&address)
                .into_iter()
                .flat_map(|slots| slots.range(start..))
                .map(|(key, value)| (*key, *value)),
            limit,
            bytes,
            64,
        )))
    }

    fn account_range_proof(&self, keys: &[B256]) -> Result<Vec<Bytes>, ProviderError> {
        if self.fail_proof {
            return Err(read_error())
        }
        Ok(trie(&self.state.account_leaves(), keys).1)
    }

    fn storage_range_proof(
        &self,
        address: B256,
        keys: &[B256],
    ) -> Result<Vec<Bytes>, ProviderError> {
        if self.fail_proof {
            return Err(read_error())
        }
        Ok(trie(&self.state.storage_leaves(address), keys).1)
    }
}

fn read_error() -> ProviderError {
    ProviderError::other(std::io::Error::other("fixture backend read failed"))
}

fn range<T>(
    items: impl Iterator<Item = (B256, T)>,
    limit: B256,
    bytes: usize,
    item_bytes: usize,
) -> RangeResponse<(B256, T)> {
    let mut values = Vec::new();
    for (hash, value) in items {
        values.push((hash, value));
        let end = if hash >= limit {
            Some(RangeEnd::HashLimit)
        } else if values.len().saturating_mul(item_bytes) > bytes {
            Some(RangeEnd::ByteLimit)
        } else {
            None
        };
        if let Some(end) = end {
            return RangeResponse { items: values, end }
        }
    }
    RangeResponse { items: values, end: RangeEnd::Exhausted }
}

fn trie(leaves: &[(B256, Vec<u8>)], keys: &[B256]) -> (B256, Vec<Bytes>) {
    let retainer = ProofRetainer::new(keys.iter().copied().map(Nibbles::unpack).collect());
    let mut builder = HashBuilder::default().with_proof_retainer(retainer);
    for (key, value) in leaves {
        builder.add_leaf(Nibbles::unpack(key), value);
    }
    let root = builder.root();
    (
        root,
        builder
            .take_proof_nodes()
            .into_nodes_sorted()
            .into_iter()
            .map(|(_, bytes)| bytes)
            .collect(),
    )
}

pub(super) fn state_with_accounts() -> FixtureState {
    let a = B256::repeat_byte(1);
    let b = B256::repeat_byte(2);
    FixtureState {
        accounts: BTreeMap::from([
            (
                a,
                Account {
                    nonce: 1,
                    balance: U256::from(2),
                    bytecode_hash: Some(B256::repeat_byte(0x22)),
                },
            ),
            (b, Account { nonce: 3, balance: U256::from(4), bytecode_hash: None }),
        ]),
        storage: BTreeMap::from([(
            a,
            BTreeMap::from([
                (B256::repeat_byte(0x11), U256::from(0x0102)),
                (B256::repeat_byte(0x22), U256::from(0xff)),
                (B256::repeat_byte(0x33), U256::from(7)),
            ]),
        )]),
    }
}

#[test]
fn snap_account_range_response_encodes_accounts_and_proof() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let request = handler.provider().account_request();
    let SnapResponse::AccountRange(response) =
        handler.serve(SnapProtocolMessage::GetAccountRange(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.request_id, 1);
    assert_eq!(response.accounts.len(), 2);
    let mut full_body = vec![0xf8, 0x44, 0x01, 0x02, 0xa0];
    full_body
        .extend_from_slice(handler.provider().state.storage_root(B256::repeat_byte(1)).as_slice());
    full_body.push(0xa0);
    full_body.extend_from_slice(B256::repeat_byte(0x22).as_slice());
    assert_eq!(response.accounts[0].body.as_ref(), full_body);
    assert_eq!(response.accounts[1].body.as_ref(), [0xc4, 3, 4, 0x80, 0x80]);
    assert_boundary_proof(handler.provider().root, B256::ZERO, None, &response.proof);
    let last = response.accounts.last().unwrap();
    assert_boundary_proof(
        handler.provider().root,
        last.hash,
        Some(alloy_rlp::encode(last.trie_account().unwrap())),
        &response.proof,
    );
    assert_eq!(handler.provider().resolutions.load(Ordering::Relaxed), 1);
    let wire = SnapProtocolMessage::AccountRange(response.clone())
        .encode_versioned(SnapVersion::V2)
        .unwrap();
    assert_eq!(
        SnapProtocolMessage::decode_versioned(SnapVersion::V2, &wire).unwrap(),
        SnapProtocolMessage::AccountRange(response)
    );
}

#[test]
fn snap_storage_range_response_encodes_values_and_proof() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let account = B256::repeat_byte(1);
    let mut request = handler.provider().storage_request(vec![account]);
    request.starting_hash = B256::repeat_byte(0x10);
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.slots[0][0].data.as_ref(), [0x82, 0x01, 0x02]);
    assert_eq!(response.slots[0][1].data.as_ref(), [0x81, 0xff]);
    let root = handler.provider().state.storage_root(account);
    assert_boundary_proof(root, B256::repeat_byte(0x10), None, &response.proof);
    assert_boundary_proof(root, B256::repeat_byte(0x33), Some(vec![7]), &response.proof);
    assert_eq!(handler.provider().resolutions.load(Ordering::Relaxed), 1);
}

#[test]
fn snap_storage_ranges_only_bound_the_first_account() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let empty_account = B256::repeat_byte(2);
    let storage_account = B256::repeat_byte(1);
    let mut request = handler.provider().storage_request(vec![empty_account, storage_account]);
    request.limit_hash = B256::repeat_byte(0x10);
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert!(response.slots[0].is_empty());
    assert_eq!(response.slots[1].len(), 3);
    assert!(response.proof.is_empty());
    let reads = handler.provider().storage_reads.lock().unwrap();
    assert_eq!(reads[0].2, B256::repeat_byte(0x10));
    assert_eq!(reads[1].1, B256::ZERO);
    assert_eq!(reads[1].2, B256::repeat_byte(0xff));
}

#[test]
fn snap_storage_range_proves_finite_limit_from_zero_origin() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let account = B256::repeat_byte(1);
    let mut request = handler.provider().storage_request(vec![account, B256::repeat_byte(2)]);
    request.limit_hash = B256::repeat_byte(0x20);
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.slots.len(), 1);
    assert_eq!(response.slots[0].len(), 2);
    let root = handler.provider().state.storage_root(account);
    assert_boundary_proof(root, B256::ZERO, None, &response.proof);
    assert_boundary_proof(root, B256::repeat_byte(0x22), Some(vec![0x81, 0xff]), &response.proof);
}

#[test]
fn snap_byte_codes_response_preserves_found_code_order() {
    let mut provider = Fixture::new(FixtureState::default());
    let code = Bytes::from_static(&[0x60, 0]);
    let later = Bytes::from_static(&[0x60, 1]);
    provider.codes.insert(keccak256(&code), Bytecode::new_raw(code.clone()));
    provider.codes.insert(keccak256(&later), Bytecode::new_raw(later.clone()));
    let handler = SnapRequestHandler::new(provider);
    let request = GetByteCodesMessage {
        request_id: 3,
        hashes: vec![KECCAK_EMPTY, B256::repeat_byte(0xff), keccak256(&code), keccak256(&later)],
        response_bytes: 1,
    };
    let SnapResponse::ByteCodes(response) =
        handler.serve(SnapProtocolMessage::GetByteCodes(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.request_id, 3);
    assert_eq!(response.codes, [Bytes::new(), code]);
}

#[test]
fn snap_storage_ranges_limit_account_lookups() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let account = B256::repeat_byte(2);
    let request =
        handler.provider().storage_request(vec![account; MAX_STORAGE_RANGE_ACCOUNTS_SERVE + 1]);
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.slots.len(), MAX_STORAGE_RANGE_ACCOUNTS_SERVE);
    assert_eq!(
        handler.provider().storage_reads.lock().unwrap().len(),
        MAX_STORAGE_RANGE_ACCOUNTS_SERVE
    );
}

#[test]
fn snap_storage_range_reports_missing_account_without_partial_response() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let request =
        handler.provider().storage_request(vec![B256::repeat_byte(2), B256::repeat_byte(0x99)]);
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)),
        Err(SnapRequestError::AccountUnavailable(_))
    ));
}

#[test]
fn snap_provider_errors_are_distinct_from_retained_empty_state() {
    let mut provider = Fixture::new(FixtureState::default());
    assert_eq!(provider.root, EMPTY_ROOT_HASH);
    let request = provider.account_request();
    provider.fail_reads = true;
    let handler = SnapRequestHandler::new(provider);
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetAccountRange(request.clone())),
        Err(SnapRequestError::Provider(_))
    ));
    let handler = SnapRequestHandler::new(Fixture::new(FixtureState::default()));
    let SnapResponse::AccountRange(response) =
        handler.serve(SnapProtocolMessage::GetAccountRange(request.clone())).unwrap()
    else {
        panic!("unexpected response")
    };
    assert!(response.accounts.is_empty());
    let unavailable = GetAccountRangeMessage { root_hash: B256::ZERO, ..request };
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetAccountRange(unavailable)),
        Err(SnapRequestError::StateRootUnavailable(_))
    ));
}

#[test]
fn snap_requests_fail_on_inconsistent_provider_results() {
    for fail_storage_root in [true, false] {
        let mut provider = Fixture::new(state_with_accounts());
        let request = provider.account_request();
        provider.fail_storage_root = fail_storage_root;
        provider.fail_proof = !fail_storage_root;
        let handler = SnapRequestHandler::new(provider);
        assert!(matches!(
            handler.serve(SnapProtocolMessage::GetAccountRange(request)),
            Err(SnapRequestError::Provider(_))
        ));
    }
}

#[tokio::test]
async fn snap_bal_requests_read_the_injected_store_and_route_response() {
    let handler = SnapRequestHandler::new(Fixture::new(FixtureState::default()));
    let raw = Bytes::from_static(&[0xc0]);
    let hash = B256::repeat_byte(0x44);
    handler
        .provider()
        .bals
        .insert(NumHash::new(1, hash), Sealed::new_unchecked(raw.clone(), keccak256(&raw)))
        .unwrap();
    let request = SnapProtocolMessage::GetBlockAccessLists(GetBlockAccessListsMessage {
        request_id: 77,
        block_hashes: vec![hash, B256::ZERO],
        response_bytes: 100,
    });
    let (tx, rx) = oneshot::channel();
    handler.on_request(request, tx);
    let SnapResponse::BlockAccessLists(response) = rx.await.unwrap().unwrap() else {
        panic!("unexpected response")
    };
    assert_eq!(response.request_id, 77);
    assert_eq!(response.block_access_lists.0, [Some(raw), None]);
    let wire: SnapProtocolMessage = SnapResponse::BlockAccessLists(response).into();
    assert_eq!(wire.request_id(), 77);
}

#[test]
fn snap_budget_and_request_kinds_are_bounded() {
    assert_eq!(response_budget(u64::MAX), SOFT_RESPONSE_LIMIT);
    let handler = SnapRequestHandler::new(Fixture::new(FixtureState::default()));
    let request = GetByteCodesMessage {
        request_id: 1,
        hashes: vec![B256::ZERO; MAX_BYTE_CODES_SERVE + 10],
        response_bytes: u64::MAX,
    };
    handler.serve(SnapProtocolMessage::GetByteCodes(request)).unwrap();
    assert_eq!(handler.provider().code_reads.load(Ordering::Relaxed), MAX_BYTE_CODES_SERVE);
    assert!(matches!(
        handler.serve(SnapProtocolMessage::ByteCodes(ByteCodesMessage {
            request_id: 1,
            codes: Vec::new(),
        })),
        Err(SnapRequestError::UnsupportedRequest)
    ));
    let request = GetBlockAccessListsMessage {
        request_id: 1,
        block_hashes: vec![B256::ZERO; MAX_BLOCK_ACCESS_LISTS_SERVE + 10],
        response_bytes: u64::MAX,
    };
    let SnapResponse::BlockAccessLists(response) =
        handler.serve(SnapProtocolMessage::GetBlockAccessLists(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.block_access_lists.0.len(), MAX_BLOCK_ACCESS_LISTS_SERVE);
}

/// Finds the requested boundary's path in a sorted, merged proof (source237 fixture logic).
fn assert_boundary_proof(root: B256, key: B256, expected_value: Option<Vec<u8>>, proof: &[Bytes]) {
    let root_reference = RlpNode::word_rlp(&root);
    let root_index = proof
        .iter()
        .position(|node| RlpNode::from_rlp(node).as_slice() == root_reference.as_slice())
        .expect("proof must contain the committed root");
    let key = Nibbles::unpack(key);
    let remaining: Vec<_> = proof
        .iter()
        .enumerate()
        .filter_map(|(index, node)| (index != root_index).then_some(node))
        .collect();
    let mut path = vec![&proof[root_index]];
    fn verifies_subsequence<'a>(
        root: B256,
        key: Nibbles,
        expected: &Option<Vec<u8>>,
        remaining: &[&'a Bytes],
        next: usize,
        path: &mut Vec<&'a Bytes>,
    ) -> bool {
        if verify_proof(root, key, expected.clone(), path.iter().copied()).is_ok() {
            return true
        }
        for index in next..remaining.len() {
            path.push(remaining[index]);
            if verifies_subsequence(root, key, expected, remaining, index + 1, path) {
                return true
            }
            path.pop();
        }
        false
    }
    assert!(
        verifies_subsequence(root, key, &expected_value, &remaining, 0, &mut path),
        "invalid or incomplete proof at boundary {key:?}"
    );
}

#[test]
fn snap_range_budget_stops_at_soft_limit_and_proves_the_boundary() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let mut request = handler.provider().account_request();
    request.response_bytes = 1;
    let SnapResponse::AccountRange(response) =
        handler.serve(SnapProtocolMessage::GetAccountRange(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.accounts.len(), 1);
    let last = &response.accounts[0];
    assert_boundary_proof(
        handler.provider().root,
        last.hash,
        Some(alloy_rlp::encode(last.trie_account().unwrap())),
        &response.proof,
    );

    let account = B256::repeat_byte(1);
    let mut request = handler.provider().storage_request(vec![account, B256::repeat_byte(2)]);
    request.response_bytes = 1;
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(response.slots.len(), 1);
    assert_eq!(response.slots[0].len(), 1);
    assert_boundary_proof(
        handler.provider().state.storage_root(account),
        response.slots[0][0].hash,
        Some(response.slots[0][0].data.to_vec()),
        &response.proof,
    );
}

#[test]
fn snap_storage_zero_budget_is_a_legitimate_empty_response() {
    let handler = SnapRequestHandler::new(Fixture::new(state_with_accounts()));
    let mut request = handler.provider().storage_request(vec![B256::repeat_byte(1)]);
    request.response_bytes = 0;
    let SnapResponse::StorageRanges(response) =
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)).unwrap()
    else {
        panic!("unexpected response")
    };
    assert!(response.slots.is_empty());
    assert!(response.proof.is_empty());
    assert!(handler.provider().storage_reads.lock().unwrap().is_empty());
}

#[test]
fn snap_storage_and_bytecode_failures_do_not_become_empty_successes() {
    let mut provider = Fixture::new(state_with_accounts());
    let request = provider.storage_request(vec![B256::repeat_byte(1)]);
    provider.fail_reads = true;
    let handler = SnapRequestHandler::new(provider);
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)),
        Err(SnapRequestError::Provider(_))
    ));
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetByteCodes(GetByteCodesMessage {
            request_id: 4,
            hashes: vec![B256::ZERO],
            response_bytes: 100,
        })),
        Err(SnapRequestError::Provider(_))
    ));
}

#[test]
fn snap_storage_proof_failure_does_not_return_partial_slots() {
    let mut provider = Fixture::new(state_with_accounts());
    let mut request = provider.storage_request(vec![B256::repeat_byte(1)]);
    request.starting_hash = B256::repeat_byte(0x10);
    provider.fail_proof = true;
    let handler = SnapRequestHandler::new(provider);
    assert!(matches!(
        handler.serve(SnapProtocolMessage::GetStorageRanges(request)),
        Err(SnapRequestError::Provider(_))
    ));
}
