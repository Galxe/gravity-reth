//! Blocks/Headers management for the p2p network.

use crate::{
    budget::DEFAULT_BUDGET_TRY_DRAIN_DOWNLOADERS, metered_poll_nested_stream_with_budget,
    metrics::EthRequestHandlerMetrics,
};
use alloy_consensus::{BlockHeader, ReceiptWithBloom};
use alloy_eips::BlockHashOrNumber;
use alloy_rlp::{length_of_length, Encodable};
use futures::StreamExt;
use reth_eth_wire::{
    message::MAX_MESSAGE_SIZE, BlockAccessLists, BlockBodies, BlockHeaders, Cells,
    EthNetworkPrimitives, GetBlockAccessLists, GetBlockBodies, GetBlockHeaders, GetCells,
    GetNodeData, GetReceipts, GetReceipts70, HeadersDirection, NetworkPrimitives, NodeData,
    Receipts, Receipts69, Receipts70,
};
use reth_network_api::test_utils::PeersHandle;
use reth_network_p2p::error::RequestResult;
use reth_network_peers::PeerId;
use reth_primitives_traits::Block;
use reth_storage_api::{BlockReader, HeaderProvider};
use reth_transaction_pool::{blobstore::NoopBlobStore, BlobStore};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc::Receiver, oneshot};
use tokio_stream::wrappers::ReceiverStream;

// Limits: <https://github.com/ethereum/go-ethereum/blob/b0d44338bbcefee044f1f635a84487cbbd8f0538/eth/protocols/eth/handler.go#L34-L56>

/// Maximum number of receipts to serve.
///
/// Used to limit lookups.
pub const MAX_RECEIPTS_SERVE: usize = 1024;

/// Maximum number of block headers to serve.
///
/// Used to limit lookups.
pub const MAX_HEADERS_SERVE: usize = 1024;

/// Maximum number of block headers to serve.
///
/// Used to limit lookups. With 24KB block sizes nowadays, the practical limit will always be
/// `SOFT_RESPONSE_LIMIT`.
pub const MAX_BODIES_SERVE: usize = 1024;

/// Maximum number of block access lists to serve.
///
/// Used to limit lookups.
pub const MAX_BLOCK_ACCESS_LISTS_SERVE: usize = 1024;

/// Maximum number of cell lookups to serve.
///
/// Used to limit lookups.
pub const MAX_CELLS_SERVE: usize = 1024;

/// Maximum size of replies to data retrievals: 2MB
pub const SOFT_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;

/// Upper bound on the headers reserved once the first requested header resolves.
///
/// Header responses are bound by [`MAX_HEADERS_SERVE`] rather than by size, so a full batch reaches
/// its final capacity within a few doublings while a batch that runs past the local tip wastes at
/// most this many slots.
const MAX_HEADERS_RESERVE: usize = 64;

/// Upper bound on the block bodies or receipt lists reserved once the first requested block
/// resolves.
///
/// These responses are bound by [`SOFT_RESPONSE_LIMIT`], so the first block's size predicts how
/// many fit and this only caps the estimate for tiny blocks.
const MAX_BLOCKS_RESERVE: usize = 16;

/// Manages eth related requests on top of the p2p network.
///
/// This can be spawned to another task and is supposed to be run as background service.
#[derive(Debug)]
#[must_use = "Manager does nothing unless polled."]
pub struct EthRequestHandler<C, N: NetworkPrimitives = EthNetworkPrimitives> {
    /// The client type that can interact with the chain.
    client: C,
    /// Blob store used for serving blob cell requests.
    blob_store: Box<dyn BlobStore>,
    /// Incoming request from the [`NetworkManager`](crate::NetworkManager).
    incoming_requests: ReceiverStream<IncomingEthRequest<N>>,
    /// Metrics for the eth request handler.
    metrics: EthRequestHandlerMetrics,
}

// === impl EthRequestHandler ===
impl<C, N: NetworkPrimitives> EthRequestHandler<C, N> {
    /// Create a new instance
    pub fn new(client: C, _peers: PeersHandle, incoming: Receiver<IncomingEthRequest<N>>) -> Self {
        Self {
            client,
            blob_store: Box::<NoopBlobStore>::default(),
            incoming_requests: ReceiverStream::new(incoming),
            metrics: Default::default(),
        }
    }

    /// Set blob store for the request handler
    pub fn with_blob_store(mut self, blob_store: Box<dyn BlobStore>) -> Self {
        self.blob_store = blob_store;
        self
    }
}

/// Number of response items to reserve once the first requested item has resolved.
///
/// Assumes the remaining items are about as large as the first one and reserves as many as fit into
/// [`SOFT_RESPONSE_LIMIT`], bounded by the requested `count` and `max`.
fn response_reserve_hint(count: usize, first_len: usize, max: usize) -> usize {
    SOFT_RESPONSE_LIMIT.div_ceil(first_len.max(1)).min(count).min(max)
}

impl<C, N> EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: BlockReader,
{
    /// Returns the list of requested headers
    fn get_headers_response(&self, request: GetBlockHeaders) -> Vec<C::Header> {
        let GetBlockHeaders { start_block, limit, skip, direction } = request;

        let mut headers = Vec::new();

        let mut block: BlockHashOrNumber = match start_block {
            BlockHashOrNumber::Hash(start) => start.into(),
            BlockHashOrNumber::Number(num) => {
                let Some(hash) = self.client.block_hash(num).unwrap_or_default() else {
                    return headers
                };
                hash.into()
            }
        };

        let skip = skip as u64;
        let mut total_bytes = 0;

        for _ in 0..limit {
            if let Some(header) = self.client.header_by_hash_or_number(block).unwrap_or_default() {
                let number = header.number();
                let parent_hash = header.parent_hash();
                let len = header.length();

                if headers.is_empty() {
                    let count = limit.min(MAX_HEADERS_SERVE as u64) as usize;
                    headers.reserve(response_reserve_hint(count, len, MAX_HEADERS_RESERVE));
                }

                total_bytes += len;
                headers.push(header);

                if headers.len() >= MAX_HEADERS_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }

                match direction {
                    HeadersDirection::Rising => {
                        if let Some(next) = number.checked_add(1).and_then(|n| n.checked_add(skip))
                        {
                            block = next.into()
                        } else {
                            break
                        }
                    }
                    HeadersDirection::Falling => {
                        if skip > 0 {
                            // prevent under flows for block.number == 0 and `block.number - skip <
                            // 0`
                            if let Some(next) =
                                number.checked_sub(1).and_then(|num| num.checked_sub(skip))
                            {
                                block = next.into()
                            } else {
                                break
                            }
                        } else {
                            block = parent_hash.into()
                        }
                    }
                }
            } else {
                break
            }
        }

        headers
    }

    fn on_headers_request(
        &self,
        _peer_id: PeerId,
        request: GetBlockHeaders,
        response: oneshot::Sender<RequestResult<BlockHeaders<C::Header>>>,
    ) {
        self.metrics.eth_headers_requests_received_total.increment(1);
        let headers = self.get_headers_response(request);
        let _ = response.send(Ok(BlockHeaders(headers)));
    }

    fn on_bodies_request(
        &self,
        _peer_id: PeerId,
        request: GetBlockBodies,
        response: oneshot::Sender<RequestResult<BlockBodies<<C::Block as Block>::Body>>>,
    ) {
        self.metrics.eth_bodies_requests_received_total.increment(1);
        let count = request.0.len();
        let mut bodies = Vec::new();

        let mut total_bytes = 0;

        for hash in request {
            if let Some(block) = self.client.block_by_hash(hash).unwrap_or_default() {
                let body = block.into_body();
                let len = body.length();
                if bodies.is_empty() {
                    bodies.reserve(response_reserve_hint(count, len, MAX_BLOCKS_RESERVE));
                }
                total_bytes += len;
                bodies.push(body);

                if bodies.len() >= MAX_BODIES_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }
            } else {
                break
            }
        }

        let _ = response.send(Ok(BlockBodies(bodies)));
    }

    /// Replies to `GetNodeData`.
    ///
    /// State serving via eth `GetNodeData` was removed; answer with an empty payload so eth/66
    /// peers receive a response instead of hanging until the request timeout when the oneshot is
    /// dropped unanswered.
    fn on_node_data_request(
        &self,
        _peer_id: PeerId,
        _request: GetNodeData,
        response: oneshot::Sender<RequestResult<NodeData>>,
    ) {
        self.metrics.eth_node_data_requests_received_total.increment(1);
        let _ = response.send(Ok(NodeData(vec![])));
    }

    fn on_receipts_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts,
        response: oneshot::Sender<RequestResult<Receipts<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let receipts = self.get_receipts_response(request, |receipts_by_block| {
            receipts_by_block.into_iter().map(ReceiptWithBloom::from).collect::<Vec<_>>()
        });

        let _ = response.send(Ok(Receipts(receipts)));
    }

    fn on_receipts69_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts,
        response: oneshot::Sender<RequestResult<Receipts69<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let receipts = self.get_receipts_response(request, |receipts_by_block| {
            // skip bloom filter for eth69
            receipts_by_block
        });

        let _ = response.send(Ok(Receipts69(receipts)));
    }

    /// Handles partial responses for [`GetReceipts70`] queries.
    ///
    /// This will adhere to the soft limit but allow filling the last vec partially. A response
    /// never ends with an empty incomplete block: if not even the next receipt fits, the response
    /// ends after the last complete block, except when it would otherwise be empty, in which case
    /// the first receipt is served even if it exceeds the soft limit. If it cannot fit within
    /// [`MAX_MESSAGE_SIZE`], including the response framing, the response is empty and complete.
    fn on_receipts70_request(
        &self,
        _peer_id: PeerId,
        request: GetReceipts70,
        response: oneshot::Sender<RequestResult<Receipts70<C::Receipt>>>,
    ) {
        self.metrics.eth_receipts_requests_received_total.increment(1);

        let GetReceipts70 { first_block_receipt_index, block_hashes } = request;

        let count = block_hashes.len();
        let mut receipts = Vec::new();
        let mut total_bytes = 0usize;
        let mut last_block_incomplete = false;

        // The request ID is unavailable here. Reserve the message code, the largest request ID,
        // the incomplete flag, and the three RLP list headers to bound the full encoded response.
        let max_first_receipt_size = MAX_MESSAGE_SIZE -
            (1 + u64::MAX.length() + false.length() + 3 * length_of_length(MAX_MESSAGE_SIZE));

        for (idx, hash) in block_hashes.into_iter().enumerate() {
            if idx >= MAX_RECEIPTS_SERVE {
                break
            }

            let Some(mut block_receipts) =
                self.client.receipts_by_block(BlockHashOrNumber::Hash(hash)).unwrap_or_default()
            else {
                break
            };

            if idx == 0 && first_block_receipt_index > 0 {
                let skip = first_block_receipt_index as usize;
                if skip >= block_receipts.len() {
                    block_receipts.clear();
                } else {
                    block_receipts.drain(0..skip);
                }
            }

            let block_size = block_receipts.length();
            if receipts.is_empty() {
                receipts.reserve(response_reserve_hint(count, block_size, MAX_BLOCKS_RESERVE));
            }

            if total_bytes + block_size <= SOFT_RESPONSE_LIMIT {
                total_bytes += block_size;
                receipts.push(block_receipts);
                continue;
            }

            // If nothing has been added to the response yet, serve the first receipt even if it
            // exceeds the soft limit, provided the framed response fits within the hard limit.
            let always_serve_first = receipts.is_empty();
            let remaining = block_receipts.len();
            let mut partial_block = Vec::new();
            for receipt in block_receipts {
                let receipt_size = receipt.length();
                if total_bytes + receipt_size > SOFT_RESPONSE_LIMIT &&
                    !(always_serve_first &&
                        partial_block.is_empty() &&
                        receipt_size <= max_first_receipt_size)
                {
                    break;
                }
                total_bytes += receipt_size;
                partial_block.push(receipt);
            }

            // An empty partial block delivers nothing, so it is never sent. The requester can
            // simply continue with this block in its next request.
            if !partial_block.is_empty() {
                last_block_incomplete = partial_block.len() < remaining;
                receipts.push(partial_block);
            }
            break;
        }

        let _ = response.send(Ok(Receipts70 { last_block_incomplete, receipts }));
    }

    #[inline]
    fn get_receipts_response<T, F>(&self, request: GetReceipts, transform_fn: F) -> Vec<Vec<T>>
    where
        F: Fn(Vec<C::Receipt>) -> Vec<T>,
        T: Encodable,
    {
        let count = request.0.len();
        let mut receipts = Vec::new();
        let mut total_bytes = 0;

        for hash in request {
            if let Some(receipts_by_block) =
                self.client.receipts_by_block(BlockHashOrNumber::Hash(hash)).unwrap_or_default()
            {
                let transformed_receipts = transform_fn(receipts_by_block);
                let len = transformed_receipts.length();
                if receipts.is_empty() {
                    receipts.reserve(response_reserve_hint(count, len, MAX_BLOCKS_RESERVE));
                }
                total_bytes += len;
                receipts.push(transformed_receipts);

                if receipts.len() >= MAX_RECEIPTS_SERVE || total_bytes > SOFT_RESPONSE_LIMIT {
                    break
                }
            } else {
                break
            }
        }

        receipts
    }

    fn on_cells_request(
        &self,
        _peer_id: PeerId,
        request: GetCells,
        response: oneshot::Sender<RequestResult<Cells>>,
    ) {
        let mut cells_response = Cells { cell_mask: request.cell_mask, ..Default::default() };
        let mut total_bytes = 0;
        let cell_mask = request.cell_mask();

        for hash in request.hashes.into_iter().take(MAX_CELLS_SERVE) {
            let Some(cells) = self.blob_store.get_cells(hash, cell_mask).unwrap_or_default() else {
                continue;
            };

            total_bytes += hash.length() + cells.length();
            cells_response.hashes.push(hash);
            cells_response.cells.push(cells);

            if total_bytes > SOFT_RESPONSE_LIMIT {
                break
            }
        }

        let _ = response.send(Ok(cells_response));
    }
}

/// An endless future.
///
/// This should be spawned or used as part of `tokio::select!`.
impl<C, N> Future for EthRequestHandler<C, N>
where
    N: NetworkPrimitives,
    C: BlockReader<Block = N::Block, Receipt = N::Receipt>
        + HeaderProvider<Header = N::BlockHeader>
        + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut acc = Duration::ZERO;
        let maybe_more_incoming_requests = metered_poll_nested_stream_with_budget!(
            acc,
            "net::eth",
            "Incoming eth requests stream",
            DEFAULT_BUDGET_TRY_DRAIN_DOWNLOADERS,
            this.incoming_requests.poll_next_unpin(cx),
            |incoming| {
                match incoming {
                    IncomingEthRequest::GetBlockHeaders { peer_id, request, response } => {
                        this.on_headers_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetBlockBodies { peer_id, request, response } => {
                        this.on_bodies_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetNodeData { peer_id, request, response } => {
                        this.on_node_data_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetReceipts { peer_id, request, response } => {
                        this.on_receipts_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetReceipts69 { peer_id, request, response } => {
                        this.on_receipts69_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetReceipts70 { peer_id, request, response } => {
                        this.on_receipts70_request(peer_id, request, response)
                    }
                    IncomingEthRequest::GetBlockAccessLists { response, .. } => {
                        // gravity does not maintain a BAL store; answer with an empty list
                        let _ = response.send(Ok(BlockAccessLists(Vec::new())));
                    }
                    IncomingEthRequest::GetCells { peer_id, request, response } => {
                        this.on_cells_request(peer_id, request, response)
                    }
                }
            },
        );

        this.metrics.acc_duration_poll_eth_req_handler.set(acc.as_secs_f64());

        // stream is fully drained and import futures pending
        if maybe_more_incoming_requests {
            // make sure we're woken up again
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

/// All `eth` request related to blocks delegated by the network.
#[derive(Debug)]
pub enum IncomingEthRequest<N: NetworkPrimitives = EthNetworkPrimitives> {
    /// Request Block headers from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockHeaders {
        /// The ID of the peer to request block headers from.
        peer_id: PeerId,
        /// The specific block headers requested.
        request: GetBlockHeaders,
        /// The channel sender for the response containing block headers.
        response: oneshot::Sender<RequestResult<BlockHeaders<N::BlockHeader>>>,
    },
    /// Request Block bodies from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockBodies {
        /// The ID of the peer to request block bodies from.
        peer_id: PeerId,
        /// The specific block bodies requested.
        request: GetBlockBodies,
        /// The channel sender for the response containing block bodies.
        response: oneshot::Sender<RequestResult<BlockBodies<N::BlockBody>>>,
    },
    /// Request Node Data from the peer.
    ///
    /// The response should be sent through the channel.
    GetNodeData {
        /// The ID of the peer to request node data from.
        peer_id: PeerId,
        /// The specific node data requested.
        request: GetNodeData,
        /// The channel sender for the response containing node data.
        response: oneshot::Sender<RequestResult<NodeData>>,
    },
    /// Request Receipts from the peer.
    ///
    /// The response should be sent through the channel.
    GetReceipts {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested.
        request: GetReceipts,
        /// The channel sender for the response containing receipts.
        response: oneshot::Sender<RequestResult<Receipts<N::Receipt>>>,
    },
    /// Request Receipts from the peer without bloom filter.
    ///
    /// The response should be sent through the channel.
    GetReceipts69 {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested.
        request: GetReceipts,
        /// The channel sender for the response containing Receipts69.
        response: oneshot::Sender<RequestResult<Receipts69<N::Receipt>>>,
    },
    /// Request Receipts from the peer using eth/70.
    ///
    /// The response should be sent through the channel.
    GetReceipts70 {
        /// The ID of the peer to request receipts from.
        peer_id: PeerId,
        /// The specific receipts requested including the `firstBlockReceiptIndex`.
        request: GetReceipts70,
        /// The channel sender for the response containing Receipts70.
        response: oneshot::Sender<RequestResult<Receipts70<N::Receipt>>>,
    },
    /// Request Block Access Lists from the peer.
    ///
    /// The response should be sent through the channel.
    GetBlockAccessLists {
        /// The ID of the peer to request block access lists from.
        peer_id: PeerId,
        /// The requested block hashes.
        request: GetBlockAccessLists,
        /// The channel sender for the response containing block access lists.
        response: oneshot::Sender<RequestResult<BlockAccessLists>>,
    },
    /// Request Cells from the peer.
    ///
    /// The response should be sent through the channel.
    GetCells {
        /// The ID of the peer to request cells from.
        peer_id: PeerId,
        /// The requested block hashes.
        request: GetCells,
        /// The channel sender for the response containing cells.
        response: oneshot::Sender<RequestResult<Cells>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::{
        eip4844::{BlobAndProofV1, BlobAndProofV2, BlobCellsAndProofsV1},
        eip7594::{BlobCellMask, BlobTransactionSidecarVariant, Cell},
    };
    use alloy_primitives::{TxHash, B128, B256};
    use reth_network_api::test_utils::PeersHandle;
    use reth_storage_api::noop::NoopProvider;
    use reth_transaction_pool::blobstore::{BlobStoreCleanupStat, BlobStoreError};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use test_case::test_case;
    use tokio::sync::mpsc;

    #[derive(Debug, Default)]
    struct CountingBlobStore {
        get_cells_calls: Arc<AtomicUsize>,
        expected_cell_mask: Option<BlobCellMask>,
    }

    impl BlobStore for CountingBlobStore {
        fn insert(
            &self,
            _tx: B256,
            _data: BlobTransactionSidecarVariant,
        ) -> Result<(), BlobStoreError> {
            Ok(())
        }

        fn insert_all(
            &self,
            _txs: Vec<(B256, BlobTransactionSidecarVariant)>,
        ) -> Result<(), BlobStoreError> {
            Ok(())
        }

        fn delete(&self, _tx: B256) -> Result<(), BlobStoreError> {
            Ok(())
        }

        fn delete_all(&self, _txs: Vec<B256>) -> Result<(), BlobStoreError> {
            Ok(())
        }

        fn cleanup(&self) -> BlobStoreCleanupStat {
            BlobStoreCleanupStat::default()
        }

        fn get(
            &self,
            _tx: B256,
        ) -> Result<Option<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
            Ok(None)
        }

        fn contains(&self, _tx: B256) -> Result<bool, BlobStoreError> {
            Ok(false)
        }

        fn get_all(
            &self,
            _txs: Vec<B256>,
        ) -> Result<Vec<(B256, Arc<BlobTransactionSidecarVariant>)>, BlobStoreError> {
            Ok(vec![])
        }

        fn get_exact(
            &self,
            txs: Vec<B256>,
        ) -> Result<Vec<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
            if txs.is_empty() {
                return Ok(vec![])
            }

            Err(BlobStoreError::MissingSidecar(txs[0]))
        }

        fn get_by_versioned_hashes_v1(
            &self,
            versioned_hashes: &[B256],
        ) -> Result<Vec<Option<BlobAndProofV1>>, BlobStoreError> {
            Ok(vec![None; versioned_hashes.len()])
        }

        fn get_by_versioned_hashes_v2(
            &self,
            _versioned_hashes: &[B256],
        ) -> Result<Option<Vec<BlobAndProofV2>>, BlobStoreError> {
            Ok(None)
        }

        fn get_by_versioned_hashes_v3(
            &self,
            versioned_hashes: &[B256],
        ) -> Result<Vec<Option<BlobAndProofV2>>, BlobStoreError> {
            Ok(vec![None; versioned_hashes.len()])
        }

        fn get_by_versioned_hashes_v4(
            &self,
            versioned_hashes: &[B256],
            _cell_mask: BlobCellMask,
        ) -> Result<Vec<Option<BlobCellsAndProofsV1>>, BlobStoreError> {
            Ok(vec![None; versioned_hashes.len()])
        }

        fn has_versioned_hashes(
            &self,
            versioned_hashes: &[B256],
        ) -> Result<Vec<bool>, BlobStoreError> {
            Ok(vec![false; versioned_hashes.len()])
        }

        fn get_cells(
            &self,
            _tx_hash: TxHash,
            cell_mask: BlobCellMask,
        ) -> Result<Option<Vec<Cell>>, BlobStoreError> {
            self.get_cells_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(expected) = self.expected_cell_mask {
                assert_eq!(cell_mask, expected);
                return Ok(Some(vec![Cell::default()]))
            }
            Ok(None)
        }

        fn data_size_hint(&self) -> Option<usize> {
            Some(0)
        }

        fn blobs_len(&self) -> usize {
            0
        }
    }

    #[tokio::test]
    async fn get_cells_request_limits_blob_store_lookups() {
        let (peers_tx, _) = mpsc::unbounded_channel();
        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        let get_cells_calls = Arc::new(AtomicUsize::new(0));
        let blob_store = CountingBlobStore {
            get_cells_calls: Arc::clone(&get_cells_calls),
            ..Default::default()
        };
        let handler = EthRequestHandler::<NoopProvider>::new(
            NoopProvider::default(),
            PeersHandle::new(peers_tx),
            incoming_rx,
        )
        .with_blob_store(Box::new(blob_store));
        let (response, rx) = oneshot::channel();
        let request =
            GetCells { hashes: vec![B256::ZERO; MAX_CELLS_SERVE + 1], cell_mask: B128::default() };

        handler.on_cells_request(PeerId::default(), request, response);

        let cells = rx.await.unwrap().unwrap();
        assert!(cells.hashes.is_empty());
        assert_eq!(get_cells_calls.load(Ordering::Relaxed), MAX_CELLS_SERVE);
    }
    #[test_case(0)]
    #[test_case(7)]
    #[test_case(8)]
    #[test_case(63)]
    #[test_case(64)]
    #[test_case(127)]
    #[tokio::test]
    async fn get_cells_request_uses_little_endian_wire_mask(index: u32) {
        use alloy_rlp::{Decodable, Encodable};

        let (peers_tx, _) = mpsc::unbounded_channel();
        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        let numeric_mask = 1u128 << index;
        let blob_store = CountingBlobStore {
            expected_cell_mask: Some(BlobCellMask::from_bits(numeric_mask)),
            ..Default::default()
        };
        let handler = EthRequestHandler::<NoopProvider>::new(
            NoopProvider::default(),
            PeersHandle::new(peers_tx),
            incoming_rx,
        )
        .with_blob_store(Box::new(blob_store));
        let wire_mask = B128::from(numeric_mask.to_le_bytes());
        let request = GetCells { hashes: vec![B256::ZERO], cell_mask: wire_mask };
        let mut encoded = Vec::new();
        request.encode(&mut encoded);
        let request = GetCells::decode(&mut encoded.as_slice()).unwrap();
        let (response, rx) = oneshot::channel();

        handler.on_cells_request(PeerId::default(), request, response);

        let cells = rx.await.unwrap().unwrap();
        assert_eq!(cells.hashes, vec![B256::ZERO]);
        assert_eq!(cells.cells, vec![vec![Cell::default()]]);
        assert_eq!(cells.cell_mask, wire_mask);
        encoded.clear();
        cells.encode(&mut encoded);
        assert_eq!(Cells::decode(&mut encoded.as_slice()).unwrap(), cells);
    }
}
