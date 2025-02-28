//! Pipeline execution layer extension

use alloy_primitives::B256;
use once_cell::sync::OnceCell;
use reth_primitives::{Address, TransactionSigned};
use reth_rpc_types::engine::PayloadId;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Mutex,
};

/// Ordered block determined by consensus layer to execute
pub struct OrderedBlock {
    /// BlockId of the block generated by Gravity SDK
    pub block_id: B256,
    /// Parent block hash
    pub parent_hash: B256,
    /// Ordered transactions in the block
    pub transactions: Vec<TransactionSigned>,
    /// Senders of the transactions in the block
    pub senders: Vec<Address>,
}

#[derive(Debug)]
pub struct ExecutedBlockMeta {
    /// Which payload attributes is used to execute the block
    pub payload_id: PayloadId,
    /// Which ordered block is used to execute the block
    pub block_id: B256,
    /// Block hash of the executed block
    pub block_hash: B256,
}

/// Called by Coordinator
#[derive(Debug)]
pub struct PipeExecLayerApi {
    ordered_block_tx: UnboundedSender<OrderedBlock>,
    executed_block_hash_rx: UnboundedReceiver<ExecutedBlockMeta>,
    ready_to_get_payload_rx: UnboundedReceiver<PayloadId>,
    ready_to_new_payload_rx: UnboundedReceiver<B256 /* block hash */>,
}

impl PipeExecLayerApi {
    /// Push ordered block to EL for execution.
    /// Returns `None` if the channel has been closed.
    pub fn push_ordered_block(&self, block: OrderedBlock) -> Option<()> {
        self.ordered_block_tx.send(block).ok()
    }

    /// Pull executed block hash from EL for verification.
    /// Returns `None` if the channel has been closed.
    pub async fn pull_executed_block_hash(
        &mut self,
        payload_id: PayloadId,
        block_id: B256,
    ) -> Option<B256> {
        let block_meta = self.executed_block_hash_rx.recv().await?;
        assert_eq!(payload_id, block_meta.payload_id);
        assert_eq!(block_id, block_meta.block_id);
        Some(block_meta.block_hash)
    }

    /// Wait until EL is ready to process get_payload.
    /// Returns `None` if the channel has been closed.
    pub async fn ready_to_get_payload(&mut self, payload_id: PayloadId) -> Option<()> {
        let payload_id_ = self.ready_to_get_payload_rx.recv().await?;
        assert_eq!(payload_id, payload_id_);
        Some(())
    }

    /// Wait until EL is ready to process new_payload.
    /// Returns `None` if the channel has been closed.
    pub async fn ready_to_new_payload(&mut self, block_hash: B256) -> Option<()> {
        let block_hash_ = self.ready_to_new_payload_rx.recv().await?;
        assert_eq!(block_hash, block_hash_);
        Some(())
    }
}

/// Owned by EL
#[derive(Debug)]
pub struct PipeExecLayerExt {
    /// Receive ordered block from Coordinator
    ordered_block_rx: Mutex<UnboundedReceiver<OrderedBlock>>,
    /// Send executed block hash to Coordinator
    executed_block_hash_tx: UnboundedSender<ExecutedBlockMeta>,
    /// Send ready to process get_payload signal to Coordinator
    ready_to_get_payload_tx: UnboundedSender<PayloadId>,
    /// Send ready to process new_payload signal to Coordinator
    ready_to_new_payload_tx: UnboundedSender<B256 /* block hash */>,
}

impl PipeExecLayerExt {
    /// Pull next ordered block from Coordinator.
    /// Returns `None` if the channel has been closed.
    pub async fn pull_ordered_block(&self) -> Option<OrderedBlock> {
        self.ordered_block_rx.lock().await.recv().await
    }

    /// Pull next ordered block from Coordinator.
    /// Returns `None` if the channel has been closed.
    pub fn blocking_pull_ordered_block(&self) -> Option<OrderedBlock> {
        self.ordered_block_rx.blocking_lock().blocking_recv()
    }

    /// Push execited block hash to Coordinator.
    /// Returns `None` if the channel has been closed.
    pub fn push_executed_block_hash(&self, block_meta: ExecutedBlockMeta) -> Option<()> {
        self.executed_block_hash_tx.send(block_meta).ok()
    }

    /// Send ready to process get_payload signal to Coordinator.
    /// Returns `None` if the channel has been closed.
    pub fn send_ready_to_get_payload(&self, payload_id: PayloadId) -> Option<()> {
        self.ready_to_get_payload_tx.send(payload_id).ok()
    }

    /// Send ready to process new_payload signal to Coordinator.
    /// Returns `None` if the channel has been closed.
    pub fn send_ready_to_new_payload(&self, block_hash: B256) -> Option<()> {
        self.ready_to_new_payload_tx.send(block_hash).ok()
    }
}

pub static PIPE_EXEC_LAYER_EXT: OnceCell<PipeExecLayerExt> = OnceCell::new();

pub fn new_pipe_exec_layer_api() -> PipeExecLayerApi {
    let (ordered_block_tx, ordered_block_rx) = tokio::sync::mpsc::unbounded_channel();
    let (executed_block_hash_tx, executed_block_hash_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ready_to_get_payload_tx, ready_to_get_payload_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ready_to_new_payload_tx, ready_to_new_payload_rx) = tokio::sync::mpsc::unbounded_channel();

    let _ = PIPE_EXEC_LAYER_EXT.get_or_init(|| PipeExecLayerExt {
        ordered_block_rx: Mutex::new(ordered_block_rx),
        executed_block_hash_tx,
        ready_to_get_payload_tx,
        ready_to_new_payload_tx,
    });

    PipeExecLayerApi {
        ordered_block_tx,
        executed_block_hash_rx,
        ready_to_get_payload_rx,
        ready_to_new_payload_rx,
    }
}
