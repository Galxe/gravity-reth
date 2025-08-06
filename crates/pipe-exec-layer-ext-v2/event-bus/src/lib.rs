//! Event bus for the pipe execution layer.

use alloy_primitives::TxHash;
use once_cell::sync::OnceCell;
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_primitives::NodePrimitives;
use std::any::Any;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

/// A static instance of `PipeExecLayerEventBus` used for dispatching events.
pub static PIPE_EXEC_LAYER_EVENT_BUS: OnceCell<Box<dyn Any + Send + Sync>> = OnceCell::new();

/// Get a reference to the global `PipeExecLayerEventBus` instance.
pub fn get_pipe_exec_layer_event_bus<N: NodePrimitives>(
) -> Option<&'static PipeExecLayerEventBus<N>> {
    PIPE_EXEC_LAYER_EVENT_BUS
        .get()
        .map(|ext| ext.downcast_ref::<PipeExecLayerEventBus<N>>().unwrap())
}

/// Event to make a block canonical
#[derive(Debug)]
pub struct MakeCanonicalEvent<N: NodePrimitives> {
    /// The executed block with trie updates
    pub executed_block: ExecutedBlockWithTrieUpdates<N>,
    /// A sender to notify when event processing is complete
    pub tx: oneshot::Sender<()>,
}

/// Event to wait for persistence of the block
#[derive(Debug)]
pub struct WaitForPersistenceEvent {
    /// The block number to wait for
    pub block_number: u64,
    /// A sender to notify when event processing is complete
    pub tx: oneshot::Sender<()>,
}

/// Events emitted by the pipeline execution layer
#[derive(Debug)]
pub enum PipeExecLayerEvent<N: NodePrimitives> {
    /// Make executed block canonical
    MakeCanonical(MakeCanonicalEvent<N>),
    /// Wait for persistence of the block
    WaitForPersistence(WaitForPersistenceEvent),
}

/// Event bus for the pipe execution layer.
#[derive(Debug)]
pub struct PipeExecLayerEventBus<N: NodePrimitives> {
    /// Receive events from `PipeExecService`
    pub event_rx: std::sync::Mutex<Option<std::sync::mpsc::Receiver<PipeExecLayerEvent<N>>>>,
    /// Receive discarded txs from `PipeExecService`
    pub discard_txs: tokio::sync::Mutex<Option<UnboundedReceiver<Vec<TxHash>>>>,
}
