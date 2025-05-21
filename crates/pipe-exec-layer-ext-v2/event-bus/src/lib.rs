use alloy_primitives::TxHash;
use once_cell::sync::OnceCell;
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_primitives::NodePrimitives;
use std::any::Any;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

/// A static instance of `PipeExecLayerEventBus` used for dispatching events.
pub static PIPE_EXEC_LAYER_EVENT_BUS: OnceCell<Box<dyn Any + Send + Sync>> = OnceCell::new();

pub fn get_pipe_exec_layer_event_bus<N: NodePrimitives>(
) -> Option<&'static PipeExecLayerEventBus<N>> {
    PIPE_EXEC_LAYER_EVENT_BUS
        .get()
        .map(|ext| ext.downcast_ref::<PipeExecLayerEventBus<N>>().unwrap())
}

#[derive(Debug)]
pub enum PipeExecLayerEvent<N: NodePrimitives> {
    /// Make executed block canonical
    MakeCanonical(ExecutedBlockWithTrieUpdates<N>, oneshot::Sender<()>),
}

/// Called by EL.
#[derive(Debug)]
pub struct PipeExecLayerEventBus<N: NodePrimitives> {
    /// Receive events from PipeExecService
    pub event_rx: std::sync::Mutex<Option<std::sync::mpsc::Receiver<PipeExecLayerEvent<N>>>>,
    /// Receive discarded txs from PipeExecService
    pub discard_txs: tokio::sync::Mutex<Option<UnboundedReceiver<Vec<TxHash>>>>,
}
