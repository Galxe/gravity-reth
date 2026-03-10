//! Event bus for the pipe execution layer.

use alloy_primitives::TxHash;
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives::NodePrimitives;
use std::{sync::OnceLock, thread::sleep, time::Duration};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};
use tracing::info;

/// A static instance of `PipeExecLayerEventBus` used for dispatching events.
/// Uses typed `OnceLock` instead of `Box<dyn Any>` to eliminate the runtime downcast
/// and make type mismatches a compile-time error.
pub static PIPE_EXEC_LAYER_EVENT_BUS: OnceLock<PipeExecLayerEventBus<EthPrimitives>> =
    OnceLock::new();

/// Get a reference to the global `PipeExecLayerEventBus` instance.
/// Blocks until the event bus is initialized, with a maximum timeout of 120 seconds.
pub fn get_pipe_exec_layer_event_bus() -> &'static PipeExecLayerEventBus<EthPrimitives> {
    const MAX_WAIT_SECS: u64 = 120;
    let start = std::time::Instant::now();
    loop {
        if let Some(event_bus) = PIPE_EXEC_LAYER_EVENT_BUS.get() {
            return event_bus;
        }
        assert!(
            start.elapsed().as_secs() < MAX_WAIT_SECS,
            "PipeExecLayerEventBus not initialized after {}s — \
             likely a startup ordering bug",
            MAX_WAIT_SECS
        );
        if start.elapsed().as_secs().is_multiple_of(5) {
            info!("Wait PipeExecLayerEventBus ready...");
        }
        sleep(Duration::from_secs(1));
    }
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
