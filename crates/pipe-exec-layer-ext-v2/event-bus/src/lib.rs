//! Event bus for the pipe execution layer.

use alloy_primitives::TxHash;
use crossbeam_channel::{Receiver, Sender};
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives::NodePrimitives;
use std::sync::LazyLock;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};

/// A static instance of `PipeExecLayerEventBus` used for dispatching events.
///
/// The channels are created on first access, independently of `PipeExecService`. The node-side
/// consumers (engine tree, txpool maintenance) start while the node is still launching and take
/// their receivers right away; `PipeExecService` can only be built after the node has launched,
/// and takes the senders then. Neither side waits for the other, so a slow node launch (e.g.
/// heavy `ExEx` initialization) cannot break startup.
pub static PIPE_EXEC_LAYER_EVENT_BUS: LazyLock<PipeExecLayerEventBus<EthPrimitives>> =
    LazyLock::new(PipeExecLayerEventBus::new);

/// Get a reference to the global `PipeExecLayerEventBus` instance.
pub fn get_pipe_exec_layer_event_bus() -> &'static PipeExecLayerEventBus<EthPrimitives> {
    &PIPE_EXEC_LAYER_EVENT_BUS
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
    /// Send events to the engine tree, taken by `PipeExecService`
    event_tx: std::sync::Mutex<Option<Sender<PipeExecLayerEvent<N>>>>,
    /// Receive events from `PipeExecService`
    pub event_rx: std::sync::Mutex<Option<Receiver<PipeExecLayerEvent<N>>>>,
    /// Send discarded txs to the txpool, taken by `PipeExecService`
    discard_txs_tx: std::sync::Mutex<Option<UnboundedSender<Vec<TxHash>>>>,
    /// Receive discarded txs from `PipeExecService`
    pub discard_txs: tokio::sync::Mutex<Option<UnboundedReceiver<Vec<TxHash>>>>,
}

impl<N: NodePrimitives> PipeExecLayerEventBus<N> {
    fn new() -> Self {
        // crossbeam rather than std mpsc: the engine tree selects on this receiver together with
        // its persistence completion channel.
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let (discard_txs_tx, discard_txs_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            event_tx: std::sync::Mutex::new(Some(event_tx)),
            event_rx: std::sync::Mutex::new(Some(event_rx)),
            discard_txs_tx: std::sync::Mutex::new(Some(discard_txs_tx)),
            discard_txs: tokio::sync::Mutex::new(Some(discard_txs_rx)),
        }
    }

    /// Takes the event and discarded-txs senders for the process's single `PipeExecService`.
    ///
    /// The senders are moved out rather than cloned, so the receivers observe a disconnect once
    /// the pipe service drops them.
    ///
    /// # Panics
    ///
    /// Panics if the senders were already taken.
    pub fn take_senders(&self) -> (Sender<PipeExecLayerEvent<N>>, UnboundedSender<Vec<TxHash>>) {
        let event_tx =
            self.event_tx.lock().unwrap().take().expect("pipe exec event sender already taken");
        let discard_txs_tx = self
            .discard_txs_tx
            .lock()
            .unwrap()
            .take()
            .expect("pipe exec discarded-txs sender already taken");
        (event_tx, discard_txs_tx)
    }
}

/// Node primitives the pipe execution layer can drive.
///
/// The event bus is a single global instance bound to concrete primitives, while the engine tree
/// is generic over [`NodePrimitives`]. Requiring this trait on the tree turns a primitives
/// mismatch into a compile error instead of a runtime type check.
pub trait PipeExecPrimitives: NodePrimitives {
    /// Returns the global event bus carrying events of these primitives.
    fn pipe_exec_layer_event_bus() -> &'static PipeExecLayerEventBus<Self>;
}

impl PipeExecPrimitives for EthPrimitives {
    fn pipe_exec_layer_event_bus() -> &'static PipeExecLayerEventBus<Self> {
        get_pipe_exec_layer_event_bus()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn receivers_are_available_before_pipe_service_starts() {
        // Node-side consumers (engine tree, txpool maintenance) start while the node is still
        // launching, long before `PipeExecService` exists. Taking their receivers must not
        // depend on the pipe service having started.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let bus = get_pipe_exec_layer_event_bus();
            let event_rx = bus.event_rx.lock().unwrap().take().unwrap();
            let discard_txs_rx = bus.discard_txs.try_lock().unwrap().take().unwrap();
            let _ = tx.send((event_rx, discard_txs_rx));
        });
        let (event_rx, mut discard_txs_rx) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("taking the receivers waited for the pipe service to start");

        // The pipe service starts later and takes the senders.
        let (event_tx, discard_txs_tx) = get_pipe_exec_layer_event_bus().take_senders();
        let (done_tx, _done_rx) = oneshot::channel();
        event_tx
            .send(PipeExecLayerEvent::WaitForPersistence(WaitForPersistenceEvent {
                block_number: 7,
                tx: done_tx,
            }))
            .unwrap();
        assert!(matches!(
            event_rx.try_recv(),
            Ok(PipeExecLayerEvent::WaitForPersistence(event)) if event.block_number == 7
        ));
        discard_txs_tx.send(vec![TxHash::ZERO]).unwrap();
        assert_eq!(discard_txs_rx.try_recv().unwrap(), vec![TxHash::ZERO]);

        // The bus keeps no sender, so the engine tree sees a disconnect when the pipe service
        // stops.
        drop(event_tx);
        assert!(event_rx.recv().is_err());
    }
}
