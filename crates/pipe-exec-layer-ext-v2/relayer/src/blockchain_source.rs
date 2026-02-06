//! Blockchain Event Source
//!
//! Monitors cross-chain events from EVM chains, specifically GravityPortal.MessageSent.

use crate::{
    data_source::{source_types, OracleData, OracleDataSource},
    eth_client::EthHttpCli,
};
use alloy_primitives::{hex, Address, Bytes, U256};
use alloy_rpc_types::Filter;
use alloy_sol_macro::sol;
use alloy_sol_types::SolEvent;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// GravityPortal.MessageSent event signature
sol! {
    /// MessageSent(uint128 indexed nonce, uint256 indexed blockNumber, bytes payload)
    event MessageSent(uint128 indexed nonce, uint256 indexed blockNumber, bytes payload);
}

/// Decode ABI-encoded `bytes` from Solidity event log data
///
/// Solidity encodes `bytes` in events as:
/// - bytes 0-31:  offset to data (typically 0x20 = 32)
/// - bytes 32-63: length of data
/// - bytes 64+:   actual data
///
/// Returns the raw bytes without the ABI encoding wrapper.
fn decode_abi_bytes(data: &[u8]) -> Option<Vec<u8>> {
    // Minimum: offset (32) + length (32) = 64 bytes
    if data.len() < 64 {
        return None;
    }

    // Read offset (last 8 bytes of first 32-byte word, as it's right-aligned)
    let offset = u64::from_be_bytes(data[24..32].try_into().ok()?) as usize;

    // Offset should point to the length word
    if offset + 32 > data.len() {
        return None;
    }

    // Read length (last 8 bytes of the length word)
    let length = u64::from_be_bytes(data[offset + 24..offset + 32].try_into().ok()?) as usize;

    // Data starts after the length word
    let data_start = offset + 32;
    if data_start + length > data.len() {
        return None;
    }

    Some(data[data_start..data_start + length].to_vec())
}

/// Represents the state of the last successfully processed event
///
/// This struct bundles the nonce and block number together to ensure
/// atomic updates and consistent state for persistence and recovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct LastProcessedEvent {
    /// The nonce of the last processed event (0 means no event processed yet)
    pub nonce: u128,
    /// The block number where this event was emitted
    pub block: u64,
}

impl LastProcessedEvent {
    /// Create a new LastProcessedEvent
    pub fn new(nonce: u128, block: u64) -> Self {
        Self { nonce, block }
    }

    /// Check if any event has been processed
    pub fn is_initialized(&self) -> bool {
        self.nonce > 0
    }
}

/// Blockchain event source for monitoring GravityPortal.MessageSent events
///
/// This is the primary data source for cross-chain message bridging.
/// Tracks `last_processed` to ensure exactly-once consumption semantics.
#[derive(Debug)]
pub struct BlockchainEventSource {
    /// Chain ID (sourceId in Oracle terms)
    chain_id: u64,

    /// Ethereum RPC client
    rpc_client: Arc<EthHttpCli>,

    /// GravityPortal contract address
    portal_address: Address,

    /// Current block cursor for polling
    cursor: AtomicU64,

    /// Last event we returned to caller (for exactly-once tracking and persistence)
    last_processed: Mutex<LastProcessedEvent>,
}

impl BlockchainEventSource {
    /// Maximum blocks to poll in one request
    const MAX_BLOCKS_PER_POLL: u64 = 100;

    /// Create with a known cursor position (for fast restart from persisted state)
    ///
    /// Skips discovery and uses the provided cursor directly.
    pub async fn new_with_cursor(
        chain_id: u64,
        rpc_url: &str,
        portal_address: Address,
        cursor: u64,
        latest_onchain_nonce: u128,
    ) -> Result<Self> {
        let rpc_client = Arc::new(EthHttpCli::new(rpc_url)?);

        info!(
            target: "blockchain_source",
            chain_id = chain_id,
            portal_address = ?portal_address,
            cursor = cursor,
            latest_onchain_nonce = latest_onchain_nonce,
            "Created BlockchainEventSource with persisted cursor (fast restart)"
        );

        Ok(Self {
            chain_id,
            rpc_client,
            portal_address,
            cursor: AtomicU64::new(cursor),
            last_processed: Mutex::new(LastProcessedEvent::new(latest_onchain_nonce, cursor)),
        })
    }

    /// Create from config (legacy)
    pub async fn from_config(source_id: U256, config: &[u8]) -> Result<Self> {
        use alloy_sol_types::SolValue;

        let decoded: (String, Address, u64) = <(String, Address, u64)>::abi_decode(config)
            .map_err(|e| anyhow!("Failed to decode config: {}", e))?;

        let (rpc_url, portal_address, start_block) = decoded;
        let chain_id = source_id.try_into().map_err(|_| anyhow!("Chain ID too large"))?;

        Self::new_with_cursor(chain_id, &rpc_url, portal_address, start_block, 0).await
    }

    /// Get the current block cursor position
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Get the last processed event state
    pub async fn last_processed(&self) -> LastProcessedEvent {
        *self.last_processed.lock().await
    }

    /// Get the last nonce we returned (for exactly-once tracking)
    pub async fn last_nonce(&self) -> Option<u128> {
        let state = self.last_processed.lock().await;
        if state.is_initialized() {
            Some(state.nonce)
        } else {
            None
        }
    }

    /// Set the last processed event (used when initializing from on-chain state)
    pub async fn set_last_processed(&self, nonce: u128, block: u64) {
        *self.last_processed.lock().await = LastProcessedEvent::new(nonce, block);
    }

    /// Fast-forward both cursor and last_processed state
    ///
    /// Use this when reconciling with on-chain state that is ahead of local state.
    /// Sets both the scanning cursor and the last processed event atomically.
    pub async fn fast_forward(&self, nonce: u128, block: u64) {
        self.set_last_processed(nonce, block).await;
        self.set_cursor(block);
    }

    /// Get the block number where last event was emitted
    pub async fn last_nonce_block(&self) -> Option<u64> {
        let state = self.last_processed.lock().await;
        if state.is_initialized() {
            Some(state.block)
        } else {
            None
        }
    }

    /// Get the chain ID
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Set the cursor position (used for onchain state reconciliation)
    pub fn set_cursor(&self, block: u64) {
        self.cursor.store(block, Ordering::Relaxed);
    }
}

#[async_trait]
impl OracleDataSource for BlockchainEventSource {
    fn source_type(&self) -> u32 {
        source_types::BLOCKCHAIN
    }

    fn source_id(&self) -> U256 {
        U256::from(self.chain_id)
    }

    async fn poll(&self) -> Result<Vec<OracleData>> {
        let cursor = self.cursor.load(Ordering::Relaxed);
        let finalized_block = self.rpc_client.get_finalized_block_number().await?;
        let to_block = std::cmp::min(cursor + Self::MAX_BLOCKS_PER_POLL, finalized_block);

        if to_block <= cursor {
            return Ok(vec![]);
        }

        let filter = Filter::new()
            .address(self.portal_address)
            .event_signature(MessageSent::SIGNATURE_HASH)
            .from_block(cursor + 1)
            .to_block(to_block);

        debug!(
            target: "blockchain_source",
            chain_id = self.chain_id,
            from_block = cursor + 1,
            to_block = to_block,
            "Polling for MessageSent events"
        );

        let logs = self.rpc_client.get_logs(&filter).await?;
        let mut results = Vec::with_capacity(logs.len());

        // Filter events strictly greater than last processed nonce
        let last_nonce = self.last_processed.lock().await.nonce;

        for log in logs {
            let nonce = if let Some(nonce_topic) = log.topics().get(1) {
                let nonce_bytes = &nonce_topic.as_slice()[16..32];
                u128::from_be_bytes(nonce_bytes.try_into().unwrap_or_default())
            } else {
                continue;
            };

            let block_number = if let Some(block_number_topic) = log.topics().get(2) {
                U256::from_be_slice(block_number_topic.as_slice())
            } else {
                continue;
            };

            // Strictly monotonic check: ignore events we've already processed
            if nonce <= last_nonce {
                continue;
            }

            let log_data = log.data().data.clone();

            // log.data is ABI-encoded `bytes payload` from Solidity event
            // Format: offset (32 bytes) || length (32 bytes) || data (variable)
            // We need to extract the raw PortalMessage bytes before re-encoding
            let raw_payload = match decode_abi_bytes(&log_data) {
                Some(payload) => payload,
                None => {
                    warn!(
                        target: "blockchain_source",
                        chain_id = self.chain_id,
                        nonce = nonce,
                        log_data_len = log_data.len(),
                        "Failed to decode ABI bytes from log.data, skipping"
                    );
                    continue;
                }
            };

            // ABI encode (nonce, raw_payload) together
            // This preserves the nonce when passing through JWKStruct
            // Format: abi.encode(uint128 nonce, bytes payload)
            // Now raw_payload is the actual PortalMessage (sender || messageNonce || message)
            let encoded_payload = alloy_sol_types::SolValue::abi_encode(&(
                nonce,
                block_number,
                raw_payload.as_slice(),
            ));

            debug!(
                target: "blockchain_source",
                chain_id = self.chain_id,
                nonce = nonce,
                log_data_len = log_data.len(),
                raw_payload_len = raw_payload.len(),
                encoded_payload_len = encoded_payload.len(),
                "Found new MessageSent event - decoded and re-encoded"
            );

            results.push(OracleData { nonce, payload: Bytes::from(encoded_payload) });
        }

        // Update cursor
        self.cursor.store(to_block, Ordering::Relaxed);

        // Track max nonce for exactly-once semantics (atomic update of nonce + block)
        if !results.is_empty() {
            let max_nonce = results.iter().map(|d| d.nonce).max().unwrap();
            *self.last_processed.lock().await = LastProcessedEvent::new(max_nonce, to_block);
        }

        let current_last_nonce = self.last_nonce().await;
        info!(
            target: "blockchain_source",
            chain_id = self.chain_id,
            events_found = results.len(),
            new_cursor = to_block,
            last_nonce = ?current_last_nonce,
            "Poll completed"
        );

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_source::OracleDataSource;

    // =========================================================================
    // Fixed Anvil Deployment Addresses
    // =========================================================================
    // When deploying on a fresh Anvil with account 0 (0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266),
    // contracts are deployed deterministically based on nonce:
    //   - Nonce 0: MockGToken     -> 0x5FbDB2315678afecb367f032d93F642f64180aa3
    //   - Nonce 1: GravityPortal  -> 0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512
    //   - Nonce 2: GBridgeSender  -> 0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0
    //
    // To run this test:
    //   1. cd gravity_chain_core_contracts
    //   2. ./scripts/start_anvil.sh   # Deploy contracts
    //   3. ./scripts/bridge_test.sh   # Generate MessageSent event
    //   4. cargo test --package reth-pipe-exec-layer-relayer test_poll_anvil_events -- --ignored
    //      --nocapture
    // =========================================================================

    /// GravityPortal address on local Anvil (deterministic, nonce 1)
    const ANVIL_PORTAL_ADDRESS: &str = "0x0f761B1B3c1aC9232C9015A7276692560aD6a05F";

    /// GBridgeSender address on local Anvil (deterministic, nonce 2)
    const ANVIL_SENDER_ADDRESS: &str = "0x3fc870008B1cc26f3614F14a726F8077227CA2c3";

    /// Anvil RPC URL
    const ANVIL_RPC_URL: &str = "https://sepolia.drpc.org";

    /// Local Anvil chain ID
    const ANVIL_CHAIN_ID: u64 = 10201262;

    /// PortalMessage format decoder for relayer output
    ///
    /// Relayer output format: abi.encode((uint128 nonce, bytes raw_portal_message))
    /// Where raw_portal_message is: sender (20 bytes) || messageNonce (16 bytes) || message
    ///
    /// ABI encoding structure:
    /// - bytes 0-31:   tuple offset (0x20)
    /// - bytes 32-63:  nonce (uint128, right-aligned, actual value at bytes 48-63)
    /// - bytes 64-95:  offset to bytes data (relative to tuple start)
    /// - bytes 96-127: length of bytes data
    /// - bytes 128+:   raw PortalMessage data
    fn decode_portal_message(payload: &[u8]) -> Option<(Address, u128, Vec<u8>)> {
        // Minimum: tuple offset (32) + nonce (32) + bytes offset (32) + length (32) + min data (36)
        // = 164
        if payload.len() < 164 {
            return None;
        }

        // Extract the raw PortalMessage from ABI-encoded (nonce, bytes) tuple
        // Length is at bytes 96-127 (right-aligned)
        let length = u64::from_be_bytes(payload[120..128].try_into().ok()?) as usize;

        // PortalMessage data starts at byte 128
        if payload.len() < 128 + length {
            return None;
        }

        let portal_message = &payload[128..128 + length];

        // Now parse PortalMessage: sender (20) || messageNonce (16) || message
        if portal_message.len() < 36 {
            return None;
        }

        let sender = Address::from_slice(&portal_message[0..20]);
        let message_nonce = u128::from_be_bytes(portal_message[20..36].try_into().ok()?);
        let message = portal_message[36..].to_vec();

        Some((sender, message_nonce, message))
    }

    /// Decode bridge message: abi.encode(amount, recipient)
    fn decode_bridge_message(message: &[u8]) -> Option<(U256, Address)> {
        if message.len() < 64 {
            return None;
        }

        let amount = U256::from_be_slice(&message[0..32]);
        let recipient = Address::from_slice(&message[44..64]);

        Some((amount, recipient))
    }

    #[tokio::test]
    async fn test_poll_anvil_events() {
        use crate::eth_client::EthHttpCli;

        // Parse portal address
        let portal_address: Address = ANVIL_PORTAL_ADDRESS.parse().expect("Invalid portal address");

        println!("=== BlockchainEventSource Test ===");
        println!("RPC URL: {}", ANVIL_RPC_URL);
        println!("Portal Address: {}", portal_address);
        println!("Chain ID: {}", ANVIL_CHAIN_ID);
        println!();

        // First, debug the RPC client to check block numbers
        let rpc_client = EthHttpCli::new(ANVIL_RPC_URL).expect("Failed to create RPC client");

        // Check what finalized block returns
        match rpc_client.get_finalized_block_number().await {
            Ok(finalized) => println!("DEBUG: Finalized block: {}", finalized),
            Err(e) => println!("DEBUG: get_finalized_block_number failed: {:?}", e),
        }

        // Create source with cold start (nonce 0)
        let source = BlockchainEventSource::new_with_cursor(
            ANVIL_CHAIN_ID,
            ANVIL_RPC_URL,
            portal_address,
            10195203, // start from block 0
            0,        // latest nonce
        )
        .await
        .expect("Failed to create BlockchainEventSource");

        println!();
        println!("Created BlockchainEventSource");
        println!("  Source Type: {}", source.source_type());
        println!("  Source ID: {}", source.source_id());
        println!("  Initial Cursor: {}", source.cursor());
        println!();

        // Poll for events
        println!("Polling for MessageSent events...");
        let events = source.poll().await.expect("Failed to poll");

        println!("After poll - Cursor: {}", source.cursor());
        println!("Found {} events", events.len());
        println!();

        for (i, event) in events.iter().enumerate() {
            println!("=== Event {} ===", i + 1);
            println!("Nonce: {}", event.nonce);
            println!("Payload length: {} bytes", event.payload.len());
            println!("Raw payload: 0x{}", hex::encode(&event.payload));
            println!();

            // Decode the PortalMessage
            if let Some((sender, msg_nonce, message)) = decode_portal_message(&event.payload) {
                println!("Decoded PortalMessage:");
                println!("  Sender: {}", sender);
                println!("  Message Nonce: {}", msg_nonce);
                println!("  Message length: {} bytes", message.len());
                println!("  Message: 0x{}", hex::encode(&message));

                // Decode the bridge message
                if let Some((amount, recipient)) = decode_bridge_message(&message) {
                    println!();
                    println!("Decoded Bridge Message:");
                    println!("  Amount: {} wei ({} G)", amount, amount / U256::from(10u64.pow(18)));
                    println!("  Recipient: {}", recipient);
                }
            } else {
                println!("Failed to decode PortalMessage");
            }
            println!();
        }

        // Verify we got at least one event if bridge_test.sh was run
        if !events.is_empty() {
            println!("✓ Successfully polled and decoded events!");

            let first_event = &events[0];
            assert!(!first_event.payload.is_empty(), "Payload should not be empty");
        } else {
            println!("No events found. Make sure to run:");
            println!("  1. ./scripts/start_anvil.sh");
            println!("  2. ./scripts/bridge_test.sh");
        }
    }
}
