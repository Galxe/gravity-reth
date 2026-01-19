//! Blockchain Event Source
//!
//! Monitors cross-chain events from EVM chains, specifically GravityPortal.MessageSent.

use crate::{
    data_source::{source_types, OracleData, OracleDataSource},
    eth_client::EthHttpCli,
};
use alloy_primitives::{Address, Bytes, U256};
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
    /// MessageSent(uint128 indexed nonce, bytes payload)
    event MessageSent(uint128 indexed nonce, bytes payload);
}

/// Blockchain event source for monitoring GravityPortal.MessageSent events
///
/// This is the primary data source for cross-chain message bridging.
/// Tracks `last_returned_nonce` to ensure exactly-once consumption semantics.
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

    /// Last nonce we returned to caller (for exactly-once tracking)
    last_returned_nonce: Mutex<u128>,
}

impl BlockchainEventSource {
    /// Maximum blocks to poll in one request
    const MAX_BLOCKS_PER_POLL: u64 = 100;

    /// Chunk size for backward search (stays within RPC limits)
    const DISCOVERY_CHUNK_SIZE: u64 = 500;

    /// Create a new BlockchainEventSource with auto-discovery
    ///
    /// # Arguments
    /// * `chain_id` - The EVM chain ID
    /// * `rpc_url` - RPC endpoint URL
    /// * `portal_address` - GravityPortal contract address
    /// * `config_start_block` - Static start block from config (fallback)
    /// * `latest_onchain_nonce` - The latest nonce recorded on Gravity chain
    pub async fn new_with_discovery(
        chain_id: u64,
        rpc_url: &str,
        portal_address: Address,
        config_start_block: u64,
        latest_onchain_nonce: u128,
    ) -> Result<Self> {
        let rpc_client = Arc::new(EthHttpCli::new(rpc_url)?);

        // Auto-discover start block based on latest_onchain_nonce
        let start_block = if latest_onchain_nonce > 0 {
            match Self::discover_start_block(
                &rpc_client,
                portal_address,
                latest_onchain_nonce,
                config_start_block,
            )
            .await
            {
                Ok(block) => {
                    info!(
                        target: "blockchain_source",
                        chain_id,
                        latest_nonce = latest_onchain_nonce,
                        discovered_block = block,
                        "Auto-discovered start block from event log"
                    );
                    block
                }
                Err(e) => {
                    // Fallback to config_start_block if discovery fails
                    warn!(
                        target: "blockchain_source",
                        chain_id,
                        error = ?e,
                        config_start_block,
                        "Failed to discover start block, using config start block"
                    );
                    config_start_block
                }
            }
        } else {
            // Cold start (nonce 0): use config
            info!(
                target: "blockchain_source",
                chain_id,
                config_start_block,
                "Cold start (nonce 0), using config start block"
            );
            config_start_block
        };

        info!(
            target: "blockchain_source",
            chain_id = chain_id,
            portal_address = ?portal_address,
            final_start_block = start_block,
            latest_onchain_nonce = latest_onchain_nonce,
            "Created BlockchainEventSource"
        );

        Ok(Self {
            chain_id,
            rpc_client,
            portal_address,
            cursor: AtomicU64::new(start_block),
            last_returned_nonce: Mutex::new(latest_onchain_nonce),
        })
    }

    /// Discover the block number where the event with `target_nonce` occurred
    ///
    /// Uses chunked backward search from finalized block to config_start_block.
    /// Each query is limited to DISCOVERY_CHUNK_SIZE blocks to stay within RPC limits.
    async fn discover_start_block(
        client: &EthHttpCli,
        address: Address,
        target_nonce: u128,
        config_start_block: u64,
    ) -> Result<u64> {
        let finalized = client.get_finalized_block_number().await?;

        if finalized <= config_start_block {
            return Err(anyhow!(
                "Finalized block {} <= config_start_block {}",
                finalized,
                config_start_block
            ));
        }

        let nonce_topic = U256::from(target_nonce);
        let mut to_block = finalized;
        let mut chunks_searched = 0u32;

        // Search backwards in chunks
        while to_block > config_start_block {
            let from_block =
                to_block.saturating_sub(Self::DISCOVERY_CHUNK_SIZE).max(config_start_block);

            let filter = Filter::new()
                .address(address)
                .event_signature(MessageSent::SIGNATURE_HASH)
                .topic1(nonce_topic)
                .from_block(from_block)
                .to_block(to_block);

            match client.get_logs(&filter).await {
                Ok(logs) => {
                    if let Some(log) = logs.first() {
                        if let Some(block_number) = log.block_number {
                            info!(
                                target: "blockchain_source",
                                target_nonce,
                                block_number,
                                chunks_searched,
                                "Found target nonce in event log"
                            );
                            return Ok(block_number);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        target: "blockchain_source",
                        from_block,
                        to_block,
                        error = ?e,
                        "RPC get_logs failed, continuing search"
                    );
                }
            }

            to_block = from_block;
            chunks_searched += 1;

            // Log progress for long searches
            if chunks_searched % 10 == 0 {
                debug!(
                    target: "blockchain_source",
                    chunks_searched,
                    current_block = to_block,
                    "Still searching for nonce..."
                );
            }
        }

        Err(anyhow!(
            "Event with nonce {} not found in range [{}, {}]",
            target_nonce,
            config_start_block,
            finalized
        ))
    }

    /// Legacy new method for compatibility (will call new_with_discovery with nonce 0)
    pub async fn new(
        chain_id: u64,
        rpc_url: &str,
        portal_address: Address,
        start_block: u64,
    ) -> Result<Self> {
        Self::new_with_discovery(chain_id, rpc_url, portal_address, start_block, 0).await
    }

    /// Create from config (legacy)
    pub async fn from_config(source_id: U256, config: &[u8]) -> Result<Self> {
        use alloy_sol_types::SolValue;

        let decoded: (String, Address, u64) = <(String, Address, u64)>::abi_decode(config)
            .map_err(|e| anyhow!("Failed to decode config: {}", e))?;

        let (rpc_url, portal_address, start_block) = decoded;
        let chain_id = source_id.try_into().map_err(|_| anyhow!("Chain ID too large"))?;

        Self::new(chain_id, &rpc_url, portal_address, start_block).await
    }

    /// Get the current block cursor position
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Get the last nonce we returned (for exactly-once tracking)
    pub async fn last_nonce(&self) -> Option<u128> {
        let n = *self.last_returned_nonce.lock().await;
        if n > 0 {
            Some(n)
        } else {
            None
        }
    }

    /// Set the last nonce (used when initializing from on-chain state)
    pub async fn set_last_nonce(&self, nonce: u128) {
        *self.last_returned_nonce.lock().await = nonce;
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

        // Filter events strictly greater than last_returned_nonce
        let last_nonce = *self.last_returned_nonce.lock().await;

        for log in logs {
            let nonce = if let Some(nonce_topic) = log.topics().get(1) {
                let nonce_bytes = &nonce_topic.as_slice()[16..32];
                u128::from_be_bytes(nonce_bytes.try_into().unwrap_or_default())
            } else {
                continue;
            };

            // Strictly monotonic check: ignore events we've already processed
            if nonce <= last_nonce {
                continue;
            }

            let payload = log.data().data.clone();

            debug!(
                target: "blockchain_source",
                chain_id = self.chain_id,
                nonce = nonce,
                payload_len = payload.len(),
                "Found new MessageSent event"
            );

            results.push(OracleData { nonce, payload: Bytes::from(payload.to_vec()) });
        }

        // Update cursor
        self.cursor.store(to_block, Ordering::Relaxed);

        // Track max nonce for exactly-once semantics
        if !results.is_empty() {
            let max_nonce = results.iter().map(|d| d.nonce).max().unwrap();
            *self.last_returned_nonce.lock().await = max_nonce;
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
    //   1. cd /home/jingyue/projects/gravity_chain_core_contracts
    //   2. ./scripts/start_anvil.sh   # Deploy contracts
    //   3. ./scripts/bridge_test.sh   # Generate MessageSent event
    //   4. cargo test --package reth-pipe-exec-layer-relayer test_poll_anvil_events -- --ignored
    //      --nocapture
    // =========================================================================

    /// GravityPortal address on local Anvil (deterministic, nonce 1)
    const ANVIL_PORTAL_ADDRESS: &str = "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512";

    /// GBridgeSender address on local Anvil (deterministic, nonce 2)
    const ANVIL_SENDER_ADDRESS: &str = "0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0";

    /// Anvil RPC URL
    const ANVIL_RPC_URL: &str = "http://localhost:8546";

    /// Local Anvil chain ID
    const ANVIL_CHAIN_ID: u64 = 31337;

    /// PortalMessage format decoder
    /// Payload format: sender (20 bytes) || nonce (16 bytes) || message (variable)
    fn decode_portal_message(payload: &[u8]) -> Option<(Address, u128, Vec<u8>)> {
        if payload.len() < 36 {
            return None;
        }

        // The payload is ABI-encoded as `bytes`, so first decode the outer wrapper
        // ABI encoding: offset (32 bytes) || length (32 bytes) || data
        if payload.len() < 64 {
            return None;
        }

        // Read offset and length
        let offset = u64::from_be_bytes(payload[24..32].try_into().ok()?) as usize;
        if offset + 32 > payload.len() {
            return None;
        }

        let length =
            u64::from_be_bytes(payload[offset + 24..offset + 32].try_into().ok()?) as usize;
        let data_start = offset + 32;
        if data_start + length > payload.len() {
            return None;
        }

        let inner_data = &payload[data_start..data_start + length];

        // Now parse PortalMessage: sender (20) || nonce (16) || message
        if inner_data.len() < 36 {
            return None;
        }

        let sender = Address::from_slice(&inner_data[0..20]);
        let nonce = u128::from_be_bytes(inner_data[20..36].try_into().ok()?);
        let message = inner_data[36..].to_vec();

        Some((sender, nonce, message))
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
        use std::sync::Arc;

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
        let source = BlockchainEventSource::new(
            ANVIL_CHAIN_ID,
            ANVIL_RPC_URL,
            portal_address,
            0, // start from block 0
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

    #[tokio::test]
    #[ignore] // Requires Anvil running
    async fn test_source_creation() {
        let portal_address: Address = ANVIL_PORTAL_ADDRESS.parse().unwrap();

        let source =
            BlockchainEventSource::new(ANVIL_CHAIN_ID, ANVIL_RPC_URL, portal_address, 0).await;

        assert!(source.is_ok(), "Should create source successfully");

        let source = source.unwrap();
        assert_eq!(source.source_type(), source_types::BLOCKCHAIN);
        assert_eq!(source.source_id(), U256::from(ANVIL_CHAIN_ID));
    }
}
