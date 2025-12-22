//! Relayer for gravity protocol tasks

use crate::{
    eth_client::EthHttpCli,
    parser::{AccountActivityType, GravityTask, ParsedTask},
};
use alloy_primitives::{Address, B256};
use alloy_rpc_types::{BlockNumberOrTag, Filter, FilterBlockOption, Log};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolEvent, SolValue};
use anyhow::{anyhow, Result};
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// DepositGravityEvent(address user, uint256 amount, address targetValidator, uint256 blockNumber);
pub const DEPOSIT_GRAVITY_EVENT_SIGNATURE: [u8; 32] = [
    0xd5, 0x3b, 0xfb, 0x63, 0x0c, 0x04, 0x65, 0x4c, 0x6d, 0x1d, 0xa5, 0x02, 0x0f, 0x14, 0x67, 0x4f,
    0x19, 0x0f, 0x92, 0xc2, 0x57, 0xc9, 0x2d, 0x9b, 0x15, 0xd8, 0xec, 0xb4, 0x05, 0x05, 0x7c, 0x14,
];

/// ChangeRecord(bytes32 indexed key, bytes32 indexed value, uint256 blockNumber, address indexed
/// updater, uint256 sequenceNumber);
pub const CHANGE_RECORD_EVENT_SIGNATURE: [u8; 32] = [
    0xf6, 0x9d, 0x80, 0xcc, 0x71, 0xff, 0xd8, 0x74, 0x04, 0x59, 0x74, 0xba, 0x04, 0x6a, 0x0b, 0xee,
    0x23, 0x1a, 0xd0, 0x5e, 0xc4, 0x59, 0x0b, 0xdd, 0xe9, 0x85, 0x75, 0xcf, 0xe0, 0x7f, 0xd7, 0x66,
];

sol! {
    struct UnsupportedJWK {
        bytes id;
        bytes payload;
    }

    event DepositGravityEvent(
        address user,
        uint256 amount,
        address targetAddress,
        uint256 blockNumber
    );

    event ChangeRecord(
        bytes32 key,
        bytes32 value,
        uint256 blockNumber,
        address updater,
        uint256 sequenceNumber
    );
}
/// Represents the current state of observation for a gravity task
///
/// This struct tracks the block number and observed value of the last observed state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObserveState {
    /// The block number at which the observation was made
    pub block_number: u64,
    /// The actual observed value (block, events, storage slot, or none)
    pub observed_value: ObservedValue,
}

/// Result of a polling operation, containing the observed state and the maximum block queried
#[derive(Debug, Clone)]
pub struct PollResult {
    /// The observed state from this poll
    pub observed_state: ObserveState,
    /// The maximum block number that was actually queried in this poll
    pub max_queried_block: u64,
    /// Whether the observed state was updated
    pub updated: bool,
}

/// Represents different types of observed values from blockchain monitoring
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservedValue {
    /// Observed block information
    Block {
        /// Hash of the observed block
        block_hash: B256,
        /// Number of the observed block
        block_number: u64,
    },
    /// Observed event logs
    Events {
        /// Collection of event logs that were observed
        logs: Vec<EventLog>,
    },
    /// Observed storage slot value
    StorageSlot {
        /// Storage slot that was observed
        slot: B256,
        /// Value stored in the slot
        value: B256,
    },
    /// No observation made
    None,
}

/// Represents different types of event data that can be processed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDataType {
    /// Raw event data without specific parsing
    Raw,
    /// Deposit gravity event with structured data
    DepositGravityEvent,
    /// Change record event with structured data
    ChangeRecord,
}

/// Represents a blockchain event log with all relevant metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLog {
    /// Contract address that emitted the event
    pub address: Address,
    /// Event topics (indexed parameters)
    pub topics: Vec<B256>,
    /// Event data (non-indexed parameters)
    pub data: Vec<u8>,
    /// Block number where the event occurred
    pub block_number: u64,
    /// Transaction hash that triggered the event
    pub transaction_hash: B256,
    /// Log index within the transaction
    pub log_index: u64,
    /// Event data type identifier for categorizing events
    pub data_type: u8,
}

impl From<&Log> for EventLog {
    /// Converts an Alloy Log to an EventLog
    ///
    /// # Arguments
    /// * `log` - The Alloy Log to convert
    ///
    /// # Returns
    /// * `EventLog` - The converted event log
    fn from(log: &Log) -> Self {
        let mut event_log = Self {
            address: log.address(),
            topics: log.topics().to_vec(),
            data: log.data().data.to_vec(),
            block_number: log.block_number.unwrap_or_default(),
            transaction_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default(),
            data_type: 0,
        };

        // Automatically determine and set the event data type
        event_log.update_data_type();
        event_log
    }
}

impl EventLog {
    /// Determines the event data type based on the event signature (first topic)
    ///
    /// # Returns
    /// * `EventDataType` - The detected event data type
    pub fn determine_event_data_type(&self) -> EventDataType {
        // First check if we have topics (event signature)
        if self.topics.is_empty() {
            return EventDataType::Raw;
        }

        // Get the event signature from the first topic
        let event_signature = self.topics[0];

        // Match based on event signature using cached values
        if event_signature == DEPOSIT_GRAVITY_EVENT_SIGNATURE {
            let stake_event = DepositGravityEvent::abi_decode_data(&self.data).unwrap();
            info!(target: "relayer stake event",
                user=?stake_event.0,
                amount=?stake_event.1,
                target_validator=?stake_event.2,
                block_number=?stake_event.3,
                "relayer stake event created"
            );
            EventDataType::DepositGravityEvent
        } else if event_signature == CHANGE_RECORD_EVENT_SIGNATURE {
            // ChangeRecord has indexed parameters (key, value, updater) in topics and
            // non-indexed parameters (blockNumber, sequenceNumber) in data
            let change_record_data = ChangeRecord::abi_decode_data(&self.data).unwrap();
            // Extract indexed parameters from topics (topics[1] = key, topics[2] = value, topics[3]
            // = updater)
            let key = if self.topics.len() > 1 { Some(self.topics[1]) } else { None };
            let value = if self.topics.len() > 2 { Some(self.topics[2]) } else { None };
            let updater = if self.topics.len() > 3 {
                Some(Address::from_slice(&self.topics[3].0[12..]))
            } else {
                None
            };
            info!(target: "relayer change record event",
                key=?key,
                value=?value,
                block_number=?change_record_data.0,
                updater=?updater,
                sequence_number=?change_record_data.1,
                "relayer change record event created"
            );
            EventDataType::ChangeRecord
        } else {
            EventDataType::Raw
        }
    }

    /// Updates the data_type field based on the determined event type
    pub fn update_data_type(&mut self) {
        let event_type = self.determine_event_data_type();
        self.data_type = match event_type {
            EventDataType::Raw => 0,
            EventDataType::DepositGravityEvent => 1,
            EventDataType::ChangeRecord => 2,
        };
    }
}

impl Into<JWKStruct> for &EventLog {
    fn into(self) -> JWKStruct {
        let unsupported_jwk = UnsupportedJWK {
            id: self.data_type.to_string().into_bytes().into(),
            payload: self.data.clone().into(),
        };
        debug!(target: "relayer", "generate unsupported_jwk: {:?}", unsupported_jwk.abi_encode());
        JWKStruct {
            type_name: "0x1::jwks::UnsupportedJWK".to_string(),
            data: unsupported_jwk.abi_encode().into(),
        }
    }
}

/// Internal state for managing a gravity task
#[derive(Debug)]
struct TaskState {
    task: ParsedTask,
    cursor: Mutex<u64>,
    last_observed: Mutex<Arc<ObserveState>>,
}

impl TaskState {
    /// Creates a new TaskState instance
    ///
    /// # Arguments
    /// * `task` - The parsed task to manage
    /// * `start_block` - The block number to start monitoring from
    /// * `last_observed` - The last observed state
    ///
    /// # Returns
    /// * `TaskState` - The new task state instance
    fn new(task: ParsedTask, start_block: u64, last_observed: Arc<ObserveState>) -> Self {
        Self { task, cursor: Mutex::new(start_block), last_observed: Mutex::new(last_observed) }
    }

    /// Gets the current cursor position
    ///
    /// # Returns
    /// * `u64` - The current block number cursor
    async fn get_cursor(&self) -> u64 {
        *self.cursor.lock().await
    }

    /// Updates the cursor position
    ///
    /// # Arguments
    /// * `cursor` - The new cursor position
    async fn update_cursor(&self, cursor: u64) {
        *self.cursor.lock().await = cursor;
    }

    /// Gets the last observed state
    ///
    /// # Returns
    /// * `Arc<ObserveState>` - The last observed state
    async fn last_observed(&self) -> Arc<ObserveState> {
        self.last_observed.lock().await.clone()
    }

    /// Checks if the observed value should trigger an update
    ///
    /// # Arguments
    /// * `observed_value` - The newly observed value to compare
    ///
    /// # Returns
    /// * `bool` - True if the value has changed and should trigger an update
    async fn should_update(&self, observed_value: &ObservedValue) -> bool {
        self.last_observed().await.observed_value != *observed_value
    }

    /// Updates the last observed state
    ///
    /// # Arguments
    /// * `last_observed` - The new observed state
    async fn update_last_observed(&self, last_observed: ObserveState) {
        *self.last_observed.lock().await = Arc::new(last_observed);
    }
}

/// Main relayer for gravity protocol tasks
///
/// This struct handles the monitoring and polling of various blockchain events,
/// blocks, and storage slots based on parsed gravity tasks.
pub struct GravityRelayer {
    /// Ethereum client for blockchain communication
    eth_client: Arc<EthHttpCli>,
    /// Internal task state management
    task_state: TaskState,
}

impl std::fmt::Debug for GravityRelayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GravityRelayer")
            .field("eth_client", &self.eth_client)
            .field("task_state", &self.task_state)
            .finish()
    }
}

impl GravityRelayer {
    /// Maximum number of blocks to poll in one request to avoid overwhelming the system
    const MAX_BLOCKS_PER_POLL: u64 = 100;

    /// Calculates the appropriate block range for polling based on cursor and finalized block
    ///
    /// # Arguments
    /// * `cursor` - The current cursor position
    /// * `finalized_block` - The latest finalized block number
    ///
    /// # Returns
    /// * `u64` - The calculated to_block for polling
    fn calculate_poll_block_range(cursor: u64, finalized_block: u64) -> u64 {
        std::cmp::min(cursor + Self::MAX_BLOCKS_PER_POLL, finalized_block)
    }

    /// Extracts the from_block number from a Filter if it's a numeric block number
    ///
    /// # Arguments
    /// * `filter` - The filter to extract from_block from
    ///
    /// # Returns
    /// * `Option<u64>` - The block number if it's a numeric value, None otherwise
    fn extract_from_block_number(filter: &Filter) -> Option<u64> {
        match &filter.block_option {
            FilterBlockOption::Range { from_block, .. } => {
                if let Some(block_num) = from_block {
                    match block_num {
                        BlockNumberOrTag::Number(n) => Some(*n),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Creates a new GravityRelayer instance
    ///
    /// # Arguments
    /// * `rpc_url` - The RPC endpoint URL for blockchain communication
    /// * `task` - The parsed task to monitor
    ///
    /// # Returns
    /// * `Result<GravityRelayer>` - The new relayer instance or error
    ///
    /// # Errors
    /// * Returns an error if unable to connect to the RPC endpoint or get finalized block
    pub async fn new(rpc_url: &str, task: ParsedTask, from_block: u64) -> Result<Self> {
        let eth_client = Arc::new(EthHttpCli::new(rpc_url)?);

        // Get the starting block number from the task filter or use the provided from_block
        // parameter If both exist, use the maximum of the two
        let start_block_number = match &task.task {
            GravityTask::MonitorEvent(filter) => {
                if let Some(uri_from_block) = Self::extract_from_block_number(filter) {
                    // Use max of URI fromBlock and parameter from_block
                    std::cmp::max(uri_from_block, from_block)
                } else {
                    from_block
                }
            }
            _ => from_block,
        };
        let last_observed =
            ObserveState { block_number: start_block_number, observed_value: ObservedValue::None };

        info!(target: "relayer",
            rpc_url=?rpc_url,
            from_block=?start_block_number,
            task=?task,
            "relayer created"
        );
        let task_state = TaskState::new(task.clone(), start_block_number, Arc::new(last_observed));
        Ok(Self { eth_client, task_state })
    }

    /// Polls the current task once for updates
    ///
    /// This method delegates to specific polling methods based on the task type.
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result containing observed state and max queried block
    ///
    /// # Errors
    /// * Returns an error if polling fails for any reason
    pub async fn poll_once(&self) -> Result<PollResult> {
        let task_uri = &self.task_state.task.original_uri;
        let state = match &self.task_state.task.task {
            GravityTask::MonitorEvent(filter) => self.poll_event_task(task_uri, filter).await,
            GravityTask::MonitorBlockHead => self.poll_block_head_task(task_uri).await,
            GravityTask::MonitorStorage { account, slot } => {
                self.poll_storage_slot_task(task_uri, *account, *slot).await
            }
            GravityTask::MonitorAccount { address, activity_type } => {
                self.poll_account_activity_task(task_uri, *address, activity_type).await
            }
        };
        match state {
            Ok(poll_result) => match poll_result.observed_state.observed_value {
                ObservedValue::None => Err(anyhow!("Fetched none")),
                _ => Ok(poll_result),
            },
            Err(e) => {
                error!("Error polling task {}: {}", task_uri, e);
                Err(e)
            }
        }
    }

    /// Converts an observed state into JWK structures for Gravity protocol
    ///
    /// # Arguments
    /// * `observed_state` - The observed state to convert
    ///
    /// # Returns
    /// * `Result<Vec<JWKStruct>>` - A vector of JWK structures or error
    ///
    /// # Errors
    /// * Returns an error if serialization fails
    pub async fn convert_specific_observed_value(
        observed_state: ObserveState,
    ) -> Result<Vec<JWKStruct>> {
        let jwk = match observed_state.observed_value {
            ObservedValue::Events { logs } => logs.iter().map(|log| log.into()).collect(),
            _ => {
                vec![JWKStruct {
                    type_name: "0".to_string(),
                    data: serde_json::to_vec(&observed_state).expect("failed to serialize state"),
                }]
            }
        };
        Ok(jwk)
    }

    /// Polls for event logs based on the provided filter
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    /// * `filter` - The event filter to apply
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result with observed state and max queried block
    async fn poll_event_task(&self, task_uri: &str, filter: &Filter) -> Result<PollResult> {
        let cursor = self.task_state.get_cursor().await;
        let previous_value = self.task_state.last_observed().await;

        let mut scoped_filter = filter.clone();
        scoped_filter = scoped_filter.from_block(cursor);

        // Get finalized block with retry logic
        let finalized_block = self.eth_client.get_finalized_block_number().await?;

        // Calculate the appropriate block range for polling
        let to_block = Self::calculate_poll_block_range(cursor, finalized_block);
        scoped_filter = scoped_filter.to_block(to_block);

        debug!(target: "relayer",
            task_uri=?task_uri,
            scoped_filter=?scoped_filter,
            "polling event task"
        );
        let logs = self.eth_client.get_logs(&scoped_filter).await?;

        let new_logs: Vec<EventLog> = logs
            .iter()
            .filter(|log| log.block_number.unwrap_or(0) > cursor)
            .map(|log| log.into())
            .collect();

        if new_logs.is_empty() {
            // Update cursor to the to_block we actually queried
            let next_cursor = to_block;
            self.task_state.update_cursor(next_cursor).await;
            debug!(target: "relayer",
                task_uri=?task_uri,
                next_cursor=?next_cursor,
                "polling event task with no new logs"
            );
            // Return previous value with max_queried_block
            return Ok(PollResult {
                observed_state: (*previous_value).clone(),
                max_queried_block: to_block,
                updated: false,
            });
        }

        let observed_value = ObservedValue::Events { logs: new_logs.clone() };

        let should_update = self.task_state.should_update(&observed_value).await;

        let return_value = if should_update {
            // Update cursor to the to_block we actually queried, not just the max log block
            let new_cursor = to_block;
            self.task_state.update_cursor(new_cursor).await;
            let new_value =
                ObserveState { block_number: new_cursor, observed_value: observed_value.clone() };

            self.task_state.update_last_observed(new_value.clone()).await;
            PollResult { observed_state: new_value, max_queried_block: to_block, updated: true }
        } else {
            // Even if no update, we should still advance the cursor to avoid getting stuck
            self.task_state.update_cursor(to_block).await;
            PollResult {
                observed_state: (*previous_value).clone(),
                max_queried_block: to_block,
                updated: false,
            }
        };

        debug!(target: "relayer",
            task_uri=?task_uri,
            cursor=?cursor,
            should_update=?should_update,
            "polling event task completed"
        );
        Ok(return_value)
    }

    /// Polls for the latest block head
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result with observed state and max queried block
    async fn poll_block_head_task(&self, task_uri: &str) -> Result<PollResult> {
        let finalized_block = self.eth_client.get_finalized_block_number().await?;

        let cursor = self.task_state.get_cursor().await;
        let previous_value = self.task_state.last_observed().await;

        // For block head polling, we only need to check the latest finalized block
        let to_block = Self::calculate_poll_block_range(cursor, finalized_block);

        let return_value = if to_block > cursor {
            let block_hash = match self.eth_client.get_block(to_block).await? {
                Some(block) => block.header.hash,
                None => B256::ZERO,
            };

            let observed_value = ObservedValue::Block { block_hash, block_number: to_block };

            let should_update = self.task_state.should_update(&observed_value).await;

            if should_update {
                self.task_state.update_cursor(to_block).await;
                let new_value =
                    ObserveState { block_number: to_block, observed_value: observed_value.clone() };

                self.task_state.update_last_observed(new_value.clone()).await;
                PollResult { observed_state: new_value, max_queried_block: to_block, updated: true }
            } else {
                // Even if no update, we should still advance the cursor to avoid getting stuck
                self.task_state.update_cursor(to_block).await;
                PollResult {
                    observed_state: (*previous_value).clone(),
                    max_queried_block: to_block,
                    updated: false,
                }
            }
        } else {
            PollResult {
                observed_state: (*previous_value).clone(),
                max_queried_block: to_block,
                updated: false,
            }
        };

        debug!(target: "relayer",
            task_uri=?task_uri,
            cursor=?cursor,
            "polling block head task completed"
        );
        Ok(return_value)
    }

    /// Polls for storage slot value changes
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    /// * `account` - The contract address to monitor
    /// * `slot` - The storage slot to monitor
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result with observed state and max queried block
    async fn poll_storage_slot_task(
        &self,
        task_uri: &str,
        account: Address,
        slot: B256,
    ) -> Result<PollResult> {
        let cursor = self.task_state.get_cursor().await;
        let finalized_block = self.eth_client.get_finalized_block_number().await?;

        // For storage slot polling, we only need to check the latest finalized block
        let to_block = Self::calculate_poll_block_range(cursor, finalized_block);

        let current_value = self.eth_client.get_storage_at(account, slot).await?;
        let observed_value = ObservedValue::StorageSlot { slot, value: current_value };

        let should_update = self.task_state.should_update(&observed_value).await;
        let previous_value = self.task_state.last_observed().await;

        let return_value = if should_update {
            self.task_state.update_cursor(to_block).await;
            let new_value =
                ObserveState { block_number: to_block, observed_value: observed_value.clone() };
            self.task_state.update_last_observed(new_value.clone()).await;
            PollResult { observed_state: new_value, max_queried_block: to_block, updated: true }
        } else {
            // Even if no update, we should still advance the cursor to avoid getting stuck
            self.task_state.update_cursor(to_block).await;
            PollResult {
                observed_state: (*previous_value).clone(),
                max_queried_block: to_block,
                updated: false,
            }
        };
        debug!(target: "relayer",
            task_uri=?task_uri,
            cursor=?cursor,
            "polling storage slot task completed"
        );
        Ok(return_value)
    }

    /// Polls for account activity based on the specified activity type
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    /// * `address` - The account address to monitor
    /// * `activity_type` - The type of activity to monitor
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result with observed state and max queried block
    async fn poll_account_activity_task(
        &self,
        _task_uri: &str,
        _address: Address,
        _activity_type: &AccountActivityType,
    ) -> Result<PollResult> {
        // TODO: Implement account activity monitoring
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex, Bytes, B256};
    use alloy_rpc_types::Filter;
    use reth_primitives::Log;

    use crate::{
        relayer::DepositGravityEvent, EthHttpCli, GravityRelayer, ObservedValue, UriParser,
    };
    use alloy_sol_macro::sol;
    use alloy_sol_types::SolEvent;

    sol! {
        contract USDC {
            event USDCTransfer(
                address indexed from,
                address indexed to,
                uint256 amount,
                uint256 timestamp
            );

            event ChangeRecord(
                bytes32 key,
                bytes32 value,
                uint256 blockNumber,
                address updater,
                uint256 sequenceNumber
            );
        }
    }

    #[tokio::test]
    async fn test_parsed_and_run() {
        let uri = std::env::var("TEST_URI")
            .expect("TEST_URI environment variable must be set for this test");
        let rpc_url = std::env::var("RPC_URL")
            .expect("RPC_URL environment variable must be set for this test");

        // let uri = "gravity://31337/event?address=0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512&
        // topic0=0x3915136b10c16c5f181f4774902f3baf9e44a5f700cabf5c826ee1caed313624";
        let parser = UriParser::new();
        let task = parser.parse(&uri).expect("Failed to parse test URI");
        println!("task: {:?}", task);

        let relayer =
            GravityRelayer::new(&rpc_url, task, 0).await.expect("Failed to create relayer");

        let state = relayer.poll_once().await.expect("Failed to poll relayer");
        println!("state: {:?}", state);

        match state.observed_state.observed_value {
            ObservedValue::Events { logs } => {
                for log in logs {
                    let log_obj = Log::new(log.address, log.topics, Bytes::from(log.data))
                        .expect("Failed to create log object");
                    let decoded = USDC::USDCTransfer::decode_log(&log_obj)
                        .expect("Failed to decode USDC transfer event");

                    let data = decoded.data;
                    let from = data.from;
                    let to = data.to;
                    let amount = data.amount;
                    let timestamp = data.timestamp;
                    println!(
                        "from: {:?}, to: {:?}, amount: {:?}, timestamp: {:?}",
                        from, to, amount, timestamp
                    );
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_change_record_parsed_and_run() {
        // URI components - modify these values as needed
        let chain_id = "31337";
        let contract_address = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
        let topic0 = "0xf69d80cc71ffd874045974ba046a0bee231ad05ec4590bdde98575cfe07fd766"; // ChangeRecord event signature
        let from_block = "120";

        // Construct URI from components
        let uri = format!(
            "gravity://{}/event?address={}&topic0={}&fromBlock={}",
            chain_id, contract_address, topic0, from_block
        );
        let rpc_url = "http://localhost:8545".to_string();

        // ChangeRecord event signature: ChangeRecord(bytes32,bytes32,uint256,address,uint256)
        // topic0: 0xf69d80cc71ffd874045974ba046a0bee231ad05ec4590bdde98575cfe07fd766
        let parser = UriParser::new();
        let task = parser.parse(&uri).expect("Failed to parse test URI");
        println!("task: {:?}", task);

        let relayer =
            GravityRelayer::new(&rpc_url, task, 0).await.expect("Failed to create relayer");

        let state = relayer.poll_once().await.expect("Failed to poll relayer");
        println!("state: {:?}", state);

        match state.observed_state.observed_value {
            ObservedValue::Events { logs } => {
                for log in logs {
                    let log_obj = Log::new(log.address, log.topics, Bytes::from(log.data))
                        .expect("Failed to create log object");
                    let decoded = USDC::ChangeRecord::decode_log(&log_obj)
                        .expect("Failed to decode ChangeRecord event");

                    let data = decoded.data;
                    let key = data.key;
                    let value = data.value;
                    let block_number = data.blockNumber;
                    let updater = data.updater;
                    let sequence_number = data.sequenceNumber;
                    println!(
                        "key: {:?}, value: {:?}, block_number: {:?}, updater: {:?}, sequence_number: {:?}",
                        key, value, block_number, updater, sequence_number
                    );
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_direct() {
        // Create mock eth client - this needs actual test implementation
        let rpc_url = "https://sepolia.drpc.org".to_string();
        let eth_client = EthHttpCli::new(&rpc_url).expect("Failed to create ETH client");

        let deposit_gravity_event_signature: [u8; 32] = [
            0xd5, 0x3b, 0xfb, 0x63, 0x0c, 0x04, 0x65, 0x4c, 0x6d, 0x1d, 0xa5, 0x02, 0x0f, 0x14,
            0x67, 0x4f, 0x19, 0x0f, 0x92, 0xc2, 0x57, 0xc9, 0x2d, 0x9b, 0x15, 0xd8, 0xec, 0xb4,
            0x05, 0x05, 0x7c, 0x14,
        ];
        let filter = Filter::new()
            .address(address!("0x283fC6799867BF96bF862a05BDade3EE89132027"))
            .event_signature(B256::from(deposit_gravity_event_signature))
            .from_block(9565280)
            .to_block(9565290);

        let logs = eth_client.get_logs(&filter).await.expect("Failed to get logs");
        println!("logs: {:?}", logs);

        for log in logs {
            let decoded = log.log_decode::<DepositGravityEvent>().expect("Failed to decode log");
            let data = decoded.data();
            let user = data.user;
            let amount = data.amount;
            let target_address = data.targetAddress;
            let block_number = data.blockNumber;
            println!(
                "user: {:?}, amount: {:?}, target_validator: {:?}, block_number: {:?}",
                user, amount, target_address, block_number
            );
        }
    }
}
