//! Relayer for gravity protocol tasks

use crate::{
    eth_client::EthHttpCli,
    parser::{AccountActivityType, GravityTask, ParsedTask},
};
use alloy_primitives::{hex, Address, B256};
use alloy_rpc_types::{Filter, Log};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Represents the current state of observation for a gravity task
///
/// This struct tracks the block number, observed value, timestamp, and version
/// of the last observed state for monitoring purposes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObserveState {
    /// The block number at which the observation was made
    pub block_number: u64,
    /// The actual observed value (block, events, storage slot, or none)
    pub observed_value: ObservedValue,
}

/// Represents different types of observed values from blockchain monitoring
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservedValue {
    /// Observed block information
    Block { block_hash: B256, block_number: u64 },
    /// Observed event logs
    Events { logs: Vec<EventLog> },
    /// Observed storage slot value
    StorageSlot { slot: B256, value: B256 },
    /// No observation made
    None,
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
        Self {
            address: log.address(),
            topics: log.topics().to_vec(),
            data: log.data().data.to_vec(),
            block_number: log.block_number.unwrap_or_default(),
            transaction_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default(),
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
    pub async fn new(rpc_url: &str, task: ParsedTask) -> Result<Self> {
        let eth_client = Arc::new(EthHttpCli::new(rpc_url)?);

        // Retry getting the finalized block number with exponential backoff
        let start_block_number = eth_client.get_finalized_block_number().await?;

        let last_observed =
            ObserveState { block_number: start_block_number, observed_value: ObservedValue::None };

        let task_state = TaskState::new(task.clone(), start_block_number, Arc::new(last_observed));
        Ok(Self { eth_client, task_state })
    }

    /// Polls the current task once for updates
    ///
    /// This method delegates to specific polling methods based on the task type.
    ///
    /// # Returns
    /// * `Result<ObserveState>` - The current observed state or error
    ///
    /// # Errors
    /// * Returns an error if polling fails for any reason
    pub async fn poll_once(&self) -> Result<ObserveState> {
        let task_uri = &self.task_state.task.original_uri;
        match &self.task_state.task.task {
            GravityTask::MonitorEvent(filter) => self.poll_event_task(task_uri, filter).await,
            GravityTask::MonitorBlockHead => self.poll_block_head_task(task_uri).await,
            GravityTask::MonitorStorage { account, slot } => {
                self.poll_storage_slot_task(task_uri, *account, *slot).await
            }
            GravityTask::MonitorAccount { address, activity_type } => {
                self.poll_account_activity_task(task_uri, *address, activity_type).await
            }
        }
    }

    /// Polls for event logs based on the provided filter
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    /// * `filter` - The event filter to apply
    ///
    /// # Returns
    /// * `Result<ObserveState>` - The observed state with new events or error
    async fn poll_event_task(&self, task_uri: &str, filter: &Filter) -> Result<ObserveState> {
        let cursor = self.task_state.get_cursor().await;
        let previous_value = self.task_state.last_observed().await;

        let mut scoped_filter = filter.clone();
        scoped_filter = scoped_filter.from_block(cursor);

        // Get finalized block with retry logic
        let finalized_block = self.eth_client.get_finalized_block_number().await?;
        scoped_filter = scoped_filter.to_block(finalized_block);

        debug!("Polling event task {} with filter: {:?}", task_uri, scoped_filter);
        let logs = self.eth_client.get_logs(&scoped_filter).await?;

        let new_logs: Vec<EventLog> = logs
            .iter()
            .filter(|log| log.block_number.unwrap_or(0) > cursor)
            .map(|log| log.into())
            .collect();

        if new_logs.is_empty() {
            // Use finalized_block as the next cursor if no to_block is specified
            let next_cursor = scoped_filter.get_to_block().unwrap_or(finalized_block);
            self.task_state.update_cursor(next_cursor).await;
            debug!("Polling event task {} with no new logs, cursor: {}", task_uri, next_cursor);
            return Ok((*previous_value).clone());
        }

        let observed_value = ObservedValue::Events { logs: new_logs.clone() };

        let should_update = self.task_state.should_update(&observed_value).await;

        let return_value = if should_update {
            let new_cursor =
                new_logs.iter().max_by_key(|log| log.block_number).unwrap().block_number;
            self.task_state.update_cursor(new_cursor).await;
            let new_value =
                ObserveState { block_number: new_cursor, observed_value: observed_value.clone() };

            self.task_state.update_last_observed(new_value.clone()).await;
            new_value
        } else {
            (*previous_value).clone()
        };

        debug!("Polling event task {} completed, cursor: {}", task_uri, cursor);
        Ok(return_value)
    }

    /// Polls for the latest block head
    ///
    /// # Arguments
    /// * `task_uri` - The URI being monitored (for logging)
    ///
    /// # Returns
    /// * `Result<ObserveState>` - The observed state with new block or error
    async fn poll_block_head_task(&self, task_uri: &str) -> Result<ObserveState> {
        let latest_block = self.eth_client.get_finalized_block_number().await?;

        let cursor = self.task_state.get_cursor().await;
        let previous_value = self.task_state.last_observed().await;

        let return_value = if latest_block > cursor {
            let block_hash = match self.eth_client.get_block(latest_block).await? {
                Some(block) => block.header.hash,
                None => B256::ZERO,
            };

            let observed_value = ObservedValue::Block { block_hash, block_number: latest_block };

            let should_update = self.task_state.should_update(&observed_value).await;

            if should_update {
                self.task_state.update_cursor(latest_block).await;
                let new_value = ObserveState {
                    block_number: latest_block,
                    observed_value: observed_value.clone(),
                };

                self.task_state.update_last_observed(new_value.clone()).await;
                new_value
            } else {
                (*previous_value).clone()
            }
        } else {
            (*previous_value).clone()
        };

        debug!("Polling block head task {} completed, cursor: {}", task_uri, cursor);
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
    /// * `Result<ObserveState>` - The observed state with storage value or error
    async fn poll_storage_slot_task(
        &self,
        task_uri: &str,
        account: Address,
        slot: B256,
    ) -> Result<ObserveState> {
        let current_value = self.eth_client.get_storage_at(account, slot).await?;

        let cursor = self.task_state.get_cursor().await;

        let current_block = self.eth_client.get_finalized_block_number().await?;

        let observed_value = ObservedValue::StorageSlot { slot, value: current_value };

        let should_update = self.task_state.should_update(&observed_value).await;

        let previous_value = self.task_state.last_observed().await;

        let return_value = if should_update {
            self.task_state.update_cursor(current_block).await;
            let new_value = ObserveState {
                block_number: current_block,
                observed_value: observed_value.clone(),
            };
            self.task_state.update_last_observed(new_value.clone()).await;
            new_value
        } else {
            (*previous_value).clone()
        };
        debug!("Polling storage slot task {} completed, cursor: {}", task_uri, cursor);
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
    /// * `Result<ObserveState>` - The observed state with account activity or error
    async fn poll_account_activity_task(
        &self,
        task_uri: &str,
        address: Address,
        activity_type: &AccountActivityType,
    ) -> Result<ObserveState> {
        match activity_type {
            AccountActivityType::Erc20Transfer => {
                // Create ERC20 Transfer event filter
                // Transfer event signature: Transfer(address,address,uint256)
                let transfer_topic = B256::from(hex!(
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                ));

                // This filter construction needs proper OR logic implementation
                // For now, create a basic filter
                let filter = Filter::new().event_signature(transfer_topic);
                // TODO: Implement proper OR logic for topic1/topic2 to monitor both from and to

                self.poll_event_task(task_uri, &filter).await
            }
            AccountActivityType::AllTransactions => {
                // Requiring iterating through all transactions in blocks, which is performance
                // intensive
                warn!("AllTransactions monitoring is not yet implemented for address: {}", address);
                Ok(ObserveState { block_number: 0, observed_value: ObservedValue::None })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex, Bytes, B256};
    use alloy_rpc_types::Filter;
    use reth_primitives::Log;

    use crate::{EthHttpCli, GravityRelayer, ObservedValue, UriParser};
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

        let relayer = GravityRelayer::new(&rpc_url, task).await.expect("Failed to create relayer");

        let state = relayer.poll_once().await.expect("Failed to poll relayer");
        println!("state: {:?}", state);

        match state.observed_value {
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
    async fn test_direct() {
        // Create mock eth client - this needs actual test implementation
        let rpc_url = std::env::var("RPC_URL")
            .expect("RPC_URL environment variable must be set for this test");
        let eth_client = EthHttpCli::new(&rpc_url).expect("Failed to create ETH client");

        let filter = Filter::new()
            .address(address!("0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"))
            .event_signature(B256::from(hex!(
                "0x3915136b10c16c5f181f4774902f3baf9e44a5f700cabf5c826ee1caed313624"
            )))
            .from_block(10)
            .to_block(100);

        let logs = eth_client.get_logs(&filter).await.expect("Failed to get logs");
        println!("logs: {:?}", logs);

        for log in logs {
            let decoded = log.log_decode::<USDC::USDCTransfer>().expect("Failed to decode log");
            let data = decoded.data();
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
}
