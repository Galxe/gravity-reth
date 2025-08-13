//! Relayer for gravity protocol tasks

use crate::eth_client::EthHttpCli;
use crate::parser::{AccountActivityType, GravityTask, ParsedTask};
use alloy_primitives::{hex, Address, B256};
use alloy_rpc_types::{Filter, Log};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObserveState {
    pub block_number: u64,
    pub observed_value: ObservedValue,
    pub timestamp: u64,
    /// OnChain version
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservedValue {
    Block { block_hash: B256, block_number: u64 },
    Events { logs: Vec<EventLog> },
    StorageSlot { slot: B256, value: B256 },
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    pub block_number: u64,
    pub transaction_hash: B256,
    pub log_index: u64,
}

impl From<&Log> for EventLog {
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

#[derive(Debug)]
struct TaskState {
    task: ParsedTask,
    cursor: Mutex<u64>,
    last_observed: Mutex<Arc<ObserveState>>,
}

impl TaskState {
    fn new(task: ParsedTask, start_block: u64, last_observed: Arc<ObserveState>) -> Self {
        Self { task, cursor: Mutex::new(start_block), last_observed: Mutex::new(last_observed) }
    }

    async fn get_cursor(&self) -> u64 {
        *self.cursor.lock().await
    }

    async fn update_cursor(&self, cursor: u64) {
        *self.cursor.lock().await = cursor;
    }

    async fn last_observed(&self) -> Arc<ObserveState> {
        self.last_observed.lock().await.clone()
    }

    async fn should_update(&self, observed_value: &ObservedValue) -> bool {
        self.last_observed().await.observed_value != *observed_value
    }

    async fn update_last_observed(&self, last_observed: ObserveState) {
        *self.last_observed.lock().await = Arc::new(last_observed);
    }
}

/// GravityRelayer
pub struct GravityRelayer {
    /// ETH客户端
    eth_client: Arc<EthHttpCli>,
    /// 任务状态映射
    task_state: TaskState,
}

impl std::fmt::Debug for GravityRelayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GravityRelayer")
            .field("eth_client", &self.eth_client)
            .field("task_state", &"<task_state>")
            .finish()
    }
}

impl GravityRelayer {
    pub fn new(rpc_url: &str, task: ParsedTask, last_state: ObserveState) -> Self {
        let start_block = last_state.block_number;

        let task_state = TaskState::new(task.clone(), start_block, Arc::new(last_state));
        let eth_client = Arc::new(EthHttpCli::new(rpc_url));
        Self { eth_client, task_state }
    }

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

    async fn poll_event_task(&self, task_uri: &str, filter: &Filter) -> Result<ObserveState> {
        let cursor = self.task_state.get_cursor().await;
        let previous_value = self.task_state.last_observed().await;

        let mut scoped_filter = filter.clone();
        scoped_filter = scoped_filter.from_block(cursor);

        let finalized_block = self.eth_client.get_finalized_block_number().await.unwrap();
        scoped_filter = scoped_filter.to_block(finalized_block);

        let logs = self.eth_client.get_logs(&scoped_filter).await?;

        let new_logs: Vec<EventLog> = logs
            .iter()
            .filter(|log| log.block_number.unwrap_or(0) > cursor)
            .map(|log| log.into())
            .collect();

        if new_logs.is_empty() {
            let next_cursor = scoped_filter.get_to_block().unwrap();
            self.task_state.update_cursor(next_cursor).await;
            debug!("轮询事件任务 {} 完成，cursor: {}", task_uri, next_cursor);
            return Ok((*previous_value).clone());
        }

        let observed_value = ObservedValue::Events { logs: new_logs.clone() };

        let should_update = self.task_state.should_update(&observed_value).await;

        let return_value = if should_update {
            let new_cursor =
                new_logs.iter().max_by_key(|log| log.block_number).unwrap().block_number;
            self.task_state.update_cursor(new_cursor).await;
            let new_value = ObserveState {
                block_number: new_cursor,
                observed_value: observed_value.clone(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: previous_value.version + 1,
            };

            self.task_state.update_last_observed(new_value.clone()).await;
            new_value
        } else {
            (*previous_value).clone()
        };

        debug!("轮询事件任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(return_value)
    }

    /// 轮询区块头任务  
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
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    version: previous_value.version + 1,
                };

                self.task_state.update_last_observed(new_value.clone()).await;
                new_value
            } else {
                (*previous_value).clone()
            }
        } else {
            (*previous_value).clone()
        };

        debug!("轮询区块头任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(return_value)
    }

    /// 轮询存储槽任务
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
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: previous_value.version + 1,
            };
            self.task_state.update_last_observed(new_value.clone()).await;
            new_value
        } else {
            (*previous_value).clone()
        };
        debug!("轮询存储槽任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(return_value)
    }

    /// 轮询账户活动任务
    async fn poll_account_activity_task(
        &self,
        task_uri: &str,
        address: Address,
        activity_type: &AccountActivityType,
    ) -> Result<ObserveState> {
        match activity_type {
            AccountActivityType::Erc20Transfer => {
                // 创建ERC20 Transfer事件的filter
                // Transfer事件签名: Transfer(address,address,uint256)
                let transfer_topic = B256::from(hex!(
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                ));

                // This filter construction needs proper OR logic implementation
                // For now, create a basic filter
                let filter = Filter::new().event_signature(transfer_topic);
                // TODO: Implement proper OR logic for topic1/topic2 to monitor both from and to transfers

                self.poll_event_task(task_uri, &filter).await
            }
            AccountActivityType::AllTransactions => {
                // 这需要遍历区块中的所有交易，性能较低
                warn!("AllTransactions monitoring is not yet implemented for address: {}", address);
                Ok(ObserveState {
                    block_number: 0,
                    observed_value: ObservedValue::None,
                    timestamp: 0,
                    version: 0,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_relayer_add_remove_task() {
        // 创建mock eth客户端 - 这里需要实际的测试实现
        // let eth_client = Arc::new(EthHttpCli::new("http://localhost:8545", 1).unwrap());
        // let relayer = GravityRelayer::new(eth_client, RelayerConfig::default());

        // // 解析任务
        // let parser = UriParser::new();
        // let task = parser.parse("gravity://chain/l1/eth/0x123456789abcdef123456789abcdef1234567890/event/Epoch_Change").unwrap();

        // // 添加任务
        // relayer.add_task(task).await.unwrap();
        // let tasks = relayer.get_tasks().await;
        // assert_eq!(tasks.len(), 1);

        // // 移除任务
        // relayer.remove_task(&tasks[0]).await.unwrap();
        // let tasks = relayer.get_tasks().await;
        // assert_eq!(tasks.len(), 0);
    }
}
