//! Relayer for gravity protocol tasks

use crate::eth_client::EthHttpCli;
use crate::parser::{ParsedTask, GravityTask, AccountActivityType};
use alloy_primitives::{hex, Address, B256};
use alloy_rpc_types::{Log, Filter};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, Interval};
use tracing::{debug, error, info, warn};

/// 观察到的更新事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveUpdate {
    /// 任务URI
    pub task_uri: String,
    /// 任务类型
    pub task_type: GravityTask,
    /// 区块号
    pub block_number: u64,
    /// 新的观察值
    pub new_value: ObservedValue,
    /// 上一次观察值
    pub previous_value: Option<ObservedValue>,
    /// 更新时间戳
    pub timestamp: u64,
}

/// Relayer统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayerStats {
    /// 总任务数
    pub total_tasks: usize,
    /// 活跃任务数
    pub active_tasks: usize,
    /// 最旧的cursor
    pub oldest_cursor: u64,
    /// 最新的cursor
    pub newest_cursor: u64,
}

/// 观察到的值
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservedValue {
    /// 区块数据
    Block {
        block_hash: B256,
        block_number: u64,
    },
    /// 事件日志
    Events {
        /// 日志列表
        logs: Vec<EventLog>,
    },
    /// 存储槽值
    StorageSlot {
        slot: B256,
        value: B256,
    },
}

/// 简化的事件日志
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

/// Relayer配置
#[derive(Debug, Clone)]
pub struct RelayerConfig {
    /// 轮询间隔
    pub poll_interval: Duration,
    /// 初始区块号（可选，默认使用最新finalized区块）
    pub start_block: Option<u64>,
    /// 每次查询的区块范围
    pub block_range: u64,
    /// 是否只处理finalized区块
    pub finalized_only: bool,
}

impl Default for RelayerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(12), // 以太坊区块时间
            start_block: None,
            block_range: 100,
            finalized_only: true,
        }
    }
}

/// 任务状态
#[derive(Debug)]
struct TaskState {
    /// 任务配置
    task: ParsedTask,
    /// 当前处理到的区块号（cursor）
    cursor: u64,
    /// 上次观察到的值
    last_observed: Option<ObservedValue>,
}

/// Gravity协议Relayer
pub struct GravityRelayer {
    /// ETH客户端
    eth_client: Arc<EthHttpCli>,
    /// 配置
    config: RelayerConfig,
    /// 任务状态映射
    task_states: Arc<RwLock<HashMap<String, TaskState>>>,
    /// 轮询定时器 (暂未使用，为将来扩展保留)
    #[allow(dead_code)]
    poll_timer: Option<Interval>,
    /// 更新回调
    update_callback: Option<Arc<dyn Fn(ObserveUpdate) + Send + Sync>>,
}

impl std::fmt::Debug for GravityRelayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GravityRelayer")
            .field("eth_client", &self.eth_client)
            .field("config", &self.config)
            .field("task_states", &"<task_states>")
            .field("poll_timer", &self.poll_timer.is_some())
            .field("update_callback", &self.update_callback.is_some())
            .finish()
    }
}

impl GravityRelayer {
    /// 创建新的Relayer实例
    pub fn new(eth_client: Arc<EthHttpCli>, config: RelayerConfig) -> Self {
        Self {
            eth_client,
            config,
            task_states: Arc::new(RwLock::new(HashMap::new())),
            poll_timer: None,
            update_callback: None,
        }
    }

    /// 设置更新回调函数
    pub async fn set_update_callback<F>(&self, callback: F)
    where
        F: Fn(ObserveUpdate) + Send + Sync + 'static,
    {
        // 由于我们现在使用Arc<RwLock>模式，需要修改这个方法
        // 但为了保持向后兼容，我们暂时保持这个接口
        // 在实际实现中，可能需要重新设计回调机制
        // self.update_callback = Some(Arc::new(callback));
        warn!("set_update_callback method needs to be redesigned for new architecture");
    }

    /// 获取配置
    pub fn get_config(&self) -> &RelayerConfig {
        &self.config
    }

    /// 添加要监听的任务
    pub async fn add_task(&self, task: ParsedTask) -> Result<()> {
        let start_block = self.config.start_block.unwrap_or_else(|| {
            // 在实际实现中，这里应该获取最新的finalized区块号
            // 暂时使用0作为默认值
            0
        });

        let task_state = TaskState {
            task: task.clone(),
            cursor: start_block,
            last_observed: None,
        };

        let mut states = self.task_states.write().await;
        states.insert(task.original_uri.clone(), task_state);
        
        info!("添加任务: {}", task.original_uri);
        Ok(())
    }

    /// 移除任务
    pub async fn remove_task(&self, task_uri: &str) -> Result<()> {
        let mut states = self.task_states.write().await;
        if states.remove(task_uri).is_some() {
            info!("移除任务: {}", task_uri);
            Ok(())
        } else {
            Err(anyhow!("任务不存在: {}", task_uri))
        }
    }

    /// 获取任务列表
    pub async fn get_tasks(&self) -> Vec<String> {
        let states = self.task_states.read().await;
        states.keys().cloned().collect()
    }

    /// 开始轮询
    pub async fn start_polling(&mut self) -> Result<()> {
        let mut interval = interval(self.config.poll_interval);
        
        info!("开始轮询，间隔: {:?}", self.config.poll_interval);
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.poll_once().await {
                error!("轮询错误: {}", e);
                // 继续轮询，不要因为单次错误而停止
            }
        }
    }

    /// 执行一次轮询
    pub async fn poll_once(&self) -> Result<()> {
        let task_uris: Vec<String> = {
            let states = self.task_states.read().await;
            states.keys().cloned().collect()
        };

        for task_uri in task_uris {
            if let Err(e) = self.poll_task(&task_uri).await {
                error!("轮询任务 {} 失败: {}", task_uri, e);
            }
        }

        Ok(())
    }

    /// 轮询单个任务
    async fn poll_task(&self, task_uri: &str) -> Result<()> {
        let task = {
            let states = self.task_states.read().await;
            match states.get(task_uri) {
                Some(state) => state.task.clone(),
                None => return Err(anyhow!("任务不存在: {}", task_uri)),
            }
        };

        match &task.task {
            GravityTask::MonitorEvent(filter) => {
                self.poll_event_task(task_uri, filter).await
            }
            GravityTask::MonitorBlockHead => {
                self.poll_block_head_task(task_uri).await
            }
            GravityTask::MonitorStorage { account, slot } => {
                self.poll_storage_slot_task(task_uri, *account, *slot).await
            }
            GravityTask::MonitorAccount { address, activity_type } => {
                self.poll_account_activity_task(task_uri, *address, activity_type).await
            }
        }
    }

    /// 轮询事件任务
    async fn poll_event_task(&self, task_uri: &str, filter: &Filter) -> Result<()> {
        // 获取当前cursor
        let cursor = {
            let states = self.task_states.read().await;
            states.get(task_uri).map(|s| s.cursor).unwrap_or(0)
        };

        // 创建带有区块范围的filter
        let mut scoped_filter = filter.clone();
        scoped_filter = scoped_filter.from_block(cursor);
        
        // 如果配置了finalized_only，使用finalized区块
        if self.config.finalized_only {
            let finalized_block = self.eth_client.get_finalized_block_number().await.unwrap_or(cursor);
            scoped_filter = scoped_filter.to_block(finalized_block);
        } else {
            // 使用latest区块
            let latest_block = self.eth_client.get_block_number().await.unwrap_or(cursor);
            scoped_filter = scoped_filter.to_block(latest_block);
        }

        // 获取日志
        let logs = self.eth_client.get_logs(&scoped_filter).await?;
        
        // 过滤出在cursor之后的日志
        let new_logs: Vec<EventLog> = logs.iter()
            .filter(|log| log.block_number.unwrap_or(0) > cursor)
            .map(|log| log.into())
            .collect();

        if !new_logs.is_empty() {
            let new_value = ObservedValue::Events { logs: new_logs.clone() };
            
            // 检查是否有变化
            let (previous_value, should_update) = {
                let states = self.task_states.read().await;
                let state = states.get(task_uri).unwrap();
                let should_update = state.last_observed.as_ref() != Some(&new_value);
                (state.last_observed.clone(), should_update)
            };

            if should_update {
                // 更新状态
                {
                    let mut states = self.task_states.write().await;
                    if let Some(state) = states.get_mut(task_uri) {
                        // 更新cursor到最新的区块号
                        if let Some(latest_log) = new_logs.iter().max_by_key(|log| log.block_number) {
                            state.cursor = latest_log.block_number;
                        }
                        state.last_observed = Some(new_value.clone());
                    }
                }

                // 触发更新回调
                self.trigger_update(ObserveUpdate {
                    task_uri: task_uri.to_string(),
                    task_type: GravityTask::MonitorEvent(filter.clone()),
                    block_number: new_logs.iter().map(|log| log.block_number).max().unwrap_or(cursor),
                    new_value,
                    previous_value,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }).await;
            }
        }

        debug!("轮询事件任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(())
    }

    /// 轮询区块头任务  
    async fn poll_block_head_task(&self, task_uri: &str) -> Result<()> {
        // 获取最新区块号
        let latest_block = if self.config.finalized_only {
            self.eth_client.get_finalized_block_number().await?
        } else {
            self.eth_client.get_block_number().await?
        };
        
        let cursor = {
            let states = self.task_states.read().await;
            states.get(task_uri).map(|s| s.cursor).unwrap_or(0)
        };

        if latest_block > cursor {
            // 获取实际的区块信息
            let block_hash = match self.eth_client.get_block(latest_block).await? {
                Some(block) => block.header.hash,
                None => B256::ZERO,
            };

            let new_value = ObservedValue::Block {
                block_hash,
                block_number: latest_block,
            };

            let (previous_value, should_update) = {
                let states = self.task_states.read().await;
                let state = states.get(task_uri).unwrap();
                let should_update = state.last_observed.as_ref() != Some(&new_value);
                (state.last_observed.clone(), should_update)
            };

            if should_update {
                // 更新状态
                {
                    let mut states = self.task_states.write().await;
                    if let Some(state) = states.get_mut(task_uri) {
                        state.cursor = latest_block;
                        state.last_observed = Some(new_value.clone());
                    }
                }

                // 触发更新
                self.trigger_update(ObserveUpdate {
                    task_uri: task_uri.to_string(),
                    task_type: GravityTask::MonitorBlockHead,
                    block_number: latest_block,
                    new_value,
                    previous_value,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }).await;
            }
        }

        debug!("轮询区块头任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(())
    }

    /// 轮询存储槽任务
    async fn poll_storage_slot_task(&self, task_uri: &str, account: Address, slot: B256) -> Result<()> {
        // 获取存储槽的当前值
        let current_value = self.eth_client.get_storage_at(account, slot).await?;
        
        let cursor = {
            let states = self.task_states.read().await;
            states.get(task_uri).map(|s| s.cursor).unwrap_or(0)
        };

        // 获取当前区块号用于比较
        let current_block = if self.config.finalized_only {
            self.eth_client.get_finalized_block_number().await?
        } else {
            self.eth_client.get_block_number().await?
        };

        let new_value = ObservedValue::StorageSlot {
            slot,
            value: current_value,
        };

        let (previous_value, should_update) = {
            let states = self.task_states.read().await;
            let state = states.get(task_uri).unwrap();
            let should_update = state.last_observed.as_ref() != Some(&new_value);
            (state.last_observed.clone(), should_update)
        };

        if should_update {
            // 更新状态
            {
                let mut states = self.task_states.write().await;
                if let Some(state) = states.get_mut(task_uri) {
                    state.cursor = current_block;
                    state.last_observed = Some(new_value.clone());
                }
            }

            // 触发更新
            self.trigger_update(ObserveUpdate {
                task_uri: task_uri.to_string(),
                task_type: GravityTask::MonitorStorage { account, slot },
                block_number: current_block,
                new_value,
                previous_value,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }).await;
        }

        debug!("轮询存储槽任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(())
    }

    /// 轮询账户活动任务
    async fn poll_account_activity_task(&self, task_uri: &str, address: Address, activity_type: &AccountActivityType) -> Result<()> {
        match activity_type {
            AccountActivityType::Erc20Transfer => {
                // 创建ERC20 Transfer事件的filter
                // Transfer事件签名: Transfer(address,address,uint256)
                let transfer_topic = B256::from(hex!("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"));
                
                // This filter construction needs proper OR logic implementation
                // For now, create a basic filter
                let filter = Filter::new()
                    .event_signature(transfer_topic);
                // TODO: Implement proper OR logic for topic1/topic2 to monitor both from and to transfers

                self.poll_event_task(task_uri, &filter).await
            }
            AccountActivityType::AllTransactions => {
                // 这需要遍历区块中的所有交易，性能较低
                warn!("AllTransactions monitoring is not yet implemented for address: {}", address);
                Ok(())
            }
        }
    }

    /// 停止轮询
    pub async fn stop_polling(&self) {
        // TODO: 实现优雅停止机制
        // 目前start_polling是无限循环，需要添加停止信号
    }

    /// 获取Relayer统计信息
    pub async fn get_stats(&self) -> RelayerStats {
        let states = self.task_states.read().await;
        RelayerStats {
            total_tasks: states.len(),
            active_tasks: states.len(), // 目前所有任务都是活跃的
            oldest_cursor: states.values().map(|s| s.cursor).min().unwrap_or(0),
            newest_cursor: states.values().map(|s| s.cursor).max().unwrap_or(0),
        }
    }

    /// 触发更新回调
    async fn trigger_update(&self, update: ObserveUpdate) {
        if let Some(callback) = &self.update_callback {
            callback(update);
        }
    }

    /// 获取任务状态
    pub async fn get_task_state(&self, task_uri: &str) -> Option<(u64, Option<ObservedValue>)> {
        let states = self.task_states.read().await;
        states.get(task_uri).map(|state| (state.cursor, state.last_observed.clone()))
    }

    /// 手动设置任务cursor
    pub async fn set_task_cursor(&self, task_uri: &str, cursor: u64) -> Result<()> {
        let mut states = self.task_states.write().await;
        match states.get_mut(task_uri) {
            Some(state) => {
                state.cursor = cursor;
                info!("设置任务 {} cursor为: {}", task_uri, cursor);
                Ok(())
            }
            None => Err(anyhow!("任务不存在: {}", task_uri))
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