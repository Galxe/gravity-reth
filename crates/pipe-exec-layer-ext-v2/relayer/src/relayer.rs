//! Relayer for gravity protocol tasks

use crate::eth_client::EthHttpCli;
use alloy_primitives::{Address, B256};
use alloy_rpc_types::Log;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, Interval};
use tracing::{debug, error, info, warn};

use crate::parser::{ParsedTask, TaskType};

/// 观察到的更新事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveUpdate {
    /// 任务URI
    pub task_uri: String,
    /// 任务类型
    pub task_type: TaskType,
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
    pub fn set_update_callback<F>(&mut self, callback: F)
    where
        F: Fn(ObserveUpdate) + Send + Sync + 'static,
    {
        self.update_callback = Some(Arc::new(callback));
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

        match &task.task_type {
            TaskType::Event { contract_address, event_name } => {
                self.poll_event_task(task_uri, *contract_address, event_name).await
            }
            TaskType::Block { block_hash: _ } => {
                self.poll_block_task(task_uri).await
            }
            TaskType::StorageSlot { contract_address, slot } => {
                self.poll_storage_slot_task(task_uri, *contract_address, *slot).await
            }
        }
    }

    /// 轮询事件任务
    async fn poll_event_task(&self, task_uri: &str, contract_address: Address, _event_name: &str) -> Result<()> {
        // 获取当前cursor
        let cursor = {
            let states = self.task_states.read().await;
            states.get(task_uri).map(|s| s.cursor).unwrap_or(0)
        };

        // 调用get_eth_logs获取日志
        // 注意：这里需要修复eth_client中的get_eth_logs方法签名
        let logs = self.eth_client.get_eth_logs(contract_address, vec![]).await?;
        
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
                    task_type: TaskType::Event {
                        contract_address,
                        event_name: _event_name.to_string(),
                    },
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

    /// 轮询区块任务  
    async fn poll_block_task(&self, task_uri: &str) -> Result<()> {
        // 获取最新区块号
        let latest_block = self.eth_client.get_block_number().await?;
        
        let cursor = {
            let states = self.task_states.read().await;
            states.get(task_uri).map(|s| s.cursor).unwrap_or(0)
        };

        if latest_block > cursor {
            let new_value = ObservedValue::Block {
                block_hash: B256::ZERO, // 在实际实现中应该获取真实的区块哈希
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
                    task_type: TaskType::Block {
                        block_hash: B256::ZERO, // 从task中获取
                    },
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

        debug!("轮询区块任务 {} 完成，cursor: {}", task_uri, cursor);
        Ok(())
    }

    /// 轮询存储槽任务
    async fn poll_storage_slot_task(&self, task_uri: &str, _contract_address: Address, _slot: B256) -> Result<()> {
        // TODO: 实现存储槽查询
        // 这需要调用 eth_getStorageAt RPC方法
        // 目前EthHttpCli还没有这个方法，需要添加
        
        warn!("存储槽任务轮询尚未实现: {}", task_uri);
        Ok(())
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