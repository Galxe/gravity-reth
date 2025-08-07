//! Relayer Manager for lifecycle management

use crate::eth_client::EthHttpCli;
use crate::parser::{ParsedTask, UriParser};
use crate::relayer::{GravityRelayer, RelayerConfig, ObserveUpdate};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// 单个relayer的状态
#[derive(Debug)]
struct RelayerInstance {
    /// relayer实例
    relayer: Arc<GravityRelayer>,
    /// 轮询任务句柄
    poll_handle: Option<JoinHandle<()>>,
    /// 停止信号发送器
    stop_sender: Option<mpsc::Sender<()>>,
}

/// Relayer管理器
pub struct RelayerManager {
    /// ETH客户端
    eth_client: Arc<EthHttpCli>,
    /// URI解析器
    uri_parser: UriParser,
    /// 默认配置
    default_config: RelayerConfig,
    /// 活跃的relayer实例
    relayers: Arc<RwLock<HashMap<String, RelayerInstance>>>,
    /// 全局更新回调
    update_callback: Arc<Mutex<Option<Arc<dyn Fn(ObserveUpdate) + Send + Sync>>>>,
    /// 管理器是否在运行
    is_running: Arc<RwLock<bool>>,
}

impl RelayerManager {
    /// 创建新的RelayerManager
    pub fn new(eth_client: Arc<EthHttpCli>, default_config: RelayerConfig) -> Self {
        Self {
            eth_client,
            uri_parser: UriParser::new(),
            default_config,
            relayers: Arc::new(RwLock::new(HashMap::new())),
            update_callback: Arc::new(Mutex::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// 设置全局更新回调
    pub async fn set_update_callback<F>(&self, callback: F)
    where
        F: Fn(ObserveUpdate) + Send + Sync + 'static,
    {
        let mut cb = self.update_callback.lock().await;
        *cb = Some(Arc::new(callback));
    }

    /// 启动管理器
    pub async fn start(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Err(anyhow!("RelayerManager is already running"));
        }
        *is_running = true;
        info!("RelayerManager started");
        Ok(())
    }

    /// 停止管理器
    pub async fn stop(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if !*is_running {
            return Ok(());
        }

        info!("Stopping RelayerManager...");
        
        // 停止所有relayer
        let uris: Vec<String> = {
            let relayers = self.relayers.read().await;
            relayers.keys().cloned().collect()
        };

        for uri in uris {
            if let Err(e) = self.remove_uri(&uri).await {
                error!("Failed to remove URI {} during shutdown: {}", uri, e);
            }
        }

        *is_running = false;
        info!("RelayerManager stopped");
        Ok(())
    }

    /// 添加URI监控
    pub async fn add_uri(&self, uri: &str) -> Result<()> {
        let is_running = self.is_running.read().await;
        if !*is_running {
            return Err(anyhow!("RelayerManager is not running"));
        }

        // 检查URI是否已存在
        {
            let relayers = self.relayers.read().await;
            if relayers.contains_key(uri) {
                return Err(anyhow!("URI {} is already being monitored", uri));
            }
        }

        // 解析URI
        let task = self.uri_parser.parse(uri)?;
        info!("Adding URI: {} -> {:?}", uri, task);

        // 创建新的relayer实例
        let relayer = Arc::new(GravityRelayer::new(
            self.eth_client.clone(),
            self.default_config.clone(),
        ));

        // 设置回调
        if let Some(callback) = self.update_callback.lock().await.clone() {
            relayer.set_update_callback(move |update| {
                callback(update);
            }).await;
        }

        // 添加任务到relayer
        relayer.add_task(task).await?;

        // 创建停止信号通道
        let (stop_sender, mut stop_receiver) = mpsc::channel::<()>(1);

        // 启动轮询任务
        let relayer_clone = relayer.clone();
        let uri_clone = uri.to_string();
        let poll_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(relayer_clone.get_config().poll_interval);
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = relayer_clone.poll_once().await {
                            error!("Poll error for URI {}: {}", uri_clone, e);
                        }
                    }
                    _ = stop_receiver.recv() => {
                        debug!("Received stop signal for URI: {}", uri_clone);
                        break;
                    }
                }
            }
            
            info!("Polling stopped for URI: {}", uri_clone);
        });

        // 保存relayer实例
        let instance = RelayerInstance {
            relayer,
            poll_handle: Some(poll_handle),
            stop_sender: Some(stop_sender),
        };

        let mut relayers = self.relayers.write().await;
        relayers.insert(uri.to_string(), instance);

        info!("Successfully added URI: {}", uri);
        Ok(())
    }

    /// 移除URI监控
    pub async fn remove_uri(&self, uri: &str) -> Result<()> {
        let mut relayers = self.relayers.write().await;
        
        match relayers.remove(uri) {
            Some(mut instance) => {
                info!("Removing URI: {}", uri);

                // 发送停止信号
                if let Some(stop_sender) = instance.stop_sender.take() {
                    if let Err(_) = stop_sender.send(()).await {
                        warn!("Failed to send stop signal for URI: {}", uri);
                    }
                }

                // 等待轮询任务结束
                if let Some(poll_handle) = instance.poll_handle.take() {
                    if let Err(e) = poll_handle.await {
                        warn!("Failed to join poll task for URI {}: {}", uri, e);
                    }
                }

                info!("Successfully removed URI: {}", uri);
                Ok(())
            }
            None => Err(anyhow!("URI {} not found", uri)),
        }
    }

    /// 获取所有监控的URI
    pub async fn get_uris(&self) -> Vec<String> {
        let relayers = self.relayers.read().await;
        relayers.keys().cloned().collect()
    }

    /// 获取URI的状态
    pub async fn get_uri_status(&self, uri: &str) -> Option<bool> {
        let relayers = self.relayers.read().await;
        relayers.get(uri).map(|instance| {
            instance.poll_handle.as_ref().map_or(false, |h| !h.is_finished())
        })
    }

    /// 获取管理器统计信息
    pub async fn get_stats(&self) -> ManagerStats {
        let relayers = self.relayers.read().await;
        let total_uris = relayers.len();
        let active_uris = relayers
            .values()
            .filter(|instance| {
                instance.poll_handle.as_ref().map_or(false, |h| !h.is_finished())
            })
            .count();

        ManagerStats {
            total_uris,
            active_uris,
            is_running: *self.is_running.read().await,
        }
    }

    /// 优雅关闭所有relayer
    pub async fn graceful_shutdown(&self, timeout_secs: u64) -> Result<()> {
        info!("Starting graceful shutdown with timeout: {}s", timeout_secs);
        
        // 使用超时机制确保关闭不会无限期等待
        match tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            self.stop(),
        ).await {
            Ok(result) => {
                info!("Graceful shutdown completed");
                result
            }
            Err(_) => {
                warn!("Graceful shutdown timed out, forcing shutdown");
                // 强制停止所有任务
                let mut relayers = self.relayers.write().await;
                for (uri, instance) in relayers.drain() {
                    if let Some(handle) = instance.poll_handle {
                        handle.abort();
                        warn!("Force aborted polling task for URI: {}", uri);
                    }
                }
                *self.is_running.write().await = false;
                Ok(())
            }
        }
    }
}

/// 管理器统计信息
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// 总URI数量
    pub total_uris: usize,
    /// 活跃URI数量
    pub active_uris: usize,
    /// 管理器是否在运行
    pub is_running: bool,
}

/// 为了优雅退出，实现Drop trait
impl Drop for RelayerManager {
    fn drop(&mut self) {
        // 注意：Drop trait中不能使用异步代码
        // 这里只是记录日志，实际的清理应该在graceful_shutdown中完成
        debug!("RelayerManager is being dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_manager_lifecycle() {
        // 这里需要mock的EthHttpCli来进行测试
        // let eth_client = Arc::new(EthHttpCli::new("http://localhost:8545", 1).unwrap());
        // let config = RelayerConfig::default();
        // let manager = RelayerManager::new(eth_client, config);

        // // 测试启动和停止
        // manager.start().await.unwrap();
        // assert!(manager.get_stats().await.is_running);

        // manager.stop().await.unwrap();
        // assert!(!manager.get_stats().await.is_running);
    }
}