//! Relayer Manager for lifecycle management

use crate::parser::UriParser;
use crate::relayer::{GravityRelayer, ObserveState};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};
pub struct RelayerManager {
    uri_parser: UriParser,
    relayers: Arc<RwLock<HashMap<String, Arc<GravityRelayer>>>>,
}

impl RelayerManager {
    /// 创建新的RelayerManager
    pub fn new() -> Self {
        Self {
            uri_parser: UriParser::new(),
            relayers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_uri(&self, uri: &str, rpc_url: &str, last_state: ObserveState) -> Result<()> {
        {
            let relayers = self.relayers.read().await;
            if relayers.contains_key(rpc_url) {
                return Err(anyhow!("RPC URL {} is already being monitored", rpc_url));
            }
        }

        let task = self.uri_parser.parse(uri)?;
        info!("Adding URI: {} -> {:?}", uri, task);

        let relayer = GravityRelayer::new(rpc_url, task, last_state);

        let mut relayers = self.relayers.write().await;
        relayers.insert(uri.to_string(), Arc::new(relayer));

        info!("Successfully added URI: {}", uri);
        Ok(())
    }

    pub async fn poll_uri(&self, uri: &str) -> Result<ObserveState> {
        let relayers = { self.relayers.read().await };
        let relayer = relayers.get(uri).ok_or(anyhow!("URI {} not found, relayers: {:?}", uri, relayers))?;
        relayer.poll_once().await
    }
}

/// 管理器统计信息
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// 总URI数量
    pub total_uris: usize,
    /// 活跃URI数量
    pub active_uris: usize,
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
