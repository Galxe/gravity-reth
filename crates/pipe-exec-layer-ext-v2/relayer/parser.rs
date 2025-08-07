//! URI parser for gravity protocol tasks

use alloy_primitives::{Address, B256};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// 任务类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    /// 监听区块
    Block {
        block_hash: B256,
    },
    /// 监听事件
    Event {
        contract_address: Address,
        event_name: String,
    },
    /// 监听存储槽
    StorageSlot {
        contract_address: Address,
        slot: B256,
    },
}

/// 解析后的任务
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedTask {
    /// 任务类型
    pub task_type: TaskType,
    /// 原始URI
    pub original_uri: String,
    /// 网络标识 (l1, l2等)
    pub network: String,
    /// 链标识 (eth等)
    pub chain: String,
}

/// URI解析器
#[derive(Debug, Default)]
pub struct UriParser;

impl UriParser {
    /// 创建新的URI解析器
    pub fn new() -> Self {
        Self
    }

    /// 解析gravity URI
    /// 
    /// 支持的格式：
    /// - gravity://chain/l1/eth/block/0x1234
    /// - gravity://chain/l1/eth/0x123456/event/Epoch_Change
    /// - gravity://chain/l1/eth/0x123456/storage_slot/0x12345
    pub fn parse(&self, uri: &str) -> Result<ParsedTask> {
        // 检查scheme
        if !uri.starts_with("gravity://") {
            return Err(anyhow!("无效的URI scheme，期望 'gravity://'"));
        }

        // 移除scheme
        let path = &uri[10..]; // "gravity://".len() = 10

        // 分割路径
        let parts: Vec<&str> = path.split('/').collect();
        
        if parts.len() < 4 {
            return Err(anyhow!("URI路径太短，至少需要 chain/network/chain_type/..."));
        }

        // 验证第一部分是 "chain"
        if parts[0] != "chain" {
            return Err(anyhow!("URI必须以 'chain' 开头"));
        }

        let network = parts[1].to_string(); // l1, l2等
        let chain = parts[2].to_string();   // eth等
        
        // 根据剩余部分确定任务类型
        match parts.len() {
            5 => {
                // gravity://chain/l1/eth/block/0x1234
                if parts[3] == "block" {
                    let block_hash = B256::from_str(parts[4])
                        .map_err(|e| anyhow!("无效的区块哈希 '{}': {}", parts[4], e))?;
                    
                    Ok(ParsedTask {
                        task_type: TaskType::Block { block_hash },
                        original_uri: uri.to_string(),
                        network,
                        chain,
                    })
                } else {
                    Err(anyhow!("未知的任务类型 '{}'", parts[3]))
                }
            }
            6 => {
                // gravity://chain/l1/eth/0x123456/event/Epoch_Change
                // gravity://chain/l1/eth/0x123456/storage_slot/0x12345
                let contract_address = Address::from_str(parts[3])
                    .map_err(|e| anyhow!("无效的合约地址 '{}': {}", parts[3], e))?;

                match parts[4] {
                    "event" => {
                        let event_name = parts[5].to_string();
                        Ok(ParsedTask {
                            task_type: TaskType::Event {
                                contract_address,
                                event_name,
                            },
                            original_uri: uri.to_string(),
                            network,
                            chain,
                        })
                    }
                    "storage_slot" => {
                        let slot = B256::from_str(parts[5])
                            .map_err(|e| anyhow!("无效的存储槽 '{}': {}", parts[5], e))?;
                        Ok(ParsedTask {
                            task_type: TaskType::StorageSlot {
                                contract_address,
                                slot,
                            },
                            original_uri: uri.to_string(),
                            network,
                            chain,
                        })
                    }
                    _ => Err(anyhow!("未知的任务类型 '{}'", parts[4]))
                }
            }
            _ => Err(anyhow!("无效的URI格式，路径段数量不正确"))
        }
    }

    /// 批量解析多个URI
    pub fn parse_batch(&self, uris: &[String]) -> Result<Vec<ParsedTask>> {
        let mut tasks = Vec::new();
        for uri in uris {
            tasks.push(self.parse(uri)?);
        }
        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_block_uri() {
        let parser = UriParser::new();
        let uri = "gravity://chain/l1/eth/block/0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        
        let result = parser.parse(uri).unwrap();
        
        assert_eq!(result.network, "l1");
        assert_eq!(result.chain, "eth");
        assert_eq!(result.original_uri, uri);
        
        match result.task_type {
            TaskType::Block { block_hash } => {
                assert_eq!(
                    block_hash,
                    B256::from_str("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap()
                );
            }
            _ => panic!("Expected Block task type"),
        }
    }

    #[test]
    fn test_parse_event_uri() {
        let parser = UriParser::new();
        let uri = "gravity://chain/l1/eth/0x123456789abcdef123456789abcdef1234567890/event/Epoch_Change";
        
        let result = parser.parse(uri).unwrap();
        
        assert_eq!(result.network, "l1");
        assert_eq!(result.chain, "eth");
        
        match result.task_type {
            TaskType::Event { contract_address, event_name } => {
                assert_eq!(
                    contract_address,
                    Address::from_str("0x123456789abcdef123456789abcdef1234567890").unwrap()
                );
                assert_eq!(event_name, "Epoch_Change");
            }
            _ => panic!("Expected Event task type"),
        }
    }

    #[test]
    fn test_parse_storage_slot_uri() {
        let parser = UriParser::new();
        let uri = "gravity://chain/l1/eth/0x123456789abcdef123456789abcdef1234567890/storage_slot/0x0000000000000000000000000000000000000000000000000000000000000001";
        
        let result = parser.parse(uri).unwrap();
        
        match result.task_type {
            TaskType::StorageSlot { contract_address, slot } => {
                assert_eq!(
                    contract_address,
                    Address::from_str("0x123456789abcdef123456789abcdef1234567890").unwrap()
                );
                assert_eq!(
                    slot,
                    B256::from_str("0x0000000000000000000000000000000000000000000000000000000000000001").unwrap()
                );
            }
            _ => panic!("Expected StorageSlot task type"),
        }
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let parser = UriParser::new();
        let uri = "http://chain/l1/eth/block/0x1234";
        
        let result = parser.parse(uri);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("无效的URI scheme"));
    }

    #[test]
    fn test_parse_invalid_format() {
        let parser = UriParser::new();
        let uri = "gravity://chain/l1";
        
        let result = parser.parse(uri);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("URI路径太短"));
    }

    #[test]
    fn test_parse_batch() {
        let parser = UriParser::new();
        let uris = vec![
            "gravity://chain/l1/eth/block/0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "gravity://chain/l1/eth/0x123456789abcdef123456789abcdef1234567890/event/Epoch_Change".to_string(),
        ];
        
        let results = parser.parse_batch(&uris).unwrap();
        assert_eq!(results.len(), 2);
        
        match &results[0].task_type {
            TaskType::Block { .. } => {}
            _ => panic!("Expected Block task type"),
        }
        
        match &results[1].task_type {
            TaskType::Event { .. } => {}
            _ => panic!("Expected Event task type"),
        }
    }
}
