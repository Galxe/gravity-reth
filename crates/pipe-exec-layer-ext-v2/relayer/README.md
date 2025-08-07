# Gravity Protocol Relayer

这是一个用于Gravity协议的URI解析器和区块链事件中继器。

## 功能特性

### URI解析器 (UriParser)
支持解析以下格式的Gravity URI：
- `gravity://chain/l1/eth/block/0x1234` - 监听特定区块
- `gravity://chain/l1/eth/0x123456/event/Epoch_Change` - 监听合约事件
- `gravity://chain/l1/eth/0x123456/storage_slot/0x12345` - 监听存储槽变化

### 中继器 (GravityRelayer)
- 周期性轮询以太坊节点
- 维护处理进度游标(cursor)
- 检测数据变化并生成更新事件
- 支持finalized区块过滤
- 可配置轮询间隔和区块范围

## 基本使用

```rust
use reth_pipe_exec_layer_relayer::{
    EthHttpCli, GravityRelayer, RelayerConfig, UriParser, ObserveUpdate
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 创建URI解析器
    let parser = UriParser::new();
    
    // 2. 解析任务URI
    let task = parser.parse("gravity://chain/l1/eth/0x123456789abcdef123456789abcdef1234567890/event/Epoch_Change")?;
    
    // 3. 创建ETH客户端
    let eth_client = Arc::new(EthHttpCli::new("https://rpc.ankr.com/eth", 1)?);
    
    // 4. 配置Relayer
    let config = RelayerConfig {
        poll_interval: Duration::from_secs(12),
        start_block: Some(19000000),
        block_range: 100,
        finalized_only: true,
    };
    
    // 5. 创建Relayer
    let mut relayer = GravityRelayer::new(eth_client, config);
    
    // 6. 设置更新回调
    relayer.set_update_callback(|update: ObserveUpdate| {
        println!("检测到更新: {:?}", update);
    });
    
    // 7. 添加任务
    relayer.add_task(task).await?;
    
    // 8. 开始轮询
    relayer.start_polling().await?;
    
    Ok(())
}
```

## 数据结构

### ParsedTask
```rust
pub struct ParsedTask {
    pub task_type: TaskType,
    pub original_uri: String,
    pub network: String,
    pub chain: String,
}
```

### TaskType
```rust
pub enum TaskType {
    Block { block_hash: B256 },
    Event { contract_address: Address, event_name: String },
    StorageSlot { contract_address: Address, slot: B256 },
}
```

### ObserveUpdate
```rust
pub struct ObserveUpdate {
    pub task_uri: String,
    pub task_type: TaskType,
    pub block_number: u64,
    pub new_value: ObservedValue,
    pub previous_value: Option<ObservedValue>,
    pub timestamp: u64,
}
```

### ObservedValue
```rust
pub enum ObservedValue {
    Block { block_hash: B256, block_number: u64 },
    Events { logs: Vec<EventLog> },
    StorageSlot { slot: B256, value: B256 },
}
```

## 配置选项

### RelayerConfig
- `poll_interval`: 轮询间隔（默认12秒）
- `start_block`: 起始区块号（可选）
- `block_range`: 每次查询的区块范围（默认100）
- `finalized_only`: 是否只处理finalized区块（默认true）

## 运行示例

```bash
cargo run --example basic_usage
```

## 注意事项

1. 确保提供有效的以太坊RPC端点
2. 存储槽监听功能需要RPC支持`eth_getStorageAt`方法
3. 建议在生产环境中使用较长的轮询间隔以避免过多RPC调用
4. 回调函数应该快速执行，避免阻塞轮询循环

## TODO

- [ ] 实现存储槽查询功能
- [ ] 添加RPC错误重试机制
- [ ] 支持WebSocket连接以实现实时更新
- [ ] 添加更多的事件过滤选项
- [ ] 实现持久化存储cursor状态