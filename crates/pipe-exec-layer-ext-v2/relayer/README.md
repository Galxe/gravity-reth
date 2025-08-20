# Gravity Protocol Relayer

A URI parser and blockchain event relayer for the Gravity protocol.

## Features

### URI Parser (UriParser)
Supports parsing Gravity URIs in the following formats:
- `gravity://mainnet/block?strategy=head` - Monitor latest block
- `gravity://mainnet/event?address=0x...&topic0=0x...` - Monitor contract events
- `gravity://mainnet/storage?account=0x...&slot=0x...` - Monitor storage slot changes
- `gravity://mainnet/account/0x.../activity?type=erc20_transfer` - Monitor account activity

### Relayer (GravityRelayer)
- Periodically polls Ethereum nodes
- Maintains processing cursor state
- Detects data changes and generates update events
- Supports finalized block filtering
- Configurable polling intervals and block ranges

### Relayer Manager (RelayerManager)
- Manages multiple relayers for different URIs
- Centralized lifecycle management
- Supports multiple RPC endpoints
- Provides unified interface for adding and polling URIs

## Basic Usage

```rust
use reth_pipe_exec_layer_relayer::{
    RelayerManager, UriParser, ObserveState, ObservedValue
};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Create URI parser
    let parser = UriParser::new();
    
    // 2. Parse task URI
    let task = parser.parse("gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")?;
    
    // 3. Create RelayerManager
    let manager = RelayerManager::new();
    
    // 4. Define initial state
    let last_state = ObserveState {
        block_number: 19000000,
        observed_value: ObservedValue::None,
        timestamp: 0,
        version: 0,
    };
    
    // 5. Add URI to manager
    manager.add_uri(&task.original_uri, "https://rpc.ankr.com/eth", last_state).await?;
    
    // 6. Poll for updates
    let current_state = manager.poll_uri(&task.original_uri).await?;
    println!("Current state: {:?}", current_state);
    
    Ok(())
}
```

## Data Structures

### ParsedTask
```rust
pub struct ParsedTask {
    /// The parsed gravity task to be executed
    pub task: GravityTask,
    /// The original URI string that was parsed
    pub original_uri: String,
    /// The chain identifier (e.g., "mainnet", "testnet")
    pub chain_specifier: String,
}
```

### GravityTask
```rust
pub enum GravityTask {
    /// Monitor event task, contains a Filter object that can be directly used with Alloy
    MonitorEvent(Filter),
    /// Monitor block head task
    MonitorBlockHead,
    /// Monitor storage slot task
    MonitorStorage { account: Address, slot: B256 },
    /// Monitor account activity task (abstract layer)
    MonitorAccount { address: Address, activity_type: AccountActivityType },
}
```

### ObserveState
```rust
pub struct ObserveState {
    /// The block number at which the observation was made
    pub block_number: u64,
    /// The actual observed value (block, events, storage slot, or none)
    pub observed_value: ObservedValue,
    /// Chain timestamp to ensure consistency
    pub timestamp: u64,
    /// OnChain version for tracking changes
    pub version: u64,
}
```

### ObservedValue
```rust
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
```

### EventLog
```rust
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
```

## URI Format Examples

### Block Monitoring
```rust
// Monitor latest block
"gravity://mainnet/block?strategy=head"
```

### Event Monitoring
```rust
// Monitor specific contract events
"gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// Monitor events with multiple topics
"gravity://mainnet/event?address=0x...&topic0=0x...&topic1=0x..."

// Monitor events with OR conditions
"gravity://mainnet/event?topic0=0x...,0x..."
```

### Storage Monitoring
```rust
// Monitor storage slot changes
"gravity://mainnet/storage?account=0x123456789abcdef123456789abcdef1234567890&slot=0x0000000000000000000000000000000000000000000000000000000000000001"
```

### Account Activity Monitoring
```rust
// Monitor ERC20 transfers for specific address
"gravity://mainnet/account/0x123456789abcdef123456789abcdef1234567890/activity?type=erc20_transfer"

// Monitor all transactions for specific address
"gravity://mainnet/account/0x123456789abcdef123456789abcdef1234567890/activity?type=all_transactions"
```

## Advanced Usage

### Multiple URI Management
```rust
use reth_pipe_exec_layer_relayer::{RelayerManager, ObserveState, ObservedValue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manager = RelayerManager::new();
    
    let uris = vec![
        "gravity://mainnet/block?strategy=head",
        "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "gravity://mainnet/storage?account=0x123456789abcdef123456789abcdef1234567890&slot=0x0",
    ];
    
    let initial_state = ObserveState {
        block_number: 19000000,
        observed_value: ObservedValue::None,
        timestamp: 0,
        version: 0,
    };
    
    // Add multiple URIs
    for uri in &uris {
        manager.add_uri(uri, "https://rpc.ankr.com/eth", initial_state.clone()).await?;
    }
    
    // Poll all URIs
    for uri in &uris {
        match manager.poll_uri(uri).await {
            Ok(state) => println!("URI {}: {:?}", uri, state),
            Err(e) => println!("Error polling {}: {}", uri, e),
        }
    }
    
    Ok(())
}
```

### Batch URI Parsing
```rust
use reth_pipe_exec_layer_relayer::UriParser;

let parser = UriParser::new();
let uris = vec![
    "gravity://mainnet/block?strategy=head".to_string(),
    "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string(),
];

let tasks = parser.parse_batch(&uris)?;
for task in tasks {
    println!("Parsed task: {:?}", task);
}
```

## Error Handling

The library uses `anyhow::Result` for error handling. Common error scenarios include:

- Invalid URI format
- Unsupported chain specifiers
- Missing required parameters
- Invalid Ethereum addresses or topics
- RPC connection failures
- Network timeouts

## Performance Considerations

1. **RPC Rate Limiting**: The library includes built-in retry logic with exponential backoff
2. **Finalized Blocks**: By default, only finalized blocks are processed to ensure consistency
3. **Cursor Management**: Efficient cursor tracking prevents reprocessing of already seen data
4. **Batch Operations**: Support for batch URI parsing and management

## Dependencies

- `alloy-primitives`: Ethereum primitives and types
- `alloy-rpc-types`: RPC types and filters
- `tokio`: Async runtime
- `anyhow`: Error handling
- `tracing`: Logging and debugging
- `serde`: Serialization/deserialization

## Running Examples

```bash
# Run the basic usage example
cargo run --example new_usage

# Run tests
cargo test

# Generate documentation
cargo doc --open
```

## Notes

1. Ensure you provide valid Ethereum RPC endpoints
2. Storage slot monitoring requires RPC support for `eth_getStorageAt` method
3. Consider using longer polling intervals in production to avoid excessive RPC calls
4. The library automatically handles retries and backoff for failed requests
5. All operations are async and should be run in a Tokio runtime

## TODO

- [ ] Add WebSocket support for real-time updates
- [ ] Support for more event filtering options
- [ ] Add metrics and monitoring capabilities
- [ ] Implement connection pooling for multiple RPC endpoints