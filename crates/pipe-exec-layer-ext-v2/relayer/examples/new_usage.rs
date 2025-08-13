//! Example showing the new RelayerManager usage with the refactored design

use reth_pipe_exec_layer_relayer::{
    ObserveState, ObservedValue, RelayerManager, UriParser
};
use reth_tracing::{LayerInfo, LogFormat, RethTracer, Tracer};
use tracing::level_filters::LevelFilter;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    // tracing_subscriber::init();
    // env::set_var("NO_PROXY", "ethereum-holesky-rpc.publicnode.com");
    let tracer = RethTracer::new().with_stdout(LayerInfo::new(
                  LogFormat::Terminal,
                  LevelFilter::INFO.to_string(),
                  "trace".to_string(),
                  None,
    ));

    tracer.init().unwrap();

    // 1. Create ETH client
    let rpc_url = "https://ethereum-holesky-rpc.publicnode.com";
    
    // 3. Create RelayerManager
    let manager = RelayerManager::new();

    // 6. Add multiple URIs - each will get its own relayer instance
    let uris = vec![
        // Monitor latest block on mainnet
        // "gravity://mainnet/block?strategy=head",
        
        // Monitor USDC Transfer events
        "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        
        // Monitor storage slot on a contract
        // "gravity://mainnet/storage?account=0x123456789abcdef123456789abcdef1234567890&slot=0x0",
        
        // Monitor ERC20 transfers for a specific address
        // "gravity://mainnet/account/0x123456789abcdef123456789abcdef1234567890/activity?type=erc20_transfer",
    ];
    
    // Add each URI (creates separate relayer for each)
    for uri in &uris {
        let last_state = ObserveState {
            block_number: 4330800,
            observed_value: ObservedValue::None,
            timestamp: 0,
            version: 0,
        };
        match manager.add_uri(uri, rpc_url, last_state).await {
            Ok(()) => info!("Successfully added URI: {}", uri),
            Err(e) => info!("Failed to add URI {}: {}", uri, e),
        }
    }

    for uri in &uris {
        match manager.poll_url(uri).await {
            Ok(state) => info!("Successfully polled URI: {} -> {:?}", uri, state),
            Err(e) => info!("Failed to poll URI {}: {}", uri, e),
        }
    }

    // 8. Let it run for a while
    info!("Relayers are running. Press Ctrl+C to stop...");
    tokio::time::sleep(Duration::from_secs(60)).await;
    
    info!("Shutdown complete!");
    Ok(())
}

/// Example showing how to use the new UriParser
#[allow(dead_code)]
fn parser_example() -> anyhow::Result<()> {
    let parser = UriParser::new();
    
    // New URI format examples
    let uris = vec![
        "gravity://mainnet/block?strategy=head",
        "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "gravity://mainnet/storage?account=0x123456789abcdef123456789abcdef1234567890&slot=0x0",
        "gravity://mainnet/account/0x123456789abcdef123456789abcdef1234567890/activity?type=erc20_transfer",
    ];
    
    for uri in uris {
        match parser.parse(uri) {
            Ok(task) => {
                info!("Parsed URI: {} -> Chain: {}, Task: {:?}", 
                    uri, task.chain_specifier, task.task);
            }
            Err(e) => {
                info!("Failed to parse URI {}: {}", uri, e);
            }
        }
    }
    
    Ok(())
}