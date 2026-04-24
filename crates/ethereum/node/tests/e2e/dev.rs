use alloy_eips::eip2718::Encodable2718;
use alloy_genesis::Genesis;
use alloy_primitives::Address;
use futures::StreamExt;
use reth_chainspec::ChainSpec;
use reth_e2e_test_utils::{transaction::TransactionTestContext, wallet::Wallet};
use reth_node_api::{BlockBody, FullNodeComponents, FullNodePrimitives, NodeTypes};
use reth_node_builder::{rpc::RethRpcAddOns, FullNode, NodeBuilder, NodeConfig, NodeHandle};
use reth_node_core::args::DevArgs;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_provider::{providers::BlockchainProvider, CanonStateSubscriptions};
use reth_rpc_eth_api::{helpers::EthTransactions, EthApiServer};
use reth_tasks::TaskManager;
use std::sync::Arc;

const DEV_CHAIN_ID: u64 = 2600;

#[tokio::test]
async fn can_run_dev_node() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    let tasks = TaskManager::current();
    let exec = tasks.executor();

    let node_config = NodeConfig::test()
        .with_chain(custom_chain())
        .with_dev(DevArgs { dev: true, ..Default::default() });
    let NodeHandle { node, .. } = NodeBuilder::new(node_config.clone())
        .testing_node(exec.clone())
        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
        .with_components(EthereumNode::components())
        .with_add_ons(EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    assert_chain_advances(&node).await;

    Ok(())
}

#[tokio::test]
async fn can_run_dev_node_custom_attributes() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    let tasks = TaskManager::current();
    let exec = tasks.executor();

    let node_config = NodeConfig::test()
        .with_chain(custom_chain())
        .with_dev(DevArgs { dev: true, ..Default::default() });
    let fee_recipient = Address::random();
    let NodeHandle { node, .. } = NodeBuilder::new(node_config.clone())
        .testing_node(exec.clone())
        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
        .with_components(EthereumNode::components())
        .with_add_ons(EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .map_debug_payload_attributes(move |mut attributes| {
            attributes.suggested_fee_recipient = fee_recipient;
            attributes
        })
        .await?;

    assert_chain_advances(&node).await;

    assert!(
        node.rpc_registry.eth_api().balance(fee_recipient, Default::default()).await.unwrap() > 0
    );

    assert!(
        node.rpc_registry
            .eth_api()
            .block_by_number(Default::default(), false)
            .await
            .unwrap()
            .unwrap()
            .header
            .beneficiary ==
            fee_recipient
    );

    Ok(())
}

async fn assert_chain_advances<N, AddOns>(node: &FullNode<N, AddOns>)
where
    N: FullNodeComponents<Provider: CanonStateSubscriptions>,
    AddOns: RethRpcAddOns<N, EthApi: EthTransactions>,
    N::Types: NodeTypes<Primitives: FullNodePrimitives>,
{
    let mut notifications = node.provider.canonical_state_stream();

    // Submit a signed EIP-1559 tx through RPC. The signer is the first address of the shared
    // test mnemonic (funded in `custom_chain`'s genesis); helper-generated txs carry
    // 100 Gwei max_fee_per_gas, clearing Gravity's 50 Gwei protocol floor.
    let wallet = Wallet::new(1).with_chain_id(DEV_CHAIN_ID).wallet_gen().remove(0);
    let raw_tx = TransactionTestContext::transfer_tx_bytes(DEV_CHAIN_ID, wallet).await;

    let eth_api = node.rpc_registry.eth_api();
    let hash = eth_api.send_raw_transaction(raw_tx).await.unwrap();
    println!("submitted transaction: {hash}");

    let head = notifications.next().await.unwrap();

    let tx = &head.tip().body().transactions()[0];
    assert_eq!(tx.trie_hash(), hash);
    println!("mined transaction: {hash}");
}

fn custom_chain() -> Arc<ChainSpec> {
    let custom_genesis = r#"
{

    "nonce": "0x42",
    "timestamp": "0x0",
    "extraData": "0x5343",
    "gasLimit": "0x13880",
    "difficulty": "0x400000000",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266": {
            "balance": "0x4a47e3c12448f4ad000000"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "ethash": {},
        "chainId": 2600,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}
"#;
    let genesis: Genesis = serde_json::from_str(custom_genesis).unwrap();
    Arc::new(genesis.into())
}
