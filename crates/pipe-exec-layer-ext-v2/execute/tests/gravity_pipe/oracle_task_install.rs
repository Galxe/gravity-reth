//! Installation at the real canonical pipe's epoch-change early return.

use crate::{node, BlockInput, Builder, Chain, Node, RpcClient, BLOCK_TIMEOUT};
use alloy_primitives::{address, keccak256, Address, Bytes, B256, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use gravity_storage::block_view_storage::BlockViewStorage;
use reth_node_builder::EngineNodeLauncher;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_pipe_exec_layer_ext_v2::{new_pipe_exec_layer_api, ExecutionArgs};
use reth_provider::{providers::BlockchainProvider, BlockHashReader, HeaderProvider};
use serde_json::json;
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

const TASK_CONFIG: Address = address!("00000000000000000000000000000001625f1009");
const NATIVE_ORACLE: Address = address!("00000000000000000000000000000001625f4000");
const RECEIVER: Address = address!("595475934ed7d9faa7fca28341c2ce583904a44e");
const BRIDGE: Address = address!("E82c61Ac9Ec2041b493118051afa4F18a55dC876");
const CALLBACK_RUNTIME: &[u8] = include_bytes!(
    "../../../../ethereum/evm/src/hardfork/bytecodes/oracle_task_install/NativeOracle.bin"
);

sol! {
    struct OracleTask { bytes config; uint64 updatedAt; }
    function getSourceTypes() external view returns (uint32[]);
    function getSourceIds(uint32 sourceType) external view returns (uint256[]);
    function getTaskNames(uint32 sourceType, uint256 sourceId) external view returns (bytes32[]);
    function getTask(uint32 sourceType, uint256 sourceId, bytes32 taskName) external view returns (OracleTask);
    function getDefaultCallback(uint32 sourceType) external view returns (address);
}

fn read<C: SolCall>(chain: &Chain<'_>, address: Address, call: C, number: u64) -> C::Return {
    C::abi_decode_returns(&chain.call(address, call.abi_encode().into(), number)).unwrap()
}

#[test]
fn oracle_task_install_canonical_pipe_epoch_boundary() {
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let install_time = start + 2;
    let mut genesis: serde_json::Value =
        serde_json::from_str(include_str!("mainnet_genesis.json")).unwrap();
    genesis["config"]["chainId"] = json!(7771625);
    for fork in ["pragueTime", "alphaTime", "betaTime"] {
        genesis["config"][fork] = json!(0);
    }
    genesis["config"]["gammaTime"] = json!(start + 1);
    genesis["config"]["oracleTaskInstallTime"] = json!(install_time);
    let receiver_code: Bytes =
        genesis["alloc"][format!("{RECEIVER:#x}")]["code"].as_str().unwrap().parse().unwrap();
    genesis["config"]["oracleTaskInstall"] = json!({
        "sourceId": "0x1", "taskName": B256::repeat_byte(1),
        "taskConfig": "0x616263", "receiverCodeHash": keccak256(receiver_code),
        "trustedBridge": BRIDGE,
    });
    // Keep real initialized protocol contracts and Receiver immutables. Only
    // discovery/task state and source0's default start empty.
    genesis["alloc"][format!("{TASK_CONFIG:#x}")]["storage"] = json!({});
    let default_slot = U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
    genesis["alloc"][format!("{NATIVE_ORACLE:#x}")]["storage"]
        .as_object_mut()
        .unwrap()
        .retain(|key, _| key.parse::<U256>().unwrap() != default_slot);
    genesis["alloc"]["0x00000000000000000000000000000001625f1005"]["storage"]["0x00"] =
        json!(B256::from((U256::from(1) << 136) | U256::from(1_000_000)));

    node::run_node(&genesis.to_string(), "data/oracle_task_install_epoch", move |builder| {
        run_epoch_boundary(builder, start, install_time)
    });
}

async fn run_epoch_boundary(builder: Builder, start: u64, install_time: u64) -> eyre::Result<()> {
    let handle = builder
        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
        .with_components(EthereumNode::gravity_components())
        .with_add_ons(EthereumAddOns::default())
        .launch_with_fn(|builder| {
            let launcher = EngineNodeLauncher::new(
                builder.task_executor().clone(),
                builder.config().datadir(),
                reth_engine_primitives::TreeConfig::default(),
            );
            builder.launch_with(launcher)
        })
        .await?;
    let spec = handle.node.chain_spec();
    let eth_api = handle.node.rpc_registry.eth_api().clone();
    let rpc = RpcClient::new(handle.node.rpc_server_handle().http_local_addr().unwrap());
    let provider = handle.node.provider;
    let genesis_header = provider.header_by_number(0)?.unwrap();
    let (args_tx, args_rx) = tokio::sync::oneshot::channel();
    let pipe = new_pipe_exec_layer_api(
        spec.clone(),
        BlockViewStorage::new(provider.clone()),
        genesis_header.clone(),
        provider.block_hash(0)?.unwrap(),
        args_rx,
        eth_api.clone(),
    );
    args_tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();
    let mut node = Node::new(pipe, &eth_api, genesis_header.timestamp).await;
    let first = tokio::time::timeout(
        BLOCK_TIMEOUT,
        node.produce_block(BlockInput {
            timestamp_us: (start + 1) * 1_000_000,
            ..Default::default()
        }),
    )
    .await
    .expect("Gamma block persistence");
    assert!(!first.epoch_changed);
    assert!(node.dkg_in_progress(), "first block must open the actual DKG session");
    tokio::task::block_in_place(|| {
        let chain = Chain::new(&rpc, spec.chain.id());
        assert_eq!(
            keccak256(chain.code(NATIVE_ORACLE, first.number)),
            keccak256(include_bytes!("../../../../ethereum/evm/src/hardfork/bytecodes/gamma/NativeOracle.bin"))
        );
        assert!(read(&chain, TASK_CONFIG, getSourceTypesCall {}, first.number).is_empty());
        assert_eq!(
            read(&chain, NATIVE_ORACLE, getDefaultCallbackCall { sourceType: 0 }, first.number),
            Address::ZERO
        );
    });
    let activation = tokio::time::timeout(
        BLOCK_TIMEOUT,
        node.produce_block(BlockInput {
            timestamp_us: install_time * 1_000_000,
            may_change_epoch: true,
            ..Default::default()
        }),
    )
    .await
    .expect("installation epoch block persistence");
    assert!(activation.epoch_changed, "exercise canonical EpochChanged early return");
    assert_eq!(activation.timestamp, install_time);
    assert_eq!(activation.epoch, first.epoch + 1);
    tokio::task::block_in_place(|| {
        let chain = Chain::new(&rpc, spec.chain.id());
        assert_eq!(chain.code(NATIVE_ORACLE, activation.number).as_ref(), CALLBACK_RUNTIME);
        assert_eq!(read(&chain, TASK_CONFIG, getSourceTypesCall {}, activation.number), vec![0]);
        assert_eq!(
            read(&chain, TASK_CONFIG, getSourceIdsCall { sourceType: 0 }, activation.number),
            vec![U256::from(1)]
        );
        assert_eq!(
            read(
                &chain,
                TASK_CONFIG,
                getTaskNamesCall { sourceType: 0, sourceId: U256::from(1) },
                activation.number
            ),
            vec![B256::repeat_byte(1)]
        );
        let task = read(
            &chain,
            TASK_CONFIG,
            getTaskCall { sourceType: 0, sourceId: U256::from(1), taskName: B256::repeat_byte(1) },
            activation.number,
        );
        assert_eq!(task.config.as_ref(), b"abc");
        assert_eq!(task.updatedAt, install_time);
        assert_eq!(
            read(
                &chain,
                NATIVE_ORACLE,
                getDefaultCallbackCall { sourceType: 0 },
                activation.number
            ),
            RECEIVER
        );
        // Check persisted slot directly as well: RPC replay must not be able to
        // supply an installation that canonical commit omitted.
        let default = U256::from_be_bytes(keccak256((U256::ZERO, U256::from(2)).abi_encode()).0);
        assert_eq!(chain.storage(NATIVE_ORACLE, default, activation.number), RECEIVER.into_word());
        let map =
            |key: U256, root: U256| U256::from_be_bytes(keccak256((key, root).abi_encode()).0);
        let task_base = map(
            U256::from_be_bytes(B256::repeat_byte(1).0),
            map(U256::from(1), map(U256::ZERO, U256::from(1))),
        );
        assert_eq!(
            chain.storage(TASK_CONFIG, task_base + U256::from(1), activation.number),
            B256::from(U256::from(install_time))
        );
    });
    let following = tokio::time::timeout(
        BLOCK_TIMEOUT,
        node.produce_block(BlockInput {
            timestamp_us: (install_time + 1) * 1_000_000,
            ..Default::default()
        }),
    )
    .await
    .expect("post-installation normal block persistence");
    assert!(!following.epoch_changed);
    tokio::task::block_in_place(|| {
        let chain = Chain::new(&rpc, spec.chain.id());
        assert_eq!(chain.code(NATIVE_ORACLE, following.number).as_ref(), CALLBACK_RUNTIME);
        assert_eq!(
            read(
                &chain,
                TASK_CONFIG,
                getTaskCall { sourceType: 0, sourceId: U256::from(1), taskName: B256::repeat_byte(1) },
                following.number
            ).updatedAt,
            install_time
        );
    });
    Ok(())
}
