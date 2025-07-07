#![allow(missing_docs)]
use std::fmt::Debug;

use alloy_consensus::Header;
use alloy_primitives::{address, Address, Bytes, TxKind, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_cli_commands::{launcher::FnLauncher, NodeCommand};
use reth_cli_runner::CliRunner;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_evm::{ConfigureEvm, Evm};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_builder::EngineNodeLauncher;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_provider::{providers::BlockchainProvider, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, State};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use revm::{context::TxEnv, Database};

const GRAVITY_FRAMEWORK_ADDRESS: Address = address!("00000000000000000000000000000000000000ff");
const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000000000000f0");
const BLOCK_MODULE_ADDRESS: Address = address!("00000000000000000000000000000000000000f1");
const CONSENSUS_CONFIG_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f2");
const VALIDATOR_SET_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f3");

sol! {
    contract Reconfiguration {
        function getCurrentEpoch() external view returns (uint64);
    }
}

sol! {
    contract ConsensusConfigContract {
        function setForNextEpoch(bytes calldata newConfig) external onlyAptosFramework;
        function getCurrentConfig() external view returns (bytes memory);
        function getPendingConfig() external view returns (bytes memory, bool);
    }
}

sol! {
    contract ValidatorSetContract {
        function setForNextEpoch(bytes calldata newConfig) external onlyAptosFramework;
        function getCurrentConfig() external view returns (bytes memory);
        function getPendingConfig() external view returns (bytes memory, bool);
    }
}

fn new_system_call_txn(contract: Address, input: Bytes) -> TxEnv {
    TxEnv {
        caller: GRAVITY_FRAMEWORK_ADDRESS,
        gas_limit: 30_000_000,
        gas_price: 0,
        kind: TxKind::Call(contract),
        value: U256::ZERO,
        data: input,
        chain_id: None,
        ..Default::default()
    }
}

fn test_gravity_system_call<DB: Database<Error: Debug + Send + Sync + 'static>>(
    db: DB,
    evm_config: EthEvmConfig,
) {
    let evm_env = evm_config.evm_env(&Header {
        gas_limit: 30_000_000,
        excess_blob_gas: Some(0),
        ..Default::default()
    });
    let db = State::builder().with_bundle_update().with_database(db).build();
    let mut evm = evm_config.evm_with_env(db, evm_env);
    let result = evm
        .transact_raw(new_system_call_txn(
            RECONFIGURATION_ADDRESS,
            Reconfiguration::getCurrentEpochCall {}.abi_encode().into(),
        ))
        .unwrap();
    let returns =
        Reconfiguration::getCurrentEpochCall::abi_decode_returns(result.result.output().unwrap())
            .unwrap();
    assert_eq!(returns, 1);

    let result = evm
        .transact_raw(new_system_call_txn(
            VALIDATOR_SET_CONTRACT_ADDRESS,
            ValidatorSetContract::getCurrentConfigCall {}.abi_encode().into(),
        ))
        .unwrap();
    let returns = ValidatorSetContract::getCurrentConfigCall::abi_decode_returns(
        result.result.output().unwrap(),
    )
    .unwrap();
    assert_eq!(
        returns,
        Bytes::from([
            0, 1, 45, 134, 180, 10, 29, 105, 44, 7, 73, 160, 160, 66, 110, 32, 33, 238, 36,
            226, 67, 13, 160, 245, 187, 156, 42, 230, 197, 134, 191, 62, 10, 15, 1, 0, 0, 0, 0,
            0, 0, 0, 48, 133, 29, 65, 147, 45, 134, 111, 95, 171, 237, 102, 115, 137, 142, 21,
            71, 62, 106, 10, 220, 245, 3, 61, 44, 147, 129, 108, 107, 17, 92, 133, 173, 52, 81,
            224, 186, 198, 29, 87, 13, 94, 217, 242, 62, 30, 127, 119, 196, 11, 1, 9, 2, 0,
            127, 0, 0, 1, 5, 232, 7, 11, 1, 9, 2, 0, 127, 0, 0, 1, 5, 232, 7, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ])
    );
}

#[test]
fn test() {
    std::panic::set_hook(Box::new({
        |panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            eprintln!("Panic occurred: {panic_info}\nBacktrace:\n{backtrace}");
            std::process::exit(1);
        }
    }));

    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(
            LogFormat::Terminal,
            LevelFilter::DEBUG.to_string(),
            "".to_string(),
            Some("always".to_string()),
        ))
        .init();

    let runner = CliRunner::try_default_runtime().unwrap();
    let command: NodeCommand<EthereumChainSpecParser> = NodeCommand::try_parse_args_from([
        "reth",
        "--chain",
        "gravity.json",
        "--with-unused-ports",
        "--dev",
        "--datadir",
        "gravity_genesis_test_data",
    ])
    .unwrap();

    runner
        .run_command_until_exit(|ctx| {
            command.execute(
                ctx,
                FnLauncher::new::<EthereumChainSpecParser, _>(|builder, _| async move {
                    let handle = builder
                        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
                        .with_components(EthereumNode::components())
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
                    let db = StateProviderDatabase::new(handle.node.provider.latest().unwrap());
                    test_gravity_system_call(db, EthEvmConfig::new(handle.node.chain_spec()));
                    Ok(())
                }),
            )
        })
        .unwrap();
}
