#![allow(missing_docs)]
use std::fmt::Debug;

use alloy_primitives::{address, Address, Bytes, TxKind, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_cli_commands::NodeCommand;
use reth_cli_runner::CliRunner;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_builder::{engine_tree_config, EngineNodeLauncher};
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_provider::{providers::BlockchainProvider, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use revm::{
    primitives::{Env, SpecId, TxEnv},
    Database, DatabaseCommit, EvmBuilder, StateBuilder,
};

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
        gas_price: U256::ZERO,
        transact_to: TxKind::Call(contract),
        value: U256::ZERO,
        data: input,
        ..Default::default()
    }
}

fn test_gravity_system_call<DB: Database<Error: Debug>>(db: DB) {
    let mut env = Env::default();
    env.cfg.chain_id = 7771625;
    let db = StateBuilder::new().with_bundle_update().with_database(db).build();
    let mut evm = EvmBuilder::default()
        .with_db(db)
        .with_spec_id(SpecId::LATEST)
        .with_env(Box::new(env))
        .build();

    *evm.tx_mut() = new_system_call_txn(
        RECONFIGURATION_ADDRESS,
        Reconfiguration::getCurrentEpochCall {}.abi_encode().into(),
    );
    let result = evm.transact().unwrap();
    let returns = Reconfiguration::getCurrentEpochCall::abi_decode_returns(
        result.result.output().unwrap(),
        false,
    )
    .unwrap();
    assert_eq!(returns._0, 1);

    // *evm.tx_mut() = new_system_call_txn(
    //     CONSENSUS_CONFIG_CONTRACT_ADDRESS,
    //     ConsensusConfigContract::getCurrentConfigCall {}.abi_encode().into(),
    // );
    // let result = evm.transact().unwrap();
    // let returns = ConsensusConfigContract::getCurrentConfigCall::abi_decode_returns(
    //     result.result.output().unwrap(),
    //     false,
    // )
    // .unwrap();
    // assert_eq!(
    //     returns._0,
    //     Bytes::from([
    //         3, 1, 1, 10, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 10, 0, 0, 0,
    //         0, 0, 0, 0, 1, 0, 0, 0,
    //     ])
    // );

    *evm.tx_mut() = new_system_call_txn(
        VALIDATOR_SET_CONTRACT_ADDRESS,
        ValidatorSetContract::getCurrentConfigCall {}.abi_encode().into(),
    );
    let result = evm.transact().unwrap();
    let returns = ValidatorSetContract::getCurrentConfigCall::abi_decode_returns(
        result.result.output().unwrap(),
        false,
    )
    .unwrap();
    assert_eq!(
        returns._0,
        Bytes::from([
            0, 1, 202, 175, 197, 182, 88, 240, 89, 13, 126, 49, 222, 145, 237, 222, 127, 5,
            174, 145, 150, 29, 8, 4, 236, 99, 77, 117, 53, 150, 155, 125, 23, 31, 1, 0, 0, 0,
            0, 0, 0, 0, 48, 153, 255, 137, 244, 83, 217, 169, 191, 39, 62, 58, 232, 182, 27,
            153, 162, 179, 54, 237, 199, 182, 235, 155, 142, 48, 130, 73, 253, 89, 243, 183,
            98, 17, 119, 29, 126, 13, 170, 169, 127, 173, 17, 81, 140, 74, 216, 234, 189, 25,
            1, 23, 47, 105, 112, 52, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 47, 116, 99, 112,
            47, 50, 48, 50, 53, 25, 1, 23, 47, 105, 112, 52, 47, 49, 50, 55, 46, 48, 46, 48,
            46, 49, 47, 116, 99, 112, 47, 50, 48, 50, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0,
        ])
    )
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

    let runner = CliRunner::default();
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
            command.execute(ctx, |builder, _| async move {
                let handle = builder
                    .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
                    .with_components(EthereumNode::components())
                    .with_add_ons(EthereumAddOns::default())
                    .launch_with_fn(|builder| {
                        let launcher = EngineNodeLauncher::new(
                            builder.task_executor().clone(),
                            builder.config().datadir(),
                            engine_tree_config::TreeConfig::default(),
                        );
                        builder.launch_with(launcher)
                    })
                    .await?;
                let db = StateProviderDatabase::new(handle.node.provider.latest().unwrap());
                test_gravity_system_call(db);
                Ok(())
            })
        })
        .unwrap();
}
