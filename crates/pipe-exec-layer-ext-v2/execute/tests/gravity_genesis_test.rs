#![allow(missing_docs)]
#![allow(dead_code)]

use std::{fmt::Debug, sync::Arc};

use alloy_consensus::Header;
use alloy_primitives::{address, Address, Bytes, TxKind, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::on_chain_config::{
    validator_config::ValidatorConfig, validator_info::ValidatorInfo as GravityValidatorInfo,
    validator_set::ValidatorSet as GravityValidatorSet,
};
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::{launcher::FnLauncher, NodeCommand};
use reth_cli_runner::CliRunner;
use reth_db::DatabaseEnv;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_evm::{ConfigureEvm, Evm};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_builder::{EngineNodeLauncher, NodeBuilder, WithLaunchContext};
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_provider::{providers::BlockchainProvider, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, State};
use reth_tracing::{
    tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer,
};
use revm::{context::TxEnv, Database};

// Test addresses matching gravity_chain_core_contracts/src/foundation/SystemAddresses.sol
const GRAVITY_FRAMEWORK_ADDRESS: Address = address!("0x0000000000000000000000000000000000000000");
const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000001625f2003");
const BLOCK_MODULE_ADDRESS: Address = address!("00000000000000000000000000000001625f2004");
const CONSENSUS_CONFIG_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000001625f1007");
const VALIDATOR_SET_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000001625f2001"); // VALIDATOR_MANAGER
const EPOCH_MANAGER_ADDRESS: Address = address!("00000000000000000000000000000001625f2003"); // RECONFIGURATION

// sol! {
//     contract EpochManager {
//         function getCurrentEpochInfo() external view returns (uint64 epoch, uint64
// lastTransitionTime, uint64 interval);     }
// }

sol! {
    function getCurrentEpochInfo() external view returns (uint64 epoch, uint64 lastTransitionTime, uint64 interval);
}

sol! {
    contract ConsensusConfigContract {
        function setForNextEpoch(bytes calldata newConfig) external onlyAptosFramework;
        function getCurrentConfig() external view returns (bytes memory);
        function getPendingConfig() external view returns (bytes memory, bool);
    }
}

sol! {
    contract ValidatorManager {
        enum ValidatorStatus {
            PENDING_ACTIVE, // 0
            ACTIVE, // 1
            PENDING_INACTIVE, // 2
            INACTIVE // 3
        }

        // Commission structure
        struct Commission {
            uint64 rate; // the commission rate charged to delegators(10000 is 100%)
            uint64 maxRate; // maximum commission rate which validator can ever charge
            uint64 maxChangeRate; // maximum daily increase of the validator commission
        }

        /// Complete validator information (merged from multiple contracts)
        struct ValidatorInfo {
            // Basic information (from ValidatorManager)
            bytes consensusPublicKey;
            Commission commission;
            string moniker;
            bool registered;
            address stakeCreditAddress;
            ValidatorStatus status;
            uint256 votingPower; // Changed from uint64 to uint256 to prevent overflow
            uint256 validatorIndex;
            uint256 updateTime;
            address operator;
            bytes validatorNetworkAddresses; // BCS serialized Vec<NetworkAddress>
            bytes fullnodeNetworkAddresses; // BCS serialized Vec<NetworkAddress>
            bytes aptosAddress; // [u8; 32]
        }

        struct ValidatorSet {
            ValidatorInfo[] activeValidators; // Active validators for the current epoch
            ValidatorInfo[] pendingInactive; // Pending validators to leave in next epoch (still active)
            ValidatorInfo[] pendingActive; // Pending validators to join in next epoch
            uint256 totalVotingPower; // Current total voting power
            uint256 totalJoiningPower; // Total voting power waiting to join in the next epoch
        }

        function getValidatorSet() external returns (ValidatorSet memory);

        event Log(string message, uint256 value);
    }
}

pub fn convert_account(acc: &Address) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(acc.as_slice());
    // ExternalAccountAddress::new(bytes)
    bytes
}

/// Convert Solidity `ValidatorInfo` to Gravity API `ValidatorInfo`
pub fn convert_validator_info(
    solidity_info: &ValidatorManager::ValidatorInfo,
) -> GravityValidatorInfo {
    println!("solidity_info called");
    // Convert Address to AccountAddress (20 bytes -> AccountAddress)
    let account_address =
        gravity_api_types::u256_define::AccountAddress::from_bytes(&solidity_info.aptosAddress);

    todo!()
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

fn test_gravity_system_call<DB: Database<Error: Debug + Send + Sync + 'static> + Debug>(
    db: DB,
    evm_config: EthEvmConfig,
) {
    let evm_env = evm_config.evm_env(&Header {
        gas_limit: 30_000_000,
        excess_blob_gas: Some(0),
        ..Default::default()
    });
    let db = State::builder().with_bundle_update().with_database(db).build();
    let mut evm = evm_config.evm_with_env(db, evm_env.unwrap());
    let result = evm
        .transact_raw(new_system_call_txn(
            EPOCH_MANAGER_ADDRESS,
            getCurrentEpochInfoCall {}.abi_encode().into(),
        ))
        .unwrap();
    let returns =
        getCurrentEpochInfoCall::abi_decode_returns(result.result.output().unwrap()).unwrap();
    assert_eq!(returns.epoch, 1);

    let result = evm
        .transact_raw(new_system_call_txn(
            VALIDATOR_SET_CONTRACT_ADDRESS,
            ValidatorManager::getValidatorSetCall {}.abi_encode().into(),
        ))
        .unwrap();
    let solidity_validator_set =
        ValidatorManager::getValidatorSetCall::abi_decode_returns(result.result.output().unwrap())
            .unwrap();
    result.result.logs().iter().for_each(|log| {
        if let Ok(parsed) = ValidatorManager::Log::decode_log(log) {
            println!("txn event Log: {:?}, {:?}.", parsed.message, parsed.value);
        }
    });

    // Convert to Gravity validator set
    let gravity_validator_set = GravityValidatorSet {
        active_validators: solidity_validator_set
            .activeValidators
            .iter()
            .map(convert_validator_info)
            .collect(),
        pending_inactive: solidity_validator_set
            .pendingInactive
            .iter()
            .map(convert_validator_info)
            .collect(),
        pending_active: solidity_validator_set
            .pendingActive
            .iter()
            .map(convert_validator_info)
            .collect(),
        total_voting_power: solidity_validator_set.totalVotingPower.to::<u128>(),
        total_joining_power: solidity_validator_set.totalJoiningPower.to::<u128>(),
    };
    println!("gravity_validator_set: {gravity_validator_set:?}");
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
            String::new(),
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
                FnLauncher::new::<EthereumChainSpecParser, _>(
                    |builder: WithLaunchContext<
                        NodeBuilder<
                            Arc<DatabaseEnv>,
                            <EthereumChainSpecParser as ChainSpecParser>::ChainSpec,
                        >,
                    >,
                     _| async move {
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
                    },
                ),
            )
        })
        .unwrap();
}
