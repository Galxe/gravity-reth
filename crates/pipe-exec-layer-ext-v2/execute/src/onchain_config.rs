use crate::{ExecuteOrderedBlockResult, OrderedBlock};
use alloy_consensus::{constants::EMPTY_WITHDRAWALS, Header, TxLegacy, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE, BlockId};
use alloy_primitives::{address, Address, Bytes, PrimitiveSignature, TxKind, U256};
use alloy_rpc_types_eth::{state::EvmOverrides, TransactionInput, TransactionRequest};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::{
    config_storage::{OnChainConfig, OnChainConfigResType},
    events::contract_event::GravityEvent, on_chain_config::validator_config::ValidatorConfig,
    on_chain_config::validator_info::ValidatorInfo as GravityValidatorInfo,
    on_chain_config::validator_set::ValidatorSet as GravityValidatorSet,
};
use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};
use reth_evm::Evm;
use reth_execution_types::BlockExecutionOutput;
use reth_primitives::Receipt;
use reth_rpc_eth_api::helpers::EthCall;
use revm::db::BundleState;
use revm_primitives::{EvmState, ExecutionResult};
use std::{fmt::Debug, sync::OnceLock};
use tokio::runtime::Runtime;

// Gravity-aptos compatible types
use bcs;
use serde::{Deserialize, Serialize};

// Solidity struct definitions for validator set
sol! {
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
        address payable feeAddress; // Fee receiving address
        bytes voteAddress; // BLS voting address
        Commission commission;
        string moniker;
        uint256 createdTime;
        bool registered;
        address stakeCreditAddress;
        ValidatorStatus status;
        uint256 votingPower; // Changed from uint64 to uint256 to prevent overflow
        uint256 validatorIndex;
        uint256 lastEpochActive;
        uint256 updateTime; // Last update time
        address operator;
    }

    struct ValidatorSet {
        uint8 consensusScheme; // Consensus scheme (0 for BFT)
        ValidatorInfo[] activeValidators; // Active validators for the current epoch
        ValidatorInfo[] pendingInactive; // Pending validators to leave in next epoch (still active)
        ValidatorInfo[] pendingActive; // Pending validators to join in next epoch
        uint256 totalVotingPower; // Current total voting power
        uint256 totalJoiningPower; // Total voting power waiting to join in the next epoch
    }

    function getValidatorSet() external view returns (ValidatorSet memory);
}

const GRAVITY_FRAMEWORK_ADDRESS: Address = address!("00000000000000000000000000000000000000ff");
const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000000000000f0");
const BLOCK_MODULE_ADDRESS: Address = address!("00000000000000000000000000000000000000f1");
const CONSENSUS_CONFIG_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f2");
const VALIDATOR_SET_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f3");

pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");
pub const GENESIS_ADDR: Address = address!("0000000000000000000000000000000000001008");
pub const SYSTEM_CALLER: Address = address!("00000000000000000000000000000000000000ff");
pub const PERFORMANCE_TRACKER_ADDR: Address = address!("00000000000000000000000000000000000000f1");
pub const EPOCH_MANAGER_ADDR: Address = address!("00000000000000000000000000000000000000f3");
pub const STAKE_CONFIG_ADDR: Address = address!("0000000000000000000000000000000000002008");
pub const DELEGATION_ADDR: Address = address!("0000000000000000000000000000000000002009");
pub const VALIDATOR_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002010");
pub const VALIDATOR_PERFORMANCE_TRACKER_ADDR: Address =
    address!("000000000000000000000000000000000000200b");
pub const BLOCK_ADDR: Address = address!("0000000000000000000000000000000000002001");
pub const TIMESTAMP_ADDR: Address = address!("0000000000000000000000000000000000002004");
pub const JWK_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002002");
pub const KEYLESS_ACCOUNT_ADDR: Address = address!("000000000000000000000000000000000000200A");
pub const SYSTEM_REWARD_ADDR: Address = address!("0000000000000000000000000000000000001002");
pub const GOV_HUB_ADDR: Address = address!("0000000000000000000000000000000000001007");
pub const STAKE_CREDIT_ADDR: Address = address!("0000000000000000000000000000000000002003");
pub const GOV_TOKEN_ADDR: Address = address!("0000000000000000000000000000000000002005");
pub const GOVERNOR_ADDR: Address = address!("0000000000000000000000000000000000002006");
pub const TIMELOCK_ADDR: Address = address!("0000000000000000000000000000000000002007");

static ETH_CALL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Debug)]
pub(crate) struct OnchainConfigFetcher<EthApi> {
    eth_api: EthApi,
}

impl<EthApi> OnchainConfigFetcher<EthApi>
where
    EthApi: EthCall,
{
    pub(crate) fn new(eth_api: EthApi) -> Self {
        Self { eth_api }
    }

    /// Simulate the call to the contract at block number and return the result.
    fn eth_call(&self, from: Address, to: Address, input: Bytes, block_number: u64) -> Bytes {
        let rt_handle = ETH_CALL_RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4.min(std::thread::available_parallelism().unwrap().get()))
                    .thread_name("OnchainConfigFetcher")
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime")
            })
            .handle();
        // TODO(nekomoto): Handle the case where the block is not found.
        tokio::task::block_in_place(|| {
            rt_handle.block_on(async {
                const RETRY: u64 = 5;
                let mut count = 0;
                loop {
                    match self
                        .eth_api
                        .call(
                            TransactionRequest {
                                from: Some(from),
                                to: Some(TxKind::Call(to)),
                                input: TransactionInput::new(input.clone()),
                                ..Default::default()
                            },
                            Some(BlockId::from(block_number)),
                            EvmOverrides::new(None, None),
                        )
                        .await
                    {
                        Ok(result) => return result,
                        Err(err) => {
                            tracing::warn!(
                                "Failed to execute eth_call at {block_number}, retrying... (attempt {count}/{RETRY}): {err}"
                            );
                            count += 1;
                            if count > RETRY {
                                panic!("Failed to execute eth_call: {err}");
                            }
                            // Sleep for a short duration before retrying
                            tokio::time::sleep(std::time::Duration::from_millis(10 * count)).await;
                        }
                    }
                }
            })
        })
    }

    #[cfg(not(feature = "pipe_test"))]
    pub(crate) fn fetch_epoch(&self, block_number: u64) -> u64 {
        sol! {
            function getCurrentEpoch() external view returns (uint256 epoch, uint256 lastTransitionTime, uint256 interval);
        }

        let call = getCurrentEpochCall {};
        let input: Bytes = call.abi_encode().into();
        let result =
            self.eth_call(SYSTEM_CALLER, EPOCH_MANAGER_ADDR, input, block_number);
        let epoch_info = getCurrentEpochCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getCurrentEpoch return value");
        epoch_info.epoch.to::<u64>()
    }

    #[cfg(feature = "pipe_test")]
    pub(crate) fn fetch_epoch(&self, block_number: u64) -> u64 {
        0
    }

    pub(crate) fn fetch_consensus_config(&self, block_number: u64) -> Bytes {
        sol! {
            function getCurrentConfig() external view returns (bytes memory);
        }
        let call = getCurrentConfigCall {};
        let input: Bytes = call.abi_encode().into();
        let result = self.eth_call(
            GRAVITY_FRAMEWORK_ADDRESS,
            CONSENSUS_CONFIG_CONTRACT_ADDRESS,
            input,
            block_number,
        );
        getCurrentConfigCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getCurrentConfig return value")
            ._0
    }

    pub(crate) fn fetch_validator_set(&self, block_number: u64) -> Bytes {
        let call = getValidatorSetCall {};
        let input: Bytes = call.abi_encode().into();
        
        let result = self.eth_call(SYSTEM_CALLER, VALIDATOR_MANAGER_ADDR, input, block_number);
        
        // Decode the Solidity validator set
        let solidity_validator_set = getValidatorSetCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getValidatorSet return value");

        // Convert Solidity validator infos to Gravity API validator infos
        let convert_validator_info = |solidity_info: &ValidatorInfo| -> GravityValidatorInfo {
            GravityValidatorInfo::new(
                gravity_api_types::u256_define::AccountAddress::from_bytes(&solidity_info.feeAddress.to_vec()),
                solidity_info.votingPower.to::<u64>(),
                ValidatorConfig::new(
                    solidity_info.consensusPublicKey.clone().into(),
                    solidity_info.voteAddress.clone().into(),
                    vec![], // fullnode_network_addresses - empty for now
                    solidity_info.validatorIndex.to::<u64>(),
                ),
            )
        };

        // Convert to Gravity validator set and serialize to BCS format
        let gravity_validator_set = GravityValidatorSet {
            active_validators: solidity_validator_set._0.activeValidators
                .iter()
                .map(convert_validator_info)
                .collect(),
            pending_inactive: solidity_validator_set._0.pendingInactive
                .iter()
                .map(convert_validator_info)
                .collect(),
            pending_active: solidity_validator_set._0.pendingActive
                .iter()
                .map(convert_validator_info)
                .collect(),
            total_voting_power: solidity_validator_set._0.totalVotingPower.to::<u128>(),
            total_joining_power: solidity_validator_set._0.totalJoiningPower.to::<u128>(),
        };
        
        bcs::to_bytes(&gravity_validator_set)
            .expect("Failed to serialize validator set")
            .into()
    }

    pub(crate) fn fetch_config_bytes(
        &self,
        config_name: OnChainConfig,
        block_number: u64,
    ) -> OnChainConfigResType {
        match config_name {
            OnChainConfig::ConsensusConfig => self.fetch_consensus_config(block_number).0.into(),
            OnChainConfig::Epoch => self.fetch_epoch(block_number).into(),
            OnChainConfig::ValidatorSet => self.fetch_validator_set(block_number).0.into(),
            _ => todo!("Implement fetching for other config types"),
        }
    }
}

pub(crate) struct MetadataTxnResult {
    pub result: ExecutionResult,
    pub txn: TransactionSigned,
}

impl MetadataTxnResult {
    pub(crate) fn emit_new_epoch(&self) -> Option<(u64, Bytes)> {
        sol! {
            event NewEpoch(uint64 indexed epoch, bytes validators);
        }

        for log in self.result.logs() {
            match NewEpoch::decode_log(log, false) {
                Ok(event) => return Some((event.epoch, event.validators.clone().into())),
                Err(_) => continue,
            }
        }
        None
    }

    pub(crate) fn into_executed_ordered_block_result(
        self,
        ordered_block: &OrderedBlock,
        state: BundleState,
        validators: Bytes,
    ) -> ExecuteOrderedBlockResult {
        let tx_type = self.txn.tx_type();
        let mut block = Block {
            header: Header {
                beneficiary: ordered_block.coinbase,
                timestamp: ordered_block.timestamp,
                mix_hash: ordered_block.prev_randao,
                base_fee_per_gas: Some(0),
                number: ordered_block.number,
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                nonce: BEACON_NONCE.into(),
                ..Default::default()
            },
            body: BlockBody { transactions: vec![self.txn], ..Default::default() },
        };

        // Shanghai fork fields
        block.header.withdrawals_root = Some(EMPTY_WITHDRAWALS);
        block.body.withdrawals = Some(Withdrawals::default());

        // Cancun fork fields
        // FIXME: Is it OK to use the parent's block id as `parent_beacon_block_root` before
        // execution?
        block.header.parent_beacon_block_root = Some(ordered_block.parent_id);
        block.header.excess_blob_gas = Some(0);
        block.header.blob_gas_used = Some(0);

        let new_epoch = ordered_block.epoch + 1;
        ExecuteOrderedBlockResult {
            block,
            senders: vec![GRAVITY_FRAMEWORK_ADDRESS],
            execution_output: BlockExecutionOutput {
                state,
                receipts: vec![Receipt {
                    tx_type,
                    success: true,
                    cumulative_gas_used: 0,
                    logs: self.result.into_logs(),
                }],
                requests: Default::default(),
                gas_used: 0,
            },
            txs_info: vec![],
            gravity_events: vec![GravityEvent::NewEpoch(new_epoch, validators.clone().into())],
            epoch: new_epoch,
        }
    }

    pub(crate) fn insert_to_executed_ordered_block_result(
        self,
        result: &mut ExecuteOrderedBlockResult,
    ) {
        result.execution_output.receipts.insert(
            0,
            Receipt {
                tx_type: self.txn.tx_type(),
                success: true,
                cumulative_gas_used: 0,
                logs: self.result.into_logs(),
            },
        );
        result.block.body.transactions.insert(0, self.txn);
        result.senders.insert(0, GRAVITY_FRAMEWORK_ADDRESS);
    }
}

fn new_system_call_txn(contract: Address, input: Bytes) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce: 0,
            gas_price: 0,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        PrimitiveSignature::test_signature(),
    )
}

pub(crate) fn transact_metadata_contract_call(
    evm: &mut impl Evm<Error: Debug>,
    timestamp_us: u64,
) -> (MetadataTxnResult, EvmState) {
    sol! {
        function blockPrologue(uint64 _timestamp_microseconds) external onlyVm whenInitialized;
    }

    let call = blockPrologueCall { _timestamp_microseconds: timestamp_us };
    let input: Bytes = call.abi_encode().into();
    let mut result = evm
        .transact_system_call(GRAVITY_FRAMEWORK_ADDRESS, BLOCK_MODULE_ADDRESS, input.clone())
        .unwrap();
    assert!(result.result.is_success(), "Failed to execute blockPrologue: {:?}", result.result);
    result.state.remove(&GRAVITY_FRAMEWORK_ADDRESS);
    result.state.remove(&evm.block().coinbase);
    (
        MetadataTxnResult {
            result: result.result,
            txn: new_system_call_txn(BLOCK_MODULE_ADDRESS, input),
        },
        result.state,
    )
}
