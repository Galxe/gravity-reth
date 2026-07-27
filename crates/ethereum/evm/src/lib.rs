//! EVM config for vanilla ethereum.
//!
//! # Revm features
//!
//! This crate does __not__ enforce specific revm features such as `blst` or `c-kzg`, which are
//! critical for revm's evm internals, it is the responsibility of the implementer to ensure the
//! proper features are selected.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use crate::parallel_execute::GrevmExecutor;
use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::Header;
use alloy_evm::{
    eth::{EthBlockExecutionCtx, EthBlockExecutorFactory},
    precompiles::DynPrecompile,
    Database, EthEvmFactory, Evm,
};
use alloy_primitives::{Address, Bytes, U256};
use alloy_rpc_types_engine::ExecutionData;
use core::{convert::Infallible, fmt::Debug};
use gravity_primitives::get_gravity_config;
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardforks, MAINNET};
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::{
    eth::NextEvmEnvAttributes,
    execute::{BasicBlockExecutor, BlockExecutionError},
    parallel_execute::{ParallelExecutor, WrapExecutor},
    ConfigureEvm, EvmEnv, NextBlockEnvAttributes, ParallelDatabase,
};
use reth_primitives_traits::{constants::GRAVITY_TX_GAS_LIMIT_CAP, SealedBlock, SealedHeader};
use revm::{
    context::{
        result::{ExecutionResult, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    context_interface::block::BlobExcessGasAndPrice,
    database::{State, WrapDatabaseRef},
    primitives::hardfork::SpecId,
    DatabaseCommit,
};

#[cfg(feature = "std")]
use reth_evm::{ConfigureEngineEvm, ExecutableTxIterator};
#[cfg(feature = "std")]
use {
    alloy_eips::Decodable2718,
    reth_evm::{EvmEnvFor, ExecutionCtxFor},
    reth_primitives_traits::{SignedTransaction, TxTy},
    reth_storage_errors::any::AnyError,
};

pub use alloy_evm::EthEvm;

mod config;
use alloy_evm::eth::spec::EthExecutorSpec;
pub use config::{revm_spec, revm_spec_by_timestamp_and_block_number};
use reth_ethereum_forks::Hardforks;

/// Helper type with backwards compatible methods to obtain Ethereum executor
/// providers.
#[doc(hidden)]
pub mod execute {
    use crate::EthEvmConfig;

    #[deprecated(note = "Use `EthEvmConfig` instead")]
    pub type EthExecutorProvider = EthEvmConfig;
}

pub mod hardfork;
pub mod parallel_execute;

// ============================================================================
// Gravity system-tx gas-exempt gating
// ============================================================================
//
// Both the canonical execution layer (this crate's serial `transact_system_txn`
// + grevm `parallel_execute.rs::transact_system_txn`) and every RPC replay path
// that re-executes a persisted system tx (sender == `SYSTEM_CALLER`) MUST gate
// the cfg-side fee/balance disables on the SAME predicate, queried against the
// timestamp of the block being executed/replayed. Any drift between callsites
// forks state root on system-tx blocks.
//
// The predicate itself (`is_system_tx_gas_exempt`) is defined in `reth-chainspec`
// alongside `SYSTEM_CALLER`/`is_gravity_system_caller`, so every callsite (this
// crate's serial + grevm twins, the pipe layer's system-tx construction, and all
// RPC replay paths in `reth-rpc-eth-api` / `reth-rpc`) reuses a single function
// without crate-edge gymnastics.

pub use reth_chainspec::{is_gravity_system_caller, is_system_tx_gas_exempt, SYSTEM_CALLER};

mod build;
pub use build::EthBlockAssembler;

mod receipt;
pub use receipt::RethReceiptBuilder;

#[cfg(feature = "test-utils")]
mod test_utils;
#[cfg(feature = "test-utils")]
pub use test_utils::*;

/// Ethereum-related EVM configuration.
#[derive(Debug, Clone)]
pub struct EthEvmConfig<C = ChainSpec, EvmFactory = EthEvmFactory> {
    /// Inner [`EthBlockExecutorFactory`].
    pub executor_factory: EthBlockExecutorFactory<RethReceiptBuilder, Arc<C>, EvmFactory>,
    /// Ethereum block assembler.
    pub block_assembler: EthBlockAssembler<C>,
}

impl EthEvmConfig {
    /// Creates a new Ethereum EVM configuration for the ethereum mainnet.
    pub fn mainnet() -> Self {
        Self::ethereum(MAINNET.clone())
    }
}

impl<ChainSpec> EthEvmConfig<ChainSpec> {
    /// Creates a new Ethereum EVM configuration with the given chain spec.
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self::ethereum(chain_spec)
    }

    /// Creates a new Ethereum EVM configuration.
    pub fn ethereum(chain_spec: Arc<ChainSpec>) -> Self {
        Self::new_with_evm_factory(chain_spec, EthEvmFactory::default())
    }
}

impl<ChainSpec, EvmFactory> EthEvmConfig<ChainSpec, EvmFactory> {
    /// Creates a new Ethereum EVM configuration with the given chain spec and EVM factory.
    pub fn new_with_evm_factory(chain_spec: Arc<ChainSpec>, evm_factory: EvmFactory) -> Self {
        Self {
            block_assembler: EthBlockAssembler::new(chain_spec.clone()),
            executor_factory: EthBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                evm_factory,
            ),
        }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.executor_factory.spec()
    }
}

/// Pin the per-tx gas cap to Gravity's Monad-style value once `Osaka` is active.
///
/// alloy-evm's `for_eth_block` / `for_eth_next_block` and `evm_env_for_payload` set
/// `tx_gas_limit_cap = Some(MAX_TX_GAS_LIMIT_OSAKA)` (EIP-7825, `2^24`) under OSAKA. Gravity
/// overrides that with [`GRAVITY_TX_GAS_LIMIT_CAP`] (30M) so the 30M system transactions clear
/// the cap. Must stay in lockstep with the consensus-side check (`reth-consensus-common`) and
/// the pipe `tx_filter` guard.
const fn apply_gravity_tx_gas_cap(cfg_env: &mut CfgEnv, osaka_active: bool) {
    if osaka_active {
        cfg_env.tx_gas_limit_cap = Some(GRAVITY_TX_GAS_LIMIT_CAP);
    }
}

impl<ChainSpec> ConfigureEvm for EthEvmConfig<ChainSpec>
where
    ChainSpec: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static,
{
    type Primitives = EthPrimitives;
    type Error = Infallible;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>>;
    type BlockAssembler = EthBlockAssembler<ChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv<SpecId>, Self::Error> {
        let mut evm_env = EvmEnv::for_eth_block(
            header,
            self.chain_spec(),
            self.chain_spec().chain().id(),
            self.chain_spec().blob_params_at_timestamp(header.timestamp),
        );
        apply_gravity_tx_gas_cap(
            &mut evm_env.cfg_env,
            self.chain_spec().is_osaka_active_at_timestamp(header.timestamp),
        );
        Ok(evm_env)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv, Self::Error> {
        let mut evm_env = EvmEnv::for_eth_next_block(
            parent,
            NextEvmEnvAttributes {
                timestamp: attributes.timestamp,
                suggested_fee_recipient: attributes.suggested_fee_recipient,
                prev_randao: attributes.prev_randao,
                gas_limit: attributes.gas_limit,
                slot_number: attributes.slot_number,
            },
            self.chain_spec().next_block_base_fee(parent, attributes.timestamp).unwrap_or_default(),
            self.chain_spec(),
            self.chain_spec().chain().id(),
            self.chain_spec().blob_params_at_timestamp(attributes.timestamp),
        );
        apply_gravity_tx_gas_cap(
            &mut evm_env.cfg_env,
            self.chain_spec().is_osaka_active_at_timestamp(attributes.timestamp),
        );
        Ok(evm_env)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error> {
        Ok(EthBlockExecutionCtx {
            tx_count_hint: Some(block.transaction_count()),
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            ommers: &block.body().ommers,
            withdrawals: block.body().withdrawals.as_ref().map(|w| Cow::Borrowed(w.as_slice())),
            extra_data: block.header().extra_data.clone(),
            slot_number: block.header().slot_number,
        })
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<EthBlockExecutionCtx<'_>, Self::Error> {
        Ok(EthBlockExecutionCtx {
            tx_count_hint: None,
            parent_hash: parent.hash(),
            parent_beacon_block_root: attributes.parent_beacon_block_root,
            ommers: &[],
            withdrawals: attributes.withdrawals.map(|w| Cow::Owned(w.into_inner())),
            extra_data: attributes.extra_data,
            slot_number: attributes.slot_number,
        })
    }

    fn parallel_executor<'a, DB: ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<dyn ParallelExecutor<Primitives = Self::Primitives, Error = BlockExecutionError> + 'a>
    {
        if get_gravity_config().disable_grevm {
            Box::new(WrapExecutor::new(BasicBlockExecutor::new(self.clone(), WrapDatabaseRef(db))))
        } else {
            Box::new(GrevmExecutor::new(self.chain_spec().clone(), self, db))
        }
    }

    fn transact_system_txn<DB: Database>(
        &self,
        db: &mut State<DB>,
        mut evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, BlockExecutionError> {
        // revm v40+ removed `set_state_clear_flag`: the database layer always applies
        // post-EIP-161 commit semantics, and grevm's `ParallelState::set_state_clear_flag`
        // is now a no-op stub. The PR #363 invariant (serial `disable-grevm` ↔ parallel
        // grevm backend must agree on system-tx block state roots) is now upheld by
        // default on both sides without a caller-side toggle.

        // Gravity Alpha hardfork: gas-exempt the `SYSTEM_CALLER`-sourced system
        // transactions on the L1 (cfg-side) lever. Combined with the L2
        // (construction-side) `gas_price = 0` at the pipe layer, this drops the
        // SYSTEM_CALLER fee bill to zero while preserving gas metering, calldata,
        // state writes, receipts and `gas_used`.
        //
        // MUST stay byte-identical with the grevm twin in
        // `parallel_execute.rs::transact_system_txn`. Any drift forks state root.
        let block_ts: u64 = evm_env.block_env.timestamp.saturating_to();
        if is_system_tx_gas_exempt(self.chain_spec().as_ref(), block_ts) {
            evm_env.cfg_env.disable_base_fee = true;
            evm_env.cfg_env.disable_balance_check = true;
            // `disable_nonce_check` deliberately left `false` — SYSTEM_CALLER's
            // nonce sequence is part of the protocol contract.
        }

        let (execution_result, evm_state) = {
            let mut evm = self.evm_with_env(&mut *db, evm_env);
            for (addr, precompile) in precompiles {
                Evm::precompiles_mut(&mut evm).apply_precompile(&addr, move |_| Some(precompile));
            }
            let result = Evm::transact_raw(&mut evm, tx_env).map_err(|e| {
                BlockExecutionError::msg(alloc::format!("system txn execution failed: {e:?}"))
            })?;
            (result.result, result.state)
        };
        db.commit(evm_state);
        Ok(execution_result)
    }
}

#[cfg(feature = "std")]
impl<ChainSpec> ConfigureEngineEvm<ExecutionData> for EthEvmConfig<ChainSpec>
where
    ChainSpec: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static,
{
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        let timestamp = payload.payload.timestamp();
        let block_number = payload.payload.block_number();

        let blob_params = self.chain_spec().blob_params_at_timestamp(timestamp);
        let spec =
            revm_spec_by_timestamp_and_block_number(self.chain_spec(), timestamp, block_number);

        // configure evm env based on parent block
        let mut cfg_env = CfgEnv::new()
            .with_chain_id(self.chain_spec().chain().id())
            .with_spec_and_mainnet_gas_params(spec);

        if let Some(blob_params) = &blob_params {
            cfg_env.set_max_blobs_per_tx(blob_params.max_blobs_per_tx);
        }

        apply_gravity_tx_gas_cap(
            &mut cfg_env,
            self.chain_spec().is_osaka_active_at_timestamp(timestamp),
        );

        // derive the EIP-4844 blob fees from the header's `excess_blob_gas` and the current
        // blobparams
        let blob_excess_gas_and_price =
            payload.payload.excess_blob_gas().zip(blob_params).map(|(excess_blob_gas, params)| {
                let blob_gasprice = params.calc_blob_fee(excess_blob_gas);
                BlobExcessGasAndPrice { excess_blob_gas, blob_gasprice }
            });

        let block_env = BlockEnv {
            number: U256::from(block_number),
            beneficiary: payload.payload.fee_recipient(),
            timestamp: U256::from(timestamp),
            difficulty: if spec >= SpecId::MERGE {
                U256::ZERO
            } else {
                payload.payload.as_v1().prev_randao.into()
            },
            prevrandao: (spec >= SpecId::MERGE).then(|| payload.payload.as_v1().prev_randao),
            gas_limit: payload.payload.gas_limit(),
            basefee: payload.payload.saturated_base_fee_per_gas(),
            blob_excess_gas_and_price,
            slot_num: payload.payload.as_v4().map(|v4| v4.slot_number).unwrap_or_default(),
        };

        Ok(EvmEnv { cfg_env, block_env })
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        Ok(EthBlockExecutionCtx {
            tx_count_hint: Some(payload.payload.transactions().len()),
            parent_hash: payload.parent_hash(),
            parent_beacon_block_root: payload.sidecar.parent_beacon_block_root(),
            ommers: &[],
            withdrawals: payload.payload.withdrawals().map(|w| Cow::Borrowed(w.as_slice())),
            extra_data: payload.payload.as_v1().extra_data.clone(),
            slot_number: payload.payload.as_v4().map(|v4| v4.slot_number),
        })
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        let txs = payload.payload.transactions().clone();
        let convert = |tx: Bytes| {
            let tx =
                TxTy::<Self::Primitives>::decode_2718_exact(tx.as_ref()).map_err(AnyError::new)?;
            let signer = tx.try_recover().map_err(AnyError::new)?;
            Ok::<_, AnyError>(tx.with_signer(signer))
        };

        Ok((txs, convert))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header;
    use alloy_genesis::Genesis;
    use reth_chainspec::{Chain, ChainSpec};
    use reth_evm::{execute::ProviderError, EvmEnv};
    use revm::{
        context::{BlockEnv, CfgEnv},
        database::CacheDB,
        database_interface::EmptyDBTyped,
        inspector::NoOpInspector,
    };

    #[test]
    fn test_fill_cfg_and_block_env() {
        // Create a default header
        let header = Header::default();

        // Build the ChainSpec for Ethereum mainnet, activating London, Paris, and Shanghai
        // hardforks
        let chain_spec = ChainSpec::builder()
            .chain(Chain::mainnet())
            .genesis(Genesis::default())
            .london_activated()
            .paris_activated()
            .shanghai_activated()
            .build();

        // Use the `EthEvmConfig` to fill the `cfg_env` and `block_env` based on the ChainSpec,
        // Header, and total difficulty
        let EvmEnv { cfg_env, .. } =
            EthEvmConfig::new(Arc::new(chain_spec.clone())).evm_env(&header).unwrap();

        // Assert that the chain ID in the `cfg_env` is correctly set to the chain ID of the
        // ChainSpec
        assert_eq!(cfg_env.chain_id, chain_spec.chain().id());
    }

    #[test]
    fn test_evm_with_env_default_spec() {
        let evm_config = EthEvmConfig::mainnet();

        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        let evm_env = EvmEnv::default();

        let evm = evm_config.evm_with_env(db, evm_env.clone());

        // Check that the EVM environment
        assert_eq!(evm.block, evm_env.block_env);
        assert_eq!(evm.cfg, evm_env.cfg_env);
    }

    #[test]
    fn test_evm_with_env_custom_cfg() {
        let evm_config = EthEvmConfig::mainnet();

        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        // Create a custom configuration environment with a chain ID of 111
        let cfg = CfgEnv::default().with_chain_id(111);

        let evm_env = EvmEnv { cfg_env: cfg.clone(), ..Default::default() };

        let evm = evm_config.evm_with_env(db, evm_env);

        // Check that the EVM environment is initialized with the custom environment
        assert_eq!(evm.cfg, cfg);
    }

    #[test]
    fn test_evm_with_env_custom_block_and_tx() {
        let evm_config = EthEvmConfig::mainnet();

        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        // Create customs block and tx env
        let block = BlockEnv {
            basefee: 1000,
            gas_limit: 10_000_000,
            number: U256::from(42),
            ..Default::default()
        };

        let evm_env = EvmEnv { block_env: block, ..Default::default() };

        let evm = evm_config.evm_with_env(db, evm_env.clone());

        // Verify that the block and transaction environments are set correctly
        assert_eq!(evm.block, evm_env.block_env);

        // Default spec ID
        assert_eq!(evm.cfg.spec, SpecId::default());
    }

    #[test]
    fn test_evm_with_spec_id() {
        let evm_config = EthEvmConfig::mainnet();

        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        let evm_env = EvmEnv {
            cfg_env: CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::PETERSBURG),
            ..Default::default()
        };

        let evm = evm_config.evm_with_env(db, evm_env);

        // Check that the spec ID is setup properly
        assert_eq!(evm.cfg.spec, SpecId::PETERSBURG);
    }

    #[test]
    fn test_evm_with_env_and_default_inspector() {
        let evm_config = EthEvmConfig::mainnet();
        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        let evm_env = EvmEnv::default();

        let evm = evm_config.evm_with_env_and_inspector(db, evm_env.clone(), NoOpInspector {});

        // Check that the EVM environment is set to default values
        assert_eq!(evm.block, evm_env.block_env);
        assert_eq!(evm.cfg, evm_env.cfg_env);
    }

    #[test]
    fn test_evm_with_env_inspector_and_custom_cfg() {
        let evm_config = EthEvmConfig::mainnet();
        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        let cfg_env = CfgEnv::default().with_chain_id(111);
        let block = BlockEnv::default();
        let evm_env = EvmEnv { cfg_env: cfg_env.clone(), block_env: block };

        let evm = evm_config.evm_with_env_and_inspector(db, evm_env, NoOpInspector {});

        // Check that the EVM environment is set with custom configuration
        assert_eq!(evm.cfg, cfg_env);
        assert_eq!(evm.cfg.spec, SpecId::default());
    }

    #[test]
    fn test_evm_with_env_inspector_and_custom_block_tx() {
        let evm_config = EthEvmConfig::mainnet();
        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        // Create custom block and tx environment
        let block = BlockEnv {
            basefee: 1000,
            gas_limit: 10_000_000,
            number: U256::from(42),
            ..Default::default()
        };
        let evm_env = EvmEnv { block_env: block, ..Default::default() };

        let evm = evm_config.evm_with_env_and_inspector(db, evm_env.clone(), NoOpInspector {});

        // Verify that the block and transaction environments are set correctly
        assert_eq!(evm.block, evm_env.block_env);
        assert_eq!(evm.cfg.spec, SpecId::default());
    }

    #[test]
    fn test_evm_with_env_inspector_and_spec_id() {
        let evm_config = EthEvmConfig::mainnet();
        let db = CacheDB::<EmptyDBTyped<ProviderError>>::default();

        let evm_env = EvmEnv {
            cfg_env: CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::PETERSBURG),
            ..Default::default()
        };

        let evm = evm_config.evm_with_env_and_inspector(db, evm_env.clone(), NoOpInspector {});

        // Check that the spec ID is set properly
        assert_eq!(evm.block, evm_env.block_env);
        assert_eq!(evm.cfg, evm_env.cfg_env);
        assert_eq!(evm.tx, Default::default());
    }
}
