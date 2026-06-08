//! Contains RPC handler implementations specific to endpoints that call/execute within evm.

use crate::EthApi;
use alloy_consensus::BlockHeader;
use alloy_primitives::{B256, U256};
use gravity_precompiles::randomness_by_height::{
    create_randomness_by_height_precompile, RandomnessByHeightGasPolicy, RandomnessByHeightLookup,
    RandomnessByHeightProvider, RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR,
};
use reth_chainspec::{ChainSpecProvider, EthChainSpec, GravityHardfork};
use reth_errors::ProviderError;
use reth_evm::{precompiles::PrecompilesMap, Evm, SpecFor, TxEnvFor};
use reth_rpc_convert::RpcConvert;
use reth_rpc_eth_api::{
    helpers::{estimate::EstimateCall, Call, EthCall},
    FromEvmError, RpcNodeCore,
};
use reth_rpc_eth_types::EthApiError;
use reth_storage_api::HeaderProvider;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct HeaderRandomnessProvider<Provider> {
    provider: Provider,
    reference_number: u64,
    current_randomness: Option<B256>,
    gas_policy: RandomnessByHeightGasPolicy,
}

impl<Provider> HeaderRandomnessProvider<Provider> {
    const fn new(
        provider: Provider,
        reference_number: u64,
        current_randomness: Option<B256>,
        gas_policy: RandomnessByHeightGasPolicy,
    ) -> Self {
        Self { provider, reference_number, current_randomness, gas_policy }
    }
}

fn randomness_by_height_gas_policy_at_block<C: EthChainSpec + ?Sized>(
    _chain_spec: &C,
    _block_number: u64,
) -> RandomnessByHeightGasPolicy {
    RandomnessByHeightGasPolicy::DEFAULT
}

impl<Provider> RandomnessByHeightProvider for HeaderRandomnessProvider<Provider>
where
    Provider: HeaderProvider,
{
    type Error = ProviderError;

    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error> {
        if height == self.reference_number && self.current_randomness.is_some() {
            return Ok(self.gas_policy.recent(self.current_randomness));
        }

        if height > self.reference_number {
            return Ok(self.gas_policy.recent(None));
        }

        let is_recent = self.reference_number - height <= self.gas_policy.recent_window;
        self.provider
            .header_by_number(height)
            .map(|header| header.and_then(|header| header.mix_hash()))
            .map(|value| {
                if is_recent {
                    self.gas_policy.recent(value)
                } else {
                    self.gas_policy.storage(value)
                }
            })
    }
}

impl<N, Rpc> EthCall for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<
        Primitives = N::Primitives,
        Error = EthApiError,
        TxEnv = TxEnvFor<N::Evm>,
        Spec = SpecFor<N::Evm>,
    >,
{
}

impl<N, Rpc> Call for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<
        Primitives = N::Primitives,
        Error = EthApiError,
        TxEnv = TxEnvFor<N::Evm>,
        Spec = SpecFor<N::Evm>,
    >,
{
    #[inline]
    fn call_gas_limit(&self) -> u64 {
        self.inner.gas_cap()
    }

    #[inline]
    fn max_simulate_blocks(&self) -> u64 {
        self.inner.max_simulate_blocks()
    }

    fn register_custom_precompiles<EV>(
        &self,
        evm: &mut EV,
        block_number: U256,
        current_randomness: Option<B256>,
    ) where
        EV: Evm<Precompiles = PrecompilesMap>,
    {
        let Ok(block_number) = u64::try_from(block_number) else { return };
        let chain_spec = self.provider().chain_spec();
        if !chain_spec
            .gravity_hardforks()
            .is_fork_active_at_block(GravityHardfork::Alpha, block_number)
        {
            return
        }

        let precompile =
            create_randomness_by_height_precompile(Arc::new(HeaderRandomnessProvider::new(
                self.provider().clone(),
                block_number,
                current_randomness,
                randomness_by_height_gas_policy_at_block(chain_spec.as_ref(), block_number),
            )));
        evm.precompiles_mut()
            .apply_precompile(&RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR, move |_| Some(precompile));
    }
}

impl<N, Rpc> EstimateCall for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<
        Primitives = N::Primitives,
        Error = EthApiError,
        TxEnv = TxEnvFor<N::Evm>,
        Spec = SpecFor<N::Evm>,
    >,
{
}
