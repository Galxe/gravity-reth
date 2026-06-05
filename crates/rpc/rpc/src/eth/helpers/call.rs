//! Contains RPC handler implementations specific to endpoints that call/execute within evm.

use crate::EthApi;
use alloy_consensus::BlockHeader;
use alloy_primitives::U256;
use gravity_precompiles::randomness_by_height::{
    create_randomness_by_height_precompile, RandomnessByHeightLookup, RandomnessByHeightProvider,
    RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR, RANDOMNESS_BY_HEIGHT_RECENT_WINDOW,
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
}

impl<Provider> HeaderRandomnessProvider<Provider> {
    const fn new(provider: Provider, reference_number: u64) -> Self {
        Self { provider, reference_number }
    }
}

impl<Provider> RandomnessByHeightProvider for HeaderRandomnessProvider<Provider>
where
    Provider: HeaderProvider,
{
    type Error = ProviderError;

    fn randomness_by_height(&self, height: u64) -> Result<RandomnessByHeightLookup, Self::Error> {
        if height > self.reference_number {
            return Ok(RandomnessByHeightLookup::recent(None));
        }

        let is_recent = self.reference_number - height <= RANDOMNESS_BY_HEIGHT_RECENT_WINDOW;
        self.provider
            .header_by_number(height)
            .map(|header| header.and_then(|header| header.mix_hash()))
            .map(|value| {
                if is_recent {
                    RandomnessByHeightLookup::recent(value)
                } else {
                    RandomnessByHeightLookup::storage(value)
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

    fn register_custom_precompiles<EV>(&self, evm: &mut EV, block_number: U256)
    where
        EV: Evm<Precompiles = PrecompilesMap>,
    {
        let Ok(block_number) = u64::try_from(block_number) else { return };
        if !self
            .provider()
            .chain_spec()
            .gravity_hardforks()
            .is_fork_active_at_block(GravityHardfork::Alpha, block_number)
        {
            return
        }

        let precompile = create_randomness_by_height_precompile(Arc::new(
            HeaderRandomnessProvider::new(self.provider().clone(), block_number),
        ));
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
