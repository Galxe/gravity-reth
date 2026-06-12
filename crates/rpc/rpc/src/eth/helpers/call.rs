//! Contains RPC handler implementations specific to endpoints that call/execute within evm.

use crate::EthApi;
use alloy_consensus::BlockHeader;
use alloy_primitives::{B256, U256};
use gravity_precompiles::randomness_by_height::{
    create_randomness_by_height_precompile, randomness_by_height_gas_policy_at_block,
    RandomnessByHeightGasPolicy, RandomnessByHeightLookup, RandomnessByHeightProvider,
    RANDOMNESS_BY_HEIGHT_PRECOMPILE_ADDR,
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
        block_timestamp: U256,
        current_randomness: Option<B256>,
    ) where
        EV: Evm<Precompiles = PrecompilesMap>,
    {
        let Ok(block_number) = u64::try_from(block_number) else { return };
        let Ok(block_timestamp) = u64::try_from(block_timestamp) else { return };
        let chain_spec = self.provider().chain_spec();
        if !chain_spec
            .gravity_hardforks()
            .is_fork_active_at_timestamp(GravityHardfork::Alpha, block_timestamp)
        {
            return
        }

        // For RPC calls the gas tier is anchored to the EVM block environment being simulated.
        // This keeps eth_call, estimateGas, and debug tracing aligned with the execution context.
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header;
    use alloy_primitives::{BlockHash, BlockNumber};
    use reth_primitives_traits::SealedHeader;
    use std::{collections::BTreeMap, ops::RangeBounds};

    #[derive(Clone, Debug, Default)]
    struct MockHeaderProvider {
        headers: BTreeMap<u64, Header>,
    }

    impl MockHeaderProvider {
        fn with_header(mut self, number: u64, randomness: B256) -> Self {
            self.headers
                .insert(number, Header { number, mix_hash: randomness, ..Default::default() });
            self
        }
    }

    impl HeaderProvider for MockHeaderProvider {
        type Header = Header;

        fn header(&self, _block_hash: &BlockHash) -> Result<Option<Self::Header>, ProviderError> {
            Ok(None)
        }

        fn header_by_number(&self, num: u64) -> Result<Option<Self::Header>, ProviderError> {
            Ok(self.headers.get(&num).cloned())
        }

        fn header_td(&self, _hash: &BlockHash) -> Result<Option<U256>, ProviderError> {
            Ok(None)
        }

        fn header_td_by_number(&self, _number: BlockNumber) -> Result<Option<U256>, ProviderError> {
            Ok(None)
        }

        fn headers_range(
            &self,
            range: impl RangeBounds<BlockNumber>,
        ) -> Result<Vec<Self::Header>, ProviderError> {
            Ok(self
                .headers
                .iter()
                .filter_map(|(number, header)| range.contains(number).then_some(header.clone()))
                .collect())
        }

        fn sealed_header(
            &self,
            number: BlockNumber,
        ) -> Result<Option<SealedHeader<Self::Header>>, ProviderError> {
            Ok(self.header_by_number(number)?.map(SealedHeader::seal_slow))
        }

        fn sealed_headers_while(
            &self,
            range: impl RangeBounds<BlockNumber>,
            mut predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
        ) -> Result<Vec<SealedHeader<Self::Header>>, ProviderError> {
            Ok(self
                .headers_range(range)?
                .into_iter()
                .map(SealedHeader::seal_slow)
                .take_while(|header| predicate(header))
                .collect())
        }
    }

    fn provider(
        reference_number: u64,
        current_randomness: Option<B256>,
    ) -> HeaderRandomnessProvider<MockHeaderProvider> {
        HeaderRandomnessProvider::new(
            MockHeaderProvider::default()
                .with_header(100, B256::with_last_byte(100))
                .with_header(140, B256::with_last_byte(140)),
            reference_number,
            current_randomness,
            RandomnessByHeightGasPolicy { recent_window: 10, recent_gas: 4, lookup_gas: 20 },
        )
    }

    #[test]
    fn header_randomness_provider_uses_current_randomness_from_evm_env() {
        let lookup =
            provider(150, Some(B256::with_last_byte(1))).randomness_by_height(150).unwrap();

        assert_eq!(
            lookup,
            RandomnessByHeightLookup { value: Some(B256::with_last_byte(1)), gas_used: 4 }
        );
    }

    #[test]
    fn header_randomness_provider_falls_back_to_header_for_current_height() {
        let lookup = HeaderRandomnessProvider::new(
            MockHeaderProvider::default().with_header(150, B256::with_last_byte(150)),
            150,
            None,
            RandomnessByHeightGasPolicy { recent_window: 10, recent_gas: 4, lookup_gas: 20 },
        )
        .randomness_by_height(150)
        .unwrap();

        assert_eq!(
            lookup,
            RandomnessByHeightLookup { value: Some(B256::with_last_byte(150)), gas_used: 4 }
        );
    }

    #[test]
    fn header_randomness_provider_charges_recent_tier_for_recent_headers_and_future_misses() {
        let provider = provider(150, Some(B256::with_last_byte(1)));

        assert_eq!(
            provider.randomness_by_height(140).unwrap(),
            RandomnessByHeightLookup { value: Some(B256::with_last_byte(140)), gas_used: 4 }
        );
        assert_eq!(
            provider.randomness_by_height(151).unwrap(),
            RandomnessByHeightLookup { value: None, gas_used: 4 }
        );
    }

    #[test]
    fn header_randomness_provider_charges_lookup_tier_for_older_headers_and_misses() {
        let provider = provider(150, Some(B256::with_last_byte(1)));

        assert_eq!(
            provider.randomness_by_height(100).unwrap(),
            RandomnessByHeightLookup { value: Some(B256::with_last_byte(100)), gas_used: 20 }
        );
        assert_eq!(
            provider.randomness_by_height(99).unwrap(),
            RandomnessByHeightLookup { value: None, gas_used: 20 }
        );
    }
}
