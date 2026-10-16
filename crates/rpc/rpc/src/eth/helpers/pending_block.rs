//! Support for building a pending block with transactions from local view of mempool.

use crate::{eth::core::EthApiReadContext, EthApi};
use reth_evm::ConfigureEvmForRead;
use reth_rpc_convert::RpcConvert;
use reth_rpc_eth_api::{
    helpers::{pending_block::PendingEnvBuilder, LoadPendingBlock},
    FromEvmError, RpcNodeCore,
};
use reth_rpc_eth_types::{builder::config::PendingBlockKind, EthApiError, PendingBlock};
use reth_storage_api::ProviderReadView;

impl<N, Rpc> LoadPendingBlock for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    EthApiError: FromEvmError<N::Evm>,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = EthApiError>,
{
    fn with_view(&self) -> Result<Self, Self::Error> {
        if !matches!(&self.read_context, EthApiReadContext::LiveRpc) {
            return Ok(self.clone());
        }
        let provider = self
            .provider()
            .view()
            .map_err(<EthApiError as From<reth_errors::ProviderError>>::from)?;
        let evm = self.evm_config().with_read_provider(provider.clone());
        let mut scoped = self.clone();
        scoped.read_context = EthApiReadContext::View { provider, evm };
        Ok(scoped)
    }

    fn with_consistent_view(&self) -> Result<Self, Self::Error> {
        let provider = self
            .provider()
            .consistent_view()
            .map_err(<EthApiError as From<reth_errors::ProviderError>>::from)?;
        let evm = self.evm_config().with_read_provider(provider.clone());
        let mut scoped = self.clone();
        scoped.read_context = EthApiReadContext::View { provider, evm };
        Ok(scoped)
    }

    #[inline]
    fn pending_block(&self) -> &tokio::sync::Mutex<Option<PendingBlock<Self::Primitives>>> {
        self.inner.pending_block()
    }

    #[inline]
    fn pending_env_builder(&self) -> &dyn PendingEnvBuilder<Self::Evm> {
        self.inner.pending_env_builder()
    }

    #[inline]
    fn pending_block_kind(&self) -> PendingBlockKind {
        self.inner.pending_block_kind()
    }
}
