//! Helper trait for full rpc provider

use reth_chainspec::{ChainSpecProvider, EthereumHardforks};

use crate::{BlockReaderIdExt, ProviderReadView, StageCheckpointReader, StateProviderFactory};

/// Helper trait to unify all provider traits required to support `eth` RPC server behaviour, for
/// simplicity.
pub trait FullRpcProvider:
    StateProviderFactory
    + ProviderReadView
    + ChainSpecProvider<ChainSpec: EthereumHardforks>
    + BlockReaderIdExt
    + StageCheckpointReader
    + Clone
    + Unpin
    + 'static
{
}

impl<T> FullRpcProvider for T where
    T: StateProviderFactory
        + ProviderReadView
        + ChainSpecProvider<ChainSpec: EthereumHardforks>
        + BlockReaderIdExt
        + StageCheckpointReader
        + Clone
        + Unpin
        + 'static
{
}
