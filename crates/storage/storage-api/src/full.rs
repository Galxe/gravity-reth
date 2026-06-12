//! Helper trait for full rpc provider

use reth_chainspec::{ChainSpecProvider, EthereumHardforks};

use crate::{
    BlockNumberToBlockIdReader, BlockReaderIdExt, HeaderProvider, StageCheckpointReader,
    StateProviderFactory, TransactionsProvider,
};

/// Helper trait to unify all provider traits required to support `eth` RPC server behaviour, for
/// simplicity.
///
/// `BlockNumberToBlockIdReader` is included so the Gravity `gravity_blockIdByNumber` RPC and any
/// other consumer can resolve a block number to the Aptos consensus `block_id` from a provider
/// that meets the standard RPC contract.
pub trait FullRpcProvider:
    StateProviderFactory
    + ChainSpecProvider<ChainSpec: EthereumHardforks>
    + BlockReaderIdExt
    + BlockNumberToBlockIdReader
    + HeaderProvider
    + TransactionsProvider
    + StageCheckpointReader
    + Clone
    + Unpin
    + 'static
{
}

impl<T> FullRpcProvider for T where
    T: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthereumHardforks>
        + BlockReaderIdExt
        + BlockNumberToBlockIdReader
        + HeaderProvider
        + TransactionsProvider
        + StageCheckpointReader
        + Clone
        + Unpin
        + 'static
{
}
