//! Chain reads the Gravity execution rules need beyond the EVM database.

use alloc::vec::Vec;
use alloy_primitives::{Address, B256, U256};
use core::fmt::Debug;
use reth_ethereum_primitives::Receipt;
use reth_storage_errors::provider::ProviderResult;

/// Read access to the canonical chain for [`GravityEvmConfig`](super::GravityEvmConfig).
///
/// The EVM database only holds the state a block executes on. Replaying a committed Gravity
/// block also needs headers, receipts and the state after the block; the node implements this
/// over its provider.
pub trait GravityChainReader: Debug + Send + Sync {
    /// Returns the `mix_hash` of the canonical header at `number`.
    fn mix_hash_by_number(&self, number: u64) -> ProviderResult<Option<B256>>;

    /// Returns the hash of the canonical block at `number`.
    fn canonical_hash(&self, number: u64) -> ProviderResult<Option<B256>>;

    /// Returns the timestamp of the header with `hash`.
    fn header_timestamp(&self, hash: B256) -> ProviderResult<Option<u64>>;

    /// Returns the receipts of the block with `hash`.
    fn receipts_by_block_hash(&self, hash: B256) -> ProviderResult<Option<Vec<Receipt>>>;

    /// Returns the value of `slot` of `address` in the state after the block with `hash`.
    fn storage_after_block(
        &self,
        hash: B256,
        address: Address,
        slot: B256,
    ) -> ProviderResult<Option<U256>>;
}

/// Adapts a provider, including a captured RPC provider, to Gravity's auxiliary chain reads.
#[derive(Clone)]
pub struct ProviderChainReader<P>(pub P);

impl<P> Debug for ProviderChainReader<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ProviderChainReader").finish_non_exhaustive()
    }
}

impl<P> GravityChainReader for ProviderChainReader<P>
where
    P: reth_storage_api::HeaderProvider<Header = alloy_consensus::Header>
        + reth_storage_api::BlockHashReader
        + reth_storage_api::ReceiptProvider<Receipt = Receipt>
        + reth_storage_api::StateProviderFactory
        + Send
        + Sync,
{
    fn mix_hash_by_number(&self, number: u64) -> ProviderResult<Option<B256>> {
        Ok(self.0.header_by_number(number)?.map(|header| header.mix_hash))
    }

    fn canonical_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        self.0.block_hash(number)
    }

    fn header_timestamp(&self, hash: B256) -> ProviderResult<Option<u64>> {
        Ok(self.0.header(&hash)?.map(|header| header.timestamp))
    }

    fn receipts_by_block_hash(&self, hash: B256) -> ProviderResult<Option<Vec<Receipt>>> {
        self.0.receipts_by_block(hash.into())
    }

    fn storage_after_block(
        &self,
        hash: B256,
        address: Address,
        slot: B256,
    ) -> ProviderResult<Option<U256>> {
        self.0.state_by_block_hash(hash)?.storage(address, slot)
    }
}
