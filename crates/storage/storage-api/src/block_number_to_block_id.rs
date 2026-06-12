use alloy_primitives::{BlockNumber, B256};
use reth_storage_errors::provider::ProviderResult;

/// Maps a block number to the Aptos consensus `block_id`.
///
/// Implementations of this trait are used by the EVM `BLOCKHASH` opcode path so
/// that EVM-visible block hashes equal the consensus `block_id` rather than the
/// keccak hash of the RLP-encoded header. JSON-RPC methods like
/// `eth_getBlockByNumber` continue to return the keccak header hash; only EVM
/// execution sees `block_id`.
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait BlockNumberToBlockIdReader: Send + Sync {
    /// Returns the Aptos consensus `block_id` for the given block number, or
    /// `None` if the number has never been committed or predates the upgrade
    /// point at which Gravity began recording `block_id`.
    fn block_id_by_number(&self, number: BlockNumber) -> ProviderResult<Option<B256>>;
}

#[cfg(test)]
fn _object_safe(_: Box<dyn BlockNumberToBlockIdReader>) {}
