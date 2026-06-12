use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};

/// Gravity-specific RPC namespace.
///
/// Exposes the Aptos consensus `block_id` for a given block number. Use this
/// when you need the consensus-layer block identifier as seen by the EVM
/// `BLOCKHASH` opcode — `eth_getBlockByNumber(n).hash` continues to return the
/// keccak header hash.
#[cfg_attr(not(feature = "client"), rpc(server, namespace = "gravity"))]
#[cfg_attr(feature = "client", rpc(server, client, namespace = "gravity"))]
pub trait GravityApi {
    /// Returns the Aptos consensus `block_id` for the given block number, or
    /// `null` if the number has never been committed (or predates the upgrade
    /// point at which Gravity began recording `block_id`).
    ///
    /// Supports the same tags as `eth_blockNumber`:
    /// `"latest" | "finalized" | "safe" | "earliest"`. `"pending"` always
    /// returns `null` because pending blocks have not yet been assigned a
    /// consensus `block_id`.
    #[method(name = "blockIdByNumber")]
    async fn block_id_by_number(&self, number: BlockNumberOrTag) -> RpcResult<Option<B256>>;
}
