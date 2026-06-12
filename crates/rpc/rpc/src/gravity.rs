use std::sync::Arc;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use reth_rpc_api::GravityApiServer;
use reth_storage_api::{BlockIdReader, BlockNumberToBlockIdReader};

/// `gravity` namespace implementation.
///
/// Exposes the Aptos consensus `block_id` for a given block number. The
/// `BLOCKHASH` opcode and the JSON-RPC layer disagree by design: the EVM sees
/// `block_id` (via the `BlockNumberToBlockId` table queried inside
/// `EvmStateProvider::block_hash`), while `eth_getBlockByNumber(n).hash`
/// continues to return `keccak(rlp(header))`. This namespace makes the
/// consensus `block_id` directly queryable.
pub struct GravityApi<Provider> {
    inner: Arc<GravityApiInner<Provider>>,
}

impl<Provider> GravityApi<Provider> {
    /// Create a new `GravityApi` over `provider`.
    pub fn new(provider: Provider) -> Self {
        Self { inner: Arc::new(GravityApiInner { provider }) }
    }

    /// Borrow the underlying provider.
    pub fn provider(&self) -> &Provider {
        &self.inner.provider
    }
}

impl<Provider> Clone for GravityApi<Provider> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<Provider> std::fmt::Debug for GravityApi<Provider> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GravityApi").finish_non_exhaustive()
    }
}

struct GravityApiInner<Provider> {
    provider: Provider,
}

#[async_trait]
impl<Provider> GravityApiServer for GravityApi<Provider>
where
    Provider: BlockIdReader + BlockNumberToBlockIdReader + Clone + 'static,
{
    async fn block_id_by_number(&self, number: BlockNumberOrTag) -> RpcResult<Option<B256>> {
        // Resolve tags to a concrete block number. `"pending"` is intentionally
        // not supported: pending blocks have not been assigned a consensus
        // `block_id` yet, so returning `null` is the only honest answer.
        if matches!(number, BlockNumberOrTag::Pending) {
            return Ok(None);
        }

        let Some(resolved) = self
            .provider()
            .convert_block_number(number)
            .map_err(|err| reth_rpc_server_types::result::internal_rpc_err(err.to_string()))?
        else {
            return Ok(None);
        };

        self.provider()
            .block_id_by_number(resolved)
            .map_err(|err| reth_rpc_server_types::result::internal_rpc_err(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::BlockNumHash;
    use alloy_primitives::BlockNumber;
    use reth_chainspec::ChainInfo;
    use reth_errors::ProviderResult;
    use reth_storage_api::{BlockHashReader, BlockNumReader};
    use std::collections::HashMap;

    /// Minimal provider stub implementing the trait stack `GravityApi` needs.
    /// Records what was queried so the `"pending"` test can verify the impl
    /// short-circuits before reading any provider state.
    #[derive(Clone, Default)]
    struct StubProvider {
        block_ids: HashMap<BlockNumber, B256>,
        // Set by `block_id_by_number`; the "pending" test asserts this stays
        // unset proving we never reached the table lookup.
        last_queried: std::sync::Arc<std::sync::Mutex<Option<BlockNumber>>>,
    }

    impl BlockHashReader for StubProvider {
        fn block_hash(&self, _: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(None)
        }
        fn canonical_hashes_range(
            &self,
            _: BlockNumber,
            _: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            Ok(Vec::new())
        }
    }

    impl BlockNumReader for StubProvider {
        fn chain_info(&self) -> ProviderResult<ChainInfo> {
            Ok(ChainInfo::default())
        }
        fn best_block_number(&self) -> ProviderResult<BlockNumber> {
            Ok(0)
        }
        fn last_block_number(&self) -> ProviderResult<BlockNumber> {
            Ok(0)
        }
        fn block_number(&self, _: B256) -> ProviderResult<Option<BlockNumber>> {
            Ok(None)
        }
    }

    impl BlockIdReader for StubProvider {
        fn pending_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(Some(BlockNumHash { number: 99, hash: B256::repeat_byte(0xCC) }))
        }
        fn safe_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(None)
        }
        fn finalized_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(None)
        }
    }

    impl BlockNumberToBlockIdReader for StubProvider {
        fn block_id_by_number(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
            *self.last_queried.lock().unwrap() = Some(number);
            Ok(self.block_ids.get(&number).copied())
        }
    }

    #[tokio::test]
    async fn pending_tag_returns_none_without_querying_provider() {
        let provider = StubProvider::default();
        let last_queried = provider.last_queried.clone();
        let api = GravityApi::new(provider);

        let res = api.block_id_by_number(BlockNumberOrTag::Pending).await.unwrap();
        assert_eq!(res, None, "pending must always return null");
        assert!(
            last_queried.lock().unwrap().is_none(),
            "pending must short-circuit before touching the block_id table",
        );
    }

    #[tokio::test]
    async fn concrete_number_dispatches_to_reader() {
        let mut provider = StubProvider::default();
        provider.block_ids.insert(7, B256::repeat_byte(0x77));
        let api = GravityApi::new(provider);

        let hit = api.block_id_by_number(BlockNumberOrTag::Number(7)).await.unwrap();
        assert_eq!(hit, Some(B256::repeat_byte(0x77)));

        let miss = api.block_id_by_number(BlockNumberOrTag::Number(8)).await.unwrap();
        assert_eq!(miss, None);
    }
}
