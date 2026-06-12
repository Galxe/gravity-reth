//! Override wrapper that makes the EVM `BLOCKHASH` opcode return Gravity's
//! Aptos consensus `block_id` instead of the canonical keccak header hash.
//!
//! ## Not on the live call path
//!
//! Gravity-reth currently achieves the BLOCKHASH alignment via the supertrait
//! route: `BlockNumberToBlockIdReader` is a supertrait of `StateProvider`, and
//! the blanket `impl<T: StateProvider> EvmStateProvider for T` in
//! [`crate::database`] dispatches `block_hash` straight to `block_id_by_number`.
//! Every EVM execution path that goes through [`crate::database::StateProviderDatabase`]
//! is already covered, so this wrapper is intentionally **unused** in
//! production code.
//!
//! It is **retained on purpose** as:
//! 1. An escape hatch for any future EVM call site that wraps a non- `StateProvider` DB and still
//!    needs the BLOCKHASH override.
//! 2. An invariant-pinning fixture — the unit tests below assert the `{block_id, 0}`-never-keccak
//!    rule directly on the wrapper, so any regression that re-introduces a keccak fallback fails
//!    them.
//!
//! Don't delete this module unless you're explicitly removing the escape
//! hatch.
//!
//! Wrap any [`Database`] / [`DatabaseRef`] used for EVM execution
//! (eth_call / debug_trace / trace / bundle / pending_block …) in
//! [`BlockIdOverrideDb`]. All non-`block_hash` methods delegate to the inner
//! DB unchanged.
//!
//! ## Miss semantics — **never** falls back to keccak header hash
//!
//! When the reader has no `block_id` for the requested number (block predates
//! the upgrade point at which Gravity began recording it, or hasn't been
//! committed yet), the wrapper returns `B256::ZERO`. This mirrors the existing
//! [`StateProviderDatabase::block_hash_ref`](crate::database::StateProviderDatabase)
//! "miss → `unwrap_or_default`" semantics so the EVM sees a binary
//! `{block_id, 0x0}` rather than a three-valued
//! `{block_id, keccak header hash, 0x0}` distribution. Cross-upgrade contracts
//! only need a single "is zero?" branch — the same shape as the existing
//! "current_block - n > 256 → 0" rule baked into the opcode itself.

use crate::primitives::alloy_primitives::BlockNumber;
use alloy_primitives::{Address, B256, U256};
use reth_storage_api::BlockNumberToBlockIdReader;
use reth_storage_errors::provider::ProviderError;
use revm::{bytecode::Bytecode, state::AccountInfo, Database, DatabaseRef};

/// Wraps an EVM [`Database`] so that `block_hash(n)` returns Gravity's Aptos
/// consensus `block_id` instead of the keccak header hash.
#[derive(Clone, Debug)]
pub struct BlockIdOverrideDb<DB, R> {
    inner: DB,
    block_id_reader: R,
}

impl<DB, R> BlockIdOverrideDb<DB, R> {
    /// Wrap `inner` with the given [`BlockNumberToBlockIdReader`].
    pub const fn new(inner: DB, block_id_reader: R) -> Self {
        Self { inner, block_id_reader }
    }

    /// Consume the wrapper and return the inner DB.
    pub fn into_inner(self) -> DB {
        self.inner
    }

    /// Borrow the inner DB.
    pub const fn inner(&self) -> &DB {
        &self.inner
    }

    /// Mutably borrow the inner DB.
    pub const fn inner_mut(&mut self) -> &mut DB {
        &mut self.inner
    }

    /// Borrow the underlying reader.
    pub const fn reader(&self) -> &R {
        &self.block_id_reader
    }
}

impl<DB, R> Database for BlockIdOverrideDb<DB, R>
where
    DB: Database<Error = ProviderError>,
    R: BlockNumberToBlockIdReader,
{
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner.basic(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: BlockNumber) -> Result<B256, Self::Error> {
        Ok(self.block_id_reader.block_id_by_number(number)?.unwrap_or_default())
    }
}

impl<DB, R> DatabaseRef for BlockIdOverrideDb<DB, R>
where
    DB: DatabaseRef<Error = ProviderError>,
    R: BlockNumberToBlockIdReader,
{
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: BlockNumber) -> Result<B256, Self::Error> {
        Ok(self.block_id_reader.block_id_by_number(number)?.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_storage_errors::provider::ProviderResult;

    /// Tiny stand-in inner DB whose `Error` is `ProviderError` so the wrapper's
    /// trait bounds line up. We assert the wrapper **never** calls into inner's
    /// `block_hash`, so all the stub methods just panic when touched —
    /// the `block_hash_returns_zero_on_miss_never_keccak` test relies on this:
    /// if the wrapper ever falls back, the test panics rather than silently
    /// returning a keccak-shaped hash.
    #[derive(Default)]
    struct PanickingInnerDb;

    impl Database for PanickingInnerDb {
        type Error = ProviderError;
        fn basic(&mut self, _: Address) -> Result<Option<AccountInfo>, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn code_by_hash(&mut self, _: B256) -> Result<Bytecode, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn storage(&mut self, _: Address, _: U256) -> Result<U256, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn block_hash(&mut self, _: BlockNumber) -> Result<B256, Self::Error> {
            unreachable!(
                "BlockIdOverrideDb::block_hash must never delegate to inner — \
                 doing so would re-introduce keccak header hash into EVM-visible state"
            )
        }
    }

    impl DatabaseRef for PanickingInnerDb {
        type Error = ProviderError;
        fn basic_ref(&self, _: Address) -> Result<Option<AccountInfo>, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn code_by_hash_ref(&self, _: B256) -> Result<Bytecode, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn storage_ref(&self, _: Address, _: U256) -> Result<U256, Self::Error> {
            unreachable!("inner DB must not be consulted")
        }
        fn block_hash_ref(&self, _: BlockNumber) -> Result<B256, Self::Error> {
            unreachable!(
                "BlockIdOverrideDb::block_hash_ref must never delegate to inner — \
                 doing so would re-introduce keccak header hash into EVM-visible state"
            )
        }
    }

    #[derive(Default)]
    struct StubReader(std::collections::HashMap<BlockNumber, B256>);

    impl BlockNumberToBlockIdReader for StubReader {
        fn block_id_by_number(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(self.0.get(&number).copied())
        }
    }

    fn h(b: u8) -> B256 {
        let mut x = [0u8; 32];
        x[31] = b;
        B256::from(x)
    }

    #[test]
    fn block_hash_returns_block_id_on_hit() {
        let reader = StubReader(std::collections::HashMap::from([(7, h(1))]));
        let mut db = BlockIdOverrideDb::new(PanickingInnerDb, reader);
        assert_eq!(<BlockIdOverrideDb<_, _> as Database>::block_hash(&mut db, 7).unwrap(), h(1));
    }

    #[test]
    fn block_hash_returns_zero_on_miss_never_keccak() {
        // Critical invariant: on miss the wrapper must return B256::ZERO,
        // NOT fall back to inner.block_hash() (which for some DBs would yield
        // the keccak header hash). The inner DB above panics on any access,
        // so this test would crash rather than pass if the fallback were ever
        // restored.
        let reader = StubReader::default();
        let mut db = BlockIdOverrideDb::new(PanickingInnerDb, reader);
        assert_eq!(
            <BlockIdOverrideDb<_, _> as Database>::block_hash(&mut db, 999).unwrap(),
            B256::ZERO
        );
    }

    #[test]
    fn database_ref_mirrors_database() {
        let reader = StubReader(std::collections::HashMap::from([(42, h(2))]));
        let db = BlockIdOverrideDb::new(PanickingInnerDb, reader);
        assert_eq!(db.block_hash_ref(42).unwrap(), h(2));
        assert_eq!(db.block_hash_ref(43).unwrap(), B256::ZERO);
    }
}
