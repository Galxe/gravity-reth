use alloy_primitives::{B256, U256};
use reth_primitives_traits::Account;
use reth_trie::{BranchNodeCompact, Nibbles};

pub trait TrieCacheReader: Clone + Send + Sync {
    fn hashed_account(&self, hash_address: B256) -> Option<Account>;

    fn hashed_storage(&self, hash_address: B256, hash_slot: B256) -> Option<U256>;

    fn trie_account(&self, nibbles: Nibbles) -> Option<BranchNodeCompact>;

    fn trie_storage(&self, hash_address: B256, nibbles: Nibbles) -> Option<BranchNodeCompact>;
}

#[derive(Clone)]
pub struct NoopTrieCacheReader {}
impl TrieCacheReader for NoopTrieCacheReader {
    fn hashed_account(&self, _: B256) -> Option<Account> {
        None
    }

    fn hashed_storage(&self, _: B256, _: B256) -> Option<U256> {
        None
    }

    fn trie_account(&self, _: Nibbles) -> Option<BranchNodeCompact> {
        None
    }

    fn trie_storage(&self, _: B256, _: Nibbles) -> Option<BranchNodeCompact> {
        None
    }
}
