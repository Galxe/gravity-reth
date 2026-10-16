//! [`StateProvider`](crate::StateProvider) implementations
pub(crate) mod historical;
pub(crate) mod latest;
pub(crate) mod macros;

use alloy_primitives::map::hash_map::Entry;
use reth_db_api::{tables, transaction::DbTx};
use reth_storage_errors::provider::ProviderResult;
use reth_trie::HashedPostState;

/// Storage changes also rewrite account leaves. Explicit account entries, including deletions,
/// take precedence over persisted accounts.
fn complete_storage_accounts<TX: DbTx>(tx: &TX, state: &mut HashedPostState) -> ProviderResult<()> {
    for address in state.storages.keys() {
        if let Entry::Vacant(entry) = state.accounts.entry(*address) {
            entry.insert(tx.get::<tables::HashedAccounts>(*address)?);
        }
    }
    Ok(())
}
