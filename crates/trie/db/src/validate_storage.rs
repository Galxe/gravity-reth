//! Read-only validation of the current V2 trie across its two database shards.

use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_storage_errors::db::DatabaseError;
use reth_trie::{
    nested_trie::{Node, StoredNode},
    Nibbles, RlpNode, StoredNibbles, StoredNibblesSubKey, TrieAccount, EMPTY_ROOT_HASH,
};

/// Checks reachable account/storage nodes against a canonical root without modifying the trie.
///
/// An interrupted legacy merged commit can leave either trie shard ahead without recording its
/// block range in the state database. Checking only the account root misses an ahead storage
/// shard, and trusting cached parent hashes misses inconsistent children. This traversal checks
/// both, with memory bounded by the 64-nibble trie depth rather than the state size.
/// Unreferenced storage roots are rejected because a later account-creation overlay could read
/// them. Other unreachable nodes cannot affect a traversal from a checked root.
pub fn validate_storage_trie<TX: DbTx>(
    tx: &TX,
    expected_state_root: B256,
) -> Result<bool, DatabaseError> {
    let mut storage_cursor = tx.cursor_dup_read::<tables::StoragesTrieV2>()?;
    let mut entry = storage_cursor.first()?;
    let mut storage_roots = 0usize;
    while let Some((_, node)) = entry {
        storage_roots += usize::from(node.path.is_empty());
        entry = storage_cursor.next_no_dup()?;
    }

    let mut reachable_storage_roots = 0usize;
    let mut account_reader = |path: &Nibbles| {
        tx.get::<tables::AccountsTrieV2>(StoredNibbles(*path))?
            .map(|node| decode_checked(node).ok_or(DatabaseError::Decode))
            .transpose()
    };
    let mut account_leaf = |path: Nibbles, mut value: &[u8]| {
        let Ok(account) = TrieAccount::decode(&mut value) else { return Ok(false) };
        if !value.is_empty() {
            return Ok(false)
        }
        let hashed_address = B256::from_slice(&path.pack());
        let mut cursor = tx.cursor_dup_read::<tables::StoragesTrieV2>()?;
        let mut storage_reader = |path: &Nibbles| {
            let path = StoredNibblesSubKey(*path);
            let node = cursor
                .get_by_key_subkey(hashed_address, path.clone())?
                .filter(|entry| entry.path == path)
                .map(|entry| decode_checked(entry.node).ok_or(DatabaseError::Decode))
                .transpose()?;
            if path.is_empty() && node.is_some() {
                reachable_storage_roots += 1;
            }
            Ok(node)
        };
        let mut storage_leaf = |_: Nibbles, mut value: &[u8]| {
            let Ok(value_decoded) = U256::decode(&mut value) else { return Ok(false) };
            Ok(value.is_empty() && !value_decoded.is_zero())
        };
        validate_trie(&mut storage_reader, account.storage_root, &mut storage_leaf)
    };
    Ok(validate_trie(&mut account_reader, expected_state_root, &mut account_leaf)? &&
        reachable_storage_roots == storage_roots)
}

/// The execution decoder assumes valid stored bytes and panics on malformed lengths. Check its
/// slice and reference preconditions here so an auxiliary RPC scan cannot abort persistence.
fn decode_checked(node: StoredNode) -> Option<Node> {
    let bytes = node.as_ref();
    match *bytes.first()? {
        0 => {
            let mask_start = bytes.len().checked_sub(16)?;
            if mask_start < 1 {
                return None
            }
            let mut start = 1usize;
            for &length in &bytes[mask_start..] {
                if length != u8::MAX {
                    let end = start.checked_add(usize::from(length))?;
                    if end > mask_start {
                        return None
                    }
                    let reference = bytes.get(start..end)?;
                    if reference.is_empty() || RlpNode::from_raw(reference).is_none() {
                        return None
                    }
                    start = end;
                }
            }
            if start != mask_start {
                return None
            }
        }
        1 => {
            let reference_end = 2 + usize::from(*bytes.get(1)?);
            let reference = bytes.get(2..reference_end)?;
            if reference.is_empty() || RlpNode::from_raw(reference).is_none() {
                return None
            }
            let odd = *bytes.get(reference_end)?;
            let packed = bytes.get(reference_end + 1..)?;
            if odd > 1 || packed.len() > 32 || (odd == 1 && packed.is_empty()) {
                return None
            }
        }
        2 => {
            let length = *bytes.get(1)?;
            let packed_length = usize::from(length >> 1);
            if packed_length > 32 || (length & 1 == 1 && packed_length == 0) {
                return None
            }
            bytes.get(2..2 + packed_length)?;
        }
        _ => return None,
    }
    Some(Node::from(node))
}

fn validate_trie<R, F>(
    reader: &mut R,
    expected_root: B256,
    leaf: &mut F,
) -> Result<bool, DatabaseError>
where
    R: FnMut(&Nibbles) -> Result<Option<Node>, DatabaseError>,
    F: FnMut(Nibbles, &[u8]) -> Result<bool, DatabaseError>,
{
    let Some(mut root) = reader(&Nibbles::new())? else {
        return Ok(expected_root == EMPTY_ROOT_HASH)
    };
    root.build_hash(&mut Vec::new());
    if root.hash() != expected_root {
        return Ok(false)
    }
    validate_node(root, Nibbles::new(), reader, leaf)
}

fn validate_node<R, F>(
    node: Node,
    path: Nibbles,
    reader: &mut R,
    leaf: &mut F,
) -> Result<bool, DatabaseError>
where
    R: FnMut(&Nibbles) -> Result<Option<Node>, DatabaseError>,
    F: FnMut(Nibbles, &[u8]) -> Result<bool, DatabaseError>,
{
    match node {
        Node::FullNode { children, .. } => {
            if path.len() >= 64 || children[16].is_some() {
                return Ok(false)
            }
            for (nibble, child) in children.into_iter().take(16).enumerate() {
                if let Some(child) = child {
                    let Node::HashNode(reference) = *child else { return Ok(false) };
                    let mut child_path = path;
                    child_path.push_unchecked(nibble as u8);
                    if !validate_child(child_path, reference, reader, leaf)? {
                        return Ok(false)
                    }
                }
            }
            Ok(true)
        }
        Node::ShortNode { key, value, .. } => {
            if path.len() + key.len() > 64 {
                return Ok(false)
            }
            let mut child_path = path;
            child_path.extend(&key);
            match *value {
                Node::ValueNode(value) if child_path.len() == 64 => leaf(child_path, &value),
                Node::HashNode(reference) if !key.is_empty() && child_path.len() < 64 => {
                    validate_child(child_path, reference, reader, leaf)
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

fn validate_child<R, F>(
    path: Nibbles,
    expected: RlpNode,
    reader: &mut R,
    leaf: &mut F,
) -> Result<bool, DatabaseError>
where
    R: FnMut(&Nibbles) -> Result<Option<Node>, DatabaseError>,
    F: FnMut(Nibbles, &[u8]) -> Result<bool, DatabaseError>,
{
    let Some(mut node) = reader(&path)? else { return Ok(false) };
    if node.build_hash(&mut Vec::new()) != &expected {
        return Ok(false)
    }
    validate_node(node, path, reader, leaf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nested_hash::NestedStateRoot;
    use reth_primitives_traits::Account;
    use reth_provider::{
        test_utils::create_test_provider_factory, DatabaseProviderFactory, TrieWriterV2,
    };
    use reth_trie::{HashedPostState, HashedStorage};

    #[test]
    fn validates_v2_trie_and_rejects_ahead_storage_shard() {
        let factory = create_test_provider_factory();
        let address = B256::with_last_byte(10);
        let mut state = HashedPostState::default().with_accounts([
            (address, Some(Account::default())),
            (B256::with_last_byte(20), Some(Account::default())),
        ]);
        // Shared prefixes exercise extensions and embedded storage leaf references.
        state.storages.insert(
            address,
            HashedStorage::from_iter(
                false,
                [
                    (B256::with_last_byte(1), U256::from(1)),
                    (B256::with_last_byte(17), U256::from(2)),
                ],
            ),
        );
        let provider = factory.provider_rw().unwrap();
        let (root, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&state).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();
        {
            let provider = factory.database_provider_ro().unwrap();
            assert!(validate_storage_trie(provider.tx_ref(), root).unwrap());
            assert!(!validate_storage_trie(provider.tx_ref(), B256::ZERO).unwrap());
        }

        state
            .storages
            .get_mut(&address)
            .unwrap()
            .storage
            .insert(B256::with_last_byte(1), U256::from(9));
        let provider = factory.provider_rw().unwrap();
        let (_, mut updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&state).unwrap();
        updates.account_nodes.clear();
        updates.removed_nodes.clear();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        // Cached account hashes alone cannot detect an independently committed storage shard.
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            root
        );
        assert!(!validate_storage_trie(provider.tx_ref(), root).unwrap());
    }
}
