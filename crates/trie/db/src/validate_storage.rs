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
    use reth_db_api::transaction::DbTxMut;
    use reth_primitives_traits::Account;
    use reth_provider::{
        test_utils::{create_test_provider_factory, MockNodeTypesWithDB},
        DatabaseProviderFactory, ProviderFactory, TrieWriterV2,
    };
    use reth_trie::{
        nested_trie::StorageNodeEntry, updates::TrieUpdatesV2, HashedPostState, HashedStorage,
    };

    fn initial_state() -> HashedPostState {
        let mut state = HashedPostState::default();
        for address in [B256::with_last_byte(10), B256::with_last_byte(20)] {
            state.accounts.insert(address, Some(Account::default()));
        }
        let mut storage = HashedStorage::default();
        for (slot, value) in [(1, 1), (17, 2)] {
            storage.storage.insert(B256::with_last_byte(slot), U256::from(value));
        }
        state.storages.insert(B256::with_last_byte(10), storage);
        state
    }

    fn initial_trie() -> (ProviderFactory<MockNodeTypesWithDB>, B256) {
        let factory = create_test_provider_factory();
        let provider = factory.provider_rw().unwrap();
        let (root, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial_state()).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();
        (factory, root)
    }

    fn malformed_nodes() -> Vec<StoredNode> {
        let mut nodes = vec![vec![], vec![3], vec![0], vec![1, 33, 0], vec![2, 64, 0]];
        let mut mask = [u8::MAX; 16];
        mask[0] = 34;
        let mut oversized_branch = vec![0];
        oversized_branch.extend([0; 34]);
        oversized_branch.extend(mask);
        nodes.push(oversized_branch);

        mask[0] = 1;
        let mut truncated_branch = vec![0];
        truncated_branch.extend(mask);
        nodes.push(truncated_branch);

        let mut oversized_extension = vec![1, 1, 0xc0, 0];
        oversized_extension.extend([0; 33]);
        nodes.push(oversized_extension);

        let mut oversized_leaf = vec![2, 66];
        oversized_leaf.extend([0; 33]);
        nodes.push(oversized_leaf);
        nodes.into_iter().map(StoredNode::from).collect()
    }

    #[test]
    fn rejects_malformed_nodes_in_both_shards_without_panicking() {
        for storage_shard in [false, true] {
            let (factory, root) = initial_trie();
            for (case, node) in malformed_nodes().into_iter().enumerate() {
                let provider = factory.provider_rw().unwrap();
                if storage_shard {
                    provider
                        .tx_ref()
                        .put::<tables::StoragesTrieV2>(
                            B256::with_last_byte(10),
                            StorageNodeEntry {
                                path: StoredNibblesSubKey::from(Vec::<u8>::new()),
                                node,
                            },
                        )
                        .unwrap();
                } else {
                    provider
                        .tx_ref()
                        .put::<tables::AccountsTrieV2>(StoredNibbles::from(Vec::<u8>::new()), node)
                        .unwrap();
                }
                provider.commit().unwrap();

                let provider = factory.database_provider_ro().unwrap();
                assert!(
                    matches!(
                        validate_storage_trie(provider.tx_ref(), root),
                        Err(DatabaseError::Decode)
                    ),
                    "storage_shard={storage_shard}, malformed case={case}"
                );
            }
        }
    }

    #[test]
    fn validates_empty_trie_and_rejects_wrong_root() {
        let factory = create_test_provider_factory();
        let provider = factory.database_provider_ro().unwrap();
        assert!(validate_storage_trie(provider.tx_ref(), EMPTY_ROOT_HASH).unwrap());
        assert!(!validate_storage_trie(provider.tx_ref(), B256::ZERO).unwrap());
    }

    #[test]
    fn validates_extensions_empty_storage_and_embedded_children() {
        let factory = create_test_provider_factory();
        let provider = factory.provider_rw().unwrap();
        let (root, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial_state()).unwrap();
        // Slots sharing 62 nibbles produce extensions and short embedded leaf references.
        assert!(updates.storage_tries.values().any(|updates| {
            updates.storage_nodes.values().any(|node| {
                let Node::FullNode { children, .. } = node else { return false };
                children.iter().flatten().any(|child| {
                    matches!(child.as_ref(), Node::HashNode(reference) if reference.as_slice().len() < 32)
                })
            })
        }));
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        assert!(validate_storage_trie(provider.tx_ref(), root).unwrap());
        assert!(!validate_storage_trie(provider.tx_ref(), B256::ZERO).unwrap());
    }

    #[test]
    fn rejects_storage_shard_ahead_of_account_root() {
        let (factory, root) = initial_trie();

        let provider = factory.provider_rw().unwrap();
        let mut ahead = initial_state();
        ahead
            .storages
            .get_mut(&B256::with_last_byte(10))
            .unwrap()
            .storage
            .insert(B256::with_last_byte(1), U256::from(9));
        let (_, mut updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&ahead).unwrap();
        updates.account_nodes.clear();
        updates.removed_nodes.clear();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        // Looking only at the account root cannot detect the independently committed storage.
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            root
        );
        assert!(!validate_storage_trie(provider.tx_ref(), root).unwrap());
    }

    #[test]
    fn rejects_changed_embedded_child_with_unchanged_parent_hashes() {
        let (factory, root) = initial_trie();

        let provider = factory.provider_rw().unwrap();
        let mut ahead = initial_state();
        ahead
            .storages
            .get_mut(&B256::with_last_byte(10))
            .unwrap()
            .storage
            .insert(B256::with_last_byte(1), U256::from(9));
        let (_, mut updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&ahead).unwrap();
        updates.account_nodes.clear();
        updates.removed_nodes.clear();
        for storage in updates.storage_tries.values_mut() {
            storage.removed_nodes.clear();
            storage.storage_nodes.retain(|path, node| {
                !path.is_empty() &&
                    matches!(node, Node::ShortNode { value, .. } if matches!(value.as_ref(), Node::ValueNode(_)))
            });
        }
        assert!(updates.storage_tries.values().any(|storage| !storage.storage_nodes.is_empty()));
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            root
        );
        assert!(!validate_storage_trie(provider.tx_ref(), root).unwrap());
    }

    #[test]
    fn rejects_orphan_storage_root_when_account_shard_is_unchanged() {
        let (factory, root) = initial_trie();

        let provider = factory.provider_rw().unwrap();
        let address = B256::with_last_byte(40);
        let mut ahead = HashedPostState::default();
        ahead.accounts.insert(address, Some(Account::default()));
        let mut storage = HashedStorage::default();
        storage.storage.insert(B256::with_last_byte(1), U256::from(1));
        ahead.storages.insert(address, storage);
        let (_, updates) = NestedStateRoot::new(provider.tx_ref(), None).calculate(&ahead).unwrap();
        provider
            .write_trie_updatesv2(&TrieUpdatesV2 {
                storage_tries: updates.storage_tries,
                ..Default::default()
            })
            .unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        assert_eq!(
            NestedStateRoot::new(provider.tx_ref(), None)
                .root(&HashedPostState::default())
                .unwrap(),
            root
        );
        assert!(!validate_storage_trie(provider.tx_ref(), root).unwrap());
    }

    #[test]
    fn rejects_empty_account_trie_with_orphan_storage() {
        let factory = create_test_provider_factory();
        let provider = factory.provider_rw().unwrap();
        let (_, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&initial_state()).unwrap();
        provider
            .write_trie_updatesv2(&TrieUpdatesV2 {
                storage_tries: updates.storage_tries,
                ..Default::default()
            })
            .unwrap();
        provider.commit().unwrap();

        let provider = factory.database_provider_ro().unwrap();
        assert!(!validate_storage_trie(provider.tx_ref(), EMPTY_ROOT_HASH).unwrap());
    }
}
