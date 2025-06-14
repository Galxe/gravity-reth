use std::sync::{Mutex, OnceLock};

use alloy_primitives::{
    map::{HashMap, HashSet},
    B256,
};
use alloy_trie::EMPTY_ROOT_HASH;
use nybbles::Nibbles;
use reth_storage_errors::db::DatabaseError;

use crate::nested_trie::node::{Node, NodeFlag};

pub trait TrieReader {
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError>;
}

#[derive(Default, Debug, Clone)]
pub struct TrieOutput {
    pub removed_nodes: HashSet<Nibbles>,
    pub update_nodes: HashMap<Nibbles, Node>,
}

#[derive(Debug)]
pub struct Trie<R>
where
    R: TrieReader,
{
    root: Option<Node>,
    reader: R,
    removed_nodes: HashSet<Nibbles>,
    update_nodes: HashMap<Nibbles, Node>,
    parallel: bool,
}

impl<R> Trie<R>
where
    R: TrieReader,
{
    pub fn new(mut reader: R, parallel: bool) -> Result<Self, DatabaseError> {
        let root = reader.read(&Nibbles::new())?;
        Ok(Self {
            root,
            reader,
            removed_nodes: Default::default(),
            update_nodes: Default::default(),
            parallel,
        })
    }

    fn new_with_root(reader: R, root: Option<Node>) -> Self {
        Self {
            root,
            reader,
            removed_nodes: Default::default(),
            update_nodes: Default::default(),
            parallel: false,
        }
    }

    pub fn take_output(mut self) -> TrieOutput {
        if let Some(root) = self.root.take() {
            self.take_output_inner(root, Nibbles::new());
        }
        TrieOutput { removed_nodes: self.removed_nodes, update_nodes: self.update_nodes }
    }

    fn take_output_inner(&mut self, node: Node, prefix: Nibbles) {
        if !node.dirty() {
            return;
        }
        // convert child to hash node
        match node {
            Node::FullNode { children, flags } => {
                let mut convert: [Option<Box<Node>>; 17] = Default::default();
                for (nibble, child) in children.into_iter().enumerate() {
                    if let Some(child) = child {
                        let rlp = child.cached_rlp().unwrap().clone();
                        let mut child_path = prefix.clone();
                        child_path.push_unchecked(nibble as u8);
                        self.take_output_inner(*child, child_path);
                        convert[nibble] = Some(Box::new(Node::HashNode(rlp)));
                    }
                }
                self.update_nodes.insert(prefix, Node::FullNode { children: convert, flags });
            }
            Node::ShortNode { key, value, flags } => {
                match *value {
                    next_node @ Node::FullNode { .. } => {
                        let convert =
                            Box::new(Node::HashNode(next_node.cached_rlp().unwrap().clone()));
                        let mut next_path = prefix.clone();
                        next_path.extend_from_slice_unchecked(&key);
                        self.take_output_inner(next_node, next_path);
                        self.update_nodes
                            .insert(prefix, Node::ShortNode { key, value: convert, flags });
                    }
                    leaf_node @ Node::ValueNode { .. } => {
                        self.update_nodes.insert(
                            prefix,
                            Node::ShortNode { key, value: Box::new(leaf_node), flags },
                        );
                    }
                    hash_node @ Node::HashNode(..) => {
                        // When two consecutive ShortNodes are merged into one ShortNode,
                        // the child node may not be visited and still is a HashNode.
                        self.update_nodes.insert(
                            prefix,
                            Node::ShortNode { key, value: Box::new(hash_node), flags },
                        );
                    }
                    // assert next_node != HashNode, because current node is dirty
                    Node::ShortNode { .. } => unreachable!("Consecutive ShortNodes"),
                }
            }
            _ => unreachable!(),
        }
    }

    fn insert_inner(
        &mut self,
        node: Option<Node>,
        prefix: Nibbles,
        key: Nibbles,
        value: Node,
    ) -> Result<(bool, Node), DatabaseError> {
        if key.is_empty() {
            return Ok((true, value));
        }

        if let Some(node) = node {
            match node {
                Node::FullNode { mut children, mut flags } => {
                    let index = key[0] as usize;
                    let child = children[index].take().map(|n| *n);
                    let mut new_prefix = prefix.clone();
                    new_prefix.push_unchecked(key[0]);
                    let (dirty, new_node) =
                        self.insert_inner(child, new_prefix, key.slice(1..), value)?;
                    children[index] = Some(Box::new(new_node));
                    if dirty {
                        flags.mark_diry();
                    }
                    Ok((dirty, Node::FullNode { children, flags }))
                }
                Node::ShortNode { key: node_key, value: node_value, mut flags } => {
                    // If the whole key matches, keep this short node as is
                    // and only update the value.
                    let matchlen = key.common_prefix_length(&node_key);
                    if matchlen == node_key.len() {
                        let next_node = Some(*node_value);
                        let mut new_prefix = prefix.clone();
                        new_prefix.extend_from_slice_unchecked(&key[..matchlen]);
                        let (dirty, new_node) =
                            self.insert_inner(next_node, new_prefix, key.slice(matchlen..), value)?;
                        if dirty {
                            flags.mark_diry();
                        }
                        return Ok((
                            dirty,
                            Node::ShortNode { key: node_key, value: Box::new(new_node), flags },
                        ));
                    }
                    // Otherwise branch out at the index where they differ.
                    let mut children: [Option<Box<Node>>; 17] = Default::default();
                    let matchlen_inc = matchlen + 1;
                    let mut new_prefix = prefix.clone();
                    new_prefix.extend_from_slice_unchecked(&node_key[..matchlen_inc]);
                    let (_, extension_node) = self.insert_inner(
                        None,
                        new_prefix,
                        node_key.slice(matchlen_inc..),
                        *node_value,
                    )?;
                    children[node_key[matchlen] as usize] = Some(Box::new(extension_node));

                    let mut new_prefix = prefix.clone();
                    // if matchlen == key.len(), value is the value of the branch node.
                    new_prefix.extend_from_slice_unchecked(&key[..matchlen_inc]);
                    let (_, new_node) =
                        self.insert_inner(None, new_prefix, key.slice(matchlen_inc..), value)?;
                    children[key[matchlen] as usize] = Some(Box::new(new_node));

                    let branch = Node::FullNode { children, flags: NodeFlag::dirty_node() };
                    if matchlen == 0 {
                        Ok((true, branch))
                    } else {
                        Ok((
                            true,
                            Node::ShortNode {
                                key: key.slice(..matchlen),
                                value: Box::new(branch),
                                flags: NodeFlag::dirty_node(),
                            },
                        ))
                    }
                }
                Node::HashNode(rlp_node) => {
                    let real_node = self.reader.read(&prefix)?.unwrap();
                    let (dirty, new_node) =
                        self.insert_inner(Some(real_node), prefix, key, value)?;
                    if dirty {
                        Ok((true, new_node))
                    } else {
                        Ok((false, Node::HashNode(rlp_node)))
                    }
                }
                _ => unreachable!(),
            }
        } else {
            Ok((
                true,
                Node::ShortNode { key, value: Box::new(value), flags: NodeFlag::dirty_node() },
            ))
        }
    }

    pub fn insert(&mut self, key: Nibbles, value: Node) -> Result<(), DatabaseError> {
        let root = self.root.take();
        let (_, new_root) = self.insert_inner(root, Nibbles::new(), key, value)?;
        self.root = Some(new_root);
        Ok(())
    }

    pub fn parallel_update<F>(
        &mut self,
        batches: [Vec<(Nibbles, Option<Node>)>; 16], // Some for insert, None for delete
        f: F,
    ) -> Result<(), DatabaseError>
    where
        F: Fn() -> Result<R, DatabaseError> + Send + Sync,
    {
        if self.parallel && self.root.is_some() {
            let root = self.root.take().unwrap();
            if let Node::FullNode { mut children, .. } = root {
                let abort: OnceLock<DatabaseError> = Default::default();
                let removed_nodes: Mutex<HashSet<Nibbles>> = Default::default();

                std::thread::scope(|scope| {
                    for (child, batch) in children.iter_mut().zip(batches.into_iter()) {
                        if batch.is_empty() {
                            continue;
                        }
                        scope.spawn(|| {
                            let prefix = batch[0].0.slice(0..1);
                            let child_root = child.take().map(|n| *n);
                            let reader = match f() {
                                Ok(reader) => reader,
                                Err(e) => {
                                    abort.get_or_init(|| e);
                                    return;
                                }
                            };
                            let mut child_trie = Trie::new_with_root(reader, child_root);
                            for (key, value) in batch {
                                let child_root = child_trie.root.take();
                                let result = if let Some(value) = value {
                                    child_trie
                                        .insert_inner(
                                            child_root,
                                            prefix.clone(),
                                            key.slice(1..),
                                            value,
                                        )
                                        .map(|(dirty, node)| (dirty, Some(node)))
                                } else {
                                    child_trie.delete_inner(
                                        child_root,
                                        prefix.clone(),
                                        key.slice(1..),
                                    )
                                };
                                match result {
                                    Ok((_, node)) => {
                                        child_trie.root = node;
                                    }
                                    Err(e) => {
                                        abort.get_or_init(|| e);
                                        return;
                                    }
                                }
                            }
                            let _ = child_trie.hash();
                            *child = child_trie.root.take().map(|n| Box::new(n));
                            if !child_trie.removed_nodes.is_empty() {
                                removed_nodes.lock().unwrap().extend(child_trie.removed_nodes);
                            }
                        });
                    }
                });
                if let Some(abort) = abort.into_inner() {
                    return Err(abort);
                }
                let mut removed_nodes = removed_nodes.into_inner().unwrap();
                // check the root node can be FullNode or other
                let mut pos = -1;
                for (i, child) in children.iter().enumerate() {
                    if child.is_some() {
                        if pos == -1 {
                            pos = i as i32;
                        } else {
                            pos = -2;
                            break;
                        }
                    }
                }
                if pos >= 0 {
                    // Fall back into a extension node
                    let nibble_path = Nibbles::from_nibbles_unchecked([pos as u8]);
                    let single_child = *children[pos as usize].take().unwrap();
                    let single_child = self.resolve(single_child, nibble_path.clone())?.unwrap();

                    let new_node = if let Node::ShortNode { key: cn_key, value: cn_value, .. } =
                        single_child
                    {
                        let mut new_key = nibble_path.clone();
                        new_key.extend_from_slice_unchecked(&cn_key);
                        removed_nodes.insert(nibble_path.clone());
                        Node::ShortNode {
                            key: new_key,
                            value: cn_value,
                            flags: NodeFlag::dirty_node(),
                        }
                    } else {
                        Node::ShortNode {
                            key: nibble_path,
                            value: Box::new(single_child),
                            flags: NodeFlag::dirty_node(),
                        }
                    };
                    self.root = Some(new_node);
                } else if pos == -2 {
                    self.root = Some(Node::FullNode { children, flags: NodeFlag::dirty_node() });
                } else if pos == -1 {
                    self.root = None;
                    removed_nodes.insert(Nibbles::new());
                }

                if self.removed_nodes.is_empty() {
                    self.removed_nodes = removed_nodes;
                } else {
                    self.removed_nodes.extend(removed_nodes);
                }
                return Ok(());
            } else {
                self.root = Some(root);
            }
        }
        for batch in batches {
            for (key, value) in batch {
                if let Some(value) = value {
                    self.insert(key, value)?;
                } else {
                    self.delete(key)?;
                }
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, key: Nibbles) -> Result<(), DatabaseError> {
        let root = self.root.take();
        let (_, new_root) = self.delete_inner(root, Nibbles::new(), key)?;
        self.root = new_root;
        Ok(())
    }

    fn delete_inner(
        &mut self,
        node: Option<Node>,
        prefix: Nibbles,
        key: Nibbles,
    ) -> Result<(bool, Option<Node>), DatabaseError> {
        if let Some(node) = node {
            match node {
                Node::FullNode { mut children, flags } => {
                    let index = key[0] as usize;
                    let child = children[index].take().map(|n| *n);
                    let mut new_prefix = prefix.clone();
                    new_prefix.push_unchecked(key[0]);
                    let (dirty, new_node) = self.delete_inner(child, new_prefix, key.slice(1..))?;
                    children[index] = new_node.map(|n| Box::new(n));
                    if !dirty {
                        return Ok((false, Some(Node::FullNode { children, flags })));
                    }

                    // Because n is a full node, it must've contained at least two children
                    // before the delete operation. If the new child value is non-nil, n still
                    // has at least two children after the deletion, and cannot be reduced to
                    // a short node.
                    if children[index].is_some() {
                        return Ok((
                            true,
                            Some(Node::FullNode { children, flags: NodeFlag::dirty_node() }),
                        ));
                    }

                    // Reduction:
                    // Check how many non-nil entries are left after deleting and
                    // reduce the full node to a short node if only one entry is
                    // left. Since n must've contained at least two children
                    // before deletion (otherwise it would not be a full node) n
                    // can never be reduced to nil.
                    //
                    // When the loop is done, pos contains the index of the single
                    // value that is left in n or -2 if n contains at least two
                    // values.
                    let mut pos = -1;
                    for (i, child) in children.iter().enumerate() {
                        if child.is_some() {
                            if pos == -1 {
                                pos = i as i32;
                            } else {
                                pos = -2;
                                break;
                            }
                        }
                    }
                    // pos can't be -2
                    if pos >= 0 {
                        let mut single_child = *children[pos as usize].take().unwrap();
                        if pos != 16 {
                            // If the remaining entry is a short node, it replaces
                            // n and its key gets the missing nibble tacked to the
                            // front. This avoids creating an invalid
                            // shortNode{..., shortNode{...}}.  Since the entry
                            // might not be loaded yet, resolve it just for this
                            // check.
                            let mut child_path = prefix.clone();
                            child_path.push_unchecked(pos as u8);
                            single_child = self.resolve(single_child, child_path)?.unwrap();
                        }
                        if let Node::ShortNode { key: cn_key, value: cn_value, .. } = single_child {
                            // Replace the entire full node with the short node.
                            // Mark the original short node as deleted since the
                            // value is embedded into the parent now.
                            let mut new_key = Nibbles::from_nibbles_unchecked([pos as u8]);
                            new_key.extend_from_slice_unchecked(&cn_key);
                            // current node is fall back into short node, and will concat the next
                            // short node, so the next short node is
                            // removed.
                            let mut delete_child_path = prefix.clone();
                            delete_child_path.push_unchecked(pos as u8);
                            self.removed_nodes.insert(delete_child_path);
                            Ok((
                                true,
                                Some(Node::ShortNode {
                                    key: new_key,
                                    value: cn_value,
                                    flags: NodeFlag::dirty_node(),
                                }),
                            ))
                        } else {
                            // Otherwise, n is replaced by a one-nibble short node
                            // containing the child.
                            let new_key = Nibbles::from_nibbles_unchecked([pos as u8]);
                            Ok((
                                true,
                                Some(Node::ShortNode {
                                    key: new_key,
                                    value: Box::new(single_child),
                                    flags: NodeFlag::dirty_node(),
                                }),
                            ))
                        }
                    } else {
                        Ok((true, Some(Node::FullNode { children, flags: NodeFlag::dirty_node() })))
                    }
                }
                Node::ShortNode { key: node_key, value: node_value, flags } => {
                    let matchlen = key.common_prefix_length(&node_key);
                    if matchlen < node_key.len() {
                        // don't replace n on mismatch
                        return Ok((
                            false,
                            Some(Node::ShortNode { key: node_key, value: node_value, flags }),
                        ));
                    }
                    if matchlen == key.len() {
                        // The matched short node is deleted entirely and track
                        // it in the deletion set. The same the valueNode doesn't
                        // need to be tracked at all since it's always embedded.
                        self.removed_nodes.insert(prefix);
                        return Ok((true, None)); // remove n entirely for whole matches
                    }
                    let next_node = Some(*node_value);
                    let mut new_prefix = prefix.clone();
                    new_prefix.extend_from_slice_unchecked(&key[..matchlen]);
                    let (dirty, new_node) =
                        self.delete_inner(next_node, new_prefix, key.slice(matchlen..))?;
                    let new_node = new_node.unwrap();
                    if !dirty {
                        return Ok((
                            false,
                            Some(Node::ShortNode {
                                key: node_key,
                                value: Box::new(new_node),
                                flags,
                            }),
                        ));
                    }
                    match new_node {
                        Node::ShortNode { key: child_key, value: child_value, .. } => {
                            let mut extend_key = node_key.clone();
                            extend_key.extend_from_slice_unchecked(&child_key);
                            // the next short node is concat by current short node,
                            // so the next short node is removed.
                            let mut delete_next_path = prefix.clone();
                            delete_next_path.extend_from_slice_unchecked(&node_key);
                            self.removed_nodes.insert(delete_next_path);
                            Ok((
                                true,
                                Some(Node::ShortNode {
                                    key: extend_key,
                                    value: child_value,
                                    flags: NodeFlag::dirty_node(),
                                }),
                            ))
                        }
                        _ => Ok((
                            true,
                            Some(Node::ShortNode {
                                key: node_key,
                                value: Box::new(new_node),
                                flags: NodeFlag::dirty_node(),
                            }),
                        )),
                    }
                }
                Node::HashNode(rlp_node) => {
                    let real_node = self.reader.read(&prefix)?;
                    let (dirty, new_node) = self.delete_inner(real_node, prefix, key)?;
                    if dirty {
                        Ok((true, new_node))
                    } else {
                        Ok((false, Some(Node::HashNode(rlp_node))))
                    }
                }
                _ => unreachable!(),
            }
        } else {
            Ok((false, None))
        }
    }

    fn resolve(&mut self, node: Node, path: Nibbles) -> Result<Option<Node>, DatabaseError> {
        if let Node::HashNode(..) = &node {
            self.reader.read(&path)
        } else {
            Ok(Some(node))
        }
    }

    pub fn hash(&mut self) -> B256 {
        if let Some(root) = &mut self.root {
            if let Node::FullNode { children, flags } = root {
                if self.parallel && flags.rlp.is_none() {
                    std::thread::scope(|scope| {
                        for child in children {
                            if let Some(node) = child {
                                scope.spawn(|| {
                                    let mut buf = Vec::new();
                                    node.build_hash(&mut buf);
                                });
                            }
                        }
                    });
                }
            }
            let mut buf = Vec::new();
            root.build_hash(&mut buf);
            root.hash()
        } else {
            EMPTY_ROOT_HASH
        }
    }
}
