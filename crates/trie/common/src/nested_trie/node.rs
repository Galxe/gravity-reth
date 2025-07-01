use std::sync::Arc;

use alloy_primitives::{keccak256, B256};
use alloy_rlp::{length_of_length, BufMut, Encodable, Header, EMPTY_STRING_CODE};
use alloy_trie::{
    nodes::{encode_path_leaf, RlpNode},
    BranchNodeCompact, TrieMask,
};
use nybbles::Nibbles;

use crate::StoredNibblesSubKey;

/// Cache hash value(RlpNode) of current Node to prevent duplicate caculations,
/// and the `dirty` indicates whether current Node has been updated.
/// `NodeFlag` is not stored in database, and read as default when a Node is loaded
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct NodeFlag {
    /// Cached hash for current node
    pub rlp: Option<RlpNode>,
    /// Whether current node is changed
    pub dirty: bool,
}

impl NodeFlag {
    /// Create a new node with hash
    pub const fn new(rlp: Option<RlpNode>) -> Self {
        Self { rlp, dirty: false }
    }

    /// Create a dirty node
    pub const fn dirty_node() -> Self {
        Self { rlp: None, dirty: true }
    }

    /// Mark current node as dirty, and wipe the cached hash
    pub fn mark_dirty(&mut self) {
        self.rlp.take();
        self.dirty = true;
    }

    /// Reset current node
    pub fn reset(&mut self) {
        self.rlp.take();
        self.dirty = false;
    }
}

/// Nested node type for MPT node:
///
/// `FullNode` for branch node
///
/// `ShortNode` for extension node(when value is `HashNode`), or leaf node(when value is
/// `ValueNode`)
///
/// `ValueNode` to store the slot value or trie account
///
/// `HashNode` is used as the index of nested `Node` for extension/branch node
///
/// The nested `Node` of extension/branch node is read as `HashNode` when loaded from database,
/// and replaced as the actual `Node` after updated. As the same way, when storing extension/branch
/// node, nested node need to be replaced with `HashNode` which do not have a nested structure.
#[derive(Debug, PartialEq, Eq)]
pub enum Node {
    /// Branch Node
    FullNode {
        /// Children of each branch + Current node value(always None for ethereum)
        children: [Option<Box<Node>>; 17],
        /// Node flag to mark dirty and cache node hash
        flags: NodeFlag,
    },
    /// extension node(when value is `HashNode`), or leaf node(when value is `ValueNode`)
    ShortNode {
        /// shared prefix(Extension Node), or key end(Leaf Node)
        key: Nibbles,
        /// next node(Extension Node), or leaf value(Leaf Node)
        value: Box<Node>,
        /// node flag
        flags: NodeFlag,
    },
    /// value node for leaf node
    ValueNode(Vec<u8>),
    /// hash node to retrieve the real nested node
    HashNode(RlpNode),
}

/// Deep copying nested `Node` is a very costly operation, and to void accidental copy, only the
/// copy of non-nested `Node` is allowed.
impl Clone for Node {
    fn clone(&self) -> Self {
        match self {
            Self::FullNode { children, flags } => {
                // only nested hash node can be cloned
                for child in children.iter().flatten() {
                    match child.as_ref() {
                        Self::HashNode(_) => {}
                        _ => {
                            panic!("Only non-nested node can be cloned!");
                        }
                    }
                }
                Self::FullNode { children: children.clone(), flags: flags.clone() }
            }
            Self::ShortNode { key, value, flags } => {
                match value.as_ref() {
                    Self::FullNode { .. } | Self::ShortNode { .. } => {
                        panic!("Only non-nested node can be cloned!");
                    }
                    _ => {}
                }
                Self::ShortNode { key: key.clone(), value: value.clone(), flags: flags.clone() }
            }
            Self::ValueNode(value) => Self::ValueNode(value.clone()),
            Self::HashNode(rlp) => Self::HashNode(rlp.clone()),
        }
    }
}

/// Used for custom serialization operations
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
enum NodeType {
    FullNode = 0,
    ShortNode = 1,
    ValueNode = 2,
    HashNode = 3,
}

impl NodeType {
    const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::FullNode),
            1 => Some(Self::ShortNode),
            2 => Some(Self::ValueNode),
            3 => Some(Self::HashNode),
            _ => None,
        }
    }
}

/// This is a terrible design, which comes from the limitations of MDBX:
/// when there is a `dup-key` query, MDBX will only return data that is greater than
/// or equal to the key, and only return the value, without the matched key. We have to
/// save both key-value in the value field to determine whether the exact seek is
/// accurate. Just like `StorageEntry`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize, serde::Deserialize))]
pub struct StorageNodeEntry {
    /// dup-key for storage slot
    pub path: StoredNibblesSubKey,
    /// stored node
    pub node: StoredNode,
}

impl StorageNodeEntry {
    /// Create a new storage node
    pub fn new(path: StoredNibblesSubKey, node: Node) -> Self {
        Self { path, node: node.into() }
    }

    /// Create a new storage node with reference
    pub fn create(path: &StoredNibblesSubKey, node: &Node) -> Self {
        Self::new(path.clone(), node.clone())
    }
}

#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for StorageNodeEntry {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        let path_len = self.path.to_compact(buf);
        let node_len = self.node.to_compact(buf);
        path_len + node_len
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let (path, buf) = StoredNibblesSubKey::from_compact(buf, 33);
        let (node, buf) = StoredNode::from_compact(buf, len - 33);
        let this = Self { path, node };
        (this, buf)
    }
}

/// The `Node` structure is nested, and using `derive(serde)` for serialization
/// would result in data that is not compact enough for our needs.
/// Therefore, we implement a custom serialization method for `Node` and store
/// the serialized data directly as a `Vec<u8>`.
/// To ensure forward compatibility with future changes to the `Node` structure,
/// a `version` field is included to indicate the serialization format version.
pub type StoredNode = Vec<u8>;

impl From<Node> for StoredNode {
    fn from(node: Node) -> Self {
        let mut buf = Self::with_capacity(1024);
        // version
        buf.push(0u8);
        match node {
            Node::FullNode { children, .. } => {
                buf.push(NodeType::FullNode as u8);
                for child in children {
                    if let Some(child) = child {
                        buf.push(1u8);
                        if let Node::HashNode(rlp) = *child {
                            buf.push(rlp.len() as u8);
                            buf.extend_from_slice(&rlp);
                        } else {
                            unreachable!("Only nested HashNode can be serialized!");
                        }
                    } else {
                        buf.push(0u8);
                    }
                }
            }
            Node::ShortNode { key, value, .. } => {
                buf.push(NodeType::ShortNode as u8);
                buf.push(key.len() as u8);
                buf.extend_from_slice(&key);
                match *value {
                    Node::HashNode(rlp) => {
                        // extension node
                        buf.push(NodeType::HashNode as u8);
                        buf.push(rlp.len() as u8);
                        buf.extend_from_slice(&rlp);
                    }
                    Node::ValueNode(value) => {
                        // leaf node
                        buf.push(NodeType::ValueNode as u8);
                        buf.push(value.len() as u8);
                        buf.extend_from_slice(&value);
                    }
                    _ => {
                        unreachable!("Only nested HashNode/ValueNode can be serialized!");
                    }
                }
            }
            Node::ValueNode(value) => {
                buf.push(NodeType::ValueNode as u8);
                buf.push(value.len() as u8);
                buf.extend_from_slice(&value);
            }
            Node::HashNode(rlp_node) => {
                buf.push(NodeType::HashNode as u8);
                buf.push(rlp_node.len() as u8);
                buf.extend_from_slice(&rlp_node);
            }
        }
        buf
    }
}

impl From<StoredNode> for Node {
    fn from(value: StoredNode) -> Self {
        let mut i = 0;
        let version = value[i];
        assert!(version == 0, "Unresolved Node version");
        i += 1;
        let node_type = NodeType::from_u8(value[i]);
        i += 1;
        match node_type {
            Some(NodeType::FullNode) => {
                // FullNode
                let mut children: [Option<Box<Self>>; 17] = Default::default();
                for child in &mut children {
                    let marker = value[i];
                    i += 1;
                    if marker == 1 {
                        let len = value[i] as usize;
                        i += 1;
                        let rlp = RlpNode::from_raw(&value[i..i + len]).unwrap();
                        i += len;
                        *child = Some(Box::new(Self::HashNode(rlp)));
                    }
                }
                Self::FullNode { children, flags: NodeFlag::new(None) }
            }
            Some(NodeType::ShortNode) => {
                // ShortNode
                let key_len = value[i] as usize;
                i += 1;
                let mut key = Nibbles::new();
                key.extend_from_slice_unchecked(&value[i..i + key_len]);
                i += key_len;
                let next_node_type = NodeType::from_u8(value[i]);
                i += 1;
                let next_node = match next_node_type {
                    Some(NodeType::HashNode) => {
                        // extension node
                        let rlp_len = value[i] as usize;
                        i += 1;
                        Self::HashNode(RlpNode::from_raw(&value[i..i + rlp_len]).unwrap())
                    }
                    Some(NodeType::ValueNode) => {
                        // leaf node
                        let val_len = value[i] as usize;
                        i += 1;
                        Self::ValueNode(value[i..i + val_len].to_vec())
                    }
                    _ => unreachable!(),
                };
                Self::ShortNode { key, value: Box::new(next_node), flags: NodeFlag::new(None) }
            }
            Some(NodeType::ValueNode) => {
                // ValueNode
                let val_len = value[i] as usize;
                i += 1;
                Self::ValueNode(value[i..i + val_len].to_vec())
            }
            Some(NodeType::HashNode) => {
                // HashNode
                let rlp_len = value[i] as usize;
                i += 1;
                Self::HashNode(RlpNode::from_raw(&value[i..i + rlp_len]).unwrap())
            }
            _ => {
                unreachable!("Unexpected Node type: {:?}", node_type);
            }
        }
    }
}

impl Node {
    /// Get the cached hash
    pub const fn cached_rlp(&self) -> Option<&RlpNode> {
        match self {
            Self::FullNode { children: _, flags } | Self::ShortNode { key: _, value: _, flags } => {
                flags.rlp.as_ref()
            }
            Self::ValueNode(_) => None,
            Self::HashNode(rlp_node) => Some(rlp_node),
        }
    }

    /// Test whether current node is changed
    pub const fn dirty(&self) -> bool {
        match self {
            Self::FullNode { children: _, flags } | Self::ShortNode { key: _, value: _, flags } => {
                flags.dirty
            }
            Self::ValueNode(_) => true,
            Self::HashNode(_) => false,
        }
    }

    /// Reset current node
    pub fn reset(mut self) -> Self {
        match &mut self {
            Self::FullNode { children: _, flags } | Self::ShortNode { key: _, value: _, flags } => {
                flags.reset()
            }
            _ => {}
        }
        self
    }

    /// Convert to `BranchNodeCompact`
    pub fn convert_node_compact(children: &[Option<Box<Self>>; 17]) -> BranchNodeCompact {
        let mut state_mask = TrieMask::default();
        let mut tree_mask = TrieMask::default();
        let mut hash_mask = TrieMask::default();
        let mut hashes = Vec::new();

        for (i, child) in children.iter().enumerate() {
            if let Some(child) = child {
                state_mask.set_bit(i as u8);
                match child.as_ref() {
                    Self::HashNode(rlp) => {
                        hash_mask.set_bit(i as u8);
                        let hash =
                            rlp.as_hash().unwrap_or_else(|| alloy_primitives::keccak256(rlp));
                        hashes.push(hash);
                    }
                    _ => {
                        tree_mask.set_bit(i as u8);
                    }
                }
            }
        }

        BranchNodeCompact {
            state_mask,
            tree_mask,
            hash_mask,
            hashes: Arc::new(hashes),
            root_hash: None,
        }
    }

    /// Convert to `BranchNodeCompact`
    pub fn branch_node_compact(&self) -> BranchNodeCompact {
        if let Self::FullNode { children, .. } = self {
            Self::convert_node_compact(children)
        } else {
            panic!("Only FullNode can be converted to BranchNodeCompact")
        }
    }

    /// Build hash for current node
    pub fn build_hash(&mut self, buf: &mut Vec<u8>) -> &RlpNode {
        match self {
            Self::FullNode { children, flags } => {
                if flags.rlp.is_none() {
                    let header = Header {
                        list: true,
                        payload_length: branch_node_rlp_length(children, buf),
                    };
                    buf.clear();
                    header.encode(buf);
                    for child in children {
                        if let Some(child) = child {
                            buf.put_slice(child.cached_rlp().unwrap());
                        } else {
                            buf.put_u8(EMPTY_STRING_CODE);
                        }
                    }
                    flags.rlp = Some(RlpNode::from_rlp(buf));
                }
                flags.rlp.as_ref().unwrap()
            }
            Self::ShortNode { key, value, flags } => {
                if flags.rlp.is_none() {
                    if let Self::ValueNode(value) = value.as_ref() {
                        // leaf node
                        let value = value.as_ref();
                        let header =
                            Header { list: true, payload_length: leaf_node_rlp_length(key, value) };
                        buf.clear();
                        header.encode(buf);
                        encode_path_leaf(key, true).as_slice().encode(buf);
                        Encodable::encode(value, buf);
                    } else {
                        // extension node
                        let header = Header {
                            list: true,
                            payload_length: extension_node_rlp_length(key, value, buf),
                        };
                        buf.clear();
                        header.encode(buf);
                        encode_path_leaf(key, false).as_slice().encode(buf);
                        buf.put_slice(value.cached_rlp().unwrap());
                    }
                    flags.rlp = Some(RlpNode::from_rlp(buf));
                }
                flags.rlp.as_ref().unwrap()
            }
            Self::HashNode(rlp_node) => rlp_node,
            _ => unreachable!(),
        }
    }

    /// Get the hash of current node
    pub fn hash(&self) -> B256 {
        if let Some(node_ref) = self.cached_rlp() {
            if let Some(hash) = node_ref.as_hash() {
                hash
            } else {
                keccak256(node_ref)
            }
        } else {
            panic!("build hash first!");
        }
    }
}

/// Returns the length of RLP encoded fields of branch node.
fn branch_node_rlp_length(children: &mut [Option<Box<Node>>; 17], buf: &mut Vec<u8>) -> usize {
    let mut payload_length = 0;
    for child in children {
        if let Some(child) = child {
            if let Some(rlp) = child.cached_rlp() {
                payload_length += rlp.len();
            } else {
                payload_length += child.build_hash(buf).len();
            }
        } else {
            payload_length += 1;
        }
    }
    payload_length
}

/// Returns the length of RLP encoded fields of extension node.
fn extension_node_rlp_length(
    shared_nibbles: &Nibbles,
    next_node: &mut Box<Node>,
    buf: &mut Vec<u8>,
) -> usize {
    let mut encoded_key_len = shared_nibbles.len() / 2 + 1;
    // For extension nodes the first byte cannot be greater than 0x80.
    if encoded_key_len != 1 {
        encoded_key_len += length_of_length(encoded_key_len);
    }
    if let Some(rlp) = next_node.cached_rlp() {
        encoded_key_len += rlp.len();
    } else {
        encoded_key_len += next_node.build_hash(buf).len();
    }
    encoded_key_len
}

/// Returns the length of RLP encoded fields of leaf node.
fn leaf_node_rlp_length(key_end: &Nibbles, value: &[u8]) -> usize {
    let mut encoded_key_len = key_end.len() / 2 + 1;
    // For leaf nodes the first byte cannot be greater than 0x80.
    if encoded_key_len != 1 {
        encoded_key_len += length_of_length(encoded_key_len);
    }
    encoded_key_len + Encodable::length(value)
}
