use alloc::{boxed::Box, sync::Arc, vec::Vec};

use alloy_primitives::{keccak256, Bytes, B256};
use alloy_rlp::{length_of_length, BufMut, Encodable, Header, EMPTY_STRING_CODE};
use alloy_trie::{
    nodes::{encode_path_leaf, BranchNode, ExtensionNode, LeafNode, RlpNode, TrieNode},
    BranchNodeCompact, TrieMask,
};
use bytes::BytesMut;
use nybbles::Nibbles;
use reth_primitives_traits::SubkeyContainedValue;

use crate::StoredNibblesSubKey;

/// Cache hash value(RlpNode) of current Node to prevent duplicate calculations,
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
                Self::ShortNode { key: *key, value: value.clone(), flags: flags.clone() }
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
    BranchNode = 0,
    ExtensionNode = 1,
    LeafNode = 2,
}

impl NodeType {
    const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::BranchNode),
            1 => Some(Self::ExtensionNode),
            2 => Some(Self::LeafNode),
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
#[cfg_attr(feature = "test-utils", derive(arbitrary::Arbitrary))]
pub struct StorageNodeEntry {
    /// dup-key for storage slot
    pub path: StoredNibblesSubKey,
    /// stored node
    pub node: StoredNode,
}

impl SubkeyContainedValue for StorageNodeEntry {
    fn subkey_length(&self) -> Option<usize> {
        Some(self.path.len().div_ceil(2) + 1)
    }
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
        let encoded_len = buf[0];
        let odd = encoded_len % 2 == 0;
        let pack_len = (encoded_len / 2) as usize;
        let mut nibbles = Nibbles::unpack(&buf[1..1 + pack_len]);
        if odd {
            nibbles.pop();
        }
        let path = StoredNibblesSubKey(nibbles);
        let (node, buf) = StoredNode::from_compact(&buf[pack_len + 1..], len - pack_len - 1);
        let this = Self { path, node };
        (this, buf)
    }
}

/// The `Node` structure is nested, and using `derive(serde)` for serialization
/// would result in data that is not compact enough for our needs.
/// Therefore, we implement a custom serialization method for `Node` and store
/// the serialized data directly as a `Bytes`.
pub type StoredNode = Bytes;

impl From<Node> for StoredNode {
    fn from(node: Node) -> Self {
        let mut buf = BytesMut::with_capacity(256);
        match node {
            Node::FullNode { children, .. } => {
                buf.put_u8(NodeType::BranchNode as u8);
                let mut mask = [u8::MAX; 16];
                for (i, child) in children.into_iter().enumerate() {
                    if let Some(child) = child {
                        if let Node::HashNode(rlp) = *child {
                            mask[i] = rlp.len() as u8;
                            buf.extend_from_slice(&rlp);
                        } else {
                            unreachable!("Only nested HashNode can be serialized in FullNode!");
                        }
                    }
                }
                buf.extend(mask);
            }
            Node::ShortNode { key, value, .. } => {
                match *value {
                    Node::HashNode(rlp) => {
                        // extension node
                        buf.put_u8(NodeType::ExtensionNode as u8);
                        buf.put_u8(rlp.len() as u8);
                        buf.extend_from_slice(&rlp);
                        buf.put_u8((key.len() % 2) as u8);
                        buf.extend(key.pack());
                    }
                    Node::ValueNode(value) => {
                        // leaf node
                        buf.put_u8(NodeType::LeafNode as u8);
                        let pack = key.pack();
                        let pack_len = pack.len() as u8;
                        let encoded_len = pack_len << 1 | (key.len() as u8 % 2);
                        buf.put_u8(encoded_len);
                        buf.extend(pack);
                        buf.extend(value);
                    }
                    _ => {
                        unreachable!(
                            "Only nested HashNode/ValueNode can be serialized in ShortNode!"
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
        Self::from(buf.freeze())
    }
}

impl From<StoredNode> for Node {
    fn from(value: StoredNode) -> Self {
        let value = value.as_ref();
        let node_type = NodeType::from_u8(value[0]);
        match node_type {
            Some(NodeType::BranchNode) => {
                // FullNode
                let mut children: [Option<Box<Self>>; 17] = Default::default();
                let mask: [u8; 16] = value[value.len() - 16..].try_into().unwrap();
                let mut start = 1;
                for i in 0..16 {
                    if mask[i] != u8::MAX {
                        let end = start + mask[i] as usize;
                        let rlp = RlpNode::from_raw(&value[start..end]).unwrap();
                        children[i] = Some(Box::new(Self::HashNode(rlp)));
                        start = end;
                    }
                }
                Self::FullNode { children, flags: NodeFlag::new(None) }
            }
            Some(NodeType::ExtensionNode) => {
                // ShortNode
                let rlp_len = value[1] as usize;
                let next = Self::HashNode(RlpNode::from_raw(&value[2..rlp_len + 2]).unwrap());
                let odd = value[rlp_len + 2];
                let mut shared_nibbles = Nibbles::unpack(&value[rlp_len + 3..]);
                if odd == 1 {
                    shared_nibbles.pop();
                }
                Self::ShortNode {
                    key: shared_nibbles,
                    value: Box::new(next),
                    flags: NodeFlag::new(None),
                }
            }
            Some(NodeType::LeafNode) => {
                // ValueNode
                let encoded_len = value[1];
                let odd = encoded_len & 1u8;
                let pack_len = (encoded_len >> 1) as usize;
                let mut key_end = Nibbles::unpack(&value[2..2 + pack_len]);
                if odd == 1 {
                    key_end.pop();
                }
                let value = value[2 + pack_len..].to_vec();
                Self::ShortNode {
                    key: key_end,
                    value: Box::new(Self::ValueNode(value)),
                    flags: NodeFlag::new(None),
                }
            }
            _ => {
                unreachable!("Unexpected Node type: {:?}", node_type);
            }
        }
    }
}

impl From<Node> for TrieNode {
    fn from(node: Node) -> Self {
        match node {
            Node::FullNode { children, .. } => {
                let mut stack = Vec::new();
                let mut state_mask = TrieMask::default();
                for (i, child) in children.into_iter().enumerate() {
                    if let Some(child) = child {
                        state_mask.set_bit(i as u8);
                        if let Node::HashNode(rlp) = *child {
                            stack.push(rlp);
                        } else {
                            panic!("FullNode children should be HashNode for proof conversion");
                        }
                    }
                }
                TrieNode::Branch(BranchNode::new(stack, state_mask))
            }
            Node::ShortNode { key, value, .. } => {
                match *value {
                    Node::ValueNode(v) => {
                        // Leaf node
                        TrieNode::Leaf(LeafNode::new(key, v))
                    }
                    Node::HashNode(rlp) => {
                        // Extension node
                        TrieNode::Extension(ExtensionNode::new(key, rlp))
                    }
                    _ => panic!(
                        "ShortNode value should be ValueNode or HashNode for proof conversion"
                    ),
                }
            }
            Node::HashNode(_) => panic!("HashNode cannot be directly converted to TrieNode"),
            Node::ValueNode(_) => panic!("ValueNode cannot be directly converted to TrieNode"),
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

    /// Set cached hash
    pub fn set_rlp(&mut self, rlp: RlpNode) {
        match self {
            Self::FullNode { children: _, flags } | Self::ShortNode { key: _, value: _, flags } => {
                flags.rlp = Some(rlp);
            }
            _ => {}
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

#[cfg(test)]
mod tests {
    use reth_codecs::Compact;

    use super::*;

    #[test]
    fn test_storage_entry_compact() {
        let nibbles = Nibbles::from_nibbles(&[0x0A, 0x0B, 0x0C, 0x0D, 0x00]);
        let subkey = StoredNibblesSubKey(nibbles.clone());
        let node = Node::ShortNode {
            key: nibbles,
            value: Box::new(Node::ValueNode(vec![1u8, 3, 5])),
            flags: Default::default(),
        };
        let storage_entry = StorageNodeEntry::new(subkey, node);
        let mut buf = BytesMut::with_capacity(256);
        let encode_len = storage_entry.to_compact(&mut buf);
        let (decode_entry, buf) = StorageNodeEntry::from_compact(&buf, encode_len);
        assert_eq!(buf.len(), 0);
        assert_eq!(storage_entry, decode_entry);
    }
}
