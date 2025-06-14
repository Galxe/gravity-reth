use alloy_primitives::{keccak256, B256};
use alloy_rlp::{length_of_length, BufMut, Encodable, Header, EMPTY_STRING_CODE};
use alloy_trie::nodes::{encode_path_leaf, RlpNode};
use nybbles::Nibbles;

/// Cache hash value(RlpNode) of current Node to prevent duplicate caculations,
/// and the `dirty` indicates whether current Node has been updated.
/// NodeFlag is not stored in database, and read as default when a Node is loaded
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct NodeFlag {
    pub rlp: Option<RlpNode>,
    pub dirty: bool,
}

impl NodeFlag {
    pub fn new(rlp: Option<RlpNode>) -> Self {
        Self { rlp, dirty: false }
    }

    pub fn dirty_node() -> Self {
        Self { rlp: None, dirty: true }
    }

    // mark current node as dirty, and wipe the cached hash
    pub fn mark_diry(&mut self) {
        self.rlp.take();
        self.dirty = true;
    }

    // for test use: serialize and deserialize
    pub fn reset(&mut self) {
        self.rlp.take();
        self.dirty = false;
    }
}

/// Nested node type for MPT node:
///
/// `FullNode` for branch node
///
/// `ShortNode` for extension node(when value is HashNode), or leaf node(when value is ValueNode)
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
    FullNode { children: [Option<Box<Node>>; 17], flags: NodeFlag },
    ShortNode { key: Nibbles, value: Box<Node>, flags: NodeFlag },
    ValueNode(Vec<u8>),
    HashNode(RlpNode),
}

/// Deep copying nested `Node` is a very costly operation, and to void accidental copy, only the
/// copy of non-nested `Node` is allowed.
impl Clone for Node {
    fn clone(&self) -> Self {
        match self {
            Self::FullNode { children, flags } => {
                // only nested hash node can be cloned
                for child in children {
                    if let Some(child) = child {
                        match child.as_ref() {
                            Node::HashNode(_) => {}
                            _ => {
                                panic!("Only non-nested node can be cloned!");
                            }
                        }
                    }
                }
                Self::FullNode { children: children.clone(), flags: flags.clone() }
            }
            Self::ShortNode { key, value, flags } => {
                match value.as_ref() {
                    Node::FullNode { .. } | Node::ShortNode { .. } => {
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
enum NodeType {
    FullNode = 0,
    ShortNode = 1,
    ValueNode = 2,
    HashNode = 3,
}

impl NodeType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(NodeType::FullNode),
            1 => Some(NodeType::ShortNode),
            2 => Some(NodeType::ValueNode),
            3 => Some(NodeType::HashNode),
            _ => None,
        }
    }
}

/// This is a highly bad design, which comes from the limitations of MDBX:
/// when there is a `dup-key` query, MDBX will only return data that is greater than
/// or equal to the key, and only return the value, not the matched key. We have to
/// save both key-value in the value field to determine whether the exact seek is
/// accurate. Just like `StorageEntry`.
#[derive(Debug, Clone)]
pub struct NodeEntry {
    pub path: Nibbles,
    pub node: Node,
}
pub type StoredNode = Vec<u8>;

impl From<NodeEntry> for StoredNode {
    fn from(value: NodeEntry) -> Self {
        let NodeEntry { path, node } = value;
        let mut buf = Vec::with_capacity(1024);
        // version
        buf.push(0u8);
        // path
        buf.push(path.len() as u8);
        buf.extend_from_slice(&path);
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

impl From<StoredNode> for NodeEntry {
    fn from(value: StoredNode) -> Self {
        let mut i = 0;
        let version = value[i];
        if version != 0 {
            panic!("Unresolved Node version");
        }
        i += 1;
        let path_len = value[i] as usize;
        i += 1;
        let mut path = Nibbles::new();
        path.extend_from_slice_unchecked(&value[i..i + path_len]);
        i += path_len;
        let node_type = NodeType::from_u8(value[i]);
        i += 1;
        let node = match node_type {
            Some(NodeType::FullNode) => {
                // FullNode
                let mut children: [Option<Box<Node>>; 17] = Default::default();
                for child in children.iter_mut() {
                    let marker = value[i];
                    i += 1;
                    if marker == 1 {
                        let len = value[i] as usize;
                        i += 1;
                        let rlp = RlpNode::from_raw(&value[i..i + len]).unwrap();
                        i += len;
                        *child = Some(Box::new(Node::HashNode(rlp)));
                    }
                }
                Node::FullNode { children, flags: NodeFlag::new(None) }
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
                        Node::HashNode(RlpNode::from_raw(&value[i..i + rlp_len]).unwrap())
                    }
                    Some(NodeType::ValueNode) => {
                        // leaf node
                        let val_len = value[i] as usize;
                        i += 1;
                        Node::ValueNode(value[i..i + val_len].to_vec())
                    }
                    _ => unreachable!(),
                };
                Node::ShortNode { key, value: Box::new(next_node), flags: NodeFlag::new(None) }
            }
            Some(NodeType::ValueNode) => {
                // ValueNode
                let val_len = value[i] as usize;
                i += 1;
                Node::ValueNode(value[i..i + val_len].to_vec())
            }
            Some(NodeType::HashNode) => {
                // HashNode
                let rlp_len = value[i] as usize;
                i += 1;
                Node::HashNode(RlpNode::from_raw(&value[i..i + rlp_len]).unwrap())
            }
            _ => {
                unreachable!("Unexpected Node type: {:?}", node_type);
            }
        };
        NodeEntry { path, node }
    }
}

impl Node {
    pub fn cached_rlp(&self) -> Option<&RlpNode> {
        match self {
            Node::FullNode { children: _, flags } => flags.rlp.as_ref(),
            Node::ShortNode { key: _, value: _, flags } => flags.rlp.as_ref(),
            Node::ValueNode(_) => None,
            Node::HashNode(rlp_node) => Some(&rlp_node),
        }
    }

    pub fn dirty(&self) -> bool {
        match self {
            Node::FullNode { children: _, flags } => flags.dirty,
            Node::ShortNode { key: _, value: _, flags } => flags.dirty,
            Node::ValueNode(_) => true,
            Node::HashNode(_) => false,
        }
    }

    // for test use
    pub fn reset(&mut self) {
        match self {
            Node::FullNode { children: _, flags } => flags.reset(),
            Node::ShortNode { key: _, value: _, flags } => flags.reset(),
            _ => {}
        }
    }

    pub fn build_hash(&mut self, buf: &mut Vec<u8>) -> &RlpNode {
        match self {
            Node::FullNode { children, flags } => {
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
            Node::ShortNode { key, value, flags } => {
                if flags.rlp.is_none() {
                    if let Node::ValueNode(value) = value.as_ref() {
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
            Node::HashNode(rlp_node) => rlp_node,
            _ => unreachable!(),
        }
    }

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
