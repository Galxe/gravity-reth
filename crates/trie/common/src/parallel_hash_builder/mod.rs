//! The implementation of the hash builder.

use alloy_primitives::{keccak256, B256};
use alloy_trie::{
    nodes::{BranchNodeRef, ExtensionNodeRef, LeafNode, RlpNode},
    BranchNodeCompact, HashMap, Nibbles, TrieMask, EMPTY_ROOT_HASH,
};
use core::cmp;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    };
use tracing::trace;

mod value;
pub use value::{HashBuilderValue, HashBuilderValueRef};

/// A component used to construct the root hash of the trie.
///
/// The primary purpose of a Hash Builder is to build the Merkle proof that is essential for
/// verifying the integrity and authenticity of the trie's contents. It achieves this by
/// constructing the root hash from the hashes of child nodes according to specific rules, depending
/// on the type of the node (branch, extension, or leaf).
///
/// Here's an overview of how the Hash Builder works for each type of node:
///  * Branch Node: The Hash Builder combines the hashes of all the child nodes of the branch node,
///    using a cryptographic hash function like SHA-256. The child nodes' hashes are concatenated
///    and hashed, and the result is considered the hash of the branch node. The process is repeated
///    recursively until the root hash is obtained.
///  * Extension Node: In the case of an extension node, the Hash Builder first encodes the node's
///    shared nibble path, followed by the hash of the next child node. It concatenates these values
///    and then computes the hash of the resulting data, which represents the hash of the extension
///    node.
///  * Leaf Node: For a leaf node, the Hash Builder first encodes the key-path and the value of the
///    leaf node. It then concatenates the encoded key-path and value, and computes the hash of this
///    concatenated data, which represents the hash of the leaf node.
///
/// The Hash Builder operates recursively, starting from the bottom of the trie and working its way
/// up, combining the hashes of child nodes and ultimately generating the root hash. The root hash
/// can then be used to verify the integrity and authenticity of the trie's data by constructing and
/// verifying Merkle proofs.
#[derive(Debug, Clone)]
pub enum RawRlpNode {
    Leaf(LeafNode),
    Word(B256),
    Extension((Nibbles, Arc<RawRlpNode>)),
    Branch(
        (
            Vec<Arc<RawRlpNode>>,
            TrieMask,
            TrieMask,
            usize,
            Option<Sender<Vec<B256>>>,
            Option<Sender<B256>>,
        ),
    ),
    Default,
}

impl Default for RawRlpNode {
    fn default() -> Self {
        RawRlpNode::Default
    }
}

impl RawRlpNode {
    fn rlp(&self) -> RlpNode {
        let mut rlp_buf = vec![];
        match self {
            RawRlpNode::Leaf(leaf_node) => leaf_node.as_ref().rlp(&mut rlp_buf),
            RawRlpNode::Word(word) => RlpNode::word_rlp(word),
            RawRlpNode::Extension((key, child)) => {
                ExtensionNodeRef::new(key, &child.rlp()).rlp(&mut rlp_buf)
            }
            RawRlpNode::Branch((
                stack,
                state_mask,
                hash_mask,
                first_child_idx,
                tx,
                root_hash_tx,
            )) => {
                let children: Vec<RlpNode> = stack
                    .par_iter()
                    .skip(*first_child_idx)
                    .map(|raw_rlp_node| raw_rlp_node.rlp())
                    .collect();

                let mut all_children = vec![RlpNode::default(); *first_child_idx];
                all_children.extend(children);
                let branch_node = BranchNodeRef::new(&all_children, *state_mask);
                if let Some(tx) = tx {
                    let _ = tx.send(branch_node.child_hashes(*hash_mask).collect());
                }
                let rlp = branch_node.rlp(&mut rlp_buf);
                if let Some(tx) = root_hash_tx {
                    let hash =
                        if let Some(hash) = rlp.as_hash() { hash } else { keccak256(rlp.clone()) };
                    let _ = tx.send(hash);
                }
                rlp
            }
            _ => panic!("Cannot be Default"),
        }
    }
}

#[derive(Debug, Default)]
#[allow(missing_docs)]
pub struct ParallelHashBuilder {
    pub key: Nibbles,
    pub value: HashBuilderValue,
    pub stack: Vec<Arc<RawRlpNode>>, // RlpNode

    pub state_masks: Vec<TrieMask>,
    pub tree_masks: Vec<TrieMask>,
    pub hash_masks: Vec<TrieMask>,

    pub stored_in_database: bool,

    pub updated_branch_nodes: Option<
        HashMap<Nibbles, (BranchNodeCompact, Option<Receiver<Vec<B256>>>, Option<Receiver<B256>>)>,
    >,

    pub counter: usize,
}

impl ParallelHashBuilder {
    /// Enables the Hash Builder to store updated branch nodes.
    ///
    /// Call [HashBuilder::split] to get the updates to branch nodes.
    pub fn with_updates(mut self, retain_updates: bool) -> Self {
        self.set_updates(retain_updates);
        self
    }

    /// Enables the Hash Builder to store updated branch nodes.
    ///
    /// Call [HashBuilder::split] to get the updates to branch nodes.
    pub fn set_updates(&mut self, retain_updates: bool) {
        if retain_updates {
            self.updated_branch_nodes = Some(HashMap::default());
        }
    }

    /// Splits the [HashBuilder] into a [HashBuilder] and hash builder updates.
    pub fn split(mut self) -> (Self, HashMap<Nibbles, BranchNodeCompact>) {
        let updates = self.updated_branch_nodes.take();
        let res = updates
            .unwrap_or_default()
            .into_iter()
            .map(|(key, (mut branch_node, rx, root_hash_rx))| {
                if let Some(rx) = rx {
                    branch_node.hashes = rx.recv().unwrap().into();
                }
                if let Some(root_hash_rx) = root_hash_rx {
                    branch_node.root_hash = Some(root_hash_rx.recv().unwrap());
                }
                (key, branch_node)
            })
            .collect();
        (self, res)
    }

    /// The number of total updates accrued.
    /// Returns `0` if [Self::with_updates] was not called.
    pub fn updates_len(&self) -> usize {
        self.updated_branch_nodes.as_ref().map(|u| u.len()).unwrap_or(0)
    }

    /// Adds a new leaf element and its value to the trie hash builder.
    ///
    /// # Panics
    ///
    /// Panics if the new key does not come after the current key.
    pub fn add_leaf(&mut self, key: Nibbles, value: &[u8]) {
        assert!(key > self.key, "add_leaf key {:?} self.key {:?}", key, self.key);
        self.add_leaf_unchecked(key, value);
    }

    /// Adds a new leaf element and its value to the trie hash builder,
    /// without checking the order of the new key. This is only for
    /// performance-critical usage that guarantees keys are inserted
    /// in sorted order.
    pub fn add_leaf_unchecked(&mut self, key: Nibbles, value: &[u8]) {
        debug_assert!(key > self.key, "add_leaf_unchecked key {:?} self.key {:?}", key, self.key);
        if !self.key.is_empty() {
            self.update(&key);
        }
        self.set_key_value(key, HashBuilderValueRef::Bytes(value));
    }

    /// Adds a new branch element and its hash to the trie hash builder.
    pub fn add_branch(&mut self, key: Nibbles, value: B256, stored_in_database: bool) {
        assert!(
            key > self.key || (self.key.is_empty() && key.is_empty()),
            "add_branch key {:?} self.key {:?}",
            key,
            self.key
        );
        if !self.key.is_empty() {
            self.update(&key);
        } else if key.is_empty() {
            self.stack.push(Arc::new(RawRlpNode::Word(value.clone())));
            self.counter += 1;
        }
        self.set_key_value(key, HashBuilderValueRef::Hash(&value));
        self.stored_in_database = stored_in_database;
    }

    /// Returns the current root hash of the trie builder.
    pub fn root(&mut self) -> B256 {
        // Clears the internal state
        if !self.key.is_empty() {
            self.update(&Nibbles::default());
            self.key.clear();
            self.value.clear();
        }
        let root = self.current_root();
        root
    }

    #[inline]
    fn set_key_value(&mut self, key: Nibbles, value: HashBuilderValueRef<'_>) {
        self.log_key_value("old value");
        self.key = key;
        self.value.set_from_ref(value);
        self.log_key_value("new value");
    }

    fn log_key_value(&self, msg: &str) {
        trace!(target: "trie::hash_builder",
            key = ?self.key,
            value = ?self.value,
            "{msg}",
        );
    }

    fn current_root(&mut self) -> B256 {
        if let Some(node_ref) = self.stack.last() {
            let rlp = node_ref.rlp();
            if let Some(hash) = rlp.as_hash() {
                hash
            } else {
                keccak256(rlp)
            }
        } else {
            EMPTY_ROOT_HASH
        }
    }

    /// Given a new element, it appends it to the stack and proceeds to loop through the stack state
    /// and convert the nodes it can into branch / extension nodes and hash them. This ensures
    /// that the top of the stack always contains the merkle root corresponding to the trie
    /// built so far.
    fn update(&mut self, succeeding: &Nibbles) {
        let mut build_extensions = false;
        // current / self.key is always the latest added element in the trie
        let mut current = self.key.clone();
        debug_assert!(!current.is_empty());

        trace!(target: "trie::hash_builder", ?current, ?succeeding, "updating merkle tree");

        let mut i = 0usize;
        loop {
            let _span = tracing::trace_span!(target: "trie::hash_builder", "loop", i, ?current, build_extensions).entered();

            let preceding_exists = !self.state_masks.is_empty();
            let preceding_len = self.state_masks.len().saturating_sub(1);

            let common_prefix_len = succeeding.common_prefix_length(current.as_slice());
            let len = cmp::max(preceding_len, common_prefix_len);
            assert!(len < current.len(), "len {} current.len {}", len, current.len());

            trace!(
                target: "trie::hash_builder",
                ?len,
                ?common_prefix_len,
                ?preceding_len,
                preceding_exists,
                "prefix lengths after comparing keys"
            );

            // Adjust the state masks for branch calculation
            let extra_digit = current[len];
            if self.state_masks.len() <= len {
                let new_len = len + 1;
                trace!(target: "trie::hash_builder", new_len, old_len = self.state_masks.len(), "scaling state masks to fit");
                self.state_masks.resize(new_len, TrieMask::default());
            }
            self.state_masks[len] |= TrieMask::from_nibble(extra_digit);
            trace!(
                target: "trie::hash_builder",
                ?extra_digit,
                state_masks = ?self.state_masks,
            );

            // Adjust the tree masks for exporting to the DB
            if self.tree_masks.len() < current.len() {
                self.resize_masks(current.len());
            }

            let mut len_from = len;
            if !succeeding.is_empty() || preceding_exists {
                len_from += 1;
            }
            trace!(target: "trie::hash_builder", "skipping {len_from} nibbles");

            // The key without the common prefix
            let short_node_key = current.slice(len_from..);
            trace!(target: "trie::hash_builder", ?short_node_key);

            // Concatenate the 2 nodes together
            if !build_extensions {
                match self.value.as_ref() {
                    HashBuilderValueRef::Bytes(leaf_value) => {
                        let leaf_node = Arc::new(RawRlpNode::Leaf(LeafNode::new(
                            short_node_key.clone(),
                            leaf_value.to_vec(),
                        )));
                        trace!(
                            target: "trie::hash_builder",
                            ?leaf_node,
                            "pushing leaf node",
                        );
                        self.stack.push(leaf_node);
                        self.counter += 1;
                    }
                    HashBuilderValueRef::Hash(hash) => {
                        trace!(target: "trie::hash_builder", ?hash, "pushing branch node hash");
                        self.stack.push(Arc::new(RawRlpNode::Word(*hash)));
                        self.counter += 1;

                        if self.stored_in_database {
                            self.tree_masks[current.len() - 1] |=
                                TrieMask::from_nibble(current.last().unwrap());
                        }
                        self.hash_masks[current.len() - 1] |=
                            TrieMask::from_nibble(current.last().unwrap());

                        build_extensions = true;
                    }
                }
            }

            if build_extensions && !short_node_key.is_empty() {
                self.update_masks(&current, len_from);
                let stack_last = self.stack.pop().expect("there should be at least one stack item");
                let extension_node =
                    Arc::new(RawRlpNode::Extension((short_node_key.clone(), stack_last)));
                trace!(
                    target: "trie::hash_builder",
                    ?extension_node,
                    "pushing extension node",
                );
                self.stack.push(extension_node);
                self.counter += 1;
                self.resize_masks(len_from);
            }

            if preceding_len <= common_prefix_len && !succeeding.is_empty() {
                trace!(target: "trie::hash_builder", "no common prefix to create branch nodes from, returning");
                return;
            }

            // Insert branch nodes in the stack
            if !succeeding.is_empty() || preceding_exists {
                let (root_hash_tx, root_hash_rx) = if len == 0 {
                    let (tx, rx) = std::sync::mpsc::channel();
                    (Some(tx), Some(rx))
                } else {
                    (None, None)
                };
                // Pushes the corresponding branch node to the stack
                let rx = self.push_branch_node(&current, len, root_hash_tx);
                // Need to store the branch node in an efficient format outside of the hash builder
                self.store_branch_node(&current, len, rx, root_hash_rx);
            }

            self.state_masks.resize(len, TrieMask::default());
            self.resize_masks(len);

            if preceding_len == 0 {
                trace!(target: "trie::hash_builder", "0 or 1 state masks means we have no more elements to process");
                return;
            }

            current.truncate(preceding_len);
            trace!(target: "trie::hash_builder", ?current, "truncated nibbles to {} bytes", preceding_len);

            trace!(target: "trie::hash_builder", state_masks = ?self.state_masks, "popping empty state masks");
            while self.state_masks.last() == Some(&TrieMask::default()) {
                self.state_masks.pop();
            }

            build_extensions = true;

            i += 1;
        }
    }

    /// Given the size of the longest common prefix, it proceeds to create a branch node
    /// from the state mask and existing stack state, and store its RLP to the top of the stack,
    /// after popping all the relevant elements from the stack.
    ///
    /// Returns the hashes of the children of the branch node, only if `updated_branch_nodes` is
    /// enabled.
    fn push_branch_node(
        &mut self,
        current: &Nibbles,
        len: usize,
        sender: Option<Sender<B256>>,
    ) -> Option<Receiver<Vec<B256>>> {
        let state_mask = self.state_masks[len];
        let hash_mask = self.hash_masks[len];
        let first_child_idx = self.stack.len() - state_mask.count_ones() as usize;
        // Avoid calculating this value if it's not needed.
        let (branch_node, rx) = if self.updated_branch_nodes.is_some() {
            let (tx, rx) = mpsc::channel();
            (
                Arc::new(RawRlpNode::Branch((
                    self.stack.clone(),
                    state_mask,
                    hash_mask,
                    first_child_idx,
                    Some(tx),
                    sender,
                ))),
                Some(rx),
            )
        } else {
            (
                Arc::new(RawRlpNode::Branch((
                    self.stack.clone(),
                    state_mask,
                    hash_mask,
                    first_child_idx,
                    None,
                    sender,
                ))),
                None,
            )
        };

        // Clears the stack from the branch node elements
        trace!(
            target: "trie::hash_builder",
            new_len = first_child_idx,
            old_len = self.stack.len(),
            "resizing stack to prepare branch node"
        );
        self.stack.resize_with(first_child_idx, Default::default);

        trace!(target: "trie::hash_builder", ?branch_node, "pushing branch node with {state_mask:?} mask from stack");
        self.stack.push(branch_node);
        self.counter += 1;
        rx
    }

    /// Given the current nibble prefix and the highest common prefix length, proceeds
    /// to update the masks for the next level and store the branch node and the
    /// masks in the database. We will use that when consuming the intermediate nodes
    /// from the database to efficiently build the trie.
    fn store_branch_node(
        &mut self,
        current: &Nibbles,
        len: usize,
        rx: Option<Receiver<Vec<B256>>>,
        root_hash_rx: Option<Receiver<B256>>,
    ) {
        if len > 0 {
            let parent_index = len - 1;
            self.hash_masks[parent_index] |= TrieMask::from_nibble(current[parent_index]);
        }

        let store_in_db_trie = !self.tree_masks[len].is_empty() || !self.hash_masks[len].is_empty();
        if store_in_db_trie {
            if len > 0 {
                let parent_index = len - 1;
                self.tree_masks[parent_index] |= TrieMask::from_nibble(current[parent_index]);
            }

            if self.updated_branch_nodes.is_some() {
                let common_prefix = current.slice(..len);
                let hashes_len = self.hash_masks[len].count_ones() as usize;
                let node = BranchNodeCompact::new(
                    self.state_masks[len],
                    self.tree_masks[len],
                    self.hash_masks[len],
                    vec![B256::ZERO; hashes_len],
                    None,
                );
                trace!(target: "trie::hash_builder", ?node, "intermediate node");
                self.updated_branch_nodes
                    .as_mut()
                    .unwrap()
                    .insert(common_prefix, (node, rx, root_hash_rx));
            }
        }
    }

    fn update_masks(&mut self, current: &Nibbles, len_from: usize) {
        if len_from > 0 {
            let flag = TrieMask::from_nibble(current[len_from - 1]);

            self.hash_masks[len_from - 1] &= !flag;

            if !self.tree_masks[current.len() - 1].is_empty() {
                self.tree_masks[len_from - 1] |= flag;
            }
        }
    }

    fn resize_masks(&mut self, new_len: usize) {
        trace!(
            target: "trie::hash_builder",
            new_len,
            old_tree_mask_len = self.tree_masks.len(),
            old_hash_mask_len = self.hash_masks.len(),
            "resizing tree/hash masks"
        );
        self.tree_masks.resize(new_len, TrieMask::default());
        self.hash_masks.resize(new_len, TrieMask::default());
    }
}
