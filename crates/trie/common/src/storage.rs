<<<<<<< HEAD
use reth_primitives_traits::SubkeyContainedValue;

use super::{BranchNodeCompact, StoredNibblesSubKey};

/// Account storage trie node.
=======
use super::{BranchNodeCompact, PackedStoredNibblesSubKey, StoredNibblesSubKey};
use reth_primitives_traits::ValueWithSubKey;

/// Account storage trie node.
///
/// `nibbles` is the subkey when used as a value in the `StorageTrie` table.
>>>>>>> v2.3.0
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize, serde::Deserialize))]
pub struct StorageTrieEntry {
    /// The nibbles of the intermediate node
    pub nibbles: StoredNibblesSubKey,
    /// Encoded node.
    pub node: BranchNodeCompact,
}

<<<<<<< HEAD
impl SubkeyContainedValue for StorageTrieEntry {
    fn subkey_length(&self) -> Option<usize> {
        Some(self.nibbles.len().div_ceil(2) + 1)
=======
impl ValueWithSubKey for StorageTrieEntry {
    type SubKey = StoredNibblesSubKey;

    fn get_subkey(&self) -> Self::SubKey {
        self.nibbles.clone()
>>>>>>> v2.3.0
    }
}

// NOTE: Removing reth_codec and manually encode subkey
// and compress second part of the value. If we have compression
// over whole value (Even SubKey) that would mess up fetching of values with seek_by_key_subkey
#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for StorageTrieEntry {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        let nibbles_len = self.nibbles.to_compact(buf);
        let node_len = self.node.to_compact(buf);
        nibbles_len + node_len
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
<<<<<<< HEAD
        use nybbles::Nibbles;

        let encoded_len = buf[0];
        let odd = encoded_len.is_multiple_of(2);
        let pack_len = (encoded_len / 2) as usize;
        let mut nibbles = Nibbles::unpack(&buf[1..1 + pack_len]);
        if odd {
            nibbles.pop();
        }
        let path = StoredNibblesSubKey(nibbles);
        let (node, buf) = BranchNodeCompact::from_compact(&buf[pack_len + 1..], len - pack_len - 1);
        let this = Self { nibbles: path, node };
=======
        let (nibbles, buf) = StoredNibblesSubKey::from_compact(buf, 65);
        let (node, buf) = BranchNodeCompact::from_compact(buf, len - 65);
        let this = Self { nibbles, node };
>>>>>>> v2.3.0
        (this, buf)
    }
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(StorageTrieEntry);

/// Account storage trie node with packed nibble encoding (storage v2).
///
/// Same as [`StorageTrieEntry`] but uses [`PackedStoredNibblesSubKey`] (33 bytes)
/// instead of [`StoredNibblesSubKey`] (65 bytes).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize, serde::Deserialize))]
pub struct PackedStorageTrieEntry {
    /// The nibbles of the intermediate node
    pub nibbles: PackedStoredNibblesSubKey,
    /// Encoded node.
    pub node: BranchNodeCompact,
}

impl ValueWithSubKey for PackedStorageTrieEntry {
    type SubKey = PackedStoredNibblesSubKey;

    fn get_subkey(&self) -> Self::SubKey {
        self.nibbles.clone()
    }
}

#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for PackedStorageTrieEntry {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        let nibbles_len = self.nibbles.to_compact(buf);
        let node_len = self.node.to_compact(buf);
        nibbles_len + node_len
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let (nibbles, buf) = PackedStoredNibblesSubKey::from_compact(buf, 33);
        let (node, buf) = BranchNodeCompact::from_compact(buf, len - 33);
        (Self { nibbles, node }, buf)
    }
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(PackedStorageTrieEntry);
