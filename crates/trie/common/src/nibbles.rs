use alloc::vec::Vec;
use derive_more::Deref;
pub use nybbles::Nibbles;

/// The representation of nibbles of the merkle trie stored in the database.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Index)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test-utils", derive(arbitrary::Arbitrary))]
pub struct StoredNibbles(pub Nibbles);

impl From<Nibbles> for StoredNibbles {
    #[inline]
    fn from(value: Nibbles) -> Self {
        Self(value)
    }
}

impl From<Vec<u8>> for StoredNibbles {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self(Nibbles::from_nibbles_unchecked(value))
    }
}

#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for StoredNibbles {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        for i in self.0.iter() {
            buf.put_u8(i);
        }
        self.0.len()
    }

    fn from_compact(mut buf: &[u8], len: usize) -> (Self, &[u8]) {
        use bytes::Buf;

        let nibbles = &buf[..len];
        buf.advance(len);
        (Self(Nibbles::from_nibbles_unchecked(nibbles)), buf)
    }
}

/// The representation of nibbles of the merkle trie stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deref)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test-utils", derive(arbitrary::Arbitrary))]
pub struct StoredNibblesSubKey(pub Nibbles);

impl From<Nibbles> for StoredNibblesSubKey {
    #[inline]
    fn from(value: Nibbles) -> Self {
        Self(value)
    }
}

impl From<Vec<u8>> for StoredNibblesSubKey {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self(Nibbles::from_nibbles_unchecked(value))
    }
}

impl From<StoredNibblesSubKey> for Nibbles {
    #[inline]
    fn from(value: StoredNibblesSubKey) -> Self {
        value.0
    }
}

#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for StoredNibblesSubKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        let pack = self.0.pack();
        let encoded_len = self.len() + 1;
        buf.put_u8(encoded_len as u8);
        buf.put_slice(&pack);
        pack.len() + 1
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let encoded_len = buf[0];
        let odd = encoded_len.is_multiple_of(2);
        let pack_len = (encoded_len / 2) as usize;
        let mut nibbles = Nibbles::unpack(&buf[1..1 + pack_len]);
        if odd {
            nibbles.pop();
        }

        (Self(nibbles), &buf[pack_len + 1..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use reth_codecs::Compact;

    #[test]
    fn test_stored_nibbles_from_nibbles() {
        let nibbles = Nibbles::from_nibbles_unchecked(vec![0x02, 0x04, 0x06]);
        let stored = StoredNibbles::from(nibbles);
        assert_eq!(stored.0, nibbles);
    }

    #[test]
    fn test_stored_nibbles_from_vec() {
        let bytes = vec![0x02, 0x04, 0x06];
        let stored = StoredNibbles::from(bytes.clone());
        assert_eq!(stored.0.to_vec(), bytes);
    }

    #[test]
    fn test_stored_nibbles_to_compact() {
        let stored = StoredNibbles::from(vec![0x02, 0x04]);
        let mut buf = BytesMut::with_capacity(10);
        let len = stored.to_compact(&mut buf);
        assert_eq!(len, 2);
        assert_eq!(buf, &vec![0x02, 0x04][..]);
    }

    #[test]
    fn test_stored_nibbles_from_compact() {
        let buf = vec![0x02, 0x04, 0x06];
        let (stored, remaining) = StoredNibbles::from_compact(&buf, 2);
        assert_eq!(stored.0.to_vec(), vec![0x02, 0x04]);
        assert_eq!(remaining, &[0x06]);
    }

    #[test]
    fn test_stored_nibbles_subkey_to_compact() {
        let subkey = StoredNibblesSubKey::from(vec![0x00, 0x02, 0x00, 0x04]);
        let mut buf = BytesMut::with_capacity(33);
        let len = subkey.to_compact(&mut buf);
        assert_eq!(len, 3);
        assert_eq!(buf[1..3], [0x02, 0x04]);
        assert_eq!(buf[0], 5); // Length byte
    }

    #[test]
    fn test_stored_nibbles_subkey_from_compact() {
        let buf = vec![0x05, 0x02, 0x04, 0xff];
        let (subkey, remaining) = StoredNibblesSubKey::from_compact(&buf, 4);
        assert_eq!(subkey.0.to_vec(), vec![0x00, 0x02, 0x00, 0x04]);
        assert_eq!(remaining, &[0xff]);
    }

    #[test]
    fn test_serialization_stored_nibbles() {
        let stored = StoredNibbles::from(vec![0x02, 0x04]);
        let serialized = serde_json::to_string(&stored).unwrap();
        let deserialized: StoredNibbles = serde_json::from_str(&serialized).unwrap();
        assert_eq!(stored, deserialized);
    }

    #[test]
    fn test_serialization_stored_nibbles_subkey() {
        let subkey = StoredNibblesSubKey::from(vec![0x02, 0x04]);
        let serialized = serde_json::to_string(&subkey).unwrap();
        let deserialized: StoredNibblesSubKey = serde_json::from_str(&serialized).unwrap();
        assert_eq!(subkey, deserialized);
    }
}
