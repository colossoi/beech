//! Canonical Thrift metadata and Parquet leaf encoding.
use crate::{Id, NodeRef};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub mod parquet;
#[cfg(test)]
mod tests;
pub mod thrift;

const FORMAT_VERSION: u8 = 1;

// Private identifiers used by the codecs and content hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FormatTag {
    Leaf = 1,
    Internal = 2,
    Schema = 3,
    Table = 4,
    Transaction = 5,
    Root = 6,
    Key = 8,
}

pub(crate) fn object_id(kind: FormatTag, bytes: &[u8]) -> Id {
    let mut h = Sha256::new();
    h.update(b"beech.object\0");
    h.update([FORMAT_VERSION, kind as u8]);
    h.update(bytes);
    Id::from(<[u8; 32]>::from(h.finalize()))
}

/// Canonical metadata bytes and their content identity, produced by a typed encoder.
#[derive(Debug, Clone)]
pub struct EncodedObject {
    id: Id,
    bytes: Bytes,
}
impl EncodedObject {
    fn new(kind: FormatTag, bytes: Vec<u8>) -> Self {
        Self {
            id: object_id(kind, &bytes),
            bytes: bytes.into(),
        }
    }
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

#[derive(Debug, Clone)]
pub struct EncodedNode {
    pub(crate) reference: NodeRef,
    pub(crate) bytes: Bytes,
}
impl EncodedNode {
    pub fn reference(&self) -> &NodeRef {
        &self.reference
    }
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}
