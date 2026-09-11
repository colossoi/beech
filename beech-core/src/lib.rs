//! A content-addressed prolly tree with Thrift internal nodes and Parquet leaves.
#[cfg(test)]
extern crate self as beech_core;
use crate::error::{bail, beech_error};
pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field};
use std::{collections::BTreeMap, sync::Arc, time::SystemTime};
pub mod codec;
mod decimal;
#[cfg(test)]
mod decimal_tests;
mod error;
#[cfg(test)]
mod lib_tests;
pub mod plan;
#[cfg(test)]
mod plan_tests;
pub mod query;
#[cfg(test)]
mod query_tests;
mod schema;
pub mod storage;
#[cfg(test)]
mod test_support;
pub mod value;
pub use decimal::Decimal;
pub use error::{BeechError, Result};
pub use schema::TableSchema;
pub use value::{Key, KeyOrdering, Row, Scalar};

pub const ROW_ID_COLUMN: &str = "__beech_row_id";

/// A content address produced by an encoder or supplied by an object reference.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 32]);
impl Id {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn from_hex(text: &str) -> Result<Self> {
        if text.len() != 64 || !text.is_ascii() {
            return Err(BeechError::InvalidId);
        }
        let mut out = [0; 32];
        for (i, b) in out.iter_mut().enumerate() {
            *b = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).map_err(|_| BeechError::InvalidId)?;
        }
        Ok(Self(out))
    }
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Ok(Self(bytes.try_into().map_err(|_| BeechError::InvalidId)?))
    }
}
impl From<[u8; 32]> for Id {
    fn from(b: [u8; 32]) -> Self {
        Self(b)
    }
}
#[cfg(test)]
impl From<i64> for Id {
    fn from(i: i64) -> Self {
        let mut b = [0; 32];
        b[..8].copy_from_slice(&i.to_le_bytes());
        Self(b)
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}
impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// One inclusive upper fence per child, including the final child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRef {
    id: Id,
    height: u32,
    row_count: u64,
    max_key: Key,
}
impl NodeRef {
    /// Reconstruct a reference supplied by an external catalog.
    pub fn new(schema: &TableSchema, id: Id, height: u32, row_count: u64, max_key: Key) -> Result<Self> {
        let reference = Self {
            id,
            height,
            row_count,
            max_key,
        };
        reference.validate(schema)?;
        Ok(reference)
    }
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn height(&self) -> u32 {
        self.height
    }
    pub fn row_count(&self) -> u64 {
        self.row_count
    }
    pub fn max_key(&self) -> &Key {
        &self.max_key
    }
    pub(crate) fn validate(&self, schema: &TableSchema) -> Result<()> {
        if self.row_count == 0 {
            bail!(
                InvalidNode,
                "node {}: row count must be positive, found 0",
                self.id
            );
        }
        schema.validate_key(&self.max_key, false).map_err(|error| error.with_node_context(self.id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
    schema: TableSchema,
    height: u32,
    children: Vec<NodeRef>,
}
impl InternalNode {
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
    pub fn height(&self) -> u32 {
        self.height
    }
    pub fn children(&self) -> &[NodeRef] {
        &self.children
    }
    pub fn new(schema: &TableSchema, height: u32, children: Vec<NodeRef>) -> Result<Self> {
        let node = Self {
            schema: schema.clone(),
            height,
            children,
        };
        node.validate(schema)?;
        Ok(node)
    }
    pub(crate) fn validate(&self, schema: &TableSchema) -> Result<()> {
        if &self.schema != schema {
            bail!(
                InvalidNode,
                "internal schema {:?} does not match expected schema {schema:?}",
                self.schema
            );
        }
        if self.height == 0 {
            bail!(InvalidNode, "internal height must be positive, found 0");
        }
        if self.children.is_empty() {
            bail!(
                InvalidNode,
                "internal node has no children at height {}",
                self.height
            );
        }
        for (index, child) in self.children.iter().enumerate() {
            child.validate(schema)?;
            if child.height != self.height - 1 {
                bail!(
                    InvalidNode,
                    "child {index} (node {}): height {} does not match expected {} for parent height {}",
                    child.id,
                    child.height,
                    self.height - 1,
                    self.height
                );
            }
        }
        for (index, pair) in self.children.windows(2).enumerate() {
            if pair[0].max_key.compare_key(&pair[1].max_key)? != std::cmp::Ordering::Less {
                bail!(
                    InvalidNode,
                    "child {index} (node {}) maximum key is not less than child {} (node {}) maximum key",
                    pair[0].id,
                    index + 1,
                    pair[1].id
                );
            }
        }
        self.row_count()?;
        Ok(())
    }
    pub fn row_count(&self) -> Result<u64> {
        let total = self.children.iter().enumerate().try_fold(0u64, |total, (index, child)| {
            total.checked_add(child.row_count).ok_or_else(|| {
                beech_error!(
                    InvalidNode,
                    "row count overflow adding child {index} (node {}): {total} + {} exceeds {}",
                    child.id,
                    child.row_count,
                    u64::MAX
                )
            })
        })?;
        Ok(total)
    }
    pub fn seek(&self, schema: &TableSchema, key: &Key) -> Result<Option<usize>> {
        self.validate(schema)?;
        schema.validate_key(key, false)?;
        let mut lo = 0;
        let mut hi = self.children.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.children[mid].max_key.compare_key(key)? == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok((lo < self.children.len()).then_some(lo))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    name: String,
    schema: TableSchema,
    root: Option<NodeRef>,
}
impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
    pub fn root(&self) -> Option<&NodeRef> {
        self.root.as_ref()
    }
    pub fn new(name: impl Into<String>, schema: TableSchema, root: Option<NodeRef>) -> Result<Self> {
        let table = Self {
            name: name.into(),
            schema,
            root,
        };
        table.validate()?;
        Ok(table)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            bail!(Schema, "table name is empty");
        }
        if let Some(root) = &self.root {
            root.validate(&self.schema)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Root {
    id: Id,
}
impl Root {
    pub fn new(transaction_id: Id) -> Self {
        Self { id: transaction_id }
    }
    /// The referenced transaction's ID, not the encoded root object's ID.
    pub fn transaction_id(&self) -> Id {
        self.id
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    prev_id: Id,
    transaction_time: SystemTime,
    tables: BTreeMap<String, Id>,
}
impl Transaction {
    pub fn previous_id(&self) -> Id {
        self.prev_id
    }
    pub fn time(&self) -> SystemTime {
        self.transaction_time
    }
    pub fn tables(&self) -> &BTreeMap<String, Id> {
        &self.tables
    }
    pub fn new(prev_id: Id, transaction_time: SystemTime, tables: BTreeMap<String, Id>) -> Result<Self> {
        let txn = Self {
            prev_id,
            transaction_time,
            tables,
        };
        txn.validate()?;
        Ok(txn)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.tables.keys().any(|name| name.is_empty()) {
            bail!(Schema, "table directory contains an empty name");
        }
        Ok(())
    }
}

/// Metadata and leaf readers remain separate so navigation never decodes payload columns.
pub trait NodeSource {
    fn get_internal(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Arc<InternalNode>>;
    fn open_leaf(&self, reference: &NodeRef, schema: &TableSchema) -> Result<storage::Leaf>;
}
