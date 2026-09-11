use super::{BackingStore, CacheStats, Leaf, accounting, cache::Cache, leaf::Columns};
use crate::codec::FormatTag;
use crate::{codec::parquet::LeafMetadata, error::bail, *};
use std::sync::Arc;

/// Budgets count retained decoded data and estimated cache overhead, not process
/// RSS. Zero disables retention. Oversized values are served without admission.
#[derive(Debug, Clone)]
pub struct RepositoryOptions {
    pub metadata_cache_bytes: usize,
    pub column_cache_bytes: usize,
    /// Hash the complete leaf on metadata admission. Immutable objects need no
    /// repeated verification on cache hits. Metadata objects are always hashed.
    pub verify_leaves: bool,
}
impl Default for RepositoryOptions {
    fn default() -> Self {
        Self {
            metadata_cache_bytes: 16 * 1024 * 1024,
            column_cache_bytes: 128 * 1024 * 1024,
            verify_leaves: false,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct RepositoryStats {
    pub metadata: CacheStats,
    pub columns: CacheStats,
}

// Private decoded cache values share one metadata budget; all external reads are
// typed. Format tags are only codec/cache details, never part of BackingStore.
pub(super) enum Metadata {
    Root(Arc<Root>),
    Transaction(Arc<Transaction>),
    Table(Arc<Table>),
    Schema(Arc<TableSchema>),
    Internal(Arc<InternalNode>),
    Leaf(Arc<LeafMetadata>),
}
/// Shared access to immutable content. Its lifetime is independent of snapshots.
pub struct Repository {
    store: Arc<dyn BackingStore>,
    options: RepositoryOptions,
    metadata: Cache<(Id, u8), Metadata>,
    column_data: Arc<Columns>,
}
impl Repository {
    pub fn new(store: impl BackingStore + 'static) -> Self {
        Self::with_options(store, RepositoryOptions::default())
    }
    pub fn with_options(store: impl BackingStore + 'static, options: RepositoryOptions) -> Self {
        let store: Arc<dyn BackingStore> = Arc::new(store);
        Self {
            column_data: Arc::new(Columns::new(store.clone(), options.column_cache_bytes)),
            metadata: Cache::new(options.metadata_cache_bytes, accounting::metadata),
            store,
            options,
        }
    }
    pub fn stats(&self) -> Result<RepositoryStats> {
        Ok(RepositoryStats {
            metadata: self.metadata.stats()?,
            columns: self.column_data.stats()?,
        })
    }
    fn load(
        &self,
        kind: FormatTag,
        id: Id,
        decode: impl FnOnce(&[u8]) -> Result<Metadata>,
    ) -> Result<Arc<Metadata>> {
        self.metadata.get_or_load((id, kind as u8), || {
            let bytes = self.store.get(&id)?.read_all()?;
            if codec::object_id(kind, &bytes) != id {
                return Err(BeechError::HashMismatch(id));
            }
            decode(&bytes)
        })
    }
    pub fn get_root(&self, id: &Id) -> Result<Arc<Root>> {
        let value = self.load(FormatTag::Root, *id, |b| {
            Ok(Metadata::Root(Arc::new(codec::thrift::decode_root(b)?)))
        })?;
        let Metadata::Root(root) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        Ok(root.clone())
    }
    pub fn get_transaction(&self, id: &Id) -> Result<Arc<Transaction>> {
        let value = self.load(FormatTag::Transaction, *id, |b| {
            Ok(Metadata::Transaction(Arc::new(
                codec::thrift::decode_transaction(b)?,
            )))
        })?;
        let Metadata::Transaction(txn) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        Ok(txn.clone())
    }
    pub fn get_schema(&self, id: &Id) -> Result<Arc<TableSchema>> {
        let value = self.load(FormatTag::Schema, *id, |b| {
            Ok(Metadata::Schema(Arc::new(codec::thrift::decode_schema(b)?)))
        })?;
        let Metadata::Schema(schema) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        Ok(schema.clone())
    }
    pub fn table_by_id(&self, id: &Id) -> Result<Arc<Table>> {
        let value = self.load(FormatTag::Table, *id, |b| {
            Ok(Metadata::Table(Arc::new(codec::thrift::decode_table(b)?)))
        })?;
        let Metadata::Table(table) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        Ok(table.clone())
    }
    pub fn get_table(&self, transaction: &Transaction, name: &str) -> Result<Arc<Table>> {
        let id = transaction.tables.get(name).ok_or_else(|| BeechError::NoSuchTable(name.into()))?;
        let table = self.table_by_id(id)?;
        if table.name != name {
            bail!(
                Wire,
                "table {id}: name {:?} does not match directory name {name:?}",
                table.name
            );
        }
        Ok(table)
    }
    pub fn snapshot(self: &Arc<Self>, root_id: Id) -> Result<Snapshot> {
        let root = self.get_root(&root_id)?;
        let transaction = self.get_transaction(&root.transaction_id())?;
        Ok(Snapshot {
            repository: self.clone(),
            root_id,
            transaction,
        })
    }
}
impl NodeSource for Repository {
    fn get_internal(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Arc<InternalNode>> {
        reference.validate(schema)?;
        if reference.height == 0 {
            bail!(
                InvalidNode,
                "node {}: expected internal reference, found height 0",
                reference.id
            );
        }
        let value = self
            .load(FormatTag::Internal, reference.id, |bytes| {
                Ok(Metadata::Internal(Arc::new(codec::thrift::decode_internal(
                    bytes, schema,
                )?)))
            })
            .map_err(|error| error.with_node_context(reference.id))?;
        let Metadata::Internal(node) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        node.validate(schema).map_err(|error| error.with_node_context(reference.id))?;
        if node.height != reference.height {
            bail!(
                InvalidNode,
                "node {}: internal height {} does not match reference height {}",
                reference.id,
                node.height,
                reference.height
            );
        }
        let count = node.row_count().map_err(|error| error.with_node_context(reference.id))?;
        if count != reference.row_count {
            bail!(
                InvalidNode,
                "node {}: internal row count {count} does not match reference row count {}",
                reference.id,
                reference.row_count
            );
        }
        let max = &node.children.last().expect("validated fanout").max_key;
        if max != &reference.max_key {
            bail!(
                InvalidNode,
                "node {}: internal maximum key {max:?} does not match reference maximum key {:?}",
                reference.id,
                reference.max_key
            );
        }
        Ok(node.clone())
    }
    fn open_leaf(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Leaf> {
        reference.validate(schema)?;
        if reference.height != 0 {
            bail!(
                InvalidNode,
                "node {}: expected leaf reference with height 0, found height {}",
                reference.id,
                reference.height
            );
        }
        let value = self.metadata.get_or_load((reference.id, FormatTag::Leaf as u8), || {
            let file = self.store.get(&reference.id)?;
            if self.options.verify_leaves
                && codec::object_id(FormatTag::Leaf, &file.read_all()?) != reference.id
            {
                return Err(BeechError::HashMismatch(reference.id));
            }
            let metadata = LeafMetadata::load(file, reference, schema)?;
            Ok(Metadata::Leaf(Arc::new(metadata)))
        })?;
        let Metadata::Leaf(metadata) = value.as_ref() else {
            unreachable!("typed metadata cache key")
        };
        metadata.validate(reference, schema)?;
        Ok(Leaf {
            id: reference.id,
            metadata: metadata.clone(),
            columns: self.column_data.clone(),
        })
    }
}

/// A selected immutable root and transaction sharing the repository's caches.
#[derive(Clone)]
pub struct Snapshot {
    repository: Arc<Repository>,
    root_id: Id,
    transaction: Arc<Transaction>,
}
impl Snapshot {
    pub fn root_id(&self) -> Id {
        self.root_id
    }
    pub fn transaction(&self) -> &Arc<Transaction> {
        &self.transaction
    }
    pub fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.repository.get_table(&self.transaction, name)
    }
}
impl NodeSource for Snapshot {
    fn get_internal(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Arc<InternalNode>> {
        self.repository.get_internal(reference, schema)
    }
    fn open_leaf(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Leaf> {
        self.repository.open_leaf(reference, schema)
    }
}
