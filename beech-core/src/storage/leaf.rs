use super::{BackingStore, accounting, cache::Cache};
use crate::{
    Id, RecordBatch, Result,
    codec::parquet::{self, LeafMetadata},
    error::{bail, beech_error},
    value::ColumnStatistics,
};
use arrow_array::{ArrayRef, RecordBatchOptions};
use arrow_schema::SchemaRef;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ColumnKey {
    leaf: Id,
    column: usize,
}
pub(super) struct Column {
    pub(super) array: ArrayRef,
}
pub(super) struct Columns {
    store: Arc<dyn BackingStore>,
    cache: Cache<ColumnKey, Column>,
}
impl Columns {
    pub(super) fn new(store: Arc<dyn BackingStore>, budget: usize) -> Self {
        Self {
            store,
            cache: Cache::new(budget, accounting::column),
        }
    }
    pub(super) fn stats(&self) -> Result<super::CacheStats> {
        self.cache.stats()
    }
    fn get(&self, id: Id, metadata: &LeafMetadata, column: usize) -> Result<ArrayRef> {
        let value = self.cache.get_or_load(ColumnKey { leaf: id, column }, || {
            let array = parquet::decode_column(self.store.get(&id)?, metadata, column)
                .map_err(|error| error.with_node_context(id))?;
            Ok(Column { array })
        })?;
        Ok(value.array.clone())
    }
}

/// An immutable leaf with exactly one Parquet row group. Opening it loads
/// metadata; reads share decoded columns. Each iterator owns its batch position.
#[derive(Clone)]
pub struct Leaf {
    pub(super) id: Id,
    pub(super) metadata: Arc<LeafMetadata>,
    pub(super) columns: Arc<Columns>,
}
impl Leaf {
    pub fn id(&self) -> Id {
        self.id
    }
    pub(crate) fn row_count(&self) -> u64 {
        self.metadata.row_count()
    }
    pub(crate) fn statistics(&self, column: usize) -> &ColumnStatistics {
        self.metadata.statistics(column)
    }
    /// Physical indexes include row ID at 0. Columns are returned in physical
    /// order, with duplicates removed. No column data is read until iteration.
    pub fn read(&self, physical_columns: &[usize], batch_size: usize) -> Result<LeafBatches> {
        if batch_size == 0 {
            bail!(Query, "leaf {}: batch size must be positive", self.id);
        }
        let schema = self.metadata.schema();
        let columns =
            physical_columns.iter().copied().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
        for &column in &columns {
            if column >= schema.fields().len() {
                bail!(
                    Query,
                    "leaf {}: column {column} is outside 0..{}",
                    self.id,
                    schema.fields().len()
                );
            }
        }
        let schema = Arc::new(schema.project(&columns)?);
        Ok(LeafBatches {
            leaf: self.clone(),
            columns,
            batch_size,
            schema,
            current: None,
            position: 0,
            finished: false,
        })
    }
}
/// Projected batches sharing cached Arrow buffers for a single leaf.
pub struct LeafBatches {
    leaf: Leaf,
    columns: Vec<usize>,
    batch_size: usize,
    schema: SchemaRef,
    current: Option<RecordBatch>,
    position: usize,
    finished: bool,
}
impl LeafBatches {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.current.is_none() {
            let count = usize::try_from(self.leaf.row_count()).map_err(|_| {
                beech_error!(
                    InvalidNode,
                    "leaf {}: row count exceeds addressable memory",
                    self.leaf.id
                )
            })?;
            let arrays = self
                .columns
                .iter()
                .map(|&column| self.leaf.columns.get(self.leaf.id, &self.leaf.metadata, column))
                .collect::<Result<Vec<_>>>()?;
            self.current = Some(RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(count)),
            )?);
        }
        let batch = self.current.as_ref().expect("decoded leaf");
        if self.position == batch.num_rows() {
            return Ok(None);
        }
        let count = self.batch_size.min(batch.num_rows() - self.position);
        let result = batch.slice(self.position, count);
        self.position += count;
        Ok(Some(result))
    }
}
impl Iterator for LeafBatches {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.next_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            result => {
                self.finished = true;
                self.current = None;
                match result {
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                    Ok(Some(_)) => unreachable!(),
                }
            }
        }
    }
}
impl std::iter::FusedIterator for LeafBatches {}
