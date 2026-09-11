//! Approximate byte weights used only for cache admission and eviction.
//!
//! Keep allocation formulas here, separate from loading, validation, and codecs.
//! These preserve the existing estimates: shared allocations are not deduplicated,
//! some inline headers are counted more than once, and container overhead is
//! heuristic. They exclude decoding temporaries and values retained by callers
//! after eviction, so the resulting budgets do not cap process memory.

use super::{leaf::Column, repository::Metadata};
use crate::{Id, InternalNode, Key, NodeRef, Root, Scalar, Table, TableSchema, Transaction};
use crate::{codec::parquet::LeafMetadata, value::ColumnStatistics};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::mem::size_of;

pub(super) fn column(value: &Column) -> usize {
    size_of::<Column>() + value.array.get_array_memory_size()
}

pub(super) fn metadata(value: &Metadata) -> usize {
    size_of::<Metadata>()
        + 4 * size_of::<usize>()
        + match value {
            Metadata::Root(_) => size_of::<Root>(),
            Metadata::Transaction(t) => {
                size_of::<Transaction>()
                    + t.tables
                        .keys()
                        .map(|name| size_of::<(String, Id)>() + name.capacity() + 4 * size_of::<usize>())
                        .sum::<usize>()
            }
            Metadata::Table(t) => {
                size_of::<Table>()
                    + t.name.capacity()
                    + schema_size(&t.schema)
                    + t.root.as_ref().map_or(0, reference_size)
            }
            Metadata::Schema(s) => schema_size(s),
            Metadata::Internal(n) => {
                size_of::<InternalNode>()
                    + schema_size(n.schema())
                    + n.children.capacity() * size_of::<NodeRef>()
                    + n.children.iter().map(|c| key_size(&c.max_key)).sum::<usize>()
            }
            Metadata::Leaf(m) => m.estimated_size(),
        }
}

/// Add the cache's key/entry layout and a bookkeeping overhead estimate.
pub(super) fn with_entry_overhead<K, E>(value_bytes: usize) -> usize {
    value_bytes.saturating_add(size_of::<(K, E)>()).saturating_add(3 * size_of::<usize>())
}

fn key_size(key: &Key) -> usize {
    key.capacity() * size_of::<Scalar>() + key.iter().map(scalar_allocation).sum::<usize>()
}

fn reference_size(reference: &NodeRef) -> usize {
    size_of::<NodeRef>() + key_size(&reference.max_key)
}

fn schema_size(schema: &TableSchema) -> usize {
    size_of::<TableSchema>()
        + schema.fields().size()
        + size_of::<arrow_schema::Schema>()
        + size_of_val(schema.key_columns())
}

pub(crate) fn leaf_metadata_size(
    value: &ArrowReaderMetadata,
    schema: &TableSchema,
    statistics: &[ColumnStatistics],
) -> usize {
    size_of::<LeafMetadata>()
        + schema_size(schema)
        + value.metadata().memory_size()
        + value.schema().fields().size()
        + size_of::<arrow_schema::Schema>()
        + value
            .schema()
            .metadata()
            .iter()
            .map(|(k, v)| 2 * size_of::<String>() + k.capacity() + v.capacity())
            .sum::<usize>()
        + size_of_val(statistics)
        + statistics
            .iter()
            .filter_map(|s| s.bounds.as_ref())
            .map(|(min, max)| scalar_allocation(min) + scalar_allocation(max))
            .sum::<usize>()
}

fn scalar_allocation(value: &Scalar) -> usize {
    match value {
        Scalar::Utf8(s) => s.capacity(),
        Scalar::Binary(b) => b.capacity(),
        _ => 0,
    }
}
