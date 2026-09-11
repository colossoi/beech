use crate::{tree::build_tree, BuildOptions, ObjectSink};
use beech_core::{
    query::RowCursor, BeechError, Key, KeyOrdering, NodeRef, NodeSource, Result, Scalar, Table,
};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Change {
    Insert {
        key: Key,
        row_id: i64,
        record: Vec<Scalar>,
    },
    Update {
        key: Key,
        row_id: i64,
        record: Vec<Scalar>,
    },
    Delete {
        key: Key,
    },
}
impl Change {
    pub fn key(&self) -> &Key {
        match self {
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Apply strictly key-sorted, unique changes, rebuilding through the Parquet
/// writer. Empty changes reuse the old root. This is a full rebuild, not COW.
pub fn rebuild_with_changes<I, NS: NodeSource, W: ObjectSink>(
    changes: I,
    table: &Table,
    source: &NS,
    writer: &mut W,
    options: BuildOptions,
) -> Result<Option<NodeRef>>
where
    I: IntoIterator<Item = Change>,
{
    let changes: Vec<_> = changes.into_iter().collect();
    for change in &changes {
        table.schema().validate_key(change.key(), false)?;
        if let Change::Insert { key, row_id, record } | Change::Update { key, row_id, record } = change {
            if table.schema().key_from_row(&(*row_id, record.clone()))?.compare_key(key)? != Ordering::Equal
            {
                return Err(BeechError::Query("change key does not match row".into()));
            }
        }
    }
    for pair in changes.windows(2) {
        if pair[0].key().compare_key(pair[1].key())? != Ordering::Less {
            return Err(BeechError::Query(
                "changes must have strictly increasing unique keys".into(),
            ));
        }
    }
    if changes.is_empty() {
        return Ok(table.root().cloned());
    }
    let mut existing =
        RowCursor::new(source, table, vec![])?.collect::<Result<Vec<_>>>()?.into_iter().peekable();
    let mut merged = vec![];
    for change in changes {
        while let Some(row) = existing.peek() {
            if table.schema().key_from_row(row)?.compare_key(change.key())? != Ordering::Less {
                break;
            }
            merged.push(existing.next().unwrap());
        }
        let found = match existing.peek() {
            Some(row) => table.schema().key_from_row(row)?.compare_key(change.key())? == Ordering::Equal,
            None => false,
        };
        match change {
            Change::Insert { key, row_id, record } => {
                if found {
                    return Err(BeechError::Query(format!("duplicate key: {key:?}")));
                }
                merged.push((row_id, record));
            }
            Change::Update { key, row_id, record } => {
                if !found {
                    return Err(BeechError::Query(format!("key not found: {key:?}")));
                }
                existing.next();
                merged.push((row_id, record));
            }
            Change::Delete { key } => {
                if !found {
                    return Err(BeechError::Query(format!("key not found: {key:?}")));
                }
                existing.next();
            }
        }
    }
    merged.extend(existing);
    build_tree(writer, table.schema(), merged, options)
}
