use crate::tree::shape;
use crate::{
    batch_from_rows,
    transaction::{read_frame, records, write_frame, RowRecord},
    BuildOptions, ObjectSink, Transaction, TransactionStats,
};
use beech_core::{
    codec::{
        self,
        thrift::{decode_key, encode_key},
    },
    query::RowCursor,
    BeechError, Id, InternalNode, Key, KeyOrdering, NodeRef, NodeSource, Result, Row, Scalar, Table,
};
use beech_disk::{SortLimits, Workspace};
use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
};

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

/// Spool mutations and apply them in submission order to a private working tree.
/// Repeated keys see earlier edits. Abort publication on error; finalization may
/// already have staged objects.
pub fn apply_changes(
    changes: impl IntoIterator<Item = Change>,
    table: &Table,
    source: &impl NodeSource,
    sink: &mut impl ObjectSink,
    options: BuildOptions,
) -> Result<Table> {
    let mut transaction = Transaction::new(table.schema().clone(), SortLimits::default())?;
    for change in changes {
        transaction.push(change)?;
    }
    transaction.apply(table, source, sink, options)
}
// Temporary identities are private to this processor; only final content IDs
// leave it. References carry the same routing metadata for either location.
#[derive(Clone)]
enum Location {
    Stored(Id),
    Working(u64),
}
#[derive(Clone)]
struct Reference {
    location: Location,
    height: u32,
    count: u64,
    key: Key,
}
impl Reference {
    fn stored(node: &NodeRef) -> Self {
        Self {
            location: Location::Stored(node.id()),
            height: node.height(),
            count: node.row_count(),
            key: node.max_key().clone(),
        }
    }
    fn node(&self, table: &Table) -> Result<NodeRef> {
        let Location::Stored(id) = self.location else {
            return Err(BeechError::InvalidNode(
                "temporary reference cannot be published".into(),
            ));
        };
        NodeRef::new(table.schema(), id, self.height, self.count, self.key.clone())
    }
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        let location = match self.location {
            Location::Stored(id) => Scalar::Binary(id.as_bytes().to_vec()),
            Location::Working(id) => Scalar::UInt64(id),
        };
        let mut values = vec![
            location,
            Scalar::UInt64(self.height.into()),
            Scalar::UInt64(self.count),
        ];
        values.extend(self.key.iter().cloned());
        write_frame(writer, &encode_key(&values).map_err(io::Error::other)?)
    }
    fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
        let Some(bytes) = read_frame(reader)? else {
            return Ok(None);
        };
        let mut values = decode_key(&bytes).map_err(io::Error::other)?.into_iter();
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid working reference");
        let location = match values.next() {
            Some(Scalar::Binary(id)) => Location::Stored(Id::from_slice(&id).map_err(io::Error::other)?),
            Some(Scalar::UInt64(id)) => Location::Working(id),
            _ => return Err(invalid()),
        };
        let (Some(Scalar::UInt64(height)), Some(Scalar::UInt64(count))) = (values.next(), values.next())
        else {
            return Err(invalid());
        };
        Ok(Some(Self {
            location,
            height: height.try_into().map_err(|_| invalid())?,
            count,
            key: values.collect(),
        }))
    }
}

pub(crate) fn apply_ordered(
    changes: impl Iterator<Item = io::Result<Change>>,
    workspace: &Workspace,
    table: &Table,
    input_bytes: u64,
    source: &impl NodeSource,
    sink: &mut impl ObjectSink,
    options: BuildOptions,
) -> Result<(Table, TransactionStats)> {
    let started = std::time::Instant::now();
    let mut tree = WorkingTree {
        workspace,
        table,
        source,
        options,
        next_id: 0,
        scratch_bytes: input_bytes,
        stats: TransactionStats {
            input_bytes,
            peak_scratch_bytes: input_bytes,
            ..Default::default()
        },
    };
    let mut root = table.root().map(Reference::stored);
    for change in changes {
        tree.stats.operations += 1;
        let (mut level, changed) = tree.edit(root.as_ref(), change?, true)?;
        if changed {
            while level.len() > 1 {
                level = tree.branches(level, None)?;
            }
            root = level.pop();
        }
    }
    let root = root.as_ref().map(|r| tree.finalize(r, sink)).transpose()?;
    tree.stats.final_height = root.as_ref().map(NodeRef::height);
    tree.stats.elapsed = started.elapsed();
    Ok((table.with_root(root)?, tree.stats))
}

struct WorkingTree<'a, S> {
    workspace: &'a Workspace,
    table: &'a Table,
    source: &'a S,
    options: BuildOptions,
    next_id: u64,
    scratch_bytes: u64,
    stats: TransactionStats,
}
impl<S: NodeSource> WorkingTree<'_, S> {
    fn path(&self, id: u64) -> std::path::PathBuf {
        self.workspace.path().join(format!("node-{id}"))
    }
    fn remove(&mut self, reference: &Reference) -> Result<()> {
        if let Location::Working(id) = reference.location {
            let bytes = fs::metadata(self.path(id))?.len();
            fs::remove_file(self.path(id))?;
            self.scratch_bytes -= bytes;
        }
        Ok(())
    }
    fn writer(&mut self, id: u64) -> Result<BufWriter<File>> {
        let old = match fs::metadata(self.path(id)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let file = File::create(self.path(id))?;
        self.scratch_bytes -= old;
        Ok(BufWriter::new(file))
    }
    fn finish_write(&mut self, mut writer: BufWriter<File>, leaf: bool) -> Result<()> {
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        self.scratch_bytes += bytes;
        self.stats.peak_scratch_bytes = self.stats.peak_scratch_bytes.max(self.scratch_bytes);
        self.stats.scratch_bytes_written += bytes;
        if leaf {
            self.stats.leaf_writes += 1;
        } else {
            self.stats.branch_writes += 1;
        }
        Ok(())
    }
    fn reuse(reference: Option<&Reference>) -> Option<u64> {
        reference.and_then(|r| match r.location {
            Location::Working(id) => Some(id),
            _ => None,
        })
    }
    fn allocate(&mut self, reuse: &mut Option<u64>) -> Result<u64> {
        if let Some(id) = reuse.take() {
            return Ok(id);
        }
        let id = self.next_id;
        self.next_id =
            id.checked_add(1).ok_or_else(|| BeechError::InvalidNode("temporary ID overflow".into()))?;
        Ok(id)
    }
    fn rows(&self, reference: Option<&Reference>) -> Result<Vec<Row>> {
        let Some(reference) = reference else {
            return Ok(vec![]);
        };
        match reference.location {
            Location::Stored(_) => {
                let table = self.table.with_root(Some(reference.node(self.table)?))?;
                RowCursor::new(self.source, &table, vec![])?.collect()
            }
            Location::Working(id) => records(BufReader::new(File::open(self.path(id))?), RowRecord::read)
                .map(|r| Ok(r?.0))
                .collect(),
        }
    }
    fn children(&self, reference: &Reference) -> Result<Vec<Reference>> {
        match reference.location {
            Location::Stored(_) => Ok(self
                .source
                .get_internal(&reference.node(self.table)?, self.table.schema())?
                .children()
                .iter()
                .map(Reference::stored)
                .collect()),
            Location::Working(id) => records(BufReader::new(File::open(self.path(id))?), Reference::read)
                .map(|r| Ok(r?))
                .collect(),
        }
    }
    fn edit(
        &mut self,
        reference: Option<&Reference>,
        change: Change,
        is_root: bool,
    ) -> Result<(Vec<Reference>, bool)> {
        if reference.is_none_or(|r| r.height == 0) {
            return self.edit_leaf(reference, change);
        }
        let reference = reference.unwrap();
        self.stats.branch_visits += 1;
        let mut children = self.children(reference)?;
        let mut index = children.len() - 1;
        for (i, child) in children.iter().enumerate() {
            if child.key.compare_key(change.key())? != Ordering::Less {
                index = i;
                break;
            }
        }
        let (replacement, changed) = self.edit(Some(&children[index]), change, false)?;
        if !changed {
            return Ok((vec![reference.clone()], false));
        }
        children.splice(index..=index, replacement);
        // The updated root's children are already in memory. Only read another
        // node when a real collapse promotes it and may expose another singleton.
        if is_root && children.len() == 1 {
            self.remove(reference)?;
            self.stats.root_collapses += 1;
            let mut root = children.pop().unwrap();
            while root.height > 0 {
                let mut children = self.children(&root)?;
                if children.len() != 1 {
                    break;
                }
                self.remove(&root)?;
                self.stats.root_collapses += 1;
                root = children.pop().unwrap();
            }
            return Ok((vec![root], true));
        }
        if children.is_empty() {
            self.remove(reference)?;
        }
        let output = self.branches(children, Self::reuse(Some(reference)))?;
        self.stats.branch_splits += output.len().saturating_sub(1) as u64;
        Ok((output, true))
    }
    fn edit_leaf(
        &mut self,
        reference: Option<&Reference>,
        change: Change,
    ) -> Result<(Vec<Reference>, bool)> {
        self.stats.leaf_visits += u64::from(reference.is_some());
        let mut rows = self.rows(reference)?;
        let mut index = rows.len();
        let mut found = false;
        for (i, row) in rows.iter().enumerate() {
            let order = self.table.schema().key_from_row(row)?.compare_key(change.key())?;
            if order != Ordering::Less {
                index = i;
                found = order == Ordering::Equal;
                break;
            }
        }
        match change {
            Change::Insert { key, row_id, record } => {
                if found {
                    return Err(BeechError::Query(format!("duplicate key: {key:?}")));
                }
                rows.insert(index, (row_id, record));
            }
            Change::Update { key, row_id, record } => {
                if !found {
                    return Err(BeechError::Query(format!("key not found: {key:?}")));
                }
                let row = (row_id, record);
                if rows[index] == row {
                    self.stats.no_op_updates += 1;
                    return Ok((reference.cloned().into_iter().collect(), false));
                }
                rows[index] = row;
            }
            Change::Delete { key } => {
                if !found {
                    return Err(BeechError::Query(format!("key not found: {key:?}")));
                }
                rows.remove(index);
            }
        }
        if rows.is_empty() {
            if let Some(reference) = reference {
                self.remove(reference)?;
            }
        }
        let mut reuse = Self::reuse(reference);
        let mut output = vec![];
        shape(
            rows.into_iter().map(Ok),
            self.options,
            1,
            codec::thrift::encode_row_for_splitting,
            |rows| {
                let key = self.table.schema().key_from_row(rows.last().unwrap())?;
                let count = rows.len() as u64;
                let id = self.allocate(&mut reuse)?;
                let mut writer = self.writer(id)?;
                for row in rows {
                    RowRecord(row).write(&mut writer)?;
                }
                self.finish_write(writer, true)?;
                output.push(Reference {
                    location: Location::Working(id),
                    height: 0,
                    count,
                    key,
                });
                Ok(())
            },
        )?;
        self.stats.leaf_splits += output.len().saturating_sub(1) as u64;
        Ok((output, true))
    }
    fn branches(&mut self, children: Vec<Reference>, mut reuse: Option<u64>) -> Result<Vec<Reference>> {
        let mut output = vec![];
        shape(
            children.into_iter().map(Ok),
            self.options,
            2,
            |r| encode_key(&r.key),
            |children| {
                let height = children[0]
                    .height
                    .checked_add(1)
                    .ok_or_else(|| BeechError::InvalidNode("tree height overflow".into()))?;
                let count = children
                    .iter()
                    .try_fold(0u64, |n, r| n.checked_add(r.count))
                    .ok_or_else(|| BeechError::InvalidNode("row count overflow".into()))?;
                let key = children.last().unwrap().key.clone();
                let id = self.allocate(&mut reuse)?;
                let mut writer = self.writer(id)?;
                for child in children {
                    child.write(&mut writer)?;
                }
                self.finish_write(writer, false)?;
                output.push(Reference {
                    location: Location::Working(id),
                    height,
                    count,
                    key,
                });
                Ok(())
            },
        )?;
        Ok(output)
    }
    fn finalize(&mut self, reference: &Reference, sink: &mut impl ObjectSink) -> Result<NodeRef> {
        if let Location::Stored(_) = reference.location {
            return reference.node(self.table);
        }
        let node = if reference.height == 0 {
            codec::parquet::encode_leaf(
                self.table.schema(),
                &batch_from_rows(self.table.schema(), &self.rows(Some(reference))?)?,
            )?
        } else {
            let children = self
                .children(reference)?
                .iter()
                .map(|r| self.finalize(r, sink))
                .collect::<Result<Vec<_>>>()?;
            codec::thrift::encode_internal(
                &InternalNode::new(self.table.schema(), reference.height, children)?,
                self.table.schema(),
            )?
        };
        sink.put(node.reference().id(), node.bytes())?;
        self.stats.staged_bytes += node.bytes().len() as u64;
        if reference.height == 0 {
            self.stats.leaves_staged += 1;
        } else {
            self.stats.branches_staged += 1;
        }
        Ok(node.reference().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beech_core::{
        storage::{FileStore, Repository},
        DataType, Field, TableSchema,
    };

    #[test]
    fn repeated_edits_reuse_the_same_flushed_working_file() {
        let workspace = Workspace::new().unwrap();
        let schema = TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap();
        let table = Table::new("t", schema, None, 100).unwrap();
        let source = Repository::new(FileStore::new(workspace.path()));
        let mut tree = WorkingTree {
            workspace: &workspace,
            table: &table,
            source: &source,
            options: BuildOptions::new(100_000, 1).unwrap(),
            next_id: 0,
            scratch_bytes: 0,
            stats: TransactionStats::default(),
        };
        let key = vec![Scalar::Int64(1)];
        let (mut nodes, _) = tree
            .edit(
                None,
                Change::Insert {
                    key: key.clone(),
                    row_id: 0,
                    record: key.clone(),
                },
                true,
            )
            .unwrap();
        let mut peak = fs::metadata(tree.path(0)).unwrap().len();
        for id in 1..100 {
            let (next, changed) = tree
                .edit(
                    Some(&nodes[0]),
                    Change::Update {
                        key: key.clone(),
                        row_id: id,
                        record: key.clone(),
                    },
                    true,
                )
                .unwrap();
            assert!(changed);
            nodes = next;
            assert_eq!(tree.rows(Some(&nodes[0])).unwrap(), vec![(id, key.clone())]);
            assert_eq!(tree.stats.leaf_writes, id as u64 + 1);
            assert_eq!(tree.stats.leaf_visits, id as u64);
            let bytes = fs::metadata(tree.path(0)).unwrap().len();
            assert_eq!(tree.scratch_bytes, bytes);
            peak = peak.max(bytes);
            assert_eq!(tree.stats.peak_scratch_bytes, peak);
            assert!(tree.stats.scratch_bytes_written > bytes);
            assert_eq!(tree.next_id, 1);
            assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 1);
        }
        tree.remove(&nodes[0]).unwrap();
        assert_eq!(tree.scratch_bytes, 0);
        assert_eq!(tree.stats.peak_scratch_bytes, peak);
    }
}
