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
use beech_disk::{ExternalSort, PageStore, SortLimits, Workspace};
use std::{
    cmp::Ordering,
    io::{self, BufRead, Write},
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

struct OrderedChange {
    sequence: u64,
    change: Change,
}

type ChangeOrder = fn(&OrderedChange, &OrderedChange) -> Ordering;

/// Disk-backed mutations ordered by key at consumption time. Sequence numbers
/// retain submission order for repeated operations on the same key.
pub struct MutationBatch {
    sort: Option<ExternalSort<OrderedChange, ChangeOrder>>,
    next_sequence: u64,
}

impl MutationBatch {
    pub fn new(limits: SortLimits) -> Result<Self> {
        let workspace = Workspace::new()?;
        let compare: ChangeOrder = |left, right| {
            left.change
                .key()
                .compare_key(right.change.key())
                .expect("validated mutation keys")
                .then_with(|| left.sequence.cmp(&right.sequence))
        };
        Ok(Self {
            sort: Some(ExternalSort::new(
                &workspace,
                limits,
                compare,
                write_ordered_change,
                read_ordered_change,
                |change| change.change.memory_size() + std::mem::size_of::<u64>(),
            )),
            next_sequence: 0,
        })
    }

    pub fn push(&mut self, change: Change) -> Result<()> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| BeechError::Query("mutation sequence overflow".into()))?;
        self.sort
            .as_mut()
            .ok_or_else(|| BeechError::Query("mutation batch already consumed".into()))?
            .push(OrderedChange { sequence, change })?;
        Ok(())
    }

    pub fn apply(mut self, working: &mut WorkingTable) -> Result<()> {
        let mut sorted = self
            .sort
            .take()
            .ok_or_else(|| BeechError::Query("mutation batch already consumed".into()))?
            .finish()?;
        for change in sorted.reader()? {
            working.apply(change?.change)?;
        }
        Ok(())
    }
}

fn write_ordered_change(change: &OrderedChange, writer: &mut dyn Write) -> io::Result<()> {
    writer.write_all(&change.sequence.to_le_bytes())?;
    change.change.write(writer)
}

fn read_ordered_change(reader: &mut dyn BufRead) -> io::Result<Option<OrderedChange>> {
    let mut sequence = [0; 8];
    match reader.read(&mut sequence[..1])? {
        0 => return Ok(None),
        1 => reader.read_exact(&mut sequence[1..])?,
        _ => unreachable!("one-byte read returned more than one byte"),
    }
    let change = Change::read(reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing mutation"))?;
    Ok(Some(OrderedChange {
        sequence: u64::from_le_bytes(sequence),
        change,
    }))
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
pub(crate) struct Reference {
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

// Private working nodes; scratch encoding is independent of final object formats.
pub(crate) enum WorkingNode {
    Leaf(Vec<Row>),
    Branch(Vec<Reference>),
}
impl WorkingNode {
    #[cfg(test)]
    pub(crate) fn pages(workspace: &beech_disk::Workspace) -> io::Result<PageStore<Self>> {
        Self::pages_with_budget(workspace, 8 * 1024 * 1024)
    }
    pub(crate) fn pages_with_budget(
        workspace: &beech_disk::Workspace,
        budget: usize,
    ) -> io::Result<PageStore<Self>> {
        Ok(PageStore::new(workspace, Self::write, Self::read)?.with_cache(budget, Self::decoded_size))
    }
    fn decoded_size(&self) -> usize {
        fn key_bytes(key: &Vec<Scalar>) -> usize {
            key.capacity() * std::mem::size_of::<Scalar>()
                + key
                    .iter()
                    .map(|v| match v {
                        Scalar::Utf8(v) => v.capacity(),
                        Scalar::Binary(v) => v.capacity(),
                        _ => 0,
                    })
                    .sum::<usize>()
        }
        std::mem::size_of::<Self>()
            + match self {
                Self::Leaf(rows) => {
                    rows.capacity() * std::mem::size_of::<Row>()
                        + rows.iter().map(|(_, row)| key_bytes(row)).sum::<usize>()
                }
                Self::Branch(children) => {
                    children.capacity() * std::mem::size_of::<Reference>()
                        + children.iter().map(|r| key_bytes(&r.key)).sum::<usize>()
                }
            }
    }
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            Self::Leaf(rows) => {
                writer.write_all(&[0])?;
                for row in rows {
                    RowRecord(row.clone()).write(writer)?;
                }
            }
            Self::Branch(children) => {
                writer.write_all(&[1])?;
                for child in children {
                    child.write(writer)?;
                }
            }
        }
        Ok(())
    }
    fn read(reader: &mut dyn BufRead) -> io::Result<Self> {
        let mut tag = [0];
        reader.read_exact(&mut tag)?;
        match tag[0] {
            0 => Ok(Self::Leaf(
                records(reader, RowRecord::read).map(|r| r.map(|r| r.0)).collect::<io::Result<_>>()?,
            )),
            1 => Ok(Self::Branch(
                records(reader, Reference::read).collect::<io::Result<_>>()?,
            )),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid working node")),
        }
    }
}

/// A queryable, private table view backed by a decoded LRU and temporary key-value store
/// used for incremental updates. Mutations are visible to scans immediately;
/// dropping the value rolls them back.
pub struct WorkingTable {
    tree: WorkingTree<std::sync::Arc<dyn NodeSource>>,
    root: Option<Reference>,
    max_row_id: i64,
}

/// Cursor state for an ordered scan of a [`WorkingTable`].
pub struct WorkingScan {
    pending: Vec<Reference>,
    rows: std::vec::IntoIter<Row>,
}

impl WorkingTable {
    pub fn new(
        table: Table,
        source: std::sync::Arc<dyn NodeSource>,
        options: BuildOptions,
    ) -> Result<Self> {
        Self::with_decoded_budget(table, source, options, 8 * 1024 * 1024)
    }
    /// Budget retained decoded allocations; zero disables caching. Oversized nodes spill directly.
    pub fn with_decoded_budget(
        table: Table,
        source: std::sync::Arc<dyn NodeSource>,
        options: BuildOptions,
        budget: usize,
    ) -> Result<Self> {
        let workspace = beech_disk::Workspace::new()?;
        let root = table.root().map(Reference::stored);
        let max_row_id = table.max_row_id();
        Ok(Self {
            tree: WorkingTree {
                pages: WorkingNode::pages_with_budget(&workspace, budget)?,
                table,
                source,
                options,
                next_id: 0,
                stats: TransactionStats::default(),
            },
            root,
            max_row_id,
        })
    }

    pub fn table(&self) -> &Table {
        &self.tree.table
    }

    pub fn next_row_id(&self) -> Result<i64> {
        self.max_row_id.checked_add(1).ok_or_else(|| BeechError::Query("rowid space exhausted".into()))
    }

    pub fn page_stats(&self) -> beech_disk::PageStats {
        self.tree.pages.stats()
    }

    pub fn apply(&mut self, change: Change) -> Result<()> {
        self.tree.stats.operations += 1;
        if let Change::Insert { row_id, .. } | Change::Update { row_id, .. } = &change {
            self.max_row_id = self.max_row_id.max(*row_id);
        }
        let (mut level, changed) = self.tree.edit(self.root.as_ref(), change, true)?;
        if changed {
            while level.len() > 1 {
                level = self.tree.branches(level, None)?;
            }
            self.root = level.pop();
        }
        Ok(())
    }

    pub fn scan(&self) -> WorkingScan {
        WorkingScan {
            pending: self.root.clone().into_iter().collect(),
            rows: Vec::new().into_iter(),
        }
    }

    pub fn next_row(&mut self, scan: &mut WorkingScan) -> Result<Option<Row>> {
        loop {
            if let Some(row) = scan.rows.next() {
                return Ok(Some(row));
            }
            let Some(reference) = scan.pending.pop() else {
                return Ok(None);
            };
            if reference.height == 0 {
                scan.rows = self.tree.rows(Some(&reference))?.into_iter();
            } else {
                let mut children = self.tree.children(&reference)?;
                children.reverse();
                scan.pending.extend(children);
            }
        }
    }

    pub fn row_by_id(&mut self, row_id: i64) -> Result<Option<Row>> {
        let mut scan = self.scan();
        while let Some(row) = self.next_row(&mut scan)? {
            if row.0 == row_id {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub fn row_by_key(&mut self, key: &[Scalar]) -> Result<Option<Row>> {
        let mut scan = self.scan();
        while let Some(row) = self.next_row(&mut scan)? {
            match self.tree.table.schema().key_from_row(&row)?.compare_key(&key.to_vec())? {
                Ordering::Equal => return Ok(Some(row)),
                Ordering::Greater => return Ok(None),
                Ordering::Less => (),
            }
        }
        Ok(None)
    }

    pub fn finish(&mut self, sink: &mut impl ObjectSink) -> Result<Table> {
        let root = self.root.as_ref().map(|root| self.tree.finalize(root, sink)).transpose()?;
        self.tree.table.clone().with_max_row_id(self.max_row_id).with_root(root)
    }
}

pub(crate) fn apply_ordered(
    changes: impl Iterator<Item = io::Result<Change>>,
    pages: PageStore<WorkingNode>,
    table: &Table,
    input_bytes: u64,
    source: &impl NodeSource,
    sink: &mut impl ObjectSink,
    options: BuildOptions,
) -> Result<(Table, TransactionStats)> {
    let started = std::time::Instant::now();

    let mut tree = WorkingTree {
        pages,
        table: table.clone(),
        source: BorrowedSource(source),
        options,
        next_id: 0,
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
    let pages = tree.pages.stats();
    tree.stats.peak_scratch_bytes = input_bytes + pages.peak_disk_bytes;
    tree.stats.scratch_bytes_written = pages.bytes_written;
    tree.stats.peak_decoded_bytes = pages.peak_cached_bytes;
    tree.stats.decoded_cache_hits = pages.cache_hits;
    tree.stats.decoded_cache_misses = pages.cache_misses;
    tree.stats.dirty_evictions = pages.dirty_evictions;
    tree.stats.final_height = root.as_ref().map(NodeRef::height);
    tree.stats.elapsed = started.elapsed();
    Ok((table.with_root(root)?, tree.stats))
}

struct BorrowedSource<'a>(&'a dyn NodeSource);
impl NodeSource for BorrowedSource<'_> {
    fn get_internal(
        &self,
        reference: &NodeRef,
        schema: &beech_core::TableSchema,
    ) -> Result<std::sync::Arc<InternalNode>> {
        self.0.get_internal(reference, schema)
    }

    fn open_leaf(
        &self,
        reference: &NodeRef,
        schema: &beech_core::TableSchema,
    ) -> Result<beech_core::storage::Leaf> {
        self.0.open_leaf(reference, schema)
    }
}

struct WorkingTree<S> {
    pages: PageStore<WorkingNode>,
    table: Table,
    source: S,
    options: BuildOptions,
    next_id: u64,
    stats: TransactionStats,
}
impl<S: NodeSource> WorkingTree<S> {
    fn remove(&mut self, reference: &Reference) -> Result<()> {
        if let Location::Working(id) = reference.location {
            self.pages.remove(id)?;
        }
        Ok(())
    }
    fn write_node(&mut self, id: u64, node: WorkingNode, leaf: bool) -> Result<()> {
        self.pages.write(id, node)?;
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
    fn rows(&mut self, reference: Option<&Reference>) -> Result<Vec<Row>> {
        let Some(reference) = reference else {
            return Ok(vec![]);
        };
        match reference.location {
            Location::Stored(_) => {
                let table = self.table.with_root(Some(reference.node(&self.table)?))?;
                let result = RowCursor::new(&self.source, &table, vec![])?.collect();

                result
            }
            Location::Working(id) => Ok(self.pages.read(id, |node| match node {
                WorkingNode::Leaf(rows) => Ok(rows.clone()),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected working leaf",
                )),
            })?),
        }
    }
    fn children(&mut self, reference: &Reference) -> Result<Vec<Reference>> {
        match reference.location {
            Location::Stored(_) => {
                let children = self
                    .source
                    .get_internal(&reference.node(&self.table)?, self.table.schema())?
                    .children()
                    .iter()
                    .map(Reference::stored)
                    .collect();

                Ok(children)
            }
            Location::Working(id) => Ok(self.pages.read(id, |node| match node {
                WorkingNode::Branch(children) => Ok(children.clone()),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected working branch",
                )),
            })?),
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
                self.write_node(id, WorkingNode::Leaf(rows), true)?;
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
                self.write_node(id, WorkingNode::Branch(children), false)?;
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
            return reference.node(&self.table);
        }
        let node = if reference.height == 0 {
            let schema = self.table.schema().clone();
            let rows = self.rows(Some(reference))?;
            codec::parquet::encode_leaf(&schema, &batch_from_rows(&schema, &rows)?)?
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
    use beech_disk::Workspace;

    #[test]
    fn working_table_scans_disk_backed_mutations_in_key_order() {
        let workspace = Workspace::new().unwrap();
        let schema = TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap();
        let table = Table::new("t", schema, None, -1).unwrap();
        let source = std::sync::Arc::new(Repository::new(FileStore::new(workspace.path())));
        let mut working = WorkingTable::new(table, source, BuildOptions::new(64, 8).unwrap()).unwrap();
        for value in (0..100).rev() {
            working
                .apply(Change::Insert {
                    key: vec![Scalar::Int64(value)],
                    row_id: value,
                    record: vec![Scalar::Int64(value)],
                })
                .unwrap();
        }
        working
            .apply(Change::Update {
                key: vec![Scalar::Int64(50)],
                row_id: 500,
                record: vec![Scalar::Int64(50)],
            })
            .unwrap();
        working
            .apply(Change::Delete {
                key: vec![Scalar::Int64(25)],
            })
            .unwrap();
        assert_eq!(
            working.row_by_id(500).unwrap().unwrap().1,
            vec![Scalar::Int64(50)]
        );
        assert!(working.row_by_id(25).unwrap().is_none());
        let mut scan = working.scan();
        let mut rows = Vec::new();
        while let Some(row) = working.next_row(&mut scan).unwrap() {
            rows.push(row);
        }
        assert_eq!(rows.len(), 99);
        assert!(rows.windows(2).all(|rows| rows[0].1[0].compare(&rows[1].1[0]).unwrap().is_lt()));
        assert!(working.page_stats().cached_bytes > 0);
        assert_eq!(working.page_stats().bytes_written, 0);
    }

    #[test]
    fn repeated_edits_reuse_the_same_temporary_id() {
        let workspace = Workspace::new().unwrap();
        let schema = TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap();
        let table = Table::new("t", schema, None, 100).unwrap();
        let source = std::sync::Arc::new(Repository::new(FileStore::new(workspace.path())));
        let mut tree = WorkingTree {
            pages: WorkingNode::pages(&workspace).unwrap(),
            table,
            source,
            options: BuildOptions::new(100_000, 1).unwrap(),
            next_id: 0,
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
            assert_eq!(tree.pages.stats().bytes_written, 0);
            assert_eq!(tree.next_id, 1);
            assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 1);
        }
        tree.remove(&nodes[0]).unwrap();
    }
}
