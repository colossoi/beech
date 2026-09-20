use crate::transaction::records;
use crate::{
    transaction::{RefRecord, RowRecord, SortedChanges},
    tree::{self, Nodes},
    BuildOptions, ObjectSink, Transaction,
};
use beech_core::{
    query::RowCursor, BeechError, Key, KeyOrdering, NodeRef, NodeSource, Result, Scalar, Table,
};
use beech_disk::{SortLimits, Spool, Workspace};
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

/// Spool a batch, sort it by key on disk, and update affected paths. Repeated
/// keys are rejected. Abort publication on error; objects may already be staged.
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
struct Changes {
    reader: SortedChanges,
    head: Option<Change>,
}
impl Changes {
    fn new(reader: SortedChanges) -> Result<Self> {
        let mut changes = Self { reader, head: None };
        changes.head = changes.reader.next().transpose()?;
        Ok(changes)
    }
    fn within(&self, upper: Option<&Key>) -> Result<bool> {
        let Some(head) = &self.head else {
            return Ok(false);
        };
        Ok(match upper {
            Some(key) => head.key().compare_key(key)? != Ordering::Greater,
            None => true,
        })
    }
    fn take(&mut self) -> Result<Change> {
        let change = self.head.take().expect("checked change head");
        self.head = self.reader.next().transpose()?;
        Ok(change)
    }
}
pub(crate) fn apply_sorted(
    changes: SortedChanges,
    workspace: &Workspace,
    table: &Table,
    source: &impl NodeSource,
    sink: &mut impl ObjectSink,
    options: BuildOptions,
) -> Result<Table> {
    let mut editor = Editor {
        changes: Changes::new(changes)?,
        workspace,
        table,
        source,
        sink,
        options,
    };
    let level = match table.root() {
        Some(root) => editor.edit_node(root, None, true)?,
        None => editor.edit_leaf(None, None)?,
    };
    table.with_root(tree::finish_tree(
        editor.sink,
        table.schema(),
        level,
        options,
        workspace,
    )?)
}
struct Editor<'a, S, W> {
    changes: Changes,
    workspace: &'a Workspace,
    table: &'a Table,
    source: &'a S,
    sink: &'a mut W,
    options: BuildOptions,
}
impl<S: NodeSource, W: ObjectSink> Editor<'_, S, W> {
    fn edit_node(&mut self, reference: &NodeRef, upper: Option<&Key>, is_root: bool) -> Result<Nodes> {
        if !self.changes.within(upper)? {
            return tree::singleton(self.workspace, reference);
        }
        if reference.height() == 0 {
            return self.edit_leaf(Some(reference), upper);
        }
        let node = self.source.get_internal(reference, self.table.schema())?;
        let mut children = Nodes::new(self.workspace)?;
        let mut unchanged = true;
        for (index, child) in node.children().iter().enumerate() {
            // The rightmost path inherits the parent's bound, including appends.
            let bound = if index + 1 == node.children().len() { upper } else { Some(child.max_key()) };
            let mut replacement = self.edit_node(child, bound, false)?;
            unchanged &= replacement.len() == 1;
            for record in records(replacement.reader()?, RefRecord::read) {
                let record = record?;
                let reference = record.node(self.table.schema())?;
                unchanged &= &reference == child;
                children.append(|writer| RefRecord::new(&reference).write(writer))?;
            }
        }
        if unchanged {
            return tree::singleton(self.workspace, reference);
        }
        if is_root || children.is_empty() {
            return Ok(children);
        }
        tree::build_parents(
            self.sink,
            self.table.schema(),
            children,
            self.options,
            self.workspace,
        )
    }
    fn edit_leaf(&mut self, reference: Option<&NodeRef>, upper: Option<&Key>) -> Result<Nodes> {
        let table = self.table.with_root(reference.cloned())?;
        let mut rows = RowCursor::new(self.source, &table, vec![])?.peekable();
        let mut merged = Spool::new(self.workspace)?;
        let mut changed = false;
        loop {
            if matches!(rows.peek(), Some(Err(_))) {
                return Err(rows.next().unwrap().unwrap_err());
            }
            let has_change = self.changes.within(upper)?;
            let order = match (rows.peek(), has_change) {
                (None, false) => break,
                (Some(_), false) => Ordering::Less,
                (None, true) => Ordering::Greater,
                (Some(Ok(row)), true) => self
                    .table
                    .schema()
                    .key_from_row(row)?
                    .compare_key(self.changes.head.as_ref().unwrap().key())?,
                _ => unreachable!(),
            };
            if order == Ordering::Less {
                let row = RowRecord(rows.next().unwrap()?);
                merged.append(|writer| row.write(writer))?;
                continue;
            }
            let found = order == Ordering::Equal;
            let change = self.changes.take()?;
            match change {
                Change::Insert { key, row_id, record } => {
                    if found {
                        return Err(BeechError::Query(format!("duplicate key: {key:?}")));
                    }
                    changed = true;
                    merged.append(|writer| RowRecord((row_id, record)).write(writer))?;
                }
                Change::Update { key, row_id, record } => {
                    if !found {
                        return Err(BeechError::Query(format!("key not found: {key:?}")));
                    }
                    let row = (row_id, record);
                    changed |= rows.next().unwrap()? != row;
                    merged.append(|writer| RowRecord(row).write(writer))?;
                }
                Change::Delete { key } => {
                    if !found {
                        return Err(BeechError::Query(format!("key not found: {key:?}")));
                    }
                    rows.next().unwrap()?;
                    changed = true;
                }
            }
        }
        if !changed {
            if let Some(reference) = reference {
                return tree::singleton(self.workspace, reference);
            }
        }
        tree::build_leaves(
            self.sink,
            self.table.schema(),
            records(merged.reader()?, RowRecord::read).map(|r| Ok(r?.0)),
            self.options,
            self.workspace,
        )
    }
}
