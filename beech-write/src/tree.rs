use crate::transaction::records;
use crate::{batch_from_rows, transaction::RefRecord, BuildOptions, Change, ObjectSink, Transaction};
use beech_core::{
    codec::{self, EncodedNode},
    BeechError, InternalNode, NodeRef, Result, Row, Table, TableSchema,
};
use beech_disk::{SortLimits, Spool, Workspace};
use beech_shaper::ProbShaper;

/// Spool and externally sort incoming rows, then stream the tree into the sink.
pub fn build_table<W: ObjectSink>(
    writer: &mut W,
    name: String,
    schema: TableSchema,
    rows: impl IntoIterator<Item = Row>,
    options: BuildOptions,
) -> Result<Table> {
    Table::new(name.clone(), schema.clone(), None, -1)?;
    let mut transaction = Transaction::new(schema.clone(), SortLimits::default())?;
    for row in rows {
        transaction.push(Change::Insert {
            key: schema.key_from_row(&row)?,
            row_id: row.0,
            record: row.1,
        })?;
    }
    transaction.build(name, writer, options)
}
fn save_node(writer: &mut impl ObjectSink, node: EncodedNode) -> Result<NodeRef> {
    writer.put(node.reference().id(), node.bytes())?;
    Ok(node.reference().clone())
}
pub(crate) type Nodes = Spool;
pub(crate) fn build_leaves(
    writer: &mut impl ObjectSink,
    schema: &TableSchema,
    rows: impl IntoIterator<Item = Result<Row>>,
    options: BuildOptions,
    workspace: &Workspace,
) -> Result<Nodes> {
    let mut level = Nodes::new(workspace)?;
    shape(
        rows,
        options,
        1,
        codec::thrift::encode_row_for_splitting,
        |batch| {
            let reference = RefRecord::new(&save_node(
                writer,
                codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &batch)?)?,
            )?);
            level.append(|out| reference.write(out))?;
            Ok(())
        },
    )?;
    Ok(level)
}
pub(crate) fn finish_tree(
    writer: &mut impl ObjectSink,
    schema: &TableSchema,
    mut level: Nodes,
    options: BuildOptions,
    workspace: &Workspace,
) -> Result<Option<NodeRef>> {
    while level.len() > 1 {
        level = build_parents(writer, schema, level, options, workspace)?;
    }
    records(level.reader()?, RefRecord::read).next().map(|r| r?.node(schema)).transpose()
}
pub(crate) fn build_parents(
    writer: &mut impl ObjectSink,
    schema: &TableSchema,
    mut level: Nodes,
    options: BuildOptions,
    workspace: &Workspace,
) -> Result<Nodes> {
    let mut output = Nodes::new(workspace)?;
    let children = records(level.reader()?, RefRecord::read).map(|r| r?.node(schema));
    shape(
        children,
        options,
        2,
        |child| codec::thrift::encode_key(child.max_key()),
        |children| {
            let height = children[0]
                .height()
                .checked_add(1)
                .ok_or_else(|| BeechError::InvalidNode("tree height overflow".into()))?;
            let node = save_node(
                writer,
                codec::thrift::encode_internal(&InternalNode::new(schema, height, children)?, schema)?,
            )?;
            output.append(|writer| RefRecord::new(&node).write(writer))?;
            Ok(())
        },
    )?;
    Ok(output)
}

// Hold one complete group to absorb a final undersized group. The two-child
// minimum for branches guarantees progress even with a one-byte target.
pub(crate) fn shape<T>(
    items: impl IntoIterator<Item = Result<T>>,
    options: BuildOptions,
    minimum: usize,
    bytes: impl Fn(&T) -> Result<Vec<u8>>,
    mut emit: impl FnMut(Vec<T>) -> Result<()>,
) -> Result<()> {
    let mut group = vec![];
    let mut pending = vec![];
    let mut size = 0usize;
    let mut shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
    for item in items {
        let item = item?;
        let bytes = bytes(&item)?;
        size = size.saturating_add(bytes.len());
        let split = shaper.is_complete(&bytes) || size >= options.target_bytes.saturating_mul(4);
        group.push(item);
        if split && group.len() >= minimum {
            if minimum == 1 {
                emit(std::mem::take(&mut group))?;
            } else {
                if !pending.is_empty() {
                    emit(std::mem::take(&mut pending))?;
                }
                pending = std::mem::take(&mut group);
            }
            size = 0;
            shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
        }
    }
    if group.len() < minimum {
        pending.extend(group);
    } else if !group.is_empty() {
        if !pending.is_empty() {
            emit(std::mem::take(&mut pending))?;
        }
        pending = group;
    }
    if !pending.is_empty() {
        emit(pending)?;
    }
    Ok(())
}
