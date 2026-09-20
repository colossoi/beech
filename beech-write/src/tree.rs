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
pub(crate) fn singleton(workspace: &Workspace, reference: &NodeRef) -> Result<Nodes> {
    let mut nodes = Nodes::new(workspace)?;
    nodes.append(|writer| RefRecord::new(reference).write(writer))?;
    Ok(nodes)
}
pub(crate) fn build_leaves(
    writer: &mut impl ObjectSink,
    schema: &TableSchema,
    rows: impl IntoIterator<Item = Result<Row>>,
    options: BuildOptions,
    workspace: &Workspace,
) -> Result<Nodes> {
    let mut level = Nodes::new(workspace)?;
    let mut shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
    let mut batch = vec![];
    let mut size = 0usize;
    for row in rows {
        let row = row?;
        let bytes = codec::thrift::encode_row_for_splitting(&row)?;
        size = size.saturating_add(bytes.len());
        let split = shaper.is_complete(&bytes) || size >= options.target_bytes.saturating_mul(4);
        batch.push(row);
        if split {
            let reference = RefRecord::new(&save_node(
                writer,
                codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &batch)?)?,
            )?);
            level.append(|out| reference.write(out))?;
            batch.clear();
            size = 0;
            shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
        }
    }
    if !batch.is_empty() {
        let reference = RefRecord::new(&save_node(
            writer,
            codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &batch)?)?,
        )?);
        level.append(|out| reference.write(out))?;
    }
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
    let mut pending = vec![];
    let mut group = vec![];
    let mut size = 0usize;
    let mut shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
    let mut emit = |children: Vec<NodeRef>| -> Result<()> {
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
    };
    for child in records(level.reader()?, RefRecord::read) {
        let child = child?.node(schema)?;
        let bytes = codec::thrift::encode_key(child.max_key())?;
        size = size.saturating_add(bytes.len());
        let split = shaper.is_complete(&bytes) || size >= options.target_bytes.saturating_mul(4);
        group.push(child);
        if split && group.len() >= 2 {
            if !pending.is_empty() {
                emit(std::mem::take(&mut pending))?;
            }
            pending = std::mem::take(&mut group);
            size = 0;
            shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
        }
    }
    // Hold one completed group so a final singleton can join it.
    if group.len() == 1 {
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
    Ok(output)
}
