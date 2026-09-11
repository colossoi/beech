use crate::{batch_from_rows, BuildOptions, ObjectSink};
use beech_core::{
    codec::{self, EncodedNode},
    BeechError, InternalNode, KeyOrdering, NodeRef, Result, Row, Table, TableSchema,
};
use beech_shaper::ProbShaper;
use std::cmp::Ordering;

/// Validate and sort logical rows, then stage the tree objects. Does not publish a snapshot.
pub fn build_table<W: ObjectSink>(
    writer: &mut W,
    name: String,
    schema: TableSchema,
    rows: Vec<Row>,
    options: BuildOptions,
) -> Result<Table> {
    // Validate the name before staging any data.
    Table::new(name.clone(), schema.clone(), None)?;
    let rows = sorted_rows(&schema, rows)?;
    let root = build_tree(writer, &schema, rows, options)?;
    Table::new(name, schema, root)
}

fn sorted_rows(schema: &TableSchema, rows: Vec<Row>) -> Result<Vec<Row>> {
    let mut keyed =
        rows.into_iter().map(|row| Ok((schema.key_from_row(&row)?, row))).collect::<Result<Vec<_>>>()?;
    // Every key has already been validated against exactly the same schema.
    keyed.sort_by(|a, b| a.0.compare_key(&b.0).expect("schema-validated keys"));
    for pair in keyed.windows(2) {
        if pair[0].0.compare_key(&pair[1].0)? == Ordering::Equal {
            return Err(BeechError::Query(format!("duplicate key: {:?}", pair[0].0)));
        }
    }
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

fn save_node<W: ObjectSink>(writer: &mut W, node: EncodedNode) -> Result<NodeRef> {
    writer.put(node.reference().id(), node.bytes())?;
    Ok(node.reference().clone())
}
pub(crate) fn build_tree<W: ObjectSink>(
    writer: &mut W,
    schema: &TableSchema,
    rows: Vec<Row>,
    options: BuildOptions,
) -> Result<Option<NodeRef>> {
    let mut level = vec![];
    let mut shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
    let mut batch = vec![];
    for row in rows {
        let split = shaper.is_complete(&codec::thrift::encode_row_for_splitting(&row)?);
        batch.push(row);
        if split {
            level.push(save_node(
                writer,
                codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &batch)?)?,
            )?);
            batch.clear();
            shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
        }
    }
    if !batch.is_empty() {
        level.push(save_node(
            writer,
            codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &batch)?)?,
        )?);
    }
    while level.len() > 1 {
        let mut groups = vec![];
        let mut children = vec![];
        let mut shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
        for child in level {
            let split = shaper.is_complete(&codec::thrift::encode_key(child.max_key())?);
            children.push(child);
            // Every parent must reduce the number of nodes, even with tiny targets.
            if split && children.len() >= 2 {
                groups.push(std::mem::take(&mut children));
                shaper = ProbShaper::new(options.target_bytes, options.stddev_bytes);
            }
        }
        if children.len() == 1 && !groups.is_empty() {
            groups.last_mut().unwrap().extend(children);
        } else if !children.is_empty() {
            groups.push(children);
        }
        level = groups
            .into_iter()
            .map(|children| {
                let height = children[0]
                    .height()
                    .checked_add(1)
                    .ok_or_else(|| BeechError::InvalidNode("tree height overflow".into()))?;
                save_node(
                    writer,
                    codec::thrift::encode_internal(&InternalNode::new(schema, height, children)?, schema)?,
                )
            })
            .collect::<Result<_>>()?;
    }
    Ok(level.pop())
}
