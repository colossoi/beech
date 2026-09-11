#[path = "../tests/support/mod.rs"]
mod fixtures;
use crate::{codec::EncodedNode, *};
use beech_shaper::ProbShaper;
pub(crate) use fixtures::{MemoryStore, batch_from_rows};
use std::sync::Arc;
pub fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("group", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, false),
        ],
        vec![0],
    )
    .unwrap()
}
pub fn rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            (
                10_000 + i as i64,
                vec![
                    Scalar::Int64(i as i64),
                    Scalar::Int32((i / 10) as i32),
                    if i % 7 == 0 { Scalar::Null } else { Scalar::Utf8(format!("row {i}")) },
                    Scalar::Binary(vec![(i % 255) as u8; 37]),
                ],
            )
        })
        .collect()
}
pub fn build(
    schema: &TableSchema,
    rows: &[Row],
    leaf_rows: usize,
    fanout: usize,
) -> (Table, Arc<MemoryStore>, Vec<EncodedNode>) {
    assert!(leaf_rows > 0 && fanout >= 2);
    let leaves = rows
        .chunks(leaf_rows)
        .map(|chunk| codec::parquet::encode_leaf(schema, &batch_from_rows(schema, chunk).unwrap()).unwrap())
        .collect();
    finish(schema, leaves, fanout)
}
fn finish(
    schema: &TableSchema,
    leaves: Vec<EncodedNode>,
    fanout: usize,
) -> (Table, Arc<MemoryStore>, Vec<EncodedNode>) {
    let store = Arc::new(MemoryStore::default());
    let mut objects = leaves;
    for node in &objects {
        store.put(node.reference().id(), node.bytes().clone()).unwrap();
    }
    let mut level: Vec<_> = objects.iter().map(|n| n.reference.clone()).collect();
    while level.len() > 1 {
        let mut groups: Vec<Vec<NodeRef>> = level.chunks(fanout).map(|c| c.to_vec()).collect();
        if groups.len() > 1 && groups.last().unwrap().len() == 1 {
            let last = groups.pop().unwrap();
            groups.last_mut().unwrap().extend(last);
        }
        level = groups
            .into_iter()
            .map(|children| {
                let height = children[0].height + 1;
                let node = codec::thrift::encode_internal(
                    &InternalNode::new(schema, height, children).unwrap(),
                    schema,
                )
                .unwrap();
                store.put(node.reference().id(), node.bytes().clone()).unwrap();
                let r = node.reference.clone();
                objects.push(node);
                r
            })
            .collect();
    }
    (
        Table::new("items", schema.clone(), level.pop()).unwrap(),
        store,
        objects,
    )
}
pub fn build_prolly(schema: &TableSchema, rows: &[Row]) -> (Table, Arc<MemoryStore>, Vec<EncodedNode>) {
    let mut shaper = ProbShaper::new(512, 128);
    let mut chunk = vec![];
    let mut leaves = vec![];
    for row in rows {
        chunk.push(row.clone());
        if shaper.is_complete(&codec::thrift::encode_row_for_splitting(row).unwrap()) {
            leaves.push(
                codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &chunk).unwrap()).unwrap(),
            );
            chunk.clear();
            shaper = ProbShaper::new(512, 128);
        }
    }
    if !chunk.is_empty() {
        leaves
            .push(codec::parquet::encode_leaf(schema, &batch_from_rows(schema, &chunk).unwrap()).unwrap());
    }
    // Use content-defined boundaries at each internal level too, keeping at least
    // two children per parent so the construction always makes progress.
    let store = Arc::new(MemoryStore::default());
    let mut objects = leaves;
    for node in &objects {
        store.put(node.reference().id(), node.bytes().clone()).unwrap();
    }
    let mut level: Vec<_> = objects.iter().map(|n| n.reference.clone()).collect();
    while level.len() > 1 {
        let mut shaper = ProbShaper::new(128, 32);
        let mut groups = vec![];
        let mut group = vec![];
        for child in level {
            let split = codec::thrift::encode_key(&child.max_key).unwrap();
            group.push(child);
            let boundary = shaper.is_complete(&split);
            if boundary && group.len() >= 2 {
                groups.push(std::mem::take(&mut group));
                shaper = ProbShaper::new(128, 32);
            }
        }
        if !group.is_empty() {
            if group.len() == 1 && !groups.is_empty() {
                groups.last_mut().unwrap().extend(group);
            } else {
                groups.push(group);
            }
        }
        level = groups
            .into_iter()
            .map(|children| {
                let node = codec::thrift::encode_internal(
                    &InternalNode::new(schema, children[0].height + 1, children).unwrap(),
                    schema,
                )
                .unwrap();
                store.put(node.reference().id(), node.bytes().clone()).unwrap();
                let r = node.reference.clone();
                objects.push(node);
                r
            })
            .collect();
    }
    (
        Table::new("items", schema.clone(), level.pop()).unwrap(),
        store,
        objects,
    )
}
