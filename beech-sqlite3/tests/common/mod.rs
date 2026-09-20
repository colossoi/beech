#![allow(dead_code)]

use beech_core::{
    DataType, Field, Id, InternalNode, Root, Row, Scalar, Table, TableSchema, Transaction,
    codec::{
        EncodedObject,
        parquet::encode_leaf,
        thrift::{encode_internal, encode_root, encode_table, encode_transaction},
    },
};
use beech_disk::Workspace;
use std::{collections::BTreeMap, path::Path, time::UNIX_EPOCH};

#[path = "../../../beech-core/tests/support/mod.rs"]
mod support;

pub type IntRecord = Vec<(&'static str, i32)>;

/// Existing integer fixtures, now encoded with the core Parquet/Thrift codecs.
pub fn make_test_tree(rows: Vec<(i64, IntRecord)>, key_columns: Vec<usize>, table_name: &str) -> Workspace {
    let fields = rows[0].1.iter().map(|(name, _)| Field::new(*name, DataType::Int32, false)).collect();
    let schema = TableSchema::new(fields, key_columns).unwrap();
    let rows = rows
        .into_iter()
        .map(|(id, values)| {
            (
                id,
                values.into_iter().map(|(_, value)| Scalar::Int32(value)).collect(),
            )
        })
        .collect::<Vec<_>>();
    make_tree(&schema, &rows, table_name)
}

pub fn make_tree(schema: &TableSchema, rows: &[Row], table_name: &str) -> Workspace {
    let tmp = Workspace::new().unwrap();
    write_tree(tmp.path(), schema, rows, table_name);
    tmp
}

pub fn write_tree(dir: &Path, schema: &TableSchema, rows: &[Row], table_name: &str) {
    let mut nodes = rows
        .chunks(64)
        .map(|rows| {
            let batch = support::batch_from_rows(schema, rows).unwrap();
            let encoded = encode_leaf(schema, &batch).unwrap();
            std::fs::write(dir.join(encoded.reference().id().to_string()), encoded.bytes()).unwrap();
            encoded.reference().clone()
        })
        .collect::<Vec<_>>();
    while nodes.len() > 1 {
        nodes = nodes
            .chunks(16)
            .map(|children| {
                let node = InternalNode::new(schema, children[0].height() + 1, children.to_vec()).unwrap();
                let encoded = encode_internal(&node, schema).unwrap();
                std::fs::write(dir.join(encoded.reference().id().to_string()), encoded.bytes()).unwrap();
                encoded.reference().clone()
            })
            .collect();
    }
    let table = Table::new(
        table_name,
        schema.clone(),
        nodes.pop(),
        rows.iter().map(|r| r.0).max().unwrap_or(-1),
    )
    .unwrap();
    let table_id = save(dir, encode_table(&table).unwrap());
    let transaction = Transaction::new(
        Id::default(),
        UNIX_EPOCH,
        BTreeMap::from([(table_name.into(), table_id)]),
    )
    .unwrap();
    let transaction_id = save(dir, encode_transaction(&transaction).unwrap());
    let root_id = save(dir, encode_root(&Root::new(transaction_id)).unwrap());
    std::fs::write(dir.join("root"), root_id.to_string()).unwrap();
}

fn save(dir: &Path, object: EncodedObject) -> Id {
    std::fs::write(dir.join(object.id().to_string()), object.bytes()).unwrap();
    object.id()
}

pub fn int_record(k: i32, v: i32) -> IntRecord {
    vec![("k", k), ("v", v)]
}
pub fn int_row(i: i64, v: i32) -> (i64, IntRecord) {
    (i, int_record(i as i32, v))
}
pub fn two_part_record(a: i32, b: i32, v: i32) -> IntRecord {
    vec![("a", a), ("b", b), ("v", v)]
}

pub fn setup_vtab(dir: &Path, table_name: &str, local_name: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE \"{}\" USING beech('{}', '{}')",
        local_name.replace('"', "\"\""),
        dir.display().to_string().replace('\'', "''"),
        table_name.replace('\'', "''"),
    ))
    .unwrap();
    conn
}
