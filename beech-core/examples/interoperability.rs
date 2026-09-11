//! cargo run -p beech-core --example interoperability -- write target/interop
//! Run with verify to read independently produced fixtures from the output directory.
#[path = "../tests/support/mod.rs"]
mod support;
use beech_core::{
    query::{ConstraintOp, Predicate, RowCursor},
    storage::{FileStore, Repository},
    *,
};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::UNIX_EPOCH};
use support::{MemoryStore, batch_from_rows};
fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec![0],
    )
    .unwrap()
}
fn save(store: &FileStore, object: &codec::EncodedObject) -> Result<Id> {
    std::fs::write(store.object_path(&object.id()), object.bytes())?;
    Ok(object.id())
}
fn decimal_schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("key", DataType::Decimal128(38, 4), false),
            Field::new("small", DataType::Decimal128(9, 2), true),
            Field::new("medium", DataType::Decimal128(18, 4), true),
            Field::new("wide", DataType::Decimal128(38, 18), true),
        ],
        vec![0],
    )
    .unwrap()
}
fn decimal_rows() -> Vec<Row> {
    (0..6)
        .map(|i| {
            let values = [(38, 4), (9, 2), (18, 4), (38, 18)]
                .into_iter()
                .enumerate()
                .map(|(col, (precision, scale))| {
                    if col > 0 && i == 3 {
                        return Scalar::Null;
                    }
                    let max = 10i128.pow(precision) - 1;
                    Scalar::Decimal(Decimal::new([-max, -12345, -1, 0, 12345, max][i], scale).unwrap())
                })
                .collect();
            (2000 + i as i64, values)
        })
        .collect()
}
fn write_decimals(dir: &Path) -> Result<()> {
    let s = decimal_schema();
    let rows = decimal_rows();
    let leaf = codec::parquet::encode_leaf(&s, &batch_from_rows(&s, &rows)?)?;
    std::fs::write(dir.join("rust-decimal.parquet"), leaf.bytes())?;
    std::fs::write(
        dir.join("decimal-schema.thrift"),
        codec::thrift::encode_schema(&s)?.bytes(),
    )?;
    let children = rows
        .chunks(3)
        .map(|rows| Ok(codec::parquet::encode_leaf(&s, &batch_from_rows(&s, rows)?)?.reference().clone()))
        .collect::<Result<Vec<_>>>()?;
    let internal = codec::thrift::encode_internal(&InternalNode::new(&s, 1, children)?, &s)?;
    std::fs::write(dir.join("decimal-internal.thrift"), internal.bytes())?;
    Ok(())
}
fn verify_decimals(dir: &Path) -> Result<()> {
    let s = decimal_schema();
    let expected = decimal_rows();
    for suffix in ["fixed", "int"] {
        let name = format!("python-decimal-{suffix}");
        let id = Id::from_hex(std::fs::read_to_string(dir.join(format!("{name}.id")))?.trim())?;
        let store = Arc::new(MemoryStore::default());
        store.put(id, std::fs::read(dir.join(format!("{name}.parquet")))?)?;
        let reference = NodeRef::new(&s, id, 0, 6, s.key_from_row(expected.last().unwrap())?)?;
        let table = Table::new("decimals", s.clone(), Some(reference))?;
        let source = Repository::with_options(
            store,
            storage::RepositoryOptions {
                verify_leaves: true,
                ..Default::default()
            },
        );
        assert_eq!(
            RowCursor::new(&source, &table, vec![])?.collect::<Result<Vec<_>>>()?,
            expected
        );
    }
    let internal =
        codec::thrift::decode_internal(&std::fs::read(dir.join("python-decimal-internal.thrift"))?, &s)?;
    assert_eq!(
        internal.children().last().unwrap().max_key(),
        &s.key_from_row(expected.last().unwrap())?
    );
    println!(
        "Verified all 38 decimal digits, nulls, integer/fixed Parquet storage, and Thrift decimal keys."
    );
    Ok(())
}
fn write(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let s = schema();
    let store = FileStore::new(dir);
    let rows: Vec<_> = (0..6)
        .map(|i| {
            (
                1000 + i,
                vec![
                    Scalar::Int64(i),
                    if i == 2 { Scalar::Null } else { Scalar::Utf8(format!("rust {i}")) },
                ],
            )
        })
        .collect();
    let mut children = vec![];
    for chunk in rows.chunks(2) {
        let node = codec::parquet::encode_leaf(&s, &batch_from_rows(&s, chunk)?)?;
        std::fs::write(store.object_path(&node.reference().id()), node.bytes())?;
        children.push(node.reference().clone());
    }
    let node = codec::thrift::encode_internal(&InternalNode::new(&s, 1, children)?, &s)?;
    std::fs::write(store.object_path(&node.reference().id()), node.bytes())?;
    let table = Table::new("example", s.clone(), Some(node.reference().clone()))?;
    let table_id = save(&store, &codec::thrift::encode_table(&table)?)?;
    let txn = Transaction::new(
        Id::default(),
        UNIX_EPOCH,
        BTreeMap::from([("example".into(), table_id)]),
    )?;
    let txn_id = save(&store, &codec::thrift::encode_transaction(&txn)?)?;
    let root_id = save(&store, &codec::thrift::encode_root(&Root::new(txn_id))?)?;
    std::fs::write(dir.join("root-id.txt"), root_id.to_string())?;
    std::fs::write(
        dir.join("schema.thrift"),
        codec::thrift::encode_schema(&s)?.bytes(),
    )?;
    let repository = std::sync::Arc::new(Repository::new(store));
    let source = repository.snapshot(root_id)?;
    let table = source.table("example")?;
    assert_eq!(
        RowCursor::new(&source, &table, vec![])?.collect::<Result<Vec<_>>>()?,
        rows
    );
    println!("Wrote and reopened a tree with 3 Parquet leaves and a Thrift root.");
    write_decimals(dir)?;
    Ok(())
}
fn verify(dir: &Path) -> Result<()> {
    let s = schema();
    let bytes = std::fs::read(dir.join("python.parquet"))?;
    let store = Arc::new(MemoryStore::default());
    let id = Id::from_hex(std::fs::read_to_string(dir.join("python-id.txt"))?.trim())?;
    store.put(id, bytes)?;
    let table = Table::new(
        "python",
        s.clone(),
        Some(NodeRef::new(&s, id, 0, 4, vec![Scalar::Int64(9)])?),
    )?;
    let source = Repository::with_options(
        store,
        storage::RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    let rows = RowCursor::new(
        &source,
        &table,
        vec![Predicate::new(0, ConstraintOp::Ge, Scalar::Int64(8))],
    )?
    .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        rows,
        vec![
            (1008, vec![Scalar::Int64(8), Scalar::Utf8("python 8".into())]),
            (1009, vec![Scalar::Int64(9), Scalar::Null])
        ]
    );
    let internal = codec::thrift::decode_internal(&std::fs::read(dir.join("python-internal.thrift"))?, &s)?;
    assert_eq!(internal.children().len(), 3);
    println!("Verified Python-written single-row-group Parquet and Thrift in Rust.");
    verify_decimals(dir)?;
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("write");
    let dir = Path::new(args.get(2).map(String::as_str).unwrap_or("target/interop"));
    match mode {
        "write" => write(dir),
        "verify" => verify(dir),
        _ => Err(BeechError::Query("expected write or verify".into())),
    }
}
