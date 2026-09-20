use beech_core::{query::RowCursor, BeechError, DataType, Field, NodeRef, Scalar, Table, TableSchema};
use beech_test_fixtures::{build_simple_table, MemoryStore};
use beech_write::{apply_changes, BuildOptions, Change, Writer};
use std::sync::Arc;
fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ],
        vec![0],
    )
    .unwrap()
}
fn int_row(i: i64, v: i32) -> beech_core::Row {
    (i, vec![Scalar::Int32(i as i32), Scalar::Int32(v)])
}
fn collect_row_ids(source: &beech_core::storage::Repository, table: &Table) -> Vec<i64> {
    RowCursor::new(source, table, vec![]).unwrap().map(|r| r.unwrap().0).collect()
}
fn collect_pairs(source: &beech_core::storage::Repository, table: &Table) -> Vec<(i64, i32, i32)> {
    RowCursor::new(source, table, vec![])
        .unwrap()
        .map(|r| {
            let (id, values) = r.unwrap();
            let (Scalar::Int32(k), Scalar::Int32(v)) = (&values[0], &values[1]) else {
                panic!("wrong types")
            };
            (id, *k, *v)
        })
        .collect()
}
// --- Build ----------------------------------------------------------

#[test]
fn write_rows_creates_readable_tree() {
    let rows: Vec<_> = (0..100).map(|i| int_row(i, (i * 10) as i32)).collect();
    let (_store, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..100).collect::<Vec<_>>());
}

#[test]
fn write_single_row_yields_single_leaf() {
    let rows = vec![int_row(0, 7)];
    let (_store, _source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let root_id = table.root().expect("root should exist").clone();
    assert_eq!(root_id.height(), 0);
    assert_eq!(root_id.row_count(), 1);
}

#[test]
fn write_many_rows_grows_to_multiple_levels() {
    let rows: Vec<_> = (0..2000).map(|i| int_row(i, (i % 100) as i32)).collect();
    let (_store, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let root_id = table.root().unwrap().clone();
    assert!(root_id.height() >= 1);
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids.len(), 2000);
}

// --- Merge ----------------------------------------------------------

fn build_seed_tree(n: i64) -> (MemoryStore, Arc<Table>, NodeRef) {
    let rows: Vec<_> = (0..n).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let root = table.root().unwrap().clone();
    drop(source);
    (store, table, root)
}

#[test]
fn merge_insert_into_existing_tree() {
    let (store, table, _root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Insert {
        key: vec![Scalar::Int32(100)],
        row_id: 100,
        record: vec![Scalar::Int32(100), Scalar::Int32(999)],
    }];
    let new_root = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap();
    assert!(new_root.root().is_some());
    writer.commit().unwrap();

    let new_table = new_root;
    let ids = collect_row_ids(&store.node_source(), &new_table);
    assert_eq!(ids.len(), 11);
    assert_eq!(*ids.last().unwrap(), 100);
}

#[test]
fn merge_update_existing_row() {
    let (store, table, _root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Update {
        key: vec![Scalar::Int32(5)],
        row_id: 5,
        record: vec![Scalar::Int32(5), Scalar::Int32(7777)],
    }];
    let new_root = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap();
    writer.commit().unwrap();
    let new_table = new_root;
    let pairs = collect_pairs(&store.node_source(), &new_table);
    let (_, _, v) = pairs.iter().find(|(_, k, _)| *k == 5).unwrap();
    assert_eq!(*v, 7777);
}

#[test]
fn merge_delete_existing_row() {
    let (store, table, _root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Delete {
        key: vec![Scalar::Int32(3)],
    }];
    let new_root = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap();
    writer.commit().unwrap();
    let new_table = new_root;
    let ids = collect_row_ids(&store.node_source(), &new_table);
    assert_eq!(ids, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn merge_empty_changes_returns_existing_root() {
    let (store, table, root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let new_root = apply_changes(
        std::iter::empty::<Change>().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap();
    assert_eq!(new_root.root(), Some(&root));
    writer.commit().unwrap();
}

#[test]
fn merge_full_delete_returns_no_root() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes: Vec<Change> = (0..5)
        .map(|i| Change::Delete {
            key: vec![Scalar::Int32(i)],
        })
        .collect();
    let new_root = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap();
    assert!(new_root.root().is_none());
    writer.commit().unwrap();
}

#[test]
fn merge_insert_with_existing_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Insert {
        key: vec![Scalar::Int32(2)],
        row_id: 99,
        record: vec![Scalar::Int32(2), Scalar::Int32(999)],
    }];
    let err = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap_err();
    match err {
        BeechError::Query(message) if message.contains("duplicate key") => (),
        other => panic!("expected DuplicateKey, got {:?}", other),
    }
}

#[test]
fn merge_update_unknown_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Update {
        key: vec![Scalar::Int32(999)],
        row_id: 999,
        record: vec![Scalar::Int32(999), Scalar::Int32(0)],
    }];
    let err = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap_err();
    match err {
        BeechError::Query(message) if message.contains("key not found") => (),
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

#[test]
fn merge_delete_unknown_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Delete {
        key: vec![Scalar::Int32(999)],
    }];
    let err = apply_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        BuildOptions::new(64, 16).unwrap(),
    )
    .unwrap_err();
    match err {
        BeechError::Query(message) if message.contains("key not found") => (),
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

// --- Round-trip property tests --------------------------------------

#[test]
fn tree_round_trip_1_row() {
    let rows: Vec<_> = (0..1).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, vec![0]);
}

#[test]
fn tree_round_trip_10_rows() {
    let rows: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..10).collect::<Vec<_>>());
}

#[test]
fn tree_round_trip_1000_rows() {
    let rows: Vec<_> = (0..1000).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, schema(), 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..1000).collect::<Vec<_>>());
}

#[test]
fn smallest_targets_terminate_and_rebuild_deterministically() {
    let rows: Vec<_> = (0..25).rev().map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", rows.clone(), schema(), 1, 1).unwrap();
    assert_eq!(collect_row_ids(&source, &table), (0..25).collect::<Vec<_>>());
    let mut writer = store.writer();
    let rebuilt = beech_write::build_table(
        &mut writer,
        "t".into(),
        schema(),
        rows,
        BuildOptions::new(1, 1).unwrap(),
    )
    .unwrap();
    assert_eq!(rebuilt.root(), table.root());
    writer.abort().unwrap();
    assert_eq!(collect_row_ids(&source, &table).len(), 25);
}

#[test]
fn malformed_changes_are_rejected_without_staging_objects() {
    let (store, table, _) = build_seed_tree(5);
    let source = store.node_source();
    let cases = vec![
        vec![
            Change::Delete {
                key: vec![Scalar::Int32(1)],
            },
            Change::Delete {
                key: vec![Scalar::Int32(1)],
            },
        ],
        vec![Change::Delete {
            key: vec![Scalar::Utf8("wrong".into())],
        }],
        vec![Change::Insert {
            key: vec![Scalar::Int32(6)],
            row_id: 6,
            record: vec![Scalar::Int32(7), Scalar::Int32(0)],
        }],
        vec![Change::Update {
            key: vec![Scalar::Int32(1)],
            row_id: 1,
            record: vec![Scalar::Int32(1), Scalar::Null],
        }],
    ];
    for changes in cases {
        let mut writer = store.writer();
        assert!(apply_changes(changes, &table, &source, &mut writer, BuildOptions::default()).is_err());
        assert_eq!(writer.num_to_commit(), 0);
        writer.abort().unwrap();
    }
    assert_eq!(collect_row_ids(&source, &table), (0..5).collect::<Vec<_>>());
}

#[test]
fn all_scalar_types_and_nulls_survive_parquet_writer() {
    use beech_core::Decimal;
    let types = vec![
        DataType::Int64,
        DataType::Boolean,
        DataType::Int32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, 2),
        DataType::Utf8,
        DataType::Binary,
    ];
    let schema = TableSchema::new(
        types.into_iter().enumerate().map(|(i, t)| Field::new(format!("c{i}"), t, i != 0)).collect(),
        vec![0],
    )
    .unwrap();
    let rows = vec![
        (
            -7,
            vec![
                Scalar::Int64(1),
                Scalar::Boolean(true),
                Scalar::Int32(i32::MIN),
                Scalar::UInt64(u64::MAX),
                Scalar::Float32(-0.0),
                Scalar::Float64(f64::from_bits(0x7ff8000000000001)),
                Scalar::Decimal(Decimal::new(10i128.pow(38) - 1, 2).unwrap()),
                Scalar::Utf8("é\0".into()),
                Scalar::Binary(vec![0, 255]),
            ],
        ),
        (
            9,
            std::iter::once(Scalar::Int64(2)).chain(std::iter::repeat_n(Scalar::Null, 8)).collect(),
        ),
    ];
    let (_, source, table) = build_simple_table("types", rows.clone(), schema, 64, 16).unwrap();
    let decoded =
        RowCursor::new(&source, &table, vec![]).unwrap().collect::<beech_core::Result<Vec<_>>>().unwrap();
    assert_eq!(decoded, rows);
}

#[test]
fn empty_table_can_be_published_and_then_inserted_into() {
    let (store, source, table) = build_simple_table("t", vec![], schema(), 64, 16).unwrap();
    assert!(table.root().is_none());
    let mut writer = store.writer();
    let root = apply_changes(
        [Change::Insert {
            key: vec![Scalar::Int32(1)],
            row_id: 7,
            record: vec![Scalar::Int32(1), Scalar::Int32(3)],
        }],
        &table,
        &source,
        &mut writer,
        BuildOptions::default(),
    )
    .unwrap();
    writer.commit().unwrap();
    let table = root;
    assert_eq!(collect_row_ids(&store.node_source(), &table), vec![7]);
}
