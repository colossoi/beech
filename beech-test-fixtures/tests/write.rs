use apache_avro::types::Value;
use beech_core::query::RowCursor;
use beech_core::{BeechError, DomainError, NodeSource, Table};
use beech_test_fixtures::{build_simple_table, MemoryNodeSource, MemoryStore};
use beech_write::{merge_changes, Change, Writer};
use std::sync::Arc;

fn int_record(k: i32, v: i32) -> Value {
    Value::Record(vec![
        ("k".to_string(), Value::Int(k)),
        ("v".to_string(), Value::Int(v)),
    ])
}

fn int_row(i: i64, v: i32) -> (i64, Value) {
    (i, int_record(i as i32, v))
}

fn collect_row_ids(source: &MemoryNodeSource, table: &Table) -> Vec<i64> {
    let mut cursor = RowCursor::new(table);
    cursor.init(vec![], vec![]);
    cursor.advance_to_left(source).unwrap();
    let mut out = Vec::new();
    while let Some((pid, slot)) = cursor.current().cloned() {
        let node = source.get_node(&pid, &table.schema).unwrap();
        let leaf = node.as_leaf().unwrap();
        out.push(leaf.entry(slot).unwrap().row_id());
        cursor.next(source).unwrap();
    }
    out
}

fn collect_pairs(source: &MemoryNodeSource, table: &Table) -> Vec<(i64, i32, i32)> {
    let mut cursor = RowCursor::new(table);
    cursor.init(vec![], vec![]);
    cursor.advance_to_left(source).unwrap();
    let mut out = Vec::new();
    while let Some((pid, slot)) = cursor.current().cloned() {
        let node = source.get_node(&pid, &table.schema).unwrap();
        let leaf = node.as_leaf().unwrap();
        let entry = leaf.entry(slot).unwrap();
        let row_id = entry.row_id();
        let values = entry.values();
        let k = match &values[0] {
            Value::Int(i) => *i,
            other => panic!("unexpected key type {:?}", other),
        };
        let v = match &values[1] {
            Value::Int(i) => *i,
            other => panic!("unexpected value type {:?}", other),
        };
        out.push((row_id, k, v));
        cursor.next(source).unwrap();
    }
    out
}

// --- Build ----------------------------------------------------------

#[test]
fn write_rows_creates_readable_tree() {
    let rows: Vec<_> = (0..100).map(|i| int_row(i, (i * 10) as i32)).collect();
    let (_store, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..100).collect::<Vec<_>>());
}

#[test]
fn write_single_row_yields_single_leaf() {
    let rows = vec![int_row(0, 7)];
    let (_store, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let root_id = table.root.as_ref().expect("root should exist").clone();
    let node = source.get_node(&root_id, &table.schema).unwrap();
    assert!(node.is_leaf(), "single-row tree root should be a leaf");
    assert_eq!(node.len(), 1);
}

#[test]
fn write_many_rows_grows_to_multiple_levels() {
    let rows: Vec<_> = (0..2000).map(|i| int_row(i, (i % 100) as i32)).collect();
    let (_store, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let root_id = table.root.as_ref().unwrap().clone();
    let root = source.get_node(&root_id, &table.schema).unwrap();
    assert!(root.is_internal(), "large tree root should be internal");
    assert!(root.depth() >= 1);
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids.len(), 2000);
}

// --- Merge ----------------------------------------------------------

fn build_seed_tree(n: i64) -> (MemoryStore, Arc<Table>, beech_core::Id) {
    let rows: Vec<_> = (0..n).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let root = table.root.as_ref().unwrap().clone();
    drop(source);
    (store, table, root)
}

#[test]
fn merge_insert_into_existing_tree() {
    let (store, table, _root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Insert {
        key: vec![Value::Int(100)],
        row_id: 100,
        record: vec![Value::Int(100), Value::Int(999)],
    }];
    let new_root = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap();
    assert!(new_root.is_some());
    writer.commit().unwrap();

    let new_table = Table::new(
        table.id.clone(),
        table.name.clone(),
        new_root,
        table.schema.clone(),
    )
    .unwrap();
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
        key: vec![Value::Int(5)],
        row_id: 5,
        record: vec![Value::Int(5), Value::Int(7777)],
    }];
    let new_root = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap();
    writer.commit().unwrap();
    let new_table = Table::new(
        table.id.clone(),
        table.name.clone(),
        new_root,
        table.schema.clone(),
    )
    .unwrap();
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
        key: vec![Value::Int(3)],
    }];
    let new_root = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap();
    writer.commit().unwrap();
    let new_table = Table::new(
        table.id.clone(),
        table.name.clone(),
        new_root,
        table.schema.clone(),
    )
    .unwrap();
    let ids = collect_row_ids(&store.node_source(), &new_table);
    assert_eq!(ids, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn merge_empty_changes_returns_existing_root() {
    let (store, table, root) = build_seed_tree(10);
    let source = store.node_source();
    let mut writer = store.writer();
    let new_root = merge_changes(
        std::iter::empty::<Change>().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap();
    assert_eq!(new_root, Some(root));
    writer.commit().unwrap();
}

#[test]
fn merge_full_delete_returns_no_root() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes: Vec<Change> = (0..5)
        .map(|i| Change::Delete {
            key: vec![Value::Int(i as i32)],
        })
        .collect();
    let new_root = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap();
    assert!(new_root.is_none());
    writer.commit().unwrap();
}

#[test]
fn merge_insert_with_existing_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Insert {
        key: vec![Value::Int(2)],
        row_id: 99,
        record: vec![Value::Int(2), Value::Int(999)],
    }];
    let err = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap_err();
    match err {
        BeechError::Domain(DomainError::DuplicateKey { .. }) => (),
        other => panic!("expected DuplicateKey, got {:?}", other),
    }
}

#[test]
fn merge_update_unknown_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Update {
        key: vec![Value::Int(999)],
        row_id: 999,
        record: vec![Value::Int(999), Value::Int(0)],
    }];
    let err = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap_err();
    match err {
        BeechError::Domain(DomainError::KeyNotFound { .. }) => (),
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

#[test]
fn merge_delete_unknown_key_errors() {
    let (store, table, _root) = build_seed_tree(5);
    let source = store.node_source();
    let mut writer = store.writer();
    let changes = vec![Change::Delete {
        key: vec![Value::Int(999)],
    }];
    let err = merge_changes(
        changes.into_iter().peekable(),
        &table,
        &source,
        &mut writer,
        64,
        16,
    )
    .unwrap_err();
    match err {
        BeechError::Domain(DomainError::KeyNotFound { .. }) => (),
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

// --- Round-trip property tests --------------------------------------

#[test]
fn tree_round_trip_1_row() {
    let rows: Vec<_> = (0..1).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, vec![0]);
}

#[test]
fn tree_round_trip_10_rows() {
    let rows: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..10).collect::<Vec<_>>());
}

#[test]
fn tree_round_trip_1000_rows() {
    let rows: Vec<_> = (0..1000).map(|i| int_row(i, i as i32)).collect();
    let (_s, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    let ids = collect_row_ids(&source, &table);
    assert_eq!(ids, (0..1000).collect::<Vec<_>>());
}
