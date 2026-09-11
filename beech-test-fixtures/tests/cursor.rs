use apache_avro::types::Value;
use beech_core::query::{Constraint, ConstraintOp, RowCursor};
use beech_core::{Id, NodeSource, Table};
use beech_test_fixtures::{build_simple_table, MemoryNodeSource};
use std::sync::Arc;

fn int_row(i: i64, v: i32) -> (i64, Value) {
    let record = Value::Record(vec![
        ("k".to_string(), Value::Int(i as i32)),
        ("v".to_string(), Value::Int(v)),
    ]);
    (i, record)
}

fn two_part_row(row_id: i64, a: i32, b: i32, payload: i32) -> (i64, Value) {
    let record = Value::Record(vec![
        ("a".to_string(), Value::Int(a)),
        ("b".to_string(), Value::Int(b)),
        ("v".to_string(), Value::Int(payload)),
    ]);
    (row_id, record)
}

fn drain_row_ids(cursor: &mut RowCursor, source: &MemoryNodeSource, table: &Table) -> Vec<i64> {
    let mut out = Vec::new();
    loop {
        let (page_id, slot) = match cursor.current() {
            Some(c) => c.clone(),
            None => break,
        };
        let node = source.get_node(&page_id, &table.schema).unwrap();
        let leaf = node.as_leaf().expect("cursor points at a leaf");
        let entry = leaf.entry(slot).expect("slot in range");
        out.push(entry.row_id());
        cursor.next(source).unwrap();
    }
    out
}

fn build_int_table(n: i64) -> (MemoryNodeSource, Arc<Table>) {
    let rows: Vec<(i64, Value)> = (0..n).map(|i| int_row(i, (i * 10) as i32)).collect();
    let (_store, source, table) = build_simple_table("t", rows, vec![0], 64, 16).unwrap();
    (source, table)
}

#[test]
fn cursor_full_scan_yields_all_rows() {
    let (source, table) = build_int_table(200);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![], vec![]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    let expected: Vec<i64> = (0..200).collect();
    assert_eq!(ids, expected);
    assert!(cursor.eof());
}

#[test]
fn cursor_eq_constraint_yields_single_row() {
    let (source, table) = build_int_table(50);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![Constraint::new(0, ConstraintOp::Eq)], vec![Value::Int(17)]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert_eq!(ids, vec![17]);
}

#[test]
fn cursor_range_constraint_yields_subset() {
    let (source, table) = build_int_table(50);
    let mut cursor = RowCursor::new(&table);
    cursor.init(
        vec![
            Constraint::new(0, ConstraintOp::Ge),
            Constraint::new(0, ConstraintOp::Lt),
        ],
        vec![Value::Int(10), Value::Int(20)],
    );
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert_eq!(ids, (10..20).collect::<Vec<_>>());
}

#[test]
fn cursor_done_iterating_clears_stack() {
    let (source, table) = build_int_table(20);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![], vec![]);
    cursor.advance_to_left(&source).unwrap();
    let _ids = drain_row_ids(&mut cursor, &source, &table);
    assert!(cursor.eof());
    assert!(cursor.current().is_none());
    assert_eq!(cursor.depth(), 0);
}

#[test]
fn cursor_composite_key_prefix_eq() {
    let rows: Vec<(i64, Value)> =
        (0..5).flat_map(|a| (0..4).map(move |b| two_part_row(a * 10 + b, a as i32, b as i32, 0))).collect();
    let expected_row_count = rows.len() as i64;
    let (_store, source, table) = build_simple_table("t", rows, vec![0, 1], 64, 16).unwrap();

    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![Constraint::new(0, ConstraintOp::Eq)], vec![Value::Int(3)]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert_eq!(ids, vec![30, 31, 32, 33]);

    let mut scan = RowCursor::new(&table);
    scan.init(vec![], vec![]);
    scan.advance_to_left(&source).unwrap();
    let all = drain_row_ids(&mut scan, &source, &table);
    assert_eq!(all.len() as i64, expected_row_count);
}

#[test]
fn cursor_advance_to_next_leaf_crosses_nodes() {
    let (source, table) = build_int_table(500);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![], vec![]);
    cursor.advance_to_left(&source).unwrap();
    let mut visited = 0usize;
    let mut last_page: Option<Id> = None;
    let mut leaf_transitions = 0usize;
    while let Some((pid, _slot)) = cursor.current().cloned() {
        if last_page.as_ref() != Some(&pid) {
            if last_page.is_some() {
                leaf_transitions += 1;
            }
            last_page = Some(pid);
        }
        visited += 1;
        cursor.next(&source).unwrap();
        if visited >= 500 {
            break;
        }
    }
    assert_eq!(visited, 500);
    assert!(
        leaf_transitions >= 2,
        "expected cursor to cross multiple leaves, got {} transitions",
        leaf_transitions
    );
}

// BUG: Eq below all keys returns row 0 — advance_to_left positions at the
// first leaf entry without checking whether it satisfies the Eq constraint.
#[test]
#[ignore]
fn constraint_before_beginning_yields_nothing() {
    let (source, table) = build_int_table(10);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![Constraint::new(0, ConstraintOp::Eq)], vec![Value::Int(-100)]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert!(ids.is_empty(), "expected empty result for Eq=-100, got {:?}", ids);
}

#[test]
fn constraint_after_end_yields_nothing() {
    let (source, table) = build_int_table(10);
    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![Constraint::new(0, ConstraintOp::Eq)], vec![Value::Int(9999)]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert!(ids.is_empty());

    let mut cursor = RowCursor::new(&table);
    cursor.init(vec![Constraint::new(0, ConstraintOp::Ge)], vec![Value::Int(9999)]);
    cursor.advance_to_left(&source).unwrap();
    let ids = drain_row_ids(&mut cursor, &source, &table);
    assert!(ids.is_empty());
}
