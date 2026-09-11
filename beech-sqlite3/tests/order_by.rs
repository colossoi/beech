mod common;
use common::*;

#[test]
fn order_by_key_asc_consumed() {
    let rows: Vec<_> = (0..50).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    // ORDER BY k ASC should be consumed by the vtab (no sort step).
    let plan = conn
        .prepare("EXPLAIN QUERY PLAN SELECT k FROM tt ORDER BY k ASC")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n");
    // If the vtab consumed the ORDER BY, SQLite should NOT show "USE TEMP
    // B-TREE FOR ORDER BY" in the plan output.
    assert!(
        !plan.to_uppercase().contains("TEMP B-TREE"),
        "expected no sort step, got: {}",
        plan,
    );

    // Also verify the results are actually sorted.
    let mut stmt = conn.prepare("SELECT k FROM tt ORDER BY k ASC").unwrap();
    let ks: Vec<i32> = stmt.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(ks, (0..50).collect::<Vec<i32>>());
}

#[test]
fn order_by_key_desc_not_consumed() {
    let rows: Vec<_> = (0..20).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    // DESC is not natively supported, so results should still be correct
    // even though SQLite adds its own sort.
    let mut stmt = conn.prepare("SELECT k FROM tt ORDER BY k DESC").unwrap();
    let ks: Vec<i32> = stmt.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    let expected: Vec<i32> = (0..20).rev().collect();
    assert_eq!(ks, expected);
}
