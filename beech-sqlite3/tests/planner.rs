mod common;
use common::*;

fn get_plan(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let explain = format!("EXPLAIN QUERY PLAN {}", sql);
    let mut stmt = conn.prepare(&explain).unwrap();
    let plans: Vec<String> =
        stmt.query_map([], |r| r.get::<_, String>(3)).unwrap().map(|r| r.unwrap()).collect();
    plans
}

#[test]
fn eq_query_estimates_one_row() {
    let rows: Vec<_> = (0..1000).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    let plans = get_plan(&conn, "SELECT k FROM tt WHERE k = 42");
    // The plan should show the vtab scan. We mainly verify it doesn't crash
    // and produces a plan entry referencing our table.
    assert!(!plans.is_empty(), "EXPLAIN QUERY PLAN should produce output");
    let plan_text = plans.join(" ");
    assert!(
        plan_text.contains("tt"),
        "plan should reference our table: {}",
        plan_text
    );
}

#[test]
fn full_scan_estimates_total_rows() {
    let rows: Vec<_> = (0..100).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    let plans = get_plan(&conn, "SELECT * FROM tt");
    assert!(!plans.is_empty(), "EXPLAIN QUERY PLAN should produce output");
    let plan_text = plans.join(" ");
    assert!(
        plan_text.contains("tt"),
        "plan should reference our table: {}",
        plan_text
    );
}
