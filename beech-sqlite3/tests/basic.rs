mod common;
use common::*;

#[test]
fn module_registers() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
}

#[test]
fn create_virtual_table() {
    let rows: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let _conn = setup_vtab(tmp.path(), "t", "test_table");
}

#[test]
fn select_star_returns_all_rows() {
    let n = 100i64;
    let rows: Vec<_> = (0..n).map(|i| int_row(i, (i * 10) as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "test_table");

    let count: i64 = conn.query_row("SELECT count(*) FROM test_table", [], |r| r.get(0)).unwrap();
    assert_eq!(count, n);
}

#[test]
fn count_returns_row_count() {
    let n = 50i64;
    let rows: Vec<_> = (0..n).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "test_table");

    let count: i64 = conn.query_row("SELECT count(*) FROM test_table", [], |r| r.get(0)).unwrap();
    assert_eq!(count, n);
}
