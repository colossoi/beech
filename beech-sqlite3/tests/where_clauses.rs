mod common;
use common::*;

#[test]
fn where_eq_on_key() {
    let rows: Vec<_> = (0..50).map(|i| int_row(i, (i * 10) as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    let (k, v): (i32, i32) = conn
        .query_row("SELECT k, v FROM tt WHERE k = 17", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(k, 17);
    assert_eq!(v, 170);
}

#[test]
fn where_range_on_key() {
    let rows: Vec<_> = (0..50).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    let mut stmt = conn.prepare("SELECT k FROM tt WHERE k >= 10 AND k < 20").unwrap();
    let ks: Vec<i32> = stmt.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(ks, (10..20).collect::<Vec<i32>>());
}

#[test]
fn where_eq_on_non_key_column() {
    let rows: Vec<_> = (0..20).map(|i| int_row(i, (i % 5) as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    // v is not a key column, so this is a full scan with SQLite filtering.
    let mut stmt = conn.prepare("SELECT k FROM tt WHERE v = 3").unwrap();
    let ks: Vec<i32> = stmt.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    // keys 3, 8, 13, 18 all have v = 3
    assert_eq!(ks, vec![3, 8, 13, 18]);
}
