mod common;
use common::*;

#[test]
fn missing_root_file_surfaces_error() {
    let tmp = beech_disk::Workspace::new().unwrap();
    // No tree written — root file doesn't exist.
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    let sql = format!(
        "CREATE VIRTUAL TABLE tt USING beech('{}', 'table')",
        tmp.path().display(),
    );
    let result = conn.execute_batch(&sql);
    assert!(result.is_err(), "should fail with missing root file");
}

#[test]
fn missing_table_in_transaction_surfaces_not_found() {
    let rows: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    // The tree was written under table name "t" but we ask for "nonexistent".
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    let sql = format!(
        "CREATE VIRTUAL TABLE tt USING beech('{}', 'nonexistent')",
        tmp.path().display(),
    );
    let result = conn.execute_batch(&sql);
    assert!(result.is_err(), "should fail when table name doesn't match");
}

#[test]
fn missing_leaf_surfaces_its_id_during_query() {
    use beech_core::{
        Id,
        storage::{FileStore, Repository},
    };
    use std::sync::Arc;

    let tmp = make_test_tree((0..5).map(|i| int_row(i, i as i32)).collect(), vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");
    let repository = Arc::new(Repository::new(FileStore::new(tmp.path())));
    let root_id = Id::from_hex(std::fs::read_to_string(tmp.path().join("root")).unwrap().trim()).unwrap();
    let table = repository.snapshot(root_id).unwrap().table("t").unwrap();
    let leaf_id = table.root().unwrap().id();
    std::fs::remove_file(tmp.path().join(leaf_id.to_string())).unwrap();
    let error = conn.query_row("SELECT k FROM tt", [], |r| r.get::<_, i32>(0)).unwrap_err();
    assert!(error.to_string().contains(&leaf_id.to_string()), "{error}");
}

#[test]
fn module_rejects_ignored_arguments() {
    let tmp = make_test_tree(vec![int_row(0, 0)], vec![0], "t");
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    for args in ["'unused', 't'", "'t', 'option=value'"] {
        let error = conn
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE tt USING beech('{}', {args})",
                tmp.path().display()
            ))
            .unwrap_err();
        assert!(error.to_string().contains("Usage:"), "{error}");
    }
}
