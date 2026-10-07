mod common;
use common::*;

fn rows(conn: &rusqlite::Connection) -> Vec<(i64, i32, i32)> {
    conn.prepare("SELECT rowid,k,v FROM tt ORDER BY k")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn insert_update_delete_and_reopen() {
    let tmp = make_test_tree(vec![int_row(1, 10), int_row(2, 20)], vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    conn.execute("INSERT INTO tt(k,v) VALUES(3,30)", []).unwrap();
    assert_eq!(rows(&conn), vec![(1, 1, 10), (2, 2, 20), (3, 3, 30)]);

    conn.execute("UPDATE tt SET k=4,v=40,rowid=8 WHERE k=2", []).unwrap();
    conn.execute("DELETE FROM tt WHERE k=1", []).unwrap();
    assert_eq!(rows(&conn), vec![(3, 3, 30), (8, 4, 40)]);

    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), vec![(3, 3, 30), (8, 4, 40)]);
}

#[test]
fn statement_changes_are_atomic() {
    let tmp = make_test_tree(vec![int_row(1, 10), int_row(2, 20)], vec![0], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    let error = conn.execute("UPDATE tt SET k=9", []).unwrap_err();
    assert!(error.to_string().contains("duplicate key"), "{error}");
    assert_eq!(rows(&conn), vec![(1, 1, 10), (2, 2, 20)]);

    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), vec![(1, 1, 10), (2, 2, 20)]);
}

#[test]
fn explicit_transaction_rollback_discards_changes() {
    let tmp = make_test_tree(vec![int_row(1, 10)], vec![0], "t");
    let mut conn = setup_vtab(tmp.path(), "t", "tt");
    let transaction = conn.transaction().unwrap();
    transaction.execute("INSERT INTO tt(k,v) VALUES(2,20)", []).unwrap();
    assert_eq!(rows(&transaction), vec![(1, 1, 10), (2, 2, 20)]);
    transaction.execute("UPDATE tt SET k=3,v=30 WHERE k=2", []).unwrap();
    transaction.execute("DELETE FROM tt WHERE k=1", []).unwrap();
    assert_eq!(rows(&transaction), vec![(2, 3, 30)]);
    transaction.rollback().unwrap();
    assert_eq!(rows(&conn), vec![(1, 1, 10)]);
}

#[test]
fn explicit_transaction_reads_and_commits_pending_changes() {
    let tmp = make_test_tree(vec![int_row(1, 10), int_row(2, 20)], vec![0], "t");
    let mut conn = setup_vtab(tmp.path(), "t", "tt");
    let transaction = conn.transaction().unwrap();
    transaction.execute("INSERT INTO tt(k,v) VALUES(3,30)", []).unwrap();
    transaction.execute("UPDATE tt SET v=200 WHERE k=2", []).unwrap();
    transaction.execute("DELETE FROM tt WHERE k=1", []).unwrap();
    assert_eq!(rows(&transaction), vec![(2, 2, 200), (3, 3, 30)]);
    transaction.commit().unwrap();
    assert_eq!(rows(&conn), vec![(2, 2, 200), (3, 3, 30)]);
}

#[test]
fn repeated_key_mutations_keep_transaction_order_at_commit() {
    let tmp = make_test_tree(vec![int_row(1, 10), int_row(4, 40)], vec![0], "t");
    let mut conn = setup_vtab(tmp.path(), "t", "tt");
    let transaction = conn.transaction().unwrap();

    transaction.execute("UPDATE tt SET v=11 WHERE k=1", []).unwrap();
    transaction.execute("UPDATE tt SET v=12 WHERE k=1", []).unwrap();
    transaction.execute("DELETE FROM tt WHERE k=4", []).unwrap();
    transaction.execute("UPDATE tt SET k=4,v=14 WHERE k=1", []).unwrap();
    transaction.execute("UPDATE tt SET v=15 WHERE k=4", []).unwrap();

    assert_eq!(rows(&transaction), vec![(1, 4, 15)]);
    transaction.commit().unwrap();
    assert_eq!(rows(&conn), vec![(1, 4, 15)]);
    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), vec![(1, 4, 15)]);
}

#[test]
fn stale_writer_does_not_overwrite_a_newer_root() {
    let tmp = make_test_tree(vec![int_row(1, 10)], vec![0], "t");
    let first = setup_vtab(tmp.path(), "t", "tt");
    let second = setup_vtab(tmp.path(), "t", "tt");
    first.execute("INSERT INTO tt(k,v) VALUES(2,20)", []).unwrap();
    let error = second.execute("INSERT INTO tt(k,v) VALUES(3,30)", []).unwrap_err();
    assert!(error.to_string().contains("repository changed"), "{error}");
    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), vec![(1, 1, 10), (2, 2, 20)]);
}

#[test]
fn multi_row_updates_and_deletes_keep_the_scan_stable() {
    let tmp = make_test_tree((0..200).map(|i| int_row(i, i as i32)).collect(), vec![0], "t");
    let mut conn = setup_vtab(tmp.path(), "t", "tt");
    let transaction = conn.transaction().unwrap();
    assert_eq!(
        transaction.execute("UPDATE tt SET v=v+1000 WHERE k >= 25 AND k < 175", []).unwrap(),
        150
    );
    assert_eq!(
        transaction.execute("DELETE FROM tt WHERE k % 3 = 0", []).unwrap(),
        67
    );
    let actual = rows(&transaction);
    let expected = (0..200)
        .filter(|k| k % 3 != 0)
        .map(|k| (k as i64, k, if (25..175).contains(&k) { k + 1000 } else { k }))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    transaction.commit().unwrap();
    assert_eq!(rows(&conn), expected);
    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), expected);
}

#[test]
fn alternating_table_writes_preserve_a_shared_repository() {
    use beech_core::{
        Id, Table,
        storage::{FileStore, Repository},
    };
    use beech_write::{FileWriter, Writer, publish_table};

    let tmp = make_test_tree(vec![int_row(1, 10)], vec![0], "t");
    let repository = Repository::new(FileStore::new(tmp.path()));
    let root = Id::from_hex(std::fs::read_to_string(tmp.path().join("root")).unwrap().trim()).unwrap();
    let repository = std::sync::Arc::new(repository);
    let snapshot = repository.snapshot(root).unwrap();
    let first = snapshot.table("t").unwrap();
    let second = Table::new(
        "u",
        first.schema().clone(),
        first.root().cloned(),
        first.max_row_id(),
    )
    .unwrap();
    let mut writer = FileWriter::new(tmp.path()).unwrap();
    publish_table(
        &mut writer,
        &second,
        snapshot.transaction().tables().clone(),
        Some(repository.get_root(&root).unwrap().transaction_id()),
    )
    .unwrap();
    writer.commit().unwrap();

    let conn = setup_vtab(tmp.path(), "t", "tt");
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE other USING beech('{}','u')",
        tmp.path().display()
    ))
    .unwrap();
    // Force both virtual tables to retain snapshots before either one writes.
    assert_eq!(
        conn.query_row("SELECT v FROM other", [], |row| row.get::<_, i32>(0)).unwrap(),
        10
    );
    for value in 11..15 {
        conn.execute("UPDATE tt SET v=?", [value]).unwrap();
        conn.execute("UPDATE other SET v=?", [value * 10]).unwrap();
    }
    drop(conn);
    let first = setup_vtab(tmp.path(), "t", "tt");
    let second = setup_vtab(tmp.path(), "u", "tt");
    assert_eq!(rows(&first), vec![(1, 1, 14)]);
    assert_eq!(rows(&second), vec![(1, 1, 140)]);
}

#[test]
fn commit_preserves_live_view_after_non_monotonic_key_changes() {
    let tmp = make_test_tree((0..200).map(|i| int_row(i, i as i32)).collect(), vec![0], "t");
    let mut conn = setup_vtab(tmp.path(), "t", "tt");
    let transaction = conn.transaction().unwrap();
    transaction.execute("UPDATE tt SET k=1000-k,v=v+1", []).unwrap();
    transaction.execute("DELETE FROM tt WHERE k%3=0", []).unwrap();
    transaction.execute("INSERT INTO tt(rowid,k,v) VALUES(5000,-1,42)", []).unwrap();
    transaction.execute("UPDATE tt SET k=2000 WHERE k=-1", []).unwrap();
    let expected = rows(&transaction);
    assert_eq!(expected.len(), 134);
    transaction.commit().unwrap();
    assert_eq!(rows(&conn), expected);
    let reopened = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(rows(&reopened), expected);
}
