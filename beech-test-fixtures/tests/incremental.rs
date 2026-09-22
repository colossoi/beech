use beech_core::{
    query::RowCursor,
    storage::{Leaf, Repository},
    DataType, Field, Id, InternalNode, NodeRef, NodeSource, Result, Row, Scalar, Table, TableSchema,
};
use beech_test_fixtures::build_simple_table;
use beech_write::{apply_changes, BuildOptions, Change, Writer};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
};
fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ],
        vec![0],
    )
    .unwrap()
}
fn row(key: i32, value: i32) -> Row {
    (i64::from(key), vec![Scalar::Int32(key), Scalar::Int32(value)])
}
fn read(source: &impl NodeSource, table: &Table) -> Vec<Row> {
    RowCursor::new(source, table, vec![]).unwrap().collect::<Result<_>>().unwrap()
}
struct PathOnly<'a> {
    source: &'a Repository,
    allowed: HashSet<Id>,
    visited: Mutex<HashSet<Id>>,
}
impl NodeSource for PathOnly<'_> {
    fn get_internal(&self, r: &NodeRef, s: &TableSchema) -> Result<Arc<InternalNode>> {
        assert!(self.allowed.contains(&r.id()), "read outside the edited path");
        self.visited.lock().unwrap().insert(r.id());
        self.source.get_internal(r, s)
    }
    fn open_leaf(&self, r: &NodeRef, s: &TableSchema) -> Result<Leaf> {
        assert!(self.allowed.contains(&r.id()), "read an unaffected leaf");
        self.visited.lock().unwrap().insert(r.id());
        self.source.open_leaf(r, s)
    }
}
#[test]
fn point_update_and_noop_only_read_the_affected_path() {
    let rows: Vec<_> = (0..1000).map(|i| row(i, i)).collect();
    let (store, source, table) = build_simple_table("t", rows.clone(), schema(), 256, 64).unwrap();
    let mut reference = table.root().unwrap().clone();
    assert!(reference.height() > 1);
    let mut allowed = HashSet::new();
    loop {
        allowed.insert(reference.id());
        if reference.height() == 0 {
            break;
        }
        let node = source.get_internal(&reference, table.schema()).unwrap();
        let index = node.seek(table.schema(), &vec![Scalar::Int32(500)]).unwrap().unwrap();
        reference = node.children()[index].clone();
    }
    let checked = PathOnly {
        source: &source,
        allowed,
        visited: Mutex::new(HashSet::new()),
    };
    let mut writer = store.writer();
    let change = |value| Change::Update {
        key: vec![Scalar::Int32(500)],
        row_id: 500,
        record: row(500, value).1,
    };
    let mut tx = beech_write::Transaction::new(schema(), beech_write::SortLimits::default()).unwrap();
    tx.push(change(500)).unwrap();
    let (unchanged, stats) =
        tx.apply_with_stats(&table, &checked, &mut writer, BuildOptions::new(256, 64).unwrap()).unwrap();
    assert_eq!(stats.operations, 1);
    assert_eq!(stats.no_op_updates, 1);
    assert_eq!(stats.leaf_visits, 1);
    assert_eq!(stats.branch_visits, table.root().unwrap().height() as u64);
    assert_eq!(stats.leaf_writes + stats.branch_writes, 0);
    assert_eq!(stats.leaves_staged + stats.branches_staged, 0);
    assert_eq!(stats.peak_scratch_bytes, stats.input_bytes);
    assert_eq!(&unchanged, table.as_ref());
    assert_eq!(writer.num_to_commit(), 0);
    let root = apply_changes(
        [change(501)],
        &table,
        &checked,
        &mut writer,
        BuildOptions::new(256, 64).unwrap(),
    )
    .unwrap();
    writer.commit().unwrap();
    assert_eq!(*checked.visited.lock().unwrap(), checked.allowed);
    let updated = root;
    let mut expected = rows.clone();
    expected[500] = row(500, 501);
    assert_eq!(read(&source, &updated), expected);
    assert_eq!(read(&source, &table), rows); // Old snapshot remains intact.
}
#[test]
fn mixed_edits_match_a_row_model_through_growth_and_deletion() {
    for options in [
        BuildOptions::new(1, 1).unwrap(),
        BuildOptions::new(128, 32).unwrap(),
    ] {
        let (store, source, initial) = build_simple_table("t", vec![], schema(), 128, 32).unwrap();
        let mut table = (*initial).clone();
        let mut model = BTreeMap::new();
        let mut seed = 7u32;
        for round in 0..60 {
            let mut changes = Vec::new();
            for _ in 0..12 {
                seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                let key = (seed % 80) as i32;
                let k = vec![Scalar::Int32(key)];
                let previous = model.remove(&key);
                let change = if previous.is_some() && seed.is_multiple_of(3) {
                    Change::Delete { key: k }
                } else {
                    model.insert(key, row(key, round));
                    if previous.is_some() {
                        Change::Update {
                            key: k,
                            row_id: i64::from(key),
                            record: row(key, round).1,
                        }
                    } else {
                        Change::Insert {
                            key: k,
                            row_id: i64::from(key),
                            record: row(key, round).1,
                        }
                    }
                };
                changes.push(change);
            }
            let mut writer = store.writer();
            let root = apply_changes(changes, &table, &source, &mut writer, options).unwrap();
            writer.commit().unwrap();
            table = root;
            assert_eq!(read(&source, &table), model.values().cloned().collect::<Vec<_>>());
            assert_eq!(table.root().map_or(0, |r| r.row_count()), model.len() as u64);
        }
        let mut writer = store.writer();
        let root = apply_changes(
            model.keys().map(|&k| Change::Delete {
                key: vec![Scalar::Int32(k)],
            }),
            &table,
            &source,
            &mut writer,
            options,
        )
        .unwrap();
        assert!(root.root().is_none());
        writer.commit().unwrap();
    }
}

#[test]
fn disk_transaction_applies_a_large_append_and_preserves_the_old_snapshot() {
    use beech_write::{SortLimits, Transaction};
    let (store, source, table) = build_simple_table("t", vec![row(0, 0)], schema(), 256, 64).unwrap();
    let mut transaction = Transaction::new(schema(), SortLimits::new(512, 3).unwrap()).unwrap();
    for key in (1..2000).rev() {
        let row = row(key, key);
        transaction
            .push(Change::Insert {
                key: vec![Scalar::Int32(key)],
                row_id: row.0,
                record: row.1,
            })
            .unwrap();
    }
    let mut writer = store.writer();
    let updated =
        transaction.apply(&table, &source, &mut writer, BuildOptions::new(256, 64).unwrap()).unwrap();
    writer.commit().unwrap();
    assert_eq!(updated.max_row_id(), 1999);
    assert_eq!(
        read(&source, &updated),
        (0..2000).map(|key| row(key, key)).collect::<Vec<_>>()
    );
    assert_eq!(read(&source, &table), vec![row(0, 0)]);
}

#[test]
fn duplicate_inserts_fail_before_object_writes() {
    use beech_write::{SortLimits, Transaction};
    let (store, source, table) = build_simple_table("t", vec![row(0, 0)], schema(), 256, 64).unwrap();
    let mut transaction = Transaction::new(schema(), SortLimits::new(1, 2).unwrap()).unwrap();
    for key in [10, 2, 7, 10] {
        transaction
            .push(Change::Insert {
                key: vec![Scalar::Int32(key)],
                row_id: key as i64,
                record: row(key, key).1,
            })
            .unwrap();
    }
    let mut writer = store.writer();
    assert!(transaction.apply(&table, &source, &mut writer, BuildOptions::default()).is_err());
    assert_eq!(writer.num_to_commit(), 0);
}

#[test]
fn ordered_repeated_keys_stage_only_the_final_leaf() {
    use beech_write::{ObjectSink, SortLimits, Transaction};
    struct CountSink<'a, W>(&'a mut W, usize);
    impl<W: ObjectSink> ObjectSink for CountSink<'_, W> {
        fn put(&mut self, id: Id, bytes: &[u8]) -> std::io::Result<()> {
            self.1 += 1;
            self.0.put(id, bytes)
        }
    }
    let (store, source, table) = build_simple_table("t", vec![row(0, 0)], schema(), 256, 64).unwrap();
    let mut transaction = Transaction::new(schema(), SortLimits::default()).unwrap();
    transaction
        .push(Change::Insert {
            key: vec![Scalar::Int32(1)],
            row_id: 100,
            record: row(1, 1).1,
        })
        .unwrap();
    for value in 2..100 {
        transaction
            .push(Change::Update {
                key: vec![Scalar::Int32(1)],
                row_id: 100,
                record: row(1, value).1,
            })
            .unwrap();
    }
    transaction
        .push(Change::Delete {
            key: vec![Scalar::Int32(1)],
        })
        .unwrap();
    transaction
        .push(Change::Insert {
            key: vec![Scalar::Int32(1)],
            row_id: 1,
            record: row(1, 999).1,
        })
        .unwrap();
    let mut writer = store.writer();
    let mut sink = CountSink(&mut writer, 0);
    let (updated, stats) = transaction
        .apply_with_stats(&table, &source, &mut sink, BuildOptions::new(100_000, 1).unwrap())
        .unwrap();
    assert_eq!(sink.1, 1, "intermediate versions must never reach the sink");
    assert_eq!(stats.operations, 101);
    assert_eq!(stats.leaf_visits, 101);
    assert_eq!(stats.branch_visits, 0);
    assert_eq!(stats.leaf_writes, 101);
    assert_eq!(stats.leaves_staged, 1);
    assert_eq!(stats.branches_staged, 0);
    assert_eq!(stats.final_height, Some(0));
    assert_eq!(stats.peak_scratch_bytes, stats.input_bytes);
    assert!(stats.peak_page_cache_bytes > 0);
    assert_eq!(stats.scratch_bytes_written, 0);
    assert!(stats.staged_bytes > 0);

    assert_eq!(updated.max_row_id(), 100);
    writer.commit().unwrap();
    assert_eq!(read(&source, &updated), vec![row(0, 0), row(1, 999)]);
    assert_eq!(read(&source, &table), vec![row(0, 0)]);
}

#[test]
fn ordered_edits_grow_collapse_empty_and_restart_the_tree() {
    use beech_write::{SortLimits, Transaction};
    let (store, source, table) = build_simple_table("t", vec![], schema(), 128, 32).unwrap();
    let mut tx = Transaction::new(schema(), SortLimits::default()).unwrap();
    for key in 0..80 {
        tx.push(Change::Insert {
            key: vec![Scalar::Int32(key)],
            row_id: key.into(),
            record: row(key, key).1,
        })
        .unwrap();
    }
    for key in (0..80).rev() {
        tx.push(Change::Delete {
            key: vec![Scalar::Int32(key)],
        })
        .unwrap();
    }
    tx.push(Change::Insert {
        key: vec![Scalar::Int32(-1)],
        row_id: 0,
        record: row(-1, 7).1,
    })
    .unwrap();
    let mut writer = store.writer();
    let updated = tx.apply(&table, &source, &mut writer, BuildOptions::new(1, 1).unwrap()).unwrap();
    assert_eq!(updated.root().unwrap().height(), 0);
    assert_eq!(updated.max_row_id(), 79);
    assert_eq!(writer.num_to_commit(), 1);
    writer.commit().unwrap();
    assert_eq!(read(&source, &updated), vec![(0, row(-1, 7).1)]);
}

#[test]
fn page_cache_limits_preserve_the_same_tree() {
    use beech_write::{SortLimits, Transaction};
    let (store, source, table) =
        build_simple_table("t", (0..50).map(|k| row(k, 0)).collect(), schema(), 256, 64).unwrap();
    let mut expected_root = None;
    for limit in [0, 128, 8 * 1024 * 1024] {
        let mut tx =
            Transaction::new(schema(), SortLimits::default()).unwrap().with_page_cache_bytes(limit);
        for step in 0..200 {
            let key = step % 50;
            tx.push(Change::Update {
                key: vec![Scalar::Int32(key)],
                row_id: key as i64,
                record: row(key, step).1,
            })
            .unwrap();
        }
        let mut writer = store.writer();
        let (updated, stats) =
            tx.apply_with_stats(&table, &source, &mut writer, BuildOptions::new(256, 64).unwrap()).unwrap();
        writer.commit().unwrap();
        assert_eq!(
            read(&source, &updated),
            (0..50).map(|k| row(k, 150 + k)).collect::<Vec<_>>()
        );
        let root = updated.root().unwrap().id();
        if let Some(expected) = expected_root {
            assert_eq!(root, expected);
        }
        expected_root = Some(root);
        assert!(stats.peak_page_cache_bytes <= limit);
        if limit == 8 * 1024 * 1024 {
            assert_eq!(stats.scratch_bytes_written, 0);
        } else {
            assert!(stats.scratch_bytes_written > 0);
        }
    }
}
