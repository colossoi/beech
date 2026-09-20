use super::*;
use beech_core::{
    codec,
    query::RowCursor,
    storage::{FileStore, Repository},
    DataType, Field, Scalar, TableSchema,
};
use std::{collections::BTreeMap, fs, sync::Arc};
fn schema() -> TableSchema {
    TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap()
}
fn rows() -> Vec<beech_core::Row> {
    vec![(1, vec![Scalar::Int64(1)]), (2, vec![Scalar::Int64(2)])]
}
fn publish(dir: &std::path::Path) -> Publication {
    let mut writer = FileWriter::new(dir).unwrap();
    let table = build_table(&mut writer, "t".into(), schema(), rows(), BuildOptions::default()).unwrap();
    let publication = publish_table(&mut writer, &table, BTreeMap::new(), None).unwrap();
    writer.commit().unwrap();
    publication
}
#[test]
fn validation_happens_before_staging() {
    let dir = beech_disk::Workspace::new().unwrap();
    let mut w = FileWriter::new(dir.path()).unwrap();
    assert!(BuildOptions::new(0, 1).is_err());
    assert!(BuildOptions::new(1, 0).is_err());
    for rows in [
        vec![(1, vec![Scalar::Utf8("bad".into())])],
        vec![(1, vec![Scalar::Int64(1)]), (2, vec![Scalar::Int64(1)])],
    ] {
        assert!(build_table(&mut w, "t".into(), schema(), rows, BuildOptions::default()).is_err());
        assert_eq!(w.num_to_commit(), 0);
    }
}
#[test]
fn publish_reopens_root_and_parquet_rows() {
    let dir = beech_disk::Workspace::new().unwrap();
    let publication = publish(dir.path());
    assert_eq!(
        fs::read_to_string(dir.path().join("root")).unwrap(),
        publication.root_id.to_string()
    );
    let repository = Arc::new(Repository::new(FileStore::new(dir.path())));
    let snapshot = repository.snapshot(publication.root_id).unwrap();
    let table = snapshot.table("t").unwrap();
    assert_eq!(
        RowCursor::new(&snapshot, &table, vec![]).unwrap().collect::<beech_core::Result<Vec<_>>>().unwrap(),
        rows()
    );
    let bytes = fs::read(dir.path().join(table.root().unwrap().id().to_string())).unwrap();
    assert_eq!(&bytes[..4], b"PAR1");
}
#[test]
fn abort_and_drop_preserve_committed_data() {
    let dir = beech_disk::Workspace::new().unwrap();
    let first = publish(dir.path());
    let original = fs::read(dir.path().join("root")).unwrap();
    for explicit_abort in [true, false] {
        let mut w = FileWriter::new(dir.path()).unwrap();
        let table = build_table(&mut w, "t".into(), schema(), vec![], BuildOptions::default()).unwrap();
        let next = publish_table(&mut w, &table, BTreeMap::new(), Some(first.transaction_id)).unwrap();
        assert_eq!(fs::read(dir.path().join("root")).unwrap(), original);
        if explicit_abort {
            w.abort().unwrap();
        } else {
            drop(w);
        }
        assert_eq!(fs::read(dir.path().join("root")).unwrap(), original);
        assert!(!dir.path().join(next.root_id.to_string()).exists());
        assert!(dir.path().join(first.root_id.to_string()).exists());
    }
}
#[test]
fn reusing_objects_never_stages_overwrites_or_deletes_them() {
    let dir = beech_disk::Workspace::new().unwrap();
    let first = publish(dir.path());
    let original = fs::read(dir.path().join(first.root_id.to_string())).unwrap();
    let mut w = FileWriter::new(dir.path()).unwrap();
    w.put(first.root_id, &original).unwrap();
    w.put(first.root_id, &original).unwrap();
    assert_eq!(w.num_to_commit(), 0);
    w.abort().unwrap();
    assert_eq!(
        fs::read(dir.path().join(first.root_id.to_string())).unwrap(),
        original
    );
}
#[test]
fn failed_commit_preserves_root() {
    let dir = beech_disk::Workspace::new().unwrap();
    publish(dir.path());
    let original = fs::read(dir.path().join("root")).unwrap();
    let mut w = FileWriter::new(dir.path()).unwrap();
    let table = build_table(&mut w, "other".into(), schema(), rows(), BuildOptions::default()).unwrap();
    let next = publish_table(&mut w, &table, BTreeMap::new(), None).unwrap();
    // A directory cannot be reused as an object if it appears after staging.
    fs::create_dir(dir.path().join(next.table_id.to_string())).unwrap();
    assert!(w.commit().is_err());
    assert_eq!(fs::read(dir.path().join("root")).unwrap(), original);
    assert!(dir.path().join(next.table_id.to_string()).is_dir());
}
#[test]
fn writer_lock_is_exclusive_and_released_on_drop() {
    let dir = beech_disk::Workspace::new().unwrap();
    let w = FileWriter::new(dir.path()).unwrap();
    assert!(FileWriter::new(dir.path()).is_err());
    drop(w);
    FileWriter::new(dir.path()).unwrap();
}
#[test]
fn snapshot_replacement_retains_other_tables_and_history() {
    let dir = beech_disk::Workspace::new().unwrap();
    let first = publish(dir.path());
    let mut w = FileWriter::new(dir.path()).unwrap();
    let repository = Repository::new(FileStore::new(dir.path()));
    let previous = repository.get_transaction(&first.transaction_id).unwrap();
    let table = build_table(&mut w, "other".into(), schema(), vec![], BuildOptions::default()).unwrap();
    let second = publish_table(
        &mut w,
        &table,
        previous.tables().clone(),
        Some(first.transaction_id),
    )
    .unwrap();
    w.commit().unwrap();
    let next = repository.get_transaction(&second.transaction_id).unwrap();
    assert_eq!(next.tables().len(), 2);
    assert_eq!(next.previous_id(), first.transaction_id);
    assert_eq!(
        repository.get_table(&next, "t").unwrap().root().unwrap().row_count(),
        2
    );
    assert!(repository.get_table(&next, "other").unwrap().root().is_none());
}
#[test]
fn cannot_stage_missing_root() {
    let dir = beech_disk::Workspace::new().unwrap();
    let mut w = FileWriter::new(dir.path()).unwrap();
    let root = codec::thrift::encode_root(&beech_core::Root::new(Id::default())).unwrap();
    assert!(w.stage_root(root.id()).is_err());
    w.put(root.id(), root.bytes()).unwrap();
    w.stage_root(root.id()).unwrap();
}

#[test]
fn commit_reuses_object_that_appeared_after_staging() {
    let dir = beech_disk::Workspace::new().unwrap();
    let object = codec::thrift::encode_root(&beech_core::Root::new(Id::default())).unwrap();
    let mut writer = FileWriter::new(dir.path()).unwrap();
    writer.put(object.id(), object.bytes()).unwrap();
    writer.put(object.id(), object.bytes()).unwrap();
    assert_eq!(writer.num_to_commit(), 1);
    assert!(!dir.path().join(object.id().to_string()).exists());
    fs::write(dir.path().join(object.id().to_string()), object.bytes()).unwrap();
    writer.commit().unwrap();
    assert_eq!(
        fs::read(dir.path().join(object.id().to_string())).unwrap(),
        object.bytes().as_ref()
    );
}

#[cfg(unix)]
#[test]
fn reuse_does_not_require_reading_existing_object_contents() {
    use std::os::unix::fs::PermissionsExt;
    let dir = beech_disk::Workspace::new().unwrap();
    let object = codec::thrift::encode_root(&beech_core::Root::new(Id::default())).unwrap();
    let path = dir.path().join(object.id().to_string());
    fs::write(&path, object.bytes()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let mut writer = FileWriter::new(dir.path()).unwrap();
    writer.put(object.id(), object.bytes()).unwrap();
    assert_eq!(writer.num_to_commit(), 0);
    writer.commit().unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(fs::read(path).unwrap(), object.bytes().as_ref());
}

#[test]
fn ordered_updates_publish_only_at_commit_and_abort_on_staging_failure() {
    use std::io;
    struct FailAfterOne<'a>(&'a mut FileWriter, usize);
    impl ObjectSink for FailAfterOne<'_> {
        fn put(&mut self, id: Id, bytes: &[u8]) -> io::Result<()> {
            if self.1 == 1 {
                return Err(io::Error::other("injected staging failure"));
            }
            self.1 += 1;
            self.0.put(id, bytes)
        }
    }
    let dir = beech_disk::Workspace::new().unwrap();
    let first = publish(dir.path());
    let repository = Arc::new(Repository::new(FileStore::new(dir.path())));
    for fail in [true, false] {
        let mut writer = FileWriter::new(dir.path()).unwrap();
        // Snapshot selection happens under the writer lock.
        let old = repository.snapshot(first.root_id).unwrap();
        let table = old.table("t").unwrap();
        let mut tx = Transaction::new(schema(), SortLimits::default()).unwrap();
        for key in 3..12 {
            tx.push(Change::Insert {
                key: vec![Scalar::Int64(key)],
                row_id: key,
                record: vec![Scalar::Int64(key)],
            })
            .unwrap();
        }
        if fail {
            assert!(tx
                .apply(
                    &table,
                    &old,
                    &mut FailAfterOne(&mut writer, 0),
                    BuildOptions::new(1, 1).unwrap()
                )
                .is_err());
            assert!(writer.num_to_commit() > 0);
            writer.abort().unwrap();
            assert_eq!(
                fs::read_to_string(dir.path().join("root")).unwrap(),
                first.root_id.to_string()
            );
            assert!(!fs::read_dir(dir.path()).unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".beech-stage-")));
        } else {
            let updated = tx.apply(&table, &old, &mut writer, BuildOptions::new(1, 1).unwrap()).unwrap();
            let next = publish_table(
                &mut writer,
                &updated,
                old.transaction().tables().clone(),
                Some(first.transaction_id),
            )
            .unwrap();
            assert_eq!(
                fs::read_to_string(dir.path().join("root")).unwrap(),
                first.root_id.to_string()
            );
            writer.commit().unwrap();
            assert_eq!(
                fs::read_to_string(dir.path().join("root")).unwrap(),
                next.root_id.to_string()
            );
            let new = repository.snapshot(next.root_id).unwrap();
            let new_table = new.table("t").unwrap();
            assert_eq!(RowCursor::new(&new, &new_table, vec![]).unwrap().count(), 11);
        }
        assert_eq!(
            RowCursor::new(&old, &table, vec![]).unwrap().collect::<beech_core::Result<Vec<_>>>().unwrap(),
            rows()
        );
    }
}
