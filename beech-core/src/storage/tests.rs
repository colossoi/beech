use super::*;
use crate::codec::FormatTag;
use crate::{
    test_support::{MemoryStore, batch_from_rows, build, rows, schema},
    *,
};
use arrow_array::{Int32Array, Int64Array};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

fn read(leaf: &Leaf, columns: &[usize], batch_size: usize) -> Vec<RecordBatch> {
    leaf.read(columns, batch_size).unwrap().collect::<Result<Vec<_>>>().unwrap()
}
fn first_i64(batch: &RecordBatch, column: usize) -> i64 {
    batch.column(column).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}
fn save(store: &MemoryStore, object: codec::EncodedObject) -> Id {
    store.put(object.id(), object.bytes().clone()).unwrap();
    object.id()
}
fn publish(store: &MemoryStore, table: &Table, previous: Id, micros: u64) -> (Id, Id) {
    let table_id = save(store, codec::thrift::encode_table(table).unwrap());
    let txn = Transaction::new(
        previous,
        UNIX_EPOCH + Duration::from_micros(micros),
        BTreeMap::from([(table.name().to_owned(), table_id)]),
    )
    .unwrap();
    let txn_id = save(store, codec::thrift::encode_transaction(&txn).unwrap());
    (
        save(store, codec::thrift::encode_root(&Root::new(txn_id)).unwrap()),
        txn_id,
    )
}

#[test]
fn projections_and_batch_sizes_reuse_decoded_buffers() {
    let s = schema();
    let (table, store, _) = build(&s, &rows(12), 12, 2);
    let reference = table.root().unwrap();
    let repository = Repository::with_options(
        store.clone(),
        RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    let leaf = repository.open_leaf(reference, &s).unwrap();
    let opens = store.opens();
    let again = repository.open_leaf(reference, &s).unwrap();
    assert_eq!(
        store.opens(),
        opens,
        "immutable footer hits need no I/O or rehashing"
    );
    let reader = leaf.read(&[1, 2], 3).unwrap();
    assert_eq!(repository.stats().unwrap().columns.loads, 0, "reads are lazy");
    let a = reader.collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(
        a.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        [3, 3, 3, 3]
    );
    assert_eq!(repository.stats().unwrap().columns.loads, 2);
    let b = read(&again, &[3, 2, 2], 5);
    assert_eq!(b.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(), [5, 5, 2]);
    let a_values = a[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap().values();
    let b_values = b[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap().values();
    assert_eq!(
        a_values.as_ptr(),
        b_values.as_ptr(),
        "overlapping projections share decoded buffers"
    );
    let stats = repository.stats().unwrap();
    assert_eq!(stats.columns.loads, 3);
    assert_eq!(stats.columns.hits, 1);
    let before = store.opens();
    read(&leaf, &[1, 2, 3], 11);
    assert_eq!(store.opens(), before, "warm columns do not read encoded bytes");
    let empty = read(&leaf, &[], 8);
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 12);
    assert!(empty.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(repository.stats().unwrap().columns.loads, 3);
}

#[test]
fn snapshots_share_unchanged_objects_and_old_roots_remain_readable() {
    let s = schema();
    let old_rows = rows(12);
    let (old_table, store, _) = build(&s, &old_rows, 4, 3);
    let (old_root, old_txn) = publish(&store, &old_table, Id::default(), 0);
    let schema_id = save(&store, codec::thrift::encode_schema(&s).unwrap());
    let mut new_rows = old_rows.clone();
    new_rows.last_mut().unwrap().1[2] = Scalar::Utf8("changed".into());
    let (new_table, _, objects) = build(&s, &new_rows, 4, 3);
    for object in &objects {
        store.put(object.reference().id(), object.bytes().clone()).unwrap();
    }
    let (new_root, _) = publish(&store, &new_table, old_txn, 1);
    let repository = Arc::new(Repository::new(store));
    assert!(Arc::ptr_eq(
        &repository.get_root(&old_root).unwrap(),
        &repository.get_root(&old_root).unwrap()
    ));
    assert!(Arc::ptr_eq(
        &repository.get_schema(&schema_id).unwrap(),
        &repository.get_schema(&schema_id).unwrap()
    ));
    let old = repository.snapshot(old_root).unwrap();
    let old_again = repository.snapshot(old_root).unwrap();
    assert!(Arc::ptr_eq(old.transaction(), old_again.transaction()));
    let table = old.table("items").unwrap();
    assert!(Arc::ptr_eq(&table, &old_again.table("items").unwrap()));
    assert!(Arc::ptr_eq(
        &old.get_internal(table.root().unwrap(), &s).unwrap(),
        &old_again.get_internal(table.root().unwrap(), &s).unwrap(),
    ));
    let scan = |snapshot: &Snapshot| {
        query::RowCursor::new(snapshot, &snapshot.table("items").unwrap(), vec![])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(scan(&old), old_rows);
    let loaded = repository.stats().unwrap().columns.loads;
    let new = repository.snapshot(new_root).unwrap();
    assert_ne!(old.root_id(), new.root_id());
    assert_eq!(new.transaction().previous_id(), old_txn);
    assert_eq!(scan(&new), new_rows);
    assert_eq!(
        repository.stats().unwrap().columns.loads - loaded,
        5,
        "only the changed leaf's five physical columns are decoded"
    );
    let loaded = repository.stats().unwrap().columns.loads;
    assert_eq!(scan(&old), old_rows);
    assert_eq!(repository.stats().unwrap().columns.loads, loaded);
    drop(repository);
    assert_eq!(scan(&old), old_rows, "snapshots retain shared repository access");
}

#[test]
fn column_eviction_is_incremental_and_keeps_live_arrays_valid() {
    let s = schema();
    let (_, store, objects) = build(&s, &rows(12), 4, 3);
    let calibration = Repository::new(store.clone());
    let leaf = calibration.open_leaf(objects[0].reference(), &s).unwrap();
    read(&leaf, &[1], 4);
    let weight = calibration.stats().unwrap().columns.bytes;
    let repository = Repository::with_options(
        store,
        RepositoryOptions {
            column_cache_bytes: weight * 2,
            ..Default::default()
        },
    );
    let leaves =
        objects[..3].iter().map(|o| repository.open_leaf(o.reference(), &s).unwrap()).collect::<Vec<_>>();
    read(&leaves[0], &[1], 4);
    let retained = read(&leaves[1], &[1], 4);
    read(&leaves[0], &[1], 4); // Promote A, so inserting C evicts B.
    read(&leaves[2], &[1], 4);
    let stats = repository.stats().unwrap();
    assert_eq!(stats.columns.entries, 2);
    assert_eq!(stats.columns.evictions, 1);
    assert!(stats.columns.bytes <= weight * 2);
    assert_eq!(stats.metadata.entries, 3);
    assert_eq!(stats.metadata.evictions, 0);
    read(&leaves[0], &[1], 4);
    assert_eq!(repository.stats().unwrap().columns.loads, 3, "hot A survived");
    assert_eq!(
        first_i64(&retained[0], 0),
        4,
        "eviction does not invalidate live arrays"
    );
    read(&leaves[1], &[1], 4);
    assert_eq!(repository.stats().unwrap().columns.loads, 4, "evicted B reloads");
}

#[test]
fn metadata_uses_its_own_budget_and_promotes_hits() {
    let store = Arc::new(MemoryStore::default());
    let roots = (1..=3)
        .map(|n| {
            save(
                &store,
                codec::thrift::encode_root(&Root::new(Id::from(n))).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let calibration = Repository::new(store.clone());
    calibration.get_root(&roots[0]).unwrap();
    let weight = calibration.stats().unwrap().metadata.bytes;
    let repository = Repository::with_options(
        store,
        RepositoryOptions {
            metadata_cache_bytes: 2 * weight,
            ..Default::default()
        },
    );
    let a = repository.get_root(&roots[0]).unwrap();
    let b = repository.get_root(&roots[1]).unwrap();
    assert!(Arc::ptr_eq(&a, &repository.get_root(&roots[0]).unwrap()));
    repository.get_root(&roots[2]).unwrap();
    assert!(Arc::ptr_eq(&a, &repository.get_root(&roots[0]).unwrap()));
    let stats = repository.stats().unwrap().metadata;
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.evictions, 1);
    assert!(stats.bytes <= weight * 2);
    assert!(!Arc::ptr_eq(&b, &repository.get_root(&roots[1]).unwrap()));
    assert_eq!(b.transaction_id(), Id::from(2));
}

#[test]
fn zero_and_oversized_budgets_serve_without_retaining() {
    for budget in [0, 1] {
        let s = schema();
        let (table, store, _) = build(&s, &rows(4), 4, 2);
        let repository = Repository::with_options(
            store,
            RepositoryOptions {
                metadata_cache_bytes: budget,
                column_cache_bytes: budget,
                ..Default::default()
            },
        );
        let leaf = repository.open_leaf(table.root().unwrap(), &s).unwrap();
        repository.open_leaf(table.root().unwrap(), &s).unwrap();
        assert_eq!(
            read(&leaf, &[1], 2).iter().map(RecordBatch::num_rows).sum::<usize>(),
            4
        );
        read(&leaf, &[1], 3);
        let stats = repository.stats().unwrap();
        for stats in [stats.metadata, stats.columns] {
            assert_eq!(stats.loads, 2);
            assert_eq!(stats.entries, 0);
            assert_eq!(stats.bytes, 0);
        }
    }
}

struct PausingStore {
    store: Arc<MemoryStore>,
    pause: AtomicBool,
    entered: mpsc::Sender<()>,
    released: (Mutex<bool>, Condvar),
    calls: AtomicUsize,
}
impl BackingStore for PausingStore {
    fn get(&self, id: &Id) -> Result<ObjectFile> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            let (lock, wake) = &self.released;
            let _guard = wake.wait_while(lock.lock().unwrap(), |released| !*released).unwrap();
        }
        self.store.get(id)
    }
}
#[test]
fn simultaneous_column_misses_share_one_load_even_without_retention() {
    for budget in [0, 1024 * 1024] {
        let s = schema();
        let (table, store, _) = build(&s, &rows(12), 12, 2);
        let (entered, received) = mpsc::channel();
        let store = Arc::new(PausingStore {
            store,
            pause: AtomicBool::new(false),
            entered,
            released: (Mutex::new(false), Condvar::new()),
            calls: AtomicUsize::new(0),
        });
        let repository = Arc::new(Repository::with_options(
            store.clone(),
            RepositoryOptions {
                column_cache_bytes: budget,
                ..Default::default()
            },
        ));
        let leaf = repository.open_leaf(table.root().unwrap(), &s).unwrap();
        store.pause.store(true, Ordering::SeqCst);
        let mut threads = vec![];
        for _ in 0..8 {
            let leaf = leaf.clone();
            threads.push(std::thread::spawn(move || read(&leaf, &[1], 12).pop().unwrap()));
        }
        received.recv_timeout(Duration::from_secs(10)).unwrap();
        // Release the loader even on assertion failure, so the test cannot strand workers.
        let deadline = Instant::now() + Duration::from_secs(10);
        while repository.stats().unwrap().columns.misses < 8 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let misses = repository.stats().unwrap().columns.misses;
        *store.released.0.lock().unwrap() = true;
        store.released.1.notify_all();
        let batches = threads.into_iter().map(|t| t.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(misses, 8);
        assert_eq!(
            store.calls.load(Ordering::SeqCst),
            2,
            "one footer load and one column load"
        );
        assert_eq!(repository.stats().unwrap().columns.loads, 1);
        let ptr = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        for batch in &batches {
            assert_eq!(
                batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(),
                ptr
            );
        }
        assert_eq!(
            repository.stats().unwrap().columns.entries,
            usize::from(budget != 0)
        );
    }
}

#[test]
fn unrelated_loads_proceed_while_one_key_is_loading() {
    let cache = Arc::new(cache::Cache::<u8, u8>::new(1024, |_| 1));
    let (entered, received) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let other = cache.clone();
    let worker = std::thread::spawn(move || {
        other
            .get_or_load(1, || {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(1)
            })
            .unwrap()
    });
    received.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(*cache.get_or_load(2, || Ok(2)).unwrap(), 2);
    release.send(()).unwrap();
    assert_eq!(*worker.join().unwrap(), 1);
}

#[test]
fn failed_loads_are_retryable_and_bad_references_cannot_poison_cache() {
    let s = schema();
    let (table, store, _) = build(&s, &rows(4), 4, 2);
    let repository = Repository::new(store.clone());
    let root = codec::thrift::encode_root(&Root::new(Id::from(1))).unwrap();
    assert!(matches!(
        repository.get_root(&root.id()),
        Err(BeechError::NotFound(_))
    ));
    save(&store, root.clone());
    let loaded = repository.get_root(&root.id()).unwrap();
    assert!(Arc::ptr_eq(&loaded, &repository.get_root(&root.id()).unwrap()));
    let good = table.root().unwrap();
    let mut bad = good.clone();
    bad.row_count += 1;
    assert!(repository.open_leaf(&bad, &s).is_err());
    let leaf = repository.open_leaf(good, &s).unwrap();
    assert!(
        repository.open_leaf(&bad, &s).is_err(),
        "hits still validate reference counts"
    );
    let wrong_schema =
        TableSchema::new(vec![Field::new("other", DataType::Int64, false)], vec![0]).unwrap();
    assert!(repository.open_leaf(good, &wrong_schema).is_err());
    assert_eq!(first_i64(&read(&leaf, &[1], 4)[0], 0), 0);
}

#[test]
fn decoder_chunks_do_not_change_leaf_batching() {
    let s = TableSchema::new(vec![Field::new("key", DataType::Int64, false)], vec![0]).unwrap();
    let rows = (0..65_540).map(|i| (i, vec![Scalar::Int64(i)])).collect::<Vec<_>>();
    let object = crate::codec::parquet::encode_leaf(&s, &batch_from_rows(&s, &rows).unwrap()).unwrap();
    let store = MemoryStore::default();
    store.put(object.reference().id(), object.bytes().clone()).unwrap();
    let repository = Repository::new(store);
    let leaf = repository.open_leaf(object.reference(), &s).unwrap();
    let batches = read(&leaf, &[1], 65_538);
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        [65_538, 2]
    );
    assert_eq!(first_i64(&batches[1], 0), 65_538);
    read(&leaf, &[1], 3000);
    assert_eq!(repository.stats().unwrap().columns.loads, 1);
}

#[test]
fn leaves_require_exactly_one_row_group() {
    let schema = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec![0],
    )
    .unwrap();
    for (bytes, count) in [
        (
            include_bytes!("../../tests/fixtures/python-multi-row-group.parquet").as_slice(),
            2,
        ),
        (
            include_bytes!("../../tests/fixtures/python-no-row-groups.parquet").as_slice(),
            0,
        ),
    ] {
        let id = codec::object_id(FormatTag::Leaf, bytes);
        let store = MemoryStore::default();
        store.put(id, bytes::Bytes::copy_from_slice(bytes)).unwrap();
        let reference = NodeRef::new(&schema, id, 0, 4, vec![Scalar::Int64(9)]).unwrap();
        let repository = Repository::new(store);
        let Err(BeechError::InvalidNode(message)) = repository.open_leaf(&reference, &schema) else {
            panic!("expected row-group contract rejection");
        };
        assert!(message.contains(&id.to_string()));
        assert!(message.contains(&format!("expected exactly one Parquet row group, found {count}")));
        assert_eq!(repository.stats().unwrap().metadata.entries, 0);
        assert_eq!(repository.stats().unwrap().columns.loads, 0);
    }
}

#[test]
fn loading_panic_releases_coordination_for_retry() {
    let cache = cache::Cache::<u8, u8>::new(1024, |_| 1);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = cache.get_or_load(1, || -> Result<u8> { panic!("test loader panic") });
    }));
    assert!(result.is_err());
    assert_eq!(*cache.get_or_load(1, || Ok(7)).unwrap(), 7);
}

#[test]
fn concurrent_file_ranges_use_independent_offsets() {
    use std::io::Read;
    let directory = beech_disk::Workspace::new().unwrap();
    let path = directory.path().join("object");
    let bytes = (0..8192).map(|i| ((i * 31) ^ (i >> 8)) as u8).collect::<Vec<_>>();
    std::fs::write(&path, &bytes).unwrap();
    let file = ObjectFile::from_file(std::fs::File::open(path).unwrap()).unwrap();
    std::thread::scope(|scope| {
        for thread in 0..8 {
            let file = file.clone();
            let bytes = &bytes;
            scope.spawn(move || {
                for iteration in 0..100 {
                    let offset = (thread * 503 + iteration * 31) % 8000;
                    let mut reader = file.reader(offset as u64).unwrap();
                    let mut block = [0; 61];
                    reader.read_exact(&mut block[..29]).unwrap();
                    assert_eq!(
                        file.read_range((offset + 99) as u64, 31).unwrap().as_ref(),
                        &bytes[offset + 99..offset + 130]
                    );
                    reader.read_exact(&mut block[29..]).unwrap();
                    assert_eq!(&block, &bytes[offset..offset + 61]);
                }
            });
        }
    });
}

#[test]
fn positional_read_errors_reach_callers_and_parquet() {
    use std::io::Read;
    struct WriteOnlyStore(ObjectFile);
    impl BackingStore for WriteOnlyStore {
        fn get(&self, _: &Id) -> Result<ObjectFile> {
            Ok(self.0.clone())
        }
    }
    let schema = schema();
    let node =
        crate::codec::parquet::encode_leaf(&schema, &batch_from_rows(&schema, &rows(2)).unwrap()).unwrap();
    let directory = beech_disk::Workspace::new().unwrap();
    let path = directory.path().join("leaf");
    std::fs::write(&path, node.bytes()).unwrap();
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let object = ObjectFile::from_file(file).unwrap();
    assert!(object.read_range(0, 4).is_err());
    assert!(object.reader(0).unwrap().read(&mut [0; 4]).is_err());
    let repository = Repository::new(WriteOnlyStore(object));
    assert!(matches!(
        repository.open_leaf(node.reference(), &schema),
        Err(BeechError::Parquet(_))
    ));
    assert_eq!(repository.stats().unwrap().metadata.entries, 0);
}

#[test]
fn interleaved_file_readers_have_independent_positions() {
    use std::io::Read;
    let dir = beech_disk::Workspace::new().unwrap();
    let path = dir.path().join("bytes");
    std::fs::write(&path, b"0123456789abcdef").unwrap();
    let file = ObjectFile::from_file(std::fs::File::open(path).unwrap()).unwrap();
    let mut a = file.reader(0).unwrap();
    let mut b = file.reader(8).unwrap();
    let mut bytes = [0; 2];
    a.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"01");
    b.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"89");
    assert_eq!(file.read_range(12, 2).unwrap().as_ref(), b"cd");
    a.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"23");
    b.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ab");
}
