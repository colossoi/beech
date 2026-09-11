use crate::codec::FormatTag;
use crate::{
    query::{ConstraintOp as Op, KeyRange, Predicate, RowCursor, Scan, ScanRequest},
    storage::{FileStore, Repository},
    test_support::*,
    *,
};
use std::{ops::Bound, sync::Arc};
fn values(batches: Vec<RecordBatch>) -> Vec<Vec<Scalar>> {
    batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|r| {
                b.columns().iter().map(|a| Scalar::from_array(a.as_ref(), r).unwrap()).collect::<Vec<_>>()
            })
        })
        .collect()
}
#[test]
fn cursor_full_scan_crosses_every_separator_and_preserves_ids() {
    for (n, leaf_rows, fanout) in [(0, 3, 2), (1, 4, 3), (67, 3, 2), (93, 7, 4)] {
        let data = rows(n);
        let (table, store, _) = build(&schema(), &data, leaf_rows, fanout);
        let src = Repository::new(store);
        assert_eq!(
            RowCursor::new(&src, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
            data
        );
    }
}
#[test]
fn every_point_and_range_matches_a_reference_with_and_without_pruning() {
    let data = rows(31);
    let (table, store, _) = build(&schema(), &data, 3, 2);
    let src = Repository::new(store);
    for op in [Op::Eq, Op::Gt, Op::Ge, Op::Lt, Op::Le] {
        for bound in -1..33 {
            let expected: Vec<_> = data
                .iter()
                .filter(|row| {
                    let k = row.0 - 10000;
                    match op {
                        Op::Eq => k == bound,
                        Op::Gt => k > bound,
                        Op::Ge => k >= bound,
                        Op::Lt => k < bound,
                        Op::Le => k <= bound,
                        _ => false,
                    }
                })
                .map(|r| vec![r.1[0].clone()])
                .collect();
            for stats in [true, false] {
                let mut req = ScanRequest::all(&table);
                req.projection = vec![0];
                req.batch_size = 2;
                req.use_statistics = stats;
                req.predicates = vec![Predicate::new(0, op, Scalar::Int64(bound))];
                assert_eq!(
                    values(Scan::new(&src, &table, req).unwrap().collect::<Result<_>>().unwrap()),
                    expected,
                    "{op:?} {bound}, stats={stats}"
                );
            }
        }
    }
}
#[test]
fn composite_prefix_and_open_closed_bounds_are_correct() {
    let s = TableSchema::new(
        vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ],
        vec![0, 1],
    )
    .unwrap();
    let data: Vec<_> =
        (0..42).map(|i| (i, vec![Scalar::Int32((i / 7) as i32), Scalar::Int64(i % 7)])).collect();
    let (table, store, _) = build(&s, &data, 4, 2);
    let src = Repository::new(store);
    for a in 0..6 {
        for b in -1..8 {
            let mut req = ScanRequest::all(&table);
            req.predicates = vec![
                Predicate::new(0, Op::Eq, Scalar::Int32(a)),
                Predicate::new(1, Op::Gt, Scalar::Int64(b)),
                Predicate::new(1, Op::Le, Scalar::Int64(5)),
            ];
            let expected: Vec<_> = data
                .iter()
                .filter(|r| r.1[0] == Scalar::Int32(a) && matches!(r.1[1],Scalar::Int64(k) if k>b && k<=5))
                .map(|r| r.1.clone())
                .collect();
            assert_eq!(
                values(Scan::new(&src, &table, req).unwrap().collect::<Result<_>>().unwrap()),
                expected
            );
        }
        let mut req = ScanRequest::all(&table);
        req.range = KeyRange::prefix(vec![Scalar::Int32(a)]);
        assert_eq!(
            Scan::new(&src, &table, req).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>(),
            7
        );
    }
    let mut req = ScanRequest::all(&table);
    req.range = KeyRange {
        lower: Bound::Excluded(vec![Scalar::Int32(2)]),
        upper: Bound::Included(vec![Scalar::Int32(3)]),
    };
    assert_eq!(
        values(Scan::new(&src, &table, req).unwrap().collect::<Result<_>>().unwrap()),
        data[21..28].iter().map(|r| r.1.clone()).collect::<Vec<_>>()
    );
}

#[test]
fn string_and_binary_composite_keys_filter_sliced_batches() {
    use Scalar::{Binary, Null, Utf8};

    let s = TableSchema::new(
        vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, true),
        ],
        vec![2, 0],
    )
    .unwrap();
    let data = vec![
        (0, vec![Null, Utf8("keep".into()), Null]),
        (1, vec![Binary(vec![]), Utf8("keep".into()), Null]),
        (2, vec![Binary(vec![]), Utf8("skip".into()), Utf8("".into())]),
        (3, vec![Binary(vec![0]), Utf8("keep".into()), Utf8("a".into())]),
        (4, vec![Binary(vec![0, 255]), Null, Utf8("a".into())]),
        (5, vec![Binary(vec![1]), Utf8("keep".into()), Utf8("a".into())]),
        (6, vec![Binary(vec![255]), Utf8("keep".into()), Utf8("é".into())]),
    ];
    let batch = batch_from_rows(&s, &data).unwrap();
    assert_eq!(
        value::validate_leaf(&s, &batch.slice(1, 6)).unwrap(),
        vec![Utf8("é".into()), Binary(vec![255])]
    );
    let mut invalid = data.clone();
    invalid.swap(3, 4);
    assert!(value::validate_leaf(&s, &batch_from_rows(&s, &invalid).unwrap()).is_err());
    invalid[4] = invalid[3].clone();
    assert!(value::validate_leaf(&s, &batch_from_rows(&s, &invalid).unwrap()).is_err());

    let (table, store, _) = build(&s, &data, data.len(), 2);
    let source = Repository::new(store);
    for (range, expected) in [
        (KeyRange::prefix(vec![Null]), vec![0, 1]),
        (KeyRange::prefix(vec![Utf8("a".into())]), vec![3, 4, 5]),
        (
            KeyRange {
                lower: Bound::Included(vec![Utf8("a".into()), Binary(vec![0])]),
                upper: Bound::Excluded(vec![Utf8("a".into()), Binary(vec![1])]),
            },
            vec![3, 4],
        ),
    ] {
        let mut request = ScanRequest::all(&table);
        request.batch_size = 2;
        request.range = range;
        assert_eq!(
            values(Scan::new(&source, &table, request).unwrap().collect::<Result<_>>().unwrap()),
            expected.into_iter().map(|i| data[i].1.clone()).collect::<Vec<_>>()
        );
    }
    let mut request = ScanRequest::all(&table);
    request.batch_size = 2;
    request.projection = vec![2, 0];
    request.predicates = vec![
        Predicate::new(1, Op::Eq, Utf8("keep".into())),
        Predicate::new(0, Op::Ge, Binary(vec![0])),
    ];
    assert_eq!(
        values(Scan::new(&source, &table, request).unwrap().collect::<Result<_>>().unwrap()),
        [3, 5, 6].map(|i| vec![data[i].1[2].clone(), data[i].1[0].clone()])
    );
}
#[test]
fn projection_order_predicate_only_columns_and_nulls() {
    let data = rows(55);
    let (table, store, _) = build(&schema(), &data, 8, 3);
    let src = Repository::new(store);
    let mut req = ScanRequest::all(&table);
    req.projection = vec![2, 0];
    req.include_row_id = true;
    req.predicates = vec![
        Predicate::new(1, Op::Ge, Scalar::Int32(2)),
        Predicate::new(2, Op::IsNotNull, Scalar::Null),
    ];
    let got = values(Scan::new(&src, &table, req).unwrap().collect::<Result<_>>().unwrap());
    let expected = data
        .iter()
        .filter(|r| r.0 >= 10020 && !r.1[2].is_null())
        .map(|r| vec![Scalar::Int64(r.0), r.1[2].clone(), r.1[0].clone()])
        .collect::<Vec<_>>();
    assert_eq!(got, expected);
    let mut req = ScanRequest::all(&table);
    req.projection = vec![];
    req.predicates = vec![Predicate::new(2, Op::IsNull, Scalar::Null)];
    let batches = Scan::new(&src, &table, req).unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 8);
    assert!(batches.iter().all(|b| b.num_columns() == 0));
}
#[test]
fn non_key_statistics_skip_groups_and_missing_stats_read_rows() {
    let (table, store, _) = build(&schema(), &rows(100), 10, 3);
    let src = Repository::new(store);
    let mut req = ScanRequest::all(&table);
    req.predicates = vec![Predicate::new(1, Op::Eq, Scalar::Int32(5))];
    let mut scan = Scan::new(&src, &table, req).unwrap();
    assert_eq!(scan.by_ref().map(|b| b.unwrap().num_rows()).sum::<usize>(), 10);
    assert_eq!(scan.metrics().pruned_leaves, 9);
    // Independent writer configuration intentionally omits statistics.
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::KeyValue,
            properties::{EnabledStatistics, WriterProperties},
        },
    };
    let s = schema();
    let batch = batch_from_rows(&s, &rows(12)).unwrap();
    let mut bytes = vec![];
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_key_value_metadata(Some(vec![
            KeyValue::new("beech.format".into(), Some("1".into())),
            KeyValue::new(
                "beech.schema".into(),
                Some(codec::thrift::encode_schema(&s).unwrap().id().to_string()),
            ),
        ]))
        .build();
    let mut w = ArrowWriter::try_new(&mut bytes, s.physical_schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let store = Arc::new(test_support::MemoryStore::default());
    let id = codec::object_id(FormatTag::Leaf, &bytes);
    store.put(id, bytes).unwrap();
    let table = Table::new(
        "missing stats",
        s,
        Some(NodeRef {
            id,
            height: 0,
            row_count: 12,
            max_key: vec![Scalar::Int64(11)],
        }),
    )
    .unwrap();
    let src = Repository::new(store);
    let mut req = ScanRequest::all(&table);
    req.predicates = vec![Predicate::new(1, Op::Eq, Scalar::Int32(1))];
    let mut scan = Scan::new(&src, &table, req).unwrap();
    assert_eq!(scan.by_ref().map(|b| b.unwrap().num_rows()).sum::<usize>(), 2);
    assert_eq!(scan.metrics().pruned_leaves, 0);
}
#[test]
fn float_predicates_do_not_confuse_total_key_order_with_numeric_equality() {
    let s = TableSchema::new(vec![Field::new("key", DataType::Float64, true)], vec![0]).unwrap();
    let data = vec![
        (0, vec![Scalar::Null]),
        (1, vec![Scalar::Float64(f64::NEG_INFINITY)]),
        (2, vec![Scalar::Float64(-0.0)]),
        (3, vec![Scalar::Float64(0.0)]),
        (4, vec![Scalar::Float64(f64::INFINITY)]),
        (5, vec![Scalar::Float64(f64::NAN)]),
    ];
    let (table, store, _) = build(&s, &data, data.len(), 2);
    let src = Repository::new(store);
    for stats in [true, false] {
        for (op, val, expected) in [
            (Op::Eq, Scalar::Float64(0.0), 2),
            (Op::Eq, Scalar::Float64(f64::NAN), 0),
            (Op::Eq, Scalar::Null, 0),
            (Op::IsNull, Scalar::Null, 1),
            (Op::Gt, Scalar::Float64(0.0), 1),
        ] {
            let mut req = ScanRequest::all(&table);
            req.use_statistics = stats;
            req.predicates = vec![Predicate::new(0, op, val)];
            assert_eq!(
                Scan::new(&src, &table, req).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>(),
                expected
            );
        }
    }
}
#[test]
fn unsigned_values_across_signed_boundary_filter_correctly() {
    let s = TableSchema::new(vec![Field::new("key", DataType::UInt64, false)], vec![0]).unwrap();
    let data = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX]
        .iter()
        .enumerate()
        .map(|(i, &v)| (i as i64, vec![Scalar::UInt64(v)]))
        .collect::<Vec<_>>();
    let (table, store, _) = build(&s, &data, 3, 2);
    let src = Repository::new(store);
    for value in data.iter().map(|r| r.1[0].clone()) {
        assert_eq!(
            RowCursor::new(&src, &table, vec![Predicate::new(0, Op::Eq, value.clone())])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .iter()
                .map(|r| r.1[0].clone())
                .collect::<Vec<_>>(),
            vec![value]
        );
    }
}
#[test]
fn invalid_queries_and_broken_references_return_errors_once() {
    let (table, store, _) = build(&schema(), &rows(20), 4, 2);
    let src = Repository::new(store);
    for projection in [vec![0, 0], vec![99]] {
        let mut req = ScanRequest::all(&table);
        req.projection = projection;
        assert!(Scan::new(&src, &table, req).is_err());
    }
    for p in [
        Predicate::new(0, Op::Unknown, Scalar::Int64(1)),
        Predicate::new(99, Op::Eq, Scalar::Int64(1)),
        Predicate::new(0, Op::Eq, Scalar::Utf8("1".into())),
    ] {
        let mut req = ScanRequest::all(&table);
        req.predicates = vec![p];
        assert!(Scan::new(&src, &table, req).is_err());
    }
    let mut broken = table.clone();
    broken.root.as_mut().unwrap().id = Id::from(1234);
    let mut scan = Scan::new(&src, &broken, ScanRequest::all(&broken)).unwrap();
    assert!(scan.next().unwrap().is_err());
    assert!(scan.next().is_none());
}
#[test]
fn cached_internal_nodes_still_validate_reference_and_schema() {
    let (table, store, _) = build(&schema(), &rows(12), 4, 3);
    let src = Repository::new(store);
    let r = table.root.unwrap();
    src.get_internal(&r, &table.schema).unwrap();
    let mut bad = r.clone();
    bad.row_count += 1;
    assert!(src.get_internal(&bad, &table.schema).is_err());
    let s = TableSchema::new(vec![Field::new("key", DataType::Int32, false)], vec![0]).unwrap();
    assert!(src.get_internal(&r, &s).is_err());
}
#[test]
fn narrow_projection_retains_less_decoded_data_and_local_files_work() {
    let s = schema();
    let mut data = rows(1000);
    for (i, row) in data.iter_mut().enumerate() {
        let mut payload = vec![];
        for j in 0..64 {
            payload.extend(codec::object_id(FormatTag::Leaf, format!("{i}/{j}").as_bytes()).as_bytes());
        }
        row.1[3] = Scalar::Binary(payload);
    }
    let (table, store, nodes) = build(&s, &data, 1000, 2);
    let src = Repository::new(store);
    src.open_leaf(table.root.as_ref().unwrap(), &s).unwrap();
    let mut req = ScanRequest::all(&table);
    req.projection = vec![0];
    assert_eq!(
        Scan::new(&src, &table, req).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>(),
        1000
    );
    let narrow = src.stats().unwrap().columns.bytes;
    assert_eq!(
        Scan::new(&src, &table, ScanRequest::all(&table))
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum::<usize>(),
        1000
    );
    let wide = src.stats().unwrap().columns.bytes;
    assert!(narrow * 4 < wide, "narrow={narrow}, wide={wide}");
    let dir = tempfile::tempdir().unwrap();
    let files = FileStore::new(dir.path());
    std::fs::write(files.object_path(&nodes[0].reference.id), &nodes[0].bytes).unwrap();
    let src = Repository::with_options(
        files,
        storage::RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    assert_eq!(
        RowCursor::new(&src, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
        data
    );
}
#[test]
fn corrupt_parquet_and_hash_mismatch_are_errors() {
    let (table, _, nodes) = build(&schema(), &rows(2), 3, 2);
    let dir = tempfile::tempdir().unwrap();
    let files = FileStore::new(dir.path());
    let r = table.root.as_ref().unwrap();
    let mut bytes = nodes[0].bytes.to_vec();
    bytes.truncate(bytes.len() - 5);
    std::fs::write(files.object_path(&r.id), bytes).unwrap();
    let src = Repository::with_options(
        files,
        storage::RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    assert!(matches!(
        src.open_leaf(r, &table.schema),
        Err(BeechError::HashMismatch(_))
    ));
    let files = FileStore::new(dir.path());
    let src = Repository::new(files);
    assert!(src.open_leaf(r, &table.schema).is_err());
}
#[test]
fn deterministic_prolly_rebuilds_reuse_subtrees_and_old_roots_remain_readable() {
    let s = schema();
    let data = rows(250);
    let (old, store, objects) = build_prolly(&s, &data);
    let (same, _, same_objects) = build_prolly(&s, &data);
    assert_eq!(old, same);
    assert_eq!(
        objects.iter().map(|n| n.reference.id).collect::<Vec<_>>(),
        same_objects.iter().map(|n| n.reference.id).collect::<Vec<_>>()
    );
    for change in 0..3 {
        let mut modified = data.clone();
        match change {
            0 => modified.last_mut().unwrap().1[2] = Scalar::Utf8("updated".into()),
            1 => {
                modified.pop();
            }
            _ => modified.push(rows(251).pop().unwrap()),
        }
        let (new, _, new_objects) = build_prolly(&s, &modified);
        assert_ne!(old.root, new.root);
        for n in &new_objects {
            store.put(n.reference().id(), n.bytes().clone()).unwrap();
        }
        let src = Repository::new(store.clone());
        assert_eq!(
            RowCursor::new(&src, &old, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
            data
        );
        assert_eq!(
            RowCursor::new(&src, &new, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
            modified
        );
        let shared =
            new_objects.iter().filter(|n| objects.iter().any(|o| o.reference.id == n.reference.id)).count();
        assert!(shared > objects.len() / 2, "expected unchanged prefix reuse");
    }
}
