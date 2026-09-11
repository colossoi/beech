use crate::codec::FormatTag;
use crate::{
    query::{ConstraintOp as Op, KeyRange, Predicate, RowCursor, Scan, ScanRequest},
    storage::Repository,
    test_support::{batch_from_rows, build},
    *,
};
use arrow_array::{Decimal128Array, Int64Array};
use std::{cmp::Ordering, ops::Bound, sync::Arc};

fn decimal(unscaled: i128, scale: u8) -> Scalar {
    Scalar::Decimal(Decimal::new(unscaled, scale).unwrap())
}
fn schema(precision: u8, scale: i8) -> TableSchema {
    TableSchema::new(
        vec![Field::new("amount", DataType::Decimal128(precision, scale), true)],
        vec![0],
    )
    .unwrap()
}

#[test]
fn decimal_precision_scale_and_order_are_exact() {
    let limit = 10i128.pow(38);
    assert!(Decimal::new(limit - 1, 38).is_ok());
    assert!(Decimal::new(1 - limit, 0).is_ok());
    for invalid in [limit, -limit, i128::MIN, i128::MAX] {
        assert!(Decimal::new(invalid, 0).is_err());
    }
    assert!(Decimal::new(0, 39).is_err());
    for (precision, scale) in [(0, 0), (39, 0), (9, -1), (9, 10)] {
        assert!(
            TableSchema::new(
                vec![Field::new("v", DataType::Decimal128(precision, scale), false)],
                vec![0]
            )
            .is_err()
        );
    }
    assert_eq!(
        decimal(-12345, 2).compare(&decimal(-1, 2)).unwrap(),
        Ordering::Less
    );
    assert_eq!(decimal(1, 38).compare(&decimal(2, 38)).unwrap(), Ordering::Less);
    assert_eq!(Scalar::Null.compare(&decimal(-1, 0)).unwrap(), Ordering::Less);
    assert!(decimal(10, 1).compare(&decimal(100, 2)).is_err());
    assert!(decimal(1, 0).compare(&Scalar::Int64(1)).is_err());
    assert!(batch_from_rows(&schema(3, 2), &[(0, vec![decimal(1000, 2)])]).is_err());
    assert!(batch_from_rows(&schema(3, 2), &[(0, vec![decimal(-1000, 2)])]).is_err());
    assert!(batch_from_rows(&schema(3, 2), &[(0, vec![decimal(100, 1)])]).is_err());
    assert!(batch_from_rows(&schema(3, 2), &[(0, vec![decimal(999, 2)])]).is_ok());
}

#[test]
fn decimal_tree_roundtrip_preserves_nulls_boundaries_and_all_38_digits() {
    let s = TableSchema::new(
        vec![
            Field::new("key", DataType::Decimal128(38, 4), true),
            Field::new("amount", DataType::Decimal128(38, 18), true),
        ],
        vec![0],
    )
    .unwrap();
    let limit = 10i128.pow(38) - 1;
    let mut rows = vec![(i64::MIN, vec![Scalar::Null, Scalar::Null])];
    rows.extend(
        [-limit, -12345, -1, 0, 1, 12345, limit]
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as i64, vec![decimal(v, 4), decimal(-v, 18)])),
    );
    let (table, store, nodes) = build(&s, &rows, 2, 2);
    assert!(table.root().unwrap().height() > 1);
    let source = Repository::with_options(
        store,
        storage::RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    assert_eq!(
        RowCursor::new(&source, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
        rows
    );
    let internal = codec::thrift::decode_internal(nodes.last().unwrap().bytes(), &s).unwrap();
    assert_eq!(
        internal.children().last().unwrap().max_key(),
        &vec![decimal(limit, 4)]
    );
    let encoded = codec::thrift::encode_schema(&s).unwrap();
    assert_eq!(codec::thrift::decode_schema(encoded.bytes()).unwrap(), s);
    let empty = Table::new("empty", s.clone(), None).unwrap();
    assert_eq!(
        codec::thrift::decode_table(codec::thrift::encode_table(&empty).unwrap().bytes()).unwrap(),
        empty
    );
    assert_ne!(
        codec::thrift::encode_schema(&schema(9, 2)).unwrap().id(),
        codec::thrift::encode_schema(&schema(10, 2)).unwrap().id()
    );
    assert_ne!(
        codec::thrift::encode_schema(&schema(9, 2)).unwrap().id(),
        codec::thrift::encode_schema(&schema(9, 3)).unwrap().id()
    );
    assert_ne!(
        codec::thrift::encode_key(&vec![decimal(123, 2)]).unwrap(),
        codec::thrift::encode_key(&vec![decimal(123, 3)]).unwrap()
    );
}

#[test]
fn decimal_ranges_and_predicates_match_integer_reference() {
    let s = schema(9, 2);
    let rows = (-25..=25).map(|v| (v as i64, vec![decimal(v, 2)])).collect::<Vec<_>>();
    let (table, store, _) = build(&s, &rows, 4, 3);
    let source = Repository::new(store);
    for statistics in [false, true] {
        for op in [Op::Eq, Op::Lt, Op::Le, Op::Gt, Op::Ge] {
            for bound in [-26, -5, 0, 13, 26] {
                let mut request = ScanRequest::all(&table);
                request.use_statistics = statistics;
                request.include_row_id = true;
                request.predicates = vec![Predicate::new(0, op, decimal(bound, 2))];
                let actual = Scan::new(&source, &table, request)
                    .unwrap()
                    .flat_map(|batch| {
                        let batch = batch.unwrap();
                        batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
                    })
                    .collect::<Vec<_>>();
                let expected = rows
                    .iter()
                    .filter(|(value, _)| match op {
                        Op::Eq => *value == bound as i64,
                        Op::Lt => *value < bound as i64,
                        Op::Le => *value <= bound as i64,
                        Op::Gt => *value > bound as i64,
                        Op::Ge => *value >= bound as i64,
                        _ => unreachable!(),
                    })
                    .map(|(value, _)| *value)
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual, expected,
                    "op={op:?}, bound={bound}, statistics={statistics}"
                );
            }
        }
    }
    let mut request = ScanRequest::all(&table);
    request.range = KeyRange {
        lower: Bound::Excluded(vec![decimal(-5, 2)]),
        upper: Bound::Included(vec![decimal(5, 2)]),
    };
    assert_eq!(
        Scan::new(&source, &table, request).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>(),
        10
    );
    let mut req = ScanRequest::all(&table);
    req.predicates = vec![Predicate::new(0, Op::Eq, decimal(1, 3))];
    assert!(req.validate(&table).is_err());
    req.predicates[0].value = decimal(1, 2);
    assert_eq!(
        Scan::new(&source, &table, req).unwrap().map(|b| b.unwrap().num_rows()).sum::<usize>(),
        1
    );
}

#[test]
fn decimal_nonkey_statistics_prune_across_negative_and_positive_values() {
    for precision in [9, 18, 38] {
        let s = TableSchema::new(
            vec![
                Field::new("key", DataType::Int64, false),
                Field::new("amount", DataType::Decimal128(precision, 2), true),
            ],
            vec![0],
        )
        .unwrap();
        let factor = 10i128.pow(u32::from(precision) - 3);
        let rows = (-50..50)
            .map(|v| {
                (
                    v,
                    vec![
                        Scalar::Int64(v),
                        if v == 0 { Scalar::Null } else { decimal(v as i128 * factor, 2) },
                    ],
                )
            })
            .collect::<Vec<_>>();
        let (table, store, _) = build(&s, &rows, 10, 4);
        let source = Repository::new(store);
        for target in [-25, 25] {
            let mut request = ScanRequest::all(&table);
            request.projection = vec![0];
            request.predicates = vec![Predicate::new(1, Op::Eq, decimal(target as i128 * factor, 2))];
            let mut scan = Scan::new(&source, &table, request).unwrap();
            let batches = scan.by_ref().collect::<Result<Vec<_>>>().unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert_eq!(
                Scalar::from_array(batches[0].column(0).as_ref(), 0).unwrap(),
                Scalar::Int64(target)
            );
            assert_eq!(scan.metrics().pruned_leaves, 9, "precision={precision}");
        }
        let nulls = RowCursor::new(&source, &table, vec![Predicate::new(1, Op::IsNull, Scalar::Null)])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(nulls, vec![rows[50].clone()]);
    }
}

#[test]
fn decimal_writer_rejects_out_of_precision_nonkey_arrow_values() {
    let s = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(3, 2), false),
        ],
        vec![0],
    )
    .unwrap();
    let bad = Decimal128Array::from(vec![1000]).with_precision_and_scale(3, 2).unwrap();
    let batch = RecordBatch::try_new(
        s.physical_schema(),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(bad),
        ],
    )
    .unwrap();
    assert!(codec::parquet::encode_leaf(&s, &batch).is_err());
}

#[test]
fn independent_decimal_files_preserve_values_and_prune_all_physical_widths() {
    use parquet::basic::Type;
    let s = TableSchema::new(
        vec![
            Field::new("key", DataType::Decimal128(38, 4), false),
            Field::new("small", DataType::Decimal128(9, 2), true),
            Field::new("medium", DataType::Decimal128(18, 4), true),
            Field::new("wide", DataType::Decimal128(38, 18), true),
        ],
        vec![0],
    )
    .unwrap();
    let expected = (0..6)
        .map(|i| {
            let values = [(38, 4), (9, 2), (18, 4), (38, 18)]
                .into_iter()
                .enumerate()
                .map(|(col, (precision, scale))| {
                    if col > 0 && i == 3 {
                        return Scalar::Null;
                    }
                    let max = 10i128.pow(precision) - 1;
                    decimal([-max, -12345, -1, 0, 12345, max][i], scale)
                })
                .collect();
            (2000 + i as i64, values)
        })
        .collect::<Vec<Row>>();
    for (bytes, integer_storage) in [
        (
            include_bytes!("../tests/fixtures/python-decimal-fixed.parquet").as_slice(),
            false,
        ),
        (
            include_bytes!("../tests/fixtures/python-decimal-int.parquet").as_slice(),
            true,
        ),
    ] {
        let id = codec::object_id(FormatTag::Leaf, bytes);
        let store = test_support::MemoryStore::default();
        store.put(id, bytes::Bytes::copy_from_slice(bytes)).unwrap();
        let root = NodeRef::new(&s, id, 0, 6, s.key_from_row(expected.last().unwrap()).unwrap()).unwrap();
        let table = Table::new("python-decimal", s.clone(), Some(root)).unwrap();
        let source = Repository::with_options(
            store,
            storage::RepositoryOptions {
                verify_leaves: true,
                ..Default::default()
            },
        );
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::copy_from_slice(bytes),
        )
        .unwrap();
        let group = reader.metadata().row_group(0);
        assert_eq!(
            group.column(2).column_type(),
            if integer_storage { Type::INT32 } else { Type::FIXED_LEN_BYTE_ARRAY }
        );
        assert_eq!(
            group.column(3).column_type(),
            if integer_storage { Type::INT64 } else { Type::FIXED_LEN_BYTE_ARRAY }
        );
        assert_eq!(group.column(4).column_type(), Type::FIXED_LEN_BYTE_ARRAY);
        assert_eq!(
            RowCursor::new(&source, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap(),
            expected
        );
        for (col, scale) in [(1, 2), (2, 4), (3, 18)] {
            for statistics in [false, true] {
                let mut request = ScanRequest::all(&table);
                request.projection = vec![0];
                request.predicates = vec![Predicate::new(col, Op::Ge, decimal(12345, scale))];
                request.use_statistics = statistics;
                let mut scan = Scan::new(&source, &table, request).unwrap();
                let output = scan.by_ref().collect::<Result<Vec<_>>>().unwrap();
                assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
                assert_eq!(scan.metrics().pruned_leaves, 0);
                let DataType::Decimal128(precision, _) = s.fields()[col].data_type() else {
                    unreachable!()
                };
                let mut request = ScanRequest::all(&table);
                request.predicates = vec![Predicate::new(
                    col,
                    Op::Lt,
                    decimal(-(10i128.pow(u32::from(*precision)) - 1), scale),
                )];
                request.use_statistics = statistics;
                let mut scan = Scan::new(&source, &table, request).unwrap();
                assert!(scan.next().is_none());
                assert_eq!(scan.metrics().pruned_leaves, usize::from(statistics));
            }
        }
    }
}
