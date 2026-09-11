use beech_core::{
    query::{ConstraintOp, Predicate, RowCursor},
    DataType, Field, Scalar, TableSchema,
};
use beech_test_fixtures::build_simple_table;
fn schema(keys: Vec<usize>) -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ],
        keys,
    )
    .unwrap()
}
#[test]
fn full_scan_crosses_leaves_and_stays_exhausted() {
    let rows = (0..500)
        .map(|i| (i, vec![Scalar::Int32(i as i32), Scalar::Int32(i as i32 * 10)]))
        .collect::<Vec<_>>();
    let (_, source, table) = build_simple_table("t", rows.clone(), schema(vec![0]), 64, 16).unwrap();
    assert!(table.root().unwrap().height() > 0);
    let mut cursor = RowCursor::new(&source, &table, vec![]).unwrap();
    assert_eq!(
        cursor.by_ref().collect::<beech_core::Result<Vec<_>>>().unwrap(),
        rows
    );
    assert!(cursor.next().is_none());
    assert!(cursor.next().is_none());
}
#[test]
fn equality_ranges_and_out_of_bounds() {
    let rows = (0..50).map(|i| (i, vec![Scalar::Int32(i as i32), Scalar::Int32(0)])).collect();
    let (_, source, table) = build_simple_table("t", rows, schema(vec![0]), 64, 16).unwrap();
    for (predicates, expected) in [
        (
            vec![Predicate::new(0, ConstraintOp::Eq, Scalar::Int32(17))],
            vec![17],
        ),
        (
            vec![
                Predicate::new(0, ConstraintOp::Ge, Scalar::Int32(10)),
                Predicate::new(0, ConstraintOp::Lt, Scalar::Int32(20)),
            ],
            (10..20).collect(),
        ),
        (
            vec![Predicate::new(0, ConstraintOp::Eq, Scalar::Int32(-100))],
            vec![],
        ),
        (
            vec![Predicate::new(0, ConstraintOp::Eq, Scalar::Int32(9999))],
            vec![],
        ),
        (
            vec![Predicate::new(0, ConstraintOp::Ge, Scalar::Int32(9999))],
            vec![],
        ),
    ] {
        let ids: Vec<_> =
            RowCursor::new(&source, &table, predicates).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(ids, expected);
    }
}
#[test]
fn composite_prefix_crosses_leaves() {
    let rows = (0..5)
        .flat_map(|a| {
            (0..4).map(move |b| (a * 10 + b, vec![Scalar::Int32(a as i32), Scalar::Int32(b as i32)]))
        })
        .collect();
    let (_, source, table) = build_simple_table("t", rows, schema(vec![0, 1]), 64, 16).unwrap();
    let ids: Vec<_> = RowCursor::new(
        &source,
        &table,
        vec![Predicate::new(0, ConstraintOp::Eq, Scalar::Int32(3))],
    )
    .unwrap()
    .map(|r| r.unwrap().0)
    .collect();
    assert_eq!(ids, vec![30, 31, 32, 33]);
    assert_eq!(RowCursor::new(&source, &table, vec![]).unwrap().count(), 20);
}
