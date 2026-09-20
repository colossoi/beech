use crate::{
    plan::{CandidateConstraint, estimate, select_key_prefix},
    query::ConstraintOp as Op,
    *,
};
fn table(row_count: u64) -> Table {
    let schema = TableSchema::new(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ],
        vec![0, 1],
    )
    .unwrap();
    let root = (row_count > 0).then(|| {
        NodeRef::new(
            &schema,
            Id::from(1),
            0,
            row_count,
            vec![Scalar::Int64(row_count as i64), Scalar::Int32(0)],
        )
        .unwrap()
    });
    Table::new("t", schema, root, row_count as i64 - 1).unwrap()
}
fn cc(column: usize, op: Op) -> CandidateConstraint {
    CandidateConstraint { column, op }
}
#[test]
fn empty_nonkey_unknown_and_skipped_prefix_are_full_scans() {
    let t = table(100);
    for candidates in [
        vec![],
        vec![cc(2, Op::Eq)],
        vec![cc(0, Op::Unknown)],
        vec![cc(1, Op::Eq)],
        vec![cc(usize::MAX, Op::Eq)],
    ] {
        let selected = select_key_prefix(t.schema(), &candidates);
        assert!(selected.is_empty());
        assert_eq!(
            estimate(&t, selected.iter().map(|&i| candidates[i])).estimated_rows,
            100
        );
    }
}
#[test]
fn equality_prefix_and_range_choose_stable_candidates() {
    let t = table(1000);
    let candidates = [cc(1, Op::Gt), cc(0, Op::Gt), cc(0, Op::Eq), cc(0, Op::Eq)];
    assert_eq!(select_key_prefix(t.schema(), &candidates), vec![2, 0]);
    assert_eq!(
        select_key_prefix(t.schema(), &[cc(0, Op::Ge), cc(1, Op::Eq)]),
        vec![0]
    );
}
#[test]
fn estimates_form_a_gradient_and_empty_tables_cost_zero() {
    let t = table(1_000_000);
    let full = estimate(&t, []);
    let prefix = estimate(&t, [cc(0, Op::Eq)]);
    let range = estimate(&t, [cc(0, Op::Eq), cc(1, Op::Gt)]);
    let point = estimate(&t, [cc(0, Op::Eq), cc(1, Op::Eq)]);
    assert!(point.estimated_cost < range.estimated_cost);
    assert!(range.estimated_cost < prefix.estimated_cost);
    assert!(prefix.estimated_cost < full.estimated_cost);
    let empty = estimate(&table(0), []);
    assert_eq!(empty.estimated_rows, 0);
    assert_eq!(empty.estimated_cost, 0.0);
}
