//! Shared key-prefix selection and scan-cost estimation for database adapters.
use crate::{Table, TableSchema, query::ConstraintOp};

#[derive(Debug, Clone, PartialEq)]
pub struct PlanEstimate {
    pub estimated_cost: f64,
    pub estimated_rows: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct CandidateConstraint {
    pub column: usize,
    pub op: ConstraintOp,
}

/// Return indexes into `candidates`, in key order: equalities followed by at most
/// one range. Keeping candidate identity lets each adapter map its own arguments.
pub fn select_key_prefix(schema: &TableSchema, candidates: &[CandidateConstraint]) -> Vec<usize> {
    let mut by_part = vec![vec![]; schema.key_columns().len()];
    for (index, c) in candidates.iter().enumerate() {
        if !matches!(
            c.op,
            ConstraintOp::Eq | ConstraintOp::Lt | ConstraintOp::Le | ConstraintOp::Gt | ConstraintOp::Ge
        ) {
            continue;
        }
        if let Some(part) = schema.column_key_index(c.column) {
            by_part[part].push(index);
        }
    }
    let mut selected = vec![];
    for indexes in by_part {
        let chosen =
            indexes.iter().find(|&&i| candidates[i].op == ConstraintOp::Eq).or_else(|| indexes.first());
        let Some(&index) = chosen else { break };
        selected.push(index);
        if candidates[index].op != ConstraintOp::Eq {
            break;
        }
    }
    selected
}

/// Estimate a scan using constraints selected by `select_key_prefix`, in key order.
pub fn estimate(table: &Table, search: impl IntoIterator<Item = CandidateConstraint>) -> PlanEstimate {
    let mut search = search.into_iter().peekable();
    let mut eq = 0;
    while search.peek().is_some_and(|c| c.op == ConstraintOp::Eq) {
        search.next();
        eq += 1;
    }
    let has_range = search.peek().is_some();
    let total = table.root().map_or(0, |root| root.row_count()) as f64;
    let (rows, cost) = if total == 0.0 {
        (0.0, 0.0)
    } else if eq == table.schema().key_columns().len() {
        (1.0, total.log2().max(1.0))
    } else {
        let rows = (total / 10f64.powi(eq as i32) * if has_range { 0.5 } else { 1.0 }).max(1.0);
        (rows, rows)
    };
    PlanEstimate {
        estimated_cost: cost,
        estimated_rows: rows.round() as i64,
    }
}
