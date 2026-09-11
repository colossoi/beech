//! SQLite access plans: argument slots, binding, and idx_str serialization.
use beech_core::{
    BeechError, Id, Result, Scalar, Table,
    plan::{self, CandidateConstraint, PlanEstimate},
    query::{ConstraintOp, Predicate, ScanRequest},
};
use std::io::Cursor;
use thrift::{
    TConfiguration,
    protocol::{TCompactInputProtocol, TCompactOutputProtocol, TSerializable},
};
#[path = "generated/plan.rs"]
mod generated;
use generated as g;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AccessPlan {
    pub(super) table_id: Id,
    pub(super) search: Vec<SearchSlot>,
    pub(super) preserves_order: bool,
    pub(super) estimate: PlanEstimate,
}
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SearchSlot {
    pub(super) key_part: i32,
    pub(super) column: i32,
    pub(super) op: ConstraintOp,
    pub(super) argv_index: i32,
}
impl AccessPlan {
    pub(super) fn select(table_id: Id, table: &Table, candidates: &[CandidateConstraint]) -> Result<Self> {
        let selected = plan::select_key_prefix(table.schema(), candidates);
        let estimate = plan::estimate(table, selected.iter().map(|&i| candidates[i]));
        let search = selected
            .into_iter()
            .enumerate()
            .map(|(part, index)| {
                let c = candidates[index];
                let checked = |n: usize| {
                    i32::try_from(n)
                        .map_err(|_| BeechError::Query("plan index exceeds i32 representation".into()))
                };
                Ok(SearchSlot {
                    key_part: checked(part)?,
                    column: checked(c.column)?,
                    op: c.op,
                    argv_index: checked(part + 1)?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            table_id,
            search,
            preserves_order: false,
            estimate,
        })
    }
    fn validate(&self) -> Result<()> {
        if !self.estimate.estimated_cost.is_finite()
            || self.estimate.estimated_cost < 0.0
            || self.estimate.estimated_rows < 0
        {
            return Err(BeechError::Query("invalid plan estimate".into()));
        }
        for (i, s) in self.search.iter().enumerate() {
            if usize::try_from(s.key_part).ok() != Some(i)
                || s.column < 0
                || usize::try_from(s.argv_index).ok() != Some(i + 1)
                || !matches!(
                    s.op,
                    ConstraintOp::Eq
                        | ConstraintOp::Lt
                        | ConstraintOp::Le
                        | ConstraintOp::Gt
                        | ConstraintOp::Ge
                )
                || (i + 1 < self.search.len() && s.op != ConstraintOp::Eq)
            {
                return Err(BeechError::Query("invalid search slots".into()));
            }
        }
        Ok(())
    }
    /// Bind arguments using the ID and table loaded from the same catalog entry.
    pub(super) fn bind(&self, table_id: Id, table: &Table, args: &[Scalar]) -> Result<ScanRequest> {
        self.validate()?;
        if self.table_id != table_id || args.len() != self.search.len() {
            return Err(BeechError::Query("plan table or argument mismatch".into()));
        }
        let mut request = ScanRequest::all(table);
        for (s, value) in self.search.iter().zip(args) {
            if table.schema().key_columns().get(s.key_part as usize) != Some(&(s.column as usize)) {
                return Err(BeechError::Query("plan key column mismatch".into()));
            }
            request.predicates.push(Predicate::new(s.column as usize, s.op, value.clone()));
        }
        request.validate(table)?;
        Ok(request)
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        encode_plan(self)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        decode_plan(bytes)
    }
}

// Retain the existing version/tag bytes; this is SQLite transport, not a stored object.
const HEADER: &[u8; 5] = b"BCH\x01\x07";
fn encode(value: &g::AccessPlan) -> Result<Vec<u8>> {
    let mut bytes = HEADER.to_vec();
    value.write_to_out_protocol(&mut TCompactOutputProtocol::with_config(
        &mut bytes,
        TConfiguration::no_limits(),
    ))?;
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<g::AccessPlan> {
    if !bytes.starts_with(HEADER) {
        return Err(BeechError::Wire("wrong access-plan header".into()));
    }
    let length = bytes.len() - HEADER.len();
    let config = TConfiguration::builder()
        .max_message_size(Some(length))
        .max_frame_size(Some(length))
        .max_string_size(Some(length))
        .max_container_size(Some(length))
        .build()?;
    let mut reader = Cursor::new(&bytes[HEADER.len()..]);
    let value =
        g::AccessPlan::read_from_in_protocol(&mut TCompactInputProtocol::with_config(&mut reader, config))?;
    if reader.position() as usize != length || encode(&value)? != bytes {
        return Err(BeechError::Wire(
            "noncanonical or trailing access-plan data".into(),
        ));
    }
    Ok(value)
}
fn encode_plan(p: &AccessPlan) -> Result<Vec<u8>> {
    p.validate()?;
    i32::try_from(p.search.len())
        .map_err(|_| BeechError::Wire("plan search slot count exceeds Thrift i32 representation".into()))?;
    encode(&g::AccessPlan::new(
        p.table_id.as_bytes().to_vec(),
        p.search
            .iter()
            .map(|s| g::SearchSlot::new(s.key_part, s.column, s.op as i32, s.argv_index))
            .collect(),
        p.preserves_order,
        p.estimate.estimated_cost.into(),
        p.estimate.estimated_rows,
    ))
}
fn decode_plan(bytes: &[u8]) -> Result<AccessPlan> {
    let p: g::AccessPlan = decode(bytes)?;
    let result = AccessPlan {
        table_id: Id::from_slice(&p.table_id)?,
        search: p
            .search
            .into_iter()
            .map(|s| {
                Ok(SearchSlot {
                    key_part: s.key_part,
                    column: s.column,
                    op: constraint_op(s.op)?,
                    argv_index: s.argv_index,
                })
            })
            .collect::<Result<_>>()?,
        preserves_order: p.preserves_order,
        estimate: PlanEstimate {
            estimated_cost: p.estimated_cost.into_inner(),
            estimated_rows: p.estimated_rows,
        },
    };
    result.validate()?;
    Ok(result)
}

fn constraint_op(code: i32) -> Result<ConstraintOp> {
    Ok(match code {
        0 => ConstraintOp::Unknown,
        1 => ConstraintOp::Eq,
        2 => ConstraintOp::Gt,
        3 => ConstraintOp::Le,
        4 => ConstraintOp::Lt,
        5 => ConstraintOp::Ge,
        6 => ConstraintOp::IsNull,
        7 => ConstraintOp::IsNotNull,
        _ => {
            return Err(BeechError::Query(format!(
                "unknown operator code {code}; expected 0..=7"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConstraintOp as Op;
    use beech_core::{DataType, Field, NodeRef, TableSchema, codec};
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
                Id::from([1; 32]),
                0,
                row_count,
                vec![Scalar::Int64(row_count as i64), Scalar::Int32(0)],
            )
            .unwrap()
        });
        Table::new("t", schema, root).unwrap()
    }
    fn cc(column: usize, op: Op) -> CandidateConstraint {
        CandidateConstraint { column, op }
    }
    #[test]
    fn thrift_plan_roundtrip_and_binding_validate_context() {
        let t = table(1000);
        let table_id = codec::thrift::encode_table(&t).unwrap().id();
        let p = AccessPlan::select(table_id, &t, &[cc(1, Op::Gt), cc(0, Op::Eq)]).unwrap();
        let bytes = p.encode().unwrap();
        let decoded = AccessPlan::decode(&bytes).unwrap();
        assert_eq!(p, decoded);
        assert_eq!(
            decoded.bind(table_id, &t, &[Scalar::Int64(2), Scalar::Int32(4)]).unwrap().predicates.len(),
            2
        );
        assert!(decoded.bind(table_id, &t, &[]).is_err());
        assert!(decoded.bind(Id::from([99; 32]), &t, &[Scalar::Int64(2), Scalar::Int32(4)]).is_err());
        let mut bad = p;
        bad.search[1].column = 99;
        assert!(bad.bind(table_id, &t, &[Scalar::Int64(2), Scalar::Int32(4)]).is_err());
    }
}
