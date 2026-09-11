//! Key-ordered tree scans with column projection and exact residual filtering.
use crate::error::{bail, beech_error};
use crate::value::{ColumnStatistics, ScalarRef, prefix_cmp_at};
use crate::{value::prefix_cmp, *};
use arrow_array::{Array, BooleanArray};
use arrow_select::filter::filter_record_batch;
use std::{cmp::Ordering, collections::BTreeSet, ops::Bound};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ConstraintOp {
    Unknown = 0,
    Eq = 1,
    Gt = 2,
    Le = 3,
    Lt = 4,
    Ge = 5,
    IsNull = 6,
    IsNotNull = 7,
}
#[derive(Debug, Clone)]
pub struct Predicate {
    pub column: usize,
    pub op: ConstraintOp,
    pub value: Scalar,
}
impl Predicate {
    pub fn new(column: usize, op: ConstraintOp, value: Scalar) -> Self {
        Self { column, op, value }
    }
    fn validate(&self, schema: &TableSchema) -> Result<()> {
        let field = schema
            .fields()
            .get(self.column)
            .ok_or_else(|| beech_error!(Query, "predicate column out of bounds"))?;
        if self.op == ConstraintOp::Unknown {
            bail!(Query, "unsupported predicate");
        }
        if !matches!(self.op, ConstraintOp::IsNull | ConstraintOp::IsNotNull) {
            self.value.validate_type(field.data_type(), true)?;
        }
        Ok(())
    }
    fn matches(&self, value: ScalarRef<'_>) -> Result<bool> {
        if self.op == ConstraintOp::IsNull {
            return Ok(value.is_null());
        }
        if self.op == ConstraintOp::IsNotNull {
            return Ok(!value.is_null());
        }
        if value.is_null() || self.value.is_null() {
            return Ok(false);
        }
        // Predicates use numeric float semantics; keys use IEEE total ordering.
        let cmp = match (value, &self.value) {
            (ScalarRef::Float32(a), Scalar::Float32(b)) => a.partial_cmp(b),
            (ScalarRef::Float64(a), Scalar::Float64(b)) => a.partial_cmp(b),
            _ => Some(value.compare(&self.value.as_ref())?),
        };
        Ok(cmp.is_some_and(|o| match self.op {
            ConstraintOp::Eq => o == Ordering::Equal,
            ConstraintOp::Lt => o == Ordering::Less,
            ConstraintOp::Le => o != Ordering::Greater,
            ConstraintOp::Gt => o == Ordering::Greater,
            ConstraintOp::Ge => o != Ordering::Less,
            _ => false,
        }))
    }
    fn may_match(&self, stats: &ColumnStatistics, row_count: u64) -> Result<bool> {
        if !matches!(self.op, ConstraintOp::IsNull | ConstraintOp::IsNotNull) && self.value.is_null() {
            return Ok(false);
        }
        let nulls = stats.null_count;
        if self.op == ConstraintOp::IsNull {
            return Ok(nulls != Some(0));
        }
        if nulls == Some(row_count) {
            return Ok(false);
        }
        if self.op == ConstraintOp::IsNotNull {
            return Ok(true);
        }
        let Some((min, max)) = &stats.bounds else {
            return Ok(true);
        };
        if min.compare(max)? == Ordering::Greater {
            return Ok(true);
        }
        let min_cmp = min.compare(&self.value)?;
        let max_cmp = max.compare(&self.value)?;
        Ok(match self.op {
            ConstraintOp::Eq => min_cmp != Ordering::Greater && max_cmp != Ordering::Less,
            ConstraintOp::Lt => min_cmp == Ordering::Less,
            ConstraintOp::Le => min_cmp != Ordering::Greater,
            ConstraintOp::Gt => max_cmp == Ordering::Greater,
            ConstraintOp::Ge => max_cmp != Ordering::Less,
            _ => true,
        })
    }
}

/// A bound may be a full key or a prefix. `Included([a])` includes every `(a, ...)`.
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub lower: Bound<Key>,
    pub upper: Bound<Key>,
}
impl Default for KeyRange {
    fn default() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}
impl KeyRange {
    pub fn prefix(key: Key) -> Self {
        Self {
            lower: Bound::Included(key.clone()),
            upper: Bound::Included(key),
        }
    }
    fn validate(&self, schema: &TableSchema) -> Result<()> {
        for bound in [&self.lower, &self.upper] {
            if let Bound::Included(k) | Bound::Excluded(k) = bound {
                schema.validate_key(k, true)?;
            }
        }
        Ok(())
    }
    fn contains(&self, columns: &[&dyn Array], row: usize) -> Result<bool> {
        let lower = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(k) => prefix_cmp_at(columns, row, k)? != Ordering::Less,
            Bound::Excluded(k) => prefix_cmp_at(columns, row, k)? == Ordering::Greater,
        };
        let upper = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(k) => prefix_cmp_at(columns, row, k)? != Ordering::Greater,
            Bound::Excluded(k) => prefix_cmp_at(columns, row, k)? == Ordering::Less,
        };
        Ok(lower && upper)
    }
    fn intersects(&self, max: &Key, lower_exclusive: Option<&Key>) -> Result<bool> {
        let after_lower = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(k) => prefix_cmp(max, k)? != Ordering::Less,
            Bound::Excluded(k) => prefix_cmp(max, k)? == Ordering::Greater,
        };
        if !after_lower {
            return Ok(false);
        }
        if let Some(low) = lower_exclusive {
            match &self.upper {
                Bound::Unbounded => {}
                Bound::Excluded(k) => {
                    if prefix_cmp(low, k)? != Ordering::Less {
                        return Ok(false);
                    }
                }
                Bound::Included(k) => {
                    let cmp = prefix_cmp(low, k)?;
                    if cmp == Ordering::Greater || (cmp == Ordering::Equal && k.len() == low.len()) {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}
/// User columns are zero-based and returned in this exact order.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub projection: Vec<usize>,
    pub include_row_id: bool,
    pub predicates: Vec<Predicate>,
    pub range: KeyRange,
    pub batch_size: usize,
    #[cfg(test)]
    pub(crate) use_statistics: bool,
}
impl ScanRequest {
    pub fn all(table: &Table) -> Self {
        Self {
            projection: (0..table.schema.fields().len()).collect(),
            include_row_id: false,
            predicates: vec![],
            range: KeyRange::default(),
            batch_size: 1024,
            #[cfg(test)]
            use_statistics: true,
        }
    }
    /// Check a concrete scan request against its table before execution.
    pub fn validate(&self, table: &Table) -> Result<()> {
        table.validate()?;
        self.range.validate(&table.schema)?;
        if self.batch_size == 0 {
            bail!(Query, "batch size must be positive");
        }
        let columns: BTreeSet<_> = self.projection.iter().copied().collect();
        if columns.len() != self.projection.len()
            || columns.iter().any(|&c| c >= table.schema.fields().len())
        {
            bail!(Query, "duplicate or invalid projection column");
        }
        for predicate in &self.predicates {
            predicate.validate(&table.schema)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct ScanMetrics {
    pub internal_nodes: usize,
    pub leaves: usize,
    pub pruned_nodes: usize,
    pub pruned_leaves: usize,
    pub output_rows: usize,
}
struct Pending {
    reference: NodeRef,
    lower: Option<Key>,
}
pub struct Scan<'a> {
    source: &'a dyn NodeSource,
    table: Table,
    request: ScanRequest,
    ranges: Vec<KeyRange>,
    pending: Vec<Pending>,
    current: Option<storage::LeafBatches>,
    physical: Vec<usize>,
    finished: bool,
    metrics: ScanMetrics,
}
fn predicate_range(schema: &TableSchema, predicates: &[Predicate]) -> KeyRange {
    let mut prefix = vec![];
    for &col in schema.key_columns() {
        if matches!(
            schema.fields()[col].data_type(),
            DataType::Float32 | DataType::Float64
        ) {
            break;
        }
        if let Some(p) =
            predicates.iter().find(|p| p.column == col && p.op == ConstraintOp::Eq && !p.value.is_null())
        {
            prefix.push(p.value.clone());
            continue;
        }
        let mut range =
            if prefix.is_empty() { KeyRange::default() } else { KeyRange::prefix(prefix.clone()) };
        if let Some(p) = predicates.iter().find(|p| {
            p.column == col && matches!(p.op, ConstraintOp::Gt | ConstraintOp::Ge) && !p.value.is_null()
        }) {
            let mut k = prefix.clone();
            k.push(p.value.clone());
            range.lower = if p.op == ConstraintOp::Gt { Bound::Excluded(k) } else { Bound::Included(k) };
        }
        if let Some(p) = predicates.iter().find(|p| {
            p.column == col && matches!(p.op, ConstraintOp::Lt | ConstraintOp::Le) && !p.value.is_null()
        }) {
            let mut k = prefix.clone();
            k.push(p.value.clone());
            range.upper = if p.op == ConstraintOp::Lt { Bound::Excluded(k) } else { Bound::Included(k) };
        }
        return range;
    }
    if prefix.is_empty() { KeyRange::default() } else { KeyRange::prefix(prefix) }
}
impl<'a> Scan<'a> {
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }
    pub fn new(source: &'a dyn NodeSource, table: &Table, request: ScanRequest) -> Result<Self> {
        request.validate(table)?;
        let columns: BTreeSet<_> = request.projection.iter().copied().collect();
        let mut physical: BTreeSet<usize> = columns.iter().map(|i| i + 1).collect();
        if request.include_row_id {
            physical.insert(0);
        }
        for &key in table.schema.key_columns() {
            physical.insert(key + 1);
        }
        for p in &request.predicates {
            physical.insert(p.column + 1);
        }
        let ranges = vec![
            request.range.clone(),
            predicate_range(&table.schema, &request.predicates),
        ];
        let pending = table
            .root
            .iter()
            .map(|r| Pending {
                reference: r.clone(),
                lower: None,
            })
            .collect();
        Ok(Self {
            source,
            table: table.clone(),
            request,
            ranges,
            pending,
            current: None,
            physical: physical.into_iter().collect(),
            finished: false,
            metrics: ScanMetrics::default(),
        })
    }
    fn matches_node(&self, p: &Pending) -> Result<bool> {
        for r in &self.ranges {
            if !r.intersects(&p.reference.max_key, p.lower.as_ref())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn projected_slot(&self, physical: usize) -> Result<usize> {
        self.physical.binary_search(&physical).map_err(|_| beech_error!(Query, "missing scan column"))
    }
    fn filter_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let key_columns = self
            .table
            .schema
            .key_columns()
            .iter()
            .map(|&c| Ok(batch.column(self.projected_slot(c + 1)?).as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut mask = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut keep = true;
            for r in &self.ranges {
                if !r.contains(&key_columns, row)? {
                    keep = false;
                    break;
                }
            }
            if keep {
                for p in &self.request.predicates {
                    let value = ScalarRef::from_array(
                        batch.column(self.projected_slot(p.column + 1)?).as_ref(),
                        row,
                    )?;
                    if !p.matches(value)? {
                        keep = false;
                        break;
                    }
                }
            }
            mask.push(keep);
        }
        let filtered = filter_record_batch(batch, &BooleanArray::from(mask))?;
        let mut output = vec![];
        if self.request.include_row_id {
            output.push(self.projected_slot(0)?);
        }
        for &c in &self.request.projection {
            output.push(self.projected_slot(c + 1)?);
        }
        Ok(filtered.project(&output)?)
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.current {
                if let Some(batch) = reader.next() {
                    let batch = self.filter_batch(&batch?)?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.metrics.output_rows += batch.num_rows();
                    return Ok(Some(batch));
                }
                self.current = None;
            }
            let Some(pending) = self.pending.pop() else {
                return Ok(None);
            };
            if !self.matches_node(&pending)? {
                self.metrics.pruned_nodes += 1;
                continue;
            }
            if pending.reference.height > 0 {
                let node = self.source.get_internal(&pending.reference, &self.table.schema)?;
                self.metrics.internal_nodes += 1;
                let mut children = vec![];
                let mut lower = pending.lower;
                for child in &node.children {
                    children.push(Pending {
                        reference: child.clone(),
                        lower: lower.clone(),
                    });
                    lower = Some(child.max_key.clone());
                }
                self.pending.extend(children.into_iter().rev());
            } else {
                let leaf = self.source.open_leaf(&pending.reference, &self.table.schema)?;
                self.metrics.leaves += 1;
                let mut keep = true;
                #[cfg(test)]
                let use_statistics = self.request.use_statistics;
                #[cfg(not(test))]
                let use_statistics = true;
                if use_statistics {
                    for predicate in &self.request.predicates {
                        if !predicate.may_match(leaf.statistics(predicate.column), leaf.row_count())? {
                            keep = false;
                            break;
                        }
                    }
                }
                if keep {
                    self.current = Some(leaf.read(&self.physical, self.request.batch_size)?);
                } else {
                    self.metrics.pruned_leaves += 1;
                }
            }
        }
    }
}
impl Iterator for Scan<'_> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.next_batch() {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                self.pending.clear();
                self.current = None;
                Some(Err(e))
            }
        }
    }
}
impl std::iter::FusedIterator for Scan<'_> {}

/// Materialize rows only at this adapter boundary. Batch scans remain column-oriented.
pub struct RowCursor<'a> {
    scan: Scan<'a>,
    batch: Option<RecordBatch>,
    row: usize,
}
impl<'a> RowCursor<'a> {
    pub fn new(source: &'a dyn NodeSource, table: &Table, predicates: Vec<Predicate>) -> Result<Self> {
        let mut request = ScanRequest::all(table);
        request.include_row_id = true;
        request.predicates = predicates;
        Ok(Self {
            scan: Scan::new(source, table, request)?,
            batch: None,
            row: 0,
        })
    }
}
impl Iterator for RowCursor<'_> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                let row = self.row;
                self.row += 1;
                return Some((|| {
                    let Scalar::Int64(id) = Scalar::from_array(batch.column(0).as_ref(), row)? else {
                        bail!(Schema, "invalid row ID");
                    };
                    let values = batch.columns()[1..]
                        .iter()
                        .map(|a| Scalar::from_array(a.as_ref(), row))
                        .collect::<Result<_>>()?;
                    Ok((id, values))
                })());
            }
            match self.scan.next() {
                Some(Ok(batch)) => {
                    self.batch = Some(batch);
                    self.row = 0;
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}
