//! Writable SQLite tables backed by Beech repository snapshots.
//!
//! Register with [create_beech_module], then:
//!
//! ```sql
//! CREATE VIRTUAL TABLE items USING beech('path/to/data', 'items');
//! ```
//!
//! The directory contains objects named by hexadecimal content ID and a text
//! file named "root" containing the current root object's ID. Each virtual
//! table retains the snapshot resolved when it connects.
//!
//! Boolean and signed integers map to INTEGER, floats to REAL, strings to TEXT,
//! and binary to BLOB. UInt64 and Decimal128 use TEXT to retain their full precision;
//! SQLite arithmetic on these text values follows SQLite's numeric conversion rules.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt64Array,
};
use beech_core::{
    BeechError, DataType, Decimal, Id, Key, KeyOrdering, RecordBatch, Row, Scalar, Table,
    plan::CandidateConstraint,
    query::{ConstraintOp, Scan},
    storage::{FileStore, Repository},
};
use beech_write::{
    BuildOptions, Change, FileWriter, MutationBatch, SortLimits, WorkingScan, WorkingTable, Writer,
    publish_table,
};
use plan::AccessPlan;
use rusqlite::{
    Result,
    types::ValueRef,
    vtab::{
        Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts, Module, TransactionVTab,
        UpdateVTab, Updates, VTab, VTabConnection, VTabCursor, VTabKind, sqlite3_vtab, sqlite3_vtab_cursor,
    },
};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    ffi::{CStr, CString, c_int},
    path::{Path, PathBuf},
    sync::Arc,
};

mod plan;

#[repr(C)]
struct BeechTable {
    base: sqlite3_vtab,
    repository: Arc<Repository>,
    table: Arc<Table>,
    table_id: Id,
    data_path: PathBuf,
    table_name: String,
    root_id: Id,
    transaction_id: Id,
    tables: BTreeMap<String, Id>,
    write: Option<WriteState>,
}

struct WriteState {
    working: RefCell<WorkingTable>,
    mutations: MutationBatch,
    row_keys: RefCell<HashMap<i64, Key>>,
    dirty: bool,
    pending_commit: Option<PendingCommit>,
}

struct PendingCommit {
    writer: FileWriter,
    table: Table,
    root_id: Id,
    table_id: Id,
}

impl BeechTable {
    fn connect_snapshot(data_path: &str, table_name: &str) -> beech_core::Result<Self> {
        let path = Path::new(data_path);
        let repository = Arc::new(Repository::new(FileStore::new(path)));
        let root_id = Id::from_hex(std::fs::read_to_string(path.join("root"))?.trim())?;
        let snapshot = repository.snapshot(root_id)?;
        let transaction_id = repository.get_root(&root_id)?.transaction_id();
        let table = snapshot.table(table_name)?;
        let table_id = *snapshot
            .transaction()
            .tables()
            .get(table_name)
            .ok_or_else(|| BeechError::NoSuchTable(table_name.into()))?;
        Ok(Self {
            base: sqlite3_vtab::default(),
            repository,
            table,
            table_id,
            data_path: path.to_owned(),
            table_name: table_name.into(),
            root_id,
            transaction_id,
            tables: snapshot.transaction().tables().clone(),
            write: None,
        })
    }

    fn ensure_write(&mut self) -> beech_core::Result<&mut WriteState> {
        if self.write.is_none() {
            self.write = Some(WriteState {
                working: RefCell::new(WorkingTable::new(
                    self.table.as_ref().clone(),
                    self.repository.clone(),
                    BuildOptions::default(),
                )?),
                mutations: MutationBatch::new(SortLimits::default())?,
                row_keys: RefCell::new(HashMap::new()),
                dirty: false,
                pending_commit: None,
            });
        }
        Ok(self.write.as_mut().expect("initialized above"))
    }
}

// SAFETY: repr(C), with SQLite's base as the first field. Rusqlite owns allocation.
unsafe impl<'vtab> VTab<'vtab> for BeechTable {
    type Aux = ();
    type Cursor = BeechCursor<'vtab>;

    fn connect(
        _db: &mut VTabConnection,
        _aux: Option<&Self::Aux>,
        _module_name: &[u8],
        _database_name: &[u8],
        _local_table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let args = args.iter().map(|a| parse_arg(a)).collect::<Result<Vec<_>>>()?;
        let [data_path, table_name] = args.as_slice() else {
            return Err(rusqlite::Error::ModuleError(
                "Usage: CREATE VIRTUAL TABLE name USING beech(data_path, table_name)".into(),
            ));
        };
        let vtab = Self::connect_snapshot(data_path, table_name).map_err(into_rusqlite_error)?;
        let columns = vtab
            .table
            .schema()
            .fields()
            .iter()
            .map(|field| {
                format!(
                    "{} {}",
                    quote_identifier(field.name()),
                    sqlite_type(field.data_type())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let declaration = CString::new(format!("CREATE TABLE x({columns})"))
            .map_err(|error| rusqlite::Error::ModuleError(error.to_string()))?;
        Ok((declaration.into(), vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        // Preserve SQLite constraint indexes; duplicate predicates are distinct
        // inputs and must never share an argv slot.
        let mut candidates = Vec::new();
        let mut sqlite_indexes = Vec::new();
        for (index, c) in info.constraints().enumerate() {
            let Ok(column) = usize::try_from(c.column()) else {
                continue;
            };
            let Some(op) = from_sqlite_op(c.operator()) else {
                continue;
            };
            if !c.is_usable() {
                continue;
            }
            let Some(field) = self.table.schema().fields().get(column) else {
                continue;
            };
            if !matches!(
                field.data_type(),
                DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::Utf8 | DataType::Binary
            ) {
                continue;
            }
            if field.data_type() == &DataType::Utf8 && info.collation(index)? != "BINARY" {
                continue;
            }
            candidates.push(CandidateConstraint { column, op });
            sqlite_indexes.push(index);
        }
        let (mut plan, selected) =
            AccessPlan::select(self.table_id, &self.table, &candidates).map_err(into_rusqlite_error)?;
        plan.preserves_order = order_by_matches_key(info, &self.table);
        let used = info.col_used();
        // SQLite bit 63 represents every column from index 63 onward.
        plan.projection.retain(|&column| used & (1u64 << column.min(63)) != 0);
        for (slot, candidate) in plan.search.iter().zip(selected) {
            let mut usage = info.constraint_usage(sqlite_indexes[candidate]);
            usage.set_argv_index(slot.argv_index);
            // A runtime value may require SQLite's affinity conversion or
            // comparison rules. Keep its recheck even when we narrow the scan.
            usage.set_omit(false);
        }
        info.set_order_by_consumed(plan.preserves_order);
        info.set_estimated_cost(plan.estimate.estimated_cost);
        info.set_estimated_rows(plan.estimate.estimated_rows);
        info.set_idx_str(&to_hex(&plan.encode().map_err(into_rusqlite_error)?));
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(BeechCursor {
            base: sqlite3_vtab_cursor::default(),
            vtab: self,
            scan: None,
            batch: None,
            row: 0,
            projection: Vec::new(),
            transaction_scan: None,
            transaction_row: None,
        })
    }
}

impl<'vtab> CreateVTab<'vtab> for BeechTable {
    const KIND: VTabKind = VTabKind::Default;
}

impl UpdateVTab<'_> for BeechTable {
    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()> {
        let row_id = sqlite_row_id(arg)?;
        let schema = self.table.schema().clone();
        let state = self.ensure_write().map_err(into_rusqlite_error)?;
        let working = state.working.get_mut();
        let key = match state.row_keys.get_mut().remove(&row_id) {
            Some(key) => key,
            None => {
                let row = working
                    .row_by_id(row_id)
                    .map_err(into_rusqlite_error)?
                    .ok_or_else(|| rusqlite::Error::ModuleError(format!("no row with rowid {row_id}")))?;
                schema.key_from_row(&row).map_err(into_rusqlite_error)?
            }
        };
        let change = Change::Delete { key };
        state.mutations.push(change.clone()).map_err(into_rusqlite_error)?;
        working.apply(change).map_err(into_rusqlite_error)?;
        state.dirty = true;
        Ok(())
    }

    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let schema = self.table.schema().clone();
        check_mutation_arity(args.len(), schema.fields().len())?;
        let requested = args.iter().nth(1).expect("arity checked");
        let record = sqlite_record(&schema, args.iter().skip(2))?;
        let state = self.ensure_write().map_err(into_rusqlite_error)?;
        let working = state.working.get_mut();
        let (row_id, explicit_row_id) = match requested {
            ValueRef::Null => (working.next_row_id().map_err(into_rusqlite_error)?, false),
            value => (sqlite_row_id(value)?, true),
        };
        if explicit_row_id && working.row_by_id(row_id).map_err(into_rusqlite_error)?.is_some() {
            return Err(rusqlite::Error::ModuleError(format!("duplicate rowid {row_id}")));
        }
        let key = schema.key_from_row(&(row_id, record.clone())).map_err(into_rusqlite_error)?;
        if working.row_by_key(&key).map_err(into_rusqlite_error)?.is_some() {
            return Err(rusqlite::Error::ModuleError("duplicate key".into()));
        }
        let change = Change::Insert { key, row_id, record };
        state.mutations.push(change.clone()).map_err(into_rusqlite_error)?;
        working.apply(change).map_err(into_rusqlite_error)?;
        state.dirty = true;
        Ok(row_id)
    }

    fn update(&mut self, args: &Updates<'_>) -> Result<()> {
        let schema = self.table.schema().clone();
        check_mutation_arity(args.len(), schema.fields().len())?;
        let mut values = args.iter();
        let old_row_id = sqlite_row_id(values.next().expect("arity checked"))?;
        let new_row_id = sqlite_row_id(values.next().expect("arity checked"))?;
        let record = sqlite_record(&schema, values)?;
        let state = self.ensure_write().map_err(into_rusqlite_error)?;
        let working = state.working.get_mut();
        let old_key = match state.row_keys.get_mut().remove(&old_row_id) {
            Some(key) => key,
            None => {
                let row = working.row_by_id(old_row_id).map_err(into_rusqlite_error)?.ok_or_else(|| {
                    rusqlite::Error::ModuleError(format!("no row with rowid {old_row_id}"))
                })?;
                schema.key_from_row(&row).map_err(into_rusqlite_error)?
            }
        };
        if old_row_id != new_row_id && working.row_by_id(new_row_id).map_err(into_rusqlite_error)?.is_some()
        {
            return Err(rusqlite::Error::ModuleError(format!(
                "duplicate rowid {new_row_id}"
            )));
        }
        let new_key = schema.key_from_row(&(new_row_id, record.clone())).map_err(into_rusqlite_error)?;
        if old_key.compare_key(&new_key).map_err(into_rusqlite_error)?.is_ne()
            && working.row_by_key(&new_key).map_err(into_rusqlite_error)?.is_some()
        {
            return Err(rusqlite::Error::ModuleError("duplicate key".into()));
        }
        if old_key.compare_key(&new_key).map_err(into_rusqlite_error)?.is_eq() {
            let change = Change::Update {
                key: new_key,
                row_id: new_row_id,
                record,
            };
            state.mutations.push(change.clone()).map_err(into_rusqlite_error)?;
            working.apply(change).map_err(into_rusqlite_error)?;
        } else {
            let delete = Change::Delete { key: old_key };
            let insert = Change::Insert {
                key: new_key,
                row_id: new_row_id,
                record,
            };
            state.mutations.push(delete.clone()).map_err(into_rusqlite_error)?;
            state.mutations.push(insert.clone()).map_err(into_rusqlite_error)?;
            working.apply(delete).map_err(into_rusqlite_error)?;
            working.apply(insert).map_err(into_rusqlite_error)?;
        }
        state.dirty = true;
        Ok(())
    }
}

impl TransactionVTab<'_> for BeechTable {
    fn begin(&mut self) -> Result<()> {
        self.write = None;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        let Some(mut state) = self.write.take() else {
            return Ok(());
        };
        if !state.dirty {
            self.write = Some(state);
            return Ok(());
        }
        if state.pending_commit.is_some() {
            self.write = Some(state);
            return Ok(());
        }
        let mut writer =
            FileWriter::new(&self.data_path).map_err(BeechError::from).map_err(into_rusqlite_error)?;
        let current_root = Id::from_hex(
            std::fs::read_to_string(self.data_path.join("root"))
                .map_err(BeechError::from)
                .map_err(into_rusqlite_error)?
                .trim(),
        )
        .map_err(into_rusqlite_error)?;
        if current_root != self.root_id {
            return Err(rusqlite::Error::ModuleError(
                "repository changed since this virtual table connected".into(),
            ));
        }
        let mut commit_view = WorkingTable::new(
            self.table.as_ref().clone(),
            self.repository.clone(),
            BuildOptions::default(),
        )
        .map_err(into_rusqlite_error)?;
        let mutations = std::mem::replace(
            &mut state.mutations,
            MutationBatch::new(SortLimits::default()).map_err(into_rusqlite_error)?,
        );
        mutations.apply(&mut commit_view).map_err(into_rusqlite_error)?;
        let table = commit_view.finish(&mut writer).map_err(into_rusqlite_error)?;
        let publication = publish_table(
            &mut writer,
            &table,
            self.tables.clone(),
            Some(self.transaction_id),
        )
        .map_err(into_rusqlite_error)?;
        state.pending_commit = Some(PendingCommit {
            writer,
            table,
            root_id: publication.root_id,
            table_id: publication.table_id,
        });
        self.write = Some(state);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let Some(mut state) = self.write.take() else {
            return Ok(());
        };
        let Some(pending) = state.pending_commit.take() else {
            return Ok(());
        };
        pending.writer.commit().map_err(BeechError::from).map_err(into_rusqlite_error)?;
        self.table = Arc::new(pending.table);
        self.table_id = pending.table_id;
        self.root_id = pending.root_id;
        self.tables.insert(self.table_name.clone(), pending.table_id);
        self.transaction_id =
            self.repository.get_root(&pending.root_id).map_err(into_rusqlite_error)?.transaction_id();
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        if let Some(mut state) = self.write.take()
            && let Some(pending) = state.pending_commit.take()
        {
            pending.writer.abort().map_err(BeechError::from).map_err(into_rusqlite_error)?;
        }
        Ok(())
    }
}

fn order_by_matches_key(info: &IndexInfo, table: &Table) -> bool {
    info.order_bys().enumerate().all(|(part, order)| {
        let Ok(column) = usize::try_from(order.column()) else {
            return false;
        };
        !order.is_order_by_desc()
            && table.schema().key_columns().get(part) == Some(&column)
            // Floats may contain NaNs (SQLite exposes these as NULL), while
            // decimal/unsigned values are exposed as text. Let SQLite sort them.
            && matches!(table.schema().fields()[column].data_type(),
                DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::Binary)
        // Text ORDER BY collation is not exposed by IndexInfo.
    })
}

fn from_sqlite_op(op: IndexConstraintOp) -> Option<ConstraintOp> {
    use IndexConstraintOp::*;
    Some(match op {
        SQLITE_INDEX_CONSTRAINT_EQ => ConstraintOp::Eq,
        SQLITE_INDEX_CONSTRAINT_GT => ConstraintOp::Gt,
        SQLITE_INDEX_CONSTRAINT_LE => ConstraintOp::Le,
        SQLITE_INDEX_CONSTRAINT_LT => ConstraintOp::Lt,
        SQLITE_INDEX_CONSTRAINT_GE => ConstraintOp::Ge,
        _ => return None,
    })
}

/// Only push comparisons whose values need no SQLite coercion. A failed
/// conversion means a broader scan, followed by SQLite's normal WHERE check.
fn search_value(typ: &DataType, value: ValueRef<'_>) -> Option<Scalar> {
    Some(match (typ, value) {
        (DataType::Boolean, ValueRef::Integer(i @ 0..=1)) => Scalar::Boolean(i != 0),
        (DataType::Int32, ValueRef::Integer(i)) => Scalar::Int32(i.try_into().ok()?),
        (DataType::Int64, ValueRef::Integer(i)) => Scalar::Int64(i),
        (DataType::Utf8, ValueRef::Text(text)) => Scalar::Utf8(std::str::from_utf8(text).ok()?.into()),
        (DataType::Binary, ValueRef::Blob(bytes)) => Scalar::Binary(bytes.into()),
        _ => return None,
    })
}

fn check_mutation_arity(actual: usize, columns: usize) -> Result<()> {
    if actual == columns + 2 {
        Ok(())
    } else {
        Err(rusqlite::Error::ModuleError(format!(
            "mutation has {actual} arguments, expected {}",
            columns + 2
        )))
    }
}

fn sqlite_row_id(value: ValueRef<'_>) -> Result<i64> {
    match value {
        ValueRef::Integer(value) => Ok(value),
        _ => Err(rusqlite::Error::ModuleError("rowid must be an integer".into())),
    }
}

fn sqlite_record<'a>(
    schema: &beech_core::TableSchema,
    values: impl Iterator<Item = ValueRef<'a>>,
) -> Result<Vec<Scalar>> {
    schema
        .fields()
        .iter()
        .zip(values)
        .map(|(field, value)| sqlite_scalar(field.data_type(), value))
        .collect()
}

fn sqlite_scalar(typ: &DataType, value: ValueRef<'_>) -> Result<Scalar> {
    if matches!(value, ValueRef::Null) {
        return Ok(Scalar::Null);
    }
    let invalid = || rusqlite::Error::ModuleError(format!("value does not match {typ}"));
    match (typ, value) {
        (DataType::Boolean, ValueRef::Integer(0)) => Ok(Scalar::Boolean(false)),
        (DataType::Boolean, ValueRef::Integer(1)) => Ok(Scalar::Boolean(true)),
        (DataType::Int32, ValueRef::Integer(value)) => {
            value.try_into().map(Scalar::Int32).map_err(|_| invalid())
        }
        (DataType::Int64, ValueRef::Integer(value)) => Ok(Scalar::Int64(value)),
        (DataType::UInt64, ValueRef::Text(value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok())
            .map(Scalar::UInt64)
            .ok_or_else(invalid),
        (DataType::Float32, ValueRef::Real(value)) => Ok(Scalar::Float32(value as f32)),
        (DataType::Float32, ValueRef::Integer(value)) => Ok(Scalar::Float32(value as f32)),
        (DataType::Float64, ValueRef::Real(value)) => Ok(Scalar::Float64(value)),
        (DataType::Float64, ValueRef::Integer(value)) => Ok(Scalar::Float64(value as f64)),
        (DataType::Decimal128(_, scale), ValueRef::Text(value)) => {
            let value = std::str::from_utf8(value).map_err(|_| invalid())?;
            parse_decimal(value, u8::try_from(*scale).map_err(|_| invalid())?)
                .map(Scalar::Decimal)
                .ok_or_else(invalid)
        }
        (DataType::Utf8, ValueRef::Text(value)) => {
            std::str::from_utf8(value).map(|value| Scalar::Utf8(value.into())).map_err(|_| invalid())
        }
        (DataType::Binary, ValueRef::Blob(value)) => Ok(Scalar::Binary(value.into())),
        _ => Err(invalid()),
    }
}

fn parse_decimal(text: &str, scale: u8) -> Option<Decimal> {
    let (negative, unsigned) = text.strip_prefix('-').map_or((false, text), |rest| (true, rest));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return None;
    }
    let digits = format!("{whole}{fraction:0<width$}", width = scale as usize);
    let unsigned: u128 = digits.parse().ok()?;
    let unscaled =
        if negative { -(i128::try_from(unsigned).ok()?) } else { i128::try_from(unsigned).ok()? };
    Decimal::new(unscaled, scale).ok()
}

#[repr(C)]
struct BeechCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    vtab: &'vtab BeechTable,
    scan: Option<Scan<'vtab>>,
    batch: Option<RecordBatch>,
    row: usize,
    projection: Vec<usize>,
    transaction_scan: Option<WorkingScan>,
    transaction_row: Option<Row>,
}

impl BeechCursor<'_> {
    fn load_batch(&mut self) -> Result<()> {
        self.row = 0;
        self.batch = None;
        if let Some(scan) = &mut self.scan {
            for batch in scan {
                let batch = batch.map_err(into_rusqlite_error)?;
                if batch.num_rows() > 0 {
                    self.batch = Some(batch);
                    break;
                }
            }
        }
        Ok(())
    }

    fn current_batch(&self) -> Result<&RecordBatch> {
        self.batch.as_ref().ok_or_else(|| rusqlite::Error::ModuleError("cursor is at EOF".into()))
    }

    fn next_transaction_row(&mut self) -> Result<()> {
        let Some(scan) = &mut self.transaction_scan else {
            return Ok(());
        };
        let write = self
            .vtab
            .write
            .as_ref()
            .ok_or_else(|| rusqlite::Error::ModuleError("transaction ended during scan".into()))?;
        self.transaction_row = write
            .working
            .try_borrow_mut()
            .map_err(|_| rusqlite::Error::ModuleError("working table is already borrowed".into()))?
            .next_row(scan)
            .map_err(into_rusqlite_error)?;
        Ok(())
    }
}

// SAFETY: repr(C), with the required base first. SQLite closes cursors before
// disconnecting their vtab, so the borrowed repository outlives each scan.
unsafe impl VTabCursor for BeechCursor<'_> {
    fn filter(&mut self, _idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        self.scan = None;
        self.batch = None;
        self.transaction_scan = None;
        self.transaction_row = None;
        self.row = 0;
        let text = idx_str.ok_or_else(|| rusqlite::Error::ModuleError("missing access plan".into()))?;
        let plan = AccessPlan::decode(&from_hex(text).map_err(into_rusqlite_error)?)
            .map_err(into_rusqlite_error)?;
        if args.len() != plan.search.len() {
            return Err(rusqlite::Error::ModuleError(
                "plan argument count mismatch".into(),
            ));
        }
        let values = plan
            .search
            .iter()
            .zip(args.iter())
            .map(|(slot, value)| {
                self.vtab
                    .table
                    .schema()
                    .fields()
                    .get(slot.column as usize)
                    .and_then(|field| search_value(field.data_type(), value))
            })
            .collect::<Vec<_>>();
        let request =
            plan.bind(self.vtab.table_id, &self.vtab.table, &values).map_err(into_rusqlite_error)?;
        self.projection.clone_from(&request.projection);
        if let Some(write) = &self.vtab.write {
            let scan = write
                .working
                .try_borrow()
                .map_err(|_| rusqlite::Error::ModuleError("working table is already borrowed".into()))?
                .scan();
            self.transaction_scan = Some(scan);
            return self.next_transaction_row();
        }
        self.scan = Some(
            Scan::new(self.vtab.repository.as_ref(), &self.vtab.table, request)
                .map_err(into_rusqlite_error)?,
        );
        self.load_batch()
    }

    fn next(&mut self) -> Result<()> {
        if self.transaction_scan.is_some() {
            return self.next_transaction_row();
        }
        if let Some(batch) = &self.batch {
            self.row += 1;
            if self.row >= batch.num_rows() {
                self.load_batch()?;
            }
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        if self.transaction_scan.is_some() { self.transaction_row.is_none() } else { self.batch.is_none() }
    }

    fn column(&self, ctx: &mut Context, column: c_int) -> Result<()> {
        let column =
            usize::try_from(column).map_err(|_| rusqlite::Error::InvalidColumnIndex(usize::MAX))?;
        if let Some(row) = &self.transaction_row {
            let value = row.1.get(column).ok_or(rusqlite::Error::InvalidColumnIndex(column))?;
            return set_scalar(ctx, value);
        }
        let projected = self
            .projection
            .iter()
            .position(|&c| c == column)
            .ok_or(rusqlite::Error::InvalidColumnIndex(column))?;
        set_column(
            ctx,
            self.current_batch()?.column(projected + 1).as_ref(),
            self.row,
        )
    }

    fn rowid(&self) -> Result<i64> {
        if self.transaction_scan.is_some() {
            let row = self
                .transaction_row
                .as_ref()
                .ok_or_else(|| rusqlite::Error::ModuleError("cursor is at EOF".into()));
            let row = row?;
            if let Some(write) = &self.vtab.write {
                let key = self.vtab.table.schema().key_from_row(row).map_err(into_rusqlite_error)?;
                write.row_keys.borrow_mut().insert(row.0, key);
            }
            return Ok(row.0);
        }
        let array = self
            .current_batch()?
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| rusqlite::Error::ModuleError("invalid row-id array".into()))?;
        Ok(array.value(self.row))
    }
}

fn set_scalar(ctx: &mut Context, value: &Scalar) -> Result<()> {
    match value {
        Scalar::Null => ctx.set_result(&rusqlite::types::Null),
        Scalar::Boolean(value) => ctx.set_result(value),
        Scalar::Int32(value) => ctx.set_result(value),
        Scalar::Int64(value) => ctx.set_result(value),
        Scalar::UInt64(value) => ctx.set_result(&value.to_string()),
        Scalar::Float32(value) => ctx.set_result(value),
        Scalar::Float64(value) => ctx.set_result(value),
        Scalar::Decimal(value) => ctx.set_result(&decimal_text(value.unscaled(), value.scale() as usize)),
        Scalar::Utf8(value) => ctx.set_result(value),
        Scalar::Binary(value) => ctx.set_result(value),
    }
}

fn decimal_text(unscaled: i128, scale: usize) -> String {
    let mut digits = format!("{:0width$}", unscaled.unsigned_abs(), width = scale + 1);
    if scale > 0 {
        digits.insert(digits.len() - scale, '.');
    }
    if unscaled < 0 {
        digits.insert(0, '-');
    }
    digits
}

/// Borrow text and binary values directly from the batch; SQLite copies them
/// into its result. No owned Row or Key is materialized for xColumn.
fn set_column(ctx: &mut Context, array: &dyn Array, row: usize) -> Result<()> {
    if array.is_null(row) {
        return ctx.set_result(&rusqlite::types::Null);
    }
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| rusqlite::Error::ModuleError("column array type mismatch".into()))?
                .value(row)
        };
    }
    match array.data_type() {
        DataType::Boolean => ctx.set_result(&value!(BooleanArray)),
        DataType::Int32 => ctx.set_result(&value!(Int32Array)),
        DataType::Int64 => ctx.set_result(&value!(Int64Array)),
        DataType::Float32 => ctx.set_result(&value!(Float32Array)),
        DataType::Float64 => ctx.set_result(&value!(Float64Array)),
        DataType::Utf8 => ctx.set_result(&value!(StringArray)),
        DataType::Binary => ctx.set_result(&value!(BinaryArray)),
        DataType::UInt64 => ctx.set_result(&value!(UInt64Array).to_string()),
        DataType::Decimal128(_, scale) => {
            let unscaled = value!(Decimal128Array);
            ctx.set_result(&decimal_text(unscaled, *scale as usize))
        }
        typ => Err(rusqlite::Error::ModuleError(format!(
            "unsupported column type {typ}"
        ))),
    }
}

fn sqlite_type(typ: &DataType) -> &'static str {
    match typ {
        DataType::Boolean | DataType::Int32 | DataType::Int64 => "INTEGER",
        DataType::Float32 | DataType::Float64 => "REAL",
        DataType::Binary => "BLOB",
        _ => "TEXT",
    }
}

fn into_rusqlite_error(error: BeechError) -> rusqlite::Error {
    use rusqlite::ffi;
    let code = match &error {
        BeechError::Io(_) => ffi::SQLITE_IOERR,
        BeechError::Query(_) | BeechError::NoSuchTable(_) => ffi::SQLITE_ERROR,
        _ => ffi::SQLITE_CORRUPT,
    };
    rusqlite::Error::SqliteFailure(ffi::Error::new(code), Some(error.to_string()))
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn parse_arg(arg: &[u8]) -> Result<String> {
    let arg = std::str::from_utf8(arg).map_err(|e| rusqlite::Error::ModuleError(e.to_string()))?.trim();
    for quote in ['\'', '"'] {
        if arg.len() >= 2 && arg.starts_with(quote) && arg.ends_with(quote) {
            return Ok(arg[1..arg.len() - 1].replace(&format!("{quote}{quote}"), &quote.to_string()));
        }
    }
    Ok(arg.into())
}

fn to_hex(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn from_hex(hex: &str) -> beech_core::Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) || !hex.is_ascii() {
        return Err(BeechError::Wire("invalid access-plan hex string".into()));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| BeechError::Wire(format!("invalid access-plan hex at byte {i}")))
        })
        .collect()
}

/// Register the writable Beech virtual table module with a SQLite connection.
pub fn create_beech_module(conn: &rusqlite::Connection) -> Result<()> {
    const MODULE: Module<'_, BeechTable> = Module::update_module_with_tx();
    conn.create_module::<BeechTable, _>("beech", &MODULE, None)
}

// SQLite finds this C entry point when the DLL is loaded with `.load`.
#[cfg(feature = "loadable_extension")]
#[unsafe(no_mangle)]
unsafe extern "C" fn sqlite3_extension_init(
    db: *mut rusqlite::ffi::sqlite3,
    error_message: *mut *mut std::ffi::c_char,
    api: *mut rusqlite::ffi::sqlite3_api_routines,
) -> c_int {
    // SAFETY: SQLite supplies the live connection, error output, and API table.
    // The callback only registers our module; rusqlite borrows the connection
    // and translates initialization errors into SQLite's return convention.
    unsafe {
        rusqlite::Connection::extension_init2(db, error_message, api, |connection| {
            create_beech_module(&connection)?;
            Ok(false) // SQLite may unload the DLL when the connection closes.
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argument_parsing() {
        assert_eq!(parse_arg(b"'a''b'").unwrap(), "a'b");
    }

    #[test]
    fn test_hex_functions() {
        let data = vec![0, 1, 2, 3, 10, 15, 255];
        assert_eq!(to_hex(&data), "000102030a0fff");
        assert_eq!(from_hex(&to_hex(&data)).unwrap(), data);
        assert!(from_hex("invalid").is_err());
        assert!(from_hex("0g").is_err());
    }
}
