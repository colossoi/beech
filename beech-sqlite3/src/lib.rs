//! SQLite Virtual Table Interface for Beech Prolly Trees
//!
//! This module provides a read-only SQLite virtual table implementation that allows
//! querying beech prolly trees using standard SQL syntax.
//!
//! # Usage
//!
//! ```sql
//! CREATE VIRTUAL TABLE my_table USING beech(
//!     'path/to/data',  -- Path to directory containing .bch files
//!     'source_name',   -- Source identifier (currently unused)
//!     'table_name'     -- Name of the table within the prolly tree
//! );
//!
//! SELECT * FROM my_table WHERE id > 100;
//! ```

use apache_avro::Schema;
use beech_core::plan::CandidateConstraint;
use beech_core::query::{Constraint, ConstraintOp};
use beech_core::{
    BeechError, Column, DomainError, Id, NodeSource, QueryError, SchemaError, StorageError, Table,
    WireError,
};
use log::debug;
use plan::AccessPlan;
use rusqlite::Result;
use rusqlite::ffi::ErrorCode;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor, VTabKind,
    read_only_module, sqlite3_vtab, sqlite3_vtab_cursor,
};
use std::collections::HashMap;
use std::ffi::c_int;
use std::path::PathBuf;
use std::sync::Arc;

mod plan;
mod store;

#[repr(C)]
struct BeechTable {
    base: sqlite3_vtab,
    remote_table_name: String,
    source: Arc<dyn NodeSource>,
    data_path: PathBuf,
    /// Snapshot of the table and its object ID, captured at connect
    /// time. Safe to hold for the vtab's lifetime: nodes, tables, and
    /// transactions are content-addressed and immutable; only the root
    /// file is mutable. A vtab that snapshots at connect keeps serving
    /// that snapshot until reopened.
    meta: TableMeta,
}

struct TableMeta {
    table: Arc<Table>,
    table_id: Id,
}
fn avro_type_to_sqlite_type(typ: &Schema) -> &str {
    use apache_avro::Schema::*;
    match typ {
        Boolean => "boolean",
        Int => "integer",
        Long => "integer",
        Float => "real",
        Double => "real",
        String => "text",
        Bytes => "blob",
        _ => "text",
    }
}

fn parse_options(args: &[String]) -> Result<HashMap<String, String>> {
    args.iter()
        .map(|s| {
            let pieces: Vec<&str> = s.splitn(2, "=").collect();
            match &pieces[..] {
                [k, v] => Ok((k.to_string(), v.to_string())),
                [k] => Ok((k.to_string(), "".to_string())),
                _ => Err(rusqlite::Error::InvalidParameterName(format!(
                    "Invalid option: {s}"
                ))),
            }
        })
        .collect()
}

fn to_column_spec(c: &Column) -> Option<String> {
    if c.name == "rowid" {
        None
    } else {
        let col_type = avro_type_to_sqlite_type(&c.typ);
        Some(format!("{} {}", c.name, col_type))
    }
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
    fn decode_nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    let bytes = hex.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(WireError::InvalidHex("odd-length hex string".to_string()).into());
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    for i in (0..bytes.len()).step_by(2) {
        let high = decode_nibble(bytes[i])
            .ok_or_else(|| WireError::InvalidHex(format!("invalid digit: {:?}", bytes[i] as char)))?;
        let low = decode_nibble(bytes[i + 1])
            .ok_or_else(|| WireError::InvalidHex(format!("invalid digit: {:?}", bytes[i + 1] as char)))?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn parse_arg(a: &[u8]) -> Result<String> {
    let mut arg = String::from_utf8(a.to_vec()).map_err(|_| {
        rusqlite::Error::InvalidParameterName(format!("Invalid argument: {}", String::from_utf8_lossy(a)))
    })?;

    // Strip surrounding single or double quotes if they exist
    if (arg.starts_with('\'') && arg.ends_with('\'')) || (arg.starts_with('"') && arg.ends_with('"')) {
        arg = arg[1..arg.len() - 1].to_string();
    }

    Ok(arg)
}

fn avro_value_from_sqlite(value: ValueRef<'_>) -> beech_core::Result<apache_avro::types::Value> {
    use apache_avro::types::Value;
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(i) => Ok(Value::Long(i)),
        ValueRef::Real(f) => Ok(Value::Double(f)),
        ValueRef::Text(s) => Ok(Value::String(std::str::from_utf8(s).map(str::to_string)?)),
        ValueRef::Blob(b) => Ok(Value::Bytes(b.to_vec())),
    }
}

impl BeechTable {
    fn do_connect(
        _db: &mut VTabConnection,
        _module_name: &str,
        local_table_name: &str,
        data_path: &str,
        _source_name: &str,
        remote_table_name: &str,
        _options: &HashMap<String, String>,
    ) -> beech_core::Result<(String, Self)> {
        let data_path = PathBuf::from(data_path);

        // Create FileStore with proper .bch extension
        let store = store::file::FileStore::new(data_path.clone(), |key: &Id| {
            PathBuf::from(format!("{}.bch", key))
        });
        let source = beech_core::source::LocalFile::new(store);

        // Read the root file to get the current transaction ID and resolve
        // the immutable snapshot we'll serve for this vtab's lifetime.
        let root_file_path = data_path.join("root");
        let transaction_id_str = std::fs::read_to_string(&root_file_path)?;
        let transaction_id = Id::from_hex(transaction_id_str.trim())?;
        let transaction = source.get_transaction(&transaction_id)?;
        let tab = source.get_table(&transaction, remote_table_name)?;
        let table_id = *transaction
            .tables()
            .get(remote_table_name)
            .ok_or_else(|| BeechError::NoSuchTable(remote_table_name.into()))?;

        let column_spec: String =
            tab.columns().iter().filter_map(to_column_spec).collect::<Vec<_>>().join(", ");
        let create_sql = format!("CREATE TABLE {local_table_name} ({column_spec});");

        let table = BeechTable {
            base: sqlite3_vtab::default(),
            remote_table_name: remote_table_name.to_string(),
            source: Arc::new(source),
            data_path,
            meta: TableMeta { table: tab, table_id },
        };
        Ok((create_sql, table))
    }
    fn do_best_index(&self, info: &mut IndexInfo) -> beech_core::Result<()> {
        let table = &self.meta.table;

        // Gather candidate constraints, dropping unsupported operators.
        let candidates: Vec<CandidateConstraint> = info
            .constraints_and_usages()
            .filter_map(|(c, _)| {
                let column = usize::try_from(c.column()).ok()?;
                from_sqlite_op(c.operator()).map(|op| CandidateConstraint { column, op })
            })
            .collect();

        let mut plan = AccessPlan::select(self.meta.table_id, table, &candidates)?;

        // ORDER BY consumption: the cursor walks leaves in ascending key
        // order. If SQLite's requested ordering is a leading prefix of the
        // key columns, all ascending, we can satisfy it for free.
        plan.preserves_order = order_by_matches_key(info, table);
        info.set_order_by_consumed(plan.preserves_order);

        // Mark each consumed constraint with its argv slot and tell SQLite
        // the cursor honors it (omit recheck).
        for (sqlite_constraint, mut usage) in info.constraints_and_usages() {
            let Some(op) = from_sqlite_op(sqlite_constraint.operator()) else {
                continue;
            };
            let col = sqlite_constraint.column();
            if let Some(slot) = plan.search.iter().find(|s| s.column == col && s.op == op) {
                usage.set_argv_index(slot.argv_index);
                usage.set_omit(true);
            }
        }

        info.set_estimated_cost(plan.estimate.estimated_cost);
        info.set_estimated_rows(plan.estimate.estimated_rows);

        // Serialize plan to idx_str for the filter() side.
        let bytes = plan.encode()?;
        info.set_idx_str(&to_hex(&bytes));
        Ok(())
    }
    fn do_open(&self) -> beech_core::Result<BeechCursor> {
        let cursor = BeechCursor::new(&self.meta.table, Arc::clone(&self.source));
        Ok(cursor)
    }
}

/// True iff SQLite's ORDER BY is satisfied by ascending key-part traversal.
///
/// The cursor walks leaves left-to-right, producing rows in ascending key
/// order. We can consume the ORDER BY if:
/// - it is empty (trivially satisfied), or
/// - it lists a leading prefix of the key columns in key-part order, all
///   ascending.
///
/// Anything DESC, any non-key column, or any reordering fails the check
/// and SQLite will add its own sort.
fn order_by_matches_key(info: &IndexInfo, table: &Table) -> bool {
    let order = info.order_bys();
    let mut seen = 0usize;
    for ob in order {
        if ob.is_order_by_desc() {
            return false;
        }
        let col = ob.column();
        let Some(part) = table.column_key_index(col as usize) else {
            return false;
        };
        if part != seen {
            return false;
        }
        seen += 1;
    }
    true
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

unsafe impl<'vtab> VTab<'vtab> for BeechTable {
    type Aux = ();
    type Cursor = BeechCursor;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        let parsed_args = args.iter().map(|a| parse_arg(a)).collect::<Result<Vec<String>>>()?;

        match &parsed_args[..]
        {
            [
                module_name_arg,        // args[0] = "beech"
                _database_name_arg,     // args[1] = "main" (ignore)
                local_table_name_arg,   // args[2] = "test_table"
                data_path_arg,          // args[3] = "/tmp/test_data"
                source_arg,             // args[4] = "test_source"
                remote_table_arg,       // args[5] = "table"
                option_args @ ..,
            ] => {
                let options = parse_options(option_args).map_err(|_| {
                    rusqlite::Error::InvalidParameterName(format!(
                        "Invalid options: {}",
                        option_args
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(" ")
                    ))
                })?;

                Self::do_connect(
                    db,
                    module_name_arg,
                    local_table_name_arg,
                    data_path_arg,      // Now correctly points to args[3]
                    source_arg,
                    remote_table_arg,
                    &options,
                )
                .map_err(into_rusqlite_error)
            }
            _ => Err(rusqlite::Error::InvalidParameterName(
                "Usage: CREATE VIRTUAL TABLE name USING beech(data_path, source_name, table_name [, options...])".to_string()
            )),
        }
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        self.do_best_index(info).map_err(into_rusqlite_error)
    }
    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        self.do_open().map_err(into_rusqlite_error)
    }
}

impl<'vtab> CreateVTab<'vtab> for BeechTable {
    const KIND: VTabKind = VTabKind::Default;

    fn create(db: &mut VTabConnection, aux: Option<&Self::Aux>, args: &[&[u8]]) -> Result<(String, Self)> {
        // For our virtual table, create and connect are the same
        Self::connect(db, aux, args)
    }
}

fn into_rusqlite_error(be: BeechError) -> rusqlite::Error {
    let message = be.to_string();
    let code = match &be {
        BeechError::Storage(StorageError::KeyNotFound { .. }) => ErrorCode::NotFound,
        BeechError::Storage(StorageError::Io(_)) | BeechError::Storage(StorageError::Mmap { .. }) => {
            ErrorCode::OperationInterrupted
        }
        BeechError::Domain(DomainError::NoSuchTable { .. })
        | BeechError::Domain(DomainError::KeyNotFound { .. }) => ErrorCode::NotFound,
        BeechError::Domain(DomainError::DuplicateKey { .. }) => ErrorCode::ConstraintViolation,
        BeechError::Domain(DomainError::InvalidArgs(_)) => ErrorCode::ApiMisuse,
        BeechError::Schema(_) => ErrorCode::TypeMismatch,
        BeechError::Wire(_) | BeechError::Query(_) => ErrorCode::DatabaseCorrupt,
    };
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error {
            code,
            extended_code: 0,
        },
        Some(message),
    )
}

#[repr(C)]
struct BeechCursor {
    base: sqlite3_vtab_cursor,
    cursor: beech_core::query::RowCursor,
    source: Arc<dyn NodeSource>,
}

impl BeechCursor {
    fn new(table: &Table, source: Arc<dyn NodeSource>) -> Self {
        Self {
            base: sqlite3_vtab_cursor::default(),
            cursor: beech_core::query::RowCursor::new(table),
            source,
        }
    }

    fn get_current_row(&self) -> beech_core::Result<Option<Vec<apache_avro::types::Value>>> {
        if let Some((node_id, row_idx)) = self.cursor.current() {
            let node = self.source.get_node(node_id, &self.cursor.table.schema)?;

            match &*node {
                beech_core::Node::Leaf(leaf) => {
                    if let Some(entry) = leaf.entry(*row_idx) {
                        Ok(Some(entry.values().to_vec()))
                    } else {
                        Ok(None)
                    }
                }
                beech_core::Node::Internal(_) => Err(QueryError::UnexpectedNodeType {
                    expected: "leaf",
                    got: "branch",
                }
                .into()),
            }
        } else {
            Ok(None)
        }
    }
    fn do_filter(
        &mut self,
        _idx_num: c_int,
        maybe_idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> beech_core::Result<()> {
        // Decode the AccessPlan produced by xBestIndex. xBestIndex always
        // emits one (possibly empty) plan, so an absent idx_str is a bug.
        let idx_str = maybe_idx_str.ok_or_else(|| {
            SchemaError::Mismatch("xFilter called without an AccessPlan in idx_str".to_string())
        })?;
        let bytes = from_hex(idx_str)?;
        let plan = AccessPlan::decode(&bytes)?;

        if plan.table_id != self.cursor.table.id {
            return Err(SchemaError::Mismatch("table id mismatch in AccessPlan".to_string()).into());
        }
        debug!(
            "beech_filter(): schema matches, {} search slots",
            plan.search.len()
        );

        // Build the cursor's (constraint, value) pairs from the plan in
        // key-part order, pulling each value from the explicitly-specified
        // argv slot rather than relying on iteration order.
        let arg_refs: Vec<_> = args.iter().collect();
        let mut constraints = Vec::with_capacity(plan.search.len());
        let mut values = Vec::with_capacity(plan.search.len());
        for slot in &plan.search {
            let idx = slot.argv_index as usize - 1;
            let value_ref = arg_refs.get(idx).ok_or_else(|| {
                SchemaError::Mismatch(format!(
                    "AccessPlan references argv_index {} but only {} args provided",
                    slot.argv_index,
                    arg_refs.len()
                ))
            })?;
            let avro_value = avro_value_from_sqlite(*value_ref)?;
            constraints.push(Constraint::new(slot.column, slot.op));
            values.push(avro_value);
        }

        self.cursor.init(constraints, values);
        self.cursor.advance_to_left(&*self.source)?;
        Ok(())
    }
}

unsafe impl VTabCursor for BeechCursor {
    // Required methods
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        self.do_filter(idx_num, idx_str, args).map_err(into_rusqlite_error)
    }
    fn next(&mut self) -> Result<()> {
        self.cursor.next(&*self.source).map_err(into_rusqlite_error)
    }
    fn eof(&self) -> bool {
        self.cursor.eof()
    }
    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        match self.get_current_row() {
            Ok(Some(values)) => {
                if let Some(field_value) = values.get(i as usize) {
                    // Convert Avro value to SQLite value
                    match field_value {
                        apache_avro::types::Value::Null => ctx.set_result(&rusqlite::types::Null)?,
                        apache_avro::types::Value::Boolean(b) => ctx.set_result(b)?,
                        apache_avro::types::Value::Int(n) => ctx.set_result(n)?,
                        apache_avro::types::Value::Long(n) => ctx.set_result(n)?,
                        apache_avro::types::Value::Float(f) => ctx.set_result(f)?,
                        apache_avro::types::Value::Double(f) => ctx.set_result(f)?,
                        apache_avro::types::Value::Bytes(b) => ctx.set_result(b)?,
                        apache_avro::types::Value::String(s) => ctx.set_result(s)?,
                        _ => {
                            // For complex types, convert to string representation
                            ctx.set_result(&format!("{field_value:?}"))?
                        }
                    }
                } else {
                    ctx.set_result(&rusqlite::types::Null)?;
                }
                Ok(())
            }
            Ok(None) => {
                ctx.set_result(&rusqlite::types::Null)?;
                Ok(())
            }
            Err(e) => Err(into_rusqlite_error(e)),
        }
    }
    fn rowid(&self) -> Result<i64> {
        let Some((node_id, row_idx)) = self.cursor.current() else {
            return Ok(0);
        };
        let node = self.source.get_node(node_id, &self.cursor.table.schema).map_err(into_rusqlite_error)?;
        match &*node {
            beech_core::Node::Leaf(leaf) => {
                let entry = leaf.entry(*row_idx).ok_or_else(|| {
                    into_rusqlite_error(
                        QueryError::ChildIndexOutOfBounds {
                            index: *row_idx,
                            len: leaf.len(),
                        }
                        .into(),
                    )
                })?;
                Ok(entry.row_id())
            }
            beech_core::Node::Internal(_) => Err(into_rusqlite_error(
                QueryError::UnexpectedNodeType {
                    expected: "leaf",
                    got: "branch",
                }
                .into(),
            )),
        }
    }
}

/// Create and register the beech virtual table module with SQLite
pub fn create_beech_module(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let module = read_only_module::<BeechTable>();
    conn.create_module::<BeechTable, _>("beech", module, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argument_parsing() {
        // Test parse_options function
        let args = vec!["key1=value1".to_string(), "key2=value2".to_string()];
        let options = parse_options(&args).unwrap();

        assert_eq!(options.get("key1"), Some(&"value1".to_string()));
        assert_eq!(options.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_hex_functions() {
        let data = vec![0x00, 0x01, 0x02, 0x03, 0x0a, 0x0f, 0xff];
        let hex = to_hex(&data);
        assert_eq!(hex, "000102030a0fff");

        let decoded = from_hex(&hex).unwrap();
        assert_eq!(decoded, data);

        // Test invalid hex
        assert!(from_hex("invalid").is_err());
        assert!(from_hex("0g").is_err());
    }
}
