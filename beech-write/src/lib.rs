use apache_avro::{
    schema::{Name, RecordField},
    types::Value,
    Schema,
};
use beech_core::{
    wire::{encode_node, encode_root, encode_table, encode_transaction, find_key_columns},
    DomainError, Id, InternalNode, Key, KeyOrdering, LeafEntry, LeafNode, Node, NodeSource, QueryError,
    Result, Root, SchemaError, Table, TableSchema, Transaction,
};
use beech_shaper::ProbShaper;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    path::Path,
    time::SystemTime,
};

/// Transactional sink for node/table/transaction/root writes.
pub trait Writer {
    fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>;

    fn commit(self) -> std::io::Result<()>;
    fn abort(self) -> std::io::Result<()>;
    fn num_to_commit(&self) -> usize;
}

/// Summary retained after a node has been built and written.
/// Parents use this to assemble branch nodes.
#[derive(Debug, Clone)]
pub struct NodeSummary {
    pub id: Id,
    pub highest_key: Key,
    pub depth: i32,
    pub row_count: i64,
}

/// Represents a change to apply to a table.
#[derive(Debug, Clone)]
pub enum Change {
    Insert {
        key: Key,
        row_id: i64,
        record: Vec<Value>,
    },
    Update {
        key: Key,
        row_id: i64,
        record: Vec<Value>,
    },
    Delete {
        key: Key,
    },
}

impl Change {
    pub fn key(&self) -> &Key {
        match self {
            Change::Insert { key, .. } | Change::Update { key, .. } | Change::Delete { key } => key,
        }
    }
}

/// Fully materialized row used by build/merge pipelines.
#[derive(Debug, Clone)]
struct RowRecord {
    key: Key,
    row_id: i64,
    values: Vec<Value>,
}

/// Internal representation of a node before/after write.
#[derive(Debug, Clone)]
struct BuiltNode {
    id: Id,
    node: Node,
    highest_key: Key,
    depth: i32,
    row_count: i64,
}

impl BuiltNode {
    fn summary(&self) -> NodeSummary {
        NodeSummary {
            id: self.id.clone(),
            highest_key: self.highest_key.clone(),
            depth: self.depth,
            row_count: self.row_count,
        }
    }
}

// =============================================================================
// Public entry points
// =============================================================================

/// Rebuild a table tree from sorted input rows and write table/transaction/root.
/// This is the simple "create a fresh version" path.
pub fn write_rows_to_prolly_tree<W: Writer>(
    writer: &mut W,
    table_name: String,
    key_column_indices: Vec<usize>,
    rows: Vec<(i64, Value)>, // (row_id, record)
    target_node_size: usize,
    node_size_stddev: usize,
    prev_id: Option<Id>,
) -> Result<(Id, Id)> {
    if rows.is_empty() {
        return Err(DomainError::InvalidArgs("no rows provided".to_string()).into());
    }

    let row_schema = infer_row_schema_from_record(&rows[0].1)?;
    let key_schema = extract_key_schema(&row_schema, &key_column_indices)?;
    let schema = TableSchema {
        key_scheme: key_schema,
        row_scheme: row_schema,
    };
    let key_columns = find_key_columns(&schema)?;

    let mut row_records = Vec::with_capacity(rows.len());
    for (row_id, record) in rows {
        let values = record_to_fields(&record)?;
        let key = key_from_row_values(&key_columns, &values);
        row_records.push(RowRecord { key, row_id, values });
    }

    row_records.sort_by(|a, b| a.key.compare_key(&b.key));
    reject_duplicate_keys(&row_records)?;

    let root_node_id =
        build_and_write_tree(writer, &schema, row_records, target_node_size, node_size_stddev)?;
    write_table_transaction_and_root(writer, table_name, schema, root_node_id, prev_id)
}

/// Apply sorted changes to an existing table and write a fresh tree.
///
/// This implementation chooses correctness and clarity over incremental
/// copy-on-write optimization: it reads all existing rows, merges the
/// changes, and rebuilds the full tree through the same node-building
/// pipeline used by `write_rows_to_prolly_tree`.
pub fn merge_changes<I, NS, W>(
    changes: std::iter::Peekable<I>,
    table: &Table,
    node_source: &NS,
    writer: &mut W,
    target_node_size: usize,
    node_size_stddev: usize,
) -> Result<Option<Id>>
where
    I: Iterator<Item = Change>,
    NS: NodeSource,
    W: Writer,
{
    let existing = collect_existing_rows(table, node_source)?;
    let merged = merge_sorted_rows_and_changes(existing, changes)?;

    if merged.is_empty() {
        return Ok(None);
    }

    build_and_write_tree(writer, &table.schema, merged, target_node_size, node_size_stddev)
}

// =============================================================================
// Tree building
// =============================================================================

fn build_and_write_tree<W: Writer>(
    writer: &mut W,
    schema: &TableSchema,
    rows: Vec<RowRecord>,
    target_node_size: usize,
    node_size_stddev: usize,
) -> Result<Option<Id>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let leaf_summaries =
        build_and_write_leaf_nodes(writer, schema, rows, target_node_size, node_size_stddev)?;

    build_and_write_branch_levels(writer, schema, leaf_summaries, target_node_size, node_size_stddev)
}

fn build_and_write_leaf_nodes<W: Writer>(
    writer: &mut W,
    schema: &TableSchema,
    rows: Vec<RowRecord>,
    target_node_size: usize,
    node_size_stddev: usize,
) -> Result<Vec<NodeSummary>> {
    let mut summaries = Vec::new();
    let mut shaper = ProbShaper::new(target_node_size, node_size_stddev);
    let mut batch: Vec<RowRecord> = Vec::new();

    for row in rows {
        let row_bytes = serialize_row_for_splitting(&row.values)?;
        batch.push(row);

        if shaper.is_complete(&row_bytes) {
            let built = build_leaf_page(std::mem::take(&mut batch), &shaper)?;
            let summary = write_built_node(writer, schema, built)?;
            summaries.push(summary);
            shaper = ProbShaper::new(target_node_size, node_size_stddev);
        }
    }

    if !batch.is_empty() {
        let built = build_leaf_page(batch, &shaper)?;
        let summary = write_built_node(writer, schema, built)?;
        summaries.push(summary);
    }

    Ok(summaries)
}

fn build_and_write_branch_levels<W: Writer>(
    writer: &mut W,
    schema: &TableSchema,
    mut current_level: Vec<NodeSummary>,
    target_node_size: usize,
    node_size_stddev: usize,
) -> Result<Option<Id>> {
    if current_level.is_empty() {
        return Ok(None);
    }

    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        let mut shaper = ProbShaper::new(target_node_size, node_size_stddev);
        let mut batch: Vec<NodeSummary> = Vec::new();

        for child in current_level {
            let split_bytes = serialize_key_for_splitting(&child.highest_key)?;
            batch.push(child);

            if shaper.is_complete(&split_bytes) {
                let built = build_branch_page(std::mem::take(&mut batch), &shaper)?;
                let summary = write_built_node(writer, schema, built)?;
                next_level.push(summary);
                shaper = ProbShaper::new(target_node_size, node_size_stddev);
            }
        }

        if !batch.is_empty() {
            let built = build_branch_page(batch, &shaper)?;
            let summary = write_built_node(writer, schema, built)?;
            next_level.push(summary);
        }

        current_level = next_level;
    }

    Ok(Some(current_level.remove(0).id))
}

fn build_leaf_page(rows: Vec<RowRecord>, shaper: &ProbShaper) -> Result<BuiltNode> {
    if rows.is_empty() {
        return Err(DomainError::InvalidArgs("leaf node requires at least one row".to_string()).into());
    }

    let id: Id = shaper.id().into();

    let keys: Vec<Key> = rows.iter().map(|r| r.key.clone()).collect();
    let highest_key =
        keys.last().cloned().ok_or_else(|| QueryError::Malformed("leaf node had no highest key"))?;

    let entries: Vec<LeafEntry> = rows
        .into_iter()
        .map(|r| LeafEntry {
            row: (r.row_id, r.values),
        })
        .collect();

    let row_count = entries.len() as i64;

    let node = Node::Leaf(LeafNode { keys, entries });

    Ok(BuiltNode {
        id,
        node,
        highest_key,
        depth: 0,
        row_count,
    })
}

fn build_branch_page(children: Vec<NodeSummary>, shaper: &ProbShaper) -> Result<BuiltNode> {
    if children.is_empty() {
        return Err(DomainError::InvalidArgs("branch node requires at least one child".to_string()).into());
    }

    let id: Id = shaper.id().into();

    let highest_key = children
        .last()
        .map(|c| c.highest_key.clone())
        .ok_or_else(|| QueryError::Malformed("branch node had no highest key"))?;

    let depth = children[0].depth + 1;
    let row_count: i64 = children.iter().map(|c| c.row_count).sum();

    let child_ids: Vec<Id> = children.iter().map(|c| c.id.clone()).collect();
    let keys: Vec<Key> = if children.len() > 1 {
        children[..children.len() - 1].iter().map(|c| c.highest_key.clone()).collect()
    } else {
        vec![]
    };

    let node = Node::Internal(InternalNode {
        keys,
        children: child_ids,
        subtree_height: depth as u32,
        subtree_row_count: row_count as u64,
    });

    Ok(BuiltNode {
        id,
        node,
        highest_key,
        depth,
        row_count,
    })
}

fn write_built_node<W: Writer>(
    writer: &mut W,
    schema: &TableSchema,
    built: BuiltNode,
) -> Result<NodeSummary> {
    let bytes = encode_node(&built.node, built.id.clone(), schema)?;
    writer.write(file_name(&built.id), &bytes)?;
    Ok(built.summary())
}

// =============================================================================
// Merge pipeline
// =============================================================================

fn collect_existing_rows<NS: NodeSource>(table: &Table, node_source: &NS) -> Result<Vec<RowRecord>> {
    let Some(root_id) = &table.root else {
        return Ok(vec![]);
    };

    let mut rows = Vec::new();
    collect_rows_from_node(root_id, &table.schema, table, node_source, &mut rows)?;
    Ok(rows)
}

fn collect_rows_from_node<NS: NodeSource>(
    node_id: &Id,
    schema: &TableSchema,
    table: &Table,
    node_source: &NS,
    out: &mut Vec<RowRecord>,
) -> Result<()>
where
    NS: NodeSource,
{
    let node = node_source.get_node(node_id, schema)?;

    match &*node {
        Node::Leaf(leaf) => {
            for entry in &leaf.entries {
                out.push(RowRecord {
                    key: entry.key(table),
                    row_id: entry.row.0,
                    values: entry.row.1.clone(),
                });
            }
        }
        Node::Internal(branch) => {
            for child_id in &branch.children {
                collect_rows_from_node(child_id, schema, table, node_source, out)?;
            }
        }
    }

    Ok(())
}

fn merge_sorted_rows_and_changes<I>(
    existing: Vec<RowRecord>,
    mut changes: std::iter::Peekable<I>,
) -> Result<Vec<RowRecord>>
where
    I: Iterator<Item = Change>,
{
    let mut out = Vec::with_capacity(existing.len());
    let mut existing_it = existing.into_iter().peekable();

    while let Some(row) = existing_it.peek() {
        match changes.peek() {
            None => {
                out.extend(existing_it);
                return Ok(out);
            }
            Some(change) => match row.key.compare_key(change.key()) {
                Ordering::Less => {
                    out.push(existing_it.next().expect("peeked row exists"));
                }
                Ordering::Equal => {
                    let row = existing_it.next().expect("peeked row exists");
                    let change = changes.next().expect("peeked change exists");
                    match change {
                        Change::Insert { key, .. } => {
                            return Err(DomainError::DuplicateKey { key }.into());
                        }
                        Change::Update { key, row_id, record } => {
                            out.push(RowRecord {
                                key,
                                row_id,
                                values: record,
                            });
                        }
                        Change::Delete { .. } => {
                            let _ = row;
                        }
                    }
                }
                Ordering::Greater => {
                    let change = changes.next().expect("peeked change exists");
                    match change {
                        Change::Insert { key, row_id, record } => {
                            out.push(RowRecord {
                                key,
                                row_id,
                                values: record,
                            });
                        }
                        Change::Update { key, .. } | Change::Delete { key } => {
                            return Err(DomainError::KeyNotFound { key }.into());
                        }
                    }
                }
            },
        }
    }

    while let Some(change) = changes.next() {
        match change {
            Change::Insert { key, row_id, record } => {
                out.push(RowRecord {
                    key,
                    row_id,
                    values: record,
                });
            }
            Change::Update { key, .. } | Change::Delete { key } => {
                return Err(DomainError::KeyNotFound { key }.into());
            }
        }
    }

    reject_duplicate_keys(&out)?;
    Ok(out)
}

fn reject_duplicate_keys(rows: &[RowRecord]) -> Result<()> {
    for pair in rows.windows(2) {
        if pair[0].key.compare_key(&pair[1].key) == Ordering::Equal {
            return Err(DomainError::DuplicateKey {
                key: pair[1].key.clone(),
            }
            .into());
        }
    }
    Ok(())
}

// =============================================================================
// Table / transaction / root objects
// =============================================================================

fn write_table_transaction_and_root<W: Writer>(
    writer: &mut W,
    table_name: String,
    schema: TableSchema,
    root_node_id: Option<Id>,
    prev_id: Option<Id>,
) -> Result<(Id, Id)> {
    let table_id = generate_table_id(root_node_id.as_ref(), &table_name);
    let table = Table::new(table_id.clone(), table_name.clone(), root_node_id, schema)?;
    let table_bytes = encode_table(
        &table,
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64,
    )?;
    writer.write(file_name(&table_id), &table_bytes)?;

    let transaction_time = SystemTime::now();
    let prev_id_for_transaction = prev_id.unwrap_or_default();
    let transaction_id = generate_transaction_id(&prev_id_for_transaction, &transaction_time);

    let mut tables = HashMap::new();
    tables.insert(table_name, table_id.clone());

    let transaction = Transaction {
        id: transaction_id.clone(),
        prev_id: prev_id_for_transaction,
        transaction_time,
        tables,
    };
    let transaction_bytes = encode_transaction(&transaction)?;
    writer.write(file_name(&transaction_id), &transaction_bytes)?;

    let root = Root {
        id: transaction_id.clone(),
    };
    let root_bytes = encode_root(&root)?;
    writer.write("root.bch", &root_bytes)?;

    Ok((transaction_id, table_id))
}

// =============================================================================
// Schema helpers
// =============================================================================

pub fn infer_row_schema_from_record(record: &Value) -> Result<Schema> {
    match record {
        Value::Record(fields) => {
            let schema_fields: Result<Vec<_>> = fields
                .iter()
                .enumerate()
                .map(|(i, (name, value))| {
                    let field_schema = match value {
                        Value::String(_) => Schema::String,
                        Value::Long(_) => Schema::Long,
                        Value::Double(_) => Schema::Double,
                        Value::Float(_) => Schema::Float,
                        Value::Int(_) => Schema::Int,
                        Value::Boolean(_) => Schema::Boolean,
                        Value::Bytes(_) => Schema::Bytes,
                        _ => return Err(SchemaError::unsupported_field(name, value).into()),
                    };

                    Ok(RecordField {
                        name: name.clone(),
                        doc: None,
                        aliases: None,
                        default: None,
                        schema: field_schema,
                        order: apache_avro::schema::RecordFieldOrder::Ascending,
                        position: i,
                        custom_attributes: BTreeMap::new(),
                    })
                })
                .collect();

            let fields = schema_fields?;
            let mut lookup = BTreeMap::new();
            for (i, field) in fields.iter().enumerate() {
                lookup.insert(field.name.clone(), i);
            }

            Ok(Schema::Record(apache_avro::schema::RecordSchema {
                name: Name::new("Record")?,
                aliases: None,
                doc: None,
                fields,
                lookup,
                attributes: BTreeMap::new(),
            }))
        }
        _ => Err(SchemaError::Mismatch("expected record value".to_string()).into()),
    }
}

fn extract_key_schema(row_schema: &Schema, key_column_indices: &[usize]) -> Result<Schema> {
    if let Schema::Record(row_record) = row_schema {
        let key_fields: Result<Vec<_>> = key_column_indices
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                row_record
                    .fields
                    .get(col_idx)
                    .ok_or_else(|| {
                        beech_core::BeechError::from(SchemaError::MissingKeyColumn {
                            name: format!("index {col_idx}"),
                        })
                    })
                    .map(|field| RecordField {
                        name: field.name.clone(),
                        doc: None,
                        aliases: None,
                        default: None,
                        schema: field.schema.clone(),
                        order: apache_avro::schema::RecordFieldOrder::Ascending,
                        position: i,
                        custom_attributes: BTreeMap::new(),
                    })
            })
            .collect();

        let key_fields = key_fields?;
        let mut lookup = BTreeMap::new();
        for (i, field) in key_fields.iter().enumerate() {
            lookup.insert(field.name.clone(), i);
        }

        Ok(Schema::Record(apache_avro::schema::RecordSchema {
            name: Name::new("Key")?,
            aliases: None,
            doc: None,
            fields: key_fields,
            lookup,
            attributes: BTreeMap::new(),
        }))
    } else {
        Err(SchemaError::Mismatch("expected record schema".to_string()).into())
    }
}

fn record_to_fields(record: &Value) -> Result<Vec<Value>> {
    match record {
        Value::Record(fields) => Ok(fields.iter().map(|(_, value)| value.clone()).collect()),
        _ => Err(SchemaError::Mismatch("expected record value".to_string()).into()),
    }
}

fn key_from_row_values(key_columns: &[usize], row: &[Value]) -> Key {
    key_columns.iter().map(|&i| row[i].clone()).collect()
}

// =============================================================================
// Stable split serialization helpers
// =============================================================================

fn serialize_row_for_splitting(row_values: &[Value]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for value in row_values {
        encode_value_for_split(value, &mut bytes)?;
    }
    Ok(bytes)
}

fn serialize_key_for_splitting(key: &Key) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for value in key {
        encode_value_for_split(value, &mut bytes)?;
    }
    Ok(bytes)
}

fn encode_value_for_split(value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => out.push(0),
        Value::Boolean(b) => {
            out.push(1);
            out.push(if *b { 1 } else { 0 });
        }
        Value::Int(n) => {
            out.push(2);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Long(n) => {
            out.push(3);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float(f) => {
            out.push(4);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Double(d) => {
            out.push(5);
            out.extend_from_slice(&d.to_le_bytes());
        }
        Value::String(s) => {
            out.push(6);
            let len = s.len() as u64;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.push(7);
            let len = b.len() as u64;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(b);
        }
        other => {
            return Err(SchemaError::Mismatch(format!(
                "unsupported value in split serialization: {other:?}"
            ))
            .into());
        }
    }
    Ok(())
}

// =============================================================================
// IDs / filenames
// =============================================================================

fn file_name(id: &Id) -> String {
    format!("{id}.bch")
}

fn generate_table_id(root_node_id: Option<&Id>, table_name: &str) -> Id {
    let mut hasher = Sha256::new();

    if let Some(root_id) = root_node_id {
        hasher.update(root_id.as_bytes());
    }
    hasher.update(table_name.as_bytes());

    let hash_result = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash_result[..]);
    Id::from(id_bytes)
}

fn generate_transaction_id(prev_id: &Id, transaction_time: &SystemTime) -> Id {
    let mut hasher = Sha256::new();
    hasher.update(prev_id.as_bytes());

    let duration = transaction_time.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    hasher.update(duration.as_secs().to_le_bytes());
    hasher.update(duration.subsec_nanos().to_le_bytes());

    let hash_result = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash_result[..]);
    Id::from(id_bytes)
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
