use apache_avro::{types::Value, Schema};
use beech_core::{Id, Node, NodeSource};
use beech_write::{infer_row_schema_from_record, write_rows_to_prolly_tree, Writer};
use clap::Parser;
#[cfg(test)]
use log::debug;
use log::info;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Serialize)]
struct NodeInspection {
    node_id: String,
    node_type: String,
    num_keys: usize,
    num_rows: Option<usize>,
    children: Option<Vec<String>>,
    keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TableInfo {
    name: String,
    id: String,
    columns: usize,
    key_columns: usize,
    root_node: Option<String>,
    tree_depth: i32,
    total_rows: i64,
}

#[derive(Debug, Serialize)]
struct TransactionInfo {
    transaction_date: String,
    transaction_id: String,
    tables: Vec<TableInfo>,
}

mod key_columns;
mod memory;
mod txio;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum LoadMode {
    /// Replace any existing table with the same name
    Replace,
    /// Insert rows into an existing table (schema must match)
    Insert,
}

use key_columns::KeyColumns;

#[derive(Parser)]
#[command(name = "beech-cli")]
#[command(about = "Beech prolly tree tools")]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
enum Commands {
    /// Load CSV data into prolly tree format
    LoadCsv {
        /// CSV file to read
        csv_file: PathBuf,

        /// Output directory for .bch files
        #[arg(short, long, default_value = "/tmp")]
        output_dir: PathBuf,

        /// Table name
        #[arg(short, long, default_value = "table")]
        table_name: String,

        /// Key columns (comma-separated indices like "0,1")
        #[arg(short, long, default_value = "0")]
        key_columns: KeyColumns,

        /// Target node size in bytes
        #[arg(long, default_value = "1000")]
        target_node_size: usize,

        /// Standard deviation for probabilistic node splitting
        #[arg(long, default_value = "100")]
        node_size_stddev: usize,

        /// Whether CSV has headers
        #[arg(long, default_value = "true")]
        has_headers: bool,

        /// Load mode: replace or insert
        #[arg(long, value_enum, default_value = "replace")]
        mode: LoadMode,
    },
    /// Show information about a prolly tree
    Info {
        /// Directory containing .bch files and root file
        #[arg(short, long, default_value = "/tmp")]
        data_dir: PathBuf,
    },
    /// Inspect a specific node file and show its metadata in JSON format
    Inspect {
        /// Directory containing .bch files
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Node ID to inspect (32-byte hex string) or full path to .bch file
        node_id_or_path: String,
    },
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    match args.command {
        Commands::LoadCsv {
            csv_file,
            output_dir,
            table_name,
            key_columns,
            target_node_size,
            node_size_stddev,
            has_headers,
            mode,
        } => load_csv_command(
            csv_file,
            output_dir,
            table_name,
            key_columns,
            target_node_size,
            node_size_stddev,
            has_headers,
            mode,
        ),
        Commands::Info { data_dir } => info_command(data_dir),
        Commands::Inspect {
            data_dir,
            node_id_or_path,
        } => inspect_command(data_dir, node_id_or_path),
    }
}

fn load_csv_command(
    csv_file: PathBuf,
    output_dir: PathBuf,
    table_name: String,
    key_columns: KeyColumns,
    target_node_size: usize,
    node_size_stddev: usize,
    has_headers: bool,
    mode: LoadMode,
) -> anyhow::Result<()> {
    info!("Reading CSV file: {}", csv_file.display());

    // Read and parse CSV data
    let records = read_csv_to_records(&csv_file, has_headers)?;
    info!("Loaded {} records", records.len());

    // Get the first record to infer schema for key columns
    let row_schema = if let Some((_, first_record)) = records.first() {
        infer_row_schema_from_record(first_record)?
    } else {
        return Err(anyhow::anyhow!("No records found in CSV"));
    };

    // Convert key columns to indices
    let key_column_indices = key_columns.to_indices(&row_schema)?;

    match mode {
        LoadMode::Replace => {
            replace_mode_load(
                output_dir,
                table_name,
                key_column_indices,
                records,
                target_node_size,
                node_size_stddev,
            )?;
        }
        LoadMode::Insert => {
            unimplemented!("Insert mode is not yet implemented");
        }
    }

    Ok(())
}

fn replace_mode_load(
    output_dir: PathBuf,
    table_name: String,
    key_column_indices: Vec<usize>,
    records: Vec<(i64, Value)>,
    target_node_size: usize,
    node_size_stddev: usize,
) -> anyhow::Result<()> {
    // Create writer
    let mut writer = txio::Writer::new(output_dir.clone());

    // Write prolly tree
    let (transaction_id, table_id) = beech_write::write_rows_to_prolly_tree(
        &mut writer,
        table_name,
        key_column_indices,
        records.clone(),
        target_node_size,
        node_size_stddev,
        None, // For the first transaction, no previous transaction
    )?;

    info!("Successfully loaded {} records into new table", records.len());
    info!("Transaction ID: {transaction_id}");
    info!("Table ID: {table_id}");

    let num_files = writer.num_to_commit();
    writer.commit()?;

    // Write the root file with the current transaction ID
    let root_file_path = output_dir.join("root");
    std::fs::write(&root_file_path, transaction_id.to_string())?;

    info!(
        "Successfully wrote {} files to {}",
        num_files + 1, // +1 for the root file
        output_dir.display()
    );
    info!("Root ID: {transaction_id}");
    info!("Table ID: {table_id}");

    Ok(())
}

fn info_command(data_dir: PathBuf) -> anyhow::Result<()> {
    use beech_core::{source::LocalFile, NodeSource};

    // Read the root file to get the current transaction ID
    let root_file_path = data_dir.join("root");
    let transaction_id_str = std::fs::read_to_string(&root_file_path)
        .map_err(|e| anyhow::anyhow!("Failed to read root file at {}: {}", root_file_path.display(), e))?;

    let transaction_id = Id::from_hex(transaction_id_str.trim())?;

    // Create a NodeSource to read the data
    let backing_store = txio::FileBackingStore::new(data_dir);
    let node_source = LocalFile::new(backing_store);

    // Get the transaction
    let transaction = node_source.get_transaction(&transaction_id)?;

    // Collect table information
    let mut tables = Vec::new();
    for (table_name, table_id) in &transaction.tables {
        let table = node_source.get_table(&transaction, table_name)?;

        let (root_node, tree_depth, total_rows) = if let Some(root_id) = &table.root {
            let root = node_source.get_node(root_id, &table.schema)?;
            (Some(root_id.to_string()), root.depth(), root.row_count())
        } else {
            (None, 0, 0)
        };

        tables.push(TableInfo {
            name: table_name.clone(),
            id: table_id.to_string(),
            columns: table.columns().len(),
            key_columns: table.key_columns.len(),
            root_node,
            tree_depth,
            total_rows,
        });
    }

    // Format the transaction date using jiff
    let transaction_date = {
        let duration_since_epoch = transaction
            .transaction_time
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("Invalid system time: {}", e))?;

        let total_nanos = duration_since_epoch.as_secs() as i128 * 1_000_000_000
            + duration_since_epoch.subsec_nanos() as i128;

        let timestamp = jiff::Timestamp::from_nanosecond(total_nanos)?;

        timestamp.to_string()
    };

    // Create the transaction info structure
    let transaction_info = TransactionInfo {
        transaction_date,
        transaction_id: transaction_id.to_string(),
        tables,
    };

    // Output as JSON
    let json = serde_json::to_string_pretty(&transaction_info)?;
    println!("{}", json);

    Ok(())
}

fn inspect_command(data_dir: Option<PathBuf>, node_id_or_path: String) -> anyhow::Result<()> {
    use beech_core::{source::LocalFile, NodeSource};
    use std::path::Path;

    // Determine if input is a file path or just a node ID
    let (data_dir, node_id) = if node_id_or_path.contains('/') && node_id_or_path.ends_with(".bch") {
        // Input is a file path - extract directory and node ID
        let path = Path::new(&node_id_or_path);
        let directory = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Cannot determine directory from path: {}", node_id_or_path))?
            .to_path_buf();

        let file_stem = path
            .file_stem()
            .ok_or_else(|| anyhow::anyhow!("Cannot extract filename from path: {}", node_id_or_path))?
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid filename encoding: {}", node_id_or_path))?;

        (directory, file_stem.to_string())
    } else {
        // Input is just a node ID - use provided or default directory
        let dir = data_dir.unwrap_or_else(|| PathBuf::from("/tmp"));
        (dir, node_id_or_path)
    };

    // Parse the node ID from hex string
    let node_id = Id::from_hex(&node_id)?;

    // Read the root file to get the current transaction ID
    let root_file_path = data_dir.join("root");
    let transaction_id_str = std::fs::read_to_string(&root_file_path)
        .map_err(|e| anyhow::anyhow!("Failed to read root file at {}: {}", root_file_path.display(), e))?;

    let transaction_id = Id::from_hex(transaction_id_str.trim())?;

    // Create a NodeSource to read the data
    let backing_store = txio::FileBackingStore::new(data_dir);
    let node_source = LocalFile::new(backing_store);

    // Get the transaction
    let transaction = node_source.get_transaction(&transaction_id)?;

    // We need to find a table to get schemas - use the first available table
    let (table_name, _) = transaction
        .tables
        .iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No tables found in transaction"))?;

    let table = node_source.get_table(&transaction, table_name)?;

    // Get the node
    let node = node_source.get_node(&node_id, &table.schema)?;

    // Create the inspection structure
    let inspection = match &*node {
        Node::Leaf(leaf) => NodeInspection {
            node_id: node_id.to_string(),
            node_type: "leaf".to_string(),
            num_keys: leaf.keys.len(),
            num_rows: Some(leaf.len()),
            children: None,
            keys: leaf.keys.iter().map(|k| format!("{:?}", k)).collect(),
        },
        Node::Internal(internal) => NodeInspection {
            node_id: node_id.to_string(),
            node_type: "branch".to_string(),
            num_keys: internal.keys.len(),
            num_rows: None,
            children: Some(internal.children.iter().map(|id| id.to_string()).collect()),
            keys: internal.keys.iter().map(|k| format!("{:?}", k)).collect(),
        },
    };

    // Output as JSON
    let json = serde_json::to_string_pretty(&inspection)?;
    println!("{}", json);

    Ok(())
}

fn read_csv_to_records(path: &PathBuf, has_headers: bool) -> anyhow::Result<Vec<(i64, Value)>> {
    let mut reader = csv::ReaderBuilder::new().has_headers(has_headers).from_path(path)?;

    let headers = if has_headers { Some(reader.headers()?.clone()) } else { None };

    let mut records = Vec::new();
    for (row_id, record) in reader.records().enumerate() {
        let record = record?;

        let field_names = if let Some(ref headers) = headers {
            headers.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        } else {
            (0..record.len()).map(|i| format!("col_{i}")).collect()
        };

        let field_values: anyhow::Result<Vec<_>> = record
            .iter()
            .map(|field| {
                // Try to parse as different types
                if let Ok(int_val) = field.parse::<i64>() {
                    Ok(Value::Long(int_val))
                } else if let Ok(float_val) = field.parse::<f64>() {
                    Ok(Value::Double(float_val))
                } else if let Ok(bool_val) = field.parse::<bool>() {
                    Ok(Value::Boolean(bool_val))
                } else {
                    Ok(Value::String(field.to_string()))
                }
            })
            .collect();

        let field_values = field_values?;
        let avro_record = Value::Record(field_names.into_iter().zip(field_values).collect());

        records.push((row_id as i64, avro_record));
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use beech_core::NodeSource;

    // Helper functions for creating field values
    fn str_field(name: &str, value: &str) -> (String, Value) {
        (name.to_string(), Value::String(value.to_string()))
    }

    fn int_field(name: &str, value: i64) -> (String, Value) {
        (name.to_string(), Value::Long(value))
    }

    fn float_field(name: &str, value: f64) -> (String, Value) {
        (name.to_string(), Value::Double(value))
    }

    fn bool_field(name: &str, value: bool) -> (String, Value) {
        (name.to_string(), Value::Boolean(value))
    }

    fn long_field(name: &str, value: i64) -> (String, Value) {
        (name.to_string(), Value::Long(value))
    }

    fn string_field(name: &str, value: &str) -> (String, Value) {
        (name.to_string(), Value::String(value.to_string()))
    }

    fn record(fields: Vec<(String, Value)>) -> Value {
        Value::Record(fields)
    }

    #[test]
    fn test_write_and_read_small_prolly_tree() {
        // Create test data - small set of records
        let test_rows = vec![
            (
                0,
                record(vec![
                    str_field("name", "Alice"),
                    int_field("age", 25),
                    float_field("score", 95.5),
                ]),
            ),
            (
                1,
                record(vec![
                    str_field("name", "Bob"),
                    int_field("age", 30),
                    float_field("score", 87.2),
                ]),
            ),
            (
                2,
                record(vec![
                    str_field("name", "Charlie"),
                    int_field("age", 22),
                    float_field("score", 92.1),
                ]),
            ),
        ];

        // Write to memory
        let mut writer = memory::Writer::new();
        let (transaction_id, _table_id) = write_rows_to_prolly_tree(
            &mut writer,
            "test_table".to_string(),
            vec![0], // Key on "name" column
            test_rows.clone(),
            1000,
            100,
            None, // For test, no previous transaction
        )
        .expect("Failed to write prolly tree");

        // Create NodeSource for reading
        let node_source = memory::NodeSource::new(writer.files);

        // Get the table
        let transaction = node_source.get_transaction(&transaction_id).unwrap();
        let table = node_source.get_table(&transaction, "test_table").unwrap();

        // Test by directly accessing the leaf node (simpler approach)
        let mut retrieved_rows = Vec::new();
        if let Some(root_id) = &table.root {
            let node = node_source.get_node(root_id, &table.schema).unwrap();
            if let Node::Leaf(leaf) = node.as_ref() {
                for entry in &leaf.entries {
                    let row_values = entry.values();
                    let record = Value::Record(vec![
                        ("name".to_string(), row_values[0].clone()),
                        ("age".to_string(), row_values[1].clone()),
                        ("score".to_string(), row_values[2].clone()),
                    ]);
                    retrieved_rows.push((entry.row_id(), record));
                }
            }
        }

        // Verify we got all the data back
        assert_eq!(retrieved_rows.len(), 3);

        // Check that data matches (may be in different order due to key sorting)
        for (original_row_id, original_record) in &test_rows {
            let found = retrieved_rows.iter().any(|(retrieved_row_id, retrieved_record)| {
                retrieved_row_id == original_row_id && retrieved_record == original_record
            });
            assert!(
                found,
                "Original row {:?} not found in retrieved rows",
                (original_row_id, original_record)
            );
        }
    }

    #[test]
    fn test_memory_writer() {
        let mut writer = memory::Writer::new();

        // Write some test data
        writer.write("test.txt", b"Hello, World!").unwrap();
        writer.write("data.bin", &[1, 2, 3, 4, 5]).unwrap();

        // Verify data can be retrieved
        assert_eq!(writer.get_file("test.txt"), Some(b"Hello, World!".as_slice()));
        assert_eq!(writer.get_file("data.bin"), Some([1, 2, 3, 4, 5].as_slice()));
        assert_eq!(writer.get_file("nonexistent.txt"), None);
    }

    #[test]
    fn test_schema_inference() {
        let record = record(vec![
            str_field("name", "Test"),
            int_field("count", 42),
            bool_field("active", true),
        ]);

        let schema = infer_row_schema_from_record(&record).unwrap();

        if let Schema::Record(record_schema) = schema {
            assert_eq!(record_schema.fields.len(), 3);
            assert_eq!(record_schema.fields[0].name, "name");
            assert_eq!(record_schema.fields[1].name, "count");
            assert_eq!(record_schema.fields[2].name, "active");

            // Check lookup table is populated
            assert_eq!(record_schema.lookup.get("name"), Some(&0));
            assert_eq!(record_schema.lookup.get("count"), Some(&1));
            assert_eq!(record_schema.lookup.get("active"), Some(&2));
        } else {
            panic!("Expected record schema");
        }
    }

    #[test]
    fn test_large_prolly_tree() {
        use beech_core::query::{Constraint, ConstraintOp, RowCursor};

        // Create test data with a single numeric field - targeting 4 levels
        let num_rows = 15_000;
        let mut test_rows = Vec::new();

        for i in 0..num_rows {
            test_rows.push((i, record(vec![int_field("value", i)])));
        }

        // Write to memory with moderate node size to target 4 levels
        let mut writer = memory::Writer::new();
        let (transaction_id, _table_id) = write_rows_to_prolly_tree(
            &mut writer,
            "large_table".to_string(),
            vec![0], // Key on "value" column
            test_rows.clone(),
            300, // Smaller target node size to get 4 levels
            60,  // Smaller stddev
            None,
        )
        .expect("Failed to write large prolly tree");

        // Create NodeSource for reading
        let node_source = memory::NodeSource::new(writer.files);
        let transaction = node_source.get_transaction(&transaction_id).unwrap();
        let table = node_source.get_table(&transaction, "large_table").unwrap();

        // Test full scan with unbounded cursor
        let mut cursor = RowCursor::new(&table);
        cursor.init(vec![], vec![]); // No constraints = unbounded

        // Navigate to the first leaf node
        cursor.advance_to_left(&node_source).unwrap();

        // Test tree depth - expecting 3 levels with improved splitting algorithm
        let tree_depth = cursor.depth();
        debug!("Tree depth: {tree_depth} levels");
        assert_eq!(tree_depth, 3, "Expected 3-level tree, got depth {tree_depth}");

        // Test root node metadata - cursor depth 3 means root node depth 2 (since leaves are depth 0)
        if let Some(root_node_id) = &table.root {
            let root_node = node_source.get_node(root_node_id, &table.schema).unwrap();
            let expected_root_depth = (tree_depth - 1) as i32; // Root depth = cursor depth - 1
            assert_eq!(
                root_node.depth(),
                expected_root_depth,
                "Root node should have depth {}, got {}",
                expected_root_depth,
                root_node.depth()
            );
            assert_eq!(
                root_node.row_count(),
                num_rows,
                "Root node should have row_count {}, got {}",
                num_rows,
                root_node.row_count()
            );
            debug!(
                "Root node metadata: depth={}, row_count={}",
                root_node.depth(),
                root_node.row_count()
            );
        } else {
            panic!("Expected table to have a root node");
        }

        let mut count = 0;
        while !cursor.eof() {
            let (node_id, row_index) = cursor.current().unwrap();
            let node = node_source.get_node(node_id, &table.schema).unwrap();
            let Node::Leaf(leaf) = node.as_ref() else {
                panic!("Expected leaf node, got branch node");
            };
            let entry = leaf.entry(*row_index).unwrap();
            let row_id = entry.row_id();
            let row_values = entry.values();
            // Verify data integrity: row_id should match the value
            let Value::Long(value) = &row_values[0] else {
                panic!("Expected Long value");
            };
            assert_eq!(row_id, *value);
            // Since rows are now stored in key order, they should be sequential
            assert_eq!(row_id, count);
            count += 1;
            if count % 10000 == 0 {
                debug!("Processed {count} rows");
            }
            cursor.next(&node_source).unwrap();
        }

        // Verify we got all rows
        assert_eq!(count, num_rows);

        // Test bounded cursor with lower_bound
        let test_values = [1000, 5000, 9000];
        for &test_value in &test_values {
            let mut bounded_cursor = RowCursor::new(&table);
            // Create constraint for value >= test_value
            let constraint = Constraint::new(0, ConstraintOp::Ge);
            bounded_cursor.init(vec![constraint], vec![Value::Long(test_value)]);
            bounded_cursor.advance_to_left(&node_source).unwrap();

            // Verify the first row matches our constraint
            assert!(!bounded_cursor.eof(), "Bounded cursor should not be at EOF");
            let (node_id, row_index) = bounded_cursor.current().unwrap();
            let node = node_source.get_node(node_id, &table.schema).unwrap();
            let Node::Leaf(leaf) = node.as_ref() else {
                panic!("Expected leaf node, got branch node");
            };
            let entry = leaf.entry(*row_index).unwrap();
            let row_id = entry.row_id();
            let row_values = entry.values();
            assert!(row_id >= test_value);
            assert_eq!(row_values[0], Value::Long(row_id));
        }
    }
}
