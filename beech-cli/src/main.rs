use anyhow::{bail, Context};
use beech_core::{
    query::RowCursor,
    storage::{FileStore, Repository, Snapshot},
    Id, NodeRef, NodeSource,
};
use beech_write::{publish_table, BuildOptions, Change, FileWriter, SortLimits, Transaction, Writer};
use clap::Parser;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
mod csv_input;
mod key_columns;

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

        /// Output directory for content-addressed objects
        #[arg(short, long, default_value = "/tmp")]
        output_dir: PathBuf,

        /// Table name
        #[arg(short, long, default_value = "table")]
        table_name: String,

        /// Key columns (comma-separated names or indices)
        #[arg(short, long, default_value = "0")]
        key_columns: KeyColumns,

        /// Target canonical logical bytes per node (before Parquet compression)
        #[arg(long, default_value = "1000")]
        target_node_size: usize,

        /// Standard deviation for probabilistic node splitting
        #[arg(long, default_value = "100")]
        node_size_stddev: usize,

        /// Whether CSV has headers
        #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
        has_headers: bool,

        /// Load mode: replace or insert
        #[arg(long, value_enum, default_value = "replace")]
        mode: LoadMode,
    },
    /// Show information about a prolly tree
    Info {
        /// Directory containing objects and a root pointer
        #[arg(short, long, default_value = "/tmp")]
        data_dir: PathBuf,
    },
    /// Inspect a specific node file and show its metadata in JSON format
    Inspect {
        /// Directory containing content-addressed objects
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Node ID (64 hex characters) or full path to an object
        node_id_or_path: String,
    },
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    match Args::parse().command {
        Commands::LoadCsv {
            csv_file,
            output_dir,
            table_name,
            key_columns,
            target_node_size,
            node_size_stddev,
            has_headers,
            mode,
        } => load_csv(
            csv_file,
            output_dir,
            table_name,
            key_columns,
            BuildOptions::new(target_node_size, node_size_stddev)?,
            has_headers,
            mode,
        ),
        Commands::Info { data_dir } => {
            println!("{}", serde_json::to_string_pretty(&info(&data_dir)?)?);
            Ok(())
        }
        Commands::Inspect {
            data_dir,
            node_id_or_path,
        } => {
            let path = Path::new(&node_id_or_path);
            let (dir, id) = if path.components().count() > 1 {
                (
                    path.parent().context("object has no parent directory")?.to_path_buf(),
                    Id::from_hex(
                        path.file_name().and_then(|s| s.to_str()).context("invalid object filename")?,
                    )?,
                )
            } else {
                (
                    data_dir.unwrap_or_else(|| PathBuf::from("/tmp")),
                    Id::from_hex(&node_id_or_path)?,
                )
            };
            println!("{}", serde_json::to_string_pretty(&inspect(&dir, id)?)?);
            Ok(())
        }
    }
}
fn open_snapshot(directory: &Path) -> anyhow::Result<Option<Snapshot>> {
    let text = match std::fs::read_to_string(directory.join("root")) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let repository = Arc::new(Repository::new(FileStore::new(directory)));
    Ok(Some(repository.snapshot(Id::from_hex(text.trim())?)?))
}
fn current_snapshot(directory: &Path) -> anyhow::Result<Snapshot> {
    open_snapshot(directory)?.context("no published snapshot (root file missing)")
}
fn load_csv(
    csv_file: PathBuf,
    directory: PathBuf,
    table_name: String,
    keys: KeyColumns,
    options: BuildOptions,
    has_headers: bool,
    mode: LoadMode,
) -> anyhow::Result<()> {
    let input = csv_input::CsvInput::read(&csv_file, has_headers)?;
    // Lock before reading root so a second writer cannot publish between read and commit.
    let mut writer = FileWriter::new(&directory)?;
    let current = open_snapshot(&directory)?;
    let mut tables = Default::default();
    let mut previous_id = None;
    if let Some(snapshot) = &current {
        tables = snapshot.transaction().tables().clone();
        previous_id = Some(beech_core::codec::thrift::encode_transaction(snapshot.transaction())?.id());
    }
    let table = match mode {
        LoadMode::Replace => {
            let fields = input.infer_fields();
            let schema = beech_core::TableSchema::new(fields.clone(), keys.to_indices(&fields)?)?;
            let mut transaction = Transaction::new(schema.clone(), SortLimits::default())?;
            for row in input.rows(&schema, 0)? {
                let row = row?;
                transaction.push(Change::Insert {
                    key: schema.key_from_row(&row)?,
                    row_id: row.0,
                    record: row.1,
                })?;
            }
            transaction.build(table_name, &mut writer, options)?
        }
        LoadMode::Insert => {
            let snapshot = current.as_ref().context("insert requires an existing snapshot")?;
            let table = snapshot.table(&table_name)?;
            let fields: Vec<_> = table.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
            if keys.to_indices(&fields)? != table.schema().key_columns() {
                bail!("insert key columns must match the existing table");
            }
            let first_id = table.max_row_id().checked_add(1).context("row ID overflow")?;
            let mut transaction = Transaction::new(table.schema().clone(), SortLimits::default())?;
            for row in input.rows(table.schema(), first_id)? {
                let row = row?;
                transaction.push(Change::Insert {
                    key: table.schema().key_from_row(&row)?,
                    row_id: row.0,
                    record: row.1,
                })?;
            }
            transaction.apply(&table, snapshot, &mut writer, options)?
        }
    };
    let publication = publish_table(&mut writer, &table, tables, previous_id)?;
    writer.commit()?;
    log::info!(
        "Published root {} for table {}",
        publication.root_id,
        table.name()
    );
    Ok(())
}
#[derive(Serialize)]
struct TableInfo {
    name: String,
    id: String,
    columns: usize,
    key_columns: usize,
    root_node: Option<String>,
    tree_depth: u32,
    total_rows: u64,
    max_row_id: i64,
}
#[derive(Serialize)]
struct TransactionInfo {
    transaction_date: String,
    transaction_id: String,
    tables: Vec<TableInfo>,
}
fn info(directory: &Path) -> anyhow::Result<TransactionInfo> {
    let snapshot = current_snapshot(directory)?;
    let transaction = snapshot.transaction();
    let mut tables = vec![];
    for (name, id) in transaction.tables() {
        let table = snapshot.table(name)?;
        tables.push(TableInfo {
            name: name.clone(),
            id: id.to_string(),
            columns: table.schema().fields().len(),
            key_columns: table.schema().key_columns().len(),
            root_node: table.root().map(|r| r.id().to_string()),
            tree_depth: table.root().map_or(0, |r| r.height()),
            total_rows: table.root().map_or(0, |r| r.row_count()),
            max_row_id: table.max_row_id(),
        });
    }
    let duration = transaction.time().duration_since(std::time::UNIX_EPOCH)?;
    let date = jiff::Timestamp::from_nanosecond(i128::try_from(duration.as_nanos())?)?.to_string();
    Ok(TransactionInfo {
        transaction_date: date,
        transaction_id: beech_core::codec::thrift::encode_transaction(transaction)?.id().to_string(),
        tables,
    })
}
#[derive(Serialize)]
struct NodeInspection {
    node_id: String,
    node_type: &'static str,
    num_keys: usize,
    num_rows: u64,
    children: Option<Vec<String>>,
    keys: Vec<String>,
}
fn inspect(directory: &Path, id: Id) -> anyhow::Result<NodeInspection> {
    let snapshot = current_snapshot(directory)?;
    // Resolve the reference and its schema from every table, not just the first.
    for name in snapshot.transaction().tables().keys() {
        let table = snapshot.table(name)?;
        let mut pending: Vec<NodeRef> = table.root().cloned().into_iter().collect();
        while let Some(reference) = pending.pop() {
            if reference.id() == id {
                let (node_type, children, keys) = if reference.height() == 0 {
                    let leaf_table = table.with_root(Some(reference.clone()))?;
                    let keys = RowCursor::new(&snapshot, &leaf_table, vec![])?
                        .map(|row| Ok(format!("{:?}", table.schema().key_from_row(&row?)?)))
                        .collect::<beech_core::Result<Vec<_>>>()?;
                    ("leaf", None, keys)
                } else {
                    let node = snapshot.get_internal(&reference, table.schema())?;
                    (
                        "branch",
                        Some(node.children().iter().map(|r| r.id().to_string()).collect()),
                        node.children().iter().map(|r| format!("{:?}", r.max_key())).collect(),
                    )
                };
                return Ok(NodeInspection {
                    node_id: id.to_string(),
                    node_type,
                    num_keys: keys.len(),
                    num_rows: reference.row_count(),
                    children,
                    keys,
                });
            }
            if reference.height() > 0 {
                pending
                    .extend(snapshot.get_internal(&reference, table.schema())?.children().iter().cloned());
            }
        }
    }
    bail!("node {id} is not reachable from the current snapshot")
}
