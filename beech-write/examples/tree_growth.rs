//! Insert one row per committed transaction and record the real tree after each.
//! cargo run -p beech-write --example tree_growth -- --keep-workspace 48 target/tree-growth-demo.json
use beech_core::{
    query::RowCursor,
    storage::{FileStore, Repository},
    DataType, Field, NodeRef, NodeSource, Row, Scalar, Table, TableSchema,
};
use beech_disk::Workspace;
use beech_write::{
    publish_table, BuildOptions, Change, FileWriter, ObjectSink, SortLimits, Transaction, Writer,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

fn inspect(
    source: &impl NodeSource,
    table: &Table,
    node: &NodeRef,
    leaves: &mut usize,
    branches: &mut usize,
) -> beech_core::Result<Value> {
    let children = if node.height() == 0 {
        *leaves += 1;
        Vec::new()
    } else {
        *branches += 1;
        source
            .get_internal(node, table.schema())?
            .children()
            .iter()
            .map(|child| inspect(source, table, child, leaves, branches))
            .collect::<beech_core::Result<Vec<_>>>()?
    };
    let keys = if node.height() == 0 {
        let subtree = table.with_root(Some(node.clone()))?;
        RowCursor::new(source, &subtree, vec![])?
            .map(|row| row.map(|r| r.0))
            .collect::<beech_core::Result<Vec<_>>>()?
    } else {
        vec![]
    };
    Ok(
        json!({"id":node.id().to_string(), "height":node.height(), "rows":node.row_count(), "keys":keys, "children":children}),
    )
}
fn export_final(
    source: &Repository,
    table: &Table,
    from: &Path,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Require a new destination so no unrelated data or stale history is mixed in.
    if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(destination)?;
    let mut writer = FileWriter::new(destination)?;
    fn copy_node(
        source: &Repository,
        table: &Table,
        node: &NodeRef,
        from: &Path,
        writer: &mut FileWriter,
    ) -> beech_core::Result<()> {
        if node.height() > 0 {
            for child in source.get_internal(node, table.schema())?.children() {
                copy_node(source, table, child, from, writer)?;
            }
        }
        writer.put(node.id(), &fs::read(from.join(node.id().to_string()))?)?;
        Ok(())
    }
    if let Some(root) = table.root() {
        copy_node(source, table, root, from, &mut writer)?;
    }
    // Preserve tree IDs, but create a history-free snapshot with no dangling predecessor.
    let publication = publish_table(&mut writer, table, BTreeMap::new(), None)?;
    writer.commit()?;
    let exported = Arc::new(Repository::new(FileStore::new(destination)));
    let snapshot = exported.snapshot(publication.root_id)?;
    let exported_table = snapshot.table(table.name())?;
    assert_eq!(&*exported_table, table);
    let expected = RowCursor::new(source, table, vec![])?;
    let mut actual = RowCursor::new(&snapshot, &exported_table, vec![])?;
    for row in expected {
        assert_eq!(row?, actual.next().transpose()?.ok_or("missing exported row")?);
    }
    assert!(actual.next().is_none());
    println!(
        "Exported final tree to {} (new snapshot, no transaction history)",
        destination.display()
    );
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut keep_workspace = false;
    let mut export_directory = None;
    let mut args = Vec::new();
    let mut arguments = std::env::args().skip(1);
    while let Some(arg) = arguments.next() {
        match arg.as_str() {
            "--export-final" => {
                export_directory = Some(PathBuf::from(
                    arguments.next().ok_or("--export-final requires a destination directory")?,
                ))
            }
            "--keep-workspace" => keep_workspace = true,
            "--help" | "-h" => {
                println!("Usage: tree_growth [--keep-workspace] [--export-final DIRECTORY] [COUNT] [OUTPUT.json]");
                println!("Keep the database directory, including on failure, and print its path.");
                println!(
                    "--export-final copies only the final tree into a new directory, without history."
                );
                return Ok(());
            }
            flag if flag.starts_with('-') => return Err(format!("unknown option: {flag}").into()),
            _ => args.push(arg),
        }
    }
    if args.len() > 2 {
        return Err("expected at most COUNT and OUTPUT.json".into());
    }
    let count: i64 = args.first().map(|s| s.parse()).transpose()?.unwrap_or(48);
    if !(1..=256).contains(&count) {
        return Err("choose 1–256 insertions for this demo".into());
    }
    let output = args.get(1).map(PathBuf::from).unwrap_or_else(|| "target/tree-growth-demo.json".into());
    if let Some(path) = &export_directory {
        if path.try_exists()? {
            return Err("export destination must not exist".into());
        }
    }
    let directory = Workspace::new()?;
    let directory_path = if keep_workspace { directory.keep()? } else { directory.path().to_path_buf() };
    println!(
        "{} workspace: {}",
        if keep_workspace { "Keeping" } else { "Temporary" },
        directory_path.display()
    );
    let repository = Arc::new(Repository::new(FileStore::new(&directory_path)));
    let schema = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ],
        vec![0],
    )?;
    let mut table = Table::new("demo", schema.clone(), None, -1)?;
    let options = BuildOptions::new(128, 32)?;
    let mut previous = None;
    let mut frames = vec![json!({"step":0,"tree":null,"leaves":0,"branches":0,"height":null})];
    println!(
        "Each line is one committed insertion. Visits include repeats; bytes are logical file lengths."
    );
    println!("rows height leaves branches  visits L/B  writes L/B  scratch peak  staged bytes");
    for key in 0..count {
        let mut writer = FileWriter::new(&directory_path)?;
        // Read the starting snapshot under the write lock.
        if let Some(root) = previous {
            table = (*repository.snapshot(root)?.table("demo")?).clone();
        }
        let mut tx = Transaction::new(schema.clone(), SortLimits::default())?;
        tx.push(Change::Insert {
            key: vec![Scalar::Int64(key)],
            row_id: key,
            record: vec![Scalar::Int64(key), Scalar::Utf8(format!("value {key}"))],
        })?;
        let (updated, stats) = tx.apply_with_stats(&table, repository.as_ref(), &mut writer, options)?;
        let prev_tx =
            previous.map(|root| repository.get_root(&root).map(|r| r.transaction_id())).transpose()?;
        let publication = publish_table(&mut writer, &updated, BTreeMap::new(), prev_tx)?;
        writer.commit()?;
        previous = Some(publication.root_id);
        let snapshot = repository.snapshot(publication.root_id)?;
        table = (*snapshot.table("demo")?).clone();
        let (mut leaves, mut branches) = (0, 0);
        let tree = inspect(
            &snapshot,
            &table,
            table.root().unwrap(),
            &mut leaves,
            &mut branches,
        )?;
        let actual =
            RowCursor::new(&snapshot, &table, vec![])?.collect::<beech_core::Result<Vec<Row>>>()?;
        assert_eq!(actual.len(), (key + 1) as usize);
        assert!(actual.iter().enumerate().all(|(i, row)| row.0 == i as i64));
        let repository_bytes = fs::read_dir(&directory_path)?
            .try_fold(0, |n, entry| -> std::io::Result<u64> {
                Ok(n + entry?.metadata()?.len())
            })?;
        println!(
            "{:4} {:6} {:6} {:8} {:5}/{:<3} {:5}/{:<3} {:13} {:13}",
            key + 1,
            stats.final_height.unwrap(),
            leaves,
            branches,
            stats.leaf_visits,
            stats.branch_visits,
            stats.leaf_writes,
            stats.branch_writes,
            stats.peak_scratch_bytes,
            stats.staged_bytes
        );
        frames.push(json!({"step":key+1,"key":key,"tree":tree,"leaves":leaves,"branches":branches,"height":stats.final_height,
            "leafVisits":stats.leaf_visits,"branchVisits":stats.branch_visits,"leafWrites":stats.leaf_writes,"branchWrites":stats.branch_writes,
            "leafSplits":stats.leaf_splits,"branchSplits":stats.branch_splits,"leavesStaged":stats.leaves_staged,"branchesStaged":stats.branches_staged,
            "inputBytes":stats.input_bytes,"scratchPeak":stats.peak_scratch_bytes,"scratchWritten":stats.scratch_bytes_written,"stagedBytes":stats.staged_bytes,
            "applyMicros":stats.elapsed.as_micros(),"repositoryBytes":repository_bytes}));
    }
    if let Some(parent) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string(&frames)?;
    fs::write(&output, &json)?;
    fs::write(
        output.with_extension("html"),
        include_str!("tree_growth.html").replace("__FRAMES__", &json),
    )?;
    println!("Verified every snapshot; replay data: {}", output.display());
    if let Some(destination) = export_directory {
        export_final(repository.as_ref(), &table, &directory_path, &destination)?;
    }
    Ok(())
}
