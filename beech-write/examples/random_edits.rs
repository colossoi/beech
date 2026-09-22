//! Build 5,000 rows, then commit 1,000 deterministic random mutations together.
//! cargo run -p beech-write --release --example random_edits
use beech_core::{
    query::RowCursor,
    storage::{FileStore, Repository},
    DataType, Field, Row, Scalar, TableSchema,
};
use beech_disk::Workspace;
use beech_write::{publish_table, BuildOptions, Change, FileWriter, SortLimits, Transaction, Writer};
use std::{collections::BTreeMap, fs, sync::Arc, time::Instant};

fn record(key: i64, value: String) -> Vec<Scalar> {
    vec![Scalar::Int64(key), Scalar::Utf8(value)]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let cache_bytes = match args.next().as_deref() {
        None => 8 * 1024 * 1024,
        Some("--cache-bytes") => args.next().ok_or("missing cache byte limit")?.parse()?,
        _ => return Err("usage: random_edits [--cache-bytes BYTES]".into()),
    };
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let directory = Workspace::new()?;
    let repository = Arc::new(Repository::new(FileStore::new(directory.path())));
    let schema = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ],
        vec![0],
    )?;
    let options = BuildOptions::default();
    let mut model = BTreeMap::<i64, Row>::new();
    let mut writer = FileWriter::new(directory.path())?;
    let mut creation = Transaction::new(schema.clone(), SortLimits::default())?;
    for key in 0..5_000 {
        let row = record(key, format!("value {key}"));
        creation.push(Change::Insert {
            key: vec![Scalar::Int64(key)],
            row_id: key,
            record: row.clone(),
        })?;
        model.insert(key, (key, row));
    }
    let table = creation.build("demo".into(), &mut writer, options)?;
    let initial = publish_table(&mut writer, &table, BTreeMap::new(), None)?;
    writer.commit()?;

    let mut writer = FileWriter::new(directory.path())?;
    let old_snapshot = repository.snapshot(initial.root_id)?;
    let table = old_snapshot.table("demo")?;
    let initial_rows =
        RowCursor::new(&old_snapshot, &table, vec![])?.collect::<beech_core::Result<Vec<_>>>()?;
    let root_before = fs::read(directory.path().join("root"))?;
    let mut tx = Transaction::new(schema, SortLimits::default())?.with_page_cache_bytes(cache_bytes);
    // Fixed-seed xorshift64 makes the workload reproducible without a dependency.
    let mut seed = 0x4bee_c123_9876_abcd_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let (mut inserts, mut updates, mut deletes) = (0, 0, 0);
    let mut next_row_id = 5_000;
    let started = Instant::now();
    for step in 0..1_000 {
        let key = (random() % 6_000) as i64;
        let change = if let Some((row_id, _)) = model.get(&key) {
            if random() % 4 == 0 {
                model.remove(&key);
                deletes += 1;
                Change::Delete {
                    key: vec![Scalar::Int64(key)],
                }
            } else {
                let row_id = *row_id;
                let row = record(key, format!("edited at {step}"));
                model.insert(key, (row_id, row.clone()));
                updates += 1;
                Change::Update {
                    key: vec![Scalar::Int64(key)],
                    row_id,
                    record: row,
                }
            }
        } else {
            let row_id = next_row_id;
            next_row_id += 1;
            let row = record(key, format!("inserted at {step}"));
            model.insert(key, (row_id, row.clone()));
            inserts += 1;
            Change::Insert {
                key: vec![Scalar::Int64(key)],
                row_id,
                record: row,
            }
        };
        tx.push(change)?;
    }
    let (updated, stats) = tx.apply_with_stats(&table, repository.as_ref(), &mut writer, options)?;
    let publication = publish_table(
        &mut writer,
        &updated,
        BTreeMap::new(),
        Some(initial.transaction_id),
    )?;
    assert_eq!(fs::read(directory.path().join("root"))?, root_before);
    let commit_started = Instant::now();
    writer.commit()?;
    let commit_elapsed = commit_started.elapsed();
    let total_elapsed = started.elapsed();

    let snapshot = repository.snapshot(publication.root_id)?;
    let table = snapshot.table("demo")?;
    let actual = RowCursor::new(&snapshot, &table, vec![])?.collect::<beech_core::Result<Vec<_>>>()?;
    assert_eq!(actual, model.into_values().collect::<Vec<_>>());
    assert_eq!(
        RowCursor::new(&old_snapshot, old_snapshot.table("demo")?.as_ref(), vec![])?
            .collect::<beech_core::Result<Vec<_>>>()?,
        initial_rows
    );
    println!("5,000 initial rows; 1,000 random edits; one update commit (seed 0x4beec1239876abcd)");
    println!(
        "{inserts} inserts, {updates} updates, {deletes} deletes; {} final rows",
        actual.len()
    );
    println!(
        "Apply/finalize: {:.3}s; commit: {:.3}s; total update: {:.3}s",
        stats.elapsed.as_secs_f64(),
        commit_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64()
    );
    println!(
        "Visits: {} leaves, {} branches; page rewrites: {} leaves, {} branches",
        stats.leaf_visits, stats.branch_visits, stats.leaf_writes, stats.branch_writes
    );
    println!(
        "Final staged nodes: {} leaves, {} branches ({} bytes)",
        stats.leaves_staged, stats.branches_staged, stats.staged_bytes
    );
    println!(
        "Peak scratch: {} bytes; scratch written: {} bytes; final height: {:?}",
        stats.peak_scratch_bytes, stats.scratch_bytes_written, stats.final_height
    );
    println!(
        "Page cache: limit {cache_bytes} bytes, peak {} bytes, {} hits, {} misses, {} evictions",
        stats.peak_page_cache_bytes, stats.page_cache_hits, stats.page_cache_misses, stats.page_evictions
    );
    println!("Verified all rows, unchanged publication before commit, and old snapshot after commit.");
    Ok(())
}
