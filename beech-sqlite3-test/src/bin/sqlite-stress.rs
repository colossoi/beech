//! Time-bounded differential stress test for the SQLite adapter.
use anyhow::{Context, Result, bail, ensure};
use beech_core::{DataType, Field, Table, TableSchema};
use beech_write::{FileWriter, Writer, publish_table};
use clap::Parser;
use rusqlite::{Connection, types::Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(about = "Exercise Beech SQLite against native SQLite under time and disk budgets")]
struct Args {
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    duration_secs: u64,
    /// Monitored file-size budget (not a filesystem quota); stops at 90%.
    #[arg(long, default_value_t = 512, value_parser = clap::value_parser!(u64).range(1..=1048576))]
    max_disk_mib: u64,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(2..=64))]
    tables: u32,
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..=1000000))]
    initial_rows: u32,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Parent directory for a fresh, isolated run directory.
    #[arg(long)]
    directory: Option<PathBuf>,
    /// Preserve the run directory, including on failure.
    #[arg(long)]
    keep: bool,
    #[arg(long, hide = true)]
    worker: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(root) = &args.worker {
        return worker(&args, root);
    }
    supervise(&args)
}

// Ensure monitor errors cannot leave a writer running after its workspace is removed.
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn disk_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let meta = match fs::symlink_metadata(entry.path()) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        total = total.saturating_add(if meta.is_dir() { disk_bytes(&entry.path())? } else { meta.len() });
    }
    Ok(total)
}

fn supervise(args: &Args) -> Result<()> {
    let workspace = match &args.directory {
        Some(parent) => beech_disk::Workspace::in_directory(parent)?,
        None => beech_disk::Workspace::new()?,
    };
    let root = if args.keep { workspace.keep()? } else { workspace.path().to_owned() };
    run_supervised(args, &root)
}

fn run_supervised(args: &Args, root: &Path) -> Result<()> {
    let root = root.canonicalize()?;
    let scratch = root.join("scratch");
    fs::create_dir(&scratch)?;
    println!(
        "run={} seed={} tables={} duration={}s disk_budget={} MiB",
        root.display(),
        args.seed,
        args.tables,
        args.duration_secs,
        args.max_disk_mib
    );
    let child = Command::new(std::env::current_exe()?)
        .args([
            "--worker",
            root.to_str().context("run path is not UTF-8")?,
            "--duration-secs",
            &args.duration_secs.to_string(),
            "--tables",
            &args.tables.to_string(),
            "--initial-rows",
            &args.initial_rows.to_string(),
            "--seed",
            &args.seed.to_string(),
        ])
        .env("TMPDIR", &scratch)
        .env("TMP", &scratch)
        .env("TEMP", &scratch)
        .spawn()?;
    let mut child = Worker(child);
    let start = Instant::now();
    let mut peak = 0;
    let budget = args.max_disk_mib * 1024 * 1024;
    // Reserve headroom for writes between samples. This is not an OS quota.
    let stop_at = budget - budget / 10;
    loop {
        let bytes = disk_bytes(&root)?;
        peak = peak.max(bytes);
        if let Some(status) = child.0.try_wait()? {
            println!(
                "finished elapsed={:.2}s peak_observed_bytes={peak}",
                start.elapsed().as_secs_f64()
            );
            ensure!(
                status.success(),
                "stress worker failed ({status}); seed={}, run={}",
                args.seed,
                root.display()
            );
            return Ok(());
        }
        let reason = if bytes >= stop_at {
            Some("disk budget (90% stop threshold)")
        } else if start.elapsed() >= Duration::from_secs(args.duration_secs) {
            Some("duration")
        } else {
            None
        };
        if let Some(reason) = reason {
            child.0.kill()?;
            child.0.wait()?;
            peak = peak.max(disk_bytes(&root)?);
            println!(
                "stopped: {reason}; elapsed={:.2}s peak_observed_bytes={peak}",
                start.elapsed().as_secs_f64()
            );
            ensure!(
                peak <= budget,
                "disk budget exceeded between samples: {peak} > {budget} bytes; use a filesystem quota for a strict ceiling"
            );
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

fn open(root: &Path) -> Result<Connection> {
    let conn = Connection::open(root.join("stress.sqlite"))?;
    beech_sqlite3::create_beech_module(&conn)?;
    conn.execute_batch("PRAGMA temp_store=FILE; PRAGMA cache_size=-2048;")?;
    Ok(conn)
}

fn query(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>> {
    let mut statement = conn.prepare(sql)?;
    let columns = statement.column_count();
    Ok(statement
        .query_map([], |row| (0..columns).map(|i| row.get(i)).collect())?
        .collect::<rusqlite::Result<_>>()?)
}

fn compare(conn: &Connection, sql: &str) -> Result<()> {
    let actual = query(conn, &sql.replace("TABLE", "b"))?;
    let expected = query(conn, &sql.replace("TABLE", "r"))?;
    ensure!(
        actual == expected,
        "query mismatch: {sql}; actual rows={}, expected rows={}",
        actual.len(),
        expected.len()
    );
    Ok(())
}

fn execute(conn: &Connection, sql: &str) -> Result<()> {
    let actual = conn.execute(&sql.replace("TABLE", "b"), [])?;
    let expected = conn.execute(&sql.replace("TABLE", "r"), [])?;
    ensure!(
        actual == expected,
        "affected-row mismatch: {sql}: {actual} != {expected}"
    );
    Ok(())
}

fn verify(conn: &Connection, table: u32) -> Result<()> {
    compare(
        conn,
        &format!("SELECT rowid,k,v,payload FROM TABLE{table} ORDER BY rowid"),
    )
}

fn setup(args: &Args, root: &Path, conn: &Connection) -> Result<()> {
    let dir = root.join("repo");
    fs::create_dir(&dir)?;
    for table in 0..args.tables {
        let schema = TableSchema::new(
            vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, true),
            ],
            vec![0],
        )?;
        let name = format!("data{table}");
        let empty = Table::new(&name, schema, None, 0)?;
        let mut writer = FileWriter::new(&dir)?;
        let (tables, previous) = if table == 0 {
            (Default::default(), None)
        } else {
            let repository =
                beech_core::storage::Repository::new(beech_core::storage::FileStore::new(&dir));
            let root_id = beech_core::Id::from_hex(fs::read_to_string(dir.join("root"))?.trim())?;
            let repository = std::sync::Arc::new(repository);
            let snapshot = repository.snapshot(root_id)?;
            (
                snapshot.transaction().tables().clone(),
                Some(repository.get_root(&root_id)?.transaction_id()),
            )
        };
        publish_table(&mut writer, &empty, tables, previous)?;
        writer.commit()?;
        conn.execute_batch(&format!("CREATE VIRTUAL TABLE b{table} USING beech('{}','{name}'); CREATE TABLE r{table}(k INTEGER NOT NULL UNIQUE,v INTEGER NOT NULL,payload TEXT);", dir.display().to_string().replace('\'', "''")))?;
        conn.execute_batch("BEGIN")?;
        for id in 1..=args.initial_rows {
            execute(
                conn,
                &format!("INSERT INTO TABLE{table}(rowid,k,v,payload) VALUES({id},{id},{id},'initial')"),
            )?;
        }
        conn.execute_batch("COMMIT")?;
        verify(conn, table)?;
        println!("initialized table={table} rows={}", args.initial_rows);
    }
    Ok(())
}

fn worker(args: &Args, root: &Path) -> Result<()> {
    let mut conn = open(root)?;
    setup(args, root, &conn)?;
    let mut rng = Rng(args.seed);
    let mut next_id = i64::from(args.initial_rows) + 1;
    let mut counts = [0u64; 10];
    let mut step = 0u64;
    loop {
        let table = (rng.next() % u64::from(args.tables)) as u32;
        let operation = (rng.next() % counts.len() as u64) as usize;
        // Flush the exact step before execution so a failed/killed run is reproducible.
        println!("step={step} table={table} operation={operation} next_id={next_id}");
        let result = (|| -> Result<()> {
            match operation {
                0 | 1 => {
                    conn.execute_batch("BEGIN")?;
                    let count = 1 + rng.next() % 16;
                    for _ in 0..count {
                        let id = next_id;
                        next_id += 1;
                        let payload = if id % 3 == 0 {
                            "NULL".to_owned()
                        } else {
                            format!(
                                "'seed-{}-{}-雪{}'",
                                args.seed,
                                id,
                                "x".repeat((rng.next() % 1024) as usize)
                            )
                        };
                        execute(
                            &conn,
                            &format!(
                                "INSERT INTO TABLE{table}(rowid,k,v,payload) VALUES({id},{id},{id},{payload})"
                            ),
                        )?;
                    }
                    verify(&conn, table)?;
                    conn.execute_batch(if operation == 0 { "COMMIT" } else { "ROLLBACK" })?;
                }
                2 => execute(
                    &conn,
                    &format!("UPDATE TABLE{table} SET v=v+1 WHERE k%7={}", rng.next() % 7),
                )?,
                3 => execute(
                    &conn,
                    &format!("DELETE FROM TABLE{table} WHERE k%11={}", rng.next() % 11),
                )?,
                4 => {
                    let id = next_id;
                    next_id += 1;
                    execute(
                        &conn,
                        &format!(
                            "UPDATE TABLE{table} SET k={id},rowid=-{id},payload='changed' WHERE k=(SELECT min(k) FROM TABLE{table})"
                        ),
                    )?;
                }
                5 => {
                    conn.execute_batch("BEGIN")?;
                    execute(
                        &conn,
                        &format!("UPDATE TABLE{table} SET v=v+100,payload=NULL WHERE k%3=0"),
                    )?;
                    execute(&conn, &format!("DELETE FROM TABLE{table} WHERE k%5=0"))?;
                    verify(&conn, table)?;
                    conn.execute_batch("ROLLBACK")?;
                }
                6 => {
                    compare(
                        &conn,
                        &format!(
                            "SELECT rowid,k,v,payload FROM TABLE{table} WHERE k>={} AND v%2=0 ORDER BY k DESC LIMIT 50",
                            rng.next() % next_id as u64
                        ),
                    )?;
                    compare(
                        &conn,
                        &format!("SELECT count(*),sum(v),min(k),max(k) FROM TABLE{table}"),
                    )?;
                }
                7 => {
                    conn = open(root)?;
                    for t in 0..args.tables {
                        verify(&conn, t)?;
                    }
                }
                8 => {
                    let count: i64 =
                        conn.query_row(&format!("SELECT count(*) FROM r{table}"), [], |r| r.get(0))?;
                    if count > 0 {
                        let sql = format!(
                            "INSERT INTO TABLE{table}(rowid,k,v,payload) SELECT rowid,k,v,payload FROM TABLE{table} ORDER BY k LIMIT 1"
                        );
                        ensure!(
                            conn.execute(&sql.replace("TABLE", "b"), []).is_err(),
                            "Beech accepted duplicate rowid/key"
                        );
                        ensure!(
                            conn.execute(&sql.replace("TABLE", "r"), []).is_err(),
                            "SQLite accepted duplicate rowid/key"
                        );
                    }
                }
                9 => {
                    let other = (table + 1) % args.tables;
                    compare(
                        &conn,
                        &format!(
                            "SELECT a.k,a.v,b.v FROM TABLE{table} a JOIN TABLE{other} b ON a.k=b.k ORDER BY a.k"
                        ),
                    )?;
                }
                _ => bail!("invalid operation"),
            }
            verify(&conn, table)?;
            Ok(())
        })();
        result.with_context(|| {
            format!(
                "seed={} step={step} table={table} operation={operation}",
                args.seed
            )
        })?;
        counts[operation] += 1;
        step += 1;
        if step.is_multiple_of(25) {
            println!("verified_steps={step} operation_counts={counts:?}");
        }
    }
}
