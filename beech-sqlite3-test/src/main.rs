//! Register the SQLite adapter and optionally query a Beech snapshot directory.
//! Run with: cargo run -p beech-sqlite3-test -- <directory> [table_name]

use beech_sqlite3::create_beech_module;
use rusqlite::{Connection, Result, types::Value};

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    create_beech_module(&conn)?;
    let mut args = std::env::args().skip(1);
    let Some(directory) = args.next() else {
        println!("Beech SQLite module registered.");
        println!("Pass <directory> [table_name] to query a Parquet snapshot.");
        return Ok(());
    };
    let table = args.next().unwrap_or_else(|| "table".into());
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE data USING beech('{}', 'unused', '{}')",
        directory.replace('\'', "''"),
        table.replace('\'', "''"),
    ))?;
    let count: i64 = conn.query_row("SELECT count(*) FROM data", [], |row| row.get(0))?;
    println!("{table}: {count} rows");
    let mut statement = conn.prepare("SELECT rowid, * FROM data LIMIT 5")?;
    let columns = statement.column_count();
    for row in statement.query_map([], |row| {
        (0..columns).map(|i| row.get::<_, Value>(i)).collect::<Result<Vec<_>>>()
    })? {
        println!("{:?}", row?);
    }
    Ok(())
}
