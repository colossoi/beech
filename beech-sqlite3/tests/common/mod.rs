#![allow(dead_code)]

use apache_avro::types::Value;
use beech_write::{Writer, write_rows_to_prolly_tree};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// File-based Writer that puts nodes into a flat directory.
pub struct FileWriter {
    dir: PathBuf,
    pending: Vec<(PathBuf, Vec<u8>)>,
}

impl FileWriter {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            pending: Vec::new(),
        }
    }
}

impl Writer for FileWriter {
    fn write<P: AsRef<Path>>(&mut self, name: P, data: &[u8]) -> std::io::Result<()> {
        self.pending.push((self.dir.join(name.as_ref()), data.to_vec()));
        Ok(())
    }

    fn commit(self) -> std::io::Result<()> {
        for (path, data) in self.pending {
            std::fs::write(path, data)?;
        }
        Ok(())
    }

    fn abort(self) -> std::io::Result<()> {
        Ok(())
    }

    fn num_to_commit(&self) -> usize {
        self.pending.len()
    }
}

/// Build a prolly tree on disk in a temp directory. Returns the TempDir
/// (must be kept alive) and the table name used.
pub fn make_test_tree(rows: Vec<(i64, Value)>, key_columns: Vec<usize>, table_name: &str) -> TempDir {
    let tmp = TempDir::new().unwrap();
    let mut writer = FileWriter::new(tmp.path());
    let (transaction_id, _table_id) = write_rows_to_prolly_tree(
        &mut writer,
        table_name.to_string(),
        key_columns,
        rows,
        64,
        16,
        None,
    )
    .unwrap();
    writer.commit().unwrap();
    // The vtab reads a plain-text "root" file containing the transaction ID
    // as a hex string. beech-write writes "root.bch" (Avro-encoded), so we
    // write the text file separately, matching what beech-cli does.
    let root_file = tmp.path().join("root");
    std::fs::write(&root_file, transaction_id.to_string()).unwrap();
    tmp
}

pub fn int_record(k: i32, v: i32) -> Value {
    Value::Record(vec![
        ("k".to_string(), Value::Int(k)),
        ("v".to_string(), Value::Int(v)),
    ])
}

pub fn int_row(i: i64, v: i32) -> (i64, Value) {
    (i, int_record(i as i32, v))
}

pub fn two_part_record(a: i32, b: i32, v: i32) -> Value {
    Value::Record(vec![
        ("a".to_string(), Value::Int(a)),
        ("b".to_string(), Value::Int(b)),
        ("v".to_string(), Value::Int(v)),
    ])
}

/// Set up a Connection with the beech module loaded and a virtual table
/// pointing at the given directory.
pub fn setup_vtab(dir: &Path, table_name: &str, local_name: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    let sql = format!(
        "CREATE VIRTUAL TABLE {} USING beech('{}', 'unused', '{}')",
        local_name,
        dir.display(),
        table_name,
    );
    conn.execute_batch(&sql).unwrap();
    conn
}
