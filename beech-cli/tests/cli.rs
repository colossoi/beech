use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_beech-cli")).args(args).output().unwrap()
}
fn success(args: &[&str]) -> Output {
    let output = cli(args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn load(dir: &Path, table: &str, csv: &str, mode: &str) -> Output {
    let input = dir.join("input.csv");
    fs::write(&input, csv).unwrap();
    cli(&[
        "load-csv",
        input.to_str().unwrap(),
        "-o",
        dir.to_str().unwrap(),
        "-t",
        table,
        "--mode",
        mode,
        "--target-node-size",
        "64",
        "--node-size-stddev",
        "16",
    ])
}
fn load_ok(dir: &Path, table: &str, csv: &str, mode: &str) {
    let out = load(dir, table, csv, mode);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}
fn info(dir: &Path) -> Value {
    serde_json::from_slice(&success(&["info", "-d", dir.to_str().unwrap()]).stdout).unwrap()
}
fn sqlite(dir: &Path, table: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE data USING beech('{}', 'unused', '{}')",
        dir.display().to_string().replace('\'', "''"),
        table
    ))
    .unwrap();
    conn
}
#[test]
fn csv_replace_insert_info_inspect_and_sqlite_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    load_ok(path, "items", "id,name\n3,three\n1,one\n", "replace");
    let original_root = fs::read(path.join("root")).unwrap();
    load_ok(path, "items", "id,name\n2,two\n4,four\n", "insert");
    assert_ne!(fs::read(path.join("root")).unwrap(), original_root);
    let metadata = info(path);
    assert_eq!(metadata["tables"][0]["total_rows"], 4);
    let root = metadata["tables"][0]["root_node"].as_str().unwrap();
    let inspection: Value =
        serde_json::from_slice(&success(&["inspect", "-d", path.to_str().unwrap(), root]).stdout).unwrap();
    assert_eq!(inspection["num_rows"], 4);
    let by_path: Value =
        serde_json::from_slice(&success(&["inspect", path.join(root).to_str().unwrap()]).stdout).unwrap();
    assert_eq!(inspection, by_path);
    let conn = sqlite(path, "items");
    let mut stmt = conn.prepare("SELECT rowid, id, name FROM data ORDER BY id").unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, 1, "one".into()),
            (2, 2, "two".into()),
            (0, 3, "three".into()),
            (3, 4, "four".into())
        ]
    );
}
#[test]
fn failed_insert_leaves_root_and_rows_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    load_ok(path, "items", "id,name\n1,one\n", "replace");
    let root = fs::read(path.join("root")).unwrap();
    for csv in [
        "id,name\n1,duplicate\n",
        "id,name\n2,a\n2,b\n",
        "id,name\nwrong,type\n",
        "id,other\n2,two\n",
    ] {
        let result = load(path, "items", csv, "insert");
        assert!(!result.status.success());
        assert_eq!(fs::read(path.join("root")).unwrap(), root);
        assert_eq!(
            sqlite(path, "items")
                .query_row("SELECT count(*) FROM data", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}
#[test]
fn replacing_one_table_preserves_others_and_inspects_their_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    load_ok(path, "a", "id,value\n1,alpha\n", "replace");
    load_ok(path, "z", "name,active\nz,true\n", "replace");
    load_ok(path, "a", "id,value\n2,beta\n", "replace");
    let metadata = info(path);
    assert_eq!(metadata["tables"].as_array().unwrap().len(), 2);
    let root = metadata["tables"][1]["root_node"].as_str().unwrap();
    let inspection: Value =
        serde_json::from_slice(&success(&["inspect", "-d", path.to_str().unwrap(), root]).stdout).unwrap();
    assert_eq!(inspection["node_type"], "leaf");
    assert!(inspection["keys"][0].as_str().unwrap().contains('z'));
    assert_eq!(
        sqlite(path, "z").query_row("SELECT name FROM data", [], |r| r.get::<_, String>(0)).unwrap(),
        "z"
    );
}
#[test]
fn csv_inference_uses_whole_column_and_supports_headerless_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    load_ok(path, "mixed", "id,value\n1,123\n2,text\n", "replace");
    assert_eq!(
        sqlite(path, "mixed")
            .query_row("SELECT value FROM data WHERE id = 1", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "123"
    );
    let input = path.join("headerless.csv");
    fs::write(&input, "x,3\ny,4\n").unwrap();
    success(&[
        "load-csv",
        input.to_str().unwrap(),
        "-o",
        path.to_str().unwrap(),
        "-t",
        "plain",
        "--has-headers",
        "false",
        "--key-columns",
        "col_0",
    ]);
    assert_eq!(
        sqlite(path, "plain").query_row("SELECT count(*) FROM data", [], |r| r.get::<_, i64>(0)).unwrap(),
        2
    );
}
#[test]
fn invalid_build_options_fail_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    fs::write(&input, "id\n1\n").unwrap();
    let out = cli(&[
        "load-csv",
        input.to_str().unwrap(),
        "-o",
        dir.path().to_str().unwrap(),
        "--target-node-size",
        "0",
    ]);
    assert!(!out.status.success());
    assert!(!dir.path().join("root").exists());
}

#[test]
fn csv_large_integers_do_not_lose_precision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    load_ok(path, "unsigned", "id,value\n1,18446744073709551615\n", "replace");
    assert_eq!(
        sqlite(path, "unsigned")
            .query_row("SELECT value FROM data", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "18446744073709551615"
    );
    load_ok(path, "mixed", "id,value\n1,9007199254740993\n2,1.5\n", "replace");
    assert_eq!(
        sqlite(path, "mixed")
            .query_row("SELECT value FROM data WHERE id = 1", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "9007199254740993"
    );
    load_ok(
        path,
        "huge",
        "id,value\n1,999999999999999999999999999999999999999999999\n",
        "replace",
    );
    assert_eq!(
        sqlite(path, "huge").query_row("SELECT value FROM data", [], |r| r.get::<_, String>(0)).unwrap(),
        "999999999999999999999999999999999999999999999"
    );
}
