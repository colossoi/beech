mod common;
use beech_core::{DataType, Decimal, Field, Row, Scalar, TableSchema};
use common::*;
use rusqlite::types::Value;

fn ids(conn: &rusqlite::Connection, sql: &str, value: &Value) -> Vec<i64> {
    conn.prepare(sql)
        .unwrap()
        .query_map([value], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn comparisons_and_rebinding_match_sqlite_tables() {
    let tmp = make_test_tree(
        (0..1300).map(|i| int_row(i, (i % 7) as i32)).collect(),
        vec![0],
        "t",
    );
    let conn = setup_vtab(tmp.path(), "t", "tt");
    conn.execute_batch(
        "CREATE TABLE reference(k INTEGER, v INTEGER);
        INSERT INTO reference(rowid, k, v) SELECT rowid, k, v FROM tt;",
    )
    .unwrap();
    let values = [
        Value::Null,
        Value::Integer(17),
        Value::Integer(i64::MAX),
        Value::Real(17.0),
        Value::Real(17.5),
        Value::Text("17".into()),
        Value::Text("17.5".into()),
        Value::Text("nonnumeric".into()),
        Value::Blob(vec![17]),
    ];
    for op in ["=", "<", "<=", ">", ">="] {
        let sql = format!("SELECT rowid FROM tt WHERE k {op} ? ORDER BY rowid");
        let mut statement = conn.prepare(&sql).unwrap();
        for value in &values {
            let actual = statement
                .query_map([value], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let expected = ids(&conn, &sql.replace("FROM tt", "FROM reference"), value);
            assert_eq!(actual, expected, "{op} {value:?}");
        }
    }
    for condition in [
        "k >= 63 AND k >= 65 AND k < 130",
        "k = 17 AND k = 18",
        "k IN (1, 64, 1024, 1299)",
        "k BETWEEN 1000 AND 1040 AND v = 3",
    ] {
        let sql = format!("SELECT rowid FROM tt WHERE {condition} AND ? IS NULL ORDER BY rowid");
        assert_eq!(
            ids(&conn, &sql, &Value::Null),
            ids(&conn, &sql.replace("FROM tt", "FROM reference"), &Value::Null),
            "{condition}"
        );
    }
    // Force the virtual table to be both the outer and inner side of a join.
    for query in [
        "SELECT a.rowid FROM tt a CROSS JOIN reference b ON a.k=b.k WHERE b.k IN (63,64,1024) ORDER BY a.rowid",
        "SELECT a.rowid FROM reference b CROSS JOIN tt a ON a.k=b.k WHERE b.k IN (63,64,1024) ORDER BY a.rowid",
        "SELECT a.rowid FROM tt a JOIN tt b ON a.k=b.k WHERE b.k IN (63,64,1024) ORDER BY a.rowid",
    ] {
        let rows: Vec<i64> = conn
            .prepare(query)
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows, vec![63, 64, 1024]);
    }
}

#[test]
fn text_collations_and_nulls_match_sqlite_tables() {
    let schema = TableSchema::new(
        vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ],
        vec![0],
    )
    .unwrap();
    let rows = [None, Some("A"), Some("B"), Some("a"), Some("a\0x"), Some("b")]
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            (
                i as i64,
                vec![
                    key.map(|s| Scalar::Utf8(s.into())).unwrap_or(Scalar::Null),
                    if i == 0 { Scalar::Null } else { Scalar::Binary(vec![i as u8, 0, 255]) },
                ],
            )
        })
        .collect::<Vec<Row>>();
    let tmp = make_tree(&schema, &rows, "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");
    conn.execute_batch(
        "CREATE TABLE reference(k TEXT, payload BLOB);
        INSERT INTO reference(rowid,k,payload) SELECT rowid,k,payload FROM tt;",
    )
    .unwrap();
    for condition in [
        "k = ?",
        "k >= ?",
        "k = ? COLLATE NOCASE",
        "k IS NULL AND ? IS NULL",
        "k IS NOT NULL AND ? IS NOT NULL",
        "payload = ?",
    ] {
        for value in [Value::Null, Value::Text("a".into()), Value::Blob(vec![3, 0, 255])] {
            let sql = format!("SELECT rowid FROM tt WHERE {condition} ORDER BY rowid");
            assert_eq!(
                ids(&conn, &sql, &value),
                ids(&conn, &sql.replace("FROM tt", "FROM reference"), &value),
                "{condition} {value:?}"
            );
        }
    }
    for order in ["k", "k COLLATE NOCASE", "k DESC"] {
        let sql = format!("SELECT rowid FROM tt WHERE ? IS NULL ORDER BY {order}, rowid");
        assert_eq!(
            ids(&conn, &sql, &Value::Null),
            ids(&conn, &sql.replace("FROM tt", "FROM reference"), &Value::Null)
        );
    }
}

#[test]
fn all_scalar_types_and_quoted_names() {
    let schema = TableSchema::new(
        vec![
            Field::new("select", DataType::Int64, false),
            Field::new("a\"b", DataType::Boolean, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("decimal", DataType::Decimal128(38, 2), false),
            Field::new("text", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
        ],
        vec![0],
    )
    .unwrap();
    let row = (
        -42,
        vec![
            Scalar::Int64(i64::MIN),
            Scalar::Boolean(true),
            Scalar::Float32(1.25),
            Scalar::Float64(-2.5),
            Scalar::UInt64(u64::MAX),
            Scalar::Decimal(Decimal::new(-12345678901234567890123456789012345678, 2).unwrap()),
            Scalar::Utf8("a\0b".into()),
            Scalar::Binary(vec![0, 128, 255]),
        ],
    );
    let tmp = make_tree(&schema, &[row], "ta'ble");
    let conn = setup_vtab(tmp.path(), "ta'ble", "local\"table");
    let values: Vec<Value> = conn
        .query_row("SELECT rowid,* FROM \"local\"\"table\"", [], |r| {
            (0..9).map(|i| r.get(i)).collect()
        })
        .unwrap();
    assert_eq!(
        values,
        vec![
            Value::Integer(-42),
            Value::Integer(i64::MIN),
            Value::Integer(1),
            Value::Real(1.25),
            Value::Real(-2.5),
            Value::Text(u64::MAX.to_string()),
            Value::Text("-123456789012345678901234567890123456.78".into()),
            Value::Text("a\0b".into()),
            Value::Blob(vec![0, 128, 255]),
        ]
    );
}

#[test]
fn projection_after_column_63_and_empty_tables() {
    let schema = TableSchema::new(
        (0..70).map(|i| Field::new(format!("c{i}"), DataType::Int32, false)).collect(),
        vec![0],
    )
    .unwrap();
    let tmp = make_tree(&schema, &[(321, (0..70).map(Scalar::Int32).collect())], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");
    let result: (i64, i32, i32) = conn
        .query_row("SELECT rowid,c69,c63 FROM tt", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(result, (321, 69, 63));
    let empty = make_tree(&schema, &[], "t");
    let conn = setup_vtab(empty.path(), "t", "tt");
    let count: i64 = conn.query_row("SELECT count(*) FROM tt WHERE c0=1", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn connected_table_keeps_immutable_snapshot() {
    let schema = TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap();
    let tmp = make_tree(&schema, &[(1, vec![Scalar::Int64(1)])], "t");
    let old = setup_vtab(tmp.path(), "t", "tt");
    write_tree(tmp.path(), &schema, &[(2, vec![Scalar::Int64(2)])], "t");
    let new = setup_vtab(tmp.path(), "t", "tt");
    assert_eq!(
        old.query_row("SELECT k FROM tt", [], |r| r.get::<_, i64>(0)).unwrap(),
        1
    );
    assert_eq!(
        new.query_row("SELECT k FROM tt", [], |r| r.get::<_, i64>(0)).unwrap(),
        2
    );
}
