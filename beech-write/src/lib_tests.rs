#![allow(unused_imports)]
use super::*;
use apache_avro::types::Value;
use beech_core::{BeechError, DomainError, SchemaError};
use std::path::Path;

// Minimal in-crate Writer for tests that just need to exercise error paths.
struct NoopWriter {
    num: usize,
}
impl NoopWriter {
    fn new() -> Self {
        Self { num: 0 }
    }
}
impl Writer for NoopWriter {
    fn write<P: AsRef<Path>>(&mut self, _name: P, _data: &[u8]) -> std::io::Result<()> {
        self.num += 1;
        Ok(())
    }
    fn commit(self) -> std::io::Result<()> {
        Ok(())
    }
    fn abort(self) -> std::io::Result<()> {
        Ok(())
    }
    fn num_to_commit(&self) -> usize {
        self.num
    }
}

fn int_record(k: i32, v: i32) -> Value {
    Value::Record(vec![
        ("k".to_string(), Value::Int(k)),
        ("v".to_string(), Value::Int(v)),
    ])
}

// --- infer_row_schema_from_record -----------------------------------

#[test]
fn infer_row_schema_supported_types() {
    let rec = Value::Record(vec![
        ("a".to_string(), Value::Int(1)),
        ("b".to_string(), Value::Long(2)),
        ("c".to_string(), Value::String("x".to_string())),
        ("d".to_string(), Value::Double(1.5)),
        ("e".to_string(), Value::Float(1.5)),
        ("f".to_string(), Value::Boolean(true)),
        ("g".to_string(), Value::Bytes(vec![0, 1, 2])),
    ]);
    let schema = infer_row_schema_from_record(&rec).unwrap();
    // Round-trip through to_string to make sure it's a valid record schema.
    let s = schema.canonical_form();
    assert!(s.contains(r#""name":"a""#));
    assert!(s.contains(r#""name":"g""#));
}

#[test]
fn infer_row_schema_rejects_unsupported() {
    let rec = Value::Record(vec![("xs".to_string(), Value::Array(vec![Value::Int(1)]))]);
    let err = infer_row_schema_from_record(&rec).unwrap_err();
    match err {
        BeechError::Schema(SchemaError::UnsupportedFieldType { .. }) => (),
        other => panic!("unexpected error: {:?}", other),
    }
}

// --- write_rows_to_prolly_tree error paths -------------------------

#[test]
fn write_rejects_empty_input() {
    let mut w = NoopWriter::new();
    let err =
        write_rows_to_prolly_tree(&mut w, "t".to_string(), vec![0], vec![], 64, 16, None).unwrap_err();
    match err {
        BeechError::Domain(DomainError::InvalidArgs(_)) => (),
        other => panic!("expected InvalidArgs, got {:?}", other),
    }
}

#[test]
fn write_rejects_unsupported_field_type() {
    let mut w = NoopWriter::new();
    let rows = vec![(
        1i64,
        Value::Record(vec![
            ("k".to_string(), Value::Int(1)),
            ("xs".to_string(), Value::Array(vec![Value::Int(1)])),
        ]),
    )];
    let err = write_rows_to_prolly_tree(&mut w, "t".to_string(), vec![0], rows, 64, 16, None).unwrap_err();
    match err {
        BeechError::Schema(SchemaError::UnsupportedFieldType { .. }) => (),
        other => panic!("expected UnsupportedFieldType, got {:?}", other),
    }
}

#[test]
fn write_rejects_duplicate_keys_in_input() {
    let mut w = NoopWriter::new();
    let rows = vec![
        (1i64, int_record(5, 10)),
        (2i64, int_record(5, 20)), // same key
    ];
    let err = write_rows_to_prolly_tree(&mut w, "t".to_string(), vec![0], rows, 64, 16, None).unwrap_err();
    match err {
        BeechError::Domain(DomainError::DuplicateKey { .. }) => (),
        other => panic!("expected DuplicateKey, got {:?}", other),
    }
}

// --- Change key accessor -------------------------------------------

#[test]
fn change_key_accessor_returns_key() {
    let k = vec![Value::Int(42)];
    let ins = Change::Insert {
        key: k.clone(),
        row_id: 1,
        record: vec![Value::Int(42), Value::Int(0)],
    };
    let upd = Change::Update {
        key: k.clone(),
        row_id: 1,
        record: vec![Value::Int(42), Value::Int(9)],
    };
    let del = Change::Delete { key: k.clone() };
    assert_eq!(ins.key(), &k);
    assert_eq!(upd.key(), &k);
    assert_eq!(del.key(), &k);
}
