use super::FormatTag;
use crate::{storage::Repository, test_support::*, *};
use std::{
    collections::BTreeMap,
    time::{Duration, UNIX_EPOCH},
};
#[test]
fn root_has_golden_thrift_compact_bytes() {
    let root = Root { id: Id::default() };
    let mut golden = vec![0x42, 0x43, 0x48, 1, 6, 0x18, 0x20];
    golden.extend([0; 32]);
    golden.push(0);
    assert_eq!(
        codec::thrift::encode_root(&root).unwrap().bytes().as_ref(),
        golden.as_slice()
    );
    assert_eq!(
        codec::object_id(FormatTag::Root, &golden).to_string(),
        "b48a87d648e888c0820f9275e3a5dfef2b190b87e59fa58fa7f9439efc78df8e"
    );
    assert_eq!(codec::thrift::decode_root(&golden).unwrap(), root);
}
#[test]
fn schema_and_empty_table_round_trip_preserve_key_positions() {
    let s = TableSchema::new(
        vec![
            Field::new("value", DataType::Utf8, true),
            Field::new("key", DataType::UInt64, false),
        ],
        vec![1],
    )
    .unwrap();
    let bytes = codec::thrift::encode_schema(&s).unwrap();
    assert_eq!(codec::thrift::decode_schema(bytes.bytes()).unwrap(), s);
    let t = Table::new("empty", s, None).unwrap();
    assert_eq!(
        codec::thrift::decode_table(codec::thrift::encode_table(&t).unwrap().bytes()).unwrap(),
        t
    );
}
#[test]
fn transaction_directory_is_canonical_and_preserves_microseconds() {
    let mut map = BTreeMap::new();
    map.insert("z".into(), Id::from(1));
    map.insert("a".into(), Id::from(2));
    for time in [
        UNIX_EPOCH + Duration::from_micros(1234567),
        UNIX_EPOCH - Duration::from_micros(1234),
    ] {
        let txn = Transaction::new(Id::from(7), time, map.clone()).unwrap();
        let encoded = codec::thrift::encode_transaction(&txn).unwrap();
        assert_eq!(codec::thrift::decode_transaction(encoded.bytes()).unwrap(), txn);
    }
}
#[test]
fn wire_rejects_truncation_wrong_kind_version_and_noncanonical_fields() {
    let bytes = codec::thrift::encode_root(&Root { id: Id::from(1) }).unwrap().bytes().to_vec();
    for end in 0..bytes.len() {
        assert!(codec::thrift::decode_root(&bytes[..end]).is_err(), "prefix {end}");
    }
    let mut bad = bytes.clone();
    bad.push(0);
    assert!(codec::thrift::decode_root(&bad).is_err());
    let mut bad = bytes.clone();
    bad[3] = 2;
    assert!(codec::thrift::decode_root(&bad).is_err());
    assert!(codec::thrift::decode_table(&bytes).is_err());
    let mut bad = bytes.clone();
    bad.pop();
    bad.extend([0x15, 0x0e, 0]);
    assert!(codec::thrift::decode_root(&bad).is_err());
    let bad = [0x42, 0x43, 0x48, 1, 6, 0x18, 0xff, 0xff, 0xff, 0xff, 0x07];
    assert!(codec::thrift::decode_root(&bad).is_err());
}
#[test]
fn internal_hash_binds_child_id_with_same_separator() {
    let s = schema();
    let (_, _, objects) = build(&s, &rows(12), 4, 3);
    let original = objects.last().unwrap();
    let mut node = codec::thrift::decode_internal(&original.bytes, &s).unwrap();
    node.children[0].id = Id::from(999);
    let changed = codec::thrift::encode_internal(&node, &s).unwrap();
    assert_ne!(changed.reference.id, original.reference.id);
    assert_eq!(changed.reference.max_key, original.reference.max_key);
}
#[test]
fn parquet_bytes_and_hash_include_row_ids() {
    let s = schema();
    let data = rows(30);
    let batch = batch_from_rows(&s, &data).unwrap();
    let whole = codec::parquet::encode_leaf(&s, &batch).unwrap();
    assert_eq!(&whole.bytes[..4], b"PAR1");
    assert_eq!(&whole.bytes[whole.bytes.len() - 4..], b"PAR1");
    let mut changed = data;
    changed[0].0 += 1;
    assert_ne!(
        whole.reference.id,
        codec::parquet::encode_leaf(&s, &batch_from_rows(&s, &changed).unwrap()).unwrap().reference.id
    );
}
#[test]
fn leaf_rejects_unsorted_duplicate_and_empty_rows() {
    let s = schema();
    let mut data = rows(4);
    data.swap(0, 1);
    assert!(codec::parquet::encode_leaf(&s, &batch_from_rows(&s, &data).unwrap()).is_err());
    data = rows(4);
    data[1].1[0] = data[0].1[0].clone();
    assert!(codec::parquet::encode_leaf(&s, &batch_from_rows(&s, &data).unwrap()).is_err());
    assert!(codec::parquet::encode_leaf(&s, &RecordBatch::new_empty(s.physical_schema())).is_err());
}
#[test]
fn all_scalar_payloads_and_key_bits_round_trip() {
    let types = [
        DataType::Boolean,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
    ];
    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    fields.extend(types.iter().enumerate().map(|(i, t)| Field::new(format!("v{i}"), t.clone(), true)));
    let s = TableSchema::new(fields, vec![0]).unwrap();
    let mut v = vec![
        Scalar::Int64(0),
        Scalar::Boolean(true),
        Scalar::Int32(i32::MIN),
        Scalar::Int64(i64::MIN),
        Scalar::UInt64(u64::MAX),
        Scalar::Float32(f32::from_bits(0x7fc00001)),
        Scalar::Float64(-0.0),
        Scalar::Utf8("héllo".into()),
        Scalar::Binary(vec![0, 255]),
    ];
    let mut data = vec![(i64::MIN, v.clone())];
    v = vec![Scalar::Null; 9];
    v[0] = Scalar::Int64(1);
    data.push((i64::MAX, v));
    let (table, store, _) = build(&s, &data, 1, 3);
    let source = Repository::new(store);
    let got = query::RowCursor::new(&source, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(got, data);
}
#[test]
fn root_transaction_and_table_can_be_reopened() {
    let (table, store, _) = build(&schema(), &rows(17), 4, 3);
    let encoded = codec::thrift::encode_table(&table).unwrap();
    let table_id = encoded.id();
    store.put(table_id, encoded.bytes().clone()).unwrap();
    let txn = Transaction::new(
        Id::default(),
        UNIX_EPOCH,
        BTreeMap::from([(table.name.clone(), table_id)]),
    )
    .unwrap();
    let encoded = codec::thrift::encode_transaction(&txn).unwrap();
    let txn_id = encoded.id();
    store.put(txn_id, encoded.bytes().clone()).unwrap();
    let encoded = codec::thrift::encode_root(&Root::new(txn_id)).unwrap();
    let root_id = encoded.id();
    store.put(root_id, encoded.bytes().clone()).unwrap();
    let source = Repository::new(store);
    let root = source.get_root(&root_id).unwrap();
    let loaded = source.get_transaction(&root.id).unwrap();
    assert_eq!(source.get_table(&loaded, "items").unwrap().as_ref(), &table);
}

#[test]
fn independently_written_pyarrow_leaf_is_readable() {
    let s = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec![0],
    )
    .unwrap();
    let bytes = bytes::Bytes::from_static(include_bytes!("../../tests/fixtures/python-leaf.parquet"));
    let store = std::sync::Arc::new(test_support::MemoryStore::default());
    let id = codec::object_id(FormatTag::Leaf, &bytes);
    store.put(id, bytes).unwrap();
    let table = Table::new(
        "python",
        s,
        Some(NodeRef {
            id,
            height: 0,
            row_count: 4,
            max_key: vec![Scalar::Int64(9)],
        }),
    )
    .unwrap();
    let source = Repository::new(store);
    assert_eq!(
        source.open_leaf(table.root.as_ref().unwrap(), &table.schema).unwrap().row_count(),
        4
    );
    let rows = query::RowCursor::new(&source, &table, vec![]).unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(
        rows,
        (6..10)
            .map(|i| (
                1000 + i,
                vec![
                    Scalar::Int64(i),
                    if i == 9 { Scalar::Null } else { Scalar::Utf8(format!("python {i}")) }
                ]
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn decimal_wire_rejects_bad_parameters_and_noncanonical_payloads() {
    use super::thrift::generated as g;
    use thrift::protocol::{TCompactOutputProtocol, TSerializable};
    fn encode(tag: u8, value: &impl TSerializable) -> Vec<u8> {
        let mut bytes = vec![b'B', b'C', b'H', 1, tag];
        value.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut bytes)).unwrap();
        bytes
    }
    for (code, precision, scale) in [
        (9, None, Some(2)),
        (9, Some(9), None),
        (9, Some(0), Some(0)),
        (9, Some(39), Some(0)),
        (9, Some(3), Some(4)),
        (9, Some(9), Some(-1)),
        (3, Some(9), Some(2)),
    ] {
        let schema = g::Schema::new(
            vec![g::Column::new("v".into(), code, false, precision, scale)],
            vec![0],
        );
        assert!(codec::thrift::decode_schema(&encode(3, &schema)).is_err());
    }
    let s = TableSchema::new(
        vec![Field::new("amount", DataType::Decimal128(38, 2), true)],
        vec![0],
    )
    .unwrap();
    for (bytes, scale) in [
        (vec![0; 15], 2),
        (vec![0; 17], 2),
        (vec![0; 16], -1),
        (vec![0; 16], 39),
        (10i128.pow(38).to_le_bytes().to_vec(), 2),
        (123i128.to_le_bytes().to_vec(), 3),
    ] {
        let scalar = g::Scalar::DecimalValue(g::Decimal::new(bytes, scale));
        let node = g::InternalNode::new(
            codec::thrift::encode_schema(&s).unwrap().id().as_bytes().to_vec(),
            1,
            vec![
                g::NodeRef::new(vec![0; 32], 0, 1, vec![scalar]),
                g::NodeRef::new(
                    vec![1; 32],
                    0,
                    1,
                    vec![g::Scalar::DecimalValue(g::Decimal::new(
                        999i128.to_le_bytes().to_vec(),
                        2,
                    ))],
                ),
            ],
        );
        assert!(codec::thrift::decode_internal(&encode(2, &node), &s).is_err());
    }
}

#[test]
fn hash_binds_kind_and_content() {
    assert_ne!(
        codec::object_id(FormatTag::Leaf, b"x"),
        codec::object_id(FormatTag::Internal, b"x")
    );
    assert_ne!(
        codec::object_id(FormatTag::Leaf, b"x"),
        codec::object_id(FormatTag::Leaf, b"y")
    );
}
