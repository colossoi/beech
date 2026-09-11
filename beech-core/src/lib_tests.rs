use crate::{test_support::*, *};
use std::cmp::Ordering;

#[test]
fn id_hex_round_trip_and_rejection() {
    let text = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let id = Id::from_hex(text).unwrap();
    assert_eq!(id.to_string(), text);
    assert_eq!(Id::from_hex(&text.to_uppercase()).unwrap(), id);
    for bad in ["abc", &"z".repeat(64), &"é".repeat(32)] {
        assert!(Id::from_hex(bad).is_err());
    }
    assert!(Id::from_slice(&[0; 31]).is_err());
}
#[test]
fn scalar_order_is_typed_and_lexicographic() {
    assert_eq!(
        vec![Scalar::Null].compare_key(&vec![Scalar::Int64(0)]).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        vec![Scalar::Int64(1), Scalar::Int32(3)]
            .compare_key(&vec![Scalar::Int64(2), Scalar::Int32(1)])
            .unwrap(),
        Ordering::Less
    );
    assert!(Scalar::Int64(1).compare(&Scalar::Utf8("1".into())).is_err());
    assert!(Scalar::Int64(1).compare(&Scalar::UInt64(1)).is_err());
    assert_eq!(
        Scalar::Float64(-0.0).compare(&Scalar::Float64(0.0)).unwrap(),
        Ordering::Less
    );
    assert_ne!(Scalar::Float64(-0.0), Scalar::Float64(0.0));
    let nan = Scalar::Float64(f64::from_bits(0x7ff8000000000001));
    assert_eq!(nan, nan.clone());
}

#[test]
fn scalar_views_borrow_sliced_arrow_buffers() {
    use crate::value::ScalarRef;
    use arrow_array::{BinaryArray, StringArray};

    let strings = StringArray::from(vec![Some("skip"), Some("héllo"), None]).slice(1, 2);
    let ScalarRef::Utf8(text) = ScalarRef::from_array(&strings, 0).unwrap() else {
        panic!("expected borrowed string");
    };
    assert_eq!(text, "héllo");
    assert_eq!(text.as_ptr(), strings.value(0).as_ptr());
    assert!(ScalarRef::from_array(&strings, 1).unwrap().is_null());
    assert!(ScalarRef::from_array(&strings, 2).is_err());

    let binary = BinaryArray::from(vec![Some(&b"skip"[..]), Some(&b"\0\xff"[..]), None]).slice(1, 2);
    let ScalarRef::Binary(bytes) = ScalarRef::from_array(&binary, 0).unwrap() else {
        panic!("expected borrowed bytes");
    };
    assert_eq!(bytes, b"\0\xff");
    assert_eq!(bytes.as_ptr(), binary.value(0).as_ptr());
    assert!(ScalarRef::from_array(&binary, 1).unwrap().is_null());
}
#[test]
fn schema_rejects_invalid_columns_and_keys() {
    let f = Field::new("x", DataType::Int64, false);
    for keys in [vec![], vec![1], vec![0, 0]] {
        assert!(TableSchema::new(vec![f.clone()], keys).is_err());
    }
    assert!(TableSchema::new(vec![f.clone(), f], vec![0]).is_err());
    assert!(TableSchema::new(vec![Field::new(ROW_ID_COLUMN, DataType::Int64, false)], vec![0]).is_err());
    assert!(TableSchema::new(vec![Field::new("x", DataType::Date32, false)], vec![0]).is_err());
    assert!(batch_from_rows(&schema(), &[(1, vec![Scalar::Null; 4])]).is_err());
}
#[test]
fn internal_fences_cover_final_child_and_reject_bad_structure() {
    let s = schema();
    let (_, _, objects) = build(&s, &rows(12), 4, 3);
    let internal = codec::thrift::decode_internal(&objects.last().unwrap().bytes, &s).unwrap();
    for (key, slot) in [
        (-1, Some(0)),
        (3, Some(0)),
        (4, Some(1)),
        (7, Some(1)),
        (8, Some(2)),
        (11, Some(2)),
        (12, None),
    ] {
        assert_eq!(internal.seek(&s, &vec![Scalar::Int64(key)]).unwrap(), slot);
    }
    let mut bad = internal.clone();
    bad.children.swap(0, 1);
    assert!(bad.validate(&s).is_err());
    let mut bad = internal.clone();
    bad.children[0].height = 1;
    assert!(bad.validate(&s).is_err());
    let mut bad = internal.clone();
    bad.children[0].row_count = u64::MAX;
    assert!(bad.validate(&s).is_err());
    assert!(InternalNode::new(&s, 1, vec![]).is_err());
}
#[test]
fn empty_and_single_leaf_root_contract() {
    let (empty, _, _) = build(&schema(), &[], 4, 3);
    assert!(empty.root.is_none());
    let (single, _, _) = build(&schema(), &rows(2), 4, 3);
    assert_eq!(single.root.unwrap().height, 0);
}
