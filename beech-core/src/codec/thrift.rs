//! Beech metadata envelopes containing generated Thrift Compact Protocol structs.
//! Readers reject noncanonical field order, unknown/duplicate fields, and trailing bytes.
use super::{EncodedNode, EncodedObject, FormatTag, object_id};
use crate::error::{bail, beech_error};
use crate::*;
#[path = "generated/beech.rs"]
pub(super) mod generated;
use generated as g;
use std::{
    collections::BTreeMap,
    io::Cursor,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thrift::{
    TConfiguration,
    protocol::{TCompactInputProtocol, TCompactOutputProtocol, TSerializable},
};
const MAGIC: &[u8; 4] = b"BCH\x01";

fn input_config(encoded_len: usize) -> thrift::Result<TConfiguration> {
    // A declared string/container cannot contain more bytes/elements than this
    // object contains. This checks malformed lengths without imposing a size policy.
    TConfiguration::builder()
        .max_message_size(Some(encoded_len))
        .max_frame_size(Some(encoded_len))
        .max_string_size(Some(encoded_len))
        .max_container_size(Some(encoded_len))
        .build()
}
fn encode<T: TSerializable>(kind: FormatTag, value: &T) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.push(kind as u8);
    value.write_to_out_protocol(&mut TCompactOutputProtocol::with_config(
        &mut bytes,
        TConfiguration::no_limits(),
    ))?;
    Ok(bytes)
}
fn decode<T: TSerializable>(kind: FormatTag, bytes: &[u8]) -> Result<T> {
    if bytes.len() < 5 || &bytes[..4] != MAGIC || bytes[4] != kind as u8 {
        bail!(Wire, "wrong magic, format version, or object kind");
    }
    let mut reader = Cursor::new(&bytes[5..]);
    let value = T::read_from_in_protocol(&mut TCompactInputProtocol::with_config(
        &mut reader,
        input_config(bytes.len() - 5)?,
    ))?;
    if reader.position() as usize != bytes.len() - 5 || encode(kind, &value)? != bytes {
        bail!(Wire, "noncanonical or trailing wire data");
    }
    Ok(value)
}
fn to_scalar(v: &Scalar) -> Result<g::Scalar> {
    let len = match v {
        Scalar::Utf8(s) => s.len(),
        Scalar::Binary(b) => b.len(),
        _ => 0,
    };
    u32::try_from(len)
        .map_err(|_| beech_error!(Wire, "scalar byte length {len} exceeds Thrift u32 representation"))?;
    Ok(match v {
        Scalar::Null => g::Scalar::NullValue(true),
        Scalar::Boolean(x) => g::Scalar::BooleanValue(*x),
        Scalar::Int32(x) => g::Scalar::Int32Value(*x),
        Scalar::Int64(x) => g::Scalar::Int64Value(*x),
        Scalar::UInt64(x) => g::Scalar::Uint64Value(x.to_le_bytes().to_vec()),
        Scalar::Float32(x) => g::Scalar::Float32Bits(x.to_bits() as i32),
        Scalar::Float64(x) => g::Scalar::Float64Bits(x.to_bits() as i64),
        Scalar::Decimal(value) => g::Scalar::DecimalValue(g::Decimal::new(
            value.unscaled().to_le_bytes().to_vec(),
            i32::from(value.scale()),
        )),
        Scalar::Utf8(x) => g::Scalar::StringValue(x.clone()),
        Scalar::Binary(x) => g::Scalar::BinaryValue(x.clone()),
    })
}
fn from_scalar(v: g::Scalar) -> Result<Scalar> {
    Ok(match v {
        g::Scalar::NullValue(true) => Scalar::Null,
        g::Scalar::NullValue(false) => bail!(Wire, "false null marker"),
        g::Scalar::BooleanValue(x) => Scalar::Boolean(x),
        g::Scalar::Int32Value(x) => Scalar::Int32(x),
        g::Scalar::Int64Value(x) => Scalar::Int64(x),
        g::Scalar::Uint64Value(x) => Scalar::UInt64(u64::from_le_bytes(
            x.try_into().map_err(|_| beech_error!(Wire, "invalid uint64 width"))?,
        )),
        g::Scalar::Float32Bits(x) => Scalar::Float32(f32::from_bits(x as u32)),
        g::Scalar::Float64Bits(x) => Scalar::Float64(f64::from_bits(x as u64)),
        g::Scalar::DecimalValue(value) => Scalar::Decimal(Decimal::new(
            i128::from_le_bytes(
                value.unscaled.try_into().map_err(|_| beech_error!(Wire, "invalid decimal width"))?,
            ),
            u8::try_from(value.scale).map_err(|_| beech_error!(Wire, "invalid decimal scale"))?,
        )?),
        g::Scalar::StringValue(x) => Scalar::Utf8(x),
        g::Scalar::BinaryValue(x) => Scalar::Binary(x),
    })
}
fn to_thrift_schema(s: &TableSchema) -> Result<g::Schema> {
    i32::try_from(s.fields().len())
        .map_err(|_| beech_error!(Wire, "column count exceeds Thrift i32 representation"))?;
    i32::try_from(s.key_columns().len())
        .map_err(|_| beech_error!(Wire, "key count exceeds Thrift i32 representation"))?;
    Ok(g::Schema::new(
        s.fields()
            .iter()
            .map(|f| {
                let (precision, scale) = match f.data_type() {
                    DataType::Decimal128(precision, scale) => {
                        (Some(i32::from(*precision)), Some(i32::from(*scale)))
                    }
                    _ => (None, None),
                };
                u32::try_from(f.name().len())
                    .map_err(|_| beech_error!(Wire, "column name exceeds Thrift string representation"))?;
                Ok(g::Column::new(
                    f.name().clone(),
                    type_code(f.data_type())?,
                    f.is_nullable(),
                    precision,
                    scale,
                ))
            })
            .collect::<Result<_>>()?,
        s.key_columns()
            .iter()
            .map(|&i| {
                i32::try_from(i)
                    .map_err(|_| beech_error!(Wire, "key index exceeds Thrift i32 representation"))
            })
            .collect::<Result<_>>()?,
    ))
}
fn from_thrift_schema(s: g::Schema) -> Result<TableSchema> {
    TableSchema::new(
        s.columns
            .into_iter()
            .map(|c| {
                Ok(Field::new(
                    c.name,
                    from_code(c.data_type, c.precision, c.scale)?,
                    c.nullable,
                ))
            })
            .collect::<Result<_>>()?,
        s.key_columns
            .into_iter()
            .map(|i| usize::try_from(i).map_err(|_| beech_error!(Schema, "negative key index")))
            .collect::<Result<_>>()?,
    )
}
pub fn encode_schema(s: &TableSchema) -> Result<EncodedObject> {
    encode(FormatTag::Schema, &to_thrift_schema(s)?)
        .map(|bytes| EncodedObject::new(FormatTag::Schema, bytes))
}
pub fn decode_schema(bytes: &[u8]) -> Result<TableSchema> {
    from_thrift_schema(decode(FormatTag::Schema, bytes)?)
}
pub(super) fn schema_id(s: &TableSchema) -> Result<Id> {
    Ok(encode_schema(s)?.id())
}
/// Stable logical input for chunking. This is separate from stored object hashes.
pub fn encode_key(key: &Key) -> Result<Vec<u8>> {
    i32::try_from(key.len())
        .map_err(|_| beech_error!(Wire, "key arity exceeds Thrift i32 representation"))?;
    // A standalone key uses the NodeRef schema with fixed fields for stable framing.
    encode(
        FormatTag::Key,
        &g::NodeRef::new(
            vec![0; 32],
            0,
            0,
            key.iter().map(to_scalar).collect::<Result<_>>()?,
        ),
    )
}
pub fn encode_row_for_splitting(row: &Row) -> Result<Vec<u8>> {
    // Retain full-row boundary input, including the row ID in the new format.
    let mut values = vec![Scalar::Int64(row.0)];
    values.extend(row.1.iter().cloned());
    encode_key(&values)
}
fn to_ref(r: &NodeRef) -> Result<g::NodeRef> {
    i32::try_from(r.max_key.len())
        .map_err(|_| beech_error!(Wire, "node {}: key arity exceeds Thrift i32 representation", r.id))?;
    Ok(g::NodeRef::new(
        r.id.as_bytes().to_vec(),
        i32::try_from(r.height).map_err(|_| {
            beech_error!(
                Wire,
                "node {}: height {} exceeds Thrift i32 representation",
                r.id,
                r.height
            )
        })?,
        i64::try_from(r.row_count).map_err(|_| {
            beech_error!(
                Wire,
                "node {}: row count {} exceeds Thrift i64 representation",
                r.id,
                r.row_count
            )
        })?,
        r.max_key.iter().map(to_scalar).collect::<Result<_>>()?,
    ))
}
fn from_ref(r: g::NodeRef) -> Result<NodeRef> {
    let id = Id::from_slice(&r.id)?;
    Ok(NodeRef {
        id,
        height: u32::try_from(r.height)
            .map_err(|_| beech_error!(Wire, "node {id}: height {} is negative", r.height))?,
        row_count: u64::try_from(r.row_count)
            .map_err(|_| beech_error!(Wire, "node {id}: row count {} is negative", r.row_count))?,
        max_key: r.max_key.into_iter().map(from_scalar).collect::<Result<_>>()?,
    })
}
pub fn encode_internal(node: &InternalNode, schema: &TableSchema) -> Result<EncodedNode> {
    node.validate(schema)?;
    i32::try_from(node.children.len())
        .map_err(|_| beech_error!(Wire, "child count exceeds Thrift i32 representation"))?;
    let row_count = node.row_count()?;
    i64::try_from(row_count).map_err(|_| {
        beech_error!(
            Wire,
            "internal row count {row_count} exceeds Thrift i64 representation"
        )
    })?;
    let bytes = encode(
        FormatTag::Internal,
        &g::InternalNode::new(
            schema_id(schema)?.as_bytes().to_vec(),
            i32::try_from(node.height).map_err(|_| {
                beech_error!(
                    Wire,
                    "internal height {} exceeds Thrift i32 representation",
                    node.height
                )
            })?,
            node.children.iter().map(to_ref).collect::<Result<_>>()?,
        ),
    )?;
    let reference = NodeRef {
        id: object_id(FormatTag::Internal, &bytes),
        height: node.height,
        row_count,
        max_key: node.children.last().expect("validated fanout").max_key.clone(),
    };
    Ok(EncodedNode {
        reference,
        bytes: bytes.into(),
    })
}
pub fn decode_internal(bytes: &[u8], schema: &TableSchema) -> Result<InternalNode> {
    let node: g::InternalNode = decode(FormatTag::Internal, bytes)?;
    let actual_schema = Id::from_slice(&node.schema_id)?;
    let expected_schema = schema_id(schema)?;
    if actual_schema != expected_schema {
        bail!(
            InvalidNode,
            "internal schema {actual_schema} does not match expected schema {expected_schema}"
        );
    }
    let result = InternalNode {
        schema: schema.clone(),
        height: u32::try_from(node.height)
            .map_err(|_| beech_error!(Wire, "internal height {} is negative", node.height))?,
        children: node.children.into_iter().map(from_ref).collect::<Result<_>>()?,
    };
    result.validate(schema)?;
    let row_count = result.row_count()?;
    i64::try_from(row_count).map_err(|_| {
        beech_error!(
            Wire,
            "internal row count {row_count} exceeds Thrift i64 representation"
        )
    })?;
    Ok(result)
}
pub fn encode_table(table: &Table) -> Result<EncodedObject> {
    table.validate()?;
    u32::try_from(table.name.len())
        .map_err(|_| beech_error!(Wire, "table name exceeds Thrift string representation"))?;
    encode(
        FormatTag::Table,
        &g::Table::new(
            table.name.clone(),
            to_thrift_schema(&table.schema)?,
            table.root.as_ref().map(to_ref).transpose()?,
        ),
    )
    .map(|bytes| EncodedObject::new(FormatTag::Table, bytes))
}
pub fn decode_table(bytes: &[u8]) -> Result<Table> {
    let t: g::Table = decode(FormatTag::Table, bytes)?;
    Table::new(
        t.name,
        from_thrift_schema(t.schema)?,
        t.root.map(from_ref).transpose()?,
    )
}
fn micros(time: SystemTime) -> Result<i64> {
    let (negative, d) = match time.duration_since(UNIX_EPOCH) {
        Ok(d) => (false, d),
        Err(e) => (true, e.duration()),
    };
    // Preserve the original codec's acceptance of SystemTime values, truncating
    // to the stored precision rather than rejecting sub-microsecond inputs.
    let n = i128::try_from(d.as_micros()).map_err(|_| beech_error!(Wire, "timestamp overflow"))?;
    i64::try_from(if negative { -n } else { n }).map_err(|_| beech_error!(Wire, "timestamp overflow"))
}
pub fn encode_transaction(t: &Transaction) -> Result<EncodedObject> {
    i32::try_from(t.tables.len())
        .map_err(|_| beech_error!(Wire, "table directory count exceeds Thrift i32 representation"))?;
    t.validate()?;
    for name in t.tables.keys() {
        u32::try_from(name.len())
            .map_err(|_| beech_error!(Wire, "table name exceeds Thrift string representation"))?;
    }
    encode(
        FormatTag::Transaction,
        &g::Transaction::new(
            t.prev_id.as_bytes().to_vec(),
            micros(t.transaction_time)?,
            t.tables
                .iter()
                .map(|(name, id)| g::TableRef::new(name.clone(), id.as_bytes().to_vec()))
                .collect(),
        ),
    )
    .map(|bytes| EncodedObject::new(FormatTag::Transaction, bytes))
}
pub fn decode_transaction(bytes: &[u8]) -> Result<Transaction> {
    let t: g::Transaction = decode(FormatTag::Transaction, bytes)?;
    if t.tables.windows(2).any(|p| p[0].name >= p[1].name) {
        bail!(Wire, "table directory must be strictly sorted");
    }
    let tables = t
        .tables
        .into_iter()
        .map(|v| Ok((v.name, Id::from_slice(&v.id)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let delta = Duration::from_micros(t.timestamp_micros.unsigned_abs());
    let time =
        if t.timestamp_micros < 0 { UNIX_EPOCH.checked_sub(delta) } else { UNIX_EPOCH.checked_add(delta) }
            .ok_or_else(|| beech_error!(Wire, "timestamp out of range"))?;
    Transaction::new(Id::from_slice(&t.prev_id)?, time, tables)
}
pub fn encode_root(root: &Root) -> Result<EncodedObject> {
    encode(FormatTag::Root, &g::Root::new(root.id.as_bytes().to_vec()))
        .map(|bytes| EncodedObject::new(FormatTag::Root, bytes))
}
pub fn decode_root(bytes: &[u8]) -> Result<Root> {
    let r: g::Root = decode(FormatTag::Root, bytes)?;
    Ok(Root {
        id: Id::from_slice(&r.transaction_id)?,
    })
}
fn type_code(typ: &DataType) -> Result<i32> {
    Ok(match typ {
        DataType::Boolean => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::UInt64 => 4,
        DataType::Float32 => 5,
        DataType::Float64 => 6,
        DataType::Utf8 => 7,
        DataType::Binary => 8,
        DataType::Decimal128(_, _) => 9,
        _ => bail!(Schema, "unsupported type: {typ:?}"),
    })
}
fn from_code(code: i32, precision: Option<i32>, scale: Option<i32>) -> Result<DataType> {
    if code == 9 {
        let precision = precision
            .and_then(|p| u8::try_from(p).ok())
            .ok_or_else(|| beech_error!(Schema, "missing or invalid decimal precision"))?;
        let scale = scale
            .and_then(|s| i8::try_from(s).ok())
            .ok_or_else(|| beech_error!(Schema, "missing or invalid decimal scale"))?;
        return Ok(DataType::Decimal128(precision, scale));
    }
    if precision.is_some() || scale.is_some() {
        bail!(Schema, "precision/scale on a non-decimal column");
    }
    Ok(match code {
        1 => DataType::Boolean,
        2 => DataType::Int32,
        3 => DataType::Int64,
        4 => DataType::UInt64,
        5 => DataType::Float32,
        6 => DataType::Float64,
        7 => DataType::Utf8,
        8 => DataType::Binary,
        _ => bail!(Schema, "unknown type code"),
    })
}
