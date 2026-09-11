// Beech format version 1. Regenerate with Apache Thrift 0.24.0.
// All integers are range-checked at the domain boundary. IDs are exactly 32 bytes.
namespace rs beech

struct Decimal {
  // Signed 128-bit unscaled integer, exactly 16 little-endian two's-complement bytes.
  1: required binary unscaled,
  2: required i32 scale
}

union Scalar {
  1: bool null_value,
  2: bool boolean_value,
  3: i32 int32_value,
  4: i64 int64_value,
  // Unsigned 64-bit integer, exactly 8 little-endian bytes.
  5: binary uint64_value,
  // IEEE bits preserve Float32 and Float64, including signed zero/NaN payloads.
  6: i32 float32_bits,
  7: i64 float64_bits,
  8: string string_value,
  9: binary binary_value,
  10: Decimal decimal_value
}

struct Column {
  1: required string name,
  // 1=Boolean, 2=Int32, 3=Int64, 4=UInt64, 5=Float32, 6=Float64, 7=Utf8, 8=Binary, 9=Decimal128
  2: required i32 data_type,
  3: required bool nullable
  // Required for Decimal128 (1 <= precision <= 38, 0 <= scale <= precision).
  // Absent for all other types, preserving their canonical bytes.
  4: optional i32 precision,
  5: optional i32 scale
}

struct Schema {
  1: required list<Column> columns,
  2: required list<i32> key_columns
}

struct NodeRef {
  1: required binary id,
  // 0 is a leaf; positive values count edges down to leaves.
  2: required i32 height,
  3: required i64 row_count,
  4: required list<Scalar> max_key
}

struct InternalNode {
  1: required binary schema_id,
  2: required i32 height,
  3: required list<NodeRef> children
}

struct Table {
  1: required string name,
  2: required Schema schema,
  3: optional NodeRef root
}

struct TableRef {
  1: required string name,
  2: required binary id
}

struct Transaction {
  1: required binary prev_id,
  2: required i64 timestamp_micros,
  // Sorted by UTF-8 name, no maps in content-addressed records.
  3: required list<TableRef> tables
}

struct Root {
  1: required binary transaction_id
}
