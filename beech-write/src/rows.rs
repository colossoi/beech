use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, UInt64Array,
};
use beech_core::{BeechError, DataType, RecordBatch, Result, Row, Scalar, TableSchema};
use std::sync::Arc;

/// Convert schema-validated logical rows to a physical Arrow batch.
pub fn batch_from_rows(schema: &TableSchema, rows: &[Row]) -> Result<RecordBatch> {
    for row in rows {
        schema.key_from_row(row)?;
    }
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0)))];
    for (col, field) in schema.fields().iter().enumerate() {
        macro_rules! numeric {
            ($arr:ty,$variant:ident) => {
                Arc::new(<$arr>::from(
                    rows.iter()
                        .map(|r| match &r.1[col] {
                            Scalar::$variant(v) => Some(*v),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef
            };
        }
        arrays.push(match field.data_type() {
            DataType::Boolean => numeric!(BooleanArray, Boolean),
            DataType::Int32 => numeric!(Int32Array, Int32),
            DataType::Int64 => numeric!(Int64Array, Int64),
            DataType::UInt64 => numeric!(UInt64Array, UInt64),
            DataType::Float32 => numeric!(Float32Array, Float32),
            DataType::Float64 => numeric!(Float64Array, Float64),
            DataType::Decimal128(precision, scale) => Arc::new(
                Decimal128Array::from(
                    rows.iter()
                        .map(|r| match &r.1[col] {
                            Scalar::Decimal(value) => Some(value.unscaled()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(*precision, *scale)?,
            ),
            DataType::Utf8 => Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| match &r.1[col] {
                        Scalar::Utf8(v) => Some(v.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Binary => Arc::new(BinaryArray::from(
                rows.iter()
                    .map(|r| match &r.1[col] {
                        Scalar::Binary(v) => Some(v.as_slice()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            typ => {
                return Err(BeechError::Schema(format!("unsupported data type: {typ:?}")));
            }
        });
    }
    Ok(RecordBatch::try_new(schema.physical_schema(), arrays)?)
}
