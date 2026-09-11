use crate::error::{bail, beech_error};
use crate::{DataType, Decimal, RecordBatch, Result, TableSchema};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt64Array,
};
use std::cmp::Ordering;

pub type Key = Vec<Scalar>;
pub type Row = (i64, Vec<Scalar>);
/// Keys use strict schema types, nulls first, and IEEE total order for floats.
/// Floating-point identity preserves signed zero and NaN payload bits.
#[derive(Debug, Clone)]
pub enum Scalar {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal(Decimal),
    Utf8(String),
    Binary(Vec<u8>),
}
impl Scalar {
    pub fn compare(&self, other: &Self) -> Result<Ordering> {
        self.as_ref().compare(&other.as_ref())
    }
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    pub(crate) fn validate_type(&self, typ: &DataType, nullable: bool) -> Result<()> {
        self.as_ref().validate_type(typ, nullable)
    }
    pub fn from_array(array: &dyn Array, row: usize) -> Result<Self> {
        Ok(ScalarRef::from_array(array, row)?.into_owned())
    }
    pub(crate) fn as_ref(&self) -> ScalarRef<'_> {
        match self {
            Self::Null => ScalarRef::Null,
            Self::Boolean(v) => ScalarRef::Boolean(*v),
            Self::Int32(v) => ScalarRef::Int32(*v),
            Self::Int64(v) => ScalarRef::Int64(*v),
            Self::UInt64(v) => ScalarRef::UInt64(*v),
            Self::Float32(v) => ScalarRef::Float32(*v),
            Self::Float64(v) => ScalarRef::Float64(*v),
            Self::Decimal(v) => ScalarRef::Decimal(*v),
            Self::Utf8(v) => ScalarRef::Utf8(v),
            Self::Binary(v) => ScalarRef::Binary(v),
        }
    }
}

/// A scalar view whose string and binary contents borrow existing storage.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScalarRef<'a> {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal(Decimal),
    Utf8(&'a str),
    Binary(&'a [u8]),
}
impl<'a> ScalarRef<'a> {
    pub(crate) fn compare(&self, other: &Self) -> Result<Ordering> {
        Ok(match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Int32(a), Self::Int32(b)) => a.cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::UInt64(a), Self::UInt64(b)) => a.cmp(b),
            (Self::Float32(a), Self::Float32(b)) => a.total_cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.total_cmp(b),
            (Self::Decimal(a), Self::Decimal(b)) if a.scale() == b.scale() => {
                a.unscaled().cmp(&b.unscaled())
            }
            (Self::Utf8(a), Self::Utf8(b)) => a.cmp(b),
            (Self::Binary(a), Self::Binary(b)) => a.cmp(b),
            _ => {
                bail!(Schema, "incomparable scalar types; explicit conversion required");
            }
        })
    }
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    pub(crate) fn validate_type(&self, typ: &DataType, nullable: bool) -> Result<()> {
        let valid = match (self, typ) {
            (Self::Null, _) => nullable,
            (Self::Boolean(_), DataType::Boolean)
            | (Self::Int32(_), DataType::Int32)
            | (Self::Int64(_), DataType::Int64)
            | (Self::UInt64(_), DataType::UInt64)
            | (Self::Float32(_), DataType::Float32)
            | (Self::Float64(_), DataType::Float64)
            | (Self::Utf8(_), DataType::Utf8)
            | (Self::Binary(_), DataType::Binary) => true,
            (Self::Decimal(value), DataType::Decimal128(precision, scale)) => {
                *scale >= 0
                    && *scale as u8 == value.scale()
                    && value.scale() <= *precision
                    && value.fits_precision(*precision)
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(beech_error!(
                Schema,
                "value {self:?} does not match {typ:?}, nullable={nullable}"
            ))
        }
    }
    pub(crate) fn from_array(array: &'a dyn Array, row: usize) -> Result<Self> {
        if row >= array.len() {
            bail!(Query, "row index out of bounds");
        }
        if array.is_null(row) {
            return Ok(Self::Null);
        }
        macro_rules! primitive {
            ($t:ty,$v:ident) => {
                Self::$v(
                    array
                        .as_any()
                        .downcast_ref::<$t>()
                        .ok_or_else(|| beech_error!(Schema, "array type mismatch"))?
                        .value(row),
                )
            };
        }
        Ok(match array.data_type() {
            DataType::Boolean => primitive!(BooleanArray, Boolean),
            DataType::Int32 => primitive!(Int32Array, Int32),
            DataType::Int64 => primitive!(Int64Array, Int64),
            DataType::UInt64 => primitive!(UInt64Array, UInt64),
            DataType::Float32 => primitive!(Float32Array, Float32),
            DataType::Float64 => primitive!(Float64Array, Float64),
            DataType::Decimal128(precision, scale) => {
                let array = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| beech_error!(Schema, "array type mismatch"))?;
                let value = Self::Decimal(Decimal::new(
                    array.value(row),
                    u8::try_from(*scale).map_err(|_| beech_error!(Schema, "negative decimal scale"))?,
                )?);
                value.validate_type(&DataType::Decimal128(*precision, *scale), false)?;
                value
            }
            DataType::Utf8 => Self::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| beech_error!(Schema, "array type mismatch"))?
                    .value(row),
            ),
            DataType::Binary => Self::Binary(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| beech_error!(Schema, "array type mismatch"))?
                    .value(row),
            ),
            typ => bail!(Schema, "unsupported array {typ:?}"),
        })
    }
    fn into_owned(self) -> Scalar {
        match self {
            Self::Null => Scalar::Null,
            Self::Boolean(v) => Scalar::Boolean(v),
            Self::Int32(v) => Scalar::Int32(v),
            Self::Int64(v) => Scalar::Int64(v),
            Self::UInt64(v) => Scalar::UInt64(v),
            Self::Float32(v) => Scalar::Float32(v),
            Self::Float64(v) => Scalar::Float64(v),
            Self::Decimal(v) => Scalar::Decimal(v),
            Self::Utf8(v) => Scalar::Utf8(v.into()),
            Self::Binary(v) => Scalar::Binary(v.into()),
        }
    }
}
impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other).is_ok_and(|o| o == Ordering::Equal)
    }
}
impl Eq for Scalar {}
pub trait KeyOrdering {
    fn compare_key(&self, other: &Self) -> Result<Ordering>;
}
impl KeyOrdering for Key {
    fn compare_key(&self, other: &Self) -> Result<Ordering> {
        for (a, b) in self.iter().zip(other) {
            let cmp = a.compare(b)?;
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(self.len().cmp(&other.len()))
    }
}
pub(crate) fn prefix_cmp(key: &Key, prefix: &Key) -> Result<Ordering> {
    for (a, b) in key.iter().zip(prefix) {
        let cmp = a.compare(b)?;
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}
/// Compare adjacent keys without materializing their scalar values.
fn compare_rows(columns: &[&dyn Array], left: usize, right: usize) -> Result<Ordering> {
    for &array in columns {
        let cmp = ScalarRef::from_array(array, left)?.compare(&ScalarRef::from_array(array, right)?)?;
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

pub(crate) fn prefix_cmp_at(columns: &[&dyn Array], row: usize, prefix: &Key) -> Result<Ordering> {
    for (&array, value) in columns.iter().zip(prefix) {
        let cmp = ScalarRef::from_array(array, row)?.compare(&value.as_ref())?;
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn key_at(schema: &TableSchema, batch: &RecordBatch, row: usize) -> Result<Key> {
    schema
        .key_columns()
        .iter()
        .map(|&col| {
            let array = batch
                .columns()
                .get(col + 1)
                .ok_or_else(|| beech_error!(Schema, "physical batch arity mismatch"))?;
            Scalar::from_array(array.as_ref(), row)
        })
        .collect()
}

/// Decoded summary of a column. Missing or inexact bounds remain unknown.
#[derive(Debug, Clone, Default)]
pub(crate) struct ColumnStatistics {
    pub(crate) null_count: Option<u64>,
    pub(crate) bounds: Option<(Scalar, Scalar)>,
}

/// Check leaf rows against the table schema and the prolly tree's key ordering.
pub(crate) fn validate_leaf(schema: &TableSchema, batch: &RecordBatch) -> Result<Key> {
    if batch.schema() != schema.physical_schema() {
        bail!(Schema, "leaf physical schema mismatch");
    }
    for (array, field) in batch.columns().iter().zip(batch.schema().fields()) {
        if !field.is_nullable() && array.null_count() != 0 {
            bail!(Schema, "null in required column");
        }
        // Arrow's precision/scale annotation alone does not check the values.
        // Validate every decimal column, including ones outside the key.
        if let DataType::Decimal128(precision, _) = field.data_type() {
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| beech_error!(Schema, "array type mismatch"))?
                .validate_decimal_precision(*precision)?;
        }
    }
    if batch.num_rows() == 0 {
        bail!(InvalidNode, "leaf has 0 rows; empty tables have no root");
    }
    // Schema, nullability and decimal precision were checked column-wise above.
    let columns: Vec<&dyn Array> =
        schema.key_columns().iter().map(|&col| batch.column(col + 1).as_ref()).collect();
    for row in 1..batch.num_rows() {
        if compare_rows(&columns, row - 1, row)? != Ordering::Less {
            // Only materialize invalid keys to include them in the error.
            let key = key_at(schema, batch, row)?;
            let prev = key_at(schema, batch, row - 1)?;
            bail!(
                InvalidNode,
                "leaf row {row}: key {key:?} is not greater than previous row's key {prev:?}; keys must be unique and sorted"
            );
        }
    }
    key_at(schema, batch, batch.num_rows() - 1)
}
