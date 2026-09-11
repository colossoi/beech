use crate::error::{bail, beech_error};
use crate::{DataType, Field, Key, ROW_ID_COLUMN, Result, Row, Scalar};
use arrow_schema::{Schema, SchemaRef};
use std::{collections::HashSet, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    fields: SchemaRef,
    key_columns: Vec<usize>,
}
impl TableSchema {
    pub fn new(fields: Vec<Field>, key_columns: Vec<usize>) -> Result<Self> {
        if fields.is_empty() {
            bail!(Schema, "require at least one column");
        }
        let mut names = HashSet::new();
        for field in &fields {
            validate_type(field.data_type())?;
            if field.name().is_empty()
                || field.name() == ROW_ID_COLUMN
                || !names.insert(field.name())
                || !field.metadata().is_empty()
            {
                bail!(
                    Schema,
                    "empty, duplicate, reserved column name, or unsupported field metadata"
                );
            }
        }
        if key_columns.is_empty()
            || key_columns.iter().any(|&k| k >= fields.len())
            || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
        {
            bail!(Schema, "keys must be a nonempty, unique list of column indexes");
        }
        Ok(Self {
            fields: Arc::new(Schema::new(fields)),
            key_columns,
        })
    }
    pub fn fields(&self) -> &arrow_schema::Fields {
        self.fields.fields()
    }
    pub fn arrow_schema(&self) -> SchemaRef {
        self.fields.clone()
    }
    pub fn physical_schema(&self) -> SchemaRef {
        let mut fields = vec![Arc::new(Field::new(ROW_ID_COLUMN, DataType::Int64, false))];
        fields.extend(self.fields.fields().iter().cloned());
        Arc::new(Schema::new(fields))
    }
    pub fn key_columns(&self) -> &[usize] {
        &self.key_columns
    }
    pub(crate) fn column_key_index(&self, col: usize) -> Option<usize> {
        self.key_columns.iter().position(|&k| k == col)
    }
    pub(crate) fn validate_value(&self, col: usize, value: &Scalar) -> Result<()> {
        let field =
            self.fields.fields().get(col).ok_or_else(|| beech_error!(Schema, "column out of bounds"))?;
        value.validate_type(field.data_type(), field.is_nullable())
    }
    pub(crate) fn validate_key(&self, key: &Key, prefix: bool) -> Result<()> {
        if key.is_empty()
            || key.len() > self.key_columns.len()
            || (!prefix && key.len() != self.key_columns.len())
        {
            bail!(Schema, "key arity mismatch");
        }
        for (value, &col) in key.iter().zip(&self.key_columns) {
            self.validate_value(col, value)?;
        }
        Ok(())
    }
    pub fn key_from_row(&self, row: &Row) -> Result<Key> {
        if row.1.len() != self.fields.fields().len() {
            bail!(Schema, "row arity mismatch");
        }
        for (i, value) in row.1.iter().enumerate() {
            self.validate_value(i, value)?;
        }
        Ok(self.key_columns.iter().map(|&i| row.1[i].clone()).collect())
    }
}
fn validate_type(typ: &DataType) -> Result<()> {
    match typ {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary => Ok(()),
        DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision) && *scale >= 0 && *scale as u8 <= *precision =>
        {
            Ok(())
        }
        _ => bail!(Schema, "unsupported type: {typ:?}"),
    }
}
