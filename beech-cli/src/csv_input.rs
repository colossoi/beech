use anyhow::{bail, Context};
use beech_core::{DataType, Decimal, Field, Row, Scalar, TableSchema};
use std::path::Path;

pub struct CsvInput {
    names: Vec<String>,
    records: Vec<csv::StringRecord>,
}
impl CsvInput {
    pub fn read(path: &Path, has_headers: bool) -> anyhow::Result<Self> {
        let mut reader = csv::ReaderBuilder::new().has_headers(has_headers).from_path(path)?;
        let names =
            if has_headers { reader.headers()?.iter().map(str::to_owned).collect() } else { vec![] };
        let records = reader.records().collect::<Result<Vec<_>, _>>()?;
        let first = records.first().context("no records found in CSV")?;
        let names =
            if has_headers { names } else { (0..first.len()).map(|i| format!("col_{i}")).collect() };
        Ok(Self { names, records })
    }
    /// Infer one compatible type per entire column, falling back to text.
    pub fn infer_fields(&self) -> Vec<Field> {
        self.names
            .iter()
            .enumerate()
            .map(|(column, name)| {
                let values: Vec<_> = self.records.iter().map(|r| &r[column]).collect();
                let typ = if values.iter().all(|v| v.parse::<i64>().is_ok()) {
                    DataType::Int64
                } else if values.iter().all(|v| v.parse::<u64>().is_ok()) {
                    DataType::UInt64
                } else if values.iter().all(|v| v.parse::<bool>().is_ok()) {
                    DataType::Boolean
                } else if values.iter().all(|v| {
                    // Avoid rounding large integers when a column also contains floats.
                    let digits = v.strip_prefix('-').or_else(|| v.strip_prefix('+')).unwrap_or(v);
                    let integer = !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit());
                    v.parse::<f64>().is_ok()
                        && (!integer || v.parse::<i128>().is_ok_and(|i| i.unsigned_abs() <= (1u128 << 53)))
                }) {
                    DataType::Float64
                } else {
                    DataType::Utf8
                };
                Field::new(name, typ, false)
            })
            .collect()
    }
    pub fn rows(&self, schema: &TableSchema, first_id: i64) -> anyhow::Result<Vec<Row>> {
        if self.names.len() != schema.fields().len()
            || self.names.iter().zip(schema.fields()).any(|(name, field)| name != field.name())
        {
            bail!("CSV columns must match the table schema in name and order");
        }
        self.records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                let row_id = first_id.checked_add(i64::try_from(i)?).context("row ID overflow")?;
                let values = record
                    .iter()
                    .zip(schema.fields())
                    .map(|(value, field)| {
                        parse(value, field.data_type())
                            .with_context(|| format!("CSV row {}, column '{}'", i + 1, field.name()))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let row = (row_id, values);
                schema.key_from_row(&row)?;
                Ok(row)
            })
            .collect()
    }
}
fn parse(value: &str, typ: &DataType) -> anyhow::Result<Scalar> {
    Ok(match typ {
        DataType::Int32 => Scalar::Int32(value.parse()?),
        DataType::Int64 => Scalar::Int64(value.parse()?),
        DataType::UInt64 => Scalar::UInt64(value.parse()?),
        DataType::Float32 => Scalar::Float32(value.parse()?),
        DataType::Float64 => Scalar::Float64(value.parse()?),
        DataType::Boolean => Scalar::Boolean(value.parse()?),
        DataType::Utf8 => Scalar::Utf8(value.into()),
        DataType::Binary => Scalar::Binary(value.as_bytes().to_vec()),
        DataType::Decimal128(_, scale) => {
            let unsigned = value.strip_prefix('-').or_else(|| value.strip_prefix('+')).unwrap_or(value);
            let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
            if whole.is_empty()
                || !whole.bytes().all(|c| c.is_ascii_digit())
                || !fraction.bytes().all(|c| c.is_ascii_digit())
                || fraction.len() > *scale as usize
            {
                bail!("expected decimal with at most {scale} fractional digits");
            }
            let digits = format!(
                "{}{}{}{}",
                if value.starts_with('-') { "-" } else { "" },
                whole,
                fraction,
                "0".repeat(*scale as usize - fraction.len())
            );
            Scalar::Decimal(Decimal::new(digits.parse()?, *scale as u8)?)
        }
        other => bail!("CSV input does not support {other:?}"),
    })
}
