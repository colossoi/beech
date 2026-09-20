use anyhow::{bail, Context};
use beech_core::{DataType, Decimal, Field, Row, Scalar, TableSchema};
use beech_disk::{NamedTempFile, Workspace};
use std::{fs::File, path::Path};

/// A disk snapshot of the input. Inference keeps only per-column type candidates.
pub struct CsvInput {
    names: Vec<String>,
    fields: Vec<Field>,
    has_headers: bool,
    file: NamedTempFile,
    _workspace: Workspace,
}
impl CsvInput {
    pub fn read(path: &Path, has_headers: bool) -> anyhow::Result<Self> {
        let workspace = Workspace::new()?;
        let mut file = workspace.file()?;
        std::io::copy(&mut File::open(path)?, &mut file)?;
        let mut reader = csv::ReaderBuilder::new().has_headers(has_headers).from_path(file.path())?;
        let header = reader.headers()?;
        let names: Vec<_> = if has_headers {
            header.iter().map(str::to_owned).collect()
        } else {
            (0..header.len()).map(|i| format!("col_{i}")).collect()
        };
        let mut candidates = vec![[true; 4]; names.len()];
        let mut count = 0;
        for record in reader.records() {
            let record = record?;
            count += 1;
            for (value, types) in record.iter().zip(&mut candidates) {
                types[0] &= value.parse::<i64>().is_ok();
                types[1] &= value.parse::<u64>().is_ok();
                types[2] &= value.parse::<bool>().is_ok();
                let digits = value.strip_prefix('-').or_else(|| value.strip_prefix('+')).unwrap_or(value);
                let integer = !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit());
                types[3] &= value.parse::<f64>().is_ok()
                    && (!integer || value.parse::<i128>().is_ok_and(|i| i.unsigned_abs() <= (1u128 << 53)));
            }
        }
        if count == 0 {
            bail!("no records found in CSV");
        }
        let fields = names
            .iter()
            .zip(candidates)
            .map(|(name, types)| {
                let typ = if types[0] {
                    DataType::Int64
                } else if types[1] {
                    DataType::UInt64
                } else if types[2] {
                    DataType::Boolean
                } else if types[3] {
                    DataType::Float64
                } else {
                    DataType::Utf8
                };
                Field::new(name, typ, false)
            })
            .collect();
        Ok(Self {
            names,
            fields,
            has_headers,
            file,
            _workspace: workspace,
        })
    }
    pub fn infer_fields(&self) -> Vec<Field> {
        self.fields.clone()
    }
    pub fn rows(
        &self,
        schema: &TableSchema,
        first_id: i64,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Row>> + '_> {
        if self.names.len() != schema.fields().len()
            || self.names.iter().zip(schema.fields()).any(|(name, field)| name != field.name())
        {
            bail!("CSV columns must match the table schema in name and order");
        }
        let schema = schema.clone();
        let reader = csv::ReaderBuilder::new().has_headers(self.has_headers).from_path(self.file.path())?;
        Ok(reader.into_records().enumerate().map(move |(i, record)| {
            let record = record?;
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
        }))
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
