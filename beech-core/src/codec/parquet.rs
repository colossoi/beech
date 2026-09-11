//! One immutable Parquet file per prolly leaf; one row group in writer profile v1.
use super::{EncodedNode, FORMAT_VERSION, FormatTag, object_id, thrift::schema_id};
use crate::error::{bail, beech_error};
use crate::{
    storage::{ObjectFile, ObjectReader},
    value::ColumnStatistics,
    *,
};
use arrow_array::ArrayRef;
use parquet::file::{
    reader::{ChunkReader, Length},
    statistics::Statistics,
};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::Compression,
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties},
    },
};
const WRITER_PROFILE: &str = "beech-leaf-v1/arrow-rs-59.3.0/snappy";

pub fn encode_leaf(schema: &TableSchema, batch: &RecordBatch) -> Result<EncodedNode> {
    let max_key = value::validate_leaf(schema, batch)?;
    let count = batch.num_rows();
    // Parquet represents row counts as signed 64-bit integers.
    i64::try_from(count).map_err(|_| {
        beech_error!(
            InvalidNode,
            "leaf row count {count} exceeds Parquet i64 representation"
        )
    })?;
    let properties = WriterProperties::builder()
        .set_created_by(WRITER_PROFILE.into())
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(None)
        .set_max_row_group_bytes(None)
        .set_write_batch_size(1024)
        .set_data_page_row_count_limit(20_000)
        .set_data_page_size_limit(1024 * 1024)
        .set_key_value_metadata(Some(vec![
            KeyValue::new("beech.format".into(), Some(FORMAT_VERSION.to_string())),
            KeyValue::new("beech.schema".into(), Some(schema_id(schema)?.to_string())),
            KeyValue::new("beech.profile".into(), Some(WRITER_PROFILE.into())),
        ]))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.physical_schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;
    let reference = NodeRef {
        id: object_id(FormatTag::Leaf, &bytes),
        height: 0,
        row_count: count as u64,
        max_key,
    };
    Ok(EncodedNode {
        reference,
        bytes: bytes.into(),
    })
}

/// Decoded footer only. Backing handles and scan state are not retained here.
pub(crate) struct LeafMetadata {
    metadata: ArrowReaderMetadata,
    table_schema: TableSchema,
    statistics: Vec<ColumnStatistics>,
}
impl LeafMetadata {
    pub(crate) fn load(file: ObjectFile, reference: &NodeRef, schema: &TableSchema) -> Result<Self> {
        let mut result = Self {
            metadata: ArrowReaderMetadata::load(&ParquetFile(file), ArrowReaderOptions::default())?,
            table_schema: schema.clone(),
            statistics: vec![],
        };
        result.validate_format(reference, schema)?;
        let group = result.metadata.metadata().row_group(0);
        result.statistics = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column, field)| {
                let Some(stats) = group.column(column + 1).statistics() else {
                    return ColumnStatistics::default();
                };
                ColumnStatistics {
                    null_count: stats.null_count_opt(),
                    bounds: if stats.min_is_exact() && stats.max_is_exact() {
                        decode_bounds(stats, field.data_type())
                    } else {
                        None
                    },
                }
            })
            .collect();
        Ok(result)
    }
    pub(crate) fn schema(&self) -> &arrow_schema::SchemaRef {
        self.metadata.schema()
    }
    pub(crate) fn row_count(&self) -> u64 {
        self.metadata.metadata().file_metadata().num_rows() as u64
    }
    pub(crate) fn statistics(&self, column: usize) -> &ColumnStatistics {
        &self.statistics[column]
    }
    pub(crate) fn estimated_size(&self) -> usize {
        crate::storage::accounting::leaf_metadata_size(&self.metadata, &self.table_schema, &self.statistics)
    }
    pub(crate) fn validate(&self, reference: &NodeRef, schema: &TableSchema) -> Result<()> {
        reference.validate(schema)?;
        if reference.height != 0 {
            bail!(
                InvalidNode,
                "node {}: expected leaf reference with height 0, found height {}",
                reference.id,
                reference.height
            );
        }
        if &self.table_schema != schema {
            bail!(
                Schema,
                "leaf {}: cached schema {:?} does not match requested schema {schema:?}",
                reference.id,
                self.table_schema
            );
        }
        let fm = self.metadata.metadata().file_metadata();
        if fm.num_rows() < 0 || fm.num_rows() as u64 != reference.row_count {
            bail!(
                InvalidNode,
                "node {}: Parquet row count {} does not match reference row count {}",
                reference.id,
                fm.num_rows(),
                reference.row_count
            );
        }
        Ok(())
    }
    fn validate_format(&self, reference: &NodeRef, schema: &TableSchema) -> Result<()> {
        let groups = self.metadata.metadata().num_row_groups();
        if groups != 1 {
            bail!(
                InvalidNode,
                "leaf {}: expected exactly one Parquet row group, found {groups}",
                reference.id
            );
        }
        self.validate(reference, schema)?;
        let schema_id = schema_id(schema)?;
        let group_rows = self.metadata.metadata().row_group(0).num_rows();
        let file_rows = self.metadata.metadata().file_metadata().num_rows();
        if group_rows != file_rows {
            bail!(
                InvalidNode,
                "leaf {}: row group count {group_rows} does not match file row count {file_rows}",
                reference.id
            );
        }
        let actual = self.metadata.schema();
        if actual.fields() != schema.physical_schema().fields() {
            bail!(
                Schema,
                "leaf {}: Parquet fields {:?} do not match table schema {} fields {:?}",
                reference.id,
                actual.fields(),
                schema_id,
                schema.physical_schema().fields()
            );
        }
        let fm = self.metadata.metadata().file_metadata();
        let kv = fm
            .key_value_metadata()
            .ok_or_else(|| beech_error!(Wire, "leaf {}: missing Beech key-value metadata", reference.id))?;
        for (key, value) in [
            ("beech.format", FORMAT_VERSION.to_string()),
            ("beech.schema", schema_id.to_string()),
        ] {
            let values: Vec<_> = kv.iter().filter(|x| x.key == key).collect();
            if values.len() != 1 {
                bail!(
                    Wire,
                    "leaf {}: expected exactly one {key} metadata entry, found {}",
                    reference.id,
                    values.len()
                );
            }
            if values[0].value.as_deref() != Some(value.as_str()) {
                bail!(
                    Wire,
                    "leaf {}: {key} metadata is {:?}, expected {value:?}",
                    reference.id,
                    values[0].value
                );
            }
        }
        Ok(())
    }
}

/// Decode one complete leaf column. Caller batch sizes do not enter
/// the cache identity. Each column has its own Arrow buffers, shared by scans.
pub(crate) fn decode_column(file: ObjectFile, metadata: &LeafMetadata, column: usize) -> Result<ArrayRef> {
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(ParquetFile(file), metadata.metadata.clone());
    let mask = ProjectionMask::roots(builder.parquet_schema(), [column]);
    let reader = builder.with_projection(mask).with_batch_size(65_536).build()?;
    let mut arrays = Vec::new();
    for batch in reader {
        arrays.push(batch?.column(0).clone());
    }
    let array = match arrays.len() {
        0 => arrow_array::new_empty_array(metadata.metadata.schema().field(column).data_type()),
        1 => arrays.pop().expect("one decoded array"),
        _ => arrow_select::concat::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?,
    };
    let expected = metadata.metadata.metadata().file_metadata().num_rows();
    if array.len() as u64 != expected as u64 {
        bail!(
            InvalidNode,
            "column {column}: decoded {} rows, expected {expected}",
            array.len()
        );
    }
    Ok(array)
}

// Parquet decimal byte statistics are signed, big-endian two's-complement.
// Unsupported widths/values are uncertain statistics and must not prune rows.
fn decimal_stat(bytes: &[u8], scale: u8) -> Option<Scalar> {
    let first = *bytes.first()?;
    if bytes.len() > 16 {
        return None;
    }
    let mut unscaled = [if first & 0x80 == 0 { 0 } else { 0xff }; 16];
    unscaled[16 - bytes.len()..].copy_from_slice(bytes);
    Decimal::new(i128::from_be_bytes(unscaled), scale).ok().map(Scalar::Decimal)
}

fn decode_bounds(stats: &Statistics, typ: &DataType) -> Option<(Scalar, Scalar)> {
    match (stats, typ) {
        (Statistics::Boolean(s), DataType::Boolean) => {
            s.min_opt().zip(s.max_opt()).map(|(a, b)| (Scalar::Boolean(*a), Scalar::Boolean(*b)))
        }
        (Statistics::Int32(s), DataType::Int32) => {
            s.min_opt().zip(s.max_opt()).map(|(a, b)| (Scalar::Int32(*a), Scalar::Int32(*b)))
        }
        (Statistics::Int64(s), DataType::Int64) => {
            s.min_opt().zip(s.max_opt()).map(|(a, b)| (Scalar::Int64(*a), Scalar::Int64(*b)))
        }
        (Statistics::Int64(s), DataType::UInt64) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(a, b)| (Scalar::UInt64(*a as u64), Scalar::UInt64(*b as u64))),
        (Statistics::Int32(s), DataType::Decimal128(_, scale)) => {
            s.min_opt().zip(s.max_opt()).and_then(|(a, b)| {
                Some((
                    decimal_stat(&a.to_be_bytes(), *scale as u8)?,
                    decimal_stat(&b.to_be_bytes(), *scale as u8)?,
                ))
            })
        }
        (Statistics::Int64(s), DataType::Decimal128(_, scale)) => {
            s.min_opt().zip(s.max_opt()).and_then(|(a, b)| {
                Some((
                    decimal_stat(&a.to_be_bytes(), *scale as u8)?,
                    decimal_stat(&b.to_be_bytes(), *scale as u8)?,
                ))
            })
        }
        (Statistics::FixedLenByteArray(s), DataType::Decimal128(_, scale)) => {
            s.min_opt().zip(s.max_opt()).and_then(|(a, b)| {
                Some((
                    decimal_stat(a.data(), *scale as u8)?,
                    decimal_stat(b.data(), *scale as u8)?,
                ))
            })
        }
        (Statistics::ByteArray(s), DataType::Decimal128(_, scale)) => {
            s.min_opt().zip(s.max_opt()).and_then(|(a, b)| {
                Some((
                    decimal_stat(a.data(), *scale as u8)?,
                    decimal_stat(b.data(), *scale as u8)?,
                ))
            })
        }
        (Statistics::ByteArray(s), DataType::Utf8) => s.min_opt().zip(s.max_opt()).and_then(|(a, b)| {
            Some((
                Scalar::Utf8(std::str::from_utf8(a.data()).ok()?.into()),
                Scalar::Utf8(std::str::from_utf8(b.data()).ok()?.into()),
            ))
        }),
        (Statistics::ByteArray(s), DataType::Binary) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(a, b)| (Scalar::Binary(a.data().into()), Scalar::Binary(b.data().into()))),
        _ => None, // Float/NaN or unsupported statistics: always evaluate rows.
    }
}

#[derive(Clone)]
struct ParquetFile(ObjectFile);
impl Length for ParquetFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}
impl ChunkReader for ParquetFile {
    type T = ObjectReader;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(self.0.reader(start)?)
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        Ok(self.0.read_range(start, length)?)
    }
}
