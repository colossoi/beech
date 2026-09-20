use crate::{BuildOptions, Change, ObjectSink};
use beech_core::{
    codec::thrift::{decode_key, encode_key},
    BeechError, Id, Key, KeyOrdering, NodeRef, NodeSource, Result, Row, Scalar, Table, TableSchema,
};
use beech_disk::{ExternalSort, SortLimits, SortedRuns, Spool, Workspace};
use std::{
    cmp::Ordering,
    fs::File,
    io::{self, BufRead, BufReader, Read, Write},
};

type ChangeOrder = fn(&Change, &Change) -> Ordering;
pub(crate) type SortedChanges = Box<dyn Iterator<Item = io::Result<Change>>>;

/// Incoming changes are validated and spooled immediately. Sorting and tree
/// editing read this spool; transaction-sized row collections are never needed.
pub struct Transaction {
    pub(crate) workspace: Workspace,
    schema: TableSchema,
    input: Spool,
    sort_limits: SortLimits,
    max_row_id: Option<i64>,
}
impl Transaction {
    pub fn new(schema: TableSchema, sort_limits: SortLimits) -> Result<Self> {
        let workspace = Workspace::new()?;
        Ok(Self {
            input: Spool::new(&workspace)?,
            workspace,
            schema,
            sort_limits,
            max_row_id: None,
        })
    }
    pub fn push(&mut self, change: Change) -> Result<()> {
        self.schema.validate_key(change.key(), false)?;
        if let Change::Insert { key, row_id, record } | Change::Update { key, row_id, record } = &change {
            if self.schema.key_from_row(&(*row_id, record.clone()))?.compare_key(key)? != Ordering::Equal {
                return Err(BeechError::Query("change key does not match row".into()));
            }
            self.max_row_id = Some(self.max_row_id.map_or(*row_id, |old| old.max(*row_id)));
        }
        self.input.append(|writer| change.write(writer))?;
        Ok(())
    }
    fn sorted(&mut self) -> Result<SortedRuns<Change, ChangeOrder>> {
        let compare: ChangeOrder = |a, b| a.key().compare_key(b.key()).expect("schema-validated keys");
        let mut sort = ExternalSort::new(
            &self.workspace,
            self.sort_limits,
            compare,
            Change::write,
            Change::read,
            Change::memory_size,
        );
        for change in records(self.input.reader()?, Change::read) {
            sort.push(change?)?;
        }
        let mut sorted = sort.finish()?;
        let mut previous: Option<Key> = None;
        for change in sorted.reader()? {
            let change = change?;
            if previous
                .as_ref()
                .is_some_and(|key| key.compare_key(change.key()).unwrap() == Ordering::Equal)
            {
                return Err(BeechError::Query("duplicate change key".into()));
            }
            previous = Some(change.key().clone());
        }
        Ok(sorted)
    }
    pub fn apply(
        mut self,
        table: &Table,
        source: &impl NodeSource,
        sink: &mut impl ObjectSink,
        options: BuildOptions,
    ) -> Result<Table> {
        if table.schema() != &self.schema {
            return Err(BeechError::Schema(
                "transaction schema does not match table".into(),
            ));
        }
        let mut changes = self.sorted()?;
        if changes.is_empty() {
            return Ok(table.clone());
        }
        let mut updated = table.clone();
        if let Some(id) = self.max_row_id {
            updated = updated.with_max_row_id(id);
        }
        crate::update::apply_sorted(
            Box::new(changes.reader()?),
            &self.workspace,
            &updated,
            source,
            sink,
            options,
        )
    }
    pub fn build(
        mut self,
        name: String,
        sink: &mut impl ObjectSink,
        options: BuildOptions,
    ) -> Result<Table> {
        let table = Table::new(name, self.schema.clone(), None, self.max_row_id.unwrap_or(-1))?;
        let mut changes = self.sorted()?;
        let rows = changes.reader()?.map(|change| match change? {
            Change::Insert { row_id, record, .. } => Ok((row_id, record)),
            _ => Err(BeechError::Query("new tables require insert changes".into())),
        });
        let level = crate::tree::build_leaves(sink, &self.schema, rows, options, &self.workspace)?;
        table.with_root(crate::tree::finish_tree(
            sink,
            &self.schema,
            level,
            options,
            &self.workspace,
        )?)
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid transaction record")
}
fn scalar_bytes(values: &Key) -> usize {
    values.capacity() * std::mem::size_of::<Scalar>()
        + values
            .iter()
            .map(|v| match v {
                Scalar::Utf8(s) => s.capacity(),
                Scalar::Binary(b) => b.capacity(),
                _ => 0,
            })
            .sum::<usize>()
}

impl Change {
    pub(crate) fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_frame(writer, &self.encode()?)
    }
    pub(crate) fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
        read_frame(reader)?.map(|bytes| Self::decode(&bytes)).transpose()
    }
    fn encode(&self) -> io::Result<Vec<u8>> {
        let (tag, id, row) = match self {
            Self::Insert { row_id, record, .. } => (0, *row_id, record.as_slice()),
            Self::Update { row_id, record, .. } => (1, *row_id, record.as_slice()),
            Self::Delete { .. } => (2, 0, &[][..]),
        };
        let mut values = vec![
            Scalar::Int64(tag),
            Scalar::Int64(id),
            Scalar::Int64(self.key().len() as i64),
        ];
        values.extend(self.key().iter().cloned());
        values.extend(row.iter().cloned());
        encode_key(&values).map_err(io::Error::other)
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut values = decode_key(bytes).map_err(io::Error::other)?.into_iter();
        let (Some(Scalar::Int64(tag)), Some(Scalar::Int64(row_id)), Some(Scalar::Int64(n))) =
            (values.next(), values.next(), values.next())
        else {
            return Err(invalid());
        };
        let n = usize::try_from(n).map_err(|_| invalid())?;
        if n > values.len() {
            return Err(invalid());
        }
        let key = values.by_ref().take(n).collect();
        let record: Vec<_> = values.collect();
        match tag {
            0 => Ok(Self::Insert { key, row_id, record }),
            1 => Ok(Self::Update { key, row_id, record }),
            2 if record.is_empty() => Ok(Self::Delete { key }),
            _ => Err(invalid()),
        }
    }
    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + scalar_bytes(self.key())
            + match self {
                Self::Insert { record, .. } | Self::Update { record, .. } => scalar_bytes(record),
                _ => 0,
            }
    }
}
pub(crate) struct RowRecord(pub Row);
impl RowRecord {
    pub(crate) fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_frame(writer, &self.encode()?)
    }
    pub(crate) fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
        read_frame(reader)?.map(|bytes| Self::decode(&bytes)).transpose()
    }
    fn encode(&self) -> io::Result<Vec<u8>> {
        beech_core::codec::thrift::encode_row_for_splitting(&self.0).map_err(io::Error::other)
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut values = decode_key(bytes).map_err(io::Error::other)?.into_iter();
        let Some(Scalar::Int64(id)) = values.next() else {
            return Err(invalid());
        };
        Ok(Self((id, values.collect())))
    }
}
/// Node references are spooled too: a large append must not accumulate a whole level.
pub(crate) struct RefRecord {
    id: Id,
    height: u32,
    count: u64,
    key: Key,
}
impl RefRecord {
    pub fn new(node: &NodeRef) -> Self {
        Self {
            id: node.id(),
            height: node.height(),
            count: node.row_count(),
            key: node.max_key().clone(),
        }
    }
    pub fn node(self, schema: &TableSchema) -> Result<NodeRef> {
        NodeRef::new(schema, self.id, self.height, self.count, self.key)
    }
}
impl RefRecord {
    pub(crate) fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_frame(writer, &self.encode()?)
    }
    pub(crate) fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
        read_frame(reader)?.map(|bytes| Self::decode(&bytes)).transpose()
    }
    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut values = vec![
            Scalar::Binary(self.id.as_bytes().to_vec()),
            Scalar::Int64(self.height as i64),
            Scalar::UInt64(self.count),
        ];
        values.extend(self.key.iter().cloned());
        encode_key(&values).map_err(io::Error::other)
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut values = decode_key(bytes).map_err(io::Error::other)?.into_iter();
        let (Some(Scalar::Binary(id)), Some(Scalar::Int64(height)), Some(Scalar::UInt64(count))) =
            (values.next(), values.next(), values.next())
        else {
            return Err(invalid());
        };
        Ok(Self {
            id: Id::from_slice(&id).map_err(io::Error::other)?,
            height: height.try_into().map_err(|_| invalid())?,
            count,
            key: values.collect(),
        })
    }
}

// Transaction framing belongs to the writer's codec, not the disk crate.
fn write_frame(writer: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
    writer.write_all(bytes)
}
fn read_frame(reader: &mut dyn BufRead) -> io::Result<Option<Vec<u8>>> {
    if reader.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let mut header = [0; 8];
    reader.read_exact(&mut header)?;
    let length = u64::from_le_bytes(header);
    let mut bytes = Vec::new();
    // Grow from bytes actually read, never allocate from an unchecked length.
    reader.take(length).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated transaction record",
        ));
    }
    Ok(Some(bytes))
}
pub(crate) fn records<T>(
    mut reader: BufReader<File>,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
) -> impl Iterator<Item = io::Result<T>> {
    let mut ended = false;
    std::iter::from_fn(move || {
        if ended {
            return None;
        }
        match decode(&mut reader) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => {
                ended = true;
                None
            }
            Err(error) => {
                ended = true;
                Some(Err(error))
            }
        }
    })
}

#[cfg(test)]
mod framing_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frames_preserve_boundaries_and_empty_values() {
        let mut bytes = Vec::new();
        for value in [b"abc".as_slice(), b"", b"defg"] {
            write_frame(&mut bytes, value).unwrap();
        }
        let mut reader = BufReader::with_capacity(2, Cursor::new(bytes));
        for value in [b"abc".as_slice(), b"", b"defg"] {
            assert_eq!(read_frame(&mut reader).unwrap().as_deref(), Some(value));
        }
        assert!(read_frame(&mut reader).unwrap().is_none());
    }

    #[test]
    fn truncated_frames_fail_without_allocating_the_declared_length() {
        let mut huge = u64::MAX.to_le_bytes().to_vec();
        huge.push(1);
        for bytes in [vec![1, 2, 3], huge] {
            let error = read_frame(&mut Cursor::new(bytes)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
    }
}
