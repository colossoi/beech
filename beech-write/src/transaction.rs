use crate::{BuildOptions, Change, ObjectSink, TransactionStats};
use beech_core::{
    codec::thrift::{decode_key, encode_key},
    BeechError, Id, Key, KeyOrdering, NodeRef, NodeSource, Result, Row, Scalar, Table, TableSchema,
};
use beech_disk::{ExternalSort, SortLimits, SortedRuns, Spool, Workspace};
use std::{
    cmp::Ordering,
    io::{self, BufRead, Read, Write},
};

type ChangeOrder = fn(&Change, &Change) -> Ordering;

/// Incoming changes are validated and spooled immediately. Updates process the
/// spool in submission order; bulk creation sorts it. A failed push discards the
/// workspace and makes this transaction unusable.
pub struct Transaction {
    schema: TableSchema,
    // None means a failed push has discarded the transaction scratch files.
    scratch: Option<(Spool, Workspace)>,
    sort_limits: SortLimits,
    max_row_id: Option<i64>,
    page_cache_bytes: usize,
}
impl Transaction {
    /// Sort limits apply to bulk creation only.
    pub fn new(schema: TableSchema, sort_limits: SortLimits) -> Result<Self> {
        let workspace = Workspace::new()?;
        Ok(Self {
            scratch: Some((Spool::new(&workspace)?, workspace)),
            schema,
            sort_limits,
            max_row_id: None,
            page_cache_bytes: 8 * 1024 * 1024,
        })
    }
    /// Set the mutable-page cache byte limit (default 8 MiB). Zero forces
    /// disk-only updates. Does not affect the input spool or bulk creation.
    pub fn with_page_cache_bytes(mut self, bytes: usize) -> Self {
        self.page_cache_bytes = bytes;
        self
    }
    pub fn push(&mut self, change: Change) -> Result<()> {
        let result = self.push_inner(change);
        if result.is_err() {
            self.scratch = None;
        }
        result
    }
    fn push_inner(&mut self, change: Change) -> Result<()> {
        let (input, _) = self.scratch.as_mut().ok_or_else(failed)?;
        self.schema.validate_key(change.key(), false)?;
        if let Change::Insert { key, row_id, record } | Change::Update { key, row_id, record } = &change {
            if self.schema.key_from_row(&(*row_id, record.clone()))?.compare_key(key)? != Ordering::Equal {
                return Err(BeechError::Query("change key does not match row".into()));
            }
            self.max_row_id = Some(self.max_row_id.map_or(*row_id, |old| old.max(*row_id)));
        }
        input.append(|writer| change.write(writer))?;
        Ok(())
    }
    fn sorted(&mut self) -> Result<SortedRuns<Change, ChangeOrder>> {
        let (input, workspace) = self.scratch.as_mut().ok_or_else(failed)?;
        let compare: ChangeOrder = |a, b| a.key().compare_key(b.key()).expect("schema-validated keys");
        let mut sort = ExternalSort::new(
            workspace,
            self.sort_limits,
            compare,
            Change::write,
            Change::read,
            Change::memory_size,
        );
        for change in records(input.reader()?, Change::read) {
            let change = change?;
            if !matches!(change, Change::Insert { .. }) {
                return Err(BeechError::Query("new tables require insert changes".into()));
            }
            sort.push(change)?;
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
    /// Apply ordered mutations privately, then stage only final reachable nodes.
    /// On error, discard the sink/writer; finalization may have staged objects.
    pub fn apply(
        self,
        table: &Table,
        source: &impl NodeSource,
        sink: &mut impl ObjectSink,
        options: BuildOptions,
    ) -> Result<Table> {
        self.apply_with_stats(table, source, sink, options).map(|(table, _)| table)
    }
    /// Apply ordered mutations and return their measured costs. See
    /// [`TransactionStats`] for accounting scope; publication is measured separately.
    pub fn apply_with_stats(
        mut self,
        table: &Table,
        source: &impl NodeSource,
        sink: &mut impl ObjectSink,
        options: BuildOptions,
    ) -> Result<(Table, TransactionStats)> {
        if table.schema() != &self.schema {
            return Err(BeechError::Schema(
                "transaction schema does not match table".into(),
            ));
        }
        let (mut input, workspace) = self.scratch.take().ok_or_else(failed)?;
        let mut updated = table.clone();
        if let Some(id) = self.max_row_id {
            updated = updated.with_max_row_id(id);
        }
        let reader = input.reader()?;
        let input_bytes = reader.get_ref().metadata()?.len();
        crate::update::apply_ordered(
            records(reader, Change::read),
            crate::update::WorkingNode::pages(&workspace, self.page_cache_bytes),
            &updated,
            input_bytes,
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
        let workspace = &self.scratch.as_ref().ok_or_else(failed)?.1;
        let level = crate::tree::build_leaves(sink, &self.schema, rows, options, workspace)?;
        table.with_root(crate::tree::finish_tree(
            sink,
            &self.schema,
            level,
            options,
            workspace,
        )?)
    }
}
fn failed() -> BeechError {
    BeechError::Query("transaction has failed".into())
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
pub(crate) fn write_frame(writer: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
    writer.write_all(bytes)
}
pub(crate) fn read_frame(reader: &mut dyn BufRead) -> io::Result<Option<Vec<u8>>> {
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
    mut reader: impl BufRead,
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
        let mut reader = std::io::BufReader::with_capacity(2, Cursor::new(bytes));
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

#[cfg(test)]
mod failure_tests {
    use super::*;
    use beech_core::{
        storage::{FileStore, Repository},
        DataType, Field,
    };
    struct NoWrites;
    impl ObjectSink for NoWrites {
        fn put(&mut self, _: Id, _: &[u8]) -> io::Result<()> {
            panic!("failed transaction must not stage objects")
        }
    }
    fn schema() -> TableSchema {
        TableSchema::new(vec![Field::new("k", DataType::Int64, false)], vec![0]).unwrap()
    }
    fn insert(key: i64) -> Change {
        Change::Insert {
            key: vec![Scalar::Int64(key)],
            row_id: key,
            record: vec![Scalar::Int64(key)],
        }
    }
    #[test]
    fn failed_push_discards_scratch_and_rejects_further_use() {
        for build in [false, true] {
            let mut tx = Transaction::new(schema(), SortLimits::default()).unwrap();
            let path = tx.scratch.as_ref().unwrap().1.path().to_path_buf();
            tx.push(insert(1)).unwrap();
            assert!(tx
                .push(Change::Delete {
                    key: vec![Scalar::Utf8("bad".into())]
                })
                .is_err());
            assert!(!path.exists());
            assert!(tx.push(insert(2)).is_err());
            if build {
                assert!(tx.build("t".into(), &mut NoWrites, BuildOptions::default()).is_err());
            } else {
                let source = Repository::new(FileStore::new(&path));
                let table = Table::new("t", schema(), None, -1).unwrap();
                assert!(tx.apply(&table, &source, &mut NoWrites, BuildOptions::default()).is_err());
            }
        }
    }
    #[test]
    fn workspace_write_failure_aborts_before_staging_and_cleans_up() {
        let mut tx = Transaction::new(schema(), SortLimits::default()).unwrap().with_page_cache_bytes(0);
        tx.push(insert(1)).unwrap();
        let path = tx.scratch.as_ref().unwrap().1.path().to_path_buf();
        // Force the first mutable node write to fail, without relying on permissions.
        std::fs::create_dir(path.join("node-0")).unwrap();
        let source = Repository::new(FileStore::new(&path));
        let table = Table::new("t", schema(), None, -1).unwrap();
        assert!(tx.apply(&table, &source, &mut NoWrites, BuildOptions::default()).is_err());
        assert!(!path.exists());
    }
    #[test]
    fn staging_failure_discards_the_working_directory() {
        struct Fail;
        impl ObjectSink for Fail {
            fn put(&mut self, _: Id, _: &[u8]) -> io::Result<()> {
                Err(io::Error::other("injected staging failure"))
            }
        }
        let mut tx = Transaction::new(schema(), SortLimits::default()).unwrap();
        tx.push(insert(1)).unwrap();
        let path = tx.scratch.as_ref().unwrap().1.path().to_path_buf();
        let source = Repository::new(FileStore::new(&path));
        let table = Table::new("t", schema(), None, -1).unwrap();
        assert!(tx.apply(&table, &source, &mut Fail, BuildOptions::default()).is_err());
        assert!(!path.exists());
    }
}
