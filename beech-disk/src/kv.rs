use crate::Workspace;
use redb::{
    BackendError, Database, Durability, ReadableDatabase, StorageBackend, TableDefinition,
    backends::FileBackend,
};
use std::io;
use std::ops::Bound;

const VALUES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("scratch");

/// Single-owner temporary byte storage. Mutations are immediately readable.
/// No durability or reopen guarantee is provided. Returned bytes are owned;
/// provider guards and transaction lifetimes never escape this interface.
pub trait ScratchStore {
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    /// Replace a value and return the previous payload length, if present.
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<Option<u64>>;
    /// Delete a value and return its payload length, if present.
    fn delete(&mut self, key: &[u8]) -> io::Result<Option<u64>>;
}

/// Disposable redb provider. Each mutation ends its non-durable transaction,
/// avoiding an application-level pending-write buffer or decoded cache.
pub struct RedbScratchStore {
    // Close the database before releasing the last workspace owner.
    database: Database,
    _file: crate::NamedTempFile,
    _workspace: Workspace,
}
impl RedbScratchStore {
    pub fn new(workspace: &Workspace) -> io::Result<Self> {
        let file = workspace.file()?;
        let database = Database::builder()
            .create_with_backend(NoSyncBackend(
                FileBackend::new(file.reopen()?).map_err(io::Error::other)?,
            ))
            .map_err(io::Error::other)?;
        let mut txn = database.begin_write().map_err(io::Error::other)?;
        txn.set_durability(Durability::None).map_err(io::Error::other)?;
        txn.open_table(VALUES).map_err(io::Error::other)?;
        txn.commit().map_err(io::Error::other)?;
        Ok(Self {
            database,
            _workspace: workspace.clone(),
            _file: file,
        })
    }
}
impl ScratchStore for RedbScratchStore {
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let txn = self.database.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(VALUES).map_err(io::Error::other)?;
        Ok(table.get(key).map_err(io::Error::other)?.map(|v| v.value().to_vec()))
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<Option<u64>> {
        let mut txn = self.database.begin_write().map_err(io::Error::other)?;
        txn.set_durability(Durability::None).map_err(io::Error::other)?;
        let previous = {
            let mut table = txn.open_table(VALUES).map_err(io::Error::other)?;
            table.insert(key, value).map_err(io::Error::other)?.map(|v| v.value().len() as u64)
        };
        txn.commit().map_err(io::Error::other)?;
        Ok(previous)
    }
    fn delete(&mut self, key: &[u8]) -> io::Result<Option<u64>> {
        let mut txn = self.database.begin_write().map_err(io::Error::other)?;
        txn.set_durability(Durability::None).map_err(io::Error::other)?;
        let previous = {
            let mut table = txn.open_table(VALUES).map_err(io::Error::other)?;
            table.remove(key).map_err(io::Error::other)?.map(|v| v.value().len() as u64)
        };
        txn.commit().map_err(io::Error::other)?;
        Ok(previous)
    }
}

// Only for private disposable databases: redb also requests syncs at creation
// and shutdown. Forward I/O and locking, but never persist scratch durably.
#[derive(Debug)]
struct NoSyncBackend(FileBackend);
impl StorageBackend for NoSyncBackend {
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.write(offset, data)
    }
    fn close(&self) -> io::Result<()> {
        self.0.close()
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.0.try_lock_range(start, end)
    }
    fn try_lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.0.try_lock_shared_range(start, end)
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.lock_range(start, end)
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.lock_shared_range(start, end)
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.unlock_range(start, end)
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.0.query_lock_range(start, end)
    }
}
