//! Transactional in-memory storage for writer and reader integration tests.
use beech_core::{
    storage::{BackingStore, ObjectFile, Repository},
    BeechError, Id, Result, Row, Table, TableSchema,
};
use beech_write::{BuildOptions, ObjectSink, Writer};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;
#[derive(Clone, Default)]
pub struct MemoryStore {
    files: Files,
}
impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn writer(&self) -> MemoryWriter {
        MemoryWriter {
            files: self.files.clone(),
            pending: HashMap::new(),
        }
    }
    pub fn node_source(&self) -> Repository {
        Repository::with_options(
            self.clone(),
            beech_core::storage::RepositoryOptions {
                verify_leaves: true,
                ..Default::default()
            },
        )
    }
    pub fn file_count(&self) -> usize {
        self.files.lock().unwrap().len()
    }
    pub fn get(&self, name: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(name).cloned()
    }
    /// Raw mutation for corruption tests only.
    pub fn insert(&self, name: impl Into<String>, bytes: Vec<u8>) {
        self.files.lock().unwrap().insert(name.into(), bytes);
    }
    pub fn remove(&self, name: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().remove(name)
    }
}
impl BackingStore for MemoryStore {
    fn get(&self, id: &Id) -> Result<ObjectFile> {
        self.get(&id.to_string()).map(|b| ObjectFile::from_bytes(b.into())).ok_or(BeechError::NotFound(*id))
    }
}
pub struct MemoryWriter {
    files: Files,
    pending: HashMap<String, Vec<u8>>,
}
impl ObjectSink for MemoryWriter {
    fn put(&mut self, id: Id, bytes: &[u8]) -> std::io::Result<()> {
        let name = id.to_string();
        if let Some(old) =
            self.pending.get(&name).cloned().or_else(|| self.files.lock().unwrap().get(&name).cloned())
        {
            if old != bytes {
                return Err(std::io::Error::other("cannot replace immutable object"));
            }
        }
        self.pending.insert(name, bytes.to_vec());
        Ok(())
    }
}
impl Writer for MemoryWriter {
    fn stage_root(&mut self, root_id: Id) -> std::io::Result<()> {
        let name = root_id.to_string();
        if !self.pending.contains_key(&name) && !self.files.lock().unwrap().contains_key(&name) {
            return Err(std::io::Error::other("root object is not available"));
        }
        self.pending.insert("root".into(), name.into_bytes());
        Ok(())
    }
    fn commit(self) -> std::io::Result<()> {
        let mut files = self.files.lock().unwrap();
        for (name, bytes) in &self.pending {
            if name != "root" && files.get(name).is_some_and(|old| old != bytes) {
                return Err(std::io::Error::other("cannot replace immutable object"));
            }
        }
        files.extend(self.pending);
        Ok(())
    }
    fn abort(self) -> std::io::Result<()> {
        Ok(())
    }
    fn num_to_commit(&self) -> usize {
        self.pending.len()
    }
}

pub fn build_simple_table(
    name: &str,
    rows: Vec<Row>,
    schema: TableSchema,
    target: usize,
    stddev: usize,
) -> Result<(MemoryStore, Repository, Arc<Table>)> {
    let store = MemoryStore::new();
    let mut writer = store.writer();
    let table = beech_write::build_table(
        &mut writer,
        name.into(),
        schema,
        rows,
        BuildOptions::new(target, stddev)?,
    )?;
    let publication = beech_write::publish_table(&mut writer, &table, Default::default(), None)?;
    writer.commit()?;
    let source = store.node_source();
    let table = source.table_by_id(&publication.table_id)?;
    // Also exercise the published root and transaction chain.
    let root = source.get_root(&publication.root_id)?;
    assert_eq!(root.transaction_id(), publication.transaction_id);
    let transaction = source.get_transaction(&root.transaction_id())?;
    source.get_table(&transaction, name)?;
    Ok((store, source, table))
}
