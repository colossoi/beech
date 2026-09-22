use crate::Workspace;
use beech_mem::lru::Lru;
use std::{
    fs,
    io::{self, BufRead, BufReader, BufWriter, Write},
};

struct Page<T> {
    value: T,
    disk_len: Option<u64>,
}

/// Decoded mutable scratch pages with a byte-bounded LRU cache.
/// Caller-supplied codecs run only on spill and reload; hits retain decoded values.
/// Uses `node-{id}` files in the supplied workspace; one store must exclusively
/// own that namespace. Dropping the workspace cleans up spilled pages.
///
/// The limit covers caller-estimated resident values (at least one byte per page),
/// not cache metadata or active caller/codec buffers. Zero disables caching.
/// Oversized pages go directly to disk. No scratch writes are durability-synced.
/// Any operation error poisons the store: discard it and abort the transaction.
pub struct PageStore<T> {
    workspace: Workspace,
    limit: usize,
    pages: Lru<u64, Page<T>>,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
    decode: fn(&mut dyn BufRead) -> io::Result<T>,
    memory_size: fn(&T) -> usize,
    failed: bool,
    stats: PageStats,
}

/// Estimated resident value and scratch-file accounting, excluding
/// filesystem allocation overhead. Spilled-page metadata stays on disk.
#[derive(Clone, Copy, Debug, Default)]
pub struct PageStats {
    pub resident_bytes: usize,
    pub peak_resident_bytes: usize,
    pub disk_bytes: u64,
    pub peak_disk_bytes: u64,
    pub bytes_written: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl<T> PageStore<T> {
    pub fn new(
        workspace: &Workspace,
        byte_limit: usize,
        encode: fn(&T, &mut dyn Write) -> io::Result<()>,
        decode: fn(&mut dyn BufRead) -> io::Result<T>,
        memory_size: fn(&T) -> usize,
    ) -> Self {
        Self {
            workspace: workspace.clone(),
            limit: byte_limit,
            encode,
            decode,
            memory_size,
            pages: Lru::new(),
            failed: false,
            stats: PageStats::default(),
        }
    }
    pub fn stats(&self) -> PageStats {
        self.stats
    }
    fn path(&self, id: u64) -> std::path::PathBuf {
        self.workspace.path().join(format!("node-{id}"))
    }
    fn check(&self) -> io::Result<()> {
        if self.failed { Err(io::Error::other("page store failed")) } else { Ok(()) }
    }
    fn spill(&mut self, id: u64, value: &T) -> io::Result<()> {
        let mut writer = BufWriter::new(fs::File::create(self.path(id))?);
        (self.encode)(value, &mut writer)?;
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        self.stats.disk_bytes += bytes;
        self.stats.peak_disk_bytes = self.stats.peak_disk_bytes.max(self.stats.disk_bytes);
        self.stats.bytes_written += bytes;
        Ok(())
    }
    fn admit(&mut self, id: u64, value: T, disk_len: Option<u64>) -> io::Result<()> {
        let charge = (self.memory_size)(&value).max(1);
        while charge > self.limit - self.stats.resident_bytes {
            let (victim, page) = self.pages.pop_lru().unwrap();
            self.stats.resident_bytes = self.pages.current_size();
            if page.disk_len.is_none() {
                self.spill(victim, &page.value)?;
            }
            self.stats.evictions += 1;
        }
        self.pages.insert(id, Page { value, disk_len }, charge);
        self.stats.resident_bytes = self.pages.current_size();
        self.stats.peak_resident_bytes = self.stats.peak_resident_bytes.max(self.stats.resident_bytes);
        Ok(())
    }
    /// Replace a decoded page and recompute its weight. Resident replacements
    /// require neither encoding nor disk I/O. Oversized values spill immediately.
    pub fn write(&mut self, id: u64, value: T) -> io::Result<()> {
        self.check()?;
        let result = (|| {
            self.remove_inner(id)?;
            if (self.memory_size)(&value).max(1) > self.limit {
                self.spill(id, &value)
            } else {
                self.admit(id, value, None)
            }
        })();
        self.failed = result.is_err();
        result
    }
    /// Read a decoded page. Cache hits do not invoke the decoder. Oversized
    /// reloads are decoded for this call but not retained in the cache.
    pub fn read<R>(&mut self, id: u64, read: impl FnOnce(&T) -> io::Result<R>) -> io::Result<R> {
        self.check()?;
        let result = (|| {
            if self.pages.contains(&id) {
                self.stats.hits += 1;
            } else {
                self.stats.misses += 1;
                let mut reader = BufReader::new(fs::File::open(self.path(id))?);
                let disk_len = reader.get_ref().metadata()?.len();
                let value = (self.decode)(&mut reader)?;
                if (self.memory_size)(&value).max(1) > self.limit {
                    return read(&value);
                }
                self.admit(id, value, Some(disk_len))?;
            }
            read(&self.pages.get(&id).unwrap().value)
        })();
        self.failed = result.is_err();
        result
    }
    fn remove_inner(&mut self, id: u64) -> io::Result<()> {
        if let Some(page) = self.pages.remove(&id) {
            self.stats.resident_bytes = self.pages.current_size();
            if let Some(bytes) = page.disk_len {
                fs::remove_file(self.path(id))?;
                self.stats.disk_bytes -= bytes;
            }
        } else {
            let path = self.path(id);
            match fs::metadata(&path) {
                Ok(meta) => {
                    fs::remove_file(path)?;
                    self.stats.disk_bytes -= meta.len();
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    /// Remove a page from memory and disk, if present.
    pub fn remove(&mut self, id: u64) -> io::Result<()> {
        self.check()?;
        let result = self.remove_inner(id);
        self.failed = result.is_err();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store(workspace: &Workspace, limit: usize) -> PageStore<Vec<u8>> {
        PageStore::new(
            workspace,
            limit,
            |value, writer| writer.write_all(value),
            |reader| {
                let mut value = Vec::new();
                reader.read_to_end(&mut value)?;
                Ok(value)
            },
            Vec::len,
        )
    }
    fn read(store: &mut PageStore<Vec<u8>>, id: u64) -> Vec<u8> {
        store.read(id, |value| Ok(value.clone())).unwrap()
    }
    #[test]
    fn reads_promote_and_dirty_eviction_preserves_bytes() {
        let workspace = Workspace::new().unwrap();
        let mut store = store(&workspace, 8);
        store.write(1, vec![1; 4]).unwrap();
        store.write(2, vec![2; 4]).unwrap();
        assert_eq!(read(&mut store, 1), vec![1; 4]);
        store.write(3, vec![3; 4]).unwrap();
        assert!(workspace.path().join("node-2").exists());
        assert!(!workspace.path().join("node-1").exists());
        assert_eq!(read(&mut store, 2), vec![2; 4]);
        assert_eq!(store.stats().resident_bytes, 8);
        assert_eq!(store.stats().bytes_written, 8);
        store.write(4, vec![4; 4]).unwrap(); // spills dirty 3, retains clean 2
        store.write(5, vec![5; 4]).unwrap(); // evicts clean 2 without rewriting it
        assert_eq!(store.stats().bytes_written, 12);
        store.write(2, vec![9; 4]).unwrap();
        assert_eq!(read(&mut store, 2), vec![9; 4]);
        store.remove(2).unwrap();
        assert!(!workspace.path().join("node-2").exists());
    }
    #[test]
    fn zero_and_oversized_pages_bypass_cache() {
        for limit in [0, 3] {
            let workspace = Workspace::new().unwrap();
            let mut store = store(&workspace, limit);
            store.write(1, vec![7; 4]).unwrap();
            assert_eq!(read(&mut store, 1), vec![7; 4]);
            assert_eq!(store.stats().resident_bytes, 0);
            assert_eq!(store.stats().disk_bytes, 4);
            store.write(1, vec![8; 5]).unwrap();
            assert_eq!(read(&mut store, 1), vec![8; 5]);
            assert_eq!(store.stats().disk_bytes, 5);
            store.remove(1).unwrap();
            assert_eq!(store.stats().disk_bytes, 0);
        }
    }
    #[test]
    fn spill_failure_poisons_store_and_drop_cleans_up() {
        let workspace = Workspace::new().unwrap();
        let path = workspace.path().to_path_buf();
        let mut store = store(&workspace, 4);
        store.write(1, vec![1; 4]).unwrap();
        fs::create_dir(path.join("node-1")).unwrap();
        assert!(store.write(2, vec![2; 4]).is_err());
        assert!(store.write(3, vec![]).is_err());
        assert!(store.read(1, |_| Ok(())).is_err());
        assert!(store.remove(1).is_err());
        drop(workspace);
        drop(store);
        assert!(!path.exists());
    }
    #[test]
    fn cached_replacements_do_not_write_files() {
        let workspace = Workspace::new().unwrap();
        let mut store = store(&workspace, 16);
        for n in 0..100 {
            store.write(1, vec![n; 8]).unwrap();
            assert_eq!(read(&mut store, 1), vec![n; 8]);
        }
        assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 0);
        assert_eq!(store.stats().bytes_written, 0);
        store.remove(1).unwrap();
        assert_eq!(store.stats().resident_bytes, 0);
    }
    #[test]
    fn codecs_run_only_on_spill_and_reload_and_use_decoded_weight() {
        use std::cell::Cell;
        thread_local! {
            static ENCODED: Cell<usize> = const { Cell::new(0) };
            static DECODED: Cell<usize> = const { Cell::new(0) };
        }
        let workspace = Workspace::new().unwrap();
        let mut store = PageStore::new(
            &workspace,
            16,
            |v: &Vec<u8>, w| {
                ENCODED.with(|n| n.set(n.get() + 1));
                w.write_all(v)
            },
            |r| {
                DECODED.with(|n| n.set(n.get() + 1));
                let mut v = Vec::new();
                r.read_to_end(&mut v)?;
                Ok(v)
            },
            |v| v.len() * 4,
        );
        for n in 0..20 {
            store.write(1, vec![n; 4]).unwrap();
            assert_eq!(read(&mut store, 1), vec![n; 4]);
        }
        assert_eq!(ENCODED.get(), 0);
        assert_eq!(DECODED.get(), 0);
        store.write(2, vec![2; 4]).unwrap();
        assert_eq!(ENCODED.get(), 1);
        assert_eq!(read(&mut store, 1), vec![19; 4]);
        assert_eq!(ENCODED.get(), 2);
        assert_eq!(DECODED.get(), 1);
        assert_eq!(store.stats().resident_bytes, 16);
        assert_eq!(read(&mut store, 1), vec![19; 4]);
        assert_eq!(DECODED.get(), 1);
        // A larger replacement must be reweighed and bypass the cache.
        store.write(1, vec![7; 5]).unwrap();
        assert_eq!(store.stats().resident_bytes, 0);
        assert_eq!(read(&mut store, 1), vec![7; 5]);
        assert_eq!(store.stats().resident_bytes, 0);
    }
    #[test]
    fn partial_encoding_failure_poisons_the_store() {
        let workspace = Workspace::new().unwrap();
        let mut store = PageStore::new(
            &workspace,
            0,
            |_: &u64, writer| {
                writer.write_all(b"partial")?;
                Err(io::Error::other("encode failed"))
            },
            |_| Ok(0),
            |_| 8,
        );
        assert!(store.write(0, 1).is_err());
        assert!(store.read(0, |_| Ok(())).is_err());
        assert!(store.write(1, 2).is_err());
    }
    #[test]
    fn zero_estimates_still_consume_budget() {
        let workspace = Workspace::new().unwrap();
        for limit in [0, 1] {
            let mut store = PageStore::new(
                &workspace,
                limit,
                |_: &(), w| w.write_all(&[0]),
                |r| {
                    let mut tag = [0];
                    r.read_exact(&mut tag)?;
                    Ok(())
                },
                |_| 0,
            );
            store.write(1, ()).unwrap();
            assert_eq!(store.stats().resident_bytes, limit);
            store.write(2, ()).unwrap();
            assert_eq!(store.stats().resident_bytes, limit);
            assert_eq!(store.stats().peak_resident_bytes, limit);
            assert_eq!(store.stats().evictions, limit as u64);
            store.read(1, |_| Ok(())).unwrap();
            assert_eq!(store.stats().resident_bytes, limit);
            store.remove(1).unwrap();
            store.remove(2).unwrap();
        }
    }
}
