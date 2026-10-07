use crate::{RedbScratchStore, ScratchStore, Workspace};
use std::collections::{BTreeSet, HashMap};
use std::io::{self, BufRead, Cursor, Write};

/// Typed temporary pages with an optional byte-budgeted decoded LRU.
/// Dirty entries reach the provider only on eviction or explicit flush.
/// Drop discards private state without flushing; errors poison the store.
pub struct PageStore<T> {
    store: Box<dyn ScratchStore>,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
    decode: fn(&mut dyn BufRead) -> io::Result<T>,
    failed: bool,
    stats: PageStats,
    cache: HashMap<u64, Cached<T>>,
    lru: BTreeSet<(u128, u64)>,
    clock: u128,
    budget: usize,
    size: Option<fn(&T) -> usize>,
}

struct Cached<T> {
    value: T,
    bytes: usize,
    dirty: bool,
    stamp: u128,
}

/// Logical encoded payload accounting, excluding database pages, provider cache,
/// filesystem overhead and active codec buffers. Not physical disk I/O.
#[derive(Clone, Copy, Debug, Default)]
pub struct PageStats {
    pub cached_bytes: u64,
    pub peak_cached_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub dirty_evictions: u64,
    pub disk_bytes: u64,
    pub peak_disk_bytes: u64,
    pub bytes_written: u64,
    #[cfg(test)]
    pub encode_calls: u64,
    #[cfg(test)]
    pub decode_calls: u64,
}
impl<T> PageStore<T> {
    pub fn new(
        workspace: &Workspace,
        encode: fn(&T, &mut dyn Write) -> io::Result<()>,
        decode: fn(&mut dyn BufRead) -> io::Result<T>,
    ) -> io::Result<Self> {
        Ok(Self::with_store(
            RedbScratchStore::new(workspace)?,
            encode,
            decode,
        ))
    }
    pub fn with_store(
        store: impl ScratchStore + 'static,
        encode: fn(&T, &mut dyn Write) -> io::Result<()>,
        decode: fn(&mut dyn BufRead) -> io::Result<T>,
    ) -> Self {
        Self {
            store: Box::new(store),
            encode,
            decode,
            failed: false,
            stats: PageStats::default(),
            cache: HashMap::new(),
            lru: BTreeSet::new(),
            clock: 0,
            budget: 0,
            size: None,
        }
    }
    pub fn stats(&self) -> PageStats {
        self.stats
    }
    fn check(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other("page store failed"))
        } else {
            Ok(())
        }
    }
    /// Enable decoded retention. The size callback must include owned allocations.
    /// Zero disables retention; oversized values go directly to the provider.
    /// Cache bookkeeping, caller clones and codec buffers are outside this budget.
    /// Configure before caching pages; panics if decoded entries already exist.
    pub fn with_cache(mut self, budget: usize, size: fn(&T) -> usize) -> Self {
        assert!(self.cache.is_empty(), "configure retention before caching pages");
        self.budget = budget;
        self.size = Some(size);
        self
    }
    fn take_cached(&mut self, id: u64) -> Option<Cached<T>> {
        let entry = self.cache.remove(&id)?;
        self.lru.remove(&(entry.stamp, id));
        self.stats.cached_bytes -= entry.bytes as u64;
        Some(entry)
    }
    fn insert_cached(&mut self, id: u64, value: T, bytes: usize, dirty: bool) {
        self.clock += 1;
        let stamp = self.clock;
        self.lru.insert((stamp, id));
        self.stats.cached_bytes += bytes as u64;
        self.stats.peak_cached_bytes = self.stats.peak_cached_bytes.max(self.stats.cached_bytes);
        self.cache.insert(
            id,
            Cached {
                value,
                bytes,
                dirty,
                stamp,
            },
        );
    }
    fn touch(&mut self, id: u64) {
        let entry = self.cache.get_mut(&id).unwrap();
        self.lru.remove(&(entry.stamp, id));
        self.clock += 1;
        entry.stamp = self.clock;
        self.lru.insert((entry.stamp, id));
    }
    fn write_disk(&mut self, id: u64, value: &T) -> io::Result<()> {
        let mut bytes = Vec::new();

        #[cfg(test)]
        {
            self.stats.encode_calls += 1;
        }
        (self.encode)(value, &mut bytes)?;

        let previous = self.store.put(&id.to_be_bytes(), &bytes)?.unwrap_or(0);

        self.stats.disk_bytes = self.stats.disk_bytes - previous + bytes.len() as u64;
        self.stats.peak_disk_bytes = self.stats.peak_disk_bytes.max(self.stats.disk_bytes);
        self.stats.bytes_written += bytes.len() as u64;
        Ok(())
    }
    fn make_room(&mut self, bytes: usize) -> io::Result<()> {
        while self.stats.cached_bytes as usize > self.budget - bytes {
            let (_, id) = *self.lru.first().unwrap();
            let entry = self.take_cached(id).unwrap();
            if entry.dirty {
                self.write_disk(id, &entry.value)?;
                self.stats.dirty_evictions += 1;
            }
        }
        Ok(())
    }
    pub fn write(&mut self, id: u64, value: T) -> io::Result<()> {
        self.check()?;
        let result = (|| {
            self.take_cached(id);
            let bytes = self.size.map(|size| size(&value).max(1));
            if let Some(bytes) = bytes.filter(|&bytes| bytes <= self.budget) {
                self.make_room(bytes)?;
                self.insert_cached(id, value, bytes, true);
                Ok(())
            } else {
                self.write_disk(id, &value)
            }
        })();
        self.failed = result.is_err();
        result
    }
    pub fn read<R>(&mut self, id: u64, read: impl FnOnce(&T) -> io::Result<R>) -> io::Result<R> {
        self.check()?;
        let result = (|| {
            if self.cache.contains_key(&id) {
                self.stats.cache_hits += 1;
                self.touch(id);
                return read(&self.cache[&id].value);
            }
            self.stats.cache_misses += 1;

            let bytes = self
                .store
                .get(&id.to_be_bytes())?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "temporary page missing"))?;

            #[cfg(test)]
            {
                self.stats.decode_calls += 1;
            }
            let value = (self.decode)(&mut Cursor::new(bytes))?;

            let size = self.size.map(|size| size(&value).max(1));
            if let Some(bytes) = size.filter(|&bytes| bytes <= self.budget) {
                self.make_room(bytes)?;
                self.insert_cached(id, value, bytes, false);
                read(&self.cache[&id].value)
            } else {
                read(&value)
            }
        })();
        self.failed = result.is_err();
        result
    }
    /// Persist the latest dirty values; useful when the caller needs a KV checkpoint.
    /// Immutable-object finalization may read the cache directly instead.
    pub fn flush(&mut self) -> io::Result<()> {
        self.check()?;
        let result = (|| {
            let ids: Vec<_> = self.lru.iter().map(|&(_, id)| id).collect();
            for id in ids {
                let mut entry = self.take_cached(id).unwrap();
                if entry.dirty {
                    self.write_disk(id, &entry.value)?;
                    entry.dirty = false;
                }
                self.stats.cached_bytes += entry.bytes as u64;
                self.lru.insert((entry.stamp, id));
                self.cache.insert(id, entry);
            }
            Ok(())
        })();
        self.failed = result.is_err();
        result
    }
    pub fn remove(&mut self, id: u64) -> io::Result<()> {
        self.check()?;
        self.take_cached(id);

        let result = self.store.delete(&id.to_be_bytes()).map(|previous| {
            self.stats.disk_bytes -= previous.unwrap_or(0);
        });

        self.failed = result.is_err();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store(workspace: &Workspace) -> PageStore<Vec<u8>> {
        PageStore::new(
            workspace,
            |v: &Vec<u8>, w| w.write_all(v),
            |r| {
                let mut v = Vec::new();
                r.read_to_end(&mut v)?;
                Ok(v)
            },
        )
        .unwrap()
    }
    #[test]
    fn dirty_lru_spills_latest_values_and_clean_evictions_do_not_rewrite() {
        let workspace = Workspace::new().unwrap();
        let mut pages = store(&workspace).with_cache(8, Vec::len);
        pages.write(1, vec![1; 4]).unwrap();
        pages.write(2, vec![2; 4]).unwrap();
        pages.write(1, vec![9; 4]).unwrap();
        pages.read(2, |_| Ok(())).unwrap(); // 1 is now oldest.
        pages.write(3, vec![3; 4]).unwrap();
        assert_eq!(pages.stats().encode_calls, 1);
        assert_eq!(pages.read(1, |v| Ok(v.clone())).unwrap(), vec![9; 4]);
        assert_eq!(pages.stats().dirty_evictions, 2);
        pages.flush().unwrap();
        let writes = pages.stats().encode_calls;
        pages.read(2, |_| Ok(())).unwrap(); // Clean eviction needs no put.
        assert_eq!(pages.stats().encode_calls, writes);
        pages.remove(1).unwrap(); // Delete both cached and spilled versions.
        assert_eq!(
            pages.read(1, |_| Ok(())).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(pages.stats().peak_cached_bytes <= 8);
    }
    #[test]
    fn oversized_replacement_flush_and_abort() {
        let workspace = Workspace::new().unwrap();
        let mut pages = store(&workspace).with_cache(4, Vec::len);
        pages.write(1, vec![1; 3]).unwrap();
        pages.write(1, vec![2; 5]).unwrap(); // Oversized replacement bypasses cache.
        assert_eq!(pages.stats().cached_bytes, 0);
        assert_eq!(pages.read(1, |v| Ok(v.clone())).unwrap(), vec![2; 5]);
        pages.write(1, vec![3; 2]).unwrap(); // Shadows old disk value.
        pages.flush().unwrap();
        pages.flush().unwrap();
        assert_eq!(pages.stats().encode_calls, 2);
        pages.write(2, vec![4; 4]).unwrap();
        assert_eq!(pages.read(1, |v| Ok(v.clone())).unwrap(), vec![3; 2]);
        pages.write(1, vec![5; 2]).unwrap();
        assert_eq!(pages.stats().encode_calls, 3);
        // Drop must discard this latest dirty version, without calling the codec.
        drop(pages);
    }
    #[test]
    fn failed_dirty_eviction_and_flush_poison_store() {
        for flush in [false, true] {
            let workspace = Workspace::new().unwrap();
            let mut pages = PageStore::new(
                &workspace,
                |_: &Vec<u8>, _| Err(io::Error::other("spill failed")),
                |_| Ok(Vec::new()),
            )
            .unwrap()
            .with_cache(4, Vec::len);
            pages.write(1, vec![1; 4]).unwrap();
            let result = if flush { pages.flush() } else { pages.write(2, vec![2; 4]) };
            assert!(result.is_err());
            assert!(pages.read(1, |_| Ok(())).is_err());
            assert!(pages.remove(1).is_err());
        }
    }
    #[test]
    fn replacements_deletion_isolation_and_cleanup() {
        let workspace = Workspace::new().unwrap();
        let path = workspace.path().to_path_buf();
        let mut a = store(&workspace);
        let mut b = store(&workspace);
        a.write(1, vec![1; 4]).unwrap();
        b.write(1, vec![9]).unwrap();
        a.write(1, vec![2; 5]).unwrap();
        assert_eq!(a.read(1, |v| Ok(v.clone())).unwrap(), vec![2; 5]);
        assert_eq!(b.read(1, |v| Ok(v.clone())).unwrap(), vec![9]);
        assert_eq!(a.stats().disk_bytes, 5);
        assert_eq!(a.stats().peak_disk_bytes, 5);
        assert_eq!(a.stats().bytes_written, 9);
        assert_eq!(a.stats().encode_calls, 2);
        assert_eq!(a.stats().decode_calls, 1);
        a.remove(1).unwrap();
        a.remove(1).unwrap();
        assert_eq!(a.stats().disk_bytes, 0);
        assert_eq!(a.read(1, |_| Ok(())).unwrap_err().kind(), io::ErrorKind::NotFound);
        assert!(a.write(2, vec![]).is_err());
        drop(workspace);
        assert!(path.exists());
        drop(a);
        drop(b);
        assert!(!path.exists());
    }
    #[test]
    fn encoding_and_decoding_failures_poison_store() {
        let workspace = Workspace::new().unwrap();
        let mut a = PageStore::new(
            &workspace,
            |_: &u64, w| {
                w.write_all(b"partial")?;
                Err(io::Error::other("encode failed"))
            },
            |_| Ok(0),
        )
        .unwrap();
        assert!(a.write(0, 1).is_err());
        assert_eq!(a.stats().bytes_written, 0);
        assert!(a.remove(0).is_err());
        let mut b = PageStore::new(
            &workspace,
            |_: &u64, w| w.write_all(&[0]),
            |_| Err::<u64, _>(io::Error::other("decode failed")),
        )
        .unwrap();
        b.write(0, 1).unwrap();
        assert!(b.read(0, |_| Ok(())).is_err());
        assert!(b.write(1, 2).is_err());
    }
    #[test]
    fn provider_failures_poison_store() {
        struct Broken;
        impl ScratchStore for Broken {
            fn get(&mut self, _: &[u8]) -> io::Result<Option<Vec<u8>>> {
                Err(io::Error::other("get"))
            }
            fn put(&mut self, _: &[u8], _: &[u8]) -> io::Result<Option<u64>> {
                Err(io::Error::other("put"))
            }
            fn delete(&mut self, _: &[u8]) -> io::Result<Option<u64>> {
                Err(io::Error::other("delete"))
            }
        }
        for operation in 0..3 {
            let mut a = PageStore::with_store(Broken, |_: &(), _| Ok(()), |_| Ok(()));
            let result = match operation {
                0 => a.write(0, ()),
                1 => a.read(0, |_| Ok(())),
                _ => a.remove(0),
            };
            assert!(result.is_err());
            assert!(a.write(1, ()).is_err());
            assert!(a.read(1, |_| Ok(())).is_err());
            assert!(a.remove(1).is_err());
        }
    }
}
