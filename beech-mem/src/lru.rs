#[cfg(unix)]
use crate::mmap::MappedBuffer;
#[cfg(unix)]
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

struct Entry<V> {
    value: V,
    weight: usize,
    stamp: u128,
}

/// Weighted LRU storage. Admission and eviction policy belong to the caller;
/// `pop_lru` returns ownership so dirty values can be persisted before dropping.
pub struct Lru<K, V> {
    entries: HashMap<K, Entry<V>>,
    order: BTreeMap<u128, K>,
    clock: u128,
    size: usize,
}
impl<K: Eq + Hash + Clone, V> Default for Lru<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
impl<K: Eq + Hash + Clone, V> Lru<K, V> {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            clock: 0,
            size: 0,
        }
    }
    pub fn current_size(&self) -> usize {
        self.size
    }
    pub fn contains(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }
    pub fn peek(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|e| &e.value)
    }
    pub fn get(&mut self, key: &K) -> Option<&V> {
        let entry = self.entries.get_mut(key)?;
        self.order.remove(&entry.stamp);
        self.clock += 1;
        entry.stamp = self.clock;
        self.order.insert(entry.stamp, key.clone());
        Some(&entry.value)
    }
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.order.remove(&entry.stamp);
        self.size -= entry.weight;
        Some(entry.value)
    }
    pub fn insert(&mut self, key: K, value: V, weight: usize) -> Option<V> {
        let old = self.remove(&key);
        self.clock += 1;
        self.order.insert(self.clock, key.clone());
        self.entries.insert(
            key,
            Entry {
                value,
                weight,
                stamp: self.clock,
            },
        );
        self.size += weight;
        old
    }
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        let (_, key) = self.order.pop_first()?;
        let entry = self.entries.remove(&key).unwrap();
        self.size -= entry.weight;
        Some((key, entry.value))
    }
}

#[cfg(unix)]
pub struct Cache<K> {
    entries: Lru<K, Arc<MappedBuffer>>,
    max_size: usize,
}
#[cfg(unix)]
impl<K: Hash + Eq + Clone> Cache<K> {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: Lru::new(),
            max_size,
        }
    }
    pub fn get(&mut self, key: &K) -> Option<Arc<MappedBuffer>> {
        self.entries.get(key).cloned()
    }
    pub fn peek(&self, key: &K) -> Option<&Arc<MappedBuffer>> {
        self.entries.peek(key)
    }
    pub fn contains(&self, key: &K) -> bool {
        self.entries.contains(key)
    }
    pub fn put(&mut self, key: K, value: Arc<MappedBuffer>) {
        let weight = value.len();
        self.entries.insert(key, value, weight);
        while self.entries.current_size() > self.max_size {
            self.entries.pop_lru();
        }
    }
    pub fn current_size(&self) -> usize {
        self.entries.current_size()
    }
}
#[cfg(all(test, unix))]
#[path = "lru_tests.rs"]
mod tests;
