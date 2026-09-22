#![allow(unused_imports)]
use super::*;
use crate::mmap::MappedBuffer;
use std::sync::Arc;

fn buf(len: usize) -> Arc<MappedBuffer> {
    MappedBuffer::anonymous(len, None).unwrap()
}

#[test]
fn evicts_oldest() {
    // Capacity in bytes is 200 — three 100-byte buffers force one eviction.
    let mut cache: Cache<i32> = Cache::new(200);
    cache.put(1, buf(100));
    cache.put(2, buf(100));
    assert!(cache.contains(&1));
    assert!(cache.contains(&2));
    cache.put(3, buf(100));
    assert!(!cache.contains(&1), "key 1 should have been evicted");
    assert!(cache.contains(&2));
    assert!(cache.contains(&3));
}

#[test]
fn generic_lru_promotes_replaces_and_returns_evictions() {
    let mut lru = Lru::new();
    lru.insert(1, "one", 3);
    lru.insert(2, "two", 3);
    assert_eq!(lru.get(&1), Some(&"one"));
    assert_eq!(lru.pop_lru(), Some((2, "two")));
    assert_eq!(lru.insert(1, "replacement", 11), Some("one"));
    assert_eq!(lru.current_size(), 11);
    assert_eq!(lru.remove(&1), Some("replacement"));
    assert_eq!(lru.current_size(), 0);
    assert_eq!(lru.pop_lru(), None);
}
