use super::accounting;
use crate::{Result, error::beech_error};
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    sync::{Arc, Mutex, OnceLock, Weak},
};

/// Cache activity and approximate memory accounting.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub entries: usize,
    /// Estimated bytes for retained entries. Active readers may retain evicted values.
    pub bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub evictions: u64,
}
struct Entry<V> {
    value: Arc<V>,
    weight: usize,
    stamp: u128,
}
struct Flight<V> {
    gate: Mutex<()>,
    value: OnceLock<Arc<V>>,
}
struct State<K, V> {
    entries: HashMap<K, Entry<V>>,
    order: BTreeMap<u128, K>,
    flights: HashMap<K, Weak<Flight<V>>>,
    clock: u128,
    stats: CacheStats,
}
pub(super) struct Cache<K, V> {
    budget: usize,
    estimate_size: fn(&V) -> usize,
    state: Mutex<State<K, V>>,
}
impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    pub(super) fn new(budget: usize, estimate_size: fn(&V) -> usize) -> Self {
        Self {
            budget,
            estimate_size,
            state: Mutex::new(State {
                entries: HashMap::new(),
                order: BTreeMap::new(),
                flights: HashMap::new(),
                clock: 0,
                stats: CacheStats::default(),
            }),
        }
    }
    pub(super) fn stats(&self) -> Result<CacheStats> {
        let state = self.state.lock().map_err(|_| beech_error!(Wire, "cache lock poisoned"))?;
        Ok(CacheStats {
            entries: state.entries.len(),
            ..state.stats
        })
    }
    pub(super) fn get_or_load(&self, key: K, load: impl FnOnce() -> Result<V>) -> Result<Arc<V>> {
        let flight = {
            let mut state = self.state.lock().map_err(|_| beech_error!(Wire, "cache lock poisoned"))?;
            state.clock += 1;
            let stamp = state.clock;
            if let Some(entry) = state.entries.get_mut(&key) {
                let old = entry.stamp;
                entry.stamp = stamp;
                let value = entry.value.clone();
                state.order.remove(&old);
                state.order.insert(stamp, key);
                state.stats.hits += 1;
                return Ok(value);
            }
            state.stats.misses += 1;
            let flight = state.flights.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let flight = Arc::new(Flight {
                    gate: Mutex::new(()),
                    value: OnceLock::new(),
                });
                state.flights.insert(key.clone(), Arc::downgrade(&flight));
                flight
            });
            Ticket {
                cache: self,
                key: key.clone(),
                flight,
            }
        };
        // Only the per-key gate is held during I/O. Unrelated loads and cache hits proceed.
        // Waiters reuse the result even if it is oversized or evicted before they wake.
        let _gate = flight.flight.gate.lock().map_err(|_| beech_error!(Wire, "cache load panicked"))?;
        if let Some(value) = flight.flight.value.get() {
            return Ok(value.clone());
        }
        {
            let mut state = self.state.lock().map_err(|_| beech_error!(Wire, "cache lock poisoned"))?;
            state.stats.loads += 1;
        }
        // Failed loads are not retained; a subsequent waiter may retry.
        let value = load()?;
        let weight = accounting::with_entry_overhead::<K, Entry<V>>((self.estimate_size)(&value));
        let value = Arc::new(value);
        if self.budget != 0 && weight <= self.budget {
            let mut state = self.state.lock().map_err(|_| beech_error!(Wire, "cache lock poisoned"))?;
            while state.stats.bytes > self.budget - weight {
                let (_, oldest) = state.order.pop_first().expect("nonempty weighted cache");
                let removed = state.entries.remove(&oldest).expect("LRU entry exists");
                state.stats.bytes -= removed.weight;
                state.stats.evictions += 1;
            }
            state.clock += 1;
            let stamp = state.clock;
            state.order.insert(stamp, key.clone());
            state.entries.insert(
                key,
                Entry {
                    value: value.clone(),
                    weight,
                    stamp,
                },
            );
            state.stats.bytes += weight;
        }
        let _ = flight.flight.value.set(value.clone());
        Ok(value)
    }
}
struct Ticket<'a, K: Eq + Hash, V> {
    cache: &'a Cache<K, V>,
    key: K,
    flight: Arc<Flight<V>>,
}
impl<K: Eq + Hash, V> Drop for Ticket<'_, K, V> {
    fn drop(&mut self) {
        // Clean coordination entries on success, error, or unwinding. Holding
        // the state lock prevents another caller from upgrading the weak pointer.
        if let Ok(mut state) = self.cache.state.lock()
            && Arc::strong_count(&self.flight) == 1
        {
            state.flights.remove(&self.key);
        }
    }
}
