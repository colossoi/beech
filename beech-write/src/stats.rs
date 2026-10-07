use std::time::Duration;

/// Costs of processing an ordered update transaction, before snapshot publication.
/// Visits count once per mutation per existing node on its search path, including
/// repeated visits and no-ops. They are not counts of distinct objects or physical
/// reads (the repository may cache reads). Finalization reads are excluded.
#[derive(Debug, Clone, Default)]
pub struct TransactionStats {
    pub operations: u64,
    pub no_op_updates: u64,
    pub leaf_visits: u64,
    pub branch_visits: u64,
    /// Complete temporary page replacements.
    pub leaf_writes: u64,
    pub branch_writes: u64,
    /// Additional nodes produced by local shaping, excluding new root levels.
    pub leaf_splits: u64,
    pub branch_splits: u64,
    pub root_collapses: u64,
    /// Final reachable tree objects sent to the sink, excluding snapshot metadata.
    pub leaves_staged: u64,
    pub branches_staged: u64,
    /// Bytes sent to the sink; sinks may deduplicate these objects.
    pub staged_bytes: u64,
    pub input_bytes: u64,
    /// Peak input spool plus live encoded working-node payload bytes.
    /// Excludes decoded retained nodes, database pages/cache, filesystem overhead, repository objects, and
    /// the publication staging directory. Measured incrementally, without scans.
    pub peak_scratch_bytes: u64,
    /// Cumulative encoded page bytes sent to the KV store, counting repeated versions.
    /// This is logical payload accounting, not physical disk I/O.
    pub scratch_bytes_written: u64,
    /// Retained decoded allocation estimate; excludes LRU bookkeeping and transient clones.
    pub peak_decoded_bytes: u64,
    /// Cache activity across mutation application and finalization.
    pub decoded_cache_hits: u64,
    pub decoded_cache_misses: u64,
    pub dirty_evictions: u64,
    /// Root height; None for an empty tree and Some(0) for a single leaf.
    pub final_height: Option<u32>,
    /// Apply and finalization time; excludes input spooling and publication.
    pub elapsed: Duration,
}
