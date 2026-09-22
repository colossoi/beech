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
    /// Complete page replacements, including cached rewrites.
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
    /// Peak sum of file lengths in the private workspace: input spool and working
    /// nodes. Excludes filesystem allocation overhead, repository objects, and
    /// the publication staging directory. Measured incrementally, without scans.
    pub peak_scratch_bytes: u64,
    /// Cumulative page bytes spilled to disk, counting repeated versions.
    pub scratch_bytes_written: u64,
    /// Peak estimated decoded page bytes; excludes active copies and cache metadata.
    pub peak_page_cache_bytes: usize,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
    pub page_evictions: u64,
    /// Root height; None for an empty tree and Some(0) for a single leaf.
    pub final_height: Option<u32>,
    /// Apply and finalization time; excludes input spooling and publication.
    pub elapsed: Duration,
}
