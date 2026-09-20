//! Schema-driven tree construction, incremental updates, and atomic snapshot publication.
mod file;
mod rows;
mod snapshot;
mod stats;
pub use stats::TransactionStats;
mod transaction;
mod tree;
mod update;
use beech_core::{BeechError, Id, Result};
pub use beech_disk::SortLimits;
pub use file::FileWriter;
pub use rows::batch_from_rows;
pub use snapshot::{publish_table, Publication};
pub use transaction::Transaction;
pub use tree::build_table;
pub use update::{apply_changes, Change};

/// Stage immutable objects under IDs supplied by the codecs. Existing bytes
/// must never be overwritten. Callers must supply the correct codec-produced
/// ID for the bytes; sinks may trust existing objects without checking contents.
pub trait ObjectSink {
    fn put(&mut self, id: Id, bytes: &[u8]) -> std::io::Result<()>;
}

/// A publication transaction. Commit makes objects available before replacing
/// the root pointer. Abort/drop must never remove previously committed objects.
pub trait Writer: ObjectSink {
    fn stage_root(&mut self, root_id: Id) -> std::io::Result<()>;
    fn commit(self) -> std::io::Result<()>;
    fn abort(self) -> std::io::Result<()>;
    fn num_to_commit(&self) -> usize;
}

/// Targets measure canonical logical input bytes, not compressed Parquet size.
#[derive(Clone, Copy, Debug)]
pub struct BuildOptions {
    pub(crate) target_bytes: usize,
    pub(crate) stddev_bytes: usize,
}
impl BuildOptions {
    pub fn new(target_bytes: usize, stddev_bytes: usize) -> Result<Self> {
        if target_bytes == 0 || stddev_bytes == 0 {
            return Err(BeechError::Query(
                "node target and standard deviation must be positive".into(),
            ));
        }
        Ok(Self {
            target_bytes,
            stddev_bytes,
        })
    }
}
impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            target_bytes: 1000,
            stddev_bytes: 100,
        }
    }
}
#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
