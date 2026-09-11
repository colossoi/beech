//! Immutable byte storage, shared decoded-object access, and snapshot handles.
pub(crate) mod accounting;
mod backend;
mod cache;
mod leaf;
mod repository;
#[cfg(test)]
mod tests;

pub(crate) use backend::ObjectReader;
pub use backend::{BackingStore, FileStore, ObjectFile};
pub use cache::CacheStats;
pub use leaf::{Leaf, LeafBatches};
pub use repository::{Repository, RepositoryOptions, RepositoryStats, Snapshot};
