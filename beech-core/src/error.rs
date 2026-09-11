use crate::Id;
use thiserror::Error;

// Construct a message-bearing error using format! syntax. Keeping this separate
// from bail! also supports map_err/ok_or_else closures and tail expressions.
// Examples:
//   beech_error!(Schema, "column {column}: expected {}, found {actual:?}", expected)
//   value.ok_or_else(|| beech_error!(Wire, "missing {field}"))?
// These helpers target String-bearing variants; typed payloads such as IDs use
// their constructors directly.
macro_rules! beech_error {
    ($variant:ident, $($args:tt)+) => {
        $crate::BeechError::$variant(::std::format!($($args)+))
    };
}

// Return early with a formatted error; use the same variant/message syntax.
//   bail!(InvalidNode, "node {id}: invalid height {height}");
macro_rules! bail {
    ($variant:ident, $($args:tt)+) => {
        return ::core::result::Result::Err($crate::error::beech_error!($variant, $($args)+))
    };
}

pub(crate) use {bail, beech_error};

#[derive(Debug, Error)]
pub enum BeechError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("Thrift error: {0}")]
    Thrift(#[from] thrift::Error),
    #[error("invalid 32-byte object ID")]
    InvalidId,
    #[error("schema error: {0}")]
    Schema(String),
    #[error("invalid node: {0}")]
    InvalidNode(String),
    #[error("invalid wire data: {0}")]
    Wire(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("object not found: {0}")]
    NotFound(Id),
    #[error("no such table: {0}")]
    NoSuchTable(String),
    #[error("content hash mismatch for {0}")]
    HashMismatch(Id),
}
impl BeechError {
    // Add identity at the loading boundary, where an internal node's ID is known.
    // Preserve validation categories and leave typed errors (and their causes) intact.
    pub(crate) fn with_node_context(self, id: Id) -> Self {
        match self {
            Self::InvalidNode(message) => beech_error!(InvalidNode, "node {id}: {message}"),
            Self::Schema(message) => beech_error!(Schema, "node {id}: {message}"),
            Self::Wire(message) => beech_error!(Wire, "node {id}: {message}"),
            other => other,
        }
    }
}
pub type Result<T> = std::result::Result<T, BeechError>;
