pub mod serialize;

use crate::buffer::chunk::Row;
use serde::{Deserialize, Serialize};

/// Unique monotonically increasing WAL file sequence number.
pub type WalFileSequenceNumber = u64;

/// A batch of writes targeting a specific table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBatch {
    pub db_name: String,
    pub table_name: String,
    pub chunk_time: i64,
    pub rows: Vec<Row>,
}

/// A WAL operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Write(WriteBatch),
    Noop,
}

/// The serialized content of a single WAL file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalContents {
    pub wal_file_number: WalFileSequenceNumber,
    pub ops: Vec<WalOp>,
    pub persist_timestamp_ms: i64,
}
