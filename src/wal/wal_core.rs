use crate::buffer::Buffer;
use crate::config::WalConfig;
use crate::wal::{WalContents, WalFileSequenceNumber, WalOp, WriteBatch};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tracing;

/// Manages WAL file creation, buffering, flushing, replay, and cleanup.
pub struct WalManager {
    wal_dir: PathBuf,
    meta_dir: PathBuf,
    current_seq: WalFileSequenceNumber,
    op_count: usize,
    op_limit: usize,
    pending_ops: Vec<WalOp>,
}

impl WalManager {
    /// Create a new WalManager with the given data directory and WAL config.
    ///
    /// Ensures the `wal` and `meta` subdirectories exist, then determines
    /// the next WAL sequence number by scanning on-disk files.
    pub async fn new(
        data_dir: &Path,
        config: &WalConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let wal_dir = data_dir.join("wal");
        let meta_dir = data_dir.join("meta");
        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&meta_dir)?;

        // Find max existing WAL seq and flushed seq
        let _flushed_wal_seq = Self::load_last_snapshot(&meta_dir);
        let max_existing = Self::max_wal_seq(&wal_dir);

        // Start from the next available sequence
        let next_seq = max_existing.map(|s| s + 1).unwrap_or(1);

        Ok(WalManager {
            wal_dir,
            meta_dir,
            current_seq: next_seq,
            op_count: 0,
            op_limit: config.max_write_buffer_ops,
            pending_ops: Vec::with_capacity(config.max_write_buffer_ops),
        })
    }

    /// Buffer a write op. Returns `BufferFull` error if over limit.
    pub fn buffer_op(&mut self, op: WalOp) -> Result<(), WalError> {
        if self.op_count >= self.op_limit {
            return Err(WalError::BufferFull(self.op_count));
        }
        self.op_count += 1;
        self.pending_ops.push(op);
        Ok(())
    }

    /// Block until the WAL file is persisted. Returns the ops to be applied to the Buffer.
    pub async fn flush(&mut self) -> Result<Vec<WalOp>, WalError> {
        if self.pending_ops.is_empty() {
            return Ok(Vec::new());
        }

        let ops = std::mem::take(&mut self.pending_ops);
        self.op_count = 0;

        let contents = WalContents {
            wal_file_number: self.current_seq,
            ops: ops.clone(),
            persist_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };

        let data = contents.serialize_to_file();
        let path = self.wal_file_path(self.current_seq);
        tokio::fs::write(&path, &data).await.map_err(|e| {
            WalError::WriteError(format!("write WAL {}: {}", self.current_seq, e))
        })?;

        tracing::debug!(
            seq = self.current_seq,
            ops = contents.ops.len(),
            bytes = data.len(),
            "WAL file flushed"
        );

        self.current_seq += 1;
        Ok(ops)
    }

    /// Replay WAL files after startup, applying their ops to the given buffer.
    ///
    /// Only replays WAL files with a sequence number greater than the
    /// flushed (snapshotted) sequence stored in `meta/last_snapshot.json`.
    pub async fn replay(
        &self,
        buffer: &Mutex<Buffer>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let flushed_seq = Self::load_last_snapshot(&self.meta_dir);
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".wal") {
                if let Some(seq) = name_str
                    .strip_suffix(".wal")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    if seq > flushed_seq {
                        wal_files.push((seq, entry.path()));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(seq, _)| *seq);

        for (seq, path) in &wal_files {
            let data = tokio::fs::read(path).await?;
            let contents = WalContents::deserialize_from_file(&data)
                .map_err(|e| format!("replay seq {}: {}", seq, e))?;
            for op in &contents.ops {
                match op {
                    WalOp::Write(batch) => {
                        let mut buf = buffer.lock().await;
                        apply_write_batch(&mut buf, batch, *seq);
                    }
                    WalOp::Noop => {}
                }
            }
            tracing::info!(seq = seq, ops = contents.ops.len(), "WAL replayed");
        }

        tracing::info!(
            files = wal_files.len(),
            flushed_seq = flushed_seq,
            "WAL replay complete"
        );
        Ok(())
    }

    /// Clean up WAL files with a sequence number <= `through_seq`.
    pub async fn cleanup(&self, through_seq: WalFileSequenceNumber) {
        let entries = match fs::read_dir(&self.wal_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Cannot read WAL dir for cleanup: {}", e);
                return;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(seq) = name_str
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<u64>().ok())
            {
                if seq <= through_seq {
                    let _ = fs::remove_file(entry.path());
                    tracing::debug!(seq = seq, "WAL file cleaned up");
                }
            }
        }
    }

    fn wal_file_path(&self, seq: WalFileSequenceNumber) -> PathBuf {
        self.wal_dir.join(format!("{:020}.wal", seq))
    }

    fn load_last_snapshot(meta_dir: &Path) -> u64 {
        let path = meta_dir.join("last_snapshot.json");
        match fs::read_to_string(&path) {
            Ok(content) => {
                #[derive(serde::Deserialize)]
                struct SnapshotMeta {
                    flushed_wal_seq: u64,
                }
                serde_json::from_str::<SnapshotMeta>(&content)
                    .map(|m| m.flushed_wal_seq)
                    .unwrap_or(0)
            }
            Err(_) => 0,
        }
    }

    fn max_wal_seq(wal_dir: &Path) -> Option<u64> {
        let mut max_seq = None;
        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(seq) = name_str
                    .strip_suffix(".wal")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_seq = Some(max_seq.map_or(seq, |m: u64| m.max(seq)));
                }
            }
        }
        max_seq
    }
}

/// Errors that can occur during WAL operations.
#[derive(Debug)]
pub enum WalError {
    /// The WAL buffer has reached its configured capacity.
    BufferFull(usize),
    /// An I/O error occurred while writing a WAL file.
    WriteError(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::BufferFull(n) => write!(f, "WAL buffer full with {} ops", n),
            WalError::WriteError(e) => write!(f, "WAL write error: {}", e),
        }
    }
}

impl std::error::Error for WalError {}

/// Apply a `WriteBatch` to the in-memory buffer.
///
/// Ensures the target table and chunk exist, evolves the schema as needed,
/// inserts each row, and builds the tag index.
pub fn apply_write_batch(buffer: &mut Buffer, batch: &WriteBatch, wal_seq: u64) {
    let table = buffer.get_or_create_table(&batch.db_name, &batch.table_name);

    let chunk_time = batch.chunk_time;

    // Ensure fields exist in the table schema
    for row in &batch.rows {
        for fv in &row.field_values {
            if let Some(val) = fv {
                let _ = table.schema.ensure_field(
                    "", // field name comes from the LP parsing context
                    val.field_type(),
                );
            }
        }
    }

    // Clone tag_keys to avoid concurrent mutable borrow of table and chunk
    let tag_keys = table.schema.tag_keys.clone();
    let chunk = table.get_or_create_chunk(chunk_time);

    for (row_idx, row) in batch.rows.iter().enumerate() {
        chunk.insert(row.clone(), wal_seq);

        // Build tag index inline (avoids borrow conflict with build_tag_index)
        for (key_idx, key) in tag_keys.iter().enumerate() {
            if let Some(value) = row.tag_values.get(key_idx) {
                chunk
                    .tag_index
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(row_idx);
            }
        }
    }
}
