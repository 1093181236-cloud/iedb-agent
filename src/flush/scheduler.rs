use crate::buffer::Buffer;
use crate::config::Config;
use crate::flush::http_upload::{self, UploadError};
use crate::flush::parquet::flush_chunks_to_parquet;
use crate::flush::s3;
use crate::wal::wal_core::WalManager;
use reqwest::Client;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing;

pub struct SnapshotScheduler {
    pub buffer: Arc<Mutex<Buffer>>,
    pub wal: Arc<Mutex<WalManager>>,
    pub config: Arc<Config>,
    pub client: Client,
    pub staging_dir: PathBuf,
}

impl SnapshotScheduler {
    pub fn new(
        buffer: Arc<Mutex<Buffer>>,
        wal: Arc<Mutex<WalManager>>,
        config: Arc<Config>,
        client: Client,
    ) -> Self {
        let staging_dir = config.data.dir.join("staging");
        SnapshotScheduler {
            buffer,
            wal,
            config,
            client,
            staging_dir,
        }
    }

    /// Run the background snapshot + memory protection loop.
    pub async fn run(&self) {
        let snapshot_interval = Duration::from_secs(self.config.snapshot_interval_secs() as u64);
        let memory_check_interval = Duration::from_secs(5);
        let mut last_snapshot = Instant::now();

        loop {
            tokio::time::sleep(memory_check_interval).await;

            // Check memory pressure
            let total_bytes = {
                let buf = self.buffer.lock().await;
                buf.total_estimated_size()
            };

            let memory_limit = self.config.memory_limit_bytes();
            let should_force = total_bytes >= memory_limit;
            let should_timed = last_snapshot.elapsed() >= snapshot_interval;

            if should_force || should_timed {
                if should_force {
                    tracing::warn!(
                        total_bytes = total_bytes,
                        limit = memory_limit,
                        "Memory limit reached, forcing snapshot"
                    );
                }

                match self.do_snapshot().await {
                    Ok(n) => {
                        tracing::info!(chunks_flushed = n, "Snapshot complete");
                        last_snapshot = Instant::now();
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Snapshot failed");
                        // On failure: chunks stay in memory, WAL stays, staging has parquet
                        // Memory protection will handle spill if needed
                    }
                }
            }
        }
    }

    /// Execute one snapshot cycle.
    async fn do_snapshot(&self) -> Result<usize, String> {
        let snapshot_interval_ns = self.config.snapshot_interval_secs() * 1_000_000_000;
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let end_time_marker = ((now_ns - snapshot_interval_ns) / snapshot_interval_ns)
            * snapshot_interval_ns;

        let mut chunks_to_flush: Vec<(String, String, Vec<usize>)> = Vec::new();

        {
            let buf = self.buffer.lock().await;
            for (db_name, tables) in &buf.databases {
                for (table_name, table) in tables {
                    let indices: Vec<usize> = table
                        .chunks
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.chunk_time < end_time_marker)
                        .map(|(i, _)| i)
                        .collect();
                    if !indices.is_empty() {
                        chunks_to_flush.push((db_name.clone(), table_name.clone(), indices));
                    }
                }
            }
        }

        let mut flushed_count = 0;

        for (db_name, table_name, chunk_indices) in &chunks_to_flush {
            let chunks: Vec<crate::buffer::chunk::Chunk> = {
                let buf = self.buffer.lock().await;
                let table = buf.get_table(db_name, table_name).ok_or("table not found")?;
                chunk_indices.iter().map(|&i| table.chunks[i].clone()).collect()
            };

            let chunk_refs: Vec<&crate::buffer::chunk::Chunk> = chunks.iter().collect();

            let table_for_schema = {
                let buf = self.buffer.lock().await;
                buf.get_table(db_name, table_name).cloned()
            };

            let table = table_for_schema.ok_or("table not found")?;
            let parquet_data =
                flush_chunks_to_parquet(&table, &chunk_refs).map_err(|e| format!("parquet write: {}", e))?;

            // Upload
            let upload_result = match self.config.flush.backend.as_str() {
                "s3" => {
                    let s3_cfg = self.config.s3.as_ref().ok_or("S3 config missing")?;
                    let key = s3::s3_key(
                        &self.config.agent.id,
                        db_name,
                        table_name,
                        chunks.first().map(|c| c.time_min).unwrap_or(0),
                    );
                    s3::upload_to_s3(&self.client, s3_cfg, &key, &parquet_data).await
                }
                _ => {
                    // Default: HTTP upload
                    match http_upload::upload_parquet(
                        &self.client,
                        &self.config.iotedgedb.url,
                        db_name,
                        table_name,
                        &parquet_data,
                    )
                    .await
                    {
                        Ok(()) => Ok(()),
                        Err(UploadError::Http(e)) => Err(e),
                        Err(UploadError::ServerError { status, body }) => {
                            Err(format!("HTTP {} {}", status, body))
                        }
                    }
                }
            };

            match upload_result {
                Ok(()) => {
                    // Success: remove chunks, write metadata, clean WAL
                    let snapshot_wal_seq = {
                        let mut buf = self.buffer.lock().await;
                        for &idx in chunk_indices.iter().rev() {
                            if let Some(table) = buf.get_table_mut(db_name, table_name) {
                                table.chunks.remove(idx);
                            }
                        }

                        // Compute safe wal seq
                        let mut min_wal = u64::MAX;
                        for (_, tables) in &buf.databases {
                            for (_, t) in tables {
                                for c in &t.chunks {
                                    if c.min_wal_seq < min_wal {
                                        min_wal = c.min_wal_seq;
                                    }
                                }
                            }
                        }
                        if min_wal == u64::MAX {
                            0
                        } else {
                            min_wal.saturating_sub(1)
                        }
                    };

                    // Write metadata
                    let meta = serde_json::json!({
                        "flushed_wal_seq": snapshot_wal_seq,
                        "snapshot_ts": chrono::Utc::now().to_rfc3339(),
                    });
                    let meta_path = self.config.data.dir.join("meta").join("last_snapshot.json");
                    let meta_str = serde_json::to_string(&meta).unwrap();
                    std::fs::write(&meta_path, &meta_str)
                        .map_err(|e| format!("meta write: {}", e))?;
                    // fsync the directory for durability
                    if let Ok(f) = std::fs::File::open(meta_path.parent().unwrap()) {
                        let _ = f.sync_all();
                    }

                    // Clean WAL
                    self.wal.lock().await.cleanup(snapshot_wal_seq).await;

                    flushed_count += 1;
                }
                Err(e) => {
                    // Failure: save to staging
                    tracing::warn!(
                        db = %db_name,
                        table = %table_name,
                        error = %e,
                        "Upload failed, saving to staging"
                    );
                    http_upload::staging_save(&self.staging_dir, db_name, table_name, &parquet_data)
                        .map_err(|e| format!("staging save: {}", e))?;
                    // chunk stays in memory, WAL stays
                }
            }
        }

        Ok(flushed_count)
    }
}
