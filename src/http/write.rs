use crate::buffer::Buffer;
use crate::buffer::chunk::{Row, FieldValue as BFieldValue};
use crate::config::Config;
use crate::wal::wal_core::WalManager;
use crate::wal::{WriteBatch, WalOp};
use hyper::{body::Incoming, Request, Response, StatusCode, Method};
use http_body_util::BodyExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use influxdb_line_protocol::parse_lines;

pub struct WriteHandler {
    pub buffer: Arc<Mutex<Buffer>>,
    pub wal: Arc<Mutex<WalManager>>,
    pub config: Arc<Config>,
}

impl WriteHandler {
    pub async fn handle(&self, req: Request<Incoming>) -> Result<Response<String>, hyper::Error> {
        if req.method() != Method::POST {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body("POST only".into())
                .unwrap());
        }

        // Parse query params
        let uri = req.uri();
        let query: Vec<(String, String)> = uri.query()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            })
            .unwrap_or_default();

        let db = query.iter()
            .find(|(k, _)| k == "db")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "default".into());

        // Read body
        let body_bytes = req.into_body().collect().await?.to_bytes();
        let lp_str = match std::str::from_utf8(&body_bytes) {
            Ok(s) => s,
            Err(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("invalid utf-8".into())
                    .unwrap());
            }
        };

        // Parse line protocol
        let snapshot_interval_ns = self.config.snapshot_interval_secs().saturating_mul(1_000_000_000);
        let mut rows_by_table: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();

        for line in parse_lines(lp_str) {
            match line {
                Ok(parsed) => {
                    let table_name = parsed.series.measurement.to_string();

                    // Build tag values in alphabetical order (sorted by key for consistency)
                    let mut tag_pairs: Vec<(String, String)> = Vec::new();
                    if let Some(ref tag_set) = parsed.series.tag_set {
                        for (k, v) in tag_set {
                            tag_pairs.push((k.to_string(), v.to_string()));
                        }
                    }
                    tag_pairs.sort_by(|a, b| a.0.cmp(&b.0));

                    let tag_values: Vec<String> = tag_pairs.iter().map(|(_, v)| v.clone()).collect();

                    // Build field values
                    let mut field_pairs: Vec<(String, BFieldValue)> = Vec::new();
                    for (key, value) in &parsed.field_set {
                        let val = match value {
                            influxdb_line_protocol::FieldValue::I64(v) => BFieldValue::I64(*v),
                            influxdb_line_protocol::FieldValue::F64(v) => BFieldValue::F64(*v),
                            influxdb_line_protocol::FieldValue::U64(v) => BFieldValue::U64(*v),
                            influxdb_line_protocol::FieldValue::Boolean(v) => BFieldValue::Bool(*v),
                            influxdb_line_protocol::FieldValue::String(v) => BFieldValue::String(v.to_string()),
                        };
                        field_pairs.push((key.to_string(), val));
                    }

                    let time_ns = parsed.timestamp.unwrap_or_else(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as i64
                    });

                    let row = Row {
                        time: time_ns,
                        tag_values,
                        field_values: field_pairs.iter().map(|(_, v)| Some(v.clone())).collect(),
                    };

                    let entry = rows_by_table.entry(table_name)
                        .or_insert_with(|| Vec::new());
                    entry.push(row);
                }
                Err(e) => {
                    tracing::warn!("LP parse error (line skipped): {}", e);
                }
            }
        }

        if rows_by_table.is_empty() {
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body("0 rows".into())
                .unwrap());
        }

        let mut total_rows = 0;

        // Buffer into WAL then memory
        for (table_name, rows) in rows_by_table {
            let chunk_time = (rows[0].time / snapshot_interval_ns) * snapshot_interval_ns;

            let batch = WriteBatch {
                db_name: db.clone(),
                table_name,
                chunk_time,
                rows,
            };

            total_rows += batch.rows.len();

            // Try to buffer in WAL
            let wal_result = {
                let mut wal = self.wal.lock().await;
                wal.buffer_op(WalOp::Write(batch.clone()))
            };

            if let Err(e) = wal_result {
                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(format!("{}", e))
                    .unwrap());
            }

            // Immediately apply to memory buffer (unconfirmed write)
            {
                let mut buf = self.buffer.lock().await;
                crate::wal::wal_core::apply_write_batch(
                    &mut buf,
                    &batch,
                    0, // wal_seq will be updated on WAL flush
                );
            }
        }

        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(format!("{} rows", total_rows))
            .unwrap())
    }
}
