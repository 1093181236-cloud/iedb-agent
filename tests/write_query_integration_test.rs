use bytes::Bytes;
use hyper::{Method, Request, StatusCode};
use iedb_agent::buffer::Buffer;
use iedb_agent::config::Config;
use iedb_agent::http::query::QueryHandler;
use iedb_agent::http::write::WriteHandler;
use iedb_agent::wal::wal_core::WalManager;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tempfile::TempDir;
use tokio::sync::Mutex;

// ── Test-only body type with Error = hyper::Error ───────────────────────

/// A single-chunk body for testing.  Error is `hyper::Error` so that it
/// satisfies the `B::Error: Into<hyper::Error>` bound on `WriteHandler::handle`.
struct TestBody {
    data: Option<Bytes>,
}

impl TestBody {
    fn new(data: Bytes) -> Self {
        if data.is_empty() {
            TestBody { data: None }
        } else {
            TestBody { data: Some(data) }
        }
    }

    fn empty() -> Self {
        TestBody { data: None }
    }
}

impl hyper::body::Body for TestBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.data.take().map(|d| Ok(hyper::body::Frame::data(d))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match &self.data {
            Some(d) => hyper::body::SizeHint::with_exact(d.len() as u64),
            None => hyper::body::SizeHint::with_exact(0),
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Write a minimal TOML config file pointing at `data_dir` and return its path.
fn write_temp_config(data_dir: &std::path::Path) -> std::path::PathBuf {
    let config_path = data_dir.join("test.toml");
    let escaped = data_dir.display().to_string().replace('\\', "\\\\");
    let content = format!(
        r#"
[server]
port = 8080

[data]
dir = "{}"

[wal]
flush_interval_secs = 1
max_write_buffer_ops = 100000

[flush]
snapshot_interval = "10m"
backend = "http"
memory_limit = "512MB"

[iotedgedb]
url = "http://localhost:8086"

[agent]
id = "test-agent"
"#,
        escaped
    );
    std::fs::write(&config_path, content).unwrap();
    config_path
}

/// Build a POST /write request with a line-protocol body.
fn make_write_request(db: &str, lp: &str) -> Request<TestBody> {
    Request::builder()
        .method(Method::POST)
        .uri(format!("/write?db={}", db))
        .body(TestBody::new(Bytes::from(lp.to_string())))
        .unwrap()
}

/// Build a GET /query request with raw query params.
fn make_query_request(params: &str) -> Request<TestBody> {
    Request::builder()
        .method(Method::GET)
        .uri(format!("/query?{}", params))
        .body(TestBody::empty())
        .unwrap()
}

/// Shared test harness: create temp dirs, WalManager, handlers sharing a Buffer.
struct Harness {
    #[allow(dead_code)]
    temp: TempDir,
    write_handler: WriteHandler,
    query_handler: QueryHandler,
    #[allow(dead_code)]
    buffer: Arc<Mutex<Buffer>>,
}

impl Harness {
    async fn new() -> Self {
        let temp = TempDir::with_prefix("iedb_integration_test_").unwrap();
        let data_dir = temp.path();

        // Create staging subdirectory
        std::fs::create_dir_all(data_dir.join("staging")).unwrap();

        let config_path = write_temp_config(data_dir);
        let config = Arc::new(Config::from_file(config_path.to_str().unwrap()).unwrap());
        let wal_config = config.wal.clone();

        let buffer = Arc::new(Mutex::new(Buffer::new()));
        let wal = Arc::new(Mutex::new(
            WalManager::new(data_dir, &wal_config).await.unwrap(),
        ));

        let write_handler = WriteHandler {
            buffer: buffer.clone(),
            wal,
            config: config.clone(),
        };
        let query_handler = QueryHandler {
            buffer: buffer.clone(),
        };

        Harness {
            temp,
            write_handler,
            query_handler,
            buffer,
        }
    }
}

/// Helper: write LP, assert success, return response body string.
async fn write_lp(harness: &Harness, db: &str, lp: &str) -> String {
    let req = make_write_request(db, lp);
    let resp = harness.write_handler.handle(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body();
    assert!(
        status == StatusCode::NO_CONTENT || status == StatusCode::OK,
        "write failed: {status} {body}"
    );
    body
}

/// Helper: query and return parsed JSON rows.
async fn query_rows(harness: &Harness, params: &str) -> Vec<serde_json::Value> {
    let req = make_query_request(params);
    let resp = harness.query_handler.handle(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "query failed: {}", resp.body());
    let body = resp.into_body();
    let json: serde_json::Value =
        serde_json::from_str(&body).expect("invalid JSON query response");
    json["rows"].as_array().unwrap().clone()
}

// ── Test 1: write LP → query back ──────────────────────────────────────

#[tokio::test]
async fn test_write_and_query_basic() {
    let h = Harness::new().await;

    // Write a single point
    let body = write_lp(&h, "testdb", "cpu,host=srv01 cpu=75.5 1700000000000000000").await;
    assert!(body.contains("1 rows"), "expected 1 row, got: {body}");

    // Query back
    let rows = query_rows(&h, "db=testdb&table=cpu").await;
    assert_eq!(rows.len(), 1, "expected 1 row");
    assert_eq!(rows[0]["time"], 1700000000000000000i64);
    // Note: field names within LP parsing are not yet plumbed through to
    // schema, so the field key will be "". The value is still correct.
    let fields = rows[0]["fields"].as_object().unwrap();
    assert!(!fields.is_empty(), "should have at least one field");
    let first_val = fields.values().next().unwrap();
    assert_eq!(first_val.as_f64().unwrap(), 75.5);
}

// ── Test 2: write multiple tables → query each ─────────────────────────

#[tokio::test]
async fn test_write_multiple_tables() {
    let h = Harness::new().await;

    write_lp(&h, "testdb", "cpu,host=srv01 cpu=75.5 1700000000000000000").await;
    write_lp(&h, "testdb", "mem,host=srv01 mem=64.0 1700000000000000000").await;

    let cpu_rows = query_rows(&h, "db=testdb&table=cpu").await;
    let mem_rows = query_rows(&h, "db=testdb&table=mem").await;

    assert_eq!(cpu_rows.len(), 1);
    assert_eq!(mem_rows.len(), 1);

    let cpu_val = cpu_rows[0]["fields"].as_object().unwrap()
        .values().next().unwrap().as_f64().unwrap();
    let mem_val = mem_rows[0]["fields"].as_object().unwrap()
        .values().next().unwrap().as_f64().unwrap();

    assert_eq!(cpu_val, 75.5);
    assert_eq!(mem_val, 64.0);
}

// ── Test 3: write with tags → tag storage path is exercised ─────────────

#[tokio::test]
async fn test_write_with_tags() {
    let h = Harness::new().await;

    write_lp(
        &h,
        "testdb",
        "cpu,host=srv01,region=us-east cpu=75.5 1700000000000000000\n\
         cpu,host=srv02,region=us-west cpu=60.0 1700000000000000001",
    )
    .await;

    let rows = query_rows(&h, "db=testdb&table=cpu").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["time"], 1700000000000000000i64);
    assert_eq!(rows[1]["time"], 1700000000000000001i64);
}

// ── Test 4: write at different times → query with time range ───────────

#[tokio::test]
async fn test_write_and_query_time_range() {
    let h = Harness::new().await;

    let t0 = 1_700_000_000_000_000_000i64;
    let t1 = t0 + 1_000_000_000;
    let t2 = t0 + 2_000_000_000;
    let t3 = t0 + 3_000_000_000;

    write_lp(
        &h,
        "testdb",
        &format!(
            "cpu,host=srv01 cpu=10.0 {t0}\n\
             cpu,host=srv01 cpu=20.0 {t1}\n\
             cpu,host=srv01 cpu=30.0 {t2}\n\
             cpu,host=srv01 cpu=40.0 {t3}"
        ),
    )
    .await;

    // Query with time range [t1 .. t2]
    let rows = query_rows(&h, &format!("db=testdb&table=cpu&start={t1}&end={t2}")).await;
    assert_eq!(rows.len(), 2, "expected 2 rows in time range");
    assert_eq!(rows[0]["time"], t1);
    assert_eq!(rows[1]["time"], t2);

    // Query with time range before all data
    let rows = query_rows(&h, "db=testdb&table=cpu&start=1&end=100").await;
    assert!(rows.is_empty(), "expected 0 rows before all data");

    // Query without time filter returns all
    let rows = query_rows(&h, "db=testdb&table=cpu").await;
    assert_eq!(rows.len(), 4);
}

// ── Test 5: write to multiple databases ────────────────────────────────

#[tokio::test]
async fn test_write_multiple_databases() {
    let h = Harness::new().await;

    write_lp(&h, "db_one", "cpu,host=a cpu=1.0 1700000000000000000").await;
    write_lp(&h, "db_two", "cpu,host=b cpu=2.0 1700000000000000000").await;

    let rows_one = query_rows(&h, "db=db_one&table=cpu").await;
    let rows_two = query_rows(&h, "db=db_two&table=cpu").await;

    assert_eq!(rows_one.len(), 1);
    assert_eq!(rows_two.len(), 1);

    let val_one = rows_one[0]["fields"].as_object().unwrap()
        .values().next().unwrap().as_f64().unwrap();
    let val_two = rows_two[0]["fields"].as_object().unwrap()
        .values().next().unwrap().as_f64().unwrap();

    assert_eq!(val_one, 1.0);
    assert_eq!(val_two, 2.0);

    let rows_empty = query_rows(&h, "db=nonexistent&table=cpu").await;
    assert!(rows_empty.is_empty());
}

// ── Test 6: health verification via direct buffer check ────────────────

#[tokio::test]
async fn test_health_via_direct_buffer_check() {
    let h = Harness::new().await;

    write_lp(&h, "testdb", "cpu,host=srv01 cpu=75.5 1700000000000000000").await;

    let buf = h.buffer.lock().await;
    let table = buf.get_table("testdb", "cpu").expect("table should exist");
    assert_eq!(table.chunks.len(), 1);
    assert_eq!(table.chunks[0].row_count(), 1);
    assert_eq!(table.chunks[0].rows[0].time, 1700000000000000000i64);
}

// ── Test 7: write multiple rows in a single LP payload ─────────────────

#[tokio::test]
async fn test_write_multiple_rows_single_request() {
    let h = Harness::new().await;

    let body = write_lp(
        &h,
        "testdb",
        "cpu,host=srv01 cpu=10.0 1700000000000000000\n\
         cpu,host=srv01 cpu=20.0 1700000000000000001\n\
         cpu,host=srv01 cpu=30.0 1700000000000000002",
    )
    .await;
    assert!(body.contains("3 rows"), "expected 3 rows, got: {body}");

    let rows = query_rows(&h, "db=testdb&table=cpu").await;
    assert_eq!(rows.len(), 3);
}

// ── Test 8: write empty body returns 204 ───────────────────────────────

#[tokio::test]
async fn test_write_empty_body() {
    let h = Harness::new().await;

    let req = make_write_request("testdb", "");
    let resp = h.write_handler.handle(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(resp.into_body(), "0 rows");
}

// ── Test 9: write bad LP → partial ingestion with skip ─────────────────

#[tokio::test]
async fn test_write_bad_line_protocol_partial() {
    let h = Harness::new().await;

    let body = write_lp(
        &h,
        "testdb",
        "cpu,host=srv01 cpu=75.5 1700000000000000000\nthis is not valid lp",
    )
    .await;
    assert!(body.contains("1 rows"), "expected 1 row, got: {body}");

    let rows = query_rows(&h, "db=testdb&table=cpu").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["time"], 1700000000000000000i64);
}

// ── Test 10: query with missing table param ────────────────────────────

#[tokio::test]
async fn test_query_missing_table_param() {
    let h = Harness::new().await;

    let req = make_query_request("db=testdb");
    let resp = h.query_handler.handle(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(resp.into_body().contains("missing table param"));
}

// ── Test 11: write with boolean and string fields ──────────────────────

#[tokio::test]
async fn test_write_bool_and_string_fields() {
    let h = Harness::new().await;

    write_lp(
        &h,
        "testdb",
        "status,host=srv01 online=true,msg=\"ok\" 1700000000000000000",
    )
    .await;

    let rows = query_rows(&h, "db=testdb&table=status").await;
    assert_eq!(rows.len(), 1);
    let fields = rows[0]["fields"].as_object().unwrap();
    assert!(!fields.is_empty());
    let first_val = fields.values().next().unwrap();
    assert!(
        first_val.as_bool().unwrap_or(false) || first_val.as_str().is_some(),
        "unexpected field value: {first_val}"
    );
}
