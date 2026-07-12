# iedb-agent

IotEdgeDB edge ingest agent — a lightweight Rust service for collecting, buffering, and forwarding time-series data from edge devices to [IotEdgeDB](https://github.com/Mengdal/IotEdgeDB).

```
Edge Device                              IotEdgeDB Server
┌──────────────┐                        ┌──────────────────┐
│  iedb-agent  │── flush: Parquet ───→  │  /api/v1/ingest  │
│              │                        │                  │
│  WAL + Buffer│←── query: HTTP ─────── │  DuckDB + Parquet│
└──────────────┘                        └──────────────────┘
  ARM32 / ARM64                           amd64 / ARM64
```

## Features

- **Line Protocol ingestion** — InfluxDB-compatible LP format via HTTP
- **WAL durability** — Write-Ahead Log with CRC32 integrity, crash-safe replay
- **In-memory buffer** — Tag-indexed time-partitioned chunks for fast queries
- **Incremental Parquet flush** — Time-chunked snapshot to Parquet, HTTP or S3 upload
- **Zero Arrow/DataFusion** — Pure row-based design, minimal dependencies
- **ARM32 / ARM64** — Static musl binaries via `cargo-zigbuild`, GLIBC-free
- **Agent registration** — Auto-register with IotEdgeDB, heartbeat with table metadata

## Quick Start

```bash
# Build
cargo build --release

# Configure
cp iedb-agent-arm32.toml.example iedb-agent.toml
# Edit: set [iotededb].url and [agent].id

# Run
./target/release/iedb-agent
```

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/write?db=<name>` | Write Line Protocol (text/plain body) |
| `GET` | `/query?db=<name>&table=<name>&start=<ns>&end=<ns>&tag=<k>=<v>` | Query memory buffer → JSON |
| `GET` | `/health` | Health check → `ok` |

### Write example

```bash
curl -X POST "http://localhost:8080/write?db=mydb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $(date +%s)000000000"
```

### Query example

```bash
# All rows
curl "http://localhost:8080/query?db=mydb&table=cpu"

# With time range and tag filter
curl "http://localhost:8080/query?db=mydb&table=cpu&start=1700000000000000000&end=1800000000000000000&tag=host=srv01"
```

### Response format

```json
{
  "rows": [
    {
      "time": 1700000000000000000,
      "tags": {"host": "srv01"},
      "fields": {"cpu": 75.5, "mem": 62.3}
    }
  ]
}
```

## Configuration

```toml
[server]
port = 8080

[data]
dir = "/var/lib/iedb-agent"

[wal]
flush_interval_secs = 1          # WAL buffer flush interval
max_write_buffer_ops = 100000    # Max buffered ops before rejecting writes

[flush]
snapshot_interval = "10m"        # Chunk boundary + snapshot frequency
backend = "http"                 # "http" (default) or "s3"
memory_limit = "512MB"           # Max in-memory buffer; triggers force-snapshot

# HTTP mode (default, no S3 needed)
[iotedgedb]
url = "http://iotededb:8000"

# S3 mode (multi-agent production)
[s3]
bucket = "mybucket"
region = "us-east-1"
endpoint = "https://s3.amazonaws.com"
access_key = "..."
secret_key = "..."

[agent]
id = "agent-01"                  # Unique agent identifier
```

## Architecture

```
POST /write (Line Protocol)
  │
  ├─ 1. Parse LP → rows
  ├─ 2. Compute chunk_time = floor(time / snapshot_interval)
  ├─ 3. WalBuffer (op_limit gate)
  ├─ 4. WAL flush (1s) → {data}/wal/{seq}.wal
  ├─ 5. Buffer insert → Table.chunks (Vec<Chunk>, tag-indexed)
  └─ 204

GET /query
  │
  ├─ tag_index lookup → candidate rows
  ├─ time range filter
  └─ JSON response

Snapshot (every snapshot_interval or memory pressure)
  │
  ├─ end_time_marker = now - snapshot_interval
  ├─ Collect chunks where chunk_time < end_time_marker
  ├─ merge-sort + dedup → Parquet bytes
  ├─ Upload: HTTP POST to iotededb OR S3 PUT
  ├─ Success → remove chunks, write last_snapshot.json, clean WAL
  └─ Failure → save to staging/, keep chunks + WAL, retry
```

### Data model

```rust
// Schema shared per table
TableSchema { tag_keys: Vec<String>, field_defs: Vec<FieldDef> }

// Values only — keys from schema
Row { time: i64, tag_values: Vec<String>, field_values: Vec<Option<FieldValue>> }

// Time-partitioned chunk with tag index
Chunk { chunk_time, rows, tag_index, min_wal_seq, max_wal_seq }
```

### Memory protection

```
1. WAL buffer op_limit    → BufferFull → HTTP 503
2. memory_limit exceeded  → force snapshot → release staging-covered chunks
3. Still over limit       → HTTP 503
```

## ARM Cross-Compilation

### ARM32 (armv7)

```bash
cargo install cargo-zigbuild
cargo zigbuild --target armv7-unknown-linux-musleabihf --release
```

Binary: `target/armv7-unknown-linux-musleabihf/release/iedb-agent`
(5.5MB, statically linked, no GLIBC dependency)

### ARM64 (aarch64)

```bash
cargo zigbuild --target aarch64-unknown-linux-musl --release
```

Binary: `target/aarch64-unknown-linux-musl/release/iedb-agent`
(6MB, statically linked)

### Pre-built binaries

Download from [GitHub Releases](https://github.com/1093181236-cloud/iedb-agent/releases):
- `iedb-agent-armv7` — ARM32 (Raspberry Pi, embedded boards)
- `iedb-agent-aarch64` — ARM64 (Raspberry Pi 4/5, AWS Graviton)

## Deployment

### ARM32 device

```bash
# Download pre-built binary
gh release download v0.1.1 --repo 1093181236-cloud/iedb-agent --pattern "*-armv7"

# Copy to device
scp iedb-agent-armv7 root@192.168.1.100:/usr/local/bin/iedb-agent

# Create config on device
cat > /etc/iedb-agent.toml << 'EOF'
[server]
port = 8080

[data]
dir = "/var/lib/iedb-agent"

[flush]
snapshot_interval = "60s"
backend = "http"
memory_limit = "128MB"

[iotedgedb]
url = "http://192.168.1.1:8000"

[agent]
id = "edge-gateway-01"
EOF

# Run
iedb-agent
```

### Deploy with iotededb (end-to-end test)

```bash
# 1. Start iotededb (with auth disabled)
IEDB_AUTH_ENABLED=false iedb serve --config iedb.toml

# 2. Start iedb-agent on ARM32 device
# 3. Write test data
curl -X POST "http://ARM32_IP:8080/write?db=test" \
  -d "cpu,host=srv01 cpu=75.5 $(date +%s)000000000"

# 4. Query agent memory buffer
curl "http://ARM32_IP:8080/query?db=test&table=cpu"

# 5. Query iotededb (Parquet + agent buffer merge)
curl -X POST "http://IOTEDGEDB_IP:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -H "x-iedb-database: test" \
  -d '{"sql":"SELECT * FROM cpu ORDER BY time"}'
```

## Development

```bash
# Run tests
cargo test                 # 65 unit tests
cargo test --test '*'      # 11 integration tests
```

## Dependencies

All pure Rust, zero C dependencies. ARM32/ARM64 compatible.

| Crate | Purpose |
|-------|---------|
| `hyper` | HTTP server |
| `reqwest` + `rustls` | HTTP client, TLS (ring) |
| `bitcode` | WAL binary serialization |
| `parquet` (snap + flate2) | Parquet file writer |
| `influxdb-line-protocol` | LP parser |
| `aws-sigv4` | S3 request signing |

## License

MIT OR Apache-2.0
