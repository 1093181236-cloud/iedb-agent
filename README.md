# iedb-agent

IotEdgeDB 边缘采集代理 — 一个轻量级 Rust 服务，用于从边缘设备采集、缓冲和转发时序数据至 [IotEdgeDB](https://github.com/Mengdal/IotEdgeDB)。

```
边缘设备                                   IotEdgeDB 服务器
┌──────────────┐                        ┌──────────────────┐
│  iedb-agent  │── 刷写: Parquet ───→  │  /api/v1/ingest  │
│              │                        │                  │
│  WAL + 缓冲  │←── 查询: HTTP ─────── │  DuckDB + Parquet│
└──────────────┘                        └──────────────────┘
  ARM32 / ARM64                           amd64 / ARM64
```

## 目录

- [特性](#特性)
- [快速开始](#快速开始)
- [HTTP API](#http-api)
- [配置](#配置)
- [架构设计](#架构设计)
- [数据模型](#数据模型)
- [刷写与上传](#刷写与上传)
- [内存保护](#内存保护)
- [ARM 交叉编译](#arm-交叉编译)
- [部署](#部署)
- [端到端测试](#端到端测试)
- [开发](#开发)
- [依赖](#依赖)
- [许可证](#许可证)

## 特性

- **Line Protocol 采集** — 通过 HTTP 接口接收兼容 InfluxDB 的 Line Protocol 格式时序数据
- **WAL 持久化** — 预写日志（Write-Ahead Log），带 CRC32 完整性校验和崩溃安全回放
- **内存缓冲** — 基于标签索引的时间分区 Chunk，支持快速查询
- **增量 Parquet 刷写** — 按时间 Chunk 快照生成 Parquet 文件，支持 HTTP 或 S3 上传
- **零 Arrow/DataFusion 依赖** — 纯行式设计，极简依赖树，专为资源受限的边缘设备打造
- **ARM32 / ARM64 支持** — 通过 `cargo-zigbuild` 生成静态 musl 二进制文件，无 GLIBC 依赖
- **Agent 注册与心跳** — 自动向 IotEdgeDB 注册，周期性上报表元数据变更
- **内存保护与背压** — 可配置内存上限，超限时触发强制刷写或拒绝写入（HTTP 503）
- **失败重试** — 上传失败的 Parquet 文件暂存至本地 staging 目录，后台自动重试
- **认证支持** — 支持 IotEdgeDB Bearer Token 认证

## 快速开始

```bash
# 构建
cargo build --release

# 配置
cp iedb-agent-arm32.toml.example iedb-agent.toml
# 编辑: 设置 [iotededb].url 和 [agent].id

# 运行
./target/release/iedb-agent
```

## HTTP API

| 方法 | 路径 | 说明 |
|--------|------|-------------|
| `POST` | `/write?db=<name>` | 写入 Line Protocol 数据（body 为 text/plain） |
| `GET` | `/query?db=<name>&table=<name>&start=<ns>&end=<ns>&tag=<k>=<v>` | 查询内存缓冲 → JSON |
| `GET` | `/health` | 健康检查 → `ok` |

### 写入示例

```bash
curl -X POST "http://localhost:8080/write?db=mydb" \
  -d "cpu,host=srv01 cpu=75.5,mem=62.3 $(date +%s)000000000"
```

### 查询示例

```bash
# 查询所有行
curl "http://localhost:8080/query?db=mydb&table=cpu"

# 带时间范围和标签过滤
curl "http://localhost:8080/query?db=mydb&table=cpu&start=1700000000000000000&end=1800000000000000000&tag=host=srv01"
```

### 响应格式

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

## 配置

```toml
[server]
port = 8080
# max_body_bytes = 10485760  # 请求体最大字节数（默认 10MB）

[data]
dir = "/var/lib/iedb-agent"

[wal]
flush_interval_secs = 1          # WAL 缓冲区刷写间隔
max_write_buffer_ops = 100000    # WAL 缓冲区最大操作数，超限则拒绝写入

[flush]
snapshot_interval = "10m"        # Chunk 边界 + 快照频率
backend = "http"                 # "http"（默认）或 "s3"
memory_limit = "512MB"           # 内存缓冲区上限，超限触发强制快照

# HTTP 模式（默认，无需 S3）
[iotedgedb]
url = "http://iotededb:8000"
# token = "iedb_xxxxxxxxxxxxxxxxxxxx"   # Bearer Token（iotededb 开启认证时必填）

# S3 模式（多 Agent 生产环境）
[s3]
bucket = "mybucket"
region = "us-east-1"
endpoint = "https://s3.amazonaws.com"
access_key = "..."
secret_key = "..."

[agent]
id = "agent-01"                  # 唯一 Agent 标识符
```

### 配置说明

| 字段 | 类型 | 默认值 | 说明 |
|-------|------|---------|-------------|
| `server.port` | u16 | `8080` | HTTP 监听端口 |
| `server.max_body_bytes` | usize | `10485760` (10MB) | 单次写入请求体大小上限 |
| `data.dir` | path | `/var/lib/iedb-agent` | 数据存储目录（WAL、meta、staging） |
| `wal.flush_interval_secs` | u64 | `1` | WAL 缓冲区刷写到磁盘的间隔（秒） |
| `wal.max_write_buffer_ops` | usize | `100000` | WAL 缓冲操作数上限，超限返回 503 |
| `flush.snapshot_interval` | string | `"10m"` | 快照间隔（支持 `s`/`m` 后缀） |
| `flush.backend` | string | `"http"` | 上传后端：`"http"` 或 `"s3"` |
| `flush.memory_limit` | string | `"512MB"` | 内存上限（支持 `MB`/`GB` 后缀） |
| `iotedgedb.url` | string | 必填 | IotEdgeDB 服务地址 |
| `iotedgedb.token` | string | 可选 | IotEdgeDB 认证 Bearer Token |
| `agent.id` | string | 必填 | 全局唯一的 Agent 标识 |

## 架构设计

### 整体架构

项目采用模块化设计，由六个核心模块组成：

| 模块 | 路径 | 职责 |
|--------|------|-----------|
| `agent` | `src/agent/` | Agent 注册、心跳上报、表变更检测 |
| `buffer` | `src/buffer/` | 内存缓冲：Chunk 管理、标签索引、查询引擎 |
| `config` | `src/config.rs` | TOML 配置文件解析、参数校验 |
| `flush` | `src/flush/` | Parquet 生成、HTTP/S3 上传、快照调度、失败暂存 |
| `http` | `src/http/` | HTTP 服务：`/write` 和 `/query` 端点 |
| `wal` | `src/wal/` | WAL 文件管理：缓冲、序列化、CRC32 校验、崩溃回放 |

### 数据写入流程

```
POST /write (Line Protocol)
  │
  ├─ 1. 解析 LP → 按表分组行
  ├─ 2. 计算 chunk_time = floor(time / snapshot_interval)
  ├─ 3. WalManager.buffer_op() 缓冲写入（op_limit 门控）
  ├─ 4. WAL 刷写（1s 间隔）→ {data}/wal/{seq}.wal（CRC32 完整性校验）
  ├─ 5. Buffer 插入 → Table.chunks（标签索引，按 chunk_time 排序）
  └─ 204 No Content
```

写入采用**同步刷写 WAL 后插入 Buffer**的单一写入路径，确保：
- WAL 先于内存持久化（崩溃安全）
- 单次 HTTP 请求内完成 WAL + Buffer（无异步空洞）
- WAL 缓冲超限时直接返回 HTTP 503

### 数据查询流程

```
GET /query
  │
  ├─ 标签索引查找 → 候选行索引
  ├─ 时间范围过滤
  └─ JSON 响应（时间戳 ns → µs，字段按 schema 输出）
```

### 快照流程

```
快照触发（每 snapshot_interval 或内存压力）
  │
  ├─ end_time_marker = now - snapshot_interval
  ├─ 收集 chunk_time < end_time_marker 的 Chunk
  ├─ 归并排序 + 去重 → Parquet 字节
  ├─ 上传: HTTP POST 到 iotededb 或 S3 PUT
  ├─ 成功 → 移除 Chunk，写 last_snapshot.json（fsync 保证持久化），清理 WAL
  └─ 失败 → 保存到 staging/，保留 Chunk + WAL，后台重试
```

### 崩溃恢复

```
启动
  │
  ├─ 读取 meta/last_snapshot.json → flushed_wal_seq
  ├─ 扫描 wal/ 目录 → 找到 seq > flushed_wal_seq 的 WAL 文件
  ├─ 按序回放 WAL 操作到 Buffer → 恢复未刷写数据
  └─ 正常服务
```

## 数据模型

```rust
// 表级共享 Schema
TableSchema { tag_keys: Vec<String>, field_defs: Vec<FieldDef> }

// Schema 演化：新 tag/field 首次出现时自动注册
impl TableSchema {
    fn ensure_tag_key(&mut self, key: &str) -> usize;
    fn ensure_field(&mut self, name: &str, value_type: FieldType) -> usize;
}

// Row 只存值——key 从 Schema 获取
Row { time: i64, tag_values: Vec<String>, field_values: Vec<Option<FieldValue>> }

// 时间分区 Chunk，带标签倒排索引
Chunk { chunk_time, rows, tag_index, min_wal_seq, max_wal_seq }

// 支持 5 种字段类型
enum FieldValue { I64(i64), F64(f64), U64(u64), Bool(bool), String(String) }
```

### Schema 演化

Line Protocol 中首次出现的新 tag key 或 field 会被自动注册到表的 Schema 中。同一表的所有 Chunk 共享同一 Schema，保证 Parquet 文件 schema 的一致性。

## 刷写与上传

### 刷写触发条件

| 条件 | 行为 |
|----------|--------|
| 定时触发（`snapshot_interval` 到期） | 收集已过期的 Chunk 并刷写 |
| 内存压力（`memory_limit` 超限） | 强制刷写以释放内存 |
| 二者同时满足 | 优先执行一次快照 |

### 上传后端

#### HTTP 模式（默认）

- Parquet 字节通过 HTTP POST 发送至 `{iotededb_url}/api/v1/ingest/parquet?db=<name>&measurement=<name>`
- 支持 Bearer Token 认证
- 适用于单 Agent 或小规模部署

#### S3 模式

- 使用 AWS SigV4 签名直传 S3
- S3 key 格式：`{db}/{table}/{year}/{month}/{day}/{hour}/{agent_id}_{timestamp}_{nanos}.parquet`
- 通过 key 路径实现按时间的自动分区
- 适用于多 Agent 大规模生产环境

### 失败处理（Staging 重试）

上传失败时：
1. Parquet 文件保存到 `{data_dir}/staging/{db}/{table}/{timestamp}.parquet`
2. 原地保留 Chunk 和 WAL（不丢失数据）
3. 后台任务每 30 秒扫描 staging 目录并重试上传
4. 上传成功后自动删除临时文件

## 内存保护

三级内存保护机制：

```
1. WAL 缓冲区 op_limit    → BufferFull → HTTP 503 Service Unavailable
2. memory_limit 被超出     → 强制快照 → 释放 staging 覆盖的 Chunk
3. 快照后仍超限           → HTTP 503 Service Unavailable
```

- `memory_limit` 通过 `flush.snapshot_interval` 配置（支持 `MB`/`GB` 后缀）
- 定时快照和强制快照共享同一快照调度器，避免重复刷写
- 内存估算基于每行平均字节数 × Chunk 行数（每个 Chunk 最小 64 字节）

## ARM 交叉编译

### ARM32 (armv7)

```bash
cargo install cargo-zigbuild
cargo zigbuild --target armv7-unknown-linux-musleabihf --release
```

产物: `target/armv7-unknown-linux-musleabihf/release/iedb-agent`
（约 5.5MB，静态链接，无 GLIBC 依赖）

### ARM64 (aarch64)

```bash
cargo zigbuild --target aarch64-unknown-linux-musl --release
```

产物: `target/aarch64-unknown-linux-musl/release/iedb-agent`
（约 6MB，静态链接）

### 预编译二进制

从 [GitHub Releases](https://github.com/1093181236-cloud/iedb-agent/releases) 下载：
- `iedb-agent-armv7` — ARM32（树莓派、嵌入式主板）
- `iedb-agent-aarch64` — ARM64（树莓派 4/5、AWS Graviton）

### CI/CD

项目使用 GitHub Actions 自动构建：
- **native** — macOS ARM64 原生构建 + 测试
- **arm64** — `aarch64-unknown-linux-musl` 交叉编译（zigbuild）
- **arm32** — `armv7-unknown-linux-musleabihf` 交叉编译（zigbuild）
- **release** — 标签推送时自动创建 GitHub Release 并附带二进制文件

## 部署

### ARM32 设备

```bash
# 下载预编译二进制
gh release download v0.1.1 --repo 1093181236-cloud/iedb-agent --pattern "*-armv7"

# 拷贝到设备
scp iedb-agent-armv7 root@192.168.1.100:/usr/local/bin/iedb-agent

# 在设备上创建配置
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

# 创建数据目录
mkdir -p /var/lib/iedb-agent/{wal,meta,staging}

# 运行
iedb-agent
```

### systemd 服务（推荐）

```ini
[Unit]
Description=IotEdgeDB Edge Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/iedb-agent
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

保存为 `/etc/systemd/system/iedb-agent.service`，然后：

```bash
systemctl daemon-reload
systemctl enable --now iedb-agent
systemctl status iedb-agent
```

### ARM32 配置调优建议

| 设备内存 | 建议 `memory_limit` | 建议 `snapshot_interval` |
|---------------|------------------------|-----------------------------|
| 256 MB | `"64MB"` | `"30s"` |
| 512 MB | `"128MB"` | `"60s"` |
| 1 GB | `"256MB"` | `"120s"` |

## 端到端测试

```bash
# 使用项目自带脚本
bash scripts/e2e_test.sh
```

或手动执行：

```bash
# 1. 启动 iotededb
iedb serve --config iedb.toml

# 1a. 如果启用了认证，创建 Agent Token:
curl -X POST "http://IOTEDGEDB_IP:8000/api/v1/auth/tokens" \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "edge-agent-01", "permissions": ["write"], "expires_in": "365d"}'
# 将返回的 token 写入 iedb-agent.toml: [iotedgedb] token = "..."

# 2. 在 ARM32 设备上启动 iedb-agent
# 3. 写入测试数据
curl -X POST "http://ARM32_IP:8080/write?db=test" \
  -d "cpu,host=srv01 cpu=75.5 $(date +%s)000000000"

# 4. 查询 Agent 内存缓冲
curl "http://ARM32_IP:8080/query?db=test&table=cpu"

# 5. 查询 iotededb（Parquet + Agent 缓冲联合查询）
curl -X POST "http://IOTEDGEDB_IP:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -H "x-iedb-database: test" \
  -d '{"sql":"SELECT * FROM cpu ORDER BY time"}'
```

## 开发

```bash
# 运行测试
cargo test                 # 65+ 单元测试
cargo test --test '*'      # 11 集成测试

# 查看测试覆盖率（需要 cargo-llvm-cov）
cargo llvm-cov --html

# 代码检查
cargo clippy -- -D warnings
cargo fmt -- --check
```

### 测试覆盖

| 测试类别 | 文件 | 覆盖内容 |
|------------|------|------------------|
| 单元测试 | `src/buffer/chunk.rs` | Schema 演化、Chunk CRUD、标签索引 |
| 单元测试 | `src/buffer/mod.rs` | Buffer CRUD、内存估算 |
| 单元测试 | `src/wal/wal_core.rs` | WAL 缓冲限流、刷写、apply_write_batch |
| 单元测试 | `src/wal/serialize.rs` | WAL 序列化往返、CRC 完整性 |
| 单元测试 | `src/flush/scheduler.rs` | end_time_marker 计算、Chunk 选择 |
| 单元测试 | `src/flush/parquet.rs` | Parquet 写入往返验证 |
| 单元测试 | `src/flush/http_upload.rs` | Staging 保存、目录结构 |
| 单元测试 | `src/flush/s3.rs` | S3 key 生成、agent/timestamp 分区 |
| 单元测试 | `src/agent/mod.rs` | 心跳变更检测、增删改、多库多表 |
| 单元测试 | `src/config.rs` | 配置解析、默认值、参数校验 |
| 集成测试 | `tests/write_query_integration_test.rs` | 端到端 write → query、多表、时间范围、多库、布尔/字符串类型、错误处理 |

## 依赖

全部为纯 Rust 依赖，零 C 依赖。ARM32/ARM64 兼容。

| Crate | 用途 |
|-------|---------|
| `hyper` | HTTP 服务器 |
| `reqwest` + `rustls` | HTTP 客户端、TLS（基于 ring） |
| `bitcode` | WAL 二进制序列化 |
| `parquet`（snap + flate2） | Parquet 文件写入（Snappy 压缩） |
| `influxdb-line-protocol` | Line Protocol 解析器 |
| `aws-sigv4` | S3 请求 AWS SigV4 签名 |
| `aws-credential-types` | AWS 凭证类型 |
| `aws-smithy-runtime-api` | AWS SDK 运行时抽象 |
| `tokio` | 异步运行时（多线程、IO、同步原语） |
| `serde` + `serde_json` | 配置、API 序列化/反序列化 |
| `toml` | TOML 配置文件解析 |
| `clap` | CLI 参数解析 |
| `tracing` + `tracing-subscriber` | 结构化日志 |
| `chrono` | 时间戳处理 |
| `crc32fast` | WAL CRC32 完整性校验 |
| `url` | URL 编解码 |

## 项目结构

```
iedb-agent/
├── Cargo.toml                          # 项目依赖与元数据
├── iedb-agent.toml                     # 默认配置文件
├── iedb-agent-arm32.toml.example       # ARM32 示例配置
├── src/
│   ├── main.rs                         # 入口：启动所有后台任务 + HTTP 服务
│   ├── lib.rs                          # 模块声明
│   ├── config.rs                       # 配置解析与校验
│   ├── agent/
│   │   └── mod.rs                      # Agent 注册、心跳、表变更计算
│   ├── buffer/
│   │   ├── mod.rs                      # Buffer: 多库多表管理
│   │   ├── chunk.rs                    # Chunk、Row、Schema、标签索引
│   │   └── query.rs                    # 查询引擎：时间/标签过滤
│   ├── flush/
│   │   ├── mod.rs                      # 模块声明
│   │   ├── scheduler.rs                # 快照调度器：定时 + 内存压力
│   │   ├── parquet.rs                  # Parquet 生成（归并排序 + 列写入）
│   │   ├── http_upload.rs              # HTTP 上传 + Staging 暂存
│   │   └── s3.rs                       # S3 上传（SigV4 签名）
│   ├── http/
│   │   ├── mod.rs                      # 模块声明
│   │   ├── write.rs                    # POST /write 处理器
│   │   └── query.rs                    # GET /query 处理器
│   └── wal/
│       ├── mod.rs                      # WAL 数据类型（WalOp、WriteBatch）
│       ├── wal_core.rs                 # WAL 管理器：缓冲、刷写、回放、清理
│       └── serialize.rs                # WAL 序列化（bitcode + CRC32）
├── tests/
│   └── write_query_integration_test.rs # 11 个集成测试
├── scripts/
│   └── e2e_test.sh                     # 端到端测试脚本
├── cross/
│   └── armv7-unknown-linux-gnueabihf.toml  # ARM32 交叉编译配置
├── .cargo/
│   └── config.toml                     # Cargo 构建配置
└── .github/workflows/
    └── build.yml                       # CI/CD：构建 + 测试 + 发布
```

## 许可证

MIT OR Apache-2.0
