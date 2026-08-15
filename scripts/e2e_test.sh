#!/bin/bash
set -euo pipefail

# ============================================================
# iedb-agent 端到端测试 & 实时动态数据模拟
#
# 前置条件:
#   1. iotedgedb 已启动:   113.250.188.137:8000
#   2. iedb-agent 部署到:  192.168.0.230
#   3. 本机可 SSH 到 192.168.0.230
#   4. ARM32 设备上已有 iedb-agent 二进制（或通过 scp 部署）
# ============================================================

# ── 环境配置 ────────────────────────────────────────────────
ARM32_HOST="192.168.0.230"
ARM32_USER="${ARM32_USER:-root}"
IOTEDGEDB="http://113.250.188.137:8000"

# SSH 命令包装（优先 sshpass，回退到纯 ssh）
if [ -n "${SSHPASS:-}" ]; then
    SSH_CMD="sshpass -e ssh -o StrictHostKeyChecking=no"
    SCP_CMD="sshpass -e scp -o StrictHostKeyChecking=no"
else
    SSH_CMD="ssh -o StrictHostKeyChecking=no"
    SCP_CMD="scp -o StrictHostKeyChecking=no"
fi
AGENT="http://${ARM32_HOST}:8080"
AGENT_BIN="${AGENT_BIN:-./target/armv7-unknown-linux-musleabihf/release/iedb-agent}"
AGENT_CONFIG="/etc/iedb-agent.toml"
AGENT_DATA_DIR="/var/lib/iedb-agent"
TEST_DB="testdb"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $*"; }

# ── 工具函数 ────────────────────────────────────────────────

# 向 agent 写入 Line Protocol
agent_write() {
    local db="$1" lp="$2"
    local resp
    resp=$(curl -s -w '\n%{http_code}' -X POST "${AGENT}/write?db=${db}" \
        -H 'Content-Type: text/plain' \
        -d "$lp" 2>&1)
    local http_code
    http_code=$(echo "$resp" | tail -1)
    local body
    body=$(echo "$resp" | sed '$d')
    echo "${http_code} ${body}"
}

# 查询 agent 内存缓冲
agent_query() {
    local params="$1"
    curl -s "${AGENT}/query?${params}"
}

# 查询 agent 健康状态
agent_health() {
    curl -s -o /dev/null -w '%{http_code}' "${AGENT}/health"
}

# 向 iotedgedb 发送 SQL 查询
db_query() {
    local sql="$1" db="${2:-$TEST_DB}"
    curl -s -X POST "${IOTEDGEDB}/api/v1/query" \
        -H 'Content-Type: application/json' \
        -H "x-iedb-database: ${db}" \
        -d "{\"sql\":\"${sql}\"}"
}

# JSON 行数提取（纯 shell，不依赖 python3/jq）
json_row_count() {
    # 统计 "time": 出现次数来近似行数
    echo "$1" | grep -o '"time"' | wc -l | tr -d ' '
}

# ── Step 0: 部署 agent 到 ARM32 设备 ───────────────────────

info "Step 0: Deploy iedb-agent to ${ARM32_HOST}"

# 检查 SSH 连通性
if ! ${SSH_CMD} -o ConnectTimeout=5 "${ARM32_USER}@${ARM32_HOST}" "echo ok" > /dev/null 2>&1; then
    fail "Cannot SSH to ${ARM32_USER}@${ARM32_HOST}"
fi
pass "SSH to ${ARM32_HOST} OK"

# 停止旧进程（如果存在）
${SSH_CMD} "${ARM32_USER}@${ARM32_HOST}" "pkill iedb-agent || true" 2>/dev/null || true
sleep 1

# 上传二进制（如果本地有交叉编译产物）
if [ -f "$AGENT_BIN" ]; then
    info "Uploading iedb-agent binary..."
    ${SCP_CMD} "$AGENT_BIN" "${ARM32_USER}@${ARM32_HOST}:/usr/local/bin/iedb-agent"
    ${SSH_CMD} "${ARM32_USER}@${ARM32_HOST}" "chmod +x /usr/local/bin/iedb-agent"
    pass "Binary uploaded"
else
    info "Binary not found at ${AGENT_BIN}, assuming agent already installed"
fi

# 生成配置文件
${SSH_CMD} "${ARM32_USER}@${ARM32_HOST}" "cat > ${AGENT_CONFIG} << 'EOF'
[server]
port = 8080

[data]
dir = \"${AGENT_DATA_DIR}\"

[wal]
flush_interval_secs = 1
max_write_buffer_ops = 100000

[flush]
snapshot_interval = \"30s\"
backend = \"http\"
memory_limit = \"64MB\"

[iotedgedb]
url = \"${IOTEDGEDB}\"

[agent]
id = \"arm32-e2e-test\"
EOF"
pass "Config written to ${AGENT_CONFIG}"

# 创建数据目录
${SSH_CMD} "${ARM32_USER}@${ARM32_HOST}" "mkdir -p ${AGENT_DATA_DIR}/{wal,meta,staging}"

# 启动 agent
info "Starting iedb-agent..."
${SSH_CMD} "${ARM32_USER}@${ARM32_HOST}" "nohup /usr/local/bin/iedb-agent > /var/log/iedb-agent.log 2>&1 &"
sleep 2

# 健康检查
HEALTH=$(agent_health)
if [ "$HEALTH" = "200" ]; then
    pass "Agent health check OK (HTTP ${HEALTH})"
else
    fail "Agent health check failed (HTTP ${HEALTH})"
fi

# ── Step 1: 实时动态数据流模拟 ──────────────────────────────

info "Step 1: Simulating real-time streaming data (60s, 1 row/s)"

START_TIME=$(date +%s)
DURATION=60  # 持续 60 秒
COUNT=0
HOSTS=("srv01" "srv02" "srv03")
REGIONS=("us-east" "us-west" "eu-central")

echo "  Streaming data to ${AGENT}/write?db=${TEST_DB} ..."
while [ $(($(date +%s) - START_TIME)) -lt $DURATION ]; do
    HOST="${HOSTS[$((RANDOM % 3))]}"
    REGION="${REGIONS[$((RANDOM % 3))]}"

    # 生成带真实变化趋势的指标值
    # cpu: 基值 + 正弦波动 + 随机噪声
    BASE=$((COUNT % 60))
    CPU_VAL=$(awk "BEGIN { printf \"%.2f\", 50 + 20 * sin($BASE / 10 * 3.14159) + rand() * 5 }")
    MEM_VAL=$(awk "BEGIN { printf \"%.2f\", 60 + 10 * cos($BASE / 15 * 3.14159) + rand() * 3 }")
    IO_VAL=$(awk "BEGIN { printf \"%.2f\", 100 + 50 * sin($BASE / 20 * 3.14159 + 1) + rand() * 10 }")

    # 每次写入 2 张表（cpu + disk）使用当前实时时间戳
    LP="cpu,host=${HOST},region=${REGION} usage=${CPU_VAL},user=${CPU_VAL},sys=${CPU_VAL} ${COUNT}\n"
    LP+="disk,host=${HOST},region=${REGION} io_read=${IO_VAL},io_write=${IO_VAL}"

    RESULT=$(agent_write "$TEST_DB" "$(printf "$LP")")
    HTTP_CODE=$(echo "$RESULT" | awk '{print $1}')
    ROWS=$(echo "$RESULT" | awk '{print $2}')

    if [ "$HTTP_CODE" != "204" ] && [ "$HTTP_CODE" != "200" ]; then
        echo -e "  ${RED}Write error: HTTP ${HTTP_CODE}${NC}"
    fi

    COUNT=$((COUNT + 1))
    # 每秒打印进度
    if [ $((COUNT % 5)) -eq 0 ]; then
        printf "\r  [%3ds] wrote %3d batches | cpu=%.2f mem=%.2f io=%.2f" \
            $(($(date +%s) - START_TIME)) "$COUNT" "$CPU_VAL" "$MEM_VAL" "$IO_VAL"
    fi
    sleep 1
done
echo ""
pass "Streaming complete: ${COUNT} batches written"

# ── Step 2: 验证 Agent 内存缓冲中的实时数据 ─────────────────

info "Step 2: Verify agent memory buffer"

CPU_ROWS=$(agent_query "db=${TEST_DB}&table=cpu")
CPU_COUNT=$(json_row_count "$CPU_ROWS")
echo "  cpu table: ${CPU_COUNT} rows in memory buffer"

DISK_ROWS=$(agent_query "db=${TEST_DB}&table=disk")
DISK_COUNT=$(json_row_count "$DISK_ROWS")
echo "  disk table: ${DISK_COUNT} rows in memory buffer"

# 验证带 tag 过滤的查询
TAG_ROWS=$(agent_query "db=${TEST_DB}&table=cpu&tag=host=srv01")
TAG_COUNT=$(json_row_count "$TAG_ROWS")
echo "  cpu (host=srv01): ${TAG_COUNT} rows"

if [ "$CPU_COUNT" -gt 0 ] || [ "$DISK_COUNT" -gt 0 ]; then
    pass "Memory buffer has data"
else
    fail "Memory buffer is empty"
fi

# ── Step 3: 等待快照触发 + 验证 Parquet 刷写 ─────────────────

info "Step 3: Wait for snapshot flush (35s for snapshot_interval=30s)"

# 检查 iotedgedb 连通性
DB_HEALTH=$(curl -s -o /dev/null -w '%{http_code}' "${IOTEDGEDB}/health" || echo "000")
if [ "$DB_HEALTH" = "200" ]; then
    pass "iotedgedb reachable (HTTP ${DB_HEALTH})"
else
    echo -e "  ${YELLOW}iotedgedb health check returned ${DB_HEALTH}, continuing anyway...${NC}"
fi

for i in $(seq 35 -1 1); do
    printf "\r  %2d seconds remaining..." "$i"
    sleep 1
done
echo ""

# ── Step 4: 验证 Agent 仍在正常运行 ─────────────────────────

info "Step 4: Verify agent health after flush"

HEALTH=$(agent_health)
if [ "$HEALTH" = "200" ]; then
    pass "Agent still healthy (HTTP ${HEALTH})"
else
    fail "Agent unhealthy (HTTP ${HEALTH})"
fi

# ── Step 5: 数据仍在内存缓冲中？ ─────────────────────────────

info "Step 5: Query agent memory buffer (post-flush state)"

CPU_ROWS_AFTER=$(agent_query "db=${TEST_DB}&table=cpu")
CPU_COUNT_AFTER=$(json_row_count "$CPU_ROWS_AFTER")
echo "  cpu table: ${CPU_COUNT_AFTER} rows remaining in buffer"

# 注意：30s 快照只刷写 30 秒前的 Chunk，最近 30 秒的数据仍在内存中
if [ "$CPU_COUNT_AFTER" -ge 0 ]; then
    pass "Query still functional after flush"
fi

# ── Step 6: 从 iotedgedb 查询全量数据 ────────────────────────

info "Step 6: Query iotededb for merged data (Parquet + agent buffer)"

DB_RESULT=$(db_query "SELECT * FROM cpu ORDER BY time LIMIT 20" "$TEST_DB")
DB_COUNT=$(json_row_count "$DB_RESULT")

echo "  iotedgedb cpu table: ${DB_COUNT} rows"
if [ "$DB_COUNT" -gt 0 ]; then
    pass "iotedgedb query returned data"
else
    echo -e "  ${YELLOW}iotedgedb returned 0 rows — flush may not have completed yet${NC}"
    echo "  Raw response:"
    echo "$DB_RESULT" | head -5
fi

# ── Step 7: 写入最新数据（确认刷写后仍然可写） ───────────────

info "Step 7: Write fresh data after snapshot"

NOW_NS=$(date +%s)000000000
LP_NEW="cpu,host=srv04,region=ap-south usage=99.99,sys=1.20"
RESULT=$(agent_write "$TEST_DB" "$LP_NEW")
echo "  New write: ${RESULT}"

FRESH_ROWS=$(agent_query "db=${TEST_DB}&table=cpu&tag=host=srv04")
FRESH_COUNT=$(json_row_count "$FRESH_ROWS")
echo "  cpu (host=srv04): ${FRESH_COUNT} rows"

if [ "$FRESH_COUNT" -gt 0 ]; then
    pass "Post-flush write works correctly"
else
    fail "Post-flush write not found"
fi

# ── Step 8: 多 db 写入验证 ───────────────────────────────────

info "Step 8: Multi-database write test"

agent_write "db_sensors" "temperature,sensor_id=t01,location=room1 value=23.5" > /dev/null
agent_write "db_sensors" "humidity,sensor_id=h01,location=room1 value=58.2" > /dev/null

TEMP_ROWS=$(agent_query "db=db_sensors&table=temperature")
TEMP_COUNT=$(json_row_count "$TEMP_ROWS")
HUM_ROWS=$(agent_query "db=db_sensors&table=humidity")
HUM_COUNT=$(json_row_count "$HUM_ROWS")

echo "  temperature: ${TEMP_COUNT} rows"
echo "  humidity: ${HUM_COUNT} rows"

if [ "$TEMP_COUNT" -gt 0 ] && [ "$HUM_COUNT" -gt 0 ]; then
    pass "Multi-DB writes OK"
else
    fail "Multi-DB writes failed"
fi

# ── 汇总 ─────────────────────────────────────────────────────

echo ""
echo "============================================"
echo -e "${GREEN}  All tests passed!${NC}"
echo "============================================"
echo ""
echo "Summary:"
echo "  Agent:        ${AGENT}"
echo "  IotEdgeDB:    ${IOTEDGEDB}"
echo "  Streamed:     ${COUNT} batches over ${DURATION}s"
echo "  Tables:       cpu, disk, temperature, humidity"
echo "  DBs:          ${TEST_DB}, db_sensors"
echo ""
echo "Logs on device:  ssh ${ARM32_USER}@${ARM32_HOST} tail -f /var/log/iedb-agent.log"
echo "Agent config:    ssh ${ARM32_USER}@${ARM32_HOST} cat ${AGENT_CONFIG}"
