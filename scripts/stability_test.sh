#!/bin/bash
set -euo pipefail

# ============================================================
# iedb-agent 长期稳定性测试 — 持续推送实时数据
#
# 用法:
#   ./stability_test.sh [duration_seconds] [batch_interval_ms]
#
#   默认: 无限运行, 每 1000ms 推送一批
#   Ctrl+C 停止, 自动打印汇总统计
#
#   示例:
#     ./stability_test.sh              # 无限运行, 1 batch/s
#     ./stability_test.sh 3600         # 运行 1 小时
#     ./stability_test.sh 86400 500    # 运行 24 小时, 2 batch/s
# ============================================================

# ── 配置 ────────────────────────────────────────────────────
AGENT_HOST="${AGENT_HOST:-192.168.0.230}"
AGENT_PORT="${AGENT_PORT:-8080}"
AGENT="http://${AGENT_HOST}:${AGENT_PORT}"
DURATION="${1:-0}"           # 0 = 无限
BATCH_INTERVAL="${2:-1}"     # 批次间隔（秒），支持小数如 0.5
REPORT_INTERVAL="${REPORT_INTERVAL:-30}"  # 统计报告间隔（秒）

# ── 模拟数据参数 ─────────────────────────────────────────────
HOSTS=("srv01" "srv02" "srv03" "srv04" "srv05")
REGIONS=("us-east" "us-west" "eu-central" "ap-south" "ap-northeast")
DBS=("metrics" "sensors" "logs")
METRIC_TYPES=("cpu" "mem" "disk" "net")

# ── 颜色 ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── 统计 ────────────────────────────────────────────────────
TOTAL_BATCHES=0
TOTAL_ROWS=0
SUCCESS_COUNT=0
ERROR_COUNT=0
LAST_REPORT_TIME=$(date +%s)
START_TIME=$(date +%s)
LINE_CLEAR="\033[2K\r"

# 累计写入字节数
TOTAL_BYTES=0

# ── 清理 ────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo ""
    echo "============================================"
    echo -e "${CYAN}  Stability Test Summary${NC}"
    echo "============================================"
    local elapsed
    elapsed=$(($(date +%s) - START_TIME))
    local hours=$((elapsed / 3600))
    local mins=$(((elapsed % 3600) / 60))
    local secs=$((elapsed % 60))

    printf "  Duration:       %02dh %02dm %02ds\n" "$hours" "$mins" "$secs"
    echo "  Total batches:  ${TOTAL_BATCHES}"
    echo "  Total rows:     ${TOTAL_ROWS}"
    echo "  Total bytes:    ${TOTAL_BYTES}"
    echo "  Success:        ${SUCCESS_COUNT}"
    echo "  Errors:         ${ERROR_COUNT}"
    if [ "$TOTAL_BATCHES" -gt 0 ]; then
        local rate
        # max(elapsed, 1) 防止运行不足 1 秒时 awk 除零
        rate=$(awk "BEGIN { printf \"%.2f\", ${TOTAL_BATCHES} / (${elapsed} > 1 ? ${elapsed} : 1) }")
        echo "  Avg rate:       ${rate} batches/s"
        local success_rate
        success_rate=$(awk "BEGIN { printf \"%.2f\", ${SUCCESS_COUNT} * 100.0 / ${TOTAL_BATCHES} }")
        echo "  Success rate:   ${success_rate}%"
    fi

    # 最终健康检查
    echo ""
    local health
    health=$(curl -s -o /dev/null -w '%{http_code}' "${AGENT}/health" 2>/dev/null || echo "unreachable")
    echo "  Agent health:   HTTP ${health}"

    exit 0
}
trap cleanup SIGINT SIGTERM

# ── 预检 ────────────────────────────────────────────────────
echo -e "${CYAN}=== iedb-agent Stability Test ===${NC}"
echo "  Agent:   ${AGENT}"
echo "  Duration: $([ "$DURATION" -eq 0 ] && echo 'indefinite' || echo "${DURATION}s")"
echo "  Interval: ${BATCH_INTERVAL}s per batch"
echo "  Report:   every ${REPORT_INTERVAL}s"
echo ""

# 检查 agent 是否可达
HEALTH=$(curl -s -o /dev/null -w '%{http_code}' "${AGENT}/health" 2>/dev/null || echo "000")
if [ "$HEALTH" != "200" ]; then
    echo -e "${RED}[FATAL] Agent not reachable at ${AGENT} (HTTP ${HEALTH})${NC}"
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Agent reachable at ${AGENT}"

echo ""
echo -e "${YELLOW}Starting data stream... (Ctrl+C to stop)${NC}"
echo ""

# ── 主循环 ──────────────────────────────────────────────────
BATCH_NUM=0

while true; do
    # 检查运行时长
    if [ "$DURATION" -gt 0 ] && [ $(($(date +%s) - START_TIME)) -ge "$DURATION" ]; then
        break
    fi

    BATCH_NUM=$((BATCH_NUM + 1))

    # 随机选择: 1-3 个数据库, 1-3 个指标类型
    NUM_DBS=$((1 + RANDOM % 3))
    NUM_METRICS=$((1 + RANDOM % 2))  # 每批 1-2 个 measurement

    BATCH_ROWS=0
    BATCH_HAS_ERROR=false

    for _ in $(seq 1 $NUM_DBS); do
        DB="${DBS[$((RANDOM % ${#DBS[@]}))]}"

        for _ in $(seq 1 $NUM_METRICS); do
            METRIC="${METRIC_TYPES[$((RANDOM % ${#METRIC_TYPES[@]}))]}"
            HOST="${HOSTS[$((RANDOM % ${#HOSTS[@]}))]}"
            REGION="${REGIONS[$((RANDOM % ${#REGIONS[@]}))]}"

            # 生成带真实趋势的时序数据
            T=$((BATCH_NUM % 3600))
            USAGE=$(awk "BEGIN { printf \"%.3f\", 50 + 25 * sin($T / 60 * 3.14159) + rand() * 10 }")
            IOPS=$(awk "BEGIN { printf \"%.1f\", 200 + 100 * sin($T / 120 * 3.14159 + 0.5) + rand() * 30 }")
            LATENCY=$(awk "BEGIN { printf \"%.2f\", 1.5 + sin($T / 90 * 3.14159 + 1.2) + rand() * 0.5 }")

            # 构建 Line Protocol
            # 格式: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 <timestamp_ns>
            LP="${METRIC},host=${HOST},region=${REGION} usage=${USAGE},iops=${IOPS},latency=${LATENCY}"

            # 发送请求（瞬时失败自动重试，最多 3 次）
            HTTP_CODE="000"
            for RETRY in 1 2 3; do
                HTTP_CODE=$(curl -s -m 10 -o /dev/null -w '%{http_code}' \
                    -X POST "${AGENT}/write?db=${DB}" \
                    -H 'Content-Type: text/plain' \
                    -d "$LP" 2>/dev/null || echo "000")
                if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ]; then
                    break
                fi
                if [ "$RETRY" -lt 3 ]; then sleep 0.5; fi
            done

            if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ]; then
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
                BATCH_ROWS=$((BATCH_ROWS + 1))
                TOTAL_BYTES=$((TOTAL_BYTES + ${#LP}))
            else
                ERROR_COUNT=$((ERROR_COUNT + 1))
                BATCH_HAS_ERROR=true
            fi
        done
    done

    TOTAL_BATCHES=$((TOTAL_BATCHES + 1))
    TOTAL_ROWS=$((TOTAL_ROWS + BATCH_ROWS))

    # ── 定期统计报告 ────────────────────────────────────────
    NOW=$(date +%s)
    if [ $((NOW - LAST_REPORT_TIME)) -ge "$REPORT_INTERVAL" ]; then
        LAST_REPORT_TIME=$NOW
        ELAPSED=$((NOW - START_TIME))
        H=$((ELAPSED / 3600))
        M=$(((ELAPSED % 3600) / 60))
        S=$((ELAPSED % 60))

        RATE=$(awk "BEGIN { printf \"%.1f\", ${TOTAL_BATCHES} / ${ELAPSED} }")
        if [ "$TOTAL_BATCHES" -gt 0 ]; then
            ERR_PCT=$(awk "BEGIN { printf \"%.2f\", ${ERROR_COUNT} * 100.0 / ${TOTAL_BATCHES} }")
        else
            ERR_PCT="0.00"
        fi

        # 查询 agent 内存缓冲状态
        BUF_QUERY=$(curl -s -m 5 "${AGENT}/query?db=metrics&table=cpu" 2>/dev/null || echo '{"rows":[]}')
        # 注意: grep 无匹配时返回 1，pipefail 会杀死脚本，必须 || true
        BUF_COUNT=$(echo "$BUF_QUERY" | grep -o '"time"' | wc -l | tr -d ' ' || true)

        # 健康状态
        AGENT_HEALTH=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "${AGENT}/health" 2>/dev/null || echo "down")

        STATUS_ICON=$([ "$AGENT_HEALTH" = "200" ] && echo -e "${GREEN}●${NC}" || echo -e "${RED}●${NC}")

        printf "${LINE_CLEAR}${STATUS_ICON} %s | batches: %6d | rows: %8d | rate: %5s/s | err: %6s%% | buf(cpu): %5d | " \
            "$(date '+%H:%M:%S')" \
            "$TOTAL_BATCHES" \
            "$TOTAL_ROWS" \
            "$RATE" \
            "$ERR_PCT" \
            "$BUF_COUNT"
    fi

    # 错误时即时输出
    if [ "$BATCH_HAS_ERROR" = true ]; then
        echo ""
        echo -e "  ${RED}[ERROR]${NC} Write failure at $(date '+%H:%M:%S')"
    fi

    # 间隔控制
    sleep "$BATCH_INTERVAL"
done

# 正常结束（非 Ctrl+C）
cleanup
