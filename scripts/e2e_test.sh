#!/bin/bash
set -euo pipefail

ARM32="192.168.0.230"
IOTEDGEDB="http://192.168.0.118:8000"
AGENT="http://${ARM32}:8080"

echo "=== Step 1: Write historical data (will be flushed to Parquet) ==="
TIMESTAMP_OLD=$(date +%s%N)
sleep 1

# Write multiple lines — these will age past snapshot_interval
for i in $(seq 1 100); do
  curl -s -X POST "${AGENT}/write?db=testdb" \
    -d "cpu,host=srv01 cpu=$(echo "scale=1; $RANDOM/100" | bc),mem=$(echo "scale=1; $RANDOM/100" | bc) ${TIMESTAMP_OLD}000000" > /dev/null
done
echo "Wrote 100 historical rows at $(date -r $((TIMESTAMP_OLD/1000000000)))"

echo ""
echo "=== Step 2: Wait for snapshot_interval (35s) to trigger flush ==="
echo "Waiting 35 seconds for snapshot_interval (30s) + buffer..."
for i in $(seq 35 -1 1); do
  printf "\r  %2d seconds remaining..." $i
  sleep 1
done
echo ""
echo "Flush should have completed."

echo ""
echo "=== Step 3: Write recent data (stays in memory buffer) ==="
TIMESTAMP_NEW=$(date +%s%N)
for i in $(seq 1 50); do
  curl -s -X POST "${AGENT}/write?db=testdb" \
    -d "cpu,host=srv02 cpu=$(echo "scale=1; $RANDOM/100" | bc),dsk=$(echo "scale=1; $RANDOM/100" | bc) ${TIMESTAMP_NEW}000000" > /dev/null
done
echo "Wrote 50 recent rows at $(date -r $((TIMESTAMP_NEW/1000000000)))"

echo ""
echo "=== Step 4: Verify agent memory buffer has recent data ==="
RESP=$(curl -s "${AGENT}/query?db=testdb&table=cpu&tag=host=srv02")
echo "Agent query (tag=host=srv02):"
echo "$RESP" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'  {len(d[\"rows\"])} rows in memory buffer')" 2>/dev/null || echo "$RESP" | head -3

echo ""
echo "=== Step 5: Query iotededb for full data (Parquet + agent buffer) ==="
echo "Query iotededb for all cpu data:"
curl -s -X POST "${IOTEDGEDB}/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM cpu WHERE time > '\''2024-01-01'\'' ORDER BY time"}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'  Total rows: {len(d.get(\"rows\",d))}')" 2>/dev/null || echo "  (raw response above)"

echo ""
echo "=== Test Complete ==="
echo "Expected: historical data (100 rows via Parquet) + recent data (50 rows via agent buffer) = ~150 rows total"
