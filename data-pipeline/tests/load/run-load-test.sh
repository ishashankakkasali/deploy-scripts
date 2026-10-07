#!/usr/bin/env bash
# Load Test for CCE Data Pipeline
# Inserts CloudEvents directly into PostgreSQL (source of truth) to simulate production load.
# Debezium then replicates them to ClickHouse via Kafka — tests the full CDC path.
#
# Usage: ./tests/load/run-load-test.sh [rows-per-second] [duration-seconds] [pg-host]
#
# Examples:
#   ./tests/load/run-load-test.sh 500 60          # 500 rps for 60 seconds
#   ./tests/load/run-load-test.sh 1000 300        # 1000 rps for 5 minutes

set -euo pipefail

RPS="${1:-100}"
DURATION="${2:-60}"
PG_HOST="${3:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-cce_app_user}"
PG_DB="${PG_DB:-ccedb}"

TOTAL=$((RPS * DURATION))
BATCH_SIZE=100
SLEEP_INTERVAL=$(echo "scale=4; ${BATCH_SIZE} / ${RPS}" | bc)

EVENT_TYPES=("org.cce.fhir.encounter" "org.cce.fhir.observation" "org.cce.fhir.medicationrequest" "org.cce.fhir.procedure" "org.cce.fhir.condition")
FACILITIES=("facility-001" "facility-002" "facility-003" "facility-004" "facility-005")
SOURCES=("emr-system-a" "emr-system-b" "ehr-gateway" "manual-entry" "api-ingest")

echo "=== CCE Pipeline Load Test (PostgreSQL direct insert) ==="
echo "Rate:       ${RPS} rows/second"
echo "Duration:   ${DURATION} seconds"
echo "Total:      ${TOTAL} rows"
echo "Batch size: ${BATCH_SIZE}"
echo "Target:     ${PG_HOST}:${PG_PORT}/${PG_DB}"
echo ""

# Verify PostgreSQL is reachable
if ! psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to PostgreSQL at ${PG_HOST}:${PG_PORT}/${PG_DB}"
    exit 1
fi
echo "✓ PostgreSQL reachable"
echo ""

generate_batch_sql() {
    local batch_start="$1"
    local batch_count="$2"
    local ts
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    echo "INSERT INTO inbound_event_log (id, cloudevents_id, source, event_type, subject, facility_id, raw_payload, status, received_at) VALUES"

    local sep=""
    for ((i = 0; i < batch_count; i++)); do
        local seq=$((batch_start + i))
        local event_type="${EVENT_TYPES[$((RANDOM % ${#EVENT_TYPES[@]}))]}"
        local facility="${FACILITIES[$((RANDOM % ${#FACILITIES[@]}))]}"
        local source="${SOURCES[$((RANDOM % ${#SOURCES[@]}))]}"
        local patient_id="patient-$(printf '%05d' $((RANDOM % 10000)))"
        local practitioner_id="doc-$(printf '%03d' $((RANDOM % 200)))"
        local event_id="load-${seq}-$(date +%s%N)"

        local payload="{\"specversion\":\"1.0\",\"id\":\"${event_id}\",\"type\":\"${event_type}\",\"source\":\"${source}\",\"facilityid\":\"${facility}\",\"time\":\"${ts}\",\"subject\":\"${patient_id}\",\"data\":{\"resourceType\":\"Encounter\",\"id\":\"enc-${seq}\",\"subject\":{\"reference\":\"Patient/${patient_id}\"},\"participant\":[{\"individual\":{\"reference\":\"Practitioner/${practitioner_id}\"}}]}}"

        echo "${sep}(gen_random_uuid(), '${event_id}', '${source}', '${event_type}', '${patient_id}', '${facility}', '${payload}', 'ACCEPTED', now())"
        sep=","
    done
    echo ";"
}

SENT=0
START_TIME=$(date +%s)

echo "Starting load test at $(date -u +%H:%M:%S)..."
echo ""

while [[ $SENT -lt $TOTAL ]]; do
    REMAINING=$((TOTAL - SENT))
    CURRENT_BATCH=$((BATCH_SIZE < REMAINING ? BATCH_SIZE : REMAINING))

    # Generate and execute batch SQL
    generate_batch_sql "$SENT" "$CURRENT_BATCH" | \
        psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q

    SENT=$((SENT + CURRENT_BATCH))

    # Progress every 10 batches
    if ((SENT % (RPS * 10) == 0 || SENT >= TOTAL)); then
        ELAPSED=$(($(date +%s) - START_TIME))
        ACTUAL_RATE=$((SENT / (ELAPSED + 1)))
        echo "  Sent: ${SENT}/${TOTAL} | Elapsed: ${ELAPSED}s | Actual rate: ~${ACTUAL_RATE} rps"
    fi

    [[ $SENT -lt $TOTAL ]] && sleep "$SLEEP_INTERVAL"
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
ACTUAL_RATE=$((TOTAL / (ELAPSED + 1)))

echo ""
echo "=== Load Test Complete ==="
echo "Total inserted: ${TOTAL} rows into PostgreSQL"
echo "Duration:       ${ELAPSED}s"
echo "Actual rate:    ~${ACTUAL_RATE} rows/second"
echo ""
echo "--- Verification ---"
echo "Wait ~30s for Debezium → Kafka → ClickHouse to replicate, then run:"
echo "  curl 'http://localhost:8123/?database=cce_analytics' --data-binary \\"
echo "    \"SELECT count() FROM inbound_event_logs WHERE received_at >= now() - INTERVAL $((DURATION + 60)) SECOND\""
