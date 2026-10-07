#!/usr/bin/env bash
# =================================================================================================
# migrate-1x-to-2x.sh — upgrade a CCE 1.x database to 2.0 IN PLACE (database migration), step by step
# =================================================================================================
# The 2.0 services reshape the 1.x tables with their own Flyway migrations (Protocol V2, Matcher V2-V7),
# baselined at 1. Nothing is replayed. Written to test this path on a copy of a deployment (e.g. a
# laptop with a restored 1.x database) and compare it with the replay upgrade; see MIGRATION-TEST-RUNBOOK.md.
#
#   migrate-1x-to-2x.sh check                  what the migration needs, and what Matcher V2 would refuse
#   migrate-1x-to-2x.sh snapshot               save the 1.x numbers 'report' compares against
#   migrate-1x-to-2x.sh backup                 pg_dump of the database
#   migrate-1x-to-2x.sh stop-1x --yes          stop and remove the 1.x services (compliance, scheduler)
#   migrate-1x-to-2x.sh fix-orphans --yes      null step links to events that are nowhere (Matcher V2 check 3)
#   migrate-1x-to-2x.sh deploy-2x              create the 2.0 services, stopped, with Flyway baseline 1
#   migrate-1x-to-2x.sh copy-offsets --yes     Matcher's consumer group starts where compliance stopped
#   migrate-1x-to-2x.sh migrate --yes          Protocol V2, then Matcher V2-V7, then Protocol again
#   migrate-1x-to-2x.sh verify-schema          Matcher's migration/verify.sql
#   migrate-1x-to-2x.sh fix-history-ids --yes  identity history ids -> sequence-backed (else Step SLA cannot start)
#   migrate-1x-to-2x.sh start --yes            start Step SLA (Matcher and Protocol already run)
#   migrate-1x-to-2x.sh rebuild-clickhouse --yes   ClickHouse rebuilt from the migrated Postgres, then Insights
#   migrate-1x-to-2x.sh verify                 lag, DLQ, unprocessed events, Postgres vs ClickHouse
#   migrate-1x-to-2x.sh report [--replay-report <file>]   1.x vs migrated (vs a replay report), as markdown
#   migrate-1x-to-2x.sh status                 services and row counts
#
#   every command takes --config <file> (default: migration.env next to this script)
# =================================================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '[migrate %s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$*"; }
die()  { printf '\n  \033[31mSTOP\033[0m %s\n\n' "$*" >&2; exit 1; }
trap 'printf "\n  \033[31mSTOP\033[0m the %s step failed unexpectedly (line %s). The error is shown above.\n\n" "${COMMAND:-?}" "$LINENO" >&2' ERR

CONFIG_FILE="$SCRIPT_DIR/migration.env"; COMMAND=""; YES=""; REPLAY_REPORT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONFIG_FILE="${2:-}"; shift 2;;
    --yes) YES=1; shift;;
    --replay-report) REPLAY_REPORT="${2:-}"; shift 2;;
    -*) die "Unknown option: $1";;
    *) [ -z "$COMMAND" ] && COMMAND="$1" || die "One command at a time."; shift;;
  esac
done
[ -n "$COMMAND" ] || { sed -n '2,25p' "$0"; exit 1; }
[ -f "$CONFIG_FILE" ] || die "No settings file at $CONFIG_FILE."
# shellcheck disable=SC1090
. "$CONFIG_FILE"

# ---- settings (defaults) ---------------------------------------------------------------------------
NAME="${NAME:-migration}"
STATE_DIR="${STATE_DIR:-$HOME/cce-migration/$NAME}"
COMPOSE_DIR="${COMPOSE_DIR:?Set COMPOSE_DIR in $CONFIG_FILE}"
COMPOSE_FILES="${COMPOSE_FILES:?Set COMPOSE_FILES (1.x file, 2.0 file, baseline-1 override) in $CONFIG_FILE}"
OLD_SERVICES="${OLD_SERVICES:-cce-compliance-service cce-scheduler-service}"
PROTOCOL="${PROTOCOL:-cce-protocol-service}"; MATCHER="${MATCHER:-cce-matcher-service}"
STEP_SLA="${STEP_SLA:-cce-step-sla-service}"
INSIGHTS_SERVICES="${INSIGHTS_SERVICES-cce-insights-service cce-insights-ui}"
PG_EXEC="${PG_EXEC:?Set PG_EXEC, e.g. \"docker exec -i cce-postgres\"}"; PG_DB="${PG_DB:-ccedb}"
KAFKA_EXEC="${KAFKA_EXEC:?Set KAFKA_EXEC, e.g. \"docker exec -i kafka\"}"
KAFKA_TOOLS_DIR="${KAFKA_TOOLS_DIR:-}"; KAFKA_TOOL_SUFFIX="${KAFKA_TOOL_SUFFIX:-}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
INBOUND_TOPIC="${INBOUND_TOPIC:-cce.events.inbound}"; DLQ_TOPIC="${DLQ_TOPIC:-$INBOUND_TOPIC.dlq}"
OLD_GROUP="${OLD_GROUP:-cce-compliance-service}"; NEW_GROUP="${NEW_GROUP:-cce-matcher-service}"
CONNECT_EXEC="${CONNECT_EXEC:?Set CONNECT_EXEC, e.g. \"docker exec -i cce-kafka-connect\"}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"; CONNECTOR="${CONNECTOR:-cce-ccedb-source}"
CDC_SLOT="${CDC_SLOT:-cce_analytics_slot}"
CH_HTTP="${CH_HTTP:-http://localhost:8123}"; CH_DB="${CH_DB:-cce_analytics}"
if [ -n "${SECRETS_FILE:-}" ]; then [ -r "$SECRETS_FILE" ] || die "Cannot read $SECRETS_FILE"; . "$SECRETS_FILE"; fi
CH_USER="${CH_USER:-${CLICKHOUSE_USER:-cce_pipeline}}"; CH_PASSWORD="${CH_PASSWORD:-${CLICKHOUSE_PASSWORD:-}}"
DATA_PIPELINE_DIR="${DATA_PIPELINE_DIR:?Set DATA_PIPELINE_DIR (the 2.0 data-pipeline)}"
MATCHER_MIGRATION_DIR="${MATCHER_MIGRATION_DIR:?Set MATCHER_MIGRATION_DIR (cce-matcher-service/migration, for verify.sql)}"
START_TIMEOUT="${START_TIMEOUT:-300}"
mkdir -p "$STATE_DIR"

# ---- helpers -------------------------------------------------------------------------------------
psql_q() { $PG_EXEC sh -c "psql -U \"\${POSTGRES_USER:-postgres}\" -d $PG_DB -v ON_ERROR_STOP=1 -X -q -At -F '|'"; }
sql()    { printf '%s\n' "$1" | psql_q; }
compose() { (cd "$COMPOSE_DIR" && COMPOSE_FILE="$COMPOSE_FILES" docker compose "$@"); }
kafka()  { local tool="$1"; shift; $KAFKA_EXEC "${KAFKA_TOOLS_DIR:+$KAFKA_TOOLS_DIR/}$tool$KAFKA_TOOL_SUFFIX" --bootstrap-server "$KAFKA_BOOTSTRAP" "$@"; }
connect() { local m="$1" p="$2"; shift 2; $CONNECT_EXEC curl -s -X "$m" -H 'Content-Type: application/json' "$CONNECT_URL$p" "$@"; }
ch()     { curl -s -u "$CH_USER:$CH_PASSWORD" --data-binary "$1" "$CH_HTTP/"; }
state()  { local st; st=$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null | head -1) || true; echo "${st:-absent}"; }
words()  { echo $*; }   # collapse a whitespace-separated list
need_yes() { [ -n "$YES" ] || die "This step changes something. Re-run it with --yes once you have read what it does."; }
table_exists() { [ "$(sql "SELECT to_regclass('public.$1') IS NOT NULL")" = t ]; }
wait_started() {   # $1 = container, $2 = UTC start time: until Spring Boot says Started (0) or failed (1)
  # The log is read into a variable first: 'docker logs | grep -q' breaks under pipefail (grep stops
  # at the first match, docker logs gets SIGPIPE, and the pipeline reads as "not found").
  local waited=0 out
  while :; do
    out=$(docker logs --since "$2" "$1" 2>&1 || true)
    if grep -qE 'APPLICATION FAILED TO START|Application run failed' <<<"$out"; then return 1; fi
    if grep -qE 'Started [A-Za-z]+ in [0-9.]+ seconds' <<<"$out"; then return 0; fi
    sleep 5; waited=$((waited + 5)); [ "$waited" -le "$START_TIMEOUT" ] || die "$1 did not finish starting in ${START_TIMEOUT}s (docker logs $1)."
  done
}
start_fresh() {   # start $1 so that it logs a new start-up: stop it first if it is already running (a repeated 'migrate')
  [ "$(state "$1")" != running ] || compose stop "$1"
  compose up -d --no-deps "$1"
}
ledger() { table_exists "$1" && sql "SELECT string_agg(version || CASE WHEN success THEN '' ELSE ' FAILED' END, ', ' ORDER BY installed_rank) FROM $1" || echo "(none)"; }
group_lag() { kafka kafka-consumer-groups --describe --group "$1" 2>/dev/null | awk -v t="$2" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6; n++} END{ if (n) print s; else print "-" }'; }
topic_records() {
  local e s
  e=$(kafka kafka-get-offsets --topic "$1" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  s=$(kafka kafka-get-offsets --topic "$1" --time -2 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  echo $((e - s))
}
purge_topic() {   # delete every record currently in the topic
  local json
  json=$(kafka kafka-get-offsets --topic "$1" --time -1 2>/dev/null | awk -F: -v t="$1" '$3>0 {p=p (p==""?"":",") sprintf("{\"topic\":\"%s\",\"partition\":%s,\"offset\":%s}",t,$2,$3)} END{ if (p!="") print "{\"version\":1,\"partitions\":[" p "]}" }')
  [ -n "$json" ] || return 0
  printf '%s' "$json" | $KAFKA_EXEC sh -c 'cat > /tmp/migrate-purge.json'
  kafka kafka-delete-records --offset-json-file /tmp/migrate-purge.json >/dev/null
  log "  purged $1"
}

# The numbers 'report' compares, "section|key|value". Rows are read as JSON (to_jsonb), so the same
# queries work on the 1.x tables and on the migrated ones (1.x step_instance.state, 2.0 step_status).
facts() {
  local ev; if table_exists matcher_event_log; then ev=matcher_event_log; else ev=compliance_event_log; fi
  psql_q <<SQL
SELECT 'events', 'accepted inbound events', count(*) FROM inbound_event_log WHERE status = 'ACCEPTED';
SELECT 'events', 'processed', count(*) FROM $ev;
SELECT 'events', 'accepted, never processed', count(*) FROM inbound_event_log i WHERE i.status = 'ACCEPTED'
   AND NOT EXISTS (SELECT 1 FROM $ev m WHERE m.cloudevents_id = i.cloudevents_id AND m.source = i.source);
SELECT 'rows', t.tablename, (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from public.%I', t.tablename), false, true, '')))[1]::text
  FROM pg_tables t WHERE t.schemaname = 'public' AND t.tablename = ANY (ARRAY['compliance_event_log','matcher_event_log',
   'protocol_definition','action_definition','trigger_index','protocol_instance','protocol_instance_history','step_instance',
   'step_instance_history','step_sla_state_transition','deviation','intelligence_event_log','facility','notification_tracker',
   'audit_log','scheduler_lease','scheduler_scan_cursor']);
SELECT 'enrolments', regexp_replace(coalesce(to_jsonb(d)->>'url', to_jsonb(d)->'definition'->>'url', '?'), '^.*/', ''), count(*)
  FROM protocol_instance pi LEFT JOIN protocol_definition d ON d.id = pi.protocol_definition_id GROUP BY 2;
SELECT 'steps', coalesce(to_jsonb(s)->>'step_status', to_jsonb(s)->>'state', '?'), count(*) FROM step_instance s GROUP BY 2;
SELECT 'step-verdicts', coalesce(to_jsonb(s)->>'sla_status', to_jsonb(s)->>'completion_status', '(none)'), count(*) FROM step_instance s GROUP BY 2;
SELECT 'deviations', deviation_type, count(*) FROM deviation GROUP BY 2;
SELECT 'processing', coalesce(processing_status, '?'), count(*) FROM $ev GROUP BY 2;
SELECT 'history-days', 'steps (step_instance_history changed_at)', count(DISTINCT changed_at::date) FROM step_instance_history;
SELECT 'history-days', 'deviations (detected_at)', count(DISTINCT detected_at::date) FROM deviation;
SQL
}

# ---- commands ------------------------------------------------------------------------------------
cmd_check() {
  log "Settings: $CONFIG_FILE; state in $STATE_DIR"
  sql "SELECT current_database()" >/dev/null && ok "Postgres ($PG_EXEC, $PG_DB)" || die "Postgres not reachable via $PG_EXEC"
  kafka kafka-topics --list >/dev/null 2>&1 && ok "Kafka ($KAFKA_EXEC)" || die "Kafka not reachable via $KAFKA_EXEC"
  grep -q RUNNING <<<"$(connect GET "/connectors/$CONNECTOR/status" || true)" && ok "Kafka Connect: $CONNECTOR running" || warn "Kafka Connect: $CONNECTOR not running"
  grep -q 1 <<<"$(ch "SELECT 1" || true)" && ok "ClickHouse ($CH_HTTP)" || die "ClickHouse not reachable at $CH_HTTP (password from SECRETS_FILE?)"
  [ -f "$DATA_PIPELINE_DIR/cdc/01-configure-replication.sql" ] && ok "2.0 data-pipeline: $DATA_PIPELINE_DIR" || die "No data-pipeline at $DATA_PIPELINE_DIR"
  [ -f "$MATCHER_MIGRATION_DIR/verify.sql" ] && ok "verify.sql: $MATCHER_MIGRATION_DIR" || die "No verify.sql in $MATCHER_MIGRATION_DIR"
  log "Services"
  local s; for s in $OLD_SERVICES $PROTOCOL $MATCHER $STEP_SLA $INSIGHTS_SERVICES; do printf '  %-28s %s\n' "$s" "$(state "$s")"; done
  log "Is this a 1.x database? (Matcher V2 reshapes the 1.x tables; on 2.0 tables it does nothing)"
  if table_exists compliance_event_log && ! table_exists matcher_event_log; then ok "1.x tables present (compliance_event_log, no matcher_event_log)"
  else warn "not a 1.x database: compliance_event_log $(table_exists compliance_event_log && echo present || echo absent), matcher_event_log $(table_exists matcher_event_log && echo present || echo absent)"; return 0; fi
  log "What Matcher V2 would refuse"
  local v
  v=$(sql "SELECT coalesce(string_agg(DISTINCT processing_status, ', '), '') FROM compliance_event_log WHERE processing_status NOT IN ('MATCHED','ZERO_MATCH','DUPLICATE')")
  [ -z "$v" ] && ok "1. every processing_status is MATCHED, ZERO_MATCH or DUPLICATE" || warn "1. unexpected processing_status: $v (V2 stops)"
  v=$(sql "SELECT count(*) FROM step_instance WHERE state = 'COMPLETED' AND completed_at IS NULL AND completion_status IS NULL")
  [ "$v" = 0 ] && ok "2. every completed step has completed_at or completion_status" || warn "2. $v completed steps with neither (V2 stops)"
  v=$(sql "SELECT count(*) FROM step_instance s WHERE s.completed_by_event_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM compliance_event_log e WHERE e.id = s.completed_by_event_id)")
  [ "$v" = 0 ] && ok "3. every step's completed_by_event_id is in compliance_event_log" || warn "3. $v steps point at an event not in compliance_event_log (V2 stops): see 'fix-orphans'"
  log "What Matcher V2 assumes but does not check"
  v=$(sql "SELECT string_agg((definition->>'url') || ': ' || (definition->'action'->0->>'id') || ' -> ' || coalesce(definition->'action'->0->'relatedAction'->0->>'actionId', definition->'action'->0->'relatedAction'->0->>'targetId', '(none)'), '; ') FROM protocol_definition")
  printf '  4. first step and the step its relatedAction names: %s\n' "$v"
  printf '     2.0 reads relatedAction as the step a step WAITS ON. If the first step names the next one, the\n'
  printf '     protocol is still in the 1.x direction, and V2 does not convert it.\n'
  v=$(sql "SELECT count(*) FROM notification_tracker WHERE status = 'ACTIVE'" 2>/dev/null || echo "-")
  printf '  open alert incidents (not a blocker for the migration): %s\n' "$v"
}

cmd_snapshot() {
  facts > "$STATE_DIR/before.tsv"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$STATE_DIR/started"
  ok "before-numbers saved: $STATE_DIR/before.tsv ($(wc -l < "$STATE_DIR/before.tsv") lines)"
}

cmd_backup() {
  local f="$STATE_DIR/ccedb_before_migration_$(date -u +%Y%m%d_%H%M%S).dump"
  $PG_EXEC sh -c "pg_dump -U \"\${POSTGRES_USER:-postgres}\" -d $PG_DB -Fc" > "$f"
  [ "$(head -c 5 "$f")" = PGDMP ] || die "The backup is not a valid pg_dump."
  echo "$f" > "$STATE_DIR/backup"
  ok "backup written: $(du -h "$f" | cut -f1)  $f"
}

cmd_stop_1x() {
  need_yes
  log "Stopping and removing $OLD_SERVICES (the collector keeps running; events wait in $INBOUND_TOPIC)"
  compose stop $OLD_SERVICES; compose rm -f $OLD_SERVICES
  local s; for s in $OLD_SERVICES; do [ "$(state "$s")" = absent ] || die "$s still exists"; done
  ok "removed: $OLD_SERVICES"
}

cmd_fix_orphans() {
  need_yes
  local n; n=$(sql "SELECT count(*) FROM step_instance s WHERE s.completed_by_event_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM compliance_event_log e WHERE e.id = s.completed_by_event_id)")
  [ "$n" != 0 ] || { ok "no step points at a missing event: nothing to do"; return; }
  sql "SELECT 'steps ' || count(*) || ', source ' || string_agg(DISTINCT coalesce(completed_by_source, '?'), ',') || ', completed ' || min(completed_at)::date || ' .. ' || max(completed_at)::date
         FROM step_instance s WHERE s.completed_by_event_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM compliance_event_log e WHERE e.id = s.completed_by_event_id)" \
    | sed 's/^/  /' | tee "$STATE_DIR/orphans.txt"
  sql "UPDATE step_instance s SET completed_by_event_id = NULL WHERE s.completed_by_event_id IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM compliance_event_log e WHERE e.id = s.completed_by_event_id)" >/dev/null
  ok "$n step links to missing events set to NULL (the steps stay completed); details in $STATE_DIR/orphans.txt"
}

cmd_deploy_2x() {
  log "Creating $PROTOCOL $MATCHER $STEP_SLA ${INSIGHTS_SERVICES} (stopped) from $COMPOSE_FILES"
  # a running 1.x Insights would be recreated on the 2.0 image and keep running: stop it first
  local s b; for s in $INSIGHTS_SERVICES; do [ "$(state "$s")" != running ] || compose stop "$s"; done
  compose up --no-start --no-deps $PROTOCOL $MATCHER $STEP_SLA $INSIGHTS_SERVICES
  for s in $PROTOCOL $MATCHER; do
    b=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$s" | sed -n 's/^CCE_FLYWAY_BASELINE_VERSION=//p')
    [ "$b" = 1 ] || die "$s has CCE_FLYWAY_BASELINE_VERSION=${b:-unset}; the migration needs 1 (the baseline-1 compose file in COMPOSE_FILES)."
  done
  for s in $PROTOCOL $MATCHER $STEP_SLA $INSIGHTS_SERVICES; do printf '  %-28s %s\n' "$s" "$(state "$s")"; done
  ok "created, not started; Protocol and Matcher baseline at 1"
}

cmd_copy_offsets() {
  need_yes
  [ "$(state "$MATCHER")" != running ] || die "$MATCHER is running: its offsets can only be set while its group is idle."
  kafka kafka-consumer-groups --describe --group "$OLD_GROUP" 2>/dev/null | awk -v t="$INBOUND_TOPIC" 'NR==1 || $2==t' | sed 's/^/  /'
  local csv; csv=$(kafka kafka-consumer-groups --describe --group "$OLD_GROUP" 2>/dev/null | awk -v t="$INBOUND_TOPIC" '$2==t && $4 ~ /^[0-9]+$/ {print $2","$3","$4}')
  if [ -z "$csv" ]; then
    warn "$OLD_GROUP has no committed offsets on $INBOUND_TOPIC: $NEW_GROUP is set to the end of the topic instead"
    kafka kafka-consumer-groups --group "$NEW_GROUP" --topic "$INBOUND_TOPIC" --reset-offsets --to-latest --execute | sed 's/^/  /'
  else
    printf '%s\n' "$csv" | $KAFKA_EXEC sh -c 'cat > /tmp/migrate-offsets.csv'
    kafka kafka-consumer-groups --group "$NEW_GROUP" --reset-offsets --from-file /tmp/migrate-offsets.csv --execute | sed 's/^/  /'
  fi
  ok "$NEW_GROUP now starts where $OLD_GROUP stopped"
}

cmd_migrate() {
  need_yes
  local t
  log "Protocol: V1 baselined, V2 aligns the 1.x protocol tables"
  t=$(date -u +%Y-%m-%dT%H:%M:%SZ); start_fresh $PROTOCOL
  wait_started $PROTOCOL "$t" || warn "$PROTOCOL did not start (expected: its Hibernate check needs the sequence Matcher's V6 sets); its migration is checked below"
  docker logs --since "$t" $PROTOCOL 2>&1 | grep -oE 'Successfully (baselined|applied)[^(]*' | sed 's/^/  /' || true
  [ "$(ledger flyway_schema_history_protocol)" = "1, 2" ] || die "Protocol ledger is '$(ledger flyway_schema_history_protocol)', not '1, 2'. docker logs $PROTOCOL"
  compose stop $PROTOCOL
  ok "Protocol migrated (ledger 1, 2)"
  log "Matcher: V1 baselined, V2 reshapes the 1.x tables, then the later migrations"
  t=$(date -u +%Y-%m-%dT%H:%M:%SZ); start_fresh $MATCHER
  if ! wait_started $MATCHER "$t"; then
    docker logs --since "$t" $MATCHER 2>&1 | grep -iE 'Cannot |ERROR|migration' | grep -v '^\s*at ' | tail -6 | cut -c1-220 | sed 's/^/    /'
    die "$MATCHER failed to start (above). The database is as the failed migration left it: restore the backup to start again."
  fi
  docker logs --since "$t" $MATCHER 2>&1 | grep -oE 'Successfully (baselined|applied)[^(]*' | sed 's/^/  /' || true
  local ml; ml=$(ledger flyway_schema_history_matcher)
  [[ "$ml" =~ ^1,\ 2,\ 3,\ 4,\ 5,\ 6(,\ [0-9]+)*$ ]] || die "Matcher ledger is '$ml', not '1, 2, 3, 4, 5, 6' (and later versions)."
  ok "Matcher migrated (ledger $ml) and running"
  log "Protocol again (now that Matcher's V6 is in)"
  t=$(date -u +%Y-%m-%dT%H:%M:%SZ); start_fresh $PROTOCOL
  wait_started $PROTOCOL "$t" || die "$PROTOCOL failed to start after Matcher's migration: docker logs $PROTOCOL"
  ok "Protocol running"
}

cmd_verify_schema() {
  $PG_EXEC sh -c "psql -U \"\${POSTGRES_USER:-postgres}\" -d $PG_DB -X" < "$MATCHER_MIGRATION_DIR/verify.sql"
}

# A 1.x schema built by Hibernate (ddl-auto=update, as on UAT) has IDENTITY ids on the two history
# tables. Matcher V6 keeps them identity; identity sequences are not in information_schema.sequences,
# where Hibernate's schema check looks, so Step SLA refuses to start ("missing sequence"). This gives
# them an ordinary sequence-backed id, as V1 creates it, stepping by 50 and continuing after the highest
# id used. A stand-in for a Matcher V6 fix; a no-op where not needed.
cmd_fix_history_ids() {
  need_yes
  local n; n=$(sql "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name IN ('step_instance_history','protocol_instance_history') AND column_name = 'id' AND is_identity = 'YES'")
  [ "$n" != 0 ] || { ok "the history ids are already sequence-backed: nothing to do"; return; }
  psql_q <<'SQL' >/dev/null
BEGIN;
DO $$
DECLARE t text; hi bigint;
BEGIN
  FOREACH t IN ARRAY ARRAY['step_instance_history', 'protocol_instance_history'] LOOP
    IF (SELECT is_identity FROM information_schema.columns WHERE table_schema = 'public' AND table_name = t AND column_name = 'id') = 'YES' THEN
      EXECUTE format('SELECT greatest(coalesce((SELECT max(id) FROM %I), 0), (SELECT last_value FROM %s))', t, pg_get_serial_sequence('public.' || t, 'id')) INTO hi;
      EXECUTE format('ALTER TABLE %I ALTER COLUMN id DROP IDENTITY', t);
      EXECUTE format('CREATE SEQUENCE %I INCREMENT BY 50 OWNED BY %I.id', t || '_id_seq', t);
      EXECUTE format('SELECT setval(%L, %s, true)', t || '_id_seq', hi);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN id SET DEFAULT nextval(%L)', t, t || '_id_seq');
    END IF;
  END LOOP;
END $$;
COMMIT;
SQL
  sql "SELECT sequence_name || ' increment ' || increment FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name LIKE '%history_id_seq'" | sed 's/^/  /'
  ok "history ids are sequence-backed (stepping by 50); Step SLA can start"
}

cmd_start() {
  need_yes
  [ "$(state "$MATCHER")" = running ] || die "$MATCHER is not running: run 'migrate' first."
  local t; t=$(date -u +%Y-%m-%dT%H:%M:%SZ); start_fresh $STEP_SLA
  wait_started $STEP_SLA "$t" || die "$STEP_SLA failed to start: docker logs $STEP_SLA"
  ok "Step SLA running: its first sweep judges every deadline already past"
}

cmd_rebuild_clickhouse() {
  need_yes
  [ -n "$CH_PASSWORD" ] || die "No ClickHouse password (SECRETS_FILE)."
  local saved="$STATE_DIR/connector-config.json" s
  log "1/6 Stopping Insights ($INSIGHTS_SERVICES) while ClickHouse is rebuilt"
  for s in $INSIGHTS_SERVICES; do [ "$(state "$s")" != running ] || compose stop "$s"; done
  log "2/6 Removing the connector (its configuration saved), its offsets and its replication slot"
  if grep -q '"connector.class"' <<<"$(connect GET "/connectors/$CONNECTOR/config" || true)"; then
    (umask 077; connect GET "/connectors/$CONNECTOR/config" > "$saved")
    connect PUT "/connectors/$CONNECTOR/stop" >/dev/null; sleep 3
    connect DELETE "/connectors/$CONNECTOR/offsets" >/dev/null; connect DELETE "/connectors/$CONNECTOR" >/dev/null; sleep 3
  fi
  [ -s "$saved" ] || die "No connector configuration to reuse (none running, none saved in $saved)."
  sql "SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots WHERE slot_name = '$CDC_SLOT' AND NOT active" >/dev/null
  log "3/6 Dropping ClickHouse database $CH_DB, emptying the CDC topics"
  ch "DROP DATABASE IF EXISTS $CH_DB SYNC" >/dev/null
  for s in $(kafka kafka-topics --list 2>/dev/null | grep -E '^cce\.public\.' || true); do purge_topic "$s"; done
  log "4/6 CDC settings on Postgres ($DATA_PIPELINE_DIR/cdc/01-configure-replication.sql)"
  psql_q < "$DATA_PIPELINE_DIR/cdc/01-configure-replication.sql" >/dev/null
  ok "publication tables: $(sql 'SELECT count(*) FROM pg_publication_tables')"
  log "5/6 ClickHouse schema, and the connector again (tables from the data-pipeline, connection from the saved configuration)"
  ( cd "$DATA_PIPELINE_DIR" && CH_HTTP="$CH_HTTP" CH_USER="$CH_USER" CLICKHOUSE_USER="$CH_USER" CLICKHOUSE_PASSWORD="$CH_PASSWORD" CH_DB="$CH_DB" CLICKHOUSE_DB="$CH_DB" python3 scripts/apply-schema.py schema \
      01-create-tables 02-kafka-ingestion 03-create-materialized-views 04-create-indexes 05-create-dictionary \
      06-current-state-rollups 08-reference-tables 07-daily-summary-aggregates ) | sed 's/^/  /'
  python3 - "$DATA_PIPELINE_DIR/connectors/debezium-postgres-source.json" "$saved" <<'PY' | connect PUT "/connectors/$CONNECTOR/config" --data-binary @- -o /dev/null -w '  connector %{http_code}\n'
import json, sys
new = json.load(open(sys.argv[1])); new = new.get("config", new)
old = json.load(open(sys.argv[2]))
for k, v in list(new.items()):
    if isinstance(v, str) and "${" in v:   # connection placeholders: the running deployment's values
        if k not in old: sys.exit(f"no saved value for {k}")
        new[k] = old[k]
print(json.dumps(new))
PY
  log "6/6 Waiting for the copy to settle (CDC tables only), backfilling past days, starting Insights"
  local prev="" cur stable=0 i
  for i in $(seq 1 120); do
    cur=$(ch "SELECT sum(total_rows) FROM system.tables WHERE database = '$CH_DB' AND engine LIKE '%MergeTree' AND name NOT LIKE 'mv\\_%' AND name NOT LIKE 'rollup\\_%'" | tr -d '[:space:]')
    log "  rows in ClickHouse: ${cur:-?}"
    if [ -n "$cur" ] && [ "$cur" != 0 ] && [ "$cur" = "$prev" ]; then stable=$((stable + 1)); [ "$stable" -ge 3 ] && break; else stable=0; fi
    prev="$cur"; sleep 15
  done
  [ "$stable" -ge 3 ] || die "The copy did not settle in 30 minutes. Check the connector, then run this step again."
  ok "copy settled"
  local from to; from=$(sql "SELECT to_char(min(received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM inbound_event_log"); to=$(date -u +%Y-%m-%d)
  BACKFILL_FILE="$DATA_PIPELINE_DIR/schema/09-historical-backfill.sql" FROM_DATE="$from" TO_DATE="$to" \
  CH_HTTP="$CH_HTTP" CH_USER="$CH_USER" CH_PASSWORD="$CH_PASSWORD" CH_DB="$CH_DB" python3 - <<'PY'
import os, re, urllib.request, urllib.parse
text = open(os.environ["BACKFILL_FILE"]).read()
text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
stmts = [s.strip() for s in text.split(";") if s.strip() and not s.strip().lower().startswith("use ")]
q = urllib.parse.urlencode({"database": os.environ["CH_DB"], "param_from_date": os.environ["FROM_DATE"], "param_to_date": os.environ["TO_DATE"]})
h = {"X-ClickHouse-User": os.environ["CH_USER"], "X-ClickHouse-Key": os.environ["CH_PASSWORD"]}
for s in stmts:
    urllib.request.urlopen(urllib.request.Request(os.environ["CH_HTTP"].rstrip("/") + "/?" + q, data=s.encode(), headers=h), timeout=600).read()
print(f"  {len(stmts)} statement(s) sent, 0 failed (rollup refill + past days, schema/09)")
PY
  ok "backfill done ($from .. $to)"
  for s in $INSIGHTS_SERVICES; do compose up -d --no-deps "$s" >/dev/null; printf '  %-28s %s\n' "$s" "$(state "$s")"; done
}

cmd_verify() {
  printf '  %-44s %s\n' "accepted inbound events" "$(sql "SELECT count(*) FROM inbound_event_log WHERE status = 'ACCEPTED'")"
  printf '  %-44s %s\n' "accepted events never processed" "$(sql "SELECT count(*) FROM inbound_event_log i WHERE i.status = 'ACCEPTED' AND NOT EXISTS (SELECT 1 FROM matcher_event_log m WHERE m.cloudevents_id = i.cloudevents_id AND m.source = i.source)")"
  printf '  %-44s %s\n' "lag of $NEW_GROUP" "$(group_lag "$NEW_GROUP" "$INBOUND_TOPIC")"
  printf '  %-44s %s\n' "records in $DLQ_TOPIC" "$(topic_records "$DLQ_TOPIC")"
  local pg chv; pg=$(sql "SELECT count(*) FROM step_instance"); chv=$(ch "SELECT count() FROM $CH_DB.step_instances FINAL WHERE _is_deleted = 0" | tr -d '[:space:]')
  printf '  %-44s Postgres %s / ClickHouse %s %s\n' "steps (CDC check)" "$pg" "$chv" "$([ "$pg" = "$chv" ] && echo '— match' || echo '— not yet equal')"
  printf '  %-44s %s\n' "CDC connector" "$(connect GET "/connectors/$CONNECTOR/status" | grep -oE '"state":"[A-Z]+"' | tr '\n' ' ')"
  printf '  %-44s protocol %s | matcher %s\n' "Flyway ledgers" "$(ledger flyway_schema_history_protocol)" "$(ledger flyway_schema_history_matcher)"
  local s; for s in $PROTOCOL $MATCHER $STEP_SLA $INSIGHTS_SERVICES; do printf '  %-44s %s\n' "$s" "$(state "$s")"; done
}

cmd_status() {
  local s t; for s in $OLD_SERVICES $PROTOCOL $MATCHER $STEP_SLA $INSIGHTS_SERVICES; do printf '  %-28s %s\n' "$s" "$(state "$s")"; done
  facts | awk -F'|' '$1=="rows" || $1=="events" {printf "  %-44s %s\n", $2, $3}'
}

cmd_report() {
  [ -f "$STATE_DIR/before.tsv" ] || die "No before-numbers: run 'snapshot' before the migration."
  local now out; now=$(mktemp); facts > "$now"
  out="$STATE_DIR/migration-report-$(date -u +%Y%m%d_%H%M%S).md"
  NAME="$NAME" BACKUP="$(cat "$STATE_DIR/backup" 2>/dev/null || true)" ORPHANS="$(cat "$STATE_DIR/orphans.txt" 2>/dev/null || true)" \
  python3 - "$STATE_DIR/before.tsv" "$now" "$REPLAY_REPORT" > "$out" <<'PY'
import os, sys, re, datetime
def load(p):
    d = {}
    for line in open(p):
        parts = line.rstrip("\n").rsplit("|", 1)
        head = parts[0].split("|", 1)
        if len(parts) == 2 and len(head) == 2 and parts[1].lstrip("-").isdigit():
            d[(head[0], head[1])] = int(parts[1])
    return d
b, a = load(sys.argv[1]), load(sys.argv[2])
# the replay report's "After" column, by its section titles
titles = {"events": "Events", "rows": "Rows per table", "enrolments": "Enrolments per protocol", "steps": "Steps by status",
          "step-verdicts": "Steps by deadline verdict", "deviations": "Deviations by type", "processing": "Processed events by result"}
r = {}
if sys.argv[3]:
    sec = None
    for line in open(sys.argv[3]):
        m = re.match(r"^## (.+)", line)
        if m: sec = next((k for k, t in titles.items() if t == m.group(1).strip()), None); continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if sec and len(cells) == 4 and cells[0] and cells[0] != "---" and cells[2].replace(",", "").isdigit():
            r[(sec, cells[0])] = int(cells[2])
def fmt(v): return "—" if v is None else str(v)
print(f"# Migration report: {os.environ['NAME']}\n")
print(f"- Written: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC; upgrade by database migration (Flyway baseline 1)")
print(f"- Backup before the migration: `{os.environ.get('BACKUP') or '—'}`")
if os.environ.get("ORPHANS"): print(f"- Step links to missing events nulled before the migration: {os.environ['ORPHANS'].strip()}")
if sys.argv[3]: print(f"- Compared with the replay report `{sys.argv[3]}` (its After column)")
for sec in ["events", "rows", "enrolments", "steps", "step-verdicts", "deviations", "processing", "history-days"]:
    keys = sorted({k for (s, k) in list(b) + list(a) + list(r) if s == sec})
    if not keys: continue
    print(f"\n## {titles.get(sec, 'Days covered by the history')}\n")
    if r and sec != "history-days":
        print("| | 1.x (before) | Migration | Replay | Migration − replay |\n|---|---:|---:|---:|---:|")
        for k in keys:
            x, y, z = b.get((sec, k)), a.get((sec, k)), r.get((sec, k))
            d = "" if y is None or z is None else f"{y - z:+d}"
            print(f"| {k} | {fmt(x)} | {fmt(y)} | {fmt(z)} | {d} |")
    else:
        print("| | 1.x (before) | Migration | Difference |\n|---|---:|---:|---:|")
        for k in keys:
            x, y = b.get((sec, k)), a.get((sec, k))
            d = "" if x is None or y is None else f"{y - x:+d}"
            print(f"| {k} | {fmt(x)} | {fmt(y)} | {d} |")
PY
  rm -f "$now"
  ok "report written: $out"
}

case "$COMMAND" in
  check) cmd_check;; snapshot) cmd_snapshot;; backup) cmd_backup;; stop-1x) cmd_stop_1x;; fix-orphans) cmd_fix_orphans;;
  deploy-2x) cmd_deploy_2x;; copy-offsets) cmd_copy_offsets;; migrate) cmd_migrate;; verify-schema) cmd_verify_schema;;
  fix-history-ids) cmd_fix_history_ids;; start) cmd_start;; rebuild-clickhouse) cmd_rebuild_clickhouse;; verify) cmd_verify;; report) cmd_report;; status) cmd_status;;
  *) sed -n '2,25p' "$0"; exit 1;;
esac
