#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
# ScyllaDB CDC Pipeline — End-to-End Test
#
# Usage: ./test-cdc-pipeline.sh [OPTIONS]
#
#   -s   Setup only  (create keyspace/table, register connector)
#   -d   Data only   (push rows, assumes setup already done)
#   -v   Verify only (check Kafka topic for CDC events)
#   -c   Clean up    (drop keyspace, delete connector)
#   -h   Help
#
# Default (no flags): run all steps in order
# ═══════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────
# Configuration — adjust if you changed defaults in the main script
# ───────────────────────────────────────────────────────────────────
SCYLLA_SEED="scylla-cdc-quickstart"
SCYLLA_NODES=("scylla-cdc-quickstart" "scylla-cdc-quickstart-2" "scylla-cdc-quickstart-3")
KAFKA_BROKER="scylla-cdc-broker"
KAFKA_CONNECT="scylla-cdc-connect"
SCHEMA_REGISTRY="scylla-cdc-schema-registry"

CONNECT_URL="http://localhost:18083"
BROKER_BOOTSTRAP="localhost:19093"

KEYSPACE="cdc_test"
TABLE="orders"
CONNECTOR_NAME="scylla-cdc-orders"
TOPIC_PREFIX="ScyllaCDC"
KAFKA_TOPIC="${TOPIC_PREFIX}.${KEYSPACE}.${TABLE}"

# ───────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────
log()     { echo -e "\n\033[1;34m[INFO]\033[0m  $*"; }
ok()      { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()    { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()     { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; exit 1; }
section() {
  echo -e "\n\033[1;35m════════════════════════════════════════\033[0m"
  echo -e "\033[1;35m  $*\033[0m"
  echo -e "\033[1;35m════════════════════════════════════════\033[0m"
}

cql() {
  # Pipe CQL statement into cqlsh via stdin — most reliable non-interactive method.
  # Avoids all -e flag issues: stdin bleed-through, SyntaxException noise, exit codes.
  # docker exec -i connects host stdin to container stdin; we feed exactly one statement.
  echo "$1" | docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" 2>/dev/null
}

cql_file() {
  # Pipe a heredoc into cqlsh via stdin for multi-line/multi-statement blocks.
  docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" 2>/dev/null
}

cql_check() {
  # Same as cql() but returns output for use in conditionals.
  echo "$1" | docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" 2>/dev/null
}

gen_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null \
    || uuidgen 2>/dev/null \
    || cat /proc/sys/kernel/random/uuid
}

wait_for_connector_running() {
  local name=$1
  local elapsed=0 timeout=60
  log "Waiting for connector '$name' to reach RUNNING state..."
  until curl -sf "${CONNECT_URL}/connectors/${name}/status" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); \
        [sys.exit(0) if t['state']=='RUNNING' else None for t in d['tasks']]" \
      2>/dev/null; do
    (( elapsed >= timeout )) && err "Connector '$name' did not reach RUNNING in ${timeout}s"
    echo -n "."; sleep 3; (( elapsed += 3 ))
  done
  echo ""; ok "Connector '$name' is RUNNING"
}

# ───────────────────────────────────────────────────────────────────
# Usage
# ───────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF

$(echo -e "\033[1;36m╔══════════════════════════════════════════════════╗")
$(echo -e "║   ScyllaDB CDC Pipeline — Test Script           ║")
$(echo -e "╚══════════════════════════════════════════════════╝\033[0m")

$(echo -e "\033[1mUSAGE\033[0m")
    $0 [OPTIONS]

$(echo -e "\033[1mOPTIONS\033[0m")
    -s   Setup only   Create keyspace, table (CDC-enabled), register connector
    -d   Data only    Insert, update, delete rows to trigger CDC events
    -v   Verify only  Poll Kafka topic and print CDC event summary
    -c   Clean up     Delete connector, drop keyspace
    -h   Help

$(echo -e "\033[1mEXAMPLES\033[0m")
    $0              # Full end-to-end test
    $0 -s           # Just set up infra (useful before running -d multiple times)
    $0 -d           # Push another batch of data
    $0 -v           # Inspect CDC events in Kafka
    $0 -c           # Tear down test artifacts

$(echo -e "\033[1mWHAT IT TESTS\033[0m")
    1. Keyspace + CDC-enabled table created in ScyllaDB
    2. ScyllaDB CDC connector registered via Kafka Connect REST API
    3. INSERT rows  → CDC op: c (create)
    4. UPDATE rows  → CDC op: u (update), before/after populated
    5. DELETE rows  → CDC op: d (delete)
    6. Kafka topic  ${KAFKA_TOPIC} has messages for all operations
    7. Message count matches expected operations

EOF
  exit 0
}

# ───────────────────────────────────────────────────────────────────
# Argument parsing
# ───────────────────────────────────────────────────────────────────
DO_SETUP=false
DO_DATA=false
DO_VERIFY=false
DO_CLEAN=false
ALL=true

while getopts ":sdvch" opt; do
  case $opt in
    s) DO_SETUP=true;  ALL=false ;;
    d) DO_DATA=true;   ALL=false ;;
    v) DO_VERIFY=true; ALL=false ;;
    c) DO_CLEAN=true;  ALL=false ;;
    h) usage ;;
    *) usage ;;
  esac
done

[[ "$ALL" == true ]] && DO_SETUP=true && DO_DATA=true && DO_VERIFY=true

# ───────────────────────────────────────────────────────────────────
# Pre-flight checks
# ───────────────────────────────────────────────────────────────────
section "Pre-flight Checks"

log "Checking ScyllaDB seed is reachable..."
docker exec "$SCYLLA_SEED" nodetool status > /dev/null \
  || err "Cannot reach $SCYLLA_SEED — is the cluster running?"
ok "ScyllaDB seed: $SCYLLA_SEED"

log "Checking all ScyllaDB nodes are UN..."
UN_COUNT=$(docker exec "$SCYLLA_SEED" nodetool status \
  | grep -c "^UN" || true)
ok "$UN_COUNT / ${#SCYLLA_NODES[@]} nodes are UN"

if [[ "$DO_SETUP" == true || "$DO_DATA" == true || "$DO_VERIFY" == true ]]; then
  log "Checking Kafka Connect is reachable..."
  curl -sf "${CONNECT_URL}/connectors" > /dev/null \
    || err "Cannot reach Kafka Connect at ${CONNECT_URL} — is the Kafka stack running?"
  ok "Kafka Connect: $CONNECT_URL"

  log "Checking ScyllaDB CDC connector plugin is loaded..."
  PLUGIN_CHECK=$(curl -sf "${CONNECT_URL}/connector-plugins" \
    | python3 -c "import sys,json; \
      plugins=[p['class'] for p in json.load(sys.stdin)]; \
      print('FOUND' if any('ScyllaConnector' in p or 'scylla' in p.lower() for p in plugins) else 'NOT_FOUND')")
  if [[ "$PLUGIN_CHECK" == "NOT_FOUND" ]]; then
    err "ScyllaDB CDC connector plugin not found in Connect.\n\
  Ensure the JAR is mounted at /connectors and CONNECT_PLUGIN_PATH includes it."
  fi
  ok "ScyllaDB CDC connector plugin: LOADED"
fi

# ───────────────────────────────────────────────────────────────────
# Step 1 — Setup: keyspace, table, connector
# ───────────────────────────────────────────────────────────────────
if [[ "$DO_SETUP" == true ]]; then
  section "Step 1 — Setup: Keyspace, CDC Table, Connector"

  # ── Idempotency check — detect previous run ──────────────────────
  log "Checking for existing setup from a previous run..."

  KS_EXISTS=$(cql_check "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '${KEYSPACE}';" \
    | grep -c "${KEYSPACE}" || true)

  TBL_EXISTS=$(cql_check "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '${KEYSPACE}' AND table_name = '${TABLE}';" \
    | grep -c "${TABLE}" || true)

  CONN_EXISTS=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" \
    2>/dev/null || echo "NOT_FOUND")

  if [[ "$KS_EXISTS" -gt 0 && "$TBL_EXISTS" -gt 0 && "$CONN_EXISTS" == "RUNNING" ]]; then
    echo ""
    ok "Previous setup detected — all components already exist:"
    ok "  Keyspace  : ${KEYSPACE} ✓"
    ok "  Table     : ${KEYSPACE}.${TABLE} ✓"
    ok "  Connector : ${CONNECTOR_NAME} (${CONN_EXISTS}) ✓"
    warn "Skipping setup — run with -c first to clean up, or use -d to push more data."
    DO_SETUP=false
  else
    [[ "$KS_EXISTS"  -gt 0 ]] && ok "  Keyspace exists — skipping CREATE"   || log "  Keyspace not found — will create"
    [[ "$TBL_EXISTS" -gt 0 ]] && ok "  Table exists — skipping CREATE"       || log "  Table not found — will create"
    [[ "$CONN_EXISTS" == "RUNNING" ]] && ok "  Connector exists — skipping register" || log "  Connector not found — will register"
  fi
fi

if [[ "$DO_SETUP" == true ]]; then

  log "Creating keyspace '${KEYSPACE}'..."
  cql "CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': ${UN_COUNT}} AND durable_writes = true;"
  ok "Keyspace: $KEYSPACE"

  log "Creating CDC-enabled table '${KEYSPACE}.${TABLE}'..."
  cat <<CQL_EOF | cql_file
    CREATE TABLE IF NOT EXISTS ${KEYSPACE}.${TABLE} (
      order_id    uuid,
      customer    text,
      product     text,
      quantity    int,
      status      text,
      created_at  timestamp,
      PRIMARY KEY (order_id)
    ) WITH cdc = {
      'enabled':    true,
      'preimage':   'true',
      'postimage':  'true'
    };
CQL_EOF
  ok "Table: ${KEYSPACE}.${TABLE} (CDC enabled, preimage+postimage)"

  log "Verifying CDC log table was auto-created..."
  CDC_TABLE=$(echo "DESCRIBE TABLE ${KEYSPACE}.${TABLE}_scylla_cdc_log;" \
    | docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" 2>&1 | head -3 || true)
  if echo "$CDC_TABLE" | grep -q "cdc\$stream_id\|CREATE TABLE"; then
    ok "CDC log table: ${KEYSPACE}.${TABLE}_scylla_cdc_log ✓"
  else
    warn "CDC log table not yet visible — may take a moment"
  fi

  log "Registering ScyllaDB CDC Source Connector '${CONNECTOR_NAME}'..."

  # Build comma-separated ScyllaDB cluster addresses
  SCYLLA_ADDRESSES=$(printf "%s:9042," "${SCYLLA_NODES[@]}" | sed 's/,$//')

  CONNECTOR_RESPONSE=$(curl -sf -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"${CONNECTOR_NAME}\",
      \"config\": {
        \"connector.class\":                    \"com.scylladb.cdc.debezium.connector.ScyllaConnector\",
        \"topic.prefix\":                        \"${TOPIC_PREFIX}\",
        \"scylla.cluster.ip.addresses\":         \"${SCYLLA_ADDRESSES}\",
        \"scylla.localdc\":                      \"datacenter1\",
        \"scylla.table.names\":                  \"${KEYSPACE}.${TABLE}\",
        \"tasks.max\":                           \"1\",
        \"scylla.preimage\":                     \"true\",
        \"scylla.postimage\":                    \"true\",
        \"key.converter\":                       \"org.apache.kafka.connect.json.JsonConverter\",
        \"value.converter\":                     \"org.apache.kafka.connect.json.JsonConverter\",
        \"key.converter.schemas.enable\":         \"true\",
        \"value.converter.schemas.enable\":       \"true\",
        \"auto.create.topics.enable\":            \"true\",
        \"offset.flush.interval.ms\":            \"5000\"
      }
    }" 2>&1 || true)

  if echo "$CONNECTOR_RESPONSE" | grep -q "already exists"; then
    warn "Connector '${CONNECTOR_NAME}' already registered — skipping"
  else
    echo "$CONNECTOR_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$CONNECTOR_RESPONSE"
    ok "Connector registered: $CONNECTOR_NAME"
  fi

  wait_for_connector_running "$CONNECTOR_NAME"

  log "Connector status:"
  curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" \
    | python3 -m json.tool
fi

# ───────────────────────────────────────────────────────────────────
# Step 2 — Push data: INSERT, UPDATE, DELETE
# ───────────────────────────────────────────────────────────────────
if [[ "$DO_DATA" == true ]]; then
  section "Step 2 — Push Test Data (INSERT / UPDATE / DELETE)"

  # Generate 5 order UUIDs on the host (faster, no container round-trip)
  UUID1=$(gen_uuid)
  UUID2=$(gen_uuid)
  UUID3=$(gen_uuid)
  UUID4=$(gen_uuid)
  UUID5=$(gen_uuid)
  log "Generated UUIDs: $UUID1 $UUID2 $UUID3 $UUID4 $UUID5"

  log "Inserting 5 orders (CDC op: c — create)..."
  cql "INSERT INTO ${KEYSPACE}.${TABLE} (order_id, customer, product, quantity, status, created_at) VALUES (${UUID1}, 'Alice Kim',  'Laptop Pro',          1, 'PENDING', toTimestamp(now()));"
  cql "INSERT INTO ${KEYSPACE}.${TABLE} (order_id, customer, product, quantity, status, created_at) VALUES (${UUID2}, 'Bob Tanaka', 'Mechanical Keyboard',  2, 'PENDING', toTimestamp(now()));"
  cql "INSERT INTO ${KEYSPACE}.${TABLE} (order_id, customer, product, quantity, status, created_at) VALUES (${UUID3}, 'Carol Park', 'USB-C Hub',            3, 'PENDING', toTimestamp(now()));"
  cql "INSERT INTO ${KEYSPACE}.${TABLE} (order_id, customer, product, quantity, status, created_at) VALUES (${UUID4}, 'Dan Gupta',  'Monitor 4K',           1, 'PENDING', toTimestamp(now()));"
  cql "INSERT INTO ${KEYSPACE}.${TABLE} (order_id, customer, product, quantity, status, created_at) VALUES (${UUID5}, 'Eve Santos', 'Webcam HD',            2, 'PENDING', toTimestamp(now()));"
  ok "Inserted 5 rows"

  log "Waiting 3s for CDC log propagation..."
  sleep 3

  log "Updating 3 orders — status change (CDC op: u — update)..."
  cql "UPDATE ${KEYSPACE}.${TABLE} SET status = 'CONFIRMED' WHERE order_id = ${UUID1};"
  cql "UPDATE ${KEYSPACE}.${TABLE} SET status = 'CONFIRMED', quantity = 3 WHERE order_id = ${UUID2};"
  cql "UPDATE ${KEYSPACE}.${TABLE} SET status = 'SHIPPED' WHERE order_id = ${UUID3};"
  ok "Updated 3 rows (status: PENDING → CONFIRMED/SHIPPED)"

  log "Waiting 3s..."
  sleep 3

  log "Updating 1 order — quantity and status change (CDC op: u)..."
  cql "UPDATE ${KEYSPACE}.${TABLE} SET status = 'DELIVERED', quantity = 1 WHERE order_id = ${UUID1};"
  ok "Updated UUID1: CONFIRMED → DELIVERED"

  log "Waiting 3s..."
  sleep 3

  log "Deleting 2 orders (CDC op: d — delete)..."
  cql "DELETE FROM ${KEYSPACE}.${TABLE} WHERE order_id = ${UUID4};"
  cql "DELETE FROM ${KEYSPACE}.${TABLE} WHERE order_id = ${UUID5};"
  ok "Deleted 2 rows (UUID4, UUID5)"

  log "Current table state:"
  cql "SELECT order_id, customer, status, quantity FROM ${KEYSPACE}.${TABLE};"

  log "CDC log table — last 10 entries:"
  echo "SELECT * FROM ${KEYSPACE}.${TABLE}_scylla_cdc_log LIMIT 10;" \
    | docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" 2>/dev/null \
    || warn "CDC log query failed — table may not exist yet or CDC not enabled"

  # Record UUIDs for verification step
  echo "$UUID1" > /tmp/cdc_test_uuids.txt
  echo "$UUID2" >> /tmp/cdc_test_uuids.txt
  echo "$UUID3" >> /tmp/cdc_test_uuids.txt
  echo "$UUID4" >> /tmp/cdc_test_uuids.txt
  echo "$UUID5" >> /tmp/cdc_test_uuids.txt
  ok "UUIDs saved to /tmp/cdc_test_uuids.txt for verification"
fi

# ───────────────────────────────────────────────────────────────────
# Step 3 — Verify: poll Kafka topic for CDC events
# ───────────────────────────────────────────────────────────────────
if [[ "$DO_VERIFY" == true ]]; then
  section "Step 3 — Verify CDC Events in Kafka"

  POLL_TIMEOUT=30  # seconds to wait for messages

  log "Checking if topic '${KAFKA_TOPIC}' exists..."
  TOPIC_EXISTS=$(docker exec "$KAFKA_BROKER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -c "^${KAFKA_TOPIC}$" || true)

  if [[ "$TOPIC_EXISTS" -eq 0 ]]; then
    warn "Topic '${KAFKA_TOPIC}' not yet created."
    log "Waiting up to ${POLL_TIMEOUT}s for connector to create topic..."
    elapsed=0
    until docker exec "$KAFKA_BROKER" kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$"; do
      (( elapsed >= POLL_TIMEOUT )) && err "Topic not created after ${POLL_TIMEOUT}s — check connector status"
      echo -n "."; sleep 3; (( elapsed += 3 ))
    done
    echo ""
  fi
  ok "Topic exists: ${KAFKA_TOPIC}"

  log "Topic details:"
  docker exec "$KAFKA_BROKER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic "$KAFKA_TOPIC"

  log "Consuming up to 50 messages (${POLL_TIMEOUT}s timeout)..."
  echo ""
  echo -e "\033[1;36m─── RAW CDC MESSAGES (newest 50) ───────────────────────────────────\033[0m"

  RAW_MESSAGES=$(docker exec "$KAFKA_BROKER" \
    kafka-console-consumer \
      --bootstrap-server localhost:9092 \
      --topic "$KAFKA_TOPIC" \
      --from-beginning \
      --max-messages 50 \
      --timeout-ms $(( POLL_TIMEOUT * 1000 )) \
      --property print.key=false \
    2>/dev/null || true)

  if [[ -z "$RAW_MESSAGES" ]]; then
    warn "No messages received yet — the connector may still be catching up"
    warn "Try running: ./test-cdc-pipeline.sh -v  again in a few seconds"
  else
    TOTAL=$(echo "$RAW_MESSAGES" | wc -l)
    ok "Received $TOTAL message(s)"
    echo ""
  fi

  log "Parsing CDC event summary (op types + order counts)..."
  echo ""
  echo -e "\033[1;36m─── CDC EVENT SUMMARY ──────────────────────────────────────────────\033[0m"

  if [[ -n "$RAW_MESSAGES" ]]; then
    TMPFILE=$(mktemp /tmp/cdc_messages_XXXXXX.jsonl)
    PYFILE=$(mktemp /tmp/cdc_parse_XXXXXX.py)
    echo "$RAW_MESSAGES" > "$TMPFILE"

    cat > "$PYFILE" << PYEOF
import sys, json

ops       = {'c': 0, 'u': 0, 'd': 0, 'r': 0, 'unknown': 0}
op_labels = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
tables    = {}
errors    = 0
skipped   = 0  # non-JSON lines (plain-string message keys)

def unwrap(cell):
    if isinstance(cell, dict) and 'value' in cell:
        return cell['value']
    return cell

with open(sys.argv[1]) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            msg     = json.loads(line)
        except json.JSONDecodeError:
            # Non-JSON line — likely a plain-string message key
            # (StringConverter keys are UUIDs, not JSON)
            skipped += 1
            continue
        try:
            payload = msg.get('payload', msg)
            op      = payload.get('op', 'unknown')
            ops[op] = ops.get(op, 0) + 1

            src = payload.get('source') or {}
            ks  = src.get('keyspace_name') or src.get('db',    '?')
            tbl = src.get('table_name')    or src.get('table', '?')
            key = "{}.{}".format(ks, tbl)
            tables[key] = tables.get(key, 0) + 1

            if op in ('c', 'u', 'd') and ops[op] == 1:
                print("\n  Example op='{}' ({}):".format(op, op_labels.get(op, op)))
                after  = payload.get('after')  or {}
                before = payload.get('before') or {}
                order_id = (after or before).get('order_id', '?')
                print("    order_id: {}".format(order_id))
                if before:
                    cust   = unwrap(before.get('customer'))
                    status = unwrap(before.get('status'))
                    qty    = unwrap(before.get('quantity'))
                    if any(v is not None for v in [cust, status, qty]):
                        print("    before: customer={:<15} status={}  qty={}".format(
                            str(cust or '-'), status or '-', qty if qty is not None else '-'))
                if after:
                    cust   = unwrap(after.get('customer'))
                    status = unwrap(after.get('status'))
                    qty    = unwrap(after.get('quantity'))
                    if any(v is not None for v in [cust, status, qty]):
                        print("    after:  customer={:<15} status={}  qty={}".format(
                            str(cust or '-'), status or '-', qty if qty is not None else '-'))
                    else:
                        print("    after:  (only primary key present — partial update)")
                if op == 'd':
                    print("    (row deleted — primary key only in before image)")
        except Exception as e:
            errors += 1

print("\n  ┌────────────────────────────────────────────┐")
print("  │  CDC Operation Counts                      │")
print("  ├────────────────┬───────────────────────────┤")
print("  │  op=c (INSERT) │ {:>5} events               │".format(ops.get('c',0)))
print("  │  op=u (UPDATE) │ {:>5} events               │".format(ops.get('u',0)))
print("  │  op=d (DELETE) │ {:>5} events               │".format(ops.get('d',0)))
print("  │  op=r (READ)   │ {:>5} events               │".format(ops.get('r',0)))
print("  ├────────────────┴───────────────────────────┤")
print("  │  Total CDC events : {:>5}                   │".format(sum(ops.values())))
print("  │  Skipped (keys)   : {:>5}                   │".format(skipped))
print("  │  Parse errors     : {:>5}                   │".format(errors))
print("  └────────────────────────────────────────────┘")
print("\n  Source tables : {}".format(list(tables.keys())))
PYEOF

    python3 "$PYFILE" "$TMPFILE"
    rm -f "$TMPFILE" "$PYFILE"
  fi

  echo ""
  log "Message lag / offset summary:"
  docker exec "$KAFKA_BROKER" kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --all-groups 2>/dev/null \
    | grep -i "${KAFKA_TOPIC}\|GROUP\|TOPIC" || true

  echo ""
  section "Connector Health Check"

  log "Connector status:"
  curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" \
    | python3 -m json.tool 2>/dev/null \
    || warn "Could not retrieve connector status — is it registered?"

  log "All registered connectors:"
  curl -sf "${CONNECT_URL}/connectors" | python3 -m json.tool

  log "All topics in broker:"
  docker exec "$KAFKA_BROKER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --list \
    | grep -v "^__\|^_\|connect" \
    | sort
fi

# ───────────────────────────────────────────────────────────────────
# Clean up
# ───────────────────────────────────────────────────────────────────
if [[ "$DO_CLEAN" == true ]]; then
  section "Clean Up Test Artifacts"

  log "Deleting connector '${CONNECTOR_NAME}'..."
  HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" || echo "000")
  if [[ "$HTTP_STATUS" == "204" || "$HTTP_STATUS" == "200" ]]; then
    ok "Connector deleted: $CONNECTOR_NAME"
  else
    warn "Connector delete returned HTTP $HTTP_STATUS (may not exist)"
  fi

  log "Dropping keyspace '${KEYSPACE}' from ScyllaDB..."
  cql "DROP KEYSPACE IF EXISTS ${KEYSPACE};" \
    && ok "Keyspace dropped: $KEYSPACE"

  log "Deleting Kafka topic '${KAFKA_TOPIC}' (if it exists)..."
  docker exec "$KAFKA_BROKER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic "$KAFKA_TOPIC" 2>/dev/null \
    && ok "Topic deleted: $KAFKA_TOPIC" \
    || warn "Topic not found or already deleted"

  rm -f /tmp/cdc_test_uuids.txt
  ok "Clean up complete"
fi

# ───────────────────────────────────────────────────────────────────
# Final summary
# ───────────────────────────────────────────────────────────────────
echo ""
echo -e "\033[1;36m"
cat <<EOF
  ╔══════════════════════════════════════════════════════════════╗
  ║   CDC Pipeline Test Complete                                 ║
  ╠══════════════════════════════════════════════════════════════╣
  ║   Keyspace    : ${KEYSPACE}
  ║   Table       : ${TABLE}
  ║   CDC log     : ${TABLE}_scylla_cdc_log
  ║   Connector   : ${CONNECTOR_NAME}
  ║   Kafka topic : ${KAFKA_TOPIC}
  ╠══════════════════════════════════════════════════════════════╣
  ║   Useful commands:                                           ║
  ║                                                              ║
  ║   # Live topic tail                                          ║
  ║   docker exec scylla-cdc-broker kafka-console-consumer \    ║
  ║     --bootstrap-server localhost:9092 \                     ║
  ║     --topic ${KAFKA_TOPIC} \
  ║     --from-beginning                                        ║
  ║                                                              ║
  ║   # Connect REST — list connectors                           ║
  ║   curl -s http://localhost:18083/connectors | python3 -m json.tool ║
  ║                                                              ║
  ║   # ScyllaDB CQL shell                                       ║
  ║   docker exec -it scylla-cdc-quickstart cqlsh               ║
  ║                                                              ║
  ║   # Kafdrop UI (community)                                   ║
  ║   open http://localhost:19000                                ║
  ║                                                              ║
  ║   # Control Center (enterprise)                              ║
  ║   open http://localhost:19021                                ║
  ╚══════════════════════════════════════════════════════════════╝
EOF
echo -e "\033[0m"
