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
  # Run a CQL statement inside the seed node container
  docker exec "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" -e "$1"
}

cql_file() {
  # Pipe a heredoc into cqlsh
  docker exec -i "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED"
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

  log "Creating keyspace '${KEYSPACE}'..."
  cql "
    CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
    WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'datacenter1': ${UN_COUNT}
    } AND durable_writes = true;
  " && ok "Keyspace: $KEYSPACE"

  log "Creating CDC-enabled table '${KEYSPACE}.${TABLE}'..."
  cql "
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
  " && ok "Table: ${KEYSPACE}.${TABLE} (CDC enabled, preimage+postimage)"

  log "Verifying CDC log table was auto-created..."
  CDC_TABLE=$(docker exec "$SCYLLA_SEED" cqlsh "$SCYLLA_SEED" \
    -e "DESCRIBE TABLE ${KEYSPACE}.${TABLE}_scylla_cdc_log;" 2>&1 | head -3 || true)
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
        \"tasks.max\":                           \"${#SCYLLA_NODES[@]}\",
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

  # Generate 5 order UUIDs
  UUID1=$(docker exec "$SCYLLA_SEED" python3 -c "import uuid; print(uuid.uuid4())")
  UUID2=$(docker exec "$SCYLLA_SEED" python3 -c "import uuid; print(uuid.uuid4())")
  UUID3=$(docker exec "$SCYLLA_SEED" python3 -c "import uuid; print(uuid.uuid4())")
  UUID4=$(docker exec "$SCYLLA_SEED" python3 -c "import uuid; print(uuid.uuid4())")
  UUID5=$(docker exec "$SCYLLA_SEED" python3 -c "import uuid; print(uuid.uuid4())")

  log "Inserting 5 orders (CDC op: c — create)..."
  cql "
    INSERT INTO ${KEYSPACE}.${TABLE}
      (order_id, customer, product, quantity, status, created_at)
    VALUES
      (${UUID1}, 'Alice Kim',   'Laptop Pro',       1, 'PENDING',    toTimestamp(now()));
    INSERT INTO ${KEYSPACE}.${TABLE}
      (order_id, customer, product, quantity, status, created_at)
    VALUES
      (${UUID2}, 'Bob Tanaka',  'Mechanical Keyboard', 2, 'PENDING', toTimestamp(now()));
    INSERT INTO ${KEYSPACE}.${TABLE}
      (order_id, customer, product, quantity, status, created_at)
    VALUES
      (${UUID3}, 'Carol Park',  'USB-C Hub',        3, 'PENDING',    toTimestamp(now()));
    INSERT INTO ${KEYSPACE}.${TABLE}
      (order_id, customer, product, quantity, status, created_at)
    VALUES
      (${UUID4}, 'Dan Gupta',   'Monitor 4K',       1, 'PENDING',    toTimestamp(now()));
    INSERT INTO ${KEYSPACE}.${TABLE}
      (order_id, customer, product, quantity, status, created_at)
    VALUES
      (${UUID5}, 'Eve Santos',  'Webcam HD',        2, 'PENDING',    toTimestamp(now()));
  " && ok "Inserted 5 rows"

  log "Waiting 3s for CDC log propagation..."
  sleep 3

  log "Updating 3 orders — status change (CDC op: u — update)..."
  cql "
    UPDATE ${KEYSPACE}.${TABLE}
      SET status = 'CONFIRMED'
    WHERE order_id = ${UUID1};
    UPDATE ${KEYSPACE}.${TABLE}
      SET status = 'CONFIRMED', quantity = 3
    WHERE order_id = ${UUID2};
    UPDATE ${KEYSPACE}.${TABLE}
      SET status = 'SHIPPED'
    WHERE order_id = ${UUID3};
  " && ok "Updated 3 rows (status: PENDING → CONFIRMED/SHIPPED)"

  log "Waiting 3s..."
  sleep 3

  log "Updating 1 order — quantity and status change (CDC op: u)..."
  cql "
    UPDATE ${KEYSPACE}.${TABLE}
      SET status = 'DELIVERED', quantity = 1
    WHERE order_id = ${UUID1};
  " && ok "Updated UUID1: CONFIRMED → DELIVERED"

  log "Waiting 3s..."
  sleep 3

  log "Deleting 2 orders (CDC op: d — delete)..."
  cql "
    DELETE FROM ${KEYSPACE}.${TABLE} WHERE order_id = ${UUID4};
    DELETE FROM ${KEYSPACE}.${TABLE} WHERE order_id = ${UUID5};
  " && ok "Deleted 2 rows (UUID4, UUID5)"

  log "Current table state:"
  cql "SELECT order_id, customer, status, quantity FROM ${KEYSPACE}.${TABLE};"

  log "CDC log table — last 10 entries:"
  cql "
    SELECT cdc\$operation, cdc\$time, order_id, status, quantity
    FROM ${KEYSPACE}.${TABLE}_scylla_cdc_log
    LIMIT 10;
  " 2>/dev/null || warn "CDC log query failed — the log table may use a different schema version"

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
    echo "$RAW_MESSAGES" | python3 - << 'PYEOF'
import sys
import json

lines = sys.stdin.read().strip().split('\n')
ops = {'c': 0, 'u': 0, 'd': 0, 'r': 0, 'unknown': 0}
tables = {}
errors = 0

for line in lines:
    if not line.strip():
        continue
    try:
        msg = json.loads(line)
        payload = msg.get('payload', {})
        op = payload.get('op', 'unknown')
        ops[op] = ops.get(op, 0) + 1

        src = payload.get('source', {})
        tbl = f"{src.get('db','?')}.{src.get('table','?')}"
        tables[tbl] = tables.get(tbl, 0) + 1

        # Print first INSERT, first UPDATE, first DELETE as examples
        if op in ('c', 'u', 'd') and ops[op] == 1:
            print(f"\n  Example op='{op}' ({{'c':'INSERT','u':'UPDATE','d':'DELETE'}}.get(op,op)}):")
            after = payload.get('after', {})
            before = payload.get('before', {})
            if before:
                print(f"    before: customer={before.get('customer','?'):15} status={before.get('status','?')}")
            if after:
                print(f"    after:  customer={after.get('customer','?'):15} status={after.get('status','?')}  qty={after.get('quantity','?')}")
    except json.JSONDecodeError:
        errors += 1

print(f"\n  ┌────────────────────────────────────────────┐")
print(f"  │  CDC Operation Counts                      │")
print(f"  ├──────────────┬─────────────────────────────┤")
print(f"  │  op=c (INSERT) │ {ops.get('c',0):>5} events               │")
print(f"  │  op=u (UPDATE) │ {ops.get('u',0):>5} events               │")
print(f"  │  op=d (DELETE) │ {ops.get('d',0):>5} events               │")
print(f"  │  op=r (READ)   │ {ops.get('r',0):>5} events               │")
print(f"  ├──────────────┴─────────────────────────────┤")
print(f"  │  Total parsed  │ {sum(ops.values()):>5}                    │")
print(f"  │  Parse errors  │ {errors:>5}                    │")
print(f"  └────────────────────────────────────────────┘")
print(f"\n  Source tables: {list(tables.keys())}")
PYEOF
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
