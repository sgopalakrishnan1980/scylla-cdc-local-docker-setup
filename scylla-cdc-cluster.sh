#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
# ScyllaDB CDC Cluster Bootstrap
#
# Usage: ./scylla-cdc-cluster.sh [-k community|enterprise] [-r]
#
#   -k community   Start Confluent community images alongside ScyllaDB
#   -k enterprise  Start Confluent enterprise images alongside ScyllaDB
#   -r             Remove all containers, network, /etc/hosts entries
#
# Community  → cp-kafka broker, cp-kafka-connect, Kafdrop UI
# Enterprise → cp-server broker, cp-server-connect-datagen, Control Center
# ═══════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────
# Static configuration
# ───────────────────────────────────────────────────────────────────
NETWORK="scylla-cdc-bridge"
HOSTS_FILE="/etc/hosts"
HOSTS_MARKER="# scylla-cdc-bridge managed block"

# ScyllaDB
SCYLLA_IMAGE="scylladb/scylla"
SCYLLA_NODES=(scylla-cdc-quickstart scylla-cdc-quickstart-2 scylla-cdc-quickstart-3)
SEED_NODE="${SCYLLA_NODES[0]}"
BOOT_WAIT=30       # seconds between node starts
READY_TIMEOUT=180  # seconds to wait for UN state

# Confluent — shared
CONFLUENT_VERSION="7.6.1"
KAFKA_BROKER="scylla-cdc-broker"
KAFKA_SCHEMA_REGISTRY="scylla-cdc-schema-registry"
KAFKA_CONNECT="scylla-cdc-connect"
KAFKA_REST_PROXY="scylla-cdc-rest-proxy"
KAFKA_KSQLDB="scylla-cdc-ksqldb"

# Confluent — community-only UI
KAFKA_KAFDROP="scylla-cdc-kafdrop"

# Confluent — enterprise-only UI
KAFKA_CONTROL_CENTER="scylla-cdc-control-center"

# ScyllaDB CDC Source Connector — resolved at runtime from Maven Central
MAVEN_GROUP_PATH="com/scylladb"
MAVEN_ARTIFACT="scylla-cdc-source-connector"
MAVEN_METADATA_URL="https://repo1.maven.org/maven2/${MAVEN_GROUP_PATH}/${MAVEN_ARTIFACT}/maven-metadata.xml"
MAVEN_REPO_BASE="https://repo1.maven.org/maven2/${MAVEN_GROUP_PATH}/${MAVEN_ARTIFACT}"
CONNECTORS_DIR="$(pwd)/volumes/connectors"
MIN_RECOMMENDED_MAJOR=2   # banner fires if resolved version is below this

# ───────────────────────────────────────────────────────────────────
# Runtime state (set after arg parsing)
# ───────────────────────────────────────────────────────────────────
WITH_KAFKA=false
KAFKA_EDITION=""        # "community" | "enterprise"
REMOVE=false

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

require_sudo()     { sudo -v 2>/dev/null || err "This script requires sudo privileges."; }
container_exists() { docker inspect "$1" &>/dev/null; }

remove_if_exists() {
  local name=$1
  if container_exists "$name"; then
    docker rm -f "$name" && ok "Removed: $name"
  else
    warn "Not found, skipping: $name"
  fi
}

get_container_ip() {
  docker inspect "$1" \
    --format "{{(index .NetworkSettings.Networks \"${NETWORK}\").IPAddress}}" 2>/dev/null
}

# ───────────────────────────────────────────────────────────────────
# /etc/hosts — idempotent tagged block
# ───────────────────────────────────────────────────────────────────
hosts_remove_block() {
  sudo sed -i "/^${HOSTS_MARKER}/,/^# end ${HOSTS_MARKER}/d" "$HOSTS_FILE"
}

hosts_add_block() {
  local entries=("$@")
  {
    echo ""
    echo "${HOSTS_MARKER}"
    for entry in "${entries[@]}"; do echo "$entry"; done
    echo "# end ${HOSTS_MARKER}"
  } | sudo tee -a "$HOSTS_FILE" > /dev/null
}

# ───────────────────────────────────────────────────────────────────
# Usage
# ───────────────────────────────────────────────────────────────────
usage() {
  local exit_code=${1:-1}
  cat <<EOF

$(echo -e "\033[1;36m╔══════════════════════════════════════════════════════════════════╗")
$(echo -e "║           ScyllaDB CDC Cluster Bootstrap — Help                  ║")
$(echo -e "╚══════════════════════════════════════════════════════════════════╝\033[0m")

$(echo -e "\033[1mUSAGE\033[0m")
    $0 [OPTIONS]

$(echo -e "\033[1mOPTIONS\033[0m")
    $(echo -e "\033[1;33m-k <edition>\033[0m")
        Start the Confluent Kafka stack alongside the ScyllaDB cluster.
        Edition must be one of:

        $(echo -e "\033[1mcommunity\033[0m")
            Uses free, open-licensed Confluent community Docker images.
            No license key required.

            Component        Image
            ─────────────────────────────────────────────────────────────
            Broker (KRaft)   confluentinc/cp-kafka:${CONFLUENT_VERSION}
            Kafka Connect    confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}
                             └─ Datagen plugin installed at boot via confluent-hub
            Schema Registry  confluentinc/cp-schema-registry:${CONFLUENT_VERSION}
            REST Proxy       confluentinc/cp-kafka-rest:${CONFLUENT_VERSION}
            ksqlDB           confluentinc/cp-ksqldb-server:${CONFLUENT_VERSION}
            Web UI           obsidiandynamics/kafdrop (OSS)
                             └─ http://localhost:19000

        $(echo -e "\033[1menterprise\033[0m")
            Uses Confluent Platform enterprise images.
            Includes Control Center and pre-bundled Datagen connector.
            A trial license is auto-applied for 30 days.

            Component        Image
            ─────────────────────────────────────────────────────────────
            Broker (KRaft)   confluentinc/cp-server:${CONFLUENT_VERSION}
            Kafka Connect    cnfldemos/cp-server-connect-datagen:0.6.4-${CONFLUENT_VERSION}
                             └─ Datagen pre-bundled, no install step needed
            Schema Registry  confluentinc/cp-schema-registry:${CONFLUENT_VERSION}
            REST Proxy       confluentinc/cp-kafka-rest:${CONFLUENT_VERSION}
            ksqlDB           confluentinc/cp-ksqldb-server:${CONFLUENT_VERSION}
            Web UI           confluentinc/cp-enterprise-control-center:${CONFLUENT_VERSION}
                             └─ http://localhost:19021

    $(echo -e "\033[1;33m-r\033[0m")
        Remove all containers (ScyllaDB + Kafka, both editions), delete the
        Docker network, and scrub the managed /etc/hosts block.
        Safe to run regardless of which edition was started.

    $(echo -e "\033[1;33m-h\033[0m")
        Show this help message and exit.

$(echo -e "\033[1mSCAFFOLDING\033[0m")
    All containers are attached to a single Docker bridge network:

        Network : ${NETWORK}

    ScyllaDB nodes (always started):

        scylla-cdc-quickstart    ← seed node
        scylla-cdc-quickstart-2  ← joins after seed is UN
        scylla-cdc-quickstart-3  ← joins 30s after node 2

    Kafka containers (started with -k):

        scylla-cdc-broker
        scylla-cdc-schema-registry
        scylla-cdc-connect
        scylla-cdc-rest-proxy
        scylla-cdc-ksqldb
        scylla-cdc-kafdrop          (community only)
        scylla-cdc-control-center   (enterprise only)

$(echo -e "\033[1mSCALYLADB CDC CONNECTOR\033[0m")
    When -k is used the ScyllaDB CDC Source Connector JAR is resolved from
    Maven Central at runtime (latest version auto-detected):

        GroupId  : com.scylladb
        Artifact : scylla-cdc-source-connector
        Classifier: jar-with-dependencies (fat JAR)
        Metadata : https://repo1.maven.org/maven2/com/scylladb/
                   scylla-cdc-source-connector/maven-metadata.xml
        Host dir : ./volumes/connectors/
        Container: /connectors/  (bind-mounted into scylla-cdc-connect)

    The script checks the resolved version against the 2.0.x minimum:
      ✓ 2.0.x or higher → green OK banner, proceed normally
      ✗ below 2.0.x     → red WARNING banner + 10s countdown
                          (tablets architecture not supported in older versions)

$(echo -e "\033[1mKERNEL TUNING\033[0m")
    Applied at every run (requires sudo). Persisted to:
        /etc/sysctl.d/99-scylla-cdc.conf

        fs.aio-max-nr              = 2147483647  (INT_MAX — Seastar AIO)
        net.core.somaxconn         = 4096
        vm.swappiness              = 1
        net.ipv4.tcp_rmem          = 4096 87380 16777216
        net.ipv4.tcp_wmem          = 4096 65536 16777216

$(echo -e "\033[1mSERVICE PORTS\033[0m")
    Service                Port    Protocol
    ─────────────────────────────────────────────
    ScyllaDB CQL           9042    TCP (per node, container network only)
    Kafka Broker           19093   TCP (host-accessible, maps to container 9093)
    Schema Registry        18081    HTTP
    Kafka Connect REST     18083    HTTP
    Kafka REST Proxy       18082    HTTP
    ksqlDB                 18088    HTTP
    Kafdrop UI             19000    HTTP  (community)
    Control Center         19021    HTTP  (enterprise)

$(echo -e "\033[1mEXAMPLES\033[0m")
    # ScyllaDB 3-node cluster only
    $0

    # ScyllaDB + Confluent community (free, Kafdrop UI)
    $0 -k community

    # ScyllaDB + Confluent enterprise (Control Center, 30-day trial)
    $0 -k enterprise

    # Tear down everything (both editions, network, /etc/hosts)
    $0 -r

EOF
  exit "$exit_code"
}

# ───────────────────────────────────────────────────────────────────
# Argument parsing
# ───────────────────────────────────────────────────────────────────
while getopts ":k:rh" opt; do
  case $opt in
    k)
      WITH_KAFKA=true
      KAFKA_EDITION="${OPTARG}"
      if [[ "$KAFKA_EDITION" != "community" && "$KAFKA_EDITION" != "enterprise" ]]; then
        err "Invalid edition '${KAFKA_EDITION}'. Must be 'community' or 'enterprise'."
      fi
      ;;
    r) REMOVE=true ;;
    h) usage 0 ;;
    :) err "Option -${OPTARG} requires an argument (community|enterprise)." ;;
    *) usage 1 ;;
  esac
done

# ───────────────────────────────────────────────────────────────────
# Resolve image names and UI container based on edition
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  if [[ "$KAFKA_EDITION" == "community" ]]; then
    IMG_BROKER="confluentinc/cp-kafka:${CONFLUENT_VERSION}"
    IMG_CONNECT="confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}"
    IMG_SCHEMA_REGISTRY="confluentinc/cp-schema-registry:${CONFLUENT_VERSION}"
    IMG_REST_PROXY="confluentinc/cp-kafka-rest:${CONFLUENT_VERSION}"
    IMG_KSQLDB="confluentinc/cp-ksqldb-server:${CONFLUENT_VERSION}"
    IMG_UI="obsidiandynamics/kafdrop:latest"
    KAFKA_UI_CONTAINER="$KAFKA_KAFDROP"
    CONNECT_NEEDS_DATAGEN_INSTALL=true   # install via confluent-hub at boot
  else
    IMG_BROKER="confluentinc/cp-server:${CONFLUENT_VERSION}"
    IMG_CONNECT="cnfldemos/cp-server-connect-datagen:0.6.4-${CONFLUENT_VERSION}"
    IMG_SCHEMA_REGISTRY="confluentinc/cp-schema-registry:${CONFLUENT_VERSION}"
    IMG_REST_PROXY="confluentinc/cp-kafka-rest:${CONFLUENT_VERSION}"
    IMG_KSQLDB="confluentinc/cp-ksqldb-server:${CONFLUENT_VERSION}"
    IMG_UI="confluentinc/cp-enterprise-control-center:${CONFLUENT_VERSION}"
    KAFKA_UI_CONTAINER="$KAFKA_CONTROL_CENTER"
    CONNECT_NEEDS_DATAGEN_INSTALL=false  # Datagen pre-bundled in enterprise image
  fi

  KAFKA_CONTAINERS=(
    "$KAFKA_BROKER"
    "$KAFKA_SCHEMA_REGISTRY"
    "$KAFKA_CONNECT"
    "$KAFKA_REST_PROXY"
    "$KAFKA_KSQLDB"
    "$KAFKA_UI_CONTAINER"
  )
else
  KAFKA_CONTAINERS=()
fi

# ───────────────────────────────────────────────────────────────────
# Cleanup / Remove mode  (-r)
# ───────────────────────────────────────────────────────────────────
do_remove() {
  section "Cleanup"
  log "Removing ScyllaDB containers..."
  for node in "${SCYLLA_NODES[@]}"; do remove_if_exists "$node"; done

  log "Removing Kafka containers (all editions)..."
  for c in \
    "$KAFKA_BROKER" "$KAFKA_SCHEMA_REGISTRY" "$KAFKA_CONNECT" \
    "$KAFKA_REST_PROXY" "$KAFKA_KSQLDB" \
    "$KAFKA_KAFDROP" "$KAFKA_CONTROL_CENTER"; do
    remove_if_exists "$c"
  done

  log "Removing Docker network: $NETWORK"
  if docker network inspect "$NETWORK" &>/dev/null; then
    docker network rm "$NETWORK" && ok "Removed network: $NETWORK"
  else
    warn "Network not found: $NETWORK"
  fi

  log "Cleaning /etc/hosts managed block..."
  hosts_remove_block && ok "Removed /etc/hosts entries"

  ok "Cleanup complete."
  exit 0
}

require_sudo
[[ "$REMOVE" == true ]] && do_remove

# ───────────────────────────────────────────────────────────────────
# Banner
# ───────────────────────────────────────────────────────────────────
echo -e "\n\033[1;36m"
cat <<EOF
  ╔══════════════════════════════════════════════════════════╗
  ║   ScyllaDB CDC Cluster Bootstrap                        ║
  ║   Network  : ${NETWORK}                      ║
  ║   ScyllaDB : ${#SCYLLA_NODES[@]} nodes                                   ║
  ║   Kafka    : ${WITH_KAFKA} $([ "$WITH_KAFKA" == "true" ] && echo "(${KAFKA_EDITION})" || echo "")
  ╚══════════════════════════════════════════════════════════╝
EOF
echo -e "\033[0m"

# ───────────────────────────────────────────────────────────────────
# Step 1 — Kernel tuning
# ───────────────────────────────────────────────────────────────────
section "Step 1 — Kernel Tuning"

SYSCTL_CONF="/etc/sysctl.d/99-scylla-cdc.conf"
sudo touch "$SYSCTL_CONF"

declare -A SYSCTL_PARAMS=(
  ["fs.aio-max-nr"]="2147483647"   # Linux kernel INT_MAX — absolute maximum
  ["net.core.somaxconn"]="4096"
  ["vm.swappiness"]="1"
  ["net.ipv4.tcp_rmem"]="4096 87380 16777216"
  ["net.ipv4.tcp_wmem"]="4096 65536 16777216"
)

for key in "${!SYSCTL_PARAMS[@]}"; do
  val="${SYSCTL_PARAMS[$key]}"
  sudo sysctl -w "${key}=${val}" > /dev/null
  sudo sed -i "/^${key}/d" "$SYSCTL_CONF"
  echo "${key} = ${val}" | sudo tee -a "$SYSCTL_CONF" > /dev/null
  ok "sysctl: ${key} = ${val}"
done
sudo sysctl -p "$SYSCTL_CONF" > /dev/null
ok "Persisted to $SYSCTL_CONF"

# ───────────────────────────────────────────────────────────────────
# Step 2 — Docker network
# ───────────────────────────────────────────────────────────────────
section "Step 2 — Docker Network"

if ! docker network inspect "$NETWORK" &>/dev/null; then
  docker network create "$NETWORK"
  ok "Created: $NETWORK"
else
  ok "Already exists: $NETWORK"
fi

# ───────────────────────────────────────────────────────────────────
# Step 3 — Remove stale containers
# ───────────────────────────────────────────────────────────────────
section "Step 3 — Stale Container Check"

ALL_CONTAINERS=("${SCYLLA_NODES[@]}" "${KAFKA_CONTAINERS[@]}")
for c in "${ALL_CONTAINERS[@]}"; do
  if container_exists "$c"; then
    warn "Stale: $c — removing..."
    docker rm -f "$c"
  fi
done
ok "Stale container check complete"

# ───────────────────────────────────────────────────────────────────
# Step 3b — Download ScyllaDB CDC Source Connector JAR
# Must happen before Connect starts (bind-mount at container launch)
# ───────────────────────────────────────────────────────────────────
# ───────────────────────────────────────────────────────────────────
# Step 3b — Resolve & download ScyllaDB CDC Source Connector from
#            Maven Central (before Connect container starts)
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  section "Step 3b — ScyllaDB CDC Connector (Maven Central)"

  mkdir -p "$CONNECTORS_DIR"

  # ── Resolve latest version from maven-metadata.xml ──────────────
  log "Querying Maven Central for latest version..."
  log "Metadata URL: $MAVEN_METADATA_URL"

  MAVEN_METADATA=$(curl -fsSL "$MAVEN_METADATA_URL") \
    || err "Failed to fetch Maven metadata — check network or URL:\n  $MAVEN_METADATA_URL"

  # Extract <latest> tag; fall back to last <version> entry if absent
  SCYLLA_CDC_CONNECTOR_VERSION=$(echo "$MAVEN_METADATA" \
    | grep -oP '(?<=<latest>)[^<]+' | head -1)

  if [[ -z "$SCYLLA_CDC_CONNECTOR_VERSION" ]]; then
    SCYLLA_CDC_CONNECTOR_VERSION=$(echo "$MAVEN_METADATA" \
      | grep -oP '(?<=<version>)[^<]+' | tail -1)
  fi

  [[ -z "$SCYLLA_CDC_CONNECTOR_VERSION" ]] \
    && err "Could not parse version from Maven metadata"

  SCYLLA_CDC_CONNECTOR_JAR="${MAVEN_ARTIFACT}-${SCYLLA_CDC_CONNECTOR_VERSION}-jar-with-dependencies.jar"
  SCYLLA_CDC_CONNECTOR_URL="${MAVEN_REPO_BASE}/${SCYLLA_CDC_CONNECTOR_VERSION}/${SCYLLA_CDC_CONNECTOR_JAR}"

  ok "Resolved version : $SCYLLA_CDC_CONNECTOR_VERSION"
  ok "JAR              : $SCYLLA_CDC_CONNECTOR_JAR"
  ok "Download URL     : $SCYLLA_CDC_CONNECTOR_URL"

  # ── Version check — banner if below 2.0.x ───────────────────────
  RESOLVED_MAJOR=$(echo "$SCYLLA_CDC_CONNECTOR_VERSION" | cut -d. -f1)

  if (( RESOLVED_MAJOR < MIN_RECOMMENDED_MAJOR )); then
    echo -e "\n\033[1;31m"
    cat <<'BANNER'
  ╔══════════════════════════════════════════════════════════════════════════════╗
  ║                                                                              ║
  ║   ██     ██  █████  ██████  ███    ██ ██ ███    ██  ██████                  ║
  ║   ██     ██ ██   ██ ██   ██ ████   ██ ██ ████   ██ ██                       ║
  ║   ██  █  ██ ███████ ██████  ██ ██  ██ ██ ██ ██  ██ ██   ███                 ║
  ║   ██ ███ ██ ██   ██ ██   ██ ██  ██ ██ ██ ██  ██ ██ ██    ██                 ║
  ║    ███ ███  ██   ██ ██   ██ ██   ████ ██ ██   ████  ██████                  ║
  ║                                                                              ║
  ╠══════════════════════════════════════════════════════════════════════════════╣
  ║                                                                              ║
  ║   SCYLLADB CDC CONNECTOR VERSION IS BELOW THE RECOMMENDED MINIMUM           ║
  ║                                                                              ║
  ╠══════════════════════════════════════════════════════════════════════════════╣
  ║                                                                              ║
BANNER
    echo -e "  ║   Resolved version : \033[1;33m${SCYLLA_CDC_CONNECTOR_VERSION}\033[1;31m"
    echo -e "  ║   Minimum required : \033[1;33m${MIN_RECOMMENDED_MAJOR}.0.x\033[1;31m"
    cat <<'BANNER'
  ║                                                                              ║
  ║   Versions below 2.0.0 do NOT support the ScyllaDB tablets architecture     ║
  ║   introduced in ScyllaDB 2025.x. Using an older connector against a         ║
  ║   tablets-enabled cluster will result in:                                   ║
  ║                                                                              ║
  ║     •  CDC pipeline failures at startup                                     ║
  ║     •  Silent data loss / missed CDC events                                 ║
  ║     •  Connector crashes on tablet rebalance                                ║
  ║                                                                              ║
  ║   ACTION REQUIRED:                                                           ║
  ║     Check https://repo1.maven.org/maven2/com/scylladb/                      ║
  ║           scylla-cdc-source-connector/                                      ║
  ║     for a 2.0.x release, or pin SCYLLA_CDC_CONNECTOR_VERSION manually.      ║
  ║                                                                              ║
  ║   The script will continue with the resolved version, but the CDC           ║
  ║   pipeline may not function correctly.                                       ║
  ║                                                                              ║
  ╚══════════════════════════════════════════════════════════════════════════════╝
BANNER
    echo -e "\033[0m"
    # Pause so the banner is actually read
    for i in 10 9 8 7 6 5 4 3 2 1; do
      echo -ne "\r  \033[1;33mContinuing in ${i}s ... (Ctrl-C to abort)\033[0m "
      sleep 1
    done
    echo ""
  else
    echo -e "\n\033[1;32m"
    cat <<'BANNER'
  ╔══════════════════════════════════════════════════════════════╗
  ║                                                              ║
  ║   ██████  ██   ██     ██████  ██████   ██████               ║
  ║  ██    ██ ██  ██     ██      ██    ██ ██    ██              ║
  ║  ██    ██ █████      ██      ██    ██ ██                    ║
  ║  ██    ██ ██  ██     ██      ██    ██ ██    ██              ║
  ║   ██████  ██   ██     ██████  ██████   ██████               ║
  ║                                                              ║
  ╠══════════════════════════════════════════════════════════════╣
BANNER
    echo -e "  ║   Version ${SCYLLA_CDC_CONNECTOR_VERSION} meets the 2.0.x+ requirement   ║"
    cat <<'BANNER'
  ║   Tablets architecture support: CONFIRMED                    ║
  ╚══════════════════════════════════════════════════════════════╝
BANNER
    echo -e "\033[0m"
  fi

  # ── Download if not already cached ──────────────────────────────
  if [[ ! -f "${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}" ]]; then
    log "Downloading from Maven Central..."
    curl -fSL --progress-bar \
      -o "${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}" \
      "$SCYLLA_CDC_CONNECTOR_URL" \
      || err "Download failed — check URL:\n  $SCYLLA_CDC_CONNECTOR_URL"
    ok "Saved: ${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}"
  else
    ok "Already cached — skipping download:"
    ok "  ${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}"
  fi

  if ! file "${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}" | grep -qiE "zip|java|archive"; then
    warn "File may not be a valid JAR — inspect manually:"
    file "${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}"
  fi
  ls -lh "${CONNECTORS_DIR}/${SCYLLA_CDC_CONNECTOR_JAR}"
fi

# ───────────────────────────────────────────────────────────────────
# ScyllaDB helpers
# ───────────────────────────────────────────────────────────────────
wait_for_scylla_un() {
  local node=$1 elapsed=0
  log "Waiting for $node → UN (Up/Normal)..."
  until docker exec "$node" nodetool status 2>/dev/null | grep -q "^UN"; do
    (( elapsed >= READY_TIMEOUT )) && err "Timeout: $node not UN after ${READY_TIMEOUT}s"
    echo -n "."; sleep 5; (( elapsed += 5 ))
  done
  echo ""; ok "$node is UN"
}

# ───────────────────────────────────────────────────────────────────
# Step 4 — ScyllaDB cluster (seed first, 30s apart)
# ───────────────────────────────────────────────────────────────────
section "Step 4 — ScyllaDB Cluster (3 nodes)"

log "Starting seed node: ${SCYLLA_NODES[0]}"
docker run \
  --name "${SCYLLA_NODES[0]}" \
  --hostname "${SCYLLA_NODES[0]}" \
  --network "$NETWORK" \
  -d "$SCYLLA_IMAGE"
wait_for_scylla_un "${SCYLLA_NODES[0]}"

for node in "${SCYLLA_NODES[@]:1}"; do
  log "Waiting ${BOOT_WAIT}s before starting $node..."
  sleep "$BOOT_WAIT"
  log "Starting node: $node"
  docker run \
    --name "$node" \
    --hostname "$node" \
    --network "$NETWORK" \
    -d "$SCYLLA_IMAGE" --seeds="$SEED_NODE"
  wait_for_scylla_un "$node"
done

# ───────────────────────────────────────────────────────────────────
# Kafka helpers
# ───────────────────────────────────────────────────────────────────
wait_for_http() {
  local name=$1 url=$2
  local elapsed=0 timeout=180
  log "Waiting for $name → $url"
  until curl -sf "$url" > /dev/null 2>&1; do
    (( elapsed >= timeout )) && err "Timeout: $name not ready after ${timeout}s"
    echo -n "."; sleep 5; (( elapsed += 5 ))
  done
  echo ""; ok "$name is ready"
}

wait_for_kafka_broker() {
  local elapsed=0 timeout=120
  log "Waiting for Kafka broker..."
  until docker exec "$KAFKA_BROKER" \
    kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
    (( elapsed >= timeout )) && err "Timeout: Kafka broker not ready after ${timeout}s"
    echo -n "."; sleep 5; (( elapsed += 5 ))
  done
  echo ""; ok "Kafka broker is ready"
}

# ───────────────────────────────────────────────────────────────────
# Step 5 — Confluent Kafka stack  (only when -k is passed)
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  section "Step 5 — Confluent Kafka (${KAFKA_EDITION^^}, v${CONFLUENT_VERSION})"

  log "Edition   : $KAFKA_EDITION"
  log "Broker    : $IMG_BROKER"
  log "Connect   : $IMG_CONNECT"
  log "SR        : $IMG_SCHEMA_REGISTRY"
  log "REST      : $IMG_REST_PROXY"
  log "ksqlDB    : $IMG_KSQLDB"
  log "UI        : $IMG_UI"

  # ── 5a. KRaft Cluster ID ─────────────────────────────────────────
  log "Generating KRaft Cluster ID..."
  CLUSTER_ID=$(docker run --rm "$IMG_BROKER" kafka-storage random-uuid)
  ok "Cluster ID: $CLUSTER_ID"

  # ── 5b. Broker ───────────────────────────────────────────────────
  log "Starting Kafka broker..."
  docker run -d \
    --name "$KAFKA_BROKER" \
    --hostname "$KAFKA_BROKER" \
    --network "$NETWORK" \
    -p 19093:9093 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES="broker,controller" \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@${KAFKA_BROKER}:29093" \
    -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${KAFKA_BROKER}:9092,PLAINTEXT_HOST://localhost:19093" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT" \
    -e KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE="true" \
    -e KAFKA_METRIC_REPORTERS="" \
    -e KAFKA_LOG_DIRS="/tmp/kraft-combined-logs" \
    -e CLUSTER_ID="$CLUSTER_ID" \
    "$IMG_BROKER"

  wait_for_kafka_broker

  # ── 5c. Schema Registry ──────────────────────────────────────────
  log "Starting Schema Registry..."
  docker run -d \
    --name "$KAFKA_SCHEMA_REGISTRY" \
    --hostname "$KAFKA_SCHEMA_REGISTRY" \
    --network "$NETWORK" \
    -p 18081:8081 \
    -e SCHEMA_REGISTRY_HOST_NAME="$KAFKA_SCHEMA_REGISTRY" \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="${KAFKA_BROKER}:9092" \
    -e SCHEMA_REGISTRY_LISTENERS="http://0.0.0.0:8081" \
    "$IMG_SCHEMA_REGISTRY"

  wait_for_http "Schema Registry" "http://localhost:18081/subjects"

  # ── 5d. Kafka Connect ────────────────────────────────────────────
  # Community : install Datagen via confluent-hub at container boot
  # Enterprise: Datagen is pre-bundled in the image — no install step
  log "Starting Kafka Connect (${KAFKA_EDITION})..."

  if [[ "$CONNECT_NEEDS_DATAGEN_INSTALL" == true ]]; then
    CONNECT_CMD='confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.6.4 && /etc/confluent/docker/run'
  else
    CONNECT_CMD='/etc/confluent/docker/run'
  fi

  docker run -d \
    --name "$KAFKA_CONNECT" \
    --hostname "$KAFKA_CONNECT" \
    --network "$NETWORK" \
    -p 18083:8083 \
    -v "${CONNECTORS_DIR}:/connectors" \
    -e CONNECT_BOOTSTRAP_SERVERS="${KAFKA_BROKER}:9092" \
    -e CONNECT_REST_ADVERTISED_HOST_NAME="$KAFKA_CONNECT" \
    -e CONNECT_REST_PORT=8083 \
    -e CONNECT_GROUP_ID="scylla-cdc-connect-group" \
    -e CONNECT_CONFIG_STORAGE_TOPIC="scylla-cdc-connect-configs" \
    -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000 \
    -e CONNECT_OFFSET_STORAGE_TOPIC="scylla-cdc-connect-offsets" \
    -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_STATUS_STORAGE_TOPIC="scylla-cdc-connect-status" \
    -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.storage.StringConverter" \
    -e CONNECT_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter" \
    -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
    -e CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/connectors" \
    "$IMG_CONNECT" \
    bash -c "$CONNECT_CMD"

  wait_for_http "Kafka Connect" "http://localhost:18083/connectors"

  # ── 5e. REST Proxy ───────────────────────────────────────────────
  log "Starting Kafka REST Proxy..."
  docker run -d \
    --name "$KAFKA_REST_PROXY" \
    --hostname "$KAFKA_REST_PROXY" \
    --network "$NETWORK" \
    -p 18082:8082 \
    -e KAFKA_REST_HOST_NAME="$KAFKA_REST_PROXY" \
    -e KAFKA_REST_BOOTSTRAP_SERVERS="${KAFKA_BROKER}:9092" \
    -e KAFKA_REST_LISTENERS="http://0.0.0.0:8082" \
    -e KAFKA_REST_SCHEMA_REGISTRY_URL="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
    "$IMG_REST_PROXY"

  wait_for_http "REST Proxy" "http://localhost:18082/topics"

  # ── 5f. ksqlDB ───────────────────────────────────────────────────
  log "Starting ksqlDB Server..."
  docker run -d \
    --name "$KAFKA_KSQLDB" \
    --hostname "$KAFKA_KSQLDB" \
    --network "$NETWORK" \
    -p 18088:8088 \
    -e KSQL_CONFIG_DIR="/etc/ksql" \
    -e KSQL_BOOTSTRAP_SERVERS="${KAFKA_BROKER}:9092" \
    -e KSQL_HOST_NAME="$KAFKA_KSQLDB" \
    -e KSQL_LISTENERS="http://0.0.0.0:8088" \
    -e KSQL_CACHE_MAX_BYTES_BUFFERING=0 \
    -e KSQL_KSQL_SCHEMA_REGISTRY_URL="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
    -e KSQL_KSQL_CONNECT_URL="http://${KAFKA_CONNECT}:8083" \
    -e KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR=1 \
    -e KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE="true" \
    -e KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE="true" \
    "$IMG_KSQLDB"

  wait_for_http "ksqlDB" "http://localhost:18088/info"

  # ── 5g. UI ───────────────────────────────────────────────────────
  if [[ "$KAFKA_EDITION" == "community" ]]; then
    log "Starting Kafdrop (OSS UI)..."
    docker run -d \
      --name "$KAFKA_KAFDROP" \
      --hostname "$KAFKA_KAFDROP" \
      --network "$NETWORK" \
      -p 19000:9000 \
      -e KAFKA_BROKERCONNECT="${KAFKA_BROKER}:9092" \
      -e SCHEMAREGISTRY_CONNECT="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
      -e JVM_OPTS="-Xms32M -Xmx128M" \
      "$IMG_UI"
    wait_for_http "Kafdrop" "http://localhost:19000/actuator/health"

  else
    log "Starting Confluent Control Center (enterprise UI)..."
    docker run -d \
      --name "$KAFKA_CONTROL_CENTER" \
      --hostname "$KAFKA_CONTROL_CENTER" \
      --network "$NETWORK" \
      -p 19021:9021 \
      -e CONTROL_CENTER_BOOTSTRAP_SERVERS="${KAFKA_BROKER}:9092" \
      -e CONTROL_CENTER_SCHEMA_REGISTRY_URL="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
      -e CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER="http://${KAFKA_CONNECT}:8083" \
      -e CONTROL_CENTER_KSQL_KSQLDB1_URL="http://${KAFKA_KSQLDB}:8088" \
      -e CONTROL_CENTER_KSQL_KSQLDB1_ADVERTISED_URL="http://localhost:18088" \
      -e CONTROL_CENTER_REPLICATION_FACTOR=1 \
      -e CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS=1 \
      -e CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS=1 \
      -e CONFLUENT_METRICS_TOPIC_REPLICATION=1 \
      -e PORT=9021 \
      "$IMG_UI"
    # Control Center takes ~60s to fully initialise
    wait_for_http "Control Center" "http://localhost:19021/health"
  fi

  # ── 5h. Create quickstart topics ─────────────────────────────────
  section "Step 5h — Quickstart Topics"

  for topic in pageviews users; do
    log "Creating topic: $topic"
    docker exec "$KAFKA_BROKER" kafka-topics \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions 1 \
      --replication-factor 1
    ok "Topic ready: $topic"
  done

  # ── 5i. Register Datagen connectors ──────────────────────────────
  section "Step 5i — Datagen Source Connectors"

  for connector_cfg in \
    "datagen-pageviews|pageviews|pageviews|100" \
    "datagen-users|users|users|1000"; do

    IFS='|' read -r conn_name topic quickstart interval <<< "$connector_cfg"
    log "Registering $conn_name..."
    curl -sf -X POST http://localhost:18083/connectors \
      -H "Content-Type: application/json" \
      -d "{
        \"name\": \"${conn_name}\",
        \"config\": {
          \"connector.class\": \"io.confluent.kafka.connect.datagen.DatagenConnector\",
          \"kafka.topic\": \"${topic}\",
          \"quickstart\": \"${quickstart}\",
          \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
          \"value.converter\": \"io.confluent.connect.avro.AvroConverter\",
          \"value.converter.schema.registry.url\": \"http://${KAFKA_SCHEMA_REGISTRY}:8081\",
          \"max.interval\": \"${interval}\",
          \"tasks.max\": \"1\"
        }
      }" | python3 -m json.tool || warn "${conn_name} may already exist"
    ok "$conn_name registered"
  done
fi

# ───────────────────────────────────────────────────────────────────
# Step 6 — Validate all containers are on scylla-cdc-bridge
# ───────────────────────────────────────────────────────────────────
section "Step 6 — Network Validation"

ALL_RUNNING=("${SCYLLA_NODES[@]}" "${KAFKA_CONTAINERS[@]}")
for c in "${ALL_RUNNING[@]}"; do
  net=$(docker inspect "$c" \
    --format "{{range \$k, \$v := .NetworkSettings.Networks}}{{\$k}} {{end}}" 2>/dev/null \
    || echo "unknown")
  if echo "$net" | grep -q "$NETWORK"; then
    ok "$c → $NETWORK ✓"
  else
    err "$c is NOT on $NETWORK (found: $net)"
  fi
done

# ───────────────────────────────────────────────────────────────────
# Step 7 — /etc/hosts (idempotent)
# ───────────────────────────────────────────────────────────────────
section "Step 7 — /etc/hosts"

hosts_remove_block

declare -a HOSTS_ENTRIES=()
for c in "${ALL_RUNNING[@]}"; do
  ip=$(get_container_ip "$c")
  [[ -z "$ip" ]] && err "No IP found for $c on $NETWORK"
  HOSTS_ENTRIES+=("${ip}  ${c}")
  ok "/etc/hosts: ${ip}  ${c}"
done

hosts_add_block "${HOSTS_ENTRIES[@]}"
ok "/etc/hosts updated (${#HOSTS_ENTRIES[@]} entries)"

# ───────────────────────────────────────────────────────────────────
# Step 8 — ScyllaDB nodetool status
# ───────────────────────────────────────────────────────────────────
section "Step 8 — ScyllaDB Cluster Status"

for node in "${SCYLLA_NODES[@]}"; do
  echo -e "\n\033[1;36m─── nodetool status @ ${node} ───\033[0m"
  docker exec "$node" nodetool status
done

# ───────────────────────────────────────────────────────────────────
# Step 9 — Kafka summary
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  section "Step 9 — Kafka Stack Summary"

  echo -e "\n\033[1;36mActive connectors:\033[0m"
  curl -sf http://localhost:18083/connectors | python3 -m json.tool || true

  echo -e "\n\033[1;36mAll topics:\033[0m"
  docker exec "$KAFKA_BROKER" kafka-topics --bootstrap-server localhost:9092 --list

  if [[ "$KAFKA_EDITION" == "community" ]]; then
    UI_LABEL="Kafdrop UI (OSS)"
    UI_URL="http://localhost:19000"
  else
    UI_LABEL="Control Center (enterprise)"
    UI_URL="http://localhost:19021"
  fi

  echo -e "\n\033[1;33m"
  cat <<EOF
  ┌──────────────────────────────────────────────────────────────────┐
  │   Confluent ${KAFKA_EDITION^^} — Service Endpoints
  ├─────────────────────────────────┬────────────────────────────────┤
  │  ${UI_LABEL}
  │    → ${UI_URL}
  ├─────────────────────────────────┬────────────────────────────────┤
  │  Kafka Broker  (host)           │  localhost:19093               │
  │  Kafka Broker  (container)      │  ${KAFKA_BROKER}:9092     │
  │  Schema Registry                │  http://localhost:18081         │
  │  Kafka Connect REST             │  http://localhost:18083         │
  │  Kafka REST Proxy               │  http://localhost:18082         │
  │  ksqlDB                         │  http://localhost:18088         │
  ├─────────────────────────────────┴────────────────────────────────┤
  │  ScyllaDB CQL (node 1)          │  scylla-cdc-quickstart:9042    │
  │  ScyllaDB CQL (node 2)          │  scylla-cdc-quickstart-2:9042  │
  │  ScyllaDB CQL (node 3)          │  scylla-cdc-quickstart-3:9042  │
  ├──────────────────────────────────────────────────────────────────┤
  │  ScyllaDB CDC Connector (Maven Central)
  │  Version  : ${SCYLLA_CDC_CONNECTOR_VERSION}
  │  Host path: ${CONNECTORS_DIR}
  │  Container: /connectors/${SCYLLA_CDC_CONNECTOR_JAR}
  └──────────────────────────────────────────────────────────────────┘
EOF
  echo -e "\033[0m"
fi

ok "All done. Network: $NETWORK"
