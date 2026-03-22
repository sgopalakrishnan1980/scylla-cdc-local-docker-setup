#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
# ScyllaDB CDC Cluster Bootstrap
#
# Usage: ./scylla-cdc-cluster.sh [-k community|enterprise] [-d] [-r] [-h]
#
#   -k community   Start Confluent community images alongside ScyllaDB
#   -k enterprise  Start Confluent enterprise images alongside ScyllaDB
#   -d             Enable debug mode (set -x — prints every command)
#   -r             Remove all containers, network, /etc/hosts entries
#   -h             Show help and exit
# ═══════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────
# Static configuration
# ───────────────────────────────────────────────────────────────────
NETWORK="scylla-cdc-bridge"
HOSTS_FILE="/etc/hosts"
HOSTS_MARKER="# scylla-cdc-bridge managed block"

# ScyllaDB
SCYLLA_IMAGE="scylladb/scylla:2025.4"   # floating tag — always resolves to latest 2025.4.x patch
SCYLLA_NODES=(scylla-cdc-quickstart scylla-cdc-quickstart-2 scylla-cdc-quickstart-3)
SEED_NODE="${SCYLLA_NODES[0]}"
BOOT_WAIT=30       # seconds between node starts
READY_TIMEOUT=180  # seconds to wait for UN state

# Confluent — shared
CONFLUENT_VERSION="7.6.1"
KAFKA_BROKER="scylla-cdc-broker"
KAFKA_SCHEMA_REGISTRY="scylla-cdc-schema-registry"
KAFKA_CONNECT="scylla-cdc-connect"
KAFKA_KAFDROP="scylla-cdc-kafdrop"  # OSS UI — both editions

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
DEBUG=false

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
                             └─ ScyllaDB CDC connector loaded from /connectors
            Schema Registry  confluentinc/cp-schema-registry:${CONFLUENT_VERSION}
            Web UI           obsidiandynamics/kafdrop (OSS)
                             └─ http://localhost:19000

        $(echo -e "\033[1menterprise\033[0m")
            Uses Confluent Platform enterprise broker image.
            Same Kafdrop UI as community (Control Center not started —
            it creates unnecessary internal topics).

            Component        Image
            ─────────────────────────────────────────────────────────────
            Broker (KRaft)   confluentinc/cp-server:${CONFLUENT_VERSION}
            Kafka Connect    confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}
                             └─ ScyllaDB CDC connector loaded from /connectors
            Schema Registry  confluentinc/cp-schema-registry:${CONFLUENT_VERSION}
            Web UI           obsidiandynamics/kafdrop (OSS)
                             └─ http://localhost:19000

        Not started (not required for ScyllaDB CDC):
            REST Proxy, ksqlDB, Control Center

    $(echo -e "\033[1;33m-d\033[0m")
        Enable debug mode. Sets \`set -x\` immediately after argument
        parsing so every command and its expansion is printed to stderr.
        Useful for diagnosing failures in any step.

    $(echo -e "\033[1;33m-r\033[0m")
        Deep clean — removes containers, network, volumes, dangling images,
        build cache, connector JAR directory, and /etc/hosts entries.
        Also runs: docker container prune, image prune, volume prune,
        network prune, builder prune.
        Prints a docker system df summary after cleaning.
        Safe to run regardless of which edition was started.
        NOTE: does NOT remove all pulled images — run
              'docker image prune -af' or 'docker system prune -af --volumes'
              to reclaim all image storage.

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
        scylla-cdc-kafdrop          (OSS UI — both editions)

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

    Version check against the 2.0.x minimum:
      ✓ 2.0.x or higher → green OK line, proceed
      ✗ below 2.0.x     → red error line, script exits immediately
                          (tablets architecture requires 2.0.x+)

$(echo -e "\033[1mKERNEL TUNING\033[0m")
    Applied at every run (requires sudo). Persisted to:
        /etc/sysctl.d/99-scylla-cdc.conf

        fs.aio-max-nr              = 8388608    (8M — sized for 3-node laptop cluster)
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
    Kafdrop UI             19000    HTTP

$(echo -e "\033[1mEXAMPLES\033[0m")
    # ScyllaDB 3-node cluster only
    $0

    # ScyllaDB + Confluent community (free, Kafdrop UI)
    $0 -k community

    # ScyllaDB + Confluent enterprise (Control Center, 30-day trial)
    $0 -k enterprise

    # Community with debug output
    $0 -k community -d

    # Tear down everything (both editions, network, /etc/hosts)
    $0 -r

EOF
  exit "$exit_code"
}

# ───────────────────────────────────────────────────────────────────
# Argument parsing
# ───────────────────────────────────────────────────────────────────
while getopts ":k:drh" opt; do
  case $opt in
    k)
      WITH_KAFKA=true
      KAFKA_EDITION="${OPTARG}"
      if [[ "$KAFKA_EDITION" != "community" && "$KAFKA_EDITION" != "enterprise" ]]; then
        err "Invalid edition '${KAFKA_EDITION}'. Must be 'community' or 'enterprise'."
      fi
      ;;
    d) DEBUG=true ;;
    r) REMOVE=true ;;
    h) usage 0 ;;
    :) err "Option -${OPTARG} requires an argument (community|enterprise)." ;;
    *) usage 1 ;;
  esac
done

[[ "$DEBUG" == true ]] && set -x && log "Debug mode enabled (set -x)"

# ───────────────────────────────────────────────────────────────────
# Resolve image names and UI container based on edition
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  # Required for ScyllaDB CDC pipeline:
  #   Broker        — event store
  #   Schema Registry — Kafka Connect worker dependency
  #   Kafka Connect  — ScyllaDB CDC connector host
  #   Kafdrop        — lightweight OSS UI (both editions)
  # REST Proxy, ksqlDB and Control Center are NOT started —
  # they are not required for CDC and create unnecessary topics/images.
  if [[ "$KAFKA_EDITION" == "community" ]]; then
    IMG_BROKER="confluentinc/cp-kafka:${CONFLUENT_VERSION}"
  else
    IMG_BROKER="confluentinc/cp-server:${CONFLUENT_VERSION}"
  fi
  IMG_CONNECT="confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}"
  IMG_SCHEMA_REGISTRY="confluentinc/cp-schema-registry:${CONFLUENT_VERSION}"
  IMG_KAFDROP="obsidiandynamics/kafdrop:latest"

  KAFKA_CONTAINERS=(
    "$KAFKA_BROKER"
    "$KAFKA_SCHEMA_REGISTRY"
    "$KAFKA_CONNECT"
    "$KAFKA_KAFDROP"
  )
else
  KAFKA_CONTAINERS=()
fi

# ───────────────────────────────────────────────────────────────────
# Cleanup / Remove mode  (-r)
# Deep cleans containers, volumes, images, build cache, networks,
# dangling objects, and the connector JAR directory.
# ───────────────────────────────────────────────────────────────────
do_remove() {
  section "Deep Clean"

  # ── 1. Stop and remove named containers ─────────────────────────
  log "Removing ScyllaDB containers..."
  for node in "${SCYLLA_NODES[@]}"; do remove_if_exists "$node"; done

  log "Removing Kafka containers (all editions)..."
  for c in \
    "$KAFKA_BROKER" "$KAFKA_SCHEMA_REGISTRY" "$KAFKA_CONNECT" \
    "$KAFKA_KAFDROP"; do
    remove_if_exists "$c"
  done

  # ── 2. Remove Docker network ─────────────────────────────────────
  log "Removing Docker network: $NETWORK"
  if docker network inspect "$NETWORK" &>/dev/null; then
    docker network rm "$NETWORK" && ok "Removed network: $NETWORK"
  else
    warn "Network not found: $NETWORK"
  fi

  # ── 3. Remove named volumes for our containers ───────────────────
  log "Removing named Docker volumes for ScyllaDB and Kafka..."
  for vol in \
    scylla-cdc-quickstart-data \
    scylla-cdc-quickstart-2-data \
    scylla-cdc-quickstart-3-data \
    scylla-cdc-broker-data \
    scylla-cdc-connect-data; do
    if docker volume inspect "$vol" &>/dev/null; then
      docker volume rm "$vol" && ok "Removed volume: $vol"
    fi
  done

  # ── 4. Prune stopped containers (catch any strays) ───────────────
  log "Pruning all stopped containers..."
  PRUNED_CONTAINERS=$(docker container prune -f 2>/dev/null | tail -1)
  ok "Containers pruned: ${PRUNED_CONTAINERS:-none}"

  # ── 5. Prune dangling images ──────────────────────────────────────
  log "Pruning dangling (untagged) images..."
  PRUNED_IMAGES=$(docker image prune -f 2>/dev/null | tail -1)
  ok "Images pruned: ${PRUNED_IMAGES:-none}"

  # ── 6. Prune unused volumes ───────────────────────────────────────
  log "Pruning unused Docker volumes..."
  PRUNED_VOLS=$(docker volume prune -f 2>/dev/null | tail -1)
  ok "Volumes pruned: ${PRUNED_VOLS:-none}"

  # ── 7. Prune build cache ──────────────────────────────────────────
  log "Pruning Docker build cache..."
  PRUNED_CACHE=$(docker builder prune -f 2>/dev/null | tail -1)
  ok "Build cache pruned: ${PRUNED_CACHE:-none}"

  # ── 8. Prune unused networks ──────────────────────────────────────
  log "Pruning unused Docker networks..."
  PRUNED_NETS=$(docker network prune -f 2>/dev/null | tail -1)
  ok "Networks pruned: ${PRUNED_NETS:-none}"

  # ── 9. Remove connector JAR directory ────────────────────────────
  if [[ -d "${CONNECTORS_DIR}" ]]; then
    log "Removing connector JAR directory: ${CONNECTORS_DIR}"
    rm -rf "${CONNECTORS_DIR}" && ok "Removed: ${CONNECTORS_DIR}"
  else
    warn "Connectors dir not found, skipping: ${CONNECTORS_DIR}"
  fi

  # ── 10. Clean /etc/hosts managed block ───────────────────────────
  log "Cleaning /etc/hosts managed block..."
  hosts_remove_block && ok "Removed /etc/hosts entries"

  # ── 11. Final disk usage report ──────────────────────────────────
  section "Docker Disk Usage After Clean"
  docker system df

  echo ""
  log "To remove ALL unused images (not just dangling) run:"
  echo "  docker image prune -af"
  log "To do a full system prune including all unused images:"
  echo "  docker system prune -af --volumes"

  ok "Deep clean complete."
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
  ["fs.aio-max-nr"]="8388608"       # 8M — sized for 3 ScyllaDB nodes + headroom (~2.1M/node)
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

# ── Verify kernel actually reflects the values (catches reboot/reset issues) ──
log "Verifying sysctl values are active in the running kernel..."
SYSCTL_FAIL=false
for key in "${!SYSCTL_PARAMS[@]}"; do
  expected="${SYSCTL_PARAMS[$key]}"
  actual=$(sysctl -n "$key" 2>/dev/null || echo "UNREADABLE")
  if [[ "$actual" == "$expected" ]]; then
    ok "  ${key} = ${actual} ✓"
  else
    warn "  ${key} MISMATCH — expected: ${expected}  actual: ${actual}"
    log "  Re-applying: sudo sysctl -w ${key}=${expected}"
    sudo sysctl -w "${key}=${expected}" > /dev/null \
      && ok "  ${key} re-applied ✓" \
      || { err "  Failed to set ${key} — check sudo permissions"; SYSCTL_FAIL=true; }
  fi
done
[[ "$SYSCTL_FAIL" == true ]] && err "One or more sysctl values could not be applied — aborting"

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

  # ── Version check — simple pass/fail ───────────────────────────
  RESOLVED_MAJOR=$(echo "$SCYLLA_CDC_CONNECTOR_VERSION" | cut -d. -f1)

  if (( RESOLVED_MAJOR < MIN_RECOMMENDED_MAJOR )); then
    echo -e "\033[1;31m[ERROR] Connector version ${SCYLLA_CDC_CONNECTOR_VERSION} is below the required minimum ${MIN_RECOMMENDED_MAJOR}.0.x\033[0m"
    echo -e "\033[1;31m        Tablets architecture (ScyllaDB 2025.x+) requires connector 2.0.x or higher\033[0m"
    echo -e "\033[1;31m        Check: https://repo1.maven.org/maven2/com/scylladb/scylla-cdc-source-connector/\033[0m"
    exit 1
  else
    ok "Connector version ${SCYLLA_CDC_CONNECTOR_VERSION} meets the ${MIN_RECOMMENDED_MAJOR}.0.x+ requirement ✓"
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
# ── AIO headroom check before starting ScyllaDB ─────────────────────
# Each ScyllaDB node consumes ~700K AIO slots. nodetool and other tools
# also need slots. Verify enough headroom exists before launching nodes.
section "Step 4 — ScyllaDB Cluster (3 nodes)"

AIO_MAX=$(cat /proc/sys/fs/aio-max-nr)
AIO_CURRENT=$(cat /proc/sys/fs/aio-nr)
AIO_FREE=$(( AIO_MAX - AIO_CURRENT ))
AIO_NEEDED=$(( 700000 * ${#SCYLLA_NODES[@]} ))

log "AIO capacity check:"
echo "  aio-max-nr  (limit)  : $AIO_MAX"
echo "  aio-nr      (in use) : $AIO_CURRENT"
echo "  available            : $AIO_FREE"
echo "  needed (~700K/node)  : $AIO_NEEDED"

if (( AIO_FREE < AIO_NEEDED )); then
  warn "Insufficient AIO slots. Re-applying fs.aio-max-nr..."
  sudo sysctl -w fs.aio-max-nr=8388608 \
    && ok "fs.aio-max-nr reset to 8388608" \
    || err "Could not set fs.aio-max-nr — run: sudo sysctl -w fs.aio-max-nr=8388608"
else
  ok "AIO headroom sufficient ($AIO_FREE free, $AIO_NEEDED needed)"
fi

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
    -d "$SCYLLA_IMAGE" \
    --seeds="$SEED_NODE"
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
  log "UI        : $IMG_KAFDROP"

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
  # ScyllaDB CDC connector JAR is pre-staged in /connectors via
  # bind-mount (downloaded in Step 3b from Maven Central).
  # No Datagen plugin — not needed for ScyllaDB CDC pipeline.
  log "Starting Kafka Connect (${KAFKA_EDITION})..."

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
    -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="true" \
    -e CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/connectors" \
    "$IMG_CONNECT"

  wait_for_http "Kafka Connect" "http://localhost:18083/connectors"

  # Verify the ScyllaDB CDC connector plugin loaded successfully
  log "Verifying ScyllaDB CDC connector plugin is visible to Connect..."
  PLUGIN_FOUND=$(curl -sf "http://localhost:18083/connector-plugins" \
    | python3 -c "
import sys, json
plugins = json.load(sys.stdin)
found = any('ScyllaConnector' in p.get('class','') or 'scylladb' in p.get('class','').lower()
            for p in plugins)
print('FOUND' if found else 'NOT_FOUND')
" 2>/dev/null || echo "NOT_FOUND")

  if [[ "$PLUGIN_FOUND" == "FOUND" ]]; then
    ok "ScyllaDB CDC connector plugin loaded ✓"
  else
    warn "ScyllaDB CDC connector plugin NOT found in Connect plugin list"
    warn "Check the JAR is present: docker exec $KAFKA_CONNECT ls -lh /connectors/"
    warn "Check CONNECT_PLUGIN_PATH includes /connectors"
    warn "Available plugins:"
    curl -sf "http://localhost:18083/connector-plugins" \
      | python3 -c "import sys,json; [print('  ',p['class']) for p in json.load(sys.stdin)]" \
      2>/dev/null || true
  fi

  # ── 5e. Kafdrop UI (both editions) ──────────────────────────────
  log "Starting Kafdrop (OSS UI — http://localhost:19000)..."
  docker run -d \
    --name "$KAFKA_KAFDROP" \
    --hostname "$KAFKA_KAFDROP" \
    --network "$NETWORK" \
    -p 19000:9000 \
    -e KAFKA_BROKERCONNECT="${KAFKA_BROKER}:9092" \
    -e SCHEMAREGISTRY_CONNECT="http://${KAFKA_SCHEMA_REGISTRY}:8081" \
    -e JVM_OPTS="-Xms32M -Xmx128M" \
    "$IMG_KAFDROP"
  wait_for_http "Kafdrop" "http://localhost:19000/actuator/health"

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

  echo -e "\n\033[1;36mAll topics:\033[0m"
  docker exec "$KAFKA_BROKER" kafka-topics --bootstrap-server localhost:9092 --list

  echo -e "\n\033[1;33m"
  cat <<EOF
  ┌──────────────────────────────────────────────────────────────────┐
  │   ScyllaDB CDC — Service Endpoints
  ├─────────────────────────────────┬────────────────────────────────┤
  │  Kafdrop UI                     │  http://localhost:19000         │
  │  Kafka Broker  (host)           │  localhost:19093               │
  │  Kafka Broker  (container)      │  ${KAFKA_BROKER}:9092     │
  │  Schema Registry                │  http://localhost:18081         │
  │  Kafka Connect REST             │  http://localhost:18083         │
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

# ───────────────────────────────────────────────────────────────────
# Step 10 — Connector health check (if Kafka stack is running)
# Lists all registered connectors, prints their status, and reports
# any connector or task that is not in RUNNING state.
# ───────────────────────────────────────────────────────────────────
if [[ "$WITH_KAFKA" == true ]]; then
  section "Step 10 — Connector Health Check"

  CONNECT_URL_LOCAL="http://localhost:18083"
  CONNECTOR_ERRORS=0

  # Get list of all registered connectors
  CONNECTORS_JSON=$(curl -sf "${CONNECT_URL_LOCAL}/connectors" 2>/dev/null || true)

  if [[ -z "$CONNECTORS_JSON" || "$CONNECTORS_JSON" == "[]" ]]; then
    warn "No connectors are registered yet."
    warn "Run ./test-cdc-pipeline.sh -s to register the ScyllaDB CDC connector."
  else
    CONNECTOR_NAMES=$(echo "$CONNECTORS_JSON" \
      | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" \
      2>/dev/null || true)

    TOTAL=$(echo "$CONNECTOR_NAMES" | grep -c . || true)
    log "Found ${TOTAL} registered connector(s):"

    # ── Per-connector status report ──────────────────────────────────
    while IFS= read -r connector; do
      [[ -z "$connector" ]] && continue

      STATUS_JSON=$(curl -sf \
        "${CONNECT_URL_LOCAL}/connectors/${connector}/status" \
        2>/dev/null || true)

      if [[ -z "$STATUS_JSON" ]]; then
        warn "  [$connector] Could not retrieve status"
        (( CONNECTOR_ERRORS++ ))
        continue
      fi

      # Parse connector-level and task-level states via python
      python3 - <<PYEOF
import json, sys

data = json.loads('''${STATUS_JSON}''')
name        = data.get('name', '${connector}')
conn_state  = data.get('connector', {}).get('state', 'UNKNOWN')
conn_worker = data.get('connector', {}).get('worker_id', '?')
tasks       = data.get('tasks', [])
conn_type   = data.get('type', 'unknown')

GREEN  = '\033[1;32m'
RED    = '\033[1;31m'
YELLOW = '\033[1;33m'
CYAN   = '\033[1;36m'
RESET  = '\033[0m'

state_colour = GREEN if conn_state == 'RUNNING' else RED

print(f"\n  {CYAN}Connector: {name}{RESET}  [{conn_type}]")
print(f"  {'─' * 60}")
print(f"  Connector state : {state_colour}{conn_state}{RESET}  (worker: {conn_worker})")
print(f"  Task count      : {len(tasks)}")

task_errors = 0
for task in tasks:
    tid    = task.get('id', '?')
    tstate = task.get('state', 'UNKNOWN')
    twrk   = task.get('worker_id', '?')
    trace  = task.get('trace', '')

    tc = GREEN if tstate == 'RUNNING' else RED
    print(f"\n    Task {tid}: {tc}{tstate}{RESET}  (worker: {twrk})")

    if tstate != 'RUNNING' and trace:
        # Print first 5 lines of the stack trace
        lines = trace.strip().split('\n')
        print(f"    {YELLOW}Stack trace (first 5 lines):{RESET}")
        for line in lines[:5]:
            print(f"      {line}")
        if len(lines) > 5:
            print(f"      ... ({len(lines) - 5} more lines)")
        task_errors += 1

if conn_state != 'RUNNING' or task_errors > 0:
    print(f"\n  {RED}✗ ERRORS DETECTED — connector or task(s) not RUNNING{RESET}")
    sys.exit(1)
else:
    print(f"\n  {GREEN}✓ All tasks RUNNING{RESET}")
    sys.exit(0)
PYEOF

      if [[ $? -ne 0 ]]; then
        (( CONNECTOR_ERRORS++ ))
      fi

    done <<< "$CONNECTOR_NAMES"

    # ── Final health summary ──────────────────────────────────────────
    echo ""
    if [[ "$CONNECTOR_ERRORS" -eq 0 ]]; then
      echo -e "\033[1;32m"
      cat <<'EOF'
  ╔══════════════════════════════════════════════════════════╗
  ║   ✓  ALL CONNECTORS HEALTHY — All tasks RUNNING         ║
  ╚══════════════════════════════════════════════════════════╝
EOF
      echo -e "\033[0m"
    else
      echo -e "\033[1;31m"
      cat <<EOF
  ╔══════════════════════════════════════════════════════════╗
  ║   ✗  CONNECTOR ERRORS DETECTED                          ║
  ║   Failed connectors : ${CONNECTOR_ERRORS}
  ╠══════════════════════════════════════════════════════════╣
  ║   Useful debug commands:                                 ║
  ║                                                          ║
  ║   # Full status of a specific connector                  ║
  ║   curl -s http://localhost:18083/connectors/<name>/status \\
  ║     | python3 -m json.tool                              ║
  ║                                                          ║
  ║   # Restart a failed task                                ║
  ║   curl -X POST http://localhost:18083/connectors/<name> \\
  ║     /tasks/<id>/restart                                  ║
  ║                                                          ║
  ║   # Restart the entire connector                         ║
  ║   curl -X POST http://localhost:18083/connectors/<name> \\
  ║     /restart                                             ║
  ║                                                          ║
  ║   # Connect worker logs                                  ║
  ║   docker logs scylla-cdc-connect --tail 50              ║
  ╚══════════════════════════════════════════════════════════╝
EOF
      echo -e "\033[0m"
    fi
  fi
fi

ok "All done. Network: $NETWORK"
