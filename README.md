# ScyllaDB CDC Cluster Bootstrap

A single-script infrastructure bootstrapper that stands up a 3-node ScyllaDB
cluster and optionally a full Confluent Kafka stack — all on a shared Docker
bridge network — with kernel tuning, `/etc/hosts` management, and the
ScyllaDB CDC Source Connector pre-staged from Maven Central.

---

## Table of Contents

1. [Requirements](#requirements)
2. [File Layout](#file-layout)
3. [Quick Start](#quick-start)
4. [Script Reference](#script-reference)
5. [Configuration Variables](#configuration-variables)
6. [Container Inventory](#container-inventory)
7. [Port Map](#port-map)
8. [Kernel Tuning](#kernel-tuning)
9. [ScyllaDB CDC Connector](#scylladb-cdc-connector)
10. [Edition Comparison: Community vs Enterprise](#edition-comparison)
11. [Networking Model](#networking-model)
12. [Teardown](#teardown)
13. [Troubleshooting](#troubleshooting)

---

## Requirements

### Host OS

| Requirement | Minimum | Notes |
|---|---|---|
| Linux kernel | 4.19+ | Seastar AIO requirements |
| Distribution | Any modern (Fedora, Ubuntu, Debian, RHEL, Amazon Linux) | Tested on Fedora 39+ |
| Architecture | x86_64 | ARM not supported by ScyllaDB Docker images |

### Software

| Tool | Version | Purpose |
|---|---|---|
| `docker` | 20.10+ | Container runtime |
| `bash` | 4.x+ | Script execution (associative arrays, `getopts`) |
| `curl` | Any | Maven Central metadata + JAR download |
| `sudo` | Any | Kernel sysctl tuning, `/etc/hosts` writes |
| `python3` | 3.6+ | Pretty-printing connector JSON responses |
| `file` | Any | JAR integrity check after download |
| `jq` | Optional | Useful for inspecting Kafka Connect API responses |
| `cqlsh` | Optional | Direct ScyllaDB CQL access from host |

### Docker Resources

| Resource | Recommended minimum |
|---|---|
| CPU cores | 4 (2 per ScyllaDB node minimum) |
| RAM | 8 GB (16 GB recommended with Kafka stack) |
| Disk | 20 GB free |
| Docker memory limit | Not restricted (ScyllaDB Seastar manages memory directly) |

> **macOS / Windows**: Not supported. ScyllaDB requires a native Linux kernel.
> Docker Desktop does not expose the host kernel's AIO subsystem.

---

## File Layout

```
.
├── scylla-cdc-cluster.sh       # Main bootstrap script
├── README.md                   # This file
├── infra-diagram.txt           # ASCII infrastructure diagram
├── cdc-flow-diagram.txt        # ScyllaDB → Kafka CDC flow diagram
├── test-cdc-pipeline.sh        # Data push + connector validation script
└── volumes/
    └── connectors/
        └── scylla-cdc-source-connector-<version>-jar-with-dependencies.jar
```

---

## Quick Start

```bash
# Make executable
chmod +x scylla-cdc-cluster.sh test-cdc-pipeline.sh

# ScyllaDB only (3-node cluster)
./scylla-cdc-cluster.sh

# ScyllaDB + Confluent community (free, Kafdrop UI)
./scylla-cdc-cluster.sh -k community

# ScyllaDB + Confluent enterprise (Control Center, 30-day trial)
./scylla-cdc-cluster.sh -k enterprise

# Full help
./scylla-cdc-cluster.sh -h

# Teardown everything
./scylla-cdc-cluster.sh -r
```

---

## Script Reference

```
./scylla-cdc-cluster.sh [OPTIONS]

  -k community   Start Confluent community edition alongside ScyllaDB
  -k enterprise  Start Confluent enterprise edition alongside ScyllaDB
  -r             Remove all containers, network, /etc/hosts entries
  -h             Show full help and exit
```

### Execution Sequence

```
Step 1   Kernel tuning       sysctl — aio-max-nr, somaxconn, swappiness, TCP buffers
Step 2   Docker network      Create scylla-cdc-bridge (idempotent)
Step 3   Stale cleanup       Remove containers from previous runs
Step 3b  CDC connector       Resolve latest version from Maven Central
                             Version gate: banner if < 2.0.x
                             Download fat JAR to ./volumes/connectors/
Step 4   ScyllaDB cluster    Node 1 (seed) → wait UN → Node 2 → +30s → Node 3
Step 5   Kafka stack         Broker (KRaft) → Schema Registry → Connect
         (if -k)             → REST Proxy → ksqlDB → UI
                             Create pageviews + users topics
                             Register datagen-pageviews + datagen-users connectors
Step 6   Network validation  Assert all containers are on scylla-cdc-bridge
Step 7   /etc/hosts          Write tagged block with IP → hostname for all containers
Step 8   Cluster status      nodetool status from all 3 ScyllaDB nodes
Step 9   Kafka summary       Active connectors, topic list, endpoint table
```

---

## Configuration Variables

All tunables are at the top of the script. Edit before running.

| Variable | Default | Description |
|---|---|---|
| `NETWORK` | `scylla-cdc-bridge` | Docker bridge network name |
| `SCYLLA_IMAGE` | `scylladb/scylla` | ScyllaDB Docker image |
| `SCYLLA_NODES` | 3 names | Container names for all 3 nodes |
| `BOOT_WAIT` | `30` | Seconds to wait between starting nodes 2 and 3 |
| `READY_TIMEOUT` | `180` | Seconds to wait for each node to reach UN state |
| `CONFLUENT_VERSION` | `7.6.1` | Confluent Platform image tag |
| `MAVEN_ARTIFACT` | `scylla-cdc-source-connector` | Maven artifact ID |
| `MIN_RECOMMENDED_MAJOR` | `2` | Minimum major version before warning banner |
| `CONNECTORS_DIR` | `./volumes/connectors` | Local path for connector JAR |

---

## Container Inventory

### ScyllaDB (always started)

| Container | Hostname | Role |
|---|---|---|
| `scylla-cdc-quickstart` | `scylla-cdc-quickstart` | Seed node |
| `scylla-cdc-quickstart-2` | `scylla-cdc-quickstart-2` | Replica |
| `scylla-cdc-quickstart-3` | `scylla-cdc-quickstart-3` | Replica |

### Kafka Community (`-k community`)

| Container | Hostname | Image |
|---|---|---|
| `scylla-cdc-broker` | same | `confluentinc/cp-kafka:7.6.1` |
| `scylla-cdc-schema-registry` | same | `confluentinc/cp-schema-registry:7.6.1` |
| `scylla-cdc-connect` | same | `confluentinc/cp-kafka-connect:7.6.1` |
| `scylla-cdc-rest-proxy` | same | `confluentinc/cp-kafka-rest:7.6.1` |
| `scylla-cdc-ksqldb` | same | `confluentinc/cp-ksqldb-server:7.6.1` |
| `scylla-cdc-kafdrop` | same | `obsidiandynamics/kafdrop:latest` |

### Kafka Enterprise (`-k enterprise`)

| Container | Hostname | Image |
|---|---|---|
| `scylla-cdc-broker` | same | `confluentinc/cp-server:7.6.1` |
| `scylla-cdc-schema-registry` | same | `confluentinc/cp-schema-registry:7.6.1` |
| `scylla-cdc-connect` | same | `cnfldemos/cp-server-connect-datagen:0.6.4-7.6.1` |
| `scylla-cdc-rest-proxy` | same | `confluentinc/cp-kafka-rest:7.6.1` |
| `scylla-cdc-ksqldb` | same | `confluentinc/cp-ksqldb-server:7.6.1` |
| `scylla-cdc-control-center` | same | `confluentinc/cp-enterprise-control-center:7.6.1` |

---

## Port Map

All `-p` bindings use the `1xxxx` prefix to avoid conflicts with
services already running on standard ports (Docker proxy, other Kafka
instances, Schema Registry, etc.).

| Service | Container port | Host port | Protocol |
|---|---|---|---|
| ScyllaDB CQL | 9042 | — (container network only) | TCP |
| Kafka Broker internal | 9092 | — (container network only) | TCP |
| Kafka Broker KRaft controller | 29093 | — (internal only) | TCP |
| Kafka Broker host access | 9093 | **19093** | TCP |
| Schema Registry | 8081 | **18081** | HTTP |
| Kafka Connect REST | 8083 | **18083** | HTTP |
| Kafka REST Proxy | 8082 | **18082** | HTTP |
| ksqlDB | 8088 | **18088** | HTTP |
| Kafdrop UI (community) | 9000 | **19000** | HTTP |
| Control Center (enterprise) | 9021 | **19021** | HTTP |

---

## Kernel Tuning

Applied at every run. Persisted to `/etc/sysctl.d/99-scylla-cdc.conf`.

| Parameter | Value | Reason |
|---|---|---|
| `fs.aio-max-nr` | `2147483647` (INT_MAX) | Seastar requires ~1M AIO slots per node × 3 nodes |
| `net.core.somaxconn` | `4096` | Accept queue depth for CQL + Kafka connections |
| `vm.swappiness` | `1` | Prevent Seastar memory from being swapped |
| `net.ipv4.tcp_rmem` | `4096 87380 16777216` | 16 MB TCP receive buffer ceiling |
| `net.ipv4.tcp_wmem` | `4096 65536 16777216` | 16 MB TCP send buffer ceiling |

> `fs.aio-max-nr` is a **system-wide** limit shared across all containers.
> With 3 ScyllaDB nodes each consuming ~700K slots, the default of 65536
> causes immediate Seastar startup failure.

---

## ScyllaDB CDC Connector

The script resolves the connector version dynamically from Maven Central
rather than using a hardcoded version:

```
Metadata endpoint:
  https://repo1.maven.org/maven2/com/scylladb/
    scylla-cdc-source-connector/maven-metadata.xml

Download URL pattern:
  https://repo1.maven.org/maven2/com/scylladb/
    scylla-cdc-source-connector/{VERSION}/
    scylla-cdc-source-connector-{VERSION}-jar-with-dependencies.jar
```

### Version Gate

| Resolved version | Behaviour |
|---|---|
| `>= 2.0.x` | Green OK banner, proceed |
| `< 2.0.x` | Red WARNING banner + 10-second countdown (Ctrl-C to abort) |

Versions below 2.0.0 do not support the **tablets architecture** introduced
in ScyllaDB 2025.x. Running an older connector against a tablets-enabled
cluster causes silent data loss and connector crashes on tablet rebalance.

### Bind Mount

The JAR is downloaded to `./volumes/connectors/` on the host and
bind-mounted into the Connect container at `/connectors`. The directory is
appended to `CONNECT_PLUGIN_PATH`:

```
CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components,/connectors
```

Connect discovers and loads the ScyllaDB connector automatically at startup.

---

## Edition Comparison

| Feature | Community | Enterprise |
|---|---|---|
| Broker image | `cp-kafka` | `cp-server` |
| Connect image | `cp-kafka-connect` | `cp-server-connect-datagen` |
| Datagen connector | Installed at boot via `confluent-hub` | Pre-bundled |
| Web UI | Kafdrop (OSS) — port 19000 | Control Center — port 19021 |
| License | Free, Apache 2.0 | 30-day trial, then license required |
| Schema Registry | Same | Same |
| REST Proxy | Same | Same |
| ksqlDB | Same | Same |
| ScyllaDB CDC connector | Same (mounted at /connectors) | Same |

---

## Networking Model

All containers join `scylla-cdc-bridge` — a user-defined Docker bridge.
Docker's embedded DNS resolves container hostnames within the network,
so every service connects to others by name, not IP.

```
scylla-cdc-connect  →  scylla-cdc-broker:9092          (Kafka)
scylla-cdc-connect  →  scylla-cdc-schema-registry:8081  (Schema Registry)
scylla-cdc-connect  →  scylla-cdc-quickstart:9042       (ScyllaDB seed)
```

The host gets `/etc/hosts` entries for all containers (managed block, cleaned
by `-r`), so you can `cqlsh scylla-cdc-quickstart` or
`curl http://scylla-cdc-schema-registry:8081` directly from your shell.

---

## Teardown

```bash
./scylla-cdc-cluster.sh -r
```

Removes:
- All 3 ScyllaDB containers
- All Kafka containers (both editions — safe to run without knowing which was started)
- `scylla-cdc-bridge` Docker network
- `/etc/hosts` managed block

Does **not** remove:
- `./volumes/connectors/` JAR (to avoid re-downloading)
- `/etc/sysctl.d/99-scylla-cdc.conf` (kernel tuning is safe to leave)

---

## Troubleshooting

### ScyllaDB node fails to start — `aio-max-nr` error

```
Could not initialize seastar: std::runtime_error (Your system does not
satisfy minimum AIO requirements...)
```

**Fix:** Re-run the script — Step 1 sets `fs.aio-max-nr` to `2147483647`.
If already running: `sudo sysctl -w fs.aio-max-nr=2147483647`

### Port already allocated

```
Bind for :::8081 failed: port is already allocated
```

All host ports use the `1xxxx` prefix (`18081`, `18083`, etc.) to avoid
standard port conflicts. If you still hit this, check what is running:

```bash
sudo ss -tlnp | grep -E "18081|18082|18083|18088|19000|19021|19093"
```

### Nodes 2 and 3 don't join cluster

Seed node wasn't fully up before replicas started. The script waits for `UN`
state before each subsequent node, but if you ran nodes manually:

```bash
docker restart scylla-cdc-quickstart-2
docker restart scylla-cdc-quickstart-3
watch -n 3 'docker exec scylla-cdc-quickstart nodetool status'
```

### Kafka Connect fails to load ScyllaDB CDC connector

```bash
# Verify the JAR is visible inside the container
docker exec scylla-cdc-connect ls -lh /connectors/

# Check Connect logs for plugin discovery
docker logs scylla-cdc-connect 2>&1 | grep -i "scylla\|plugin\|error"

# List loaded connector plugins
curl -s http://localhost:18083/connector-plugins | python3 -m json.tool \
  | grep -i scylla
```

### CDC connector version warning banner

If the script prints the red `WARNING` banner, the latest version on Maven
Central is below 2.0.0. Options:

1. Check [Maven Central](https://repo1.maven.org/maven2/com/scylladb/scylla-cdc-source-connector/)
   for a newer release
2. Wait — releases take time to propagate
3. Manually set a known-good version by editing `MAVEN_METADATA_URL` to
   point directly to a specific version's POM

