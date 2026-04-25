# kafka-agent

Kafka load testing dashboard — Go web application that generates configurable load against a Kafka cluster and displays real-time health and performance metrics in a browser dashboard.

## Build & Run

```bash
# Build binary
go build ./cmd

# Run locally (requires Kafka on localhost:9092)
go run ./cmd --config config/config.yaml

# Docker Compose (includes Kafka — access dashboard at http://localhost:8093)
docker compose up --build

# Go checks
go build ./...
go vet ./...
```

## Key Architecture

Single Go binary serving an embedded web UI. No database — all persistence is flat JSONL files.

```
cmd/main.go                     # Entrypoint: wires all components, starts server
internal/config/config.go       # YAML + env var config loader
internal/kafka/
  client.go                     # Sarama config factory (TLS, SASL/PLAIN/SCRAM)
  scram.go                      # SCRAM-SHA-256/512 client implementation
  admin.go                      # DescribeCluster (full node list), topic discovery, topic auto-creation
  producer.go                   # AsyncProducer-based load generator (worker pool)
  metrics_collector.go          # Polls Kafka metadata every 5s
internal/metrics/
  aggregator.go                 # Rolling p50/p95/p99 latency, msg/s, MB/s (5s window)
  snapshot.go                   # Point-in-time metrics struct (JSON serialisable)
internal/storage/
  flatfile.go                   # Append-only JSONL writer with size-based rotation
  history.go                    # Read historical run records from JSONL files
internal/dashboard/
  server.go                     # chi HTTP server, routes, broadcast loop, run saver
  handlers.go                   # REST API handlers + LogSink implementation
  ws_hub.go                     # WebSocket hub — broadcasts JSON to all connected clients
web/
  assets.go                     # go:embed wrapper (serves static/ as fs.FS)
  static/index.html             # Single-page dashboard
  static/app.js                 # WebSocket client, Chart.js charts, load test controls
  static/style.css              # Dark theme styles
config/config.yaml              # Default configuration (copied into container)
k8s/                            # Kubernetes manifests (Deployment, Service, PVC, ConfigMap)
```

## Configuration

Config is loaded from a YAML file and overridden by environment variables. All `config.*` struct fields carry both `yaml:` and `json:` tags.

| Env var | Field | Default |
|---|---|---|
| `KAFKA_BROKERS` | `kafka.brokers` | `localhost:9092` |
| `KAFKA_SASL_ENABLED` | `kafka.sasl.enabled` | `false` |
| `KAFKA_SASL_USERNAME` | `kafka.sasl.username` | `` |
| `KAFKA_SASL_PASSWORD` | `kafka.sasl.password` | `` |
| `KAFKA_TLS_ENABLED` | `kafka.tls.enabled` | `false` |
| `LOAD_TOPIC` | `load_test.topic` | `load-test` |
| `LOAD_WORKERS` | `load_test.workers` | `10` |
| `LOAD_MSG_PER_SEC` | `load_test.target_msg_per_sec` | `1000` |
| `LOAD_MSG_SIZE` | `load_test.message_size_bytes` | `1024` |
| `DASHBOARD_PORT` | `dashboard.port` | `8080` |
| `DATA_DIR` | `storage.data_dir` | `/data` |

## REST API

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness probe (always 200) |
| GET | `/ready` | Readiness probe (200 when Kafka connected) |
| GET | `/api/config` | Full running config |
| PUT | `/api/config` | Update `load_test` section (rejected while running) |
| GET | `/api/kafka/brokers` | Broker list |
| GET | `/api/kafka/topics` | Topic list with partition offsets |
| GET | `/api/kafka/metrics` | Combined Kafka health snapshot |
| POST | `/api/kafka/connect` | Connect to brokers (`{"brokers":"host:port,…"}`); registers as new instance if unknown |
| POST | `/api/kafka/disconnect` | Revert to the instance that was active before the last ad-hoc connect |
| POST | `/api/load/start` | Start load test |
| POST | `/api/load/stop` | Stop load test |
| GET | `/api/load/status` | Current test state + metrics |
| GET | `/api/metrics/current` | Latest aggregator snapshot |
| GET | `/api/metrics/history` | Completed run records from flat files |
| WS | `/ws/metrics` | Real-time stream: `type:update` (1s) and `type:log` events |

## WebSocket Message Types

```json
{ "type": "update", "status": "running|idle", "elapsed": 12.3, "metrics": {...}, "kafka": {...}, "active_instance": "kafka-1" }
{ "type": "log", "level": "info|success|warn|error", "message": "...", "ts": "..." }
```

## Producer Design

Uses a single `sarama.AsyncProducer` shared across all worker goroutines. Workers send to `asyncProd.Input()` channel; two collector goroutines drain `Successes()` and `Errors()` channels and update the aggregator. Every goroutine has `defer recover()` so a panic stops the test cleanly without crashing the server.

- `RequiredAcks = WaitForLocal` — compatible with single-broker dev clusters
- `Compression = None` — avoids codec edge cases in minimal containers
- Rate limiting: per-worker `time.Ticker` with interval = `1s / (targetRate / workers)`
- Producer is built on top of an explicit `sarama.Client`; `RefreshMetadata` is called before each test starts so a fresh run sees the current leader (not stale post-failover metadata)
- Retry window: 90 retries × 1 s = 90 s — wide enough to outlast a worst-case KRaft leader election
- Net `DialTimeout`/`ReadTimeout`/`WriteTimeout` = 10 s so a dead broker cannot wedge a goroutine
- `leader not available` errors are rate-limited to one log line every 5 s and trigger an asynchronous metadata refresh
- `ErrLeaderNotAvailable`, `ErrNotLeaderForPartition`, `ErrBrokerNotAvailable`, `ErrNetworkException` are all treated as the same recoverable category

## Failover Behavior

Topic must be created with `replication_factor ≥ 2` to survive a node loss. With RF=1, each partition lives on a single broker — killing that broker leaves the partition permanently leaderless and writes hang indefinitely. `EnsureTopic` refuses to start if the existing topic has RF<2 and prints a clear error directing the operator to delete the topic (or volume) so it can be recreated.

Default load-test topic configuration: `replication_factor=3`, `min.insync.replicas=2` — tolerates one node failure while maintaining a write quorum. After a `docker stop kafka` (leader broker), surviving brokers elect a new leader from in-sync replicas in seconds and writes resume automatically.

To recreate the topic with new RF settings: `docker compose down -v && docker compose up --build` (the `-v` removes the data volume so the old RF=1 topic is gone).

## Storage

Run records are appended as newline-delimited JSON to `/data/runs.jsonl`. Files rotate when they exceed `storage.rotation_size_mb` (default 50 MB). Mount `/data` as a volume to persist across container restarts.

## Docker

- **Host port**: `8093` → container port `8080`
- **Kafka image**: `apache/kafka:3.8.0` (KRaft mode, no Zookeeper)
- Single three-node KRaft cluster: node-1 (`kafka`, host port `9094`), node-2 (`kafka2`, host port `9095`), node-3 (`kafka3`, host port `9096`) — all share the same `CLUSTER_ID` and list all three in `KAFKA_CONTROLLER_QUORUM_VOTERS`
- All nodes configured with `REPLICATION_FACTOR=3`, `MIN_ISR=2` — tolerates one node failure while maintaining a write quorum
- All Kafka nodes have `restart: unless-stopped` so a killed node respawns automatically (only effective for `docker stop`/`kill`; `docker rm` requires `docker compose up -d <name>` to recreate)
- kafka-agent waits for all three nodes to be healthy before starting; bootstrap lists all three brokers
- Multi-stage build: `golang:1.26-alpine` builder → `distroless/static-debian12` runtime
- Non-root user (`nonroot:nonroot`) in the final image

## Kubernetes

Apply in order:
```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

- Liveness probe: `GET /health`
- Readiness probe: `GET /ready`
- Config via ConfigMap mounted at `/etc/kafka-agent/config.yaml`
- Data volume via PVC mounted at `/data`
- Service type: `LoadBalancer` (change to `ClusterIP` for internal-only)
- Storage class is unset — uses cluster default (works on GKE, EKS, AKS)

## Dashboard Features

- **Kafka Health**: all cluster nodes sourced via `DescribeCluster` (not just bootstrap brokers); each node shown as a chip with its node ID (`node-<id>`), a yellow **leader** badge on the controller node, address, and live connection status (green ✓ / red ✗); topic list with partition counts and total offsets
- **Instance Connector**: free-text broker input (`host:port,…`) with a Connect button; on success shows a green connected chip with the broker address and a disconnect (×) button that reverts to the previously active configured instance
- **Load Test Controls**: topic, workers, msg/sec target, message size, duration, key strategy
- **Event Log**: scrollable CLI-style log with timestamps; shows start/stop banners, broker state changes, errors, and server-side log events streamed via WebSocket
- **Performance Stats**: 8 live stat cards (messages sent, msg/sec, MB/sec, errors, error %, p50/p95/p99 latency)
- **Charts**: Throughput and Latency p99 — data recorded only during an active test run; horizontally scrollable with auto-scroll to live edge; cleared automatically on each new test start
- **Run History**: table of completed runs loaded from flat files
