# JCG-Kafka-Load-Tester

A self-contained Kafka load-testing dashboard. A single Go binary serves an
embedded web UI that drives a configurable producer / consumer against a
Kafka cluster and streams real-time health and performance metrics over
WebSocket.

> **AI-generated experimentation project.** This codebase was produced as
> part of experimenting with AI-assisted development (Claude Code). It is a
> personal sandbox for exploring Kafka tuning and failover behaviour rather
> than a production-supported tool — review the code before relying on it
> for anything important.

## What it does

- **Producer load test** — worker pool sending at a configurable rate,
  message size, and key strategy (random / sequential / fixed). Tracks
  msg/sec, MB/sec, error rate, and p50/p95/p99 latency over a 5 s rolling
  window.
- **Consumer load test** — consumer-group reader on the same topic that
  reports throughput, partition-summed lag, and end-to-end latency
  (producer embeds a send timestamp in the first 8 bytes of each message).
- **Live Kafka health** — every cluster node sourced via `DescribeCluster`
  (not just bootstrap brokers), with controller / leader badge, per-node
  connection state, and topic offset table.
- **Multi-instance switcher** — switch between configured Kafka clusters
  or connect to an ad-hoc broker list at runtime. Failover-aware: forces
  a metadata refresh before each test so a fresh run targets the current
  leader after a node loss.
- **Run history** — every completed producer / consumer run is appended
  to a JSONL file under `/data` and rendered in the dashboard.
- **Hardened defaults** — strict CSP, bearer-token auth (optional), CSRF
  guard on state-changing endpoints, SSRF guard on ad-hoc broker
  connections, redacted secrets in API responses and access logs.

## Architecture

```
cmd/main.go                     entrypoint — wires components, starts server
internal/config/                YAML + env-var config loader, redaction helpers
internal/kafka/                 Sarama wrappers: admin, producer, consumer,
                                metrics collector, SCRAM client
internal/metrics/               rolling p50/p95/p99 aggregator + JSON snapshot
internal/storage/               append-only JSONL writer with size rotation
internal/dashboard/             chi HTTP server, WebSocket hub, security
                                middlewares (auth / CORS / CSRF / CSP)
web/static/                     single-page dashboard (vanilla JS + Chart.js)
config/config.yaml              default configuration baked into the image
docker-compose.yml              three-node KRaft Kafka stack + agent
k8s/                            Kubernetes manifests (Deployment, Service,
                                PVC, ConfigMap)
```

## Run with Docker Compose

```bash
docker compose up --build
```

Then open <http://localhost:8093>.

### What runs in the compose stack

| Container     | Image                 | Host port | Purpose |
|---------------|-----------------------|-----------|---------|
| `kafka`       | `apache/kafka:3.8.0`  | `9094`    | KRaft node 1 (broker + controller) |
| `kafka2`      | `apache/kafka:3.8.0`  | `9095`    | KRaft node 2 (broker + controller) |
| `kafka3`      | `apache/kafka:3.8.0`  | `9096`    | KRaft node 3 (broker + controller) |
| `kafka-agent` | built from `./`       | `8093` → `8080` | Go binary serving the dashboard |

All three Kafka nodes share one cluster ID and list each other in
`KAFKA_CONTROLLER_QUORUM_VOTERS`. Each is configured with
`replication_factor=3` and `min.insync.replicas=2`, so the cluster
survives one node failure without losing the write quorum. `restart:
unless-stopped` makes killed nodes respawn — useful for failover tests
(`docker stop kafka` and watch the leader re-elect on the dashboard).

The agent container persists run history to a named volume mounted at
`/data` and waits for all three Kafka nodes to be healthy before
starting.

## Run locally (no containers)

```bash
# Requires Go 1.26+ and a Kafka broker reachable on localhost:9092
go build ./cmd
go run ./cmd --config config/config.yaml
```

## Configuration

Loaded from YAML, overridden by environment variables. Common knobs:

| Env var                          | YAML field                        | Default          |
|----------------------------------|-----------------------------------|------------------|
| `KAFKA_BROKERS`                  | `kafka.brokers`                   | `localhost:9092` |
| `KAFKA_SASL_ENABLED`             | `kafka.sasl.enabled`              | `false`          |
| `KAFKA_SASL_USERNAME`            | `kafka.sasl.username`             | —                |
| `KAFKA_SASL_PASSWORD`            | `kafka.sasl.password`             | —                |
| `KAFKA_TLS_ENABLED`              | `kafka.tls.enabled`               | `false`          |
| `LOAD_TOPIC`                     | `load_test.topic`                 | `load-test`      |
| `LOAD_WORKERS`                   | `load_test.workers`               | `10`             |
| `LOAD_MSG_PER_SEC`               | `load_test.target_msg_per_sec`    | `1000`           |
| `LOAD_MSG_SIZE`                  | `load_test.message_size_bytes`    | `1024`           |
| `DASHBOARD_PORT`                 | `dashboard.port`                  | `8080`           |
| `DATA_DIR`                       | `storage.data_dir`                | `/data`          |
| `DASHBOARD_AUTH_TOKEN`           | `security.auth_token`             | — (auth off)     |
| `DASHBOARD_ALLOWED_ORIGINS`      | `security.allowed_origins`        | — (same-origin)  |
| `DASHBOARD_ALLOWED_BROKER_HOSTS` | `security.allowed_broker_hosts`   | — (YAML hosts)   |
| `DASHBOARD_ALLOW_PRIVATE_BROKERS`| `security.allow_private_brokers`  | `false`          |

When `DASHBOARD_AUTH_TOKEN` is set the SPA prompts once for the token,
caches it in `localStorage`, and attaches it to every request.

## REST API (selected)

| Method | Path                          | Description |
|--------|-------------------------------|-------------|
| GET    | `/health`, `/ready`           | Liveness / readiness probes |
| GET    | `/auth-config`                | `{ "auth_required": bool }` for the SPA |
| GET    | `/api/config`                 | Running config (secrets redacted) |
| PUT    | `/api/config`                 | Update load-test section (idle only) |
| GET    | `/api/kafka/{brokers,topics,metrics}` | Cluster metadata snapshots |
| POST   | `/api/kafka/connect`          | Switch to a broker list (SSRF-guarded) |
| POST   | `/api/load/{start,stop}`      | Producer lifecycle |
| POST   | `/api/load/consumer/{start,stop}` | Consumer lifecycle |
| GET    | `/api/metrics/{current,history,consumer/history}` | Aggregator + run records |
| WS     | `/ws/metrics`                 | 1 s real-time push of metrics + log events |

## Kubernetes

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

The deployment uses the same image, mounts a ConfigMap at
`/etc/kafka-agent/config.yaml`, and a PVC at `/data` for run history.
Service type defaults to `LoadBalancer` — change to `ClusterIP` for
internal-only.
