# otel-sink

A high-performance OpenTelemetry collector sink for testing and development. Receives OTLP traces, metrics, and logs over HTTP, queues them through a 3-node RabbitMQ cluster, and ingests into ClickHouse for storage and querying.

## Architecture

```
OTLP Clients
    │
    │  POST /v1/traces, /v1/metrics, /v1/logs
    │  (protobuf, gzip supported)
    ▼
┌──────────────────────┐
│   Collector          │  :4318 (OTLP HTTP)
│   Fiber HTTP Server  │  :6060 (pprof)
│   200MB mem limit    │
└──────────┬───────────┘
           │  raw protobuf → RabbitMQ
           ▼
┌──────────────────────┐
│   RabbitMQ Cluster   │  3 nodes, HA mirrored queues
│   • otel.traces      │  :5672  (node 1)
│   • otel.metrics     │  :5673  (node 2)
│   • otel.logs        │  :5674  (node 3)
│                      │  :15672 (management UI)
└──────────┬───────────┘
           │  batch consume (configurable batch size)
           │  dynamic consumer scaling (1-8 workers/queue)
           ▼
┌──────────────────────┐
│   ClickHouse         │  :9000 (native)
│   • otel_traces      │  :8123 (HTTP)
│   • otel_metrics     │
│   • otel_logs        │
│   MergeTree, 30d TTL │
└──────────────────────┘
```

## Quick Start

```bash
docker compose up -d
```

This starts all services:
- **Collector** on `:4318`
- **ClickHouse** on `:9000` / `:8123`
- **RabbitMQ** (3 nodes) on `:5672-5674`, management UI on `:15672`

Point any OTLP exporter at `http://localhost:4318`.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `CLICKHOUSE_DSN` | `clickhouse://default:@clickhouse:9000/otel` | ClickHouse connection |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq-1:5672/` | RabbitMQ connection |
| `BATCH_SIZE` | `5000` | Consumer batch size (try 5000 or 10000) |

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /v1/traces` | OTLP trace ingestion |
| `POST /v1/metrics` | OTLP metric ingestion |
| `POST /v1/logs` | OTLP log ingestion |
| `GET /stats` | JSON stats (RPS, latency, memory, goroutines) |
| `:6060/debug/pprof/` | Go pprof profiling |
| `:15672` | RabbitMQ management UI (`guest`/`guest`) |

## Features

- **Protobuf + gzip** decompression on ingest
- **No decode in HTTP hot path** — raw bytes go straight to RabbitMQ
- **3-node RabbitMQ cluster** with HA mirrored queues (survives node failure)
- **Dynamic consumer scaling** — 1 to 8 workers per queue based on queue depth
- **Auto-reconnect** on RabbitMQ disconnect with 3s backoff
- **Batch inserts** to ClickHouse (flush every 5s or 1000 rows)
- **Backpressure** via semaphore (256 max concurrent HTTP handlers)
- **pprof** for CPU/heap profiling
- **`/stats`** endpoint with live analytics

## ClickHouse Schema

All tables use `MergeTree` engine, partitioned by `toYYYYMM(date)`, with 30-day TTL.

```sql
-- Traces: ORDER BY (service_name, span_name, start_time, trace_id)
SELECT service_name, span_name, avg(duration_ns)/1e6 as avg_ms
FROM otel.otel_traces
GROUP BY service_name, span_name
ORDER BY avg_ms DESC LIMIT 10;

-- Logs: ORDER BY (service_name, severity, timestamp, trace_id)
SELECT severity_text, count() FROM otel.otel_logs
GROUP BY severity_text ORDER BY count() DESC;

-- Metrics: ORDER BY (service_name, metric_name, timestamp)
SELECT metric_name, avg(value) FROM otel.otel_metrics
GROUP BY metric_name;
```

## Load Testing

Built-in load test with escalating concurrency across all 3 signal types:

```bash
go build -o loadtest ./cmd/loadtest/
./loadtest
```

Phases: 100 → 200 → 400 → 600 → 800 → 1000 → 1500 → 2000 concurrent connections. Auto-detects OOM kill.

## Stress Test Results

Tested with 200MB container memory limit:

| Phase | Concurrency | Combined RPS | Avg Latency | RSS |
|-------|------------|-------------|-------------|-----|
| warmup | 100 | 2,050 | 31ms | 130MB |
| ramp-up | 200 | 525 | 165ms | 192MB |
| OOM | 200 | — | — | >200MB |

**Cluster failover test**: Killed 1 of 3 RabbitMQ nodes during load.
- Queues remained available (HA mirroring)
- Collector kept processing with 0 errors
- Automatic reconnect to surviving nodes

## Project Structure

```
├── main.go                  # HTTP server, stats, pprof
├── clickhouse/
│   ├── client.go            # ClickHouse batch buffer + flusher
│   ├── convert.go           # OTLP protobuf → row conversion
│   └── init.sql             # DDL for 3 tables
├── queue/
│   ├── publisher.go         # RabbitMQ publisher
│   └── consumer.go          # Dynamic consumer pool with auto-scaling
├── cmd/loadtest/
│   └── main.go              # Multi-signal escalating load test
├── Dockerfile               # Multi-stage Go build
├── docker-compose.yaml      # All services (collector, 3x RabbitMQ, ClickHouse)
└── clickhouse/
    ├── init.sql              # Table DDL
    └── users.xml             # Passwordless auth
```

## License

MIT
