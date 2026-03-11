CREATE DATABASE IF NOT EXISTS otel;

-- TRACES
CREATE TABLE IF NOT EXISTS otel.otel_traces
(
    trace_id       String,
    span_id        String,
    parent_span_id String,
    service_name   LowCardinality(String),
    span_name      LowCardinality(String),
    span_kind      LowCardinality(String),
    start_time     DateTime64(9),
    end_time       DateTime64(9),
    duration_ns    Int64,
    status_code    LowCardinality(String),
    attributes     String,
    date           Date DEFAULT toDate(start_time)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (service_name, span_name, start_time, trace_id)
TTL date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- METRICS
CREATE TABLE IF NOT EXISTS otel.otel_metrics
(
    metric_name    LowCardinality(String),
    metric_type    LowCardinality(String),
    service_name   LowCardinality(String),
    timestamp      DateTime64(9),
    value          Float64,
    attributes     String,
    date           Date DEFAULT toDate(timestamp)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (service_name, metric_name, timestamp)
TTL date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- LOGS
CREATE TABLE IF NOT EXISTS otel.otel_logs
(
    timestamp      DateTime64(9),
    severity       UInt8,
    severity_text  LowCardinality(String),
    service_name   LowCardinality(String),
    body           String,
    attributes     String,
    trace_id       String,
    span_id        String,
    date           Date DEFAULT toDate(timestamp)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (service_name, severity, timestamp, trace_id)
TTL date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
