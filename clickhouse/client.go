package clickhouse

import (
	"context"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"
)

const (
	flushInterval = 5 * time.Second
	flushSize     = 1000
)

type TraceRow struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	ServiceName  string
	SpanName     string
	SpanKind     string
	StartTime    time.Time
	EndTime      time.Time
	DurationNs   int64
	StatusCode   string
	Attributes   string
}

type MetricRow struct {
	MetricName  string
	MetricType  string
	ServiceName string
	Timestamp   time.Time
	Value       float64
	Attributes  string
}

type LogRow struct {
	Timestamp    time.Time
	Severity     uint8
	SeverityText string
	ServiceName  string
	Body         string
	Attributes   string
	TraceID      string
	SpanID       string
}

type Client struct {
	conn driver.Conn
	log  zerolog.Logger

	tracesMu sync.Mutex
	traces   []TraceRow

	metricsMu sync.Mutex
	metrics   []MetricRow

	logsMu sync.Mutex
	logs   []LogRow

	cancel context.CancelFunc
	done   chan struct{}
}

func New(ctx context.Context, dsn string, log zerolog.Logger) (*Client, error) {
	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}

	// retry ping up to 5 times
	for i := 0; i < 5; i++ {
		if err = conn.Ping(ctx); err == nil {
			break
		}
		log.Warn().Err(err).Int("attempt", i+1).Msg("clickhouse ping failed, retrying")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, err
	}

	log.Info().Msg("connected to clickhouse")

	fctx, cancel := context.WithCancel(ctx)
	c := &Client{
		conn:   conn,
		log:    log,
		traces: make([]TraceRow, 0, flushSize),
		metrics: make([]MetricRow, 0, flushSize),
		logs:   make([]LogRow, 0, flushSize),
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go c.flusher(fctx)
	return c, nil
}

func (c *Client) AddTraces(rows []TraceRow) {
	c.tracesMu.Lock()
	c.traces = append(c.traces, rows...)
	n := len(c.traces)
	c.tracesMu.Unlock()
	if n >= flushSize {
		go c.flushTraces(context.Background())
	}
}

func (c *Client) AddMetrics(rows []MetricRow) {
	c.metricsMu.Lock()
	c.metrics = append(c.metrics, rows...)
	n := len(c.metrics)
	c.metricsMu.Unlock()
	if n >= flushSize {
		go c.flushMetrics(context.Background())
	}
}

func (c *Client) AddLogs(rows []LogRow) {
	c.logsMu.Lock()
	c.logs = append(c.logs, rows...)
	n := len(c.logs)
	c.logsMu.Unlock()
	if n >= flushSize {
		go c.flushLogs(context.Background())
	}
}

func (c *Client) flusher(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flushTraces(ctx)
			c.flushMetrics(ctx)
			c.flushLogs(ctx)
		}
	}
}

func (c *Client) flushTraces(ctx context.Context) {
	c.tracesMu.Lock()
	if len(c.traces) == 0 {
		c.tracesMu.Unlock()
		return
	}
	rows := c.traces
	c.traces = make([]TraceRow, 0, flushSize)
	c.tracesMu.Unlock()

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO otel.otel_traces (trace_id, span_id, parent_span_id, service_name, span_name, span_kind, start_time, end_time, duration_ns, status_code, attributes)")
	if err != nil {
		c.log.Error().Err(err).Msg("failed to prepare traces batch")
		return
	}
	for _, r := range rows {
		if err := batch.Append(r.TraceID, r.SpanID, r.ParentSpanID, r.ServiceName, r.SpanName, r.SpanKind, r.StartTime, r.EndTime, r.DurationNs, r.StatusCode, r.Attributes); err != nil {
			c.log.Error().Err(err).Msg("failed to append trace row")
		}
	}
	if err := batch.Send(); err != nil {
		c.log.Error().Err(err).Msg("failed to send traces batch")
		return
	}
	c.log.Debug().Int("count", len(rows)).Msg("flushed traces")
}

func (c *Client) flushMetrics(ctx context.Context) {
	c.metricsMu.Lock()
	if len(c.metrics) == 0 {
		c.metricsMu.Unlock()
		return
	}
	rows := c.metrics
	c.metrics = make([]MetricRow, 0, flushSize)
	c.metricsMu.Unlock()

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO otel.otel_metrics (metric_name, metric_type, service_name, timestamp, value, attributes)")
	if err != nil {
		c.log.Error().Err(err).Msg("failed to prepare metrics batch")
		return
	}
	for _, r := range rows {
		if err := batch.Append(r.MetricName, r.MetricType, r.ServiceName, r.Timestamp, r.Value, r.Attributes); err != nil {
			c.log.Error().Err(err).Msg("failed to append metric row")
		}
	}
	if err := batch.Send(); err != nil {
		c.log.Error().Err(err).Msg("failed to send metrics batch")
		return
	}
	c.log.Debug().Int("count", len(rows)).Msg("flushed metrics")
}

func (c *Client) flushLogs(ctx context.Context) {
	c.logsMu.Lock()
	if len(c.logs) == 0 {
		c.logsMu.Unlock()
		return
	}
	rows := c.logs
	c.logs = make([]LogRow, 0, flushSize)
	c.logsMu.Unlock()

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO otel.otel_logs (timestamp, severity, severity_text, service_name, body, attributes, trace_id, span_id)")
	if err != nil {
		c.log.Error().Err(err).Msg("failed to prepare logs batch")
		return
	}
	for _, r := range rows {
		if err := batch.Append(r.Timestamp, r.Severity, r.SeverityText, r.ServiceName, r.Body, r.Attributes, r.TraceID, r.SpanID); err != nil {
			c.log.Error().Err(err).Msg("failed to append log row")
		}
	}
	if err := batch.Send(); err != nil {
		c.log.Error().Err(err).Msg("failed to send logs batch")
		return
	}
	c.log.Debug().Int("count", len(rows)).Msg("flushed logs")
}

func (c *Client) Close() {
	c.cancel()
	<-c.done

	// final flush
	ctx := context.Background()
	c.flushTraces(ctx)
	c.flushMetrics(ctx)
	c.flushLogs(ctx)

	c.conn.Close()
	c.log.Info().Msg("clickhouse connection closed")
}
