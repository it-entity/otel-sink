package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"dummy-tracers/clickhouse"
	"dummy-tracers/queue"
)

var tracesPool = sync.Pool{New: func() any { return make([]byte, 0, 16*1024) }}

// analytics counters
var (
	totalRequests  atomic.Int64
	totalBytes     atomic.Int64
	activeRequests atomic.Int64
	peakActive     atomic.Int64
	traceCount     atomic.Int64
	metricCount    atomic.Int64
	logCount       atomic.Int64
	errorCount     atomic.Int64
	latencySum     atomic.Int64
)

const maxConcurrent = 256
const maxBodySize = 4 * 1024 * 1024

func main() {
	debug.SetMemoryLimit(170 * 1024 * 1024) // leave headroom under 200MB container limit

	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}).
		With().Timestamp().Logger()

	// pprof
	go func() {
		log.Info().Msg("pprof on :6060")
		http.ListenAndServe(":6060", nil)
	}()

	ctx := context.Background()

	// clickhouse
	chDSN := envOrDefault("CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000/otel")
	chClient, err := clickhouse.New(ctx, chDSN, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to clickhouse")
	}
	defer chClient.Close()

	// rabbitmq
	rmqURL := envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	batchSize := envOrDefaultInt("BATCH_SIZE", 5000)

	publisher, err := queue.NewPublisher(ctx, rmqURL, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect rabbitmq publisher")
	}
	defer publisher.Close()

	consumer, err := queue.NewConsumer(ctx, rmqURL, chClient, batchSize, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start rabbitmq consumer")
	}
	defer consumer.Close()

	sem := make(chan struct{}, maxConcurrent)

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ReduceMemoryUsage:     true,
		Concurrency:           maxConcurrent * 4,
		BodyLimit:             maxBodySize,
		ReadBufferSize:        8192,
	})

	app.Post("/v1/traces", makeHandler(log, queue.QueueTraces, &traceCount, sem, publisher))
	app.Post("/v1/metrics", makeHandler(log, queue.QueueMetrics, &metricCount, sem, publisher))
	app.Post("/v1/logs", makeHandler(log, queue.QueueLogs, &logCount, sem, publisher))
	app.Get("/stats", statsHandler())

	go reportStats(log)

	go func() {
		log.Info().Int("batch_size", batchSize).Msg("listening on :4318")
		if err := app.Listen(":4318"); err != nil {
			log.Fatal().Err(err).Msg("server failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down")
	app.Shutdown()
}

func makeHandler(log zerolog.Logger, queueName string, counter *atomic.Int64, sem chan struct{}, pub *queue.Publisher) fiber.Handler {
	return func(c *fiber.Ctx) error {
		sem <- struct{}{}
		defer func() { <-sem }()

		start := time.Now()
		active := activeRequests.Add(1)
		defer activeRequests.Add(-1)

		for {
			peak := peakActive.Load()
			if active <= peak || peakActive.CompareAndSwap(peak, active) {
				break
			}
		}

		body, err := decompress(c)
		if err != nil {
			errorCount.Add(1)
			log.Error().Err(err).Str("endpoint", c.Path()).Msg("failed to decompress")
			return c.SendStatus(fiber.StatusBadRequest)
		}

		// publish raw protobuf to rabbitmq — no decode in hot path
		if err := pub.Publish(c.Context(), queueName, body); err != nil {
			errorCount.Add(1)
			log.Error().Err(err).Str("queue", queueName).Msg("failed to publish")
			return c.SendStatus(fiber.StatusInternalServerError)
		}

		elapsed := time.Since(start)
		latencySum.Add(elapsed.Microseconds())
		totalRequests.Add(1)
		totalBytes.Add(int64(len(body)))
		counter.Add(1)

		log.Info().
			Str("endpoint", c.Path()).
			Int("size", len(body)).
			Dur("latency", elapsed).
			Int64("active", active).
			Msg("received")

		return c.SendStatus(fiber.StatusOK)
	}
}

func statsHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		total := totalRequests.Load()
		avgLatency := int64(0)
		if total > 0 {
			avgLatency = latencySum.Load() / total
		}

		return c.JSON(fiber.Map{
			"total_requests":    total,
			"total_bytes":       totalBytes.Load(),
			"active_requests":   activeRequests.Load(),
			"peak_concurrent":   peakActive.Load(),
			"traces":            traceCount.Load(),
			"metrics":           metricCount.Load(),
			"logs":              logCount.Load(),
			"errors":            errorCount.Load(),
			"avg_latency_us":    avgLatency,
			"heap_alloc_mb":     mem.HeapAlloc / 1024 / 1024,
			"rss_mb":            getRSSMB(),
			"goroutines":        runtime.NumGoroutine(),
			"gc_cycles":         mem.NumGC,
			"gc_pause_total_ms": mem.PauseTotalNs / 1_000_000,
		})
	}
}

func reportStats(log zerolog.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastTotal int64
	var lastBytes int64

	for range ticker.C {
		total := totalRequests.Load()
		bytes := totalBytes.Load()

		rps := float64(total-lastTotal) / 10.0
		bps := float64(bytes-lastBytes) / 10.0

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		if total > lastTotal {
			log.Info().
				Str("rps", fmt.Sprintf("%.1f", rps)).
				Str("throughput", formatBytes(bps)+"/s").
				Int64("total", total).
				Int64("errors", errorCount.Load()).
				Int64("peak_concurrent", peakActive.Load()).
				Str("heap", formatBytes(float64(mem.HeapAlloc))).
				Str("rss", fmt.Sprintf("%dMB", getRSSMB())).
				Int("goroutines", runtime.NumGoroutine()).
				Msg("stats")
		}

		lastTotal = total
		lastBytes = bytes
	}
}

func formatBytes(b float64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1fMB", b/1024/1024)
	case b >= 1024:
		return fmt.Sprintf("%.1fKB", b/1024)
	default:
		return fmt.Sprintf("%.0fB", b)
	}
}

func decompress(c *fiber.Ctx) ([]byte, error) {
	encoding := c.Get("Content-Encoding")
	switch encoding {
	case "gzip":
		r, err := gzip.NewReader(c.Request().BodyStream())
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case "", "identity":
		return c.Body(), nil
	default:
		return nil, fiber.NewError(fiber.StatusUnsupportedMediaType, "unsupported encoding: "+encoding)
	}
}

func getRSSMB() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	// statm fields: size resident shared text lib data dt (in pages)
	var size, resident uint64
	fmt.Sscanf(string(data), "%d %d", &size, &resident)
	pageSize := uint64(os.Getpagesize())
	return (resident * pageSize) / 1024 / 1024
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
