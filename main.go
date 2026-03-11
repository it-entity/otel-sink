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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"dummy-tracers/clickhouse"
	"dummy-tracers/queue"
	"dummy-tracers/queue/rabbitmq"
	"dummy-tracers/queue/redisq"
)

// analytics counters — minimal hot path
var (
	totalRequests atomic.Int64
	totalBytes    atomic.Int64
	traceCount    atomic.Int64
	metricCount   atomic.Int64
	logCount      atomic.Int64
	errorCount    atomic.Int64
)

const maxConcurrent = 1024
const maxBodySize = 4 * 1024 * 1024

func main() {
	debug.SetMemoryLimit(350 * 1024 * 1024)
	debug.SetGCPercent(100)

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

	batchSize := envOrDefaultInt("BATCH_SIZE", 5000)
	backend := envOrDefault("QUEUE_BACKEND", "rabbitmq")

	var pub queue.Publisher
	var con queue.Consumer

	switch backend {
	case "redis":
		redisURL := envOrDefault("REDIS_URL", "redis://localhost:6379")
		pub, err = redisq.NewPublisher(ctx, redisURL, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to connect redis publisher")
		}
		con, err = redisq.NewConsumer(ctx, redisURL, chClient, batchSize, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to start redis consumer")
		}
	default: // rabbitmq
		rmqURL := envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
		pub, err = rabbitmq.NewPublisher(ctx, rmqURL, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to connect rabbitmq publisher")
		}
		con, err = rabbitmq.NewConsumer(ctx, rmqURL, chClient, batchSize, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to start rabbitmq consumer")
		}
	}
	defer pub.Close()
	defer con.Close()

	log.Info().Str("backend", backend).Msg("queue backend selected")

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		Concurrency:          maxConcurrent * 8,
		BodyLimit:            maxBodySize,
		ReadBufferSize:       16384,
		WriteBufferSize:      2048,
	})

	app.Post("/v1/traces", makeHandler(queue.QueueTraces, &traceCount, pub))
	app.Post("/v1/metrics", makeHandler(queue.QueueMetrics, &metricCount, pub))
	app.Post("/v1/logs", makeHandler(queue.QueueLogs, &logCount, pub))
	app.Get("/stats", statsHandler())

	go reportStats(log)

	go func() {
		log.Info().Str("backend", backend).Int("batch_size", batchSize).Msg("listening on :4318")
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

func makeHandler(queueName string, counter *atomic.Int64, pub queue.Publisher) fiber.Handler {
	return func(c *fiber.Ctx) error {
		body, err := decompress(c)
		if err != nil {
			errorCount.Add(1)
			return c.SendStatus(fiber.StatusBadRequest)
		}

		pub.Publish(queueName, body)

		totalRequests.Add(1)
		totalBytes.Add(int64(len(body)))
		counter.Add(1)

		return c.SendStatus(fiber.StatusOK)
	}
}

func statsHandler() fiber.Handler {
	// cache mem stats — ReadMemStats causes STW pause, don't call on every request
	var (
		cachedMem   runtime.MemStats
		lastMemRead atomic.Int64
	)

	return func(c *fiber.Ctx) error {
		now := time.Now().Unix()
		if now-lastMemRead.Load() >= 2 {
			runtime.ReadMemStats(&cachedMem)
			lastMemRead.Store(now)
		}

		return c.JSON(fiber.Map{
			"total_requests":    totalRequests.Load(),
			"total_bytes":       totalBytes.Load(),
			"traces":            traceCount.Load(),
			"metrics":           metricCount.Load(),
			"logs":              logCount.Load(),
			"errors":            errorCount.Load(),
			"heap_alloc_mb":     cachedMem.HeapAlloc / 1024 / 1024,
			"rss_mb":            getRSSMB(),
			"goroutines":        runtime.NumGoroutine(),
			"gc_cycles":         cachedMem.NumGC,
			"gc_pause_total_ms": cachedMem.PauseTotalNs / 1_000_000,
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
				Str("heap", formatBytes(float64(mem.HeapAlloc))).
				Str("rss", fmt.Sprintf("%dMB", getRSSMB())).
				Int("goroutines", runtime.NumGoroutine()).
				Uint32("gc", mem.NumGC).
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
