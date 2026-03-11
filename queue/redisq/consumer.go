package redisq

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"dummy-tracers/clickhouse"
	"dummy-tracers/queue"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

const consumerWorkers = 4

type Consumer struct {
	client   *redis.Client
	chClient *clickhouse.Client
	log      zerolog.Logger
	batch    int
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewConsumer(ctx context.Context, url string, chClient *clickhouse.Client, batchSize int, log zerolog.Logger) (*Consumer, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opt.PoolSize = consumerWorkers * 3
	opt.MinIdleConns = consumerWorkers

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	// Create consumer groups (ignore BUSYGROUP errors — group already exists)
	for _, stream := range []string{queue.QueueTraces, queue.QueueMetrics, queue.QueueLogs} {
		client.XGroupCreateMkStream(ctx, stream, "otel-sink", "0")
	}

	cctx, cancel := context.WithCancel(ctx)
	c := &Consumer{
		client:   client,
		chClient: chClient,
		log:      log,
		batch:    batchSize,
		cancel:   cancel,
	}

	// Start consumer workers per stream
	for _, stream := range []string{queue.QueueTraces, queue.QueueMetrics, queue.QueueLogs} {
		for i := range consumerWorkers {
			c.wg.Add(1)
			go c.consumeLoop(cctx, stream, i)
		}
	}

	log.Info().Int("batch_size", batchSize).Int("workers_per_stream", consumerWorkers).Msg("redis consumer started")
	return c, nil
}

func (c *Consumer) consumeLoop(ctx context.Context, stream string, id int) {
	defer c.wg.Done()
	consumer := stream + "-w" + string(rune('0'+id))

	for {
		if ctx.Err() != nil {
			return
		}

		results, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "otel-sink",
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    int64(c.batch),
			Block:    time.Second,
		}).Result()

		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			// auto-create consumer group if missing (stream was recreated/flushed)
			if strings.HasPrefix(err.Error(), "NOGROUP") {
				c.client.XGroupCreateMkStream(ctx, stream, "otel-sink", "0")
				continue
			}
			c.log.Error().Err(err).Str("stream", stream).Msg("xreadgroup failed")
			time.Sleep(time.Second)
			continue
		}

		for _, s := range results {
			if len(s.Messages) == 0 {
				continue
			}

			c.processMessages(stream, s.Messages)

			// ACK
			ids := make([]string, len(s.Messages))
			for i, m := range s.Messages {
				ids[i] = m.ID
			}
			c.client.XAck(ctx, stream, "otel-sink", ids...)
		}
	}
}

func (c *Consumer) processMessages(stream string, msgs []redis.XMessage) {
	switch stream {
	case queue.QueueTraces:
		var allRows []clickhouse.TraceRow
		for _, msg := range msgs {
			raw, ok := msg.Values["d"].(string)
			if !ok {
				continue
			}
			var req coltracepb.ExportTraceServiceRequest
			if err := proto.Unmarshal([]byte(raw), &req); err != nil {
				c.log.Error().Err(err).Msg("unmarshal trace failed")
				continue
			}
			allRows = append(allRows, clickhouse.TracesFromProto(&req)...)
		}
		if len(allRows) > 0 {
			c.chClient.AddTraces(allRows)
		}

	case queue.QueueMetrics:
		var allRows []clickhouse.MetricRow
		for _, msg := range msgs {
			raw, ok := msg.Values["d"].(string)
			if !ok {
				continue
			}
			var req colmetricspb.ExportMetricsServiceRequest
			if err := proto.Unmarshal([]byte(raw), &req); err != nil {
				c.log.Error().Err(err).Msg("unmarshal metric failed")
				continue
			}
			allRows = append(allRows, clickhouse.MetricsFromProto(&req)...)
		}
		if len(allRows) > 0 {
			c.chClient.AddMetrics(allRows)
		}

	case queue.QueueLogs:
		var allRows []clickhouse.LogRow
		for _, msg := range msgs {
			raw, ok := msg.Values["d"].(string)
			if !ok {
				continue
			}
			var req collogspb.ExportLogsServiceRequest
			if err := proto.Unmarshal([]byte(raw), &req); err != nil {
				c.log.Error().Err(err).Msg("unmarshal log failed")
				continue
			}
			allRows = append(allRows, clickhouse.LogsFromProto(&req)...)
		}
		if len(allRows) > 0 {
			c.chClient.AddLogs(allRows)
		}
	}
}

func (c *Consumer) Close() {
	c.cancel()
	c.wg.Wait()
	c.client.Close()
	c.log.Info().Msg("redis consumer closed")
}
