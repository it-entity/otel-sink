package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"dummy-tracers/clickhouse"
	"dummy-tracers/queue"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

const (
	minWorkers      = 1
	maxWorkers      = 8
	scaleUpThresh   = 500
	scaleDownThresh = 10
	scaleInterval   = 5 * time.Second
)

type Consumer struct {
	url       string
	chClient  *clickhouse.Client
	log       zerolog.Logger
	batchSize int
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type queueWorkerPool struct {
	mu          sync.Mutex
	workers     int32
	activeCount atomic.Int32
	workerWg    sync.WaitGroup
}

func NewConsumer(ctx context.Context, url string, chClient *clickhouse.Client, batchSize int, log zerolog.Logger) (*Consumer, error) {
	conn, err := dialWithRetry(url, log)
	if err != nil {
		return nil, err
	}
	conn.Close()

	cctx, cancel := context.WithCancel(ctx)
	c := &Consumer{
		url:       url,
		chClient:  chClient,
		log:       log,
		batchSize: batchSize,
		cancel:    cancel,
	}

	c.wg.Add(3)
	go c.managePool(cctx, queue.QueueTraces, c.flushTraceBatch)
	go c.managePool(cctx, queue.QueueMetrics, c.flushMetricBatch)
	go c.managePool(cctx, queue.QueueLogs, c.flushLogBatch)

	log.Info().Int("batch_size", batchSize).Int("min_workers", minWorkers).Int("max_workers", maxWorkers).Msg("rabbitmq consumer started")
	return c, nil
}

func (c *Consumer) managePool(ctx context.Context, queueName string, flush func([]amqp.Delivery)) {
	defer c.wg.Done()

	pool := &queueWorkerPool{}
	poolCtx, poolCancel := context.WithCancel(ctx)
	defer poolCancel()

	for i := 0; i < minWorkers; i++ {
		c.addWorker(poolCtx, pool, queueName, flush)
	}

	ticker := time.NewTicker(scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pool.workerWg.Wait()
			return
		case <-ticker.C:
			depth := c.getQueueDepth(queueName)
			current := pool.activeCount.Load()

			if depth > scaleUpThresh && current < maxWorkers {
				c.addWorker(poolCtx, pool, queueName, flush)
				c.log.Info().Str("queue", queueName).Int32("workers", pool.activeCount.Load()).Int("depth", depth).Msg("scaled up consumer")
			} else if depth < scaleDownThresh && current > minWorkers {
				c.log.Info().Str("queue", queueName).Int32("workers", current).Int("depth", depth).Msg("queue depth low, workers will scale down naturally")
			}
		}
	}
}

func (c *Consumer) addWorker(ctx context.Context, pool *queueWorkerPool, queueName string, flush func([]amqp.Delivery)) {
	pool.mu.Lock()
	id := pool.workers + 1
	pool.workers = id
	pool.mu.Unlock()

	pool.activeCount.Add(1)
	pool.workerWg.Add(1)

	tag := fmt.Sprintf("%s-worker-%d", queueName, id)

	go func() {
		defer pool.workerWg.Done()
		defer pool.activeCount.Add(-1)
		c.workerLoop(ctx, pool, queueName, tag, flush)
	}()
}

func (c *Consumer) workerLoop(ctx context.Context, pool *queueWorkerPool, queueName, tag string, flush func([]amqp.Delivery)) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.consumeOnce(ctx, pool, queueName, tag, flush)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn().Err(err).Str("queue", queueName).Str("worker", tag).Msg("worker disconnected, reconnecting in 3s")
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (c *Consumer) consumeOnce(ctx context.Context, pool *queueWorkerPool, queueName, tag string, flush func([]amqp.Delivery)) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(c.batchSize, 0, false); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return err
	}

	msgs, err := ch.Consume(queueName, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	connErr := conn.NotifyClose(make(chan *amqp.Error, 1))
	c.log.Info().Str("queue", queueName).Str("worker", tag).Msg("consumer connected")

	batch := make([]amqp.Delivery, 0, c.batchSize)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	idleCount := 0

	for {
		select {
		case <-ctx.Done():
			flush(batch)
			return nil
		case amqpErr := <-connErr:
			flush(batch)
			if amqpErr != nil {
				return amqpErr
			}
			return nil
		case msg, ok := <-msgs:
			if !ok {
				flush(batch)
				return nil
			}
			idleCount = 0
			batch = append(batch, msg)
			if len(batch) >= c.batchSize {
				flush(batch)
				batch = make([]amqp.Delivery, 0, c.batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flush(batch)
				batch = make([]amqp.Delivery, 0, c.batchSize)
				idleCount = 0
			} else {
				idleCount++
				if idleCount >= 3 && pool.activeCount.Load() > minWorkers {
					c.log.Info().Str("queue", queueName).Str("worker", tag).Msg("idle worker scaling down")
					return nil
				}
			}
		}
	}
}

func (c *Consumer) getQueueDepth(queueName string) int {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return 0
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return 0
	}
	defer ch.Close()

	q, err := ch.QueueInspect(queueName)
	if err != nil {
		return 0
	}
	return q.Messages
}

func (c *Consumer) flushTraceBatch(msgs []amqp.Delivery) {
	if len(msgs) == 0 {
		return
	}
	var allRows []clickhouse.TraceRow
	for _, msg := range msgs {
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(msg.Body, &req); err != nil {
			c.log.Error().Err(err).Msg("failed to unmarshal trace from queue")
			msg.Nack(false, false)
			continue
		}
		allRows = append(allRows, clickhouse.TracesFromProto(&req)...)
	}
	if len(allRows) > 0 {
		c.chClient.AddTraces(allRows)
	}
	msgs[len(msgs)-1].Ack(true)
}

func (c *Consumer) flushMetricBatch(msgs []amqp.Delivery) {
	if len(msgs) == 0 {
		return
	}
	var allRows []clickhouse.MetricRow
	for _, msg := range msgs {
		var req colmetricspb.ExportMetricsServiceRequest
		if err := proto.Unmarshal(msg.Body, &req); err != nil {
			c.log.Error().Err(err).Msg("failed to unmarshal metric from queue")
			msg.Nack(false, false)
			continue
		}
		allRows = append(allRows, clickhouse.MetricsFromProto(&req)...)
	}
	if len(allRows) > 0 {
		c.chClient.AddMetrics(allRows)
	}
	msgs[len(msgs)-1].Ack(true)
}

func (c *Consumer) flushLogBatch(msgs []amqp.Delivery) {
	if len(msgs) == 0 {
		return
	}
	var allRows []clickhouse.LogRow
	for _, msg := range msgs {
		var req collogspb.ExportLogsServiceRequest
		if err := proto.Unmarshal(msg.Body, &req); err != nil {
			c.log.Error().Err(err).Msg("failed to unmarshal log from queue")
			msg.Nack(false, false)
			continue
		}
		allRows = append(allRows, clickhouse.LogsFromProto(&req)...)
	}
	if len(allRows) > 0 {
		c.chClient.AddLogs(allRows)
	}
	msgs[len(msgs)-1].Ack(true)
}

func (c *Consumer) Close() {
	c.cancel()
	c.wg.Wait()
	c.log.Info().Msg("rabbitmq consumer closed")
}
