package queue

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	QueueTraces  = "otel.traces"
	QueueMetrics = "otel.metrics"
	QueueLogs    = "otel.logs"

	publisherPoolSize = 4
	batchFlushSize    = 64
	batchFlushDelay   = 25 * time.Millisecond
)

type publishMsg struct {
	queue string
	body  []byte
}

type Publisher struct {
	channels [publisherPoolSize]*amqp.Channel
	conns    [publisherPoolSize]*amqp.Connection
	log      zerolog.Logger
	msgCh    chan publishMsg
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

func NewPublisher(ctx context.Context, url string, log zerolog.Logger) (*Publisher, error) {
	p := &Publisher{
		log:   log,
		msgCh: make(chan publishMsg, 2000), // bounded async buffer
	}

	// open pool of connections+channels
	for i := range publisherPoolSize {
		conn, err := dialWithRetry(url, log)
		if err != nil {
			p.closePartial(i)
			return nil, err
		}
		p.conns[i] = conn

		ch, err := conn.Channel()
		if err != nil {
			p.closePartial(i)
			return nil, err
		}
		p.channels[i] = ch

		// declare queues on first channel only
		if i == 0 {
			for _, name := range []string{QueueTraces, QueueMetrics, QueueLogs} {
				if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
					p.closePartial(i + 1)
					return nil, err
				}
			}
		}
	}

	pctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	// start batch publisher workers — one per channel
	for i := range publisherPoolSize {
		p.wg.Add(1)
		go p.batchWorker(pctx, i)
	}

	log.Info().Int("pool_size", publisherPoolSize).Int("buffer", cap(p.msgCh)).Msg("rabbitmq publisher pool started")
	return p, nil
}

// Publish is non-blocking — drops into the async channel.
// Returns nil immediately. If buffer is full, drops the message.
func (p *Publisher) Publish(_ context.Context, queue string, body []byte) error {
	// copy body since fiber reuses the buffer
	copied := make([]byte, len(body))
	copy(copied, body)

	select {
	case p.msgCh <- publishMsg{queue: queue, body: copied}:
		return nil
	default:
		// buffer full — drop message (backpressure)
		p.log.Warn().Str("queue", queue).Msg("publish buffer full, dropping message")
		return nil
	}
}

func (p *Publisher) batchWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	ch := p.channels[id]

	batch := make([]publishMsg, 0, batchFlushSize)
	timer := time.NewTimer(batchFlushDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			p.flushBatch(ch, batch)
			return

		case msg, ok := <-p.msgCh:
			if !ok {
				p.flushBatch(ch, batch)
				return
			}
			batch = append(batch, msg)
			if len(batch) >= batchFlushSize {
				p.flushBatch(ch, batch)
				batch = batch[:0]
				timer.Reset(batchFlushDelay)
			}

		case <-timer.C:
			if len(batch) > 0 {
				p.flushBatch(ch, batch)
				batch = batch[:0]
			}
			timer.Reset(batchFlushDelay)
		}
	}
}

func (p *Publisher) flushBatch(ch *amqp.Channel, msgs []publishMsg) {
	for _, msg := range msgs {
		err := ch.PublishWithContext(context.Background(), "", msg.queue, false, false, amqp.Publishing{
			ContentType:  "application/protobuf",
			DeliveryMode: amqp.Transient, // faster than Persistent for testing
			Body:         msg.body,
		})
		if err != nil {
			p.log.Error().Err(err).Str("queue", msg.queue).Msg("batch publish failed")
		}
	}
}

func (p *Publisher) closePartial(n int) {
	for i := range n {
		if p.channels[i] != nil {
			p.channels[i].Close()
		}
		if p.conns[i] != nil {
			p.conns[i].Close()
		}
	}
}

func (p *Publisher) Close() {
	p.cancel()
	p.wg.Wait()
	close(p.msgCh)
	for i := range publisherPoolSize {
		if p.channels[i] != nil {
			p.channels[i].Close()
		}
		if p.conns[i] != nil {
			p.conns[i].Close()
		}
	}
	p.log.Info().Msg("rabbitmq publisher pool closed")
}
