package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"

	"dummy-tracers/queue"
)

const (
	publisherPoolSize = 8
	batchFlushSize    = 256
	batchFlushDelay   = 5 * time.Millisecond
)

var bodyPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 16384)
	},
}

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
		msgCh: make(chan publishMsg, 16000),
	}

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

		if i == 0 {
			for _, name := range []string{queue.QueueTraces, queue.QueueMetrics, queue.QueueLogs} {
				if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
					p.closePartial(i + 1)
					return nil, err
				}
			}
		}
	}

	pctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := range publisherPoolSize {
		p.wg.Add(1)
		go p.batchWorker(pctx, i)
	}

	log.Info().Int("pool_size", publisherPoolSize).Int("buffer", cap(p.msgCh)).Msg("rabbitmq publisher pool started")
	return p, nil
}

func (p *Publisher) Publish(q string, body []byte) {
	buf := bodyPool.Get().([]byte)
	if cap(buf) < len(body) {
		buf = make([]byte, len(body))
	} else {
		buf = buf[:len(body)]
	}
	copy(buf, body)

	select {
	case p.msgCh <- publishMsg{queue: q, body: buf}:
	default:
		bodyPool.Put(buf[:0])
		p.log.Warn().Str("queue", q).Msg("publish buffer full, dropping message")
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
			DeliveryMode: amqp.Transient,
			Body:         msg.body,
		})
		bodyPool.Put(msg.body[:0])
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
