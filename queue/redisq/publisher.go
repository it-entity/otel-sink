package redisq

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const (
	publisherWorkers = 4
	batchFlushSize   = 128
	batchFlushDelay  = 2 * time.Millisecond
)

var bodyPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 16384)
	},
}

type publishMsg struct {
	stream string
	body   []byte
}

type Publisher struct {
	client *redis.Client
	log    zerolog.Logger
	msgCh  chan publishMsg
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewPublisher(ctx context.Context, url string, log zerolog.Logger) (*Publisher, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opt.PoolSize = publisherWorkers * 2
	opt.MinIdleConns = publisherWorkers

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	p := &Publisher{
		client: client,
		log:    log,
		msgCh:  make(chan publishMsg, 16000),
	}

	pctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := range publisherWorkers {
		_ = i
		p.wg.Add(1)
		go p.batchWorker(pctx)
	}

	log.Info().Int("workers", publisherWorkers).Int("buffer", cap(p.msgCh)).Msg("redis publisher started")
	return p, nil
}

func (p *Publisher) Publish(stream string, body []byte) {
	buf := bodyPool.Get().([]byte)
	if cap(buf) < len(body) {
		buf = make([]byte, len(body))
	} else {
		buf = buf[:len(body)]
	}
	copy(buf, body)

	select {
	case p.msgCh <- publishMsg{stream: stream, body: buf}:
	default:
		bodyPool.Put(buf[:0])
		p.log.Warn().Str("stream", stream).Msg("publish buffer full, dropping message")
	}
}

func (p *Publisher) batchWorker(ctx context.Context) {
	defer p.wg.Done()

	batch := make([]publishMsg, 0, batchFlushSize)
	timer := time.NewTimer(batchFlushDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			p.flushBatch(batch)
			return
		case msg, ok := <-p.msgCh:
			if !ok {
				p.flushBatch(batch)
				return
			}
			batch = append(batch, msg)
			if len(batch) >= batchFlushSize {
				p.flushBatch(batch)
				batch = batch[:0]
				timer.Reset(batchFlushDelay)
			}
		case <-timer.C:
			if len(batch) > 0 {
				p.flushBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(batchFlushDelay)
		}
	}
}

func (p *Publisher) flushBatch(msgs []publishMsg) {
	if len(msgs) == 0 {
		return
	}

	pipe := p.client.Pipeline()
	for _, msg := range msgs {
		pipe.XAdd(context.Background(), &redis.XAddArgs{
			Stream: msg.stream,
			Values: map[string]any{"d": msg.body},
			MaxLen: 10000,
			Approx: true,
		})
		bodyPool.Put(msg.body[:0])
	}

	_, err := pipe.Exec(context.Background())
	if err != nil {
		p.log.Error().Err(err).Int("count", len(msgs)).Msg("redis pipeline failed")
	}
}

func (p *Publisher) Close() {
	p.cancel()
	p.wg.Wait()
	close(p.msgCh)
	p.client.Close()
	p.log.Info().Msg("redis publisher closed")
}
