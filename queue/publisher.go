package queue

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	QueueTraces  = "otel.traces"
	QueueMetrics = "otel.metrics"
	QueueLogs    = "otel.logs"
)

type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	log  zerolog.Logger
}

func NewPublisher(ctx context.Context, url string, log zerolog.Logger) (*Publisher, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Warn().Err(err).Int("attempt", i+1).Msg("rabbitmq connect failed, retrying")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	// declare queues
	for _, name := range []string{QueueTraces, QueueMetrics, QueueLogs} {
		_, err := ch.QueueDeclare(name, true, false, false, false, nil)
		if err != nil {
			ch.Close()
			conn.Close()
			return nil, err
		}
	}

	log.Info().Msg("rabbitmq publisher connected")
	return &Publisher{conn: conn, ch: ch, log: log}, nil
}

func (p *Publisher) Publish(ctx context.Context, queue string, body []byte) error {
	return p.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/protobuf",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *Publisher) Close() {
	p.ch.Close()
	p.conn.Close()
	p.log.Info().Msg("rabbitmq publisher closed")
}
