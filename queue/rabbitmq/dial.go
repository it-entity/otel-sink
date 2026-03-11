package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

func dialWithRetry(url string, log zerolog.Logger) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		log.Warn().Err(err).Int("attempt", i+1).Msg("rabbitmq connect failed, retrying")
		time.Sleep(2 * time.Second)
	}
	return nil, err
}
