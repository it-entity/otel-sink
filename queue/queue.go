package queue

const (
	QueueTraces  = "otel.traces"
	QueueMetrics = "otel.metrics"
	QueueLogs    = "otel.logs"
)

type Publisher interface {
	Publish(queue string, body []byte)
	Close()
}

type Consumer interface {
	Close()
}
