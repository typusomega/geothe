package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "goethe"
const subsystem = "api"

// Metrics provides metrics over the API
type Metrics interface {
	EventProduced(topic string)
	EventProductionFailed(topic string)
}

// NewMetrics ctor.
func NewMetrics() Metrics {
	consumerLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "consumer_lag",
		Help:      "approximates the current lag of a consumer on a topic",
	}, []string{"consumer", "topic"})

	totalEvents := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "events_total",
		Help:      "total number of events",
	}, []string{"topic"})

	totalEventsFailedToProduce := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "events_produce_errors_total",
		Help:      "total number of events failed to produce",
	}, []string{"topic"})

	prometheus.MustRegister(consumerLag)
	prometheus.MustRegister(totalEvents)
	prometheus.MustRegister(totalEventsFailedToProduce)

	return &apiMetrics{
		consumerLag:                consumerLag,
		totalEvents:                totalEvents,
		totalEventsFailedToProduce: totalEventsFailedToProduce,
	}
}

func (it *apiMetrics) EventProduced(topic string) {
	it.totalEvents.WithLabelValues(topic).Inc()
}

func (it *apiMetrics) EventProductionFailed(topic string) {
	it.totalEventsFailedToProduce.WithLabelValues(topic).Inc()
}

type apiMetrics struct {
	consumerLag                *prometheus.GaugeVec
	totalEvents                *prometheus.CounterVec
	totalEventsFailedToProduce *prometheus.CounterVec
}
