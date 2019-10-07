package api

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "goethe"
const subsystem = "api"

// Metrics provides metrics over the API
type Metrics interface {
	EventProduced(topic string)
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

	prometheus.MustRegister(consumerLag)
	prometheus.MustRegister(totalEvents)

	return &apiMetrics{
		consumerLag: consumerLag,
		totalEvents: totalEvents,
	}
}

func (it *apiMetrics) EventProduced(topic string) {
	it.totalEvents.WithLabelValues(topic).Inc()
}

type apiMetrics struct {
	consumerLag *prometheus.GaugeVec
	totalEvents *prometheus.CounterVec
}
