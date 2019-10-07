package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/typusomega/goethe/pkg/spec"
)

const namespace = "goethe"
const subsystem = "storage"

// Metrics provides metrics over the storage performance
type Metrics interface {
	MeasurePersistEvent(call func() (*spec.Event, error)) (*spec.Event, error)
	MeasureGetIterator(call func() (EventsIterator, error)) (EventsIterator, error)
}

func (it *storageMetrics) MeasurePersistEvent(call func() (*spec.Event, error)) (*spec.Event, error) {
	timer := prometheus.NewTimer(it.persistEventDurations)
	it.persistEventRequests.Inc()
	ret, err := call()
	timer.ObserveDuration()
	if err != nil {
		it.persistEventErrors.Inc()
	}
	return ret, err
}
func (it *storageMetrics) MeasureGetIterator(call func() (EventsIterator, error)) (EventsIterator, error) {
	timer := prometheus.NewTimer(it.getIteratorDurations)
	it.getIteratorRequests.Inc()
	ret, err := call()
	timer.ObserveDuration()
	if err != nil {
		it.getIteratorErrors.Inc()
	}
	return ret, err
}

// NewMetrics ctor.
func NewMetrics() Metrics {
	persistEventDurations := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "persist_event_duration_histogram",
		Help:      "indicates how long it takes to persist an event",
		Buckets:   []float64{0.0001, 0.001, 0.002, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	})

	persistEventRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "persist_event_requests_total",
		Help:      "the number of persist event requests",
	})

	persistEventErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "persist_event_errors_total",
		Help:      "the number of failed persist event requests",
	})

	getIteratorDurations := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_iterator_duration_histogram",
		Help:      "indicates how long it takes to get an iterator for a specific event",
		Buckets:   []float64{0.0001, 0.001, 0.002, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	})

	getIteratorRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_iterator_requests_total",
		Help:      "the number of get iterator requests",
	})

	getIteratorErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_iterator_errors_total",
		Help:      "the number of failed get iterator requests",
	})

	prometheus.MustRegister(persistEventDurations)
	prometheus.MustRegister(persistEventRequests)
	prometheus.MustRegister(persistEventErrors)

	prometheus.MustRegister(getIteratorDurations)
	prometheus.MustRegister(getIteratorRequests)
	prometheus.MustRegister(getIteratorErrors)

	return &storageMetrics{
		persistEventDurations: persistEventDurations,
		persistEventRequests:  persistEventRequests,
		persistEventErrors:    persistEventErrors,
		getIteratorDurations:  getIteratorDurations,
		getIteratorRequests:   getIteratorRequests,
		getIteratorErrors:     getIteratorErrors,
	}
}

type storageMetrics struct {
	persistEventDurations prometheus.Histogram
	persistEventRequests  prometheus.Counter
	persistEventErrors    prometheus.Counter
	getIteratorDurations  prometheus.Histogram
	getIteratorRequests   prometheus.Counter
	getIteratorErrors     prometheus.Counter
}
