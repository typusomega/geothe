package storage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/typusomega/goethe/pkg/spec"
)

const namespace = "goethe"
const subsystem = "storage"

// Metrics provides metrics over the storage performance
type Metrics interface {
	// MeasureAppendEvent measures duration and success rate of appending events
	MeasureAppendEvent(call func() (*spec.Event, error)) (*spec.Event, error)
	// MeasureGetIterator measures duration and success rate of iterator retrieval
	MeasureGetIterator(call func() (EventsIterator, error)) (EventsIterator, error)
}

// MeasureAppendEvent measures duration and success rate of appending events
func (it *storageMetrics) MeasureAppendEvent(call func() (*spec.Event, error)) (*spec.Event, error) {
	timer := prometheus.NewTimer(it.appendEventDurations)
	it.appendEventRequests.Inc()
	ret, err := call()
	timer.ObserveDuration()
	if err != nil {
		it.appendEventErrors.Inc()
	}
	return ret, err
}

// MeasureGetIterator measures duration and success rate of iterator retrieval
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
	appendEventDurations := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "append_event_duration_histogram",
		Help:      "indicates how long it takes to append an event",
		Buckets:   []float64{0.0001, 0.001, 0.002, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	})

	appendEventRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "append_event_requests_total",
		Help:      "the number of append event requests",
	})

	appendEventErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "append_event_errors_total",
		Help:      "the number of failed append event requests",
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

	prometheus.MustRegister(appendEventDurations)
	prometheus.MustRegister(appendEventRequests)
	prometheus.MustRegister(appendEventErrors)

	prometheus.MustRegister(getIteratorDurations)
	prometheus.MustRegister(getIteratorRequests)
	prometheus.MustRegister(getIteratorErrors)

	return &storageMetrics{
		appendEventDurations: appendEventDurations,
		appendEventRequests:  appendEventRequests,
		appendEventErrors:    appendEventErrors,
		getIteratorDurations: getIteratorDurations,
		getIteratorRequests:  getIteratorRequests,
		getIteratorErrors:    getIteratorErrors,
	}
}

type storageMetrics struct {
	appendEventDurations prometheus.Histogram
	appendEventRequests  prometheus.Counter
	appendEventErrors    prometheus.Counter
	getIteratorDurations prometheus.Histogram
	getIteratorRequests  prometheus.Counter
	getIteratorErrors    prometheus.Counter
}
