package api

import (
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

// Producer proxies producer capabilities.
type Producer interface {
	// Produce produces the given event.
	Produce(event *spec.Event) (*spec.Event, error)
}

// NewProducer ctor.
func NewProducer(eventStorage storage.EventStorage, metrics Metrics) Producer {
	return &producer{events: eventStorage, metrics: metrics}
}

func (it *producer) Produce(event *spec.Event) (*spec.Event, error) {
	event, err := it.events.Append(event)
	if err != nil {
		return nil, err
	}
	it.metrics.EventProduced(event.GetTopic().GetId())
	return event, err
}

type producer struct {
	events  storage.EventStorage
	metrics Metrics
}
