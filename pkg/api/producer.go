package api

import (
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

type Producer interface {
	Produce(event *spec.Event) (*spec.Event, error)
}

func NewProducer(eventStorage storage.EventStorage) Producer {
	return &producer{events: eventStorage}
}

func (it *producer) Produce(event *spec.Event) (*spec.Event, error) {
	return it.events.Append(event)
}

type producer struct {
	events storage.EventStorage
}
