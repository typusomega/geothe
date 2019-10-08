package testhelpers

import (
	"context"
	"fmt"

	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

// DefaultContext for testing
var DefaultContext = context.Background()

// ErrDefault for testing
var ErrDefault = fmt.Errorf("fail")

// DefaultConsumer for testing
var DefaultConsumer = "service1"

// GeneratedID for testing
var GeneratedID = "1234567"

// DefaultTopic for testing
var DefaultTopic = spec.Topic{
	Id: "default",
}

// DifferentTopic for testing
var DifferentTopic = spec.Topic{
	Id: "different",
}

// DefaultEvent for testing
var DefaultEvent = spec.Event{
	Id:      "123",
	Topic:   &DefaultTopic,
	Payload: []byte("123"),
}

// DefaultProduceEvent for testing
var DefaultProduceEvent = spec.Event{
	Topic:   &DefaultTopic,
	Payload: []byte("test message"),
}

// EventFromADifferentTopic for testing
var EventFromADifferentTopic = spec.Event{
	Id:      "124",
	Topic:   &DifferentTopic,
	Payload: []byte("123"),
}

// DefaultCursorEventKey for testing
var DefaultCursorEventKey = storage.NewKeyGenerator().Event(DefaultCursor.GetCurrentEvent())

// DefaultCursorCursorKey for testing
var DefaultCursorCursorKey = storage.NewKeyGenerator().Cursor(&DefaultCursor)

// DefaultCursor for testing
var DefaultCursor = spec.Cursor{
	Topic:        &DefaultTopic,
	Consumer:     DefaultConsumer,
	CurrentEvent: &DefaultEvent,
}

// NoEventCursor for testing
var NoEventCursor = spec.Cursor{Topic: &DefaultTopic, Consumer: DefaultConsumer}

// ExpectedEvent for testing
var ExpectedEvent = spec.Event{
	Id:      GeneratedID,
	Topic:   &DefaultTopic,
	Payload: []byte("123"),
}
