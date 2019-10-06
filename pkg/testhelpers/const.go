package testhelpers

import (
	"context"
	"fmt"

	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

var DefaultContext = context.Background()

var ErrDefault = fmt.Errorf("fail")
var DefaultConsumer = "service1"
var GeneratedID = "1234567"
var DefaultTopic = spec.Topic{
	Id: "default",
}
var DifferentTopic = spec.Topic{
	Id: "different",
}

var DefaultEvent = spec.Event{
	Id:      "123",
	Topic:   &DefaultTopic,
	Payload: []byte("123"),
}

var DefaultProduceEvent = spec.Event{
	Topic:   &DefaultTopic,
	Payload: []byte("test message"),
}

var EventFromADifferentTopic = spec.Event{
	Id:      "124",
	Topic:   &DifferentTopic,
	Payload: []byte("123"),
}

var DefaultCursorEventKey = storage.NewKeyGenerator().Event(DefaultCursor.GetCurrentEvent())
var DefaultCursorCursorKey = storage.NewKeyGenerator().Cursor(&DefaultCursor)

var DefaultCursor = spec.Cursor{
	Topic:        &DefaultTopic,
	Consumer:     DefaultConsumer,
	CurrentEvent: &DefaultEvent,
}

var NoEventCursor = spec.Cursor{Topic: &DefaultTopic, Consumer: DefaultConsumer}

var ExpectedEvent = spec.Event{
	Id:      GeneratedID,
	Topic:   &DefaultTopic,
	Payload: []byte("123"),
}
