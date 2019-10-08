package storage

import (
	"strings"

	"github.com/joomcode/errorx"
	"github.com/typusomega/goethe/pkg/spec"
)

// KeyGenerator creates key-value store keys from domain models.
type KeyGenerator interface {
	// Cursor generates cursor key.
	Cursor(cursor *spec.Cursor) []byte
	// Event generates event key.
	Event(event *spec.Event) []byte
	// KeyToEvent reconstructs the event from the values in the given key (topic, id).
	KeyToEvent(key []byte) (*spec.Event, error)
}

// NewKeyGenerator ctor.
func NewKeyGenerator() KeyGenerator {
	return &keyGenerator{}
}

type keyGenerator struct{}

// Cursor generates cursor key.
func (keyGenerator) Cursor(cursor *spec.Cursor) []byte {
	return []byte(cursorPrefix + keySeperator + cursor.GetConsumer() + keySeperator + cursor.GetTopic().GetId())
}

// Event generates event key.
func (keyGenerator) Event(event *spec.Event) []byte {
	return []byte(eventsPrefix + keySeperator + event.GetTopic().GetId() + keySeperator + event.GetId())
}

// KeyToEvent reconstructs the event from the values in the given key.
func (keyGenerator) KeyToEvent(key []byte) (*spec.Event, error) {
	stringKey := string(key)
	parts := strings.Split(stringKey, keySeperator)
	if len(parts) != 3 {
		return nil, errorx.IllegalState.New("could not extract topic from key: %v", stringKey)
	}

	return &spec.Event{
		Id: parts[2],
		Topic: &spec.Topic{
			Id: parts[1],
		},
	}, nil
}

const keySeperator = ":"
const cursorPrefix = "CURSORS"
const eventsPrefix = "EVENTS"
