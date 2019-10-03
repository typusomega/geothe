package storage

import (
	"strconv"
	"strings"
	"time"

	"github.com/joomcode/errorx"
	"github.com/typusomega/goethe/pkg/spec"
)

type KeyGenerator interface {
	Cursor(cursor *spec.Cursor) []byte
	Event(event *spec.Event) []byte
	Index(timestamp time.Time) []byte
	KeyToEvent(key []byte) (*spec.Event, error)
}

func NewKeyGenerator() KeyGenerator {
	return &keyGenerator{}
}

type keyGenerator struct{}

func (keyGenerator) Cursor(cursor *spec.Cursor) []byte {
	return []byte(cursorPrefix + keySeperator + cursor.GetConsumer() + keySeperator + cursor.GetTopic().GetId())
}

func (keyGenerator) Event(event *spec.Event) []byte {
	return []byte(eventsPrefix + keySeperator + event.GetTopic().GetId() + keySeperator + event.GetId())
}

func (keyGenerator) Index(timestamp time.Time) []byte {
	return []byte(indexPrefix + keySeperator + strconv.FormatInt(timestamp.UnixNano(), 10))
}

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
const indexPrefix = "INDEX"
