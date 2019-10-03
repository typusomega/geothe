package storage

import (
	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

type EventStorage interface {
	Append(event *spec.Event) (*spec.Event, error)
	Read(cursor *spec.Cursor) (*spec.Cursor, error)
}

func NewEvents(db LevelDB, idGenerator IDGenerator, keyGenerator KeyGenerator) EventStorage {
	return &eventStorage{db: db, ids: idGenerator, keys: keyGenerator}
}

func (it *eventStorage) Append(event *spec.Event) (*spec.Event, error) {
	topic := event.GetTopic()
	eventToStore := &spec.Event{Id: it.ids.Next(), Topic: topic, Payload: event.GetPayload()}

	serializedEvent, err := serializeEvent(eventToStore)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)
	batch.Put(it.keys.Event(eventToStore), serializedEvent)

	err = it.db.Write(batch, nil)
	if err != nil {
		return nil, errorx.RejectedOperation.New("could not write event: [%v, %v]", topic, eventToStore)
	}

	return eventToStore, nil
}

func (it *eventStorage) Read(cursor *spec.Cursor) (*spec.Cursor, error) {
	iterator := it.db.NewIterator(nil, nil)
	defer iterator.Release()

	if cursor.GetCurrentEvent() != nil && cursor.GetCurrentEvent().GetId() != "" {
		logrus.Debug("seeking to cursor position")
		if ok := iterator.Seek(it.keys.Event(cursor.GetCurrentEvent())); !ok {
			return cursor, errors.NotFound.New("could not find event for cursor: %v", cursor)
		}
	}

	for {
		if ok := iterator.Next(); !ok {
			return cursor, errors.ResourceExhaustedError.New("no more events in topic: '%v'", cursor.GetTopic())
		}

		event, err := it.keys.KeyToEvent(iterator.Key())
		if err != nil {
			return cursor, err
		}

		if event.GetTopic().GetId() != cursor.GetTopic().GetId() {
			continue
		}

		nextEvent, err := deserializeEvent(iterator.Value())
		if err != nil {
			return cursor, err
		}

		return &spec.Cursor{
			Topic:        cursor.GetTopic(),
			Consumer:     cursor.GetConsumer(),
			CurrentEvent: nextEvent,
		}, nil
	}
}

type eventStorage struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
