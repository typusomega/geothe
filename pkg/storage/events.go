package storage

import (
	"io"

	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

type EventsIterator interface {
	io.Closer

	// Value returns the current event or nil if done.
	Value() (*spec.Event, error)

	// Next moves the iterator to the next event.
	// It returns false if the iterator is exhausted.
	Next() bool
}

type EventStorage interface {
	Append(event *spec.Event) (*spec.Event, error)
	GetIterator(event *spec.Event) (EventsIterator, error)
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

func (it *eventStorage) GetIterator(event *spec.Event) (EventsIterator, error) {
	if event.GetTopic().GetId() == "" {
		return nil, errorx.IllegalArgument.New("topic must be set")
	}

	iterator := it.db.NewIterator(nil, nil)

	if ok := iterator.Seek(it.keys.Event(event)); !ok {
		iterator.Release()
		return nil, errors.NotFound.New("could not find event equal or later than: %v", event)
	}

	return &eventsIterator{
		topic: event.GetTopic().GetId(),
		inner: iterator,
		keys:  it.keys,
	}, nil
}

type eventsIterator struct {
	topic string
	inner iterator.Iterator
	keys  KeyGenerator
}

func (it *eventsIterator) Next() bool {
	for {
		if ok := it.inner.Next(); !ok {
			return false
		}

		event, err := it.keys.KeyToEvent(it.inner.Key())
		if err != nil {
			logrus.WithError(err).Error("could not iterate over events: corrupted database state")
			return false
		}

		if event.GetTopic().GetId() == it.topic {
			return true
		}
	}
}

func (it *eventsIterator) Value() (*spec.Event, error) {
	return deserializeEvent(it.inner.Value())
}

func (it *eventsIterator) Close() error {
	it.inner.Release()
	return nil
}

type eventStorage struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
