package storage

import (
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

type EventStorage interface {
	Append(event *spec.Event) (*spec.Event, error)
	Read(cursor *spec.Cursor) (*spec.Cursor, error)
}

type CursorStorage interface {
	GetCursorFor(cursor *spec.Cursor) (*spec.Cursor, error)
	SaveCursor(cursor *spec.Cursor) error
}

type EventIndex interface {
	GetNearest(timestamp time.Time) (*spec.Event, error)
}

type CombinedStorage interface {
	EventStorage
	CursorStorage
}

func New(db LevelDB, idGenerator IDGenerator, keyGenerator KeyGenerator) CombinedStorage {
	return &DiskStorage{db: db, ids: idGenerator, keys: keyGenerator}
}

type LevelDB interface {
	io.Closer
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

func (it *DiskStorage) Append(event *spec.Event) (*spec.Event, error) {
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

func (it *DiskStorage) Read(cursor *spec.Cursor) (*spec.Cursor, error) {
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

func (it *DiskStorage) GetCursorFor(cursor *spec.Cursor) (*spec.Cursor, error) {
	foundCursor, err := it.db.Get(it.keys.Cursor(cursor), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, errors.NotFound.New("could not find cursor: %v", cursor)
		}
		return nil, err
	}
	return deserializeCursor(foundCursor)
}

func (it *DiskStorage) SaveCursor(cursor *spec.Cursor) error {
	serializedCursor, err := serializeCursor(cursor)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(it.keys.Cursor(cursor), serializedCursor)

	err = it.db.Write(batch, nil)
	if err != nil {
		return errorx.RejectedOperation.New("could not write cursor: [%v, %v]", cursor.GetConsumer(), cursor.GetTopic().GetId())
	}

	return nil
}

func (it *DiskStorage) GetNearest(timestamp time.Time) (*spec.Event, error) {
	return nil, nil
}

func serializeEvent(event *spec.Event) ([]byte, error) {
	serializedEvent, err := proto.Marshal(event)
	if err != nil {
		return nil, errorx.IllegalFormat.Wrap(err, "could not serialize event: %v", event)
	}
	return serializedEvent, nil
}

func deserializeEvent(serialized []byte) (*spec.Event, error) {
	event := &spec.Event{}
	err := proto.Unmarshal(serialized, event)
	if err != nil {
		return nil, errorx.IllegalState.Wrap(err, "could not deserialize event")
	}
	return event, nil
}

func serializeCursor(cursor *spec.Cursor) ([]byte, error) {
	serializedCursor, err := proto.Marshal(cursor)
	if err != nil {
		return nil, errorx.IllegalFormat.Wrap(err, "could not serialize cursor: %v", cursor)
	}
	return serializedCursor, nil
}

func deserializeCursor(serialized []byte) (*spec.Cursor, error) {
	cursor := &spec.Cursor{}
	err := proto.Unmarshal(serialized, cursor)
	if err != nil {
		return nil, errorx.IllegalState.Wrap(err, "could not deserialize cursor")
	}
	return cursor, nil
}

type DiskStorage struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
