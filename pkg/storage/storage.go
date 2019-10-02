package storage

import (
	"io"
	"strings"

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
	GetCursorFor(serviceID string, topic string) (*spec.Cursor, error)
	SaveCursor(cursor *spec.Cursor) error
}

type CombinedStorage interface {
	EventStorage
	CursorStorage
}

func New(db LevelDB, idGenerator IDGenerator) CombinedStorage {
	return &DiskStorage{db: db, ids: idGenerator}
}

type DiskStorage struct {
	db  LevelDB
	ids IDGenerator
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
	batch.Put(eventKeyFromEvent(eventToStore), serializedEvent)

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
		if ok := iterator.Seek(eventKeyFromCursor(cursor)); !ok {
			return cursor, errors.NotFound.New("could not find event for cursor: %v", cursor)
		}
	}

	for {
		if ok := iterator.Next(); !ok {
			return cursor, errors.ResourceExhaustedError.New("no more events in topic: '%v'", cursor.GetTopic())
		}

		topic, err := TopicFromKey(iterator.Key())
		if err != nil {
			return cursor, err
		}

		if topic != cursor.GetTopic().GetId() {
			continue
		}

		nextEvent, err := deserializeEvent(iterator.Value())
		if err != nil {
			return cursor, err
		}

		return &spec.Cursor{
			Topic:        cursor.GetTopic(),
			ServiceId:    cursor.GetServiceId(),
			CurrentEvent: nextEvent,
		}, nil
	}
}

func (it *DiskStorage) GetCursorFor(serviceID string, topic string) (*spec.Cursor, error) {
	cursor, err := it.db.Get(cursorKey(serviceID, topic), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, errors.NotFound.New("could not find cursor for: [%v, %v]", serviceID, topic)
		}
		return nil, err
	}
	return deserializeCursor(cursor)
}

func (it *DiskStorage) SaveCursor(cursor *spec.Cursor) error {
	serializedCursor, err := serializeCursor(cursor)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(cursorKeyFromCursor(cursor), serializedCursor)

	err = it.db.Write(batch, nil)
	if err != nil {
		return errorx.RejectedOperation.New("could not write cursor: [%v, %v]", cursor.GetServiceId(), cursor.GetTopic().GetId())
	}

	return nil
}

func eventKeyFromEvent(event *spec.Event) []byte {
	return []byte(event.GetTopic().GetId() + KeySeperator + event.GetId())
}

func eventKeyFromCursor(cursor *spec.Cursor) []byte {
	return []byte(cursor.GetTopic().GetId() + KeySeperator + cursor.GetCurrentEvent().GetId())
}

func cursorKey(serviceID string, topic string) []byte {
	return []byte(CursorPrefix + KeySeperator + serviceID + KeySeperator + topic)
}

func cursorKeyFromCursor(cursor *spec.Cursor) []byte {
	return cursorKey(cursor.GetTopic().GetId(), cursor.GetServiceId())
}

func TopicFromKey(key []byte) (string, error) {
	stringKey := string(key)
	topicEnd := strings.Index(stringKey, ":")
	if topicEnd <= 0 {
		return "", errorx.IllegalState.New("could not extract topic from key: %v", stringKey)
	}
	return stringKey[:topicEnd], nil
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

const KeySeperator = ":"
const CursorPrefix = "CURSOR"
