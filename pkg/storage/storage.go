package storage

//go:generate mockgen -package mocks -destination=./../mocks/mock_storage.go github.com/typusomega/goethe/pkg/storage Storage

import (
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

type Storage interface {
	io.Closer
	Append(event *spec.Event) (*spec.Event, error)
	Read(cursor *spec.Cursor) (*spec.Cursor, error)
}

func New(file string) (Storage, error) {
	db, err := leveldb.OpenFile(file, nil)
	if err != nil {
		return nil, err
	}
	return &DiskStorage{db: db}, nil
}

type DiskStorage struct {
	db *leveldb.DB
}

func (it *DiskStorage) Append(event *spec.Event) (*spec.Event, error) {
	topic := event.GetTopic()
	eventToStore := &spec.Event{Id: getNextID(), Topic: topic, Payload: event.GetPayload()}

	serializedEvent, err := serializeEvent(eventToStore)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)
	batch.Put(keyFromEvent(eventToStore), serializedEvent)

	err = it.db.Write(batch, nil)
	if err != nil {
		return nil, errorx.RejectedOperation.New("could not write: [%v, %v]", topic, eventToStore)
	}

	return eventToStore, nil
}

func (it *DiskStorage) Read(cursor *spec.Cursor) (*spec.Cursor, error) {
	iterator := it.db.NewIterator(nil, nil)
	defer iterator.Release()

	if cursor.GetCurrentEvent() != nil && cursor.GetCurrentEvent().GetId() != "" {
		logrus.Debug("seeking to cursor position")
		if ok := iterator.Seek(keyFromCursor(cursor)); !ok {
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

func keyFromEvent(event *spec.Event) []byte {
	return []byte(event.GetTopic().GetId() + KeySeperator + event.GetId())
}

func keyFromCursor(cursor *spec.Cursor) []byte {
	return []byte(cursor.GetTopic().GetId() + KeySeperator + cursor.GetCurrentEvent().GetId())
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

func deserializeEvent(serializedEvent []byte) (*spec.Event, error) {
	event := &spec.Event{}
	err := proto.Unmarshal(serializedEvent, event)
	if err != nil {
		return nil, errorx.IllegalState.Wrap(err, "could not deserialize event")
	}
	return event, nil
}

func (it *DiskStorage) Close() error {
	return it.db.Close()
}

const KeySeperator = ":"
