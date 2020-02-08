package storage

import (
	"github.com/golang/protobuf/proto"

	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

func serializeEvent(event *spec.Event) ([]byte, error) {
	serializedEvent, err := proto.Marshal(event)
	if err != nil {
		return nil, errors.InvalidArgument.Wrap(err, "could not serialize event: %v", event)
	}
	return serializedEvent, nil
}

func deserializeEvent(serialized []byte) (*spec.Event, error) {
	event := &spec.Event{}
	err := proto.Unmarshal(serialized, event)
	if err != nil {
		return nil, errors.FailedPrecondition.Wrap(err, "could not deserialize event")
	}
	return event, nil
}

func serializeCursor(cursor *spec.Cursor) ([]byte, error) {
	serializedCursor, err := proto.Marshal(cursor)
	if err != nil {
		return nil, errors.InvalidArgument.Wrap(err, "could not serialize cursor: %v", cursor)
	}
	return serializedCursor, nil
}

func deserializeCursor(serialized []byte) (*spec.Cursor, error) {
	cursor := &spec.Cursor{}
	err := proto.Unmarshal(serialized, cursor)
	if err != nil {
		return nil, errors.FailedPrecondition.Wrap(err, "could not deserialize cursor")
	}
	return cursor, nil
}
