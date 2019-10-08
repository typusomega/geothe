package testhelpers

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AssertEventEquals asserts equality all fields of events
func AssertEventEquals(t *testing.T, expected *spec.Event, actual *spec.Event) {
	assert.Equal(t, expected.GetId(), actual.GetId())
	assert.Equal(t, expected.GetTopic().GetId(), actual.GetTopic().GetId())
	assert.Equal(t, expected.GetPayload(), actual.GetPayload())
}

// AssertCursorEquals asserts equality all fields of cursors
func AssertCursorEquals(t *testing.T, expected *spec.Cursor, actual *spec.Cursor) {
	assert.Equal(t, expected.GetTopic().GetId(), actual.GetTopic().GetId())
	assert.Equal(t, expected.GetConsumer(), actual.GetConsumer())
	AssertEventEquals(t, expected.GetCurrentEvent(), actual.GetCurrentEvent())
}

// EventKey extracts the key with the default keygenerator
func EventKey(event *spec.Event) []byte {
	return storage.NewKeyGenerator().Event(event)
}

// MarshalledExpectedEvent used for testing
func MarshalledExpectedEvent() []byte {
	bytes, err := proto.Marshal(&ExpectedEvent)
	if err != nil {
		panic(err)
	}
	return bytes
}

// MarshalledDefaultCursor used for testing
func MarshalledDefaultCursor() []byte {
	bytes, err := proto.Marshal(&DefaultCursor)
	if err != nil {
		panic(err)
	}
	return bytes
}

// GetStatusCode returns the status code from the error
func GetStatusCode(err error) codes.Code {
	s, _ := status.FromError(err)
	return s.Code()
}
