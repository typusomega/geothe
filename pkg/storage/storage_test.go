package storage_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_leveldb.go github.com/typusomega/goethe/pkg/storage LevelDB
//go:generate mockgen -package mocks -destination=./../mocks/mock_idgenerator.go github.com/typusomega/goethe/pkg/storage IDGenerator
//go:generate mockgen -package mocks -destination=./../mocks/mock_leveldb_iterator.go github.com/syndtr/goleveldb/leveldb/iterator Iterator

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

func TestDiskStorage_Append(t *testing.T) {
	type args struct {
		event *spec.Event
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB, iterator *mocks.MockIterator)
		when  args
		then  func(event *spec.Event, err error)
	}{
		{
			name: "happy",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(batch *leveldb.Batch, wo *opt.WriteOptions) error {
						assert.Equal(t, batch.Len(), 1)
						return nil
					}).Times(1)
			},
			when: args{event: &defaultEvent},
			then: func(event *spec.Event, err error) {
				assert.Nil(t, err)
				assert.Equal(t, expectedEvent.GetId(), event.GetId())
				assert.Equal(t, expectedEvent.GetTopic(), event.GetTopic())
				assert.Equal(t, expectedEvent.GetPayload(), event.GetPayload())
			},
		},
		{
			name: "db write failure",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).Return(errDefault).Times(1)
			},
			when: args{event: &defaultEvent},
			then: func(event *spec.Event, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errorx.RejectedOperation))
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			idGeneratorMock := mocks.NewMockIDGenerator(controller)
			idGeneratorMock.EXPECT().Next().Return(generatedID).AnyTimes()
			if tt.given != nil {
				tt.given(dbMock, nil)
			}

			it := storage.New(dbMock, idGeneratorMock)
			got, err := it.Append(tt.when.event)
			tt.then(got, err)
		})
	}
}

func TestDiskStorage_Read(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB, iterator *mocks.MockIterator)
		when  args
		then  func(cursor *spec.Cursor, err error)
	}{
		{
			name: "existing event",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				iterator.EXPECT().Release().Times(1)
				iterator.EXPECT().Seek(gomock.Any()).DoAndReturn(func(key []byte) bool {
					assert.Equal(t, key, defaultCursorEventKey)
					return true
				}).Times(1)
				iterator.EXPECT().Next().Return(true).Times(1)
				iterator.EXPECT().Key().Return(defaultCursorEventKey).Times(1)
				iterator.EXPECT().Value().Return(marshalledExpectedEvent()).Times(1)
			},
			when: args{cursor: &defaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assertEventEquals(t, &expectedEvent, cursor.GetCurrentEvent())
				assert.Equal(t, defaultServiceID, cursor.GetServiceId())
				assert.Equal(t, defaultTopic.GetId(), cursor.GetTopic().GetId())
			},
		},
		{
			name: "empty event id",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				iterator.EXPECT().Release().Times(1)
				iterator.EXPECT().Seek(gomock.Any()).Times(0)
				iterator.EXPECT().Next().Return(true).Times(1)
				iterator.EXPECT().Key().Return(defaultCursorEventKey).Times(1)
				iterator.EXPECT().Value().Return(marshalledExpectedEvent()).Times(1)
			},
			when: args{cursor: &spec.Cursor{Topic: &defaultTopic, ServiceId: defaultServiceID}},
			then: func(cursor *spec.Cursor, err error) {
				assertEventEquals(t, &expectedEvent, cursor.GetCurrentEvent())
				assert.Equal(t, defaultServiceID, cursor.GetServiceId())
				assert.Equal(t, defaultTopic.GetId(), cursor.GetTopic().GetId())
			},
		},
		{
			name: "no more events",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				iterator.EXPECT().Release().Times(1)
				iterator.EXPECT().Seek(gomock.Any()).Times(0)
				iterator.EXPECT().Next().Return(false).Times(1)
			},
			when: args{cursor: &spec.Cursor{Topic: &defaultTopic, ServiceId: defaultServiceID}},
			then: func(cursor *spec.Cursor, err error) {
				assert.True(t, errorx.HasTrait(err, errors.ResourceExhausted()))
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			iteratorMock := mocks.NewMockIterator(controller)
			dbMock.EXPECT().NewIterator(gomock.Any(), gomock.Any()).Return(iteratorMock).AnyTimes()
			if tt.given != nil {
				tt.given(dbMock, iteratorMock)
			}

			it := storage.New(dbMock, nil)
			got, err := it.Read(tt.when.cursor)
			tt.then(got, err)
		})
	}
}

func TestDiskStorage_GetCursorFor(t *testing.T) {
	type args struct {
		serviceID string
		topic     string
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB)
		when  args
		then  func(cursor *spec.Cursor, err error)
	}{
		{
			name: "cursor not found",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(defaultCursorCursorKey), gomock.Any()).Return(nil, leveldb.ErrNotFound).Times(1)
			},
			when: args{serviceID: defaultServiceID, topic: defaultTopic.GetId()},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.HasTrait(err, errorx.NotFound()))
			},
		},
		{
			name: "unknown error",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(defaultCursorCursorKey), gomock.Any()).Return(nil, errDefault).Times(1)
			},
			when: args{serviceID: defaultServiceID, topic: defaultTopic.GetId()},
			then: func(cursor *spec.Cursor, err error) {
				assert.Equal(t, errDefault, err)
			},
		},
		{
			name: "cursor found",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(defaultCursorCursorKey), gomock.Any()).Return(marshalledDefaultCursor(), nil).Times(1)
			},
			when: args{serviceID: defaultServiceID, topic: defaultTopic.GetId()},
			then: func(cursor *spec.Cursor, err error) {
				assert.Nil(t, err)
				assertCursorEquals(t, &defaultCursor, cursor)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			if tt.given != nil {
				tt.given(dbMock)
			}

			it := storage.New(dbMock, nil)
			got, err := it.GetCursorFor(tt.when.serviceID, tt.when.topic)
			tt.then(got, err)
		})
	}
}

func TestDiskStorage_SaveCursor(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB)
		when  args
		then  func(err error)
	}{
		{
			name: "db write failure",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(batch *leveldb.Batch, wo *opt.WriteOptions) error {
						assert.Equal(t, batch.Len(), 1)
						return errDefault
					}).Times(1)
			},
			when: args{cursor: &defaultCursor},
			then: func(err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errorx.RejectedOperation))
			},
		},
		{
			name: "save success",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(batch *leveldb.Batch, wo *opt.WriteOptions) error {
						assert.Equal(t, batch.Len(), 1)
						return nil
					}).Times(1)
			},
			when: args{cursor: &defaultCursor},
			then: func(err error) {
				assert.Nil(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			if tt.given != nil {
				tt.given(dbMock)
			}

			it := storage.New(dbMock, nil)
			err := it.SaveCursor(tt.when.cursor)
			tt.then(err)
		})
	}
}

func assertEventEquals(t *testing.T, expected *spec.Event, actual *spec.Event) {
	assert.Equal(t, expected.GetId(), actual.GetId())
	assert.Equal(t, expected.GetTopic().GetId(), actual.GetTopic().GetId())
	assert.Equal(t, expected.GetPayload(), actual.GetPayload())
}
func assertCursorEquals(t *testing.T, expected *spec.Cursor, actual *spec.Cursor) {
	assert.Equal(t, expected.GetTopic().GetId(), actual.GetTopic().GetId())
	assert.Equal(t, expected.GetServiceId(), actual.GetServiceId())
	assertEventEquals(t, expected.GetCurrentEvent(), actual.GetCurrentEvent())
}

var errDefault = fmt.Errorf("fail")
var generatedID = "123456789"
var defaultServiceID = "service1"

var defaultTopic = spec.Topic{
	Id: "default",
}

var defaultEvent = spec.Event{
	Id:      "123",
	Topic:   &defaultTopic,
	Payload: []byte("123"),
}

var defaultCursorEventKey = []byte(defaultCursor.GetTopic().GetId() + storage.KeySeperator + defaultCursor.GetCurrentEvent().GetId())
var defaultCursorCursorKey = []byte(storage.CursorPrefix + storage.KeySeperator + defaultCursor.GetServiceId() + storage.KeySeperator + defaultCursor.GetTopic().GetId())

func marshalledExpectedEvent() []byte {
	bytes, err := proto.Marshal(&expectedEvent)
	if err != nil {
		panic(err)
	}
	return bytes
}

func marshalledDefaultCursor() []byte {
	bytes, err := proto.Marshal(&defaultCursor)
	if err != nil {
		panic(err)
	}
	return bytes
}

var defaultCursor = spec.Cursor{
	Topic:        &defaultTopic,
	ServiceId:    defaultServiceID,
	CurrentEvent: &defaultEvent,
}

var expectedEvent = spec.Event{
	Id:      generatedID,
	Topic:   &defaultTopic,
	Payload: []byte("123"),
}
