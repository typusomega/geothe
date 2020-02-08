package storage_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_storage_metrics.go -mock_names Metrics=MockStorageMetrics github.com/typusomega/goethe/pkg/storage Metrics

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
	"github.com/typusomega/goethe/pkg/testhelpers"
)

func Test_eventStorage_Append(t *testing.T) {
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
			when: args{event: &testhelpers.DefaultEvent},
			then: func(event *spec.Event, err error) {
				assert.Nil(t, err)
				assert.Equal(t, testhelpers.ExpectedEvent.GetId(), event.GetId())
				assert.Equal(t, testhelpers.ExpectedEvent.GetTopic(), event.GetTopic())
				assert.Equal(t, testhelpers.ExpectedEvent.GetPayload(), event.GetPayload())
			},
		},
		{
			name: "db write failure",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).Return(testhelpers.ErrDefault).Times(1)
			},
			when: args{event: &testhelpers.DefaultEvent},
			then: func(event *spec.Event, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errors.Internal))
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
			idGeneratorMock.EXPECT().Next().Return(testhelpers.GeneratedID)
			if tt.given != nil {
				tt.given(dbMock, nil)
			}

			it := storage.NewEventStorage(dbMock, idGeneratorMock, storage.NewKeyGenerator(), metrics)
			got, err := it.Append(tt.when.event)
			tt.then(got, err)
		})
	}
}

func Test_eventStorage_GetIterator(t *testing.T) {
	type args struct {
		event *spec.Event
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB, iterator *mocks.MockIterator)
		when  args
		then  func(iterator storage.EventsIterator, err error)
	}{
		{
			name:  "empty topic",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {},
			when:  args{},
			then: func(iterator storage.EventsIterator, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errors.InvalidArgument))
			},
		},
		{
			name: "no later or equal event found",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				iterator.EXPECT().Release().Times(1)
				iterator.EXPECT().Seek(storage.NewKeyGenerator().Event(&testhelpers.DefaultEvent)).Return(false).Times(1)
			},
			when: args{event: &testhelpers.DefaultEvent},
			then: func(iterator storage.EventsIterator, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.HasTrait(err, errors.NotFoundTrait))
			},
		},
		{
			name: "later or equal event found",
			given: func(db *mocks.MockLevelDB, iterator *mocks.MockIterator) {
				iterator.EXPECT().Seek(storage.NewKeyGenerator().Event(&testhelpers.DefaultEvent)).Return(true).Times(1)
			},
			when: args{event: &testhelpers.DefaultEvent},
			then: func(iterator storage.EventsIterator, err error) {
				assert.Nil(t, err)
				assert.NotNil(t, iterator)
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
			idGeneratorMock := mocks.NewMockIDGenerator(controller)
			idGeneratorMock.EXPECT().Next().Return(testhelpers.GeneratedID).AnyTimes()
			if tt.given != nil {
				tt.given(dbMock, iteratorMock)
			}

			it := storage.NewEventStorage(dbMock, idGeneratorMock, storage.NewKeyGenerator(), metrics)
			got, err := it.GetIterator(tt.when.event)
			tt.then(got, err)
		})
	}
}

func Test_eventsIterator_Next(t *testing.T) {
	tests := []struct {
		name  string
		given func(iterator *mocks.MockIterator)
		then  func(result bool)
	}{
		{
			name: "no more events",
			given: func(iterator *mocks.MockIterator) {
				iterator.EXPECT().Next().Return(false).Times(1)
			},
			then: func(result bool) {
				assert.False(t, result)
			},
		},
		{
			name: "invalid event id",
			given: func(iterator *mocks.MockIterator) {
				iterator.EXPECT().Next().Return(true).Times(1)
				iterator.EXPECT().Key().Return([]byte("invalid!")).Times(1)
			},
			then: func(result bool) {
				assert.False(t, result)
			},
		},
		{
			name: "different topics",
			given: func(iterator *mocks.MockIterator) {
				iterator.EXPECT().Next().Return(true).Times(2)
				gotWrong := false
				iterator.EXPECT().Key().DoAndReturn(func() []byte {
					if !gotWrong {
						gotWrong = true
						return testhelpers.EventKey(&testhelpers.EventFromADifferentTopic)
					}
					return testhelpers.EventKey(&testhelpers.DefaultEvent)
				}).Times(2)

			},
			then: func(result bool) {
				assert.True(t, result)
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
			iteratorMock.EXPECT().Seek(gomock.Any()).Return(true).AnyTimes()
			dbMock.EXPECT().NewIterator(gomock.Any(), gomock.Any()).Return(iteratorMock).AnyTimes()

			idGeneratorMock := mocks.NewMockIDGenerator(controller)
			idGeneratorMock.EXPECT().Next().Return(testhelpers.GeneratedID).AnyTimes()
			if tt.given != nil {
				tt.given(iteratorMock)
			}

			eventStorage := storage.NewEventStorage(dbMock, idGeneratorMock, storage.NewKeyGenerator(), metrics)
			iterator, err := eventStorage.GetIterator(&testhelpers.DefaultEvent)
			assert.Nil(t, err)

			tt.then(iterator.Next())
		})
	}
}

func Test_eventsIterator_Value(t *testing.T) {
	tests := []struct {
		name  string
		given func(iterator *mocks.MockIterator)
		then  func(event *spec.Event, err error)
	}{
		{
			name:  "serialization error",
			given: func(iterator *mocks.MockIterator) { iterator.EXPECT().Value().Return([]byte("invalid")).Times(1) },
			then: func(event *spec.Event, err error) {
				assert.NotNil(t, err)
			},
		},
		{
			name: "serialization success",
			given: func(iterator *mocks.MockIterator) {
				iterator.EXPECT().Value().Return(testhelpers.MarshalledExpectedEvent()).Times(1)
			},
			then: func(event *spec.Event, err error) {
				testhelpers.AssertEventEquals(t, &testhelpers.ExpectedEvent, event)
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
			iteratorMock.EXPECT().Seek(gomock.Any()).Return(true).AnyTimes()
			dbMock.EXPECT().NewIterator(gomock.Any(), gomock.Any()).Return(iteratorMock).AnyTimes()

			idGeneratorMock := mocks.NewMockIDGenerator(controller)
			idGeneratorMock.EXPECT().Next().Return(testhelpers.GeneratedID).AnyTimes()
			if tt.given != nil {
				tt.given(iteratorMock)
			}

			eventStorage := storage.NewEventStorage(dbMock, idGeneratorMock, storage.NewKeyGenerator(), metrics)
			iterator, err := eventStorage.GetIterator(&testhelpers.DefaultEvent)
			assert.Nil(t, err)

			tt.then(iterator.Value())
		})
	}
}

func Test_eventsIterator_Close(t *testing.T) {
	tests := []struct {
		name  string
		given func(iterator *mocks.MockIterator)
		then  func(err error)
	}{
		{
			name:  "success",
			given: func(iterator *mocks.MockIterator) { iterator.EXPECT().Release().Times(1) },
			then:  func(err error) { assert.Nil(t, err) },
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			iteratorMock := mocks.NewMockIterator(controller)
			iteratorMock.EXPECT().Seek(gomock.Any()).Return(true).AnyTimes()
			dbMock.EXPECT().NewIterator(gomock.Any(), gomock.Any()).Return(iteratorMock).AnyTimes()

			idGeneratorMock := mocks.NewMockIDGenerator(controller)
			idGeneratorMock.EXPECT().Next().Return(testhelpers.GeneratedID).AnyTimes()
			if tt.given != nil {
				tt.given(iteratorMock)
			}

			eventStorage := storage.NewEventStorage(dbMock, idGeneratorMock, storage.NewKeyGenerator(), metrics)
			iterator, err := eventStorage.GetIterator(&testhelpers.DefaultEvent)
			assert.Nil(t, err)

			tt.then(iterator.Close())
		})
	}
}

var metrics = storage.NewMetrics()
