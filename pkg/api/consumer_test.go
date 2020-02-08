package api_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_eventstorage.go github.com/typusomega/goethe/pkg/storage EventStorage
//go:generate mockgen -package mocks -destination=./../mocks/mock_cursorstorage.go github.com/typusomega/goethe/pkg/storage CursorStorage
//go:generate mockgen -package mocks -destination=./../mocks/mock_eventsiterator.go github.com/typusomega/goethe/pkg/storage EventsIterator
//go:generate mockgen -package mocks -destination=./../mocks/mock_raftcluster.go github.com/typusomega/goethe/pkg/raft Cluster

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"

	"github.com/typusomega/goethe/pkg/api"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/testhelpers"
)

func Test_consumer_Commit(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(cluster *mocks.MockCluster, eventStorage *mocks.MockEventStorage)
		when  args
		then  func(err error)
	}{
		{
			name: "cluster error",
			given: func(cluster *mocks.MockCluster, eventStorage *mocks.MockEventStorage) {
				cluster.EXPECT().CommitCursor(&testhelpers.DefaultCursor).Return(testhelpers.ErrDefault).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(err error) {
				assert.NotNil(t, err)
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "cluster success",
			given: func(cluster *mocks.MockCluster, eventStorage *mocks.MockEventStorage) {
				cluster.EXPECT().CommitCursor(&testhelpers.DefaultCursor).Return(nil).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
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
			clusterMock := mocks.NewMockCluster(controller)
			eventStorageMock := mocks.NewMockEventStorage(controller)
			cursorStorageMock := mocks.NewMockCursorStorage(controller)
			if tt.given != nil {
				tt.given(clusterMock, eventStorageMock)
			}

			it := api.NewConsumer(clusterMock, cursorStorageMock, eventStorageMock)
			err := it.Commit(tt.when.cursor)
			tt.then(err)
		})
	}
}

func Test_consumer_GetIterator(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage)
		when  args
		then  func(t *testing.T, iterator api.ConsumerIterator, err error)
	}{
		{
			name:  "no topic",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {},
			when:  args{cursor: &spec.Cursor{}},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errors.InvalidArgument))
			},
		},
		{
			name: "no cursor's currentevent id",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(&testhelpers.NoEventCursor)).Return(&testhelpers.DefaultCursor, nil).Times(1)
				eventStorage.EXPECT().GetIterator(gomock.Eq(testhelpers.DefaultCursor.GetCurrentEvent())).Return(nil, nil).Times(1)
			},
			when: args{cursor: &testhelpers.NoEventCursor},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.Nil(t, err)
				assert.NotNil(t, iterator)
			},
		},
		{
			name: "no cursor's currentevent id, cursor not found",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(&testhelpers.NoEventCursor)).Return(nil, errors.NotFound.New("fail")).Times(1)
				eventStorage.EXPECT().GetIterator(gomock.Nil()).Return(nil, nil).Times(1)
			},
			when: args{cursor: &testhelpers.NoEventCursor},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.Nil(t, err)
				assert.NotNil(t, iterator)
			},
		},
		{
			name: "no cursor's currentevent id, unknown cursor storage failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(&testhelpers.NoEventCursor)).Return(nil, testhelpers.ErrDefault).Times(1)
				eventStorage.EXPECT().GetIterator(gomock.Any()).Times(0)
			},
			when: args{cursor: &testhelpers.NoEventCursor},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.NotNil(t, err)
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "cursor's currentevent id set",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Any()).Times(0)
				eventStorage.EXPECT().GetIterator(gomock.Eq(testhelpers.DefaultCursor.GetCurrentEvent())).Return(nil, nil).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.Nil(t, err)
				assert.NotNil(t, iterator)
			},
		},
		{
			name: "eventstorage get iterator failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Any()).Times(0)
				eventStorage.EXPECT().GetIterator(gomock.Eq(testhelpers.DefaultCursor.GetCurrentEvent())).Return(nil, testhelpers.ErrDefault).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(t *testing.T, iterator api.ConsumerIterator, err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			clusterMock := mocks.NewMockCluster(controller)
			eventStorageMock := mocks.NewMockEventStorage(controller)
			cursorStorageMock := mocks.NewMockCursorStorage(controller)
			if tt.given != nil {
				tt.given(cursorStorageMock, eventStorageMock)
			}

			it := api.NewConsumer(clusterMock, cursorStorageMock, eventStorageMock)
			iterator, err := it.GetIterator(tt.when.cursor)
			tt.then(t, iterator, err)
		})
	}
}

func Test_iterator_Value(t *testing.T) {
	tests := []struct {
		name  string
		given func(inner *mocks.MockEventsIterator)
		then  func(t *testing.T, cursor *spec.Cursor, err error)
	}{
		{
			name: "inner iterator failure",
			given: func(inner *mocks.MockEventsIterator) {
				inner.EXPECT().Value().Return(nil, testhelpers.ErrDefault).Times(1)
			},
			then: func(t *testing.T, cursor *spec.Cursor, err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "inner iterator success",
			given: func(inner *mocks.MockEventsIterator) {
				inner.EXPECT().Value().Return(&testhelpers.DefaultEvent, nil).Times(1)
			},
			then: func(t *testing.T, cursor *spec.Cursor, err error) {
				assert.Nil(t, err)
				assert.Equal(t, &testhelpers.DefaultTopic, cursor.GetTopic())
				assert.Equal(t, testhelpers.DefaultConsumer, cursor.GetConsumer())
				testhelpers.AssertEventEquals(t, &testhelpers.DefaultEvent, cursor.GetCurrentEvent())
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			innerMock := mocks.NewMockEventsIterator(controller)
			if tt.given != nil {
				tt.given(innerMock)
			}

			it := api.NewIterator(testhelpers.DefaultTopic, testhelpers.DefaultConsumer, innerMock, nil)
			value, err := it.Value()
			tt.then(t, value, err)
		})
	}
}
