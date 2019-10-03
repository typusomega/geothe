package api_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_eventstorage.go github.com/typusomega/goethe/pkg/storage EventStorage
//go:generate mockgen -package mocks -destination=./../mocks/mock_cursorstorage.go github.com/typusomega/goethe/pkg/storage CursorStorage

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/typusomega/goethe/pkg/api"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
)

func Test_consumer_Stream(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage)
		when  args
		then  func(cursor *spec.Cursor, err error)
	}{
		{
			name:  "nil cursor",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {},
			when:  args{cursor: &spec.Cursor{}},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errorx.IllegalArgument))
			},
		},
		{
			name:  "no topic",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {},
			when:  args{cursor: &spec.Cursor{}},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errorx.IllegalArgument))
			},
		},
		{
			name: "no event ID set, storage success",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(noEventIDCurser.GetTopic().GetId()), gomock.Eq(noEventIDCurser.GetServiceId())).Return(&defaultCursor, nil).Times(1)
				eventStorage.EXPECT().Read(gomock.Eq(&defaultCursor)).Return(&foundCursor, nil).Times(1)
				cursorStorage.EXPECT().SaveCursor(gomock.Eq(&foundCursor)).Times(1).Return(nil)
			},
			when: args{cursor: &noEventIDCurser},
			then: func(cursor *spec.Cursor, err error) {
				assert.Nil(t, err)
				assert.Equal(t, &foundCursor, cursor)
			},
		},
		{
			name: "no event ID set, cursor storage failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(noEventIDCurser.GetTopic().GetId()), gomock.Eq(noEventIDCurser.GetServiceId())).Return(nil, errDefault).Times(1)
				eventStorage.EXPECT().Read(gomock.Any()).Return(&foundCursor, nil).Times(0)
				cursorStorage.EXPECT().SaveCursor(gomock.Any()).Times(0).Return(nil)
			},
			when: args{cursor: &noEventIDCurser},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.Equal(t, errDefault, err)
			},
		},
		{
			name: "event ID set, storage success",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Any(), gomock.Any()).Times(0)
				eventStorage.EXPECT().Read(gomock.Eq(&defaultCursor)).Return(&foundCursor, nil).Times(1)
				cursorStorage.EXPECT().SaveCursor(gomock.Eq(&foundCursor)).Times(1).Return(nil)
			},
			when: args{cursor: &defaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.Nil(t, err)
				assert.Equal(t, &foundCursor, cursor)
			},
		},
		{
			name: "event ID set, event storage failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Any(), gomock.Any()).Times(0)
				eventStorage.EXPECT().Read(gomock.Eq(&defaultCursor)).Return(nil, errDefault).Times(1)
				cursorStorage.EXPECT().SaveCursor(gomock.Any()).Times(0).Return(nil)
			},
			when: args{cursor: &defaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.Equal(t, errDefault, err)
			},
		},
		{
			name: "no event ID set, save cursor failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Eq(noEventIDCurser.GetTopic().GetId()), gomock.Eq(noEventIDCurser.GetServiceId())).Return(&defaultCursor, nil).Times(1)
				eventStorage.EXPECT().Read(gomock.Eq(&defaultCursor)).Return(&foundCursor, nil).Times(1)
				cursorStorage.EXPECT().SaveCursor(gomock.Any()).Times(0).Return(errDefault).Times(1)
			},
			when: args{cursor: &noEventIDCurser},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.Equal(t, errDefault, err)
			},
		},
		{
			name: "event ID set, save cursor failure",
			given: func(cursorStorage *mocks.MockCursorStorage, eventStorage *mocks.MockEventStorage) {
				cursorStorage.EXPECT().GetCursorFor(gomock.Any(), gomock.Any()).Times(0)
				eventStorage.EXPECT().Read(gomock.Eq(&defaultCursor)).Return(&foundCursor, nil).Times(1)
				cursorStorage.EXPECT().SaveCursor(gomock.Any()).Times(0).Return(errDefault).Times(1)
			},
			when: args{cursor: &defaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.Equal(t, errDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			eventStorageMock := mocks.NewMockEventStorage(controller)
			cursorStorageMock := mocks.NewMockCursorStorage(controller)
			if tt.given != nil {
				tt.given(cursorStorageMock, eventStorageMock)
			}

			it := api.NewConsumer(cursorStorageMock, eventStorageMock)
			got, err := it.Consume(tt.when.cursor)
			tt.then(got, err)
		})
	}
}

var defaultCursor = spec.Cursor{
	Topic:        &topic,
	ServiceId:    serviceID,
	CurrentEvent: &defaultEvent,
}

var foundCursor = spec.Cursor{
	Topic:        &topic,
	ServiceId:    serviceID,
	CurrentEvent: &defaultEvent,
}

var noEventIDCurser = spec.Cursor{
	Topic:     &topic,
	ServiceId: serviceID,
	CurrentEvent: &spec.Event{
		Topic:   &topic,
		Payload: []byte("just testing"),
	},
}
