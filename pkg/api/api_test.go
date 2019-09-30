package api_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_publish_server.go github.com/typusomega/goethe/pkg/spec Goethe_PublishServer
//go:generate mockgen -package mocks -destination=./../mocks/mock_stream_server.go github.com/typusomega/goethe/pkg/spec Goethe_StreamServer

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/typusomega/goethe/pkg/api"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAPI_Publish(t *testing.T) {
	type args struct {
		stream spec.Goethe_PublishServer
	}
	tests := []struct {
		name  string
		given func(storage *mocks.MockStorage)
		when  args
		then  func(err error)
	}{
		{
			name: "valid request, stream receive ok, storage success, stream send ok",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Append(gomock.Eq(&defaultPublishEvent)).Times(3)
			},
			when: args{stream: threeRequestsPublishServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "client cancelled context",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: contextCancelledPublishServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive eof",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: publishServer(defaultContext, t, &validPublishRequest, io.EOF, nil)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive deadline exceeded",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: statusCodePublishServer(t, codes.DeadlineExceeded)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive canceled",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: statusCodePublishServer(t, codes.Canceled)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive unknown error",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: publishServer(defaultContext, t, nil, errDefault, nil)},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name:  "invalid request: no topic",
			given: func(storage *mocks.MockStorage) { storage.EXPECT().Append(gomock.Any()).Times(0) },
			when: args{stream: publishServer(defaultContext, t, &spec.PublishRequest{
				Event: &spec.Event{Topic: &topic, Payload: []byte("123")},
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name:  "invalid request: no event content",
			given: func(storage *mocks.MockStorage) { storage.EXPECT().Append(gomock.Any()).Times(0) },
			when: args{stream: publishServer(defaultContext, t, &spec.PublishRequest{
				Topic: &topic,
				Event: &spec.Event{Topic: &topic},
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name: "storage failure",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Append(gomock.Eq(&defaultPublishEvent)).Return(nil, errDefault).Times(1)
			},
			when: args{stream: oncePublishServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name: "stream send failure",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Append(gomock.Eq(&defaultPublishEvent)).Return(&defaultEvent, nil).Times(1)
			},
			when: args{stream: oncePublishServer(defaultContext, t, errDefault)},
			then: func(err error) {
				assert.Equal(t, errDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			storageMock := mocks.NewMockStorage(controller)
			if tt.given != nil {
				tt.given(storageMock)
			}

			it := api.New(storageMock)
			err := it.Publish(tt.when.stream)
			tt.then(err)
		})
	}
}

func TestAPI_Stream(t *testing.T) {
	type args struct {
		stream spec.Goethe_StreamServer
	}
	tests := []struct {
		name  string
		given func(storage *mocks.MockStorage)
		when  args
		then  func(err error)
	}{
		{
			name: "valid cursor, stream receive ok, storage success, stream send ok",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Read(gomock.Eq(&validCursor)).Return(&validCursor, nil).Times(3)
			},
			when: args{stream: threeCursorStreamServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "client cancelled context",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: contextCancelledStreamServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive eof",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: contextCancelledStreamServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive deadline exceeded",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: statusCodeStreamServer(t, codes.DeadlineExceeded)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive canceled",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: statusCodeStreamServer(t, codes.Canceled)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive unknown error",
			given: func(storage *mocks.MockStorage) {},
			when:  args{stream: streamServer(defaultContext, t, nil, errDefault, nil)},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name:  "invalid cursor: no serviceID",
			given: func(storage *mocks.MockStorage) { storage.EXPECT().Read(gomock.Any()).Times(0) },
			when: args{stream: streamServer(defaultContext, t, &spec.Cursor{
				Topic:        &topic,
				CurrentEvent: &defaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name:  "invalid cursor: no topic",
			given: func(storage *mocks.MockStorage) { storage.EXPECT().Read(gomock.Any()).Times(0) },
			when: args{stream: streamServer(defaultContext, t, &spec.Cursor{
				ServiceId:    serviceID,
				CurrentEvent: &defaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name: "storage unkown failure",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Read(gomock.Eq(&validCursor)).Return(nil, errDefault).Times(1)
			},
			when: args{stream: onceStreamServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name: "storage failure: resource exhausted",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Read(gomock.Eq(&validCursor)).Return(nil, errors.ResourceExhaustedError.New("fail")).Times(1)
			},
			when: args{stream: onceStreamServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.ResourceExhausted, getStatusCode(err))
			},
		},
		{
			name: "storage failure: not found",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Read(gomock.Eq(&validCursor)).Return(nil, errors.NotFound.New("fail")).Times(1)
			},
			when: args{stream: onceStreamServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.NotFound, getStatusCode(err))
			},
		},
		{
			name: "stream send failure",
			given: func(storage *mocks.MockStorage) {
				storage.EXPECT().Read(gomock.Eq(&validCursor)).Return(&validCursor, nil).Times(1)
			},
			when: args{stream: onceStreamServer(defaultContext, t, errDefault)},
			then: func(err error) {
				assert.Equal(t, errDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			storageMock := mocks.NewMockStorage(controller)
			if tt.given != nil {
				tt.given(storageMock)
			}

			it := api.New(storageMock)
			err := it.Stream(tt.when.stream)
			tt.then(err)
		})
	}
}

func contextCancelledPublishServer(t *testing.T) spec.Goethe_PublishServer {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	return publishServer(ctx, t, &validPublishRequest, nil, nil)
}

func contextCancelledStreamServer(t *testing.T) spec.Goethe_StreamServer {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	return streamServer(ctx, t, &validCursor, nil, nil)
}

func statusCodePublishServer(t *testing.T, code codes.Code) *mocks.MockGoethe_PublishServer {
	return publishServer(defaultContext, t, &validPublishRequest, status.New(code, "fail").Err(), nil)
}

func statusCodeStreamServer(t *testing.T, code codes.Code) *mocks.MockGoethe_StreamServer {
	return streamServer(defaultContext, t, &validCursor, status.New(code, "fail").Err(), nil)
}

func publishServer(ctx context.Context, t *testing.T, request *spec.PublishRequest, recvErr error, sendErr error) *mocks.MockGoethe_PublishServer {
	mock := mocks.NewMockGoethe_PublishServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	mock.EXPECT().Recv().Return(request, recvErr).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(sendErr).AnyTimes()
	return mock
}

func streamServer(ctx context.Context, t *testing.T, cursor *spec.Cursor, recvErr error, sendErr error) *mocks.MockGoethe_StreamServer {
	mock := mocks.NewMockGoethe_StreamServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	mock.EXPECT().Recv().Return(cursor, recvErr).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(sendErr).AnyTimes()
	return mock
}

func oncePublishServer(ctx context.Context, t *testing.T, err error) *mocks.MockGoethe_PublishServer {
	mock := mocks.NewMockGoethe_PublishServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	done := false
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.PublishRequest, error) {
		if done {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		done = true
		return &validPublishRequest, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(err).AnyTimes()
	return mock
}

func threeRequestsPublishServer(t *testing.T) *mocks.MockGoethe_PublishServer {
	mock := mocks.NewMockGoethe_PublishServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(defaultContext).AnyTimes()
	cnt := 1
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.PublishRequest, error) {
		if cnt > 3 {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		cnt++
		return &validPublishRequest, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	return mock
}

func onceStreamServer(ctx context.Context, t *testing.T, err error) *mocks.MockGoethe_StreamServer {
	mock := mocks.NewMockGoethe_StreamServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	done := false
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.Cursor, error) {
		if done {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		done = true
		return &validCursor, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(err).AnyTimes()
	return mock
}

func threeCursorStreamServer(t *testing.T) *mocks.MockGoethe_StreamServer {
	mock := mocks.NewMockGoethe_StreamServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(defaultContext).AnyTimes()
	cnt := 1
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.Cursor, error) {
		if cnt > 3 {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		cnt++
		return &validCursor, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	return mock
}

var errDefault = fmt.Errorf("fail")
var topicID = "hello_world"
var topic = spec.Topic{Id: topicID}
var serviceID = "default"
var defaultEvent = spec.Event{
	Id:      "123345",
	Topic:   &topic,
	Payload: []byte("test message"),
}
var defaultPublishEvent = spec.Event{
	Topic:   &topic,
	Payload: []byte("test message"),
}

var validPublishRequest = spec.PublishRequest{
	Topic: &topic,
	Event: &defaultPublishEvent,
}

var validCursor = spec.Cursor{
	Topic:        &topic,
	ServiceId:    serviceID,
	CurrentEvent: &defaultEvent,
}

var defaultContext = context.TODO()

func getStatusCode(err error) codes.Code {
	s, _ := status.FromError(err)
	return s.Code()
}
