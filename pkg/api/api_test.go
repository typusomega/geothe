package api_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_produce_server.go github.com/typusomega/goethe/pkg/spec Goethe_ProduceServer
//go:generate mockgen -package mocks -destination=./../mocks/mock_consume_server.go github.com/typusomega/goethe/pkg/spec Goethe_ConsumeServer
//go:generate mockgen -package mocks -destination=./../mocks/mock_consumer.go github.com/typusomega/goethe/pkg/api Consumer
//go:generate mockgen -package mocks -destination=./../mocks/mock_producer.go github.com/typusomega/goethe/pkg/api Producer

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

func TestAPI_Produce(t *testing.T) {
	type args struct {
		stream spec.Goethe_ProduceServer
	}
	tests := []struct {
		name  string
		given func(producer *mocks.MockProducer)
		when  args
		then  func(err error)
	}{
		{
			name: "valid event, stream receive ok, producer success, stream send ok",
			given: func(producer *mocks.MockProducer) {
				producer.EXPECT().Produce(gomock.Eq(&defaultProduceEvent)).Times(3)
			},
			when: args{stream: threeEventsProduceServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "client cancelled context",
			given: func(producer *mocks.MockProducer) {},
			when:  args{stream: contextCancelledProduceServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive eof",
			given: func(producer *mocks.MockProducer) {},
			when:  args{stream: ProduceServer(defaultContext, t, &validProduceEvent, io.EOF, nil)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive deadline exceeded",
			given: func(producer *mocks.MockProducer) {},
			when:  args{stream: statusCodeProduceServer(t, codes.DeadlineExceeded)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive canceled",
			given: func(producer *mocks.MockProducer) {},
			when:  args{stream: statusCodeProduceServer(t, codes.Canceled)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive unknown error",
			given: func(producer *mocks.MockProducer) {},
			when:  args{stream: ProduceServer(defaultContext, t, nil, errDefault, nil)},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name:  "invalid event: no topic",
			given: func(producer *mocks.MockProducer) { producer.EXPECT().Produce(gomock.Any()).Times(0) },
			when:  args{stream: ProduceServer(defaultContext, t, &spec.Event{Payload: []byte("123")}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name:  "invalid event: no event content",
			given: func(producer *mocks.MockProducer) { producer.EXPECT().Produce(gomock.Any()).Times(0) },
			when:  args{stream: ProduceServer(defaultContext, t, &spec.Event{Topic: &topic}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name: "producer failure",
			given: func(producer *mocks.MockProducer) {
				producer.EXPECT().Produce(gomock.Eq(&defaultProduceEvent)).Return(nil, errDefault).Times(1)
			},
			when: args{stream: onceProduceServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name: "stream send failure",
			given: func(producer *mocks.MockProducer) {
				producer.EXPECT().Produce(gomock.Eq(&defaultProduceEvent)).Return(&defaultEvent, nil).Times(1)
			},
			when: args{stream: onceProduceServer(defaultContext, t, errDefault)},
			then: func(err error) {
				assert.Equal(t, errDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			producerMock := mocks.NewMockProducer(controller)
			consumerMock := mocks.NewMockConsumer(controller)
			if tt.given != nil {
				tt.given(producerMock)
			}

			it := api.New(producerMock, consumerMock)
			err := it.Produce(tt.when.stream)
			tt.then(err)
		})
	}
}

func TestAPI_Consume(t *testing.T) {
	type args struct {
		stream spec.Goethe_ConsumeServer
	}
	tests := []struct {
		name  string
		given func(consumer *mocks.MockConsumer)
		when  args
		then  func(err error)
	}{
		{
			name: "valid cursor, stream receive ok, consumer success, stream send ok",
			given: func(consumer *mocks.MockConsumer) {
				consumer.EXPECT().Consume(gomock.Eq(&validCursor)).Return(&validCursor, nil).Times(3)
			},
			when: args{stream: threeCursorConsumeServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "client cancelled context",
			given: func(consumer *mocks.MockConsumer) {},
			when:  args{stream: contextCancelledConsumeServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive eof",
			given: func(consumer *mocks.MockConsumer) {},
			when:  args{stream: contextCancelledConsumeServer(t)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive deadline exceeded",
			given: func(consumer *mocks.MockConsumer) {},
			when:  args{stream: statusCodeConsumeServer(t, codes.DeadlineExceeded)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive canceled",
			given: func(consumer *mocks.MockConsumer) {},
			when:  args{stream: statusCodeConsumeServer(t, codes.Canceled)},
			then: func(err error) {
				assert.Equal(t, codes.OK, getStatusCode(err))
			},
		},
		{
			name:  "stream receive unknown error",
			given: func(consumer *mocks.MockConsumer) {},
			when:  args{stream: ConsumeServer(defaultContext, t, nil, errDefault, nil)},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name:  "invalid cursor: no consumer",
			given: func(consumer *mocks.MockConsumer) { consumer.EXPECT().Consume(gomock.Any()).Times(0) },
			when: args{stream: ConsumeServer(defaultContext, t, &spec.Cursor{
				Topic:        &topic,
				CurrentEvent: &defaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name:  "invalid cursor: no topic",
			given: func(consumer *mocks.MockConsumer) { consumer.EXPECT().Consume(gomock.Any()).Times(0) },
			when: args{stream: ConsumeServer(defaultContext, t, &spec.Cursor{
				Consumer:     consumer,
				CurrentEvent: &defaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, getStatusCode(err))
			},
		},
		{
			name: "consumer unkown failure",
			given: func(consumer *mocks.MockConsumer) {
				consumer.EXPECT().Consume(gomock.Eq(&validCursor)).Return(nil, errDefault).Times(1)
			},
			when: args{stream: onceConsumeServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.Internal, getStatusCode(err))
			},
		},
		{
			name: "consumer failure: resource exhausted",
			given: func(consumer *mocks.MockConsumer) {
				consumer.EXPECT().Consume(gomock.Eq(&validCursor)).Return(nil, errors.ResourceExhaustedError.New("fail")).Times(1)
			},
			when: args{stream: onceConsumeServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.ResourceExhausted, getStatusCode(err))
			},
		},
		{
			name: "consumer failure: not found",
			given: func(consumer *mocks.MockConsumer) {
				consumer.EXPECT().Consume(gomock.Eq(&validCursor)).Return(nil, errors.NotFound.New("fail")).Times(1)
			},
			when: args{stream: onceConsumeServer(defaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.NotFound, getStatusCode(err))
			},
		},
		{
			name: "stream send failure",
			given: func(consumer *mocks.MockConsumer) {
				consumer.EXPECT().Consume(gomock.Eq(&validCursor)).Return(&validCursor, nil).Times(1)
			},
			when: args{stream: onceConsumeServer(defaultContext, t, errDefault)},
			then: func(err error) {
				assert.Equal(t, errDefault, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			producerMock := mocks.NewMockProducer(controller)
			consumerMock := mocks.NewMockConsumer(controller)
			if tt.given != nil {
				tt.given(consumerMock)
			}

			it := api.New(producerMock, consumerMock)
			err := it.Consume(tt.when.stream)
			tt.then(err)
		})
	}
}

func contextCancelledProduceServer(t *testing.T) spec.Goethe_ProduceServer {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	return ProduceServer(ctx, t, &validProduceEvent, nil, nil)
}

func contextCancelledConsumeServer(t *testing.T) spec.Goethe_ConsumeServer {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	return ConsumeServer(ctx, t, &validCursor, nil, nil)
}

func statusCodeProduceServer(t *testing.T, code codes.Code) *mocks.MockGoethe_ProduceServer {
	return ProduceServer(defaultContext, t, &validProduceEvent, status.New(code, "fail").Err(), nil)
}

func statusCodeConsumeServer(t *testing.T, code codes.Code) *mocks.MockGoethe_ConsumeServer {
	return ConsumeServer(defaultContext, t, &validCursor, status.New(code, "fail").Err(), nil)
}

func ProduceServer(ctx context.Context, t *testing.T, request *spec.Event, recvErr error, sendErr error) *mocks.MockGoethe_ProduceServer {
	mock := mocks.NewMockGoethe_ProduceServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	mock.EXPECT().Recv().Return(request, recvErr).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(sendErr).AnyTimes()
	return mock
}

func ConsumeServer(ctx context.Context, t *testing.T, cursor *spec.Cursor, recvErr error, sendErr error) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	mock.EXPECT().Recv().Return(cursor, recvErr).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(sendErr).AnyTimes()
	return mock
}

func onceProduceServer(ctx context.Context, t *testing.T, err error) *mocks.MockGoethe_ProduceServer {
	mock := mocks.NewMockGoethe_ProduceServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	done := false
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.Event, error) {
		if done {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		done = true
		return &validProduceEvent, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(err).AnyTimes()
	return mock
}

func threeEventsProduceServer(t *testing.T) *mocks.MockGoethe_ProduceServer {
	mock := mocks.NewMockGoethe_ProduceServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(defaultContext).AnyTimes()
	cnt := 1
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.Event, error) {
		if cnt > 3 {
			return nil, status.New(codes.Canceled, "done").Err()
		}

		cnt++
		return &validProduceEvent, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	return mock
}

func onceConsumeServer(ctx context.Context, t *testing.T, err error) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
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

func threeCursorConsumeServer(t *testing.T) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
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
var consumer = "default"
var defaultEvent = spec.Event{
	Id:      "123345",
	Topic:   &topic,
	Payload: []byte("test message"),
}
var defaultProduceEvent = spec.Event{
	Topic:   &topic,
	Payload: []byte("test message"),
}

var validProduceEvent = defaultProduceEvent

var validCursor = spec.Cursor{
	Topic:        &topic,
	Consumer:     consumer,
	CurrentEvent: &defaultEvent,
}

var defaultContext = context.TODO()

func getStatusCode(err error) codes.Code {
	s, _ := status.FromError(err)
	return s.Code()
}
