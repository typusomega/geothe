package api_test

//go:generate mockgen -package mocks -destination=./../mocks/mock_consume_server.go github.com/typusomega/goethe/pkg/spec Goethe_ConsumeServer
//go:generate mockgen -package mocks -destination=./../mocks/mock_consumeriterator.go github.com/typusomega/goethe/pkg/api ConsumerIterator
//go:generate mockgen -package mocks -destination=./../mocks/mock_consumer.go github.com/typusomega/goethe/pkg/api Consumer
//go:generate mockgen -package mocks -destination=./../mocks/mock_producer.go github.com/typusomega/goethe/pkg/api Producer

import (
	"context"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/typusomega/goethe/pkg/api"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/testhelpers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAPI_Produce(t *testing.T) {
	type args struct {
		ctx   context.Context
		event *spec.Event
	}
	tests := []struct {
		name  string
		given func(producer *mocks.MockProducer)
		when  args
		then  func(event *spec.Event, err error)
	}{
		{
			name: "valid event, producer success",
			given: func(producer *mocks.MockProducer) {
				producer.EXPECT().Produce(gomock.Eq(&testhelpers.DefaultProduceEvent)).Times(1)
			},
			when: args{ctx: testhelpers.DefaultContext, event: &testhelpers.DefaultProduceEvent},
			then: func(event *spec.Event, err error) {
				assert.Equal(t, codes.OK, testhelpers.GetStatusCode(err))
			},
		},
		{
			name:  "invalid event: no topic",
			given: func(producer *mocks.MockProducer) { producer.EXPECT().Produce(gomock.Any()).Times(0) },
			when:  args{ctx: testhelpers.DefaultContext, event: &spec.Event{Payload: []byte("123")}},
			then: func(event *spec.Event, err error) {
				assert.Equal(t, codes.InvalidArgument, testhelpers.GetStatusCode(err))
			},
		},
		{
			name:  "invalid event: no event content",
			given: func(producer *mocks.MockProducer) { producer.EXPECT().Produce(gomock.Any()).Times(0) },
			when:  args{event: &spec.Event{Topic: &testhelpers.DefaultTopic}},
			then: func(event *spec.Event, err error) {
				assert.Equal(t, codes.InvalidArgument, testhelpers.GetStatusCode(err))
			},
		},
		{
			name: "producer failure",
			given: func(producer *mocks.MockProducer) {
				producer.EXPECT().Produce(gomock.Eq(&testhelpers.DefaultEvent)).Return(nil, testhelpers.ErrDefault).Times(1)
			},
			when: args{event: &testhelpers.DefaultEvent},
			then: func(event *spec.Event, err error) {
				assert.Equal(t, codes.Internal, testhelpers.GetStatusCode(err))
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			producerMock := mocks.NewMockProducer(controller)
			if tt.given != nil {
				tt.given(producerMock)
			}

			it := api.New(producerMock, nil)
			event, err := it.Produce(tt.when.ctx, tt.when.event)
			tt.then(event, err)
		})
	}
}

func TestAPI_Consume(t *testing.T) {
	type args struct {
		stream spec.Goethe_ConsumeServer
	}
	tests := []struct {
		name  string
		given func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator)
		when  args
		then  func(err error)
	}{
		{
			name:  "stream receive eof",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {},
			when:  args{stream: ConsumeServer(testhelpers.DefaultContext, t, &testhelpers.DefaultCursor, io.EOF, nil)},
			then: func(err error) {
				assert.Equal(t, codes.OK, testhelpers.GetStatusCode(err))
			},
		},
		{
			name:  "stream receive deadline exceeded",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {},
			when:  args{stream: statusCodeConsumeServer(t, codes.DeadlineExceeded)},
			then: func(err error) {
				assert.Equal(t, codes.OK, testhelpers.GetStatusCode(err))
			},
		},
		{
			name:  "stream receive canceled",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {},
			when:  args{stream: statusCodeConsumeServer(t, codes.Canceled)},
			then: func(err error) {
				assert.Equal(t, codes.OK, testhelpers.GetStatusCode(err))
			},
		},
		{
			name:  "stream receive unknown error",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {},
			when:  args{stream: ConsumeServer(testhelpers.DefaultContext, t, nil, testhelpers.ErrDefault, nil)},
			then: func(err error) {
				assert.Equal(t, codes.Internal, testhelpers.GetStatusCode(err))
			},
		},
		{
			name: "invalid cursor: no consumer",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Any()).Times(0)
			},
			when: args{stream: ConsumeServer(testhelpers.DefaultContext, t, &spec.Cursor{
				Topic:        &testhelpers.DefaultTopic,
				CurrentEvent: &testhelpers.DefaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, testhelpers.GetStatusCode(err))
			},
		},
		{
			name: "invalid cursor: no topic",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Any()).Times(0)
			},
			when: args{stream: ConsumeServer(testhelpers.DefaultContext, t, &spec.Cursor{
				Consumer:     testhelpers.DefaultConsumer,
				CurrentEvent: &testhelpers.DefaultEvent,
			}, status.New(codes.OK, "").Err(), nil)},
			then: func(err error) {
				assert.Equal(t, codes.InvalidArgument, testhelpers.GetStatusCode(err))
			},
		},
		{
			name: "consumer get iterator failure",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(nil, testhelpers.ErrDefault).Times(1)
			},
			when: args{stream: onceConsumeServer(testhelpers.DefaultContext, t, status.New(codes.OK, "").Err())},
			then: func(err error) {
				assert.Equal(t, codes.Internal, testhelpers.GetStatusCode(err))
			},
		},

		{
			name: "iterator value failure",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Value().Return(nil, testhelpers.ErrDefault).Times(1)
			},
			when: args{stream: onceConsumeServer(testhelpers.DefaultContext, t, testhelpers.ErrDefault)},
			then: func(err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "stream send failure",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Value().Return(&testhelpers.DefaultCursor, nil).Times(1)
			},
			when: args{stream: onceConsumeServer(testhelpers.DefaultContext, t, testhelpers.ErrDefault)},
			then: func(err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "context cancelled",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Value().Return(&testhelpers.DefaultCursor, nil).Times(1)
			},
			when: args{stream: onceConsumeServer(cancelledContext(), t, nil)},
			then: func(err error) {
				assert.Nil(t, err)
			},
		},
		{
			name: "subsequent stream receive failure",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Next().Return(true).Times(3)
				iterator.EXPECT().Value().Return(&testhelpers.DefaultCursor, nil).Times(4)
				consumer.EXPECT().Commit(gomock.Eq(&testhelpers.DefaultCursor)).Times(3)
			},
			when: args{stream: afterThreeSendsFailureConsumeServer(t)},
			then: func(err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "consumer commit failure",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Next().Return(true).Times(2)
				iterator.EXPECT().Value().Return(&testhelpers.DefaultCursor, nil).Times(3)
				consumer.EXPECT().Commit(gomock.Eq(&testhelpers.DefaultCursor)).Return(testhelpers.ErrDefault).Times(2)
			},
			when: args{stream: threeCursorConsumeServer(t, status.New(codes.Canceled, ""))},
			then: func(err error) {
				assert.Nil(t, err)
			},
		},
		{
			name: "no more events",
			given: func(consumer *mocks.MockConsumer, iterator *mocks.MockConsumerIterator) {
				consumer.EXPECT().GetIterator(gomock.Eq(&testhelpers.DefaultCursor)).Return(iterator, nil).Times(1)
				iterator.EXPECT().Value().Return(&testhelpers.DefaultCursor, nil).Times(1)
				iterator.EXPECT().Next().Return(false).Times(1)
				consumer.EXPECT().Commit(gomock.Eq(&testhelpers.DefaultCursor)).Times(1)
			},
			when: args{stream: threeCursorConsumeServer(t, status.New(codes.Canceled, ""))},
			then: func(err error) {
				assert.Equal(t, codes.ResourceExhausted, testhelpers.GetStatusCode(err))
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			consumerMock := mocks.NewMockConsumer(controller)
			consumerIteratorMock := mocks.NewMockConsumerIterator(controller)
			if tt.given != nil {
				tt.given(consumerMock, consumerIteratorMock)
			}

			it := api.New(nil, consumerMock)
			err := it.Consume(tt.when.stream)
			tt.then(err)
		})
	}
}

func cancelledContext() context.Context {
	ctx, cncl := context.WithCancel(context.Background())
	cncl()
	return ctx
}

func statusCodeConsumeServer(t *testing.T, code codes.Code) *mocks.MockGoethe_ConsumeServer {
	return ConsumeServer(testhelpers.DefaultContext, t, &testhelpers.DefaultCursor, status.New(code, "fail").Err(), nil)
}

func ConsumeServer(ctx context.Context, t *testing.T, cursor *spec.Cursor, recvErr error, sendErr error) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(ctx).AnyTimes()
	mock.EXPECT().Recv().Return(cursor, recvErr).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(sendErr).AnyTimes()
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
		return &testhelpers.DefaultCursor, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(err).AnyTimes()
	return mock
}

func threeCursorConsumeServer(t *testing.T, rcvSt *status.Status) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(testhelpers.DefaultContext).AnyTimes()
	cnt := 1
	mock.EXPECT().Recv().DoAndReturn(func() (*spec.Cursor, error) {
		if cnt > 3 {
			return nil, rcvSt.Err()
		}

		cnt++
		return &testhelpers.DefaultCursor, nil
	}).AnyTimes()
	mock.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	return mock
}

func afterThreeSendsFailureConsumeServer(t *testing.T) *mocks.MockGoethe_ConsumeServer {
	mock := mocks.NewMockGoethe_ConsumeServer(gomock.NewController(t))
	mock.EXPECT().Context().Return(testhelpers.DefaultContext).AnyTimes()
	mock.EXPECT().Recv().Return(&testhelpers.DefaultCursor, nil).AnyTimes()
	cnt := 1
	mock.EXPECT().Send(gomock.Any()).DoAndReturn(func(_ *spec.Cursor) error {
		if cnt > 3 {
			return testhelpers.ErrDefault
		}
		cnt++
		return nil
	}).AnyTimes()
	return mock
}
