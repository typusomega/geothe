package api

import (
	"context"
	"io"

	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type API interface {
	spec.GoetheServer
}

// New creates a new API instance
func New(producer Producer, consumer Consumer) API {
	return &api{producer: producer, consumer: consumer}
}

// Produce produces the given event
func (it *api) Produce(ctx context.Context, event *spec.Event) (*spec.Event, error) {
	logrus.Debugf("received publish request for event: '%v'", event)

	if err := verifyProduceEvent(event); err != nil {
		return nil, err
	}

	producedEvent, err := it.producer.Produce(event)
	if err != nil {
		logrus.WithError(err).Errorf("could not store event: '%v' in topic: '%v'", event, event.GetTopic())
		return nil, status.Newf(codes.Internal, "could not store event: '%v' in topic: '%v'", event, event.GetTopic()).Err()
	}

	logrus.Debug("successfully stored event")
	return producedEvent, nil
}

// Consume is a bidi-stream consumption command
func (it *api) Consume(stream spec.Goethe_ConsumeServer) error {
	cursor, err := stream.Recv()

	exit, err := handleStreamError(err)
	if exit {
		return err
	}

	if err = verifyCursor(cursor); err != nil {
		return err
	}

	iterator, err := it.consumer.GetIterator(cursor)
	if err != nil {
		if errorx.HasTrait(err, errorx.NotFound()) {
			return status.Newf(codes.ResourceExhausted, "no more events in topic: %v", cursor.GetTopic()).Err()
		}
		return status.New(codes.Internal, err.Error()).Err()
	}

	for {
		newCursor, err := iterator.Value()
		if err != nil {
			return err
		}

		err = stream.Send(newCursor)
		if err != nil {
			logrus.WithError(err).Error("could not send response to client")
			return err
		}

		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		cursor, err := stream.Recv()
		exit, err := handleStreamError(err)
		if exit {
			return err
		}

		if err = verifyCursor(cursor); err != nil {
			return err
		}

		if err = it.consumer.Commit(cursor); err != nil {
			logrus.WithError(err).Warn("could not commit cursor")
		}

		if ok := iterator.Next(); !ok {
			return status.Newf(codes.ResourceExhausted, "no more events in topic: %v", cursor.GetTopic()).Err()
		}
	}
}

func verifyProduceEvent(event *spec.Event) error {
	if event.GetTopic().GetId() == "" {
		return status.New(codes.InvalidArgument, "topic id must not be empty").Err()
	}

	if len(event.GetPayload()) == 0 {
		return status.New(codes.InvalidArgument, "event content must not be empty").Err()
	}

	return nil
}

func verifyCursor(cursor *spec.Cursor) error {
	if cursor.GetConsumer() == "" {
		return status.New(codes.InvalidArgument, "cursor's service id must be set").Err()
	}

	if cursor.GetTopic() == nil || cursor.GetTopic().GetId() == "" {
		return status.New(codes.InvalidArgument, "cursor's topic must be set").Err()
	}

	return nil
}

func handleStreamError(err error) (bool, error) {
	if err == io.EOF {
		return true, nil
	}

	requestStatus, _ := status.FromError(err)
	switch requestStatus.Code() {
	case codes.DeadlineExceeded:
		return true, nil
	case codes.Canceled:
		return true, nil
	case codes.OK:
		return false, nil
	default:
		logrus.WithError(err).Error("unexpected error")
		return true, status.New(codes.Internal, err.Error()).Err()
	}
}

// api is the implementation of the grpc api
type api struct {
	producer Producer
	consumer Consumer
}
