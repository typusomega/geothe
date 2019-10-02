package api

import (
	"io"

	"github.com/joomcode/errorx"
	"github.com/sirupsen/logrus"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// New creates a new API instance
func New(producer Producer, consumer Consumer) *API {
	return &API{producer: producer, consumer: consumer}
}

// Publish is a bidi-stream publish command
func (it *API) Publish(stream spec.Goethe_PublishServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			request, err := stream.Recv()
			exit, err := handleStreamError(err)
			if exit {
				return err
			}

			logrus.Debugf("received publish request for topic: '%v' for event: '%v'", request.GetTopic(), request.GetEvent())

			if err = verifyPublishRequest(request); err != nil {
				return err
			}

			event, err := it.producer.Publish(request.GetEvent())
			if err != nil {
				logrus.WithError(err).Errorf("could not store event: '%v' in topic: '%v'", request.GetEvent(), request.GetTopic())
				return status.Newf(codes.Internal, "could not store event: '%v' in topic: '%v'", request.GetEvent(), request.GetTopic()).Err()
			}

			err = stream.Send(&spec.PublishResponse{Topic: request.GetTopic(), Event: event})
			if err != nil {
				logrus.WithError(err).Error("could not send response to client")
				return err
			}

			logrus.Debug("successfully stored event")
		}
	}
}

// Stream is a bidi-stream read command
func (it *API) Stream(stream spec.Goethe_StreamServer) error {
	for {
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

		newCursor, err := it.consumer.Stream(cursor)
		if err = handleReadError(newCursor, err); err != nil {
			return err
		}

		err = stream.Send(newCursor)
		if err != nil {
			logrus.WithError(err).Error("could not send response to client")
			return err
		}
	}
}

func verifyPublishRequest(request *spec.PublishRequest) error {
	if request.GetTopic().GetId() == "" {
		return status.New(codes.InvalidArgument, "topic id must not be empty").Err()
	}

	if len(request.GetEvent().GetPayload()) == 0 {
		return status.New(codes.InvalidArgument, "event content must not be empty").Err()
	}

	return nil
}

func verifyCursor(cursor *spec.Cursor) error {
	if cursor.GetServiceId() == "" {
		return status.New(codes.InvalidArgument, "cursor's service id must be set").Err()
	}

	if cursor.GetTopic() == nil || cursor.GetTopic().GetId() == "" {
		return status.New(codes.InvalidArgument, "cursor's topic must be set").Err()
	}

	return nil
}

func handleReadError(cursor *spec.Cursor, err error) error {
	if err != nil {
		if errorx.HasTrait(err, errorx.NotFound()) {
			logrus.WithError(err).Info("client cursor not found")
			return status.New(codes.NotFound, err.Error()).Err()
		}
		if errorx.HasTrait(err, errors.ResourceExhausted()) {
			logrus.WithError(err).Debugf("service '%v' exhausted topic '%v'", cursor.GetServiceId(), cursor.GetTopic())
			return status.New(codes.ResourceExhausted, err.Error()).Err()
		}
		return status.New(codes.Internal, err.Error()).Err()
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

// API is the implementation of the grpc API
type API struct {
	producer Producer
	consumer Consumer
}
