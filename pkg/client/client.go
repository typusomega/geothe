package client

import (
	"context"
	"io"

	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client interface {
	Publish(ctx context.Context, topic *spec.Topic, eventContent []byte) error
	ReadNext(ctx context.Context, cursor *spec.Cursor) (*spec.Cursor, error)
	ReadBlocking(ctx context.Context, cursor *spec.Cursor, out chan *spec.Cursor) error
}

func New(ctx context.Context, serverAddress string) (Client, error) {
	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	serviceClient := spec.NewGoetheClient(conn)

	return &client{service: serviceClient}, nil
}

type client struct {
	service spec.GoetheClient
}

func (it *client) Publish(ctx context.Context, topic *spec.Topic, eventContent []byte) error {
	publishStream, err := it.service.Publish(ctx)
	if err != nil {
		return err
	}

	err = publishStream.Send(&spec.PublishRequest{Topic: topic, Event: &spec.Event{Topic: topic, Payload: eventContent}})
	if err != nil {
		return err
	}

	_, err = publishStream.Recv()
	if err != nil {
		return err
	}

	return nil
}

func (it *client) ReadNext(ctx context.Context, cursor *spec.Cursor) (*spec.Cursor, error) {
	readStream, err := it.service.Stream(ctx)
	if err != nil {
		return nil, err
	}

	err = readStream.Send(cursor)
	if err != nil {
		return nil, err
	}

	newCursor, err := readStream.Recv()
	if err != nil {
		return nil, err
	}

	return newCursor, nil
}

func (it *client) ReadBlocking(ctx context.Context, startCursor *spec.Cursor, out chan *spec.Cursor) error {
	readStream, err := it.service.Stream(ctx)
	if err != nil {
		return err
	}

	lastCursor := startCursor
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			err = readStream.Send(lastCursor)
			if err != nil {
				return err
			}

			newCursor, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					continue
				}

				status, _ := status.FromError(err)
				switch status.Code() {
				case codes.ResourceExhausted:
					_ = readStream.CloseSend()
					readStream, err = it.service.Stream(ctx)
					if err != nil {
						return err
					}
					continue

				default:
					return err
				}
			}

			if err != nil {
				return err
			}

			lastCursor = newCursor
			out <- newCursor
		}
	}
}
