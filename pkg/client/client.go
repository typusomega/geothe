package client

import (
	"context"

	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc"
)

type Client interface {
	Produce(ctx context.Context, topic *spec.Topic, eventContent []byte) error
	ConsumeNext(ctx context.Context, cursor *spec.Cursor) (*spec.Cursor, error)
	ConsumeBlocking(ctx context.Context, cursor *spec.Cursor, out chan *spec.Cursor) error
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

func (it *client) Produce(ctx context.Context, topic *spec.Topic, eventContent []byte) error {
	publishStream, err := it.service.Produce(ctx)
	if err != nil {
		return err
	}

	err = publishStream.Send(&spec.Event{Topic: topic, Payload: eventContent})
	if err != nil {
		return err
	}

	_, err = publishStream.Recv()
	if err != nil {
		return err
	}

	return nil
}

func (it *client) ConsumeNext(ctx context.Context, cursor *spec.Cursor) (*spec.Cursor, error) {
	readStream, err := it.service.Consume(ctx)
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

func (it *client) ConsumeBlocking(ctx context.Context, startCursor *spec.Cursor, out chan *spec.Cursor) error {
	readStream, err := it.service.Consume(ctx)
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
				return err
			}

			lastCursor = newCursor
			out <- newCursor
		}
	}
}
