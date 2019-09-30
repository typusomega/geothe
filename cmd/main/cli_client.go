package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/typusomega/goethe/pkg/client"
	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc/status"
)

func main() {
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.AddCommand("publish", "Publish an event", "", &PublishCommand{opts: &opts})
	if err != nil {
		panic(err)
	}
	_, err = parser.AddCommand("read", "Read an event", "", &ReadCommand{opts: &opts})
	if err != nil {
		panic(err)
	}
	_, err = parser.Parse()
	if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}

var opts Opts

type Opts struct {
	ServerAddress string `short:"a" long:"server-address" description:"Address of the Goethe grpc server (e.g. 'localhost:1337')" default:"localhost:1337"`
	Topic         string `short:"t" long:"topic" description:"Topic to publish to/read from" required:"true"`
}

func (it *Opts) GetClient() (client.Client, error) {
	return client.New(context.TODO(), it.ServerAddress)
}

type PublishCommand struct {
	Positionals struct {
		EventContent string `positional-arg-name:"EVENT_CONTENT"`
	} `positional-args:"true" required:"true"`
	opts *Opts
}

func (it *PublishCommand) Execute(args []string) error {
	client, err := it.opts.GetClient()
	if err != nil {
		return err
	}

	return client.Publish(context.Background(), &spec.Topic{Id: it.opts.Topic}, []byte(it.Positionals.EventContent))
}

type ReadCommand struct {
	Positionals struct {
		LastEvent string `positional-arg-name:"LAST_EVENT"`
	} `positional-args:"true"`

	ServiceID string `short:"s" long:"service-id" description:"The service ID to use" default:"default"`
	NoWait    bool   `short:"w" long:"no-wait" description:"Streams all events consecutively without waiting for input"`

	opts *Opts
}

func (it *ReadCommand) Execute(args []string) error {
	client, err := it.opts.GetClient()
	if err != nil {
		return err
	}

	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()

	cursors := make(chan *spec.Cursor)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case cursor := <-cursors:
				println(fmt.Sprintf("%v", cursor))
			}
		}
	}()

	err = client.ReadBlocking(ctx, &spec.Cursor{
		Topic:     &spec.Topic{Id: it.opts.Topic},
		ServiceId: it.ServiceID,
		CurrentEvent: &spec.Event{
			Id: it.Positionals.LastEvent,
		},
	}, cursors)

	if err != nil {
		status, _ := status.FromError(err)
		println(fmt.Sprintf("got status %v with error: %v", status.Code(), status.Err()))
		return err
	}

	return nil
}
