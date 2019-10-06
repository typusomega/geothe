package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/typusomega/goethe/pkg/client"
	"github.com/typusomega/goethe/pkg/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.AddCommand("produce", "Produce an event", "", &ProduceCommand{opts: &opts})
	if err != nil {
		panic(err)
	}
	_, err = parser.AddCommand("consume", "Consume an event", "", &ConsumeCommand{opts: &opts})
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
}

func (it *Opts) GetClient() (client.Client, error) {
	return client.New(context.TODO(), it.ServerAddress)
}

type ProduceCommand struct {
	Positionals struct {
		EventContent string `positional-arg-name:"EVENT_CONTENT"`
	} `positional-args:"true" required:"true"`
	Topic string `short:"t" long:"topic" description:"Topic to publish to/read from" required:"true"`
	opts  *Opts
}

func (it *ProduceCommand) Execute(args []string) error {
	client, err := it.opts.GetClient()
	if err != nil {
		return err
	}

	event, err := client.Produce(context.Background(), &spec.Topic{Id: it.Topic}, []byte(it.Positionals.EventContent))
	if err != nil {
		return err
	}

	println(fmt.Sprintf("Published: '%v'", event))
	return nil
}

type ConsumeCommand struct {
	Positionals struct {
		LastEvent string `positional-arg-name:"LAST_EVENT"`
	} `positional-args:"true"`
	Topic    string `short:"t" long:"topic" description:"Topic to publish to/read from" required:"true"`
	Consumer string `short:"s" long:"consumer" description:"The consumer to use" default:"default"`
	NoWait   bool   `short:"w" long:"no-wait" description:"Streams all events consecutively without waiting for input"`

	opts *Opts
}

func (it *ConsumeCommand) Execute(args []string) error {
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

	for {
		err = client.ConsumeBlocking(ctx, &spec.Cursor{
			Topic:    &spec.Topic{Id: it.Topic},
			Consumer: it.Consumer,
			CurrentEvent: &spec.Event{
				Id:    it.Positionals.LastEvent,
				Topic: &spec.Topic{Id: it.Topic},
			},
		}, cursors)
		if err != nil {
			if err == io.EOF {
				continue
			}
			status, _ := status.FromError(err)
			switch status.Code() {
			case codes.ResourceExhausted:
				time.Sleep(time.Second)
				continue
			default:
				println(fmt.Sprintf("got status %v with error: %v", status.Code(), status.Err()))
				return err
			}
		}
	}
}
