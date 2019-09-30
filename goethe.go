package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/sirupsen/logrus"
	"github.com/typusomega/goethe/pkg/api"
	"github.com/typusomega/goethe/pkg/config"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

var mainLog = logrus.WithField("name", "main")

func main() {
	cfg := config.Get()

	address := fmt.Sprintf(":%d", cfg.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		mainLog.Fatalf("could not create listener on address: %s", address)
	}

	storage, err := storage.New(cfg.StorageFile)
	if err != nil {
		panic(err)
	}

	apiServer := api.New(storage)
	grpcServer := grpc.NewServer()
	spec.RegisterGoetheServer(grpcServer, apiServer)

	mainLog.Infof("started grpc server on '%v'", address)
	if err := grpcServer.Serve(listener); err != nil {
		storage.Close()
		mainLog.Fatalf("failed to serve grpc: '%v'", err)
	}
}
