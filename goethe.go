package main

import (
	"fmt"
	"net"
	"net/http"

	"google.golang.org/grpc"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
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

	db, err := leveldb.OpenFile(cfg.StorageFile, nil)
	if err != nil {
		panic(err)
	}

	eventStore := storage.NewEvents(db, storage.NewIDGenerator(), storage.NewKeyGenerator(), storage.NewMetrics())
	cursorStore := storage.NewCursors(db, storage.NewIDGenerator(), storage.NewKeyGenerator())
	producer := api.NewProducer(eventStore, api.NewMetrics())
	consumer := api.NewConsumer(cursorStore, eventStore)
	apiServer := api.New(producer, consumer)
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
		)),
	)
	spec.RegisterGoetheServer(grpcServer, apiServer)
	grpc_prometheus.Register(grpcServer)

	go startPrometheusServer(cfg)
	mainLog.Infof("started grpc server on '%v'", address)
	if err := grpcServer.Serve(listener); err != nil {
		db.Close()
		mainLog.Fatalf("failed to serve grpc: '%v'", err)
	}
}

func startPrometheusServer(cfg config.Config) {
	mainLog.Infof("starting prometheus listener on '0.0.0.0:%v/metrics'", cfg.PrometheusPort)
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(fmt.Sprintf(":%v", cfg.PrometheusPort), nil)
	mainLog.WithError(err).Fatalf("error while serving prometheus metrics")
}
