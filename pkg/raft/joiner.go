package raft

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/typusomega/goethe/pkg/config"
	"github.com/typusomega/goethe/pkg/spec"
)

func join(ctx context.Context, cfg config.Config) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if joinSuccessful(ctx, cfg) {
				return
			}
			time.Sleep(retryInterval)
		}
	}
}

func joinSuccessful(ctx context.Context, cfg config.Config) bool {
	timeoutCtx, cncl := context.WithTimeout(ctx, retryInterval)
	defer cncl()

	bootstrapNode := cfg.Raft.BootstrapNode
	dial, err := grpc.Dial(bootstrapNode, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Warnf("could not connect to bootstrap cluster '%s' to join cluster", bootstrapNode)
		return false
	}

	client := spec.NewRaftClient(dial)
	if _, err = client.Join(timeoutCtx, &spec.JoinRequest{
		NodeAddress: cfg.Raft.NodeName,
		NodePort:    int32(cfg.Raft.Port),
	}); err != nil {
		logrus.WithError(err).Warnf("could not connect to bootstrap cluster '%s' to join cluster; retrying in %s", bootstrapNode, retryInterval)
		return false
	}

	return true
}

var retryInterval = 1 * time.Second
