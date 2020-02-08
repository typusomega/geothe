package api

import (
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/metrics"
	"github.com/typusomega/goethe/pkg/raft"
	"github.com/typusomega/goethe/pkg/spec"
)

// Producer proxies producer capabilities.
type Producer interface {
	// Produce produces the given event.
	Produce(event *spec.Event) (*spec.Event, error)
}

// NewProducer ctor.
func NewProducer(cluster raft.Cluster, metrics metrics.Metrics) Producer {
	return &producer{cluster: cluster, metrics: metrics}
}

func (it *producer) Produce(event *spec.Event) (*spec.Event, error) {
	if err := it.cluster.CommitEvent(event); err != nil {
		it.metrics.EventProductionFailed(event.GetTopic().GetId())
		return nil, errors.Internal.Wrap(err, "could not commit event in raft cluster")
	}
	it.metrics.EventProduced(event.GetTopic().GetId())
	return event, nil
}

type producer struct {
	metrics metrics.Metrics
	cluster raft.Cluster
}
