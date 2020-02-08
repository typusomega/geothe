package raft

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/typusomega/goethe/pkg/spec"
)

// NewClusterAPI creates a new instance of a ClusterAPI
func NewClusterAPI(node Cluster) *ClusterAPI {
	return &ClusterAPI{node: node}
}

// ClusterAPI is used for raft node discovery
type ClusterAPI struct {
	node Cluster
}

// join joins the requesting node to the raft cluster
func (it *ClusterAPI) Join(ctx context.Context, request *spec.JoinRequest) (*empty.Empty, error) {
	logrus.Infof("received new join request: %s", request)

	e := &empty.Empty{}
	if err := it.node.AddVoter(request.GetNodeAddress(), request.GetNodePort()); err != nil {
		logrus.WithError(err).Errorf("could not add voter: %s", request)
		return e, status.Error(codes.Internal, err.Error())
	}
	return e, nil
}
