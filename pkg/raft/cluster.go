package raft

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"

	"github.com/typusomega/goethe/pkg/config"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

// A cluster represents a raft cluster.
type Cluster interface {
	AddVoter(address string, port int32) error
	CommitEvent(event *spec.Event) error
	CommitCursor(cursor *spec.Cursor) error
}

// CommitEvent commits the given event in the raft cluster.
func (it *cluster) CommitEvent(event *spec.Event) error {
	log := &spec.RaftLog{
		Event: &spec.RaftLog_EventProduced{EventProduced: &spec.EventProduced{Event: event}},
	}

	marshalledLog, err := proto.Marshal(log)
	if err != nil {
		return errors.Internal.Wrap(err, "could not marshal raft log")
	}

	return it.Raft.Apply(marshalledLog, 5*time.Second).Error()
}

// CommitCursor commits the given cursor in the raft cluster.
func (it *cluster) CommitCursor(cursor *spec.Cursor) error {
	log := &spec.RaftLog{
		Event: &spec.RaftLog_CursorMoved{CursorMoved: &spec.CursorMoved{Cursor: cursor}},
	}

	marshalledLog, err := proto.Marshal(log)
	if err != nil {
		return errors.Internal.Wrap(err, "could not marshal raft log")
	}

	return it.Raft.Apply(marshalledLog, 5*time.Second).Error()
}

// NewCluster creates a new raft cluster instance.
func NewCluster(ctx context.Context, config config.Config, fsm FSM) (Cluster, error) {
	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		return nil, errors.Internal.Wrap(err, "could not create data dir")
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(config.Raft.NodeName)

	logger := &logger{}
	cfg.LogOutput = logger

	notificationChannel := make(chan bool, 1)
	cfg.NotifyCh = notificationChannel

	snapshotStorePath, err := filepath.Abs(filepath.Join(config.DataDir, "raft"))
	if err != nil {
		return nil, err
	}

	snapshotsStore, err := raft.NewFileSnapshotStore(snapshotStorePath, config.Raft.LogRetention, logger)
	if err != nil {
		return nil, errors.Internal.Wrap(err, "could not create raft snapshot store")
	}

	networkTransport, err := raftTransport(fmt.Sprintf(":%d", config.Raft.Port), logger)
	if err != nil {
		return nil, errors.Internal.Wrap(err, "could not instantiate raft network transport")
	}

	node := &cluster{}
	//TODO: replace with leveldb
	raftNode, err := raft.NewRaft(cfg, fsm, raft.NewInmemStore(), raft.NewInmemStore(), snapshotsStore, networkTransport)
	if err != nil {
		return nil, err
	}
	node.Raft = raftNode

	if config.Raft.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      cfg.LocalID,
					Address: networkTransport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	if strings.Trim(config.Raft.BootstrapNode, " \n\t") != "" {
		join(ctx, config)
	}

	return node, nil
}

// AddVoter adds a new voter to the cluster.
func (it *cluster) AddVoter(address string, port int32) error {
	future := it.Raft.AddVoter(raft.ServerID(address), raft.ServerAddress(fmt.Sprintf("%s:%d", address, port)), 0, 0)
	return future.Error()
}

func raftTransport(raftAddr string, log io.Writer) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, log)
	if err != nil {
		return nil, err
	}

	return transport, nil
}

type cluster struct {
	*raft.Raft
}
