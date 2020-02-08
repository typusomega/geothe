package raft

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

// NewFSM creates a new FSM instance
func NewFSM(cursorStore storage.CursorStorage, eventStore storage.EventStorage) FSM {
	return &fsm{cursors: cursorStore, events: eventStore}
}

// FSM represents the finite state machine handling the node's state.
type FSM interface {
	raft.FSM
}

type fsm struct {
	mutex   sync.Mutex
	cursors storage.CursorStorage
	events  storage.EventStorage
}

// Apply is invoked on changes in the raft cluster.
// Each change is persisted in the cursor/event store.
func (it *fsm) Apply(raftLog *raft.Log) interface{} {
	logrus.Debug("received apply command")
	it.mutex.Lock()
	defer it.mutex.Unlock()

	log := &spec.RaftLog{}
	if err := proto.Unmarshal(raftLog.Data, log); err != nil {
		return errors.Internal.Wrap(err, "could not unmarshal raft log")
	}

	switch log.Event.(type) {
	case *spec.RaftLog_EventProduced:
		return it.eventProduced(log.GetEventProduced())
	case *spec.RaftLog_CursorMoved:
		return it.cursorMoved(log.GetCursorMoved())
	default:
		return errors.InvalidArgument.New("could not handle unknown log type")
	}
}

func (it *fsm) eventProduced(log *spec.EventProduced) error {
	_, err := it.events.Append(log.GetEvent())
	if err != nil {
		logrus.WithError(err).Error("could not handle 'eventProduced'")
	}
	return err
}

func (it *fsm) cursorMoved(log *spec.CursorMoved) error {
	err := it.cursors.SaveCursor(log.GetCursor())
	if err != nil {
		logrus.WithError(err).Error("could not handle 'cursorMoved'")
	}
	return err
}

// Snapshot creates a new snapshot of the fsm.
func (it *fsm) Snapshot() (raft.FSMSnapshot, error) {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	logrus.Info("received Snapshot")
	return &snapShot{}, nil
}

// Restore restores the fsm with the given snapshot.
func (it *fsm) Restore(reader io.ReadCloser) error {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	defer func() {
		if err := reader.Close(); err != nil {
			logrus.WithError(err).Error("could not close restore input reader")
		}
	}()

	all, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	logrus.Infof("received restore: %v", all)
	return nil
}

type snapShot struct {
}

// Persist persists the snapshot's state in the given sink.
func (it *snapShot) Persist(sink raft.SnapshotSink) error {
	logrus.Infof("received Persist: %v", sink)
	return nil
}

// Release releases the current snapshotting process.
func (it *snapShot) Release() {
	logrus.Info("received release")
}
