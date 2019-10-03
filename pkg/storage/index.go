package storage

import (
	"time"

	"github.com/typusomega/goethe/pkg/spec"
)

type EventIndex interface {
	GetNearest(timestamp time.Time) (*spec.Event, error)
}

func NewEventIndex(db LevelDB, idGenerator IDGenerator, keyGenerator KeyGenerator) EventIndex {
	return &eventIndex{db: db, ids: idGenerator, keys: keyGenerator}
}

func (it *eventIndex) GetNearest(timestamp time.Time) (*spec.Event, error) {
	return nil, nil
}

type eventIndex struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
