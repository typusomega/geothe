package api

import (
	"github.com/joomcode/errorx"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

type Consumer interface {
	Consume(cursor *spec.Cursor) (*spec.Cursor, error)
}

func NewConsumer(cursors storage.CursorStorage, events storage.EventStorage) Consumer {
	return &consumer{
		cursors: cursors,
		events:  events,
	}
}

func (it *consumer) Consume(cursor *spec.Cursor) (*spec.Cursor, error) {
	if cursor.GetTopic() == nil || cursor.GetTopic().GetId() == "" {
		return nil, errorx.IllegalArgument.New("cursor's topic id must be set")
	}

	cursorToUse := cursor
	if cursor.GetCurrentEvent().GetId() == "" {
		cursorFound, err := it.cursors.GetCursorFor(cursor)
		if err != nil && !errorx.HasTrait(err, errorx.NotFound()) {
			return nil, err
		}
		if err == nil {
			cursorToUse = cursorFound
		}
	}

	newCursor, err := it.events.Read(cursorToUse)
	if err != nil {
		return nil, err
	}

	err = it.cursors.SaveCursor(newCursor)
	if err != nil {
		return newCursor, err
	}

	return newCursor, err
}

type consumer struct {
	cursors storage.CursorStorage
	events  storage.EventStorage
}
