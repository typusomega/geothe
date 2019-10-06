package api

import (
	"io"

	"github.com/joomcode/errorx"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
)

type Consumer interface {
	GetIterator(from *spec.Cursor) (ConsumerIterator, error)
	Commit(cursor *spec.Cursor) error
}

type ConsumerIterator interface {
	io.Closer

	// Value returns the current cursor or nil if done.
	Value() (*spec.Cursor, error)

	// Next moves the iterator to the next cursor position.
	// It returns false if the iterator is exhausted.
	Next() bool
}

func NewConsumer(cursors storage.CursorStorage, events storage.EventStorage) Consumer {
	return &consumer{
		cursors: cursors,
		events:  events,
	}
}

func (it *consumer) Commit(cursor *spec.Cursor) error {
	return it.cursors.SaveCursor(cursor)
}

func (it *consumer) GetIterator(from *spec.Cursor) (ConsumerIterator, error) {
	if from.GetTopic() == nil || from.GetTopic().GetId() == "" {
		return nil, errorx.IllegalArgument.New("cursor's topic id must be set")
	}

	cursorToUse := from
	if from.GetCurrentEvent().GetId() == "" {
		cursorFound, err := it.cursors.GetCursorFor(from)
		if err != nil && !errorx.HasTrait(err, errorx.NotFound()) {
			return nil, err
		}

		if err == nil {
			cursorToUse = cursorFound
		}
	}

	inner, err := it.events.GetIterator(cursorToUse.GetCurrentEvent())
	if err != nil {
		return nil, err
	}

	return NewIterator(*from.GetTopic(), from.GetConsumer(), inner, it.cursors), nil
}

func NewIterator(topic spec.Topic, consumer string, inner storage.EventsIterator, cursors storage.CursorStorage) ConsumerIterator {
	return &consumerIterator{
		topic:    topic,
		consumer: consumer,
		inner:    inner,
		cursors:  cursors,
	}
}

func (it *consumerIterator) Close() error {
	return it.inner.Close()
}

func (it *consumerIterator) Next() bool {
	return it.inner.Next()
}

func (it *consumerIterator) Value() (*spec.Cursor, error) {
	nextEvent, err := it.inner.Value()
	if err != nil {
		return nil, err
	}
	return &spec.Cursor{
		Topic:        &it.topic,
		Consumer:     it.consumer,
		CurrentEvent: nextEvent,
	}, nil
}

type consumerIterator struct {
	topic    spec.Topic
	consumer string
	inner    storage.EventsIterator
	cursors  storage.CursorStorage
}

type consumer struct {
	cursors storage.CursorStorage
	events  storage.EventStorage
}
