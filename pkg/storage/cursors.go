package storage

import (
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

// CursorStorage persists and retrieves cursors.
type CursorStorage interface {
	// GetCursorFor retrieves the given cursor.
	GetCursorFor(cursor *spec.Cursor) (*spec.Cursor, error)
	// SaveCursor persists the given cursor.
	SaveCursor(cursor *spec.Cursor) error
}

// NewCursors ctor.
func NewCursors(db LevelDB, idGenerator IDGenerator, keyGenerator KeyGenerator) CursorStorage {
	return &cursorStorage{db: db, ids: idGenerator, keys: keyGenerator}
}

func (it *cursorStorage) GetCursorFor(cursor *spec.Cursor) (*spec.Cursor, error) {
	foundCursor, err := it.db.Get(it.keys.Cursor(cursor), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, errors.NotFound.New("could not find cursor: %v", cursor)
		}
		return nil, err
	}
	return deserializeCursor(foundCursor)
}

func (it *cursorStorage) SaveCursor(cursor *spec.Cursor) error {
	serializedCursor, err := serializeCursor(cursor)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(it.keys.Cursor(cursor), serializedCursor)

	err = it.db.Write(batch, nil)
	if err != nil {
		return errors.Internal.Wrap(err, "could not write cursor: [%v, %v]", cursor.GetConsumer(), cursor.GetTopic().GetId())
	}

	return nil
}

type cursorStorage struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
