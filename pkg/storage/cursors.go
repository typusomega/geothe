package storage

import (
	"github.com/joomcode/errorx"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/typusomega/goethe/pkg/errors"
	"github.com/typusomega/goethe/pkg/spec"
)

type CursorStorage interface {
	GetCursorFor(cursor *spec.Cursor) (*spec.Cursor, error)
	SaveCursor(cursor *spec.Cursor) error
}

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
		return errorx.RejectedOperation.New("could not write cursor: [%v, %v]", cursor.GetConsumer(), cursor.GetTopic().GetId())
	}

	return nil
}

type cursorStorage struct {
	db   LevelDB
	ids  IDGenerator
	keys KeyGenerator
}
