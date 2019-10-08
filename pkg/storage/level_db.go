package storage

import (
	"io"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDB interface is only used for testing purposes.
type LevelDB interface {
	io.Closer
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}
