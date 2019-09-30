package storage

import (
	"strconv"
	"time"
)

type IDGenerator interface {
	Next() string
}

type UnixNanoIDGenerator struct{}

func (UnixNanoIDGenerator) Next() string {
	return strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}
