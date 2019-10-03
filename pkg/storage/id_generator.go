package storage

import (
	"strconv"
	"time"
)

type IDGenerator interface {
	Next() string
}

func NewIDGenerator() IDGenerator {
	return UnixNanoIDGenerator{}
}

type UnixNanoIDGenerator struct{}

func (UnixNanoIDGenerator) Next() string {
	return strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}
