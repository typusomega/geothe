package storage

import (
	"strconv"
	"time"
)

// IDGenerator is used to create sortable and collision free IDs for events.
type IDGenerator interface {
	// Next generates the next sortable and collision free event ID.
	Next() string
}

// NewIDGenerator ctor.
func NewIDGenerator() IDGenerator {
	return unixNanoIDGenerator{}
}

// unixNanoIDGenerator uses unix nano timestamps as ID
type unixNanoIDGenerator struct{}

func (unixNanoIDGenerator) Next() string {
	return strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}
