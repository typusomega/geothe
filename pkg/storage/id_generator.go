package storage

import (
	"strconv"
	"time"
)

func getNextID() string {
	return strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
}
