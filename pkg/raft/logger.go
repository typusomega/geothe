package raft

import (
	"strings"

	"github.com/sirupsen/logrus"
)

type logger struct {
}

// Write writes the given log line
func (it *logger) Write(b []byte) (int, error) {
	structuredLogger := logrus.WithField("log", "raft")
	input := strings.Trim(string(b), "\n")

	switch input = input[strings.Index(input, "["):]; {
	case strings.HasPrefix(input, debug):
		structuredLogger.Debug(input[raftPrefixLength+len(debug):])
	case strings.HasPrefix(input, info):
		structuredLogger.Info(input[raftPrefixLength+len(info):])
	case strings.HasPrefix(input, WARN):
		structuredLogger.Warn(input[raftPrefixLength+len(WARN):])
	case strings.HasPrefix(input, ERR):
		structuredLogger.Error(input[raftPrefixLength+len(ERR):])
	default:
		structuredLogger.Info(input)
	}
	return len(b), nil
}

const (
	debug            = "[DEBUG]"
	info             = "[INFO]"
	WARN             = "[WARN]"
	ERR              = "[ERROR]"
	raftPrefixLength = 8
)
