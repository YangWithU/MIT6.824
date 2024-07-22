package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type ulogTopic string

const (
	dClient  ulogTopic = "CLNT"
	dCommit  ulogTopic = "CMIT"
	dDrop    ulogTopic = "DROP"
	dError   ulogTopic = "ERRO"
	dInfo    ulogTopic = "INFO"
	dLeader  ulogTopic = "LEAD"
	dLog     ulogTopic = "LOG1"
	dLog2    ulogTopic = "LOG2"
	dPersist ulogTopic = "PERS"
	dSnap    ulogTopic = "SNAP"
	dTerm    ulogTopic = "TERM"
	dTest    ulogTopic = "TEST"
	dTimer   ulogTopic = "TIMR"
	dTrace   ulogTopic = "TRCE"
	dVote    ulogTopic = "VOTE"
	dWarn    ulogTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		logFile, err := os.OpenFile("log.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		defer logFile.Close()
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(logFile)

		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}
