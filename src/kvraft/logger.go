package kvraft

import (
	"fmt"
	"log"
	"time"
)

type Logger struct {
	startTime time.Time
}

func (l *Logger) init() {
	l.startTime = time.Now()
}

func (logger *Logger) printf(format string, a ...interface{}) {
	prefix := fmt.Sprintf("%010d", time.Since(logger.startTime).Microseconds())
	format = prefix + format
	res := fmt.Sprintf(format, a...)
	log.Printf(res)
}
