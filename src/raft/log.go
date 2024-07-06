package raft

import "errors"

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

// Log manages log entries, its struct look like:
//
//	    snapshot /first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	// compacted log entries.
	snapshotIndex uint64 // the log index of the last compacted log entry.
	snapshotTerm  uint64 // the term of the last compacted log entry.

	// persisted log entries.
	entries []LogEntry

	applied   uint64 // the highest log index of the log entry raft knows that the application has applied.
	committed uint64 // the highest log index of the log entry raft knows that the raft cluster has committed.
}

func makeLog() Log {
	log := Log{
		snapshotIndex: 0,
		snapshotTerm:  0,
		entries:       []LogEntry{},
		applied:       0,
		committed:     0,
	}

	log.entries = append(log.entries, LogEntry{Index: log.snapshotIndex, Term: log.snapshotTerm})
	return log
}

var ErrOutOfBound = errors.New("Index out of bound")

func (log *Log) firstIndex() uint64 {
	return log.entries[0].Index
}

func (log *Log) lastIndex() uint64 {
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) toArrayIndex(idx uint64) uint64 {
	return idx - log.firstIndex()
}

// 拿index回entries[idx].term
func (log *Log) term(idx uint64) (uint64, error) {
	if idx < log.firstIndex() || idx > log.lastIndex() {
		return 0, ErrOutOfBound
	}
	idx = log.toArrayIndex(idx)
	return log.entries[idx].Term, nil
}

func (log *Log) slice(start, end uint64) ([]LogEntry, error) {
	if start <= log.firstIndex() {
		return nil, ErrOutOfBound
	}

	end = min(end, log.lastIndex())

	if start == end {
		return make([]LogEntry, 0), nil
	}
	if start > end {
		panic("Invalid [start, end) index pair")
	}

	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)

	return log.entries[start:end], nil
}

func (log *Log) committedTo(index uint64) {
	log.committed = index
}

func (log *Log) appliedTo(index uint64) {
	log.applied = index
}
