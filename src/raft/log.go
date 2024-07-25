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

	// apply 后log entry当前最大的log index
	applied uint64

	// commit 后log entry当前最大的log index
	committed uint64

	logger *Logger
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

	end = min(end, log.lastIndex()+1)

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
	oriCommitted := log.committed
	log.committed = index
	log.logger.updateCommitted(oriCommitted)
}

func (log *Log) appliedTo(index uint64) {
	oriApplied := log.applied
	log.applied = index
	log.logger.updateApplied(oriApplied)
}

func (log *Log) compactedTo(index uint64, term uint64) {
	log.snapshotIndex = index
	log.snapshotTerm = term
}

func (log *Log) mayCommittedTo(leaderCommittedIndex uint64) {
	if leaderCommittedIndex > log.committed {
		index := min(leaderCommittedIndex, log.lastIndex())
		log.committedTo(index)
	}
}

// index后面的我不要
func (log *Log) truncateSuffix(index uint64) {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return
	}

	index = log.toArrayIndex(index)

	log.logger.discardEnts(log.entries[index:])

	log.entries = log.entries[:index]
}

func (log *Log) append(entry []LogEntry) {
	log.logger.appendEnts(entry)
	log.entries = append(log.entries, entry...)
}

func (log *Log) clone(entries []LogEntry) []LogEntry {
	res := make([]LogEntry, len(entries))
	copy(res, entries)
	return res
}

// 返回log.entries[log.applied:log.committed]拷贝
func (log *Log) newCommittedEntries() []LogEntry {
	start := log.toArrayIndex(log.applied + 1)
	end := log.toArrayIndex(log.committed + 1)
	if start > end {
		return make([]LogEntry, 0)
	}
	return log.clone(log.entries[start:end])
}
