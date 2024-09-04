package raft

import (
	"errors"
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

type SnapShot struct {
	Data  []byte
	Index uint64 // last included index
	Term  uint64 // last included term
}

// Log manages log entries, its struct look like:
//
//	    snapshot /first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	snapShot SnapShot

	hasPendingSnapshot bool

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
		snapShot:           SnapShot{Index: 0, Term: 0, Data: nil},
		hasPendingSnapshot: false,
		entries:            make([]LogEntry, 1),
		applied:            0,
		committed:          0,
	}

	log.setDummyLogEntry()
	return log
}

func (log *Log) setDummyLogEntry() {
	log.entries[0].Index = log.snapShot.Index
	log.entries[0].Term = log.snapShot.Term
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
	if start > end {
		return nil, ErrOutOfBound
	}
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

	return log.clone(log.entries[start:end]), nil
}

func (log *Log) committedTo(index uint64) {
	if index > log.committed {
		oriCommitted := log.committed
		log.committed = index
		log.logger.updateCommitted(oriCommitted)
	}
}

func (log *Log) appliedTo(index uint64) {
	if index > log.applied {
		oriApplied := log.applied
		log.applied = index
		log.logger.updateApplied(oriApplied)
	}
}

// 压缩当前rf.log的entries
// 将old替换为给定snapshot,截断给定snapshot.Index+1前内容
// 设置entry[0] dummy
func (log *Log) toCompactSnapShot(snapshot SnapShot) {
	snapSuffix := make([]LogEntry, 0)
	pos := snapshot.Index + 1
	if pos <= log.lastIndex() {
		pos = log.toArrayIndex(pos)
		snapSuffix = log.entries[pos:]
	}

	log.entries = append(make([]LogEntry, 1), snapSuffix...)
	log.snapShot = snapshot
	log.setDummyLogEntry()

	log.committedTo(log.snapShot.Index)

	log.appliedTo(log.snapShot.Index)

	lastLogIndex := log.lastIndex()
	lastLogTerm, _ := log.term(lastLogIndex)
	log.logger.compactedTo(lastLogIndex, lastLogTerm)
}

func (log *Log) mayCommittedTo(leaderCommittedIndex uint64) {
	if leaderCommittedIndex > log.committed {
		index := min(leaderCommittedIndex, log.lastIndex())
		log.committedTo(index)
	}
}

// index后面的我不要
func (log *Log) truncateSuffix(index uint64) bool {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return false
	}

	index = log.toArrayIndex(index)
	if len(log.entries[index:]) > 0 {
		log.entries = log.entries[:index]
		log.logger.discardEnts(log.entries[index:])
		return true
	}
	return false
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

	log.logger.printf(SNAP, "newCommittedEntries [start=%v, end=%v) LN=%v",
		log.applied+1, log.committed+1, len(log.entries[start:end]))

	if start >= end {
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) cloneSnapShot() SnapShot {
	res := SnapShot{
		Data:  make([]byte, len(log.snapShot.Data)),
		Index: log.snapShot.Index,
		Term:  log.snapShot.Term,
	}
	copy(res.Data, log.snapShot.Data)
	return res
}
