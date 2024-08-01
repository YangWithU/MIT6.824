package raft

type RequestVoteArgs struct {
	From         int
	To           int
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	From    int
	To      int
	Term    uint64
	VotedTo int
}

type HeartBeatArgs struct {
	From           int
	To             int
	Term           uint64
	CommittedIndex uint64
}

type HeartBeatReply struct {
	From int
	To   int
	Term uint64
}

type AppendEntriesArgs struct {
	From           int
	To             int
	Term           uint64
	CommittedIndex uint64 // CommittedIndex: 发送方最新commit index
	PrevLogIndex   uint64 //
	PrevLogTerm    uint64
	Entries        []LogEntry
}

type Err int

const (
	Rejected = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	From               int
	To                 int
	Err                Err
	Term               uint64
	ConflictTerm       uint64
	LastLogIndex       uint64
	FirstConflictIndex uint64
	LogLength          uint64
}

// snapshot

type InstallSnapshotArgs struct {
	From               int
	To                 int
	Term               uint64
	Err                Err
	LastLogIndex       uint64
	ConflictTerm       uint64
	FirstConflictIndex uint64
}

type InstallSnapshotReply struct {
	From               int
	To                 int
	Term               uint64
	Err                Err
	LastLogIndex       uint64
	ConflictTerm       uint64
	FirstConflictIndex uint64
}
