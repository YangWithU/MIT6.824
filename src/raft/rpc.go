package raft

import "time"

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
	From     int
	To       int
	Term     uint64
	SnapShot SnapShot
}

type InstallSnapshotReply struct {
	From      int
	To        int
	Term      uint64
	Installed bool
}

type MessageType string

const (
	Vote        MessageType = "RequestVote"
	VoteReply   MessageType = "RequestVoteReply"
	Append      MessageType = "AppendEntries"
	AppendReply MessageType = "AppendEntriesReply"
	Snap        MessageType = "InstallSnapshot"
	SnapReply   MessageType = "InstallSnapshotReply"
)

type Message struct {
	Type         MessageType
	From         int    // warning: not used for now.
	Term         uint64 // the term in the PRC args or RPC reply.
	ArgsTerm     uint64 // the term in the RPC args. Used to differ between the term in a RPC reply.
	PrevLogIndex uint64 // used for checking of AppendEntriesReply.
}

// check term stale,
// set follower is message.type=Append or snap.
// return termStale, termChanged
func (rf *Raft) checkTerm(m Message) (ok, termChanged bool) {
	if m.Term < rf.currentTerm {
		return false, false
	}

	// 如果收到新message或来自leader的message: step down
	if m.Term > rf.currentTerm || (m.Type == Append || m.Type == Snap) {
		termChanged = rf.becomeFollower(m.Term)
		return true, termChanged
	}
	return true, false
}

// return true if the raft peer is eligible to handle the message.
func (rf *Raft) checkState(m Message) bool {
	eligible := false

	switch m.Type {
	// only a follower is eligible to handle RequestVote,AppendEntries,InstallSnapshot.
	case Vote:
		eligible = rf.state == Follower
	case Append:
		// reject new log entries if a snapshot is pending to be installed
		// in fact it's ok to append new log entries during a pending snapshot
		// but more reasonable to block
		eligible = rf.state == Follower && !rf.log.hasPendingSnapshot

	case Snap:
		eligible = rf.state == Follower && !rf.log.hasPendingSnapshot

	case VoteReply:
		// currentTerm == ArgsTerm: make sure sender in same term when sending message
		eligible = rf.state == Candidate && rf.currentTerm == m.ArgsTerm

	case AppendReply:
		// check nextIndex ensure reply is exactly response of last AppendEntries RPC
		eligible = rf.state == Leader && rf.currentTerm == m.ArgsTerm &&
			rf.peerTrackers[m.From].nextIndex-1 == m.PrevLogIndex

	case SnapReply:
		// lagBehindSnapshot ensures reply corresponds to last send InstallSnapShot RPC
		eligible = rf.state == Leader && rf.currentTerm == m.ArgsTerm &&
			rf.lagBehindSnapshot(m.From)

	default:
		panic("unexpected message type")
	}

	// follower resetElection timer when received AppendEntries and InstallSnap RPC
	if rf.state == Follower && (m.Type == Append || m.Type == Snap) {
		rf.resetElectionTimer()
	}
	return eligible
}

// 与RPC操作相关函数模板：
//
// m := Message{...} // RPC类型不同,构造时多注意
// ok, termChanged := rf.checkMessage(m)
//
//	if termChanged {
//		reply.Term = rf.term  // this line shall be omitted in reply handlers.
//		defer rf.persist()
//	}
//
//	if !ok {
//		return
//	}
func (rf *Raft) checkMessage(m Message) (ok, termChanged bool) {

	if m.Type == VoteReply || m.Type == AppendReply || m.Type == SnapReply {
		rf.peerTrackers[m.From].lastAck = time.Now()
	}

	ok, termChanged = rf.checkTerm(m)

	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}
