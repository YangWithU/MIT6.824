package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type PeerState int

const (
	Follower PeerState = iota
	Candidate
	Leader
)

const NoneVotedTo = -1
const HeartbeatTimeout = 150 * time.Millisecond
const TickInterval = 50 * time.Millisecond

// A Go object implementing a single Raft peer.
// raft peer定义
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       PeerState
	currentTerm uint64 // 当前任期, latest term server has seen

	votedTo int    // which peer cur peer vote to
	votedMe []bool // true if a peer votes me

	electionTimeout time.Duration
	lastElection    time.Time

	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time

	log          Log
	peerTrackers []PeerTracker // 跟踪每个peer的next index 和 match index.

	applyCh                chan<- ApplyMsg
	hasNewCommittedEntries sync.Cond

	logger Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (currentTerm int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	currentTerm, isLeader = int(rf.currentTerm), rf.state == Leader && !rf.killed()

	return
}

/*
	persist, readPersist: moved to storage.go!
*/

// moved to log_compaction.go

/*
	moved to rpc.go!
*/
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//type RequestVoteArgs struct {
//	// Your data here (2A, 2B).
//}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//type RequestVoteReply struct {
//	// Your data here (2A).
//}

// example RequestVote RPC handler.
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here (2A, 2B).
//}

/*
	moved to leader_election.go!
*/

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (newEntryIndex, newEntryTerm int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == Leader && !rf.killed()
	if !isLeader {
		return 0, 0, false
	}

	// Your code here (2B).

	index := rf.log.lastIndex() + 1
	term := rf.currentTerm
	entry := LogEntry{Index: index, Term: term, Data: command}
	rf.log.append([]LogEntry{entry})
	rf.persist()

	rf.broadcastAppendEntries(true)

	return int(index), int(term), isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 中心函数,选举循环
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		//Debug(dTimer, "ticker() tick!")
		rf.mu.Lock()

		switch rf.state {
		case Follower:
			fallthrough
		case Candidate:

			if rf.isLastElectionTimeout() {
				//Debug(dTimer, "isLastElectionTimeout() rf peer %v election timeout, broadcast request vote become candidate", rf.me)
				rf.logger.elecTimeout()
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}

		case Leader:
			if !rf.isQuorumPeerActive() {
				rf.logger.stepDown()
				rf.becomeFollower(rf.currentTerm)
				break
			}

			forced := false
			if rf.isLastHeartbeatTimeout() {
				rf.logger.beatTimeout()
				forced = true
				rf.resetHeartbeatTimer()
			}
			// 每个heartbeat,再发送一遍appendEntry
			rf.broadcastAppendEntries(forced)
			//rf.resetHeartbeatTimer()
		}

		rf.mu.Unlock()
		time.Sleep(TickInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}

	rf.applyCh = applyCh
	rf.hasNewCommittedEntries = *sync.NewCond(&rf.mu)

	rf.logger = *makeLogger(false, "new_log.txt")
	rf.logger.r = rf

	rf.log = makeLog()
	rf.log.logger = &rf.logger

	// 启动时读取之前存储的状态：term, votedTo, entries, committed, applied
	if rf.persister.RaftStateSize() > 0 || rf.persister.SnapshotSize() > 0 {
		rf.readPersist(rf.persister.ReadRaftState())
		rf.logger.restoreLog()
	} else {
		rf.currentTerm = 0
		rf.votedTo = NoneVotedTo
	}

	rf.state = Follower
	rf.resetElectionTimer()
	rf.logger.stateToFollower(rf.currentTerm)

	rf.peerTrackers = make([]PeerTracker, len(peers))
	rf.resetTrackedIndexes()

	// time
	rf.heartbeatTimeout = HeartbeatTimeout
	rf.resetHeartbeatTimer()

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commiter goroutine to observe new committed entries
	// and send new entries to rf.applyCh
	go rf.committer()

	return rf
}
