package raft

import (
	"math/rand"
	"time"
)

const baseElectionTimeout = 300

// 更新rf.electionTimeout和rf.lastElection到最新，与Timeout同步
func (rf *Raft) resetElectionTimer() {
	curTimeout := baseElectionTimeout + rand.Int63n(baseElectionTimeout)
	rf.electionTimeout = time.Duration(curTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) isLastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

// 初始化votedMe和votedTo状态
func (rf *Raft) resetVote() {
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedMe[rf.me] = true
	rf.votedTo = NoneVotedTo
}

func (rf *Raft) makeRequestVoteArgs(to int) *RequestVoteArgs {
	lastLogTerm, _ := rf.log.term(rf.log.lastIndex())
	return &RequestVoteArgs{
		From:         rf.me,
		To:           to,
		Term:         rf.currentTerm,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  lastLogTerm,
	}
}

// 一开始，所有人的 currentTerm 都是0
// 当一个 raft peer 尝试成为 Candidate 时，会递增自己的 currentTerm 给自己投票
// 如果一个 peer 收到更高的Term，会更新自己的 Term 并转换成 Follower
// 一个节点被选成 Leader，它的 currentTerm 不变，向 Follower 发 heartbeat
func (rf *Raft) becomeCandidate() {
	Debug(dVote, "becomeCandidate(): now %v becomeCandidate, and vote to itself", rf.me)
	rf.state = Candidate
	rf.currentTerm++
	rf.resetVote()
	rf.votedTo = rf.me
}

// 回退follower, 更新自己term到最新, 设置voteTo=None
func (rf *Raft) becomeFollower(term uint64) {
	if rf.currentTerm < term || rf.state == Candidate {
		rf.state = Follower
		rf.currentTerm = term
		rf.resetVote()
	}

	// 刷新自己的时间记录
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.resetPeerTrackers()
}

func (rf *Raft) isReceivedMajority() bool {
	count := 0
	for _, v := range rf.votedMe {
		if v {
			count++
		}
	}
	res := count*2 > len(rf.peers)
	if res {
		Debug(dVote, "isReceivedMajority(): %v has majority vote", rf.me)
	} else {
		Debug(dVote, "isReceivedMajority(): %v no majority vote, fallback to candidate", rf.me)
	}
	return res
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "handleRequestVoteReply(): args: %v, reply %v", args, reply)

	if reply.Term > rf.currentTerm {
		Debug(dVote, "handleRequestVoteReply() %v election failed, become follower", rf.me)
		rf.becomeFollower(reply.Term)
	}

	// 判断能不能选我
	if args.Term == reply.Term && rf.state == Candidate && reply.VotedTo == rf.me {
		rf.votedMe[reply.From] = true
		if rf.isReceivedMajority() {
			Debug(dVote, "handleRequestVoteReply() %v become leader", rf.me)
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) sendRequestVoteAndHandle(args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, reply); ok {
		rf.handleRequestVoteReply(args, reply)
	}
}

// 给所有peer发RequestVoteArg
func (rf *Raft) broadcastRequestVote() {
	for idx := range rf.peers {
		if idx != rf.me {
			args := rf.makeRequestVoteArgs(idx)
			Debug(dVote, "broadcastRequestVote() %v sent %v RequestVote", rf.me, args.To)
			go rf.sendRequestVoteAndHandle(args)
		}
	}
}

func (rf *Raft) checkCandidateLogNewer(candLastLogIndex, candLastLogTerm uint64) bool {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	return candLastLogTerm > lastLogTerm ||
		(candLastLogTerm == lastLogTerm && candLastLogIndex >= lastLogIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "RequestVote(): %v now handle RequestVoteArgs from %v", rf.me, args.From)

	// default values
	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.currentTerm
	reply.VotedTo = rf.votedTo

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dInfo, "RequestVote(): %v now become follower of %v", rf.me, args.From)
		rf.becomeFollower(args.Term)
	}

	// 判断能否成为 Leader
	if (rf.votedTo == NoneVotedTo || rf.votedTo == args.From) &&
		rf.checkCandidateLogNewer(args.LastLogIndex, args.LastLogTerm) {
		Debug(dInfo, "RequestVote(): %v now voted to %v", rf.me, args.From)
		rf.votedTo = args.From
		reply.VotedTo = args.From
	}

	reply.Term = rf.currentTerm
}
