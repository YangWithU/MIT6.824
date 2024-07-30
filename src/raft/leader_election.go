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

func (rf *Raft) updateTerm(term uint64) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.persist()
	}
}

func (rf *Raft) updateVoteTo(voteTo int) {
	if rf.votedTo != voteTo {
		rf.votedTo = voteTo
		rf.persist()
	}
}

// 初始化votedMe和votedTo状态
func (rf *Raft) resetVote() {
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedMe[rf.me] = true
	rf.updateVoteTo(NoneVotedTo)
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
	//Debug(dVote, "becomeCandidate(): now %v becomeCandidate, and vote to itself", rf.me)

	rf.updateTerm(rf.currentTerm + 1)
	rf.logger.stateToCandidate()

	rf.state = Candidate
	rf.resetVote()
	rf.updateVoteTo(rf.me)
}

// 回退follower, 更新自己term到最新, 设置voteTo=None
func (rf *Raft) becomeFollower(term uint64, isForced bool) {
	if isForced || rf.currentTerm < term {
		oldTerm := rf.currentTerm
		rf.updateTerm(term)
		if rf.state != Follower {
			rf.state = Follower
			rf.logger.stateToFollower(oldTerm) // should place back of rf.state & rf.currentTerm
		}
		rf.resetVote()
	}

	// 刷新自己的时间记录
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.logger.stateToLeader()
	rf.resetTrackedIndexes()
	rf.state = Leader
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
		rf.logger.recvVoteQuorum(uint64(count))
	}
	//if res {
	//	Debug(dVote, "isReceivedMajority(): %v has majority vote", rf.me)
	//} else {
	//	Debug(dVote, "isReceivedMajority(): %v no majority vote, fallback to candidate", rf.me)
	//}

	return res
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Debug(dVote, "handleRequestVoteReply(): args: %v, reply %v", args, reply)
	rf.logger.recvRVOTRes(reply)

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.currentTerm {
		//Debug(dVote, "handleRequestVoteReply() %v election failed, become follower", rf.me)
		rf.becomeFollower(reply.Term, false)
		return
	}

	// 判断能不能选我
	if args.Term != reply.Term || rf.state != Candidate {
		return
	}
	if reply.VotedTo == rf.me {
		rf.votedMe[reply.From] = true
		if rf.isReceivedMajority() {
			//Debug(dVote, "handleRequestVoteReply() %v become leader", rf.me)
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) sendRequestVoteAndHandle(args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(args, &reply)
	}
}

// 给所有peer发RequestVoteArg
func (rf *Raft) broadcastRequestVote() {
	rf.logger.bcastRVOT()
	for idx := range rf.peers {
		if idx != rf.me {
			args := rf.makeRequestVoteArgs(idx)
			//Debug(dVote, "broadcastRequestVote() %v sent %v RequestVote", rf.me, args.To)
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

func (rf *Raft) otherMoreUpToDate(candLastLogIndex, candLastLogTerm uint64) bool {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	return candLastLogTerm > candLastLogTerm ||
		(candLastLogTerm == lastLogTerm && candLastLogIndex > lastLogIndex)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvRVOT(args)

	// default values
	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.currentTerm
	reply.VotedTo = rf.votedTo

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		// 此时修改peer的votedTo = None
		rf.becomeFollower(args.Term, false)
	}

	// 我也candidate,但request我的candidate的LastLogIndex比我的大
	if rf.state == Candidate && rf.otherMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 我选择退出
		rf.becomeFollower(args.Term, true)
	}

	// 判断能否成为 Leader
	if (rf.votedTo == NoneVotedTo || rf.votedTo == args.From) &&
		rf.checkCandidateLogNewer(args.LastLogIndex, args.LastLogTerm) {

		// 接收到candidate发来的voteArg就重置自己的electionTimer保证不重复竞争
		rf.resetElectionTimer()
		rf.updateVoteTo(args.From)
		reply.VotedTo = args.From
		rf.logger.voteTo(args.From)
	} else {
		lastLogIndex := rf.log.lastIndex()
		lastLogTerm, _ := rf.log.term(lastLogIndex)
		rf.logger.rejectVoteTo(args.From, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)
	}

	reply.Term = rf.currentTerm
}
