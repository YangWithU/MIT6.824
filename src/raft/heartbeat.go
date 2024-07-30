package raft

import "time"

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) isLastHeartbeatTimeout() bool {
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) makeHeartBeatArgs(to int) *HeartBeatArgs {
	return &HeartBeatArgs{
		From:           rf.me,
		To:             to,
		Term:           rf.currentTerm,
		CommittedIndex: rf.log.committed,
	}
}

func (rf *Raft) handleHeartbeatReply(reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}
}

func (rf *Raft) sendHeartbeatAndHandle(args *HeartBeatArgs) {
	reply := HeartBeatReply{}
	if ok := rf.peers[args.To].Call("Raft.Heartbeat", args, &reply); ok {
		rf.handleHeartbeatReply(&reply)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.logger.bcastHBET()
	for idx := range rf.peers {
		if idx != rf.me {
			args := rf.makeHeartBeatArgs(idx)
			go rf.sendHeartbeatAndHandle(args)
		}
	}
}

func (rf *Raft) Heartbeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.logger.recvHBET(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.currentTerm

	// 不合格 Leader,返回防止脑裂
	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollower(args.Term)
	reply.Term = rf.currentTerm

	// 发的commit比我的log成员的新,更新log
	rf.mayCommittedTo(args.CommittedIndex)
}
