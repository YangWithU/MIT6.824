package raft

import "time"

// 比较最后entries的index是否不小于peerTracker的nextIndex,不小则hasNewEntry
func (rf *Raft) hasNewEntries(to int) bool {
	return rf.log.lastIndex() >= rf.peerTrackers[to].nextIndex
}

// appendEntry请求
// CommittedIndex:
func (rf *Raft) makeAppendEntriesArgs(to int) *AppendEntriesArgs {
	nextIndex := rf.peerTrackers[to].nextIndex
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := rf.log.term(prevLogIndex)

	// 发送的entry是之前peerTrackers记录的peer老index
	// 到现在leader新的index之间的entry
	entries, _ := rf.log.slice(nextIndex, rf.log.lastIndex()+1)
	return &AppendEntriesArgs{
		From:           rf.me,
		To:             to,
		Term:           rf.currentTerm,
		CommittedIndex: rf.log.committed,
		PrevLogIndex:   prevLogIndex,
		PrevLogTerm:    prevLogTerm,
		Entries:        entries,
	}
}

func (rf *Raft) checkLogPrefixMatched(prevLeaderLogIndex, prevLeaderLogTerm uint64) Err {
	localPrevLogTerm, err := rf.log.term(prevLeaderLogIndex)
	if err != nil {
		return IndexNotMatched
	}
	if localPrevLogTerm != prevLeaderLogTerm {
		return TermNotMatched
	}
	return Matched
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvAENT(args)

	reply.Term = rf.currentTerm
	reply.From = rf.me
	reply.To = args.From

	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollower(args.Term, false)
	reply.Term = rf.currentTerm

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		return
	}

	// 只有args.PrevLogIndex在当前这个peer的logEntry中存在才会尝试写入发送的entry
	// 遍历请求的Entries,假如本地没有请求对应的term或者index对不上
	// 那么锯掉peer本地突出来的entry,加上arg的entry,结束
	for idx, entry := range args.Entries {
		if term, err := rf.log.term(entry.Index); err != nil || term != entry.Term {
			rf.log.truncateSuffix(entry.Index)
			rf.log.append(args.Entries[idx:])
			break
		}
	}
	rf.log.mayCommittedTo(args.CommittedIndex)
}

func (rf *Raft) quorumMatched(index uint64) bool {
	matchCnt := 1
	for _, tracker := range rf.peerTrackers {
		if tracker.matchIndex >= index {
			matchCnt++
		}
	}
	return 2*matchCnt > len(rf.peers)
}

// 输入entries的index,反向遍历 (log.committed~index].
// 假如logEntries中的term与当前rf的term相同,
// 多半数peerTracker的matchIndex不小于当前index,
// 则更新log.committed为当前index
func (rf *Raft) mayCommittedTo(index uint64) {
	for i := index; i > rf.log.committed; i-- {
		if term, err := rf.log.term(i); err == nil && term == rf.currentTerm && rf.quorumMatched(i) {
			rf.log.committedTo(i)
			break
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvAENTRes(reply)

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, false)
		return
	}

	switch reply.Err {
	case Matched:
		// rf:leader; 更新rf的peerTrackers记录
		// nextIndex
		rf.peerTrackers[reply.From].nextIndex = max(rf.peerTrackers[reply.From].nextIndex,
			args.PrevLogIndex+uint64(len(args.Entries)))
		rf.peerTrackers[reply.From].matchIndex = max(rf.peerTrackers[reply.From].matchIndex,
			rf.peerTrackers[reply.From].nextIndex-1)

		rf.mayCommittedTo(rf.peerTrackers[reply.From].matchIndex)
	default:
		rf.peerTrackers[reply.From].nextIndex -= 1
	}
}

func (rf *Raft) sendAppendEntriesAndHandle(args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[args.To].Call("Raft.AppendEntries", args, &reply); ok {
		rf.handleAppendEntriesReply(args, &reply)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.logger.bcastAENT()
	for idx := range rf.peers {
		// 查看leader是否有新的entry
		if idx != rf.me && rf.hasNewEntries(idx) {
			args := rf.makeAppendEntriesArgs(idx)
			go rf.sendAppendEntriesAndHandle(args)
		}
	}
}
