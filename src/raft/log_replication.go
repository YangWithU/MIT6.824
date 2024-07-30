package raft

import (
	"time"
)

// 比较最后entries的index是否不小于peerTracker的nextIndex,不小则hasNewEntry
func (rf *Raft) hasNewEntries(to int) bool {
	return rf.log.lastIndex() >= rf.peerTrackers[to].nextIndex
}

// appendEntry请求
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

// 检查leader发送自己peerTrackers记录nextIndex-1是否在peer中存在对应log
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
func (rf *Raft) mayCommittedMatched(index uint64) {
	for i := index; i > rf.log.committed; i-- {
		if term, err := rf.log.term(i); err == nil && term == rf.currentTerm && rf.quorumMatched(i) {
			rf.log.committedTo(i)
			rf.hasNewCommittedEntries.Signal()
			break
		}
	}
}

func (rf *Raft) mayCommittedTo(index uint64) {
	if res := min(index, rf.log.lastIndex()); res > rf.log.committed {
		rf.log.committedTo(res)
		rf.hasNewCommittedEntries.Signal()
	}
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvAENTRes(reply)

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return
	}

	if rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	// 我们如果想要添加新的entry,就要保证follower的Index处于相同状态
	if rf.peerTrackers[reply.From].nextIndex-1 != args.PrevLogIndex {
		return
	}

	oriNext := rf.peerTrackers[reply.From].nextIndex
	oriMatch := rf.peerTrackers[reply.From].matchIndex

	switch reply.Err {
	case Rejected:
		// do nothing, waits
	case Matched:
		// rf:leader; 更新rf的peerTrackers记录
		rf.peerTrackers[reply.From].matchIndex = args.PrevLogIndex + uint64(len(args.Entries))

		rf.peerTrackers[reply.From].nextIndex = rf.peerTrackers[reply.From].matchIndex + 1

		// follower更新成功,将log持久化到leader,修改leader的committed
		rf.mayCommittedMatched(rf.peerTrackers[reply.From].matchIndex)
	case IndexNotMatched:
		fallthrough
	case TermNotMatched:
		rf.peerTrackers[reply.From].nextIndex -= 1

		resNext := rf.peerTrackers[reply.From].nextIndex
		resMatch := rf.peerTrackers[reply.From].matchIndex
		rf.logger.updateProgOf(uint64(args.From), oriNext, oriMatch, resNext, resMatch)
	default:
		panic("Err type invalid")
	}
}

func (rf *Raft) sendAppendEntriesAndHandle(args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.peers[args.To].Call("Raft.AppendEntries", args, &reply); ok {
		rf.handleAppendEntriesReply(args, &reply)
	}
}

func (rf *Raft) broadcastAppendEntries(isForced bool) {
	rf.logger.bcastAENT()
	for idx := range rf.peers {
		// 查看leader是否有新的entry
		if idx != rf.me && (isForced || rf.hasNewEntries(idx)) {
			args := rf.makeAppendEntriesArgs(idx)
			if len(args.Entries) > 0 {
				rf.logger.sendEnts(args.PrevLogIndex, args.PrevLogTerm, args.Entries, uint64(idx))
			} else {
				rf.logger.sendBeat(args.PrevLogIndex, args.PrevLogTerm, args.Entries, uint64(idx))
			}
			go rf.sendAppendEntriesAndHandle(args)
		}
	}
}

// 在follower被调用
// 检查follower log中是否包含PrevLogIndex这条log
// 存在则将args.entries从args第一个覆盖插入到follower的log中
// 最后修改follower的committed
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.logger.recvAENT(args)
	} else {
		rf.logger.recvHBET(args)
	}

	reply.Term = rf.currentTerm
	reply.From = rf.me
	reply.To = args.From
	reply.Err = Rejected // rejected when sender args < rf.me.term

	if args.Term < rf.currentTerm {
		return
	}

	termChanged := rf.becomeFollower(args.Term)
	if termChanged {
		reply.Term = rf.currentTerm
		defer rf.persist()
	}

	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		rf.logger.rejectEnts(uint64(args.From))
		return
	}

	rf.logger.acceptEnts(uint64(args.From))
	// 只有args.PrevLogIndex在当前这个peer的logEntry中存在才会尝试写入发送的entry
	// 遍历请求的Entries,假如本地没有请求对应的term或者index对不上
	// 那么锯掉peer本地突出来的entry,加上arg的entry,结束
	for idx, entry := range args.Entries {
		if term, err := rf.log.term(entry.Index); err != nil || term != entry.Term {
			rf.log.truncateSuffix(entry.Index)
			rf.log.append(args.Entries[idx:])
			if !termChanged {
				rf.persist() // term没变,立即persist
			}
			break
		}
	}
	curLastLogIndex := min(args.CommittedIndex, args.PrevLogIndex+uint64(len(args.Entries)))
	rf.mayCommittedTo(curLastLogIndex)
}
