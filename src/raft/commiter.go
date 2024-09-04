package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 向client发送command执行结果,并将新log标记applied
func (rf *Raft) committer() {
	rf.mu.Lock()
	for !rf.killed() {
		if rf.log.hasPendingSnapshot {
			rf.logger.hasPendingSNP()
			snapshot := rf.log.cloneSnapShot()
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot.Data,
				SnapshotTerm:  int(snapshot.Term),
				SnapshotIndex: int(snapshot.Index),
			}

			rf.mu.Lock()
			rf.log.hasPendingSnapshot = false
			rf.logger.pushSnap(snapshot.Index, snapshot.Term)
		} else if newCommittedEntries := rf.log.newCommittedEntries(); len(newCommittedEntries) > 0 {
			rf.logger.hasCmitEnt(newCommittedEntries)
			rf.mu.Unlock()

			for _, entry := range newCommittedEntries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Data, CommandIndex: int(entry.Index)}
			}

			rf.mu.Lock()
			// figure 8
			// leader commit log 时，只能提交自己当前 term 的 log（不能提交以前 term 的 log），不然就会引起日志冲突
			applied := max(rf.log.applied, newCommittedEntries[len(newCommittedEntries)-1].Index)
			rf.log.appliedTo(applied)
		} else {
			rf.logger.printf(SNAP, "N%v waits", rf.me)
			rf.hasNewCommittedEntries.Wait()
			rf.logger.printf(SNAP, "N%v awakes", rf.me)
		}
	}

	rf.mu.Unlock()
}
