package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 手动主动存snapshot,假如entries中存在
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.pullSnap(uint64(index))

	if rf.log.hasPendingSnapshot {
		return
	}

	snapshotIndex := uint64(index)
	snapshotTerm, err := rf.log.term(snapshotIndex)
	if err == nil && snapshotIndex > rf.log.snapShot.Index {
		initialSnapShot := SnapShot{Data: snapshot, Index: snapshotIndex, Term: snapshotTerm}
		rf.log.toCompactSnapShot(initialSnapShot)
		rf.persist()
	}
}

// leader log firstIndex
// TODO WHY?
func (rf *Raft) lagBehindSnapshot(to int) bool {
	return rf.log.firstIndex() >= rf.peerTrackers[to].nextIndex
}

func (rf *Raft) makeInstallSnapShotArgs(to int) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		From:     rf.me,
		To:       to,
		Term:     rf.currentTerm,
		SnapShot: rf.log.cloneSnapShot(),
	}
}

func (rf *Raft) sendInstallSnapShotAndHandle(args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if ok := rf.peers[args.To].Call("Raft.InstallSnapShot", args, &reply); ok {
		rf.handleInstallSnapShotReply(args, &reply)
	}
}

func (rf *Raft) handleInstallSnapShotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNPRes(reply)

	msg := Message{
		Type:     SnapReply,
		From:     reply.From,
		Term:     reply.Term,
		ArgsTerm: args.Term,
	}
	ok, termChanged := rf.checkMessage(msg)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.Installed {
		oriNext := rf.peerTrackers[reply.From].nextIndex
		oriMatch := rf.peerTrackers[reply.From].matchIndex

		rf.peerTrackers[reply.From].matchIndex = args.SnapShot.Index
		rf.peerTrackers[reply.From].nextIndex = rf.peerTrackers[reply.From].matchIndex + 1

		newNext := rf.peerTrackers[reply.From].nextIndex
		newMatch := rf.peerTrackers[reply.From].matchIndex
		if newNext != oriNext || newMatch != oriMatch {
			rf.logger.updateProgOf(uint64(reply.From), oriNext, oriMatch, newNext, newMatch)
		}
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNP(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.currentTerm
	reply.Installed = false

	msg := Message{
		Type: Snap,
		From: args.From,
		Term: args.Term,
	}
	ok, termChanged := rf.checkMessage(msg)
	if termChanged {
		reply.Term = rf.currentTerm
		defer rf.persist()
	}
	if !ok {
		return
	}

	if args.SnapShot.Index <= rf.log.snapShot.Index {
		reply.Installed = true
		return
	}

	rf.log.toCompactSnapShot(args.SnapShot)
	reply.Installed = true
	if !termChanged {
		defer rf.persist()
	}

	rf.log.hasPendingSnapshot = true
	rf.signalAndLog()
}
