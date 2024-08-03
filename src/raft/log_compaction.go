package raft

import "time"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := uint64(index)
	snapshotTerm, err := rf.log.term(snapshotIndex)
	if err != nil && snapshotIndex > rf.log.snapShot.Index {
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
		rf.handleInstallSnapShot(args, &reply)
	}
}

func (rf *Raft) handleInstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISNPRes(reply)

	rf.peerTrackers[reply.From].lastAck = time.Now()

	if rf.currentTerm > reply.Term {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return
	}

	if rf.currentTerm != reply.Term || rf.state != Leader {
		return
	}
	if args.SnapShot.Index >= rf.peerTrackers[reply.From].nextIndex {
		return
	}

	// TODO Why?
	if reply.Installed {
		rf.peerTrackers[reply.From].nextIndex = args.SnapShot.Index + 1
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

	if args.Term < rf.currentTerm {
		return
	}

	termChanged := rf.becomeFollower(args.Term)
	if termChanged {
		reply.Term = rf.currentTerm
		defer rf.persist()
	}
	rf.resetElectionTimer()

	if args.SnapShot.Index >= rf.log.snapShot.Index {
		rf.log.toCompactSnapShot(args.SnapShot)
		if !termChanged {
			rf.persist()
		}
	}
}
