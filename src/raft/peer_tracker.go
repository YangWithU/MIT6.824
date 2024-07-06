package raft

type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64
}

func (rf *Raft) resetPeerTrackers() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i] = &PeerTracker{matchIndex: rf.log.applied, nextIndex: rf.log.lastIndex()}
	}
}
