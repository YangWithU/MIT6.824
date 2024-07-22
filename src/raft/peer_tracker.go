package raft

import "time"

type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64
	lastAck    time.Time
}

func (rf *Raft) resetTrackedIndexes() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextIndex = rf.log.lastIndex() + 1
		rf.peerTrackers[i].matchIndex = 0
	}
}

// 验证是否过半数peer还活跃：有的peer可能由于断点等原因不再接收heartbeat
func (rf *Raft) isQuorumPeerActive() bool {
	activePeers := 1
	for idx, tracker := range rf.peerTrackers {
		if idx != rf.me &&
			time.Since(tracker.lastAck) <= 2*baseElectionTimeout*time.Millisecond {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}
