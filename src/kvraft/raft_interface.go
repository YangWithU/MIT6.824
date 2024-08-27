package kvraft

import "log"

func (kv *KVServer) propose(op *Op) bool {
	_, _, isLeader := kv.rf.Start(op)
	if isLeader {
		log.Printf("===S%v proposes (C=%v Id=%d)", kv.me, op.ClerkId, op.OpId)
	}
	return isLeader
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
