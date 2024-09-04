package kvraft

import "log"

func (kv *KVServer) propose(op *Op) (isLeader bool) {
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		if kv.isNoOp(op) {
			log.Printf("===S%v proposes no-op", kv.me)
		} else {
			log.Printf("===S%v proposes (C=%v Id=%d) at N=%v", kv.me, op.ClerkId, op.OpId, index)
		}
	}
	return isLeader
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
