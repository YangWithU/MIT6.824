package kvraft

import (
	"log"
)

func (kv *KVServer) isApplied(op *Op) bool {
	maxId, ok := kv.maxClerkAppliedOpId[op.ClerkId]
	return ok && maxId >= op.OpId
}

func (kv *KVServer) applyClientOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		kv.db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		kv.db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

// apply Op to server db
// notify waitTillAppliedOrTimeout(),
// thus KVServer would send reply RPC back to Clerk
func (kv *KVServer) mayToApplyClientOp(op *Op) bool {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxClerkAppliedOpId[op.ClerkId] = op.OpId

		kv.notify(op)
		return true
	}
	return false
}

// background g in KVServer
// waits collector() to receive from applyCh
func (kv *KVServer) executor() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()

		//msg snapshot比KVServer新
		if m.SnapshotValid && m.SnapshotIndex > kv.snapShotIndex &&
			m.SnapshotIndex >= kv.lastAppliedIndex { // snapshot

			kv.ingestSnapshot(m.Snapshot) // 将KVServer更新成最新

		} else if m.CommandValid { // new entry
			op := m.Command.(*Op)
			if kv.isNoOp(op) {
				// skip noOp
			} else if m.CommandIndex > kv.lastAppliedIndex {
				if kv.mayToApplyClientOp(op) {
					kv.lastAppliedIndex = m.CommandIndex

					log.Printf("===S%v applied client op (C=%v Id=%v) at N=%v",
						kv.me, op.ClerkId, op.OpId, m.CommandIndex)
				}
			}

			// raft state太大,主动snapshot
			// 删去kv.rf.log.entries[0,m.CommandIndex)
			if kv.enableGc && kv.approachGCLimit() {
				kv.checkpoint(m.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

// wait for new logEntry to send, quorum accepted, committed in raft cluster
// when new log done, executor()->mayToApplyClientOp()
func (kv *KVServer) waitTillAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.OpType == "Get" {
		log.Printf("===S%v receives Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	} else {
		log.Printf("===S%v receives PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	}

	if !kv.isApplied(op) {

		// propose()->Start to send op
		if !kv.propose(op) { // not leader
			return ErrWrongLeader, ""
		}

		// leader, not applied
		kv.makeNotifier(op)
		kv.wait(op)
	}

	// notifier wakes
	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
			log.Printf("S%v replies Get (C=%v Id=%v) value=%v", kv.me, op.ClerkId, op.OpId, value)
		} else {
			log.Printf("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
