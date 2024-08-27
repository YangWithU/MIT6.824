package kvraft

import (
	"log"
	"time"
)

const maxWaitingTime = 500 * time.Millisecond

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

func (kv *KVServer) maybeApplyClientOp(op *Op, index int) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxClerkAppliedOpId[op.ClerkId] = op.OpId

		kv.notify(op)

		log.Printf("===S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, index)
	}
}

func (kv *KVServer) executor() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		if msg.SnapshotValid {
			kv.ingestSnapshot()
		} else {
			op := msg.Command.(*Op)
			if kv.isNoOp(op) {
				// skip noOp
			} else {
				kv.maybeApplyClientOp(op, msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

// wakes g
func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitingTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

// wait for new logEntry to send, quorum accepted, committed in raft cluster
// when new log done, executor()->maybeApplyClientOp()
func (kv *KVServer) waitTillAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isApplied(op) {
		if !kv.propose(op) { // not leader
			return ErrWrongLeader, ""
		}

		// leader, not applied
		notifier := kv.getNotifier(op, true)
		kv.makeAlarm(op)
		notifier.done.Wait()
	}

	// notifier wakes
	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
