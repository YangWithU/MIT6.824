package kvraft

import "time"

const proposeNoOpInterval = 250 * time.Millisecond

type Op struct {
	Key     string
	Value   string
	OpType  string // Get Put Append NoOp
	OpId    int
	ClerkId int64
}

func (kv *KVServer) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *KVServer) noOpTicker() {
	for !kv.killed() {
		op := &Op{OpType: "NoOp"}
		kv.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}
