package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxRaftStateSize int // snapshot if log grows this big

	enableGc bool

	db map[string]string

	opNotifier map[int64]*Notifier // [clerkId]

	// max operation id among all applied operation of each clerk
	maxClerkAppliedOpId map[int64]int // k: clerkId, v:maxOpId

	snapShotIndex    int
	lastAppliedIndex int
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftStateSize bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftStateSize is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftStateSize int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		mu:                  sync.Mutex{},
		me:                  me,
		rf:                  raft.Make(servers, me, persister, applyCh),
		applyCh:             applyCh,
		dead:                0,
		persister:           persister,
		maxRaftStateSize:    maxRaftStateSize,
		enableGc:            maxRaftStateSize != -1,
		db:                  nil, // ingestSnapshot
		opNotifier:          make(map[int64]*Notifier),
		maxClerkAppliedOpId: nil, // ingestSnapshot
		snapShotIndex:       0,   // ingestSnapshot
	}

	// restore from snapshot
	if kv.enableGc && persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(kv.persister.ReadSnapshot())
	} else {
		kv.db = make(map[string]string)
		kv.maxClerkAppliedOpId = make(map[int64]int)
		kv.snapShotIndex = 0
	}

	// will signal() by collector
	// apply Op into server db
	go kv.executor()

	// cyclically propose no-op to make server catch up quickly
	go kv.noOpTicker()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		Key:     args.Key,
		OpType:  "Get",
		OpId:    args.OpId,
		ClerkId: args.ClerkId,
	}

	reply.Err, reply.Value = kv.waitTillAppliedOrTimeout(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  args.OpType,
		OpId:    args.OpId,
		ClerkId: args.ClerkId,
	}

	reply.Err, _ = kv.waitTillAppliedOrTimeout(op)
}
