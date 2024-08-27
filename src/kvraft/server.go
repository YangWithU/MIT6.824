package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
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

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftStateSize = maxRaftStateSize
	kv.persister = persister

	if kv.enableGc && persister.SnapshotSize() > 0 {
		kv.ingestSnapshot()
	} else {
		kv.db = make(map[string]string)
		kv.maxClerkAppliedOpId = make(map[int64]int)
	}

	kv.opNotifier = make(map[int64]*Notifier)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// waits for applyCh in background
	go kv.executor()

	// cyclically propose no-op to make server catch up quickly
	go kv.noOpTicker()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	log.Printf("===S%v receives Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
	op := &Op{
		Key:     args.Key,
		OpType:  "Get",
		OpId:    args.OpId,
		ClerkId: args.ClerkId,
	}

	reply.Err, reply.Value = kv.waitTillAppliedOrTimeout(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	log.Printf("===S%v receives PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	op := &Op{
		Key:     args.Key,
		Value:   args.Value,
		OpType:  args.OpType,
		OpId:    args.OpId,
		ClerkId: args.ClerkId,
	}

	reply.Err, _ = kv.waitTillAppliedOrTimeout(op)
}
