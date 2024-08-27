package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

const retryTimeOut = 100 * time.Millisecond // 0.1sec

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId  int64 // unique id of this clerk
	nextOpId int   // next op id to allocate for one op
	leaderId int   // leader id of cur state
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.nextOpId = 0
	ck.leaderId = 0 // default: servers[0]

	return ck
}

func (ck *Clerk) allocateOpId() int {
	res := ck.nextOpId
	ck.nextOpId++
	return res
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// You will have to modify this function.

func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, Key: key, OpId: ck.allocateOpId()}

	for {
		for i := 0; i < len(ck.servers); i++ {
			curServerId := (ck.leaderId + i) % len(ck.servers)
			log.Printf("***C%v sends Get (Id=%v K=%v) to S%v",
				args.ClerkId, args.OpId, args.Key, curServerId)

			var reply GetReply
			if ok := ck.servers[curServerId].Call("KVServer.Get", args, &reply); ok {
				if reply.Err == OK {
					ck.leaderId = curServerId
					return reply.Value
				}
			}
		}
		time.Sleep(retryTimeOut)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// You will have to modify this function.

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		OpType:  op,
		OpId:    ck.allocateOpId(),
		ClerkId: ck.clerkId,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			curServerId := (ck.leaderId + i) % len(ck.servers)
			log.Printf("***C%v sends PutAppend (Id=%v T=%v K=%v V=%v) to S%v",
				args.ClerkId, args.OpId, args.OpType, args.Key, args.Value, curServerId)

			var reply PutAppendReply
			if ok := ck.servers[curServerId].Call("KVServer.PutAppend", args, &reply); ok {
				if reply.Err == OK {
					ck.leaderId = curServerId
					return
				}
			}
		}
		time.Sleep(retryTimeOut)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
