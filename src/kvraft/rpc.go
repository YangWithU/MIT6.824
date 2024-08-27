package kvraft

const (
	OK             = "OK"
	ErrNotApplied  = "ErrNotApplied"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	OpType  string // "Put" or "Append"
	ClerkId int64
	OpId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId int64
	OpId    int
	Key     string
}

type GetReply struct {
	Err   Err
	Value string
}
