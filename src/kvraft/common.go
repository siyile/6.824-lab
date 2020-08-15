package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	ErrorTermChanged = "ErrorTermChanged"

	GET    = 1
	PUT    = 2
	APPEND = 3

	CheckInterval       = 150
	CheckLeaderInterval = 500
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	TaskId   int
	ClientId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	TaskId   int
	ClientId int
}

type GetReply struct {
	Err   Err
	Value string
}

type CheckLeaderArgs struct {
}

type CheckLeaderReply struct {
	IsLeader bool
}
