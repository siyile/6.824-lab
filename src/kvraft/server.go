package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType int
	Key         string
	Value       string
	TaskId      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	leader bool

	state map[string]string

	// Map[taskId]string
	results map[int]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// If already have result in map just return it.
	value, ok := kv.results[args.TaskId]
	if ok {
		reply.Err = OK
		reply.Value = value
		kv.mu.Unlock()
		return
	}

	index, _, ok := kv.rf.Start(Op{
		CommandType: GET,
		Key:         args.Key,
		TaskId:      args.TaskId,
	})

	// Just return if not leader.
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// We start check here
	result := make(chan string)
	go kv.checkTask(result, index, args.TaskId, kv.rf.CurrentTerm)

	kv.mu.Unlock()

	val := <-result
	if val == ErrorTermChanged {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.Value = val
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Copied from GET
	kv.mu.Lock()

	// If already have result in map just return it.
	_, ok := kv.results[args.TaskId]
	if ok {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	// Copied from GET
	var commandType int
	if args.Op == "Append" {
		commandType = APPEND
	} else {
		commandType = PUT
	}

	index, _, ok := kv.rf.Start(Op{
		CommandType: commandType,
		Key:         args.Key,
		Value:       args.Value,
		TaskId:      args.TaskId,
	})

	// Just return if not leader.
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// We start check here
	result := make(chan string)
	go kv.checkTask(result, index, args.TaskId, kv.rf.CurrentTerm)

	kv.mu.Unlock()

	val := <-result
	if val == ErrorTermChanged {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		return
	}

}

// Run infinitely until find taskId in result. Unless, term changed.
func (kv *KVServer) checkTask(valueCh chan string, index int, taskID int, taskTerm int) {
	for true {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		if kv.rf.CurrentTerm != taskTerm {
			DPrintf("server %d, term changed, taskID %d return failure", kv.me, taskID)
			valueCh <- ErrorTermChanged
			break
		}
		value, ok := kv.results[taskID]
		if ok {
			DPrintf("server %d, success taskID %d", kv.me, taskID)
			valueCh <- value
			break
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * CheckInterval)
	}
	kv.mu.Unlock()
}

// Committed from raft, any committed msg is always true.
func (kv *KVServer) checkCommit() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		taskId := op.TaskId
		kv.mu.Lock()
		_, ok := kv.results[taskId]
		if ok {
			DPrintf("server %d found duplicate taskId %d, discarding...", kv.me, taskId)
			kv.mu.Unlock()
			continue
		}
		DPrintf("server %d raft committed, task ID %d, committing to my state", kv.me, taskId)
		if op.CommandType == GET {
			value, ok := kv.state[op.Key]
			if !ok {
				value = ""
			}
			// taskId := kv.indexMap[msg.CommandIndex]
			kv.results[taskId] = value
		} else if op.CommandType == PUT {
			kv.state[op.Key] = op.Value
			// taskId := kv.indexMap[msg.CommandIndex]
			kv.results[taskId] = op.Value
		} else if op.CommandType == APPEND {
			value, ok := kv.state[op.Key]
			if !ok {
				value = ""
			}
			value += op.Value
			kv.state[op.Key] = value
			// taskId := kv.indexMap[msg.CommandIndex]
			kv.results[taskId] = op.Value
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//s
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.results = make(map[int]string)
	kv.state = make(map[string]string)

	go kv.checkCommit()

	return kv
}
