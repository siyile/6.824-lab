package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Key string
	Value string
	TaskID int
}

type task struct {
	taskID int
	value string
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

	state         map[string]string
	taskDone      []task
	taskIDToIndex map[int]int

	//taskFailed map[int]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, ok := kv.rf.Start(Op{
		CommandType: GET,
		Key:         args.Key,
		TaskID: args.TaskID,
	})

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.taskIDToIndex[args.TaskID] = index
	if ok {
		// TODO: START A GO ROUTINE CHECK WHETHER DONE OR NOT
		value := make(chan string)
		go kv.checkTask(value, index, args.TaskID)
		val := <- value
		if val == ErrNotCommitted {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			reply.Value = val
			return
		}
	} else {
		// TODO: WE HAVE ERROR HERE! WAIT TO SOLVE!
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var commandType int
	if args.Op == "Put" {
		commandType = PUT
	} else if args.Op == "Append" {
		commandType = APPEND
	}

	index, _, ok := kv.rf.Start(Op{
		CommandType: commandType,
		Key:         args.Key,
		Value: args.Value,
		TaskID: args.TaskID,
	})

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.taskIDToIndex[args.TaskID] = index
	if ok {
		// TODO: START A GO ROUTINE CHECK WHETHER DONE OR NOT
		value := make(chan string)
		go kv.checkTask(value, index, args.TaskID)
		val := <- value
		if val == ErrNotCommitted {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	} else {
		// TODO: WE HAVE ERROR HERE! WAIT TO SOLVE!
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) checkTask(valueCh chan string, index int, taskID int) {
	for true {
		kv.mu.Lock()
		if len(kv.taskDone) >= index {
			task := kv.taskDone[index]
			if task.taskID != taskID {
				valueCh <- ErrNotCommitted
			} else {
				valueCh <- task.value
			}
			break
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * CheckInterval)
	}
	kv.mu.Unlock()
}

// committed from raft
func (kv *KVServer) checkCommit() {
	for {
		msg := <- kv.applyCh
		op := msg.Command.(Op)
		kv.mu.Lock()
		if op.CommandType == GET {
			value, ok := kv.state[op.Key]
			if !ok {
				value = ""
			}
			kv.taskDone = append(kv.taskDone, task{
				taskID: op.TaskID,
				value:  value,
			})
		} else if op.CommandType == PUT {
			kv.state[op.Key] = op.Value
			kv.taskDone = append(kv.taskDone, task{
				taskID: op.TaskID,
				value:  op.Value,
			})
		} else if op.CommandType == APPEND {
			value, ok := kv.state[op.Key]
			if !ok {
				value = ""
			}
			value += op.Value
			kv.state[op.Key] = op.Value
			kv.taskDone = append(kv.taskDone, task{
				taskID: op.TaskID,
				value:  op.Value,
			})
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) checkLeader() {
	for  {
		kv.mu.Lock()
		_, kv.leader = kv.rf.GetState()
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * CheckInterval)
	}
}

func (kv *KVServer) AmILeader() bool {
	kv.mu.Lock()
	leader := kv.leader
	kv.mu.Unlock()
	return leader
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

//
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

	return kv
}
