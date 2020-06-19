package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"sync"
	"time"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leader int
	ACK int

	mu      sync.Mutex
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
	ck.leader = -1
	go ck.daemon()

	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	value := ""
	taskID := mathrand.Int()
	for true {
		ck.mu.Lock()
		if ck.leader != -1 {
			args := GetArgs{
				Key:    key,
				TaskID: taskID,
			}
			reply := GetReply{}
			ck.mu.Unlock()
			ok := ck.sendGet(ck.leader, &args, &reply)
			ck.mu.Lock()
			if !ok || reply.Err != OK {
				// pass
			} else {
				value = reply.Value
				break
			}
		}
		ck.mu.Unlock()
		time.Sleep(CheckInterval * time.Millisecond)
	}
	// You will have to modify this function.
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for true {
		ck.mu.Lock()
		if ck.leader != -1 {
			args := PutAppendArgs{
				Key:   key,
				Value: value,
				Op: op,
			}
			reply := PutAppendReply{}
			ck.mu.Unlock()
			ok := ck.sendPutAppend(ck.leader, &args, &reply)
			ck.mu.Lock()
			if !ok || reply.Err != OK {
				// pass
			} else {
				break
			}
		}
		ck.mu.Unlock()
		time.Sleep(CheckInterval * time.Millisecond)
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) checkLeader() {
	var wg sync.WaitGroup

	ck.mu.Lock()

	ck.ACK = mathrand.Int()
	prevACK := ck.ACK
	args := CheckLeaderArgs{}
	leader := -1
	oneLeader := false
	for i := range ck.servers {
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := CheckLeaderReply{}
			ck.sendCheckLeader(server, &args, &reply)

			ck.mu.Lock()
			if ck.ACK != prevACK {
				return
			}
			ck.mu.Unlock()
			if reply.IsLeader {
				if leader == -1 {
					leader = server
					oneLeader = true
				} else {
					oneLeader = false
				}
			}
		}(i)
	}

	ck.mu.Unlock()
	wg.Wait()
	ck.mu.Lock()

	if prevACK != ck.ACK {
		return
	}
	if oneLeader {
		ck.leader = leader
	} else {
		ck.leader = -1
	}

	ck.mu.Unlock()
}

func (ck *Clerk) sendCheckLeader(server int, args *CheckLeaderArgs, reply *CheckLeaderReply) bool {
	ok := ck.servers[server].Call("KVServer.CheckLeader", args, reply)
	return ok
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) daemon() {
	for true {
		go ck.checkLeader()
		time.Sleep(CheckLeaderInterval * time.Millisecond)
	}
}