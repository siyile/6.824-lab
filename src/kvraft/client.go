package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leader int
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
	//go ck.daemon()

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

	args := GetArgs{
		Key:    key,
		TaskID: taskID,
	}
	reply := GetReply{}

	if ck.leader != -1 {
		ok := ck.sendGet(ck.leader, &args, &reply)
		if !ok || reply.Err != OK {
			// try another leader forever
		} else {
			value = reply.Value
			return value
		}
	}

	for {
		for server := range ck.servers {
			ok := ck.sendGet(server, &args, &reply)
			if !ok || reply.Err != OK {
				continue
			} else {
				value = reply.Value
				ck.leader = server
				return value
			}
		}
	}


	// You will have to modify this function.

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
	taskID := mathrand.Int()

	args := PutAppendArgs{
		Key:    key,
		TaskID: taskID,
		Value: value,
		Op: op,
	}

	reply := PutAppendReply{}

	if ck.leader != -1 {
		ok := ck.sendPutAppend(ck.leader, &args, &reply)
		if !ok || reply.Err != OK {
			// try another leader forever
		} else {
			return
		}
	}

	for {
		for server := range ck.servers {
			ok := ck.sendPutAppend(server, &args, &reply)
			if !ok || reply.Err != OK {
				continue
			} else {
				ck.leader = server
				DPrintf("change leader to %d", ck.leader)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//func (ck *Clerk) checkLeader() {
//	var wg sync.WaitGroup
//
//	ck.mu.Lock()
//
//	ck.ACK = mathrand.Int()
//	prevACK := ck.ACK
//	args := CheckLeaderArgs{}
//	leader := -1
//	oneLeader := false
//	for i := range ck.servers {
//		wg.Add(1)
//		go func(server int) {
//			defer wg.Done()
//			reply := CheckLeaderReply{}
//			ck.sendCheckLeader(server, &args, &reply)
//
//			ck.mu.Lock()
//			if ck.ACK != prevACK {
//				return
//			}
//			ck.mu.Unlock()
//			if reply.IsLeader {
//				if leader == -1 {
//					leader = server
//					oneLeader = true
//				} else {
//					oneLeader = false
//				}
//			}
//		}(i)
//	}
//
//	ck.mu.Unlock()
//	wg.Wait()
//	ck.mu.Lock()
//
//	if prevACK != ck.ACK {
//		return
//	}
//	if oneLeader {
//		ck.leader = leader
//	} else {
//		ck.leader = -1
//	}
//
//	ck.mu.Unlock()
//}
//
//func (ck *Clerk) sendCheckLeader(server int, args *CheckLeaderArgs, reply *CheckLeaderReply) bool {
//	ok := ck.servers[server].Call("KVServer.CheckLeader", args, reply)
//	return ok
//}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	DPrintf("Clerk send get %s to server %s", args.Key, server)
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	DPrintf("Clerk send put/append %s/%s to server %s", args.Key, args.Value, server)
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

//func (ck *Clerk) daemon() {
//	for true {
//		go ck.checkLeader()
//		time.Sleep(CheckLeaderInterval * time.Millisecond)
//	}
//}