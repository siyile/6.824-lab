package kvraft

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientId int
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
	ck.clientId = mathrand.Int()

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
	taskId := mathrand.Int()

	args := GetArgs{
		Key:      key,
		TaskId:   taskId,
		ClientId: ck.clientId,
	}

	if ck.leader != -1 {
		reply := GetReply{}
		ok := ck.sendGet(ck.leader, &args, &reply)
		if !ok || reply.Err != OK {
			// try another leader forever
		} else {
			value = reply.Value
			DPrintf("Client success by server %d, for taskId %d", ck.leader, taskId)
			return value
		}
	}

	for {
		for server := range ck.servers {
			reply := GetReply{}
			ok := ck.sendGet(server, &args, &reply)
			if !ok || reply.Err != OK {
				continue
			} else {
				value = reply.Value
				ck.leader = server
				DPrintf("change leader to %d", ck.leader)
				DPrintf("Client success by server %d, for taskId %d", ck.leader, taskId)
				return value
			}
		}
		time.Sleep(time.Millisecond * CheckLeaderInterval)
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
	taskId := mathrand.Int()

	args := PutAppendArgs{
		Key:      key,
		TaskId:   taskId,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
	}

	if ck.leader != -1 {
		reply := PutAppendReply{}
		ok := ck.sendPutAppend(ck.leader, &args, &reply)
		if !ok || reply.Err != OK {
			// try another leader forever
		} else {
			DPrintf("Client success by server %d, for taskId %d", ck.leader, taskId)
			return
		}
	}

	for {
		for server := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.sendPutAppend(server, &args, &reply)
			if !ok || reply.Err != OK {
				continue
			} else {
				ck.leader = server
				DPrintf("change leader to %d", ck.leader)
				DPrintf("Client success by server %d, for taskId %d", ck.leader, taskId)
				return
			}
		}
		time.Sleep(time.Millisecond * CheckLeaderInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	DPrintf("Clerk send get %s to server %d, taskId %d", args.Key, server, args.TaskId)
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	DPrintf("Clerk send put/append %s/%s to server %d， taskId %d", args.Key, args.Value, server, args.TaskId)
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}
