package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const MapTask = 1
const ReduceTask = 2

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type reqArgs struct {
	// request task
	idle bool

	done bool

	// reply map task
	mapIndex int

	// reply reduce task
	reduceTaskNum int

	ack int
}

type replyArgs struct {
	assigned int // 1 for map task, 2 for reduce task, 0 for not assigned

	mapIndex int
	mapFileName string
	reduceTaskNumber int

	ack int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
