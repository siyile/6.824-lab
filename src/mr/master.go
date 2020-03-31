package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
	"math/rand"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const AbortTime = 10


type Master struct {
	// Your definitions here.
	mapTask Task
	reduceTask Task
	mapDone Task
	reduceDone Task

	mapACK Task
	reduceACK Task

	files []string
	reduceTaskNum int
}

type Task struct {
	mux sync.Mutex
	taskMap map[int]int
}

func (task Task) put(key int, value int) bool{
	task.mux.Lock()
	var res bool
	if _, ok := task.taskMap[key]; ok {
		res = false
	} else {
		res = true
	}
	task.taskMap[key] = value
	task.mux.Unlock()
	return res
}

func (task Task) contains(key int) bool{
	task.mux.Lock()
	var res bool
	if _, ok := task.taskMap[key]; ok {
		res = true
	} else {
		res = false
	}
	task.mux.Unlock()
	return res
}

func (task Task) delete(key int) bool{
	task.mux.Lock()
	var res bool
	if _, ok := task.taskMap[key]; ok {
		res = true
		delete(task.taskMap, key)
	} else {
		res = false
	}
	task.mux.Unlock()
	return res
}

func (task Task) length() int{
	task.mux.Lock()
	res := len(task.taskMap)
	task.mux.Unlock()
	return res
}

func (task Task) getRandomKey() int{
	task.mux.Lock()
	for k := range task.taskMap {
		return k
	}
	task.mux.Unlock()
	return -1
}

func (task Task) get(key int) int{
	task.mux.Lock()
	var res int
	if _, ok := task.taskMap[key]; ok {
		res = task.taskMap[key]
	} else {
		res = -1
	}
	task.mux.Unlock()
	return res
}

// Your code here -- RPC handlers for the worker to call.


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *ReqArgs, reply *ReplyArgs) error {
	if args.Idle {
		if m.mapDone.length() < len(m.files) { // assign map task
			task := m.mapTask.getRandomKey()
			if task == -1 { // currently no task
				reply.Assigned = 0
				return nil
			}
			m.mapTask.delete(task)
			ack := rand.Int()
			m.mapACK.put(task, ack)

			reply.Assigned = MapTask
			reply.MapIndex = task
			reply.MapFileName = m.files[task]
			reply.ReduceTaskNumber = m.reduceTaskNum
			reply.ACK = ack

			go m.checkMapTaskDone(task)
			return nil
		} else if m.mapDone.length() == len(m.files) && m.reduceDone.length() < m.reduceTaskNum { // assign reduce task
			task := m.reduceTask.getRandomKey()
			if task == -1 { // currently no task
				reply.Assigned = 0
				return nil
			}
			m.reduceTask.delete(task)
			ack := rand.Int()
			m.reduceACK.put(task, ack)

			reply.Assigned = ReduceTask
			reply.ReduceTaskNumber = task
			reply.ACK = ack

			go m.checkReduceTaskDone(task)
			return nil
		} else if m.reduceDone.length() == m.reduceTaskNum { // work done kill worker
			reply.Assigned = KillSignal
			return nil
		}
	}


	return nil
}

func (m *Master) MapDone(args *ReqArgs, reply *ReplyArgs) error {
	if args.ACK == m.mapACK.get(args.MapIndex) {
		m.mapDone.put(args.MapIndex, 1)
	} // otherwise discard
	fmt.Printf("#%v MAP task done!\n", args.MapIndex)
	return nil
}

func (m *Master) ReduceDone(args *ReqArgs, reply *ReplyArgs) error {
	if args.ACK == m.reduceACK.get(args.ReduceTaskNum) {
		m.reduceDone.put(args.ReduceTaskNum, 1)
	} // otherwise discard
	fmt.Printf("#%v Reduce task done!\n", args.ReduceTaskNum)
	return nil
}

func (m *Master) checkMapTaskDone(mapIndex int) {
	time.Sleep(AbortTime * time.Second)
	if m.mapDone.contains(mapIndex) {
		return
	} else { // if not done, discard task
		m.mapTask.put(mapIndex, 1)
	}
}

func (m *Master) checkReduceTaskDone(reduceTaskNumber int) {
	time.Sleep(AbortTime * time.Second)
	if m.reduceDone.contains(reduceTaskNumber) {
		return
	} else { // if not done, discard task
		m.reduceTask.put(reduceTaskNumber, 1)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.reduceDone.length() == m.reduceTaskNum

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// init new taskMap, doneMap
	m.mapTask = Task{taskMap: make(map[int]int)}
	m.reduceTask = Task{taskMap: make(map[int]int)}
	m.mapDone = Task{taskMap: make(map[int]int)}
	m.reduceDone = Task{taskMap: make(map[int]int)}
	m.mapACK = Task{taskMap: make(map[int]int)}
	m.reduceACK = Task{taskMap: make(map[int]int)}

	// init task
	m.files = files
	m.reduceTaskNum = nReduce
	for i := range files {
		m.mapTask.taskMap[i] = 1
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTask.taskMap[i] = 1
	}

	m.server()
	return &m
}
