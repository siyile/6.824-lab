package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const SleepTime = 2

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallForWork()
		if reply.Assigned == 0 {
			time.Sleep(SleepTime * time.Second)
			continue
		}

		if reply.Assigned == MapTask {
			fmt.Printf("Doing #%v MAP task!\n", reply.MapIndex)
			// open file
			filename := reply.MapFileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			_ = file.Close()

			// MAP!!
			kva := mapf(reply.MapFileName, string(content))

			/*
				save intermediate results
			*/
			// create tmp file
			var tmpfiles []*os.File
			var encoders []*json.Encoder
			for i := 0; i < reply.ReduceTaskNumber; i++ {
				tmpfile, err := ioutil.TempFile("", "intermidiatefile")
				if err != nil {
					log.Fatal(err)
				}

				enc := json.NewEncoder(tmpfile)
				encoders = append(encoders, enc)

				tmpfiles = append(tmpfiles, tmpfile)
			}

			// wrap into json
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.ReduceTaskNumber
				err := encoders[i].Encode(&kv)
				if err != nil {
					log.Fatal(err)
				}
			}

			// send message to master we are done
			if 	CallMapDone(reply.MapIndex, reply.ACK) {
				// rename to mr-mapNumber-reduceNumber & close
				for i := 0; i < reply.ReduceTaskNumber; i++ {
					_ = tmpfiles[i].Close()
					_ = os.Rename(tmpfiles[i].Name(), fmt.Sprintf("mr-%v-%v", reply.MapIndex, i))
				}
			} else {
				for i := 0; i < reply.ReduceTaskNumber; i++ {
					_ = tmpfiles[i].Close()
					_ = os.Remove(tmpfiles[i].Name())
					fmt.Printf("[Worker] Aborting #%v MAP Task\n", reply.MapIndex)
				}
			}

		} else if reply.Assigned == ReduceTask {
			fmt.Printf("Doing #%v REDUCE task!\n", reply.ReduceTaskNumber)
			// read files
			files, err := filepath.Glob(fmt.Sprintf("mr-[0-9]*-%v", reply.ReduceTaskNumber))
			if err != nil {
				log.Fatal(err)
			}
			intermediate := []KeyValue{}
			for _, filename := range files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				var kva []KeyValue
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				intermediate = append(intermediate, kva...)
				_ = file.Close()
			}

			sort.Sort(ByKey(intermediate))

			outFileName := fmt.Sprintf("mr-out-%v", reply.ReduceTaskNumber)

			tmpfile, err := ioutil.TempFile("", "intermidiatefile")
			if err != nil {
				log.Fatal(err)
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				_, _ = fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			if CallReduceDone(reply.ReduceTaskNumber, reply.ACK) {
				_ = tmpfile.Close()
				_ = os.Rename(tmpfile.Name(), outFileName)

				for _, filename := range files {
					_ = os.Remove(filename)
				}
			} else {
				_ = os.Remove(tmpfile.Name())
				fmt.Printf("[Worker] Aborting #%v REDUCE Task\n", reply.ReduceTaskNumber)
			}


		} else if reply.Assigned == KillSignal {
			return
		}
		time.Sleep(SleepTime * time.Second)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallForWork() ReplyArgs {
	args := ReqArgs{Idle: true}
	reply := ReplyArgs{}

	call("Master.GetTask", &args, &reply)

	return reply
}

func CallMapDone(mapIndex, ACK int) bool{
	args := ReqArgs{Idle: true, Done:true, MapIndex:mapIndex, ACK:ACK}
	reply := ReplyArgs{}
	call("Master.MapDone", &args, &reply)
	return ACK == reply.ACK
}

func CallReduceDone(reduceTaskNum, ACK int) bool{
	args := ReqArgs{Idle: true, Done:true, ReduceTaskNum:reduceTaskNum, ACK:ACK}
	reply := ReplyArgs{}
	call("Master.ReduceDone", &args, &reply)
	return ACK == reply.ACK
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
