package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
	"encoding/json"
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

	var reply replyArgs

	// Your worker implementation here.
	for {
		tmp := CallForWork()
		if tmp.assigned != 0 {
			reply = tmp
			break
		}
		time.Sleep(SleepTime)
	}


	if reply.assigned == MapTask {
		// open file
		filename := reply.mapFileName
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
		kva := mapf(reply.mapFileName, string(content))

		/*
			save intermediate results
		*/
		// create tmp file
		var tmpfiles []*os.File
		var encoders []*json.Encoder
		for i := 0; i < reply.reduceTaskNumber; i++ {
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
			i := ihash(kv.Key) % reply.reduceTaskNumber
			err := encoders[i].Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		// rename to mr-mapNumber-reduceNumber & close
		for i := 0; i < reply.reduceTaskNumber; i++ {
			_ = tmpfiles[i].Close()
			_ = os.Rename(tmpfiles[i].Name(), fmt.Sprintf("mr-%v-%v", reply.mapIndex, i))
		}

		// send message to master we are done
		CallMapDone(reply.mapIndex)
	} else if reply.assigned == ReduceTask {
		// read files
		files, err := filepath.Glob(fmt.Sprintf("mr-[0-9]?-%v", reply.reduceTaskNumber))
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

		outFileName := fmt.Sprintf("mr-out-%v", reply.reduceTaskNumber)
		outfile, _ := os.Create(outFileName)

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
			_, _ = fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		_ = outfile.Close()

		CallReduceDone(reply.reduceTaskNumber)
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

func CallForWork() replyArgs {
	args := reqArgs{idle:true}
	reply := replyArgs{}

	call("Master.GetTask", &args, &reply)

	return reply
}

func CallMapDone(mapIndex int) {
	args := reqArgs{idle:true, done:true, mapIndex:mapIndex}
	reply := replyArgs{}
	call("Master.MapDone", &args, &reply)
}

func CallReduceDone(reduceTaskNum int) {
	args := reqArgs{idle:true, done:true, reduceTaskNum:reduceTaskNum}
	reply := replyArgs{}
	call("Master.ReduceDone", &args, &reply)
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
