package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type TaskInfo = CallTaskReply
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	nMap, nReduce := CallInit()
	jobPhase := ReducePhase
	doneId := -1
	for jobPhase != CompletedPhase {
		taskInfo := CallForTask(doneId)
		jobPhase = taskInfo.Phase
		doneId = taskInfo.TaskId
		switch jobPhase {
		case MapPhase:
			doMap(taskInfo, nReduce, mapf)
		case ReducePhase:
			doReduce(taskInfo, nMap, reducef)
		case WaitPhase:
			doWait()
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(taskInfo TaskInfo, nReduce int, mapf func(string, string) []KeyValue) {
	filename := taskInfo.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	writeToMapFile(taskInfo.TaskId, intermediate, nReduce)
}

func writeToMapFile(taskId int, intermediate []KeyValue, nReduce int) {
	mapFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		interFileName := fmt.Sprintf("mr-%d-%d", taskId, i)
		mapFiles[i], _ = os.Create(interFileName)
		defer mapFiles[i].Close()
	}
	for _, kv := range intermediate {
		reduceJobID := ihash(kv.Key) % 10
		// 确定对应的reduce任务
		enc := json.NewEncoder(mapFiles[reduceJobID])
		enc.Encode(&kv)
	}
}

func doReduce(taskInfo TaskInfo, nMap int, reducef func(string, []string) string) {
	intermediate := readMapFiles(taskInfo, nMap)
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskInfo.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

}

func readMapFiles(taskInfo TaskInfo, nMap int) []KeyValue {
	var kva []KeyValue
	for j := 0; j < nMap; j++ {
		fileName := fmt.Sprintf("mr-%d-%d", j, taskInfo.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
			// 保存结果
		}
	}
	return kva
}

func doWait() {
	time.Sleep(time.Duration(1) * time.Second)
}

func CallForTask(doneId int) TaskInfo {
	args := CallTaskArgs{DoneId: doneId}
	reply := CallTaskReply{}

	call("Coordinator.AssignWork", &args, &reply)
	return reply
}

func CallInit() (int, int) {
	args := InitArgs{}
	reply := InitReply{}

	call("Coordinator.InitWorker", &args, &reply)
	return reply.NMap, reply.NReduce
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
