package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	closeWorker := 0 //when closeWorker = 1 close worker
	for closeWorker == 0 {
		reply, err := AskForTask()
		if err != nil {
			//fail
			continue
		}
		if reply.Close == 1 {
			//close worker process
			closeWorker = 1
			continue
		}
		if reply.IsSuccess == Fail {
			//ask for task again
			time.Sleep(time.Second)
			continue
		}
		if reply.IsSuccess == Success {
			if reply.Task.TaskType == MapTask {
				//fmt.Println(reply.Task.TaskState)
				//do map task
				DoMapTask(mapf, reply.Task)
				ReportFinish(reply.Task.TaskType, reply.Task.TaskId)

			} else if reply.Task.TaskType == ReduceTask {
				//do reduce task
				DoReduceTask(reducef, reply.Task)
				ReportFinish(reply.Task.TaskType, reply.Task.TaskId)
			}
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func AskForTask() (AssignTasksReply, error) {
	args := AssignTasksArgs{}
	reply := AssignTasksReply{}
	ret := call("Coordinator.AssignTasks", &args, &reply)
	if ret {
		return reply, nil
	}
	return AssignTasksReply{}, errors.New("Connect Failed!")
	//return nil
}

func ReportFinish(taskType, taskId int) {
	args := ReportFinishArgs{
		Tasktype: taskType,
		TaskId:   taskId,
	}
	reply := ReportFinishReply{}
	call("Coordinator.ReportFinish", &args, &reply)
}

func DoReduceTask(reducef func(string, []string) string, task Task) {
	fmt.Printf("Reduce worker get task %d\n", task.TaskId)

	//read files
	kvMap := map[string][]string{}
	nMap := task.MapNum
	for i := 0; i < nMap; i++ {

		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId)

		file, err := os.Open(filename)
		//defer file.Close()

		if err != nil {
			fmt.Println("Open " + filename + " failed")
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	// sort
	keys := []string{}
	for key, _ := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	outputFile, _ := os.Create("mr-out-" + strconv.Itoa(task.TaskId))
	defer outputFile.Close()

	//syscall.Flock(int(outputFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	//defer syscall.Flock(int(outputFile.Fd()), syscall.LOCK_UN)
	for _, key := range keys {
		countString := reducef(key, kvMap[key])
		outputFile.WriteString(fmt.Sprintf("%v %v\n", key, countString))
	}

	fmt.Printf("Reduce worker finish task %d\n", task.TaskId)

}

func DoMapTask(mapf func(string, string) []KeyValue, task Task) {
	fmt.Printf("Map worker get task %d-%s\n", task.TaskId, task.FileName)
	fmt.Println(task.ReduceNum)
	fmt.Println(task.MapNum)

	//read the files
	file, err := os.Open(task.FileName)
	defer file.Close()
	if err != nil {
		fmt.Println("Cannot open %v", task.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Cannot read %v", task.FileName)
	}

	//do map
	kva := mapf(task.FileName, string(content))

	rn := task.ReduceNum
	//rn = 0
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range kva {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		//oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		//oname := intermediateFilename(task.TaskId, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
