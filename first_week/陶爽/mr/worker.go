package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	machineid := 0
	for {
		args := TaskRequest{MachineId: machineid}
		reply := TaskResponse{}
		CallGetTask(&args, &reply)
		//time.Sleep(time.Second)
		machineid = reply.MachineId
		tasknumber := reply.TaskNumber
		//reply.State 0 map 1 reduce 2 等待分配
		if reply.State == 0 { //正在执行map
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open MapTask %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			numreduce := reply.NReduceTask
			bucket := make([][]KeyValue, numreduce)
			for _, kv := range kva {
				no := (ihash(kv.Key) % numreduce)
				bucket[no] = append(bucket[no], kv)
			}
			for i, KeyValueNum := range bucket {
				outFileName := "mr-" + strconv.Itoa(tasknumber) + "-" + strconv.Itoa(i)
				tmpFile, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					log.Fatalf("cannot open tmpFile")
				}
				enc := json.NewEncoder(tmpFile)
				err := enc.Encode(KeyValueNum)
				if err != nil {
					log.Fatalf("encode bucket error")
				}
				tmpFile.Close()
				os.Rename(tmpFile.Name(), outFileName)
			}
			//CallTaskFin()
		} else if reply.State == 1 {
			intermediate := []KeyValue{}
			numMap := reply.NMapTask
			id := strconv.Itoa(tasknumber)
			for i := 0; i < numMap; i++ {
				mapOutFilename := "mr-" + strconv.Itoa(i) + "-" + id
				inputFile, err := os.OpenFile(mapOutFilename, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", mapOutFilename)
				}
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}
			sort.Sort(ByKey(intermediate))
			outFileName := "mr-out-" + id
			tmpReduceOutFile, err := ioutil.TempFile("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("cannot open tmpreduceoutfile")
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
				fmt.Fprintf(tmpReduceOutFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			tmpReduceOutFile.Close()
			os.Rename(tmpReduceOutFile.Name(), outFileName)
			//CallTaskFin()
		} else if reply.State == 2 {
			continue
		} else if reply.State == 3 {
			break
		}
		finishargs := TaskFinRequest{State: reply.State, TaskNumber: tasknumber}
		finishreply := TaskFinResponse{}
		CallTaskFin(&finishargs, &finishreply)
		if finishreply.State == 1 {
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallGetTask(args *TaskRequest, reply *TaskResponse) {

	// declare an argument structure.

	// send the RPC request, wait for the reply.
	call("Coordinator.GetTask", &args, &reply)

	// reply.Y should be 100.
}

func CallTaskFin(args *TaskFinRequest, reply *TaskFinResponse) {
	call("Coordinator.TaskFin", &args, &reply)

}

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
