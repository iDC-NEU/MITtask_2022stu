package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"
import "time"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// declare an argument structure.
		args := TaskRequest{}
		// declare a reply structure.
		reply := TaskReply{}
		CallPushTask(&args, &reply)

		if reply.State == 0 {
			file, err := os.Open(reply.Task.FileName)
			if err !=nil {
				log.Fatalf("cannot open MapTask %v", reply.Task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read MapTask %v", reply.Task.FileName)
			}
			file.Close()
			kva := mapf(reply.Task.FileName, string(content))
			
			bucket := make([][]KeyValue, reply.MaxReduces)
			for _, kv := range kva {
				ReId := ihash(kv.Key) % reply.MaxReduces
				bucket[ReId] = append(bucket[ReId], kv)
			}
			id := strconv.Itoa(reply.Task.MapId)
			for i := 0; i < reply.MaxReduces; i++ {
				TmpFile, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					log.Fatalf("cannot open Map TmpFile: %v", TmpFile)
				}
				enc := json.NewEncoder(TmpFile)
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatalf("Encoder error")
				}
				TmpFile.Close()
				OutFilename := "mr-" + id + "-" + strconv.Itoa(i)
				os.Rename(TmpFile.Name(), OutFilename)
			}
			CallGetFinish(reply.Task.MapId)
		} else if reply.State == 1 {
			id := strconv.Itoa(reply.Task.ReduceId)
			intermediate := []KeyValue{}
			for i:=0; i<reply.MaxMaps;i++ {
				MapFile := "mr-" + strconv.Itoa(i) + "-" + id
				InputFile, err := os.OpenFile(MapFile, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("Encoder open ReduceTask InputFile: %v", MapFile)
				}
				dec := json.NewDecoder(InputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}

			sort.Sort(ByKey(intermediate))

			OutFilename := "mr-out-" + id

			TmpFile, err := ioutil.TempFile("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("Encoder open Reduce Tempfile: %v", TmpFile)
			}
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
				fmt.Fprintf(TmpFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			TmpFile.Close()
			os.Rename(TmpFile.Name(), OutFilename)
			CallGetFinish(reply.Task.ReduceId)	
		} else {
			break
		}
		time.Sleep(time.Second)
	}
	

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallPushTask(args *TaskRequest, reply *TaskReply) {

	// send the RPC request, wait for the reply.
	call("Coordinator.PushTask", &args, &reply)
	// if ok {
	// 	fmt.Printf("call PushTask success!\n")
	// } else {
	// 	fmt.Printf("call PushTask failed!\n")
	// }
}

func CallGetFinish(id int) {

	// send the RPC request, wait for the reply.
	// declare an argument structure.
	args := FinishRequest{}
	args.Id = id
	// declare a reply structure.
	reply := FinishReply{}
	call("Coordinator.GetFinish", &args, &reply)
	// if ok {
	// 	fmt.Printf("call GetFinish success!\n")
	// } else {
	// 	fmt.Printf("call GetFinish failed!\n")
	// }

	// reply.FileName.
	//fmt.Printf("reply.FileName %s\n", reply.Task.FileName)
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
