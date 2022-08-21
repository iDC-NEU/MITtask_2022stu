package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"encoding/json"
	"sort"
	"log"
	"time"
)

import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type WorkInfo struct {		//暂时未使用，在crash处理中可能会有需要
	WorkState WorkState
	WorkTask *Task
}
type WorkState int		
const( 
	WorkFree WorkState = iota
	WorkRunning
	WorkWaiting
	WorkCrash
)

type SortedKey []KeyValue

//  对struct排序需要重写len swap less函数
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// type TaskArgs struct{}
func FetchTask() Task{
	args := Task{}
	reply := Task{}
	// fmt.Println("reply:",reply)
	//存在一个尚未解决的问题：RPC调用之后获得需要执行的任务。task struct内除了nReduce无法正确赋值外，其余变量的值均正常
	//目前认为call 使用反射机制处理RPC传入reply是一个指针类型，传入DisTask()函数后应该是指针赋值，
	//形参reply和从MapTaskCh中获取局部变量task理应指向同一地址，暂不清楚为什么成员变量nReduce无法正确赋值
	ok :=call("Coordinator.DisTask",&args,&reply)
	reply.nReduce=10
	if ok && reply.TaskType!=Finished {
		fmt.Println("成功获取一个任务! 任务内容为:",reply)
	} else {
		fmt.Println("获取任务失败!")
	}
	return reply
	// return args
}
func DoMap(mapf func(string,string) []KeyValue,task *Task){
	var intermediate []KeyValue
	filename := task.TaskName
	// fmt.Println("DoMap task内容:",task)
	file, err := os.Open(filename[0])
	if err != nil {
		fmt.Println("打开文件失败！")
	}
	// 使用ioutil获取conten,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("读取文件内容失败！")
	}
	file.Close()
	// map返回一组KV结构体数组
	intermediate = mapf(filename[0], string(content))

	//initialize and loop over []KeyValue
	rn := task.nReduce
	// fmt.Println("rn:",rn)
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	//按照hash函数将相应的kv对存入二维数组
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}
func DoReduce(reducef func(string,[]string) string,task *Task){
	reduceFileNum := task.TaskId
	intermediate := sortContent(task.TaskName)
	// fmt.Println(intermediate)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir ,"mr-tmp-*")
	if err != nil {
		fmt.Println("创建临时文件失败！")
	}

	//统计排序后某个key值连续相同的次数，并将其放入一个数组，传入reducef计算结果
	i := 0
	//找到相同key串的起始，结束位置的下标
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		//从下一种key处继续处理
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)

}
// 对struct按照key值进行排序并返回有序的kv数组
func sortContent(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}


func DoFinish(task *Task){
	args := task
	reply := Task{}
	fmt.Println("Do finish:",args)
	ok := call("Coordinator.Finish",args,&reply)
	if ok && task.TaskType == MapTask{
		fmt.Println("任务id为",task.TaskId," 的map任务已经完成!")
	}else if ok && task.TaskType == ReduceTask{
		fmt.Println("任务id为",task.TaskId," 的reduce任务已经完成!")
	}else {
		fmt.Println("修改任务状态至Done时出现错误!")
	}
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// initTask:=Task{}
	// worker:=WorkInfo{
	// 	WorkState:WorkFree,
	// 	WorkTask:&initTask,
	// }
	
	flag:=true
	for flag{
		// if worker.WorkState == WorkFree ||  worker.WorkState == WorkWaiting{
			task:=FetchTask()
			// fmt.Println("FetchTask task内容:",task)
			switch task.TaskType {
			case MapTask:
				{
					// fmt.Println("FetchWork Success!")
					// worker.WorkTask=&task
					// worker.WorkState=WorkRunning
					DoMap(mapf,&task)
					DoFinish(&task)			//在map或reduce完成后 及时调整任务state，同时需要修稿coordinator taskmap中相应任务的state
					// worker.WorkState=WorkFree
				}
			case ReduceTask:
				{
					// worker.WorkTask=&task
					// worker.WorkState=WorkRunning
					DoReduce(reducef,&task)
					DoFinish(&task)
					// worker.WorkState=WorkFree
				}
			case Finished:
				{
					fmt.Println("当前阶段所有任务已经完成！")
					// worker.WorkState=WorkWaiting
					time.Sleep(time.Second*5)
				}
			}
		// }else if worker.WorkState == WorkWaiting{
			// time.Sleep(time.Second*5)
		// }else if worker.WorkState == WorkCrash{
		// 	//出现故障处理 coordinator会将该worker置为crash状态，并将
		// }

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
	// fmt.Println("call reply",reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
