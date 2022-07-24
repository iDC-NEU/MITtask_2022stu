package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskInfo struct {
	FileName string
	MapId int
	ReduceId int
	Runtime int
	State int //both runtime and state are used to help the worker unworking
}

type Coordinator struct {
	// Your definitions here.
	State int //0:start 1:map 2:Reduce 3:finish
	Mutex          sync.Mutex

	MaxMaps int
	MaxReduces int //the max num of map and reduce task

	CurMapT map[int]*TaskInfo  
	CurReduceT map[int]*TaskInfo //runnind MAP

	MapT chan TaskInfo
	ReduceT chan TaskInfo //use the chain for security

	MapFinish chan bool
	ReduceFinish chan bool
}

func (c *Coordinator) CheckRuntime() {//can't run ,need to modify after studting the "heart map"
	c.Mutex.Lock()
	time.Sleep(time.Second*2)
	if c.State == 0 {
		for i, task := range c.CurMapT {
			if task.State == 1 {
				c.CurMapT[i].Runtime = c.CurMapT[i].Runtime + 2
				if c.CurMapT[i].Runtime >= 10 { 
					c.CurMapT[i].State = 0
					c.MapT <- *task
					fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>map %d renwu: %d\n", i , c.CurMapT[i].Runtime)
				}
			}
		}
	} else if c.State == 1 {
		for i, task := range c.CurReduceT {
			if task.State == 1 {
				c.CurReduceT[i].Runtime = c.CurReduceT[i].Runtime + 2
				if c.CurReduceT[i].Runtime >= 10 {
					c.CurReduceT[i].State = 0
					c.ReduceT <- *task
					fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>reduce %d renwu: %d\n", i , c.CurMapT[i].Runtime)
				}
			}
		}
	}
	c.Mutex.Unlock()
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PushTask(args *TaskRequest, reply *TaskReply) error {
	//distribute task
	c.Mutex.Lock()
	if len(c.MapT) !=0 {
		fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>distributing map task\n")
		if c.State == 0 {
			MapTask, ok := <- c.MapT
			if ok {
				reply.Task = MapTask
				MapTask.State = 1
				MapTask.Runtime = 0
				c.CurMapT[MapTask.MapId] = &MapTask
				go c.CheckRuntime()
				fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>map renwu: %d\n",  c.CurMapT[MapTask.MapId].Runtime)
			}
		}
	}
	if c.State == 1 {

		fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>distributing Reduce task\n")
		if len(c.ReduceT) !=0 {
			ReduceTask, ok := <- c.ReduceT
			if ok {
				reply.Task = ReduceTask
				ReduceTask.State = 1
				ReduceTask.Runtime = 0
				c.CurReduceT[ReduceTask.ReduceId] = &ReduceTask
				go c.CheckRuntime()
				fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>after distributing map task\n")
			}
		}
	}
	reply.State = c.State
	reply.MaxMaps = c.MaxMaps
	reply.MaxReduces = c.MaxReduces
	c.Mutex.Unlock()

	return nil
}

func (c *Coordinator) GetFinish(args *FinishRequest, reply *FinishReply) error { 
	//the data race happended. why the MUTEX not work? I don't Know the reason after debugging once by once
	c.Mutex.Lock()
	if len(c.MapFinish) != c.MaxMaps {
		c.MapFinish <- true
		c.CurMapT[args.Id].State = 2
		if (len(c.MapFinish) == c.MaxMaps){
			c.State = 1
		}

	}else if len(c.ReduceFinish) != c.MaxReduces {
		c.ReduceFinish <- true
		c.CurReduceT[args.Id].State = 2
		if (len(c.ReduceFinish) == c.MaxReduces){
			c.State = 2
		}
	}
	c.Mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// // Your code here.
	if c.State == 2 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{State:0,MaxReduces:nReduce,MaxMaps:len(files),
		MapT:make(chan TaskInfo, len(files)),ReduceT:make(chan TaskInfo, nReduce),
		CurMapT:make(map [int]*TaskInfo), CurReduceT:make(map [int]*TaskInfo),
		MapFinish:make(chan bool, len(files)),ReduceFinish:make(chan bool, nReduce)}

	// Your code here.
	for i, file := range files {
		c.MapT <- TaskInfo{FileName: file, MapId: i, State: 0}
	}

	for i:= 0; i<nReduce; i++ {
		c.ReduceT <- TaskInfo{ReduceId: i, State: 0}
	} 

	c.server()
	return &c
}
