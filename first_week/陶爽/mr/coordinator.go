package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Task struct {
	Filename  string //打开的文件名
	MachineId int    //执行该任务的机器id
	State     int    // 0 表示未做，1表示正在做,2表示完成
	Runtime   int    //已经运行的时间，每次始终滴答，该记录+1
}

type Coordinator struct {
	// Your definitions here.
	State       int //0 map 1 reduce
	NMapTask    int
	NReduceTask int
	MapTask     map[int]*Task
	ReduceTask  map[int]*Task
	MachineNum  int        //用来给机器分配machine id 从1开始分配
	Mu          sync.Mutex //只能有一个worker访问
}

func (c *Coordinator) TimeTick() {
	c.Mu.Lock()
	if c.State == 0 {
		for taskNumber, task := range c.MapTask {
			if task.State == 1 {
				c.MapTask[taskNumber].Runtime = c.MapTask[taskNumber].Runtime + 1
				if c.MapTask[taskNumber].Runtime >= 10 { //超过10个始终滴答，默认认为该任务的主机已经挂了
					c.MapTask[taskNumber].State = 0
				}
			}
		}
	} else if c.State == 1 {
		for taskNumber, task := range c.ReduceTask { //遍历taskNumber
			if task.State == 1 {
				c.ReduceTask[taskNumber].Runtime = c.ReduceTask[taskNumber].Runtime + 1
				if c.ReduceTask[taskNumber].Runtime >= 10 {
					c.ReduceTask[taskNumber].State = 0
				}
			}
		}
	}
	c.Mu.Unlock()
}

func (c *Coordinator) UpdateMasterState() {
	for _, task := range c.MapTask {
		if task.State == 0 || task.State == 1 {
			c.State = 0 //处于map阶段
			return
		}
	}
	for _, task := range c.ReduceTask {
		if task.State == 0 || task.State == 1 {
			c.State = 1 //处于reduce阶段
			return
		}
	}
	c.State = 2
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

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	c.Mu.Lock()
	reply.State = 2
	reply.NMapTask = c.NMapTask
	reply.NReduceTask = c.NReduceTask
	if args.MachineId == 0 { //分配机器号
		c.MachineNum = c.MachineNum + 1
		reply.MachineId = c.MachineNum
	} else {
		reply.MachineId = args.MachineId
	}
	if c.State == 0 { //map
		for taskNumber, task := range c.MapTask {
			if task.State == 0 {
				reply.State = 0
				reply.Filename = task.Filename
				reply.TaskNumber = taskNumber
				c.MapTask[taskNumber].State = 1 //正在做
				break
			}
		}
	} else if c.State == 1 { //reduce
		for taskNumber, task := range c.ReduceTask {
			if task.State == 0 {
				reply.State = 1
				reply.TaskNumber = taskNumber
				c.ReduceTask[taskNumber].State = 1 //正在做
				break
			}
		}
	}
	c.Mu.Unlock()
	return nil
}

func (c *Coordinator) TaskFin(args *TaskFinRequest, reply *TaskFinResponse) error {
	c.Mu.Lock()
	//println("master called FinishTask")
	reply.State = 0
	if args.State == 0 {
		c.MapTask[args.TaskNumber].State = 2 //完成
		c.UpdateMasterState()
		// fmt.Print(m.State)
	} else if args.State == 1 {
		c.ReduceTask[args.TaskNumber].State = 2
		c.UpdateMasterState()
		if c.State == 2 { //所有任务都已经完成
			reply.State = 1
		}
	}
	c.Mu.Unlock()
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
	if c.State == 2 {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTask := make(map[int]*Task)
	reduceTask := make(map[int]*Task)
	for i, filename := range files {
		mapTask[i] = &Task{Filename: filename, MachineId: 0, State: 0, Runtime: 0}
	}
	for j := 0; j < nReduce; j++ {
		//mr-x-y  范围: x[0,7] y[0, 9]
		reduceTask[j] = &Task{Filename: "", MachineId: 0, State: 0, Runtime: 0}
	}
	c := Coordinator{State: 0, NMapTask: len(files), NReduceTask: nReduce, MapTask: mapTask, ReduceTask: reduceTask, MachineNum: 0, Mu: sync.Mutex{}}

	// Your code here.

	c.server()
	return &c
}
