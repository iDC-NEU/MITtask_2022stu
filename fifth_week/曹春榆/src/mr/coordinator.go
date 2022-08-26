package mr

import (
	"container/list"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TimeOut = 10

type JobPhase int

//type TaskState int
//// iota 初始化后会自动递增
//const (
//	Idle      TaskState = iota // value --> 0
//	InProcess                  // value --> 1
//	Completed                  // value --> 2
//)
//

type Task struct {
	//taskState   TaskState
	fileName    string
	taskId      int
	processTime int64
}

const (
	MapPhase JobPhase = iota
	WaitPhase
	ReducePhase
	CompletedPhase
)

type Coordinator struct {
	// Your definitions here.
	nMap          int
	mapDoneNum    int
	nReduce       int
	reduceDoneNum int
	jobPhase      JobPhase

	mapWorkTask    chan Task
	reduceWorkTask chan Task
	mapTasks       []Task
	reduceTaskTime []int64

	mapTaskDone    []bool
	reduceTaskDone []bool

	waitListMutex  sync.Mutex
	confirmMutex   sync.Mutex
	waitWorkIdList *list.List
}

func (c *Coordinator) init(nMap int, nReduce int) {
	c.nMap = nMap
	c.mapDoneNum = 0
	c.nReduce = nReduce
	c.reduceDoneNum = 0
	c.jobPhase = MapPhase

	c.mapWorkTask = make(chan Task, nMap)
	c.reduceWorkTask = make(chan Task, nReduce)

	c.mapTasks = make([]Task, nMap, nMap)
	c.reduceTaskTime = make([]int64, nReduce, nReduce)

	c.mapTaskDone = make([]bool, nMap, nMap)
	c.reduceTaskDone = make([]bool, nReduce, nReduce)

	c.waitWorkIdList = list.New()

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

func (c *Coordinator) InitWorker(args *InitArgs, reply *InitReply) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignWork(args *CallTaskArgs, reply *CallTaskReply) error {
	c.confirmWork(args.DoneId)
	c.trySwitchPhase()
	switch c.jobPhase {
	case MapPhase:
		c.AssignMapWork(reply)
	case ReducePhase:
		c.AssignReduceWork(reply)
	case CompletedPhase:
		c.AssignCompletedWork(reply)
	}
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

	// Your code here.

	return c.jobPhase == CompletedPhase
}

func (c *Coordinator) addTasks(files []string) {
	for i, file := range files {
		c.mapTasks[i] = Task{
			fileName:    file,
			taskId:      i,
			processTime: 0,
		}
		c.mapWorkTask <- c.mapTasks[i]
	}

	for i := 0; i < c.nReduce; i++ {
		c.reduceWorkTask <- Task{
			fileName:    "",
			taskId:      i,
			processTime: 0,
		}
	}

}

func (c *Coordinator) checkOvertime() {
	time.Sleep(time.Duration(TimeOut) * time.Second)
	var next *list.Element
	for c.jobPhase != CompletedPhase {
		currentTime := time.Now().Unix()
		minTime := TimeOut
		for ele := c.waitWorkIdList.Front(); ele != nil; ele = next {
			next = ele.Next()
			taskId := ele.Value.(int)
			interval := currentTime - c.getProcessTime(taskId)
			//fmt.Println("checkOvertime", taskId, interval)
			if interval > 0 && int(interval) < minTime {
				minTime = int(interval)
			}
			if c.checkWorkDone(taskId) {
				c.waitListMutex.Lock()
				c.waitWorkIdList.Remove(ele)
				c.waitListMutex.Unlock()
			} else if currentTime-c.getProcessTime(taskId) > TimeOut {
				c.waitListMutex.Lock()
				c.waitWorkIdList.Remove(ele)
				c.waitListMutex.Unlock()
				c.addTask(taskId)
			}
		}
		time.Sleep(time.Duration(minTime) * time.Second)
	}
}

func (c *Coordinator) confirmWork(doneId int) {
	if doneId == -1 {
		return
	}
	switch c.jobPhase {
	case MapPhase:
		c.mapTaskDone[doneId] = true
		c.confirmMutex.Lock()
		c.mapDoneNum += 1
		c.confirmMutex.Unlock()
	case ReducePhase:
		c.reduceTaskDone[doneId] = true
		c.confirmMutex.Lock()
		c.reduceDoneNum += 1
		c.confirmMutex.Unlock()
	}
}

func (c *Coordinator) trySwitchPhase() {
	switch c.jobPhase {
	case MapPhase:
		if c.mapDoneNum == c.nMap {
			c.jobPhase = ReducePhase
		}
	case ReducePhase:
		if c.reduceDoneNum == c.nReduce {
			c.jobPhase = CompletedPhase
		}
	}
}

func (c *Coordinator) AssignMapWork(reply *CallTaskReply) {
	select {
	case task := <-c.mapWorkTask:
		reply.Phase = MapPhase
		reply.TaskId = task.taskId
		reply.FileName = task.fileName
		// 记录任务时间
		c.mapTasks[task.taskId].processTime = time.Now().Unix()
		c.waitListMutex.Lock()
		c.waitWorkIdList.PushBack(task.taskId)
		c.waitListMutex.Unlock()
	default:
		reply.Phase = WaitPhase
		reply.TaskId = -1
	}
}

func (c *Coordinator) AssignReduceWork(reply *CallTaskReply) {
	select {
	case task := <-c.reduceWorkTask:
		reply.Phase = ReducePhase
		reply.TaskId = task.taskId
		reply.FileName = task.fileName
		// 记录任务时间
		c.reduceTaskTime[task.taskId] = time.Now().Unix()
		c.waitListMutex.Lock()
		c.waitWorkIdList.PushBack(task.taskId)
		c.waitListMutex.Unlock()
	default:
		reply.Phase = WaitPhase
		reply.TaskId = -1
	}
}

func (c *Coordinator) AssignCompletedWork(reply *CallTaskReply) {
	reply.Phase = CompletedPhase
	reply.TaskId = -1
}

func (c *Coordinator) checkWorkDone(id int) bool {
	switch c.jobPhase {
	case MapPhase:
		return c.mapTaskDone[id]
	case ReducePhase:
		return c.reduceTaskDone[id]
	}
	return true
}

func (c *Coordinator) getProcessTime(taskId int) int64 {
	switch c.jobPhase {
	case MapPhase:
		return c.mapTasks[taskId].processTime
	case ReducePhase:
		return c.reduceTaskTime[taskId]
	}
	return time.Now().Unix()
}

func (c *Coordinator) addTask(taskId int) {
	switch c.jobPhase {
	case MapPhase:
		c.mapWorkTask <- c.mapTasks[taskId]
	case ReducePhase:
		c.reduceWorkTask <- Task{
			taskId: taskId,
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.init(len(files), nReduce)
	c.addTasks(files)

	// Your code here.

	c.server()
	go c.checkOvertime()
	return &c
}
