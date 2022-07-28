package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	TaskId      int       //id of the task
	TaskType    int       //type of the task (map/reduce)
	TaskState   int       //state of the task
	StartedTime time.Time //to record when the task was assigned
	FileName    string    //using in maptask
	MapNum      int
	ReduceNum   int
}

const ( //issuccess
	Fail    = 0
	Success = 1
)

const ( // task type
	//noTask     = 0
	MapTask    = 1
	ReduceTask = 2
)

const ( //task state
	Waitting   = 0
	Processing = 1
	Finished   = 2
)

const ( //maintaskState
	GetNoTasks  = 0
	DoingMap    = 1
	DoingReduce = 2
	Completeed  = 3
)

type Coordinator struct {
	// Your definitions here.
	MapTasks      []Task
	ReduceTasks   []Task
	MapNum        int
	ReduceNum     int
	Mu            sync.Mutex //share lock
	MainTaskState int
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
	if c.MainTaskState == Completeed {
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
	c := Coordinator{
		MapTasks:      []Task{},
		ReduceTasks:   []Task{},
		MapNum:        len(files),
		ReduceNum:     nReduce,
		MainTaskState: GetNoTasks, //at first no tasks get
	}

	// Your code here.
	//fmt.Println("nreduce is")
	//fmt.Println(nReduce)

	GetMapTasks(files, &c)
	GetReduceTasks(nReduce, &c)

	c.MainTaskState = DoingMap

	c.server()
	return &c
}

func GetMapTasks(files []string, c *Coordinator) {
	for i := 0; i < len(files); i++ {
		c.MapTasks = append(c.MapTasks, Task{
			TaskId:    i,
			TaskType:  MapTask,
			TaskState: Waitting,
			FileName:  files[i],
			MapNum:    c.MapNum,
			ReduceNum: c.ReduceNum,
		})
	}
}

func GetReduceTasks(nreduce int, c *Coordinator) {
	//fmt.Println("c.rn is")
	//fmt.Println(c.ReduceNum)
	for i := 0; i < nreduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			TaskId:    i,
			TaskType:  ReduceTask,
			TaskState: Waitting,
			MapNum:    c.MapNum,
			ReduceNum: c.ReduceNum,
		})
	}
}

//assign the tasks
func (c *Coordinator) AssignTasks(args *AssignTasksArgs, reply *AssignTasksReply) error {

	//assign the task which is not assigned or overtime
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.MainTaskState == GetNoTasks {
		//no task, fail to assign tasks
		reply.IsSuccess = Fail
	}

	if c.MainTaskState == DoingMap {
		//assign map tasks
		reply.IsSuccess = Fail
		for i, v := range c.MapTasks {
			if v.TaskState == Waitting {

				PutTaskToWorker(reply, v)

				c.MapTasks[i].StartedTime = time.Now() //renew the time
				c.MapTasks[i].TaskState = Processing
				reply.IsSuccess = Success
				return nil
			}
			if v.TaskState == Processing && time.Now().Sub(v.StartedTime) > time.Second*10 {

				PutTaskToWorker(reply, v)

				c.MapTasks[i].StartedTime = time.Now()
				c.MapTasks[i].TaskState = Processing
				reply.IsSuccess = Success
				return nil
			}

		}
	}
	if c.MainTaskState == DoingReduce {
		//assign reduce task
		reply.IsSuccess = Fail
		for i, v := range c.ReduceTasks {
			if v.TaskState == Waitting {

				PutTaskToWorker(reply, v)

				fmt.Println("reduce task assigned", v.TaskId)

				c.ReduceTasks[i].StartedTime = time.Now() //renew the time
				c.ReduceTasks[i].TaskState = Processing
				reply.IsSuccess = Success
				return nil
			}
			if v.TaskState == Processing && time.Now().Sub(v.StartedTime) > time.Second*10 {

				PutTaskToWorker(reply, v)

				c.ReduceTasks[i].StartedTime = time.Now()
				c.ReduceTasks[i].TaskState = Processing
				reply.IsSuccess = Success
			}
		}
	}
	reply.IsSuccess = Fail
	return nil
}

func PutTaskToWorker(reply *AssignTasksReply, task Task) {
	reply.Task.TaskId = task.TaskId
	reply.Task.TaskType = task.TaskType
	reply.Task.TaskState = Processing
	reply.Task.MapNum = task.MapNum
	reply.Task.ReduceNum = task.ReduceNum
	if task.TaskType == MapTask {
		reply.Task.FileName = task.FileName
	}

}

func (c *Coordinator) ReportFinish(args *ReportFinishArgs, reply *ReportFinishReply) error {
	//renew the states

	c.Mu.Lock()

	if args.Tasktype == MapTask {
		c.MapTasks[args.TaskId].TaskState = Finished
		// map task finished
	}
	if args.Tasktype == ReduceTask {
		c.ReduceTasks[args.TaskId].TaskState = Finished
	}
	//check main task state
	c.Mu.Unlock()
	UpdateMainState(c)
	return nil
}

func UpdateMainState(c *Coordinator) {

	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.MainTaskState == DoingMap {
		//processing map
		check := 1
		for _, v := range c.MapTasks {
			if v.TaskState == Waitting || v.TaskState == Processing {
				check = 0 // not finish
			}
		}
		if check == 1 {
			c.MainTaskState = DoingReduce //update
			fmt.Println("doing reduce!")
		}
	}

	if c.MainTaskState == DoingReduce {
		check := 1
		for _, v := range c.ReduceTasks {
			if v.TaskState == Waitting || v.TaskState == Processing {
				check = 0
			}
		}
		if check == 1 {
			c.MainTaskState = Completeed //finish all
			fmt.Println("finished!")
		}
	}
}
