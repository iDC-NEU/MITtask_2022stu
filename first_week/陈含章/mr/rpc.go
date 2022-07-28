package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AssignTasksArgs struct {
}

type AssignTasksReply struct {
	Task      Task
	IsSuccess int
	Close     int //if close = 1 then close worker
}

type ReportFinishArgs struct {
	TaskId   int
	Tasktype int
}

type ReportFinishReply struct {
}

//type task struct {
//	taskId      int       //id of the task
//	taskType    int       //type of the task (map/reduce)
//	taskState   int       //state of the task
//	startedTime time.Time //to record when the task was assigned
//	fileName    string    //using in maptask
//	mapNum      int
//	reduceNum   int
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
