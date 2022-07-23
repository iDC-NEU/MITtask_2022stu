package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type TaskRequest struct {
	MachineId int
}

type TaskResponse struct {
	Filename    string
	NMapTask    int
	NReduceTask int
	TaskNumber  int
	State       int //0 map 1 reduce 2 无任务等待分配 3完成
	MachineId   int
}

type TaskFinRequest struct {
	State      int
	TaskNumber int
}

type TaskFinResponse struct {
	State int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
