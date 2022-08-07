package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

type test struct{
	a int
	b string
}

// Add your RPC definitions here.
type Task struct{
	TaskName  []string		//应该需要传入一个string数组 因为在map阶段每个任务对应一个文件 而在reduce阶段 一个任务对应一组中间文件
	TaskId int
	nReduce int
	TaskType TaskType			//任务类型1:map 2:reduce 3:finished	
	TaskState TaskState			//任务执行状态
}
type Stage int		//1:map 2:reduce	3:finished
const(
	MapStage Stage = iota
	ReduceStage
	FinishedStage
)

type TaskType int
const(
	MapTask TaskType = iota
	ReduceTask 
	Finished
)
type TaskState int
const(
	Running TaskState = iota
	Free
	Done

)
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
