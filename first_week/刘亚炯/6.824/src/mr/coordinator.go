package mr


import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
	"sync"
)


import "net"
import "os"
import "net/rpc"
import "net/http"

var signal  =1 	//用于并发控制worker访问coordinator 信号量控制并发可能会出错，这里使用更方便的互斥锁
var (
	mu sync.Mutex
)
type Coordinator struct {
	// Your definitions here.
	TaskId int									//任务的id
	Stage Stage								//程序目前处于什么状态 map or reduce
	ReduceNum int
	MapTaskCh chan *Task	//使用channel保证并发安全
	ReduceTaskCh chan *Task
	files []string
	MapTaskMap map[int]*TaskInfo 	//保存map任务的各种信息，用于判断stage改变和crash处理
	ReduceTaskMap map[int]*TaskInfo		////保存reduce任务的各种信息，用于判断stage改变和crash处理
}
type TaskInfo struct{
	StartTime time.Time  		//在taskinfo内添加time，同于检测是否有超时的任务
	state TaskState
	TaskAddr *Task
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

func(c *Coordinator)checkStageFinished() bool{
	var (
		mapDoneNum      = 0			//已经完成的map任务数
		mapUnDoneNum    = 0		//尚未完成的map任务数
		reduceDoneNum   = 0		//已经完成的reduce任务数
		reduceUnDoneNum = 0		//尚未完成的reduce任务数
	)

	// 遍历储存task信息的map 根据任务的类型计算上述四种任务的数量
	for _, v := range c.MapTaskMap {
		if v.state == Done {
			mapDoneNum++
		} else {
			mapUnDoneNum++
		}
	}
	for _, m := range c.ReduceTaskMap {
		if m.state == Done {
			reduceDoneNum++
			} else {
				reduceUnDoneNum++
		}
	}

	// 如果map或者reduce中的任务全部完成了，就需要切换到下一阶段，返回true
	//start->mapStage->ReduceStage->FinishedStage
	fmt.Println("mapDoneNum:",mapDoneNum,"mapUnDoneNum:",mapUnDoneNum,
	"reduceDoneNum:",reduceDoneNum,"reduceUnDoneNum:",reduceUnDoneNum)
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

func(c *Coordinator)changeStage() {
	if c.Stage == MapStage {
		c.Stage = ReduceStage
		//执行reduce操作
		c.getReduceTasks()
		// c.Stage = FinishedStage
	}else if c.Stage== ReduceStage {
		c.Stage = FinishedStage
	}
}

//添加对任务state的判断和更改，并据此调整stage
func(c *Coordinator)DisTask(args *Task,reply *Task) error {
	//使用互斥锁保证并发
	mu.Lock()
	//defer表示在当前函数执行完成后才会执行defer后面的函数 即释放互斥锁
	defer mu.Unlock()
	if signal==1{
		signal=0
		switch(c.Stage){
		case MapStage:
			{
				if len(c.MapTaskCh)>0{
					*reply=*<-c.MapTaskCh
					reply.TaskState = Running
					c.MapTaskMap[reply.TaskId].state=Running
					c.MapTaskMap[reply.TaskId].TaskAddr=reply
					c.MapTaskMap[reply.TaskId].StartTime = time.Now()
					fmt.Println("Map task内容:",reply)
					// fmt.Println("map内容:",c.MapTaskMap[reply.TaskId].TaskAddr)
					fmt.Println("id为",(*reply).TaskId," 的任务已被分配！")
				}else{
					signal = 1
					fmt.Println("所有任务都已经被分配！")
					reply.TaskType=Finished
					if c.checkStageFinished() {
						c.changeStage()
						fmt.Println("进入reduce阶段!")
					}
					
					return nil
				}
			}
		case ReduceStage:
			{
				if len(c.ReduceTaskCh)>0{
					*reply=*<-c.ReduceTaskCh
					reply.TaskState = Running
					c.ReduceTaskMap[reply.TaskId].state=Running
					c.ReduceTaskMap[reply.TaskId].TaskAddr=reply
					c.ReduceTaskMap[reply.TaskId].StartTime = time.Now()
					fmt.Println("Reduce task内容:",reply)
					fmt.Println("id为",(*reply).TaskId," 的任务已被分配！")
				}else{
					signal = 1
					fmt.Println("所有任务都已经被分配！")
					reply.TaskType=Finished
					if c.checkStageFinished() {
						c.changeStage()
						fmt.Println("进入结束阶段!")
					}
				
					return nil
				}
			}
		default:
			{
				fmt.Println("MapReduce任务已经全部完成!")
			}
		}
		signal = 1
	}
	// fmt.Println("DisTask task内容:",reply)
	return nil
}

func(c *Coordinator)Finish(args *Task,reply *Task) error{
	mu.Lock()
	defer mu.Unlock()
	if signal==1{
		signal = 0
		id:=args.TaskId
		// if *(*(c.MapTaskMap[id]).TaskAddr).TaskState == Running{
	
		// }
		if c.Stage == MapStage{
			// fmt.Println( c.MapTaskMap[id].TaskAddr)
			if c.MapTaskMap[id].TaskAddr.TaskState == Running {
				c.MapTaskMap[id].state=Done
				c.MapTaskMap[id].TaskAddr.TaskState =Done
				fmt.Println("id为  ",args.TaskId," 的任务已经完成!")
			}else {
				fmt.Println("id 为 ",args.TaskId," 的任务状态已经修改为Done!")
			}
		}else if c.Stage == ReduceStage {
			if c.ReduceTaskMap[id].TaskAddr.TaskState == Running {
				c.ReduceTaskMap[id].state=Done
				c.ReduceTaskMap[id].TaskAddr.TaskState =Done
				fmt.Println("id为  ",args.TaskId," 的任务已经完成!")
			}else {
				fmt.Println("id 为 ",args.TaskId," 的任务状态已经修改为Done!")
			}
		}

		signal  = 1
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
	ret := false
	mu.Lock()
	defer mu.Unlock()
	if signal == 1 {
		signal = 0
		if c.Stage == FinishedStage {
			fmt.Println("所有任务都已经完成! Coordinator即将退出")
			ret = true
		}
		signal = 1
	}
	// Your code here.


	return ret
}

func (c *Coordinator) CrashDetector() {
	for {
		//每隔2s 检查一次当前阶段的TaskMap 计算处于Running任务的耗时，如果耗时超过9s则将该任务状态重置为Free
		//并再次放入对应的channel中等待被worker调用
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.Stage == FinishedStage {
			mu.Unlock()
			break
		}
		if c.Stage == MapStage{
			for _, v := range c.MapTaskMap {

				// fmt.Println("v:",v,"details:",v.TaskAddr)
				if v.state == Running && time.Since(v.StartTime) > 9*time.Second {
					// fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAddr.TaskId, time.Since(v.StartTime))
	
					switch v.TaskAddr.TaskType {
					case MapTask:
						c.MapTaskCh <- v.TaskAddr
						v.state = Free
						v.TaskAddr.TaskState = Free
				
					}
				}
			}
		}else if c.Stage == ReduceStage {
			for _, m := range c.ReduceTaskMap {

				// fmt.Println("m:",m,"details:",m.TaskAddr)
				if m.state == Running && time.Since(m.StartTime) > 9*time.Second {
					// fmt.Printf("the task[ %d ] is crash,take [%d] s\n", m.TaskAddr.TaskId, time.Since(m.StartTime))
	
					switch m.TaskAddr.TaskType {
					case ReduceTask:
						c.ReduceTaskCh <- m.TaskAddr
						m.state = Free
						m.TaskAddr.TaskState = Free
	
					}
				}
			}
		}
		



		mu.Unlock()
	}

}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Println("nReduce:",nReduce)
	c := Coordinator{
		Stage:MapStage,
		ReduceNum:nReduce,
		files:files,
		MapTaskCh:make(chan *Task,len(files)*2),
		ReduceTaskCh:make(chan *Task,nReduce*2),
		MapTaskMap:make(map[int]*TaskInfo,len(files)),
		ReduceTaskMap:make(map[int]*TaskInfo,nReduce),
	}

	// Your code here.
	// fmt.Println("Reducenum:",nReduce)
	c.getMapTasks(files)

	c.server()
	//设置一个新的函数 在coordinator退出之前每隔一定时间检查当前阶段TaskMap中所有处于Running状态的任务耗时，并对超时的任务做出相应处理
	go c.CrashDetector()
	return &c
}

func(c *Coordinator)getMapTasks(files []string){
	for _,i := range files{
		id:=c.TaskId
		// fmt.Println("id是:",id,"nReduce是:",c.ReduceNum)
		c.TaskId++
		task:=Task{
			TaskState:Free,
			TaskType:MapTask,
			TaskId:id,
			nReduce:c.ReduceNum,
			TaskName:[]string{i}, 		//初始化一个只包含一个文件名的数组用于map
		}
		taskInfo:=TaskInfo{
			state:Free,
			TaskAddr:&task,
		}
		info,_:=c.MapTaskMap[id]
		if info != nil{
			fmt.Println("id为",id," 的任务已经存在！")
		}else{
			c.MapTaskMap[id]=&taskInfo
		}
		c.MapTaskCh <-  &task
		fmt.Println("任务：",task," 成功生成！")
	}
}

func getReduceFile(i int) []string{
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(i)) {
			s = append(s, fi.Name())
		}
	}
	return s
}
func(c *Coordinator)getReduceTasks(){
	for i:=0;i<c.ReduceNum;i++{
		task:=Task{
			TaskState:Free,
			TaskType:ReduceTask,
			TaskId:i,
			nReduce:c.ReduceNum,
			TaskName:getReduceFile(i),
		}
		taskInfo:=TaskInfo{
			state:Free,
			TaskAddr:&task,
		}
		info,_:=c.ReduceTaskMap[i]
		if info != nil{
			fmt.Println("id为",i," 的任务已经存在！")
		}else{
			c.ReduceTaskMap[i]=&taskInfo
		}
		c.ReduceTaskCh <-  &task
		fmt.Println("任务：",task," 成功生成！")

	}
}
