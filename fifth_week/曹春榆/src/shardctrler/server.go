package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Timeout = 5000

// TODO: 检查通道是否阻塞

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastRequest map[int64]int64
	commandChs  map[int]chan Op

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) init() {
	sc.lastRequest = make(map[int64]int64)
	sc.commandChs = make(map[int]chan Op)
}

func (sc *ShardCtrler) isRepeatRequest(clientId int64, requestId int64) bool {
	lastRequestId, ok := sc.lastRequest[clientId]
	if !ok {
		return false
	}
	if lastRequestId == requestId {
		return true
	}
	return false
}

const JoinOperation = "Join"
const LeaveOperation = "Leave"
const MoveOperation = "Move"
const QueryOperation = "Query"

type Op struct {
	// Your data here.
	Operation   string           // 命令的类型：GET,PUT,APPEND
	JoinServers map[int][]string // Join命令的参数
	LeaveGIDs   []int            // leave命令的参数
	MoveShard   int              // Move命令的参数
	MoveGID     int              // Move命令的参数
	QueryNum    int              // Query的参数
	ClientId    int64            // 提交该命令的机器
	RequestId   int64            // 提交该命令的请求号
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintln("进入了Join")
	op := Op{
		Operation:   JoinOperation,
		JoinServers: args.Servers,
		ClientId:    args.ClientId,
		RequestId:   args.RequestId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sc.mu.Lock()
	if _, ok := sc.commandChs[index]; !ok {
		sc.commandChs[index] = make(chan Op, 1)
	}
	commandCh := sc.commandChs[index]
	sc.mu.Unlock()

	select {
	case command := <-commandCh:
		if command.ClientId == op.ClientId && command.RequestId == command.RequestId {
			reply.Err = OK
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.WrongLeader = true
		return
	}

	close(commandCh)
	sc.mu.Lock()
	delete(sc.commandChs, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintln("进入了Leave")
	op := Op{
		Operation: LeaveOperation,
		LeaveGIDs: args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sc.mu.Lock()
	if _, ok := sc.commandChs[index]; !ok {
		sc.commandChs[index] = make(chan Op, 1)
	}
	commandCh := sc.commandChs[index]
	sc.mu.Unlock()

	select {
	case command := <-commandCh:
		if command.ClientId == op.ClientId && command.RequestId == command.RequestId {
			reply.Err = OK
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.WrongLeader = true
		return
	}

	close(commandCh)
	sc.mu.Lock()
	delete(sc.commandChs, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintln("进入了Move")
	op := Op{
		Operation: MoveOperation,
		MoveShard: args.Shard,
		MoveGID:   args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sc.mu.Lock()
	if _, ok := sc.commandChs[index]; !ok {
		sc.commandChs[index] = make(chan Op, 1)
	}
	commandCh := sc.commandChs[index]
	sc.mu.Unlock()

	select {
	case command := <-commandCh:
		if command.ClientId == op.ClientId && command.RequestId == command.RequestId {
			reply.Err = OK
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.WrongLeader = true
		return
	}

	close(commandCh)
	sc.mu.Lock()
	delete(sc.commandChs, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	DPrintln("进入了Query")
	op := Op{
		Operation: QueryOperation,
		QueryNum:  args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sc.mu.Lock()
	if _, ok := sc.commandChs[index]; !ok {
		sc.commandChs[index] = make(chan Op, 1)
	}
	commandCh := sc.commandChs[index]
	sc.mu.Unlock()

	select {
	case command := <-commandCh:
		if command.ClientId == op.ClientId && command.RequestId == op.RequestId {
			reply.Err = OK
			sc.mu.Lock()
			sc.lastRequest[command.ClientId] = command.RequestId
			if args.Num < 0 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			sc.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
		//if !sc.isRepeatRequest(command.ClientId, command.RequestId) {
		//	sc.lastRequest[command.ClientId] = command.RequestId
		//	if args.Num < 0 || args.Num >= len(sc.configs) {
		//		reply.Config = sc.configs[len(sc.configs)-1]
		//	} else {
		//		reply.Config = sc.configs[args.Num]
		//	}
		//	DPrintln(sc.me, "num:", op.QueryNum, "config长度", len(sc.configs), "返回的Config", reply.Config.Shards)
		//} else {
		//	reply.WrongLeader = true
		//}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.WrongLeader = true
		return
	}

	close(commandCh)
	sc.mu.Lock()
	delete(sc.commandChs, index)
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ExecCommand() {
	for message := range sc.applyCh {
		if message.CommandValid {
			op := message.Command.(Op)
			DPrintln(sc.me, "收到Raft完成命令", op)
			sc.mu.Lock()
			if !sc.isRepeatRequest(op.ClientId, op.RequestId) {
				if op.Operation != QueryOperation {
					sc.lastRequest[op.ClientId] = op.RequestId
				}
				switch op.Operation {
				case JoinOperation:
					sc.ExecJoin(op.JoinServers)
				case LeaveOperation:
					sc.ExecLeave(op.LeaveGIDs)
				case MoveOperation:
					sc.ExecMove(op.MoveShard, op.MoveGID)
				}
			}
			if _, ok := sc.commandChs[message.CommandIndex]; ok {
				//go func() {
				sc.commandChs[message.CommandIndex] <- op
				//}()
			}
			sc.mu.Unlock()
		}
	}

}

func (sc *ShardCtrler) ExecJoin(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	lastShardAllocate := lastConfig.Shards
	newServers := make(map[int][]string)
	shardNums := make(map[int][]int)
	// 保存旧server
	for gid, server := range lastConfig.Groups {
		newServers[gid] = server
		if gid != 0 {
			shardNums[gid] = make([]int, 0)
		}
	}
	// 加入新server
	for gid, server := range servers {
		newServers[gid] = server
		shardNums[gid] = make([]int, 0)
	}

	// 统计各个集群分配的数量
	freeShards := make([]int, 0)
	for shardId, gid := range lastShardAllocate {
		if gid == 0 {
			freeShards = append(freeShards, shardId)
		} else {
			shardNums[gid] = append(shardNums[gid], shardId)
		}
	}

	// 重新分配分片
	DPrintln("serverShards:", shardNums, "freeShards", freeShards)
	newShardMap := sc.reAllocateShard(shardNums, freeShards)
	DPrintln("\n机器：", newServers, "已分配的:")
	for i, g := range newShardMap {
		DPrintf("%d: %d, ", i, g)
	}
	DPrintln()

	// 创建新的Config
	num := len(sc.configs)
	sc.configs = append(sc.configs, Config{
		Num:    num,
		Shards: newShardMap,
		Groups: newServers,
	})
}

func (sc *ShardCtrler) ExecLeave(giDs []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newServers := make(map[int][]string)
	serverShards := make(map[int][]int)
	// 从server中剔除leave的server
	for gid, server := range lastConfig.Groups {
		newServers[gid] = server
		serverShards[gid] = make([]int, 0)
	}
	for _, gid := range giDs {
		delete(newServers, gid)
		delete(serverShards, gid)
	}

	// 统计新的shard情况
	freeShards := make([]int, 0)
	for shardId, gid := range lastConfig.Shards {
		if _, ok := serverShards[gid]; ok {
			serverShards[gid] = append(serverShards[gid], shardId)
		} else {
			freeShards = append(freeShards, shardId)
		}
	}

	// 将新的shard进行重新分配
	var newShardMap [NShards]int
	if len(newServers) != 0 {
		newShardMap = sc.reAllocateShard(serverShards, freeShards)
	}

	// 创建新的config
	newNum := len(sc.configs)
	sc.configs = append(sc.configs, Config{
		Num:    newNum,
		Shards: newShardMap,
		Groups: newServers,
	})
}

func (sc *ShardCtrler) ExecMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	if _, ok := lastConfig.Groups[gid]; !ok {
		return
	}
	// 复制之前的map
	newServers := make(map[int][]string)
	for gid_, servers := range lastConfig.Groups {
		newServers[gid_] = servers
	}

	// 将shard移动到相应的机器
	newShards := lastConfig.Shards
	newShards[shard] = gid

	// 创建新的config
	newNum := len(sc.configs)
	sc.configs = append(sc.configs, Config{
		Num:    newNum,
		Shards: newShards,
		Groups: newServers,
	})
}

func (sc *ShardCtrler) reAllocateShard(allocated map[int][]int, freeShards []int) [NShards]int {
	freeShard := freeShards
	averageShard := NShards / len(allocated)
	remain := NShards % len(allocated)
	judgeNum := averageShard
	if remain > 0 {
		judgeNum++
	}
	keys := make([]int, 0, len(allocated))
	for gid := range allocated {
		keys = append(keys, gid)
	}
	sort.Ints(keys)
	DPrintln("机器", sc.me, "keys", keys)
	for _, gid := range keys {
		gLen := len(allocated[gid])
		if gLen > judgeNum {
			freeShard = append(freeShard, allocated[gid][judgeNum:]...)
			allocated[gid] = allocated[gid][:judgeNum]
			if remain != 0 {
				remain--
				if remain == 0 {
					judgeNum = averageShard
				}
			}
		} else if gLen == judgeNum {
			if remain != 0 {
				remain--
				if remain == 0 {
					judgeNum = averageShard
				}
			}
		}
	}

	// 下面开始进行分配
	DPrintln("averageShard", averageShard, "remain", remain)
	var newShardMap [NShards]int
	freeIndex := 0
	judgeNum = averageShard
	if remain > 0 {
		judgeNum++
	}
	for _, gid := range keys {
		index := 0
		for ; index < len(allocated[gid]); index++ {
			newShardMap[allocated[gid][index]] = gid
		}
		//if averageShard == 0 && index == 0 && freeIndex < len(freeShard) {
		//	newShardMap[freeShard[freeIndex]] = gid
		//	freeIndex++
		//	index++
		//}
		for index < judgeNum && freeIndex < len(freeShard) {
			newShardMap[freeShard[freeIndex]] = gid
			if index == judgeNum-1 && remain != 0 && judgeNum == averageShard+1 {
				remain--
				if remain == 0 {
					judgeNum = averageShard
				}
			}
			freeIndex++
			index++
		}
	}
	if freeIndex != len(freeShard) {
		fmt.Println("\n机器总数", len(allocated), allocated)
		DPrintf("未分配的:")
		for ; freeIndex < len(freeShard); freeIndex++ {
			DPrintf("%d ", freeIndex)
		}
		DPrintln("\n已分配的:")
		for i, g := range newShardMap {
			DPrintf("%d: %d, ", i, g)
		}
		DPrintln()
		panic("有空闲的Shard未被分配")
	}
	return newShardMap
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	//labgob.Register(QueryArgs{})
	//labgob.Register(JoinArgs{})
	//labgob.Register(MoveArgs{})
	//labgob.Register(LeaveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.init()
	go sc.ExecCommand()

	return sc
}
