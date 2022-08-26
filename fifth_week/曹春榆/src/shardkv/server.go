package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const ConfigDuration = 100
const Timeout = 500

const (
	ConfOperation         = "Conf"
	GetOperation          = "Get"
	PutOperation          = "Put"
	AppendOperation       = "Append"
	InstallShardOperation = "InstallShard"
	SendConfirmOperation  = "SendConfirm"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation  string                    // 操作类型
	NewConf    shardctrler.Config        // 新的config
	Key        string                    // 命令使用的key
	Value      string                    // 命令添加的值
	ShardId    int                       // 对应的shard编号
	ClientId   int64                     // 提交该命令的机器
	RequestId  int64                     // 提交该命令的请求号
	Err        Err                       // 在执行过程中可能出现的错误
	ShardMap   map[int]map[string]string // 将新的shard添加到存储中
	ReqMap     map[int]map[int64]int64   // 新的shard的最新请求，防止迁移时请求重复
	ConfigNum  int                       // 安装shard的ConfigNum
	SendComNum []int                     // send确认的编号
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ck            *shardctrler.Clerk        // 用于请求config的客户端
	currentConfig shardctrler.Config        // 当前正在使用的Config
	kvStore       map[int]map[string]string // 用于存储kv的map
	kvStoreMutex  sync.Mutex                // 对于存储加的锁
	//clientLastReq    map[int64]int64           // 存储客户端的上一个请求
	indexCond        map[int]chan Op    // 用于对结果同步通信的通道
	indexCondMutex   sync.RWMutex       // 对于通信管道上的锁
	snapshotCh       chan raft.ApplyMsg // 用于创建快照的通道
	lastIncludeIndex int                // 快照中最后一个包含的索引

	shardSending   []bool                  // 该块是否正在被发送
	shardReceiving []bool                  // 该块是否在接收中
	sendShardCh    chan shardctrler.Config // 发送
	getConfCh      chan int                // 唤醒读取config线程的通道
	configingNum   int                     // 正在配置的配置号
	shardLastReq   map[int]map[int64]int64 // 每个shard对应的client的最后一个请求
	lastWakeNum    int                     // 上一个唤醒的config编号
	// 多个锁时上锁顺序为mu-->store-->index
}

func (kv *ShardKV) init() {
	kv.ck = shardctrler.MakeClerk(kv.ctrlers)
	//kv.ck.IsPrint = true
	//kv.ck.GID = kv.gid
	kv.currentConfig = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}
	kv.kvStore = make(map[int]map[string]string)
	//kv.clientLastReq = make(map[int64]int64)
	kv.shardLastReq = make(map[int]map[int64]int64)
	kv.indexCond = make(map[int]chan Op)

	kv.snapshotCh = make(chan raft.ApplyMsg, 5)
	kv.sendShardCh = make(chan shardctrler.Config, 5)
	kv.getConfCh = make(chan int, 100)

	kv.shardReceiving = make([]bool, shardctrler.NShards, shardctrler.NShards)
	kv.shardSending = make([]bool, shardctrler.NShards, shardctrler.NShards)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.RLock()
	if !kv.isKeyInServer(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	command := Op{
		Operation: GetOperation,
		ShardId:   key2shard(args.Key),
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, _ := kv.rf.Start(command)
	kv.indexCondMutex.Lock()
	if _, ok := kv.indexCond[index]; !ok {
		kv.indexCond[index] = make(chan Op, 2)
	}
	commandChan := kv.indexCond[index]
	kv.indexCondMutex.Unlock()

	select {
	case op := <-commandChan:
		if op.ClientId == command.ClientId && op.RequestId == command.RequestId {
			DPrintln("GID", kv.gid, "Get顺利返回命令", op, "是否还在", kv.isKeyInServer(op.Key))
			kv.mu.RLock()
			if !kv.isKeyInServer(op.Key) {
				reply.Err = ErrWrongGroup
				kv.mu.RUnlock()
				break
			}
			value, ok := kv.kvStore[op.ShardId][op.Key]
			kv.mu.RUnlock()
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			kv.mu.RLock()
			if kv.isKeyInServer(command.Key) {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = ErrWrongGroup
			}
			kv.mu.RUnlock()
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		DPrintln("GID", kv.gid, "Get超时返回，命令", command)
		kv.mu.RLock()
		if kv.isKeyInServer(command.Key) {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.RUnlock()
	}

	//close(commandChan)
	kv.indexCondMutex.Lock()
	delete(kv.indexCond, index)
	kv.indexCondMutex.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.DPrintln("机器", kv.me, "PutAppend, is leader")

	kv.mu.RLock()
	if args.ConfNum < kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if !kv.isKeyInServer(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	kv.DPrintln("PutAppend, is key shard， key", args.Key, "newValue", args.Value)

	command := Op{
		Operation: args.Op,
		ShardId:   key2shard(args.Key),
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, _ := kv.rf.Start(command)
	kv.indexCondMutex.Lock()
	if _, ok := kv.indexCond[index]; !ok {
		kv.indexCond[index] = make(chan Op, 2)
	}
	commandChan := kv.indexCond[index]
	kv.indexCondMutex.Unlock()

	DPrintln("机器", kv.me, "完成命令发送到Raft")

	select {
	case op := <-commandChan:
		//DPrintln("机器", kv.me, "raft读取出命令", op)
		if op.ClientId == command.ClientId && op.RequestId == op.RequestId {
			if op.Err == ErrWrongGroup {
				reply.Err = op.Err
			} else {
				reply.Err = OK
			}
		} else {
			kv.mu.RLock()
			if kv.isKeyInServer(command.Key) {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = ErrWrongGroup
			}
			kv.mu.RUnlock()
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		DPrintln("机器", kv.me, "超时")
		kv.mu.RLock()
		if kv.isKeyInServer(command.Key) {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.RUnlock()
	}

	//close(commandChan)
	kv.indexCondMutex.Lock()
	delete(kv.indexCond, index)
	kv.indexCondMutex.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) isKeyInServer(key string) bool {
	shard := key2shard(key)
	DPrintln("需要的shard", shard, "负责的机器", kv.currentConfig.Shards[shard], "本机器", kv.gid,
		"是否正在收到", kv.shardReceiving[shard], "总机器", kv.currentConfig.Groups)
	for shardId, Gid := range kv.currentConfig.Shards {
		DPrintf("%d负责%d, ", Gid, shardId)
	}
	DPrintln()
	if kv.currentConfig.Shards[shard] != kv.gid {
		return false
	}
	if kv.shardReceiving[shard] == true {
		return false
	}
	return true
}

func (kv *ShardKV) ExecCommand() {
	// TODO: 命令出来的时候直接检查是否还是在key内
	for message := range kv.applyCh {
		// TODO: InstallShard和Conf过程中可能出错了,导致程序CPU飞转，看log解决，该问题是大概率出现
		DPrintln("机器", kv.me, "gid", kv.gid, "得到的命令", message.Command, "是否是snapshot", message.SnapshotValid)
		if kv.maxraftstate != -1 {
			kv.snapshotCh <- message
		}
		//DPrintln("机器", kv.me, "完成snapshot传输", message.Command)
		index := message.CommandIndex
		if message.CommandValid && message.Command != nil && message.CommandIndex > kv.lastIncludeIndex {
			command := message.Command.(Op)
			kv.execAndPassCommand(command, index)
		}
		//DPrintln("机器", kv.me, "完成执行传输", message.Command)
	}
}

func (kv *ShardKV) execAndPassCommand(command Op, index int) {
	kv.mu.Lock()
	if command.Operation == SendConfirmOperation {
		for _, shardId := range command.SendComNum {
			kv.shardSending[shardId] = false
			delete(kv.kvStore, shardId)
			delete(kv.shardLastReq, shardId)
		}
		kv.mu.Unlock()
		return
	}
	if command.Operation == ConfOperation {
		kv.execConf(command.NewConf)
		kv.mu.Unlock()
		return
	} else if command.Operation == InstallShardOperation {
		kv.execInstallShard(command.ConfigNum, command.ShardMap, command.ReqMap)
		kv.mu.Unlock()
		return
	}
	if !kv.isKeyInServer(command.Key) {
		command.Err = ErrWrongGroup
	} else {
		if !kv.isCommandRepeat(command.ClientId, command.RequestId, command.ShardId) {
			if command.Operation == PutOperation {
				kv.DPrintln("Put添加", "key", command.Key, "value", command.Value)
				kv.kvStore[command.ShardId][command.Key] = command.Value
			} else if command.Operation == AppendOperation {
				value, ok := kv.kvStore[command.ShardId][command.Key]
				kv.DPrintln("Append添加", "key", command.Key, "value", value+command.Value)
				if !ok {
					kv.kvStore[command.ShardId][command.Key] = command.Value
				} else {
					kv.kvStore[command.ShardId][command.Key] = value + command.Value
				}
			}
			kv.shardLastReq[command.ShardId][command.ClientId] = command.RequestId
		}
	}
	kv.mu.Unlock()

	kv.indexCondMutex.RLock()
	commandCh, ok := kv.indexCond[index]
	kv.indexCondMutex.RUnlock()
	if ok {
		commandCh <- command
	}
}

func (kv *ShardKV) isCommandRepeat(clientId int64, requestId int64, shardId int) bool {
	//reqId, ok := kv.clientLastReq[shardId][clientId]
	reqId, ok := kv.shardLastReq[shardId][clientId]
	if !ok {
		return false
	}
	return requestId <= reqId
}

func (kv *ShardKV) recover() {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardSending[i] == true {
			kv.sendShardCh <- kv.currentConfig
			break
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//if me == 0 {
	//	kv.rf.IsPrint = true
	//	kv.rf.GID = gid
	//}
	kv.init()
	kv.DPrintln("完成了初始化")

	kv.InstallSnapshot(persister.ReadSnapshot())

	//go kv.recover()
	go kv.GetAndApplyConfig()
	go kv.ExecCommand()
	go kv.SendMoveShard()
	go kv.TakeSnapshot()
	return kv
}
