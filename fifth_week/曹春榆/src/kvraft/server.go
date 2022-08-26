package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const Timeout = 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOperation    = "Get"
	PutOperation    = "Put"
	AppendOperation = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // 命令的类型：GET,PUT,APPEND
	Key       string // 命令使用的key
	Value     string // 命令添加的值
	ClientId  int64  // 提交该命令的机器
	RequestId int    // 提交该命令的请求号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kvStore           map[string]string // 用于存储键值对
	kvStoreMutex      sync.RWMutex
	indexCond         map[int]chan Op // 使用channel来代替condition
	indexCondCount    map[int]int     // 记录使用该管道的个数
	indexCondMutex    sync.RWMutex    // 对于indexCond的锁
	clientLastCommand map[int64]int   // 记录client的最后提交的命令
	lastCommandMutex  sync.RWMutex
	lastIncludeIndex  int // snapshot中最后包含的索引
	snapshotCh        chan raft.ApplyMsg

	// Your definitions here.
}

func (kv *KVServer) init() {
	kv.kvStore = make(map[string]string)
	kv.indexCond = make(map[int]chan Op)
	kv.indexCondCount = make(map[int]int)
	kv.clientLastCommand = make(map[int64]int)
	kv.lastIncludeIndex = 0
	kv.snapshotCh = make(chan raft.ApplyMsg, 20)
}

func (kv *KVServer) setLastCommand(clientId int64, requestId int) {
	kv.lastCommandMutex.Lock()
	defer kv.lastCommandMutex.Unlock()
	kv.clientLastCommand[clientId] = requestId
}

func (kv *KVServer) getLastCommand(clientId int64) int {
	kv.lastCommandMutex.RLock()
	defer kv.lastCommandMutex.RUnlock()
	if _, ok := kv.clientLastCommand[clientId]; !ok {
		return -1
	}
	return kv.clientLastCommand[clientId]
}

func (kv *KVServer) isCommandRepeat(clientId int64, requestId int) bool {
	return kv.getLastCommand(clientId) == requestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	command := Op{
		Operation: GetOperation,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, _ := kv.rf.Start(command)
	kv.indexCondMutex.Lock()
	if _, ok := kv.indexCond[index]; !ok {
		kv.indexCond[index] = make(chan Op, 5)
		kv.indexCondCount[index] = 0
	}
	kv.indexCondCount[index]++
	commandChan := kv.indexCond[index]
	kv.indexCondMutex.Unlock()

	select {
	case op := <-commandChan:
		// 如果得到的命令的标识和发送的一致，则证明该命令达成共识成功，否则，存在一个可能是该leader在命令提交前成为了follower
		if op.ClientId == command.ClientId && op.RequestId == op.RequestId {
			kv.mu.Lock()
			DPrintln(kv.me, "enter get")
			value, ok := kv.kvStore[op.Key]
			DPrintln(kv.me, "leave get")
			//kv.setLastCommand(op.ClientId, op.RequestId)
			kv.mu.Unlock()
			reply.Err = OK
			if ok {
				reply.Value = value
			} else {
				reply.Value = ""
			}

		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	kv.indexCondMutex.Lock()
	kv.indexCondCount[index]--
	count := kv.indexCondCount[index]
	if count == 0 {
		delete(kv.indexCondCount, index)
		delete(kv.indexCond, index)

	}
	kv.indexCondMutex.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	command := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, _ := kv.rf.Start(command)
	kv.indexCondMutex.Lock()
	if _, ok := kv.indexCond[index]; !ok {
		kv.indexCond[index] = make(chan Op, 5)
		kv.indexCondCount[index] = 0
	}
	kv.indexCondCount[index]++
	commandChan := kv.indexCond[index]
	kv.indexCondMutex.Unlock()

	select {
	case op := <-commandChan:
		// 如果得到的命令的标识和发送的一致，则证明该命令达成共识成功，否则，存在一个可能是该leader在命令提交前成为了follower
		if op.ClientId == command.ClientId && op.RequestId == op.RequestId {
			reply.Err = OK

		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.indexCondMutex.Lock()
	kv.indexCondCount[index]--
	count := kv.indexCondCount[index]
	if count == 0 {
		delete(kv.indexCondCount, index)
		delete(kv.indexCond, index)

	}
	kv.indexCondMutex.Unlock()
}

func (kv *KVServer) ExecuteCommand() {
	for message := range kv.applyCh {
		if kv.maxraftstate != -1 {
			DPrintln(kv.me, "before snapshotCh")
			kv.snapshotCh <- message
			DPrintln(kv.me, "after snapshotCh")
		}
		index := message.CommandIndex
		if message.CommandValid && message.Command != nil && message.CommandIndex > kv.lastIncludeIndex {
			command := message.Command.(Op)
			DPrintln(kv.me, "before executeAndPassCommand")
			kv.executeAndPassCommand(command, index)
			DPrintln(kv.me, "after executeAndPassCommand")
		}

	}
	DPrintln("\n\n", kv.me, "after range kv.applyCh")
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) executeAndPassCommand(command Op, index int) {
	DPrintln(kv.me, "enter executeAndPassCommand")
	kv.mu.Lock()
	DPrintln(kv.me, "before judgeCommandRepeat")
	if !kv.isCommandRepeat(command.ClientId, command.RequestId) {
		DPrintln(kv.me, "after isCommandRepeat")
		if command.Operation == PutOperation {
			kv.kvStore[command.Key] = command.Value
		} else if command.Operation == AppendOperation {
			value, ok := kv.kvStore[command.Key]
			if !ok {
				kv.kvStore[command.Key] = command.Value
			} else {
				kv.kvStore[command.Key] = value + command.Value
			}
		}
		DPrintln(kv.me, "after putAppend")
		kv.setLastCommand(command.ClientId, command.RequestId)
		DPrintln(kv.me, "after setLastCommand")
	}
	DPrintln(kv.me, "after judgeCommandRepeat")
	kv.mu.Unlock()
	DPrintln(kv.me, "after executeUnlock")
	kv.indexCondMutex.RLock()
	commandCh, ok := kv.indexCond[index]
	kv.indexCondMutex.RUnlock()
	DPrintln(kv.me, "after indexCondMutex")
	if ok {
		DPrintln("机器", kv.me, "before commandCh")
		commandCh <- command
		DPrintln("机器", kv.me, "after commandCh")
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.init()

	// You may need initialization code here.
	kv.InstallSnapshot(persister.ReadSnapshot())
	go kv.ExecuteCommand()
	go kv.TakeSnapshot()

	return kv
}
