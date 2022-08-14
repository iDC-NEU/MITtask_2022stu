package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"bytes"

	"time"
	// "fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string//操作的字符串：Get/Put/Append
	Key string
	Value string
	ClientId int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB map[string]string //所有的数据
	ApplyCh map[int]chan Op //Operation list
	LastRequId map[int64]int //Clientid与RequestId对应,用来判断请求是否重复处理

	SnapShotLogIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Op: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}
	
	rfIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	FromRfIndex, ok := kv.ApplyCh[rfIndex]
	if !ok {
		kv.ApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = kv.ApplyCh[rfIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond * 500) :
		_, isLeader := kv.rf.GetState()
		if kv.ifDuplicate(op.ClientId, op.RequestId) && isLeader{
			value, ok := kv.GetFromDB(op)
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case rfCommitOp := <- FromRfIndex:
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId {
			value, ok := kv.GetFromDB(op)
				if ok {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
		} else{
			reply.Err = ErrWrongLeader
		}
	}

	//删除处理的Op
	kv.mu.Lock()
	delete(kv.ApplyCh, rfIndex)
	kv.mu.Unlock()
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//第一个bug，这句话是get那块复制的，导致value传个空，我是憨批，以后复制肯定看好
	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	rfIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	FromRfIndex, ok := kv.ApplyCh[rfIndex]
	if !ok {
		kv.ApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = kv.ApplyCh[rfIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond * 500) :
		if kv.ifDuplicate(op.ClientId,op.RequestId){
			reply.Err = OK
		} else{
			reply.Err = ErrWrongLeader
		}

	case rfCommitOp := <- FromRfIndex :
		if rfCommitOp.ClientId == op.ClientId && rfCommitOp.RequestId == op.RequestId  {
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.ApplyCh, rfIndex)
	kv.mu.Unlock()
	return

	
}

//判断该请求是否被处理过
func (kv *KVServer) ifDuplicate(ClientId int64, RequestId int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastRequestId, ok := kv.LastRequId[ClientId]
	if !ok {
		return false
	}
	return RequestId <= lastRequestId
}

//通过key获取Value
func (kv *KVServer) GetFromDB(op Op) (string, bool){
	kv.mu.Lock()
	value, ok := kv.DB[op.Key]
	kv.LastRequId[op.ClientId] = op.RequestId
	DPrintf("[Getoperation]  key:%s  value:%s\n", op.Key, value)//找第一个bug
	kv.mu.Unlock()
	return value, ok
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

	
	kv.DB = make(map[string]string)

	// You may need initialization code here.
	kv.DB = make(map[string]string)
	kv.ApplyCh = make(map[int]chan Op)
	kv.LastRequId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.ReadSnapShotToInstall(snapshot)
	}

	go kv.RfCommand()

	return kv
}

func (kv *KVServer) RfCommand() {
	for message := range kv.applyCh{
		if message.CommandValid {
			kv.GetCommand(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShot(message)
		}
	}
}

func (kv *KVServer) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_db map[string]string
	var persist_LastRequId map[int64]int

	if d.Decode(&persist_db) != nil || d.Decode(&persist_LastRequId) != nil {
		DPrintf("KVSERVER %d read persister got a problem！",kv.me)
	} else {
		kv.DB = persist_db
		kv.LastRequId = persist_LastRequId
	}
}

func (kv *KVServer) GetSnapShot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.SnapShotLogIndex = message.SnapshotIndex
	}
}

func (kv *KVServer) GetCommand(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.SnapShotLogIndex {
		return
	}

	if !kv.ifDuplicate(op.ClientId, op.RequestId) {

		DPrintf("[PutAppendOperation]   operation:%s, value: %s\n", op.Op, op.Value)//debug，找到了第一个bug

		if op.Op == "Put" {
			kv.PutDB(op)
		}
		if op.Op == "Append" {
			kv.AppendDB(op)
		}
	}

	if kv.maxraftstate != -1{
		kv.IfSendSnapShotCommand(message.CommandIndex,9)
	}

	// Send message to the chan of op.ClientId
	kv.SendMessage(op, message.CommandIndex)
}

func (kv *KVServer) IfSendSnapShotCommand(raftIndex int, proportion int){
	if len(kv.rf.persister.raftstate) > (kv.maxraftstate*proportion/10){
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *KVServer) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.DB)
	e.Encode(kv.LastRequId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) PutDB(op Op) {
	kv.mu.Lock()
	kv.DB[op.Key] = op.Value
	kv.LastRequId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *KVServer) AppendDB(op Op){
	kv.mu.Lock()
	value, ok := kv.DB[op.Key]
	if ok {
		kv.DB[op.Key] = value + op.Value
	} else {
		kv.DB[op.Key] = op.Value
	}
	kv.LastRequId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *KVServer) SendMessage(op Op, rfIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.ApplyCh[rfIndex]
	if ok {
		ch <- op
	}
	return ok
}
