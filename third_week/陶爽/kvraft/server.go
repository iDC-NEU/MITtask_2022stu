package kvraft

import (
	"bytes"
	"time"

	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug = false
	//Debug = true
	CONSENSUS_TIMEOUT = 500 // ms
)

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
	// reciver from applyCh , the entries that raft applied
	Operation string // "get" "put" "append"
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int

	// Your definitions here.
	kvDB          map[string]string
	waitApplyCh   map[int]chan Op
	lastRequestId map[int64]int

	lastSSPointRaftLogIndex int
}

func (kv *KVServer) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *KVServer) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}

func (kv *KVServer) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb map[string]string
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
	} else {
		kv.kvDB = persist_kvdb
		kv.lastRequestId = persist_lastRequestId
	}
}

func (kv *KVServer) ReadRaftApplyCommandLoop() {
	for message := range kv.applyCh {
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)
	DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)

	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		DPrintf("[RaftApplyMessageSendToWaitChan-->]Server %d , Send Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		ch <- op
	}
	return exist
}

func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId, op.RequestId, op.Key)
	}
	kv.DprintfKVDB()
	return value, exist
}

func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {

	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op) {

	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

func (kv *KVServer) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}

func (kv *KVServer) DprintfKVDB() {
	if !Debug {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, value := range kv.kvDB {
		DPrintf("[DBInfo ----]Key : %v, Value : %v", key, value)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: "get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[GET StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

		_, ifLeader := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: args.Opreation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[PUTAPPEND StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("[TIMEOUT PUTAPPEND !!!!]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
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
	DPrintf("[InitKVServer---]Server %d", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShotToInstall(snapshot)
	}

	go kv.ReadRaftApplyCommandLoop()
	return kv
}
