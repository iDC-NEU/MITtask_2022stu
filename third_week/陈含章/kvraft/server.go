package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(fmt.Sprintf("%06d ", time.Since(Start).Milliseconds())+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	ClientID  int64
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your definitions here.
	maxStateForRaft  int
	state            map[string]string
	sessions         map[int64]ClientRecord     // memoization for each client's last response
	waitCh           map[int]chan *CommandReply // a map of waitChs to retrieve corresponding command after agreement
	lastAppliedIndex int                        // last applied index of log entry
	persister        *raft.Persister            // the persister of the kv server
}

var Start time.Time //global timer

func (kv *KVServer) CommandRequest(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	defer DPrintf("Server %d responded client %d's request %d: %+v\n", kv.me, args.ClientID, args.RequestID, reply)

	DPrintf("Server %d received request: %+v\n", kv.me, args)
	// check for duplicates
	var duplicated bool

	clientRecord, ok := kv.sessions[args.ClientID]
	if ok && args.RequestID <= clientRecord.RequestID {
		duplicated = true
	} else {
		duplicated = false
	}

	if args.Op != "Get" && duplicated {
		reply.Err = kv.sessions[args.ClientID].LastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send command to raft for agreement
	index, _, isLeader := kv.rf.Start(Op{args.Key, args.Value, args.Op, args.ClientID, args.RequestID})

	if checkWrongLeader(index, isLeader) == false {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d sent client %d's request %d to raft at index %d\n", kv.me, args.ClientID, args.RequestID, index)

	kv.MakeReply(reply, index) //make reply
	go kv.closeWaitCh(index)
}

func (kv *KVServer) MakeReply(reply *CommandReply, index int) {
	kv.mu.Lock()
	waitCh := kv.getWaitCh(index)
	kv.mu.Unlock()

	select {
	case agreement := <-waitCh:
		reply.Err = agreement.Err
		reply.Value = agreement.Value

	case <-time.NewTimer(500 * time.Millisecond).C: //timeout
		reply.Err = ErrTimeout
	}
}

func checkWrongLeader(index int, isLeader bool) bool {

	isWrongLeader := true
	if index == -1 || !isLeader {
		isWrongLeader = false
		return isWrongLeader
	}
	return isWrongLeader

}

func (kv *KVServer) closeWaitCh(index int) {
	kv.mu.Lock()
	ch, ok := kv.waitCh[index]
	if ok {
		close(ch)
		delete(kv.waitCh, index)
	}
	kv.mu.Unlock()
}

// method to get a waitCh for an expected commit index
func (kv *KVServer) getWaitCh(index int) chan *CommandReply {
	ch, ok := kv.waitCh[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		kv.waitCh[index] = ch
		return ch
	}
	return ch
}

// method to apply command to state machine
func (kv *KVServer) applyCommand(op Op) *CommandReply {
	reply := CommandReply{Err: OK}

	switch op.Type {
	case "Get":
		val, ok := kv.state[op.Key]
		if ok {
			reply.Value = val
		}

	case "Put":
		kv.state[op.Key] = op.Value

	case "Append":
		kv.state[op.Key] += op.Value

	default:
		kv.state[op.Key] += op.Value
	}
	return &reply
}

//
// For lab 3b
//

// the method to determine if raft state is oversize
//func (kv *KVServer) needSnapshot() bool {
//	return kv.maxraftstate >= 0 && kv.persister.RaftStateSize() >= kv.maxraftstate
//}

// method to apply snapshot to state machine
func (kv *KVServer) applySnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	var State map[string]string
	var Sessions map[int64]ClientRecord
	var LastAppliedIndex int
	tempData := bytes.NewBuffer(data)
	tempDecoder := labgob.NewDecoder(tempData)

	// decode, print error but do not panic
	err1 := tempDecoder.Decode(&State)
	err2 := tempDecoder.Decode(&Sessions)
	err3 := tempDecoder.Decode(&LastAppliedIndex)
	if err1 != nil || err2 != nil || err3 != nil {
		DPrintf("Decoding error:%v, %v\n", err1, err2)
	} else {
		// apply
		kv.state = State
		kv.sessions = Sessions
		kv.lastAppliedIndex = LastAppliedIndex
	}
}

func initGlobalTimer() bool {
	if Start.IsZero() {
		Start = time.Now()
	}
	return true
}

//
// long-running applier goroutine
//
func (kv *KVServer) applier() {
	for !kv.killed() {

		applyMsg := <-kv.applyCh //wait for applyCh
		var reply *CommandReply

		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)

			kv.mu.Lock() // get lock

			if applyMsg.CommandIndex > kv.lastAppliedIndex {
				kv.lastAppliedIndex = applyMsg.CommandIndex

				// check duplicates

				clientRecord, ok := kv.sessions[op.ClientID]
				duplicated := ok && op.RequestID <= clientRecord.RequestID

				if op.Type != "Get" && duplicated {
					reply = kv.sessions[op.ClientID].LastResponse
				} else {
					reply = kv.applyCommand(op)
					// DPrintf("Server %d applied command %+v\n", kv.me, command)
					if op.Type != "Get" {
						kv.sessions[op.ClientID] = ClientRecord{op.RequestID, reply}
					}
				}

				// after applying command, compare if raft is oversized
				if kv.maxStateForRaft >= 0 && kv.persister.RaftStateSize() >= kv.maxStateForRaft {
					DPrintf("Server %d takes a snapshot till index %d\n", kv.me, applyMsg.CommandIndex)
					//kv.takeSnapshot(applyMsg.CommandIndex)

					tempData := new(bytes.Buffer)
					tempEncode := labgob.NewEncoder(tempData)
					tempEncode.Encode(kv.state)
					tempEncode.Encode(kv.sessions)
					tempEncode.Encode(kv.lastAppliedIndex)
					data := tempData.Bytes()
					kv.rf.Snapshot(applyMsg.CommandIndex, data)

				}

				// check the same term and leadership before reply
				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
					ch := kv.getWaitCh(applyMsg.CommandIndex)
					ch <- reply
				}
				kv.mu.Unlock()
			} else {
				kv.mu.Unlock()
				continue
			}

		} else { // committed snapshot
			kv.mu.Lock()
			if kv.lastAppliedIndex < applyMsg.SnapshotIndex {
				DPrintf("Server %d receives a snapshot till index %d\n", kv.me, applyMsg.SnapshotIndex)
				kv.applySnapshot(applyMsg.Snapshot)
				// server receiving snapshot must be a follower/crashed leader so no need to reply
			}
			kv.mu.Unlock()
		}
	}
	//}
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

	initGlobalTimer()

	DPrintf("Server %d launched!\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxStateForRaft = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.waitCh = make(map[int]chan *CommandReply)
	kv.sessions = make(map[int64]ClientRecord)

	// restore snapshot
	kv.applySnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	return kv
}
