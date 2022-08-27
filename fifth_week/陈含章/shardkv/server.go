package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const TIMEOUT = 500 * time.Millisecond

const POLL = 50 * time.Millisecond

const INTERVAL = 10 * time.Millisecond

const Debug = false

var gStart time.Time

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf("%06d ", time.Since(gStart).Milliseconds())
		fmt.Printf(prefix+format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int

	// Your definitions here.
	ctrl      *shardctrler.Clerk // shard controler
	persister *raft.Persister    // the persister of the kv server

	// persistent state
	db               map[int]Shard
	lastAppliedIndex int
	appliedInternal  map[int64]bool
	config           shardctrler.Config
	state            State

	// volatile state
	dead    int32
	waitChs map[int]chan *CommandReply
}

// merge read/write RPCs to one
// RPC handler for client's command

func (kv *ShardKV) GetRequest(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()

	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.state != Serving {
		reply.Err = ErrNotServing
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// send command to raft for agreement
	index, _, isLeader := kv.rf.Start(Op{
		Command:   "Request",
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	if index == -1 || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.MakeReply(reply, index)
	go kv.closeWaitCh(index)

}

func (kv *ShardKV) PutAppendRequest(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()

	// check if should serve the key
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// if not serving
	if kv.state != Serving {
		reply.Err = ErrNotServing
		kv.mu.Unlock()
		return
	}

	// check for duplicates
	if kv.checkDuplicate(args.Key, args.ClientID, args.RequestID) {
		reply.Err = kv.db[key2shard(args.Key)].Sessions[args.ClientID].LastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send command to raft for agreement
	index, _, isLeader := kv.rf.Start(Op{
		Command:   "Request",
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	if index == -1 || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.MakeReply(reply, index)

	go kv.closeWaitCh(index)

}

func (kv *ShardKV) MakeReply(reply *CommandReply, index int) {
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

// method to check if given key should be served in the current config
func (kv *ShardKV) checkShard(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

// method to check duplicated CommandRequest
func (kv *ShardKV) checkDuplicate(key string, clientID int64, requestID int) bool {
	clientRecord, ok := kv.db[key2shard(key)].Sessions[clientID]
	return ok && requestID <= clientRecord.RequestID
}

// method to get a waitCh for an expected commit index
func (kv *ShardKV) getWaitCh(index int) chan *CommandReply {
	ch, ok := kv.waitChs[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		kv.waitChs[index] = ch
		return ch
	}
	return ch
}

//close
func (kv *ShardKV) closeWaitCh(index int) {
	kv.mu.Lock()
	ch, ok := kv.waitChs[index]
	if ok {
		close(ch)
		delete(kv.waitChs, index)
	}
	kv.mu.Unlock()
}

// method to apply request command to db
func (kv *ShardKV) applyCommand(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}

	if op.Type == "Get" {
		shard, ok := kv.db[key2shard(op.Key)]
		if ok {
			reply.Value = shard.Data[op.Key]
		} // else can reply empty string for no-key
	}

	if op.Type == "Put" {
		shard, ok := kv.db[key2shard(op.Key)]
		if ok {
			shard.Data[op.Key] = op.Value
		}
	}

	if op.Type == "Append" {
		shard, ok := kv.db[key2shard(op.Key)]
		if ok {
			shard.Data[op.Key] += op.Value
		}
	}
	return reply
}

//
// From lab 3b
//

// the mothod to determine if raft state is oversized
func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate >= 0 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

// the method to take snapshot, call with lock held
func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.appliedInternal)
	e.Encode(kv.config)
	e.Encode(kv.state)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// method to apply snapshot to state machine, call with lock held
func (kv *ShardKV) applySnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var DB map[int]Shard
	var LastAppliedIndex int
	var AppliedInternal map[int64]bool
	var Config shardctrler.Config
	var State State

	// decode, print error but do not panic
	err1 := d.Decode(&DB)
	err2 := d.Decode(&LastAppliedIndex)
	err3 := d.Decode(&AppliedInternal)
	err4 := d.Decode(&Config)
	err5 := d.Decode(&State)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		DPrintf("Decoding error:%v, %v\n")
	} else {
		// apply
		kv.db = DB
		kv.lastAppliedIndex = LastAppliedIndex
		kv.appliedInternal = AppliedInternal
		kv.config = Config
		kv.state = State
	}
}

//
// For lab 4b
//

// method to start config transition under 2pc protocol
func (kv *ShardKV) startConfigTransition(newConfig shardctrler.Config) {
	// stop serving
	if kv.state != Ready {
		if !kv.changeState(Prepare) {
			return
		}
		// pull shards to newDB
		newDB := *kv.pullShards(newConfig)

		// change group state - have to be atomic
		if !kv.prepare(newConfig, newDB) {
			return
		}
	}

	// send ready message to ctrl and wait for commit message
	kv.ctrl.Ready(newConfig.Num, kv.gid)

	// garbage collection and start serving
	if !kv.changeState(Serving) {
		return
	}

}

//
// long-running applier goroutine
//
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// commited command
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)

				kv.mu.Lock()
				// if outdated, ignore
				if applyMsg.CommandIndex <= kv.lastAppliedIndex {
					kv.mu.Unlock()
					continue
				}
				// if requestOp no longer serving, ignore
				if op.Command == "Request" && (kv.state != Serving || !kv.checkShard(op.Key)) {
					kv.mu.Unlock()
					continue
				}

				kv.lastAppliedIndex = applyMsg.CommandIndex

				var reply *CommandReply

				switch op.Command {
				case "Request":
					// check for duplicates before apply to state machine
					if op.Type != "Get" && kv.checkDuplicate(op.Key, op.ClientID, op.RequestID) {
						reply = kv.db[key2shard(op.Key)].Sessions[op.ClientID].LastResponse
					} else {
						reply = kv.applyCommand(op)
						if op.Type == "Put" || op.Type == "Append" {
							kv.db[key2shard(op.Key)].Sessions[op.ClientID] = ClientRecord{op.RequestID, reply}
						}
					}

				case "Internal":
					// check for duplicates before apply to state machine
					if applied, ok := kv.appliedInternal[op.InternalID]; applied && ok {
						kv.mu.Unlock()
						continue
					}
					kv.applyCommandInternal(op)
					// mark the internal command applied
					kv.appliedInternal[op.InternalID] = true
				}

				if kv.needSnapshot() {
					// DPrintf("Server %d takes a snapshot till index %d\n", kv.me, applyMsg.CommandIndex)
					kv.takeSnapshot(applyMsg.CommandIndex)
				}

				// check the same term and leadership before reply
				if currentTerm, isLeader := kv.rf.GetState(); op.Command == "Request" && currentTerm == applyMsg.CommandTerm && isLeader {
					ch := kv.getWaitCh(applyMsg.CommandIndex)
					ch <- reply
				}
				kv.mu.Unlock()
			} else { // committed snapshot
				kv.mu.Lock()
				if kv.lastAppliedIndex < applyMsg.SnapshotIndex {
					// DPrintf("Server %d receives a snapshot till index %d\n", kv.me, applyMsg.SnapshotIndex)
					kv.applySnapshot(applyMsg.Snapshot)
					// server receiving snapshot must be a follower/crashed leader so no need to reply
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// long running poller goroutine
//
func (kv *ShardKV) poller() {
	// make sure raft applied all its logs before starting polling
	for !kv.killed() {
		if kv.sendEmpty() {
			break
		}
		time.Sleep(INTERVAL)
	}
	// DPrintf("Group %d sendEmpty finished!\n", kv.gid)
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			// ask for newer config
			kv.mu.Lock()
			var configNum int
			// if already in ready state, trying to progress to current config
			if kv.state == Ready {
				configNum = kv.config.Num - 1
			} else {
				configNum = kv.config.Num
			}
			kv.mu.Unlock()
			if newConfig := kv.ctrl.Query(configNum + 1); newConfig.Num > configNum {
				kv.startConfigTransition(newConfig)
			}
		}
		time.Sleep(POLL)
	}
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
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.ctrl = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// initialize group state
	kv.config = shardctrler.Config{}
	kv.state = Serving
	kv.appliedInternal = make(map[int64]bool)

	// initialize db
	kv.db = make(map[int]Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.db[i] = Shard{
			Num:      i,
			Sessions: make(map[int64]ClientRecord),
			Data:     make(map[string]string),
		}
	}

	kv.waitChs = make(map[int]chan *CommandReply)

	// restore snapshot
	kv.applySnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.poller()

	return kv
}

// method to apply command to internal state
func (kv *ShardKV) applyCommandInternal(op Op) {
	switch op.Type {
	case "Prepare":
		kv.config = copyConfig(op.Config)
		kv.db = copyDB(op.DB)
		kv.state = op.State

	case "State":
		kv.state = op.State

	case "Empty":
	}
}

// method to prepare for the newconfig with the pulled newDB
func (kv *ShardKV) prepare(newConfig shardctrler.Config, newDB map[int]Shard) bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "Prepare",
		InternalID: internalID,
		Config:     newConfig,
		DB:         newDB,
		State:      Ready,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to change the state of this replica group
func (kv *ShardKV) changeState(state State) bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "State",
		InternalID: internalID,
		State:      state,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to send an empty log to raft to make sure the group state is up to date
func (kv *ShardKV) sendEmpty() bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "Empty",
		InternalID: internalID,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to persistently send internal command
func (kv *ShardKV) sendCommandInternal(op Op) bool {
	// persistently send internal command
	for !kv.killed() {
		// if no longer leader, return false
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return false
		}
		// if succeessfully applied return true
		if kv.commandInternal(op) {
			return true
		}
		time.Sleep(INTERVAL)
	}
	return false
}

// method to send internal command to raft layer, return true if agreed otherwise false
func (kv *ShardKV) commandInternal(op Op) bool {
	// send command to raft layer for agreement
	index, _, isLeader := kv.rf.Start(op)
	if index == -1 || !isLeader {
		return false
	}

	// checker loop
	for !kv.killed() {
		kv.mu.Lock()
		// if successfully applied
		if applied, ok := kv.appliedInternal[op.InternalID]; applied && ok {
			kv.mu.Unlock()
			return true
		} else {
			// if not applied
			if kv.lastAppliedIndex >= index {
				kv.mu.Unlock()
				return false
			}
		}
		kv.mu.Unlock()
		time.Sleep(INTERVAL)
	}
	return false
}

// RPC handler for shard migration
func (kv *ShardKV) PullShards(args *PullShardsArgs, reply *PullShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if the group does not have the shards for the required config
	var configNum int
	if kv.state == Ready {
		configNum = kv.config.Num - 1
	} else {
		configNum = kv.config.Num
	}
	if args.ConfigNum > configNum+1 {
		return
	}

	// if not leader or still serving, return
	if _, isLeader := kv.rf.GetState(); !isLeader || kv.state == Serving {
		return
	}

	// send shards to reply
	reply.Shards = []Shard{}
	for _, shardNum := range args.Shards {
		reply.Shards = append(reply.Shards, copyShard(kv.db[shardNum]))
	}
	reply.Err = OK
}

// method to pull shards from other groups, return a new db
func (kv *ShardKV) pullShards(newConfig shardctrler.Config) *map[int]Shard {
	kv.mu.Lock()
	oldConfig := kv.config

	needShards := make(map[int][]int) // map gid->shardNum
	for shardNum, gid := range newConfig.Shards {
		source := kv.config.Shards[shardNum]
		if gid == kv.gid && source != kv.gid {
			needShards[source] = append(needShards[source], shardNum)
		}
	}

	newDB := copyDB(kv.db)
	lock := sync.Mutex{}
	kv.mu.Unlock()

	// send pullShards RPC in parallel, use wait group to sync
	var wg sync.WaitGroup
	for gid, shards := range needShards {
		if gid != 0 {
			wg.Add(1)

			go func(gid int, shards []int, newDB *map[int]Shard, lock *sync.Mutex) {
				defer wg.Done()

				args := PullShardsArgs{
					ConfigNum: newConfig.Num,
					Shards:    shards,
				}
				servers := oldConfig.Groups[gid]

				for {
					// try each server in the group
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply PullShardsReply
						ok := srv.Call("ShardKV.PullShards", &args, &reply)
						// persistently try until OK
						if ok && reply.Err == OK {
							// update the shards in the newDB
							lock.Lock()
							for _, shard := range reply.Shards {
								db := *newDB
								db[shard.Num] = shard
							}
							lock.Unlock()
							return
						}
					}
				}

			}(gid, shards, &newDB, &lock)
		}
	}
	wg.Wait()
	return &newDB
}

// helper function to make a deep copy of db
func copyDB(db map[int]Shard) map[int]Shard {
	cp := make(map[int]Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		cp[i] = copyShard(db[i])
	}
	return cp
}

// helper function to make a deep copy of shard
func copyShard(shard Shard) Shard {
	cp := Shard{
		Num:      shard.Num,
		Sessions: make(map[int64]ClientRecord),
		Data:     make(map[string]string),
	}
	for key, value := range shard.Data {
		cp.Data[key] = value
	}
	for key, value := range shard.Sessions {
		cp.Sessions[key] = value
	}
	return cp
}

// helper function to make a deep copy of config
func copyConfig(config shardctrler.Config) shardctrler.Config {
	var shards [shardctrler.NShards]int
	for shard, gid := range config.Shards {
		shards[shard] = gid
	}
	cp := shardctrler.Config{
		Num:    config.Num,
		Shards: shards,
		Groups: make(map[int][]string),
	}
	for key, value := range config.Groups {
		cp.Groups[key] = value
	}
	return cp
}
