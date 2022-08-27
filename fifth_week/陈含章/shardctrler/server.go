package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const TIMEOUT = 500 * time.Millisecond

const INTERVAL = 10 * time.Millisecond

const Debug = false

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	configs          []Config
	sessions         map[int64]ClientRecord
	waitChs          map[int]chan *CommandReply
	lastAppliedIndex int

	// for lab4b
	ready map[int]map[int]bool // map configNum -> map gid->bool, need groups in both current config and proposed config to be ready
}

type Op struct {
	// Your data here.
	ClientID  int64
	RequestID int
	Type      Command
	Servers   map[int][]string // for join: new GID -> servers mappings
	GIDs      []int            // for leave: GIDs to remove
	Shard     int              // for move: the shard to be assigned
	GID       int              // for move: the specified GID
	Num       int              // for query: desired config number
	ConfigNum int              // for ready: proposed configNum
	Group     int              // for ready: the gid of ready group
}

// merge read/write RPCs to one
// RPC handler for client's command

func (sc *ShardCtrler) QueryOrReady(args *CommandArgs, reply *CommandReply) {

	sc.mu.Lock()

	if sc.checkDuplicate(args.ClientID, args.RequestID) {
		reply.Err = sc.sessions[args.ClientID].LastResponse.Err
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	// send command to raft for agreement
	index, _, isLeader := sc.rf.Start(Op{args.ClientID, args.RequestID, args.Type, args.Servers, args.GIDs, args.Shard, args.GID, args.Num, args.ConfigNum, args.Group})
	if index == -1 || !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh := sc.getWaitCh(index)
	sc.mu.Unlock()

	select {
	case agreement := <-waitCh:
		switch args.Type {
		case Ready:
			for {
				sc.mu.Lock()

				if sc.CheckAllReady(args.ConfigNum) {
					//if all ready
					reply.Err = OK
					sc.mu.Unlock()
					break
				}
				sc.mu.Unlock()
				time.Sleep(INTERVAL)
			}

		case Query:
			reply.Err = agreement.Err
			reply.Config = agreement.Config
		}

	case <-time.NewTimer(TIMEOUT).C:
		reply.Err = ErrTimeout
	}

	go sc.closeWaitCh(index)

}

func (sc *ShardCtrler) CheckAllReady(configNum int) bool {
	isAllReady := true
	for _, ready := range sc.ready[configNum] {
		if !ready {
			isAllReady = false
			break
		}
	}
	return isAllReady
}

func (sc *ShardCtrler) JoinOrLeaveOrMove(args *CommandArgs, reply *CommandReply) {

	index, _, isLeader := sc.rf.Start(Op{args.ClientID, args.RequestID, args.Type, args.Servers, args.GIDs, args.Shard, args.GID, args.Num, args.ConfigNum, args.Group})
	if index == -1 || !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh := sc.getWaitCh(index)
	sc.mu.Unlock()

	select {
	case agreement := <-waitCh:
		reply.Err = agreement.Err
		reply.Config = agreement.Config

	case <-time.NewTimer(TIMEOUT).C:
		reply.Err = ErrTimeout
	}

	go sc.closeWaitCh(index)
}

// method to check duplicated CommandRequest
func (sc *ShardCtrler) checkDuplicate(clientID int64, requestID int) bool {
	clientRecord, ok := sc.sessions[clientID]
	return ok && requestID <= clientRecord.RequestID
}

// method to get a waitCh for an expected commit index
func (sc *ShardCtrler) getWaitCh(index int) chan *CommandReply {
	ch, ok := sc.waitChs[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		sc.waitChs[index] = ch
		return ch
	}
	return ch
}

//close
func (sc *ShardCtrler) closeWaitCh(index int) {
	sc.mu.Lock()
	ch, ok := sc.waitChs[index]
	if ok {
		close(ch)
		delete(sc.waitChs, index)
	}
	sc.mu.Unlock()
}

//func (sc *ShardCtrler) applyQuery(op Op, reply *CommandReply) {
//	if 0 <= op.Num && op.Num < len(sc.configs) {
//		reply.Config = sc.configs[op.Num]
//	} else {
//		reply.Config = sc.configs[len(sc.configs)-1]
//	}
//}

func (sc *ShardCtrler) applyQuery(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	//var currentConfig Config

	if 0 <= op.Num && op.Num < len(sc.configs) {
		reply.Config = sc.configs[op.Num]
	} else {
		reply.Config = sc.configs[len(sc.configs)-1]
	}

	return reply
}

func (sc *ShardCtrler) applyJoinOrLeave(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	var currentConfig Config

	currentConfig = sc.configs[len(sc.configs)-1]
	currentDist := make(map[int][]int)
	for shard, gid := range currentConfig.Shards {
		if gid != 0 {
			currentDist[gid] = append(currentDist[gid], shard)
		}
	}

	newGIDs := make([]int, 0)
	newGroups := make(map[int][]string)
	toRemove := make(map[int]bool)
	for _, gid := range op.GIDs {
		toRemove[gid] = true
	}

	for gid, servers := range currentConfig.Groups {
		if _, remove := toRemove[gid]; !remove {
			newGIDs = append(newGIDs, gid)
			newGroups[gid] = servers
		}
	}
	for gid, servers := range op.Servers {
		_, dup := currentConfig.Groups[gid]
		_, remove := toRemove[gid]
		if !dup && !remove {
			newGIDs = append(newGIDs, gid)
			newGroups[gid] = servers
		}
	}

	newConfig := Config{
		Num:    currentConfig.Num + 1,
		Shards: sc.getBalance(currentDist, toRemove, newGIDs),
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newConfig)

	// for lab4b, initialize new ready map
	sc.ready[newConfig.Num] = make(map[int]bool)
	for group := range currentConfig.Groups {
		sc.ready[newConfig.Num][group] = false
	}
	for group := range newConfig.Groups {
		sc.ready[newConfig.Num][group] = false
	}

	return reply

}

func (sc *ShardCtrler) applyReady(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	//var currentConfig Config

	// handle ready
	readyMap := sc.ready[op.ConfigNum]
	if _, ok := readyMap[op.Group]; ok {
		readyMap[op.Group] = true
	}

	return reply
}

func (sc *ShardCtrler) applyMove(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	var currentConfig Config

	currentConfig = sc.configs[len(sc.configs)-1]
	var newShards [NShards]int
	for shard, gid := range currentConfig.Shards {
		if shard != op.Shard {
			newShards[shard] = gid
		} else {
			newShards[shard] = op.GID
		}
	}
	newConfig := Config{
		Num:    currentConfig.Num + 1,
		Shards: newShards,
		Groups: currentConfig.Groups,
	}
	sc.configs = append(sc.configs, newConfig)

	// for lab4b, initialize new ready map
	sc.ready[newConfig.Num] = make(map[int]bool)
	for group := range currentConfig.Groups {
		sc.ready[newConfig.Num][group] = false
	}
	for group := range newConfig.Groups {
		sc.ready[newConfig.Num][group] = false
	}

	return reply
}

func (sc *ShardCtrler) getBalance(currentDist map[int][]int, toRemove map[int]bool, newGIDs []int) [NShards]int {
	newDist := make(map[int][]int, 0)
	// sort newGIDs by load in decreasing order, if equal load then by GID in increasing order
	sort.Ints(newGIDs)
	sort.SliceStable(newGIDs, func(i int, j int) bool {
		return len(currentDist[newGIDs[i]]) > len(currentDist[newGIDs[j]])
	})

	if len(newGIDs) == 0 {
		return [NShards]int{}
	}

	avg := NShards / len(newGIDs)        // avg load per group
	numHeavier := NShards % len(newGIDs) // the number of groups carrying 1 more shard
	queue := make([]int, 0)              // queue for idle shards

	var assigned [NShards]bool
	for gid, v := range currentDist {
		if _, remove := toRemove[gid]; !remove {
			for _, shard := range v {
				assigned[shard] = true
			}
		}
	}
	for i := 0; i < NShards; i++ {
		if !assigned[i] {
			queue = append(queue, i)
		}
	}

	assignLoad := func(gid int, target int) {
		switch load := len(currentDist[gid]); {
		case load > target:
			queue = append(queue, currentDist[gid][target:]...)
			newDist[gid] = currentDist[gid][:target]
		case load == target:
			newDist[gid] = currentDist[gid]
		case load < target:
			newDist[gid] = append(currentDist[gid], queue[:target-load]...)
			queue = queue[target-load:]
		}
	}

	for _, gid := range newGIDs {
		if numHeavier == 0 {
			// assign average load
			assignLoad(gid, avg)
		} else {
			// assign heavier load
			assignLoad(gid, avg+1)
			numHeavier--
		}
	}

	var newShards [NShards]int
	for gid, v := range newDist {
		for _, shard := range v {
			newShards[shard] = gid
		}
	}
	return newShards
}

//
// long-running applier goroutine
//
func (sc *ShardCtrler) applier() {
	for !sc.killed() {

		if applyMsg := <-sc.applyCh; applyMsg.CommandValid {
			op := applyMsg.Command.(Op)

			sc.mu.Lock()

			//do if not outdated
			if applyMsg.CommandIndex > sc.lastAppliedIndex {
				sc.lastAppliedIndex = applyMsg.CommandIndex

				var reply *CommandReply
				// check for duplicates before apply to state machine
				if op.Type != Query && op.Type != Ready && sc.checkDuplicate(op.ClientID, op.RequestID) {
					reply = sc.sessions[op.ClientID].LastResponse
				} else {
					switch op.Type {
					case Query:
						reply = sc.applyQuery(op)

					case Ready:
						reply = sc.applyReady(op)

					case Move:
						reply = sc.applyMove(op)

					case Leave:
						reply = sc.applyJoinOrLeave(op)

					case Join:
						reply = sc.applyJoinOrLeave(op)
					}

					if op.Type != Query && op.Type != Ready {
						sc.sessions[op.ClientID] = ClientRecord{op.RequestID, reply}
					}
				}

				// check the same term and leadership before reply
				if currentTerm, isLeader := sc.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
					ch := sc.getWaitCh(applyMsg.CommandIndex)
					ch <- reply
				}
				sc.mu.Unlock()
			} else {
				sc.mu.Unlock()
				continue
			}

		}

	}
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

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
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

	// initialize global timestamp
	//if gStart.IsZero() {
	//	gStart = time.Now()
	//}
	//DPrintf("Ctrl server %d launched!\n", me)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChs = make(map[int]chan *CommandReply)
	sc.sessions = make(map[int64]ClientRecord)

	sc.ready = make(map[int]map[int]bool)

	go sc.applier()
	return sc
}
