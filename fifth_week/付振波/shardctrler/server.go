package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "bytes"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate int
	OpApplyCh map[int]chan Op //operation数组，保存待处理的Op
	lastRequId map[int64]int // clientid -> requestID

	configs []Config // indexed by config num

	SnapShotLogIndex int
}


type Op struct {
	// Your data here.
	Operation string
	ClientId int64
	RequestId int
	NumQuery int
	ServersJoin  map[int][]string
	GidsLeave []int
	ShardMove int
	GidMove int
}

//判断该请求是否被处理过
func (sc *ShardCtrler) ifDuplicate(ClientId int64, RequestId int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastRequId, ok := sc.lastRequId[ClientId]
	if !ok {
		return false
	}
	return RequestId <= lastRequId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _,isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}
	op := Op{Operation: "join", ClientId: args.ClientId, RequestId: args.Requestid, ServersJoin: args.Servers}
	rfIndex,_,_ := sc.rf.Start(op)

	sc.mu.Lock()
	FromRfIndex, ok := sc.OpApplyCh[rfIndex]
	if !ok {
		sc.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = sc.OpApplyCh[rfIndex]
	}
	sc.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*5000):
		if sc.ifDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = WrongLeader
		}
	case raftCommitOp := <-FromRfIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = WrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.OpApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _,isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}

	op := Op{Operation: "leave", ClientId: args.ClientId, RequestId: args.Requestid, GidsLeave: args.GIDs}
	rfIndex,_,_ := sc.rf.Start(op)

	sc.mu.Lock()
	FromRfIndex, ok := sc.OpApplyCh[rfIndex]
	if !ok {
		sc.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = sc.OpApplyCh[rfIndex]
	}
	sc.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*5000):
		if sc.ifDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = WrongLeader
		}
	case raftCommitOp := <-FromRfIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		}else {
			reply.Err = WrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.OpApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _,isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}

	op := Op{Operation: "move", ClientId: args.ClientId, RequestId: args.Requestid, ShardMove: args.Shard,GidMove: args.GID}
	rfIndex,_,_ := sc.rf.Start(op)

	sc.mu.Lock()
	FromRfIndex, ok := sc.OpApplyCh[rfIndex]
	if !ok {
		sc.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = sc.OpApplyCh[rfIndex]
	}
	sc.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*5000):
		if sc.ifDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = WrongLeader
		}
	case raftCommitOp := <-FromRfIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		}else {
			reply.Err = WrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.OpApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _,isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}

	op := Op{Operation: "query", ClientId: args.ClientId, RequestId: args.Requestid, NumQuery: args.Num}
	rfIndex,_,_ := sc.rf.Start(op)

	sc.mu.Lock()
	FromRfIndex, ok := sc.OpApplyCh[rfIndex]
	if !ok {
		sc.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = sc.OpApplyCh[rfIndex]
	}
	sc.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*5000):
		_,isLeader := sc.rf.GetState()
		if sc.ifDuplicate(op.ClientId, op.RequestId) && isLeader{
			reply.Config = sc.QueryController(op)
			reply.Err = OK
		}else {
			reply.Err = WrongLeader
		}
	case raftCommitOp := <-FromRfIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Config = sc.QueryController(op)
			reply.Err = OK
		}
	}

	sc.mu.Lock()
	delete(sc.OpApplyCh, rfIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) QueryController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequId[op.ClientId] = op.RequestId
	if op.NumQuery == -1 || op.NumQuery >= len(sc.configs){
		return sc.configs[len(sc.configs) - 1]
	} else {
		return sc.configs[op.NumQuery]
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

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.maxraftstate = -1

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.OpApplyCh = make(map[int]chan Op)
	sc.lastRequId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		sc.ReadSnapShotToInstall(snapshot)
	}
	go sc.RfCommand()

	return sc
}

func (sc *ShardCtrler) RfCommand(){
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommand(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShot(message)
		}
	}
}

func (kv *ShardCtrler) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastRequId map[int64]int

	if d.Decode(&persist_config) != nil || d.Decode(&persist_lastRequId) != nil {
		
	} else {
		kv.configs = persist_config
		kv.lastRequId = persist_lastRequId
	}
}

func (kv *ShardCtrler) GetSnapShot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.SnapShotLogIndex = message.SnapshotIndex
	}
}

func (sc *ShardCtrler) GetCommand(message raft.ApplyMsg){
	op := message.Command.(Op)

	if message.CommandIndex <= sc.SnapShotLogIndex {
		return
	}
	//DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v",sc.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation)
	if !sc.ifDuplicate(op.ClientId, op.RequestId){
		if op.Operation == "join" {
			sc.JoinController(op)
		}
		if op.Operation == "leave" {
			sc.LeaveController(op)
		}
		if op.Operation == "move" {
			sc.MoveController(op)
		}
	}

	if sc.maxraftstate != -1{
		sc.IfSendSnapShotCommand(message.CommandIndex,9)
	}

	sc.SendMessage(op, message.CommandIndex)
}

func (kv *ShardCtrler) IfSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardCtrler) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.configs)
	e.Encode(kv.lastRequId)
	data := w.Bytes()
	return data
}

func (sc *ShardCtrler) SendMessage(op Op, raftIndex int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.OpApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) JoinController(op Op) {
	sc.mu.Lock()
	sc.lastRequId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.JoinConfig(op.ServersJoin))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) LeaveController(op Op) {
	sc.mu.Lock()
	sc.lastRequId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.LeaveConfig(op.GidsLeave))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) MoveController(op Op) {
	sc.mu.Lock()
	sc.lastRequId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MoveConfig(op.ShardMove,op.GidMove))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) JoinConfig(servers map[int][]string) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)
	
	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for gids, serverLists := range servers {
		tempGroups[gids] = serverLists
	}

	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}

	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.BalanceShards(GidToShardNumMap,lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) LeaveConfig(gids []int) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	ifLeaveSet := make(map[int]bool)
	for _, gid := range gids {
		ifLeaveSet[gid] = true
	}

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for _, gidLeave := range gids {
		delete(tempGroups,gidLeave)
	}

	newShard := lastConfig.Shards
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}

	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}

	}
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.BalanceShards(GidToShardNumMap,newShard),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MoveConfig(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{Num: len(sc.configs),
						Shards: [10]int{},
						Groups: map[int][]string{}}
	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}
	tempConfig.Shards[shard] = gid

	for gidss, servers := range lastConfig.Groups {
		tempConfig.Groups[gidss] = servers
	}

	return &tempConfig
}

func NumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1],numArray[j]
			}
		}
	}
	return numArray
}

func (sc *ShardCtrler) BalanceShards(GidToShardNumMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	subNum := NShards % length
	realSortNum := NumArray(GidToShardNumMap)

	for i := length - 1; i >= 0; i-- {
		resultNum := average
		if i >= length-subNum{
			resultNum = average+1
		}
		if resultNum < GidToShardNumMap[realSortNum[i]] {
			fromGid := realSortNum[i]
			changeNum := GidToShardNumMap[fromGid] - resultNum
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					lastShards[shard] = 0
					changeNum -= 1
				}
			}
			GidToShardNumMap[fromGid] = resultNum
		}
	}

	for i := 0; i < length; i++ {
		resultNum := average
		if i >= length-subNum{
			resultNum = average+1
		}
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[toGid]
			for shard, gid := range lastShards{
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = toGid
					changeNum -= 1
				}
			}
			GidToShardNumMap[toGid] = resultNum
		}

	}
	return lastShards
}

