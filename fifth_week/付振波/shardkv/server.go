package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import (
	"6.824/shardctrler"
	"sync/atomic"
	"time"
	"bytes"
)

type Shard struct {
	ShardIndex int
	DBPerShard map[string]string
	ClientIdRequestId map[int64]int
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
	Config shardctrler.Config
	MigrateData []Shard
	MigrateConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead 		 int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	ck       	 *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB []Shard
	OpApplyCh map[int]chan Op
	config shardctrler.Config
	migratingShard [10]bool

	SnapShotLogIndex int
}

func (kv *ShardKV) CheckShardState(clientNum int,shardIndex int)(bool,bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == clientNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

func (kv *ShardKV) CheckMigrateState(shard []Shard) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,data := range shard {
		if kv.migratingShard[data.ShardIndex] {
			return false
		}
	}
	return true
}


func (kv *ShardKV) ifDuplicate(newClientId int64, newRequestId int, shardNum int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ok := kv.DB[shardNum].ClientIdRequestId[newClientId]
	if !ok {
		return false
	}
	return newRequestId <= lastRequestId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIndex := key2shard(args.Key)
	isRespons, isAvali := kv.CheckShardState(args.NumConfig,shardIndex)
	if !isRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvali {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Op: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}

	rfIndex, _, _ := kv.rf.Start(op)
	
	kv.mu.Lock()
	FromRfIndex, ok := kv.OpApplyCh[rfIndex]
	if !ok {
		kv.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = kv.OpApplyCh[rfIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * 500):
		_, isLeader := kv.rf.GetState()
		if kv.ifDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) && isLeader {
			valueg, exists := kv.GetOnDB(op)
			if exists {
				reply.Err = OK
				reply.Value = valueg
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-FromRfIndex:
		//DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			valuep, existt := kv.GetOnDB(op)
			if existt {
				reply.Err = OK
				reply.Value = valuep
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.OpApplyCh, rfIndex)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) GetOnDB(op Op) (string, bool){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.DB[shardNum].DBPerShard[op.Key]
	kv.DB[shardNum].ClientIdRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
	return value,exist
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	isRespons, isAvali := kv.CheckShardState(args.NumConfig,shardIndex)
	if !isRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !isAvali {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	rfIndex, _, _ := kv.rf.Start(op)
	//DPrintf("[PUTAPPEND StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d",args.ClientId,args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	FromRfIndex, exist := kv.OpApplyCh[rfIndex]
	if !exist {
		kv.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = kv.OpApplyCh[rfIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*500) :
		if kv.ifDuplicate(op.ClientId,op.RequestId,key2shard(op.Key)){
			reply.Err = OK
		} else{
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <- FromRfIndex :
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId  {
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.OpApplyCh,rfIndex)
	kv.mu.Unlock()
	return
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
	for i:=0;i<10;i++ {
		kv.DB[i] = Shard{ShardIndex: i, DBPerShard: make(map[string]string), ClientIdRequestId: make(map[int64]int)}
	}
	kv.ck = shardctrler.MakeClerk(kv.ctrlers)
	kv.OpApplyCh = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.ReadSnapShotToInstall(snapshot)
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.RfCommand()
	go kv.PullNewConfig()
	go kv.SendShard()

	return kv
}

func (kv *ShardKV) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb []Shard
	var persist_config shardctrler.Config
	var persist_migratingShard [10]bool

	if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_config) != nil || d.Decode(&persist_migratingShard) != nil{
		
	} else {
		kv.DB = persist_kvdb
		kv.config = persist_config
		kv.migratingShard = persist_migratingShard
	}
}

func (kv *ShardKV) RfCommand() {
	for message := range kv.applyCh{
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			kv.GetCommand(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShot(message)
		}
	}
}

func (kv *ShardKV) GetSnapShot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.SnapShotLogIndex = message.SnapshotIndex
	}
}

func (kv *ShardKV) IfSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardKV) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.DB)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) GetCommand(message raft.ApplyMsg) {
	op := message.Command.(Op)
	
	if message.CommandIndex <= kv.SnapShotLogIndex {
		return
	}

	if op.Op == "newconfig" {
		kv.NewConfigOp(op)
		if kv.maxraftstate != -1{
			kv.IfSendSnapShotCommand(message.CommandIndex,9)
		}
		return
	}
	if op.Op == "migrate" {
		kv.MigrateShardsOp(op)
		if kv.maxraftstate != -1{
			kv.IfSendSnapShotCommand(message.CommandIndex,9)
		}
		kv.SendMessage(op,message.CommandIndex)
		return
	}
	if !kv.ifDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		// execute command
		if op.Op == "Put" {
			kv.PutOnDB(op)
		}
		if op.Op == "Append" {
			kv.AppendDB(op)
		}
	}
	if kv.maxraftstate != -1{
		kv.IfSendSnapShotCommand(message.CommandIndex,9)
	}

	kv.SendMessage(op,message.CommandIndex)
}

func (kv *ShardKV) NewConfigOp(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newestConfig := op.Config
	if newestConfig.Num != kv.config.Num+1 {
		return
	}
	
	for shard := 0; shard < 10;shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	kv.lockMigratingShard(newestConfig.Shards)
	kv.config = newestConfig
}

func (kv *ShardKV) lockMigratingShard(newShards [10]int) {
	oldShards := kv.config.Shards
	for shard := 0;shard < 10;shard++ {
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) MigrateShardsOp(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.MigrateConfigNum != myConfig.Num {
		return
	}
	for _, shard := range op.MigrateData {
		if !kv.migratingShard[shard.ShardIndex] {
			continue
		}
		kv.migratingShard[shard.ShardIndex] = false
		kv.DB[shard.ShardIndex] = Shard{ShardIndex: shard.ShardIndex,DBPerShard: make(map[string]string),ClientIdRequestId: make(map[int64]int)}

		if myConfig.Shards[shard.ShardIndex] == kv.gid {
			CopyShard(&kv.DB[shard.ShardIndex],shard)
		}
	}
}

func CopyShard (shard *Shard, recive Shard) {
	for key,value := range recive.DBPerShard {
		shard.DBPerShard[key] = value
	}
	for clientid,requestid := range recive.ClientIdRequestId {
		shard.ClientIdRequestId[clientid] = requestid
	}
}

func (kv *ShardKV) PutOnDB(op Op) {
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	kv.DB[shardNum].DBPerShard[op.Key] = op.Value
	kv.DB[shardNum].ClientIdRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *ShardKV) AppendDB(op Op){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value,ok := kv.DB[shardNum].DBPerShard[op.Key]
	if ok {
		kv.DB[shardNum].DBPerShard[op.Key] = value + op.Value
	} else {
		kv.DB[shardNum].DBPerShard[op.Key] = op.Value
	}
	kv.DB[shardNum].ClientIdRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()
}

func (kv *ShardKV) SendMessage(op Op, raftIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.OpApplyCh[raftIndex]
	if ok {
		ch <- op
	}
	return ok
}

func (kv *ShardKV) PullNewConfig() {
	for !kv.killed(){
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		_,isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader{
			time.Sleep(100*time.Millisecond)
			continue
		}

		newestConfig := kv.ck.Query(lastConfigNum+1)
		if newestConfig.Num == lastConfigNum+1 {
			// Got a new Config
			op := Op{Op: "newconfig", Config: newestConfig}
			kv.mu.Lock()
			if _,isLeader := kv.rf.GetState(); isLeader{
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		}

		time.Sleep(100*time.Millisecond)
	}
}

func (kv *ShardKV) isSendData() (bool, map[int][]Shard) {
	Data := kv.MakeSendData()
	if len(Data) == 0 {
		return false,make(map[int][]Shard)
	}
	return true,Data
}

func (kv *ShardKV) MakeSendData()(map[int][]Shard){
	// kv.config already be update
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Data := make(map[int][]Shard)
	for shard :=0;shard<10;shard++ {
		nowOwner := kv.config.Shards[shard]
		if kv.migratingShard[shard] && kv.gid != nowOwner{
			temp := Shard{ShardIndex: shard,DBPerShard: make(map[string]string),ClientIdRequestId: make(map[int64]int)}
			CopyShard(&temp,kv.DB[shard])
			Data[nowOwner] = append(Data[nowOwner],temp)
		}
	}
	return Data
}

func (kv *ShardKV) SendShard() {
	for !kv.killed(){
		kv.mu.Lock()
		_,isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader{
			time.Sleep(150*time.Millisecond)
			continue
		}

		noMigrateing := true
		kv.mu.Lock()
		for shard := 0;shard < 10;shard++ {
			if kv.migratingShard[shard]{
				noMigrateing = false
			}
		}
		kv.mu.Unlock()
		if noMigrateing{
			time.Sleep(150*time.Millisecond)
			continue
		}

		ok, Data := kv.isSendData()
		if !ok{
			time.Sleep(150*time.Millisecond)
			continue
		}
		kv.SendData(Data)
		time.Sleep(150*time.Millisecond)
	}
}

func (kv *ShardKV) SendData(Data map[int][]Shard) {
	for aimGid, shard := range Data {
		kv.mu.Lock()
		args := &MigrateShardArgs{ConfigNum: kv.config.Num, MigrateData: make([]Shard,0)}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()
		for _,i := range shard {
			temp := Shard{ShardIndex: i.ShardIndex,DBPerShard: make(map[string]string),ClientIdRequestId: make(map[int64]int)}
			CopyShard(&temp,i)
			args.MigrateData = append(args.MigrateData,temp)
		}

		go kv.callMigrateRPC(groupServers,args)
	}
}

func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs){
	for _, groupMember := range groupServers {
		callEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := callEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && migrateReply.Err == OK {
			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData){
				return
			} else {
				kv.rf.Start(Op{Op: "migrate",MigrateData: args.MigrateData,MigrateConfigNum: args.ConfigNum})
				return
			}
		}
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigNum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}

	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	op := Op{Op: "migrate", MigrateData: args.MigrateData, MigrateConfigNum: args.ConfigNum}

	rfIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	FromRfIndex, ok := kv.OpApplyCh[rfIndex]
	if !ok {
		kv.OpApplyCh[rfIndex] = make(chan Op, 1)
		FromRfIndex = kv.OpApplyCh[rfIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * 500):
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		tempConfig := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) && isLeader {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-FromRfIndex:
		kv.mu.Lock()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		if raftCommitOp.MigrateConfigNum == args.ConfigNum && args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.OpApplyCh, rfIndex)
	kv.mu.Unlock()
	return
}
