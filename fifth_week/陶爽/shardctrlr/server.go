package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JoinType  = "join"
	LeaveType = "leave"
	MoveType  = "move"
	QueryType = "query"

	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	// InvalidGid all shards should be assigned to GID zero (an invalid GID).
	InvalidGid = 0
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	seqMap    map[int64]int   //重复性检测	clientId -> seqId
	waitChMap map[int]chan Op //传递由下层Raft服务的appCh传过来的command	index / chan(Op)

}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SeqId       int
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

// StartServer
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
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	// You may need initialization code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.applyMsgHandlerLoop()
	return sc
}

//The Join RPC is used by an administrator to add new replica groups. Its argument is a set of mappings from unique,
//non-zero replica group identifiers (GIDs) to lists of server names.
// Join参数就是为一个组，一个组对应的就是gid -> lists of server names.
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: JoinType, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: LeaveType, SeqId: args.SeqId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(LeaveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: MoveType, SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(MoveOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// Query 相应配置号的配置
// The shardctrler replies with the configuration that has that number. If the number is -1 or bigger than the biggest
// known configuration number, the shardctrler should reply with the latest configuration. The result of Query(-1) should
// reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: QueryType, SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	//fmt.Printf("[ ----Server[%v]----] : send a Query,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(QueryOverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader

		} else {

			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// 处理applyCh发送过来的ApplyMsg
func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {

		select {
		case msg := <-sc.applyCh:
			// 走正常的Command
			if msg.CommandValid {

				index := msg.CommandIndex
				op := msg.Command.(Op)

				// 判断是不是重复的请求
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveGid, op.MoveShard))
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}

				// 将返回的ch返回waitCh
				sc.getWaitCh(index) <- op
			}

		}
	}
}

// JoinHandler 处理Join进来的gid
// The shardctrler should react by creating a new configuration that includes the new replica groups. The new
//configuration should divide the shards as evenly as possible among the full set of groups, and should move as few
//shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the
//current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {

	// 取出最后一个config将分组加进去
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片(group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}

	// 记录每个分组存了多少分片
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	// 都没存自然不需要负载均衡,初始化阶段
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}

	//需要负载均衡的情况
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards),
		Groups: newGroups,
	}
}

// LeaveHandler 处理需要离开的组
// The shardctrler should create a new configuration that does not include those groups, and that assigns those groups'
// shards to the remaining groups. The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {

	// 用set感觉更合适点但是go并没有内置的set..
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	// 取出最新配置的groups组进行填充
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}

	// 删除对应的gid的值
	for _, leaveGid := range gids {
		delete(newGroups, leaveGid)
	}

	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片(group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards

	// 对groupMap进行初始化
	for gid := range newGroups {

		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}

	}

	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// 如果这个组在leaveMap中，则置为0
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				GroupMap[gid]++
			}
		}

	}
	// 直接删没了
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

// MoveHandler 为指定的分片分配指定的组
// The shardctrler should create a new configuration in which the shard is assigned to the group. The purpose of Move is
// to allow us to test your software. A Join or Leave following a Move will likely un-do the Move, since Join and Leave
// re-balance.
func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{}}

	// 填充并赋值
	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}
	newConfig.Shards[shard] = gid

	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}

	return &newConfig
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

// 负载均衡
// GroupMap : gid -> servers[]
// lastShards : shard -> gid
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(GroupMap)

	// 先把负载多的部分free
	for i := 0; i < length; i++ {
		target := ave

		// 判断这个数是否需要更多分配，因为不可能完全均分，在前列的应该为ave+1
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		// 超出负载
		if GroupMap[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := GroupMap[overLoadGid] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	// 为负载少的group分配多出来的group
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if GroupMap[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGid {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}

	}
	return lastShards
}

// 根据sortGroupShard进行排序
// GroupMap : groupId -> shard nums
func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)

	gidSlice := make([]int, 0, length)

	// map转换成有序的slice
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}

	// 让负载压力大的排前面
	// except: 4->3 / 5->2 / 6->1 / 7-> 1 (gids -> shard nums)
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {

			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] || (GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

func moreAllocations(length int, remainder int, i int) bool {

	// 这个目的是判断index是否在安排ave+1的前列:3、3、3、1 ,ave: 10/4 = 2.5 = 2,则负载均衡后应该是2+1,2+1,2,2
	if i < length-remainder {
		return true
	} else {
		return false
	}
}
