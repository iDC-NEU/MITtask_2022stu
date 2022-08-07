package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
		"bytes"
	"sync"
	"sync/atomic"
		"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"fmt"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到leader message时的任期号

	//Volatile state on all servers:
	commitIndex int 	// 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int 	// 最后一个被应用到状态机日志的索引值 (初始为0)
										//注意command被处理的顺序为 leader收到client的command 将其写入自己的log，此时并command提交
										//leader等待大多数follower将command写入自身log后，应用该command修改自身state并通知其余follower执行apply操作
										//通常情况下commitIndex与lastApplied应该相同，当新的提交尚未apply时，lastApplied值较小

	//Volatile state on leaders
	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标 当leader与follower log出现不一致时，需要强制同步follower到matchindex对应的log entry

	role   Role        // 该节点是什么角色（状态）
	overtime time.Duration // 设置超时时间，200-400ms
	timer    *time.Ticker  // 每个节点中的计时器


	applyChan chan ApplyMsg // 用来写入通道
}

type Role int
const (
	Follower Role = iota
	Candidate
	Leader
)
type LogEntry struct {
	Term    int
	Command interface{}
}

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //Raft节点已终止
	Expire                  //投票(消息\竞选者）过期
	Voted                   //本Term内已经投过票

)

const (
	AppNormal    AppendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppRepeat                              // 追加重复 (2B
	AppCommited                            // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B

)


type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int        // 用于匹配日志的任期是否是合适的是，是否有冲突
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
	AppState AppendEntriesState // 追加状态

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int)  {

	if rf.killed() {
		return 
	}

	// paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {

		if rf.killed() {
			return 
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}

	// 必须在加在这里否则加载前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对reply的返回状态进行分支
	switch reply.AppState {

	// 目标节点crash
	case AppKilled:
		{
			return 
		}

	// 目标节点正常返回
	case AppNormal:
		{
			// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
						// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			// 2B需要判断返回的节点是否超过半数commit，才能将自身commit
			if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
				*appendNums++
			}

			// 说明返回的值已经大过了自身数组
			if rf.nextIndex[server] > len(rf.logs)+1 {
				return
			}
			rf.nextIndex[server] += len(args.Entries)
			if *appendNums > len(rf.peers)/2 {
				// 保证幂等性，不会提交第二次
				*appendNums = 0

				if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return
				}

				for rf.lastApplied < len(rf.logs) {
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied-1].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyChan <- applyMsg
					rf.commitIndex = rf.lastApplied
					// fmt.Printf("[	sendAppendEntries func-rf(%v)	] commitLog  \n", rf.me)
				}

			}
			// fmt.Printf("serve:(%v)  [	sendAppendEntries func-rf(%v)	] rf.log :%+v  ; rf.lastApplied:%v\n",
				// server,rf.me, rf.logs, rf.lastApplied)

			return
		}
	case Mismatch, AppCommited:
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term
			rf.persist()
		}
		rf.nextIndex[server] = reply.UpNextIndex

	//If AppendEntries RPC received from new leader: convert to follower(paper - 5.2)
	//reason: 出现网络分区，该Leader已经OutOfDate(过时）
	case AppOutOfDate:

		// 该节点变成追随者,并重置rf状态
		rf.role = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term
		rf.persist()
	}
	return 
}





// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 节点crash
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	// 出现网络分区，args的任期，比当前raft的任期还小，说明args之前所在的分区已经OutOfDate
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

		// 出现conflict的情况
	// paper:Reply false if log doesn’t contain an entry at prevLogIndex,whose term matches prevLogTerm (§5.3)
	// 首先要保证自身len(rf)大于0否则数组越界
	// 1、 如果preLogIndex的大于当前日志的最大的下标说明跟随者缺失日志，拒绝附加日志
	// 2、 如果preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志
	if args.PrevLogIndex > 0 && (len(rf.logs) < args.PrevLogIndex || rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {

		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	// 如果当前节点提交的Index比传过来的还高，说明当前节点的日志已经超前,需返回过去
	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommited
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}


	// 对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.role = Follower
	rf.timer.Reset(rf.overtime)

	// 对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	// 如果存在日志包那么进行追加
	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)

	}
	rf.persist()
	// 将日志提交至与Leader相同
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
		// fmt.Printf("[	AppendEntries func-rf(%v)	] commitLog  (%v).lastApplied:(%v),LeaderCommit:(%v)\n", rf.me,rf.me,rf.lastApplied,args.LeaderCommit)
	}


	return
}




// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	// fmt.Println("the peer[", rf.me, "] state is:", rf.role)
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	//fmt.Printf("%v\n", string(data))

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs

		//fmt.Printf("RaftNode[%d] persist read, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, currentTerm, votedFor, logs)
	}

}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}


//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool      // 是否投票给了该竞选人
	VoteState   VoteState // 投票状态
}




//
// example RequestVote RPC handler.
//
/*
* 我们在这个函数中需要实现将请求者的日志和被请求者的日志作对比
* 1.比较最后一项日志的Term，也就是LastLogTerm，相同的话比较索引，也就是LastLogIndex，如果当前节点较新的话就不会投票 expire
* 2.如果当前节点的Term比候选者节点的Term大，拒绝投票	expire
* 3.如果当前节点的Term比候选者节点的Term小，那么当前节点转换为Follwer状态
* 4.判断是否已经投过票，如果没投票并且能投票(日志更旧)，那么就投票给该候选者 voted/normal
*/

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// defer fmt.Printf("[	    func-RequestVote-rf(%+v)		] : return %v\n", rf.me, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前节点crash
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	//2.如果当前节点的Term比候选者节点的Term大，拒绝投票
	//reason: 出现网络分区，该竞选者已经OutOfDate(过时）
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//3.如果当前节点的Term比候选者节点的Term小，那么当前节点转换为Follwer状态
	if args.Term > rf.currentTerm {
		// 重置自身的状态
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// fmt.Printf("[	    func-RequestVote-rf(%+v)		] : rf.voted: %v\n", rf.me, rf.votedFor)
	if rf.votedFor == -1 {

		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0
		// 如果currentLogIndex下标不是-1就把term赋值过来
		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}

		//1.比较最后一项日志的Term，也就是LastLogTerm，相同的话比较索引，也就是LastLogIndex，如果当前节点较新的话就不会投票
		//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		// 论文里的第二个匹配条件，当前peer要符合arg两个参数的预期
		if args.LastLogTerm < currentLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < currentLogIndex  ) {
			reply.VoteState = Expire	
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		// if args.LastLogTerm < lastLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)) {

		// 	reply.VoteState = Expire
		// 	reply.VoteGranted = false
		// 	reply.Term = rf.currentTerm
		// 	return
		// }

		// 投票，并且返回true
		rf.votedFor = args.CandidateId

		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		rf.timer.Reset(rf.overtime)

		// fmt.Printf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)

	} else { // 4.此时term相同，但是票已经给了，存在两种情况

		reply.VoteState = Voted
		reply.VoteGranted = false

		// i:当前的节点是来自同一轮，不同竞选者的，但是票数已经给了(又或者它本身自己就是竞选者）
		if rf.votedFor != args.CandidateId {
			// 告诉reply票已经没了返回false
			return
		} else { // ii:. 当前的节点票已经给了同一个人了，但是由于sleep等网络原因，又发送了一次请求
			// 重置自身状态
			rf.role = Follower
			rf.persist()
		}

		rf.timer.Reset(rf.overtime)

	}

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)

	}

	//需要在这里加锁，因为上面的RPC请求也需要锁。在上面加锁就会导致死锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("[	sendRequestVote(%v) ] : send a election to %v\n", rf.me, server)
	/*
	* 根据投票的返回结果进行处理
	* 这里可能有三种情况
	* 1.返回值Trem小于发送者 -> 无效 抛弃这条信息
	* 2.返回值Trem大于发送者 -> 状态转移为follower 并更新纪元
	* 3.投票有效
	*/
	// 1. 由于网络分区，返回值Trem小于发送者 -> 无效 抛弃这条信息，不给予投票
	if reply.Term < rf.currentTerm {
		return false
	}

	switch reply.VoteState {
	// 消息过期有两种情况:
	// i .本身的term过期了比节点的还小
	// ii .节点日志的条目落后于节点了
	//2.返回值Trem大于发送者 -> 状态转移为follower 并更新term
   case Expire:
		{
			rf.role = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
			rf.persist()
		}
	case Normal, Voted:
		//3. 投票有效 根据是否同意投票，收集选票数量
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}

		// 票数超过一半
		if *voteNums >= (len(rf.peers)/2)+1 {

			*voteNums = 0
			// 本身就是leader在网络分区中更具有话语权的leader
			if rf.role == Leader {
				return ok
			}

			// 本身不是leader，就会成为新的leader并初始化nextIndex数组
			rf.role = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.persist()
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.timer.Reset(HeartBeatTimeout)
			// fmt.Printf("[	sendRequestVote-func-rf(%v)		] be a leader\n", rf.me)
		}
	case Killed:
		return false
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader，直接返回
	if rf.role != Leader {
		return index, term, false
	}

	isLeader = true

	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.	
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().


		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

		// 当定时器结束进行超时选举
		select {

		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			// fmt.Printf("[	    func-ticker-(%+v)		] : rf.voted: %v\n", rf.me, rf.overtime)
			rf.mu.Lock()
			// 根据自身的status进行一次ticker
			switch rf.role {

			// follower变成竞选者
			case Follower:
				rf.role = Candidate
				fallthrough
			case Candidate:
			
				// 初始化自身的任期、并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1 // 统计自身的票数

				// 每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200-400ms
				rf.timer.Reset(rf.overtime)
				rf.persist()
				// 对自身以外的节点进行选举
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			
			case Leader:
			
				// 进行心跳/日志同步
				appendNums := 1 // 正确返回的节点数量
				rf.timer.Reset(HeartBeatTimeout)
				// 构造msg
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,	//当leader 满足条件apply之后leader的commitIndex增1。这样保证了leader总是先于follower进行apply
					}

					appendEntriesReply := AppendEntriesReply{}
						// 如果nextIndex[i]长度不等于rf.logs,代表与leader的log entries不一致，需要附带过去

						appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]-1:]

						// 代表已经不是初始值0
						if rf.nextIndex[i] > 0 {
							appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
						}
	
						if appendEntriesArgs.PrevLogIndex > 0 {
							// fmt.Println("i:",i,"  len(rf.log):", len(rf.logs), "PrevLogIndex):", appendEntriesArgs.PrevLogIndex, "rf.nextIndex[i]", rf.nextIndex[i])
							appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-1].Term
						}
	
					// fmt.Printf("[	ticker(%v) ] : send a election to %v\n", rf.me, i)
					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
				}
			
			}
			rf.mu.Unlock()
		}

	
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
		rf := &Raft{}
		rf.peers = peers
		rf.persister = persister
		rf.me = me
	
		// Your initialization code here (2A, 2B, 2C).
	
		// 对应论文中的初始化
		rf.applyChan = applyCh //2B
	
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.logs = make([]LogEntry, 0)
	
		rf.commitIndex = 0
		rf.lastApplied = 0
	
		rf.nextIndex = make([]int, len(peers))
		rf.matchIndex = make([]int, len(peers))
	
		rf.role = Follower
		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms
		rf.timer = time.NewTicker(rf.overtime)
		
		// initialize from state persisted before a crash
		rf.readPersist(persister.ReadRaftState())
	
		// fmt.Printf("[ 	Make-func-rf(%v) 	]:  %v\n", rf.me, rf.overtime)
		// start ticker goroutine to start elections
		go rf.ticker()
	
		return rf

}

