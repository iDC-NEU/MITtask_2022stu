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
	"math/rand"
	"time"

	"6.824/labgob"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type LogIndexType = int

const MinTimeout = 250
const MaxTimeout = 400
const HeartBeatTime = 50

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

type LogEntry struct {
	Command interface{}
	Term    int
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
	currentTerm int            // 当前任期
	votedFor    int            // 支持的候选人
	logs        []LogEntry     // 存储log
	commitId    LogIndexType   // 已知已提交的最高日志条目的索引
	lastApplied LogIndexType   // 已经被应用到状态机的最高的日志条目的索引
	nextIndex   []LogIndexType // 对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex  []LogIndexType // 对于每个服务器，已知的已经复制到该服务器的最高日志条目的索引

	electTimeout      int        // 选举超时时间
	isLeader          bool       // 是否是leader
	lastHeartBeatTime int64      // 上一次心跳包的时间
	voteMutex         sync.Mutex // 投票的mutex
	leaderId          int        // leader的id

	lastIncludeIndex LogIndexType // 快照中最后被包含的索引
	lastIncludeTerm  int          // 快照中最后被包含的任期
	snapNums         LogIndexType // 快照中存储的条数

	logMutex sync.RWMutex // 对于log的读写锁

	isPreVoteSuccess bool // preVote投票是否成功
	isFarBehind      bool // 日志是否是远远落后的

	//isSending		[]bool		// 判断是否正在发送
	sendMutexs     []sync.Mutex // 发送的mutex
	isHeartConfirm []bool       // 开启心跳确认
	persistMutex   sync.Mutex   // 持久化log
	canElection    bool         // 是否可以进行选举, 开始为false，try过之后为true
}

func (rf *Raft) init() {
	rf.setElectTimeout()
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	rf.isLeader = false
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitId = 0
	rf.lastApplied = 0
	rf.logs = append(rf.logs, LogEntry{
		Command: nil,
		Term:    -1,
	})
	rf.leaderId = -1

	rf.lastIncludeIndex = -1
	rf.lastIncludeTerm = 0
	rf.snapNums = 0

	rf.nextIndex = make([]LogIndexType, len(rf.peers))
	rf.matchIndex = make([]LogIndexType, len(rf.peers))

	rf.isPreVoteSuccess = true // 预投票结果
	rf.isFarBehind = false

	rf.sendMutexs = make([]sync.Mutex, len(rf.peers))
	rf.isHeartConfirm = make([]bool, len(rf.peers))
	rf.canElection = false
	//rf.persistMutex.Lock()
	//rf.persist()
	//rf.persistMutex.Unlock()

}

func (rf *Raft) setElectTimeout() {
	rand.Seed(int64(rf.me) * time.Now().UnixMilli())
	rf.electTimeout = rand.Intn(MaxTimeout-MinTimeout) + MinTimeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader
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
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitId)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	rf.persistMutex.Lock()
	defer rf.persistMutex.Unlock()
	d.Decode(&rf.logs)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.commitId)
	d.Decode(&rf.votedFor)
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

type AppendEntriesArgs struct {
	Term         int          // leader的任期
	LeaderId     int          // leader的id
	PrevLogIndex LogIndexType // 紧邻新日志条目的那个日志条目的索引
	PrevLogTerm  int          // 紧邻新日志条目的那个日志条目的任期
	Entries      []LogEntry   // 日志条目
	LeaderCommit LogIndexType // leader的已知已提交的最高日志条目的索引
	IsConfirm    bool         // 是否可以进行确认
}

type AppendEntriesReply struct {
	NeedTerm  int  // 需要发送的任期
	NeedIndex int  // 需要发送的索引
	Term      int  // 当前任期
	Success   bool // 是否成功
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int          //候选人任期号
	CandidateId  int          // 请求投票的候选人ID
	LastLogIndex LogIndexType // 候选人的最后日志条目的索引
	LastLogTerm  int          // 候选人最后日志条目的任期号
	IsTryElect   bool         // 是否是预选举
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 接收者当前任期号
	VoteGranted bool // 是否投票给它
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLastLogIndex() LogIndexType {
	return LogIndexType(len(rf.logs)-1) + rf.snapNums
}

func (rf *Raft) ProcessHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if !rf.checkLeader(args.Term) {
		reply.Success = false
		return
	}
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	rf.isLeader = false
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	rf.currentTerm = args.Term
	reply.Success = true
}

func (rf *Raft) checkLeader(leaderTerm int) bool {
	return leaderTerm >= rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	if !rf.isLeader {
		return index, term, false
	}
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()

	// Your code here (2B).
	//return index, term, isLeader
	isLeader = rf.isLeader
	if isLeader {
		index, term = rf.recordCommand(command)
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	sleepTime := rf.electTimeout
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(sleepTime+2) * time.Millisecond)
		if rf.isLeader {
			continue
		}
		interval := int(time.Now().UnixMilli() - rf.lastHeartBeatTime)
		if interval > rf.electTimeout && rf.isFarBehind {
			rf.resetVotedFor()
			rf.setElectTimeout()
			sleepTime = rf.electTimeout
		}
		if interval > rf.electTimeout && !rf.isFarBehind {
			rf.setElectTimeout()
			if rf.canElection || rf.tryElection() {
				rf.canElection = true

				if rf.election() {
					rf.canElection = false
					go rf.sendHeartBeat()
				}
			}
			rf.resetVotedFor()
			sleepTime = rf.electTimeout
		} else {
			sleepTime = rf.electTimeout - interval
		}

	}
}

// 预选举
func (rf *Raft) tryElection() bool {
	if rf.killed() {
		return false
	}
	isSuccess := rf.getNeedSupport(int(len(rf.peers)/2)+1, true)
	// 如果在投票期间接收到有新的leader的心跳包，则承认新leader，结束投票
	return isSuccess
}

// 正式选举
func (rf *Raft) election() bool {
	if rf.killed() {
		return false
	}
	rf.setElectTimeout()
	// 如果投给自己失败保持follower状态
	if !rf.tryVoteForSelf() {
		return false
	}
	rf.currentTerm++
	rf.persistMutex.Lock()
	rf.persist()
	rf.persistMutex.Unlock()
	rf.isLeader = false
	lastHeartBeatTime := rf.lastHeartBeatTime
	isSuccess := rf.getNeedSupport(int(len(rf.peers)/2)+1, false)
	// 如果在投票期间接收到有新的leader的心跳包，则承认新leader，结束投票
	if lastHeartBeatTime != rf.lastHeartBeatTime {
		return false
	}
	if isSuccess {
		rf.isLeader = true
		rf.leaderId = rf.me
		rf.initFollowerIndex()
		return true
	}
	return false
}

func (rf *Raft) getNeedSupport(needNum int, isTryElect bool) bool {
	var currentTerm int
	if isTryElect {
		currentTerm = rf.currentTerm + 1
	} else {
		currentTerm = rf.currentTerm
	}
	voteArgs := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
		IsTryElect:   isTryElect,
	}
	supportNum := 1
	unSupportNum := 0
	ch := make(chan bool, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, ch chan bool) {
			voteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &voteArgs, &voteReply)
			if ok {
				ch <- voteReply.VoteGranted
			}
		}(i, ch)
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case isSupport := <-ch:
			if isSupport {
				supportNum++
				if supportNum == needNum {
					return true
				}
			} else {
				unSupportNum++
				if unSupportNum == needNum {
					rf.isFarBehind = true
					return false
				}
			}
		case <-time.After(time.Duration(MinTimeout) * time.Millisecond):
			return false
		}
	}
	return false

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	if !args.IsTryElect && args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.isLeader && args.Term >= rf.currentTerm {
		rf.isLeader = false
	}
	if !args.IsTryElect && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		return
	}
	if !args.IsTryElect && rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}

	// 如果请求的log较新，则投票给它
	if args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		rf.lastHeartBeatTime = time.Now().UnixMilli()
		if !args.IsTryElect {
			rf.canElection = false
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.persistMutex.Lock()
			rf.persist()
			rf.persistMutex.Unlock()
		}
		//rf.lastHeartBeatTime = time.Now().UnixMilli()
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false

}

func (rf *Raft) tryVoteForSelf() bool {
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	if rf.votedFor == -1 {
		rf.votedFor = rf.me
		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
		return true
	}
	return false
}

func (rf *Raft) resetVotedFor() {
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	rf.votedFor = -1
	rf.persistMutex.Lock()
	rf.persist()
	rf.persistMutex.Unlock()
}

func (rf *Raft) initFollowerIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.commitId + 1
		rf.isHeartConfirm[i] = false
	}
}

// 返回新log的index以及term
func (rf *Raft) recordCommand(command interface{}) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return 0, 0
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logMutex.Lock()
	rf.logs = append(rf.logs, logEntry)
	rf.persist()
	rf.logMutex.Unlock()
	logIndex := len(rf.logs) - 1
	go func(logIndex int) {
		if rf.sendLogs() {
			if rf.commitId < logIndex {
				rf.commitId = logIndex
			}

		}
	}(logIndex)

	return logIndex, rf.currentTerm
}

func (rf *Raft) sendLogs() bool {
	followerNum := len(rf.peers) - 1
	resultCh := make(chan bool, followerNum)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.AppendToFollower(i, resultCh)
	}
	majority := int(len(rf.peers)/2) + 1
	successNum := 1
	for i := 0; i < followerNum; i++ {
		select {
		case isSuccess := <-resultCh:
			if isSuccess {
				successNum++
				if successNum == majority {
					return true
				}
			} else {
				return false
			}
		case <-time.After(time.Duration(MinTimeout) * time.Millisecond):
			return false
		}
	}
	return false
}

func (rf *Raft) AppendToFollower(followerId int, ch chan bool) {
	startTime := time.Now().UnixMilli()
	appendReply := AppendEntriesReply{
		NeedTerm:  rf.currentTerm,
		NeedIndex: rf.nextIndex[followerId],
		Term:      rf.currentTerm,
		Success:   false,
	}
	var logLen int
	var nextIndex int
	for appendReply.Success == false {
		nextIndex = rf.adjustSendIndex(followerId, appendReply.NeedTerm, appendReply.NeedIndex)
		if nextIndex == 0 {
			nextIndex = 1
		}
		rf.logMutex.RLock()
		logEntries := rf.getSendLogs(followerId, nextIndex)
		appendArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getPrevTerm(nextIndex - 1),
			Entries:      logEntries,
			LeaderCommit: rf.commitId,
			IsConfirm:    true,
		}
		logLen = len(rf.logs)
		rf.logMutex.RUnlock()

		ok := false
		for !ok && rf.isLeader {
			ok = rf.peers[followerId].Call("Raft.AppendLogEntries", &appendArgs, &appendReply)
		}

		if appendReply.Success {
			rf.isHeartConfirm[followerId] = true
			ch <- true
		}

		if appendReply.Term > rf.currentTerm {
			rf.isLeader = false
			return
		}
		if time.Now().UnixMilli()-startTime > MinTimeout && !appendReply.Success {
			return
		}
		rf.nextIndex[followerId] = nextIndex

	}
	rf.sendMutexs[followerId].Lock()
	rf.nextIndex[followerId] = logLen
	rf.matchIndex[followerId] = logLen
	rf.sendMutexs[followerId].Unlock()

}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		if !rf.isLeader {
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendingHeartBeat(i)
		}
		time.Sleep(time.Duration(HeartBeatTime) * time.Millisecond)
	}
}

func (rf *Raft) sendingHeartBeat(followerId int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[followerId] - 1,
		PrevLogTerm:  rf.getPrevTerm(rf.nextIndex[followerId] - 1),
		Entries:      nil,
		LeaderCommit: rf.commitId,
		IsConfirm:    rf.isHeartConfirm[followerId],
	}
	reply := AppendEntriesReply{}
	rf.peers[followerId].Call("Raft.AppendLogEntries", &args, &reply)

}

func (rf *Raft) getSendLogs(followerId int, startIndex int) []LogEntry {

	// TODO: 快照后这里可能需要改
	return rf.logs[startIndex-rf.snapNums:]
}

func (rf *Raft) getPrevTerm(logIndex int) int {

	return rf.logs[logIndex-rf.snapNums].Term
}

func (rf *Raft) adjustSendIndex(followerId int, needTerm int, needIndex int) int {

	nextIndex := rf.nextIndex[followerId]
	index := needIndex - rf.snapNums
	for index >= len(rf.logs) {
		index--
	}
	if rf.logs[index].Term <= needTerm {
		nextIndex = index + rf.snapNums
	} else {
		for i := index; i != -1; i-- {
			if rf.logs[i].Term <= needTerm {
				nextIndex = i + rf.snapNums
				break
			}
		}
	}
	return nextIndex
}

func (rf *Raft) AppendLogEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if !rf.checkLeader(args.Term) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.isLeader = false

	rf.lastHeartBeatTime = time.Now().UnixMilli()
	reply.Term = rf.currentTerm
	if rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
	}
	rf.canElection = false
	rf.leaderId = args.LeaderId
	// 不带确认的心跳包的处理
	if !args.IsConfirm {
		return
	}

	// 匹配不通过的情况
	if args.PrevLogIndex > rf.snapNums+len(rf.logs)-1 ||
		rf.logs[args.PrevLogIndex-rf.snapNums].Term != args.PrevLogTerm {

		reply.Success = false
		reply.NeedTerm, reply.NeedIndex = rf.getNeedIndex(args.PrevLogTerm, args.PrevLogIndex)
		return
	}
	// 成功的情况
	// 写入新条目
	if args.Entries != nil && len(args.Entries) != 0 {
		startIndex := args.PrevLogIndex - rf.snapNums + 1
		rf.logMutex.Lock()
		if args.PrevLogIndex+len(args.Entries) < len(rf.logs)+rf.snapNums {
			isEqual := true
			j := startIndex
			for i := 0; i < len(args.Entries); i++ {
				if args.Entries[i] != rf.logs[j] {
					isEqual = false
					break
				}
				j++
			}
			if !isEqual {
				rf.logs = append(rf.logs[:startIndex], args.Entries...)
			}
		} else {
			rf.logs = append(rf.logs[:startIndex], args.Entries...)
		}
		//rf.logs = append(rf.logs[:args.PrevLogIndex-rf.snapNums+1], args.Entries...)
		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
		rf.logMutex.Unlock()
		rf.isFarBehind = false

	}
	// 判断commit
	if rf.snapNums+len(rf.logs)-1 < args.LeaderCommit {
		rf.commitId = rf.snapNums + len(rf.logs) - 1
	} else {
		rf.commitId = args.LeaderCommit
	}

	reply.Success = true

}

// 返回值第一个是需要的term，第二个是需要的index
func (rf *Raft) getNeedIndex(prevTerm int, prevIndex int) (int, int) {
	lastIndex := len(rf.logs) - 1
	realIndex := prevIndex - rf.snapNums
	// prev 比当前log长的情况
	if lastIndex+rf.snapNums < prevIndex {
		if rf.logs[lastIndex].Term <= prevTerm {
			return rf.logs[lastIndex].Term, lastIndex + rf.snapNums
		} else {
			for i := lastIndex; i != -1; i-- {
				if rf.logs[i].Term <= prevTerm {
					return rf.logs[lastIndex].Term, rf.snapNums + i
				}
			}
		}
	}
	// 相应位置的term比原来的小的情况
	if rf.logs[realIndex].Term < prevTerm {
		return rf.logs[realIndex].Term, realIndex
	}
	// 相应位置的term比原来的大的情况
	for i := realIndex; i != -1; i-- {
		if rf.logs[i].Term <= prevTerm {
			return rf.logs[i].Term, realIndex
		}
	}
	return 0, 0
}
func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		var appliedMsgs = make([]ApplyMsg, 0)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitId > rf.lastApplied && rf.lastApplied+1 < len(rf.logs) {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied-rf.snapNums].Command,
					CommandIndex: rf.lastApplied,
				})
			}
		}()
		// 锁外提交给应用层
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
}

func (rf *Raft) printState() {
	for !rf.killed() {
		time.Sleep(time.Duration(500) * time.Millisecond)
		return
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
	rf.init()

	go rf.applyLogLoop(applyCh)
	//go rf.printState()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
