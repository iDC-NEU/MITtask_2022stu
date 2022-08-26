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
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	currentTerm int      // 当前任期
	votedFor    int      // 支持的候选人
	logs        LogStore // 存储log
	commitId    int      // 已知已提交的最高日志条目的索引
	lastApplied int      // 已经被应用到状态机的最高的日志条目的索引
	nextIndex   []int    // 对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex  []int    // 对于每个服务器，已知的已经复制到该服务器的最高日志条目的索引

	electTimeout      int        // 选举超时时间
	isLeader          bool       // 是否是leader
	lastHeartBeatTime int64      // 上一次心跳包的时间
	supportNum        int        // 支持的人数
	unsupportNum      int        // 不支持的人数
	voteMutex         sync.Mutex // 投票的mutex
	leaderId          int        // leader的id
	electFailNum      int        // 选举失败次数

	logMutex sync.RWMutex // 对于log的读写锁

	isPreVoteSuccess bool // preVote投票是否成功
	isFarBehind      bool // 日志是否是远远落后的

	persistMutex    sync.Mutex // 持久化log
	canElection     bool       // 是否可以进行选举, 开始为false，try过之后为true
	isStartElection bool       // 是否开始了选举
	lastReceiveTime []int64    // leader上一次接收到follower信息的时间
	newCommandCh    chan LogEntry

	snapshot []byte // 用于存储快照
	applyCh  chan ApplyMsg

	IsPrint bool
	GID     int
}

func (rf *Raft) init(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setElectTimeout()
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	rf.isLeader = false
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitId = 0
	rf.lastApplied = 0
	rf.logs.Init()
	rf.leaderId = -1
	rf.electFailNum = 1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.newCommandCh = make(chan LogEntry, 100)

	rf.isPreVoteSuccess = true // 预投票结果
	rf.isFarBehind = false

	rf.canElection = false
	rf.lastReceiveTime = make([]int64, len(rf.peers))
	rf.supportNum = 0
	rf.unsupportNum = 0
	rf.applyCh = applyCh

	rf.logs.Append(LogEntry{
		Command: nil,
		Term:    0,
	})
}

func (rf *Raft) setElectTimeout() {
	rand.Seed(int64(rf.me) * time.Now().UnixMilli())
	rf.electTimeout = rand.Intn(MaxTimeout-MinTimeout) + MinTimeout
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
	isleader = rf.isLeader
	return term, isleader
}

func (rf *Raft) GetStateNum() int {
	return rf.persister.RaftStateSize()
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
	e.Encode(rf.logs.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitId)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs.lastIncludeTerm)
	e.Encode(rf.logs.lastIncludeIndex)
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
	d.Decode(&rf.logs.log)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.commitId)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs.lastIncludeTerm)
	d.Decode(&rf.logs.lastIncludeIndex)
	rf.lastApplied = rf.logs.lastIncludeIndex
	if rf.lastApplied < 0 {
		rf.lastApplied = 0
	}
	rf.logs.snapNum = rf.logs.lastIncludeIndex + 1
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
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
	//DPrintln("Start 对任期", rf.currentTerm, "的", rf.me, "isLeader:", rf.isLeader)
	if isLeader {
		//DPrintln("Start中发现任期", rf.currentTerm, "的leader是", rf.me)
		index, term = rf.recordCommand(command)
		if index == -1 {
			isLeader = false
		}
	} else {
		//DPrintln(rf.me, "不是leader, leader是", rf.leaderId)
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
	rf.init(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogLoop()

	return rf
}
