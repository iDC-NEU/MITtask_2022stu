package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"time"
)

//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyChan passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyChan, but set CommandValid to false for these
// other uses.
//
type State int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

// InstallSnapshotState 安装快照的状态
type InstallSnapshotState int

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

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

const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 200
	MinVoteTime  = 150

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 120
	AppliedSleep   = 30
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's status
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted status
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有的servers需要持久化的变量:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	commitIndex int
	lastApplied int

	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	applyChan chan ApplyMsg // 用来写入通道

	state      State
	voteNum    int
	votedTimer time.Time

	// 2D中用于传入快照点
	lastIncludeIndex int
	lastIncludeTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 竞选人最后日志条目的任期号
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term
	VoteGranted bool // 是否投票给了该竞选人
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int        // 用于匹配日志的任期是否是合适的是，是否有冲突
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term          int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success       bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	ConflictIndex int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent status, and also initially holds the most
// recent saved status, if any. applyChan is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

// 最小值min
func min(num int, num1 int) int {
	if num > num1 {
		return num1
	} else {
		return num
	}
}

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

// UpToDate paper中投票RPC的rule2
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludeIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludeIndex == 0 {
		return rf.lastIncludeTerm
	}
	//fmt.Printf("[GET] curIndex:%v,rf.lastIncludeIndex:%v\n", curIndex, rf.lastIncludeIndex)
	return rf.logs[curIndex-rf.lastIncludeIndex].Term
}

// 获取最后的快照日志下标(代表已存储）
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}

// 获取最后的任期
func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，否则最直接len == 0
	if len(rf.logs)-1 == 0 {
		return rf.lastIncludeTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.state = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()

	// initialize from status persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		// 时间过期发起选举
		// 此处的流程为每次每次votedTimer如果小于在sleep睡眠之前定义的时间，就代表没有votedTimer没被更新为最新的时间，则发起选举
		if rf.votedTimer.Before(nowTime) && rf.state != Leader {
			// 转变状态
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			//fmt.Printf("[++++elect++++] :Rf[%v] send a election\n", rf.me)
			rf.sendElection()
			rf.votedTimer = time.Now()

		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.state != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.state = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {

						//fmt.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						rf.state = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(i)

	}

}

// RequestVote
// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s logs is at
	// least as up-to-date as receiver’s logs, grant vote
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || // 判断日志是否conflict
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // paper中的第二个条件votedFor is null

		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}

}

func (rf *Raft) leaderAppendEntries() {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			//installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
			// 同时要注意的是比快照还小时，已经算是比较落后
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			//fmt.Printf("[TIKER-SendHeart-Rf(%v)-To(%v)] args:%+v,curStatus%v\n", rf.me, server, args, rf.status)
			re := rf.sendAppendEntries(server, &args, &reply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}

				if reply.Success {

					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 外层遍历下标是否满足,从快照最后开始反向进行
					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 大于一半，且因为是从后往前，一定会大于原本commitIndex
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}

					}
				} else { // 返回为冲突
					// 如果冲突不为-1，则进行更新
					if reply.ConflictIndex != -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("[	AppendEntries--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.ConflictIndex = -1

	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 自身的快照Index比发过来的prevLogIndex还大，所以返回冲突的下标加1(原因是冲突的下标用来更新nextIndex，nextIndex比Prev大1
	// 返回冲突下标的目的是为了减少RPC请求次数
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex() + 1
		return
	}

	// 如果自身最后的快照日志比prev小说明中间有缺失日志，such 3、4、5、6、7 返回的开头为6、7，而自身到4，缺失5
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex()
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.ConflictIndex = index + 1
					break
				}
			}
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// 进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	return
}

func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.state = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyChan <- msg

}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	//fmt.Println("[Snapshot] commintIndex", rf.commitIndex)
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	//fmt.Printf("[Snapshot-Rf(%v)]rf.commitIndex:%v,index:%v\n", rf.me, rf.commitIndex, index)
	// 更新快照下标/任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs

	// apply了快照就应该重置commitIndex、lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyChan.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//
// save Raft's persistent status to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted status.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any status?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.state != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		return index, term, true
	}
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
