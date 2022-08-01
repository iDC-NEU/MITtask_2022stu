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
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

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

	state         RaftState
	appendEntryCh chan *Entry
	heartBeat     time.Duration
	electionTime  time.Time

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         Log

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

func (l *Log) at(idx int) *Entry {
	return &l.Entries[idx]
}

func (l *Log) truncate(idx int) {
	l.Entries = l.Entries[:idx]
}

func (l *Log) slice(idx int) []Entry {
	return l.Entries[idx:]
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLog() *Entry {
	return l.at(l.len() - 1)
}

func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for servers
	// all servers 2
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	// request vote rpc receiver 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// request vote rpc receiver 2
	myLastLog := rf.log.lastLog()
	upToDate := args.LastLogTerm > myLastLog.Term ||
		(args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		DPrintf("[%v]: term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once) {
	DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	if !reply.VoteGranted {
		DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	*voteCounter++

	if *voteCounter > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
		becomeLeader.Do(func() {
			DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)
			rf.state = Leader
			lastLogIndex := rf.log.lastLog().Index
			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
			rf.appendEntries(true)
		})
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	term := rf.currentTerm
	voteCounter := 1
	lastLog := rf.log.lastLog()
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	var becomeLeader sync.Once
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId, &args, &voteCounter, &becomeLeader)
		}
	}
}

func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.log.lastLog()
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		// rules for leader 3
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(peer, &args)
		}
	}
}

func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// rules for leader 3.1
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[serverId] = reply.XLen
			} else {
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}

			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// rules for servers
	// all servers 2
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// append entries rpc 1
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// candidate rule 3
	if rf.state == Candidate {
		rf.state = Follower
	}
	// append entries rpc 2
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc 3
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//DPrintVerbose("[%v]: STATE: %v", rf.me, rf.log.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.log.lastLog().Index + 1
	term := rf.currentTerm

	log := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.log.append(log)
	rf.persist()
	DPrintf("[%v]: term %v Start %v", rf.me, term, log)
	rf.appendEntries(false)

	return index, term, true
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.appendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
		rf.mu.Unlock()
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = 50 * time.Millisecond
	rf.resetElectionTimer()

	rf.log = makeEmptyLog()
	rf.log.append(Entry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// all server rule 1
		if rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			//DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}

func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log.at(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}
