package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// raft node state
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

//time setting
const (
	ElectionTimeoutBase = time.Millisecond * 350
	SendHeartBeatCycle  = time.Millisecond * 120
)

// ApplyMsg
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

// Entry log entry
type Entry struct {
	Term          int
	Command       interface{}
	SnapshotIndex int
}

// Raft
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

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile state on all servers
	myStatus       int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	timerLock      sync.Mutex
	commitIndex    int
	lastApplied    int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	ApplyMsgChan chan ApplyMsg

	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
}

func (rf *Raft) drainFromChan() {
	select {
	case <-rf.electionTimer.C: //try to drain from the channel
	default:
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.timerLock.Lock()
	defer rf.timerLock.Unlock()
	if !rf.electionTimer.Stop() {
		rf.drainFromChan()
	}
	//rf.mu.Lock()
	rf.electionTimer.Reset(generateRandTime())
	//rf.mu.Unlock()
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	// Your code here (2A).
	term = rf.currentTerm
	if rf.myStatus == LEADER {
		isleader = true
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
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	err := enc.Encode(rf.log)
	if err != nil {
		return
	}
	err = enc.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = enc.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	rf.persister.SaveRaftState(buffer.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	read := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(read)
	var votedFor int
	var log []Entry
	var currentTerm int
	if decoder.Decode(&log) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&currentTerm) != nil {
		return
	} else {
		rf.votedFor = votedFor
		rf.log = log
		rf.currentTerm = currentTerm
	}
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	err := enc.Encode(rf.log)
	if err != nil {
		return
	}
	err = enc.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = enc.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	rf.persister.SaveStateAndSnapshot(buffer.Bytes(), snapshot)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) appendLogTrue(lastIncludedTerm int, lastIncludedIndex int) bool {
	return lastIncludedIndex <= rf.log[len(rf.log)-1].SnapshotIndex && rf.log[lastIncludedIndex-rf.log[0].SnapshotIndex].Term == lastIncludedTerm
}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node%v} receives a outdated snapshot, refuse", rf.me)
		return false
	}
	if rf.appendLogTrue(lastIncludedTerm, lastIncludedIndex) {
		rf.log = append([]Entry(nil), rf.log[lastIncludedIndex-rf.log[0].SnapshotIndex:]...)
	} else {
		rf.log = make([]Entry, 1)
	}

	rf.log[0].SnapshotIndex = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Command = nil
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persistStateAndSnapshot(snapshot)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("start create snapshot")
	if index <= rf.log[0].SnapshotIndex {
		DPrintf("{Node %v} refuse to create snapshot, outdated", rf.me)
		return
	}
	rf.log = append([]Entry(nil), rf.log[index-rf.log[0].SnapshotIndex:]...)
	rf.log[0].Command = nil
	rf.persistStateAndSnapshot(snapshot)
}

// gen rand time from 150-300
func generateRandTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomTime := time.Duration(rand.Int63n(150))
	return ElectionTimeoutBase + randomTime*time.Millisecond
}

//vote

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) grantedVotesMoreThanHalf() { //change to leader
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.log[len(rf.log)-1].SnapshotIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].SnapshotIndex
	rf.myStatus = LEADER

	rf.heartbeatTimer.Reset(SendHeartBeatCycle)
	rf.broadcastHeartBeat(true)
}
func (rf *Raft) startElection(request *RequestVoteArgs, currentTerm int) {
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			DPrintf("{Node%v} send requestvote to {Node%v} in term%v", rf.me, peer, currentTerm)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if request.Term != rf.currentTerm || currentTerm < rf.currentTerm || rf.myStatus != CANDIDATE {
					return
				}

				if response.Term > rf.currentTerm { //fail
					rf.myStatus = FOLLOWER
					rf.currentTerm = response.Term
					rf.persist()
					return
				}
				// if rf.currentTerm == request.Term && rf.myStatus == CANDIDATE {
				if response.VoteGranted {
					DPrintf("{Node%v} receive granted from {Node%v}", rf.me, peer)
					grantedVotes += 1
					if grantedVotes > len(rf.peers)/2 {
						DPrintf("{Node %v} receives majority votes in Term %v, receives %v vote", rf.me, rf.currentTerm, grantedVotes)
						fmt.Println("leader:")
						fmt.Println(rf.me)
						rf.grantedVotesMoreThanHalf() //change to leader
						return
					}
				}
			}
		}(peer)
	}
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// candidate Term out of date reject
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		// A new leader has been selected, the request term is expired, and a new term is returned
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.myStatus = FOLLOWER
		rf.votedFor, rf.currentTerm = -1, args.Term
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return
	}

	lastIndex := rf.log[len(rf.log)-1].SnapshotIndex
	lastTerm := rf.log[len(rf.log)-1].Term
	if !(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// granted
	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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

// genVoteArgs
// generate RequestVoteArgs
func (rf *Raft) genVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
		LastLogIndex: rf.log[len(rf.log)-1].SnapshotIndex,
	}
}

type IntHeap []int

func (h IntHeap) Len() int {
	return len(h)
}
func (h IntHeap) Less(i, j int) bool {
	return h[i] < h[j]
}
func (h IntHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (rf *Raft) ifPrevLogEntryExist(prevLogTerm int, prevLogIndex int) bool {
	if len(rf.log) < prevLogIndex {
		return false
	}
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true
	} else {
		return false
	}
}

func min(x1 int, x2 int) int {
	if x1 > x2 {
		return x2
	} else {
		return x1
	}
}

func max(x1 int, x2 int) int {
	if x1 > x2 {
		return x1
	} else {
		return x2
	}
}

func findKthLargest(nums []int, k int) int {
	h := &IntHeap{}
	heap.Init(h)
	for _, item := range nums {
		heap.Push(h, item)
		if h.Len() > k {
			heap.Pop(h)
		}
	}
	return (*h)[0]
}

//appendEntries

// AppendEntriesArgs
// For AppendEntriesRPC
type AppendEntriesArgs struct {
	Term         int // leader's Term
	LeaderId     int // in peers[]
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry // log entries to store
	LeaderCommit int     // leader commits index
}

// AppendEntriesReply
// For AppendEntriesRPC
type AppendEntriesReply struct {
	Term          int  // current Term
	Success       bool //
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term          int
	LeaderId      int
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) broadcastHeartBeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if rf.myStatus != LEADER {
			return
		}
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		if isHeartBeat {
			go rf.replicate(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) leaderFailed(reply *AppendEntriesReply) {
	rf.myStatus = FOLLOWER
	rf.currentTerm = reply.Term
	rf.persist()
}
func (rf *Raft) handleReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.myStatus != LEADER {
		return
	}

	if args.Term != rf.currentTerm {
		DPrintf("outdated reply")
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("leader failed")
		rf.leaderFailed(reply)
		return
	}

	if reply.Success {
		if len(args.Entries) == 0 {
			return
		}
		// only update when its not a outdated op
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

		// this log has already been commited, return directly

		majorityMatchIndex := findKthLargest(rf.matchIndex, len(rf.peers)/2+1)
		if majorityMatchIndex > rf.commitIndex && rf.log[majorityMatchIndex-rf.log[0].SnapshotIndex].Term == rf.currentTerm {
			rf.commitIndex = majorityMatchIndex
			DPrintf("leader matchIndex: %v\n", rf.matchIndex)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}

		// rf.commitIndex = preCommitIndex
		// if rf.commitIndex > rf.lastApplied {
		// 	rf.applyCond.Signal()
		// }
	} else {
		if reply.ConflictIndex == -1 {
			return
		}
		if reply.ConflictTerm != -1 {
			// term conflict
			rf.termConflict(server, args, reply)
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
}
func (rf *Raft) termConflict(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	conflictIndex := -1
	for i := args.PrevLogIndex - rf.log[0].SnapshotIndex; i > 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			conflictIndex = i + rf.log[0].SnapshotIndex
			break
		}
	}
	if conflictIndex == -1 {
		rf.nextIndex[server] = reply.ConflictIndex
	} else {
		rf.nextIndex[server] = conflictIndex + 1
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("Node{%v} receive heartbeat in term %v from {Node%v}", rf.me, rf.currentTerm, args.LeaderId)
	reply.ConflictIndex, reply.ConflictTerm = -1, -1

	if args.Term < rf.currentTerm {
		DPrintf("{Node%v} receives a outdated AppendEntriesRPC", rf.me)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	base := rf.log[0].SnapshotIndex
	rf.myStatus = FOLLOWER

	// This request has expired. During this period, a snapshot has been installed. It is just a normal heartbeat package. Reply directly
	if args.PrevLogIndex < base {
		reply.Term, reply.Success = rf.currentTerm, false
		// reply.ConflictIndex, reply.ConflictTerm = base+1, -1
		DPrintf("info in snapshot, conflictindex=%v", base+1)
		return
	}

	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()

	if len(rf.log)-1+base < args.PrevLogIndex {
		// log doesn't include prevlogindex
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = len(rf.log)+base, -1
		return
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex-base].Term {
		// Contains prevlogindex but the terms are inconsistent and do not match s
		rf.containsButTermsInconsistent(args, reply, base)
		return
	}

	for index, entry := range args.Entries {
		if entry.SnapshotIndex-base >= len(rf.log) || rf.log[entry.SnapshotIndex-base].Term != entry.Term {
			DPrintf("{Node%v} start append entries, len of past log is %v", rf.me, rf.log[len(rf.log)-1].SnapshotIndex)
			rf.log = append(rf.log[:entry.SnapshotIndex-base], args.Entries[index:]...)
			DPrintf("{Node%v} end append entries, len of now is %v", rf.me, rf.log[len(rf.log)-1].SnapshotIndex)
			break
		}
	}
	rf.commitIndexLessThanLeaderCommit(args)

	reply.Term, reply.Success = rf.currentTerm, true
}
func (rf *Raft) containsButTermsInconsistent(args *AppendEntriesArgs, reply *AppendEntriesReply, base int) {
	reply.Term, reply.Success = rf.currentTerm, false
	reply.ConflictTerm = rf.log[args.PrevLogIndex-base].Term
	for i := 0; i <= args.PrevLogIndex-base; i++ {
		if rf.log[i].Term == reply.ConflictTerm {
			reply.ConflictIndex = i + base
			// rf.log = append([]Entry(nil), rf.log[:i]...) confilct trim log
			break
		}
	}
}
func (rf *Raft) commitIndexLessThanLeaderCommit(args *AppendEntriesArgs) {
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].SnapshotIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}
func (rf *Raft) sendSnapshot(peer int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)
	if ok && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm || rf.myStatus != LEADER {
			DPrintf("{Node %v} got a outdate snapshotRPC reply from {Node %v}, discard", rf.me, peer)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.myStatus = FOLLOWER
			// rf.votedFor, rf.currentTerm = -1, reply.Term
			rf.currentTerm = reply.Term
			rf.persist()
			// rf.electionTimer.Reset(generateRandTime())
			// rf.resetElectionTimer()
			return
		}
		if reply.Term == rf.currentTerm && reply.Term != 0 {
			rf.nextIndex[peer] = args.SnapshotIndex + 1
			// rf.matchIndex[peer] = args.SnapshotIndex
		}
	}
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// rf.currentTerm, rf.votedFor = args.Term, -1
		rf.currentTerm = args.Term
		rf.persist()
		rf.myStatus = FOLLOWER
	}

	if args.SnapshotIndex < rf.log[0].SnapshotIndex {
		reply.Term = rf.currentTerm
		return
	}

	rf.myStatus = FOLLOWER
	// rf.electionTimer.Reset(generateRandTime())
	rf.resetElectionTimer()

	go func() {
		rf.ApplyMsgChan <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.SnapshotTerm,
			SnapshotIndex: args.SnapshotIndex,
		}
	}()
	reply.Term = rf.currentTerm
	return
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	term := rf.currentTerm
	isLeader := true
	// Your code here (2B).
	isLeader = rf.myStatus == LEADER
	if !isLeader {
		return index, rf.currentTerm, isLeader
	}
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, command, rf.currentTerm)
	index = len(rf.log) + rf.log[0].SnapshotIndex
	rf.log = append(rf.log, Entry{Command: command, Term: term, SnapshotIndex: index})
	rf.persist()
	rf.matchIndex[rf.me] = index
	// rf.nextIndex[rf.me] = index
	rf.broadcastHeartBeat(false)
	rf.heartbeatTimer.Reset(SendHeartBeatCycle)
	return index, term, isLeader
}

// Kill
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
	// DPrintf("{Node%v} has been killed", rf.me)
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

		//if election time out then change to candidate
		//if heartbeat time out and is leader then broadcast heartbeat

		select {
		case <-rf.electionTimer.C:
			go func() {
				DPrintf("{Node%v}'s timer finish", rf.me)
				rf.mu.Lock()
				if rf.myStatus == LEADER { //check if is leader
					rf.myStatus = FOLLOWER
					rf.mu.Unlock()
					//return
				} else {
					rf.currentTerm++
					rf.myStatus = CANDIDATE

					//fmt.Printf("elect of process %d, term is %d\n", rf.me, rf.currentTerm)
					currentTerm := rf.currentTerm
					args := rf.genVoteArgs()
					rf.votedFor = rf.me //vote for itself
					rf.persist()
					rf.mu.Unlock()
					rf.startElection(args, currentTerm)
				}

			}()
			rf.electionTimer.Reset(generateRandTime())
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.myStatus == LEADER {
				// DPrintf("{Node%v} send heartbeat, nextIndex=%v\n matchIndex=%v\n", rf.me, rf.nextIndex, rf.matchIndex)
				rf.broadcastHeartBeat(true)
				rf.heartbeatTimer.Reset(SendHeartBeatCycle)
			}
			rf.mu.Unlock()
		}
	}
	// DPrintf("raft server%v has been killed", rf.me)
}

// applier
// use condvar to sync applymsg
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
		base, lastApplied, commitIndex := rf.log[0].SnapshotIndex, rf.lastApplied, rf.commitIndex
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-base:commitIndex+1-base])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.ApplyMsgChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.SnapshotIndex,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicate(peer int) {
	rf.mu.Lock()
	if rf.myStatus != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	PrevLogIndex := rf.nextIndex[peer] - 1
	if PrevLogIndex < rf.log[0].SnapshotIndex {
		requests := InstallSnapshotArgs{
			Term:          rf.currentTerm,
			LeaderId:      rf.me,
			SnapshotIndex: rf.log[0].SnapshotIndex,
			SnapshotTerm:  rf.log[0].Term,
			Snapshot:      rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		DPrintf("{Node%v} send snapshot%v to{Node%v}", rf.me, requests, peer)
		rf.sendSnapshot(peer, &requests)
	} else {
		request := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: PrevLogIndex,
			LeaderCommit: rf.commitIndex,
		}
		if PrevLogIndex > 0 {
			request.PrevLogTerm = rf.log[request.PrevLogIndex-rf.log[0].SnapshotIndex].Term
		}
		request.Entries = make([]Entry, len(rf.log[rf.nextIndex[peer]-rf.log[0].SnapshotIndex:]))
		copy(request.Entries, rf.log[rf.nextIndex[peer]-rf.log[0].SnapshotIndex:])
		if len(request.Entries) != 0 {
			DPrintf("leader {Node%v} send log%v-%v to {Node%v}", rf.me, request.Entries[0].SnapshotIndex, request.Entries[len(request.Entries)-1].SnapshotIndex, peer)
		}
		rf.mu.Unlock()
		response := AppendEntriesReply{}
		if rf.sendAppendEntries(peer, &request, &response) {
			rf.handleReply(peer, &request, &response)
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// only wake up when need
		for !rf.need_replicate(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicate(peer)
	}
}
func (rf *Raft) need_replicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.myStatus == LEADER && rf.nextIndex[peer] <= rf.log[len(rf.log)-1].SnapshotIndex
}

// Make
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		myStatus:       FOLLOWER,
		votedFor:       -1,
		currentTerm:    0,
		electionTimer:  time.NewTimer(generateRandTime()), //set rand time
		heartbeatTimer: time.NewTimer(SendHeartBeatCycle), //set heartbeat time
		commitIndex:    0,
		lastApplied:    0,
		log:            make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		ApplyMsgChan:   applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log[0].SnapshotIndex + 1
	}
	rf.lastApplied = rf.log[0].SnapshotIndex
	rf.commitIndex = rf.log[0].SnapshotIndex
	// start ticker goroutine to start elections
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}
	go rf.ticker()
	go rf.applier()

	return rf
}
