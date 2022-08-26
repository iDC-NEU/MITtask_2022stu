package raft

import (
	"fmt"
	"time"
)

type InstallSnapshotArgs struct {
	Term             int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
	LeaderId         int
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendSnapshot(followerId int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LastIncludeIndex: rf.logs.lastIncludeIndex,
		LastIncludeTerm:  rf.logs.lastIncludeTerm,
		Snapshot:         rf.logs.snapshot,
		LeaderId:         rf.me,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[followerId].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.initFollowerInfo()
		rf.mu.Unlock()
		return
	}
	rf.nextIndex[followerId] = args.LastIncludeIndex + 1
	rf.matchIndex[followerId] = args.LastIncludeIndex
	rf.mu.Unlock()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	//if lastIncludedIndex <= rf.commitId {
	//	return false
	//}
	//
	//rf.logs.CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex, snapshot)
	//DPrintln("机器", rf.me, "安装了log，lastinclude", lastIncludedIndex)
	//rf.saveStateAndSnapshot(snapshot)
	//rf.lastApplied = lastIncludedIndex
	//rf.commitId = lastIncludedIndex
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitId || index <= rf.logs.lastIncludeIndex {
		return
	}
	DPrintln("机器", rf.me, "开始创建快照, index为", index, "log长为", rf.logs.Size())
	rf.logs.SetSnapShot(index, snapshot)
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.saveStateAndSnapshot(snapshot)
	DPrintln("机器", rf.me, "创建了快照, lastInclude", rf.logs.lastIncludeTerm, rf.logs.lastIncludeIndex)

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if !rf.checkLeader(args.Term) {
		reply.Success = false
		return
	}
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	if rf.isLeader {
		rf.leaderId = args.LeaderId
		rf.initFollowerInfo()
	}
	if rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
	}

	if args.LastIncludeIndex <= rf.logs.lastIncludeIndex {
		reply.Success = true
		return
	}

	if rf.IsPrint {
		fmt.Println("机器", rf.me, "GID", rf.GID, "安装了快照， lastinclude", args.LastIncludeTerm, args.LastIncludeIndex)
	}
	rf.logs.SetInstallSnapshot(args.LastIncludeIndex, args.LastIncludeTerm, args.Snapshot)

	if args.LastIncludeIndex > rf.commitId {
		rf.commitId = args.LastIncludeIndex
		rf.persist()
	}
	if args.LastIncludeIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludeIndex
	}

	rf.saveStateAndSnapshot(args.Snapshot)

	reply.Success = true

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludeTerm,
			SnapshotIndex: args.LastIncludeIndex,
		}
	}()
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}
