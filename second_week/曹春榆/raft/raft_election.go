package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int  //候选人任期号
	CandidateId  int  // 请求投票的候选人ID
	LastLogIndex int  // 候选人的最后日志条目的索引
	LastLogTerm  int  // 候选人最后日志条目的任期号
	IsTryElect   bool // 是否是预选举
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	sleepTime := rf.electTimeout
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//DPrintln(rf.me, "睡眠时长：", sleepTime)
		time.Sleep(time.Duration(sleepTime+2) * time.Millisecond)
		if rf.isLeader {
			continue
		}
		interval := int(time.Now().UnixMilli() - rf.lastHeartBeatTime)
		if interval > rf.electTimeout {
			rf.mu.Lock()
			rf.resetVotedFor()
			rf.isStartElection = true
			rf.setElectTimeout()
			if !rf.canElection {
				rf.tryElection()
			} else {
				rf.electFailNum++
				DPrintln("elect fail num", rf.electFailNum, "lastHeartBeatTime", rf.lastHeartBeatTime, "now", time.Now().UnixMilli())
				if rf.electFailNum%3 != 0 {
					rf.election()
				}
			}
			rf.mu.Unlock()
			//if rf.canElection || rf.tryElection() {
			//	DPrintln("leader", rf.me, "在任期", rf.currentTerm, "尝试选举成功", "canElection", rf.canElection)
			//	rf.canElection = true
			//
			//	if rf.election() {
			//		rf.canElection = false
			//		go rf.appendToFollower()
			//	}
			//}
			sleepTime = rf.electTimeout
		} else {
			sleepTime = rf.electTimeout - interval
		}

	}
}

// 预选举
func (rf *Raft) tryElection() {
	if rf.killed() {
		return
	}
	//isSuccess := rf.getNeedSupport(int(len(rf.peers)/2)+1, true)
	needNum := int(len(rf.peers)/2) + 1
	rf.getNeedSupport(needNum, true)
	return
}

// 正式选举
func (rf *Raft) election() {
	if rf.killed() {
		return
	}
	rf.setElectTimeout()
	// 如果投给自己失败保持follower状态
	if !rf.tryVoteForSelf() {
		return
	}
	//DPrintln(rf.me, "进入了候选人状态")
	rf.currentTerm++
	rf.persistMutex.Lock()
	rf.persist()
	rf.persistMutex.Unlock()
	rf.isLeader = false
	rf.getNeedSupport(int(len(rf.peers)/2)+1, false)
	return
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
		LastLogIndex: rf.logs.GetLastLogIndex(),
		LastLogTerm:  rf.logs.GetLastLogTerm(),
		IsTryElect:   isTryElect,
	}
	rf.supportNum = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, currentTerm int, needNum int) {
			voteReply := RequestVoteReply{}
			startTime := time.Now().UnixMilli()
			ok := rf.sendRequestVote(i, &voteArgs, &voteReply)
			DPrintln(rf.me, "收到", i, "投票时间：", time.Now().UnixMilli()-startTime, time.Now().UnixMilli(), "是否支持",
				voteReply.VoteGranted, "ok", ok, "isTry", isTryElect)
			DPrintln("old term", currentTerm, "new term", rf.currentTerm, "need:", needNum)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				newTerm := rf.currentTerm
				if isTryElect {
					newTerm++
				}
				if currentTerm < newTerm || rf.isLeader || (isTryElect && rf.canElection) {
					return
				}

				if voteReply.VoteGranted == true && newTerm == currentTerm && !rf.isLeader {
					rf.supportNum++
					if rf.supportNum >= needNum {
						if isTryElect {
							rf.canElection = true
							rf.supportNum = 1
							rf.unsupportNum = 0
							DPrintln("\n\n机器", rf.me, "成为了任期", currentTerm, "候选人")
							return
						} else {
							DPrintln("\n\n机器", rf.me, "成为了任期", rf.currentTerm, "的leader")
							rf.initLeaderInfo()
							go rf.appendToFollower()
							return
						}
					}
					return
				}

				if voteReply.Term > voteArgs.Term {
					if newTerm < voteReply.Term {
						rf.currentTerm = voteReply.Term
					}
					rf.initFollowerInfo()
					return

				}

			}
		}(i, currentTerm, needNum)
	}
	return false

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

func (rf *Raft) initLeaderInfo() {
	rf.electFailNum = 1
	rf.supportNum = 1
	rf.unsupportNum = 0
	rf.isLeader = true
	rf.canElection = false
	rf.leaderId = rf.me
	rf.isStartElection = false
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.commitId + 1
		rf.lastReceiveTime[i] = time.Now().UnixMilli()
	}
}

func (rf *Raft) initFollowerInfo() {
	rf.isLeader = false
	rf.votedFor = -1
	rf.persist()
	rf.supportNum = 1
	rf.unsupportNum = 0
	rf.canElection = false
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isLeader {
		DPrintln("leader", rf.me, "在任期", rf.currentTerm, "收到", args.CandidateId, "在任期", args.Term, "的投票请求")
	}
	reply.Term = rf.currentTerm
	if !args.IsTryElect && args.Term < rf.currentTerm {
		DPrintln(rf.me, "因为args.Term < rf.currentTerm", args.Term, rf.currentTerm, "所以不投票给", args.CandidateId, "isTry", args.IsTryElect)
		reply.VoteGranted = false
		return
	}

	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	if rf.isLeader && args.Term >= rf.currentTerm {
		rf.isLeader = false
	}
	if !args.IsTryElect && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		return
	}
	if !args.IsTryElect && rf.votedFor != -1 {
		DPrintln(rf.me, "投票给了", rf.votedFor, "所以不投票给", args.CandidateId, "isTry", args.IsTryElect)
		reply.VoteGranted = false
		return
	}

	//if !rf.isStartElection {
	//	DPrintln(rf.me, "没开始选举所以不投票给", args.CandidateId)
	//	reply.VoteGranted = false
	//	return
	//}

	// 如果请求的log较新，则投票给它
	DPrintln(args.LastLogTerm >= rf.logs.GetLastLogTerm(), args.LastLogIndex >= rf.logs.GetLastLogIndex(), "进行投票")
	if args.LastLogTerm > rf.logs.GetLastLogTerm() ||
		(args.LastLogTerm == rf.logs.GetLastLogTerm() && args.LastLogIndex >= rf.logs.GetLastLogIndex()) {
		//rf.lastHeartBeatTime = time.Now().UnixMilli()
		DPrintln(rf.me, "因为args raft term, index", args.LastLogTerm, rf.logs.GetLastLogTerm(), args.LastLogIndex,
			rf.logs.GetLastLogIndex(), "投票给", args.CandidateId, "isTry", args.IsTryElect)
		if !args.IsTryElect {
			rf.canElection = false
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.persistMutex.Lock()
			rf.persist()
			rf.persistMutex.Unlock()
			DPrintln(rf.me, "在任期", rf.currentTerm, "支持", args.CandidateId, "isTry", args.IsTryElect)
		}
		//rf.lastHeartBeatTime = time.Now().UnixMilli()
		reply.VoteGranted = true
		return
	}
	DPrintln(rf.me, "因为args raft term, index", args.LastLogTerm, rf.logs.GetLastLogTerm(), args.LastLogIndex,
		rf.logs.GetLastLogIndex(), "所以不投票给", args.CandidateId, "isTry", args.IsTryElect)
	reply.VoteGranted = false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
