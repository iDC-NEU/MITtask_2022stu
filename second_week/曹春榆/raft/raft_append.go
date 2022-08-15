package raft

import (
	"6.824/labgob"
	"bytes"
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader的id
	PrevLogIndex int        // 紧邻新日志条目的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目的那个日志条目的任期
	Entries      []LogEntry // 日志条目
	LeaderCommit int        // leader的已知已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	NeedTerm  int  // 需要发送的任期
	NeedIndex int  // 需要发送的索引
	Term      int  // 当前任期
	Success   bool // 是否成功
}

// 返回新log的index以及term
func (rf *Raft) recordCommand(command interface{}) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || !rf.isLeader {
		return -1, -1
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	DPrintln("leader", rf.me, "添加了", logEntry)
	rf.logMutex.Lock()
	logIndex := rf.logs.Append(logEntry)
	rf.persist()
	rf.logMutex.Unlock()
	go func() {
		rf.newCommandCh <- logEntry
	}()
	//go rf.appendToFollower()
	return logIndex, rf.currentTerm
}

func (rf *Raft) appendToFollower() {
	for !rf.killed() && rf.isLeader {
		sleepTime := HeartBeatTime
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(followerId int) {
				rf.mu.Lock()
				if !rf.isLeader {
					rf.mu.Unlock()
					return
				}
				nextIndex := rf.nextIndex[followerId]
				if nextIndex <= rf.logs.lastIncludeIndex {
					rf.mu.Unlock()
					rf.sendSnapshot(followerId)
					return
				}
				prevLogIndex := nextIndex - 1
				var prevLogTerm int
				if prevLogIndex >= rf.logs.Size() {
					prevLogTerm = rf.currentTerm
				} else {
					prevLogTerm = rf.logs.GetPrevLogTerm(nextIndex)
				}
				entries := rf.logs.GetLogsFromStart(nextIndex)
				DPrintln("leader", rf.me, "log为", rf.logs.log, "nextINdex", nextIndex, "commitId", rf.commitId)
				DPrintln("leader", rf.me, "任期", rf.currentTerm, "向", followerId, "发送", entries)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitId,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				//startTime := time.Now().UnixMilli()
				ok := rf.sendAppendEntries(followerId, &args, &reply)
				DPrintln("leader", rf.me, "收到回复", reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.lastReceiveTime[followerId] = time.Now().UnixMilli()
					if !rf.isLeader {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.initFollowerInfo()
						return
					}

					if reply.Success && len(entries) != 0 {
						rf.matchIndex[followerId] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[followerId] = rf.matchIndex[followerId] + 1
						DPrintln("机器", followerId, "调整了matchIndex，新的为", rf.matchIndex[followerId])
						oldCommitId := rf.commitId
						rf.updateCommitId()
						if rf.commitId != oldCommitId {
							sleepTime = 0
						}
					} else if !reply.Success {
						rf.adjustSendIndex(followerId, reply.NeedTerm, reply.NeedIndex)
						DPrintln("leader", rf.me, "修改了机器", followerId, "的next为", rf.nextIndex[followerId])
					}
				} else {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					unConnectNum := 0
					nowTime := time.Now().UnixMilli()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						if nowTime-rf.lastReceiveTime[i] > MaxTimeout*2 {
							unConnectNum++
						}
					}
					if unConnectNum >= (len(rf.peers)/2 + 1) {
						rf.initFollowerInfo()
						return
					}
				}

			}(i)
		}
		select {
		case <-rf.newCommandCh:
		case <-time.After(time.Duration(sleepTime) * time.Millisecond):
		}
		//time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

}

func (rf *Raft) adjustSendIndex(followerId int, needTerm int, needIndex int) {
	if needTerm == -1 || needIndex == -1 || needTerm == 0 {
		rf.nextIndex[followerId] = 0
	}
	if needIndex >= rf.logs.Size() {
		rf.nextIndex[followerId] = rf.logs.Size()
		return
	}
	index := rf.nextIndex[followerId]
	if rf.logs.GetLogTerm(needIndex) <= needTerm {
		rf.nextIndex[followerId] = needIndex
	} else if rf.logs.GetLogTerm(index) <= needTerm {
		rf.nextIndex[followerId] = index
	} else {
		isFind := false
		for i := index; i != -1; i-- {
			if rf.logs.GetLogTerm(index) <= needTerm {
				isFind = true
				rf.nextIndex[followerId] = index
				break
			}
		}
		// 如果找到最后一个都没找到，则赋为0
		if !isFind {
			rf.nextIndex[followerId] = 0
		}
	}
	index = rf.nextIndex[followerId]
	for rf.logs.GetLog(index).Term == needTerm && index > needIndex {
		index--
	}
	rf.nextIndex[followerId] = index
	if rf.nextIndex[followerId] < 0 {
		rf.nextIndex[followerId] = 0
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendLogEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitId() {
	if !rf.isLeader {
		return
	}
	majority := int(len(rf.peers)/2) + 1
	logLen := rf.logs.Size()
	for i := logLen - 1; i > rf.commitId; i-- {
		commitNum := 1
		for m := 0; m < len(rf.peers); m++ {
			if m == rf.me {
				continue
			}
			if rf.matchIndex[m] >= i {
				commitNum++
				if commitNum >= majority {
					if rf.logs.GetLog(i).Term == rf.currentTerm {
						rf.commitId = i
						DPrintln("修改了commitID，新的为", rf.commitId)
						rf.persistMutex.Lock()
						rf.persist()
						rf.persistMutex.Unlock()

					}
					return
				}
			}
		}
	}
}

func (rf *Raft) AppendLogEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if !rf.checkLeader(args.Term) {
		reply.Success = false
		DPrintln("不是leader返回")
		return
	}
	DPrintln("机器", rf.me, "的commitId为", rf.commitId, "lastInclude", rf.logs.lastIncludeTerm, rf.logs.lastIncludeIndex,
		"snap数量为", rf.logs.snapNum, "log长度为", rf.logs.Size(), "内容为", rf.logs.log)
	DPrintln("机器", rf.me, "在任期", rf.currentTerm, "收到leader", args.LeaderId, "发送的", args.Entries)
	//DPrintln("心跳lastHeartBeatTime", rf.lastHeartBeatTime)
	rf.electFailNum = 1
	rf.isStartElection = false
	rf.supportNum = 1
	rf.isLeader = false
	rf.canElection = false
	rf.leaderId = args.LeaderId
	rf.lastHeartBeatTime = time.Now().UnixMilli()
	if rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
	}

	DPrintln("机器", rf.me, "收到prevlogindex", args.PrevLogIndex, "prevTeam", args.PrevLogTerm, "realTeam", rf.logs.GetLogTerm(args.PrevLogIndex))
	if args.PrevLogIndex < rf.logs.lastIncludeIndex {
		reply.NeedIndex = rf.commitId + 1
		reply.NeedTerm = rf.logs.GetLogTerm(rf.commitId + 1)
		if reply.NeedTerm == -1 {
			reply.NeedTerm = args.Term
		}
		reply.Success = false
		return
	}
	// 匹配不通过的情况, 等于-1即发送的是全部
	if args.PrevLogIndex != -1 && (args.PrevLogIndex > rf.logs.Size()-1 ||
		rf.logs.GetLogTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		reply.Success = false
		reply.NeedTerm, reply.NeedIndex = rf.logs.GetNoConflictInfo(args.PrevLogTerm, args.PrevLogIndex)
		return
	}
	// 不带确认的心跳包的处理
	//if args.Entries == nil || len(args.Entries) == 0 {
	//	reply.Success = true
	//	return
	//}
	// 成功的情况
	// 写入新条目
	if args.Entries != nil && len(args.Entries) != 0 {

		startIndex := args.PrevLogIndex - rf.logs.snapNum + 1
		if startIndex < 0 {
			DPrintln("机器", rf.me, "超出边界", startIndex, "log中数量", rf.logs.snapNum, "prevIndex", args.PrevLogIndex)
		}
		rf.logs.WriteLogs(args.PrevLogIndex, args.Entries)
		//rf.logs = append(rf.logs[:args.PrevLogIndex-rf.snapNums+1], args.Entries...)
		DPrintln("机器", rf.me, "添加后的log为", rf.logs.log)

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.logs.log)
		DPrintln("机器", rf.me, "log长度为", len(w.Bytes()), "lastApplied", rf.lastApplied, "commit", rf.commitId)

		rf.persistMutex.Lock()
		rf.persist()
		rf.persistMutex.Unlock()
		rf.isFarBehind = false
	}
	// 判断commit
	if rf.logs.Size()-1 < args.LeaderCommit {
		rf.commitId = rf.logs.Size() - 1
	} else {
		rf.commitId = args.LeaderCommit
	}
	reply.Success = true
}

func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		var appliedMsgs = make([]ApplyMsg, 0)

		//func() {
		DPrintln(rf.me, "rf.commitId, rf.lastApplied", rf.commitId, rf.lastApplied)
		for rf.commitId > rf.lastApplied && rf.lastApplied < rf.logs.Size() {
			rf.lastApplied += 1
			command := rf.logs.GetLog(rf.lastApplied).Command
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: rf.lastApplied,
			})
		}
		//}()
		// 锁外提交给应用层
		for _, msg := range appliedMsgs {

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(msg)
			DPrintln(rf.me, "提交了消息, 长度为", len(w.Bytes()))
			rf.applyCh <- msg
			DPrintln(rf.me, "提交完成")
		}
	}
}
