package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(v ...any) {
	if Debug {
		log.Println(v...)
	}
}

func (rf *Raft) checkLeader(leaderTerm int) bool {
	return leaderTerm >= rf.currentTerm
}
