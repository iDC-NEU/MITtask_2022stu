package raft

type LogEntry struct {
	Command interface{}
	Term    int
}

type LogStore struct {
	log              []LogEntry // 存储未持久化的log
	snapshot         []byte     // 用于存储快照
	snapNum          int        // snap中存储的log数量
	lastIncludeIndex int        // 快照中最后被包含的索引
	lastIncludeTerm  int        // 快照中最后被包含的任期
}

func (logs *LogStore) Init() {
	logs.log = make([]LogEntry, 0)
	logs.snapshot = make([]byte, 0)
	logs.snapNum = 0
	logs.lastIncludeIndex = -1
	logs.lastIncludeTerm = -1
}

// 追加日志并返回其索引
func (logs *LogStore) Append(entry LogEntry) int {
	logs.log = append(logs.log, entry)
	return len(logs.log) + logs.snapNum - 1
}

func (logs *LogStore) GetLog(index int) LogEntry {
	if index < logs.snapNum || index >= len(logs.log)+logs.snapNum {
		return LogEntry{Term: -1}
	}
	return logs.log[index-logs.snapNum]
}

func (logs *LogStore) GetLogTerm(index int) int {
	if index == logs.lastIncludeIndex {
		return logs.lastIncludeTerm
	}
	if index < logs.snapNum || index >= len(logs.log)+logs.snapNum {
		return -1
	}
	return logs.log[index-logs.snapNum].Term
}

func (logs *LogStore) GetLastLogTerm() int {
	if logs.log == nil || len(logs.log) == 0 {
		return logs.lastIncludeTerm
	}
	return logs.log[len(logs.log)-1].Term
}

func (logs *LogStore) GetLastLogIndex() int {
	if logs.log == nil || len(logs.log) == 0 {
		return logs.lastIncludeIndex
	}
	return len(logs.log) + logs.snapNum - 1
}

func (logs *LogStore) GetPrevLogTerm(index int) int {
	if index < logs.lastIncludeIndex || index <= 0 {
		return -1
	}
	if logs.log == nil || len(logs.log) == 0 || index == logs.snapNum {
		return logs.lastIncludeTerm
	}
	return logs.log[index-logs.snapNum-1].Term
}

func (logs *LogStore) GetArraySize() int {
	return len(logs.log)
}

func (logs *LogStore) GetSnapSize() int {
	return logs.snapNum
}

func (logs *LogStore) GetLogsFromStart(startIndex int) []LogEntry {
	if startIndex > logs.GetLastLogIndex() {
		return []LogEntry{}
	}
	entries := make([]LogEntry, 0)
	entries = append(entries, logs.log[startIndex-logs.snapNum:]...)
	return entries
}

func (logs *LogStore) Size() int {
	return len(logs.log) + logs.snapNum
}

func (logs *LogStore) WriteLogs(prevLogIndex int, entries []LogEntry) {
	startIndex := prevLogIndex - logs.snapNum + 1
	// 下面是检查是否存在后发的包比先发的先到的情况，如果有，则不对该日志进行截断写入
	if prevLogIndex+len(entries)+1 <= logs.Size() {
		isEqual := true
		j := startIndex
		// 全等的话就是出现了包顺序问题
		for i := 0; i < len(entries); i++ {
			if entries[i].Term != logs.log[j].Term {
				isEqual = false
				break
			}
			j++
		}
		if !isEqual {
			logs.log = append(logs.log[:startIndex], entries...)
		} else {
			DPrintln("出现了顺序问题")
		}
	} else {
		logs.log = append(logs.log[:startIndex], entries...)
	}
}

// 返回的第一个为无冲突的term，第二个为无冲突的index
func (logs *LogStore) GetNoConflictInfo(prevTerm int, prevIndex int) (int, int) {
	lastIndex := len(logs.log) - 1
	// TODO: 当前log为空的情况下
	if logs.Size() == 0 {
		return 0, 0
	}
	// TODO: prev在snapshot中的情况
	if prevIndex < logs.lastIncludeIndex {
		return 0, 0
	}
	// prev 比当前log长的情况
	if logs.Size()-1 < prevIndex {
		// 如果发送过来的prev是快照后的第一个，则发送快照的最后一个回去
		if logs.GetArraySize() == 0 {
			return logs.lastIncludeTerm, logs.lastIncludeIndex
		}
		// 如果log的最后一个小于它，则直接发送最后一个的term回去
		if logs.log[lastIndex].Term <= prevTerm {
			return logs.log[lastIndex].Term, lastIndex + logs.snapNum
		} else {
			// 其他情况下，遍历log数组，查找能比prev小的term
			for i := lastIndex; i != -1; i-- {
				if logs.log[i].Term <= prevTerm {
					return logs.log[lastIndex].Term, logs.snapNum + i
				}
			}
			// 如果没找到，则发送快照里面的最小的回去
			return logs.lastIncludeTerm, logs.lastIncludeIndex
		}
	}
	realIndex := prevIndex - logs.snapNum
	// 相应位置的term比原来的小的情况
	if logs.log[realIndex].Term < prevTerm {
		return logs.log[realIndex].Term, realIndex
	}
	// 相应位置的term比原来的大的情况, 反向遍历，找到小于等于它的
	for i := realIndex; i != -1; i-- {
		if logs.log[i].Term <= prevTerm {
			return logs.log[i].Term, realIndex
		}
	}
	// 如果没找到，则返回快照里面最小的
	return logs.lastIncludeTerm, logs.lastIncludeIndex
}

func (logs *LogStore) SetSnapShot(index int, snapshot []byte) {
	if index <= logs.lastIncludeIndex {
		return
	}
	temLog := make([]LogEntry, 0)
	if index > logs.Size() {
		logs.lastIncludeTerm = logs.GetLogTerm(logs.Size() - 1)
		logs.log = temLog
	} else {
		logs.lastIncludeTerm = logs.GetLogTerm(index)
		logs.log = append(temLog, logs.log[index-logs.snapNum+1:]...)
	}
	logs.lastIncludeIndex = index
	logs.snapNum = index + 1
	logs.snapshot = snapshot

}

func (logs *LogStore) SetInstallSnapshot(index int, lastIncludeTerm int, snapshot []byte) {

	if index <= logs.lastIncludeIndex {
		return
	}
	logs.lastIncludeIndex = index
	logs.lastIncludeTerm = lastIncludeTerm
	temLog := make([]LogEntry, 0)
	DPrintln("index", index, "snapNum", logs.snapNum, "len", logs.Size())
	if index >= logs.Size() {
		logs.log = temLog
	} else {
		logs.log = append(temLog, logs.log[index-logs.snapNum+1:]...)
	}
	logs.snapNum = index + 1
	logs.snapshot = snapshot
}

func (logs *LogStore) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) {
	if lastIncludedIndex < logs.Size() && logs.GetLog(lastIncludedIndex).Term == lastIncludedTerm {
		logs.log = append([]LogEntry(nil), logs.log[lastIncludedIndex-logs.snapNum+1:]...)
	} else {
		logs.log = []LogEntry{}
	}
	logs.lastIncludeIndex = lastIncludedIndex
	logs.lastIncludeTerm = lastIncludedTerm
	logs.snapNum = lastIncludedIndex + 1
	logs.snapshot = snapshot
}
