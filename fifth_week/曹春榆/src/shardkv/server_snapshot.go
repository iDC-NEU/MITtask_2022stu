package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *ShardKV) TakeSnapshot() {
	for message := range kv.snapshotCh {
		if message.CommandValid {
			if kv.needToSnapshot() {
				kv.DPrintln("开始创建快照")
				kv.snapshot(message.CommandIndex)
			}
		} else if message.SnapshotValid {
			kv.ApplySnapshot(message)
		}
	}

	//for !kv.killed() && kv.maxraftstate != -1 {
	//	select {
	//	case message := <-kv.snapshotCh:
	//
	//	case <-time.After(time.Duration(raft.MinTimeout) * time.Millisecond):
	//		if kv.needToSnapshot() {
	//			kv.snapshot(kv.lastIncludeIndex)
	//		}
	//	}
	//}
}
func (kv *ShardKV) needToSnapshot() bool {
	//DPrintln("机器", kv.me, "GetStateNum", kv.rf.GetStateNum(), "RaftStateSize", "max", kv.maxraftstate, "*0.8", float32(kv.maxraftstate)*0.7)
	return float32(kv.rf.GetStateNum()) > float32(kv.maxraftstate)*0.7
}

func (kv *ShardKV) snapshot(lastIncludeIndex int) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	//DPrintln(kv.me, "enter snapshot")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.currentConfig)
	e.Encode(kv.shardLastReq)
	e.Encode(kv.shardSending)
	e.Encode(kv.shardReceiving)
	snapshot := w.Bytes()
	kv.DPrintln("创建了快照")
	DPrintln("机器", kv.me, "Gid", kv.gid, "创建了快照")
	kv.rf.Snapshot(lastIncludeIndex, snapshot)
	//DPrintln(kv.me, "leave snapshot")
}

func (kv *ShardKV) ApplySnapshot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.DPrintln("enter ApplySnapshot")
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.InstallSnapshot(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
	//DPrintln(kv.me, "leave ApplySnapshot")
}

func (kv *ShardKV) InstallSnapshot(snapshot []byte) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	kv.DPrintln("enter InstallSnapshot")
	if snapshot == nil || len(snapshot) <= 0 {
		kv.DPrintln(kv.me, "blank leave InstallSnapshot")
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvStore)
	d.Decode(&kv.currentConfig)
	d.Decode(&kv.shardLastReq)
	d.Decode(&kv.shardSending)
	d.Decode(&kv.shardReceiving)
	kv.DPrintln("leave InstallSnapshot")
}
