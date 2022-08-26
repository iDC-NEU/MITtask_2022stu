package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *KVServer) InstallSnapshot(snapshot []byte) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	DPrintln(kv.me, "enter InstallSnapshot")
	if snapshot == nil || len(snapshot) <= 0 {
		DPrintln(kv.me, "leave InstallSnapshot")
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvStore)
	d.Decode(&kv.clientLastCommand)
	DPrintln(kv.me, "leave InstallSnapshot")
}

func (kv *KVServer) needToSnapshot() bool {
	DPrintln("机器", kv.me, "GetStateNum", kv.rf.GetStateNum(), "RaftStateSize", "max", kv.maxraftstate, "*0.8", float32(kv.maxraftstate)*0.7)
	return float32(kv.rf.GetStateNum()) > float32(kv.maxraftstate)*0.7
}

func (kv *KVServer) snapshot(lastIncludeIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintln(kv.me, "enter snapshot")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientLastCommand)
	snapshot := w.Bytes()
	kv.rf.Snapshot(lastIncludeIndex, snapshot)
	DPrintln(kv.me, "leave snapshot")
}

func (kv *KVServer) ApplySnapshot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintln(kv.me, "enter ApplySnapshot")
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.InstallSnapshot(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
	DPrintln(kv.me, "leave ApplySnapshot")
}

func (kv *KVServer) TakeSnapshot() {
	for message := range kv.snapshotCh {
		if message.CommandValid {
			if kv.needToSnapshot() {
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
