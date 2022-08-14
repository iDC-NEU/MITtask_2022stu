package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *KVServer) InstallSnapshot(snapshot []byte) {
	kv.mu.Lock()
	kv.mu.Unlock()
	if snapshot == nil || len(snapshot) <= 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvStore)
	d.Decode(&kv.clientLastCommand)
}

func (kv *KVServer) needToSnapshot() bool {
	return float32(kv.rf.GetStateNum()) > float32(kv.maxraftstate)*0.9
}

func (kv *KVServer) snapshot(lastIncludeIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientLastCommand)
	snapshot := w.Bytes()
	kv.rf.Snapshot(lastIncludeIndex, snapshot)
}

func (kv *KVServer) ApplySnapshot(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.InstallSnapshot(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
}
