package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ConfigStale    = "ConfigStale"
	ConfigTooNew   = "ConfigTooNew"
	ErrLongWait    = "ErrLongWait"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ConfNum   int   // 请求时的configNum
	ClientId  int64 // 标识客户端
	RequestId int64 // 标识请求序列号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfNum   int   // 请求时的configNum
	ClientId  int64 // 标识客户端
	RequestId int64 // 标识请求序列号
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveArgs struct {
	ConfigNum    int
	KvShard      map[int]map[string]string
	ShardLastReq map[int]map[int64]int64
}

type MoveReply struct {
	Err Err
}

func DPrintln(v ...any) {
	if Debug {
		fmt.Println(v...)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(format, a...)
	}
}

func (kv *ShardKV) DPrintln(v ...any) {
	if Debug {
		fmt.Printf("机器 %d Gid %d ConfNum %d ", kv.me, kv.gid, kv.currentConfig.Num)
		fmt.Println(v...)
	}
}

func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf("机器 %d Gid %d ConfNum %d ", kv.me, kv.gid, kv.currentConfig.Num)
		fmt.Printf(format, a...)
	}
}
