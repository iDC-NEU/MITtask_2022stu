package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex // 用于并发加锁
	clientId  int64      // client的编号
	requestId int64      // request的id
}

func (ck *Clerk) init() {
	ck.clientId = nrand()
	ck.requestId = 0
}

func (ck *Clerk) GetAndIncreaseReq() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	requestId := ck.requestId
	ck.requestId++
	return requestId
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.init()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = ck.GetAndIncreaseReq()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			si := 0
			for {
				//for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintln("Get的回复", "请求的group", gid, "server", si, "shard", shard,
					reply, "使用的config", ck.config, "是否ok", ok)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintln("得到Get的返回值", key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok && si == len(servers)-1 {
					break
				}
				if ok && reply.Err == ErrWrongLeader || !ok {
					si = (si + 1) % len(servers)
				}

			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	args.ClientId = ck.clientId
	args.RequestId = ck.GetAndIncreaseReq()

	for {
		// TODO: 更改发送方式，发到能成功为止
		// TODO: 加锁以完成并发
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfNum = ck.config.Num

		if servers, ok := ck.config.Groups[gid]; ok {
			si := 0
			for {
				//for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}

				if ok {
					DPrintln("key ", args.Key, "发送给", servers[si], "结果", reply)
				}

				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader

				if !ok && si == len(servers)-1 {
					break
				}
				if ok && reply.Err == ErrWrongLeader || !ok {
					si = (si + 1) % len(servers)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
