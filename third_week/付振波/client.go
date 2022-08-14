package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	// "log"
	// "fmt"
	"math/big"
	mrand "math/rand"//换个名，根20行的nrand区分
	// "sync"
)
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	RequServer int//当前请求的服务器
	ClientId int64
	RequestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.RequServer = mrand.Intn(len(ck.servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.RequestId++
	server := ck.RequServer
	args := GetArgs{Key: key, ClientId: ck.ClientId, RequestId: ck.RequestId}

	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			ck.RequServer = server
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.RequestId++
	server := ck.RequServer
	for {
		args := PutAppendArgs{Key: key, Value: value, Op : op, 
						ClientId: ck.ClientId, RequestId: ck.RequestId}
		reply := PutAppendReply{}

		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.RequServer = server
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
