package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers      []*labrpc.ClientEnd
	requestMutex sync.Mutex // 用于请求自增的mutex
	// You will have to modify this struct.
	leaderId  int   // leader的id号
	clientId  int64 // client的id号
	requestId int   // 每个请求的编号
}

func (ck *Clerk) init() {
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0

}

func (ck *Clerk) getAndIncreaseRequestId() int {
	ck.requestMutex.Lock()
	defer ck.requestMutex.Unlock()
	returnValue := ck.requestId
	ck.requestId++
	return returnValue
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
	ck.init()
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
	requestId := ck.getAndIncreaseRequestId()
	clientId := ck.clientId
	reply := GetReply{}
	args := GetArgs{
		Key:       key,
		ClientId:  clientId,
		RequestId: requestId,
	}
	leaderId := ck.leaderId
	value := ""
	for {
		ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			ck.leaderId = leaderId
			value = reply.Value
			break
		} else if reply.Err == ErrNoKey {
			ck.leaderId = leaderId
			break
		} else {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}
	return value
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
	requestId := ck.getAndIncreaseRequestId()
	clientId := ck.clientId
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  clientId,
		RequestId: requestId,
	}
	reply := PutAppendReply{}
	leaderId := ck.leaderId

	for {
		ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			break
		} else {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
