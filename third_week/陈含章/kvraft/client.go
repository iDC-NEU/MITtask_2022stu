package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int   // the current leader ID
	clientID  int64 // client id
	requestID int   // serial id of requests
}

func nrand() int64 { //use for clientID
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// initialization
	ck.clientID = nrand() //make client ID
	ck.requestID = 1
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
	return ck.sendCommandRequest(key, "", "Get")
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
	ck.sendCommandRequest(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// merge read/write RPCs to one
func (ck *Clerk) sendCommandRequest(key string, value string, op string) string {
	// send request persistently
	for {
		args := CommandArgs{key, value, op, ck.clientID, ck.requestID}
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("KVServer.CommandRequest", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		// command handled successfully
		ck.requestID++
		return reply.Value
	}
}
