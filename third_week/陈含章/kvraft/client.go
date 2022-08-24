package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentLeader int   // the current leader ID
	clientID      int64 // client id
	requestID     int
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
	// You'll have to add code here

	ck.requestID = 1
	ck.clientID = nrand() //make client ID
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
	//return ck.sendCommandRequest(key, "", "Get")
	for {

		//renew args and reply
		args := CommandArgs{key, "", "Get", ck.clientID, ck.requestID}
		reply := CommandReply{}
		finish := ck.servers[ck.currentLeader].Call("KVServer.CommandRequest", &args, &reply)

		if finish && reply.Err != ErrWrongLeader && reply.Err != ErrTimeout {
			ck.requestID++
			return reply.Value

		} else {
			ck.currentLeader = changeLeader(ck.currentLeader, len(ck.servers))
			continue
		}
	}
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
	ck.CommandRequest(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) CommandRequest(key string, value string, op string) string {

	var finish bool
	var args CommandArgs
	var reply CommandReply

	for {

		//renew args and reply
		args = CommandArgs{key, value, op, ck.clientID, ck.requestID}
		reply = CommandReply{}

		finish = ck.servers[ck.currentLeader].Call("KVServer.CommandRequest", &args, &reply)

		if finish && reply.Err != ErrWrongLeader && reply.Err != ErrTimeout {
			ck.requestID++
			return reply.Value

		} else {
			// try again
			ck.currentLeader = changeLeader(ck.currentLeader, len(ck.servers))
			continue
		}
	}
}

func changeLeader(currentLeader int, allServer int) int {

	var newLeader int
	currentLeader++
	newLeader = currentLeader % allServer
	return newLeader

}
