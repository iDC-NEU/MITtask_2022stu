package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int	//表示操作的唯一序列号
	leaderId int // 确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
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
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
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
	ck.seqId++
	args:=GetArgs{
		Key:key,
		ClientId:ck.clientId,
		SeqId:ck.seqId,
	}
	
	for {
		serverId:=ck.leaderId
		reply:=GetReply{ }
		ok:=ck.servers[serverId].Call("KVServer.Get",&args,&reply)

		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId+1) % len(ck.servers)
				continue
			}
		}
		ck.leaderId = (ck.leaderId+1) % len(ck.servers)
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
	ck.seqId++
	args:=PutAppendArgs{
		Key:key,
		Value:value,
		Op:op,
		ClientId:ck.clientId,
		SeqId:ck.seqId,
	}
	
	for {
		serverId:=ck.leaderId
		reply:=PutAppendReply{ }
		ok:=ck.servers[serverId].Call("KVServer.PutAppend",&args,&reply)

		if ok {
			switch reply.Err {
			case OK:
				return 
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId+1) % len(ck.servers)
				continue
			}
		}
		ck.leaderId = (ck.leaderId+1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
