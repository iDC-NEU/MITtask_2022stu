package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader   int
	clientID int64 // client uuid
	//requestID *Counter // serial id of requests
	requestID int
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
	// Your code here.
	ck.clientID = nrand()
	ck.requestID = 0
	//ck.requestID = &Counter{}
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.requestID++
	args := &CommandArgs{
		Type:      Query,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		//RequestID: ck.requestID.CounterIncrement(1),
		Num: num,
	}

	for {
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.QueryOrReady", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			// time.Sleep(100 * time.Millisecond)
			continue
		}

		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestID++
	args := &CommandArgs{
		Type:      Join,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		//RequestID: ck.requestID.CounterIncrement(1),
		Servers: servers,
	}

	for {
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.JoinOrLeaveOrMove", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			// time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestID++
	args := &CommandArgs{
		Type:      Leave,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		//RequestID: ck.requestID.CounterIncrement(1),
		GIDs: gids,
	}

	for {
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.JoinOrLeaveOrMove", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			// time.Sleep(100 * time.Millisecond)
			continue
		}
		// command handled successfully
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestID++
	args := &CommandArgs{
		Type:      Join,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		//RequestID: ck.requestID.CounterIncrement(1),
		Shard: shard,
		GID:   gid,
	}

	for {
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.JoinOrLeaveOrMove", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			// time.Sleep(100 * time.Millisecond)
			continue
		}
		// command handled successfully
		return
	}
}

func (ck *Clerk) Ready(configNum int, gid int) {
	args := &CommandArgs{
		Type:      Ready,
		ConfigNum: configNum,
		Group:     gid,
	}

	for {
		reply := CommandReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.QueryOrReady", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			// try next
			ck.leader = (ck.leader + 1) % len(ck.servers)
			// time.Sleep(100 * time.Millisecond)
			continue
		}

		return
	}
}
