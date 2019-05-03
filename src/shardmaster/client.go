package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId         int
	nextCmdSerialNum int
	Id               int64
	mu               sync.Mutex
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
	ck.leaderId = 0
	ck.nextCmdSerialNum = 0
	ck.Id = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	ck.nextCmdSerialNum++
	ck.mu.Unlock()
	// args := &QueryArgs{}
	// Your code here.
	args := QueryArgs{num, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		//reply := QueryReply{true, "", Config{}}
		reply := QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Query", &args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	ck.nextCmdSerialNum++
	ck.mu.Unlock()
	args := JoinArgs{servers, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		//reply := JoinReply{true, ""}
		reply := JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Join", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		DPrintf("Wrong Leader.")
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	ck.nextCmdSerialNum++
	ck.mu.Unlock()
	args := LeaveArgs{gids, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		//reply := LeaveReply{true, ""}
		reply := LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Leave", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	ck.nextCmdSerialNum++
	ck.mu.Unlock()
	args := MoveArgs{shard, gid, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		//reply := MoveReply{true, ""}
		reply := MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}
