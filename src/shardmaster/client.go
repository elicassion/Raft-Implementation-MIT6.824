package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId         int
	nextCmdSerialNum int
	Id               int64
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
	ck.nextCmdSerialNum++
	// args := &QueryArgs{}
	// Your code here.
	args := QueryArgs{num, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// args := &JoinArgs{}
	// Your code here.
	// args.Servers = servers
	args := JoinArgs{servers, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args := LeaveArgs{gids, ck.Id, ck.nextCmdSerialNum}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// args := &MoveArgs{}
	// Your code here.
	// args.Shard = shard
	// args.GID = gid
	args := MoveArgs{shard, gid, ck.Id, ck.nextCmdSerialNum}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
