package shardmaster

import (
	"fmt"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

const RAFT_COMMIT_TIMEOUT = time.Duration(1 * time.Second)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	configIndex int

	waitings map[int]chan Op
	executed map[int64]int

	killChan chan bool
}

type Op struct {
	// Your data here.
	Type  string // "Put", "Append", "Get"
	Key   string
	Value string

	ClientId  int64
	SerialNum int
}

func (sm *ShardMaster) AssignShards(args *JoinArgs) {
	groupNum = len(args.Servers)
	gids := make([]int, groupNum)
	i := 0
	for k := range args.Servers {
		gids[i] = k
		i++
	}

	shardsPerGroup = NShards / groupNum
	shardsAssign = make([]int, NShards)
	for j := 0; j < shardsPerGroup*groupNum; j++ {
		shardsAssign[j] = gids[j/shardsPerGroup]
	}

	g := 0
	for k := shardsPerGroup * groupNum; k < NShards; k++ {
		shardsAssign[k] = gids[g%groupNum]
		g++
	}
	return shardsAssign
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// Num    int              // config number
	// Shards [NShards]int     // shard -> gid
	// Groups map[int][]string // gid -> servers[]
	sm.mu.Lock()
	defer sm.mu.Unlock()
	shards = AssignShards(args)
	// Servers map[int][]string // new GID -> servers mappings
	op := Op{Type: "Config"}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if kv.rf != nil {
		reply.WrongLeader = sm.PerformOp(op)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if sm.rf != nil {
		reply.WrongLeader = sm.PerformOp(op)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if kv.rf != nil {
		reply.WrongLeader = sm.PerformOp(op)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) PerformOp(op Op) bool {
	index, _, ok := sm.rf.Start(op)
	if !ok {
		return true
	} else {
		//DPrintf("[%d][Intended][I: %d][*%s* {%s, %s}]\n", kv.me, index, op.Type, op.Key, op.Value)
		completeChan := make(chan Op, 3)
		sm.mu.Lock()
		sm.waitings[index] = completeChan
		sm.mu.Unlock()

		var cOp Op
		select {
		case cOp = <-completeChan:
			return !(cOp.SerialNum == op.SerialNum && cOp.ClientId == op.ClientId)
		case <-time.After(RAFT_COMMIT_TIMEOUT):
			sm.mu.Lock()
			delete(sm.waitings, index)
			sm.mu.Unlock()
			return true
		}
		return false
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.killChan)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) RecvApplied(applied *raft.ApplyMsg) {
	if applied.CommandValid == true {
		sm.CompleteOp(applied)
	} else {
		if msg.Command.(Op).Type == "NEWLEADER" {
			sm.rf.Start("")
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitings = make(map[int]chan Op)
	sm.executed = make(map[int64]int)
	sm.killChan = make(chan bool, 1000)

	go func() {
		sm.rf.Restore(1)
		for {
			select {
			case <-sm.killChan:
				return
			case applied := <-sm.applyCh:
				sm.RecvApplied(&applied)
			}
		}
	}()

	return sm
}
