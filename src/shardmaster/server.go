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

type wArgs struct {
	Args interface{}
	Term int
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	configIndex int

	waitings map[int]chan wArgs
	executed map[int64]int

	killChan chan bool
}

//func (sm *ShardMaster) AssignShards(args *JoinArgs) {
//	groupNum = len(args.Servers)
//	gids := make([]int, groupNum)
//	i := 0
//	for k := range args.Servers {
//		gids[i] = k
//		i++
//	}
//
//	shardsPerGroup = NShards / groupNum
//	shardsAssign = make([]int, NShards)
//	for j := 0; j < shardsPerGroup*groupNum; j++ {
//		shardsAssign[j] = gids[j/shardsPerGroup]
//	}
//
//	g := 0
//	for k := shardsPerGroup * groupNum; k < NShards; k++ {
//		shardsAssign[k] = gids[g%groupNum]
//		g++
//	}
//	return shardsAssign
//}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// Num    int              // config number
	// Shards [NShards]int     // shard -> gid
	// Groups map[int][]string // gid -> servers[]
	cpyargs := JoinArgs{make(map[int][]string), args.ClientId, args.OpSerialNum}
	for gid, server := range args.Servers {
		cpyargs.Servers[gid] = append([]string{}, server...)
	}
	if sm.rf != nil {
		//DPrintf("Join\n")
		reply.WrongLeader, _ = sm.PerformOp("Join", cpyargs)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cpyargs := LeaveArgs{make([]int, 0), args.ClientId, args.OpSerialNum}
	cpyargs.GIDs = append(cpyargs.GIDs, args.GIDs...)
	if sm.rf != nil {
		reply.WrongLeader, _ = sm.PerformOp("Leave", cpyargs)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cpyargs := MoveArgs{args.Shard, args.GID, args.ClientId, args.OpSerialNum}
	if sm.rf != nil {
		reply.WrongLeader, _ = sm.PerformOp("Move", cpyargs)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.queryConfig(args.Num)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	cpyargs := QueryArgs{args.Num, args.ClientId, args.OpSerialNum}
	if sm.rf != nil {
		var returnConfig interface{}
		reply.WrongLeader, returnConfig = sm.PerformOp("Query", cpyargs)
		if !reply.WrongLeader {
			reply.Err = OK
			reply.Config = returnConfig.(Config)
			//reply.Config = sm.queryConfig(args.Num)
			DPrintf("[%d][Query Config] %v\n", sm.me, reply.Config)
		}
	}
}

func (sm *ShardMaster) PerformOp(argType string, args interface{}) (bool, interface{}) {
	index, term, ok := sm.rf.Start(args)
	if !ok {
		return true, -1
	} else {
		DPrintf("[%d][Perform] [%s] %v\n", sm.me, argType, args)
		//DPrintf("[%d][Intended][I: %d][*%s* {%s, %s}]\n", kv.me, index, op.Type, op.Key, op.Value)
		sm.mu.Lock()
		completeChan := make(chan wArgs, 3)
		sm.waitings[index] = completeChan
		sm.mu.Unlock()

		var wargs wArgs
		select {
		case wargs = <-completeChan:
			if wargs.Term != term {
				return true, -1
			} else {
				return false, wargs.Args
			}
		case <-time.After(RAFT_COMMIT_TIMEOUT):
			sm.mu.Lock()
			delete(sm.waitings, index)
			sm.mu.Unlock()
			return true, -1
		}
		return false, wargs.Args
	}
}

func (sm *ShardMaster) CompleteOp(applied *raft.ApplyMsg) {
	DPrintf("[%d][Complete] %v\n", sm.me, applied.Command)
	completeArgs := wArgs{0, applied.CommandTerm}
	if args, typeOK := applied.Command.(JoinArgs); typeOK {
		if sm.executed[args.ClientId] < args.OpSerialNum {
			sm.doJoin(&args, &completeArgs)
		}
	} else if args, typeOK := applied.Command.(LeaveArgs); typeOK {
		if sm.executed[args.ClientId] < args.OpSerialNum {
			sm.doLeave(&args, &completeArgs)
		}
	} else if args, typeOK := applied.Command.(MoveArgs); typeOK {
		if sm.executed[args.ClientId] < args.OpSerialNum {
			sm.doMove(&args, &completeArgs)
		}
	} else if args, typeOK := applied.Command.(QueryArgs); typeOK {
		//DPrintf("[Query]\n")
		qConfig := sm.queryConfig(args.Num)
		completeArgs.Args = qConfig
	}
	DPrintf("[%d][Last Config] %v\n", sm.me, sm.queryConfig(-1))
	cmdIndex := applied.CommandIndex
	completeCh, hasChan := sm.waitings[cmdIndex]
	if hasChan {
		completeCh <- completeArgs
		delete(sm.waitings, cmdIndex)
	}
}

func (sm *ShardMaster) doJoin(args *JoinArgs, completeArgs *wArgs) {
	newConfig := sm.queryConfig(-1)
	newGIDs := make([]int, 0)
	for gid, server := range args.Servers {
		if s, ok := newConfig.Groups[gid]; ok {
			newConfig.Groups[gid] = append(s, server...)
		} else {
			newConfig.Groups[gid] = append([]string{}, server...)
			newGIDs = append(newGIDs, gid)
		}
	}
	groupNum := len(newConfig.Groups)
	gids := make([]int, groupNum)
	i := 0
	for k := range newConfig.Groups {
		gids[i] = k
		i++
	}
	if groupNum == 0 {
		newConfig.Shards = [NShards]int{}
		newConfig.Num = len(sm.configs)
		sm.executed[args.ClientId] = args.OpSerialNum
		sm.configs = append(sm.configs, newConfig)
		return
	} else if groupNum <= NShards {
		shardsPerGroup := NShards / groupNum
		resShards := NShards % groupNum
		// we can't do the following because we should keep the order of shards
		// for j := 0; j < shardsPerGroup*groupNum; j++ {
		// 	newConfig.Shards[j] = gids[j/shardsPerGroup]
		// }
		// g := 0
		// for k := shardsPerGroup * groupNum; k < NShards; k++ {
		// 	newConfig.Shards[k] = gids[g%groupNum]
		// 	g++
		// }
		assignedShardsNum := make(map[int]int)
		newGIDIdx := 0
		for i := 0; i < NShards; i++ {
			gid := newConfig.Shards[i]
			// group shards are full
			if gid == 0 || shardsPerGroup == assignedShardsNum[gid] && resShards <= 0 ||
				assignedShardsNum[gid] == shardsPerGroup+1 {
				assignGid := newGIDs[newGIDIdx]
				newConfig.Shards[i] = assignGid
				assignedShardsNum[assignGid]++
				newGIDIdx++
				newGIDIdx = newGIDIdx % len(newGIDs)
			} else {
				assignedShardsNum[gid]++
				if assignedShardsNum[gid] == shardsPerGroup {
					resShards--
				}
			}

		}
	}
	newConfig.Num = len(sm.configs)
	sm.executed[args.ClientId] = args.OpSerialNum
	sm.configs = append(sm.configs, newConfig)
	for gid, _ := range args.Servers {
		_, ok := newConfig.Groups[gid]
		DPrintf("[%d][Join successed?] %v\n", sm.me, ok)
	}

}

func (sm *ShardMaster) doLeave(args *LeaveArgs, completeArgs *wArgs) {
	newConfig := sm.queryConfig(-1)
	leaveGIDs := make(map[int]bool)
	for _, gid := range args.GIDs {
		leaveGIDs[gid] = true
		delete(newConfig.Groups, gid)
	}
	groupNum := len(newConfig.Groups)
	resGIDs := make([]int, 0)
	for gid := range newConfig.Groups {
		resGIDs = append(resGIDs, gid)
	}
	if len(newConfig.Groups) == 0 {
		newConfig.Shards = [NShards]int{}
	} else {
		shardsPerGroup := NShards / groupNum
		if shardsPerGroup < 1 {
			shardsPerGroup = 1
		}
		assignedShardsNum := make(map[int]int)
		resGIDIdx := 0
		for i := 0; i < NShards; i++ {
			findAnother := false
			gid := newConfig.Shards[i]
			_, ifGIDExists := leaveGIDs[gid]
			// leave or full
			if ifGIDExists || assignedShardsNum[gid] == shardsPerGroup {
				for _, anotherGID := range resGIDs {
					assigned := assignedShardsNum[anotherGID]
					if assigned < shardsPerGroup {
						newConfig.Shards[i] = anotherGID
						assignedShardsNum[anotherGID]++
						findAnother = true
						break
					}
				}
				if findAnother == true {
					continue
				}
				// all full, assign only at most one more to the rest of each
				anotherGID := resGIDs[resGIDIdx]
				resGIDIdx++
				resGIDIdx %= len(resGIDs)
				newConfig.Shards[i] = anotherGID
				assignedShardsNum[anotherGID]++
			} else {
				assignedShardsNum[gid]++
			}
		}
	}
	newConfig.Num = len(sm.configs)
	sm.executed[args.ClientId] = args.OpSerialNum
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) doMove(args *MoveArgs, completeArgs *wArgs) {
	lastConfig := sm.queryConfig(len(sm.configs) - 1)
	lastConfig.Shards[args.Shard] = args.GID
	lastConfig.Num = len(sm.configs)
	sm.executed[args.ClientId] = args.OpSerialNum
	sm.configs = append(sm.configs, lastConfig)
}

func (sm *ShardMaster) queryConfig(i int) Config {
	var qIndex int
	if i < 0 || i >= len(sm.configs) {
		qIndex = len(sm.configs) - 1
	} else {
		qIndex = i
	}
	cpyConfig := Config{sm.configs[qIndex].Num,
		sm.configs[qIndex].Shards,
		make(map[int][]string)}
	for gid, servers := range sm.configs[qIndex].Groups {
		cpyConfig.Groups[gid] = append([]string{}, servers...)
	}
	//DPrintf("[Query Config] %v\n", cpyConfig)
	return cpyConfig
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
		//if applied.Command == "NEWLEADER" {
		//	sm.rf.Start("")
		//}
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

	labgob.Register(wArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryReply{})
	sm.applyCh = make(chan raft.ApplyMsg, 1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitings = make(map[int]chan wArgs)
	sm.executed = make(map[int64]int)
	sm.killChan = make(chan bool, 1000)

	go func() {
		sm.rf.Restore(1)
		for {
			select {
			case <-sm.killChan:
				return
			case applied := <-sm.applyCh:
				sm.mu.Lock()
				sm.RecvApplied(&applied)
				sm.mu.Unlock()
			}
		}
	}()

	return sm
}
