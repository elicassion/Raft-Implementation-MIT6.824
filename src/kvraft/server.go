package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const RAFT_COMMIT_TIMEOUT = time.Duration(5 * time.Second)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // "Put", "Append", "Get"
	Key   string
	Value string

	ClientId	int
	SerialNum	int
}

type wOp struct {
	Op       *Op
	complete chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastPerformedIndex int
	db                 map[string]string
	waitings           map[int]*wOp
	executed			map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("[%d][KVServer Recv Op][*Get* %s]\n", kv.me, args.Key)
	op := Op{Type: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if kv.rf != nil {
		reply.WrongLeader = kv.PerformOp(op)
		if !reply.WrongLeader {
			oriV, hasKey := kv.db[op.Key]
			if hasKey {
				reply.Err = OK
				reply.Value = oriV
			} else {
				reply.Err = ErrNoKey
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("[%d][KVServer Recv Op][*%s* {%s, %s}]\n", kv.me, args.Op, args.Key, args.Value)
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if kv.rf != nil {
		reply.WrongLeader = kv.PerformOp(op)
		if !reply.WrongLeader {
			reply.Err = OK
		}
	}
}

func (kv *KVServer) PerformOp(op Op) bool {
	index, _, ok := kv.rf.Start(op)
	if !ok {
		return true
	} else {
		DPrintf("[%d][Intended][I: %d][*%s* {%s, %s}]\n", kv.me, index, op.Type, op.Key, op.Value)
		completeChan := make(chan bool)
		kv.mu.Lock()
		kv.waitings[index] = &wOp{&op, completeChan}
		kv.mu.Unlock()

		var complete bool
		select{
			case complete = <-completeChan:
			case <- time.After(RAFT_COMMIT_TIMEOUT):
				complete = false
		}
		kv.mu.Lock()
		delete(kv.waitings, index)
		kv.mu.Unlock()
		return !complete
	}
}

func (kv *KVServer) CompleteOp(applied raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applied.Command.(Op)
	cmdIndex := applied.CommandIndex
	DPrintf("[Applied][I: %d][*%s* {%s, %s}]\n", cmdIndex, op.Type, op.Key, op.Value)
	oriV, hasKey := kv.db[op.Key]
	if (op.SerialNum)
	switch op.Type {
	case "Put":
		kv.db[op.Key] = op.Value
		break
	case "Append":
		if hasKey {
			kv.db[op.Key] = oriV + op.Value
		} else {
			kv.db[op.Key] = op.Value
		}
		break
	default:
		break
	}

	if kv.waitings[cmdIndex] != nil {
		if op.ClientId == kv.waitings[cmdIndex].Op.ClientId{
			kv.waitings[cmdIndex].complete <- true
		} else{
			kv.waitings[cmdIndex].complete <- false
		}
	} else {
		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.waitings = make(map[int]*wOp)
	kv.executed = make(map[int]int)
	kv.lastPerformedIndex = 0


	go func() {
		for applied := range kv.applyCh {
			kv.CompleteOp(applied)
		}
	}()
	return kv
}
