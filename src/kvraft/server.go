package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // "Put", "Append", "Get"
	Key   string
	Value string
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
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[KVServer Recv Op][*Get* %s]\n", args.Key)
	op := Op{Type: "Get", Key: args.Key, Value: ""}
	if kv.rf != nil {
		index, _, ok := kv.rf.Start(op)
		if !ok {
			reply.WrongLeader = true
		} else {
			DPrintf("[Intended Index][I: %d]\n", index)
			for {
				applied := <-kv.applyCh
				DPrintf("[Applied][I: %d]\n", applied.CommandIndex)
				kv.mu.Lock()
				if kv.lastPerformedIndex >= applied.CommandIndex {
					kv.mu.Unlock()
					continue
				}
				isCurIndex := index == applied.CommandIndex
				value := kv.PerformOp(applied.Command.(Op), isCurIndex)
				kv.lastPerformedIndex = applied.CommandIndex
				kv.mu.Unlock()
				if isCurIndex {
					reply.WrongLeader = false
					reply.Err = OK
					reply.Value = value
					return
				}
			}

		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[KVServer Recv Op][*%s* {%s, %s}]\n", args.Op, args.Key, args.Value)
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value}
	if kv.rf != nil {
		index, _, ok := kv.rf.Start(op)
		if !ok {
			reply.WrongLeader = true
		} else {
			DPrintf("[Intended Index][I: %d]\n", index)
			for {
				applied := <-kv.applyCh
				//DPrintf("[Applied][I: %d][*%s* {%s, %s}]\n", applied.CommandIndex)
				DPrintf("[Applied][I: %d]\n", applied.CommandIndex)
				kv.mu.Lock()
				if kv.lastPerformedIndex >= applied.CommandIndex {
					continue
				}
				isCurIndex := index == applied.CommandIndex
				kv.PerformOp(applied.Command.(Op), isCurIndex)
				kv.lastPerformedIndex = applied.CommandIndex
				kv.mu.Unlock()
				if isCurIndex {
					reply.WrongLeader = false
					reply.Err = OK
					return
				}
			}

		}
	}
}

func (kv *KVServer) PerformOp(op Op, isCurOp bool) string {
	oriV, hasKey := kv.db[op.Key]
	switch op.Type {
	case "Get":
		if !isCurOp {
			return ""
		} else {
			if !hasKey {
				return ""
			} else {
				return oriV
			}
		}
	case "Put":
		kv.db[op.Key] = op.Value
		return ""
	case "Append":
		if hasKey {
			kv.db[op.Key] = oriV + op.Value
		} else {
			kv.db[op.Key] = op.Value
		}
		return ""
	default:
		return ""
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

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastPerformedIndex = 0
	return kv
}
