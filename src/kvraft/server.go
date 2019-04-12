package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

const RAFT_COMMIT_TIMEOUT = time.Duration(1 * time.Second)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // "Put", "Append", "Get"
	Key   string
	Value string

	ClientId  int64
	SerialNum int
}

type WOp struct {
	Op       Op
	Complete chan bool
	Term     int
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
	waitings           map[int]chan Op
	executed           map[int64]int

	killChan chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("[%d][KVServer Recv Op][*Get* %s]\n", kv.me, args.Key)
	op := Op{Type: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, SerialNum: args.OpSerialNum}
	if kv.rf != nil {
		reply.WrongLeader = kv.PerformOp(op)
		if !reply.WrongLeader {
			kv.mu.Lock()
			oriV, hasKey := kv.db[op.Key]
			kv.mu.Unlock()
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
		//DPrintf("[%d][Intended][I: %d][*%s* {%s, %s}]\n", kv.me, index, op.Type, op.Key, op.Value)
		completeChan := make(chan Op, 3)
		kv.mu.Lock()
		kv.waitings[index] = completeChan
		kv.mu.Unlock()

		var cOp Op
		select {
		case cOp = <-completeChan:
			return !(cOp.SerialNum == op.SerialNum && cOp.ClientId == op.ClientId)
		case <-time.After(RAFT_COMMIT_TIMEOUT):
			kv.mu.Lock()
			delete(kv.waitings, index)
			kv.mu.Unlock()
			return true
		}
		return false
	}
}

func (kv *KVServer) CompleteOp(applied *raft.ApplyMsg) {

	op := applied.Command.(Op)
	cmdIndex := applied.CommandIndex
	//oriV, hasKey := kv.db[op.Key]
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastExe, seenClient := kv.executed[op.ClientId]

	if !seenClient || (seenClient && op.SerialNum > lastExe) {
		DPrintf("[%d][Applied][I: %d][*%s* {%s, %s}]\n", kv.me, cmdIndex, op.Type, op.Key, op.Value)
		switch op.Type {
		case "Put":
			kv.db[op.Key] = op.Value
			break
		case "Append":
			//if hasKey {
			kv.db[op.Key] += op.Value
			//} else {
			//	kv.db[op.Key] = op.Value
			//}
			break
		default:
			break
		}
		kv.executed[op.ClientId] = op.SerialNum
	}
	completeCh, hasChan := kv.waitings[cmdIndex]
	if hasChan {
		delete(kv.waitings, cmdIndex)
		completeCh <- op
	}
	size := kv.rf.GetSnapshotSize()
	//DPrintf("[Log Size]: %d\n", size)
	if size >= int(float32(kv.maxraftstate)*1.5) && kv.maxraftstate > 0 {
		DPrintf("[%d][Make Snapshot]\n", kv.me)
		kv.rf.Snapshot(kv.makeSnapshotData(), applied.CommandIndex)
	}

	//if kv.waitings[cmdIndex] != nil {
	//	wOpList := kv.waitings[cmdIndex]
	//	for i := range wOpList {
	//		if op.ClientId == wOpList[i].Op.ClientId && op.SerialNum == wOpList[i].Op.SerialNum {
	//			kv.waitings[cmdIndex][i].Complete <- true
	//		} else {
	//			kv.waitings[cmdIndex][i].Complete <- false
	//		}
	//	}
	//} else {
	//	return
	//}
}

func (kv *KVServer) InstallSnapshot(applied *raft.ApplyMsg) {

	data := applied.Snapshot.([]byte)
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	//var waitings map[int][]WOp
	var executed map[int64]int
	if d.Decode(&db) != nil ||
		//d.Decode(&waitings) != nil ||
		d.Decode(&executed) != nil {
		DPrintf("[Decode Error]\n")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.db = db
		//kv.waitings = waitings
		kv.executed = executed
	}
	//kv.rf.Snapshot(kv.makeSnapshotData())
}

func (kv *KVServer) RecvApplied(applied *raft.ApplyMsg) {
	if applied.CommandValid == true {
		kv.CompleteOp(applied)
	} else {
		kv.InstallSnapshot(applied)
	}
}

func (kv *KVServer) makeSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.waitings)
	e.Encode(kv.executed)
	data := w.Bytes()
	return data
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	DPrintf("[%d][Killing KV Server]\n", kv.me)
	//kv.killChan <- true
	close(kv.killChan)
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

	kv.db = make(map[string]string)
	kv.waitings = make(map[int]chan Op)
	kv.executed = make(map[int64]int)

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.killChan = make(chan bool, 1000)
	kv.lastPerformedIndex = 0

	go func() {
		//time.Sleep(1 * time.Second)
		kv.rf.Restore(1)
		for {
			select {
			case <-kv.killChan:
				return
			case applied := <-kv.applyCh:
				kv.RecvApplied(&applied)
			}
			//time.Sleep(2*time.Millisecond)
		}
		//for applied := range kv.applyCh {
		//	kv.RecvApplied(&applied)
		//}
	}()

	//go func() {
	//	kv.SurveillanceLogSize()
	//}()
	return kv
}
