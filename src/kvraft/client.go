package raftkv

import (
	//"go/constant"
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.nextCmdSerialNum = 0
	ck.Id = nrand()
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
	respChan := make(chan string, 1000)
	ck.SendGet(key, respChan)
	v := <-respChan
	return v
}

func (ck *Clerk) SendGet(key string, resp chan string) {
	ck.nextCmdSerialNum++
	for {
		args := GetArgs{key, ck.Id, ck.nextCmdSerialNum}
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				if reply.Err == OK {
					DPrintf("[%d][*Get*, %s][%v]\n", ck.leaderId, key, reply.Value)
					resp <- reply.Value
				} else {
					resp <- ""
				}
				//resp <- reply.Value
				return
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
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
	respChan := make(chan bool, 1000)
	ck.SendPutAppend(key, value, op, respChan)
	ok := <-respChan
	if ok {
		return
	}
}

func (ck *Clerk) SendPutAppend(key string, value string, op string, resp chan bool) {
	ck.nextCmdSerialNum++
	for {
		args := PutAppendArgs{key, value, op, ck.Id, ck.nextCmdSerialNum}
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				resp <- true
				DPrintf("[%d][*%s*, {%s, %s}][%t]\n", ck.leaderId, op, key, value, ok)
				return
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
