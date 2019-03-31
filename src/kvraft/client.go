package raftkv

import (
	//"go/constant"
	"labrpc"
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
	respChan := make(chan string, 200)
	go ck.SendGet(key, respChan)
	v := <-respChan
	DPrintf("[*Get*, %s][%v]\n", key, v)
	return v
}

func (ck *Clerk) SendGet(key string, resp chan string) {
	i := ck.leaderId
	ck.nextCmdSerialNum++
	for {
		args := GetArgs{key, ck.Id, ck.nextCmdSerialNum}
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			} else if reply.Err == OK {
				ck.leaderId = i
				resp <- reply.Value
				return
			}
		} else {
			i = (i + 1) % len(ck.servers)
			continue
		}
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
	respChan := make(chan bool, 200)
	go ck.SendPutAppend(key, value, op, respChan)
	ok := <-respChan
	DPrintf("[*%s*, {%s, %s}][%t]\n", op, key, value, ok)
	if ok {
		return
	}
}

func (ck *Clerk) SendPutAppend(key string, value string, op string, resp chan bool) {
	i := ck.leaderId
	ck.nextCmdSerialNum++
	for {
		args := PutAppendArgs{key, value, op, ck.Id, ck.nextCmdSerialNum}
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			} else if reply.Err == OK {
				ck.leaderId = i
				resp <- true
				return
			}
		} else {
			i = (i + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
