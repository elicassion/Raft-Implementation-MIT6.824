package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	// "fmt"
	//"log"
	//"flag"
	"sync"
	"time"
)
import "labrpc"
import (
	"math/rand"
)

import "bytes"
import "labgob"

func min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	Snapshot     interface{}
}

//
// A Go object implementing a single Raft peer.
//

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

type Logs struct {
	Lgs               []Log
	LastSnapshotIndex int
	LastSnapshotTerm  int
	LastCommand       interface{}
}

func (logs *Logs) get(i int) *Log {
	//DPrintf("[Last Snapshot Index]: %d\n", logs.LastSnapshotIndex)
	//if i == logs.LastSnapshotIndex {
	//	return &Log{logs.LastSnapshotTerm, logs.LastSnapshotIndex, logs.LastCommand}
	//}
	return &logs.Lgs[i-logs.LastSnapshotIndex]
}

func (logs *Logs) append(elems []Log) {
	logs.Lgs = append(logs.Lgs, elems...)
}

func (logs *Logs) appendSingle(l Log) {
	logs.Lgs = append(logs.Lgs, l)
}

func (logs *Logs) slice(start int, end int) []Log {
	//l := logs.len()
	if start < 0 && end >= 0 {
		if end-logs.LastSnapshotIndex == 0 {
			return make([]Log, 0)
		}
		return logs.Lgs[:end-logs.LastSnapshotIndex]
	} else if start >= 0 && end < 0 {
		return logs.Lgs[start-logs.LastSnapshotIndex:]
	} else {
		if end-logs.LastSnapshotIndex == 0 {
			return make([]Log, 0)
		}
		return append(make([]Log, 0), logs.Lgs[start-logs.LastSnapshotIndex:end-logs.LastSnapshotIndex]...)
	}
	//if end < 0 { end = 0 }
	//if end > l { end = l }
}

func (logs *Logs) len() int {
	return len(logs.Lgs)
}

func (logs *Logs) getLast() *Log {
	//DPrintf("[Log len]: %d\n", len(logs.Lgs))
	return &logs.Lgs[len(logs.Lgs)-1]
}

func (logs *Logs) dropBefore(i int) {
	DPrintf("[Log length]: %d [Dropping from]: %d\n", len(logs.Lgs), i)
	prevLastSnapshotIndex := logs.LastSnapshotIndex
	DPrintf("[Prev Last Snapshot Index]: %d\n", prevLastSnapshotIndex)
	//logs.LastSnapshotIndex = logs.Lgs[i-prevLastSnapshotIndex].Index
	//logs.LastSnapshotTerm = logs.Lgs[i-prevLastSnapshotIndex].Term
	//logs.LastCommand = logs.Lgs[i-prevLastSnapshotIndex].Command
	//logs.Lgs = logs.Lgs[i-prevLastSnapshotIndex:]
	dropPoint := 0
	for j := range logs.Lgs {
		if logs.Lgs[j].Index == i {
			dropPoint = j
			logs.LastSnapshotIndex = logs.Lgs[j].Index
			logs.LastSnapshotTerm = logs.Lgs[j].Term
			logs.LastCommand = logs.Lgs[j].Command
			break
		}
	}
	logs.Lgs = logs.Lgs[dropPoint:]
}

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	//logs        []Log
	logs        Logs
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	heartbeatChan chan bool
	electionChan  chan bool
	commitChan    chan bool

	recvVoteNum int

	killed bool

	killChan chan bool

	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	//rf.mu.Unlock()
	return term, isLeader
}

//func (rf *Raft) switchToFollower(term int) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if term != -1 {
//		rf.currentTerm = term
//	}
//	rf.state = FOLLOWER
//	rf.votedFor = NOT_VOTE
//}

func (rf *Raft) switchToLeader() {
	rf.mu.Lock()
	DPrintf("[New Leader] %d - Term %d\n", rf.me, rf.currentTerm)
	rf.state = LEADER
	// init records for followers
	//nextIndex := len(rf.logs)
	nextIndex := rf.getNextIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.heartbeat()
}

func (rf *Raft) getLastIndex() int {
	//return len(rf.logs) - 1
	//if rf.logs.len() == 0 {
	//	return rf.logs.LastSnapshotIndex
	//}
	return rf.logs.getLast().Index
}

func (rf *Raft) getLastTerm() int {
	//return rf.logs[len(rf.logs)-1].Term
	//if rf.logs.len() == 0 {
	//	return rf.logs.LastSnapshotTerm
	//}
	return rf.logs.getLast().Term
}

func (rf *Raft) getNextIndex() int {
	//if rf.logs.len() == 0 {
	//	DPrintf("[Next Index]: %d\n", rf.logs.LastSnapshotIndex+1)
	//	return rf.logs.LastSnapshotIndex + 1
	//}
	//DPrintf("[Next Index]: %d\n", rf.logs.getLast().Index+1)
	return rf.logs.getLast().Index + 1
}

func (rf *Raft) dropLog(truncIndex int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf("[%d][Dropping Log]: %d\n", rf.me, truncIndex)
	rf.logs.dropBefore(truncIndex)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) makeRaftPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

func (rf *Raft) GetSnapshotSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(snapshotData []byte, truncIndex int) {
	rf.mu.Lock()
	if truncIndex > rf.logs.LastSnapshotIndex && truncIndex <= rf.lastApplied {
		rf.dropLog(truncIndex)
		rf.persister.SaveStateAndSnapshot(rf.makeRaftPersistData(), snapshotData)
	}
	rf.mu.Unlock()
	DPrintf("[%d][Snapshot Fnished]\n", rf.me)
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedFor)
	//e.Encode(rf.logs)
	data := rf.makeRaftPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	//var logs []Log
	var logs Logs
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("[Decode Error]\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	newAppliedMsg := ApplyMsg{
		false,
		//rf.logs[i].Command,
		0,
		0,
		-1,
		data,
	}
	rf.applyChan <- newAppliedMsg
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
const NOT_VOTE = -1

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if rf.state == LEADER {
		// DPrintf("fuck\n")
	}

	//DPrintf("[%d][Term: %d][Recv Req Vote from][%d][Term: %d]\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		//DPrintf("[Rej Vote - Term] curterm %d incometerm %d\n", rf.currentTerm, args.Term)
		return
	} else if rf.currentTerm < args.Term {
		if rf.state == LEADER {
			//DPrintf("[%d][Step Down][Req Vote from Higher Term]\n", rf.me)
		}
		rf.votedFor = NOT_VOTE
		rf.currentTerm = args.Term
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
			rf.heartbeatChan <- true
		}
		//DPrintf("[%d] Recv HB (requestvote) from [%d]\n", rf.me, args.CandidateId)
		rf.persist()
	}

	reply.Term = rf.currentTerm
	// rf.currentTerm == args.Term
	if rf.votedFor == NOT_VOTE || rf.votedFor == args.CandidateId {
		if rf.getLastTerm() < args.LastLogTerm ||
			(rf.getLastTerm() == args.LastLogTerm &&
				rf.getLastIndex() <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER //critical for test 2b-2?
			rf.persist()
			return
		} else {
			//DPrintf("[%d][Rej Vote from %d] not match\n", rf.me, args.CandidateId)
		}
	} else {
		//DPrintf("[Rej Vote] %d voted for %d\n", rf.me, rf.votedFor)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.recvVoteNum = 1
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.persist()
	DPrintf("%d Term %d Hold Election [Peers = %d]\n", rf.me, rf.currentTerm, len(rf.peers))
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		requestVoteArgs := RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			rf.getLastIndex(),
			rf.getLastTerm(),
		}
		rf.mu.Unlock()

		go func(server int, requestVoteArgs RequestVoteArgs) {
			requestVoteReply := RequestVoteReply{}
			//DPrintf("[Send Vote Req to %d] Candidate: %d, Term: %d\n", server, rf.me, requestVoteArgs.Term)
			resp := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			//DPrintf("[Reqest Vote Status from %d] %t\n", server, resp)
			if resp {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// if state changed, abort
				//DPrintf("[Request Vote Reply Info] Peer: %d, Term: %d, Granted: %t\n", server, requestVoteReply.Term, requestVoteReply.VoteGranted)
				if rf.state != CANDIDATE || rf.currentTerm != requestVoteArgs.Term {
					//rf.mu.Unlock()
					return
				}
				if rf.currentTerm < requestVoteReply.Term {
					rf.currentTerm = requestVoteReply.Term
					rf.state = FOLLOWER
					rf.votedFor = NOT_VOTE
					rf.heartbeatChan <- true
					//rf.mu.Unlock()
					rf.persist()
					return
				}
				if rf.currentTerm == requestVoteReply.Term && requestVoteReply.VoteGranted {
					rf.recvVoteNum++
					// DPrintf("[%d Recv Vote from %d] Term: %d, Recv Vote Num: %d\n", rf.me, server, requestVoteReply.Term, rf.recvVoteNum)
					if rf.recvVoteNum > len(rf.peers)/2 {
						//rf.mu.Unlock()
						rf.electionChan <- true
					}
				}
			}
		}(i, requestVoteArgs)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.NextIndex = rf.getLastIndex() + 1
	//im a leader, reject
	if rf.currentTerm > args.Term {
		// send updated term
		reply.Term = rf.currentTerm
		//DPrintf("[%d][Rej Append from %d] term fall back\n", rf.me, args.LeaderID)
		//rf.mu.Unlock()
		return
	}

	// follower
	// reset heartbeat
	rf.heartbeatChan <- true
	if rf.currentTerm < args.Term {
		//rf.switchToFollower(args.Term)
		// if rf.currentTerm < args.Term {
		//DPrintf("[Update Term] %d{%d} Term %d -> %d\n", rf.me, rf.state, rf.currentTerm, args.Term)
		// }
		rf.currentTerm = args.Term
		rf.votedFor = NOT_VOTE
		rf.persist()
	}
	rf.state = FOLLOWER
	reply.Term = rf.currentTerm

	if rf.logs.LastSnapshotIndex > args.PrevLogIndex {
		reply.NextIndex = rf.logs.LastSnapshotIndex + 1
		return
	}

	// incoming index can't fill the hole
	if rf.getLastIndex() < args.PrevLogIndex {
		//DPrintf("[%d][Rej Append from %d] index ahead %d <- %d\n",
		//rf.me, args.LeaderID, rf.getLastIndex(), args.PrevLogIndex)
		reply.NextIndex = rf.getLastIndex() + 1
		//rf.mu.Unlock()
		return
	}
	// check term match
	//if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
	//DPrintf("[%d][Recv AppendEntries]PrevLogIndex: %d, LastIncludedIndex: %d\n", rf.me, args.PrevLogIndex, rf.logs.LastSnapshotIndex)
	if args.PrevLogIndex > rf.logs.LastSnapshotIndex && rf.logs.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.NextIndex = args.PrevLogIndex
		//for i := args.PrevLogIndex - 1; i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term; i-- {
		for i := args.PrevLogIndex - 1; i > max(rf.logs.LastSnapshotIndex, rf.commitIndex) && rf.logs.get(i).Term == rf.logs.get(args.PrevLogIndex).Term; i-- {
			reply.NextIndex = i
		}
		//reply.NextIndex = rf.lastApplied
		//term not match
		//DPrintf("[Rej Append from %d] term not match\n", args.LeaderID)
		//rf.logs = rf.logs[:reply.NextIndex]
		rf.logs.Lgs = rf.logs.slice(-1, reply.NextIndex)
		//rf.persist()
		//rf.mu.Unlock()
		return
	}
	//if rf.commitIndex <

	// copy
	//rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)

	rf.logs.Lgs = rf.logs.slice(-1, args.PrevLogIndex+1)
	rf.logs.append(args.Entries)
	rf.persist()
	// DPrintf("[Log Len] %d\n", len(rf.logs))
	// DPrintf("[Log Entries] ")
	// for j := range rf.logs {
	// 	DPrintf("%v ", rf.logs[j].Command)
	// }
	// DPrintf("\n")

	// leader push commit
	// !IMPORTANT do check whether the commitIndex is shrinking due to out of order RPCs
	prevCommitIndex := rf.commitIndex
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = max(min(rf.getLastIndex(), args.LeaderCommit), rf.commitIndex)
		if rf.commitIndex > prevCommitIndex {
			rf.commitChan <- true
			//DPrintf("[%d][Leader Push Commit] commitedI: %d\n", rf.me, rf.commitIndex)
		}

	}
	reply.Success = true
	reply.NextIndex = rf.getLastIndex() + 1
	//DPrintf("[%d][Copy Log Suc] prevlogindex %d afterappendlength %d \n",
	//	rf.me, args.PrevLogIndex, len(rf.logs))
	//DPrintf("[%d][Copy Log Suc] [Prev Log Index]: %d [Length After Append]: %d [Reply Next Index]: %d \n",
	//rf.me, args.PrevLogIndex, rf.getLastIndex(), reply.NextIndex)
	//return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) performCommit() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// DPrintf("[%d][Perform Commit]\n", rf.me)
	willCommitted := -1
	//for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
	for i := max(rf.commitIndex, rf.logs.LastSnapshotIndex) + 1; i <= rf.getLastIndex(); i++ {
		//DPrintf("[%d][Perform Commit][%d]Commited Index: %d, LastIncludedIndex: %d, LastIndex: %d\n", rf.me, i, rf.commitIndex, rf.logs.LastSnapshotIndex, rf.getLastIndex())
		agreeNum := 1
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			// check match
			indexMatch := rf.matchIndex[j] >= i
			// a leader is only allowed to commit logs from current term
			termMatch := rf.logs.get(i).Term == rf.currentTerm
			if indexMatch && termMatch {
				agreeNum++
			}
			if agreeNum > len(rf.peers)/2 {
				willCommitted = i
				break
			}
		}
		//if agreeNum <= len(rf.peers)/2 { break }
	}
	if willCommitted > 0 {
		rf.commitIndex = willCommitted
		rf.commitChan <- true
		DPrintf("[%d][%d Commited][%d -> %d]\n", rf.me, willCommitted, rf.commitIndex-willCommitted, rf.commitIndex)
	}

}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		//DPrintf("[%d][Still sending hb]\n", rf.me)
		return
	} else {
		// DPrintf("[%d][Sending hb]\n", rf.me)
	}
	//rf.performCommit()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()

		if rf.nextIndex[i] <= rf.logs.LastSnapshotIndex {
			// send install snapshot
			rf.mu.Unlock()
			go rf.OrderInstallSnapshot(i)

			continue
		}

		// mind rf.nextIndex[i] - 1 here
		// the index before the entry that will be sent
		prevLogIndex := min(rf.getLastIndex(), rf.nextIndex[i]-1)
		//prevLogTerm := rf.logs[prevLogIndex].Term
		//DPrintf("[%d][prevLogIndex]: %d\n", i, prevLogIndex)
		//var prevLogTerm int
		//if prevLogIndex == rf.logs.LastSnapshotIndex {
		//	prevLogTerm = rf.logs.LastSnapshotTerm
		//} else {
		prevLogTerm := rf.logs.get(prevLogIndex).Term
		//}
		entriesArgs := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			prevLogTerm,
			//make([]Log, len(rf.logs[prevLogIndex+1:])),
			make([]Log, 0),
			rf.commitIndex,
		}
		entriesArgs.Entries = append(entriesArgs.Entries, rf.logs.slice(prevLogIndex+1, -1)...)
		//DPrintf("[%d][HB]->[%d] curTerm: %d, prevLogI: %d, prevLogT: %d, nEntries: %d, cmmittedI: %d, matchIndex: %d \n",
		//	rf.me, i, rf.currentTerm, prevLogIndex, prevLogTerm, len(rf.logs[prevLogIndex+1:]), rf.commitIndex, rf.matchIndex[i])
		//DPrintf("[%d][HB]->[%d] curTerm: %d, prevLogI: %d, prevLogT: %d, nEntries: %d, cmmittedI: %d, matchIndex: %d \n",
		//rf.me, i, rf.currentTerm, prevLogIndex, prevLogTerm, len(entriesArgs.Entries), rf.commitIndex, rf.matchIndex[i])
		//copy(entriesArgs.Entries, rf.logs[prevLogIndex+1:])
		//copy(entriesArgs.Entries, rf.logs.slice(prevLogIndex+1, -1))
		// DPrintf("[Entries] ")
		// for j := range entriesArgs.Entries {
		// 	DPrintf("%v ", entriesArgs.Entries[j].Command)
		// }
		// DPrintf("\n")
		rf.mu.Unlock()
		go func(server int, entriesArgs AppendEntriesArgs) {
			//DPrintf("[Send HB from %d %d] length %d -> %d | r%d\n",
			//	rf.me, rf.state, len(entriesArgs.Entries), server, rand.Intn(7))
			reply := AppendEntriesReply{}
			resp := rf.sendAppendEntries(server, &entriesArgs, &reply)
			if resp {
				rf.updatePeerState(server, &entriesArgs, &reply)
			}
		}(i, entriesArgs)
	}
}

func (rf *Raft) updatePeerState(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	//term fall back
	//switch to and initialize as follower
	if rf.currentTerm < reply.Term {
		//DPrintf("[Step Down] %d Term: %d -> %d\n", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = NOT_VOTE
		rf.persist()
		rf.heartbeatChan <- true
		return
	}

	//update index
	if reply.Success {
		// not void hb
		if args.PrevLogIndex+len(args.Entries) >= rf.nextIndex[peer] {
			rf.nextIndex[peer] = reply.NextIndex
			rf.matchIndex[peer] = reply.NextIndex - 1
		}
		rf.performCommit()
	} else if !reply.Success {
		// not match, has been decreased
		rf.nextIndex[peer] = reply.NextIndex
	}

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type InstallSnapshotReply struct {
	Term int
}

// install snapshot
func (rf *Raft) OrderInstallSnapshot(i int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.logs.LastSnapshotIndex,
		rf.logs.LastSnapshotTerm,
		rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	go func(server int, args InstallSnapshotArgs) {
		reply := InstallSnapshotReply{}
		resp := rf.sendInstallSnapshot(server, &args, &reply)
		if resp {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != LEADER {
				return
			}
			if reply.Term != rf.currentTerm || reply.Term != args.Term {
				if reply.Term > rf.currentTerm {
					rf.votedFor = NOT_VOTE
					rf.state = FOLLOWER
					rf.currentTerm = args.Term
					rf.heartbeatChan <- true
					rf.persist()
				}
				return
			}
			rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
			rf.nextIndex[server] = max(args.LastIncludedIndex+1, rf.nextIndex[server])
			//go rf.performCommit()
			//rf.mu.Unlock()
		}
	}(i, args)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		DPrintf("[%d] Rej Snapshot from [Term]: %d\n", rf.me, args.Term)
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term && rf.state != FOLLOWER {
		rf.votedFor = NOT_VOTE
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.heartbeatChan <- true
		rf.persist()
	}
	DPrintf("[%d] Installing Snapshot from [Term]: %d [lastincluded]: I:%d T:%d ]\n",
		rf.me, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = args.Term

	if rf.logs.LastSnapshotIndex < args.LastIncludedIndex {
		if args.LastIncludedIndex-rf.logs.LastSnapshotIndex < len(rf.logs.Lgs) {
			rf.logs.dropBefore(args.LastIncludedIndex)
		} else {
			rf.logs.Lgs = []Log{{args.LastIncludedTerm, args.LastIncludedIndex, 0}}
			rf.logs.LastSnapshotIndex = args.LastIncludedIndex
			rf.logs.LastSnapshotTerm = args.LastIncludedTerm
		}
		prevCommitIndex := rf.commitIndex
		// mind the order here. otherwise the log will be lost
		rf.persister.SaveStateAndSnapshot(rf.makeRaftPersistData(), args.SnapshotData)
		rf.commitIndex = max(rf.commitIndex, rf.logs.LastSnapshotIndex)
		if prevCommitIndex < rf.commitIndex {
			rf.commitChan <- true
		}

	}
	rf.persist()
	rf.heartbeatChan <- true
	// don't need notify kvserver to physically install that snapshot, but why?

	DPrintf("[%d] Installing Snapshot Update State Finished [Commited I]: %d [Last Applied I]: %d\n", rf.me, rf.commitIndex, rf.lastApplied)

}

func (rf *Raft) discardLog() {
	rf.logs.Lgs = make([]Log, 0)
}

func (rf *Raft) Restore(stIndex int) {
	rf.mu.Lock()
	if rf.logs.LastSnapshotIndex >= stIndex {
		rf.readSnapshot(rf.persister.ReadSnapshot())
		stIndex = rf.logs.LastSnapshotIndex + 1
		rf.lastApplied = max(rf.lastApplied, rf.logs.LastSnapshotIndex)
	}
	entries := rf.logs.slice(stIndex, rf.lastApplied+1)
	rf.mu.Unlock()
	for _, op := range entries {
		rf.applyChan <- ApplyMsg{
			true,
			op.Command,
			op.Index,
			op.Term,
			make([]byte, 0),
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	//DPrintf("[%d][Append Log]\n", rf.me)
	if isLeader {
		//DPrintf("[%d][Append Log] I: %d Command: %v\n", rf.me, index, command)
		//index = len(rf.logs)
		index = rf.getNextIndex()
		//rf.logs = append(rf.logs, Log{term, command})
		if index-rf.logs.LastSnapshotIndex < len(rf.logs.Lgs) {
			rf.logs.Lgs[index-rf.logs.LastSnapshotIndex] = Log{term, index, command}
		} else {
			rf.logs.appendSingle(Log{term, index, command})
		}
		rf.persist()
		go rf.heartbeat()
		//DPrintf("[%d][Append Log] L: %d Command: %v\n", rf.me, len(rf.logs), command)
		DPrintf("[%d][Append Log] I: %d Command: %v\n", rf.me, index, command)
	}
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//DPrintf("[Killing] %d\n", rf.me)
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//rf.killed = true
	rf.killChan <- true
	//DPrintf("%d cur state %d\n", rf.me, rf.state)

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	//rf.logs = []Log{{Term: 0 }}
	rf.logs = Logs{[]Log{{Term: 0, Index: 0}}, 0, 0, nil}
	rf.votedFor = NOT_VOTE
	rf.state = FOLLOWER

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.heartbeatChan = make(chan bool, 1000)
	rf.electionChan = make(chan bool, 20)
	rf.commitChan = make(chan bool, 1000)
	rf.killChan = make(chan bool, 1000)
	rf.applyChan = applyCh
	rf.killed = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	//TODO: deal with committed index after restart
	//rf.persist()

	go rf.setTimeouts()
	go rf.commitEvent(applyCh)
	DPrintf("[Server Create] %d %d \n", rf.me, rf.currentTerm)
	return rf
}

func (rf *Raft) commitEvent(applyCh chan ApplyMsg) {
	for {
		//rf.mu.Lock()
		//if rf.killed {
		//	rf.mu.Unlock()
		//	return
		//}
		//rf.mu.Unlock()
		select {
		case <-rf.commitChan:
			rf.mu.Lock()
			//entriesToApply := make([]ApplyMsg, 0)
			if rf.lastApplied < rf.logs.LastSnapshotIndex {
				rf.lastApplied = rf.logs.LastSnapshotIndex
				applyCh <- ApplyMsg{
					false,
					-1,
					-1,
					-1,
					rf.persister.ReadSnapshot(),
				}
				rf.persist()
			} else if rf.lastApplied < rf.getLastIndex() && rf.lastApplied < rf.commitIndex {
				DPrintf("[%d][Before Apply][Last Applied: %d][Commit Index: %d][Last Log Index: %d][Last Included Log: %d]\n", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastIndex(), rf.logs.LastSnapshotIndex)
				for i := rf.lastApplied + 1; i <= rf.commitIndex && i <= rf.getLastIndex(); i++ {
					//DPrintf("[%d][Try Commit][Index: %d][CommitIndex: %d]\n", rf.me, i, rf.commitIndex)
					newAppliedMsg := ApplyMsg{
						true,
						//rf.logs[i].Command,
						rf.logs.get(i).Command,
						i,
						rf.logs.get(i).Term,
						make([]byte, 0),
					}
					//DPrintf("[%d][Applied][Index: %d]\n", rf.me, i)
					//entriesToApply = append(entriesToApply, newAppliedMsg)
					rf.lastApplied = i
					applyCh <- newAppliedMsg
				}
				DPrintf("[%d][Apply Finished][Last Applied: %d][Commit Index: %d][Last Log Index: %d][Last Included Log: %d]\n", rf.me, rf.lastApplied, rf.commitIndex, rf.getLastIndex(), rf.logs.LastSnapshotIndex)
			}
			rf.mu.Unlock()
			//for _, msg := range entriesToApply {
			//	applyCh <- msg
			//}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (rf *Raft) setTimeouts() {
	HEARTBEAT_TIMEOUT := time.Duration(160 * time.Millisecond)
	// OP_TIMEOUT := time.Duration(1000*1000*1000)
	for {
		//timeoutflag := rand.Intn(66)
		ELECTION_TIMEOUT := time.Duration((rand.Intn(200) + 200) * 1000 * 1000)
		// DPrintf("fffffff\n")
		rf.mu.Lock()
		curState := rf.state
		//killed := rf.killed
		DPrintf("[%d] State: %d VoteFor: %d\n", rf.me, rf.state, rf.votedFor)
		rf.mu.Unlock()
		//if killed {
		//	return
		//}
		switch curState {
		case FOLLOWER:
			select {
			case <-rf.heartbeatChan:
				// DPrintf("[Recv HB] %d\n", rf.me)
			case <-time.After(ELECTION_TIMEOUT):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
				// rf.startElection() // critical
				DPrintf("[HB Timeout] %d\n", rf.me)
			}
		case CANDIDATE:
			go rf.startElection()
			select {
			case <-rf.heartbeatChan:
				//DPrintf("%d hb Candidate\n", rf.me)
				rf.mu.Lock()
				rf.state = FOLLOWER
				// rf.votedFor = NOT_VOTE
				rf.mu.Unlock()
			case <-rf.electionChan:
				DPrintf("[Win Elect] %d\n", rf.me)
				rf.switchToLeader()
				// randomly sleep
			case <-time.After(ELECTION_TIMEOUT):
				time.Sleep(10 * time.Millisecond)
				// DPrintf("[Ele Timeout] %d\n", rf.me)
				// rf.startElection()
			}
		case LEADER:
			// go rf.heartbeat()
			//time.Sleep(HEARTBEAT_TIMEOUT)
			go rf.heartbeat()
			select {
			case <-time.After(HEARTBEAT_TIMEOUT):
			}
		}
	}
}
