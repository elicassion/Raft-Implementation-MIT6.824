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
	"flag"
	"sync"
	"time"
)
import "labrpc"
import (
	"math/rand"
)

// import "bytes"
// import "labgob"

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
}

//
// A Go object implementing a single Raft peer.
//

type Log struct {
	term    int
	index   int
	command interface{}
}

type State int

const (
	FOLLWER State = iota
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
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	heartbeatChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

func (rf *Raft) switchToFollower(term int) {
	rf.state = FOLLWER
	rf.currentTerm = term
	rf.votedFor = NOT_VOTE
}

func (rf *Raft) switchToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// init records for followers
	nextIndex = len(rf.logs)
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	rf.heartbeat()
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
const NOT_VOTE = -1

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.voteGranted = false

	if args.term < rf.currentTerm {
		reply.term = rf.currentTerm
		return
	}
	if args.term > rf.currentTerm {
		rf.votedFor = NOT_VOTE
		rf.state = FOLLWER
		rf.currentTerm = args.term
	}

	if rf.votedFor == NOT_VOTE || rf.votedFor == args.candidateId {
		if args.lastLogTerm > rf.getLastTerm() ||
			(args.lastLogTerm == rf.getLastTerm() && args.lastLogIndex >= rf.getLastIndex()) {
			reply.voteGranted = true
			reply.term = rf.currentTerm
			rf.votedFor = args.candidateId
			rf.state = FOLLWER
			return
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	term         int
	leaderID     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Log
	leaderCommit int
}

type AppendEntriesReply struct {
	term      int
	nextIndex int
	success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.success = false

	//im a leader, reject
	if rf.currentTerm > args.term {
		// send updated term
		reply.term = rf.currentTerm
		return
	}

	// follower
	// reset heartbeat
	rf.heartbeatChan <- true

	if rf.currentTerm < args.term {
		rf.switchToFollower(args.term)
	}

	// incoming index can't fill the hole
	if rf.getLastIndex() < args.prevLogIndex {
		return
	}
	// check term match
	termNotMatch := false
	for i := args.prevLogIndex; rf.logs[i].term != args.prevLogTerm; i-- {
		reply.nextIndex = i + 1
		if !termNotMatch {
			termNotMatch = true
		}
	}
	if termNotMatch {
		return
	}

	// copy
	rf.logs = append(rf.logs[:args.prevLogIndex+1], args.entries...)

	// leader push commit
	if rf.commitIndex < args.leaderCommit {
		rf.commitIndex = min(rf.getLastIndex(), args.leaderCommit)
	}
	reply.success = true
	return
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

func (rf *Raft) heartbeat() {
	if rf.state != LEADER {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prevLogIndex := min(rf.getLastIndex(), rf.nextIndex[i])
		prevLogTerm := rf.logs[prevLogIndex].term
		entriesArgs := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			prevLogTerm,
			make([]Log, len(rf.Logs[prevLogIndex+1:])),
			rf.commitIndex,
		}
		copy(args.entries, rf.logs[prevLogIndex+1:])
		go func(i int, entriesArgs AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			resp := rf.sendAppendEntries(i, &entriesArgs, reply)
			if resp {
				rf.updatePeerState(i, len(entriesArgs.entries), reply)
			}
		}(i, entriesArgs)
	}
}

func (rf *Raft) updatePeerState(peer int, nEntries int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	//term fall back
	//switch to and initialize as follower
	if rf.currentTerm < reply.term {
		rf.switchToFollower(reply.term)
		return
	}

	//update index
	if reply.success && nEntries > 0 {
		rf.nextIndex[peer] = reply.nextIndex
		rf.matchIndex[peer] = reply.nextIndex - 1
	} else if !reply.success {
		// not match, has been decreased
		rf.nextIndex[peer] = reply.nextIndex
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
	index := -1
	term := -1
	isLeader := true

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
	rf.logs = []Log{{term: 0}}
	rf.votedFor = NOT_VOTE
	rf.state = FOLLWER

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeatChan = make(chan bool)
	rf.electionChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.setElectTimeOut()

	return rf
}

func (rf *Raft) setElectTimeOut() {
	mu.Lock()
	defer mu.Unlock()
	time.Duration timeout = 500
	switch rf.state {
	case FOLLWER:
		go func() {
			time.Sleep(timeout)
			mu.Lock()
			switchToCandidate()
			mu.Unlock()
		}
		select {
		case <-rf.heartbeatChan:
		case <-rf.timeoutChan:
			rf.state = CANDIDATE
		}
	case CANDIDATE:
		rf.startElection()
		select {
		case <-timeoutChan:
		case <-rf.heartbeatChan:
		case <-rf.electionChan:
			rf.switchToLeader()
		}
	}
	time.Sleep(ELECTTIMEOUT)
}
