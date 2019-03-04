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
	"fmt"
	//"log"
	//"flag"
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
	Term int
	//Index   int
	Command interface{}
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
	logs        []Log

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
	fmt.Printf("[New Leader] %d - Term %d\n", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.state = LEADER
	// init records for followers
	nextIndex := len(rf.logs)
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.heartbeat()
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
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
		// fmt.Printf("fuck\n")
	}

	//fmt.Printf("[Cur Term in ReqVote] %d Term: %d\n", rf.me, rf.currentTerm)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		fmt.Printf("[Rej Vote - Term] curterm %d incometerm %d\n", rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm < args.Term {
		if rf.state == LEADER {
			fmt.Printf("[Step Down][Req Vote from Higher Term]\n")
		}
		rf.votedFor = NOT_VOTE
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == NOT_VOTE || rf.votedFor == args.CandidateId {
		if rf.getLastTerm() < args.LastLogTerm ||
			(rf.getLastTerm() == args.LastLogTerm &&
				rf.getLastIndex() <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER
			rf.heartbeatChan <- true
			return
		} else {
			fmt.Printf("[Rej Vote] not match\n")
		}
	} else {
		fmt.Printf("[Rej Vote] %d voted for %d\n", rf.me, rf.votedFor)
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
	requestVoteArgs := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastIndex(),
		rf.getLastTerm(),
	}
	fmt.Printf("%d Term %d Hold Election [Peers = %d]\n", rf.me, rf.currentTerm, len(rf.peers))
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
		rf.mu.Unlock()

		go func(server int, requestVoteArgs RequestVoteArgs) {
			requestVoteReply := RequestVoteReply{}
			fmt.Printf("[Send Vote Req to %d] Candidate: %d, Term: %d\n", server, rf.me, requestVoteArgs.Term)
			resp := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			fmt.Printf("[Reqest Vote Status from %d] %t\n", server, resp)
			if resp {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// if state changed, abort
				fmt.Printf("[Request Vote Reply Info] Peer: %d, Term: %d, Granted: %t\n", server, requestVoteReply.Term, requestVoteReply.VoteGranted)
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
					return
				}
				if requestVoteReply.VoteGranted {
					rf.recvVoteNum++
					fmt.Printf("[%d Recv Vote from %d] Term: %d, Recv Vote Num: %d\n", rf.me, server, requestVoteReply.Term, rf.recvVoteNum)
					if rf.recvVoteNum > (len(rf.peers)-1)/2 {
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
		fmt.Printf("[Rej Append from %d] term fall back\n", args.LeaderID)
		//rf.mu.Unlock()
		return
	}

	// follower
	// reset heartbeat
	rf.heartbeatChan <- true
	if rf.currentTerm < args.Term{
		//rf.switchToFollower(args.Term)
		if rf.currentTerm < args.Term {
			fmt.Printf("[Update Term] %d{s%d} Term %d -> %d\n", rf.me, rf.state, rf.currentTerm, args.Term)
		}
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = NOT_VOTE
		rf.heartbeatChan <- true
	}

	// incoming index can't fill the hole
	if rf.getLastIndex() < args.PrevLogIndex {
		fmt.Printf("[%d][Rej Append from %d] index ahead %d <- %d\n",
			rf.me, args.LeaderID, rf.getLastIndex(), args.PrevLogIndex)
		//rf.mu.Unlock()
		return
	}
	// check term match
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex - 1; i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term; i-- {
			reply.NextIndex = i + 1
		}
		//term not match
		fmt.Printf("[Rej Append from %d] term not match\n", args.LeaderID)
		//rf.mu.Unlock()
		return
	}

	// copy
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)

	// leader push commit
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(rf.getLastIndex(), args.LeaderCommit)
	}
	reply.Success = true
	// fmt.Printf("[%d][Copy Log Suc] prevlogindex %d afterappendlength %d \n",
	// 	rf.me, args.PrevLogIndex, len(rf.logs))
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
	fmt.Printf("[%d][Perform Commit]\n", rf.me)
	willCommitted := 0
	for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
		agreeNum := 0
		for j := range rf.peers {
			// check match
			indexMatch := rf.matchIndex[j] >= i
			// a leader is only allowed to commit logs from current term
			termMatch := rf.logs[i].Term == rf.currentTerm
			if indexMatch && termMatch {
				agreeNum++
			}
			if agreeNum > (len(rf.peers)-1)/2 {
				willCommitted++
			}
		}
	}
	if willCommitted > 0 {
		fmt.Printf("[%d][%d Commited]\n", rf.me, willCommitted)
		rf.commitChan <- true
		rf.commitIndex += willCommitted
	}

}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		fmt.Printf("[%d][Still sending hb]\n", rf.me)
		return
	} else {
		// fmt.Printf("[%d][Sending hb]\n", rf.me)
	}
	rf.performCommit()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		// mind rf.nextIndex[i] - 1 here
		prevLogIndex := min(rf.getLastIndex(), rf.nextIndex[i]-1)
		prevLogTerm := rf.logs[prevLogIndex].Term
		entriesArgs := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			prevLogTerm,
			make([]Log, len(rf.logs[prevLogIndex+1:])),
			rf.commitIndex,
		}
		copy(entriesArgs.Entries, rf.logs[prevLogIndex+1:])
		rf.mu.Unlock()
		go func(server int, entriesArgs AppendEntriesArgs) {
			//fmt.Printf("[Send HB from %d %d] length %d -> %d | r%d\n",
			//	rf.me, rf.state, len(entriesArgs.Entries), server, rand.Intn(7))
			reply := AppendEntriesReply{}
			resp := rf.sendAppendEntries(server, &entriesArgs, &reply)
			if resp {
				rf.updatePeerState(server, len(entriesArgs.Entries), &reply)
			}
		}(i, entriesArgs)
	}
}

func (rf *Raft) updatePeerState(peer int, nEntries int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		//rf.mu.Unlock()
		return
	}

	//term fall back
	//switch to and initialize as follower
	if rf.currentTerm < reply.Term {
		//rf.switchToFollower(reply.Term)
		fmt.Printf("[Step Down] %d Term: %d -> %d\n", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = NOT_VOTE
		//rf.heartbeatChan <- true
		//rf.mu.Unlock()
		return
	}

	//update index
	if reply.Success && nEntries > 0 {
		// not void hb
		rf.nextIndex[peer] = reply.NextIndex
		rf.matchIndex[peer] = reply.NextIndex - 1
	} else if !reply.Success {
		// not match, has been decreased
		rf.nextIndex[peer] = reply.NextIndex
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
	if isLeader {
		index = len(rf.logs)
		rf.logs = append(rf.logs, Log{term, command})
		fmt.Printf("[%d][Append Log] L: %d \n", rf.me, len(rf.logs))
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
	fmt.Printf("[Killing] %d\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
	rf.killChan <- true
	//fmt.Printf("%d cur state %d\n", rf.me, rf.state)

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
	//rf.currentTerm = 0
	rf.logs = []Log{{Term: 0}}
	rf.votedFor = NOT_VOTE
	rf.state = FOLLOWER

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeatChan = make(chan bool, 100)
	rf.electionChan = make(chan bool, 100)
	rf.commitChan = make(chan bool, 100)
	rf.killChan = make(chan bool, 100)
	rf.killed = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.setTimeouts()
	fmt.Printf("[Server Create] %d %d \n", rf.me, rf.currentTerm)
	return rf
}

func (rf *Raft) setTimeouts() {
	HEARTBEAT_TIMEOUT := time.Duration(150 * 1000 * 1000)
	// OP_TIMEOUT := time.Duration(1000*1000*1000)
	for {
		//timeoutflag := rand.Intn(66)
		ELECTION_TIMEOUT := time.Duration((rand.Intn(250) + 200) * 1000 * 1000)
		// fmt.Printf("fffffff\n")
		rf.mu.Lock()
		curState := rf.state
		// fmt.Printf("[%d] State: %d VoteFor: %d\n", rf.me, rf.state, rf.votedFor)
		rf.mu.Unlock()
		if rf.killed {
			return
		}
		// if curState == -1 {
		// 	return
		// }
		switch curState {
		case FOLLOWER:
			select {
			case <-rf.heartbeatChan:
				// fmt.Printf("[Recv HB] %d\n", rf.me)
			case <-time.After(ELECTION_TIMEOUT):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
				// rf.startElection() // critical
				fmt.Printf("[HB Timeout] %d\n", rf.me)
			// case <-time.After(time.)
			// case <-rf.killChan:
			// 	fmt.Printf("[Recv Kill %d]\n", rf.me)
			// 	return
				//default:
				//	continue
			}
		case CANDIDATE:
			rf.startElection()
			select {
			case <-rf.heartbeatChan:
				fmt.Printf("%d hb Candidate\n", rf.me)
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.votedFor = NOT_VOTE
				rf.mu.Unlock()
			case <-rf.electionChan:
				fmt.Printf("[Win Elect] %d\n", rf.me)
				rf.switchToLeader()
				// randomly sleep
			case <-time.After(ELECTION_TIMEOUT):
				fmt.Printf("[Ele Timeout] %d\n", rf.me)
				// rf.startElection()
			// case <-rf.killChan:
			// 	fmt.Printf("[Recv Kill %d]\n", rf.me)
			// 	return
			// 	//default:
				//	time.Sleep(ELECTION_TIMEOUT)
				//	fmt.Printf("[Ele Timeout] %d\n", rf.me)
				//	rf.startElection()
			}
		case LEADER:
			rf.heartbeat()
			time.Sleep(HEARTBEAT_TIMEOUT)
			// select {
			// case <-rf.killChan:
			// 	fmt.Printf("[Recv Kill %d]\n", rf.me)
			// 	return
			// case <-rf.heartbeatChan:
			// 	fmt.Printf("[Leader Recv HB]\n")
			// 	rf.mu.Lock()
			// 	rf.state = FOLLOWER
			// 	rf.votedFor = NOT_VOTE
			// 	rf.mu.Unlock()
			// case <-time.After(HEARTBEAT_TIMEOUT):
			// 	rf.heartbeat()
			// 	//default:
			// 	//	continue
			// }
			//default:
			//	select {
			//	case <-rf.killChan:
			//		fmt.Printf("[Recv Kill %d]\n", rf.me)
			//		return
			//	default:
			//		return
			//	}

		}
	}
}
