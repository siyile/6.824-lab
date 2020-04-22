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
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"

	"github.com/jinzhu/copier"
)

// import "bytes"
// import "../labgob"

const leader = 1
const follower = 2
const candidate = 3

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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	voteFor     int
	log         []Log

	// volatile state
	commitIndex int
	lastApplied int

	// custom variable
	status          int
	electionTimeout time.Time

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// helper variable
	voteCount int
}

type Log struct {
	Index int
	Term  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.status == leader
	rf.mu.Unlock()

	return term, isLeader
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

// RequestVoteArgs is  RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote is  RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d received request vote from %d", rf.me, args.CandidateID)

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		DPrintf("%d vote NO to %d, my term %d is newer than you %d", rf.me, args.CandidateID, rf.currentTerm, args.Term)
	} else if args.CandidateID == rf.voteFor || rf.voteFor == -1 {
		// compare last log
		if args.LastLogTerm != rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = rf.log[len(rf.log)-1].Term == args.LastLogTerm
		} else {
			reply.VoteGranted = len(rf.log) > args.LastLogIndex
		}

		if reply.VoteGranted {
			rf.voteFor = args.CandidateID
		}

		// debug info
		var voteGranted string
		if reply.VoteGranted {
			voteGranted = "Yes"
		} else {
			voteGranted = "No"
		}
		DPrintf("%d vote %s to %d, result from compare last log", rf.me, voteGranted, args.CandidateID)
	} else {
		DPrintf("%d I have already Vote for %d", rf.me, rf.voteFor)
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d send request vote to %d", args.CandidateID, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs is request args for append entry
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesReply is reply for append entry
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries is append entry RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d received append entry from %d", rf.me, args.LeaderID)

	if args.LeaderID == rf.me {
		return
	}

	reply.Term = rf.currentTerm

	// if is heartbeat, reset timeout
	if args.Entries == nil {
		if rf.currentTerm <= args.Term {
			if rf.currentTerm < args.Term {
				DPrintf("%d change my current term from %d to %d", rf.me, rf.currentTerm, args.Term)
			}
			rf.currentTerm = args.Term
			rf.status = follower
			rf.resetElectionTimeout()
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d send append entry to %d", args.LeaderID, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	DPrintf(time.Now().String())

	rf.currentTerm = 0
	rf.log = append(rf.log, Log{Index: 0, Term: 0})
	rf.status = follower
	rf.voteFor = -1
	rf.resetElectionTimeout()

	// go go go daemon process
	go rf.daemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

const checkInterval = 50
const heartBeatInterval = 100
const randomTimeout = 500

func (rf *Raft) daemon() {
	for {
		rf.mu.Lock()
		if rf.status == follower {
			rf.checkElection()

			rf.mu.Unlock()
			time.Sleep(checkInterval * time.Millisecond)

		} else if rf.status == leader {
			rf.mu.Unlock()
			time.Sleep(heartBeatInterval * time.Millisecond)

			go rf.heartBeat()
		} else { // candidate
			rf.checkElection()

			rf.mu.Unlock()
			time.Sleep(checkInterval * time.Millisecond)
		}
	}
}

// leader
func (rf *Raft) heartBeat() {
	rf.mu.Lock()

	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	var argsCopy AppendEntriesArgs
	copier.Copy(&argsCopy, &arg)

	var wg sync.WaitGroup

	for i := range rf.peers {
		wg.Add(1)
		reply := AppendEntriesReply{}
		go func(peer int) {
			defer wg.Done()
			rf.mu.Lock()
			if !reflect.DeepEqual(argsCopy, arg) {
				return
			}
			rf.mu.Unlock()

			rf.sendAppendEntries(peer, &arg, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reflect.DeepEqual(argsCopy, arg) && reply.Term > rf.currentTerm {
				rf.status = follower
				rf.currentTerm = reply.Term
			}
		}(i)
	}
	rf.mu.Unlock()
	wg.Wait()
}

// follower
func (rf *Raft) kickOffElection() {
	if rf.status != candidate {
		DPrintf("Error!! Kicking off election but state is not Candidate.")
	}

	// increase current term
	rf.currentTerm += 1

	DPrintf("%d Kickoff Election at term %d", rf.me, rf.currentTerm)

	// vote for self
	log := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: log.Index, LastLogTerm: log.Term}

	// assumption for vote
	var argsCopy RequestVoteArgs
	copier.Copy(&argsCopy, &args)
	rf.voteCount = (len(rf.peers) + 1) / 2

	// reset election timer
	rf.resetElectionTimeout()

	// wait group
	var wg sync.WaitGroup

	rf.mu.Unlock()

	// send requestVote to all other servers
	for i := range rf.peers {
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply := RequestVoteReply{}

			rf.mu.Lock()
			if !reflect.DeepEqual(args, argsCopy) {
				return
			}
			rf.mu.Unlock()

			rf.sendRequestVote(peer, &args, &reply)
			// reply logic
			rf.mu.Lock()

			if rf.status == candidate && reflect.DeepEqual(args, argsCopy) {
				rf.voteCount -= 1
			} else {
				rf.mu.Unlock()
				return
			}

			if rf.voteCount <= 0 {
				rf.status = leader
				DPrintf("%d now is Leader", rf.me)
				rf.mu.Unlock()
				rf.heartBeat()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	rf.mu.Lock()
}

func (rf *Raft) checkElection() {
	now := time.Now()
	if now.After(rf.electionTimeout) {
		rf.status = candidate
		rf.kickOffElection()
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(rand.Int63n(randomTimeout)+randomTimeout) * time.Millisecond)
}
