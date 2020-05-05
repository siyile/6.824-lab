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

	applyCh chan ApplyMsg

	// persistent state
	currentTerm int
	voteFor     int
	log         []entry

	// volatile state
	commitIndex int
	lastApplied int

	// custom variable
	status             int
	electionTimeout    time.Time
	lastCommitToServer int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// helper variable
	voteCount int
	majority  int
}

type entry struct {
	Index   int
	Term    int
	Command interface{}
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

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.status = follower
		DPrintf("%d change my current term from %d to %d", rf.me, rf.currentTerm, args.Term)
	}

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		DPrintf("%d vote NO to %d, my term %d is newer than you %d", rf.me, args.CandidateID, rf.currentTerm, args.Term)
	} else if rf.voteFor == -1 || args.CandidateID == rf.voteFor || args.Term > rf.currentTerm {
		lastLog := rf.log[len(rf.log)-1]
		// compare last log
		if args.LastLogTerm != lastLog.Term { // if different term
			reply.VoteGranted = lastLog.Term < args.LastLogTerm
		} else {
			reply.VoteGranted = lastLog.Index <= args.LastLogIndex
		}

		if reply.VoteGranted {
			rf.voteFor = args.CandidateID
			rf.resetElectionTimeout()
		}

		// debug info
		var voteGranted string
		if reply.VoteGranted {
			voteGranted = "Yes"
		} else {
			voteGranted = "No"
		}
		DPrintf("%d vote %s to %d, result from compare last log. my last log [%d] %d, u [%d] %d", rf.me, voteGranted, args.CandidateID, lastLog.Index, lastLog.Term, args.LastLogIndex, args.LastLogTerm)
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
	Entries      []entry
	LeaderCommit int
}

const logInconsistency = 1

// AppendEntriesReply is reply for append entry
type AppendEntriesReply struct {
	Term    int
	Success bool
	Reason  int
}

// AppendEntries is append entry RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d received append entry from %d", rf.me, args.LeaderID)

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		DPrintf("%d change my current term from %d to %d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.status = follower
	}

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%d my term: %d is newer than %d you term %d, False returned", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		reply.Success = false
		return
	}

	if args.LeaderID == rf.me {
		return
	}

	isHeartBeat := false
	if args.Entries == nil {
		isHeartBeat = true
	}
	rf.resetElectionTimeout()

	if !isHeartBeat {
		DPrintf("%d processing append entries... \n%d OLD: %s", rf.me, rf.me, rf.getLogString(rf.log))
		DPrintf("%d Incoming Entries from %d: %s", rf.me, args.LeaderID, rf.getLogString(args.Entries))
	}

	// reply false if log doesn't contain an entry at prevLogIndex
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(rf.log)-1 < args.PrevLogIndex {
			DPrintf("%d my largest index %d is smaller than %d you index %d", rf.me, len(rf.log)-1, args.LeaderID, args.PrevLogIndex)
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("%d my index%d's term %d is not match %d u %d term", rf.me, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.LeaderID, args.PrevLogTerm)
		} else {
			DPrintf("%d error!!!!! %d", rf.me, args.LeaderID)
		}
		reply.Reason = logInconsistency
		reply.Success = false
		return
	}

	// if an existing entry conflicts with new one, delete all after it
	// add entry not in the log
	for i, j := args.PrevLogIndex+1, 0; i <= len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		if i == len(rf.log) || rf.log[i].Term != args.Entries[j].Term {
			rf.log = rf.log[:i]
			rf.log = append(rf.log, args.Entries...)
			break
		}
	}

	var end int
	if isHeartBeat {
		end = rf.log[len(rf.log)-1].Index
	} else {
		lastNewEntryIndex := args.Entries[len(args.Entries)-1].Index
		if args.LeaderCommit > lastNewEntryIndex {
			end = lastNewEntryIndex
		} else {
			end = args.LeaderCommit
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= end; i++ {
			rf.commitIndex = i
			DPrintf("%d commit entry index %d", rf.me, rf.commitIndex)
		}
	}

	if !isHeartBeat {
		DPrintf("%d processed...\n%d NEW: %s", rf.me, rf.me, rf.getLogString(rf.log))
	}

	DPrintf("%d success returned to %d", rf.me, args.LeaderID)

	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.Entries == nil {
		DPrintf("%d send append entry (heart beat) to %d", args.LeaderID, server)
	} else {
		DPrintf("%d send append entry to %d", args.LeaderID, server)
	}

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
	//isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != leader {
		return index, term, false
	}

	index = len(rf.log)
	term = rf.currentTerm

	rf.log = append(rf.log, entry{
		Index:   index,
		Term:    term,
		Command: command,
	})

	DPrintf("%d received new entry from client\n%d CUR: %s", rf.me, rf.me, rf.getLogString(rf.log))

	return index, term, true
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
	rf.log = append(rf.log, entry{Index: 0, Term: 0, Command: nil})
	rf.status = follower
	rf.voteFor = -1
	rf.resetElectionTimeout()
	rf.majority = (len(rf.peers) + 1) / 2
	rf.applyCh = applyCh
	rf.lastCommitToServer = 0

	// go go go daemon process
	go rf.daemon()
	go rf.checkCommit()
	go rf.commitToServer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

const checkInterval = 125
const randomTimeout = 400
const heartBeatInterval = 125

func (rf *Raft) daemon() {
	for {
		if rf.killed() {
			// DPrintf("%d now killed", rf.me)
			return
		}
		rf.mu.Lock()
		if rf.status == follower {
			go rf.checkElection()
		} else if rf.status == leader {
			go rf.syncClock()
			go rf.commit()
		} else if rf.status == candidate { // candidate
			go rf.checkElection()
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatInterval * time.Millisecond)
	}
}

// all server
func (rf *Raft) checkCommit() {
	for {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) commitToServer() {
	for {
		for i := rf.lastCommitToServer + 1; i <= rf.commitIndex; i++ {
			DPrintf("%d commit index %d to server", rf.me, i)
			// send to tester
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
			rf.lastCommitToServer = i
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// leader
// sync clock is also heartbeat
func (rf *Raft) syncClock() {
	rf.mu.Lock()
	var wg sync.WaitGroup

	DPrintf("%d sync clock started", rf.me)
	DPrintf("%d idx %s", rf.me, rf.getNextIndexString())

	for i := range rf.peers {
		if rf.killed() {
			// DPrintf("%d now killed", rf.me)
			return
		}
		if rf.me == i {
			continue
		}

		wg.Add(1)

		// var lastLog entry

		// if rf.log[len(rf.log)-1].Index >= rf.nextIndex[i] {
		// 	lastLog = rf.log[rf.nextIndex[i]-1]
		// } else {
		// 	lastLog = rf.log[len(rf.log)-1]
		// }

		// argCopy := AppendEntriesArgs{
		// 	Term:         rf.currentTerm,
		// 	LeaderID:     rf.me,
		// 	PrevLogIndex: lastLog.Index,
		// 	PrevLogTerm:  lastLog.Term,
		// 	Entries:      nil,
		// 	LeaderCommit: rf.commitIndex,
		// }

		go func(peer int) {
			defer wg.Done()
			rf.mu.Lock()

			var lastLog entry

			if rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer] {
				lastLog = rf.log[rf.nextIndex[peer]-1]
			} else {
				lastLog = rf.log[len(rf.log)-1]
			}

			nextIndex := rf.nextIndex[peer]

			arg := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}

			// if !reflect.DeepEqual(arg, argCopy) {
			// 	rf.mu.Unlock()
			// 	return
			// }

			isHeartBeat := false
			if rf.log[len(rf.log)-1].Index < rf.nextIndex[peer] { // we need send heartbeat
				isHeartBeat = true
			}

			if !isHeartBeat {
				arg.Entries = append([]entry(nil), rf.log[rf.nextIndex[peer]:]...)
			}

			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			rf.sendAppendEntries(peer, &arg, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.killed() {
				// DPrintf("%d now killed", rf.me)
				return
			}

			// check condition is the same
			if rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer] {
				lastLog = rf.log[rf.nextIndex[peer]-1]
			} else {
				lastLog = rf.log[len(rf.log)-1]
			}

			argLater := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}

			entryBackup := arg.Entries
			arg.Entries = nil

			if !reflect.DeepEqual(argLater, arg) || rf.nextIndex[peer] != nextIndex || rf.status != leader {
				// DPrintf("%d condition %d changed! return!", rf.me, peer)
				return
			}

			// all rpc should do
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = follower
				return
			}

			arg.Entries = entryBackup

			if !isHeartBeat {
				if reply.Success == true {
					rf.matchIndex[peer] = arg.Entries[len(arg.Entries)-1].Index
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				}
			}

			if !reply.Success && reply.Reason == logInconsistency {
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
					// DPrintf("%d decerase %d to %d", rf.me, peer, rf.nextIndex[peer])
				}
			}
		}(i)
	}
	rf.mu.Unlock()
	wg.Wait()
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	for i := 0; i < 5; i++ {
		for n := rf.commitIndex + 1; n < len(rf.log); n++ {
			if rf.killed() {
				// DPrintf("%d now killed", rf.me)
				return
			}

			cnt := 0
			for i := range rf.peers {
				if rf.me == i {
					cnt++
					continue
				}
				if rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt >= rf.majority && rf.log[n].Term == rf.currentTerm {
				rf.commitIndex = n
			}
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatInterval / 5 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

// follower
func (rf *Raft) kickOffElection() {
	if rf.status != candidate {
		DPrintf("Error!! Kicking off election but state is not Candidate.")
	}

	// increase current term
	rf.currentTerm++
	rf.voteFor = -1

	DPrintf("%d Kickoff Election at term %d", rf.me, rf.currentTerm)

	// vote for self
	log := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: log.Index, LastLogTerm: log.Term}

	// assumption for vote
	var argsCopy RequestVoteArgs
	copier.Copy(&argsCopy, &args)
	voteCount := rf.majority

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
				// all rpc should do
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = follower
					rf.resetElectionTimeout()
					return
				}

				if reply.VoteGranted {
					voteCount--
				}
			} else {
				rf.mu.Unlock()
				return
			}

			if voteCount == 0 {
				rf.status = leader

				// init nextIndex & matchIndex
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				}
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.matchIndex {
					rf.matchIndex[i] = 0
				}

				DPrintf("%d now is Leader", rf.me)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	rf.mu.Lock()
}

func (rf *Raft) checkElection() {
	rf.mu.Lock()
	now := time.Now()
	if now.After(rf.electionTimeout) {
		rf.status = candidate
		rf.kickOffElection()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(rand.Int63n(randomTimeout/2)+randomTimeout) * time.Millisecond)
}

func (rf *Raft) getLogString(entries []entry) string {
	logString := ""
	for _, log := range entries {
		logString += fmt.Sprintf("[%d] %d | ", log.Index, log.Term)
	}
	logString += "\n"
	return logString
}

func (rf *Raft) getNextIndexString() string {
	indexString := ""
	for host, index := range rf.nextIndex {
		indexString += fmt.Sprintf("[%d] %d | ", host, index)
	}
	// indexString += "\n"
	return indexString
}
