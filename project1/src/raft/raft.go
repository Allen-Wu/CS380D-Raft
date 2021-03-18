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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

// Some basic config
// Heartbeat period = 150ms (at least 100ms)
// Election timeout period = 500ms ~ 750ms (need to elect new leader within 5s)
const (
	electionUpperBound = 750
	electionLowerBound = 500
	heartBeatRate      = 150
)

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

// Raft peer status of role
const (
	statusFollower  = "Follower"
	statusCandidate = "Candidate"
	statusLeader    = "Leader"
)

// A log entry object
// NOTE: The field name is captilized for RPC purpose
type LogEntry struct {
	Command interface{} // Clients' command to execute
	Term    int         // Term of log entry
}

// Entry sent over RPC by leader
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // Leader's ID
	PrevLogIndex int        // TODO
	PrevLogTerm  int        // TODO
	Entries      []LogEntry // Log entries to store
	LeaderCommit int        // TODO
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	currentTerm       int           // Current term recorded by each peer
	votedFor          int           // The idx for voted peer in voting stage. <0 means null
	roleState         string        // Follower / Candidate / Leader
	applyCh           chan ApplyMsg // Channel to apply the command to state machine
	lastRecvTimeStamp int64         // The timestamp when receiving the latest AppendEntry RPC or RequestVote RPC
	lastSendTimeStamp int64         // The timestamp when sending the message as a leader
	logs              []LogEntry    // Stored logs in the server
	electionTimeout   int           // Randomly selected timeout in ms
}

// Reset election timeout by randomly selecting from 500ms ~ 750ms
func (rf *Raft) ResetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = electionLowerBound + rand.Intn(electionUpperBound-electionLowerBound)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// TODO: Check if need lock here
	term = rf.currentTerm
	isleader = rf.roleState == statusLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	// TODO: Check if need lock here
	// Persistently store currentTerm, VoteForID, Logs
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// TODO: Check if need lock here
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var oldTerm int
	var oldVoteFor int
	var oldLogs []LogEntry
	if d.Decode(&oldTerm) != nil ||
		d.Decode(&oldVoteFor) != nil ||
		d.Decode(&oldLogs) != nil {
		// Search for better way of handling errors
		log.Fatal("Error in restoring persistent states")
	} else {
		rf.currentTerm = oldTerm
		rf.votedFor = oldVoteFor
		rf.logs = oldLogs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Responder's term
	VoteGranted bool // If agree to vote the candidate
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// func (rf *Raft) RequestVote() {
	// Your code here (2A, 2B).
	// TODO: Check if need lock here

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term >= rf.currentTerm && (rf.votedFor < 0 || rf.votedFor == args.CandidateId) {
		// Haven't vote yet or have voted for this server before
		// TODO: Check if the up-to-date comparison here is correct or not
		currLastLogTerm := 0
		if len(rf.logs) > 0 {
			currLastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		currLastLogIdx := len(rf.logs)
		ok := false
		if currLastLogTerm < args.LastLogTerm {
			ok = true
		} else if currLastLogTerm == args.LastLogTerm {
			ok = currLastLogIdx <= args.LastLogIndex
		} else {
			ok = false
		}
		if ok {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: Complete all the stuff here
	rf.lastRecvTimeStamp = int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
	rf.roleState = statusFollower
	reply.Term = rf.currentTerm
	reply.Success = false
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
	// TODO: Check if need lock here
	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := (rf.roleState == statusLeader)

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
// Goroutine for monitoring election timeout
// If timeout elapses in follower or candidate status,
// Start another election
// **No Lock Required**
//
func (rf *Raft) electionTimeoutMonitor() {
	// Periodically check per 20 msec
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.roleState == statusFollower ||
			rf.roleState == statusCandidate {
			timeNow := int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
			timeDiff := (timeNow - rf.lastRecvTimeStamp)
			// TODO: Replace hardcode with RaftElectionTimeout
			if timeDiff >= int64(rf.electionTimeout) {
				// Start election
				// Send out RequestVoteRPCs to all other peers
				fmt.Print("server ", strconv.Itoa(rf.me), " starts election.\n")
				lastLogTerm := 0
				if len(rf.logs) > 0 {
					lastLogTerm = rf.logs[len(rf.logs)-1].Term
				}
				voteRequest := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logs), lastLogTerm}

				// Vote for oneself first
				voteCnt := 1
				rf.votedFor = rf.me
				// Increment currentTerm
				rf.currentTerm += 1

				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						voteReply := RequestVoteReply{0, false}
						ok := rf.sendRequestVote(i, &voteRequest, &voteReply)
						if ok && voteReply.VoteGranted {
							voteCnt += 1
						}
					}
				}

				// 7 -> >3
				// 8 -> >4
				majorityNum := len(rf.peers) / 2
				if voteCnt > majorityNum {
					// Win the election
					rf.roleState = statusLeader
					fmt.Print("server ", strconv.Itoa(rf.me), " WINs the election.\n")
				}
				// Send the message
				rf.lastSendTimeStamp = int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
				appendRequest := AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, make([]LogEntry, 0), 0}
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						appendReply := AppendEntriesReply{0, false}
						rf.sendAppendEntries(i, &appendRequest, &appendReply)
					}
				}
			}
		} else {
			// Leader state
			// Send heartbeat or sync appendentry message periodically
			rf.lastSendTimeStamp = int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
			appendRequest := AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, make([]LogEntry, 0), 0}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					appendReply := AppendEntriesReply{0, false}
					rf.sendAppendEntries(i, &appendRequest, &appendReply)
				}
			}
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.roleState = statusFollower
	rf.applyCh = applyCh
	// Reset the timer
	rf.lastRecvTimeStamp = int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
	rf.lastSendTimeStamp = int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Reset election timeout
	rf.ResetElectionTimeout()
	// Kick off the goroutine for monitoring election timeout
	go rf.electionTimeoutMonitor()
	rf.mu.Unlock()
	return rf
}
