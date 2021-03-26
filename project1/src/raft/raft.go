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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// Some basic config
// Heartbeat period = 150ms (at least 100ms)
// Election timeout period = 500ms ~ 750ms (need to elect new leader within 5s)
const (
	electionUpperBound = 900
	electionLowerBound = 600
	heartBeatPeriod    = 150
)

// Raft peer status of role
const (
	statusFollower  = "Follower"
	statusCandidate = "Candidate"
	statusLeader    = "Leader"
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

// A log entry object
// NOTE: The field name is captilized for RPC purpose
type LogEntry struct {
	Command interface{} // Clients' command to execute
	Term    int         // Term of log entry
}

// AppendEntries sent over RPC by leader
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // Leader's ID
	PrevLogIndex int        // Check if log matches
	PrevLogTerm  int        // Check if log matches
	Entries      []LogEntry // Log entries to store
	LeaderCommit int        // Highest known committed entry
}

// AppendEntries RPC reply by candidate or follower
type AppendEntriesReply struct {
	Term          int
	AckLength     int
	ConflictTerm  int
	ConflictIndex int
	Success       bool
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
	voteCnt           int           // # of received votes from others in this term election as candidate
	lastRecvTimeStamp int64         // The timestamp when receiving the latest AppendEntry RPC or RequestVote RPC
	lastSendTimeStamp int64         // The timestamp when sending the message as a leader
	logs              []LogEntry    // Stored logs in the server
	electionTimeout   int64         // Randomly selected timeout in ms
	applyCh           chan ApplyMsg // Channel to apply the command to state machine
	commitIndex       int           // index of highest known committed log entry
	lastApplied       int           // index of highest known applied log entry
	nextIndex         []int         // index of next log entry to send, use definition in the lecture slides
	matchIndex        []int         // index of highest known log entry that has been replicated
}

// Reselect election timeout by randomly selecting from 500ms ~ 750ms
func (rf *Raft) ReselectElectionTimeout() {
	rf.electionTimeout = int64(electionLowerBound + rand.Intn(electionUpperBound-electionLowerBound))
}

// Helper function of getting system time in ms
func (rf *Raft) getSysTime() int64 {
	return int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
}

// Cancel election timeout by resetting lastRecvTimeStamp to current time
func (rf *Raft) cancelElectionTimeout() {
	rf.lastRecvTimeStamp = rf.getSysTime()
}

// NOTE: MUST be called within critical session
func (rf *Raft) transitToFollower(term int, votedFor int) {
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.roleState = statusFollower
	rf.voteCnt = 0
	// Reselect a election timeout duration
	// rf.ReselectElectionTimeout()
	rf.persist()
}

// NOTE: Must be called within critical session
func (rf *Raft) initNextMatchIndex() {
	// nextIndex -> off by 1 w.r.t. the original definition
	newNextIdx := len(rf.logs)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = newNextIdx
	}
	// matchIndex -> 0
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.roleState == statusLeader
	// rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	// Persistently store currentTerm, VoteForID, Logs
	// fmt.Print("Save state with term", strconv.Itoa(rf.currentTerm), "\n")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.commitIndex) != nil ||
		e.Encode(rf.lastApplied) != nil {
		log.Fatal("failed to encode raft persistent state")
	}
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
	var oldCommitIndex int
	var oldLastApplied int
	if d.Decode(&oldTerm) != nil ||
		d.Decode(&oldVoteFor) != nil ||
		d.Decode(&oldLogs) != nil ||
		d.Decode(&oldCommitIndex) != nil ||
		d.Decode(&oldLastApplied) != nil {
		// Search for better way of handling errors
		log.Fatal("Error in restoring persistent states")
	} else {
		// fmt.Print("Recover with old term", strconv.Itoa(oldTerm), "\n")
		rf.currentTerm = oldTerm
		rf.votedFor = oldVoteFor
		rf.logs = oldLogs
		rf.commitIndex = oldCommitIndex
		rf.lastApplied = oldLastApplied
		// fmt.Print("Server ", strconv.Itoa(rf.me), " recovers with ", strconv.Itoa(len(rf.logs)), " entries\n")
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
func (rf *Raft) sendRequestVote(
	server int,
	Term int,
	CandidateId int,
	LastLogIndex int,
	LastLogTerm int) {

	voteRequest := RequestVoteArgs{Term, CandidateId, LastLogIndex, LastLogTerm}
	voteReply := RequestVoteReply{0, false}

	// RPC call
	for {
		ok := rf.peers[server].Call("Raft.RequestVote", &voteRequest, &voteReply)
		if ok {
			break
		}
	}

	// NOTE: Need lock here to update voting results
	// Need to check the term matches as well
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == Term {
		// Not out-dated vote response
		if voteReply.VoteGranted &&
			voteReply.Term == Term &&
			rf.roleState == statusCandidate {
			rf.voteCnt += 1
			// fmt.Print("Server ", strconv.Itoa(rf.me), "receives vote from ", strconv.Itoa(server), " with term ", strconv.Itoa(rf.currentTerm), "\n")

		} else if voteReply.Term > rf.currentTerm {
			// If the candidate finds itself outdated
			// change back to follower state
			// fmt.Print("Server ", strconv.Itoa(rf.me), " should reach here\n")
			rf.transitToFollower(voteReply.Term, -1)
		}
	}
	// rf.mu.Unlock()
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Directly reject if the sent term is lower than mine
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// rf.mu.Unlock()
		return
	}

	// Step down as follower if message comes from higher term candidate
	if args.Term > rf.currentTerm {
		// Call rf.persist() in the transition function
		rf.transitToFollower(args.Term, -1)
	}

	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	// Check if candidate's log is up-to-dated
	logOk := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.logs))
	// Check if term is ok
	termOk := (args.Term > rf.currentTerm) ||
		(args.Term == rf.currentTerm && (rf.votedFor < 0 || rf.votedFor == args.CandidateId))

	if logOk && termOk {
		// Step down as a follower
		rf.transitToFollower(args.Term, args.CandidateId)
		// Need to cancel timeout since it decides to vote for others in this term
		rf.ReselectElectionTimeout()
		rf.cancelElectionTimeout()

		// Grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		// Send reject response
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	// rf.mu.Unlock()
}

// Determine if the log entry is committed or not used by leader
// NOTE: MUST be called within a critical session
func (rf *Raft) commitLogEntries() {
	// 7 -> >3
	// 8 -> >4
	majorityNum := len(rf.peers) / 2
	// Find the maximum log entry that has been replicated on majority in this term
	maxIdx := -1
	for i := 1; i <= len(rf.logs); i++ {
		cnt := 0
		// Check each peer's log status
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnt += 1
			}
		}
		if cnt > majorityNum && i > maxIdx {
			maxIdx = i
		}
	}
	if maxIdx > rf.commitIndex && rf.logs[maxIdx-1].Term == rf.currentTerm {
		// Apply to the state machine
		rf.commitIndex = maxIdx
	}
}

// AppendEntries RPC used by leader
func (rf *Raft) sendAppendEntries(
	server int,
	Term int,
	LeaderId int,
	PrevLogIndex int,
	PrevLogTerm int,
	Entries []LogEntry,
	LeaderCommit int) {

	appendRequest := AppendEntriesArgs{Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit}
	appendReply := AppendEntriesReply{0, 0, 0, 0, false}
	// Keep sending the message until it succeeds
	for {
		// rf.lastSendTimeStamp = rf.getSysTime()
		ok := rf.peers[server].Call("Raft.AppendEntries", &appendRequest, &appendReply)
		if ok {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Check the message isn't out-of-dated
	if rf.currentTerm == Term {
		if appendReply.Term == rf.currentTerm && rf.roleState == statusLeader {
			// Check if the message is out-dated or not
			// if appendReply.Success && appendReply.AckLength >= rf.matchIndex[server] {
			// 	rf.nextIndex[server] = appendReply.AckLength
			// 	rf.matchIndex[server] = appendReply.AckLength
			// 	rf.commitLogEntries()
			// } else if rf.nextIndex[server] > 0 {
			// 	// Decrement next index and retry
			// 	rf.nextIndex[server] -= 1
			// 	rf.ReplicateLog(LeaderId, server)
			// }

			// Log backtracking optimizations
			if appendReply.Success && appendReply.AckLength >= rf.matchIndex[server] {
				rf.nextIndex[server] = appendReply.AckLength
				rf.matchIndex[server] = appendReply.AckLength
				rf.commitLogEntries()
			} else {
				rf.nextIndex[server] = appendReply.ConflictIndex
				for i := rf.nextIndex[server]; i >= 0; i-- {
					if rf.logs[i].Term == appendReply.ConflictTerm {
						rf.nextIndex[server] = i + 1
						break
					}
				}
				// rf.ReplicateLog(LeaderId, server)
			}

		} else if appendReply.Term > rf.currentTerm {
			rf.transitToFollower(appendReply.Term, -1)
		}
	}

	// rf.mu.Unlock()
}

// The helper function to perform actual append entries operation
// NOTE: MUST be called within a critical session
func (rf *Raft) AppendEntriesHelper(logLength int, leaderCommit int, entries []LogEntry) {
	// fmt.Print("Reach here", strconv.Itoa(leaderCommit), "\n")
	// fmt.Print(strconv.Itoa(len(rf.logs)), "\n")
	if len(entries) > 0 && len(rf.logs) > logLength {
		k := 0
		for ; (k+logLength) < len(rf.logs) && k < len(entries); k++ {
			if rf.logs[logLength+k].Term != entries[k].Term {
				// Delete out-dated-log entries
				rf.logs = rf.logs[:(k + logLength)]
				break
			}
		}
		entries = entries[k:]
		rf.logs = append(rf.logs, entries...)
	} else if len(entries) > 0 && len(rf.logs) == logLength {
		rf.logs = append(rf.logs, entries...)
	}

	if leaderCommit > rf.commitIndex {
		// Apply the logs to state machine
		lastIdx := len(rf.logs)
		if leaderCommit < lastIdx {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastIdx
		}
	}
}

// AppendEntries RPC handler by candidate or follower
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Directly reject if the sent term is lower than mine
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.AckLength = 0
		reply.Term = rf.currentTerm
		// rf.mu.Unlock()
		return
	}

	// Step down as a follower if message comes from higher term
	if args.Term > rf.currentTerm {
		rf.transitToFollower(args.Term, -1)
	}

	// Log matching checking
	logOk := (len(rf.logs) >= args.PrevLogIndex)

	// Log backtracking optimizations
	// 1: doesn't have prevLogIndex
	if !logOk {
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
	}

	if logOk && (args.PrevLogIndex > 0) {
		// Check if the same index entry has the same term
		logOk = (args.PrevLogTerm == rf.logs[args.PrevLogIndex-1].Term)

		// 2: have prevLogIndex but with different terms
		if !logOk {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-1].Term
			reply.ConflictIndex = 1
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.logs[i-1].Term != reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
	}

	if rf.currentTerm == args.Term && logOk {
		// Matching AppendEntries RPC, append to the log!
		rf.roleState = statusFollower
		rf.AppendEntriesHelper(args.PrevLogIndex, args.LeaderCommit, args.Entries)
		rf.persist()
		// Send back success message
		reply.Success = true
		reply.AckLength = args.PrevLogIndex + len(args.Entries)
		reply.Term = rf.currentTerm
	} else {
		// Either unmatched AppendEntries RPC or out-dated RPC
		// Send back failure message
		reply.Success = false
		reply.AckLength = 0
		reply.Term = rf.currentTerm
	}

	// As long as this message comes from current term learder, reset the timer
	if rf.currentTerm == args.Term {
		rf.ReselectElectionTimeout()
		rf.cancelElectionTimeout()
	}

	// rf.mu.Unlock()
}

// NOTE: MUST be called within critical session
func (rf *Raft) ReplicateLog(leaderId int, followerId int) {
	// next index to send until the end
	var entriesToSend []LogEntry
	for i := rf.nextIndex[followerId]; i < len(rf.logs); i++ {
		entriesToSend = append(entriesToSend, rf.logs[i])
	}
	prevLogTerm := 0
	if rf.nextIndex[followerId] > 0 {
		prevLogTerm = rf.logs[rf.nextIndex[followerId]-1].Term
	}
	// Kick off the goroutine for sending AppendEntries RPC
	go rf.sendAppendEntries(followerId,
		rf.currentTerm,
		leaderId,
		rf.nextIndex[followerId],
		prevLogTerm,
		entriesToSend,
		rf.commitIndex)
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
	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := (rf.roleState == statusLeader)

	// Your code here (2B).
	if isLeader {
		// First, update own log entry
		newLogEntry := LogEntry{command, rf.currentTerm}
		rf.logs = append(rf.logs, newLogEntry)
		rf.matchIndex[rf.me] = len(rf.logs)
		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// Kick off the go routine for replicating the command
				rf.ReplicateLog(rf.me, i)
			}
		}
		// rf.lastSendTimeStamp = rf.getSysTime()
	}

	// rf.mu.Unlock()
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
// Helper function to initiate a new election process
// NOTE: MUST be called within a critical session
//
func (rf *Raft) initiateNewElection() {
	// Initiate the election
	// Use the same timeout for follower & candidate election timeout
	rf.ReselectElectionTimeout()
	// NOTE: Use lastRecvTimeStamp to record the last timestamp of starting election
	rf.cancelElectionTimeout()
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	// Transition to candidate state
	rf.currentTerm += 1
	rf.roleState = statusCandidate
	rf.voteCnt = 1
	rf.votedFor = rf.me
	rf.persist()
	// fmt.Print("Server ", strconv.Itoa(rf.me), " starts election with term ", strconv.Itoa(rf.currentTerm), ".\n")
	// fmt.Print("Log length: ", strconv.Itoa(len(rf.logs)), " Last log term: ", strconv.Itoa(lastLogTerm), " \n")

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// Kick off the goroutine for sending votes to peers
			go rf.sendRequestVote(i, rf.currentTerm, rf.me, len(rf.logs), lastLogTerm)
		}
	}
}

//
// Goroutine for monitoring election timeout
// If timeout elapses in follower or candidate status,
// Start another election
//
func (rf *Raft) electionTimeoutMonitor() {
	// Periodically check per 5 msec
	for !rf.killed() {
		rf.mu.Lock()
		if rf.roleState == statusFollower {
			timeNow := rf.getSysTime()
			timeDiff := (timeNow - rf.lastRecvTimeStamp)
			if timeDiff >= rf.electionTimeout {
				rf.initiateNewElection()
			}
		} else if rf.roleState == statusCandidate {
			// Check if receive votes from majority
			// 7 -> >3
			// 8 -> >4
			majorityNum := len(rf.peers) / 2
			if rf.voteCnt > majorityNum {
				// Win the election
				rf.roleState = statusLeader
				// Re-initialize next & match index
				rf.initNextMatchIndex()
				// fmt.Print("Server ", strconv.Itoa(rf.me), " wins the election with term ", strconv.Itoa(rf.currentTerm), ".\n")
			} else {
				// Check if election timeouts and start another election
				timeNow := rf.getSysTime()
				timeDiff := (timeNow - rf.lastRecvTimeStamp)
				if timeDiff >= rf.electionTimeout {
					rf.initiateNewElection()
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

//
// Goroutine for periodically sending out AppendEntries or
// heartbeat messages if in the role of leader
//
func (rf *Raft) appendEntriesRoutine() {
	// Periodically check per 5 msec
	for !rf.killed() {
		rf.mu.Lock()
		if rf.roleState == statusLeader {
			// Check the time diff since last time sending out AppendEntries RPC
			timeNow := rf.getSysTime()
			timeDiff := (timeNow - rf.lastSendTimeStamp)
			if timeDiff >= int64(heartBeatPeriod) {
				// Leader state
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						rf.ReplicateLog(rf.me, i)
					}
				}
				rf.lastSendTimeStamp = rf.getSysTime()
			}
		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// func (rf *Raft) applyLogEntriesRoutine() {
// 	for {
// 		rf.cv.L.Lock()
// 		if rf.lastApplied == rf.commitIndex {
// 			if rf.killed() {
// 				rf.cv.L.Unlock()
// 				break
// 			}
// 			rf.cv.Wait()
// 			rf.cv.L.Unlock()
// 		} else {
// 			rf.cv.L.Unlock()
// 			for rf.lastApplied < rf.commitIndex {
// 				// Hold locks when reaching here
// 				commandToApply := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied + 1}
// 				rf.applyCh <- commandToApply
// 				// fmt.Print("Server ", strconv.Itoa(rf.me), " applies log ", strconv.Itoa(commandIndex), " with command ", rf.logs[commandIndex].Command, "\n")
// 				rf.lastApplied += 1
// 			}
// 			if rf.killed() {
// 				break
// 			}
// 		}
// 	}
// }

func (rf *Raft) applyLogEntriesRoutine() {
	for !rf.killed() || rf.lastApplied < rf.commitIndex {
		for rf.lastApplied < rf.commitIndex {
			commandToApply := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied + 1}
			rf.applyCh <- commandToApply
			// fmt.Print("Server ", strconv.Itoa(rf.me), " applies log ", strconv.Itoa(commandIndex), " with command ", rf.logs[commandIndex].Command, "\n")
			rf.lastApplied += 1
		}
		time.Sleep(10 * time.Millisecond)
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
	rand.Seed(12)
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// DO NOT call rf.transitToFollower(0, -1) here since it will save persistant states
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.roleState = statusFollower
	rf.voteCnt = 0
	// Reset election timeout
	rf.ReselectElectionTimeout()
	rf.cancelElectionTimeout()
	rf.lastSendTimeStamp = rf.lastRecvTimeStamp
	// Initialize next & match index
	rf.initNextMatchIndex()
	// Initialize commit info
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Kick off the goroutine for monitoring election timeout
	go rf.electionTimeoutMonitor()
	// Kick off the goroutine for sending AppendEntries as leader role
	go rf.appendEntriesRoutine()
	// Kick off the goroutine for applying logs to the state machine
	go rf.applyLogEntriesRoutine()
	rf.mu.Unlock()
	return rf
}
