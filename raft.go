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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state          State
	heartbeat      chan bool
	voteCount      int
	winElection    chan bool
	becomeFollower chan bool
	alreadyVoted   chan bool
	apply          chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

func (rf *Raft) startRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendRequestVote(peer, &args, &RequestVoteReply{})
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.followerCreation(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		rf.isLogValid(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.sendToChannel(rf.alreadyVoted, true)
	}

}

func (rf *Raft) isLogValid(LastLog int, LastTerm int) bool {
	thisLastLog := rf.getLastLog()
	thisLastTerm := rf.getLastTerm()

	if LastTerm == thisLastTerm {
		if LastLog >= thisLastLog {
			return true
		} else {
			return false
		}
	}
	return false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.followerCreation(args.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.sendToChannel(rf.winElection, true)
		}
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	ErrorInd  int
	ErrorTerm int
}

func (rf *Raft) startAppendEntries() {
	if rf.state != Leader {
		return
	}

	for peer := range rf.peers {
		if peer != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				LeaderCommit: rf.commitIndex,
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			entries := rf.log[rf.nextIndex[peer]:]
			args.Entries = make([]LogEntry, len(entries))
			copy(args.Entries, entries)

			go rf.sendAppendEntries(peer, &args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ErrorInd = -1
		reply.ErrorTerm = -1
		return
	}

	if args.Term > rf.currentTerm {
		rf.followerCreation(args.Term)
	}

	lastInd := rf.getLastLog()
	rf.sendToChannel(rf.heartbeat, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ErrorInd = -1
	reply.ErrorTerm = -1

	if args.PrevLogIndex > lastInd {
		reply.ErrorInd = lastInd + 1
		return
	}

	if cfTerm := rf.log[args.PrevLogIndex].Term; cfTerm != args.PrevLogTerm {
		reply.ErrorTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.log[i].Term == cfTerm; i-- {
			reply.ErrorInd = i
		}
		reply.Success = false
		return
	}

	i, j := args.PrevLogIndex+1, 0
	for ; i < lastInd+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	rf.log = rf.log[:i]
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		lastInd = rf.getLastLog()
		if args.LeaderCommit < lastInd {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastInd
		}
		go rf.applyLogs()
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.followerCreation(args.Term)
		return
	}

	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	} else if reply.ErrorTerm < 0 {
		// follower's log shorter than leader's
		rf.nextIndex[peer] = reply.ErrorTerm
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
	} else {
		// try to find the conflictTerm in log
		newNextIndex := rf.getLastLog()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.ErrorTerm {
				break
			}
		}
		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[peer] = reply.ErrorInd
		} else {
			rf.nextIndex[peer] = newNextIndex
		}
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
	}

	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.getLastLog(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.apply <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{term, command})

	return rf.getLastLog(), term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetChannels() {
	rf.winElection = make(chan bool)
	rf.becomeFollower = make(chan bool)
	rf.alreadyVoted = make(chan bool)
	rf.heartbeat = make(chan bool)
}

func (rf *Raft) followerCreation(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	if state != Follower {
		rf.sendToChannel(rf.becomeFollower, true)
	}
}

func (rf *Raft) candidateCreation(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != state {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1

	rf.startRequestVote()
}

func (rf *Raft) leaderCreation() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}
	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastLog() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	rf.startAppendEntries()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		leadersTimeout := time.Duration(150 * time.Millisecond)
		// Your code here (2A)
		// Check if a leader election should be started.
		switch state {
		case Follower:
			select {
			case <-rf.alreadyVoted:
				continue
			case <-rf.heartbeat:
				continue
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.candidateCreation(Follower)
			}
		case Candidate:
			select {
			case <-rf.becomeFollower:
				continue
			case <-rf.winElection:
				rf.leaderCreation()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.candidateCreation(Candidate)
			}
		case Leader:
			select {
			case <-rf.becomeFollower:
				continue
			case <-time.After(leadersTimeout):
				rf.mu.Lock()
				rf.startAppendEntries()
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) getLastLog() int {
	return len(rf.log) - 1
}
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastLog()].Term
}
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.heartbeat = make(chan bool)
	rf.voteCount = 0
	rf.winElection = make(chan bool)
	rf.becomeFollower = make(chan bool)
	rf.alreadyVoted = make(chan bool)
	rf.apply = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
