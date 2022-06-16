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
	"os"
	"fmt"
	"log"
	"bytes"
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	"strconv"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HasNotVoted = -1
	NoLeader = -1
)

// states
const (
	Follower = iota
	Candidate
	Leader
)

// misc
const (
	ElectionTimeout = 500
)

type State int


// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	Term	int
	Command	interface{}
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

	// persistent state
	currentTerm int 		// currntTerm is latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor 	int		 	// votedFor candidateId that received vote in current term (or -1 if none)
	log			[]LogEntry 	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int 	 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int		 // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	leader		int		 // index into peers[] that is the current term's leader

	// volate state on leaders (re-initialized after election)
	nextIndex	[]int	 // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// misc
	state		State	
	voteCount	int
	applyCh		chan ApplyMsg
	grantVoteCh	chan bool
	winnerCh	chan bool
	heartbeatCh chan bool
	stepDownCh	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var persistedLog []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&persistedLog) != nil {
		Debug(dInfo, "readPersist failed")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = persistedLog
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex 	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term 			int
	VoteGranted 	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// step down to follower if incoming args term is greater than our current term
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// If votedFor is null or candidateId, 
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == HasNotVoted || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		Debug(dInfo, "S%d voting for S%d", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// if we granted a vote, send it on channel to runServer thread to act on
		rf.sendToChannel(rf.grantVoteCh, true)
	}
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

func (rf *Raft) isLogUpToDate(candidateLastTerm int, candidateLastIndex int) bool {
	lastTerm, lastIndex := rf.getLastTerm(), rf.getLastIndex()
	// if candidate term is same as our term, check if candidate log index is >= our index
	if candidateLastTerm == lastTerm {
		return candidateLastIndex >= lastIndex
	}

	// otherwise return true if candidate term is greater than ours, otherwise false
	return candidateLastTerm > lastTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.state != Candidate {
		return
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// if call failed, return (no vote)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "S%d term %d vote reply from S%d: votegranted: %b, term: %d", rf.me, rf.currentTerm, server, reply.VoteGranted, reply.Term)
	// if target node term is higher than ours, become a follower
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		Debug(dInfo, "S%d has received %d votes", rf.me, rf.voteCount)
		// if we reached a majority, send msg to winner channel for runServer thread to see
		if rf.voteCount == len(rf.peers)/2 +1 {
			rf.sendToChannel(rf.winnerCh, true)
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
	rf.state = Follower
	rf.voteCount = 0

	// persisted state which will be overwritten from persisted state on disk if it exists
	rf.votedFor = HasNotVoted
	rf.currentTerm = 0

	// volatile state on all servers
	rf.commitIndex = 0 	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	rf.lastApplied = 0 	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	rf.applyCh = make(chan ApplyMsg)
	rf.grantVoteCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.winnerCh = make(chan bool)
	rf.log = append(rf.log, LogEntry{Term: 0}) // initialize with empty log entry to prevent out of bounds errors
	// NOTE: rf.nextIndex and rf.matchIndex are only needed when node is a leader, and need to be re-initialized upon election

	// initialize from state persisted before a crash (currentTerm, votedFor, log)
	rf.readPersist(persister.ReadRaftState())

	// run server in separate thread 
	go rf.runServer()
	return rf
}

func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		
		// while we are leader, broadcast AppendEntries until term is over
		case Leader: 
			select {
			case <-rf.stepDownCh: // already follower by the time this msg is received
				Debug(dInfo, "S%d leader: stepdown chan", rf.me)
			case <-time.After(120 * time.Millisecond):
				rf.broadcastAppendEntries()
			}

		case Follower:
			select {
			case <-rf.grantVoteCh: // if we granted a vote, stay follower
				//Debug(dInfo, "S%d follower: grantVoteCh", rf.me)
			case <-rf.heartbeatCh: // if we received heartbeat from leader, stay follower
				//Debug(dInfo, "S%d follower: heartbeatCh", rf.me)
			// if after random election timeout between 360-500ms no vote granted and no heartbeat, run election
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				Debug(dInfo, "S%d follower: running election", rf.me)
				rf.convertToCandidate(Follower)
			}

		case Candidate:
			select {
			case <-rf.stepDownCh: // should be a follower by the time this msg is received
				Debug(dInfo, "S%d candidate: stepDownCh", rf.me)
			case <-rf.winnerCh:
				Debug(dInfo, "S%d candidate: winnerCh", rf.me)
				rf.convertToLeader()
			// rerun election if after random election timeout between 360-500ms no winner is found
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				Debug(dInfo, "S%d candidate: running election again", rf.me)
				rf.convertToCandidate(Candidate)
			}
		}
	}
}

// get a random election timeout between 360 and 500 ms
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// step down to follower state
func (rf *Raft) convertToFollower(newTerm int) {
	prevState := rf.state
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = HasNotVoted
	// if prev state wasn't follower, send msg on stepdown chan for runServer thread to act on
	if prevState != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}

// increment term and run election as a candidate
func (rf *Raft) convertToCandidate(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "converted S%d to candidate", rf.me)
	rf.resetChannels()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.broadcastVoteRequest()
}


func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetChannels()
	rf.state = Leader
	
	// matchIndex: for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	rf.matchIndex = make([]int, len(rf.peers))
	
	// rf.nextIndex: for each server, index of the next log entry to send to that server 
	// (initialized to leader last log index + 1)
	rf.nextIndex = make([]int, len(rf.peers)) 
	lastIndex := rf.getLastIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}
	// send out append entries / heartbeat
	rf.broadcastAppendEntries()
}

// runs sendVoteRequest to all peers in parallel
func (rf *Raft) broadcastVoteRequest() {
	args := &RequestVoteArgs{
		Term: 			rf.currentTerm,
		CandidateId: 	rf.me,	
		LastLogIndex: 	rf.getLastIndex(),
		LastLogTerm: 	rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term 			int	
	LeaderId		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool	
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// set default return values
	reply.Term = rf.currentTerm
	reply.Success = false

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// convert to follower if incoming term is greater than ours
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// append entries serves as a heartbeat mechanism from leader to followers
	rf.sendToChannel(rf.heartbeatCh, true)
	lastIndex := rf.getLastIndex()

	// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > lastIndex {
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// find point where any conflicting entries occur
	i, j := args.PrevLogIndex + 1, 0
	for ; i < lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			return
		}
	}

	// append new entries (potentially overwriting existing conflicting entries)
	rf.log = rf.log[:i]
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true

	// update leader id if successful
	rf.leader = args.LeaderId
	
	// update commit index if necessary to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		// if commit index has changed, apply newly committed log commands
		go rf.applyLogs()
	}
}

// send AppendEntries RPC to a specific follower node
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	Debug(dInfo, "S%d sending append entries to S%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	// TODO
}

// send AppendEntries RPC to all followers in parallel
func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[server]-1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

			// copy new log entries into args
			entries := rf.log[args.PrevLogIndex:]
			args.Entries = make([]LogEntry, len(entries))
			copy(args.Entries, entries)
			go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
		}
	}
}

// apply the committed log commands
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Command: rf.log[i].Command,
			CommandIndex: i,
			CommandValid: true,
		}
		rf.lastApplied = i
	}
}

// helper method for resetting all channels, used when follower becomes candidate for new term+election
func (rf *Raft) resetChannels() {
	rf.winnerCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

func (rf *Raft) sendToChannel(channel chan<- bool, msg bool) {
	select {
    case channel <- msg:
        // message sent
    default:
        // message dropped
	}
}