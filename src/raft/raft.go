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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//const state to identify server state
const (
	// equals: leader:0, candidate:1, follower:2
	// STATE_LEADER = 0
	// STATE_CANDIDATE = 1
	// STATE_FOLLOWER = 2

	STATE_LEADER = "leader"
	STATE_FOLLOWER = "follower"
	STATE_CANDIDATE = "candidate"
)

const (

	HB_INTERVAL = 50 * time.Millisecond //50ms
	ELECTION_MIN_TIME = 150
	ELECTION_MAX_TIME = 300
)



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {

	// the term when entry was received by leader
	Term int

	// command for state machine
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Raft 服务器需要包含的数据

	//【state and applyMsg channel】
	// current state of server
	state string

	//the count of votes that current candidate has got
	voteCount int

	// true means current server is Leader
	// "chan bool" means channel bool, a channel which can only pass bool parameters
	chanApply chan ApplyMsg

	//【persistent state on all the server】
	// lastest term server has seen
	currentTerm int

	// candidateId that received vote in current term
	votedFor int

	// log entries
	logs []LogEntry

	// 【volatile state on all servers】
	// index of highest log entry known to be commited
	commitIndex int

	// index of highest log entry applied to state machine
	lastApplied int

	// 【volatile state on leader】
	// for each server, index of next log entry to send to that server
	nextLogIndex []int

	// for each server, index of highest log entry known to be replicated on server
	matchLogIndex []int

	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == STATE_LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 持久化需要存储的数据
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 恢复（读取）存储的数据
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
	}

}

func (rf *Raft) getLastLogTerm() int {
		return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLogSize() int {
	return len(rf.logs)
}

func (rf *Raft) isLeader() bool {
		return rf.state == STATE_LEADER
}


//
// example RequestVote RPC arguments structure.
// invoked by candidate for getting votes
//
type RequestVoteArgs struct {
	// Your data here.
	// candidate's term
	Term int

	// Id of candidate that requesting vote
	CandidateId int

	// index of candidate's last log
	LastLogIndex int

	// term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	// current term, for candidate to update itself
	Term int

	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// 收到投票请求时的处理逻辑
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true

	//candidate's term < server's term, return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//server's log is newer than that of candidate
	if len(rf.logs) > 0 {
		// if server's term > candidate's term or same term, but server's log index is bigger, this server won't vote
		if rf.getLastLogTerm() > args.LastLogTerm || (rf.getLastLogTerm() == args.LastLogTerm && rf.getLogSize()-1 > args.LastLogIndex){
				may_grant_vote = false
		}
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	// term of candidate > currentTerm of server
	if args.Term > rf.currentTerm {
		//update the server's state to Follower no matter what state the server now is
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1

		if may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}

		//if the server is sure to be follower, then after each reply for vote or heartbeat, it will reset its timer
		rf.resetTimer()

		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	}
}

//
// get the number of "majority"
//
func majority(n int) int {
	return n / 2 + 1
}

//
// handle vote result
//
func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm

	// ignore old term
	if reply.Term < term {
		return
	}

	// means the current candidate isn't able to get the vote, can't become a leader, some other candidate mey become leader
	if reply.Term > term {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//TODO check persist need or not
		// rf.persist()
		rf.resetTimer()
		return
	}

	// means the current candidate gets the vote
	if rf.state == STATE_CANDIDATE && reply.VoteGranted {
		rf.voteCount++
		// if current state is candidate and gets more than half votes, it turns to be a leader
		if rf.voteCount >= majority(len(rf.peers)) {
			rf.state = STATE_LEADER

			for i := 0 ; i < len(rf.peers) ; i++ {
				if i == rf.me {
					continue
				}
				//nextLogEntryIndex: initialized to Leader's last log index + 1(that is log size)
				rf.nextLogIndex[i] = rf.getLogSize()
				rf.matchLogIndex[i] = -1
			}
			rf.resetTimer()
		}
		return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 如果RPC调用存在问题，检查传递的结构体的字段名是否为大写字母开头
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC struct
// invoked by leader to replicate log entries; also used as heartbeat
//
type AppendEntryArgs struct {
	// leader's term
	Term int

	// so follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogIndex int

	// term of PrevLogEntryIndex
	PrevLogTerm int

	// log entries to store(empty for heartbeat; may send more than one for efficiency)
	LogEntries []LogEntry

	// leader's commitIndex
	LeaderCommitIndex int
}

type AppendEntryReply struct {
	// currentTerm, for leader to update itself
	Term int

	// true if follower contained entry matching prevLogEntryIndex and prevLogEntryTerm
	Success bool

	// means server's last logs index
	CommitLogIndex int
}

//
// After receive appendEntries
// append entries to follower or used for heartbeat
//
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.resetTimer()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// reply.CommitLogIndex = rf.getLogSize()
		return
	} else {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1

		reply.Term = rf.currentTerm

		// leader's log size is less than me, not match
		// leader's prevLogEntryTerm is not equal to the [PrevLogEntryIndex] log entry term of me, not match
		if args.PrevLogIndex>=0 && (args.PrevLogIndex > rf.getLogSize()-1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm){
			// situation of not matching, system need to find the matching term and index

			reply.CommitLogIndex = rf.getLogSize()-1
			if reply.CommitLogIndex > args.PrevLogIndex {
				reply.CommitLogIndex = args.PrevLogIndex
			}

			// find the first log entry of which the term is equal to PrevLogTerm
			for reply.CommitLogIndex >=0 {
				if rf.logs[reply.CommitLogIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitLogIndex--
			}
			return
		}else if args.LogEntries != nil {
			// situation of deleting the existing entry and all that following it and appending log entries

			rf.logs = rf.logs[:args.PrevLogIndex + 1] //don't include the value at index: args.PrevLogEntryIndex + 1
			rf.logs = append(rf.logs, args.LogEntries...)
			if rf.getLogSize() > args.LeaderCommitIndex {
				rf.commitIndex = args.LeaderCommitIndex
				go rf.commitLogs()
			}
			reply.CommitLogIndex = rf.getLogSize()-1
			reply.Success = true
			return
		}else {
			// situation of heartbeat

			if rf.getLogSize() > args.LeaderCommitIndex {
					rf.commitIndex = args.LeaderCommitIndex
					go rf.commitLogs()
			}

			reply.CommitLogIndex = args.PrevLogIndex
			reply.Success = true
			return
		}
	}

}

//
// send appendEntries to a Follower
// use RPC Call()
//
func (rf *Raft) sendAppendEntryToFollower(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// send appendEntries to all Follower
// Invoked by Leader
//
func (rf *Raft) SendAppendEntriesToAllFollower() {
	for i := 0 ; i < len(rf.peers) ; i++ {
		if i==rf.me {
			continue
		}

		//rf is leader, so according to rf's parameters to build args
		var args AppendEntryArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		//TODO here, PrevLogEntryIndex means server's last log entry index
		args.PrevLogIndex = rf.nextLogIndex[i] - 1

		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}

		//if Leader's logs are more than those of server, then send all the new log entries
		if rf.nextLogIndex[i] < len(rf.logs) {
			args.LogEntries = rf.logs[rf.nextLogIndex[i]:]
		}

		args.LeaderCommitIndex = rf.commitIndex

		//for each server, have a goroutine to send appendEntries and handle the reply
		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ok := rf.sendAppendEntryToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

//
// Handle AppendEntry result
//
func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != STATE_LEADER {
		return
	}

	//Leader should degenerate to Follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = STATE_FOLLOWER
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextLogIndex[server] = reply.CommitLogIndex + 1
		rf.matchLogIndex[server] = reply.CommitLogIndex
		reply_count := 1

		for i := 0 ; i < len(rf.peers) ; i++ {
			if i==rf.me {
				continue
			}

			// statistic how many servers have appended their log entries, casue if Success, matchLogEntryIndex[server] will be updated
			if rf.matchLogIndex[i] >= rf.matchLogIndex[server] {
				reply_count++
			}
		}

		// if majority servers has updated and synchronized with Leader,
		if reply_count >= majority(len(rf.peers)) && rf.commitIndex < rf.matchLogIndex[server] && rf.logs[rf.matchLogIndex[server]].Term==rf.currentTerm {
				rf.commitIndex = rf.matchLogIndex[server]
				go rf.commitLogs()
		}
	} else {
		//if log entries are different, and cause failure, then reduce nextLogEntryIndex to hava a try again
		rf.nextLogIndex[server] = reply.CommitLogIndex + 1
		rf.SendAppendEntriesToAllFollower()
	}
}

//
// send ApplyMsg(like a kind of redo log) to chanApply
//
func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.getLogSize()-1 {
		rf.commitIndex = rf.getLogSize()-1
	}

	for i:=rf.lastApplied+1 ; i<=rf.commitIndex ; i++ {
		rf.chanApply <- ApplyMsg{Index: i+1, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	term := -1
	isLeader := false
	log := LogEntry{rf.currentTerm, command}

	if rf.state != STATE_LEADER {
		return index, term, isLeader
	}

	isLeader = (rf.state == STATE_LEADER)
	rf.logs = append(rf.logs, log)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()

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
// when peer timeout, it changes to be a candidate and invoke sendRequestVote()
//
func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	rf.mu.Unlock()

	if rf.state != STATE_LEADER {
		rf.state = STATE_CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.voteCount = 1
		rf.persist()

		args := RequestVoteArgs {
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.getLogSize()-1,
		}

		if rf.getLogSize() > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}

		//as a candidate, send vote request to other servers for votes
		for i := 0 ; i < len(rf.peers) ; i++ {
			if i == rf.me {
				continue
			}

			go func(i int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, args, &reply)
				if ok{
					rf.handleVoteResult(reply)
				}
			}(i, args)
		}
	}else {
		rf.SendAppendEntriesToAllFollower()
	}

	rf.resetTimer()
}

// Leader: 50ms for heartbeat
// Follower: 150-300ms for election
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		// start a new instance of Timer, and begin timing.
		// if the timeout is 0 second, the timers will expired immediately
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		// this func will be blocked to wait receiving a value from Timer, waiting time is 1s
		go func() {
			for {
				// timer.C is a channel that puts time into
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}

	new_timeout := HB_INTERVAL
	if rf.state != STATE_LEADER {
		//Duration: the length of time, has different unit of measurement
		new_timeout = time.Millisecond * time.Duration(ELECTION_MIN_TIME + rand.Int63n(ELECTION_MAX_TIME-ELECTION_MIN_TIME))
	}
	rf.timer.Reset(new_timeout)
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextLogIndex = make([]int, len(rf.peers))
	rf.matchLogIndex = make([]int, len(rf.peers))

	rf.state = STATE_FOLLOWER
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	rf.resetTimer()

	return rf
}
