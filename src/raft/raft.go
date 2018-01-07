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

import "sync"
import "labrpc"

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
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
)

const (

	HB_INTERVAL = 50 * time.Millisecod //50ms
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
	LogEntryIndex int

	// the term when entry was received by leader
	LogEntryTerm int

	// command for state machine
	LogEntryCommand interface{}
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

	//【channel】
	// current state of server
	// state int

	//the count of votes that current candidate has got
	voteCount int

	// true means current server is Leader
	// "chan bool" means channel bool, a channel which can only pass bool parameters
	chanLeader chan bool


	chanHeartBeat chan bool

	chanGrantVote chan bool

	chanCommit chan bool

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
	nextLogEntryIndex []int

	// for each server, index of highest log entry known to be replicated on server
	matchLogEntryIndex []int

	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER

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

	// index of candidate's last log entry
	LastLogEntryIndex int

	// term of candidate's last log entry
	LastLogEntryTerm int

}

func (rf *Raft) getLastLogEntryIndex() int {
	return rf.logs[len(rf.logs)-1].LogEntryIndex
}

func (rf *Raft) getLastLogEntryTerm() int {
	return rf.logs[len(rf.logs)-1].LogEntryTerm
}

func (rf *Raft) isLeader() bool {
	retrn rf.state == STATE_LEADER
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

	if len(rf.logs) > 0 {
		// if server's term > candidate's term or same term, but server's log index is bigger, this server won't vote
		if rf.getLastLogEntryTerm() > args.LastLogEntryTerm || (rf.getLastLogEntryTerm() == args.LastLogEntryTerm && rf.getLastLogEntryIndex > args.LastLogEntryIndex){
				may_grant_vote = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
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
		//update the server's current term
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

func majority(n int) int {
	return n / 2 + 1
}

//
// handle vote result
//
func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	// work to do
	// 1. check some args, if wrong, return directly
	// 2. handle the reply

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore old term
	if reply.Term < rf.currentTerm {
		return
	}

	term := rf.currentTerm

	// means the current candidate isn't able to get the vote, can't become a leader, some other candidate mey become leader
	if reply.Term > term {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	// means the current candidate gets the vote
	if rf.state == STATE_CANDIDATE && reply.VoteGranted {
		rf.voteCount++
		// if current state is candidate and gets more than half votes, it turns to be a leader
		if rf.voteCount >= majority(len(r.peers)) {
			rf.state = STATE_LEADER
			rf.chanLeader <- true

			for i:=0 ; i<len(rf.peers) ; i++ {
				if i == rf.me {
					continue
				}
				nextLogEntryIndex[i] = rf.getLastLogEntryIndex() + 1
				matchLogEntryIndex[i] = -1
			}
		}
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
type AppendEntriesArgs struct {
	// leader's term
	Term int

	// so follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogEntryIndex int

	// term of PrevLogEntryIndex
	PrevLogEntryTerm int

	// log entries to store(empty for heartbeat; may send more than one for efficiency)
	LogEntries []LogEntry

	// leader's commitIndex
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int

	// true if follower contained entry matching prevLogEntryIndex and prevLogEntryTerm
	Success bool

	CommitLogEntryIndex int
}

//
// append entries to follower or used for heartbeat
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.resetTimer()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.CommitLogEntryIndex = rf.getLastLogEntryIndex() + 1
		return
	} else {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		reply.Term = rf.currentTerm

		// leader prevIndex is larger than me, not match
		// same index, term not equal, not match
		term := rf.logs[args.PrevLogEntryIndex-firstLogEntryIndex].LogEntryTerm
		if args.PrevLogEntryIndex > rf.getLastLogEntryIndex() || term != args.PrevLogEntryTerm{
			// situation of not matching

			reply.CommitLogEntryIndex = rf.getLastLogEntryIndex()
			if reply.CommitLogEntryIndex > args.PrevLogEntryIndex {
				reply.CommitLogEntryIndex = args.PrevLogEntryIndex
			}

			// find the first log entry of which the term is equal to PrevLogEntryTerm
			for reply.CommitLogEntryIndex >=0 {
				if rf.logs[reply.CommitLogEntryIndex].Term == args.PrevLogEntryTerm {
					break
				}
				reply.CommitLogEntryIndex--
			}
			return
		}else if args.LogEntries != nil {
			// situation of deleting the existing entry and all that following it and appending log entries

			rf.logs = rf.logs[:args.PrevLogEntryIndex + 1]
			rf.logs = append(rf.logs, args.LogEntries...)
			if rf.getLastLogEntryIndex >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogEntries()
			}
			reply.CommitLogEntryIndex = rf.getLastLogEntryIndex()
			reply.Success = true
			return
		}else {
			// situation of heartbeat

			if rf.getLastLogEntryIndex >= args.LeaderCommitIndex {
					rf.commitIndex = args.LeaderCommitIndex
					go rf.commitLogEntries()
			}

			reply.NextLogEntryIndex = args.PrevLogEntryIndex
			reply.Success = true
			return
		}
	}

}

//
// send appendEntries to a Follower
// use RPC Call()
//
func (rf *Raft) sendAppendEntryToFollower(serverNum int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// send appendEntries to all Follower
// Invoked by Leader
//
func (rf *Raft) SendAppendEntriesToAllFollower() {
	for i:=0 ; i<len(rf.peers) ; i++ {
		if i==rf.me {
			continue
		}

		//rf is leader, so according to rf's parameters to build args
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.PrevLogEntryIndex = rf.nextLogEntryIndex[i] - 1

		if args.PrevLogEntryIndex >= 0 {
			args.PrevLogEntryTerm = rf.logs[args.PrevLogEntryIndex].LogEntryTerm
		}

		//if Leader's logs are more than those of server, then send all the new log entries
		if nextLogEntryIndex[i] < len(rf.logs) {
			args.LogEntries = rf.logs[rf.nextLogEntryIndex[i]:]
		}

		args.LeaderCommit = rf.commitIndex

		//for each server, have a goroutine to send appendEntries and handle the reply
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntryToFollower(server, args, &reply)
			if ok {
				rf.handleAppendEntries(server, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) handleAppendEntries(server int, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != STATE_LEADER {
		return
	}

	if reply.term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = STATE_FOLLOWER
		rf.resetTimer()
		return
	}

	if Success {
		rf.nextLogEntryIndex[server] = reply.CommitLogEntryIndex + 1
		rf.matchLogEntryIndex[server] = reply.CommitLogEntryIndex
		reply_count := 1

		for i := 0 ; i < len(rf.peers) ; i++ {
			if i==rf.me {
				continue
			}

			// statistic how many servers has been updated with Leader
			if rf.matchLogEntryIndex[i] >= rf.matchLogEntryIndex[server] {
				reply_count++
			}
		}

		// if majority servers has updated and synchronized with Leader,
		if reply_count >= majority(len(rf.peers)) {
				rf.commitIndex = rf.matchLogEntryIndex[server]
				go rf.commitLogEntries()
		}
	} else {
		//if log entries are different, and cause failure, then reduce nextLogEntryIndex to hava a try again
		nextLogEntryIndex[server] = rf.CommitLogEntryIndex + 1
		rf.SendAppendEntriesToAllFollower()
	}
}

func (rf *Raft) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.getLastLogEntryIndex() {
		rf.commitIndex = rf.getLastLogEntryIndex()
	}

	for i:=rf.lastApplied+1 ; i<=rf.commitIndex ; i++ {
		rf.chanApply <- ApplyMsg(Index: i+1, Command: rf.logs[i].LogEntryCommand)
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
	index := -1
	term := -1
	isLeader := true


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
			LastLogEntryIndex: rf.getLastLogEntryIndex(),
		}

		if len(rf.logs) > 0 {
			args.LastLogEntryTerm = rf.logs[args.LastLogEntryIndex].LogEntryTerm
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
		rf.timer = time.NewTimer(time.Millisecod * 1000)
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

	rf.nextLogEntryIndex = make([]int, len(rf.peers))
	rf.matchLogEntryIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	rf.resetTimer()

	return rf
}
