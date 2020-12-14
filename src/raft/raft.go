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
	"sync"
	"time"
)
import "labrpc"

import _ "bytes"
import _ "encoding/gob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int // index into peers[]
	currentTerm int
	votedFor    int
	getvoteCnt  int
	log         []byte
	commitIndex int
	lastApplied int
	nextIndex   int
	matchIndex  int
	wg          sync.WaitGroup
	ch          chan bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	term = rf.currentTerm
	var leader bool
	leader = false
	if rf.getvoteCnt > len(rf.peers)/2 {
		leader = true
	}

	// Your code here.
	return term, leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.commitIndex)
	_ = e.Encode(rf.lastApplied)
	_ = e.Encode(rf.nextIndex)
	_ = e.Encode(rf.matchIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	//TODO: fill it
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	//TODO: fill it
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.ch <- true
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		}
	}

	defer rf.wg.Done()

	//println("vote to " + strconv.Itoa(args.CandidateId))
	//print(rf.GetState())

	//rf.GetState()
	//print(args.Term)
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
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
	rf := &Raft{} //equals new(Raft)
	rf.peers = peers
	rf.persister = persister
	// Your initialization code here.
	rf.me = me //index of this server
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.getvoteCnt = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, 1)

	//println(me)   //0,1,2 have three server and the make is to every one
	//print(len(peers))

	rf.ch = make(chan bool)
	rf.wg.Add(2)
	go func() {
		time.Sleep(time.Millisecond * (100 * time.Duration(me+1)))
		rf.wg.Done()
	}()
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		select {
		case <-time.After(time.Millisecond*(100*time.Duration(me+1)) - 1):
			{
				defer rf.wg.Done()
				//println(strconv.Itoa(rf.me) + " request vote")
				args := &RequestVoteArgs{}

				rf.currentTerm = rf.currentTerm + 1
				rf.votedFor = me
				rf.getvoteCnt = 1
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				for i := range rf.peers {
					//var count int = 0
					if i == rf.me {
						continue
					}
					reply := &RequestVoteReply{}
					if rf.sendRequestVote(i, *args, reply) {
						if reply.VoteGranted {
							rf.getvoteCnt = rf.getvoteCnt + 1
						}
						//println(reply.VoteGranted)
					}

				}
			}
		case <-rf.ch:
			{
				//println(strconv.Itoa(rf.me) +" vote to others")
				//rf.wg.Done()
			}
		}
		rf.wg.Wait()
		//println("it ends")
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
