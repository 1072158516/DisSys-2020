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
	"runtime"

	"strconv"
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
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int // index into peers[]
	CurrentTerm   int
	votedFor      int
	state         int
	timeUnixHeart int64
	isgetHeart    bool
	log           []RaftLog
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	wg            sync.WaitGroup
	applymsg      chan ApplyMsg
	rb            chan bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	term = rf.CurrentTerm
	var leader bool
	leader = false
	if rf.state == 2 {
		//time.Sleep(time.Millisecond * 500)

		leader = true

	}

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
	_ = e.Encode(rf.CurrentTerm)
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

type RaftLog struct {
	Command int
	Term    int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.

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

	Term        int
	VoteGranted bool
}

//
// example Append RPC arguments structure.
//
type AppendEntriseArgs struct {
	// Your data here.
	Isheart      bool
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

//
// example Append RPC reply structure.
//
type AppendEntriseReply struct {
	// Your data here.
	Term       int
	Success    bool
	Matchindex int
}

//
// example RequestVote RPC reply structure.
//

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	//println(strconv.Itoa(rf.me) + " state = " + strconv.Itoa(rf.state) + "term: " + strconv.Itoa(rf.CurrentTerm))
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
	}
	if args.Term >= rf.CurrentTerm {
		if args.Term > rf.CurrentTerm {

			reply.VoteGranted = true
			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			//println("im in" + strconv.Itoa(rf.CurrentTerm))
			rf.votedFor = args.CandidateId
			rf.timeUnixHeart = time.Now().UnixNano() / 1000000
			rf.mu.Unlock()
		} else {
			//rf.CurrentTerm = args.Term
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				rf.mu.Lock()
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.timeUnixHeart = time.Now().UnixNano() / 1000000
				rf.mu.Unlock()
				//println(rf.timeUnixHeart)
			}
		}

		rf.Convert(0)
	}
	reply.Term = rf.CurrentTerm
}
func (rf *Raft) AppendEntries(args AppendEntriseArgs, reply *AppendEntriseReply) {
	// Your code here.

	//rf.ch <- true
	//term, _ := rf.GetState()

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
		//println("reply term big ")
	}
	if args.Term >= rf.CurrentTerm {
		//rf.Convert(0)
		if args.Isheart {
			reply.Success = true
			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			reply.Term = rf.CurrentTerm
			rf.timeUnixHeart = time.Now().UnixNano() / 1000000
			if args.LeaderCommit > rf.commitIndex {
				println("leader commit: " + strconv.Itoa(args.LeaderCommit))
				if args.LeaderCommit < len(rf.log)-1 {

					rf.commitIndex = rf.commitIndex + 1
					rf.SendCommitmss()

				} else {
					if len(rf.log)-1 > rf.commitIndex {
						rf.commitIndex = rf.commitIndex + 1
						rf.SendCommitmss()
						//println("sever : no: " + strconv.Itoa(rf.me) + strconv.Itoa(rf.commitIndex))
					}
				}

				println("sever : no: " + strconv.Itoa(rf.me) + strconv.Itoa(rf.commitIndex))
			}
			rf.mu.Unlock()
			//println("llllllllll")
			//go rf.Convert(0)
			return
		} else {
			reply.Success = true
			//rf.CurrentTerm = args.Term
			//println(args.Entries[len(args.Entries) - 1].Command)

			//println(strconv.Itoa(rf.me) + ": my term change to " + strconv.Itoa(rf.CurrentTerm))
			nextIndex := 1
			flag := false
			rf.mu.Lock()
			for i := range rf.log {
				if args.PrevLogIndex == i && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
					nextIndex = args.PrevLogIndex + 1
					flag = true

				}
			}
			if flag == false {
				reply.Success = true
				reply.Term = rf.CurrentTerm
				reply.Matchindex = -1
				return
			}
			//rf.log = append(rf.log[:nextIndex], rf.log[nextIndex+1:]...)
			rf.log = rf.log[:nextIndex]

			//if args.PrevLogIndex > len(rf.log)-1 || !(rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			//reply.Success = false
			//reply.Term = rf.CurrentTerm
			//return
			//}

			//flag = false
			for i := range args.Entries {
				rf.log = append(rf.log, args.Entries[i])
				println(strconv.Itoa(rf.me) + " append: " + strconv.Itoa(args.Entries[i].Command) + " no." + strconv.Itoa(len(rf.log)-1))
				reply.Matchindex = len(rf.log) - 1
				reply.Term = rf.CurrentTerm

			}

			//if flag{
			//	return
			//}
			//println("replymatch: " + strconv.Itoa(reply.Matchindex))

			if args.LeaderCommit > rf.commitIndex {
				//println("leader commit: " + strconv.Itoa(args.LeaderCommit))
				if args.LeaderCommit < len(rf.log)-1 {

					rf.commitIndex = rf.commitIndex + 1
					rf.SendCommitmss()

				} else {
					if len(rf.log)-1 > rf.commitIndex {
						rf.commitIndex = rf.commitIndex + 1
						rf.SendCommitmss()
						//println("sever : no: " + strconv.Itoa(rf.me) + strconv.Itoa(rf.commitIndex))
					}
				}
				reply.Success = true
				reply.Term = rf.CurrentTerm
				//println("sever : no: " + strconv.Itoa(rf.me) + strconv.Itoa(rf.commitIndex))
			}
			rf.mu.Unlock()

			//TODO:delect every confilicts

			//println("update log follower")

			rf.Convert(0)
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriseArgs, reply *AppendEntriseReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	//if server == 0{
	//	println("0 get")
	//}
	return ok
}

func (rf *Raft) SendCommitmss() {

	rf.applymsg <- ApplyMsg{
		Index:       rf.commitIndex,
		Command:     rf.log[rf.commitIndex].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}

	if rf.state == 2 {
		println("leader send " + strconv.Itoa(rf.log[rf.commitIndex].Command))

	} else {
		//println("fllower send " + strconv.Itoa(rf.log[rf.commitIndex].Command))
	}

}

func (rf *Raft) SendAppendLogs() {

	for i1 := 0; i1 < len(rf.peers); i1++ {

		if i1 == rf.me || rf.state != 2 {
			continue
		}
		i := i1
		go func() {
			//cb := make(chan bool)
			//time2 := time.NewTimer(time.Millisecond * 100)
			rf.mu.Lock()
			args := &AppendEntriseArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			//println(i)
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex > len(rf.log)-1 {
				println("whos preterm: " + strconv.Itoa(rf.me))
				//println(args.PrevLogTerm)
				println(args.PrevLogIndex)
			}

			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Isheart = false
			rf.mu.Unlock()
			//println(rf.log[len(rf.log)-1].Command)
			//println(rf.nextIndex[i])
			for j := rf.nextIndex[i]; j < len(rf.log); j++ {
				//println(strconv.Itoa(j) + " " + strconv.Itoa(len(rf.log)))
				rf.mu.Lock()
				args.Entries = append(args.Entries, rf.log[j])
				rf.mu.Unlock()
			}
			reply := &AppendEntriseReply{}
			reply.Term = -1
			reply.Matchindex = rf.matchIndex[i]
			//println(reply.Term)
			if rf.sendAppendEntries(i, *args, reply) {
				//println(strconv.Itoa(rf.me) + ": my term: " + strconv.Itoa(rf.CurrentTerm) + " reply term from " + strconv.Itoa(i) + " : term " + strconv.Itoa(reply.Term))

				if reply.Success {
					//println("heard reply from " + strconv.Itoa(i))
					if reply.Matchindex == -1 {
						rf.mu.Lock()
						rf.nextIndex[i] = rf.nextIndex[i] - 1
						rf.mu.Unlock()
						return
					}
					//println(rf.matchIndex[i])
				}
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.Convert(0)
					//println(strconv.Itoa(rf.me) + " convert to follower")
					rf.rb <- true
					runtime.Goexit()

				}
				//time.Sleep(time.Millisecond*50)
				rf.mu.Lock()
				rf.matchIndex[i] = reply.Matchindex
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				rf.mu.Unlock()

			}

			//TODO:update commitIndex
		}()
	}
	time.Sleep(time.Millisecond * 100)
	N := rf.commitIndex + 1
	count := 0
	for i := range rf.peers {
		rf.mu.Lock()
		if rf.matchIndex[i] >= N && i != rf.me {
			count++

		}
		rf.mu.Unlock()
	}
	//println(strconv.Itoa(count) + "<-count rf.len: "+ strconv.Itoa(len(rf.log)))
	if count >= (len(rf.peers)-1)/2 {
		rf.mu.Lock()
		rf.commitIndex = N
		rf.SendCommitmss()
		rf.mu.Unlock()
		//rf.wg.Add(1)
		//rf.SendAppendLogs()
	}
	//rf.wg.Done()
	//rf.SendCommitmss(rf.commitIndex,rf.log[rf.commitIndex].Command)
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
	//time.Sleep(time.Millisecond * 200)
	index := len(rf.log)
	term := rf.CurrentTerm
	isLeader := rf.state == 2

	rf.mu.Unlock()
	if isLeader {
		mylog := RaftLog{
			Command: command.(int),
			Term:    rf.CurrentTerm,
		}

		//rf.wg.Wait()
		//rf.wg.Add(len(rf.peers) - 1)
		rf.mu.Lock()
		//for i := range rf.log
		rf.log = append(rf.log, mylog)
		rf.mu.Unlock()
		//go rf.SendAppendLogs()

		//println(rf.log[len(rf.log) - 1].Command)
		//}

		rf.SendAppendLogs()

	}

	//println("index: " + strconv.Itoa(index))
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
func (rf *Raft) Convert(state int) {
	// Your code here, if desired.
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if state == rf.state && state != 0 {
		return
	}
	if state == 2 {
		rf.mu.Lock()
		rf.isgetHeart = false
		rf.state = 2
		for i := range rf.peers {
			if rf.me != i {
				rf.nextIndex[i] = len(rf.log)
				//println(rf.nextIndex[i])
				rf.matchIndex[i] = 0
			}

		}
		rf.mu.Unlock()
		//println(strconv.Itoa(rf.me) + ": im leader")
		go func() {

			//time.Sleep(time.Millisecond * 400)

			for rf.state == 2 {

				count := 0
				for i1 := 0; i1 < len(rf.peers); i1++ {
					//println(strconv.Itoa(rf.me)+"rf.state = " + strconv.Itoa(rf.state))
					//println(rf.state)
					if i1 == rf.me || rf.state != 2 {
						continue
					}
					i := i1
					go func() {
						//cb := make(chan bool)
						//time2 := time.NewTimer(time.Millisecond * 100)
						rf.mu.Lock()
						args := &AppendEntriseArgs{}
						args.Term = rf.CurrentTerm
						args.LeaderId = rf.me
						args.LeaderCommit = rf.commitIndex
						args.Isheart = true
						//println(i)
						reply := &AppendEntriseReply{}
						reply.Term = -1
						reply.Matchindex = rf.matchIndex[i]
						rf.mu.Unlock()
						//println(reply.Term)
						if rf.sendAppendEntries(i, *args, reply) {

							if reply.Success {

							}
							if reply.Term > rf.CurrentTerm {
								rf.mu.Lock()
								rf.CurrentTerm = reply.Term
								rf.mu.Unlock()
								rf.Convert(0)
								return
							}
							count++
						}

					}()
				}
				time.Sleep(time.Millisecond * 50)
				if count < 1 {
					rf.Convert(0)
				}
			}

		}()
	} else if state == 1 {
		rf.mu.Lock()
		rf.isgetHeart = false
		rf.state = 1
		rf.CurrentTerm++
		rf.votedFor = -1
		rf.mu.Unlock()
		//println(strconv.Itoa(rf.me) + " Term change to " + strconv.Itoa(rf.CurrentTerm))
		//rf.votedFor = -1
		//println("go elect")
		go rf.StartElect()

	} else if state == 0 {

		rf.mu.Lock()
		rf.state = 0
		if rf.isgetHeart == true {
			rf.mu.Unlock()
			return
		} else {
			rf.isgetHeart = true
		}
		rf.mu.Unlock()
		go func() {

			for rf.state == 0 {

				//time.Sleep(time.Millisecond * 300)
				var timezone = time.Now().UnixNano() / 1000000
				//println(timezone)
				//println(rf.timeUnixHeart)
				if timezone-rf.timeUnixHeart > 1000 {
					//println("election timeout from " + strconv.Itoa(rf.me))
					rf.mu.Lock()
					rf.votedFor = -1
					rf.mu.Unlock()
					time.Sleep(time.Millisecond * (100 * time.Duration(rf.me+1)))
					if rf.votedFor == -1 {
						rf.Convert(1)
						//println("start election from " + strconv.Itoa(rf.me))
					}
					break

				}

			}

		}()

	}

}

func (rf *Raft) StartElect() {

	args := &RequestVoteArgs{}

	var count = 0
	var cntmach = 0
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	//waitGroup := sync.WaitGroup{}
	//waitGroup.Add(len(rf.peers))
	for i1 := 0; i1 < len(rf.peers); i1++ {
		i := i1
		go func() {

			reply := new(RequestVoteReply)
			reply.Term = -1
			reply.VoteGranted = false
			//rf.sendRequestVote(i, *args, reply)
			ok := rf.sendRequestVote(i, *args, reply)
			{

				if ok {
					cntmach++
				}

				//println("request vote form " + strconv.Itoa(i) + " to " + strconv.Itoa(rf.me))
				if reply.VoteGranted {
					count++
					//println("get vote from " + strconv.Itoa(i) + "his term " + strconv.Itoa(reply.Term))
				}
				if reply.Term > rf.CurrentTerm {
					//println("convert to 0")
					rf.CurrentTerm = reply.Term
					rf.Convert(0)
					//waitGroup.Done()
					return
				}
			}
			//waitGroup.Done()
		}()

	}
	//waitGroup.Wait()
	time.Sleep(50 * time.Millisecond)
	//println(strconv.Itoa(count) + "  " + strconv.Itoa(cntmach))

	if count > cntmach/2 && cntmach > 1 {
		rf.Convert(2)
		return
	} else {
		//println(strconv.Itoa(rf.me) + "fail to be leader and now leader is" + strconv.Itoa(rf.votedFor) + "term: " + strconv.Itoa(rf.CurrentTerm))
		time.Sleep(199 * time.Millisecond)
		rf.mu.Lock()
		rf.isgetHeart = false
		rf.state = 1
		rf.CurrentTerm++
		rf.votedFor = -1
		rf.mu.Unlock()

		rf.StartElect()
		return
	}

	//rf.wg.Wait()
	//println("it ends")
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
	//println(&rf)
	rf.peers = peers
	rf.persister = persister
	rf.applymsg = applyCh
	//rf.mss = make(chan bool)

	// Your initialization code here.
	rf.me = me //index of this server
	rf.isgetHeart = false
	rf.CurrentTerm = 0
	rf.votedFor = -1
	rf.state = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	for range rf.peers {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 0)
	}

	mylog := RaftLog{
		Command: 0,
		Term:    0,
	}
	rf.log = append(rf.log, mylog)
	//rf.log = append(rf.log, 1)

	//println(me)   //0,1,2 have three server and the make is to every one
	//print(len(peers))

	go func() {
		time.Sleep(time.Millisecond * (100 * time.Duration(me+1)))
		if rf.votedFor == -1 {
			rf.Convert(1)
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
