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

	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const RPCTYPE_APP = 0
const RPCTYPE_HEARTBEAT = 1
const RPCTYPE_APPREPLY = 2
const RPC_TYPE_HEARTBEATREPLY = 3

const ROLE_FOLLOWER = 0
const ROLE_CANDIDATE = 1
const ROLE_LEADER = 2

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term         int
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	dead   int32 // set by Kill()
	deadch chan struct{}

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistant state
	CurrentTerm   int
	VotedFor      int // 当前term获得选票的candidate的Id
	Log           map[int]Entry
	CurrentLeader int

	// volatile state
	commitIndex   int
	lastApplied   int
	role          int
	electionTimer bool
	applySignal   chan struct{} // 用于通知applyCommand协程提交entry

	// for snapshot
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
	// volatile state as leader
	nextIndex  []int
	matchIndex []int

	// for debug
	electionThreadNum int
	leaderThreadNum   int
	handleAEThreadNum int
	handleISThreadNum int
	applyThreadNum    int
	tickerThreadNum   int

	// for kvserver
	isNewLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.role == ROLE_LEADER
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
	// Your code here (3C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentLeader)
	e.Encode(len(rf.Log))
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	for _, entry := range rf.Log {
		e.Encode(entry)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		CurrentTerm       int
		VotedFor          int
		CurrentLeader     int
		LogLen            int
		lastIncludedIndex int
		lastIncludedTerm  int
	)

	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&CurrentLeader) != nil ||
		d.Decode(&LogLen) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("Decode error\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.CurrentLeader = CurrentLeader
		rf.Log = make(map[int]Entry)
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		for i := 0; i < LogLen; i++ {
			e := Entry{}
			if d.Decode(&e) != nil {
				log.Fatal("Decode error\n")
			}
			rf.Log[e.CommandIndex] = e
		}
	}
	rf.snapshot = rf.persister.ReadSnapshot()

	if Debug {
		DFPrintf(rf.me, "---read from persist--- (currentTerm: %v, read %v(%v) entries, %v bytes snapshot)\n",
			rf.CurrentTerm, len(rf.Log), LogLen, len(rf.snapshot))
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < index || rf.lastIncludedIndex > index {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.Log[index].Term
	i := index
	for {
		if _, ok := rf.Log[i]; !ok {
			break
		}
		delete(rf.Log, i)
		i--
	}
	rf.snapshot = clone(snapshot)
	if Debug {
		DFPrintf(rf.me, "(%v) server %v snapshot, lastIncludedIndex: %v, log has %v entries after snapshot\n", rf.CurrentTerm, rf.me, index, len(rf.Log))
	}

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate的term
	CandidateId  int // candidate的Id
	LastLogIndex int // candidate的最后一个log entry对应的index
	LastLogTerm  int // candidate的最后一个log entry对应的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Type         int
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      map[int]Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Type    int
	Id      int
	Term    int
	Success bool
	// for optimizing nextIndex back up
	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		if Debug {
			DFPrintf(rf.me, "(%v) server %v reject voting because of lower term\n", rf.CurrentTerm, rf.me)
		}
		return
	}
	// 一旦看到比currentTerm大的RPC，直接更新currentTerm
	// 同时重置rf.VotedFor
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.role = ROLE_FOLLOWER
	}

	maxIndex := len(rf.Log) + rf.lastIncludedIndex
	maxTerm := max(rf.lastIncludedTerm, rf.Log[maxIndex].Term)

	// 判断投票的具体条件
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && // 一个term内只能为一个candidate(可以是别人，也可以是自己)投票
		(maxIndex == 0 || maxTerm < args.LastLogTerm || (maxTerm == args.LastLogTerm && maxIndex <= args.LastLogIndex)) { // candiate的log要足够新
		rf.VotedFor = args.CandidateId
		rf.electionTimer = true // 如果投票成功，则重置自己的election timer，表示自己不参加本轮竞选

		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		if Debug {
			DFPrintf(rf.me, "(%v) server %v vote for %v\n", rf.CurrentTerm, rf.me, args.CandidateId)
		}
	} else {
		if Debug {
			DFPrintf(rf.me, "(%v) server %v reject voting because of outdated log (candidate's lastLogIndex: %v, lastLogTerm: %v; rf.maxIndex: %v, maxTerm: %v)\n",
				rf.CurrentTerm, rf.me, args.LastLogIndex, args.LastLogTerm, maxIndex, maxTerm)
		}
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("\nserver %v get:\n\t%v\n\n", rf.me, *args)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	rf.electionTimer = true
	rf.CurrentLeader = args.LeaderId

	if args.Term > rf.CurrentTerm {
		rf.role = ROLE_FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	} else if rf.role == ROLE_CANDIDATE {
		rf.role = ROLE_FOLLOWER
	}

	entry, ok := rf.Log[args.PrevLogIndex]
	highestIdx := args.PrevLogIndex + len(args.Entries) // 此次复制的最高位置

	// 满足两个条件则进行复制：
	// 1. 前面的entry一致 (通过PrevLogTerm和PrevLogIndex检查)
	// 2. 此次的数据包足够新
	if (highestIdx <= rf.lastIncludedIndex) ||
		(args.PrevLogIndex == 0) ||
		(ok && entry.Term == args.PrevLogTerm) ||
		(rf.lastIncludedIndex == args.PrevLogIndex && rf.lastIncludedTerm == args.PrevLogTerm) ||
		(rf.lastIncludedIndex > args.PrevLogIndex && args.Entries[rf.lastIncludedIndex].Term == rf.lastIncludedTerm) {

		if ety, exist := rf.Log[highestIdx]; len(args.Entries) > 0 && (!exist || ety != args.Entries[highestIdx]) {
			oldLen := len(rf.Log)
			for idx, e := range args.Entries {
				if idx > rf.lastIncludedIndex {
					rf.Log[idx] = e
				}
			}
			j := highestIdx + 1
			for {
				if _, ok := rf.Log[j]; !ok {
					break
				}
				delete(rf.Log, j)
				j++
			}

			rf.persist()

			if Debug {
				DFPrintf(rf.me, "(%v) server %v append %v entries from leader, from index %v to %v (lastIncludedIndex: %v, lastIncludedTerm: %v)\n\tbefore append: %v entries, after append: %v entries, contents:\n%v",
					rf.CurrentTerm, rf.me, len(args.Entries), args.PrevLogIndex+1, highestIdx, rf.lastIncludedIndex, rf.lastIncludedTerm, oldLen, len(rf.Log), LogFmtString(args.Entries))
			}
		}

		newCommitIndex := min(args.LeaderCommit, len(rf.Log)+rf.lastIncludedIndex)
		if rf.commitIndex < newCommitIndex {
			rf.commitIndex = newCommitIndex
			go func() {
				select {
				case rf.applySignal <- struct{}{}:
				case <-time.After(1 * time.Second):
				}
			}()
		}

		reply.Id = rf.me
		reply.Success = true
		reply.Term = rf.CurrentTerm
		reply.XIndex = highestIdx

	} else {
		if !ok {
			reply.XLen = len(rf.Log) + rf.lastIncludedIndex
		} else {
			j := args.PrevLogIndex
			for rf.Log[j].Term == entry.Term && j > rf.lastIncludedIndex {
				j--
			}
			reply.XTerm = entry.Term
			reply.XIndex = j + 1
			reply.XLen = -1
		}

		reply.Id = rf.me
		reply.Success = false
		reply.Term = rf.CurrentTerm
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}

	rf.CurrentLeader = args.LeaderId
	if rf.commitIndex < args.LastIncludedIndex {
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = clone(args.Data)
		rf.commitIndex = args.LastIncludedIndex
		rf.Log = make(map[int]Entry)
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}

		rf.persist()
		go func() {
			select {
			case rf.applySignal <- struct{}{}:
			case <-time.After(1 * time.Second):
			}
		}()

		if Debug {
			DFPrintf(rf.me, "(%v) server %v has copy snapshot from leader, lastIncludedIndex: %v, lastIncludedTerm: %v\n",
				rf.CurrentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
		}
	}
	reply.Term = rf.CurrentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) GetCurrentLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentLeader
}

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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果rf不是leader，直接返回
	if rf.role != ROLE_LEADER {
		term := rf.CurrentTerm
		return -1, term, false
	}

	index := len(rf.Log) + rf.lastIncludedIndex + 1
	rf.Log[index] = Entry{
		Term:         rf.CurrentTerm,
		Command:      command,
		CommandIndex: index,
	}
	term := rf.CurrentTerm
	isLeader := true
	rf.persist()

	if Debug {
		DFPrintf(rf.me, "(%v) leader %v append entry: %v (index: %v)\n",
			rf.CurrentTerm, rf.me, rf.Log[index], index)
	}

	return index, term, isLeader
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
	go func() {
		for {
			select {
			case rf.deadch <- struct{}{}:
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(ctx context.Context) {
	// if Debug {
	// 	rf.mu.Lock()
	// 	rf.electionThreadNum++
	// 	n := rf.electionThreadNum
	// 	rf.mu.Unlock()
	// 	DFPrintf(rf.me, "%v-th startElection() thread start ...\n", n)
	// 	defer func() {
	// 		DFPrintf(rf.me, "%v-th startElection() thread return\n", n)
	// 	}()
	// }

	rf.mu.Lock()
	// 可能该协程早已开启，但是由于竞争锁而没有开始执行
	// 如果恰好在竞争锁的时间内为其他candidate投票，则自己放弃本轮竞选
	if rf.electionTimer {
		rf.mu.Unlock()
		log.Printf("server %v's election timer has been reset (term: %v)\n", rf.me, rf.CurrentTerm)
		return
	}

	rf.role = ROLE_CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me

	rf.persist()
	// if Debug {
	// 	DFPrintf(rf.me, "(%v) server %v start election...\n", rf.CurrentTerm, rf.me)
	// }

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) + rf.lastIncludedIndex,
		LastLogTerm:  max(0, rf.lastIncludedTerm, rf.Log[len(rf.Log)+rf.lastIncludedIndex].Term),
	}

	// 向其余成员发送RequestVoteRPC
	ch := make(chan RequestVoteReply)
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			select {
			case ch <- reply:
			case <-time.After(1 * time.Second):
			}
		}(s)
	}
	t := rf.CurrentTerm
	rf.mu.Unlock()

	// 收集选票
	voteCount := 1 // 初始票数为1，自己投的
	finished := 1
	done := false
	for !done {
		select {
		case v := <-ch:
			if t < v.Term {
				rf.mu.Lock()
				rf.CurrentTerm = v.Term
				rf.VotedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if v.VoteGranted {
				voteCount++
			}
			finished++
			if voteCount > len(rf.peers)/2 || finished >= len(rf.peers) {
				done = true
			}
		case <-ctx.Done():
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 为什么要判断rf.role == ROLE_CANDIDATE？
	// 因为如果在这之前收到其他leader的heartbeat，该server会直接转变为follower
	if rf.role != ROLE_CANDIDATE {
		return
	}
	// if Debug {
	// 	DFPrintf(rf.me, "(%v) server %v got %v+ votes\n", rf.CurrentTerm, rf.me, voteCount)
	// }

	// 如果返回的term大于自身的term，则直接转变为follower
	if voteCount > len(rf.peers)/2 {
		rf.role = ROLE_LEADER
		// 初始化 nextIndex, matchIndex
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = len(rf.Log) + rf.lastIncludedIndex + 1
		}
		// fmt.Printf("server %v becomes leader\n", rf.me)
		go rf.doLeaderJob()
	} else {
		rf.role = ROLE_FOLLOWER
	}
}

func (rf *Raft) handleAppendEntriesReply(ch chan AppendEntriesReply, term int) {
	count := make(map[int]map[int]struct{})

	for {
		select {
		case reply := <-ch:
			rf.mu.Lock()
			if rf.role != ROLE_LEADER || rf.killed() {
				rf.mu.Unlock()
				return
			}
			if reply.Term > term {
				rf.CurrentTerm = reply.Term
				rf.role = ROLE_FOLLOWER
				rf.VotedFor = -1
				rf.CurrentLeader = -1
				rf.persist()
				rf.mu.Unlock()
				return
			} else if reply.Success {
				if reply.XIndex > rf.matchIndex[reply.Id] {

					for i := reply.XIndex; i >= rf.nextIndex[reply.Id] && rf.Log[i].Term == rf.CurrentTerm && i > rf.commitIndex; i-- {
						if _, ok := count[i]; !ok {
							count[i] = make(map[int]struct{})
							count[i][rf.me] = struct{}{}
						}
						if _, ok := count[i][reply.Id]; !ok {
							count[i][reply.Id] = struct{}{}
						}
						if len(count[i]) > len(rf.peers)/2 {
							rf.commitIndex = i
							j := i
							for {
								if _, ok := count[j]; !ok {
									break
								}
								delete(count, j)
								j--
							}

							go func() {
								select {
								case rf.applySignal <- struct{}{}:
								case <-time.After(1 * time.Second):
								}
							}()
						}
					}
					rf.nextIndex[reply.Id] = reply.XIndex + 1
					rf.matchIndex[reply.Id] = reply.XIndex
				}
				// if Debug {
				// 	DFPrintf(rf.me, "(%v) leader %v receives reply (success): %v has replicated to index %v, nextIndex[%v] updated to %v\n",
				// 		rf.CurrentTerm, rf.me, reply.Id, reply.XIndex, reply.Id, rf.nextIndex[reply.Id])
				// }

			} else if !reply.Success && reply.Term != 0 {
				// reply.Term = 0 意味着follower没有接收到这条RPC
				oldNextIdx := rf.nextIndex[reply.Id]

				if reply.XTerm > 0 {
					j := rf.nextIndex[reply.Id]
					for j > rf.lastIncludedIndex && rf.Log[j].Term > reply.XTerm {
						j--
					}

					if j > rf.lastIncludedIndex && rf.Log[j].Term == reply.XTerm {
						rf.nextIndex[reply.Id] = max(j, rf.matchIndex[reply.Id]+1)
					} else {
						rf.nextIndex[reply.Id] = max(rf.matchIndex[reply.Id]+1, reply.XIndex)
					}
				} else {
					rf.nextIndex[reply.Id] = max(reply.XLen, rf.matchIndex[reply.Id]+1)
				}

				if Debug {
					DFPrintf(rf.me, "(%v) leader %v receives reply (fail): nextIndex[%v] back from %v to %v\n",
						rf.CurrentTerm, rf.me, reply.Id, oldNextIdx, rf.nextIndex[reply.Id])
				}
				// rf.nextIndex[reply.Id]--
			}
			rf.mu.Unlock()

		case <-rf.deadch:
			return
		}
	}
}

func (rf *Raft) handleInstallSnapshotReply(ch chan InstallSnapshotReply, term int) {
	// if Debug {
	// 	rf.mu.Lock()
	// 	rf.handleISThreadNum++
	// 	n := rf.handleISThreadNum
	// 	rf.mu.Unlock()
	// 	DFPrintf(rf.me, "%v-th handleInstallSnapshotReply() thread start ...\n", n)
	// 	defer func() {
	// 		DFPrintf(rf.me, "%v-th handleInstallSnapshotReply() thread return\n", n)
	// 	}()
	// }

	for {
		select {
		case reply := <-ch:
			rf.mu.Lock()
			if rf.role != ROLE_LEADER || rf.killed() {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.CurrentLeader = -1
				rf.role = ROLE_FOLLOWER
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

		case <-rf.deadch:
			return
		}
	}

}

// leader定时发送entries，如果没有entry，就发送空的数据包当作heartbeat
func (rf *Raft) doLeaderJob() {
	// if Debug {
	// 	rf.mu.Lock()
	// 	rf.leaderThreadNum++
	// 	n := rf.leaderThreadNum
	// 	rf.mu.Unlock()
	// 	DFPrintf(rf.me, "%v-th doLeaderJob() thread start ...\n", n)
	// 	defer func() {
	// 		DFPrintf(rf.me, "%v-th doLeaderJob() thread return\n", n)
	// 	}()
	// }

	rf.mu.Lock()
	rf.isNewLeader = true

	if Debug {
		DFPrintf(rf.me, "(%v) server %v becomes leader\n", rf.CurrentTerm, rf.me)
	}

	// 启动两个协程分别监听并处理reply
	rChAE := make(chan AppendEntriesReply)
	rChIS := make(chan InstallSnapshotReply)
	go rf.handleAppendEntriesReply(rChAE, rf.CurrentTerm)
	go rf.handleInstallSnapshotReply(rChIS, rf.CurrentTerm)

	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != ROLE_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for s := 0; s < len(rf.peers); s++ {
			rf.send(s, rChAE, rChIS)
		}

		time.Sleep(15 * time.Millisecond)
	}
}

func (rf *Raft) send(server int, rChAE chan AppendEntriesReply, rChIS chan InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != ROLE_LEADER {
		return
	}
	if server == rf.me {
		rf.electionTimer = true
		return
	}
	// 发送snapshot
	if rf.nextIndex[server] <= rf.lastIncludedIndex && rf.lastIncludedIndex > 0 {
		argsIS := InstallSnapshotArgs{
			Term:              rf.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              clone(rf.snapshot),
		}
		replyIS := InstallSnapshotReply{}
		go func() {
			rf.sendInstallSnapshot(server, &argsIS, &replyIS)

			select {
			case rChIS <- replyIS:
			case <-time.After(1 * time.Second):
			}
		}()

		rf.nextIndex[server] = rf.lastIncludedIndex + 1

		if Debug {
			DFPrintf(rf.me, "(%v) leader %v send InstallSnapshot RPC to %v\n", rf.CurrentTerm, rf.me, server)
		}
	}

	// 发送AppendEntries或Heartbeat
	argsAE := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	argsAE.PrevLogIndex = rf.nextIndex[server] - 1
	if argsAE.PrevLogIndex == rf.lastIncludedIndex {
		argsAE.PrevLogTerm = rf.lastIncludedTerm
	} else {
		e, ok := rf.Log[argsAE.PrevLogIndex]
		if !ok {
			log.Fatalf("prevLogIndex error: server %v does not have an entry at index %v\n", rf.me, argsAE.PrevLogIndex)
		}
		argsAE.PrevLogTerm = e.Term
	}
	entries := make(map[int]Entry)
	for i := rf.nextIndex[server]; i <= len(rf.Log)+rf.lastIncludedIndex; i++ {
		entries[i] = rf.Log[i]
	}
	argsAE.Entries = entries

	replyAE := AppendEntriesReply{}
	go func() {
		rf.sendAppendEntries(server, &argsAE, &replyAE)
		select {
		case rChAE <- replyAE:
		case <-time.After(1 * time.Second):
		}
	}()

}

func (rf *Raft) ticker() {
	// if Debug {
	// 	rf.mu.Lock()
	// 	rf.tickerThreadNum++
	// 	n := rf.tickerThreadNum
	// 	rf.mu.Unlock()
	// 	DFPrintf(rf.me, "%v-th ticker() thread start ...\n", n)
	// 	defer func() {
	// 		DFPrintf(rf.me, "%v-th ticker() thread return\n", n)
	// 	}()
	// }

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// 检查electionTimer
		rf.mu.Lock()
		ms := 350 + (rand.Int63() % 350)

		if rf.electionTimer {
			rf.electionTimer = false
			rf.mu.Unlock()
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			rf.mu.Unlock()
			ctx, cancle := context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond)
			go rf.startElection(ctx)
			<-ctx.Done()
			cancle()
		}
	}
}

func (rf *Raft) applyCommand(applyCh chan ApplyMsg) {
	if Debug {
		rf.mu.Lock()
		rf.applyThreadNum++
		n := rf.applyThreadNum
		rf.mu.Unlock()
		DFPrintf(rf.me, "%v-th applyCommand() thread start ...\n", n)
		defer func() {
			DFPrintf(rf.me, "%v-th applyCommand() thread return\n", n)
		}()

	}

	for {
		select {
		case <-rf.applySignal:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				if Debug {
					DFPrintf(rf.me, "(%v) server %v update commitIndex to %v (lastApplied: %v, commitIndex: %v, lastIncludedIndex: %v), start committing...\n",
						rf.CurrentTerm, rf.me, rf.commitIndex, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex)
				}

				if rf.lastIncludedIndex > rf.lastApplied {
					applyMsg := ApplyMsg{
						SnapshotValid: true,
						SnapshotTerm:  rf.lastIncludedTerm,
						SnapshotIndex: rf.lastIncludedIndex,
						Snapshot:      rf.snapshot,
					}
					rf.mu.Unlock()
					applyCh <- applyMsg

					rf.mu.Lock()
					rf.lastApplied = rf.lastIncludedIndex
					if Debug {
						DFPrintf(rf.me, "(%v) server %v commit snapshot, lastIncludedIndex: %v\n", rf.CurrentTerm, rf.me, rf.lastIncludedIndex)
					}
					rf.mu.Unlock()

				} else {
					lastApplied := rf.lastApplied
					commitIndex := rf.commitIndex
					rf.mu.Unlock()

					for i := lastApplied + 1; i <= commitIndex; i++ {
						rf.mu.Lock()
						if i <= rf.lastIncludedIndex {
							rf.mu.Unlock()
							continue
						}

						e, ok := rf.Log[i]
						if !ok {
							rf.mu.Unlock()
							log.Fatalf("server %v commit error: cannot find log entry, index: %v\n", rf.me, i)
						} else {
							rf.lastApplied++
							rf.mu.Unlock()
							applyCh <- ApplyMsg{
								CommandValid: true,
								CommandIndex: e.CommandIndex,
								Command:      e.Command,
							}

							if Debug {
								DFPrintf(rf.me, "\tserver %v commit %v (index: %v)\n", rf.me, e.Command, e.CommandIndex)
							}
						}

					}
				}
			} else {
				rf.mu.Unlock()
			}

		case <-rf.deadch:
			return
		}

	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make(map[int]Entry)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimer = true
	rf.applySignal = make(chan struct{})
	rf.role = ROLE_FOLLOWER
	rf.CurrentLeader = -1
	rf.deadch = make(chan struct{})

	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// for debug
	rf.electionThreadNum = 0
	rf.leaderThreadNum = 0
	rf.handleAEThreadNum = 0
	rf.handleISThreadNum = 0
	rf.applyThreadNum = 0
	rf.tickerThreadNum = 0

	rf.isNewLeader = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommand(applyCh)

	return rf
}
