// Core Raft implementation - Consensus Module.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// and contributors.
// This code is in the public domain.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("unreachable")
	}
}

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command and it can
// be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command is committed.
	Term int
}

type ConsensusProxy struct {
	// Chan to send tasks to run on the main loop.
	taskQueue chan func()

	// id is the server ID of the corresponding CM.
	id int

	// Chan to signal exit
	exit chan bool

	// A pointer to the CM,
	// used to send tasks to run on the main loop
	cm *ConsensusModule
}

// Submit submits a new command to the CM.
func (proxy *ConsensusProxy) Submit(command interface{}) bool {
	done := make(chan bool)
	cm := proxy.cm
	handler := func() {
		cm.dlog("Submit received by %v: %v", cm.state, command)
		if cm.state == Leader {
			cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
			cm.dlog("... log=%v", cm.log)
			done <- true
			return
		}
		done <- false
	}
	select {
	case <-proxy.exit:
		return false
	default:
		proxy.taskQueue <- handler
		return <-done
	}
}

func (proxy *ConsensusProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	done := make(chan interface{})
	cm := proxy.cm
	handler := func() {
		cm.dlog("got voteRequest %+v", args)
		done <- cm.RequestVote(args, reply)
	}
	select {
	case <-proxy.exit:
		return nil
	default:
		proxy.taskQueue <- handler
		<-done
		return nil
	}
}

func (proxy *ConsensusProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	done := make(chan interface{})
	cm := proxy.cm
	handler := func() {
		cm.dlog("got appendEntriesRequest %+v", args)
		done <- cm.AppendEntries(args, reply)
	}
	select {
	case <-proxy.exit:
		return nil
	default:
		proxy.taskQueue <- handler
		<-done
		return nil
	}
}

func (proxy *ConsensusProxy) Stop() {
	close(proxy.exit)
}

type Report struct {
	term     int
	isLeader bool
}

// Report reports the state of this CM.
func (proxy *ConsensusProxy) Report() (id int, term int, isLeader bool) {
	responseChan := make(chan Report)
	cm := proxy.cm
	handler := func() {
		term, isLeader := cm.Report()
		report := Report{
			term,
			isLeader,
		}
		responseChan <- report
	}
	select {
	case <-proxy.exit:
		return -1, -1, false
	default:
		proxy.taskQueue <- handler
		report := <-responseChan
		return proxy.id, report.term, report.isLeader
	}
}

// dlog logs a debugging message is DebugCM > 0.
func (proxy *ConsensusProxy) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", proxy.id) + format
		log.Printf(format, args...)
	}
}

// The CommitBuffer is responsible for the blocking sends
// of newly committed entries to the client.
// It pulls new entries from the CM whenever there is capacity.
type CommitBuffer struct {
	// Chan to send tasks to run on the main loop.
	taskQueue chan func()

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// A chan to receive newly committed entries.
	newCommitsChan chan []CommitEntry

	// id is the server ID of the corresponding CM.
	id int

	// Chan to signal exit
	exit chan bool

	// A pointer to the CM,
	// used to sending tasks to run on the main loop
	cm *ConsensusModule
}

func (cb *CommitBuffer) commitChanSender() {
	for {
		select {
		case entries := <-cb.newCommitsChan:
			for _, entry := range entries {
				cb.commitChan <- entry
			}

			// Ask for the next batch
			cm := cb.cm
			handler := func() {
				cm.canSendCommits = true
				cm.maybeSendCommits()
			}
			cb.taskQueue <- handler

		case <-cb.exit:
			return
		}
	}
	cb.dlog("commitChanSender done")
}

// dlog logs a debugging message is DebugCM > 0.
func (proxy *CommitBuffer) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", proxy.id) + format
		log.Printf(format, args...)
	}
}

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {

	// Chan to receive tasks to run on the main loop.
	taskQueue chan func()

	// Chan to signal exit
	exit chan bool

	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this CM. It's used to issue RPC calls
	// to peers.
	server *Server

	// The timeout for the current election round
	timeoutDuration time.Duration

	// A ticker used for elections and leadership.
	ticker *time.Ticker

	// A chan to send newly committed entries to the buffer.
	newCommitsChan chan []CommitEntry

	// A flag set when the buffer is ready for more commits
	canSendCommits bool

	// Persistent Raft state on all servers
	currentTerm   int
	votesReceived int32
	votedFor      int
	log           []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusProxy {
	taskQueue := make(chan func())
	exit := make(chan bool)

	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower

	cm.newCommitsChan = make(chan []CommitEntry)
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	cm.taskQueue = taskQueue
	cm.exit = exit
	cm.canSendCommits = true

	cb := new(CommitBuffer)
	cb.id = cm.id
	cb.commitChan = commitChan
	cb.newCommitsChan = cm.newCommitsChan
	cb.taskQueue = cm.taskQueue
	cb.exit = cm.exit
	cb.cm = cm

	go cb.commitChanSender()

	// The run loop of the consensus module,
	// all consensus state is encapsulated within this goroutine.
	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown
		// for leader election.
		<-ready

		cm.electionResetEvent = time.Now()
		cm.resetElectionTimer()

		var leaderCountdown = 0
		cm.ticker = time.NewTicker(10 * time.Millisecond)
		defer cm.ticker.Stop()

		for {
			select {
			case <-cm.exit:
				cm.dlog("Stopping")
				return

			case task := <-cm.taskQueue:
				task()

			case <-cm.ticker.C:
				if cm.state == Leader {
					leaderCountdown++
					if leaderCountdown > 4 {
						leaderCountdown = 0
						cm.leaderSendHeartbeats()
					}
					continue
				}

				// Reset it if we're not the leader anymore.
				leaderCountdown = 0

				// Start an election if we haven't heard from a leader or haven't voted for
				// someone for the duration of the timeout.
				if elapsed := time.Since(cm.electionResetEvent); elapsed >= cm.timeoutDuration {
					cm.startElection()
				}
			}
		}
	}()

	return &ConsensusProxy{
		taskQueue,
		id,
		exit,
		cm,
	}
}

// Report reports the state of this CM.
func (cm *ConsensusModule) Report() (term int, isLeader bool) {
	return cm.currentTerm, cm.state == Leader
}

// dlog logs a debugging message is DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// See figure 2 in the paper.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// See figure 2 in the paper.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
		// vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.maybeSendCommits()
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// resetElectionTimer resets the election timer.
func (cm *ConsensusModule) resetElectionTimer() {
	cm.timeoutDuration = cm.electionTimeout()
	cm.dlog("election timer started (%v), term=%d", cm.timeoutDuration, cm.currentTerm)
}

// startElection starts a new election with this CM as a candidate.
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.votesReceived = 1

	savedCurrentTerm := cm.currentTerm
	exit := cm.exit
	taskQueue := cm.taskQueue
	id := cm.id
	server := cm.server
	savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()

	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			if err := server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				handler := func() {
					cm.dlog("received RequestVoteReply %+v", reply)

					if cm.state != Candidate {
						cm.dlog("while waiting for voteReply, state = %v", cm.state)
						return
					}

					if reply.Term > savedCurrentTerm {
						cm.dlog("term out of date in RequestVoteReply")
						cm.becomeFollower(reply.Term)
					} else if reply.Term == savedCurrentTerm {
						if reply.VoteGranted {
							cm.votesReceived++
							votes := int(cm.votesReceived)
							if votes*2 > len(cm.peerIds)+1 {
								// Won the election!
								cm.dlog("wins election with %d votes", votes)
								cm.startLeader()
								cm.votesReceived = 0
							}
						}
					}
				}
				select {
				case <-exit:
					return
				default:
					taskQueue <- handler
				}
			}
		}(peerId)
	}

	// Start another election timer, in case this election is not successful.
	cm.resetElectionTimer()
}

// becomeFollower makes cm a follower and resets its state.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	cm.resetElectionTimer()
}

// startLeader switches cm into a leader state and begins process of heartbeats.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)
	cm.leaderSendHeartbeats()
}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state.
func (cm *ConsensusModule) leaderSendHeartbeats() {
	savedCurrentTerm := cm.currentTerm
	exit := cm.exit
	server := cm.server
	taskQueue := cm.taskQueue

	for _, peerId := range cm.peerIds {
		ni := cm.nextIndex[peerId]
		prevLogIndex := ni - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = cm.log[prevLogIndex].Term
		}
		entries := cm.log[ni:]

		args := AppendEntriesArgs{
			Term:         savedCurrentTerm,
			LeaderId:     cm.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: cm.commitIndex,
		}
		go func(peerId int) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				handler := func() {
					if reply.Term > savedCurrentTerm {
						cm.dlog("term out of date in heartbeat reply")
						cm.becomeFollower(reply.Term)
						return
					}

					if cm.state == Leader && savedCurrentTerm == reply.Term {
						if reply.Success {
							cm.nextIndex[peerId] = ni + len(entries)
							cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
							cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

							savedCommitIndex := cm.commitIndex
							for i := cm.commitIndex + 1; i < len(cm.log); i++ {
								if cm.log[i].Term == cm.currentTerm {
									matchCount := 1
									for _, peerId := range cm.peerIds {
										if cm.matchIndex[peerId] >= i {
											matchCount++
										}
									}
									if matchCount*2 > len(cm.peerIds)+1 {
										cm.commitIndex = i
									}
								}
							}
							if cm.commitIndex != savedCommitIndex {
								cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
								cm.maybeSendCommits()
							}
						} else {
							cm.nextIndex[peerId] = ni - 1
							cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
						}
					}
				}
				select {
				case <-exit:
					return
				default:
					taskQueue <- handler
				}
			}
		}(peerId)
	}
}

// Send new commits to the buffer, if the buffer wants more commits,
// and if there are new commits.
func (cm *ConsensusModule) maybeSendCommits() {
	if !cm.canSendCommits {
		return
	}

	savedTerm := cm.currentTerm
	savedLastApplied := cm.lastApplied
	if cm.commitIndex > cm.lastApplied {
		var entries []CommitEntry
		for i, entry := range cm.log[cm.lastApplied+1 : cm.commitIndex+1] {
			entries = append(entries, CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			})
		}
		cm.newCommitsChan <- entries
		cm.lastApplied = cm.commitIndex
		cm.canSendCommits = false
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
