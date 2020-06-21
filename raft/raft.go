// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	// candidate id that received vote in this term
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval, will be set randomly according to configElectionTimeout
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// back-up the config election time out
	configElectionTimeout int
}

func (r *Raft) randElectionTimeout() {
	r.electionTimeout = GetRandomBetween(r.configElectionTimeout, 2*r.configElectionTimeout)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := new(Raft)
	r.configElectionTimeout = c.ElectionTick
	r.heartbeatTimeout = c.HeartbeatTick
	// set rand election time out
	r.randElectionTimeout()
	r.id = c.ID
	r.State = StateFollower
	// restore or new
	r.RaftLog = newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	// restore conf
	var ps []uint64
	if len(c.peers) == 0 && len(confState.Nodes) != 0 {
		ps = confState.Nodes
	} else {
		ps = c.peers
	}
	r.Prs = make(map[uint64]*Progress)
	r.votes = make(map[uint64]bool)
	for _, v := range ps {
		r.Prs[v] = new(Progress)
		r.Prs[v].Next = r.RaftLog.LastIndex() + 1
		r.Prs[v].Match = 0
		r.votes[v] = false
	}
	return r
}

// sendAppendToAll will send AppendEntry RPC to all followers
func (r *Raft) sendAppendToAll() {
	ns := nodes(r)
	for _, v := range ns {
		if v == r.id {
			continue
		}
		// TODO temporarily ignore the response
		r.sendAppend(v)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	// send entry according to the progress
	p := r.Prs[to]
	if p == nil {
		return false
	}
	nxt := p.Next
	var preIdx uint64
	if nxt > 0 {
		preIdx = nxt - 1
	} else {
		preIdx = 0
	}
	msg.LogTerm = r.RaftLog.entries[preIdx].Term
	msg.Index = r.RaftLog.entries[preIdx].Index
	entsNeeded := r.RaftLog.logsNeeded(nxt)
	for i := range entsNeeded {
		msg.Entries = append(msg.Entries, &entsNeeded[i])
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// construct and send append response msg
func (r *Raft) sendAppendResponse(to uint64, term uint64, approved bool) {
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    term,
		// TODO [Optimization] we can send follower's current log term and idx to leader
		LogTerm: 0,
		Index:   0,
		Reject:  !approved,
	}
	r.msgs = append(r.msgs, response)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	leaderTerm := m.Term
	preLogIdx := m.Index
	preLogTerm := m.LogTerm
	leadErCommitted := m.Commit
	to := m.From
	appendEntries := make([]pb.Entry, len(m.Entries))
	for i := range m.Entries {
		appendEntries = append(appendEntries, *m.Entries[i])
	}
	if leaderTerm > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if leaderTerm < r.Term  {
		// if request term < current term, reject
		r.sendAppendResponse(m.From, r.Term, false)
	} else {
		accept, err := r.RaftLog.followerTryAppendLog(appendEntries, preLogIdx, preLogTerm, leadErCommitted)
		if err != nil {
			log.Errorf("Handel append entry RPC fail. [id:%d],[%s]", r.id, err.Error())
		}
		r.sendAppendResponse(to, r.Term, accept)
	}
}

// handleAppendResponse handles the response of the append entry request
func (r *Raft) handleAppendResponse(m pb.Message) {
	fId := m.From
	accept := !m.Reject
	pro := r.Prs[fId]
	// if accepted, change the next and match
	if accept {
		pro.Match = pro.Next
		pro.Next++
	} else {
		if pro.Next-1 == 0 {
			panic("ERROR: the follow should take the entries but did not.")
		}
		// if did not accept, the leader will move "next" one step backward
		pro.Next--
		pro.Match = min(pro.Next-1, pro.Match)
	}
	// update commit according to each follower's progress
	r.checkAndUpdateCommitIdx()
}

// update commit according to each follower's progress
func (r *Raft) checkAndUpdateCommitIdx() {
	count := 0
	majority := (len(r.Prs) + 1) / 2
	// found the highest N for committed num
	committedIdx := r.RaftLog.committed
	lastLogIdx := r.RaftLog.LastIndex()
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	var N uint64
	for N = lastLogIdx; N > committedIdx; N-- {
		term, err := r.RaftLog.Term(N)
		if err != nil {
			log.Error(err)
			return
		}
		// if the term is less than current term , there is no need to continue
		if term < r.Term {
			return
		}
		for i, pro := range r.Prs {
			if r.id == i || pro.Match >= N {
				count++
			}
		}
		if count >= majority {
			// success and update the committed
			r.RaftLog.leaderUpdateCommitted(N)
			break
		} else {
			// continue check
			count = 0
		}
	}
}

// send heartbeat to others except itself
func (r *Raft) sendHeartbeatToAll() {
	r.checkAndUpdateCommitIdx()
	ns := nodes(r)
	for _, v := range ns {
		if v == r.id {
			continue
		}
		r.sendHeartbeat(v)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// TODO to see whether can be removed
	msg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) startElection() {
	r.Vote = r.id
	ns := nodes(r)
	for _, v := range ns {
		if v == r.id {
			// vote for itself
			r.votes[v] = true
			continue
		}
		r.sendRequestVote(v)
	}
	// when only have one node, 100% elected
	result := r.checkVoteResult()
	if result {
		r.becomeLeaderAndCommitNoop()
	}
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIdx)
	msg := pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgRequestVote,
		// this current term is already + 1
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIdx,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHup will send a Hup RPC to local
// If an election timeout happened,
// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
func (r *Raft) createLocalMsgHup() pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		To:      r.id,
		From:    r.id,
	}
	return msg
}

// when leader receive this msg, it will send heartbeat to all
func (r *Raft) createLocalBeat() pb.Message {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		To:      r.id,
		From:    r.id,
	}
	return msg
}

// tick advances the internal logical clock by a single tick.
// Your Code Here (2A).
// let the logic time plus one
// electionTimeout, become candidate and start new election
func (r *Raft) tick() {
	switch r.State {
	case StateCandidate, StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			log.Debugf("Election Timeout for Raft(%d).", r.id)
			err := r.Step(r.createLocalMsgHup())
			// TODO better way to handel err
			if err != nil {
				log.Error(err)
			}
		}else {
			r.electionElapsed++
		}
	case StateLeader:
		if r.heartbeatElapsed + 1 == r.heartbeatTimeout {
			log.Debugf("HeartBeat Time out for leader Raft(%d)", r.id)
			err := r.Step(r.createLocalBeat())
			// TODO better way to handel err
			if err != nil {
				log.Error(err)
			}
		}else{
			r.heartbeatElapsed++

		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// reset time elapsed
	r.cleanState()
	r.randElectionTimeout()
	r.State = StateFollower
	r.Lead = lead
	// only update vote when update term
	r.Term = term
	r.Vote = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.randElectionTimeout()
	// reset time elapsed and clean vote record
	r.cleanState()
	// increase term
	r.Term++
}

// this func will make state transfer to candidate and start election
func (r *Raft) becameCandidateAndStartElection(){
	r.becomeCandidate()
	r.startElection()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.cleanState()
	r.cleanProgress()
}

// this func will change the state and also same some msg
func (r *Raft) becomeLeaderAndCommitNoop(){
	r.becomeLeader()
	r.RaftLog.leaderAppendNoopLog(r.Term)
	r.sendAppendToAll()
}

func (r *Raft) handelEntryPropose(entries []*pb.Entry) {
	// TODO temporarily support only append one entry
	r.RaftLog.leaderAppendLogEntry(r.Term, entries[0].Data)
}

func (r *Raft) cleanProgress() {
	ns := nodes(r)
	for _, v := range ns {
		if v == r.id {
			continue
		}
		r.Prs[v] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
}

func (r *Raft) cleanState() {
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// check heartBeat timeout
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHup:
			// this msg means should transfer to candidate and start election
			r.becameCandidateAndStartElection()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// election time out, become candidate and start election
			r.becameCandidateAndStartElection()
		case pb.MessageType_MsgHeartbeat:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				log.Info("Others has higher term. So back to follower from candidate.")
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				log.Info("Others has higher term. So back to follower from candidate.")
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
			// win the election
			if r.checkVoteResult() {
				r.becomeLeaderAndCommitNoop()
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.sendHeartbeatToAll()
			// reset heartbeat elapsed
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgPropose:
			r.handelEntryPropose(m.Entries)
		case pb.MessageType_MsgAppendResponse:
			// when the msg contains higher term, change to follower
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				log.Info("Others has higher term. So back to follower from leader.")
				return nil
			}
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	}
	return nil
}

// handleVoteResponse handel the response RPC of vote
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		// ignore
		return
	}
	if m.Term > r.Term {
		// if response's term greater than itself, become follower
		r.becomeFollower(m.Term, m.From)
	}
	if r.State == StateLeader {
		// if already been elected, just ignore
		return
	}
	if !m.Reject {
		r.votes[m.From] = !m.Reject
	}
}

// check Vote result after every time get the vote response
// return whether been elected
func (r *Raft) checkVoteResult() (elected bool) {
	elected = false
	if len(r.Prs) % 2 == 0 {
		// cannot be elected when the machine number is even
		return
	}
	majority := (len(r.Prs) + 1) / 2
	count := len(r.votes)
	elected = count >= majority
	return
}

// send the Vote result response to leader
func (r *Raft) sendVoteResponse(to uint64, term uint64, isVote bool) {
	response := pb.Message{
		From:    r.id,
		To:      to,
		Term:    term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  !isVote,
	}
	r.msgs = append(r.msgs, response)
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	// ignore if the request term <= current term
	if m.Term < r.Term {
		// reject
		// reply the higher term
		r.sendVoteResponse(m.From, r.Term, false)
		return
	}
	if m.Term > r.Term {
		// if get higher term, become follower
		log.Info("Others has higher term. So back to follower from leader.")
		r.becomeFollower(m.Term, m.From)
	}
	// if it is already vote in this term
	if r.Vote != 0 {
		if r.Vote == m.From {
			r.sendVoteResponse(m.From, m.Term, true)
		}else{
			r.sendVoteResponse(m.From, m.Term, false)
		}
		return
	}
	// has not vote in this term
	// compare the log term and index
	lastLogIDx := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIDx)
	if (m.LogTerm > lastLogTerm) || (m.LogTerm == lastLogTerm && m.Index >= lastLogIDx) {
		// 1. vote for it -> return (reject = false)
		// 2. update the term
		// 3. save the vote record
		r.Vote = m.From
		r.sendVoteResponse(m.From, m.Term, true)
	}else{
		r.sendVoteResponse(m.From, m.Term, false)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// if the heart beat from the node has higher term,
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// reset election timeout
	r.randElectionTimeout()
	r.electionElapsed = 0
	// check and update committed
	r.RaftLog.followerUpdateCommitted(m.Commit)
	// send response
	response := pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	r.msgs = append(r.msgs, response)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
