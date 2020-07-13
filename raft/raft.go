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
	"time"
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

type ElectionResult uint8

const (
	Elected ElectionResult = iota
	Fail
	Undecided
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
	// baseline of election interval, will be set randomly according to baseElectionTimeout
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
	baseElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := new(Raft)
	r.baseElectionTimeout = c.ElectionTick
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
	r.RaftLog.committed = hardState.Commit
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

// reset the election timeout to a random number
func (r *Raft) randElectionTimeout() {
	r.electionTimeout = GetRandomBetween(r.baseElectionTimeout, 2*r.baseElectionTimeout)
}

// check whether the applied index > pendingConfIndex
// if true, means this leader can accept new conf change propose
func (r *Raft) checkPendingConfChangeIdx() bool {
	return r.RaftLog.applied >= r.PendingConfIndex
}

// return the current HardState of this raft
func (r *Raft) currentHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) getCurrentLeader() uint64 {
	return r.Lead
}

func (r *Raft) getCurrentState() StateType {
	return r.State
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
	// if only one node
	r.checkAndUpdateCommitIdx()
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
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
	entsNeeded, compacted := r.RaftLog.logEntriesAfterNext(nxt)
	var msg *pb.Message
	var err error
	if compacted {
		// send the snapshot msg
		log.Debugf("%d need to send snapshot. send to %d , nxt:[%d]", r.id, to, nxt)
		if msg, err = r.sendSnapshotMsg(to, nxt); err != nil {
			log.Errorf("node %d fail to send snapshot msg: %s", r.id, err)
			return false
		}
	} else {
		// send normal append entries msg
		if msg, err = r.sendAppendEntriesMsg(preIdx, to, entsNeeded); err != nil {
			log.Errorf("node %d fail to send snapshot msg: %s", r.id, err)
			return false
		}
	}
	r.msgs = append(r.msgs, *msg)
	return true
}

func (r *Raft) sendSnapshotMsg(to, nxtIdx uint64) (*pb.Message, error) {
	// TODO can be change to using goRoutine to accelerated ?
	// check whether need to generating new snapshot
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	// if current cached snapshot not satisfied the requirement
	if !r.IsLastSnapshotOkForSending(nxtIdx) {
		for {
			log.Debugf("%d call Snapshot.", r.id)
			snapshot, err := r.RaftLog.storage.Snapshot()
			if err == nil {
				r.UpdateLastSnapshot(&snapshot)
				break
			} else {
				if err == ErrSnapshotTemporarilyUnavailable {
					// if the log is not ready, take a break
					time.Sleep(15 * time.Second)
					continue
				} else {
					// TODO is this a good idea to panic here ?
					log.Errorf("leader try generated snapshot and fail too many times.")
					return nil, err
				}
			}
		}
	}
	msg.Snapshot = r.GetLastSnapshot()
	return msg, nil
}

func (r *Raft) sendAppendEntriesMsg(preIdx, to uint64, entsNeeded []pb.Entry) (*pb.Message, error) {
	var err error
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	msg.LogTerm, err = r.RaftLog.Term(preIdx)
	if err != nil {
		log.Errorf("Fail to get Term of Log Index (%d), Error: %s", preIdx, err.Error())
		return msg, err
	}
	msg.Index = preIdx
	for i := range entsNeeded {
		msg.Entries = append(msg.Entries, &entsNeeded[i])
	}
	return msg, nil
}

// construct and send append response msg
func (r *Raft) sendAppendResponse(to, term, index uint64, approved bool) {
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    term,
		Index:   index,
		// TODO [Optimization] we can send follower's current log term and idx to leader
		LogTerm: 0,
		Reject:  !approved,
	}
	r.msgs = append(r.msgs, response)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) error {
	// Your Code Here (2A).
	var err error = nil
	leaderTerm := m.Term
	preLogIdx := m.Index
	preLogTerm := m.LogTerm
	leaderCommitted := m.Commit
	to := m.From
	appendEntries := make([]pb.Entry, len(m.Entries))
	for i := range m.Entries {
		appendEntries[i] = *m.Entries[i]
	}

	if leaderTerm < r.Term {
		// if request term < current term, reject
		r.sendAppendResponse(m.From, r.Term, 0, false)
	} else {
		// set the leader id, if pass the term check
		r.electionElapsed = 0
		r.Lead = m.From
		accept, lastIdx := r.RaftLog.followerTryAppendLog(appendEntries, preLogIdx, preLogTerm)
		if accept {
			r.RaftLog.followerUpdateCommitted(leaderCommitted, lastIdx)
		}
		r.sendAppendResponse(to, r.Term, lastIdx, accept)
	}
	return err
}

// handleAppendResponse handles the response of the append entry request
func (r *Raft) handleAppendResponse(m pb.Message) (shouldResend bool) {
	log.Debugf("message append response from %d to %d, <lastIndex:[%d], reject:[%v]>", m.From, m.To, m.Index, m.Reject)
	fid := m.From
	// whether the follower accept the entries
	accept := !m.Reject
	// whether the leader need send append again
	shouldResend = m.Reject
	pro := r.Prs[fid]
	followerIdx := m.Index
	caughtUp := true
	// if accepted, change the next and match
	if accept {
		pro.Match = followerIdx
		pro.Next = followerIdx
		pro.Next++
	} else {
		if pro.Next-1 > 0 {
			// if did not accept, the leader will move "next" one step backward
			// if the follower lagged too much,to speed up the syn process,
			// the leader can retry started from the follower's (lastLogIdx + 1)
			if pro.Next > (followerIdx + 1) {
				pro.Next = followerIdx + 1
			} else {
				pro.Next--
			}
			pro.Match = min(pro.Next-1, pro.Match)
		}
	}
	// if the follower is lag behind the leader, the leader should send append entries again
	// This situation usually happens if the follower just accept the snapshot
	if pro.Match < r.RaftLog.LastIndex() {
		caughtUp = false
	}
	// if this response is sent during the time of leader transfer
	if r.leadTransferee == m.From {
		// the leader will decide whether to send the timout now msg
		// according to whether the follower's log is up-to-date
		if caughtUp {
			r.sendTimeoutNow(r.leadTransferee)
			// make sure the leadTransferee is set 0
			// although we already add this line in cleanState(),
			// but it does not hurt anything, right?
			r.leadTransferee = 0
			return
		}
	}

	// update commit according to each follower's progress
	hasUpdated := r.checkAndUpdateCommitIdx()
	shouldResend = hasUpdated || !caughtUp || m.Reject
	log.Debugf("finish process append response from %d to %d, <resend:[%v], prog:[M %d,N %d]>",
		m.From, m.To, shouldResend, r.Prs[fid].Match, r.Prs[fid].Next)
	return
}

// update commit according to each follower's progress
func (r *Raft) checkAndUpdateCommitIdx() (hasUpdate bool) {
	// these two return will tell the leader whether the "committed" has updated
	// if so the leader should send append msg to inform the follower with the new committed
	hasUpdate = false
	committedBefore := r.RaftLog.committed
	count := uint64(0)
	majority := Majority(uint64(len(r.Prs)))
	// found the highest N for committed num
	//committedIdx := r.RaftLog.committed
	lastLogIdx := r.RaftLog.LastIndex()
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	var N uint64
	for N = lastLogIdx; N > committedBefore; N-- {
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
	hasUpdate = committedBefore != r.RaftLog.committed
	return
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
	p := r.Prs[to]
	if p == nil {
		return
	}
	nxt := p.Next
	var preIdx uint64
	if nxt > 0 {
		preIdx = nxt - 1
	} else {
		preIdx = 0
	}
	preTerm, err := r.RaftLog.Term(preIdx)
	if err != nil {
		log.Errorf("Fail to get Term of Log Index (%d), Error: %s", preIdx, err.Error())
		preIdx = r.RaftLog.LastIndex()
		preTerm, _ = r.RaftLog.Term(preIdx)
	}
	com := min(p.Match, r.RaftLog.committed)
	msg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  com,
		Index:   preIdx,
		LogTerm: preTerm,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) startElection() {
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
	if result == Elected {
		r.becomeLeader()
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
		// for candidate and follower, if the electionTimeOut, should precess hup msg to start new election
		if r.electionElapsed+1 >= r.electionTimeout {
			log.Debugf("Election Timeout for Raft(%d).", r.id)
			err := r.Step(r.createLocalMsgHup())
			if err != nil {
				log.Error(err)
			}
		} else {
			r.electionElapsed++
		}
	case StateLeader:
		// for Leader, if the heartbeatTimeout, should precess Beat msg to send hartBeat to all followers
		if r.heartbeatElapsed+1 == r.heartbeatTimeout {
			log.Debugf("HeartBeat Time out for leader Raft(%d)", r.id)
			err := r.Step(r.createLocalBeat())
			if err != nil {
				log.Error(err)
			}
		} else {
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
	r.Lead = None
	// increase term
	r.Term++
	r.Vote = r.id
}

// this func will make state transfer to candidate and start election
func (r *Raft) becameCandidateAndStartElection() {
	r.becomeCandidate()
	r.startElection()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.cleanState()
	r.cleanProgress()
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// this func will append proposed data to leader's log
func (r *Raft) leaderAppendEntries(entries []*pb.Entry) {
	r.RaftLog.leaderAppendLogEntry(r.Term, entries...)
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1
}

func (r *Raft) cleanProgress() {
	ns := nodes(r)
	for _, v := range ns {
		r.Prs[v] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
}

// every time the raftState changed,
// those state should be reset
func (r *Raft) cleanState() {
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > 0 && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	var err error = nil
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
			err = r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		case pb.MessageType_MsgTransferLeader:
			// if the follower receive a transferLeader msg, just forward to the leader
			// and put its id in the from, means the leadership will transfer to itself.
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTransferLeader,
				To:      r.Lead,
				From:    r.id,
			})
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// election time out, become candidate and start election
			r.becameCandidateAndStartElection()
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term && r.State == StateCandidate {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term && r.State == StateCandidate {
				r.becomeFollower(m.Term, m.From)
			}
			err = r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		}

	case StateLeader:
		log.Debugf("current leader: %d", r.id)
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.sendHeartbeatToAll()
			// reset heartbeat elapsed
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgPropose:
			r.leaderAppendEntries(m.Entries)
			r.sendAppendToAll()
		case pb.MessageType_MsgAppendResponse:
			resend := r.handleAppendResponse(m)
			if resend {
				r.sendAppendToAll()
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			err = r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeatResponse:
			resend := r.handleHeartBeatResponse(m)
			if resend {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			r.handleLeaderTransfer(m)
		}
	}
	return err
}

func (r *Raft) sendTimeoutNow(to uint64) {
	timeoutReq := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgTimeoutNow,
	}
	r.msgs = append(r.msgs, timeoutReq)
}

// this func will handle timeout now message
func (r *Raft) handleTimeoutNow(m pb.Message) {
	// if follower receive this msg,
	// step a hup msg immediately
	if m.Term < r.Term {
		// may no possible, but still ignore
		return
	}
	// check whether I am still a member of this group
	if !r.checkNodeExist(r.id) {
		// if not, ignore
		return
	}
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// this func will handle MessageType MsgTransferLeader
func (r *Raft) handleLeaderTransfer(m pb.Message) {
	// TODO how to stop accepting user request, maybe it is handled in upper application
	target := m.From
	// check the whether the node is exist
	if !r.checkNodeExist(target) {
		return
	}
	// set the transfer target
	r.leadTransferee = target
	// check whether this follower's log is up-to-date
	// 1. we can just use the progress,Match[i]
	// 2. we can send an append msg to this follower anyway, if we receive
	// we choose (1) here
	pro := r.Prs[r.leadTransferee]
	if pro.Match == r.RaftLog.LastIndex() {
		// up-to-date
		// let the chosen one start a new election
		r.sendTimeoutNow(r.leadTransferee)
		r.leadTransferee = 0
	} else {
		// lag behind
		// we need to make the chosen one keep up with leader
		r.sendAppend(r.leadTransferee)
	}
}

func (r *Raft) checkNodeExist(id uint64) bool {
	if _, ok := r.Prs[id]; ok {
		return true
	}
	return false
}

func (r *Raft) handleHeartBeatResponse(m pb.Message) (resendAppend bool) {
	// If the follower reject the heartBeat, and the Term is correct,
	// which means this follower is lag behind with the leader,
	// so we should send Append msg to it
	if m.Term < r.Term {
		// ignore
		return false
	}
	resendAppend = m.Reject
	if m.Reject {
		pro := r.Prs[m.From]
		followerIdx := m.Index
		if pro.Next-1 > 0 {
			// if did not accept, the leader will move "next" one step backward
			// if the follower lagged too much,to speed up the syn process,
			// the leader can retry started from the follower's (lastLogIdx + 1)
			if pro.Next > (followerIdx + 1) {
				pro.Next = followerIdx + 1
			} else {
				pro.Next--
			}
			pro.Match = min(pro.Next-1, pro.Match)
		}
	}
	return
}

// handleVoteResponse handel the response RPC of vote
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		// ignore
		return
	}
	if r.State == StateLeader {
		// if already been elected, just ignore
		return
	}
	r.votes[m.From] = !m.Reject
	// check the result of voting
	result := r.checkVoteResult()
	if result == Elected {
		r.becomeLeader()
	} else if result == Fail {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// check Vote result after every time get the vote response
// return whether been elected
func (r *Raft) checkVoteResult() (res ElectionResult) {
	majority := Majority(uint64(len(r.Prs)))
	voteNum := uint64(0)
	rejectNum := uint64(0)
	for _, v := range r.votes {
		if v {
			voteNum++
		} else {
			rejectNum++
		}
	}
	if voteNum >= majority {
		res = Elected
	} else if rejectNum >= majority {
		res = Fail
	} else {
		res = Undecided
	}
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
	// if it is already vote in this term
	if r.Vote != 0 {
		if r.Vote == m.From {
			r.sendVoteResponse(m.From, m.Term, true)
		} else {
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
	} else {
		r.sendVoteResponse(m.From, m.Term, false)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	// if the heartBeat is sent from stale leader, do nothing and response our term
	if r.Term == m.Term {
		// set the leader id if pass the term check
		r.Lead = m.From
		r.electionElapsed = 0
		// reset election timeout
		r.randElectionTimeout()
		// check and update committed
		log.Debugf("node %d, originalCommitted:[%d]", r.id, r.RaftLog.committed)
		// update committed only if leader's is greater than mine
		accept, lastIndex := r.RaftLog.followerTryAppendLog([]pb.Entry{}, m.Index, m.LogTerm)
		// should check whether it is keep-up with the leader
		if accept {
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.followerUpdateCommitted(m.Commit, r.RaftLog.LastIndex())
			}
			response.Reject = false
		} else {
			response.Reject = true
			response.Index = lastIndex
			log.Debugf("follower [%d] reject the heartBeat.", r.id)
		}
		log.Debugf("follower [%d] update committed, LeaderCommitted:[%d], leaderCommittedTerm:[%d],lastIdx:[%d], result:[%d]",
			r.id, m.Commit, m.LogTerm, r.RaftLog.LastIndex(), r.RaftLog.committed)
	}
	// send response
	r.msgs = append(r.msgs, response)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term < r.Term {
		log.Infof("snapshot received by [%d] whose term is stale.", r.id)
		// TODO how to response, here we just return
		return
	}
	snapshot := m.Snapshot
	// handle the snapshot
	if snapshot.Metadata.Index < r.RaftLog.committed {
		log.Debugf("reject snapshot, snapshot index[%d] < committed[%d]", snapshot.Metadata.Index, r.RaftLog.committed)
		r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), false)
		return
	}
	// update leader
	r.Lead = m.GetFrom()
	newPrs := make(map[uint64]*Progress)
	newVotes := make(map[uint64]bool)
	// copy old member state to new
	// update r.prs and r.votes
	for _, v := range snapshot.Metadata.ConfState.Nodes {
		if r.Prs[v] != nil {
			newPrs[v] = r.Prs[v]
		} else {
			newPrs[v] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
		if val, has := r.votes[v]; has {
			newVotes[v] = val
		}
	}
	r.Prs = newPrs
	r.votes = newVotes
	// update the log entries
	r.RaftLog.checkUpdateAndCompactLog(snapshot.Metadata.Index, snapshot.Metadata.Term)
	// put the incoming snapshot in pendingSnapshot
	// so it can be reflect in the next ready
	r.setPendingSnapshot(snapshot)
	r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), true)
}

func (r *Raft) getPendingSnapshot() *pb.Snapshot {
	return r.RaftLog.pendingSnapshot
}

func (r *Raft) finishStablePendingSnapshot() {
	r.RaftLog.pendingSnapshot = nil
}

func (r *Raft) setPendingSnapshot(snapshot *pb.Snapshot) {
	r.RaftLog.pendingSnapshot = snapshot
}

func (r *Raft) GetLastSnapshot() *pb.Snapshot {
	return r.RaftLog.lastSnapshot
}

func (r *Raft) UpdateLastSnapshot(snapshot *pb.Snapshot) {
	if r.RaftLog.lastSnapshot == nil {
		r.RaftLog.lastSnapshot = snapshot
		return
	}
	if r.RaftLog.lastSnapshot.Metadata.Index < snapshot.Metadata.Index {
		r.RaftLog.lastSnapshot = snapshot
	}
}

func (r *Raft) IsLastSnapshotOkForSending(needLogIdx uint64) bool {
	if r.RaftLog.lastSnapshot == nil {
		return false
	}
	return r.RaftLog.lastSnapshot.Metadata.Index >= needLogIdx
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	// add a new node
	// change the r.Prs
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	// remove node
	// 1. remove votes[id]
	// maybe this is a useless op, I guess
	delete(r.votes, id)
	// 2. remove progress
	delete(r.Prs, id)
	r.checkAndUpdateCommitIdx()
}
