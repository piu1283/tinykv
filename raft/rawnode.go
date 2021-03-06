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

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// ErrAnotherConfChangePending is returned when there is already a conf change pending, and here comes another one
var ErrAnotherConfChangePending = errors.New("raft: Fail to propose confChange, there already another one is processing. ")

var ErrLeaderTransferring = errors.New("raft: cannot propose any command, because it currently transferring leadership. ")

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	lastHardState pb.HardState
	// only the ready() and advance() will modify it.
	ReadyState bool
	// lastSoftState
	lastSoftState *SoftState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rn := new(RawNode)
	rn.Raft = newRaft(config)
	rn.lastHardState = rn.Raft.currentHardState()
	rn.ReadyState = true
	rn.lastSoftState = &SoftState{
		Lead:      rn.Raft.getCurrentLeader(),
		RaftState: rn.Raft.getCurrentState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	// TODO do not know whether really need this
	if rn.Raft.leadTransferee > 0 {
		return ErrLeaderTransferring
	}
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	// if there is already another conf change is processing
	// we should not continue propose
	if !rn.Raft.checkPendingConfChangeIdx() {
		return ErrAnotherConfChangePending
	}
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	err = rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
	if err != nil {
		return err
	}
	// update the pending conf Index
	rn.Raft.PendingConfIndex = rn.Raft.RaftLog.LastIndex()
	return nil
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	rn.ReadyState = false
	// check whether hardState has been modified
	currentHardState := rn.Raft.currentHardState()
	equal := isHardStateEqual(rn.lastHardState, currentHardState)
	var hs pb.HardState
	if equal {
		// if not been modified, there is no need to save it
		hs = pb.HardState{}
	} else {
		// if changed, put it to ready, and replace the lastHardState with currentHardState
		hs = currentHardState
	}
	var ss *SoftState
	curSoftSt := &SoftState{
		Lead:      rn.Raft.getCurrentLeader(),
		RaftState: rn.Raft.getCurrentState(),
	}
	if !isSoftStateEqual(curSoftSt, rn.lastSoftState) {
		ss = curSoftSt
		rn.lastSoftState = curSoftSt
	}
	var readySnap pb.Snapshot
	snapshot := rn.Raft.getPendingSnapshot()
	if !IsEmptySnap(snapshot) {
		// if has pending snapshot
		readySnap = *snapshot
		log.Debugf("[%d] has snapshot", rn.Raft.id)
	} else {
		readySnap = pb.Snapshot{}
		//log.Debugf("[%d] no snapshot",rn.Raft.id)
	}
	//log.Debugf("ready process: %d, snapshot: <%s>", rn.Raft.id, snapshot)
	return Ready{
		Messages:         rn.Raft.msgs,
		HardState:        hs,
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Snapshot:         readySnap,
		SoftState:        ss,
	}
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// if a ready is currently being processed, if yes, return false, means the raft is not ready
	return rn.ReadyState
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// change the applied, stable pointer according to the Ready
	// hardState
	if !IsEmptyHardState(rd.HardState) {
		rn.lastHardState = rd.HardState
	}
	// stable
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	// applied
	if len(rd.CommittedEntries) > 0 && rn.Raft.RaftLog.committed != 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	// clear the msgs
	rn.Raft.msgs = rn.Raft.msgs[:0]
	// clear pending snapshot
	rn.Raft.finishStablePendingSnapshot()
}

//func (rn *RawNode) UpdateLastSnapshot(snapshot *pb.Snapshot) {
//	rn.Raft.UpdateLastSnapshot(snapshot)
//}

// GetProgress return the the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

func (rn *RawNode) CompactMemoryLogEntries(compactIdx, compactTerm uint64) {
	rn.Raft.RaftLog.checkUpdateAndCompactLog(compactIdx, compactTerm)
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
