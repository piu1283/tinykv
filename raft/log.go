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
	"math"
	"sync"
)

var ErrLogIdxOutBoundary = errors.New("raft: logIdx is out of boundary. ")
var ErrLogIdxAlreadyCompacted = errors.New("raft: logIdx you want has been compacted. ")

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// mutex used for concurrently change log entries
	sync.Mutex
	// initIdx equals to the truncated index
	initIdx  uint64
	initTerm uint64
	// It is used for snapshot cache.
	// When leader needs to send snapshot to follower,
	// the lastSnapshot will be first considered.
	// If it do not satisfied the progress of follower, which might be impossible,
	// the leader will regenerating snapshot
	lastSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rLog := new(RaftLog)
	rLog.storage = storage
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		log.Error(err)
		return nil
	}
	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Error(err)
		return nil
	}
	// if log is recovered from storage
	rLog.initIdx = firstIdx - 1
	rLog.initTerm, err = storage.Term(rLog.initIdx)
	if err != nil {
		return nil
	}
	// get All stable log from storage
	ents, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		panic(err)
	}
	// restore all stable log entries to raftLog entries
	rLog.entries = append(rLog.entries, ents...)
	// change stable pointer
	rLog.stabled = rLog.LastIndex()
	return rLog
}

// this function will update initIdx and initTerm and entries according to truncate data
// If existing log entry has same index and term as snapshot’s last included entry,
// retain log entries following it and reply
func (l *RaftLog) checkUpdateAndCompactLog(truncIdx, truncTerm uint64) {
	l.Lock()
	defer l.Unlock()
	truncEntsIdx, compacted := l.LogIdx2EntryIdx(truncIdx)
	if compacted {
		return
	}
	l.initIdx = truncIdx
	l.initTerm = truncTerm
	if truncIdx >= l.LastIndex() {
		l.entries = l.entries[:0]
		l.applied = truncIdx
		l.committed = truncIdx
		l.stabled = truncIdx
	} else {
		storeTerm := l.entries[truncEntsIdx].Term
		// update initIdx, applied, commit, stable
		if storeTerm == truncTerm {
			// if we meet an entry have the same index and term with the trunc, we can save the following entries
			if truncIdx > l.applied {
				l.applied = truncIdx
				if truncIdx > l.committed {
					l.committed = truncIdx
					l.stabled = truncIdx
				}
			}
			l.entries = l.entries[truncEntsIdx + 1:]
			//l.maybeCompact()
		} else {
			l.entries = l.entries[:0]
			l.applied = truncIdx
			l.committed = truncIdx
			l.stabled = truncIdx
		}
	}
	log.Debugf("after compacted or snapshot-> [applied: %d],[committed: %d], [stabled: %d],[initIdx: %d],[initTerm: %d]", l.applied, l.committed, l.stabled, l.initIdx, l.initTerm)
}

func (l *RaftLog) leaderAppendNoopLog(term uint64) {
	l.leaderAppendLogEntry(term, []*pb.Entry{{Data: nil}}...)
}

// append log to entries
func (l *RaftLog) leaderAppendLogEntry(term uint64, entries ...*pb.Entry) {
	l.Lock()
	defer l.Unlock()
	for _, v := range entries {
		log.Debugf("leader append : [%d] entries", l.LastIndex()+1)
		l.entries = append(l.entries, pb.Entry{
			Term:  term,
			Index: l.LastIndex() + 1,
			Data:  v.Data,
		})
	}
}

func (l *RaftLog) followerUpdateCommitted(leaderCommitted uint64, lastNewLogIdx uint64) {
	if leaderCommitted > l.committed {
		l.committed = min(leaderCommitted, lastNewLogIdx)
	}
}

func (l *RaftLog) leaderUpdateCommitted(N uint64) {
	l.committed = N
}

// return whether accept the entries
// index is the last index after appended the entries
func (l *RaftLog) followerTryAppendLog(entries []pb.Entry, preLogIdx uint64, preLogTerm uint64) (accept bool, lastLogIndex uint64) {
	l.Lock()
	defer l.Unlock()
	accept = false
	var startEntryIdx uint64
	// check whether the preLogIndex is existed
	if preLogIdx > l.LastIndex() || preLogIdx < l.initIdx {
		// when preLogIdx > lastLogIdx, return false, leader should resend append entry msg with smaller preIdx
		// when preLogIdx < initIdx, means the preLogIdx is compacted, means this append msg can be discard
		log.Debug("preLogIndex > lastIndex, discard append entries.")
		return false, l.LastIndex()
	}
	// if the preLogIdx == initIdx, we start append from entries index 0
	if preLogIdx == l.initIdx {
		if preLogTerm == l.initTerm {
			startEntryIdx = 0
		} else {
			return false, l.LastIndex()
		}
	} else {
		// we eliminate the possibility that the preLogIdx < initIdx
		preEntryIdx, _ := l.LogIdx2EntryIdx(preLogIdx)
		// the term of preLogIdx does not match
		if l.entries[preEntryIdx].Index != preLogIdx || l.entries[preEntryIdx].Term != preLogTerm {
			return false, l.LastIndex()
		}
		startEntryIdx = preEntryIdx + 1
	}

	// when no append entries
	// only need to update committed
	if len(entries) == 0 {
		return true, preLogIdx
	}
	// start find conflict and append entries
	if startEntryIdx == l.LastEntriesIndex()+1 || l.isEmptyEntries() {
		// when the append entries start one next of the LastLogEntry
		l.entries = append(l.entries, entries...)
	} else {
		// careful with the conflict entry
		// start check from startEntryIdx
		appendEntsIdx := 0
		selfEntsIdx := startEntryIdx
		selfLength := uint64(len(l.entries))
		appended := false
		for selfEntsIdx < selfLength && appendEntsIdx < len(entries) {
			selfEntry := l.entries[selfEntsIdx]
			appendEntry := entries[appendEntsIdx]
			if selfEntry.Index == appendEntry.Index && selfEntry.Term == appendEntry.Term {
				selfEntsIdx++
				appendEntsIdx++
			} else {
				// not include selfEntsIdx
				l.entries = append(l.entries[:selfEntsIdx], entries[appendEntsIdx:]...)
				// update stable
				l.stabled = min(l.EntryIdx2LogIdx(selfEntsIdx-1), l.stabled)
				appended = true
				break
			}
		}
		// if the length of append entries longer than l.entries
		if !appended && appendEntsIdx < len(entries) {
			l.entries = append(l.entries, entries[appendEntsIdx:]...)
		}
	}
	// if the accept is false, the commit will be ignored
	return true, l.LastIndex()
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	cutIdx := l.initIdx
	// if the initIdx is greater than last index, we can delete all the log entries
	if l.initIdx >= l.LastIndex() {
		l.entries = l.entries[:0]
		return
	}
	// if not , we need to find the position of initlog, and delete itself and the logs after it
	entIdx := -1
	for i, v := range l.entries {
		if cutIdx == v.Index {
			entIdx = i
			break
		}
	}
	l.entries = l.entries[entIdx+1:]
}

// get the log entries leader should send to the follower
// if the compacted = true, means the leader should send snapshot to that follower
func (l *RaftLog) logEntriesAfterNext(next uint64) (ents []pb.Entry, compacted bool) {
	l.Lock()
	defer l.Unlock()
	lastIdx := l.LastIndex()
	if next > lastIdx {
		return []pb.Entry{}, false
	}
	if next <= l.initIdx {
		return []pb.Entry{}, true
	}
	entryIdx, _ := l.LogIdx2EntryIdx(next)
	//log.Debug("l.entries: ",l.entries)
	return l.entries[entryIdx:], false
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	l.Lock()
	defer l.Unlock()
	entryIdx, compacted := l.LogIdx2EntryIdx(l.stabled)
	if !compacted || l.stabled == l.initIdx {
		return l.entries[entryIdx+1:]
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.Lock()
	defer l.Unlock()
	// we do not need to worry about compacted, the appliedEntsIdx + 1 will be [0, appliedEntsIdx + 1]
	appliedEntsIdx, _ := l.LogIdx2EntryIdx(l.applied)
	committedEntsIdx, _ := l.LogIdx2EntryIdx(l.committed)
	ents = l.entries[appliedEntsIdx+1 : committedEntsIdx+1]
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	i := len(l.entries) - 1
	if i < 0 {
		return l.initIdx
	}
	return l.entries[i].Index
}

func (l *RaftLog) LastEntriesIndex() uint64 {
	if l.isEmptyEntries() {
		return 0
	}
	return uint64(len(l.entries) - 1)
}

func (l *RaftLog) EntryIdx2LogIdx(eIdx uint64) uint64 {
	return eIdx + l.initIdx + 1
}

func (l *RaftLog) isEmptyEntries() bool {
	return len(l.entries) == 0
}

// should check the binary before use it
// the second return will indicate whether the request index has been compacted
func (l *RaftLog) LogIdx2EntryIdx(logIdx uint64) (uint64, bool) {
	if logIdx <= l.initIdx {
		// The reason return maxUint64 is to indicate that the logIdx you want to convert has already been compacted
		// so in other function, the maxUint64 + 1 will be 0
		// it will not cause any trouble if you do not use appliedEntryIndex to do something without + 1
		return math.MaxUint64, true
	}
	res := logIdx - l.initIdx - 1
	return res, false
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.initIdx || i > l.LastIndex() {
		return 0, ErrLogIdxOutBoundary
	}
	if i == l.initIdx {
		return l.initTerm, nil
	}
	idx, _ := l.LogIdx2EntryIdx(i)
	return l.entries[idx].Term, nil
}
