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
	// initIdx is the idx at position 0,
	initIdx  uint64
	initTerm uint64
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
	// the log is init from scratch
	// firstIdx will be 1 if there is no entry in storage
	// lastIdx will be 0 if there if no entry in storage
	if firstIdx > lastIdx {
		rLog.initIdx = 0
		rLog.initTerm = 0
	} else {
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
	}
	return rLog
}

func (l *RaftLog) leaderAppendNoopLog(term uint64) {
	l.leaderAppendLogEntry(term, []*pb.Entry{{Data: nil}}...)
}

// append log to entries
func (l *RaftLog) leaderAppendLogEntry(term uint64, entries ...*pb.Entry) {
	l.Lock()
	defer l.Unlock()
	for _, v := range entries {
		l.entries = append(l.entries, pb.Entry{
			Term:  term,
			Index: l.LastIndex() + 1,
			Data:  v.Data,
		})
	}
}

func (l *RaftLog) followerUpdateCommitted(leaderCommitted uint64, lastNewLogIdx uint64) {
	l.committed = min(leaderCommitted, lastNewLogIdx)
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
		}else {
			return false, l.LastIndex()
		}
	} else {
		preEntryIdx := l.LogIdx2EntryIdx(preLogIdx)
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
		selfLength:=uint64(len(l.entries))
		appended:=false
		for selfEntsIdx < selfLength && appendEntsIdx < len(entries) {
			selfEntry:=l.entries[selfEntsIdx]
			appendEntry:=entries[appendEntsIdx]
			if selfEntry.Index == appendEntry.Index && selfEntry.Term == appendEntry.Term {
				selfEntsIdx++
				appendEntsIdx++
			}else{
				// not include selfEntsIdx
				l.entries = append(l.entries[:selfEntsIdx], entries[appendEntsIdx:]...)
				// update stable
				l.stabled = min(l.EntryIdx2LogIdx(selfEntsIdx - 1), l.stabled)
				appended = true
				break
			}
		}
		// if the length of append entries longer than l.entries
		if !appended && appendEntsIdx < len(entries) {
			l.entries = append(l.entries, entries[appendEntsIdx:]...)
		}
		// update committed
		log.Debug("append log successfully : ", entries)
	}
	// if the accept is false, the commit will be ignored
	return true, l.LastIndex()
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) logEntriesAfterNext(next uint64) (ents []pb.Entry) {
	l.Lock()
	defer l.Unlock()
	lastIdx := l.LastIndex()
	if next > lastIdx {
		return []pb.Entry{}
	}
	if next <= l.initIdx {
		return []pb.Entry{}
	}
	entryIdx := l.LogIdx2EntryIdx(next)
	return l.entries[entryIdx:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	l.Lock()
	defer l.Unlock()
	entryIdx := l.LogIdx2EntryIdx(l.stabled)
	res := l.entries[entryIdx+1:]
	return res
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	l.Lock()
	defer l.Unlock()
	appliedEntsIdx := l.LogIdx2EntryIdx(l.applied)
	committedEntsIdx := l.LogIdx2EntryIdx(l.committed)
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
func (l *RaftLog) LogIdx2EntryIdx(logIdx uint64) uint64 {
	return logIdx - l.initIdx - 1
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
	idx := l.LogIdx2EntryIdx(i)
	return l.entries[idx].Term, nil
}
