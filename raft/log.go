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
	rLog.entries = make([]pb.Entry, 1)
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
		// the entry[0] will be used like offset, so does initIdx and initTerm,
		// they should be updated at the same time
		rLog.entries[0].Index = rLog.initIdx
		rLog.entries[0].Term = rLog.initTerm
	}

	return rLog
}


func (l *RaftLog) leaderAppendNoopLog(term uint64) {
	l.leaderAppendLogEntry(term, nil)
}

// append log to entries
func (l *RaftLog) leaderAppendLogEntry(term uint64, data []byte){
	l.Lock()
	defer l.Unlock()
	l.entries = append(l.entries, pb.Entry{
		Term:  term,
		Index: l.LastIndex() + 1,
		Data:  data,
	})
}


func (l *RaftLog) followerUpdateCommitted (leaderCommitted uint64) {
	l.committed = max(leaderCommitted, l.LastIndex())
}

func (l *RaftLog) leaderUpdateCommitted (N uint64) {
	l.committed = N
}

func (l *RaftLog) followerTryAppendLog(entries []pb.Entry, preLogIdx uint64, preLogTerm uint64, leaderCommitted uint64) (accept bool, err error){
	l.Lock()
	defer l.Unlock()
	// if the preIdx is 0, means the log should all append
	if preLogIdx == 0 {
		l.entries = l.entries[:1]
		l.entries = append(l.entries, entries...)
		return true, nil
	}
	lastEntryIdx := l.LastEntriesIndex()
	accept = false
	for i := lastEntryIdx; i >= 0; i-- {
		// found the preLogIdx
		if preLogIdx == l.entries[i].Index {
			accept = true
			// check the preLogTerm
			var logTerm uint64
			if logTerm, err = l.Term(l.entries[i].Index); err != nil {
				return false, err
			}
			if logTerm != preLogTerm {
				// if the term not equal
				// delete this entry and all followed
				l.entries = l.entries[:l.entries[i].Index]
				// update stable
				l.stabled = min(l.EntryIdx2LogIdx(i - 1), l.stabled)
				log.Infof("Log with same idx [ %d ] has different term [ %d ]", preLogIdx, logTerm)
			}
			l.entries = append(l.entries, entries...)
			log.Info("append log successfully")
			// change commit pointer
			l.followerUpdateCommitted(leaderCommitted)
			break
		}
	}
	// if the accept is false, the commit will be ignored
	return accept,nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) logsNeeded(next uint64) (ents []pb.Entry) {
	l.Lock()
	defer l.Unlock()
	lastIdx := l.LastIndex()
	if next > lastIdx {
		return []pb.Entry{}
	}
	if next < 1{
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
	appliedEntsIdx:=l.LogIdx2EntryIdx(l.applied)
	committedEntsIDx:=l.LogIdx2EntryIdx(l.committed)
	ents = l.entries[appliedEntsIdx+1 : committedEntsIDx+1]
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	i := len(l.entries) - 1
	return l.entries[i].Index
}

func (l *RaftLog) LastEntriesIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

func (l *RaftLog) EntryIdx2LogIdx(eIdx uint64) uint64{
	return eIdx + l.initIdx
}

func (l *RaftLog) LogIdx2EntryIdx(logIdx uint64) uint64 {
	if logIdx < l.initIdx {
		// return entries[0], the log idx before that is already compacted
		return 0
	}
	if logIdx - l.initIdx > l.LastEntriesIndex() {
		return l.LastEntriesIndex()
	}
	return logIdx - l.initIdx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < 1 || i > l.LastIndex() {
		return 0, ErrLogIdxOutBoundary
	}
	entsIdx := l.LogIdx2EntryIdx(i)
	return l.entries[entsIdx].Term, nil
}
