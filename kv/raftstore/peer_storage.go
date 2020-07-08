package raftstore

import (
	"bytes"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// gennerate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	// save log
	for i := range entries {
		// make log key
		key := meta.RaftLogKey(ps.region.Id, entries[i].Index)
		_ = raftWB.SetMeta(key, &entries[i])
	}
	// update raftLocalState
	if len(entries) > 0 {
		lastAppendEntry := entries[len(entries)-1]
		// log Idx greater than last entry in "entries" will never be committed
		// Let's consider two situations:
		// 1. this peer is leader, its log will be the the most correct one.
		//    (in raftDB [committed..stable]) idx: 5,6,7,8
		//    (ready.entries) idx: 9,10,11,12,13
		// 	  res-> (in raftDB [committed..stable]) idx: 5,6,7,8,9,10,11,12,13
		// 2. this peer is a follower, its log different with the leader's since the idx 6.
		//    (in raftDB [committed..stable]) idx: 5,6,7,8,9
		//    (ready.entries) idx: 6,7
		// 	  res-> (in raftDB [committed..stable]) idx: 5,6,7 (8,9 will never be committed, so delete them)
		lastStoredIndex, err := ps.LastIndex()
		if err != nil {
			return err
		}
		for i := lastAppendEntry.Index + 1; i <= lastStoredIndex; i++ {
			deleteKey := meta.RaftLogKey(ps.region.Id, i)
			raftWB.DeleteMeta(deleteKey)
		}
		// update last log index in raftDB
		ps.raftState.LastIndex = lastAppendEntry.Index
		ps.raftState.LastTerm = lastAppendEntry.Term
	}
	return nil
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	res := new(ApplySnapResult)
	res.PrevRegion = ps.region
	res.Region = ps.region
	metadata := snapshot.GetMetadata()
	// update truncatedState
	ps.applyState.TruncatedState.Term = metadata.GetTerm()
	ps.applyState.TruncatedState.Index = metadata.GetIndex()
	// truncated index greater than current applied index
	// This snapshot is used for peer which far behind the leader (or brand new peer) to catch up,
	// In this way, it is reasonable to update applied to snapshot index.
	ps.applyState.AppliedIndex = metadata.GetIndex()
	// update raftState
	ps.raftState.HardState.Commit = metadata.GetIndex()
	ps.raftState.LastIndex = metadata.GetIndex()
	ps.raftState.LastTerm = metadata.GetTerm()
	// update region
	ps.region.Id = snapData.Region.Id
	ps.region.StartKey = snapData.Region.StartKey
	ps.region.EndKey = snapData.Region.EndKey
	ps.region.RegionEpoch = snapData.Region.RegionEpoch
	ps.region.Peers = snapData.Region.Peers
	regionLocalState := rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: ps.region,
	}
	// clear the metadata
	if err := ps.clearMeta(kvWB, raftWB); err != nil {
		return res, err
	}
	// than put new state after the deleted original state
	if err := raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		return res, err
	}
	if err := kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState); err != nil {
		return res, err
	}
	if err := kvWB.SetMeta(meta.RegionStateKey(ps.region.Id), &regionLocalState); err != nil {
		return res, err
	}
	ps.snapState.StateType = snap.SnapState_Applying
	// clean stale extra data
	ps.clearExtraData(snapData.Region)
	// send RegionTaskApply
	ch := make(chan bool, 1)
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: metadata,
		StartKey: snapData.Region.StartKey,
		EndKey:   snapData.Region.EndKey,
	}
	regionApplyRes := <-ch
	if !regionApplyRes {
		return res, errors.New("Fail to apply metaData of snapshot.")
	}
	res.Region = snapData.Region
	return res, nil
}

// TODO check it
// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	// some staff in this function might be empty, due to they have no need to update
	var applyRes *ApplySnapResult
	var err error
	hardState := ready.HardState
	entriesNeededStabled := ready.Entries
	raftWB := new(engine_util.WriteBatch)
	kvWB := new(engine_util.WriteBatch)
	if !raft.IsEmptySnap(&ready.Snapshot) {
		// if the snapshot is not empty
		if ps.validateSnap(&ready.Snapshot) {
			applyRes, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
			if err != nil {
				return applyRes, err
			}
		} else {
			log.Warnf("%s Snapshot is stale, discard it.", ps.Tag)
		}
	}
	// append unstable logs
	if err = ps.Append(entriesNeededStabled, raftWB); err != nil {
		return applyRes, err
	}
	// update hardState
	if !raft.IsEmptyHardState(hardState) {
		// update hardState in ps
		raft.CopyHardState(&hardState, ps.raftState.HardState)
	}
	// update raftState,
	if err = raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		return nil, err
	}
	// execute the write of raftDB
	if err = raftWB.WriteToDB(ps.Engines.Raft); err != nil {
		log.Errorf("Fail to write raft data to RaftDB, regionId(%d).", ps.region.Id)
		return applyRes, err
	}
	if kvWB.Len() > 0 {
		if err = kvWB.WriteToDB(ps.Engines.Kv); err != nil {
			return applyRes, err
		}
	}
	return applyRes, nil
}

func (ps *PeerStorage) updateMetaByLogCompact(cmdRequest *raft_cmdpb.RaftCmdRequest) (firstIdx uint64) {
	if cmdRequest.AdminRequest == nil {
		return
	}
	compactLogReq := cmdRequest.AdminRequest.CompactLog
	// record the previous first index
	firstIdx, _ = ps.FirstIndex()
	// update TruncatedState
	ps.applyState.TruncatedState.Index = compactLogReq.CompactIndex
	ps.applyState.TruncatedState.Term = compactLogReq.CompactTerm
	kvWB := new(engine_util.WriteBatch)
	if err := kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState); err != nil {
		log.Errorf("%s fail to update TruncatedState when compact.", ps.Tag)
		panic(err)
	}
	if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
		log.Errorf("%s fail to update TruncatedState when compact.", ps.Tag)
		panic(err)
	}
	return
}

// process & apply committed request
func (ps *PeerStorage) applyCmdRequest(cmdRequest *raft_cmdpb.RaftCmdRequest, lastIdxWillApplied uint64, needResponse bool) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, error) {
	var txn *badger.Txn
	resps := []*raft_cmdpb.Response{}
	respHeader := new(raft_cmdpb.RaftResponseHeader)
	// should contains all the put&delete modification, and also the appliedState modification
	kvWB := engine_util.WriteBatch{}
	// TODO whether should use Txn to read
	for _, req := range cmdRequest.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			// only leader needs to execute get, followers can just advance by moving applied forward
			if needResponse {
				getReq := req.Get
				val, err := engine_util.GetCF(ps.Engines.Kv, getReq.Cf, getReq.Key)
				if err != nil {
					return nil, nil, err
				}
				resps = append(resps, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			}
		case raft_cmdpb.CmdType_Put:
			putReq := req.Put
			kvWB.SetCF(putReq.Cf, putReq.Key, putReq.Value)
			if needResponse {
				resps = append(resps, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			}
		case raft_cmdpb.CmdType_Delete:
			delReq := req.Delete
			kvWB.DeleteCF(delReq.Cf, delReq.Key)
			if needResponse {
				resps = append(resps, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			}
		case raft_cmdpb.CmdType_Snap:
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: ps.region,
				},
			})
			txn = ps.Engines.Kv.NewTransaction(false)
		}
	}
	applyStateKey := meta.ApplyStateKey(ps.region.Id)
	ps.applyState.AppliedIndex = lastIdxWillApplied
	// TODO maybe need update appliedIndex
	if err := kvWB.SetMeta(applyStateKey, ps.applyState); err != nil {
		return nil, nil, err
	}
	if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
		return nil, nil, err
	}
	// responses
	cmdResponse := new(raft_cmdpb.RaftCmdResponse)
	cmdResponse.Header = respHeader
	cmdResponse.Responses = resps
	return cmdResponse, txn, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
