package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

// ErrConfAlreadyApplied is returned when proposing conf change which already been processed
var ErrConfAlreadyApplied = errors.New("raftStore: conf change proposed was already been processed.")

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// check whether is ready
	if d.RaftGroup.HasReady() {
		// get the ready information
		ready := d.RaftGroup.Ready()
		AppSnapRes, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			log.Error("fail to save ready state. ", err)
			return
		}
		// update ctx storeMeta.regionRanges according to appSnapRes.Region
		if AppSnapRes != nil && AppSnapRes.Region != nil {
			//if !util.RegionEqual(AppSnapRes.Region, AppSnapRes.PrevRegion) {
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: AppSnapRes.Region})
			//}
			d.ctx.storeMeta.regions[AppSnapRes.Region.Id] = AppSnapRes.Region
		}
		outBoundMsgs := ready.Messages
		// send all message out
		d.Send(d.ctx.trans, outBoundMsgs)
		// apply committed entries
		committedEntries := ready.CommittedEntries
		for _, v := range committedEntries {
			if len(v.Data) == 0 {
				log.Debug("%s applied empty entry.", d.Tag, v)
				continue
			}
			var cmdResponse *raft_cmdpb.RaftCmdResponse
			var txn *badger.Txn
			var err error
			switch v.EntryType {
			// handle the confChange Entry
			case eraftpb.EntryType_EntryConfChange:
				confChangeReq := new(eraftpb.ConfChange)
				if err := confChangeReq.Unmarshal(v.Data); err != nil {
					log.Errorf("fail to unmarshal data to confChange. [%s]", err.Error())
					return
				}
				var needEnd bool
				cmdResponse, needEnd = d.handleConfChange(confChangeReq)
				if needEnd {
					return
				}
			default:
				// handle normal Entry, including compactLog which is an admin request
				req := new(raft_cmdpb.RaftCmdRequest)
				if err := req.Unmarshal(v.Data); err != nil {
					return
				}
				if req.AdminRequest != nil {
					switch req.AdminRequest.CmdType {
					case raft_cmdpb.AdminCmdType_CompactLog:
						cmdResponse = d.handleCompactLogMsg(req)
					case raft_cmdpb.AdminCmdType_Split:
						cmdResponse = d.handleRegionSplitMsg(req)
					}
				} else {
					// leader needs response, follower only need to apply the modification
					cmdResponse, txn, err = d.peerStorage.applyCmdRequest(req, v.Index, d.IsLeader())
					if err != nil || cmdResponse == nil {
						log.Error("fail to apply entry. ", err, v)
						return
					}
				}
			}
			// if the idx=-1, that means this entry was not proposed in this peer, so should not update proposals
			if d.IsLeader() {
				idx := d.peer.getProposalPositionByIdxAndTerm(v.Index, v.Term)
				if idx >= 0 {
					// some cmd do not have callback function
					if d.proposals[idx].cb != nil {
						// txn maybe nil, it will has value only if request contains snapGet
						if txn != nil {
							d.proposals[idx].cb.Txn = txn
						}
						d.proposals[idx].cb.Done(cmdResponse)
					}
					// remove idx
					//d.proposals = d.proposals[1:]
					d.proposals = append(d.proposals[:idx], d.proposals[idx+1:]...)
				}
			}
		}

		d.RaftGroup.Advance(ready)
		// TODO is this a good idea to set readyState here?
		d.RaftGroup.ReadyState = true
	}
}

func (d *peerMsgHandler) handleConfChange(cc *eraftpb.ConfChange) (*raft_cmdpb.RaftCmdResponse, bool) {
	// update region meta
	cpRes := new(raft_cmdpb.ChangePeerResponse)
	adminResponse := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: cpRes,
	}
	cpRes.Region = d.Region()
	cxt := new(kvrpcpb.Context)
	if err := cxt.Unmarshal(cc.Context); err != nil {
		log.Errorf("fail to unmarshal kvrpcpb.context. [%s]", err.Error())
		return ErrResp(err),true
	}
	if cxt.RegionEpoch.ConfVer <= d.peerStorage.region.RegionEpoch.ConfVer {
		log.Warnf("Conf Change already updated, version:[%d]， curConfVer[%d]", cxt.RegionEpoch.ConfVer, d.Region().RegionEpoch.ConfVer)
		return raftAdminCmdResponse(adminResponse, d.Term(), nil) ,false
	}
	// update the ConfVer
	d.peerStorage.region.RegionEpoch.ConfVer = cxt.RegionEpoch.ConfVer
	P := cxt.Peer
	// update RegionLocalState
	kvWB := new(engine_util.WriteBatch)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		d.peerStorage.region.Peers = append(d.peerStorage.region.Peers, P)
	case eraftpb.ConfChangeType_RemoveNode:
		// if the removed peer is myself
		if P.StoreId == d.peer.storeID() {
			log.Debugf("destroy myself. regionId:[%d], storeId[%d].", d.regionId, d.storeID())
			d.destroyPeer()
			cpRes.Region = d.peerStorage.region
			return raftAdminCmdResponse(adminResponse, d.Term(), nil), true
		}
		pIdx := -1
		for i, v := range d.peerStorage.region.Peers {
			if v.Id == P.Id && v.StoreId == P.StoreId {
				pIdx = i
			}
		}
		if pIdx >= 0 {
			d.peerStorage.region.Peers = append(d.peerStorage.region.Peers[:pIdx], d.peerStorage.region.Peers[pIdx+1:]...)
		}
		// clear peerCache
		delete(d.peerCache, P.Id)
	}
	cpRes.Region = d.peerStorage.region
	d.ctx.storeMeta.regions[d.regionId] = d.Region()
	// apply conf change in raft group
	meta.WriteRegionState(kvWB, d.peerStorage.region, rspb.PeerState_Normal)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	d.peer.RaftGroup.ApplyConfChange(*cc)
	return raftAdminCmdResponse(adminResponse, d.Term(), nil), false
}

// handle leader transfer admin msg
func (d *peerMsgHandler) handleLeaderTransferMsg(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	// construct response
	response := new(raft_cmdpb.TransferLeaderResponse)
	adminResponse := &raft_cmdpb.AdminResponse{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: response,
	}
	cmdResponse := raftAdminCmdResponse(adminResponse, d.Term(), nil)
	transferReq := req.AdminRequest.TransferLeader
	targetPeer := transferReq.Peer
	// if the transfer target is current peer. we just return
	if targetPeer.StoreId == d.storeID() && d.IsLeader() {
		return cmdResponse
	}
	d.RaftGroup.TransferLeader(targetPeer.Id)
	return cmdResponse
}

// handle compact log admin msg
func (d *peerMsgHandler) handleCompactLogMsg(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	// handle logGCCompact
	// construct response
	response := new(raft_cmdpb.CompactLogResponse)
	adminResponse := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: response,
	}
	cmdResponse := raftAdminCmdResponse(adminResponse, d.Term(), nil)
	// check whether need to be compacted
	compactIdx := req.AdminRequest.CompactLog.CompactIndex
	if d.peerStorage.applyState.TruncatedState.Index >= compactIdx {
		log.Debugf("the log is already compacted")
		return cmdResponse
	}
	log.Debugf("%s update compacted before, [%s]", d.Tag, d.peerStorage.applyState.TruncatedState)
	// update state
	firstIdx := d.peerStorage.updateMetaByLogCompact(req)
	// update memory log
	d.RaftGroup.CompactMemoryLogEntries(req.AdminRequest.CompactLog.CompactIndex, req.AdminRequest.CompactLog.CompactTerm)
	// assign a asynchronously task
	d.ctx.raftLogGCTaskSender <- &runner.RaftLogGCTask{
		RaftEngine: d.peerStorage.Engines.Raft,
		RegionID:   d.regionId,
		StartIdx:   firstIdx,
		EndIdx:     compactIdx + 1,
	}
	log.Debugf("%s update compacted, [%s]", d.Tag, d.peerStorage.applyState.TruncatedState)
	// TODO seems this variable is useless
	d.peer.LastCompactedIdx = compactIdx
	_, _ = d.peerStorage.Snapshot()
	// TODO there is no need to call snapshot now after log compact.
	// TODO when we figure out how to cache the snapshot, then we can think about this
	//snapshot, err := d.peerStorage.Snapshot()
	//if err == nil {
	//	// update the LastSnapshot
	//	// if the generated snapshot is earlier than the compact, regenerating
	//	if compactIdx > snapshot.Metadata.Index {
	//		log.Debugf("%s call Snapshot.", d.Tag)
	//		_, _ = d.peerStorage.Snapshot()
	//	}else{
	//		// LastSnapshot only be updated when new snapshot index is greater than previous one
	//		d.RaftGroup.UpdateLastSnapshot(&snapshot)
	//	}
	//} else {
	//	log.Errorf(" %s snapshot is still generating when executing compactLog command.", d.Tag)
	//}
	return cmdResponse
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// needs to check whether to propose this request
	// 1. if this peer is not leader(NOT_LEADER)
	// 2. if the request term is behind curTerm(STALE_COMMAND)
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if data, err := msg.Marshal(); err != nil {
		log.Panic("Fail to marshal msg.", msg, err)
		panic(err)
	} else {
		if msg.AdminRequest != nil {
			log.Debugf("proposals admin request on [%d]: <%s>", d.PeerId(), msg.AdminRequest.CmdType.String())
			// process the admin which don't need to call proposed function
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_TransferLeader:
				cmdRes := d.handleLeaderTransferMsg(msg)
				// Immediately response after start processing this msg
				if cb != nil {
					cb.Done(cmdRes)
				}
				// transferLeader msg has no need to be added to proposal
				return
			case raft_cmdpb.AdminCmdType_ChangePeer:
				// the propose request is admin request, and need to be replicated to followers
				confChange, err := d.generateConfChange(msg)
				if err != nil {
					log.Error("Fail to construct conf change.", msg, err)
					cb.Done(ErrResp(err))
					return
				}
				if err := d.RaftGroup.ProposeConfChange(*confChange); err != nil {
					log.Warnf("Fail to propose conf change.", msg, err)
					cb.Done(ErrResp(err))
					return
				}
			case raft_cmdpb.AdminCmdType_Split:
				// regionSplit & compactLog is admin request and need to be replicated to followers
				// And executed when applied
				err := d.checkRegionSplitRequest(msg)
				if err != nil {
					cb.Done(ErrResp(err))
				}
				if err := d.RaftGroup.Propose(data); err != nil {
					log.Error("Fail to propose request.", msg, err)
					cb.Done(ErrResp(err))
				}
			case raft_cmdpb.AdminCmdType_CompactLog:
				if err := d.RaftGroup.Propose(data); err != nil {
					log.Error("Fail to propose request.", msg, err)
					cb.Done(ErrResp(err))
				}
			}
		} else {
			// the propose request is normal operation
			if err := d.RaftGroup.Propose(data); err != nil {
				log.Error("Fail to propose request.", msg, err)
				cb.Done(ErrResp(err))
			}
		}
		propo := &proposal{
			index: d.lastIndexOfProposal(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, propo)
	}
}

func (d *peerMsgHandler) checkRegionSplitRequest(req *raft_cmdpb.RaftCmdRequest) error {
	splitReq := req.AdminRequest.Split
	splitKey := splitReq.SplitKey
	// check splitKey
	err := util.CheckKeyInRegionExclusive(splitKey, d.Region())
	if err != nil {
		return err
	}
	// TODO is this correct ?
	if _, ok := d.ctx.storeMeta.regions[req.Header.RegionId]; !ok {
		return &util.ErrRegionNotFound{RegionId: req.Header.RegionId}
	}
	return nil
}

// the handle logic:
// 1. update the regionEpoch
// 2. update the region range
// 3. create the new peer
// 4. registered to the router.regions
// 5. region’s info should be inserted into regionRanges in ctx.StoreMeta
func (d *peerMsgHandler) handleRegionSplitMsg(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	log.Errorf("%s handle region split", d.Tag)
	splitReq := msg.AdminRequest.Split
	splitKey := splitReq.SplitKey
	oldRegion := d.Region()
	// change the regionEpoch
	d.Region().RegionEpoch.Version++
	// copy the region data to new region
	newRegion, err := util.DeepCopyRegionInfo(oldRegion)
	if err != nil {
		// TODO change a better way to handle it
		panic(err)
	}
	// update old & new region's key range
	oldRegion.EndKey = splitKey
	newRegion.StartKey = splitKey
	// change peer members in region
	newRegion.Id = splitReq.NewRegionId
	log.Errorf("split region id: %d", splitReq.NewRegionId)
	// update peers
	// TODO really strange
	for i := 0; i < len(splitReq.NewPeerIds); i++ {
		newRegion.Peers[i].Id = splitReq.NewPeerIds[i]
	}
	p, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.peerStorage.Engines, d.Region())
	if err != nil {
		return ErrResp(err)
	}
	d.ctx.router.register(p)
	d.ctx.storeMeta.setRegion(newRegion, p)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	SplitResp := new(raft_cmdpb.SplitResponse)
	SplitResp.Regions = append(SplitResp.Regions, newRegion)
	return raftAdminCmdResponse(&raft_cmdpb.AdminResponse{
		Split:   SplitResp,
		CmdType: raft_cmdpb.AdminCmdType_Split,
	}, d.Term(), nil)
}

// return whether this conf change needed to be processed
func (d *peerMsgHandler) checkConfChangeNeedProcessed(p *metapb.Peer, ct eraftpb.ConfChangeType) bool {
	if ct == eraftpb.ConfChangeType_AddNode {
		// if it is add operation, we need to check whether peers already has this peer
		findPeer := util.FindPeer(d.Region(), p.StoreId)
		return findPeer == nil
	} else {
		// if it is not add operation
		findPeer := util.FindPeer(d.Region(), p.StoreId)
		return findPeer != nil
	}
}

// generate eraftpb.ConfChange according to raft_cmdpb.RaftCmdRequest
// and check whether this conf change has already been executed
func (d *peerMsgHandler) generateConfChange(msg *raft_cmdpb.RaftCmdRequest) (*eraftpb.ConfChange, error) {
	cc := new(eraftpb.ConfChange)
	peer := msg.AdminRequest.ChangePeer.Peer
	needProcessed := d.checkConfChangeNeedProcessed(peer, msg.AdminRequest.ChangePeer.ChangeType)
	if !needProcessed {
		log.Warnf("skip propose conf change: [%s]:[%d]",msg.AdminRequest.ChangePeer.ChangeType.String(), peer.Id)
		return nil, ErrConfAlreadyApplied
	}
	log.Warnf("propose conf change: [%s]:[%d]",msg.AdminRequest.ChangePeer.ChangeType.String(), peer.Id)
	// check whether this conf change needed to be executed
	cc.NodeId = peer.Id
	cc.ChangeType = msg.AdminRequest.ChangePeer.ChangeType
	re := &metapb.RegionEpoch{
		ConfVer: d.Region().RegionEpoch.ConfVer + 1,
		Version: d.Region().RegionEpoch.Version,
	}
	// TODO what is this context, am I right ?
	ctx := kvrpcpb.Context{
		RegionId:    d.regionId,
		RegionEpoch: re,
		Peer:        peer,
	}
	data, err := ctx.Marshal()
	if err != nil {
		return nil, err
	}
	cc.Context = data
	return cc, nil
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale [%v] , current %v ignore it",
			regionID, msgType,msg.RegionEpoch, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
