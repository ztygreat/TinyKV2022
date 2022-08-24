package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

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

func (d *peerMsgHandler) apply(e *eraftpb.Entry, kvWB *engine_util.WriteBatch) bool {
	msg := &raft_cmdpb.RaftCmdRequest{}
	cc := new(eraftpb.ConfChange)
	if e.EntryType == eraftpb.EntryType_EntryConfChange {
		cc.Unmarshal(e.Data)
		msg.Unmarshal(cc.Context)
	} else {
		msg.Unmarshal(e.Data)
	}
	if msg.AdminRequest != nil {
		if msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
			err := util.CheckRegionEpoch(msg, d.Region(), false)
			if err != nil {
				d.HandleProposal(e, func(p *proposal) { p.cb.Done(ErrResp(err)) })
				return false
			}
			err = util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
			if err != nil {
				d.HandleProposal(e, func(p *proposal) { p.cb.Done(ErrResp(err)) })
				return false
			}
			d.peerStorage.applyState.AppliedIndex = e.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			if len(msg.AdminRequest.Split.NewPeerIds) != len(d.Region().Peers) {
				return false
			}
			newRegion := new(metapb.Region)
			oldRegion := new(metapb.Region)
			util.CloneMsg(d.Region(), newRegion)
			util.CloneMsg(d.Region(), oldRegion)
			oldRegion.RegionEpoch.Version++
			newRegion.RegionEpoch.Version++
			oldRegion.EndKey = msg.AdminRequest.Split.SplitKey
			newRegion.StartKey = msg.AdminRequest.Split.SplitKey
			newRegion.Id = msg.AdminRequest.Split.NewRegionId
			newRegion.Peers = make([]*metapb.Peer, 0)
			for id, peer := range oldRegion.Peers {
				newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
					Id:      msg.AdminRequest.Split.NewPeerIds[id],
					StoreId: peer.StoreId,
				})
			}
			meta.WriteRegionState(kvWB, oldRegion, rspb.PeerState_Normal)
			meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			// d.SizeDiffHint = 0
			// d.ApproximateSize = new(uint64)
			d.ctx.storeMeta.Lock()
			d.peerStorage.region = oldRegion
			d.ctx.storeMeta.regions[newRegion.Id] = newRegion
			d.ctx.storeMeta.regions[d.regionId] = oldRegion
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
			d.ctx.storeMeta.Unlock()

			newPeer, err1 := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
			if err1 != nil {
				panic(err1)
			}
			d.ctx.router.register(newPeer)
			d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
			response := &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_Split, Split: &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{d.Region(), newRegion}}}
			resp := new(raft_cmdpb.RaftCmdResponse)
			resp.Header = &raft_cmdpb.RaftResponseHeader{}
			resp.AdminResponse = response
			d.HandleProposal(e, func(p *proposal) { p.cb.Done(resp) })
			d.notisfyHeartbeatScheduler(d.Region(), d.peer)
			d.notisfyHeartbeatScheduler(newRegion, newPeer)
			return false
		}
		if msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_CompactLog {
			log.Infof("CompactIndex: %d, truncatedIndex: %d", msg.AdminRequest.CompactLog.CompactIndex, d.peerStorage.applyState.TruncatedState.Index)
			if msg.AdminRequest.CompactLog.CompactIndex > d.peerStorage.truncatedIndex() {
				d.peerStorage.applyState.AppliedIndex = e.Index
				d.peerStorage.applyState.TruncatedState.Index = msg.AdminRequest.CompactLog.CompactIndex
				d.peerStorage.applyState.TruncatedState.Term = msg.AdminRequest.CompactLog.CompactTerm
				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				kvWB.WriteToDB(d.peerStorage.Engines.Kv)
				kvWB.Reset()
				response := &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_CompactLog, CompactLog: &raft_cmdpb.CompactLogResponse{}}
				resp := new(raft_cmdpb.RaftCmdResponse)
				resp.Header = &raft_cmdpb.RaftResponseHeader{}
				resp.AdminResponse = response
				d.HandleProposal(e, func(p *proposal) { p.cb.Done(resp) })
				d.ScheduleCompactLog(msg.AdminRequest.CompactLog.CompactIndex)
				return false
			} else {
				d.HandleProposal(e, func(p *proposal) { p.cb.Done(ErrResp(&util.ErrStaleCommand{})) })
				return false
			}
		}
		if msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
			err := util.CheckRegionEpoch(msg, d.Region(), true)
			if err != nil {
				d.HandleProposal(e, func(p *proposal) { p.cb.Done(ErrResp(err)) })
				return false
			}
			log.Infof("id: %d, Lead: %d, changeType: %s, target: %d", d.PeerId(), d.LeaderId(), cc.ChangeType.String(), cc.NodeId)
			d.peerStorage.applyState.AppliedIndex = e.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			region := d.Region()
			if msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_AddNode {
				for _, peer := range d.Region().Peers {
					if peer.Id == msg.AdminRequest.ChangePeer.Peer.Id {
						response := &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()}}
						resp := new(raft_cmdpb.RaftCmdResponse)
						resp.Header = &raft_cmdpb.RaftResponseHeader{}
						resp.AdminResponse = response
						d.HandleProposal(e, func(p *proposal) { p.cb.Done(resp) })
						return false
					}
				}
				d.insertPeerCache(msg.AdminRequest.ChangePeer.Peer)
				region.Peers = append(region.Peers, msg.AdminRequest.ChangePeer.Peer)
				region.RegionEpoch.ConfVer++
			} else {
				d.removePeerCache(msg.AdminRequest.ChangePeer.Peer.Id)
				if msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() && d.peerStorage.snapState.StateType != snap.SnapState_Applying {
					d.destroyPeer()
					return true
				}
				for i, k := range region.Peers {
					if k.Id == msg.AdminRequest.ChangePeer.Peer.Id {
						region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
						region.RegionEpoch.ConfVer++
						break
					}
				}
			}
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.regionId] = d.Region()
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			d.peer.RaftGroup.ApplyConfChange(*cc)
			response := &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer, ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()}}
			resp := new(raft_cmdpb.RaftCmdResponse)
			resp.Header = &raft_cmdpb.RaftResponseHeader{}
			resp.AdminResponse = response
			d.HandleProposal(e, func(p *proposal) { p.cb.Done(resp) })
			if d.IsLeader() {
				d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			}
			return false
		}
	}
	if len(msg.Requests) == 0 {
		return false
	}
	var responses []*raft_cmdpb.Response
	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			// log.Infof("left.StartKey: %s, left.EndKey: %s, Key:%s", d.Region().StartKey, d.Region().EndKey, req.Get.Key)
			err := util.CheckKeyInRegion(req.Get.Key, d.Region())
			if err != nil {
				d.HandleProposal(e, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return false
			}
			// log.Infof("question")
			d.peerStorage.applyState.AppliedIndex = e.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			value, _ := engine_util.GetCF(d.peer.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			responses = append(responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: value},
			})
		case raft_cmdpb.CmdType_Put:
			err := util.CheckKeyInRegion(req.Put.Key, d.Region())
			if err != nil {
				d.HandleProposal(e, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return false
			}
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			responses = append(responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     new(raft_cmdpb.PutResponse),
			})
		case raft_cmdpb.CmdType_Delete:
			err := util.CheckKeyInRegion(req.Delete.Key, d.Region())
			if err != nil {
				d.HandleProposal(e, func(p *proposal) {
					p.cb.Done(ErrResp(err))
				})
				return false
			}
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
			responses = append(responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  new(raft_cmdpb.DeleteResponse),
			})
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				d.HandleProposal(e, func(p *proposal) {
					p.cb.Done(resp)
				})
				return false
			}
			d.peerStorage.applyState.AppliedIndex = e.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			kvWB.Reset()
			cloneRegion := &metapb.Region{}
			util.CloneMsg(d.Region(), cloneRegion)
			responses = append([]*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: cloneRegion}}})
			resp := new(raft_cmdpb.RaftCmdResponse)
			resp.Header = &raft_cmdpb.RaftResponseHeader{}
			resp.Responses = responses
			d.HandleProposal(e, func(p *proposal) {
				p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				p.cb.Done(resp)
			})
			return false
		}
	}
	resp := raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: responses,
	}
	d.HandleProposal(e, func(p *proposal) { p.cb.Done(&resp) })
	return false
}

func (d *peerMsgHandler) HandleProposal(e *eraftpb.Entry, proposalFunc func(*proposal)) {
	var i int
	for i = 0; i < len(d.proposals) && d.proposals[i].index < e.Index; i++ {
		d.proposals[i].cb.Done(ErrResp(&util.ErrStaleCommand{}))
	}
	d.proposals = d.proposals[i:]
	if len(d.proposals) > 0 && d.proposals[0].index == e.Index {
		proposal := d.proposals[0]
		if proposal.term != e.Term {
			NotifyStaleReq(e.Term, proposal.cb)
		} else {
			proposalFunc(proposal)
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if d.RaftGroup.HasReady() == false {
		return
	}
	ready := d.RaftGroup.Ready()
	result, _ := d.peerStorage.SaveReadyState(&ready)
	if result != nil {
		if reflect.DeepEqual(result.PrevRegion, d.Region()) {
			meta := d.ctx.storeMeta
			meta.Lock()
			d.peerStorage.region = result.Region
			meta.regions[d.regionId] = d.Region()
			meta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			meta.Unlock()
		}
	}
	if len(ready.Messages) != 0 {
		d.Send(d.ctx.trans, ready.Messages)
	}
	if len(ready.CommittedEntries) > 0 {
		kvWB := &engine_util.WriteBatch{}
		for _, e := range ready.CommittedEntries {
			isDestroyed := d.apply(&e, kvWB)
			if isDestroyed {
				return
			}
		}
		d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	}
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) notisfyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	cloneRegion := new(metapb.Region)
	err := util.CloneMsg(region, cloneRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          cloneRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		// log.Infof("Lead: %d", d.peer.RaftGroup.Raft.Lead)
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		// log.Infof("request type: %d", raftCMD.Request.Requests[0].CmdType)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		// log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		log.Infof("%s on split with %s", d.Tag, split.SplitKey)
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
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		d.peer.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		response := &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_TransferLeader, TransferLeader: &raft_cmdpb.TransferLeaderResponse{}}
		resp := new(raft_cmdpb.RaftCmdResponse)
		resp.Header = &raft_cmdpb.RaftResponseHeader{}
		resp.AdminResponse = response
		cb.Done(resp)
		return
	}
	// if d.peer.RaftGroup.Raft.LeadTransferee != 0 {
	// 	return
	// }
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		if msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && len(d.Region().Peers) == 2 && msg.AdminRequest.ChangePeer.Peer.Id == d.LeaderId() {
			cb.Done(ErrResp(&util.ErrStaleCommand{}))
			for _, peer := range d.Region().Peers {
				if peer.Id != d.LeaderId() {
					d.peer.RaftGroup.TransferLeader(peer.Id)
				}
			}
			return
		}
	}
	d.peer.proposals = append(d.peer.proposals, &proposal{
		cb:    cb,
		index: d.nextProposalIndex(),
		term:  d.Term(),
	})
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.peer.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    context,
		})
		return
	}
	data, _ := msg.Marshal()
	d.peer.RaftGroup.Propose(data)
	// Your Code Here (2B).

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

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
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
		log.Infof("[region %d] raft message %s is stale, current %v ignore it, msg From %d, msg To %d",
			regionID, msgType, curEpoch, msg.Message.From, msg.Message.To)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
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
	meta.Lock()
	defer meta.Unlock()
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
	meta.Lock()
	defer meta.Unlock()
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
	meta.RLock()
	defer meta.RUnlock()
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
		// log.Infof("come in onRaftGCLogGC, appliedIdx: %d, firstIdx:%d, RaftLogGcCountLimit: %d", appliedIdx, firstIdx, d.ctx.cfg.RaftLogGcCountLimit)
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
