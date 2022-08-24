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
	"math/rand"
	"sort"

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
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// numOfDeny uint64

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	electionBaseline int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// leadTransferee uint64
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := Raft{}
	raft.id = c.ID
	raft.RaftLog = newLog(c.Storage)
	raft.RaftLog.applied = c.Applied
	if raft.RaftLog.applied < raft.RaftLog.firstIndex-1 {
		raft.RaftLog.applied = raft.RaftLog.firstIndex - 1
	}
	raft.State = StateFollower
	hardState, confState, _ := c.Storage.InitialState()
	raft.Vote = hardState.Vote
	raft.Term = hardState.Term
	raft.votes = make(map[uint64]bool)
	raft.Prs = make(map[uint64]*Progress)
	if len(c.peers) > 0 {
		for _, i := range c.peers {
			raft.Prs[i] = &Progress{}
		}
	} else {
		for _, i := range confState.Nodes {
			raft.Prs[i] = &Progress{}
		}
	}
	raft.leadTransferee = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.electionBaseline = c.ElectionTick
	// rand.Seed(time.Now().Unix())
	raft.electionTimeout = rand.Intn(raft.electionBaseline) + raft.electionBaseline
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.Prs[to].Next < r.RaftLog.firstIndex {
		log.Infof("will genSnapshot, to: %d, to.next: %d, lead.firstIndex: %d, ", to, r.Prs[to].Next, r.RaftLog.firstIndex)
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false
		}
		m := pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snapshot,
			Commit:   r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, m)
		r.Prs[to].Next = snapshot.Metadata.Index + 1
		return true
	}
	var entries []*pb.Entry
	if r.Prs[to].Next <= r.RaftLog.LastIndex() {
		for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
			ent, _ := r.RaftLog.Entires(i, i)
			if len(ent) != 0 {
				entries = append(entries, &ent[0])
			}
		}
	}
	logTerm, _ := r.RaftLog.Term(r.Prs[to].Next - 1)
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.Prs[to].Next - 1,
		LogTerm: logTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

func (r *Raft) sendRequestVote(to uint64) {
	log.Debugf("come here?")
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// log.Infof("r.id: %d, r.heartbeatTimeout: %d, r.heartbeatElapsed: %d, r.electionBaseline: %d, r.electionTimeout: %d, r.electionElapsed: %d, r.State: %d, r.Lead: %d",
	// 	r.id, r.heartbeatTimeout, r.heartbeatElapsed, r.electionBaseline, r.electionTimeout, r.electionElapsed, r.State, r.Lead)
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.State == StateLeader && r.heartbeatElapsed == r.heartbeatTimeout {
		commit := r.RaftLog.committed
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			if r.Prs[k].Match < commit {
				commit = r.Prs[k].Match
			}
			m := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      k,
				From:    r.id,
				Term:    r.Term,
				Commit:  commit,
			}
			r.msgs = append(r.msgs, m)
		}
		r.heartbeatElapsed = 0
	}
	if r.State == StateFollower && r.electionElapsed == r.electionTimeout {
		// log.Infof("r.id: %d, r.Lead: %d", r.id, r.Lead)
		if len(r.Prs) == 0 {
			r.electionElapsed = 0
			return
		}
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			m := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      k,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: logterm,
			}
			r.msgs = append(r.msgs, m)
		}
		r.electionElapsed = 0
	}
	if r.State == StateCandidate && r.electionElapsed == r.electionTimeout {
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			m := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      k,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: logterm,
			}
			r.msgs = append(r.msgs, m)
		}
		r.electionElapsed = 0
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = 0
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.Lead = lead
	r.electionTimeout = rand.Intn(r.electionBaseline) + r.electionBaseline
	r.electionElapsed = 0
	r.leadTransferee = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.Lead = r.id
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionTimeout = rand.Intn(r.electionBaseline) + r.electionBaseline
	r.electionElapsed = 0
	r.leadTransferee = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionTimeout = rand.Intn(r.electionBaseline) + r.electionBaseline
	r.electionElapsed = 0
	r.leadTransferee = 0
	// r.RaftLog.committed = r.RaftLog.applied
	r.RaftLog.nextIndex = r.RaftLog.LastIndex() + 1
	for k := range r.Prs {
		r.Prs[k].Next = r.RaftLog.LastIndex() + 1
		r.Prs[k].Match = 0
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 2
	noop_entry := pb.Entry{}
	noop_entry.Term = r.Term
	noop_entry.Index = r.RaftLog.nextIndex
	r.RaftLog.entries = append(r.RaftLog.entries, noop_entry)
	r.RaftLog.nextIndex++
	if len(r.Prs) == 1 {
		// r.RaftLog.committed++
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
	//
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			m := pb.Message{MsgType: pb.MessageType_MsgHup}
			r.handleHup(m)
		case pb.MessageType_MsgTransferLeader:
			m := pb.Message{MsgType: pb.MessageType_MsgHup}
			r.handleHup(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgPropose:
			if r.leadTransferee == 0 {
				r.handlePropose(m)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			m := pb.Message{MsgType: pb.MessageType_MsgHup}
			r.handleHup(m)
		case pb.MessageType_MsgTransferLeader:
			_, ok := r.Prs[m.From]
			if ok {
				r.leadTransferee = m.From
				if r.Prs[r.leadTransferee].Match < r.RaftLog.LastIndex() {
					r.sendAppend(r.leadTransferee)
				} else {
					r.sendTimeoutNow()
				}
			}
		}
	}
	return nil
}

func (r *Raft) sendTimeoutNow() {
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      r.leadTransferee,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgTimeoutNow,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
		r.Lead = m.From
		r.Term = m.Term
		switch r.State {
		case StateFollower:
			r.electionElapsed = 0
			lastLogIndex := r.RaftLog.LastIndex()
			if m.Index > lastLogIndex {
				// 应返回拒绝
				r.msgs = append(r.msgs, pb.Message{
					From:    m.To,
					To:      m.From,
					Term:    r.Term,
					MsgType: pb.MessageType_MsgAppendResponse,
					Index:   r.RaftLog.committed,
					Reject:  true,
				})
				return
			}
			LogTerm, _ := r.RaftLog.Term(m.Index)
			if m.LogTerm != LogTerm {
				// 应返回拒绝
				r.msgs = append(r.msgs, pb.Message{
					From:    m.To,
					To:      m.From,
					Term:    r.Term,
					Index:   r.RaftLog.committed,
					MsgType: pb.MessageType_MsgAppendResponse,
					Reject:  true,
				})
				return
			}
			r.Lead = m.From
			if len(m.Entries) != 0 {
				start := 0
				for ; start < len(m.Entries); start++ {
					term, _ := r.RaftLog.Term(m.Entries[start].Index)
					if m.Entries[start].Term != term {
						break
					}
				}
				if start < len(m.Entries) {
					pEntries := m.Entries[start:]
					var mEntries []pb.Entry
					for i := 0; i < len(pEntries); i++ {
						mEntries = append(mEntries, *pEntries[i])
					}
					first := pEntries[0].Index
					var offset uint64
					if first <= r.RaftLog.stabled {
						r.RaftLog.stabled = first - 1
					}
					// if r.RaftLog.firstIndex == 1 {
					// 	log.Infof("HandleAppend, id: %d, r.RaftLog.stabled: %d, r.RaftLog.firstIndex: %d", r.id, r.RaftLog.stabled, r.RaftLog.firstIndex)
					// }
					if len(r.RaftLog.entries) == 0 {
						offset = 0
					} else {
						offset = pEntries[0].Index - r.RaftLog.entries[0].Index
					}
					switch {
					case uint64(len(r.RaftLog.entries)) > offset:
						r.RaftLog.entries = append([]pb.Entry{}, r.RaftLog.entries[:offset]...)
						r.RaftLog.entries = append(r.RaftLog.entries, mEntries...)
					case uint64(len(r.RaftLog.entries)) == offset:
						r.RaftLog.entries = append(r.RaftLog.entries, mEntries...)
					}
				}
			}
			if m.Commit > r.RaftLog.committed {
				if m.Commit > r.RaftLog.LastIndex() {
					r.RaftLog.committed = r.RaftLog.LastIndex()
				} else {
					r.RaftLog.committed = m.Commit
				}
				commit := m.Index + uint64(len(m.Entries))
				if r.RaftLog.committed > commit {
					r.RaftLog.committed = commit
				}
			}
			r.msgs = append(r.msgs, pb.Message{
				From:    m.To,
				To:      m.From,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgAppendResponse,
				Index:   m.Index + uint64(len(m.Entries)),
				Reject:  false,
			})
		case StateCandidate:
			r.becomeFollower(m.Term, m.From)
		case StateLeader:
			r.becomeFollower(m.Term, m.From)
		}
	} else {
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
		})
		return
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, 0)
		return
	}
	if m.Reject == false {
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Match = m.Index
		}
		if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		}
		if m.From == r.leadTransferee && r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow()
		}
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
		total := uint64(len(r.Prs))
		if total == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return
		}
		r.checkCommit()
		return
	}
	if m.Reject == true {
		// if r.Prs[m.From].Next-1 > r.Prs[m.From].Match {
		// 	r.Prs[m.From].Next--
		// }
		if m.Index >= r.Prs[m.From].Match {
			r.Prs[m.From].Next = m.Index + 1
		}
		r.sendAppend(m.From)
		return
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
		r.Term = m.Term
		switch r.State {
		case StateFollower:
			r.electionElapsed = 0
			r.Lead = m.From
			// lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			if r.RaftLog.committed < m.Commit {
				if m.Commit > r.RaftLog.LastIndex() {
					r.RaftLog.committed = r.RaftLog.LastIndex()
				} else {
					r.RaftLog.committed = m.Commit
				}
			}
			r.msgs = append(r.msgs, pb.Message{
				From:    m.To,
				To:      m.From,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				Reject:  false,
				Index:   r.RaftLog.committed,
			})
		case StateCandidate:
			r.becomeFollower(m.Term, 0)
		case StateLeader:
			r.becomeFollower(m.Term, 0)
		}
	} else {
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Reject:  true,
		})
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Reject == true {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}
	} else {
		// lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		// if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < r.RaftLog.LastIndex()) {
		// 	r.sendAppend(m.From)
		// }
		if r.Prs[m.From].Match < m.Index {
			r.Prs[m.From].Match = m.Index
		}
		if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		}
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
		r.checkCommit()
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	lastLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	switch r.State {
	case StateFollower:
		if r.Term > m.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		} else if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < r.RaftLog.LastIndex()) {

			if m.Term > r.Term {
				r.Term = m.Term
				r.Vote = 0
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		} else if r.Term < m.Term {
			r.Vote = m.From
			// r.Lead = m.From
			r.Term = m.Term
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			})
			r.electionElapsed = 0
		} else if r.Vote == 0 {
			r.Vote = m.From
			// r.Lead = m.From
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			})
			r.electionElapsed = 0
		} else if r.Vote != 0 && r.Vote == m.From {
			// r.Lead = m.From
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			})
			r.electionTimeout = 0
		} else if r.Vote != 0 && r.Vote != m.From {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	case StateLeader:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, 0)
			r.Term = m.Term
			if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < r.RaftLog.LastIndex()) {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			} else {
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  false,
				})
			}
			r.electionElapsed = 0
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	case StateCandidate:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, 0)
			r.Term = m.Term
			if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < r.RaftLog.LastIndex()) {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			} else {
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  false,
				})
				r.electionElapsed = 0
			}
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	}

}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateCandidate:
		if m.Reject == false {
			r.votes[m.From] = true
			election := 0
			total := len(r.Prs)
			for k := range r.votes {
				if r.votes[k] == true {
					election++
				}
			}
			if election > total/2 {
				r.becomeLeader()
			}
		} else {
			if m.Term > r.Term {
				r.becomeFollower(m.Term, 0)
			}
			r.votes[m.From] = false
			reject := 0
			total := len(r.Prs)
			for k := range r.votes {
				if r.votes[k] == false {
					reject++
				}
			}
			if reject > total/2 {
				r.becomeFollower(r.Term, 0)
			}
		}
	}
}

func (r *Raft) handleHup(m pb.Message) {
	// Your Code Here (2A).
	// log.Infof("r.id: %d, r.Lead: %d, m.MsgType :%d", r.id, r.Lead, m.MsgType)
	_, ok := r.Prs[r.id]
	if ok == false {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      k,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: logterm,
		}
		r.msgs = append(r.msgs, m)
	}
	r.electionElapsed = 0
}

func (r *Raft) handleBeat(m pb.Message) {
	// Your Code Here (2A).
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      k,
			From:    r.id,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, m)
	}
	r.heartbeatElapsed = 0
}

func (r *Raft) handlePropose(m pb.Message) {
	// Your Code Here (2A).
	for i := 0; i < len(m.Entries); i++ {
		m.Entries[i].Term = r.Term
		m.Entries[i].Index = r.RaftLog.nextIndex
		r.RaftLog.nextIndex++
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
		r.Prs[r.id].Match++
		r.Prs[r.id].Next++
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
		if len(r.Prs) == 1 {
			// r.RaftLog.committed++
			r.RaftLog.committed = r.RaftLog.LastIndex()
		}
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term >= r.Term {
		r.Lead = m.From
		r.Term = m.Term
		switch r.State {
		case StateFollower:
			log.Infof("id: %d, Lead: %d, Snapshot.Index: %d, lastLogIndex: %d, m.commit: %d", r.id, r.Lead, m.Snapshot.Metadata.Index, r.RaftLog.LastIndex(), m.Commit)
			r.electionElapsed = 0
			// lastLogIndex := r.RaftLog.LastIndex()
			// lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
			// if m.Snapshot.Metadata.Index <= lastLogIndex || m.Snapshot.Metadata.Term < lastLogTerm {
			if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
				r.msgs = append(r.msgs, pb.Message{
					From:    m.To,
					To:      m.From,
					Term:    r.Term,
					MsgType: pb.MessageType_MsgAppendResponse,
					Index:   r.RaftLog.committed,
					Reject:  false,
				})
				return
			}
			r.RaftLog.applied = m.Snapshot.Metadata.Index
			r.RaftLog.committed = m.Snapshot.Metadata.Index
			r.RaftLog.stabled = m.Snapshot.Metadata.Index
			r.RaftLog.nextIndex = m.Snapshot.Metadata.Index + 1
			r.RaftLog.firstIndex = m.Snapshot.Metadata.Index + 1
			r.RaftLog.entries = []pb.Entry{}
			r.votes = make(map[uint64]bool)
			r.Prs = make(map[uint64]*Progress)
			for _, i := range m.Snapshot.Metadata.ConfState.Nodes {
				r.Prs[i] = &Progress{}
			}
			r.RaftLog.pendingSnapshot = m.Snapshot
			r.msgs = append(r.msgs, pb.Message{
				From:    m.To,
				To:      m.From,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgAppendResponse,
				Index:   m.Snapshot.Metadata.Index,
				Reject:  false,
			})
		case StateCandidate:
			r.becomeFollower(m.Term, m.From)
		case StateLeader:
			r.becomeFollower(m.Term, m.From)
		}
	} else {
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
		})
		return
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{}
	if r.id == r.Lead {
		r.Prs[id].Match = 0
		r.Prs[id].Next = 1
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	if r.id == r.Lead {
		total := uint64(len(r.Prs))
		if total == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return
		}
		r.checkCommit()
	}
}

func (r *Raft) checkCommit() {
	var matchArr []uint64
	for _, v := range r.Prs {
		matchArr = append(matchArr, v.Match)
	}
	sort.Slice(matchArr, func(i, j int) bool {
		return matchArr[i] < matchArr[j]
	})
	half := (len(r.Prs)+1)/2 - 1
	value := matchArr[half]
	vterm, _ := r.RaftLog.Term(value)
	if vterm != r.Term {
		return
	}
	if value > r.RaftLog.committed {
		r.RaftLog.committed = value
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
}
