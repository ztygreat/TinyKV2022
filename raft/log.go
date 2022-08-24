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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	nextIndex uint64

	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	r := RaftLog{}
	r.storage = storage
	r.nextIndex, _ = storage.LastIndex()
	r.nextIndex++
	r.committed = hardState.Commit
	r.stabled, _ = storage.LastIndex()
	r.firstIndex, _ = storage.FirstIndex()
	last, _ := storage.LastIndex()
	if last >= r.firstIndex {
		r.entries, _ = storage.Entries(r.firstIndex, last+1)
	}
	r.pendingSnapshot = &pb.Snapshot{}
	if r.firstIndex == 1 {
		log.Infof("r.firstIndex: %d, r.stabled:%d", r.firstIndex, r.stabled)
	}
	return &r
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndexOfStorage, _ := l.storage.FirstIndex()
	log.Infof("maybeCompact, firstIndexOfStorage: %d, logFirstIndex: %d", firstIndexOfStorage, l.firstIndex)
	start := 0
	for i, e := range l.entries {
		if e.Index < firstIndexOfStorage {
			start = i + 1
		} else {
			break
		}
	}
	l.entries = l.entries[start:]
	l.firstIndex, _ = l.storage.FirstIndex()
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	defer func() {
		if err := recover(); err != nil {
			log.Infof("l.stabled: %d, l.firstIndex: %d, l.lastIndex: %d, commit: %d, apply: %d", l.stabled, l.firstIndex, l.LastIndex(), l.committed, l.applied)
			// if len(l.entries) > 0 {
			// 	log.Infof("l.entries[0].index: %d", l.entries[0].Index)
			// }
			for k, v := range l.entries {
				log.Infof("l.entries[%d].index: %d", k, v.Index)
			}
			panic(err)
		}
	}()
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	// log.Infof("firstIndex: %d, stabledIndex: %d, commit: %d, apply: %d", l.firstIndex, l.stabled, l.committed, l.applied)
	locateAtFirstUnStabled := l.LocateAtIndex(l.stabled + 1)
	return l.entries[locateAtFirstUnStabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, _ = l.Entires(l.applied+1, l.committed)
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.pendingSnapshot.Data) != 0 || l.pendingSnapshot.Metadata != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	length := len(l.entries)
	if length == 0 {
		// return 0
		lastIndex, _ := l.storage.LastIndex()
		return lastIndex
	}
	return l.entries[length-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	defer func() {
		if err := recover(); err != nil {
			log.Infof("i: %d, l.firstIndex: %d, l.lastIndex: %d", i, l.firstIndex, l.LastIndex())
			// if len(l.entries) > 0 {
			// 	log.Infof("l.entries[0].index: %d", l.entries[0].Index)
			// }
			for k, v := range l.entries {
				log.Infof("l.entries[%d].index: %d", k, v.Index)
			}
			panic(err)
		}
	}()
	if len(l.pendingSnapshot.Data) != 0 || l.pendingSnapshot.Metadata != nil {
		if l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if l.pendingSnapshot.Metadata.Index > i {
			return 0, nil
		}
		if len(l.entries) == 0 {
			return 0, nil
		}
	}
	if len(l.entries) == 0 || i < l.firstIndex {
		return l.storage.Term(i)
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	locateAtI := l.LocateAtIndex(i)
	return l.entries[locateAtI].Term, nil
}

func (l *RaftLog) LocateAtIndex(i uint64) uint64 {
	return i - l.firstIndex
}

func (l *RaftLog) Entires(lo, hi uint64) ([]pb.Entry, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Infof("l.stabled: %d, l.firstIndex: %d, l.lastIndex: %d, commit: %d, apply: %d", l.stabled, l.firstIndex, l.LastIndex(), l.committed, l.applied)
			// if len(l.entries) > 0 {
			// 	log.Infof("l.entries[0].index: %d", l.entries[0].Index)
			// }
			for k, v := range l.entries {
				log.Infof("l.entries[%d].index: %d", k, v.Index)
			}
			panic(err)
		}
	}()
	if len(l.entries) == 0 {
		return nil, nil
	}
	if lo < l.firstIndex || hi > l.LastIndex() {
		return nil, nil
	} else {
		locateAtLo := l.LocateAtIndex(lo)
		locateAtHi := l.LocateAtIndex(hi + 1)
		return l.entries[locateAtLo:locateAtHi], nil
	}
}
