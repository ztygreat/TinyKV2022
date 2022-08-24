// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		if store.GetState() == metapb.StoreState_Up && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	if len(suitableStores) <= 1 {
		return nil
	}
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	var originRegion *core.RegionInfo
	var originStore *core.StoreInfo
	var targetStore *core.StoreInfo
	// 按RegionSize从大到小遍历
	for i := 0; i < len(suitableStores); i++ {
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			originRegion = rc.RandomRegion(nil, nil)
		})
		if originRegion != nil {
			originStore = suitableStores[i]
			break
		}
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			originRegion = rc.RandomRegion(nil, nil)
		})
		if originRegion != nil {
			originStore = suitableStores[i]
			break
		}
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			originRegion = rc.RandomRegion(nil, nil)
		})
		if originRegion != nil {
			originStore = suitableStores[i]
			break
		}
	}
	originRegionStores := originRegion.GetStoreIds()
	if originRegion == nil || len(originRegionStores) < cluster.GetMaxReplicas() {
		return nil
	}
	// 按RegionSize从小到大遍历
	for i := len(suitableStores) - 1; i >= 0; i-- {
		_, ok := originRegionStores[suitableStores[i].GetID()]
		if ok {
			continue
		}
		targetStore = suitableStores[i]
		break
	}
	if targetStore == nil || originStore.GetRegionSize()-targetStore.GetRegionSize() <= 2*originRegion.GetApproximateSize() {
		return nil
	}
	targetPeer, _ := cluster.AllocPeer(targetStore.GetID())
	moveOprator, err := operator.CreateMovePeerOperator("balance", cluster, originRegion, operator.OpBalance, originStore.GetID(), targetStore.GetID(), targetPeer.Id)
	if err != nil {
		return nil
	}
	return moveOprator
}
