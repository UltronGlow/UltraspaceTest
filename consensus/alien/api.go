// Copyright 2021 The utg Authors
// This file is part of the utg library.
//
// The utg library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The utg library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the utg library. If not, see <http://www.gnu.org/licenses/>.

// Package alien implements the delegated-proof-of-stake consensus engine.

package alien

import (
	"container/list"
	"errors"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/consensus"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"github.com/UltronGlow/UltronGlow-Origin/rpc"
	"github.com/shopspring/decimal"
	"math/big"
	"sync"
)

var (
	errNumberTooSmall = errors.New("block number too small")
)

// API is a user facing RPC API to allow controlling the signer and voting
// mechanisms of the delegated-proof-of-stake scheme.
type API struct {
	chain  consensus.ChainHeaderReader
	alien  *Alien
	sCache *list.List
	lock   sync.RWMutex
}

type SnapCache struct {
	number uint64
	s      *Snapshot
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *API) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = api.chain.CurrentHeader()
		log.Info("api GetSnapshot", "number", number)
	} else {
		header = api.chain.GetHeaderByNumber(uint64(number.Int64()))
		log.Info("api GetSnapshot", "number", number.Int64())
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	return api.getSnapshotCache(header)
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *API) GetSnapshotAtHash(hash common.Hash) (*Snapshot, error) {
	log.Info("api GetSnapshotAtHash", "hash", hash)
	header := api.chain.GetHeaderByHash(hash)
	if header == nil {
		return nil, errUnknownBlock
	}
	return api.getSnapshotCache(header)
}

// GetSnapshotAtNumber retrieves the state snapshot at a given block.
func (api *API) GetSnapshotAtNumber(number uint64) (*Snapshot, error) {
	log.Info("api GetSnapshotAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	return api.getSnapshotCache(header)
}

func (api *API) GetSnapshotSignerAtNumber(number uint64) (*SnapshotSign, error) {
	log.Info("api GetSnapshotSignerAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSign := &SnapshotSign{
		LoopStartTime: snapshot.LoopStartTime,
		Signers:       snapshot.Signers,
		Punished:      snapshot.Punished,
		SignPledge:    make(map[common.Address]*SignPledgeItem),
	}
	if isGEPOSNewEffect(number) {
		for miner, item := range snapshot.PosPledge {
			snapshotSign.SignPledge[miner] = &SignPledgeItem{
				TotalAmount: new(big.Int).Set(item.TotalAmount),
				LastPunish:  item.LastPunish,
				DisRate:     new(big.Int).Set(item.DisRate),
			}
		}
	}
	return snapshotSign, err
}

type SnapshotSign struct {
	LoopStartTime uint64                             `json:"loopStartTime"`
	Signers       []*common.Address                  `json:"signers"`
	Punished      map[common.Address]uint64          `json:"punished"`
	SignPledge    map[common.Address]*SignPledgeItem `json:"signpledge"`
}
type SignPledgeItem struct {
	TotalAmount *big.Int `json:"totalamount"`
	LastPunish  uint64   `json:"lastpunish"`
	DisRate     *big.Int `json:"distributerate"`
}

func (api *API) GetSnapshotReleaseAtNumber(number uint64, part string) (*SnapshotRelease, error) {
	log.Info("api GetSnapshotReleaseAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotRelease := &SnapshotRelease{
		CandidatePledge: make(map[common.Address]*PledgeItem),
		FlowPledge:      make(map[common.Address]*PledgeItem),
		FlowRevenue:     make(map[common.Address]*LockBalanceData),
	}
	if part != "" {
		if part == "candidatepledge" {
			snapshotRelease.CandidatePledge = snapshot.CandidatePledge
		} else if part == "flowminerpledge" {
			if number < PledgeRevertLockEffectNumber {
				snapshotRelease.FlowPledge = snapshot.FlowPledge
			}
		} else if part == "rewardlock" {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.RewardLock, api.alien.db)
		} else if part == "flowlock" {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.FlowLock, api.alien.db)
		} else if part == "bandwidthlock" {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.BandwidthLock, api.alien.db)
		} else if part == "posplexit" {
			if snapshot.FlowRevenue.PosPgExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosPgExitLock, api.alien.db)
			}
		} else if part == "posexit" {
			if snapshot.FlowRevenue.PosExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosExitLock, api.alien.db)
			}
		} else if part == "stpentrustexit" {
			if snapshot.FlowRevenue.STPEntrustExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.STPEntrustExitLock, api.alien.db)
			}
		} else if part == "stpentrust" {
			if snapshot.FlowRevenue.STPEntrustLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.STPEntrustLock, api.alien.db)
			}
		} else if part == "splock" {
			if snapshot.FlowRevenue.SpLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpLock, api.alien.db)
			}
		} else if part == "spentrustlock" {
			if snapshot.FlowRevenue.SpEntrustLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustLock, api.alien.db)
			}
		} else if part == "spexit" {
			if snapshot.FlowRevenue.SpExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpExitLock, api.alien.db)
			}
		} else if part == "spentrustexit" {
			if snapshot.FlowRevenue.SpEntrustExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustExitLock, api.alien.db)
			}
		}
	} else {
		snapshotRelease.CandidatePledge = snapshot.CandidatePledge
		if number < PledgeRevertLockEffectNumber {
			snapshotRelease.FlowPledge = snapshot.FlowPledge
		}
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.RewardLock, api.alien.db)
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.FlowLock, api.alien.db)
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.BandwidthLock, api.alien.db)
		if number >= PledgeRevertLockEffectNumber {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosPgExitLock, api.alien.db)
		}
		if isGEPOSNewEffect(number) {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosExitLock, api.alien.db)
		}
		if isGEInitStorageManagerNumber(number) {
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.STPEntrustExitLock, api.alien.db)
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.STPEntrustLock, api.alien.db)
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpLock, api.alien.db)
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustLock, api.alien.db)
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpExitLock, api.alien.db)
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustExitLock, api.alien.db)
		}
	}
	return snapshotRelease, err
}

func (s *SnapshotRelease) appendFRItems(items []*PledgeItem) {
	for _, item := range items {
		if _, ok := s.FlowRevenue[item.TargetAddress]; !ok {
			s.FlowRevenue[item.TargetAddress] = &LockBalanceData{
				RewardBalance:   make(map[uint32]*big.Int),
				LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
				RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
				LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
			}
		}
		flowRevenusTarget := s.FlowRevenue[item.TargetAddress]
		if _, ok := flowRevenusTarget.LockBalanceV1[item.StartHigh]; !ok {
			flowRevenusTarget.LockBalanceV1[item.StartHigh] = make(map[uint32]map[common.Address]*PledgeItem)
		}
		lockBalanceV1 := flowRevenusTarget.LockBalanceV1[item.StartHigh]
		if _, ok := lockBalanceV1[item.PledgeType]; !ok {
			lockBalanceV1[item.PledgeType] = make(map[common.Address]*PledgeItem)
		}
		lockBalanceV1[item.PledgeType][item.RevenueContract] = item
	}
}

func (sr *SnapshotRelease) appendFR(FlowRevenue map[common.Address]*LockBalanceData) error {
	fr1 := FlowRevenue
	for t1, item1 := range fr1 {
		if _, ok := sr.FlowRevenue[t1]; !ok {
			sr.FlowRevenue[t1] = &LockBalanceData{
				RewardBalance:   make(map[uint32]*big.Int),
				LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
				RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
				LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
			}
		}
		rewardBalance := item1.RewardBalance
		for t2, item2 := range rewardBalance {
			sr.FlowRevenue[t1].RewardBalance[t2] = item2
		}
		rewardBalanceV1 := item1.RewardBalanceV1
		for t3, item3 := range rewardBalanceV1 {
			sr.FlowRevenue[t1].RewardBalanceV1[t3] = item3
		}

		lockBalance := item1.LockBalance
		for t3, item3 := range lockBalance {
			if _, ok := sr.FlowRevenue[t1].LockBalance[t3]; !ok {
				sr.FlowRevenue[t1].LockBalance[t3] = make(map[uint32]*PledgeItem)
			}
			t3LockBalance := sr.FlowRevenue[t1].LockBalance[t3]
			for t4, item4 := range item3 {
				if _, ok := t3LockBalance[t4]; !ok {
					t3LockBalance[t4] = item4
				}
			}
		}

		lockBalanceV1 := item1.LockBalanceV1
		if lockBalanceV1 != nil {
			for t3, item3 := range lockBalanceV1 {
				if _, ok := sr.FlowRevenue[t1].LockBalanceV1[t3]; !ok {
					sr.FlowRevenue[t1].LockBalanceV1[t3] = make(map[uint32]map[common.Address]*PledgeItem)
				}
				t3LockBalance := sr.FlowRevenue[t1].LockBalanceV1[t3]
				for t4, item4 := range item3 {
					if _, ok := t3LockBalance[t4]; !ok {
						t3LockBalance[t4] = make(map[common.Address]*PledgeItem)
					}
					for t5, item5 := range item4 {
						if _, ok := t3LockBalance[t4][t5]; !ok {
							t3LockBalance[t4][t5] = item5
						}
					}
				}
			}
		}
	}
	return nil
}

func (sr *SnapshotRelease) appendFRlockData(lockData *LockData, db ethdb.Database) error {
	sr.appendFR(lockData.FlowRevenue)
	items, err := lockData.loadCacheL1(db)
	if err == nil {
		sr.appendFRItems(items)
	}
	items, err = lockData.loadCacheL2(db)
	if err == nil {
		sr.appendFRItems(items)
	}
	return nil
}

type SnapshotRelease struct {
	CandidatePledge map[common.Address]*PledgeItem      `json:"candidatepledge"`
	FlowPledge      map[common.Address]*PledgeItem      `json:"flowminerpledge"`
	FlowRevenue     map[common.Address]*LockBalanceData `json:"flowrevenve"`
}

func (api *API) GetSnapshotFlowAtNumber(number uint64) (*SnapshotFlow, error) {
	log.Info("api GetSnapshotFlowAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	lockReward := make([]FlowRecord, 0)
	if len(headerExtra.LockReward) > 0 {
		for _, item := range headerExtra.LockReward {
			if item.IsReward == sscEnumFlwReward {
				lockReward = append(lockReward, FlowRecord{
					Target:     item.Target,
					Amount:     item.Amount,
					FlowValue1: item.FlowValue1,
					FlowValue2: item.FlowValue2,
				})
			}
		}
	}
	snapshotFlow := &SnapshotFlow{
		LockReward: lockReward,
	}
	return snapshotFlow, err
}

type SnapshotFlow struct {
	LockReward []FlowRecord `json:"flowrecords"`
}

type FlowRecord struct {
	Target     common.Address
	Amount     *big.Int
	FlowValue1 uint64 `json:"realFlowvalue"`
	FlowValue2 uint64 `json:"validFlowvalue"`
}

func (api *API) GetSnapshotFlowMinerAtNumber(number uint64) (*SnapshotFlowMiner, error) {
	log.Info("api GetSnapshotFlowMinerAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotFlowMinerAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	flowMiner := &SnapshotFlowMiner{
		DayStartTime:        snapshot.FlowMiner.DayStartTime,
		FlowMinerPrevTotal:  snapshot.FlowMiner.FlowMinerPrevTotal,
		FlowMiner:           snapshot.FlowMiner.FlowMiner,
		FlowMinerPrev:       snapshot.FlowMiner.FlowMinerPrev,
		FlowMinerReport:     []*FlowMinerReport{},
		FlowMinerPrevReport: []*FlowMinerReport{},
	}
	fMiner := snapshot.FlowMiner
	db := api.alien.db
	items := flowMiner.loadFlowMinerCache(fMiner, fMiner.FlowMinerCache, db)
	flowMiner.FlowMinerReport = append(flowMiner.FlowMinerReport, items...)
	items = flowMiner.loadFlowMinerCache(fMiner, fMiner.FlowMinerPrevCache, db)
	flowMiner.FlowMinerPrevReport = append(flowMiner.FlowMinerPrevReport, items...)
	return flowMiner, err
}

type SnapshotFlowMiner struct {
	DayStartTime        uint64                                              `json:"dayStartTime"`
	FlowMinerPrevTotal  uint64                                              `json:"flowminerPrevTotal"`
	FlowMiner           map[common.Address]map[common.Hash]*FlowMinerReport `json:"flowminerCurr"`
	FlowMinerReport     []*FlowMinerReport                                  `json:"flowminerReport"`
	FlowMinerPrev       map[common.Address]map[common.Hash]*FlowMinerReport `json:"flowminerPrev"`
	FlowMinerPrevReport []*FlowMinerReport                                  `json:"flowminerPrevReport"`
}

func (sf *SnapshotFlowMiner) loadFlowMinerCache(fMiner *FlowMinerSnap, flowMinerCache []string, db ethdb.Database) []*FlowMinerReport {
	item := []*FlowMinerReport{}
	for _, key := range flowMinerCache {
		flows, err := fMiner.load(db, key)
		if err != nil {
			log.Warn("appendFlowMinerCache load cache error", "key", key, "err", err)
			continue
		}
		item = append(item, flows...)
	}
	return item
}

func (api *API) GetSnapshotFlowReportAtNumber(number uint64) (*SnapshotFlowReport, error) {
	log.Info("api GetSnapshotFlowReportAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	flowReport := make([]MinerFlowReportRecord, 0)
	if len(headerExtra.FlowReport) > 0 {
		flowReport = append(flowReport, headerExtra.FlowReport...)
	}
	snapshotFlowReport := &SnapshotFlowReport{
		FlowReport: flowReport,
	}
	return snapshotFlowReport, err
}

type SnapshotFlowReport struct {
	FlowReport []MinerFlowReportRecord `json:"flowreport"`
}

func (api *API) GetLockRewardAtNumber(number uint64) ([]LockRewardRecord, error) {
	log.Info("api GetLockRewardAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	LockReward := make([]LockRewardRecord, 0)
	if len(headerExtra.LockReward) > 0 {
		LockReward = append(LockReward, headerExtra.LockReward...)
	}
	return LockReward, err
}

func (api *API) GetSRTBalAtNumber(number uint64) (*SnapshotSRT, error) {
	log.Info("api GetSRTBalAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSRTBalAtNumber", "err", err)
		return nil, errUnknownBlock
	}

	snapshotSRT := &SnapshotSRT{
		SrtBal: make(map[common.Address]*big.Int),
	}
	if snapshot.SRT != nil {
		srtBal := snapshot.SRT.GetAll()
		if err == nil {
			snapshotSRT.SrtBal = srtBal
		}
	}
	return snapshotSRT, err
}

type SnapshotSRT struct {
	SrtBal map[common.Address]*big.Int `json:"srtbal"`
}

func (api *API) GetSPledgeAtNumber(number uint64) (*SnapshotSPledge, error) {
	log.Info("api GetSPledgeAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledge := &SnapshotSPledge{
		StoragePledge: make(map[common.Address]*SPledge2),
	}

	for pledgeAddr, sPledge := range snapshot.StorageData.StoragePledge {
		snapshotSPledge.StoragePledge[pledgeAddr] = &SPledge2{
			PledgeStatus:                sPledge.PledgeStatus,
			StorageCapacity:             sPledge.StorageSpaces.StorageCapacity,
			Lease:                       make(map[common.Hash]*Lease2),
			LastVerificationTime:        sPledge.LastVerificationTime,
			LastVerificationSuccessTime: sPledge.LastVerificationSuccessTime,
			ValidationFailureTotalTime:  sPledge.ValidationFailureTotalTime,
		}
		if entrust, eok := snapshot.StorageData.StorageEntrust[pledgeAddr]; eok {
			snapshotSPledge.StoragePledge[pledgeAddr].Manager = entrust.Manager
			snapshotSPledge.StoragePledge[pledgeAddr].Sphash = entrust.Sphash
			snapshotSPledge.StoragePledge[pledgeAddr].Spheight = new(big.Int).Set(entrust.Spheight)
			snapshotSPledge.StoragePledge[pledgeAddr].EntrustRate = new(big.Int).Set(entrust.EntrustRate)
			snapshotSPledge.StoragePledge[pledgeAddr].ManagerAmount = new(big.Int).Set(entrust.ManagerAmount)
			snapshotSPledge.StoragePledge[pledgeAddr].Managerheight = new(big.Int).Set(entrust.Managerheight)
			snapshotSPledge.StoragePledge[pledgeAddr].HavAmount = new(big.Int).Set(entrust.PledgeAmount)
			snapshotSPledge.StoragePledge[pledgeAddr].Detail = make(map[common.Hash]*SnEntrustDetail2, 0)
			if len(entrust.Detail) > 0 {
				for hash, eDtail := range entrust.Detail {
					snapshotSPledge.StoragePledge[pledgeAddr].Detail[hash] = &SnEntrustDetail2{
						Address: eDtail.Address,
						Height:  eDtail.Height,
						Amount:  eDtail.Amount,
					}
				}
			}
		}
		lease := sPledge.Lease
		for hash, l := range lease {
			lease2 := &Lease2{
				Address:                     l.Address,
				Status:                      l.Status,
				LastVerificationTime:        l.LastVerificationTime,
				LastVerificationSuccessTime: l.LastVerificationSuccessTime,
				ValidationFailureTotalTime:  l.ValidationFailureTotalTime,
				LeaseList:                   make(map[common.Hash]*LeaseDetail2),
			}
			ll := l.LeaseList
			for lhash, item := range ll {
				lease2.LeaseList[lhash] = &LeaseDetail2{
					Deposit: item.Deposit,
				}
			}
			snapshotSPledge.StoragePledge[pledgeAddr].Lease[hash] = lease2
		}
	}
	return snapshotSPledge, err
}

type SnapshotSPledge struct {
	StoragePledge map[common.Address]*SPledge2 `json:"spledge"`
}

type SPledge2 struct {
	PledgeStatus                *big.Int                          `json:"pledgeStatus"`
	StorageCapacity             *big.Int                          `json:"storagecapacity"`
	Lease                       map[common.Hash]*Lease2           `json:"lease"`
	LastVerificationTime        *big.Int                          `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int                          `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int                          `json:"validationfailuretotaltime"`
	Manager                     common.Address                    `json:"manager"`
	Sphash                      common.Hash                       `json:"sphash"`
	Spheight                    *big.Int                          `json:"spheight"`
	EntrustRate                 *big.Int                          `json:"entrustRate"`
	ManagerAmount               *big.Int                          `json:"managerAmount"`
	Managerheight               *big.Int                          `json:"managerheight"`
	HavAmount                   *big.Int                          `json:"havAmount"`
	Detail                      map[common.Hash]*SnEntrustDetail2 `json:"entrustDetail"`
}
type Lease2 struct {
	Address                     common.Address                `json:"address"`
	Status                      int                           `json:"status"`
	LastVerificationTime        *big.Int                      `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int                      `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int                      `json:"validationfailuretotaltime"`
	LeaseList                   map[common.Hash]*LeaseDetail2 `json:"leaselist"`
}
type LeaseDetail2 struct {
	Deposit *big.Int `json:"deposit"`
}
type SnEntrustDetail2 struct {
	Address common.Address `json:"address"`
	Height  *big.Int       `json:"height"`
	Amount  *big.Int       `json:"amount"`
}

func (api *API) GetStorageRewardAtNumber(number uint64, part string) (*SnapshotStorageReward, error) {
	log.Info("api GetStorageRewardAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetStoragePledgeRewardAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotStorageReward := &SnapshotStorageReward{
		StorageReward: StorageReward{
			Reward:     make([]SpaceRewardRecord, 0),
			LockPeriod: snapshot.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod,
			RlsPeriod:  snapshot.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod,
			Interval:   snapshot.SystemConfig.LockParameters[sscEnumRwdLock].Interval,
		},
	}
	if part == "spaceLock" || part == "" {
		reward, err2 := NewStorageSnap().loadLockReward(api.alien.db, number, storagePledgeRewardkey)
		if err2 == nil && reward != nil && len(reward) > 0 {
			snapshotStorageReward.StorageReward.Reward = append(snapshotStorageReward.StorageReward.Reward, reward...)
		}
	}
	if part == "leaseLock" {
		reward, err2 := NewStorageSnap().loadLockReward(api.alien.db, number, storageLeaseRewardkey)
		if err2 == nil && reward != nil && len(reward) > 0 {
			snapshotStorageReward.StorageReward.Reward = append(snapshotStorageReward.StorageReward.Reward, reward...)
		}
	}
	if part == "revertLock" {
		reward, err2 := NewStorageSnap().loadLockReward(api.alien.db, number, revertSpaceLockRewardkey)
		if err2 == nil && reward != nil && len(reward) > 0 {
			snapshotStorageReward.StorageReward.Reward = append(snapshotStorageReward.StorageReward.Reward, reward...)
		}
	}
	if part == "blockLock" {
		if number >= StorageEffectBlockNumber {
			headerExtra := HeaderExtra{}
			err3 := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
			if err3 != nil {
				log.Info("Fail to decode header Extra", "err", err3)
				return nil, err3
			}
			if len(headerExtra.LockReward) > 0 {
				for _, item := range headerExtra.LockReward {
					if sscEnumSignerReward == item.IsReward {
						revenueAddress := item.Target
						if revenue, ok := snapshot.RevenueNormal[item.Target]; ok {
							revenueAddress = revenue.RevenueAddress
						}
						spaceRewardRecord := SpaceRewardRecord{
							Target:  item.Target,
							Amount:  item.Amount,
							Revenue: revenueAddress,
						}
						snapshotStorageReward.StorageReward.Reward = append(snapshotStorageReward.StorageReward.Reward, spaceRewardRecord)
					}
				}
			}

			reward, err2 := NewStorageSnap().loadLockReward(api.alien.db, number, signerRewardKey)
			if err2 == nil && reward != nil && len(reward) > 0 {
				snapshotStorageReward.StorageReward.Reward = append(snapshotStorageReward.StorageReward.Reward, reward...)
			}
		}
	}
	return snapshotStorageReward, err
}

type SnapshotStorageReward struct {
	StorageReward StorageReward `json:"storagereward"`
}

type StorageReward struct {
	Reward     []SpaceRewardRecord `json:"reward"`
	LockPeriod uint32              `json:"LockPeriod"`
	RlsPeriod  uint32              `json:"ReleasePeriod"`
	Interval   uint32              `json:"ReleaseInterval"`
}

func (api *API) GetStorageRatiosAtNumber(number uint64) (*SnapshotStorageRatios, error) {
	log.Info("api GetStorageRatiosAtNumber", "number", number)
	snapshotStorageRatios := &SnapshotStorageRatios{
		Ratios: make(map[common.Address]*StorageRatio),
	}
	ratios, err := NewStorageSnap().lockStorageRatios(api.alien.db, number)
	if err == nil && ratios != nil && len(ratios) > 0 {
		snapshotStorageRatios.Ratios = ratios
	}
	return snapshotStorageRatios, err
}

type SnapshotStorageRatios struct {
	Ratios map[common.Address]*StorageRatio `json:"ratios"`
}

type SnapshotRevertSRT struct {
	RevertSRT []ExchangeSRTRecord `json:"revertsrt"`
}

func (api *API) GetRevertSRTAtNumber(number uint64) (*SnapshotRevertSRT, error) {
	log.Info("api GetRevertSRTAtNumber", "number", number)
	revertSRT, err := NewStorageSnap().lockRevertSRT(api.alien.db, number)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	snapshotRevertSRT := &SnapshotRevertSRT{
		RevertSRT: revertSRT,
	}
	return snapshotRevertSRT, nil
}

type SnapshotAddrSRT struct {
	AddrSrtBal *big.Int `json:"addrsrtbal"`
}

func (api *API) GetSRTBalanceAtNumber(address common.Address, number uint64) (*SnapshotAddrSRT, error) {
	log.Info("api GetSRTBalanceAtNumber", "address", address, "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSRTBalanceAtNumber", "err", err)
		return nil, errUnknownBlock
	}

	snapshotAddrSRT := &SnapshotAddrSRT{
		AddrSrtBal: common.Big0,
	}
	if snapshot.SRT != nil {
		snapshotAddrSRT.AddrSrtBal = snapshot.SRT.Get(address)
	}

	return snapshotAddrSRT, nil
}

func (api *API) GetSRTBalance(address common.Address) (*SnapshotAddrSRT, error) {
	log.Info("api GetSRTBalance", "address", address)
	header := api.chain.CurrentHeader()
	if header == nil {
		return nil, errUnknownBlock
	}
	return api.GetSRTBalanceAtNumber(address, header.Number.Uint64())
}

func (api *API) GetSPledgeInfoByAddr(address common.Address) (*SnapshotSPledgeInfo, error) {
	log.Info("api GetSPledgeInfoByAddr", "address", address)
	header := api.chain.CurrentHeader()
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeInfoByAddr", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledgeInfo := &SnapshotSPledgeInfo{
		SPledgeInfo: make(map[common.Address]*SPledge3),
	}
	for pledgeAddr, sPledge := range snapshot.StorageData.StoragePledge {
		if pledgeAddr == address {
			leftCapacity := snapshot.StorageData.StoragePledge[pledgeAddr].StorageSpaces.StorageCapacity
			snapshotSPledgeInfo.SPledgeInfo[pledgeAddr] = &SPledge3{
				PledgeStatus:  sPledge.PledgeStatus,
				TotalCapacity: new(big.Int).Set(sPledge.TotalCapacity),
				LeftCapacity:  new(big.Int).Set(leftCapacity),
				Lease:         make([]Lease3, 0),
			}
			lease := sPledge.Lease
			for hash, l := range lease {
				snapshotSPledgeInfo.SPledgeInfo[pledgeAddr].Lease = append(snapshotSPledgeInfo.SPledgeInfo[pledgeAddr].Lease, Lease3{
					Status: l.Status,
					Hash:   hash,
				})
			}
		}
	}
	return snapshotSPledgeInfo, err
}

type SnapshotSPledgeInfo struct {
	SPledgeInfo map[common.Address]*SPledge3 `json:"spledgeinfo"`
}

type SPledge3 struct {
	PledgeStatus  *big.Int `json:"pledgeStatus"`
	TotalCapacity *big.Int `json:"totalcapacity"`
	LeftCapacity  *big.Int `json:"leftcapacity"`
	Lease         []Lease3 `json:"lease"`
}

type Lease3 struct {
	Hash   common.Hash `json:"hash"`
	Status int         `json:"status"`
}

func (api *API) GetSPledgeCapVerAtNumber(number uint64) (*SnapshotSPledgeCapVer, error) {
	log.Info("api GetSPledgeCapVerAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeCapVerAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledgeCapVer := &SnapshotSPledgeCapVer{
		SpledgeCapVer: api.calStorageVerifyPercentage(number, snapshot.getBlockPreDay(), snapshot.StorageData.copy()),
	}
	return snapshotSPledgeCapVer, err
}

type SnapshotSPledgeCapVer struct {
	SpledgeCapVer map[common.Address]*big.Int `json:"spledgecapver"`
}

func (api *API) calStorageVerifyPercentage(number uint64, blockPerday uint64, s *StorageData) map[common.Address]*big.Int {
	capSuccPer := make(map[common.Address]*big.Int, 0)
	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Set(zeroTime)
	for pledgeAddr, sPledge := range s.StoragePledge {
		capSucc := big.NewInt(0)
		storagespaces := s.StoragePledge[pledgeAddr].StorageSpaces
		sfiles := storagespaces.StorageFile
		for _, sfile := range sfiles {
			lastVerSuccTime := sfile.LastVerificationSuccessTime
			if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {

			} else {
				capSucc = new(big.Int).Add(capSucc, sfile.Capacity)
			}
		}
		leases := make(map[common.Hash]*Lease)
		for lhash, l := range sPledge.Lease {
			if l.Status == LeaseNormal || l.Status == LeaseBreach {
				leases[lhash] = l
			}
		}
		for _, lease := range leases {
			storageFile := lease.StorageFile
			for _, file := range storageFile {
				lastVerSuccTime := file.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {

				} else {
					capSucc = new(big.Int).Add(capSucc, file.Capacity)
				}
			}
		}
		per := new(big.Int).Mul(capSucc, big.NewInt(100))
		per = new(big.Int).Div(per, sPledge.TotalCapacity)
		capSuccPer[pledgeAddr] = per
	}
	return capSuccPer
}

type SnapshotSPledgeValue struct {
	SpledgeValue *big.Int `json:"spledgevalue"`
}

func (api *API) GetStorageValueAtNumber(number uint64, part string) (*SnapshotSPledgeValue, error) {
	log.Info("api GetStorageValueAtNumber", "number", number, "part", part)
	snapshotStorage := &SnapshotSPledgeValue{
		SpledgeValue: common.Big0,
	}
	key := originalTotalCapacityKey
	var err error
	var v *big.Int
	if part == "totalPledgeReward" {
		key = totalPledgeRewardKey
	}
	if part == "storageHarvest" {
		key = storageHarvestKey
	}
	if part == "leaseHarvest" {
		key = leaseHarvestKey
	}
	v, err = NewStorageSnap().loadSpledgeValue(api.alien.db, number, key)
	if err == nil && v != nil {
		snapshotStorage.SpledgeValue = v
	}
	return snapshotStorage, err
}

type SnapshotSPledgeDecimalValue struct {
	SpledgeDecimalValue decimal.Decimal `json:"spledgedecimalvalue"`
}

func (api *API) GetStorageDecimalValueAtNumber(number uint64, part string) (*SnapshotSPledgeDecimalValue, error) {
	log.Info("api GetStorageDecimalValueAtNumber", "number", number, "part", part)
	snapshotStorage := &SnapshotSPledgeDecimalValue{
		SpledgeDecimalValue: decimal.Zero,
	}
	key := totalLeaseSpaceKey
	var err error
	var v decimal.Decimal
	v, err = NewStorageSnap().loadSpledgeDecimalValue(api.alien.db, number, key)
	if err == nil {
		snapshotStorage.SpledgeDecimalValue = v
	}
	return snapshotStorage, err
}

type SnapshotSPledgeRatioValue struct {
	SpledgeRatioValue decimal.Decimal `json:"spledgeratiovalue"`
}

func (api *API) GetStorageRatioValueAtNumber(number uint64, value *big.Int, part string) (*SnapshotSPledgeRatioValue, error) {
	log.Info("api GetStorageRatioValueAtNumber", "number", number, "value", value, "part", part)
	snapshotStorage := &SnapshotSPledgeRatioValue{
		SpledgeRatioValue: decimal.Zero,
	}
	var v decimal.Decimal
	if part == "Bandwidth" {
		v = getBandwaith(value, number)
	}
	if part == "StorageRatio" {
		v = NewStorageSnap().calStorageRatio(value, number)
	}
	snapshotStorage.SpledgeRatioValue = v
	return snapshotStorage, nil
}

type SnapshotSucSPledge struct {
	SucSPledge []common.Address `json:"sucspledge"`
}

func (api *API) GetSucSPledgeAtNumber(number uint64) (*SnapshotSucSPledge, error) {
	log.Info("api GetSucSPledgeAtNumber", "number", number)
	snapshotSucSPledge := &SnapshotSucSPledge{
		SucSPledge: make([]common.Address, 0),
	}
	sucspledge, err := NewStorageSnap().loadSPledgeSucc(api.alien.db, number)
	if err == nil && sucspledge != nil && len(sucspledge) > 0 {
		snapshotSucSPledge.SucSPledge = sucspledge
	}
	return snapshotSucSPledge, err
}

type SnapshotRentSuc struct {
	RentSuc []common.Hash `json:"rentsuc"`
}

func (api *API) GetRentSucAtNumber(number uint64) (*SnapshotRentSuc, error) {
	log.Info("api GetRentSucAtNumber", "number", number)
	snapshotRentSuc := &SnapshotRentSuc{
		RentSuc: make([]common.Hash, 0),
	}
	rentSuc, err := NewStorageSnap().loadRentSucc(api.alien.db, number)
	if err == nil && rentSuc != nil && len(rentSuc) > 0 {
		snapshotRentSuc.RentSuc = rentSuc
	}
	return snapshotRentSuc, err
}

type SnapshotCapSuccAddrs struct {
	CapSuccAddrs map[common.Address]*big.Int `json:"capsuccaddrs"`
}

func (api *API) GetCapSuccAddrsAtNumber(number uint64) (*SnapshotCapSuccAddrs, error) {
	log.Info("api GetCapSuccAddrsAtNumber", "number", number)
	snapshotCapSuccAddrs := &SnapshotCapSuccAddrs{
		CapSuccAddrs: make(map[common.Address]*big.Int),
	}
	capSuccAddrs, err := NewStorageSnap().loadCapSuccAddrs(api.alien.db, number)
	if err == nil && capSuccAddrs != nil && len(capSuccAddrs) > 0 {
		snapshotCapSuccAddrs.CapSuccAddrs = capSuccAddrs
	}
	return snapshotCapSuccAddrs, err
}

func (api *API) GetGrantProfitAtNumber(number uint64) ([]consensus.GrantProfitRecord, error) {
	log.Info("api GetGrantProfitAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	grantProfit := make([]consensus.GrantProfitRecord, 0)
	if len(headerExtra.GrantProfit) > 0 {
		grantProfit = append(grantProfit, headerExtra.GrantProfit...)
	}
	return grantProfit, err
}

type SnapshotSTGbwMakeup struct {
	STGBandwidthMakeup map[common.Address]*BandwidthMakeup `json:"stgbandwidthmakeup"`
}

func (api *API) GetSTGBandwidthMakeup() (*SnapshotSTGbwMakeup, error) {
	log.Info("api GetSTGBandwidthMakeup", "number", PosrIncentiveEffectNumber)
	header := api.chain.GetHeaderByNumber(PosrIncentiveEffectNumber)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeCapVerAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSTGbwMakeup := &SnapshotSTGbwMakeup{
		STGBandwidthMakeup: snapshot.STGBandwidthMakeup,
	}
	return snapshotSTGbwMakeup, err
}

func (api *API) getSnapshotCache(header *types.Header) (*Snapshot, error) {
	number := header.Number.Uint64()
	s := api.findInSnapCache(number)
	if nil != s {
		return s, nil
	}
	return api.getSnapshotByHeader(header)
}

func (api *API) findInSnapCache(number uint64) *Snapshot {
	for i := api.sCache.Front(); i != nil; i = i.Next() {
		v := i.Value.(SnapCache)
		if v.number == number {
			return v.s
		}
	}
	return nil
}

func (api *API) getSnapshotByHeader(header *types.Header) (*Snapshot, error) {
	api.lock.Lock()
	defer api.lock.Unlock()
	number := header.Number.Uint64()
	s := api.findInSnapCache(number)
	if nil != s {
		return s, nil
	}
	cacheSize := 32
	snapshot, err := api.alien.snapshot(api.chain, number, header.Hash(), nil, nil, defaultLoopCntRecalculateSigners)
	if err != nil {
		log.Warn("Fail to getSnapshotByHeader", "err", err)
		return nil, errUnknownBlock
	}
	api.sCache.PushBack(SnapCache{
		number: number,
		s:      snapshot,
	})
	if api.sCache.Len() > cacheSize {
		api.sCache.Remove(api.sCache.Front())
	}
	return snapshot, nil
}

func (api *API) GetSnapshotReleaseAtNumber2(number uint64, part string, startLNum uint64, endLNum uint64) (*SnapshotRelease, error) {
	log.Info("api GetSnapshotReleaseAtNumber2", "number", number, "part", part, "startLNum", startLNum, "endLNum", endLNum)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotRelease := &SnapshotRelease{
		CandidatePledge: make(map[common.Address]*PledgeItem),
		FlowPledge:      make(map[common.Address]*PledgeItem),
		FlowRevenue:     make(map[common.Address]*LockBalanceData),
	}
	if part != "" {
		if part == "candidatepledge" {
			snapshotRelease.CandidatePledge = snapshot.CandidatePledge
		} else if part == "flowminerpledge" {
			if number < PledgeRevertLockEffectNumber {
				snapshotRelease.FlowPledge = snapshot.FlowPledge
			}
		} else if part == "rewardlock" {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.RewardLock, api.alien.db, startLNum, endLNum)
		} else if part == "flowlock" {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.FlowLock, api.alien.db, startLNum, endLNum)
		} else if part == "bandwidthlock" {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.BandwidthLock, api.alien.db, startLNum, endLNum)
		} else if part == "posplexit" {
			if snapshot.FlowRevenue.PosPgExitLock != nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosPgExitLock, api.alien.db, startLNum, endLNum)
			}
		} else if part == "posexit" {
			if snapshot.FlowRevenue.PosExitLock != nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosExitLock, api.alien.db, startLNum, endLNum)
			}
		} else if part == "stpentrustexit" {
			if snapshot.FlowRevenue.STPEntrustExitLock != nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.STPEntrustExitLock, api.alien.db, startLNum, endLNum)
			}
		} else if part == "stpentrust" {
			if snapshot.FlowRevenue.STPEntrustLock != nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.STPEntrustLock, api.alien.db, startLNum, endLNum)
			}
		} else if part == "splock" {
			if snapshot.FlowRevenue.SpLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpLock, api.alien.db)
			}
		} else if part == "spentrustlock" {
			if snapshot.FlowRevenue.SpEntrustLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustLock, api.alien.db)
			}
		} else if part == "spexit" {
			if snapshot.FlowRevenue.SpExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpExitLock, api.alien.db)
			}
		} else if part == "spentrustexit" {
			if snapshot.FlowRevenue.SpEntrustExitLock != nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.SpEntrustExitLock, api.alien.db)
			}
		}
	} else {
		snapshotRelease.CandidatePledge = snapshot.CandidatePledge
		if number < PledgeRevertLockEffectNumber {
			snapshotRelease.FlowPledge = snapshot.FlowPledge
		}
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.RewardLock, api.alien.db, startLNum, endLNum)
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.FlowLock, api.alien.db, startLNum, endLNum)
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.BandwidthLock, api.alien.db, startLNum, endLNum)
		if number >= PledgeRevertLockEffectNumber {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosPgExitLock, api.alien.db, startLNum, endLNum)
		}
		if isGEPOSNewEffect(number) {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosExitLock, api.alien.db, startLNum, endLNum)
		}
		if isGEInitStorageManagerNumber(number) {
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.STPEntrustExitLock, api.alien.db, startLNum, endLNum)
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.STPEntrustLock, api.alien.db, startLNum, endLNum)
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.SpLock, api.alien.db, startLNum, endLNum)
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.SpEntrustLock, api.alien.db, startLNum, endLNum)
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.SpExitLock, api.alien.db, startLNum, endLNum)
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.SpEntrustExitLock, api.alien.db, startLNum, endLNum)
		}
	}
	return snapshotRelease, err
}

func (sr *SnapshotRelease) appendFRlockData2(lockData *LockData, db ethdb.Database, startLNum uint64, endLNum uint64) error {
	sr.appendFR2(lockData.FlowRevenue, startLNum, endLNum)
	items, err := lockData.loadCacheL1(db)
	if err == nil {
		sr.appendFRItems2(items, startLNum, endLNum)
	}
	items, err = lockData.loadCacheL2(db)
	if err == nil {
		sr.appendFRItems2(items, startLNum, endLNum)
	}
	return nil
}
func (s *SnapshotRelease) appendFRItems2(items []*PledgeItem, startLNum uint64, endLNum uint64) {
	for _, item := range items {
		if _, ok := s.FlowRevenue[item.TargetAddress]; !ok {
			s.FlowRevenue[item.TargetAddress] = &LockBalanceData{
				RewardBalance:   make(map[uint32]*big.Int),
				LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
				RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
				LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
			}
		}
		if inLNumScope(item.StartHigh, startLNum, endLNum) {
			flowRevenusTarget := s.FlowRevenue[item.TargetAddress]
			if _, ok := flowRevenusTarget.LockBalanceV1[item.StartHigh]; !ok {
				flowRevenusTarget.LockBalanceV1[item.StartHigh] = make(map[uint32]map[common.Address]*PledgeItem)
			}
			lockBalanceV1 := flowRevenusTarget.LockBalanceV1[item.StartHigh]
			if _, ok := lockBalanceV1[item.PledgeType]; !ok {
				lockBalanceV1[item.PledgeType] = make(map[common.Address]*PledgeItem)
			}
			lockBalanceV1[item.PledgeType][item.RevenueContract] = item
		}
	}
}

func (sr *SnapshotRelease) appendFR2(FlowRevenue map[common.Address]*LockBalanceData, startLNum uint64, endLNum uint64) error {
	fr1 := FlowRevenue
	for t1, item1 := range fr1 {
		if _, ok := sr.FlowRevenue[t1]; !ok {
			sr.FlowRevenue[t1] = &LockBalanceData{
				RewardBalance:   make(map[uint32]*big.Int),
				LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
				RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
				LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
			}
		}
		rewardBalance := item1.RewardBalance
		for t2, item2 := range rewardBalance {
			sr.FlowRevenue[t1].RewardBalance[t2] = item2
		}
		lockBalance := item1.LockBalance
		for t3, item3 := range lockBalance {
			if inLNumScope(t3, startLNum, endLNum) {
				if _, ok := sr.FlowRevenue[t1].LockBalance[t3]; !ok {
					sr.FlowRevenue[t1].LockBalance[t3] = make(map[uint32]*PledgeItem)
				}
				t3LockBalance := sr.FlowRevenue[t1].LockBalance[t3]
				for t4, item4 := range item3 {
					if _, ok := t3LockBalance[t4]; !ok {
						t3LockBalance[t4] = item4
					}
				}
			}
		}

		lockBalanceV1 := item1.LockBalanceV1
		if lockBalanceV1 != nil {
			for t3, item3 := range lockBalanceV1 {
				if inLNumScope(t3, startLNum, endLNum) {
					if _, ok := sr.FlowRevenue[t1].LockBalanceV1[t3]; !ok {
						sr.FlowRevenue[t1].LockBalanceV1[t3] = make(map[uint32]map[common.Address]*PledgeItem)
					}
					t3LockBalance := sr.FlowRevenue[t1].LockBalanceV1[t3]
					for t4, item4 := range item3 {
						if _, ok := t3LockBalance[t4]; !ok {
							t3LockBalance[t4] = make(map[common.Address]*PledgeItem)
						}
						for t5, item5 := range item4 {
							if _, ok := t3LockBalance[t4][t5]; !ok {
								t3LockBalance[t4][t5] = item5
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func inLNumScope(num uint64, startLNum uint64, endLNum uint64) bool {
	if num >= startLNum && num <= endLNum {
		return true
	}
	return false
}

type SnapCanAutoExit struct {
	CandidateAutoExit []common.Address `json:"candidateautoexit"`
}

func (api *API) GetCandidateAutoExitAtNumber(number uint64) (*SnapCanAutoExit, error) {
	log.Info("api GetCandidateAutoExitAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil, err
	}
	snapCanAutoExit := &SnapCanAutoExit{
		CandidateAutoExit: make([]common.Address, 0),
	}
	if len(headerExtra.CandidateAutoExit) > 0 {
		snapCanAutoExit.CandidateAutoExit = append(snapCanAutoExit.CandidateAutoExit, headerExtra.CandidateAutoExit...)
	}
	return snapCanAutoExit, err
}

type SnapshotTLS struct {
	TotalLeaseSpace *big.Int `json:"totalleasespace"`
}

// totalleasespace
func (api *API) GetSnapshotTLSAtNumber(number uint64) (*SnapshotTLS, error) {
	log.Info("api GetSnapshotTLSAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotTLSAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotTLS := &SnapshotTLS{
		TotalLeaseSpace: new(big.Int).Set(snapshot.TotalLeaseSpace),
	}
	return snapshotTLS, err
}
func (api *API) GetSPoolAtNumber(number uint64) (*SpDataApi, error) {
	log.Info("api GetSPDataAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPDataAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPdata := &SpDataApi{
		PoolPledgeItem: make(map[common.Hash]*PoolPledgeApi),
	}
	for hash, spool := range snapshot.SpData.PoolPledge {
		snapshotSPdata.PoolPledgeItem[hash] = &PoolPledgeApi{
			SpAddr:         common.BigToAddress(hash.Big()),
			Address:        spool.Address,
			Manager:        spool.Manager,
			Number:         new(big.Int).Set(spool.Number),
			TotalAmount:    new(big.Int).Set(spool.TotalAmount),
			TotalCapacity:  new(big.Int).Set(spool.TotalCapacity),
			UsedCapacity:   new(big.Int).Set(spool.UsedCapacity),
			PunishNumber:   new(big.Int).Set(spool.PunishNumber),
			SnRatio:        new(big.Int).Set(spool.SnRatio),
			RevenueAddress: spool.RevenueAddress,
			ManagerAmount:  new(big.Int).Set(spool.ManagerAmount),
			Fee:            spool.Fee,
			EntrustRate:    spool.EntrustRate,
			Status:         spool.Status,
			EtDetail:       make(map[common.Hash]*EntrustDetailApi, 0),
		}
		for pledgeHash, detail := range spool.EtDetail {
			snapshotSPdata.PoolPledgeItem[hash].EtDetail[pledgeHash] = &EntrustDetailApi{
				Address: detail.Address,
				Height:  new(big.Int).Set(detail.Height),
				Amount:  new(big.Int).Set(detail.Amount),
			}
		}
	}

	return snapshotSPdata, nil
}

type SpDataApi struct {
	PoolPledgeItem map[common.Hash]*PoolPledgeApi `json:"poolpledge"`
}

type PoolPledgeApi struct {
	SpAddr         common.Address                    `json:"spAddr"`
	Address        common.Address                    `json:"address"`
	Manager        common.Address                    `json:"manager"`
	Number         *big.Int                          `json:"number"`
	TotalAmount    *big.Int                          `json:"totalAmount"`
	TotalCapacity  *big.Int                          `json:"totalcapacity"`
	UsedCapacity   *big.Int                          `json:"usedcapacity"`
	PunishNumber   *big.Int                          `json:"punishNumber"`
	SnRatio        *big.Int                          `json:"snRatio"`
	RevenueAddress common.Address                    `json:"revenueAddress"`
	ManagerAmount  *big.Int                          `json:"managerAmount"`
	Fee            uint64                            `json:"fee"`
	EntrustRate    uint64                            `json:"entrustRate"`
	Status         uint64                            `json:"status"`
	EtDetail       map[common.Hash]*EntrustDetailApi `json:"entrustDetail"`
}
type EntrustDetailApi struct {
	Address common.Address `json:"address"`
	Height  *big.Int       `json:"height"`
	Amount  *big.Int       `json:"amount"`
}

type SnapshotRewardBalanceV1 struct {
	FlowRevenue map[common.Address]*RewardBalanceV1Data `json:"flowrevenve"`
}

type RewardBalanceV1Data struct {
	RewardBalanceV1 map[uint32]map[common.Address]*LockTmpData `json:"rewardbalanceV1"`
}

func (api *API) GetSnapshotRewardBalanceV1(number uint64, part string) (*SnapshotRewardBalanceV1, error) {
	log.Info("api SnapshotRewardBalanceV1", "number", number, "part", part)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	snapshot, err := api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to SnapshotRewardBalanceV1", "err", err)
		return nil, errUnknownBlock
	}
	snapshotRelease := &SnapshotRewardBalanceV1{
		FlowRevenue: make(map[common.Address]*RewardBalanceV1Data),
	}
	if part != "" {
		if part == "rewardlock" {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.RewardLock)
		} else if part == "flowlock" {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.FlowLock)
		} else if part == "bandwidthlock" {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.BandwidthLock)
		} else if part == "posplexit" {
			if snapshot.FlowRevenue.PosPgExitLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.PosPgExitLock)
			}
		} else if part == "posexit" {
			if snapshot.FlowRevenue.PosExitLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.PosExitLock)
			}
		} else if part == "stpentrustexit" {
			if snapshot.FlowRevenue.STPEntrustExitLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.STPEntrustExitLock)
			}
		} else if part == "stpentrust" {
			if snapshot.FlowRevenue.STPEntrustLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.STPEntrustLock)
			}
		} else if part == "splock" {
			if snapshot.FlowRevenue.SpLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpLock)
			}
		} else if part == "spentrustlock" {
			if snapshot.FlowRevenue.SpEntrustLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpEntrustLock)
			}
		} else if part == "spexit" {
			if snapshot.FlowRevenue.SpExitLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpExitLock)
			}
		} else if part == "spentrustexit" {
			if snapshot.FlowRevenue.SpEntrustExitLock != nil {
				snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpEntrustExitLock)
			}
		}
	} else {
		snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.RewardLock)
		snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.FlowLock)
		snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.BandwidthLock)
		if number >= PledgeRevertLockEffectNumber {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.PosPgExitLock)
		}
		if isGEPOSNewEffect(number) {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.PosExitLock)
		}
		if isGEInitStorageManagerNumber(number) {
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.STPEntrustExitLock)
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.STPEntrustLock)
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpLock)
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpEntrustLock)
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpExitLock)
			snapshotRelease.appendRewardBalanceV1Data(snapshot.FlowRevenue.SpEntrustExitLock)
		}
	}
	return snapshotRelease, err
}

func (sr *SnapshotRewardBalanceV1) appendRewardBalanceV1Data(lockData *LockData) error {
	sr.appendRBd(lockData.FlowRevenue)
	return nil
}

func (sr *SnapshotRewardBalanceV1) appendRBd(FlowRevenue map[common.Address]*LockBalanceData) error {
	fr1 := FlowRevenue
	for t1, item1 := range fr1 {
		if _, ok := sr.FlowRevenue[t1]; !ok {
			sr.FlowRevenue[t1] = &RewardBalanceV1Data{
				RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
			}
		}
		rewardBalanceV1 := item1.RewardBalanceV1
		if rewardBalanceV1 != nil {
			for t3, item3 := range rewardBalanceV1 {
				if _, ok := sr.FlowRevenue[t1].RewardBalanceV1[t3]; !ok {
					sr.FlowRevenue[t1].RewardBalanceV1[t3] = make(map[common.Address]*LockTmpData)
				}
				t3LockBalance := sr.FlowRevenue[t1].RewardBalanceV1[t3]
				for t4, item4 := range item3 {
					if _, ok := t3LockBalance[t4]; !ok {
						t3LockBalance[t4] = &LockTmpData{
							Amount:         new(big.Int).Set(item4.Amount),
							RevenueAddress: item4.RevenueAddress,
						}
					}
				}
			}
		}
	}
	return nil
}
