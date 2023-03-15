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
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
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
	chain consensus.ChainHeaderReader
	alien *Alien
	sCache *list.List
	lock sync.RWMutex
}

type SnapCache struct {
	number uint64
	s *Snapshot
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *API) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = api.chain.CurrentHeader()
		log.Info("api GetSnapshot", "number",number)
	} else {
		header = api.chain.GetHeaderByNumber(uint64(number.Int64()))
		log.Info("api GetSnapshot", "number",number.Int64())
		if err:=api.isNumberTooSmall(header);err!=nil{
			return nil,err
		}
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
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
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
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	return api.getSnapshotCache(header)
}

func (api *API) GetSnapshotSignerAtNumber(number uint64) (*SnapshotSign, error) {
	log.Info("api GetSnapshotSignerAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSign := &SnapshotSign{
		LoopStartTime:snapshot.LoopStartTime,
		Signers: snapshot.Signers,
		Punished: snapshot.Punished,
		SignPledge:make(map[common.Address]*SignPledgeItem),
	}
	if isGEPOSNewEffect(number){
		for miner,item:=range snapshot.PosPledge{
			snapshotSign.SignPledge[miner]=&SignPledgeItem{
				TotalAmount: new(big.Int).Set(item.TotalAmount),
				LastPunish:item.LastPunish,
				DisRate:new(big.Int).Set(item.DisRate),
			}
		}
	}
	return snapshotSign, err
}


type SnapshotSign struct {
	LoopStartTime   uint64                                              `json:"loopStartTime"`
	Signers         []*common.Address                                   `json:"signers"`
	Punished        map[common.Address]uint64                           `json:"punished"`
	SignPledge      map[common.Address]*SignPledgeItem                  `json:"signpledge"`
}
type SignPledgeItem struct {
	TotalAmount *big.Int                      `json:"totalamount"`
	LastPunish  uint64                        `json:"lastpunish"`
	DisRate     *big.Int                      `json:"distributerate"`
}

func (api *API) GetSnapshotReleaseAtNumber(number uint64,part string) (*SnapshotRelease, error) {
	log.Info("api GetSnapshotReleaseAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotRelease := &SnapshotRelease{
		CandidatePledge:make(map[common.Address]*PledgeItem),
		FlowPledge: make(map[common.Address]*PledgeItem),
		FlowRevenue: make(map[common.Address]*LockBalanceData),
	}
	if part!=""{
		if part =="candidatepledge"{
			snapshotRelease.CandidatePledge=snapshot.CandidatePledge
		}else if part =="flowminerpledge"{
			if number < PledgeRevertLockEffectNumber{
				snapshotRelease.FlowPledge=snapshot.FlowPledge
			}
		}else if part =="rewardlock"{
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.RewardLock,api.alien.db)
		}else if part =="flowlock"{
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.FlowLock,api.alien.db)
		}else if part =="bandwidthlock"{
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.BandwidthLock,api.alien.db)
		}else if part =="posplexit"{
			if snapshot.FlowRevenue.PosPgExitLock!=nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosPgExitLock,api.alien.db)
			}
		}else if part =="posexit"{
			if snapshot.FlowRevenue.PosExitLock!=nil {
				snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosExitLock,api.alien.db)
			}
		}
	}else{
		snapshotRelease.CandidatePledge=snapshot.CandidatePledge
		if number < PledgeRevertLockEffectNumber{
			snapshotRelease.FlowPledge=snapshot.FlowPledge
		}
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.RewardLock,api.alien.db)
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.FlowLock,api.alien.db)
		snapshotRelease.appendFRlockData(snapshot.FlowRevenue.BandwidthLock,api.alien.db)
		if number >= PledgeRevertLockEffectNumber{
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosPgExitLock,api.alien.db)
		}
		if isGEPOSNewEffect(number){
			snapshotRelease.appendFRlockData(snapshot.FlowRevenue.PosExitLock,api.alien.db)
		}
	}
	return snapshotRelease, err
}

func (s *SnapshotRelease) appendFRItems(items []*PledgeItem) {
	for _, item := range items {
		if _, ok := s.FlowRevenue[item.TargetAddress]; !ok {
			s.FlowRevenue[item.TargetAddress] = &LockBalanceData{
				RewardBalance:make(map[uint32]*big.Int),
				LockBalance: make(map[uint64]map[uint32]*PledgeItem),
			}
		}
		flowRevenusTarget := s.FlowRevenue[item.TargetAddress]
		if _, ok := flowRevenusTarget.LockBalance[item.StartHigh]; !ok {
			flowRevenusTarget.LockBalance[item.StartHigh] = make(map[uint32]*PledgeItem)
		}
		lockBalance := flowRevenusTarget.LockBalance[item.StartHigh]
		lockBalance[item.PledgeType] = item
	}
}

func (sr *SnapshotRelease) appendFR(FlowRevenue map[common.Address]*LockBalanceData) (error) {
	fr1:=FlowRevenue
	for t1, item1 := range fr1 {
		if _, ok := sr.FlowRevenue[t1]; !ok {
			sr.FlowRevenue[t1] = &LockBalanceData{
				RewardBalance:make(map[uint32]*big.Int),
				LockBalance: make(map[uint64]map[uint32]*PledgeItem),
			}
		}
		rewardBalance:=item1.RewardBalance
		for t2, item2 := range rewardBalance {
			sr.FlowRevenue[t1].RewardBalance[t2]=item2
		}
		lockBalance:=item1.LockBalance
		for t3, item3 := range lockBalance {
			if _, ok := sr.FlowRevenue[t1].LockBalance[t3]; !ok {
				sr.FlowRevenue[t1].LockBalance[t3] = make(map[uint32]*PledgeItem)
			}
			t3LockBalance:=sr.FlowRevenue[t1].LockBalance[t3]
			for t4,item4:=range item3{
				if _, ok := t3LockBalance[t4]; !ok {
					t3LockBalance[t4] = item4
				}
			}
		}
	}
	return nil
}


func (sr *SnapshotRelease) appendFRlockData(lockData *LockData,db ethdb.Database) (error) {
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
	CandidatePledge map[common.Address]*PledgeItem                      `json:"candidatepledge"`
	FlowPledge      map[common.Address]*PledgeItem                      `json:"flowminerpledge"`
	FlowRevenue     map[common.Address]*LockBalanceData                 `json:"flowrevenve"`
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
		return nil,err
	}
	lockReward:=make([]FlowRecord,0)
	if len(headerExtra.LockReward)>0 {
		for _, item := range headerExtra.LockReward {
			if(item.IsReward==sscEnumFlwReward){
				lockReward=append(lockReward,FlowRecord{
					Target: item.Target,
					Amount: item.Amount,
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
	LockReward  []FlowRecord `json:"flowrecords"`
}

type FlowRecord struct {
	Target   common.Address
	Amount   *big.Int
	FlowValue1 uint64 `json:"realFlowvalue"`
	FlowValue2 uint64 `json:"validFlowvalue"`
}

func (api *API) GetSnapshotFlowMinerAtNumber(number uint64) (*SnapshotFlowMiner, error) {
	log.Info("api GetSnapshotFlowMinerAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotFlowMinerAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	flowMiner := &SnapshotFlowMiner{
		DayStartTime:snapshot.FlowMiner.DayStartTime,
		FlowMinerPrevTotal: snapshot.FlowMiner.FlowMinerPrevTotal,
		FlowMiner: snapshot.FlowMiner.FlowMiner,
		FlowMinerPrev:snapshot.FlowMiner.FlowMinerPrev,
		FlowMinerReport:[]*FlowMinerReport{},
		FlowMinerPrevReport:[]*FlowMinerReport{},
	}
	fMiner:=snapshot.FlowMiner
	db:=api.alien.db
	items:=flowMiner.loadFlowMinerCache(fMiner,fMiner.FlowMinerCache,db)
	flowMiner.FlowMinerReport=append(flowMiner.FlowMinerReport,items...)
	items=flowMiner.loadFlowMinerCache(fMiner,fMiner.FlowMinerPrevCache,db)
	flowMiner.FlowMinerPrevReport=append(flowMiner.FlowMinerPrevReport,items...)
	return flowMiner, err
}


type SnapshotFlowMiner struct {
	DayStartTime       uint64                                              `json:"dayStartTime"`
	FlowMinerPrevTotal uint64                                              `json:"flowminerPrevTotal"`
	FlowMiner          map[common.Address]map[common.Hash]*FlowMinerReport `json:"flowminerCurr"`
	FlowMinerReport    []*FlowMinerReport `json:"flowminerReport"`
	FlowMinerPrev      map[common.Address]map[common.Hash]*FlowMinerReport `json:"flowminerPrev"`
	FlowMinerPrevReport    []*FlowMinerReport `json:"flowminerPrevReport"`
}

func (sf *SnapshotFlowMiner) loadFlowMinerCache(fMiner *FlowMinerSnap,flowMinerCache []string,db ethdb.Database) ([]*FlowMinerReport) {
	item:=[]*FlowMinerReport{}
	for _, key := range flowMinerCache {
		flows, err := fMiner.load(db, key)
		if err != nil {
			log.Warn("appendFlowMinerCache load cache error", "key", key, "err", err)
			continue
		}
		item=append(item,flows...)
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
		return nil,err
	}
	flowReport:=make([]MinerFlowReportRecord,0)
	if len(headerExtra.FlowReport)>0 {
		flowReport=append(flowReport,headerExtra.FlowReport...)
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
		return nil,err
	}
	LockReward:=make([]LockRewardRecord,0)
	if len(headerExtra.LockReward)>0 {
		LockReward=append(LockReward,headerExtra.LockReward...)
	}
	return LockReward, err
}

func (api *API) GetSRTBalAtNumber(number uint64) (*SnapshotSRT, error) {
	log.Info("api GetSRTBalAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSRTBalAtNumber", "err", err)
		return nil, errUnknownBlock
	}

	snapshotSRT:=&SnapshotSRT{
		SrtBal:make(map[common.Address]*big.Int),
	}
	if snapshot.SRT!=nil{
		srtBal:= snapshot.SRT.GetAll()
		if err==nil{
			snapshotSRT.SrtBal=srtBal
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
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledge := &SnapshotSPledge{
		StoragePledge: make(map[common.Address]*SPledge2),
	}

	for pledgeAddr,sPledge := range snapshot.StorageData.StoragePledge {
		snapshotSPledge.StoragePledge[pledgeAddr]=&SPledge2{
			PledgeStatus:sPledge.PledgeStatus,
			StorageCapacity:sPledge.StorageSpaces.StorageCapacity,
			Lease:make(map[common.Hash]*Lease2),
			LastVerificationTime:sPledge.LastVerificationTime,
			LastVerificationSuccessTime:sPledge.LastVerificationSuccessTime,
			ValidationFailureTotalTime:sPledge.ValidationFailureTotalTime,
		}
		lease:=sPledge.Lease
		for hash,l:=range lease {
			lease2:=&Lease2{
				Address:l.Address,
				Status:l.Status,
				LastVerificationTime:l.LastVerificationTime,
				LastVerificationSuccessTime:l.LastVerificationSuccessTime,
				ValidationFailureTotalTime:l.ValidationFailureTotalTime,
				LeaseList:make(map[common.Hash]*LeaseDetail2),
			}
			ll:=l.LeaseList
			for lhash,item:=range ll{
				lease2.LeaseList[lhash]=&LeaseDetail2{
					Deposit: item.Deposit,
				}
			}
			snapshotSPledge.StoragePledge[pledgeAddr].Lease[hash]=lease2
		}
	}
	return snapshotSPledge, err
}

type SnapshotSPledge struct {
	StoragePledge map[common.Address]*SPledge2 `json:"spledge"`
}

type SPledge2 struct {
	PledgeStatus  *big.Int `json:"pledgeStatus"`
	StorageCapacity *big.Int `json:"storagecapacity"`
	Lease map[common.Hash]*Lease2 `json:"lease"`
	LastVerificationTime  *big.Int `json:"lastverificationtime"`
	LastVerificationSuccessTime  *big.Int `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime *big.Int `json:"validationfailuretotaltime"`
}
type Lease2 struct {
	Address common.Address `json:"address"`
	Status int `json:"status"`
	LastVerificationTime  *big.Int `json:"lastverificationtime"`
	LastVerificationSuccessTime  *big.Int `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime *big.Int `json:"validationfailuretotaltime"`
	LeaseList map[common.Hash]*LeaseDetail2 `json:"leaselist"`
}
type LeaseDetail2 struct {
	Deposit                    *big.Int    `json:"deposit"`
}

func (api *API) GetStorageRewardAtNumber(number uint64,part string) (*SnapshotStorageReward, error) {
	log.Info("api GetStorageRewardAtNumber", "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetStoragePledgeRewardAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotStorageReward := &SnapshotStorageReward{
		StorageReward:StorageReward{
			Reward: make([]SpaceRewardRecord,0),
			LockPeriod:snapshot.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod,
			RlsPeriod:snapshot.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod,
			Interval:snapshot.SystemConfig.LockParameters[sscEnumRwdLock].Interval,
		},
	}
	if part =="spaceLock"||part==""{
		reward,err2:=NewStorageSnap().loadLockReward(api.alien.db,number,storagePledgeRewardkey)
		if err2==nil&&reward!=nil&&len(reward)>0{
			snapshotStorageReward.StorageReward.Reward=append(snapshotStorageReward.StorageReward.Reward,reward...)
		}
	}
	if part =="leaseLock"{
		reward,err2:=NewStorageSnap().loadLockReward(api.alien.db,number,storageLeaseRewardkey)
		if err2==nil&&reward!=nil&&len(reward)>0{
			snapshotStorageReward.StorageReward.Reward=append(snapshotStorageReward.StorageReward.Reward,reward...)
		}
	}
	if part =="revertLock"{
		reward,err2:=NewStorageSnap().loadLockReward(api.alien.db,number,revertSpaceLockRewardkey)
		if err2==nil&&reward!=nil&&len(reward)>0{
			snapshotStorageReward.StorageReward.Reward=append(snapshotStorageReward.StorageReward.Reward,reward...)
		}
	}
	if part =="blockLock"{
		if number >= StorageEffectBlockNumber {
			headerExtra := HeaderExtra{}
			err3 := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
			if err3 != nil {
				log.Info("Fail to decode header Extra", "err", err3)
				return nil,err3
			}
			if len(headerExtra.LockReward)>0 {
				for _,item:=range headerExtra.LockReward{
					if sscEnumSignerReward == item.IsReward {
						revenueAddress:=item.Target
						if revenue, ok := snapshot.RevenueNormal[item.Target]; ok {
							revenueAddress = revenue.RevenueAddress
						}
						spaceRewardRecord:=SpaceRewardRecord{
							Target:item.Target,
							Amount:item.Amount,
							Revenue:revenueAddress,
						}
						snapshotStorageReward.StorageReward.Reward=append(snapshotStorageReward.StorageReward.Reward,spaceRewardRecord)
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
	Reward []SpaceRewardRecord `json:"reward"`
	LockPeriod uint32 `json:"LockPeriod"`
	RlsPeriod  uint32 `json:"ReleasePeriod"`
	Interval   uint32 `json:"ReleaseInterval"`
}

func (api *API) GetStorageRatiosAtNumber(number uint64) (*SnapshotStorageRatios, error) {
	log.Info("api GetStorageRatiosAtNumber", "number", number)
	snapshotStorageRatios := &SnapshotStorageRatios{
		Ratios:make(map[common.Address]*StorageRatio),
	}
	ratios,err:=NewStorageSnap().lockStorageRatios(api.alien.db,number)
	if err==nil&&ratios!=nil&&len(ratios)>0{
		snapshotStorageRatios.Ratios=ratios
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
	revertSRT,err:=NewStorageSnap().lockRevertSRT(api.alien.db,number)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil,err
	}
	snapshotRevertSRT:=&SnapshotRevertSRT{
		RevertSRT:revertSRT,
	}
	return snapshotRevertSRT,nil
}

type SnapshotAddrSRT struct {
	AddrSrtBal *big.Int `json:"addrsrtbal"`
}

func (api *API) GetSRTBalanceAtNumber(address common.Address,number uint64) (*SnapshotAddrSRT,error) {
	log.Info("api GetSRTBalanceAtNumber", "address",address,"number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSRTBalanceAtNumber", "err", err)
		return nil, errUnknownBlock
	}

	snapshotAddrSRT:=&SnapshotAddrSRT{
		AddrSrtBal:common.Big0,
	}
	if snapshot.SRT!=nil{
		snapshotAddrSRT.AddrSrtBal= snapshot.SRT.Get(address)
	}

	return snapshotAddrSRT,nil
}

func (api *API) GetSRTBalance(address common.Address) (*SnapshotAddrSRT,error) {
	log.Info("api GetSRTBalance", "address",address)
	header := api.chain.CurrentHeader()
	if header == nil {
		return nil, errUnknownBlock
	}
	return api.GetSRTBalanceAtNumber(address,header.Number.Uint64())
}

func (api *API) GetSPledgeInfoByAddr(address common.Address) (*SnapshotSPledgeInfo,error) {
	log.Info("api GetSPledgeInfoByAddr", "address",address)
	header := api.chain.CurrentHeader()
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeInfoByAddr", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledgeInfo := &SnapshotSPledgeInfo{
		SPledgeInfo: make(map[common.Address]*SPledge3),
	}
	for pledgeAddr,sPledge := range snapshot.StorageData.StoragePledge {
		if pledgeAddr==address{
			leftCapacity:=snapshot.StorageData.StoragePledge[pledgeAddr].StorageSpaces.StorageCapacity
			snapshotSPledgeInfo.SPledgeInfo[pledgeAddr]=&SPledge3{
				PledgeStatus:sPledge.PledgeStatus,
				TotalCapacity:new(big.Int).Set(sPledge.TotalCapacity),
				LeftCapacity:new(big.Int).Set(leftCapacity),
				Lease:make([]Lease3,0),
			}
			lease:=sPledge.Lease
			for hash,l:=range lease {
				snapshotSPledgeInfo.SPledgeInfo[pledgeAddr].Lease=append(snapshotSPledgeInfo.SPledgeInfo[pledgeAddr].Lease,Lease3{
					Status:l.Status,
					Hash: hash,
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
	TotalCapacity *big.Int               `json:"totalcapacity"`
	LeftCapacity  *big.Int               `json:"leftcapacity"`
	Lease []Lease3 `json:"lease"`
}

type Lease3 struct {
	Hash common.Hash `json:"hash"`
	Status int `json:"status"`
}

func (api *API) GetSPledgeCapVerAtNumber(number uint64) (*SnapshotSPledgeCapVer, error) {
	log.Info("api GetSPledgeCapVerAtNumber", "number",number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSPledgeCapVerAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotSPledgeCapVer := &SnapshotSPledgeCapVer{
		SpledgeCapVer: api.calStorageVerifyPercentage(number,snapshot.getBlockPreDay(),snapshot.StorageData.copy()),
	}
	return snapshotSPledgeCapVer, err
}

type SnapshotSPledgeCapVer struct {
	SpledgeCapVer map[common.Address]*big.Int `json:"spledgecapver"`
}

func (api *API) calStorageVerifyPercentage(number uint64, blockPerday uint64,s *StorageData) (map[common.Address]*big.Int) {
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
		per = new(big.Int).Div(per,  sPledge.TotalCapacity)
		capSuccPer[pledgeAddr]=per
	}
	return capSuccPer
}

type SnapshotSPledgeValue struct {
	SpledgeValue *big.Int `json:"spledgevalue"`
}

func (api *API) GetStorageValueAtNumber(number uint64,part string) (*SnapshotSPledgeValue, error) {
	log.Info("api GetStorageValueAtNumber", "number",number,"part",part)
	snapshotStorage := &SnapshotSPledgeValue{
		SpledgeValue:common.Big0,
	}
	key:=originalTotalCapacityKey
	var err error
	var v *big.Int
	if part =="totalPledgeReward"{
		key=totalPledgeRewardKey
	}
	if part =="storageHarvest"{
		key=storageHarvestKey
	}
	if part =="leaseHarvest"{
		key=leaseHarvestKey
	}
	v,err=NewStorageSnap().loadSpledgeValue(api.alien.db,number,key)
	if err==nil&&v!=nil{
		snapshotStorage.SpledgeValue=v
	}
	return snapshotStorage, err
}

type SnapshotSPledgeDecimalValue struct {
	SpledgeDecimalValue decimal.Decimal `json:"spledgedecimalvalue"`
}


func (api *API) GetStorageDecimalValueAtNumber(number uint64,part string) (*SnapshotSPledgeDecimalValue, error) {
	log.Info("api GetStorageDecimalValueAtNumber", "number",number,"part",part)
	snapshotStorage := &SnapshotSPledgeDecimalValue{
		SpledgeDecimalValue:decimal.Zero,
	}
	key:=totalLeaseSpaceKey
	var err error
	var v decimal.Decimal
	v,err=NewStorageSnap().loadSpledgeDecimalValue(api.alien.db,number,key)
	if err==nil{
		snapshotStorage.SpledgeDecimalValue=v
	}
	return snapshotStorage, err
}


type SnapshotSPledgeRatioValue struct {
	SpledgeRatioValue decimal.Decimal `json:"spledgeratiovalue"`
}

func (api *API) GetStorageRatioValueAtNumber(number uint64,value *big.Int,part string) (*SnapshotSPledgeRatioValue, error) {
	log.Info("api GetStorageRatioValueAtNumber", "number",number,"value",value,"part",part)
	snapshotStorage := &SnapshotSPledgeRatioValue{
		SpledgeRatioValue:decimal.Zero,
	}
	var v decimal.Decimal
	if part =="Bandwidth"{
		v=getBandwaith(value,number)
	}
	if part =="StorageRatio"{
		v=NewStorageSnap().calStorageRatio(value,number)
	}
	snapshotStorage.SpledgeRatioValue=v
	return snapshotStorage, nil
}

type SnapshotSucSPledge struct {
	SucSPledge []common.Address `json:"sucspledge"`
}

func (api *API) GetSucSPledgeAtNumber(number uint64) (*SnapshotSucSPledge, error) {
	log.Info("api GetSucSPledgeAtNumber", "number",number)
	snapshotSucSPledge := &SnapshotSucSPledge{
		SucSPledge:make([]common.Address,0),
	}
	sucspledge,err:=NewStorageSnap().loadSPledgeSucc(api.alien.db,number)
	if err==nil&&sucspledge!=nil&&len(sucspledge)>0{
		snapshotSucSPledge.SucSPledge=sucspledge
	}
	return snapshotSucSPledge, err
}

type SnapshotRentSuc struct {
	RentSuc []common.Hash `json:"rentsuc"`
}

func (api *API) GetRentSucAtNumber(number uint64) (*SnapshotRentSuc, error) {
	log.Info("api GetRentSucAtNumber", "number",number)
	snapshotRentSuc := &SnapshotRentSuc{
		RentSuc:make([]common.Hash,0),
	}
	rentSuc,err:=NewStorageSnap().loadRentSucc(api.alien.db,number)
	if err==nil&&rentSuc!=nil&&len(rentSuc)>0{
		snapshotRentSuc.RentSuc=rentSuc
	}
	return snapshotRentSuc, err
}


type SnapshotCapSuccAddrs struct {
	CapSuccAddrs map[common.Address]*big.Int `json:"capsuccaddrs"`
}

func (api *API) GetCapSuccAddrsAtNumber(number uint64) (*SnapshotCapSuccAddrs, error) {
	log.Info("api GetCapSuccAddrsAtNumber", "number",number)
	snapshotCapSuccAddrs := &SnapshotCapSuccAddrs{
		CapSuccAddrs:make(map[common.Address]*big.Int),
	}
	capSuccAddrs,err:=NewStorageSnap().loadCapSuccAddrs(api.alien.db,number)
	if err==nil&&capSuccAddrs!=nil&&len(capSuccAddrs)>0{
		snapshotCapSuccAddrs.CapSuccAddrs=capSuccAddrs
	}
	return snapshotCapSuccAddrs, err
}

func (api *API) GetGrantProfitAtNumber(number uint64) ([]consensus.GrantProfitRecord, error) {
	log.Info("api GetGrantProfitAtNumber", "number",number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	headerExtra := HeaderExtra{}
	err := rlp.DecodeBytes(header.Extra[extraVanity:len(header.Extra)-extraSeal], &headerExtra)
	if err != nil {
		log.Info("Fail to decode header Extra", "err", err)
		return nil,err
	}
	grantProfit:=make([]consensus.GrantProfitRecord,0)
	if len(headerExtra.GrantProfit)>0 {
		grantProfit=append(grantProfit,headerExtra.GrantProfit...)
	}
	return grantProfit, err
}

type SnapshotSTGbwMakeup struct {
	STGBandwidthMakeup map[common.Address]*BandwidthMakeup `json:"stgbandwidthmakeup"`
}

func (api *API) GetSTGBandwidthMakeup() (*SnapshotSTGbwMakeup, error) {
	log.Info("api GetSTGBandwidthMakeup", "number",PosrIncentiveEffectNumber)
	header := api.chain.GetHeaderByNumber(PosrIncentiveEffectNumber)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
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
	number:=header.Number.Uint64()
	s:=api.findInSnapCache(number)
	if nil!=s{
		return s,nil
	}
	return api.getSnapshotByHeader(header)
}

func (api *API)findInSnapCache(number uint64) *Snapshot {
	for i := api.sCache.Front(); i != nil; i = i.Next() {
		v:=i.Value.(SnapCache)
		if v.number==number{
			return v.s
		}
	}
	return nil
}

func (api *API) getSnapshotByHeader(header *types.Header) (*Snapshot,error) {
	api.lock.Lock()
	defer api.lock.Unlock()
	number:=header.Number.Uint64()
	s:=api.findInSnapCache(number)
	if nil!=s{
		return s,nil
	}
	cacheSize:=32
	snapshot,err:= api.alien.snapshot(api.chain, number, header.Hash(), nil, nil, defaultLoopCntRecalculateSigners)
	if err != nil {
		log.Warn("Fail to getSnapshotByHeader", "err", err)
		return nil, errUnknownBlock
	}
	api.sCache.PushBack(SnapCache{
		number: number,
		s:snapshot,
	})
	if api.sCache.Len()>cacheSize{
		api.sCache.Remove(api.sCache.Front())
	}
	return snapshot,nil
}

func (api *API) GetSnapshotReleaseAtNumber2(number uint64,part string,startLNum uint64,endLNum uint64) (*SnapshotRelease, error) {
	log.Info("api GetSnapshotReleaseAtNumber2", "number",number,"part",part,"startLNum",startLNum,"endLNum",endLNum)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, errUnknownBlock
	}
	if err:=api.isNumberTooSmall(header);err!=nil{
		return nil,err
	}
	snapshot,err:= api.getSnapshotCache(header)
	if err != nil {
		log.Warn("Fail to GetSnapshotSignAtNumber", "err", err)
		return nil, errUnknownBlock
	}
	snapshotRelease := &SnapshotRelease{
		CandidatePledge:make(map[common.Address]*PledgeItem),
		FlowPledge: make(map[common.Address]*PledgeItem),
		FlowRevenue: make(map[common.Address]*LockBalanceData),
	}
	if part!=""{
		if part =="candidatepledge"{
			snapshotRelease.CandidatePledge=snapshot.CandidatePledge
		}else if part =="flowminerpledge"{
			if number < PledgeRevertLockEffectNumber{
				snapshotRelease.FlowPledge=snapshot.FlowPledge
			}
		}else if part =="rewardlock"{
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.RewardLock,api.alien.db,startLNum,endLNum)
		}else if part =="flowlock"{
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.FlowLock,api.alien.db,startLNum,endLNum)
		}else if part =="bandwidthlock"{
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.BandwidthLock,api.alien.db,startLNum,endLNum)
		}else if part =="posplexit"{
			if snapshot.FlowRevenue.PosPgExitLock!=nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosPgExitLock,api.alien.db,startLNum,endLNum)
			}
		}else if part =="posexit"{
			if snapshot.FlowRevenue.PosExitLock!=nil {
				snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosExitLock,api.alien.db,startLNum,endLNum)
			}
		}
	}else{
		snapshotRelease.CandidatePledge=snapshot.CandidatePledge
		if number < PledgeRevertLockEffectNumber{
			snapshotRelease.FlowPledge=snapshot.FlowPledge
		}
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.RewardLock,api.alien.db,startLNum,endLNum)
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.FlowLock,api.alien.db,startLNum,endLNum)
		snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.BandwidthLock,api.alien.db,startLNum,endLNum)
		if number >= PledgeRevertLockEffectNumber{
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosPgExitLock,api.alien.db,startLNum,endLNum)
		}
		if isGEPOSNewEffect(number){
			snapshotRelease.appendFRlockData2(snapshot.FlowRevenue.PosExitLock,api.alien.db,startLNum,endLNum)
		}
	}
	return snapshotRelease, err
}

func (sr *SnapshotRelease) appendFRlockData2(lockData *LockData,db ethdb.Database,startLNum uint64,endLNum uint64) (error) {
	sr.appendFR2(lockData.FlowRevenue,startLNum,endLNum)
	items, err := lockData.loadCacheL1(db)
	if err == nil {
		sr.appendFRItems2(items,startLNum,endLNum)
	}
	items, err = lockData.loadCacheL2(db)
	if err == nil {
		sr.appendFRItems2(items,startLNum,endLNum)
	}
	return nil
}
func (s *SnapshotRelease) appendFRItems2(items []*PledgeItem,startLNum uint64,endLNum uint64) {
	for _, item := range items {
		if _, ok := s.FlowRevenue[item.TargetAddress]; !ok {
			s.FlowRevenue[item.TargetAddress] = &LockBalanceData{
				RewardBalance:make(map[uint32]*big.Int),
				LockBalance: make(map[uint64]map[uint32]*PledgeItem),
			}
		}
		if inLNumScope(item.StartHigh,startLNum,endLNum){
			flowRevenusTarget := s.FlowRevenue[item.TargetAddress]
			if _, ok := flowRevenusTarget.LockBalance[item.StartHigh]; !ok {
				flowRevenusTarget.LockBalance[item.StartHigh] = make(map[uint32]*PledgeItem)
			}
			lockBalance := flowRevenusTarget.LockBalance[item.StartHigh]
			lockBalance[item.PledgeType] = item
		}
	}
}

func (sr *SnapshotRelease) appendFR2(FlowRevenue map[common.Address]*LockBalanceData,startLNum uint64,endLNum uint64) (error) {
	fr1:=FlowRevenue
	for t1, item1 := range fr1 {
		if _, ok := sr.FlowRevenue[t1]; !ok {
			sr.FlowRevenue[t1] = &LockBalanceData{
				RewardBalance:make(map[uint32]*big.Int),
				LockBalance: make(map[uint64]map[uint32]*PledgeItem),
			}
		}
		rewardBalance:=item1.RewardBalance
		for t2, item2 := range rewardBalance {
			sr.FlowRevenue[t1].RewardBalance[t2]=item2
		}
		lockBalance:=item1.LockBalance
		for t3, item3 := range lockBalance {
			if inLNumScope(t3,startLNum,endLNum){
				if _, ok := sr.FlowRevenue[t1].LockBalance[t3]; !ok {
					sr.FlowRevenue[t1].LockBalance[t3] = make(map[uint32]*PledgeItem)
				}
				t3LockBalance:=sr.FlowRevenue[t1].LockBalance[t3]
				for t4,item4:=range item3{
					if _, ok := t3LockBalance[t4]; !ok {
						t3LockBalance[t4] = item4
					}
				}
			}
		}
	}
	return nil
}

func inLNumScope(num uint64, startLNum uint64, endLNum uint64) bool {
	if num>=startLNum&&num<=endLNum {
		return true
	}
	return false
}

type SnapCanAutoExit struct {
	CandidateAutoExit  []common.Address `json:"candidateautoexit"`
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
		return nil,err
	}
	snapCanAutoExit:=&SnapCanAutoExit{
		CandidateAutoExit:make([]common.Address,0),
	}
	if len(headerExtra.CandidateAutoExit)>0 {
		snapCanAutoExit.CandidateAutoExit=append(snapCanAutoExit.CandidateAutoExit,headerExtra.CandidateAutoExit...)
	}
	return snapCanAutoExit, err
}


func (api *API) ClearDbSnapDataAtNumber(startNumber uint64,number uint64) (error) {
	log.Info("api clearDbSnapDataAtNumber", "startNumber", startNumber, "number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		log.Error("api clearDbSnapDataAtNumber", "err", errUnknownBlock,"startNumber", startNumber, "number", number)
		return errUnknownBlock
	}
	blockPerDay:=api.alien.blockPerDay()
	var clearEndNumber uint64
	if header.Number.Uint64()>retainedLastSnapshot {
		clearEndNumber = header.Number.Uint64() - retainedLastSnapshot
		if clearEndNumber <= clearEndNumber%blockPerDay {
			log.Info("api clearDbSnapDataAtNumber", "clearEndNumber is too small", clearEndNumber)
			return nil
		}
		clearEndNumber = clearEndNumber - clearEndNumber%blockPerDay
	}else{
		log.Info("api clearDbSnapDataAtNumber", "clearEndNumber is too small", number)
		return errNumberTooSmall
	}
	return clearSnapDataAtNumber(startNumber,clearEndNumber,api.chain,api.alien.blockPerDay(),api.alien.db,api.alien.config.Period)
}
func clearSnapDataAtNumber(startNumber uint64,number uint64,chain consensus.ChainHeaderReader,blockPerDay uint64, db ethdb.Database,period uint64) (error) {
	log.Info("clearDbSnapDataAtNumber", "startNumber", startNumber, "number", number)
	header := chain.GetHeaderByNumber(number)
	if header == nil {
		log.Error("clearDbSnapDataAtNumber", "err", errUnknownBlock,"startNumber", startNumber, "number", number)
		return errUnknownBlock
	}
	if header.Number.Uint64()>retainedLastSnapshot{
		clearEndNumber:=number
		clearStartNumber:=startNumber
		cacheL1:="l1"
		cacheL2:="l2"

		rewardCacheL1Hash := make(map[common.Hash]uint64)
		rewardCacheL2Hash :=make(map[common.Hash]uint64)

		flowCacheL1Hash := make(map[common.Hash]uint64)
		flowCacheL2Hash :=make(map[common.Hash]uint64)

		bandwidthCacheL1Hash := make(map[common.Hash]uint64)
		bandwidthCacheL2Hash :=make(map[common.Hash]uint64)

		posplexitCacheL1Hash := make(map[common.Hash]uint64)
		posplexitCacheL2Hash :=make(map[common.Hash]uint64)

		posexitCacheL1Hash := make(map[common.Hash]uint64)
		posexitCacheL2Hash :=make(map[common.Hash]uint64)

		for clearStartNumber < clearEndNumber {
			removeHeader := chain.GetHeaderByNumber(clearStartNumber)
			hash:= removeHeader.Hash()

			if clearStartNumber%checkpointInterval == 0 {
				snapshotKey:=append([]byte("alien-"), hash[:]...)
				blob, err := db.Get(snapshotKey)
				if err == nil {
					snap := new(Snapshot)
					if err2 := json.Unmarshal(blob, snap); err2 != nil {
						log.Error("clearDbSnapDataAtNumber json.Unmarshal", "number", clearStartNumber, "hash", hash,"err",err2)
					}else{
						clearLockByCacheHash(LOCKREWARDDATA,snap.FlowRevenue.RewardLock, rewardCacheL1Hash, rewardCacheL2Hash,clearStartNumber, db)
						clearLockByCacheHash(LOCKFLOWDATA,snap.FlowRevenue.FlowLock, flowCacheL1Hash, flowCacheL2Hash,clearStartNumber, db)
						clearLockByCacheHash(LOCKBANDWIDTHDATA,snap.FlowRevenue.BandwidthLock, bandwidthCacheL1Hash, bandwidthCacheL2Hash,clearStartNumber, db)
						if snap.FlowRevenue.PosPgExitLock!=nil{
							clearLockByCacheHash(LOCKPOSEXITDATA,snap.FlowRevenue.PosPgExitLock, posplexitCacheL1Hash, posplexitCacheL2Hash,clearStartNumber, db)
						}
						if snap.FlowRevenue.PosExitLock!=nil{
							clearLockByCacheHash(LOCKPEXITDATA,snap.FlowRevenue.PosExitLock, posexitCacheL1Hash, posexitCacheL2Hash,clearStartNumber, db)
						}
					}

					if !islockSimplifyEffectBlocknumber(clearStartNumber){
						clearRewardDbDataCheckHash(LOCKREWARDDATA,db,cacheL1,hash,clearStartNumber,rewardCacheL1Hash)
					}
					err = db.Delete(snapshotKey)
					if err != nil {
						log.Error("clearDbSnapDataAtNumber snapshot from disk", "number", clearStartNumber, "hash", hash,"err",err)
					}else{
						log.Info("clearDbSnapDataAtNumber snapshot from disk", "number", clearStartNumber, "hash", hash)
					}
				}
			}

			if clearStartNumber<tallyRevenueEffectBlockNumber{
				if (clearStartNumber+1)%blockPerDay==0{
					clearRewardDbDataCheckHash(LOCKREWARDDATA,db,cacheL2,hash,clearStartNumber,rewardCacheL2Hash)
				}
			}
			//reward
			if clearStartNumber%blockPerDay==0{
				clearRewardDbDataCheckHash(LOCKREWARDDATA,db,cacheL1,hash,clearStartNumber,rewardCacheL1Hash)
				clearRewardDbDataCheckHash(LOCKREWARDDATA,db,cacheL2,hash,clearStartNumber,rewardCacheL2Hash)
			}
			//flow,bandwidth,posplexit l1
			if clearStartNumber%blockPerDay==(storageVerificationCheck/period){
				clearRewardDbDataCheckHash(LOCKFLOWDATA,db,cacheL1,hash,clearStartNumber,flowCacheL1Hash)
				clearRewardDbDataCheckHash(LOCKBANDWIDTHDATA,db,cacheL1,hash,clearStartNumber,bandwidthCacheL1Hash)
				clearRewardDbDataCheckHash(LOCKPOSEXITDATA,db,cacheL1,hash,clearStartNumber,posplexitCacheL1Hash)

				clearRewardDbDataCheckHash(LOCKFLOWDATA,db,cacheL2,hash,clearStartNumber,flowCacheL2Hash)
				clearRewardDbDataCheckHash(LOCKBANDWIDTHDATA,db,cacheL2,hash,clearStartNumber,bandwidthCacheL2Hash)

				if clearStartNumber >= StorageEffectBlockNumber {
					keys:=[8]string{storagePleageKey,storageContractKey,storageCapSuccAddrsKey,revertSpaceLockRewardkey,revertExchangeSRTkey,storageRatioskey,storagePledgeRewardkey,storageLeaseRewardkey}
					for _,key:=range keys{
						clearDbDataByNumber(key,db,clearStartNumber)
					}
				}
			}
			if clearStartNumber%blockPerDay==(payFlowRewardInterval/period){
				clearRewardDbDataCheckHash(LOCKFLOWDATA,db,cacheL2,hash,clearStartNumber,flowCacheL2Hash)
			}
			if clearStartNumber%blockPerDay==(payBandwidthRewardInterval/period){
				clearRewardDbDataCheckHash(LOCKBANDWIDTHDATA,db,cacheL2,hash,clearStartNumber,bandwidthCacheL2Hash)
			}
			if clearStartNumber%blockPerDay==(payPOSPGRedeemInterval/period){
				clearRewardDbDataCheckHash(LOCKPOSEXITDATA,db,cacheL2,hash,clearStartNumber,posplexitCacheL2Hash)
			}
			if clearStartNumber%blockPerDay==(payPOSExitInterval/period){
				clearRewardDbDataCheckHash(LOCKPEXITDATA,db,cacheL2,hash,clearStartNumber,posexitCacheL2Hash)
			}
			if clearStartNumber%blockPerDay==(checkPOSAutoExit/period){
				clearRewardDbDataCheckHash(LOCKREWARDDATA,db,cacheL2,hash,clearStartNumber,rewardCacheL2Hash)
			}
			if clearStartNumber==PosrNewCalEffectNumber{
				clearRewardDbDataCheckHash(LOCKPOSEXITDATA,db,cacheL2,hash,clearStartNumber,posplexitCacheL2Hash)
			}

			clearStartNumber++
		}

		log.Info("clearDbSnapDataAtNumber end",
			"len(rewardCacheL1Hash)", len(rewardCacheL1Hash), "rewardCacheL2Hash", len(rewardCacheL2Hash),
			"len(flowCacheL1Hash)", len(flowCacheL1Hash), "flowCacheL2Hash", len(flowCacheL2Hash),
			"len(bandwidthCacheL1Hash)", len(bandwidthCacheL1Hash), "bandwidthCacheL2Hash", len(bandwidthCacheL2Hash),
			"len(posplexitCacheL1Hash)", len(posplexitCacheL1Hash), "posplexitCacheL2Hash", len(posplexitCacheL2Hash),
			"len(posexitCacheL1Hash)", len(posexitCacheL1Hash), "posexitCacheL2Hash", len(posexitCacheL2Hash))

		log.Info("clearDbSnapDataAtNumber end", "clearStartNumber", clearStartNumber, "clearEndNumber", clearEndNumber)
	}
	return nil
}

func (api *API) ViewDbDataAtNumber(startNumber uint64,number uint64) (error) {
	log.Info("api viewDbDataAtNumber", "startNumber", startNumber,"number", number)
	header := api.chain.GetHeaderByNumber(number)
	if header == nil {
		return errUnknownBlock
	}
	dayLimit:=30*api.alien.blockPerDay()
	endNumber:=header.Number.Uint64()
	if endNumber-dayLimit>startNumber{
		startNumber=endNumber-dayLimit
	}
	log.Info("ViewDbDataAtNumber", "startNumber", startNumber, "endNumber", endNumber)
	for startNumber <= endNumber {
		removeHeader := api.chain.GetHeaderByNumber(startNumber)
		hash:= removeHeader.Hash()
		blob, err := api.alien.db.Get(append([]byte("alien-"), hash[:]...))
		if err == nil {
			log.Info("viewDbDataAtNumber snapshot from disk", "number", startNumber, "hash", hash,"len(blob)",len(blob))
		}
		lockTypes := [5]string{LOCKREWARDDATA,LOCKFLOWDATA,LOCKBANDWIDTHDATA,LOCKPOSEXITDATA,LOCKPEXITDATA}
		for _,lockType:=range lockTypes{
			api.loadCacheAtNumber(startNumber,api.alien.db,lockType,"l1",hash)
			api.loadCacheAtNumber(startNumber,api.alien.db,lockType,"l2",hash)
		}
		keys:=[9]string{storagePleageKey,storageContractKey,storageCapSuccAddrsKey,revertSpaceLockRewardkey,revertExchangeSRTkey,storageRatioskey,storagePledgeRewardkey,storageLeaseRewardkey,"flow-%d"}
		for _,key:=range keys{
			api.isExitDiskDataByNumber(key,api.alien.db,startNumber)
		}
		startNumber++
	}
	log.Info("ViewDbDataAtNumber end", "startNumber", startNumber, "endNumber", endNumber)
	return nil
}

func (api *API) loadCacheAtNumber(number uint64, db ethdb.Database,locktype string,cacheType string,hash common.Hash) (error) {
	key := append([]byte("alien-"+locktype+"-"+cacheType+"-"), hash[:]...)
	blob, err := db.Get(key)
	if err != nil {
		return err
	}
	int := bytes.NewBuffer(blob)
	items := []*PledgeItem{}
	err = rlp.Decode(int, &items)
	if err != nil {
		log.Info("loadCacheAtNumber decode err", "Locktype", locktype+" "+cacheType, "cache hash", hash, "size", len(items),"number",number)
		return err
	}
	log.Info("loadCacheAtNumber", "Locktype", locktype+" "+cacheType, "cache hash", hash, "size", len(items),"number",number)
	return err
}

func (api *API) isExitDiskDataByNumber(keyPre string,db ethdb.Database, number uint64) {
	key := fmt.Sprintf(keyPre, number)
	value,err:=db.Get([]byte(key))
	if err==nil{
		log.Info("api isExitDiskDataByNumber", "key",key,"len(value)", len(value))
	}
}

func clearRewardDbData(rewardType string,db ethdb.Database, cacheL string, hash common.Hash,number uint64) {
	keyPre:="alien-"+rewardType+"-"+cacheL+"-"
	key := append([]byte(keyPre), hash[:]...)
	err:= db.Delete(key)
	if err!=nil{
		log.Error("clearRewardDbData", "number", number, "hash",hash,"keyPre",keyPre,"err",err)
	}else{
		log.Info("clearRewardDbData", "number", number, "hash",hash,"keyPre",keyPre)
	}
}

func clearDbDataByNumber(keyPre string,db ethdb.Database, number uint64) {
	key := fmt.Sprintf(keyPre, number)
	err:=db.Delete([]byte(key))
	if err!=nil{
		log.Error("clearDbDataByNumber", "key",key,"err", err)
	}else{
		log.Info("clearDbDataByNumber", "key",key)
	}
}

func clearLockByCacheHash(lockType string, lock *LockData, cacheL1Hash map[common.Hash]uint64, cacheL2Hash map[common.Hash]uint64,number uint64, db ethdb.Database) {
	cacheL1:="l1"
	cacheL2:="l2"
	for _,l1Hash:=range lock.CacheL1{
		if _, ok := cacheL1Hash[l1Hash]; !ok {
			clearRewardDbData(lockType,db,cacheL1,l1Hash,number)
			cacheL1Hash[l1Hash]=1
		}
	}
	l2Hash:=lock.CacheL2
	if _, ok := cacheL2Hash[l2Hash]; !ok {
		clearRewardDbData(lockType,db,cacheL2,l2Hash,number)
		cacheL2Hash[l2Hash]=1
	}
}

func clearRewardDbDataCheckHash(lockType string, db ethdb.Database, l string, hash common.Hash, number uint64, cacheHash map[common.Hash]uint64) {
	if _, ok := cacheHash[hash]; !ok {
		clearRewardDbData(lockType,db,l,hash,number)
		cacheHash[hash]=1
	}
}

func (api *API) isNumberTooSmall(header *types.Header) error {
	//currentHeader := api.chain.CurrentHeader()
	//if isLtClearSnapShotNumber(currentHeader.Number.Uint64()){
	//	return nil
	//}
	//if (currentHeader.Number.Uint64()< retainedLastSnapshot)||(currentHeader.Number.Uint64()-retainedLastSnapshot>header.Number.Uint64()){
	//	return errNumberTooSmall
	//}
	return nil
}