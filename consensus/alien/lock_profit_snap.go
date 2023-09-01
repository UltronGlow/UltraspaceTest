package alien

import (
	"bytes"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/consensus"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"github.com/shopspring/decimal"
	"math/big"
	"time"
)

const (
	LOCKREWARDDATA    = "reward"
	LOCKFLOWDATA      = "flow"
	LOCKBANDWIDTHDATA = "bandwidth"
	LOCKPOSEXITDATA = "posplexit"
	LOCKPEXITDATA = "posexit"
	LOCKSTPEEXITDATA = "stpentrustexit"
	LOCKSTPEDATA = "stpentrust"
	LOCKSPLOCKDATA    = "splock"
	LOCKSPETTTDATA    = "spentrust"
	LOCKSPEXITDATA    = "spexit"
	LOCKSPETTEXITDATA = "spentrustexit"
)

type RlsLockData struct {
	LockBalance map[uint64]map[uint32]*PledgeItem // The primary key is lock number, The second key is pledge type
}
type RlsLockDataV1 struct {
	LockBalanceV1 map[uint64]map[uint32]map[common.Address]*PledgeItem // The primary key is lock number, The second key is pledge type
}

type LockData struct {
	FlowRevenue map[common.Address]*LockBalanceData `json:"flowrevenve"`
	CacheL1     []common.Hash                       `json:"cachel1"` // Store chceckout data
	CacheL2     common.Hash                         `json:"cachel2"` //Store data of the previous day
	//rlsLockBalance map[common.Address]*RlsLockData     // The release lock data
	Locktype string `json:"Locktype"`
}

func NewLockData(t string) *LockData {
	return &LockData{
		FlowRevenue: make(map[common.Address]*LockBalanceData),
		CacheL1:     []common.Hash{},
		CacheL2:     common.Hash{},
		Locktype:    t,
	}
}

func (l *LockData) copy() *LockData {
	clone := &LockData{
		FlowRevenue: make(map[common.Address]*LockBalanceData),
		CacheL1:     []common.Hash{},
		CacheL2:     l.CacheL2,
		//rlsLockBalance: nil,
		Locktype: l.Locktype,
	}
	clone.CacheL1 = make([]common.Hash, len(l.CacheL1))
	copy(clone.CacheL1, l.CacheL1)
	for who, pledges := range l.FlowRevenue {
		clone.FlowRevenue[who] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
			RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
			LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
		}
		for which, balance := range l.FlowRevenue[who].RewardBalance {
			clone.FlowRevenue[who].RewardBalance[which] = new(big.Int).Set(balance)
		}
		for when, pledge1 := range pledges.LockBalance {
			clone.FlowRevenue[who].LockBalance[when] = make(map[uint32]*PledgeItem)
			for which, pledge := range pledge1 {
				clone.FlowRevenue[who].LockBalance[when][which] = pledge.copy()
			}
		}
		if l.FlowRevenue[who].RewardBalanceV1 != nil {
			for whichType, item := range l.FlowRevenue[who].RewardBalanceV1 {
				clone.FlowRevenue[who].RewardBalanceV1[whichType] = make(map[common.Address]*LockTmpData)
				if item != nil && len(item) > 0 {
					for address, lockTmpData := range item {
						clone.FlowRevenue[who].RewardBalanceV1[whichType][address] = &LockTmpData{
							Amount:         new(big.Int).Set(lockTmpData.Amount),
							RevenueAddress: lockTmpData.RevenueAddress,
						}
					}
				}

			}
		}
		if pledges.LockBalanceV1!=nil{
			for when, pledge1 := range pledges.LockBalanceV1 {
				clone.FlowRevenue[who].LockBalanceV1[when] = make(map[uint32]map[common.Address]*PledgeItem)
				for which, pledgeType := range pledge1 {
					if _, ok := clone.FlowRevenue[who].LockBalanceV1[when][which]; !ok {
						clone.FlowRevenue[who].LockBalanceV1[when][which]=make(map[common.Address]*PledgeItem)
					}
					for source, pledge := range pledgeType {
						clone.FlowRevenue[who].LockBalanceV1[when][which][source] = pledge.copy()
					}
				}

			}
		}
	}
	return clone
}

func (s *LockData) addLockData(snap *Snapshot, item LockRewardRecord, headerNumber *big.Int) {
	if _, ok := s.FlowRevenue[item.Target]; !ok {
		s.FlowRevenue[item.Target] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
		}
	}
	flowRevenusTarget := s.FlowRevenue[item.Target]
	if _, ok := flowRevenusTarget.RewardBalance[item.IsReward]; !ok {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Set(item.Amount)
	} else {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Add(flowRevenusTarget.RewardBalance[item.IsReward], item.Amount)
	}
}

func (s *LockData) updateAllLockData(snap *Snapshot, isReward uint32, headerNumber *big.Int) {
	if isGEPOSNewEffect(headerNumber.Uint64()){
		s.updateAllLockData2(snap, isReward, headerNumber)
		return
	}
	for target, flowRevenusTarget := range s.FlowRevenue {
		if 0 >= flowRevenusTarget.RewardBalance[isReward].Cmp(big.NewInt(0)) {
			continue
		}
		if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
			flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
		}
		lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
		// use reward release
		lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
		rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
		interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
		revenueAddress := target
		revenueContract := common.Address{}
		multiSignature := common.Address{}

		if sscEnumSignerReward == isReward {
			// singer reward
			if revenue, ok := snap.RevenueNormal[target]; ok {
				revenueAddress = revenue.RevenueAddress
				revenueContract = revenue.RevenueContract
				multiSignature = revenue.MultiSignature
			}
		} else {
			// flow or bandwidth reward
			if revenue, ok := snap.RevenueFlow[target]; ok {
				revenueAddress = revenue.RevenueAddress
				revenueContract = revenue.RevenueContract
				multiSignature = revenue.MultiSignature
			}
		}
		if _, ok := lockBalance[isReward]; !ok {
			lockBalance[isReward] = &PledgeItem{
				Amount:          big.NewInt(0),
				PledgeType:      isReward,
				Playment:        big.NewInt(0),
				LockPeriod:      lockPeriod,
				RlsPeriod:       rlsPeriod,
				Interval:        interval,
				StartHigh:       headerNumber.Uint64(),
				TargetAddress:   target,
				RevenueAddress:  revenueAddress,
				RevenueContract: revenueContract,
				MultiSignature:  multiSignature,
				BurnAddress: common.Address{},
				BurnRatio: common.Big0,
				BurnAmount: common.Big0,
			}
		}
		lockBalance[isReward].Amount = new(big.Int).Add(lockBalance[isReward].Amount, flowRevenusTarget.RewardBalance[isReward])
		flowRevenusTarget.RewardBalance[isReward] = big.NewInt(0)
	}
}

func (s *LockData) updateLockData(snap *Snapshot, item LockRewardRecord, headerNumber *big.Int) {
	if _, ok := s.FlowRevenue[item.Target]; !ok {
		s.FlowRevenue[item.Target] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
		}
	}
	flowRevenusTarget := s.FlowRevenue[item.Target]
	if _, ok := flowRevenusTarget.RewardBalance[item.IsReward]; !ok {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Set(item.Amount)
	} else {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Add(flowRevenusTarget.RewardBalance[item.IsReward], item.Amount)
	}
	deposit := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e18))
	if _, ok := snap.SystemConfig.Deposit[item.IsReward]; ok {
		deposit = new(big.Int).Set(snap.SystemConfig.Deposit[item.IsReward])
	}
	if 0 > flowRevenusTarget.RewardBalance[item.IsReward].Cmp(deposit) {
		return
	}
	if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
		flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
	}
	lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
	// use reward release
	lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
	rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
	interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
	revenueAddress := item.Target
	revenueContract := common.Address{}
	multiSignature := common.Address{}

	if sscEnumSignerReward == item.IsReward {
		// singer reward
		if revenue, ok := snap.RevenueNormal[item.Target]; ok {
			revenueAddress = revenue.RevenueAddress
			revenueContract = revenue.RevenueContract
			multiSignature = revenue.MultiSignature
		}
	} else {
		if headerNumber.Uint64() >= StorageEffectBlockNumber {
			if isGTPOSRNewCalEffect(headerNumber.Uint64())&&item.IsReward==sscEnumStoragePledgeRedeemLock{

			}else {
				if revenue, ok := snap.RevenueStorage[item.Target]; ok {
					revenueAddress = revenue.RevenueAddress
					revenueContract = revenue.RevenueContract
					multiSignature = revenue.MultiSignature
				}
			}
		}else{
			// flow or bandwidth reward
			if revenue, ok := snap.RevenueFlow[item.Target]; ok {
				revenueAddress = revenue.RevenueAddress
				revenueContract = revenue.RevenueContract
				multiSignature = revenue.MultiSignature
			}
		}

	}
	if _, ok := lockBalance[item.IsReward]; !ok {
		lockBalance[item.IsReward] = &PledgeItem{
			Amount:          big.NewInt(0),
			PledgeType:      item.IsReward,
			Playment:        big.NewInt(0),
			LockPeriod:      lockPeriod,
			RlsPeriod:       rlsPeriod,
			Interval:        interval,
			StartHigh:       headerNumber.Uint64(),
			TargetAddress:   item.Target,
			RevenueAddress:  revenueAddress,
			RevenueContract: revenueContract,
			MultiSignature:  multiSignature,
			BurnAddress: common.Address{},
			BurnRatio: common.Big0,
			BurnAmount: common.Big0,
		}
	}
	lockBalance[item.IsReward].Amount = new(big.Int).Add(lockBalance[item.IsReward].Amount, flowRevenusTarget.RewardBalance[item.IsReward])
	flowRevenusTarget.RewardBalance[item.IsReward] = big.NewInt(0)
}
func (s *LockData) makePolicyLockData(snap *Snapshot, item LockRewardRecord, headerNumber *big.Int) {
	if stPledge,ok:= snap.StorageData.StoragePledge[item.Target] ;ok{
		burnRatio:=big.NewInt(0)
		freeCapacity :=new(big.Int).Sub(stPledge.TotalCapacity,getRentCapity(stPledge))
		if isIncentivePeriod(stPledge,headerNumber.Uint64(),snap.Period) {//<=30 days
			if freeCapacity.Cmp(big.NewInt(0)) >0{
				pledgePeriod :=new(big.Int).Sub(headerNumber,stPledge.Number)
				dayBlockNums := secondsPerDay / snap.Period
				incentiveDays := pledgePeriod.Uint64() / dayBlockNums
				burnRatio = decimal.NewFromInt(int64(incentiveDays)).Div(decimal.NewFromBigInt(IncentivePeriod,0)).Mul(decimal.NewFromBigInt(BurnBase, 0)).BigInt()
				maxBurnRatio:=new(big.Int).Sub(BurnBase,minRentRewardRatio)
				if burnRatio.Cmp(maxBurnRatio) > 0 {
					burnRatio =new(big.Int).Set(maxBurnRatio)
				}
				burnRatio=new(big.Int).Div(new(big.Int).Mul(burnRatio,freeCapacity),stPledge.TotalCapacity)
			}
			s.updateLockDataNew(snap, item, headerNumber,burnRatio)

		}else{// after the incentive period
			if freeCapacity.Cmp(big.NewInt(0)) >0{
				burnRatio = new(big.Int).Sub(BurnBase, minRentRewardRatio)
				burnRatio=new(big.Int).Div(new(big.Int).Mul(burnRatio,freeCapacity),stPledge.TotalCapacity)
			}
			s.updateLockDataNew(snap, item, headerNumber,burnRatio)
		}
	}
}
func (s *LockData) updateLockDataNew(snap *Snapshot, item LockRewardRecord, headerNumber *big.Int,burnRatio *big.Int) {
	if _, ok := s.FlowRevenue[item.Target]; !ok {
		s.FlowRevenue[item.Target] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
		}
	}
	flowRevenusTarget := s.FlowRevenue[item.Target]
	if _, ok := flowRevenusTarget.RewardBalance[item.IsReward]; !ok {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Set(item.Amount)
	} else {
		flowRevenusTarget.RewardBalance[item.IsReward] = new(big.Int).Add(flowRevenusTarget.RewardBalance[item.IsReward], item.Amount)
	}
	deposit := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e18))
	if _, ok := snap.SystemConfig.Deposit[item.IsReward]; ok {
		deposit = new(big.Int).Set(snap.SystemConfig.Deposit[item.IsReward])
	}
	if 0 > flowRevenusTarget.RewardBalance[item.IsReward].Cmp(deposit) {
		return
	}
	if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
		flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
	}
	lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
	// use reward release
	lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
	rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
	interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
	revenueAddress := item.Target
	revenueContract := common.Address{}
	multiSignature := common.Address{}


	// flow or bandwidth reward
	if revenue, ok := snap.RevenueStorage[item.Target]; ok {
		revenueAddress = revenue.RevenueAddress
		revenueContract = revenue.RevenueContract
		multiSignature = revenue.MultiSignature
	}

	if _, ok := lockBalance[item.IsReward]; !ok {
		lockBalance[item.IsReward] = &PledgeItem{
			Amount:          big.NewInt(0),
			PledgeType:      item.IsReward,
			Playment:        big.NewInt(0),
			LockPeriod:      lockPeriod,
			RlsPeriod:       rlsPeriod,
			Interval:        interval,
			StartHigh:       headerNumber.Uint64(),
			TargetAddress:   item.Target,
			RevenueAddress:  revenueAddress,
			RevenueContract: revenueContract,
			MultiSignature:  multiSignature,
			BurnAddress: common.Address{},
			BurnRatio: new(big.Int).Set(burnRatio),
			BurnAmount: common.Big0,
		}
	}
	lockBalance[item.IsReward].Amount = new(big.Int).Add(lockBalance[item.IsReward].Amount, flowRevenusTarget.RewardBalance[item.IsReward])
	flowRevenusTarget.RewardBalance[item.IsReward] = big.NewInt(0)
}
func (s *LockData) payProfit(hash common.Hash, db ethdb.Database, period uint64, headerNumber uint64, currentGrantProfit []consensus.GrantProfitRecord, playGrantProfit []consensus.GrantProfitRecord, header *types.Header, state *state.StateDB, payAddressAll map[common.Address]*big.Int) ([]consensus.GrantProfitRecord, []consensus.GrantProfitRecord, error) {
	if isGEInitStorageManagerNumber(headerNumber){
		return s.payProfitV1(hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	timeNow := time.Now()
	rlsLockBalance := make(map[common.Address]*RlsLockData)
	err := s.saveCacheL1(db, hash)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	items, err := s.loadCacheL1(db)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	s.appendRlsLockData(rlsLockBalance, items)

	log.Info("payProfit load from disk", "Locktype", s.Locktype, "len(rlsLockBalance)", len(rlsLockBalance), "elapsed", time.Since(timeNow), "number", header.Number.Uint64())

	for address, items := range rlsLockBalance {
		for blockNumber, item1 := range items.LockBalance {
			for which, item := range item1 {
				result, amount := paymentPledge(true, item, state, header, payAddressAll)
				if 0 == result {
					playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
						Which:           which,
						MinerAddress:    address,
						BlockNumber:     blockNumber,
						Amount:          new(big.Int).Set(amount),
						RevenueAddress:  item.RevenueAddress,
						RevenueContract: item.RevenueContract,
						MultiSignature:  item.MultiSignature,
					})
				} else if 1 == result {
					currentGrantProfit = append(currentGrantProfit, consensus.GrantProfitRecord{
						Which:           which,
						MinerAddress:    address,
						BlockNumber:     blockNumber,
						Amount:          new(big.Int).Set(amount),
						RevenueAddress:  item.RevenueAddress,
						RevenueContract: item.RevenueContract,
						MultiSignature:  item.MultiSignature,
					})
				}
			}
		}
	}
	log.Info("payProfit ", "Locktype", s.Locktype, "elapsed", time.Since(timeNow), "number", header.Number.Uint64())
	return currentGrantProfit, playGrantProfit, nil
}

func (s *LockData) updateGrantProfit(grantProfit []consensus.GrantProfitRecord, db ethdb.Database, hash common.Hash,number uint64) error {
    if isGEInitStorageManagerNumber(number){
    	return s.updateGrantProfit2(grantProfit, db, hash,number)
	}
	rlsLockBalance := make(map[common.Address]*RlsLockData)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}

	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)

	hasChanged := false
	for _, item := range grantProfit {
		if 0 != item.BlockNumber {
			if _, ok := rlsLockBalance[item.MinerAddress]; ok {
				if _, ok = rlsLockBalance[item.MinerAddress].LockBalance[item.BlockNumber]; ok {
					if pledge, ok := rlsLockBalance[item.MinerAddress].LockBalance[item.BlockNumber][item.Which]; ok {
						pledge.Playment = new(big.Int).Add(pledge.Playment, item.Amount)
						burnAmount:=calBurnAmount(pledge,item.Amount)
						if burnAmount.Cmp(common.Big0)>0{
							pledge.BurnAmount= new(big.Int).Add(pledge.BurnAmount,burnAmount)
						}
						hasChanged = true
						if 0 <= pledge.Playment.Cmp(pledge.Amount) {
							delete(rlsLockBalance[item.MinerAddress].LockBalance[item.BlockNumber], item.Which)
							if 0 >= len(rlsLockBalance[item.MinerAddress].LockBalance[item.BlockNumber]) {
								delete(rlsLockBalance[item.MinerAddress].LockBalance, item.BlockNumber)
								if 0 >= len(rlsLockBalance[item.MinerAddress].LockBalance) {
									delete(rlsLockBalance, item.MinerAddress)
								}
							}
						}
					}
				}
			}
		}
	}
	if hasChanged {
		s.saveCacheL2(db, rlsLockBalance, hash,number)
	}
	return nil
}

func (snap *LockProfitSnap) updateMergeLockData( db ethdb.Database,period uint64,hash common.Hash) error {
	log.Info("begin merge lockdata")
	err := snap.RewardLock.mergeLockData(db,period,hash)
	if err == nil {
		log.Info("updateMergeLockData","merge lockdata successful err=",err)
	}else{
		log.Info("updateMergeLockData","merge lockdata faild ",err)
	}
	return err
}
func (s *LockData) mergeLockData(db ethdb.Database,period uint64,hash common.Hash) error{
	rlsLockBalance := make(map[common.Address]*RlsLockData)
	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}

	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	mergeRlsLkBalance := make(map[common.Hash]*RlsLockData)
	blockPerDay := secondsPerDay / period
	for  _,rlsLockData := range rlsLockBalance {
		for  lockNumber,pledgeItem := range rlsLockData.LockBalance {
			bnumber :=  blockPerDay * (lockNumber / blockPerDay)+1
			for locktype,item := range  pledgeItem {
				if bnumber==1 {
					if item.TargetAddress == common.HexToAddress("ux31f440fc8dd98bdbdb72ebd8a14a469439fc3433") && item.RevenueAddress==common.HexToAddress("ux31f440fc8dd98bdbdb72ebd8a14a469439fc3433"){
						continue
					}
					if item.TargetAddress == common.HexToAddress("ux82de6bd4b822c5af6110de34a133980c456708e0") && item.RevenueAddress!=common.HexToAddress("ux82de6bd4b822c5af6110de34a133980c456708e0"){
						continue
					}
					if item.TargetAddress == common.HexToAddress("uxfeac212688fdc4d7f0f5af8caa02f981d55a7cf4") && item.RevenueAddress!=common.HexToAddress("uxfeac212688fdc4d7f0f5af8caa02f981d55a7cf4"){
						continue
					}
				}
				if bnumber == 43201 {
					if item.TargetAddress == common.HexToAddress("uxa573d8c28a709acba1eb10e605694482a92c3593") &&item.RevenueAddress==common.HexToAddress("uxa573d8c28a709acba1eb10e605694482a92c3593"){
						continue
					}
					if item.TargetAddress== common.HexToAddress("uxd691ea3fd19437bbd27a590bfca3c435c9c07c38")&&item.RevenueAddress== common.HexToAddress("uxd691ea3fd19437bbd27a590bfca3c435c9c07c38"){
						continue
					}
					if item.TargetAddress ==common.HexToAddress( "ux208bc40a411786f9ce7b4a3d1f8424a4f59406e8")&&item.RevenueAddress ==common.HexToAddress( "ux208bc40a411786f9ce7b4a3d1f8424a4f59406e8"){
						continue

					}
					if item.TargetAddress == common.HexToAddress("ux7d51170f140c47e547664ead4d1185ef864ba689")&& item.RevenueAddress == common.HexToAddress("ux7d51170f140c47e547664ead4d1185ef864ba689"){
						continue

					}
					if item.TargetAddress == common.HexToAddress("ux81ae1b55bb078102c965bb8a4faf48ecc4380f55")&&item.RevenueAddress == common.HexToAddress("ux81ae1b55bb078102c965bb8a4faf48ecc4380f55"){
						continue

					}
					if item.TargetAddress == common.HexToAddress("uxf4955c8a120b1cdf3bfd7c6dc43837dd65360f01")&&item.RevenueAddress == common.HexToAddress("uxf4955c8a120b1cdf3bfd7c6dc43837dd65360f01"){
						continue

					}
					if item.TargetAddress == common.HexToAddress("ux88eA42c6A2D9B23C52534b0e1eEcf3DEa0c6De76")&&item.RevenueAddress == common.HexToAddress("ux88eA42c6A2D9B23C52534b0e1eEcf3DEa0c6De76"){
						continue
					}
					if item.TargetAddress == common.HexToAddress("uxd9aac9B61571B9bE5717A275f41d772E9bfc745C") && item.RevenueAddress==common.HexToAddress("uxd9aac9B61571B9bE5717A275f41d772E9bfc745C"){
						continue
					}
				}

				hash :=common.HexToHash(item.TargetAddress.String()+item.RevenueAddress.String()+item.RevenueContract.String()+item.MultiSignature.String())
				if _, ok := mergeRlsLkBalance[hash]; !ok {
					mergeRlsLkBalance[hash] = &RlsLockData{
						LockBalance: make(map[uint64]map[uint32]*PledgeItem),
					}
				}
				if _, ok := mergeRlsLkBalance[hash].LockBalance[bnumber]; !ok {
					mergeRlsLkBalance[hash].LockBalance[bnumber] =  make(map[uint32]*PledgeItem)
				}
				mergepledgeItem :=	mergeRlsLkBalance[hash].LockBalance[bnumber]
				if _, ok :=mergepledgeItem[locktype]; !ok {
					mergepledgeItem[locktype]=item
					mergepledgeItem[locktype].StartHigh=bnumber
				}else{
					mergepledgeItem[locktype].Amount=new(big.Int).Add(mergepledgeItem[locktype].Amount,item.Amount)
					mergepledgeItem[locktype].Playment=new(big.Int).Add(mergepledgeItem[locktype].Playment,item.Playment)
				}
			}
		}
	}
	for _,lockdata:=range mergeRlsLkBalance{
		for blockNumber,items:=range lockdata.LockBalance{
			item:=items[3]
			if blockNumber==1 {
				if item.TargetAddress == common.HexToAddress("ux31f440fc8dd98bdbdb72ebd8a14a469439fc3433") {
					amount,_:=decimal.NewFromString("1441044761571428550400")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("112081259233333329136")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress == common.HexToAddress("ux82de6bd4b822c5af6110de34a133980c456708e0"){
					amount,_:=decimal.NewFromString("1442041061571428560400")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("112158749233333329912")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress == common.HexToAddress("uxfeac212688fdc4d7f0f5af8caa02f981d55a7cf4"){
					amount,_:=decimal.NewFromString("1436585442571428540400")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("111734423311111106144")
					item.Playment=playment.BigInt()
				}
			}
			if blockNumber == 43201 {
				if item.TargetAddress == common.HexToAddress("uxa573d8c28a709acba1eb10e605694482a92c3593"){
					amount,_:=decimal.NewFromString("411699799999999460000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20584989999999973000")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress== common.HexToAddress("uxd691ea3fd19437bbd27a590bfca3c435c9c07c38"){
					amount,_:=decimal.NewFromString("411721999999999400000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20586099999999970000")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress ==common.HexToAddress( "ux208bc40a411786f9ce7b4a3d1f8424a4f59406e8"){
					amount,_:=decimal.NewFromString("411251599999999320000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20562579999999966000")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress == common.HexToAddress("ux7d51170f140c47e547664ead4d1185ef864ba689"){
					amount,_:=decimal.NewFromString("411773799999999260000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20588689999999963000")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress == common.HexToAddress("ux81ae1b55bb078102c965bb8a4faf48ecc4380f55"){
					amount,_:=decimal.NewFromString("411274561142856400800")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20563728057142820040")
					item.Playment=playment.BigInt()
				}
				if item.TargetAddress == common.HexToAddress("uxf4955c8a120b1cdf3bfd7c6dc43837dd65360f01"){
					amount,_:=decimal.NewFromString("411273799999999260000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20563689999999963000")
					item.Playment=playment.BigInt()

				}
				if item.TargetAddress == common.HexToAddress("ux88eA42c6A2D9B23C52534b0e1eEcf3DEa0c6De76"){
					amount,_:=decimal.NewFromString("411729399999999380000")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20586469999999969000")
					item.Playment=playment.BigInt()

				}
				if item.TargetAddress == common.HexToAddress("uxd9aac9b61571b9be5717a275f41d772e9bfc745c"){
					amount,_:=decimal.NewFromString("411771622285713551600")
					item.Amount=amount.BigInt()
					playment,_:=decimal.NewFromString("20588581114285677580")
					item.Playment=playment.BigInt()

				}
			}

		}
	}
	return s.saveMereCacheL2(db,mergeRlsLkBalance,hash)
}

func (s *LockData) saveMereCacheL2(db ethdb.Database, rlsLockBalance map[common.Hash]*RlsLockData, hash common.Hash) error {
	items := []*PledgeItem{}
	for _, pledges := range rlsLockBalance {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}
	err, buf := PledgeItemEncodeRlp(items)
	if err != nil {
		return err
	}
	err = db.Put(append([]byte("alien-"+s.Locktype+"-l2-"), hash[:]...), buf)
	if err != nil {
		return err
	}
	for _, pledges := range s.FlowRevenue {
		pledges.LockBalance = make(map[uint64]map[uint32]*PledgeItem)
	}
	s.CacheL1 = []common.Hash{}
	s.CacheL2 = hash
	log.Info("LockProfitSnap saveMereCacheL2", "Locktype", s.Locktype, "cache hash", hash, "len", len(items))
	return nil
}

func (s *LockData) loadCacheL1(db ethdb.Database) ([]*PledgeItem, error) {
	result := []*PledgeItem{}
	for _, lv1 := range s.CacheL1 {
		key := append([]byte("alien-"+s.Locktype+"-l1-"), lv1[:]...)
		blob, err := db.Get(key)
		if err != nil {
			return nil, err
		}
		int := bytes.NewBuffer(blob)
		items := []*PledgeItem{}
		err = rlp.Decode(int, &items)
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
		log.Info("LockProfitSnap loadCacheL1", "Locktype", s.Locktype, "cache hash", lv1, "size", len(items))
	}
	return result, nil
}

func (s *LockData) appendRlsLockData(rlsLockBalance map[common.Address]*RlsLockData, items []*PledgeItem) {
	for _, item := range items {
		if _, ok := rlsLockBalance[item.TargetAddress]; !ok {
			rlsLockBalance[item.TargetAddress] = &RlsLockData{
				LockBalance: make(map[uint64]map[uint32]*PledgeItem),
			}
		}
		flowRevenusTarget := rlsLockBalance[item.TargetAddress]
		if _, ok := flowRevenusTarget.LockBalance[item.StartHigh]; !ok {
			flowRevenusTarget.LockBalance[item.StartHigh] = make(map[uint32]*PledgeItem)
		}
		lockBalance := flowRevenusTarget.LockBalance[item.StartHigh]
		lockBalance[item.PledgeType] = item
	}
}

func (s *LockData) saveCacheL1(db ethdb.Database, hash common.Hash) error {
	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
		pledges.LockBalance = make(map[uint64]map[uint32]*PledgeItem)
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledge := range pledgeV1Item {
						items = append(items, pledge)
					}
				}
			}
			pledges.LockBalanceV1 = make(map[uint64]map[uint32]map[common.Address]*PledgeItem)
		}
	}
	if len(items) == 0 {
		return nil
	}
	err, buf := PledgeItemEncodeRlp(items)
	if err != nil {
		return err
	}
	err = db.Put(append([]byte("alien-"+s.Locktype+"-l1-"), hash[:]...), buf)
	if err != nil {
		return err
	}
	s.CacheL1 = append(s.CacheL1, hash)
	log.Info("LockProfitSnap saveCacheL1", "Locktype", s.Locktype, "cache hash", hash, "len", len(items))
	return nil
}

func (s *LockData) saveCacheL2(db ethdb.Database, rlsLockBalance map[common.Address]*RlsLockData, hash common.Hash,number uint64) error {
	items := []*PledgeItem{}
	for _, pledges := range rlsLockBalance {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}
	err, buf := PledgeItemEncodeRlp(items)
	if err != nil {
		return err
	}
	err = db.Put(append([]byte("alien-"+s.Locktype+"-l2-"), hash[:]...), buf)
	if err != nil {
		return err
	}
	for _, pledges := range s.FlowRevenue {
		pledges.LockBalance = make(map[uint64]map[uint32]*PledgeItem)
	}
	s.CacheL1 = []common.Hash{}
	s.CacheL2 = hash
	log.Info("LockProfitSnap saveCacheL2", "Locktype", s.Locktype, "cache hash", hash, "len", len(items),"number",number)
	return nil
}

func (s *LockData) loadCacheL2(db ethdb.Database) ([]*PledgeItem, error) {
	items := []*PledgeItem{}
	nilHash := common.Hash{}
	if s.CacheL2 == nilHash {
		return items, nil
	}
	key := append([]byte("alien-"+s.Locktype+"-l2-"), s.CacheL2[:]...)
	blob, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	int := bytes.NewBuffer(blob)
	err = rlp.Decode(int, &items)
	if err != nil {
		return nil, err
	}
	log.Info("LockProfitSnap loadCacheL2", "Locktype", s.Locktype, "cache hash", s.CacheL2, "len", len(items))
	return items, nil
}

func convToPAddr(items []PledgeItem) []*PledgeItem {
	ret := []*PledgeItem{}
	for _, item := range items {
		ret = append(ret, &item)
	}
	return ret
}

type LockProfitSnap struct {
	Number        uint64      `json:"number"` // Block number where the snapshot was created
	Hash          common.Hash `json:"hash"`   // Block hash where the snapshot was created
	RewardLock    *LockData   `json:"reward"`
	FlowLock      *LockData   `json:"flow"`
	BandwidthLock *LockData   `json:"bandwidth"`
	PosPgExitLock *LockData   `json:"storagePgExit"`
	PosExitLock   *LockData   `json:"posexitlock"`
	STPEntrustExitLock *LockData `json:"stpentrustexitlock"`
	STPEntrustLock *LockData `json:"stpentrustlock"`
	SpEntrustLock      *LockData   `json:"spEntrustLock"`
	SpLock             *LockData   `json:"spLock"`
	SpExitLock         *LockData   `json:"spExitLock"`
	SpEntrustExitLock  *LockData   `json:"spEntrustExitLock"`
}

func NewLockProfitSnap() *LockProfitSnap {
	return &LockProfitSnap{
		Number:        0,
		Hash:          common.Hash{},
		RewardLock:    NewLockData(LOCKREWARDDATA),
		FlowLock:      NewLockData(LOCKFLOWDATA),
		BandwidthLock: NewLockData(LOCKBANDWIDTHDATA),
		PosPgExitLock: NewLockData(LOCKPOSEXITDATA),
		PosExitLock:   NewLockData(LOCKPEXITDATA),
		STPEntrustExitLock:   NewLockData(LOCKSTPEEXITDATA),
		STPEntrustLock:   NewLockData(LOCKSTPEDATA),
		SpEntrustLock:      NewLockData(LOCKSPETTTDATA),
		SpLock:             NewLockData(LOCKSPLOCKDATA),
		SpExitLock:         NewLockData(LOCKSPEXITDATA),
		SpEntrustExitLock:  NewLockData(LOCKSPETTEXITDATA),
	}
}
func (s *LockProfitSnap) copy() *LockProfitSnap {
	if s.Number <PledgeRevertLockEffectNumber{
		clone := &LockProfitSnap{
			Number:        s.Number,
			Hash:          s.Hash,
			RewardLock:    s.RewardLock.copy(),
			FlowLock:      s.FlowLock.copy(),
			BandwidthLock: s.BandwidthLock.copy(),
		}
		return clone
	}
	if s.PosPgExitLock == nil {
		s.PosPgExitLock =NewLockData(LOCKPOSEXITDATA)
	}
	if s.PosExitLock == nil {
		s.PosExitLock =NewLockData(LOCKPEXITDATA)
	}
	if s.STPEntrustExitLock == nil {
		s.STPEntrustExitLock =NewLockData(LOCKSTPEEXITDATA)
	}
	if s.STPEntrustLock == nil {
		s.STPEntrustLock =NewLockData(LOCKSTPEDATA)
	}
	if s.SpEntrustLock == nil {
		s.SpEntrustLock = NewLockData(LOCKSPETTTDATA)
	}
	if s.SpLock == nil {
		s.SpLock = NewLockData(LOCKSPLOCKDATA)
	}
	if s.SpExitLock == nil {
		s.SpExitLock = NewLockData(LOCKSPEXITDATA)
	}
	if s.SpEntrustExitLock == nil {
		s.SpEntrustExitLock = NewLockData(LOCKSPETTEXITDATA)
	}
		clone := &LockProfitSnap{
		Number:        s.Number,
		Hash:          s.Hash,
		RewardLock:    s.RewardLock.copy(),
		FlowLock:      s.FlowLock.copy(),
		BandwidthLock: s.BandwidthLock.copy(),
		PosPgExitLock: s.PosPgExitLock.copy(),
		PosExitLock:   s.PosExitLock.copy(),
		STPEntrustExitLock:   s.STPEntrustExitLock.copy(),
		STPEntrustLock:   s.STPEntrustLock.copy(),
		SpEntrustLock:      s.SpEntrustLock.copy(),
		SpLock:             s.SpLock.copy(),
		SpExitLock:         s.SpExitLock.copy(),
		SpEntrustExitLock:  s.SpEntrustExitLock.copy(),
	}


	return clone
}

func (s *LockProfitSnap) updateLockData(snap *Snapshot, LockReward []LockRewardRecord, headerNumber *big.Int) {
	distribute:=make(map[common.Address]*big.Int)
	distributePool:=make(map[common.Hash]*big.Int)
	currentLockReward :=make([]LockRewardNewRecord,0)
	blockNumber := headerNumber.Uint64()
	for _, item := range LockReward {
		if sscEnumSignerReward == item.IsReward {
			if islockSimplifyEffectBlocknumber(blockNumber) {
				s.RewardLock.addLockData(snap, item, headerNumber)
			} else {
				s.RewardLock.updateLockData(snap, item, headerNumber)
			}
		} else if sscEnumFlwReward == item.IsReward {
			if isLtInitStorageManagerNumber(headerNumber.Uint64()){
				s.FlowLock.updateLockData(snap, item, headerNumber)
			}else{
				currentLockReward=s.FlowLock.distributeSTPLockData(snap, item, headerNumber,distribute,distributePool,currentLockReward,sscEnumFlwReward)
			}
		} else if sscEnumBandwidthReward == item.IsReward {
			if headerNumber.Uint64() < PosrIncentiveEffectNumber {
				s.BandwidthLock.updateLockData(snap, item, headerNumber)
			}else{
				if isLtGrantEffectNumber(headerNumber.Uint64()) {
					s.BandwidthLock.makePolicyLockData(snap, item, headerNumber)
				}else{
					if isLtInitStorageManagerNumber(headerNumber.Uint64()){
						s.BandwidthLock.updateLockData(snap, item, headerNumber)
					}else{
						currentLockReward=s.BandwidthLock.distributeSTPLockData(snap, item, headerNumber,distribute,distributePool,currentLockReward,sscEnumBandwidthReward)
					}
				}
			}

		}else if sscEnumStoragePledgeRedeemLock == item.IsReward {
			if isGEInitStorageManagerNumber(headerNumber.Uint64()){
				itemNew:=LockRewardNewRecord{
					Target:item.Target,
					Amount:new(big.Int).Set(item.Amount),
					IsReward:item.IsReward,
					SourceAddress:common.Address{},
					RevenueAddress:item.Target,
				}
				s.PosPgExitLock.updateLockDataV1(snap, itemNew, headerNumber)
			}else{
				s.PosPgExitLock.updateLockData(snap, item, headerNumber)
			}
		}
	}
	if islockSimplifyEffectBlocknumber(blockNumber) {
		blockPerDay := snap.getBlockPreDay()
		if 0 == blockNumber%blockPerDay && blockNumber != 0 {
			s.RewardLock.updateAllLockData(snap, sscEnumSignerReward, headerNumber)
		}
	}
	if isGEInitStorageManagerNumber(blockNumber){
		for miner,amount:=range distribute{
			details:=snap.StorageData.StorageEntrust[miner].Detail
			totalAmount:=snap.StorageData.StorageEntrust[miner].PledgeAmount
			for _,item:=range details{
				entrustAmount:=new(big.Int).Mul(amount,item.Amount)
				entrustAmount=new(big.Int).Div(entrustAmount,totalAmount)
				currentLockReward=append(currentLockReward,LockRewardNewRecord{
					Target:item.Address,
					Amount:new(big.Int).Set(entrustAmount),
					IsReward:uint32(sscEnumSTEntrustLockReward),
					SourceAddress:miner,
					RevenueAddress:item.Address,
				})
			}
		}
		snap.FlowRevenue.updateLockDataV1(snap, currentLockReward, headerNumber)
		if len(distributePool) >0 {
			s.BandwidthLock.distributeSpReward(snap,distributePool,headerNumber)
		}
	}
}

func (s *LockProfitSnap) payProfit(db ethdb.Database, period uint64, headerNumber uint64, currentGrantProfit []consensus.GrantProfitRecord, playGrantProfit []consensus.GrantProfitRecord, header *types.Header, state *state.StateDB, payAddressAll map[common.Address]*big.Int) ([]consensus.GrantProfitRecord, []consensus.GrantProfitRecord, error) {
	number := header.Number.Uint64()
	if number == 0 {
		return currentGrantProfit, playGrantProfit, nil
	}
	if isPaySignerRewards(number, period) {
		log.Info("LockProfitSnap pay reward profit")
		return s.RewardLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPayFlowRewards(number, period) {
		log.Info("LockProfitSnap pay flow profit")
		return s.FlowLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPayBandWidthRewards(number, period) {
		log.Info("LockProfitSnap pay bandwidth profit")
		return s.BandwidthLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPayPosPledgeExit(number, period) {
		log.Info("LockProfitSnap pay POS pledge exit amount")
		return s.PosPgExitLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPayPosExit(number, period) {
		log.Info("LockProfitSnap pay POS exit amount")
		return s.PosExitLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySTPEntrustExit(number, period) {
		log.Info("LockProfitSnap pay STP entrust exit amount")
		return s.STPEntrustExitLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySTPEntrust(number, period) {
		log.Info("LockProfitSnap pay STP entrust amount")
		return s.STPEntrustLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySpReWard(number, period) {
		log.Info("LockProfitSnap pay SP Reward amount")
		return s.SpLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySpEntrustReWard(number, period) {
		log.Info("LockProfitSnap pay SP Entrust Reward amount")
		return s.SpEntrustLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySpExit(number, period) {
		log.Info("LockProfitSnap pay SP  Exit amount")
		return s.SpExitLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	if isPaySpEntrustExit(number, period) {
		log.Info("LockProfitSnap pay SP Entrust Exit amount")
		return s.SpEntrustExitLock.payProfit(s.Hash, db, period, headerNumber, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	}
	return currentGrantProfit, playGrantProfit, nil
}

func (snap *LockProfitSnap) updateGrantProfit(grantProfit []consensus.GrantProfitRecord, db ethdb.Database, headerHash common.Hash, number uint64) {
	shouldUpdateReward, shouldUpdateFlow, shouldUpdateBandwidth,shouldUpdatePosPgExit,shouldUpdatePosExit ,shouldUpdateSTPEntrustexit,shouldUpdateSTPEntrust,
		shouldUpdateSp,shouldUpdateSpEt,shouldUpdateSpExit,shouldUpdateSpEtExit:= false, false, false,false,false,false,false,false,false,false,false
	for _, item := range grantProfit {
		if 0 != item.BlockNumber {
			if item.Which == sscEnumSignerReward {
				shouldUpdateReward = true
			} else if item.Which == sscEnumFlwReward {
				shouldUpdateFlow = true
			} else if item.Which == sscEnumBandwidthReward {
				shouldUpdateBandwidth = true
			}else if item.Which == sscEnumStoragePledgeRedeemLock {
				shouldUpdatePosPgExit = true
			}else if item.Which == sscEnumPosExitLock {
				shouldUpdatePosExit = true
			}else if item.Which == sscEnumSTEntrustExitLock {
				shouldUpdateSTPEntrustexit = true
			}else if item.Which == sscEnumSTEntrustLockReward {
				shouldUpdateSTPEntrust = true
			}else if item.Which == sscSpLockReward {
				shouldUpdateSp = true
			}else if item.Which == sscSpEntrustLockReward {
				shouldUpdateSpEt = true
			}else if item.Which == sscSpExitLockReward {
				shouldUpdateSpExit = true
			}else if item.Which == sscSpEntrustExitLockReward {
				shouldUpdateSpEtExit = true
			}
		}
	}
	storeHash:=snap.Hash
	if number>=PledgeRevertLockEffectNumber{
		storeHash=headerHash
	}
	if shouldUpdateReward {
		err := snap.RewardLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Reward Error", "err", err)
		}
	}
	if shouldUpdateFlow {
		err := snap.FlowLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Flow Error", "err", err)
		}
	}
	if shouldUpdateBandwidth {
		err := snap.BandwidthLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Bandwidth Error", "err", err)
		}
	}
	if shouldUpdatePosPgExit {
		err := snap.PosPgExitLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Pos pledge exit amount Error", "err", err)
		}
	}
	if shouldUpdatePosExit {
		err := snap.PosExitLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Pos pledge exit amount Error", "err", err)
		}
	}
	if shouldUpdateSTPEntrustexit {
		err := snap.STPEntrustExitLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit STPEntrustexit amount Error", "err", err)
		}
	}
	if shouldUpdateSTPEntrust {
		err := snap.STPEntrustLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit STPEntrust amount Error", "err", err)
		}
	}
	if shouldUpdateSp {
		err := snap.SpLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit Sp amount Error", "err", err)
		}
	}
	if shouldUpdateSpEt {
		err := snap.SpEntrustLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit SP Entrust amount Error", "err", err)
		}
	}
	if shouldUpdateSpExit {
		err := snap.SpExitLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit SP Exit amount Error", "err", err)
		}
	}
	if shouldUpdateSpEtExit {
		err := snap.SpEntrustExitLock.updateGrantProfit(grantProfit, db, storeHash,number)
		if err != nil {
			log.Warn("updateGrantProfit SP Entrust Exit amount Error", "err", err)
		}
	}
}

func (snap *LockProfitSnap) saveCacheL1(db ethdb.Database) error {
	err := snap.RewardLock.saveCacheL1(db, snap.Hash)
	if err != nil {
		return err
	}
	err = snap.FlowLock.saveCacheL1(db, snap.Hash)
	if err != nil {
		return err
	}
	if snap.Number >= PledgeRevertLockEffectNumber && snap.PosPgExitLock!=nil{
		err = snap.PosPgExitLock.saveCacheL1(db, snap.Hash)
		if err != nil {
			return err
		}
	}
	if isGEPOSNewEffect(snap.Number) && snap.PosExitLock!=nil{
		err = snap.PosExitLock.saveCacheL1(db, snap.Hash)
		if err != nil {
			return err
		}
	}
	if isGEInitStorageManagerNumber(snap.Number){
		if snap.STPEntrustExitLock!=nil{
			err = snap.STPEntrustExitLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}

		if snap.STPEntrustLock!=nil{
			err = snap.STPEntrustLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}

		if snap.SpLock!=nil{
			err = snap.SpLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}

		if snap.SpEntrustLock!=nil{
			err = snap.SpEntrustLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}
		if snap.SpExitLock!=nil{
			err = snap.SpExitLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}
		if snap.SpEntrustExitLock!=nil{
			err = snap.SpEntrustExitLock.saveCacheL1(db, snap.Hash)
			if err != nil {
				return err
			}
		}
	}

	return snap.BandwidthLock.saveCacheL1(db, snap.Hash)
}

func PledgeItemEncodeRlp(items []*PledgeItem) (error, []byte) {
	out := bytes.NewBuffer(make([]byte, 0, 255))
	err := rlp.Encode(out, items)
	if err != nil {
		return err, nil
	}
	return nil, out.Bytes()
}

func (s *LockData) calPayProfit(db ethdb.Database,playGrantProfit []consensus.GrantProfitRecord, header *types.Header) ([]consensus.GrantProfitRecord, error) {
	if isGEInitStorageManagerNumber(header.Number.Uint64()){
		return s.calPayProfitV1(db, playGrantProfit, header)
	}
	timeNow := time.Now()

	rlsLockBalance := make(map[common.Address]*RlsLockData)
	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}
	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return playGrantProfit, err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return playGrantProfit, err
	}
	s.appendRlsLockData(rlsLockBalance, items)

	log.Info("calPayProfit load from disk", "Locktype", s.Locktype, "len(rlsLockBalance)", len(rlsLockBalance), "elapsed", time.Since(timeNow), "number", header.Number.Uint64())

	for address, items := range rlsLockBalance {
		for blockNumber, item1 := range items.LockBalance {
			for which, item := range item1 {
				amount := calPaymentPledge( item, header)
				if nil!= amount {
					playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
						Which:           which,
						MinerAddress:    address,
						BlockNumber:     blockNumber,
						Amount:          new(big.Int).Set(amount),
						RevenueAddress:  item.RevenueAddress,
						RevenueContract: item.RevenueContract,
						MultiSignature:  item.MultiSignature,
					})
				}
			}
		}
	}
	log.Info("calPayProfit ", "Locktype", s.Locktype, "elapsed", time.Since(timeNow), "number", header.Number.Uint64())
	return playGrantProfit, nil
}

func (s *LockData) setBandwidthMakeupPunish(stgBandwidthMakeup map[common.Address]*BandwidthMakeup, storageData *StorageData, db ethdb.Database, hash common.Hash, number uint64,pledgeBw map[common.Address]*big.Int) error{
	rlsLockBalance := make(map[common.Address]*RlsLockData)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
	}

	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)

	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalance:=itemRlsLock.LockBalance
		burnRatio:=common.Big0
		rewardRatio:=new(big.Int).Set(BurnBase)
		if _, ok := storageData.StoragePledge[minerAddress]; ok {
			if bMakeup, ok2 := stgBandwidthMakeup[minerAddress]; ok2 {
				if isGTBandwidthPunishLine(bMakeup) {
					rewardRatio=new(big.Int).Mul(pledgeBw[minerAddress],BurnBase)
					rewardRatio=new(big.Int).Div(rewardRatio,bMakeup.OldBandwidth)
					burnRatio=new(big.Int).Sub(BurnBase,rewardRatio)
				}
			}
		}
		for _,itemBlockLock:=range lockBalance{
			for _,itemWhichLock:=range itemBlockLock{
				oldBurnRatio:=new(big.Int).Set(itemWhichLock.BurnRatio)
				if burnRatio.Cmp(BurnBase)<0&&oldBurnRatio!=nil&&oldBurnRatio.Cmp(common.Big0)>0{
					//1-(1-b1)*rewardRatio
					l1:=new(big.Int).Sub(BurnBase,oldBurnRatio) //1-b1
					l3:=new(big.Int).Mul(l1,rewardRatio)
					l3=new(big.Int).Div(l3,BurnBase)
					newBurnRatio:=new(big.Int).Sub(BurnBase,l3)
					s.setBurnRatio(itemWhichLock,newBurnRatio)
				}else{
					s.setBurnRatio(itemWhichLock,burnRatio)
				}
			}
		}
	}
	s.saveCacheL2(db, rlsLockBalance, hash,number)
	return nil
}

func calBurnAmount(pledge *PledgeItem, amount *big.Int) *big.Int {
	burnAmount:=common.Big0
	if pledge.BurnRatio!=nil&&pledge.BurnRatio.Cmp(common.Big0)>0{
		burnAmount=new(big.Int).Mul(amount,pledge.BurnRatio)
		burnAmount=new(big.Int).Div(burnAmount, BurnBase)
	}
	return burnAmount
}

func (s *LockData) setStorageRemovePunish(pledge []common.Address, db ethdb.Database, hash common.Hash, number uint64) interface{} {
	rlsLockBalance := make(map[common.Address]*RlsLockData)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge2 := range pledge1 {
				items = append(items, pledge2)
			}
		}
	}

	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockData(rlsLockBalance, items)

	pledgeAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range pledge {
		pledgeAddrs[sPAddrs] = 1
	}
	hasChanged := false
	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalance:=itemRlsLock.LockBalance
		if _, ok := pledgeAddrs[minerAddress]; ok {
			hasChanged=true
			burnRatio:=new(big.Int).Set(BurnBase)
			for _,itemBlockLock:=range lockBalance{
				for _,itemWhichLock:=range itemBlockLock{
					s.setBurnRatio(itemWhichLock,burnRatio)
				}
			}
		}
	}
	if hasChanged{
		s.saveCacheL2(db, rlsLockBalance, hash,number)
	}
	return nil
}

func(s *LockData) setBurnRatio(lock *PledgeItem,burnRatio *big.Int) {
	if burnRatio.Cmp(common.Big0)>0{
		if lock.BurnRatio==nil{
			lock.BurnAddress=common.BigToAddress(big.NewInt(0))
			lock.BurnRatio=burnRatio
		}else if lock.BurnRatio.Cmp(burnRatio)<0{
			lock.BurnRatio=burnRatio
		}
		if lock.BurnAmount==nil{
			lock.BurnAmount=common.Big0
		}
	}
}



func getRentCapity(storageItem *SPledge) *big.Int{
	totalRentCapity:=big.NewInt(0)
	for _,lease:=range storageItem.Lease {
		if lease.Deposit.Cmp(big.NewInt(0)) > 0 && lease.Status   == LeaseNormal{
			totalRentCapity=new(big.Int).Add(totalRentCapity,lease.Capacity)
		}
	}
	return totalRentCapity
}

func (s *LockData) fixStorageRevertRevenue(db ethdb.Database, hash common.Hash, number uint64) interface{} {
	rlsLockBalance,err:=s.loadRlsLockBalance(db)
	if err != nil {
		return err
	}
	for _,itemRlsLock:=range rlsLockBalance{
		lockBalance:=itemRlsLock.LockBalance
		for _,itemBlockLock:=range lockBalance{
			for _,itemWhichLock:=range itemBlockLock{
				if itemWhichLock.RevenueAddress!=itemWhichLock.TargetAddress{
					itemWhichLock.RevenueAddress=itemWhichLock.TargetAddress
				}
			}
		}
	}
	s.saveCacheL2(db, rlsLockBalance, hash,number)
	return nil
}


func (s *LockData) loadRlsLockBalance(db ethdb.Database) (map[common.Address]*RlsLockData , error) {
	rlsLockBalance := make(map[common.Address]*RlsLockData)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge2 := range pledge1 {
				items = append(items, pledge2)
			}
		}
	}

	s.appendRlsLockData(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return rlsLockBalance,err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return rlsLockBalance,err
	}
	s.appendRlsLockData(rlsLockBalance, items)
	return rlsLockBalance,nil
}

func (s *LockData) updatePosExitLockData(snap *Snapshot, item CandidatePEntrustExitRecord, headerNumber *big.Int) {
	s.updatePosEnExitLockData(snap, item.Amount,item.Address,item.Target, headerNumber)
}

func (s *LockData) updateAllLockData2(snap *Snapshot, isReward uint32, headerNumber *big.Int) {
	if isGEInitStorageManagerNumber(headerNumber.Uint64()){
		s.updateAllLockData3(snap, isReward, headerNumber)
		return
	}
	distribute:=make(map[common.Address]*big.Int)
	for target, flowRevenusTarget := range s.FlowRevenue {
		if 0 >= flowRevenusTarget.RewardBalance[isReward].Cmp(big.NewInt(0)) {
			continue
		}
		if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
			flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
		}
		lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
		// use reward release
		lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
		rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
		interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
		revenueAddress := target
		revenueContract := common.Address{}
		multiSignature := common.Address{}
		// singer reward
		if revenue, ok := snap.RevenueNormal[target]; ok {
			revenueAddress = revenue.RevenueAddress
		}else{
			if revenue2, ok2 := snap.PosPledge[target]; ok2 {
				revenueAddress = revenue2.Manager
			}
		}
		if _, ok := lockBalance[isReward]; !ok {
			lockBalance[isReward] = &PledgeItem{
				Amount:          big.NewInt(0),
				PledgeType:      isReward,
				Playment:        big.NewInt(0),
				LockPeriod:      lockPeriod,
				RlsPeriod:       rlsPeriod,
				Interval:        interval,
				StartHigh:       headerNumber.Uint64(),
				TargetAddress:   target,
				RevenueAddress:  revenueAddress,
				RevenueContract: revenueContract,
				MultiSignature:  multiSignature,
				BurnAddress: common.Address{},
				BurnRatio: common.Big0,
				BurnAmount: common.Big0,
			}
		}
		posAmount:=new(big.Int).Set(flowRevenusTarget.RewardBalance[isReward])
		if snap.PosPledge[target]!=nil&& len(snap.PosPledge[target].Detail)>0{
			posRateAmount:=new(big.Int).Mul(posAmount,snap.PosPledge[target].DisRate)
			posRateAmount=new(big.Int).Div(posRateAmount,posDistributionDefaultRate)
			posLeftAmount:=new(big.Int).Sub(posAmount,posRateAmount)
			if posLeftAmount.Cmp(common.Big0)>0{
				if _, ok2 := distribute[target]; ok2 {
					distribute[target]=new(big.Int).Add(distribute[target],posLeftAmount)
				}else{
					distribute[target]=new(big.Int).Set(posLeftAmount)
				}
			}
			if posRateAmount.Cmp(common.Big0)>0{
				lockBalance[isReward].Amount = new(big.Int).Add(lockBalance[isReward].Amount, posRateAmount)
			}
		}else{
			lockBalance[isReward].Amount = new(big.Int).Add(lockBalance[isReward].Amount, posAmount)
		}
		flowRevenusTarget.RewardBalance[isReward] = big.NewInt(0)
	}

	for miner,amount:=range distribute{
		details:=snap.PosPledge[miner].Detail
		totalAmount:=snap.PosPledge[miner].TotalAmount
		for _,item:=range details{
			entrustAmount:=new(big.Int).Mul(amount,item.Amount)
			entrustAmount=new(big.Int).Div(entrustAmount,totalAmount)
			s.updateDistributeLockData(snap,item.Address,miner,entrustAmount,headerNumber)
		}
	}
}

func (s *LockData) updateDistributeLockData(snap *Snapshot, entrustTarget common.Address, revenueContract common.Address,Amount *big.Int,headerNumber *big.Int) {
	if _, ok := s.FlowRevenue[entrustTarget]; !ok {
		s.FlowRevenue[entrustTarget] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
		}
	}
	itemIsReward:=uint32(sscEnumSignerReward)
	flowRevenusTarget := s.FlowRevenue[entrustTarget]
	if _, ok := flowRevenusTarget.RewardBalance[itemIsReward]; !ok {
		flowRevenusTarget.RewardBalance[itemIsReward] = new(big.Int).Set(Amount)
	} else {
		flowRevenusTarget.RewardBalance[itemIsReward] = new(big.Int).Add(flowRevenusTarget.RewardBalance[itemIsReward], Amount)
	}
	if 0 >= flowRevenusTarget.RewardBalance[itemIsReward].Cmp(common.Big0) {
		return
	}
	if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
		flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
	}
	lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
	// use reward release
	lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
	rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
	interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
	revenueAddress := entrustTarget
	multiSignature := common.Address{}

	if _, ok := lockBalance[itemIsReward]; !ok {
		lockBalance[itemIsReward] = &PledgeItem{
			Amount:          big.NewInt(0),
			PledgeType:      itemIsReward,
			Playment:        big.NewInt(0),
			LockPeriod:      lockPeriod,
			RlsPeriod:       rlsPeriod,
			Interval:        interval,
			StartHigh:       headerNumber.Uint64(),
			TargetAddress:   entrustTarget,
			RevenueAddress:  revenueAddress,
			RevenueContract: revenueContract,
			MultiSignature:  multiSignature,
			BurnAddress: common.Address{},
			BurnRatio: common.Big0,
			BurnAmount: common.Big0,
		}
	}
	lockBalance[itemIsReward].Amount = new(big.Int).Add(lockBalance[itemIsReward].Amount, flowRevenusTarget.RewardBalance[itemIsReward])
	flowRevenusTarget.RewardBalance[itemIsReward] = big.NewInt(0)
}


func (s *LockData) setRewardRemovePunish(pledge []common.Address, db ethdb.Database, hash common.Hash, number uint64) error {
	if isGEInitStorageManagerNumber(number){
		return s.setRewardRemovePunishV1(pledge,db,hash,number)
	}
	rlsLockBalance,err:=s.loadRlsLockBalance(db)
	if err != nil {
		return err
	}
	pledgeAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range pledge {
		pledgeAddrs[sPAddrs] = 1
	}
	hasChanged := false
	burnRatio:=new(big.Int).Set(BurnBase)
	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalance:=itemRlsLock.LockBalance
		if _, ok := pledgeAddrs[minerAddress]; ok {
			hasChanged=true
			for _,itemBlockLock:=range lockBalance{
				for _,itemWhichLock:=range itemBlockLock{
					s.setBurnRatio(itemWhichLock,burnRatio)
				}
			}
		}else{
			if isLtPosAutoExitPunishChange(number){
				for _,itemBlockLock:=range lockBalance{
					for _,itemWhichLock:=range itemBlockLock{
						if _, ok2 := pledgeAddrs[itemWhichLock.RevenueContract]; ok2 {
							hasChanged=true
							s.setBurnRatio(itemWhichLock,burnRatio)
						}
					}
				}
			}
		}
	}
	if hasChanged{
		s.saveCacheL2(db, rlsLockBalance, hash,number)
	}
	return nil
}
//sn entrust transfer
func (s *LockData) updateSTPEntrustTransferLockData(snap *Snapshot, item SETransferRecord, headerNumber *big.Int) {
	s.updateSTEntrustLock(snap, item.Address,item.LockAmount, headerNumber, item.Original)
}
//sn entrust exit
func (s *LockData) updateSTPEExitLockData(snap *Snapshot, item SEExitRecord, headerNumber *big.Int) {
	s.updateSTEntrustLock(snap, item.Address,item.Amount, headerNumber, item.Target)
}
//sn exit
func (s *LockData) updateSTPExitLockData(snap *Snapshot, item *SEntrustDetail, headerNumber *big.Int,target common.Address) {
	s.updateSTEntrustLock(snap, item.Address,item.Amount, headerNumber, target)
}

func (s *LockData) updateSTEntrustLock(snap *Snapshot, itemAddress common.Address,itemAmount *big.Int, headerNumber *big.Int, target common.Address) {
	currentLockReward:=LockRewardNewRecord{
		Target:itemAddress,
		Amount:new(big.Int).Set(itemAmount),
		IsReward:uint32(sscEnumSTEntrustExitLock),
		SourceAddress:target,
		RevenueAddress:itemAddress,
	}
	s.updateLockDataV1(snap, currentLockReward, headerNumber)
}

func (s *LockData) distributeSTPLockData(snap *Snapshot, item LockRewardRecord, number *big.Int,distribute map[common.Address]*big.Int,distributePool map[common.Hash]*big.Int,currentLockReward []LockRewardNewRecord, sscEnumReward int) []LockRewardNewRecord{
	stpAmount:=new(big.Int).Set(item.Amount)
	if se,ok:=snap.StorageData.StorageEntrust[item.Target];ok{
		if sp,ok2:=snap.SpData.PoolPledge[se.Sphash];ok2&&sp.Status==spStatusActive{
			spFeeAmount:=new(big.Int).Mul(stpAmount,new(big.Int).SetUint64(sp.Fee))
			spFeeAmount=new(big.Int).Div(spFeeAmount,big.NewInt(100))
			if sp.SnRatio.Cmp(common.Big0)>0&&spFeeAmount.Cmp(common.Big0)>0{
				preCapacity:=getCapacity(sp.TotalAmount)
				spFeeAmount=new(big.Int).Div(new(big.Int).Mul(spFeeAmount,preCapacity),sp.TotalCapacity)
				if _, ok3 := distributePool[se.Sphash]; ok3 {
					distributePool[se.Sphash]=new(big.Int).Add(distributePool[se.Sphash],spFeeAmount)
				}else{
					distributePool[se.Sphash]=new(big.Int).Set(spFeeAmount)
				}
			}
			stpAmount=new(big.Int).Sub(stpAmount,spFeeAmount)
		}
	}
	revenueAddress := item.Target
	if se, ok := snap.StorageData.StorageEntrust[item.Target]; ok {
		revenueAddress = se.Manager
	}
	if revenue, ok := snap.RevenueStorage[item.Target]; ok {
		revenueAddress = revenue.RevenueAddress
	}
	if stpAmount.Cmp(common.Big0)>0 {
		if snap.StorageData.StorageEntrust[item.Target] != nil && len(snap.StorageData.StorageEntrust[item.Target].Detail) > 0 {
			stpRateAmount := new(big.Int).Mul(stpAmount, snap.StorageData.StorageEntrust[item.Target].EntrustRate)
			stpRateAmount = new(big.Int).Div(stpRateAmount, sPDistributionDefaultRate)
			stpLeftAmount := new(big.Int).Sub(stpAmount, stpRateAmount)
			if stpLeftAmount.Cmp(common.Big0) > 0 {
				currentLockReward = append(currentLockReward, LockRewardNewRecord{
					Target:         item.Target,
					Amount:         new(big.Int).Set(stpLeftAmount),
					IsReward:       uint32(sscEnumReward),
					SourceAddress:  item.Target,
					RevenueAddress: revenueAddress,
				})
			}
			if stpRateAmount.Cmp(common.Big0) > 0 {
				if _, ok2 := distribute[item.Target]; ok2 {
					distribute[item.Target] = new(big.Int).Add(distribute[item.Target], stpRateAmount)
				} else {
					distribute[item.Target] = new(big.Int).Set(stpRateAmount)
				}
			}
		} else {
				currentLockReward = append(currentLockReward, LockRewardNewRecord{
					Target:         item.Target,
					Amount:         new(big.Int).Set(stpAmount),
					IsReward:       uint32(sscEnumReward),
					SourceAddress:  item.Target,
					RevenueAddress: revenueAddress,
				})
		}
	}
	return currentLockReward
}
func (s *LockData) updatePosTransferLockData(snap *Snapshot, item POSTransferRecord, headerNumber *big.Int) {
	s.updatePosEnExitLockData(snap, item.LockAmount,item.Address,item.Original, headerNumber)
}

func (s *LockData) updatePosEnExitLockData(snap *Snapshot, itemAmount *big.Int,itemAddress common.Address,itemTarget common.Address, headerNumber *big.Int) {
	if isGEInitStorageManagerNumber(headerNumber.Uint64()){
		itemNew:=LockRewardNewRecord{
			Target:itemAddress,
			Amount:new(big.Int).Set(itemAmount),
			IsReward:uint32(sscEnumPosExitLock),
			SourceAddress:itemTarget,
			RevenueAddress:itemAddress,
		}
		s.updateLockDataV1(snap,itemNew,headerNumber)
		return
	}
	if _, ok := s.FlowRevenue[itemAddress]; !ok {
		s.FlowRevenue[itemAddress] = &LockBalanceData{
			RewardBalance: make(map[uint32]*big.Int),
			LockBalance:   make(map[uint64]map[uint32]*PledgeItem),
		}
	}
	itemIsReward := uint32(sscEnumPosExitLock)
	flowRevenusTarget := s.FlowRevenue[itemAddress]
	if _, ok := flowRevenusTarget.RewardBalance[itemIsReward]; !ok {
		flowRevenusTarget.RewardBalance[itemIsReward] = new(big.Int).Set(itemAmount)
	} else {
		flowRevenusTarget.RewardBalance[itemIsReward] = new(big.Int).Add(flowRevenusTarget.RewardBalance[itemIsReward], itemAmount)
	}
	if _, ok := flowRevenusTarget.LockBalance[headerNumber.Uint64()]; !ok {
		flowRevenusTarget.LockBalance[headerNumber.Uint64()] = make(map[uint32]*PledgeItem)
	}
	lockBalance := flowRevenusTarget.LockBalance[headerNumber.Uint64()]
	lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
	rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
	interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
	revenueAddress := itemAddress
	revenueContract := itemTarget
	multiSignature := common.Address{}

	if _, ok := lockBalance[itemIsReward]; !ok {
		lockBalance[itemIsReward] = &PledgeItem{
			Amount:          big.NewInt(0),
			PledgeType:      itemIsReward,
			Playment:        big.NewInt(0),
			LockPeriod:      lockPeriod,
			RlsPeriod:       rlsPeriod,
			Interval:        interval,
			StartHigh:       headerNumber.Uint64(),
			TargetAddress:   itemAddress,
			RevenueAddress:  revenueAddress,
			RevenueContract: revenueContract,
			MultiSignature:  multiSignature,
			BurnAddress:     common.Address{},
			BurnRatio:       common.Big0,
			BurnAmount:      common.Big0,
		}
	}
	lockBalance[itemIsReward].Amount = new(big.Int).Add(lockBalance[itemIsReward].Amount, flowRevenusTarget.RewardBalance[itemIsReward])
	flowRevenusTarget.RewardBalance[itemIsReward] = big.NewInt(0)
}
func (s *LockData) distributeSpReward(snap *Snapshot,spRewardRecord map[common.Hash]*big.Int, headerNumber *big.Int){
	currentLockReward :=make([]LockRewardNewRecord,0)
	for spHash,amount:=range spRewardRecord {
		if sp, ok := snap.SpData.PoolPledge[spHash]; ok {
			spAddr := common.BigToAddress(spHash.Big())
			etAddrMap := calculateEtPledge(sp.EtDetail)
			revenueAddr := sp.Manager
			if sp.RevenueAddress!=common.BigToAddress(common.Big0){
				revenueAddr = sp.RevenueAddress
			}
			entrustReward := new(big.Int).Div(new(big.Int).Mul(amount, big.NewInt(int64(sp.EntrustRate))), big.NewInt(100))
			spReward := new(big.Int).Sub(amount, entrustReward)
			currentLockReward = append(currentLockReward, LockRewardNewRecord{
				Target:         spAddr,
				Amount:         spReward,
				IsReward:       uint32(sscSpLockReward),
				SourceAddress:  spAddr,
				RevenueAddress: revenueAddr,
			})
			if entrustReward.Cmp(common.Big0)>0 {
				for etAddress, etAmount := range etAddrMap {
					pledgeAmount:=	new(big.Int).Div(new(big.Int).Mul(entrustReward,etAmount), sp.TotalAmount)
					currentLockReward = append(currentLockReward, LockRewardNewRecord{
						Target:         etAddress,
						Amount:         pledgeAmount ,
						IsReward:       uint32(sscSpEntrustLockReward),
						SourceAddress:  spAddr,
						RevenueAddress: etAddress,
					})
				}
			}

		}
	}

	if len(currentLockReward)>0 {
		snap.FlowRevenue.updateLockDataV1(snap, currentLockReward, headerNumber)
	}
}
func (s *LockProfitSnap) updateLockDataV1(snap *Snapshot, LockReward []LockRewardNewRecord, headerNumber *big.Int) {
	for _, item := range LockReward {
		if sscEnumSignerReward == item.IsReward {
			s.RewardLock.addLockDataV1(item, headerNumber)
		} else if sscEnumFlwReward == item.IsReward {
			s.FlowLock.addLockDataV1(item, headerNumber)
		} else if sscEnumBandwidthReward == item.IsReward {
			s.BandwidthLock.addLockDataV1(item, headerNumber)
		} else if sscSpLockReward == item.IsReward {
			s.SpLock.addLockDataV1(item, headerNumber)
		} else if sscSpEntrustLockReward == item.IsReward {
			s.SpEntrustLock.addLockDataV1(item, headerNumber)
		}else if sscSpExitLockReward == item.IsReward {
			s.SpExitLock.updateLockDataV1(snap,item, headerNumber)
		}else if sscSpEntrustExitLockReward == item.IsReward {
			s.SpEntrustExitLock.updateLockDataV1(snap,item, headerNumber)
		}else if sscEnumSTEntrustLockReward == item.IsReward {
			s.STPEntrustLock.addLockDataV1(item, headerNumber)
		}
	}

}
func  (s *LockProfitSnap) updateAllLockDataNew(snap *Snapshot, headerNumber *big.Int){
	if isLockRewardNumber(headerNumber.Uint64(), snap.Period) {
		s.RewardLock.updateAllLockDataV1(snap, sscEnumSignerReward, headerNumber)
		s.FlowLock.updateAllLockDataV1(snap, sscEnumFlwReward, headerNumber)
		s.BandwidthLock.updateAllLockDataV1(snap, sscEnumBandwidthReward, headerNumber)
		s.SpLock.updateAllLockDataV1(snap, sscSpLockReward, headerNumber)
		s.SpEntrustLock.updateAllLockDataV1(snap, sscSpEntrustLockReward, headerNumber)
		s.STPEntrustLock.updateAllLockDataV1(snap, sscEnumSTEntrustLockReward,headerNumber)
	}
}
func (s *LockData) addLockDataV1(item LockRewardNewRecord, headerNumber *big.Int) {
	if _, ok := s.FlowRevenue[item.Target]; !ok {
		s.FlowRevenue[item.Target] = &LockBalanceData{
			RewardBalance:   make(map[uint32]*big.Int),
			LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
			RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
			LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
		}
	}
	flowRevenusTarget := s.FlowRevenue[item.Target]
	if _, ok := flowRevenusTarget.RewardBalanceV1[item.IsReward]; !ok {
		flowRevenusTarget.RewardBalanceV1[item.IsReward]=make(map[common.Address]*LockTmpData)
	}
	rewardBalance:=flowRevenusTarget.RewardBalanceV1[item.IsReward]
	if balance,ok1:=rewardBalance[item.SourceAddress];ok1{
		rewardBalance[item.SourceAddress].Amount=new(big.Int).Add(balance.Amount,item.Amount)
		rewardBalance[item.SourceAddress].RevenueAddress = item.RevenueAddress
	}else{
		rewardBalance[item.SourceAddress]=&LockTmpData{
			Amount:         new(big.Int).Set(item.Amount),
			RevenueAddress: item.RevenueAddress,
		}
	}

}
func (s *LockData) updateLockDataV1(snap *Snapshot, item LockRewardNewRecord, headerNumber *big.Int) {
	if _, ok := s.FlowRevenue[item.Target]; !ok {
		s.FlowRevenue[item.Target] = &LockBalanceData{
			RewardBalance:   make(map[uint32]*big.Int),
			LockBalance:     make(map[uint64]map[uint32]*PledgeItem),
			RewardBalanceV1: make(map[uint32]map[common.Address]*LockTmpData),
			LockBalanceV1:   make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
		}
	}
	lockNumber :=headerNumber.Uint64()
	flowRevenusTarget:= s.FlowRevenue[item.Target]
	if flowRevenusTarget.LockBalanceV1[lockNumber] == nil {
		flowRevenusTarget.LockBalanceV1[lockNumber]=make(map[uint32]map[common.Address]*PledgeItem,0)
	}
	lockBalanceV1 := flowRevenusTarget.LockBalanceV1[lockNumber]
	rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
	interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
	interval  = interval * utgLockRewardInterval
	if _,ok:=lockBalanceV1[item.IsReward];!ok {
		lockBalanceV1[item.IsReward] = make(map[common.Address]*PledgeItem, 0)
	}
	if _,ok:=lockBalanceV1[item.IsReward][item.SourceAddress];!ok {
		lockBalanceV1[item.IsReward][item.SourceAddress]=&PledgeItem{
			Amount:          item.Amount,
			PledgeType:      item.IsReward,
			Playment:        big.NewInt(0),
			LockPeriod:      0,
			RlsPeriod:       rlsPeriod,
			Interval:        interval,
			StartHigh:       lockNumber,
			TargetAddress:   item.Target,
			RevenueAddress:  item.RevenueAddress,
			RevenueContract: item.SourceAddress,
			MultiSignature:  common.Address{},
			BurnAddress:     common.Address{},
			BurnRatio:       common.Big0,
			BurnAmount:      common.Big0,
		}
	}else{
		lockBalanceV1[item.IsReward][item.SourceAddress].Amount=new(big.Int).Add(lockBalanceV1[item.IsReward][item.SourceAddress].Amount,item.Amount)
	}

}
func (s *LockData) updateAllLockDataV1(snap *Snapshot, isReward uint32, headerNumber *big.Int) {
	for target, flowRevenusTarget := range s.FlowRevenue {
		locktmpData := flowRevenusTarget.RewardBalanceV1[isReward]
		totalSize := len(locktmpData)
		if locktmpData == nil || totalSize == 0 {
			continue
		}
		// use reward release
		//lockPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].LockPeriod
		rlsPeriod := snap.SystemConfig.LockParameters[sscEnumRwdLock].RlsPeriod
		interval := snap.SystemConfig.LockParameters[sscEnumRwdLock].Interval
		interval  = interval * utgLockRewardInterval
		multiSignature := common.Address{}
		for nodeAddr, item := range locktmpData {
			revenueAddress := snap.getRevenueAddressByType(isReward, nodeAddr, item.RevenueAddress)
			lockNumber := headerNumber.Uint64()
			if flowRevenusTarget.LockBalanceV1[lockNumber] == nil {
				flowRevenusTarget.LockBalanceV1[lockNumber] = make(map[uint32]map[common.Address]*PledgeItem, 0)
			}
			lockBalanceV1 := flowRevenusTarget.LockBalanceV1[lockNumber]
			revenueContract:=nodeAddr
			if nodeAddr==target&&isReward!=sscEnumSTEntrustLockReward&&isReward!=sscEnumSTEntrustExitLock{
				revenueContract=common.Address{}
			}
			if _, ok := lockBalanceV1[isReward]; !ok {
				lockBalanceV1[isReward] = make(map[common.Address]*PledgeItem, 0)
			}
			if _, ok := lockBalanceV1[isReward][revenueContract]; !ok {
				lockBalanceV1[isReward][revenueContract] = &PledgeItem{
					Amount:          item.Amount,
					PledgeType:      isReward,
					Playment:        big.NewInt(0),
					LockPeriod:      0,
					RlsPeriod:       rlsPeriod,
					Interval:        interval,
					StartHigh:       lockNumber,
					TargetAddress:   target,
					RevenueAddress:  revenueAddress,
					RevenueContract: revenueContract,
					MultiSignature:  multiSignature,
					BurnAddress:     common.Address{},
					BurnRatio:       common.Big0,
					BurnAmount:      common.Big0,
				}
			}else{
				lockBalanceV1[isReward][revenueContract].Amount=new(big.Int).Add(lockBalanceV1[isReward][revenueContract].Amount,item.Amount)
			}

		}
		delete(flowRevenusTarget.RewardBalanceV1, isReward)
	}

}

func (snap *Snapshot) getRevenueAddressByType(isReward uint32, nodeAddr common.Address, defaultAddress common.Address) common.Address {
	if isReward == sscSpEntrustLockReward || isReward == sscEnumSTEntrustExitLock || isReward == sscEnumStoragePledgeRedeemLock || isReward == sscEnumPosExitLock {
		return defaultAddress
	}
	if isReward == sscEnumSignerReward {
		if item, ok := snap.RevenueNormal[nodeAddr]; ok {
			return item.RevenueAddress
		}
	}
	if isReward == sscEnumFlwReward || isReward == sscEnumBandwidthReward {
		if item, ok := snap.RevenueStorage[nodeAddr]; ok {
			return item.RevenueAddress
		}
	}

	return defaultAddress
}
func calculateEtPledge(etMap map[common.Hash]*EntrustDetail) map[common.Address]*big.Int {
	etAddrMap := make(map[common.Address]*big.Int, 0)
	for _, detail := range etMap {
		if _, ok := etAddrMap[detail.Address]; ok {
			etAddrMap[detail.Address] = new(big.Int).Add(etAddrMap[detail.Address], detail.Amount)
		} else {
			etAddrMap[detail.Address] = detail.Amount
		}
	}
	return etAddrMap
}

func (s *LockData) updateAllLockData3(snap *Snapshot, isReward uint32, headerNumber *big.Int) {
	currentLockReward:=make([]LockRewardNewRecord,0)

	distribute:=make(map[common.Address]*big.Int)
	for target, flowRevenusTarget := range s.FlowRevenue {
		if 0 >= flowRevenusTarget.RewardBalance[isReward].Cmp(big.NewInt(0)) {
			continue
		}
		revenueAddress := target
		if revenue, ok := snap.RevenueNormal[target]; ok {
			revenueAddress = revenue.RevenueAddress
		}else{
			if revenue2, ok2 := snap.PosPledge[target]; ok2 {
				revenueAddress = revenue2.Manager
			}
		}
		posAmount:=new(big.Int).Set(flowRevenusTarget.RewardBalance[isReward])
		if snap.PosPledge[target]!=nil&& len(snap.PosPledge[target].Detail)>0{
			posRateAmount:=new(big.Int).Mul(posAmount,snap.PosPledge[target].DisRate)
			posRateAmount=new(big.Int).Div(posRateAmount,posDistributionDefaultRate)
			posLeftAmount:=new(big.Int).Sub(posAmount,posRateAmount)
			if posLeftAmount.Cmp(common.Big0)>0{
				if _, ok2 := distribute[target]; ok2 {
					distribute[target]=new(big.Int).Add(distribute[target],posLeftAmount)
				}else{
					distribute[target]=new(big.Int).Set(posLeftAmount)
				}
			}
			if posRateAmount.Cmp(common.Big0)>0{
				currentLockReward=append(currentLockReward,LockRewardNewRecord{
					Target:target,
					Amount:new(big.Int).Set(posRateAmount),
					IsReward:uint32(isReward),
					SourceAddress:target,
					RevenueAddress:revenueAddress,
				})
			}
		}else{
			currentLockReward=append(currentLockReward,LockRewardNewRecord{
				Target:target,
				Amount:new(big.Int).Set(posAmount),
				IsReward:uint32(isReward),
				SourceAddress:target,
				RevenueAddress:revenueAddress,
			})
		}
		flowRevenusTarget.RewardBalance[isReward] = big.NewInt(0)
	}

	for miner,amount:=range distribute{
		details:=snap.PosPledge[miner].Detail
		totalAmount:=snap.PosPledge[miner].TotalAmount
		for _,item:=range details{
			entrustAmount:=new(big.Int).Mul(amount,item.Amount)
			entrustAmount=new(big.Int).Div(entrustAmount,totalAmount)
			currentLockReward=append(currentLockReward,LockRewardNewRecord{
				Target:item.Address,
				Amount:new(big.Int).Set(entrustAmount),
				IsReward:uint32(isReward),
				SourceAddress:miner,
				RevenueAddress:item.Address,
			})
		}
	}
	snap.FlowRevenue.updateLockDataV1(snap, currentLockReward, headerNumber)
}
func (s *LockData) setSpIllegalLockPunish(burnSpMap map[common.Address]map[common.Address]uint64, db ethdb.Database, hash common.Hash, number uint64,isReward uint32) interface{} {
	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)

	items := []*PledgeItem{}
	for target,sourceItem:=range burnSpMap{
		if  lockBalanceData,ok:=s.FlowRevenue[target];ok{
			if balanceMap,ok1:=lockBalanceData.RewardBalanceV1[isReward];ok1{
				for delSource,_:=range sourceItem{
					delete(balanceMap, delSource)
				}
			}
		}
	}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge2 := range pledge1 {
				items = append(items, pledge2)
			}
		}
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledge := range pledgeV1Item {
						items = append(items, pledge)
					}
				}
			}
		}
	}

	s.appendRlsLockDataV1(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	hasChanged := false
	addr0:=common.BigToAddress(common.Big0)
	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalanceV1:=itemRlsLock.LockBalanceV1
		if spMap, ok := burnSpMap[minerAddress]; ok {
			burnRatio:=new(big.Int).Set(BurnBase)
			for _,itemBlockLock:=range lockBalanceV1{
				for _,itemWhichLockSource:=range itemBlockLock{
					for _,itemWhichLock:=range itemWhichLockSource{
						if itemWhichLock.RevenueContract!=addr0 {
							if _,ok1:=spMap[itemWhichLock.RevenueContract];ok1 {
								hasChanged=true
								s.setBurnRatio(itemWhichLock,burnRatio)
							}
						}else if isReward==uint32(sscSpLockReward){
							hasChanged=true
							s.setBurnRatio(itemWhichLock,burnRatio)
						}
					}
				}
			}
		}
	}
	if hasChanged{
		s.saveCacheL2V1(db, rlsLockBalance, hash,number)
	}
	return nil
}

func (s *LockData) setStorageRemovePunish2(pledge []common.Address, db ethdb.Database, hash common.Hash, number uint64,isReward uint32) interface{}{
	for _,target:=range pledge{
		if  lockBalanceData,ok:=s.FlowRevenue[target];ok{
			if balanceMap,ok1:=lockBalanceData.RewardBalanceV1[isReward];ok1{
				delete(balanceMap, target)
			}
		}
	}
	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge2 := range pledge1 {
				items = append(items, pledge2)
			}
		}
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledgeItem := range pledgeV1Item {
						items = append(items, pledgeItem)
					}
				}
			}
		}
	}

	s.appendRlsLockDataV1(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	pledgeAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range pledge {
		pledgeAddrs[sPAddrs] = 1
	}
	hasChanged := false
	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalanceV1:=itemRlsLock.LockBalanceV1
		if _, ok := pledgeAddrs[minerAddress]; ok {
			hasChanged=true
			burnRatio:=new(big.Int).Set(BurnBase)
			for _,itemBlockLock:=range lockBalanceV1{
				for _,itemWhichLockSource:=range itemBlockLock{
					for _,itemWhichLock:=range itemWhichLockSource{
						s.setBurnRatio(itemWhichLock,burnRatio)
					}
				}

			}
		}
	}
	if hasChanged{
		s.saveCacheL2V1(db, rlsLockBalance, hash,number)
	}
	return nil
}

func (s *LockData) updateGrantProfit2(grantProfit []consensus.GrantProfitRecord, db ethdb.Database, hash common.Hash,number uint64) error {

	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledge := range pledgeV1Item {
						items = append(items, pledge)
					}
				}
			}
			pledges.LockBalanceV1 = make(map[uint64]map[uint32]map[common.Address]*PledgeItem)
		}
	}

	s.appendRlsLockDataV1(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	hasChanged := false
	for _, item := range grantProfit {
		if 0 != item.BlockNumber {
			if _, ok := rlsLockBalance[item.MinerAddress]; ok {
				if _, ok = rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber]; ok {
					if pledgeV1Item, ok := rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber][item.Which]; ok {
						if pledge, ok := pledgeV1Item[item.RevenueContract]; ok {
							pledge.Playment = new(big.Int).Add(pledge.Playment, item.Amount)
							burnAmount:=calBurnAmount(pledge,item.Amount)
							if burnAmount.Cmp(common.Big0)>0{
								pledge.BurnAmount= new(big.Int).Add(pledge.BurnAmount,burnAmount)
							}
							hasChanged = true
							if 0 <= pledge.Playment.Cmp(pledge.Amount) {
								delete(rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber][item.Which], item.RevenueContract)
								if 0 >= len(rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber][item.Which]) {
									delete(rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber], item.Which)
									if 0 >= len(rlsLockBalance[item.MinerAddress].LockBalanceV1[item.BlockNumber]) {
										delete(rlsLockBalance[item.MinerAddress].LockBalanceV1, item.BlockNumber)
										if 0 >= len(rlsLockBalance[item.MinerAddress].LockBalanceV1) {
											delete(rlsLockBalance, item.MinerAddress)
										}
									}
								}
							}
						}

					}
				}
			}
		}
	}
	if hasChanged {
		s.saveCacheL2V1(db, rlsLockBalance, hash,number)
	}
	return nil
}

func (l *LockData) appendRlsLockDataV1(rlsLockBalance map[common.Address]*RlsLockDataV1, items []*PledgeItem) {
	for _, item := range items {
		if _, ok := rlsLockBalance[item.TargetAddress]; !ok {
			rlsLockBalance[item.TargetAddress] = &RlsLockDataV1{
				LockBalanceV1: make(map[uint64]map[uint32]map[common.Address]*PledgeItem),
			}
		}
		flowRevenusTarget := rlsLockBalance[item.TargetAddress]
		if _, ok := flowRevenusTarget.LockBalanceV1[item.StartHigh]; !ok {
			flowRevenusTarget.LockBalanceV1[item.StartHigh] = make(map[uint32]map[common.Address]*PledgeItem)
		}
		lockBalanceV1 := flowRevenusTarget.LockBalanceV1[item.StartHigh]
		if _,ok:=lockBalanceV1[item.PledgeType];!ok{
			lockBalanceV1[item.PledgeType]=make(map[common.Address]*PledgeItem)
		}
		lockBalanceV1[item.PledgeType][item.RevenueContract] = item
	}
}

func (s *LockData) saveCacheL2V1(db ethdb.Database, rlsLockBalance map[common.Address]*RlsLockDataV1, hash common.Hash, number uint64) error{
	items := []*PledgeItem{}
	for _, pledges := range rlsLockBalance {
		for _, pledge1 := range pledges.LockBalanceV1 {
			for _, pledgeV1Item := range pledge1 {
				for _, pledge := range pledgeV1Item {
					items = append(items, pledge)
				}
			}
		}
	}
	err, buf := PledgeItemEncodeRlp(items)
	if err != nil {
		return err
	}
	err = db.Put(append([]byte("alien-"+s.Locktype+"-l2-"), hash[:]...), buf)
	if err != nil {
		return err
	}
	for _, pledges := range s.FlowRevenue {
		pledges.LockBalance = make(map[uint64]map[uint32]*PledgeItem)
		pledges.LockBalanceV1 = make(map[uint64]map[uint32]map[common.Address]*PledgeItem)
	}
	s.CacheL1 = []common.Hash{}
	s.CacheL2 = hash
	log.Info("LockProfitSnap saveCacheL2", "Locktype", s.Locktype, "cache hash", hash, "len", len(items),"number",number)
	return nil
}


func (s *LockData) calPayProfitV1(db ethdb.Database,playGrantProfit []consensus.GrantProfitRecord, header *types.Header) ([]consensus.GrantProfitRecord, error) {
	timeNow := time.Now()

	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)
	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge := range pledge1 {
				items = append(items, pledge)
			}
		}
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledge := range pledgeV1Item {
						items = append(items, pledge)
					}
				}
			}
		}
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return playGrantProfit, err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return playGrantProfit, err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	log.Info("calPayProfit load from disk", "Locktype", s.Locktype, "len(rlsLockBalance)", len(rlsLockBalance), "elapsed", time.Since(timeNow), "number", header.Number.Uint64())

	for address, items := range rlsLockBalance {
		for blockNumber, itemType := range items.LockBalanceV1 {
			for which, itemSource := range itemType {
				for _, item := range itemSource {
					amount := calPaymentPledge( item, header)
					if nil!= amount {
						playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
							Which:           which,
							MinerAddress:    address,
							BlockNumber:     blockNumber,
							Amount:          new(big.Int).Set(amount),
							RevenueAddress:  item.RevenueAddress,
							RevenueContract: item.RevenueContract,
							MultiSignature:  item.MultiSignature,
						})
					}
				}
			}
		}
	}
	log.Info("calPayProfit ", "Locktype", s.Locktype, "elapsed", time.Since(timeNow), "number", header.Number.Uint64())
	return playGrantProfit, nil
}


func (s *LockData) setRewardRemovePunishV1(pledge []common.Address, db ethdb.Database, hash common.Hash, number uint64) error {
	rlsLockBalance,err:=s.loadRlsLockBalanceV1(db)
	if err != nil {
		return err
	}
	pledgeAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range pledge {
		pledgeAddrs[sPAddrs] = 1
	}
	hasChanged := false
	burnRatio:=new(big.Int).Set(BurnBase)
	for minerAddress,itemRlsLock:=range rlsLockBalance{
		lockBalanceV1:=itemRlsLock.LockBalanceV1
		if _, ok := pledgeAddrs[minerAddress]; ok {
			hasChanged=true
			for _,itemBlockLock:=range lockBalanceV1{
				for _,itemWhichLockType:=range itemBlockLock{
					for _,itemWhichLock:=range itemWhichLockType{
						s.setBurnRatio(itemWhichLock,burnRatio)
					}
				}
			}
		}
	}
	if hasChanged{
		s.saveCacheL2V1(db, rlsLockBalance, hash,number)
	}
	return nil
}


func (s *LockData) loadRlsLockBalanceV1(db ethdb.Database) (map[common.Address]*RlsLockDataV1 , error) {
	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)

	items := []*PledgeItem{}
	for _, pledges := range s.FlowRevenue {
		for _, pledge1 := range pledges.LockBalance {
			for _, pledge2 := range pledge1 {
				items = append(items, pledge2)
			}
		}
		if pledges.LockBalanceV1!=nil{
			for _, pledgeV1 := range pledges.LockBalanceV1 {
				for _, pledgeV1Item := range pledgeV1 {
					for _, pledge := range pledgeV1Item {
						items = append(items, pledge)
					}
				}
			}
		}
	}

	s.appendRlsLockDataV1(rlsLockBalance, items)

	items, err := s.loadCacheL1(db)
	if err != nil {
		return rlsLockBalance,err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return rlsLockBalance,err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	return rlsLockBalance,nil
}

func (s *LockData) payProfitV1(hash common.Hash, db ethdb.Database, period uint64, headerNumber uint64, currentGrantProfit []consensus.GrantProfitRecord, playGrantProfit []consensus.GrantProfitRecord, header *types.Header, state *state.StateDB, payAddressAll map[common.Address]*big.Int) ([]consensus.GrantProfitRecord, []consensus.GrantProfitRecord, error) {
	timeNow := time.Now()
	rlsLockBalance := make(map[common.Address]*RlsLockDataV1)
	err := s.saveCacheL1(db, hash)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	items, err := s.loadCacheL1(db)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)
	items, err = s.loadCacheL2(db)
	if err != nil {
		return currentGrantProfit, playGrantProfit, err
	}
	s.appendRlsLockDataV1(rlsLockBalance, items)

	log.Info("payProfit load from disk", "Locktype", s.Locktype, "len(rlsLockBalance)", len(rlsLockBalance), "elapsed", time.Since(timeNow), "number", header.Number.Uint64())

	for address, items := range rlsLockBalance {
		for blockNumber, item1 := range items.LockBalanceV1 {
			for which, itemSource := range item1 {
				for _, item := range itemSource {
					result, amount := paymentPledge(true, item, state, header, payAddressAll)
					if 0 == result {
						playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
							Which:           which,
							MinerAddress:    address,
							BlockNumber:     blockNumber,
							Amount:          new(big.Int).Set(amount),
							RevenueAddress:  item.RevenueAddress,
							RevenueContract: item.RevenueContract,
							MultiSignature:  item.MultiSignature,
						})
					} else if 1 == result {
						currentGrantProfit = append(currentGrantProfit, consensus.GrantProfitRecord{
							Which:           which,
							MinerAddress:    address,
							BlockNumber:     blockNumber,
							Amount:          new(big.Int).Set(amount),
							RevenueAddress:  item.RevenueAddress,
							RevenueContract: item.RevenueContract,
							MultiSignature:  item.MultiSignature,
						})
					}
				}
			}
		}
	}
	log.Info("payProfit ", "Locktype", s.Locktype, "elapsed", time.Since(timeNow), "number", header.Number.Uint64())
	return currentGrantProfit, playGrantProfit, nil
}