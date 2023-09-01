package alien

import (
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/consensus"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/shopspring/decimal"
	"math/big"
	"sort"
	"strconv"
)

const (
	spStatusInactive    = uint64(0)
	spStatusActive      = uint64(1)
	spStatusExited      = uint64(2)
	spStatusIllegalExit = uint64(3)

	spEntrustTypePledge   = 1
	spEntrustTypeTransfer = 2
	spEntrustTypeExit     = 3

	TargetTypePos = "PoS"
	TargetTypeSn  = "SN"
	TargetTypeSp  = "SP"

	applySpPledge            = "addsp"
	spCreateParamLen         = 7
	spCreatePledgeIndex      = 3
	spCreateFeeIndex         = 4
	spCreateEntrustRateIndex = 5
	spCreateRevenueIndex     = 6

	adJustPledge           = "spchpg"
	adJustParamSpHashIndex = 3
	adJustParamAmountIndex = 4

	spRemoveSn          = "spremovesn"
	spRemoveSpHashIndex = 3
	spRemoveSnAddrIndex = 4

	spEntrustPledge        = "spwtpg"
	spEntrustPgSpHashIndex = 3
	spEntrustPgAmountIndex = 4

	spEntrustTransferPledge       = "spwtfd"
	spEntrustTransferSpIndex      = 3
	spEntrustTransferTypeIndex    = 4
	spEntrustTransferAddressIndex = 5

	spEntrustExitPledge      = "spwtexit"
	spEntrustExitSpHashIndex = 3
	spEntrustExitEtHashIndex = 4

	spExitPledge      = "spexit"
	spExitSpHashIndex = 3

	spSetFee            = "spfee"
	spFeeSetSpHashIndex = 3
	spFeeSetFeeIndex    = 4

	spSetEntrustRate         = "spetrate"
	spEntrustRateSpHashIndex = 3
	spEntrustRateIndex       = 4

	spReveneBind         = "sprvebind"
	spBindSpHashIndex = 3
	spBindTypeIndex       = 4
	spBindReveAddrIndex       = 5
)

var (
	utgOneValue         = big.NewInt(1e+18)
	stockSnRatioMin     = decimal.NewFromFloat(0.83)
	stockSnNumMin       = uint64(5)
	SnDefaultRatioDigit = decimal.NewFromInt(1000000)
	spPledgeMinDay      = big.NewInt(90)
	spSpacePgPrice      = new(big.Int).Mul(big.NewInt(1e+16), big.NewInt(125))
	spMinPledgeAmount   = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(625))
	capacityOneTb       = big.NewInt(1099511627776)
	spEntrustMinDay     = big.NewInt(7)
)

type SpData struct {
	PoolPledge map[common.Hash]*PoolPledge `json:"poolpledge"`
	Hash       common.Hash                 `json:"validhash"`
}

/*
*
Storage pledge struct
*/
type PoolPledge struct {
	Address        common.Address                 `json:"address"`
	Manager        common.Address                 `json:"manager"`
	Number         *big.Int                       `json:"number"`
	TotalAmount    *big.Int                       `json:"totalAmount"`
	TotalCapacity  *big.Int                       `json:"totalcapacity"`
	UsedCapacity   *big.Int                       `json:"usedcapacity"`
	PunishNumber   *big.Int                       `json:"punishNumber"`
	SnRatio        *big.Int                       `json:"snRatio"`
	RevenueAddress common.Address                 `json:"revenueAddress"`
	ManagerAmount  *big.Int                       `json:"managerAmount"`
	Fee            uint64                         `json:"fee"`
	EntrustRate    uint64                         `json:"entrustRate"`
	Status         uint64                         `json:"status"`
	EtDetail       map[common.Hash]*EntrustDetail `json:"entrustDetail"`
	Hash           common.Hash                    `json:"validHash"`
}
type EntrustDetail struct {
	Address common.Address `json:"address"`
	Height  *big.Int       `json:"height"`
	Amount  *big.Int       `json:"amount"`
	Hash    common.Hash    `json:"validHash"`
}
type tempSpData struct {
	manager  common.Address
	nodeNum  uint64
	capacity *big.Int
	ratio    decimal.Decimal
	snlist   []common.Address
}
type SpApplyRecord struct {
	Hash           common.Hash
	Manager        common.Address
	RevenueAddress common.Address
	PledgeAmount   *big.Int
	Capacity       *big.Int
	Fee            uint64
	EntrustRate    uint64
	PledgeHash     common.Hash
}
type SpAdjustPledgeRecord struct {
	Hash         common.Hash
	PledgeAmount *big.Int
	EtHash       common.Hash
}
type SpRemoveSnRecord struct {
	Hash    common.Hash
	Address common.Address
}
type SpEntrustPledgeRecord struct {
	Hash          common.Hash
	Address       common.Address
	PledgeAmount  *big.Int
	Capacity      *big.Int
	PledgeHash    common.Hash
	SpType        uint64 //1 EntrustPledge  2 move 3 exit
	TargetType    string
	TargetAddress common.Address
	TargetHash    common.Hash
	//DelEtHash     []common.Hash
	LockAmount *big.Int
}
type SpFeeRecord struct {
	Hash common.Hash
	Fee  uint64
}
type SpEntrustRateRecord struct {
	Hash        common.Hash
	EntrustRate uint64
}
type SpBindRecord struct {
	Hash   common.Hash
	RevenueAddress common.Address
	Bind  bool
}

func NewSPSnap() *SpData {
	return &SpData{
		PoolPledge: make(map[common.Hash]*PoolPledge),
	}
}
func (s *SpData) copy() *SpData {
	clone := &SpData{
		PoolPledge: make(map[common.Hash]*PoolPledge),
		Hash:       s.Hash,
	}
	for address, spool := range s.PoolPledge {
		clone.PoolPledge[address] = &PoolPledge{
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
			EtDetail:       make(map[common.Hash]*EntrustDetail, 0),
			Hash:           spool.Hash,
		}

		if len(spool.EtDetail) > 0 {
			for hash, detail := range spool.EtDetail {
				clone.PoolPledge[address].EtDetail[hash] = &EntrustDetail{
					Address: detail.Address,
					Height:  new(big.Int).Set(detail.Height),
					Amount:  new(big.Int).Set(detail.Amount),
					Hash:    detail.Hash,
				}
			}
		}
	}
	return clone
}
func (a *Alien) processSPCustomTx(txDataInfo []string, headerExtra HeaderExtra, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, snapCache *Snapshot, number *big.Int, state *state.StateDB, chain consensus.ChainHeaderReader) HeaderExtra {
	if txDataInfo[posCategory] == applySpPledge {
		headerExtra.SpCreateParamter = a.spApplyPledge(headerExtra.SpCreateParamter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == adJustPledge {
		headerExtra.SpAdjustPgParamter = a.spAdJustPledge(headerExtra.SpAdjustPgParamter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spRemoveSn {
		headerExtra.SpRemoveSnParamter = a.spRemoveSn(headerExtra.SpRemoveSnParamter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spEntrustPledge {
		headerExtra.SpEttPledgeParamter = a.spEntrustPledge(headerExtra.SpEttPledgeParamter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}

	if txDataInfo[posCategory] == spEntrustTransferPledge {
		headerExtra.SpEttPledgeParamter= a.spEntrustTransferPledge(headerExtra.SpEttPledgeParamter,  txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spEntrustExitPledge {
		headerExtra.SpEttPledgeParamter = a.spEntrustExitPledge(headerExtra.SpEttPledgeParamter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spExitPledge {
		headerExtra.SpExitParameter = a.spExitPledge(headerExtra.SpExitParameter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spSetFee {
		headerExtra.SpFeeParameter = a.spSetFee(headerExtra.SpFeeParameter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spSetEntrustRate {
		headerExtra.SpEntrustParameter = a.spSetEntrustRate(headerExtra.SpEntrustParameter, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	}
	if txDataInfo[posCategory] == spReveneBind {
		headerExtra.SpBind = a.processSpBind(headerExtra.SpBind, txDataInfo, txSender, tx, receipts, snapCache, number.Uint64())
	}

	return headerExtra
}
func (snap *Snapshot) sPApply(headerExtra HeaderExtra, header *types.Header, db ethdb.Database) (*Snapshot, error) {
	snap.updateSpApplyData(headerExtra.SpCreateParamter, db, header.Number)
	snap.updateSpAdJustPledgeData(headerExtra.SpAdjustPgParamter, db, header.Number)
	snap.updateSpRemoveSnData(headerExtra.SpRemoveSnParamter, db, header.Number)
	snap.updateSpEntrustPledgeData(headerExtra.SpEttPledgeParamter, db, header.Number)
	snap.updateSpEntrustTransferData(headerExtra.SpEttPledgeParamter, db, header.Number)
	snap.updateSpEntrustExitData(headerExtra.SpEttPledgeParamter, db, header.Number)
	snap.updateSpExitPledgeData(headerExtra.SpExitParameter, db, header.Number)
	snap.updateSpFeeData(headerExtra.SpFeeParameter, db, header.Number)
	snap.updateSpEntrustRateData(headerExtra.SpEntrustParameter, db, header.Number)
	snap.updateSpBindData(headerExtra.SpBind, db, header.Number)
	return snap, nil
}

func (snap *Snapshot) initSpData(number uint64) {
	storageRatios := make(map[common.Address]*tempSpData, 0)
	for pledgeAddr, sPledge := range snap.StorageData.StoragePledge {
		if revenue, ok := snap.RevenueStorage[pledgeAddr]; ok {
			if _, ok2 := storageRatios[revenue.RevenueAddress]; !ok2 {
				storageRatios[revenue.RevenueAddress] = &tempSpData{
					manager:  revenue.RevenueAddress,
					nodeNum:  1,
					capacity: new(big.Int).Set(sPledge.TotalCapacity),
					ratio:    decimal.NewFromInt(0),
					snlist:   []common.Address{pledgeAddr},
				}
			} else {
				storageRatios[revenue.RevenueAddress].capacity = new(big.Int).Add(storageRatios[revenue.RevenueAddress].capacity, sPledge.TotalCapacity)
				storageRatios[revenue.RevenueAddress].nodeNum = storageRatios[revenue.RevenueAddress].nodeNum + 1
				storageRatios[revenue.RevenueAddress].snlist = append(storageRatios[revenue.RevenueAddress].snlist, pledgeAddr)
			}
		}
	}
	if len(storageRatios) > 0 {
		for manager, ratio := range storageRatios {
			snRatio := snap.StorageData.calStorageRatio(ratio.capacity, number)
			spHash := manager.Hash()
			if ratio.nodeNum >= stockSnNumMin || snRatio.Cmp(stockSnRatioMin) > 0 {
				snap.SpData.PoolPledge[spHash] = &PoolPledge{
					Address:        ratio.manager,
					Manager:        ratio.manager,
					Number:         big.NewInt(initStorageManagerNumber - 1),
					TotalAmount:    common.Big0,
					TotalCapacity:  new(big.Int).Set(ratio.capacity),
					UsedCapacity:   new(big.Int).Set(ratio.capacity),
					PunishNumber:   common.Big0,
					SnRatio:        snRatio.Mul(SnDefaultRatioDigit).BigInt(),
					RevenueAddress: ratio.manager,
					ManagerAmount:  new(big.Int).SetInt64(0),
					Fee:            0,
					EntrustRate:    0,
					Status:         spStatusInactive,
					EtDetail:       make(map[common.Hash]*EntrustDetail, 0),
				}
				for _, snAddr := range ratio.snlist {
					if _, ok := snap.StorageData.StoragePledge[snAddr]; ok {
						if _, ok1 := snap.StorageData.StorageEntrust[snAddr]; ok1 {
							snap.StorageData.StorageEntrust[snAddr].Sphash = spHash
							snap.StorageData.StorageEntrust[snAddr].Spheight = big.NewInt(initStorageManagerNumber - 1)
						}
					}
				}

				snap.SpData.accumulateSpPledgelHash(spHash, false)
			}
		}
		snap.SpData.accumulateSpDataHash()
	}
}

func (s *SpData) accumulateEntrustDetailHash(sphash common.Hash, hash common.Hash, accumulateAll bool) common.Hash {
	detail := s.PoolPledge[sphash].EtDetail[hash]
	data := changeOxToUx(detail.Address.String()) + detail.Amount.String() + detail.Height.String()
	detail.Hash = getHash(data)
	if accumulateAll {
		s.accumulateSpPledgelHash(sphash, accumulateAll)
	}

	return detail.Hash
}
func (s *SpData) accumulateSpPledgelHash(sphash common.Hash, accumulateAll bool) common.Hash {
	var dataArr []string
	spPledge := s.PoolPledge[sphash]
	for _, detail := range spPledge.EtDetail {
		dataArr = append(dataArr, detail.Hash.String())
	}
	dataArr = append(dataArr, changeOxToUx(spPledge.Address.String())+
		spPledge.Manager.String()+
		spPledge.Number.String()+
		spPledge.TotalAmount.String()+
		spPledge.TotalCapacity.String()+
		spPledge.UsedCapacity.String()+
		spPledge.SnRatio.String()+
		spPledge.RevenueAddress.String()+
		spPledge.ManagerAmount.String()+
		strconv.FormatUint(spPledge.Fee, 10)+
		strconv.FormatUint(spPledge.EntrustRate, 10)+
		strconv.FormatUint(spPledge.Status, 10))
	sort.Strings(dataArr)
	spPledge.Hash = getHash(dataArr)
	if accumulateAll {
		s.accumulateSpDataHash()
	}
	return spPledge.Hash
}
func (s *SpData) accumulateSpDataHash() common.Hash {
	var dataArr []string
	for spHash, spPledge := range s.PoolPledge {
		dataArr = append(dataArr, spPledge.Hash.String()+spHash.String())
	}
	sort.Strings(dataArr)
	s.Hash = getHash(dataArr)
	return s.Hash
}

func (a *Alien) spApplyPledge(spCreateParameter []SpApplyRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blockNumber *big.Int, chain consensus.ChainHeaderReader) []SpApplyRecord {
	if len(txDataInfo) < spCreateParamLen-1 {
		log.Warn("spApplyPledge", "parameter error len=", len(txDataInfo))
		return spCreateParameter
	}
	spParamter := SpApplyRecord{
		Hash:         tx.Hash(),
		Manager:      txSender,
		PledgeAmount: big.NewInt(0),
		Capacity:     big.NewInt(0),
		Fee:          uint64(0),
		EntrustRate:  uint64(0),
		PledgeHash:   tx.Hash(),
        RevenueAddress: common.Address{},
	}

	if pledgeAmount, err := decimal.NewFromString(txDataInfo[spCreatePledgeIndex]); err != nil {
		log.Warn("spApplyPledge", "pledgeAmount error", txDataInfo[spCreatePledgeIndex])
		return spCreateParameter
	} else if pledgeAmount.BigInt().Cmp(spMinPledgeAmount) < 0 {
		log.Warn("spApplyPledge", "Insufficient pledgeAmount", pledgeAmount)
		return spCreateParameter
	} else {
		spParamter.PledgeAmount = pledgeAmount.BigInt()
	}
	spParamter.Capacity = getCapacity(spParamter.PledgeAmount)
	if fee, err := strconv.Atoi(txDataInfo[spCreateFeeIndex]); err != nil {
		log.Warn("spApplyPledge", "fee format error", txDataInfo[spCreateFeeIndex])
		return spCreateParameter
	} else if fee < 0 ||fee > 100 {
		log.Warn("spApplyPledge", "fee < 0 or fee > 100", fee)
		return spCreateParameter
	} else {
		spParamter.Fee = uint64(fee)
	}
	if entrustRate, err := strconv.Atoi(txDataInfo[spCreateEntrustRateIndex]); err != nil {
		log.Warn("spApplyPledge", "EntrustRate format error", txDataInfo[spCreateEntrustRateIndex])
		return spCreateParameter
	} else if entrustRate < 0 ||entrustRate > 100{
		log.Warn("spApplyPledge", "EntrustRate< 0 or entrustRate > 100", entrustRate)
		return spCreateParameter
	} else {
		spParamter.EntrustRate = uint64(entrustRate)
	}
	if len(txDataInfo) > spCreateRevenueIndex {
		if err := spParamter.RevenueAddress.UnmarshalText1([]byte(txDataInfo[spCreateRevenueIndex])); err != nil {
			log.Warn("spApplyPledge", "RevenueAddress error", txDataInfo[spCreateRevenueIndex])
			return spCreateParameter
		}
	}
	if state.GetBalance(txSender).Cmp(spParamter.PledgeAmount) < 0 {
		log.Warn("spApplyPledge", "balance", state.GetBalance(txSender), "need pay", spParamter.PledgeAmount)
		return spCreateParameter
	}
	state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), spParamter.PledgeAmount))
	spCreateParameter = append(spCreateParameter, spParamter)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c2f958"))
	topics[1].SetBytes(spParamter.Capacity.Bytes())
	topics[2].SetBytes(spParamter.PledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return spCreateParameter
}
func (s *Snapshot) updateSpApplyData(pledgeRecord []SpApplyRecord, db ethdb.Database, number *big.Int) {
	if len(pledgeRecord) == 0 {
		return
	}
	for _, record := range pledgeRecord {
		snRatio := s.StorageData.calStorageRatio(record.Capacity, number.Uint64())
		s.SpData.PoolPledge[record.Hash] = &PoolPledge{
			Address:        record.Manager,
			Manager:        record.Manager,
			Number:         new(big.Int).Set(number),
			PunishNumber:   big.NewInt(0),
			TotalAmount:    new(big.Int).Set(record.PledgeAmount),
			TotalCapacity:  new(big.Int).Set(record.Capacity),
			UsedCapacity:   new(big.Int).SetInt64(0),
			SnRatio:        snRatio.Mul(SnDefaultRatioDigit).BigInt(),
			RevenueAddress: record.RevenueAddress,
			ManagerAmount:  new(big.Int).Set(record.PledgeAmount),
			Fee:            record.Fee,
			EntrustRate:    record.EntrustRate,
			Status:         spStatusActive,
			EtDetail:       make(map[common.Hash]*EntrustDetail, 0),
		}
		s.SpData.PoolPledge[record.Hash].EtDetail[record.PledgeHash] = &EntrustDetail{
			Address: record.Manager,
			Height:  new(big.Int).Set(number),
			Amount:  new(big.Int).Set(record.PledgeAmount),
			Hash:    getHash(changeOxToUx(record.Manager.String()) + record.PledgeAmount.String() + number.String()),
		}

		s.SpData.accumulateSpPledgelHash(record.Hash, false)
	}
	s.SpData.accumulateSpDataHash()
}
func (a *Alien) spAdJustPledge(adjustPledge []SpAdjustPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpAdjustPledgeRecord {

	if len(txDataInfo) <= adJustParamAmountIndex {
		log.Warn("spAdJustPledge", "paramter error", len(txDataInfo))
		return adjustPledge
	}
	adjtPledge := SpAdjustPledgeRecord{
		Hash:         common.Hash{},
		PledgeAmount: big.NewInt(0),
		EtHash:       tx.Hash(),
	}
	if err := adjtPledge.Hash.UnmarshalText1([]byte(txDataInfo[adJustParamSpHashIndex])); err != nil {
		log.Warn("spAdJustPledge", "Hash error", txDataInfo[adJustParamSpHashIndex])
		return adjustPledge
	}
	if pledgeAmount, err := decimal.NewFromString(txDataInfo[adJustParamAmountIndex]); err != nil {
		log.Warn("spAdJustPledge", "pledgeAmount error", txDataInfo[adJustParamAmountIndex])
		return adjustPledge
	} else if pledgeAmount.Cmp(decimal.Zero) < 0 {
		log.Warn("spAdJustPledge", "pledgeAmount  < 0 ", txDataInfo[adJustParamAmountIndex])
		return adjustPledge
	} else {
		adjtPledge.PledgeAmount = pledgeAmount.BigInt()
	}
	if sp, ok := snap.SpData.PoolPledge[adjtPledge.Hash]; ok {
		if sp.Manager != txSender {
			log.Warn("spAdJustPledge", "txSender no role ", txSender)
			return adjustPledge
		}
		if sp.Status >= spStatusExited {
			log.Warn("spAdJustPledge", "SP Status  is exiting or exited ", txSender)
			return adjustPledge
		}
		if sp.Number.Uint64() <initStorageManagerNumber && sp.ManagerAmount.Cmp(common.Big0)== 0{
			if adjtPledge.PledgeAmount.Cmp(spMinPledgeAmount) < 0 {
				log.Warn("spAdJustPledge", "first manager pledge must > 625 ", adjtPledge.PledgeAmount,"txSender",txSender)
				return adjustPledge
			}
		}
	} else {
		log.Warn("spAdJustPledge", "not find sp by spHash", adjtPledge.Hash)
		return adjustPledge
	}
	balance := state.GetBalance(txSender)
	if balance.Cmp(adjtPledge.PledgeAmount) > 0 {
		state.SubBalance(txSender, adjtPledge.PledgeAmount)
	} else {
		log.Warn("spEntrustPledge", "Insufficient Balance", balance, "PledgeAmount", adjtPledge.PledgeAmount)
		return adjustPledge
	}
	adjustPledge = append(adjustPledge, adjtPledge)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21040"))
	topics[1].SetBytes(adjtPledge.Hash.Bytes())
	topics[2].SetBytes(adjtPledge.PledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return adjustPledge
}
func (s *Snapshot) updateSpAdJustPledgeData(pledgeRecord []SpAdjustPledgeRecord, db ethdb.Database, number *big.Int) {
	if len(pledgeRecord) == 0 {
		return
	}
	for _, record := range pledgeRecord {
		if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
			sp.TotalAmount = new(big.Int).Add(sp.TotalAmount, record.PledgeAmount)
			if sp.Number.Uint64() <initStorageManagerNumber && record.PledgeAmount.Cmp(spMinPledgeAmount)>=0{
				if sp.ManagerAmount.Cmp(common.Big0) == 0 && sp.Status==spStatusInactive{
					sp.Status=spStatusActive
				}
			}
			sp.ManagerAmount = new(big.Int).Add(sp.ManagerAmount, record.PledgeAmount)
			pledgeCapacity:=getCapacity(sp.TotalAmount)
			if pledgeCapacity.Cmp(sp.TotalCapacity) >0 {
				sp.TotalCapacity =pledgeCapacity
			}

			if sp.EtDetail==nil {
				sp.EtDetail=make(map[common.Hash]*EntrustDetail)
			}
			sp.EtDetail[record.EtHash] = &EntrustDetail{
					Address: sp.Manager,
					Height:  new(big.Int).Set(number),
					Amount:  new(big.Int).Set(record.PledgeAmount),
					Hash:    getHash(changeOxToUx(sp.Manager.String()) + record.PledgeAmount.String() + number.String()),
			}
			s.SpData.accumulateSpPledgelHash(record.Hash, false)
		}
	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) spRemoveSn(removePledge []SpRemoveSnRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpRemoveSnRecord {
	if len(txDataInfo) <= spRemoveSnAddrIndex {
		log.Warn("spRemoveSn", "paramter error", len(txDataInfo))
		return removePledge
	}
	spRemovePledge := SpRemoveSnRecord{
		Hash:    common.Hash{},
		Address: common.Address{},
	}
	if err := spRemovePledge.Hash.UnmarshalText1([]byte(txDataInfo[spRemoveSpHashIndex])); err != nil {
		log.Warn("spRemoveSn", "Hash error", txDataInfo[spRemoveSpHashIndex])
		return removePledge
	}
	if err := spRemovePledge.Address.UnmarshalText1([]byte(txDataInfo[spRemoveSnAddrIndex])); err != nil {
		log.Warn("spRemoveSn", "SN address format error", txDataInfo[spRemoveSnAddrIndex])
		return removePledge
	}
	if sp, ok := snap.SpData.PoolPledge[spRemovePledge.Hash]; ok {
		if sp.Manager != txSender {
			log.Warn("spRemoveSn", "txSender no role ", txSender)
			return removePledge
		}

	} else {
		log.Warn("spAdJustPledge", "sp not exit ", spRemovePledge.Hash)
		return removePledge
	}

	if snEntrust, ok := snap.StorageData.StorageEntrust[spRemovePledge.Address]; !ok {
		log.Warn("spRemoveSn", "SN not exit", spRemovePledge.Address)
		return removePledge
	} else if snEntrust.Sphash != spRemovePledge.Hash {
		log.Warn("spRemoveSn", "address not rela sp address", spRemovePledge.Address, "sp", spRemovePledge.Hash)
		return removePledge
	}
	snTotalCapacity:=big.NewInt(0)
	if sn, ok2 := snap.StorageData.StoragePledge[spRemovePledge.Address]; ok2 {
		snTotalCapacity=sn.TotalCapacity
	}
	removePledge = append(removePledge, spRemovePledge)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21109"))
	topics[1].SetBytes(snTotalCapacity.Bytes())
	topics[2].SetBytes(spRemovePledge.Address.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return removePledge
}
func (s *Snapshot) updateSpRemoveSnData(removeRecord []SpRemoveSnRecord, db ethdb.Database, number *big.Int) {
	if len(removeRecord) == 0 {
		return
	}
	for _, record := range removeRecord {
		if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
			if snt, ok1 := s.StorageData.StorageEntrust[record.Address]; ok1 {
				snt.Sphash = common.Hash{}
				snt.Spheight = big.NewInt(0)
			}
			if sn, ok2 := s.StorageData.StoragePledge[record.Address]; ok2 {
				if sp.UsedCapacity.Cmp(sn.TotalCapacity)>0{
					sp.UsedCapacity = new(big.Int).Sub(sp.UsedCapacity, sn.TotalCapacity)
				}else{
					sp.UsedCapacity =common.Big0
				}
			}
			s.SpData.accumulateSpPledgelHash(record.Hash, false)
		}
	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) spEntrustPledge(entrustPledge []SpEntrustPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpEntrustPledgeRecord {
	if len(txDataInfo) <= spEntrustPgAmountIndex {
		log.Warn("spEntrustPledge", "paramter error", len(txDataInfo))
		return entrustPledge
	}
	entrustPg := SpEntrustPledgeRecord{
		Hash:         common.Hash{},
		Address:      txSender,
		PledgeAmount: big.NewInt(0),
		Capacity:     big.NewInt(0),
		PledgeHash:   tx.Hash(),
		SpType:       spEntrustTypePledge,
	}
	if err := entrustPg.Hash.UnmarshalText1([]byte(txDataInfo[spEntrustPgSpHashIndex])); err != nil {
		log.Warn("spEntrustPledge", "Hash error", txDataInfo[spEntrustPgSpHashIndex])
		return entrustPledge
	}
	if sp, ok := snap.SpData.PoolPledge[entrustPg.Hash]; ok {
		if sp.Status != spStatusActive {
			log.Warn("spEntrustPledge", "SP Status  need active ", txSender)
			return entrustPledge
		}
	} else {
		log.Warn("spEntrustPledge", "sp not exit ", entrustPg.Hash)
		return entrustPledge
	}

	targetPool := snap.findSPTargetMiner(txSender)
	nilAddr := common.Hash{}
	if targetPool != nilAddr && targetPool != entrustPg.Hash {
		log.Warn("spEntrustPledge", "one address can only pledge one pool ", targetPool)
		return entrustPledge
	}

	if pledgeAmount, err := decimal.NewFromString(txDataInfo[spEntrustPgAmountIndex]); err != nil {
		log.Warn("spEntrustPledge", "pledgeAmount error", txDataInfo[spEntrustPgAmountIndex])
		return entrustPledge
	} else if pledgeAmount.Cmp(decimal.Zero) < 0 {
		log.Warn("spEntrustPledge", "pledgeAmount < 0 ", pledgeAmount)
		return entrustPledge
	} else {
		entrustPg.PledgeAmount = pledgeAmount.BigInt()
		entrustPg.Capacity = getCapacity(entrustPg.PledgeAmount)
	}
	balance := state.GetBalance(txSender)
	if balance.Cmp(entrustPg.PledgeAmount) > 0 {
		state.SubBalance(txSender, entrustPg.PledgeAmount)
	} else {
		log.Warn("spEntrustPledge", "Insufficient Balance", balance, "PledgeAmount", entrustPg.PledgeAmount)
		return entrustPledge
	}
	entrustPledge = append(entrustPledge, entrustPg)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21341"))
	topics[1].SetBytes(entrustPg.Hash.Bytes())
	topics[2].SetBytes(entrustPg.PledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return entrustPledge
}
func (s *Snapshot) updateSpEntrustPledgeData(entrustRecord []SpEntrustPledgeRecord, db ethdb.Database, number *big.Int) {
	if len(entrustRecord) == 0 {
		return
	}
	for _, record := range entrustRecord {
		if record.SpType == spEntrustTypePledge {
			if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
				sp.TotalAmount = new(big.Int).Add(sp.TotalAmount, record.PledgeAmount)
				if sp.Manager == record.Address {
					sp.ManagerAmount = new(big.Int).Add(sp.ManagerAmount, record.PledgeAmount)
				}
				pledgeCapacity := getCapacity(sp.TotalAmount)
				if pledgeCapacity.Cmp(sp.TotalCapacity) > 0 {
					sp.TotalCapacity = pledgeCapacity
				}
				sp.EtDetail[record.PledgeHash] = &EntrustDetail{
					Address: record.Address,
					Height:  new(big.Int).Set(number),
					Amount:  record.PledgeAmount,
					Hash:    getHash(changeOxToUx(record.Address.String()) + record.PledgeAmount.String() + number.String()),
				}
				s.SpData.accumulateSpPledgelHash(record.Hash, false)
			}
		}
	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) spEntrustTransferPledge(entrustPledge []SpEntrustPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpEntrustPledgeRecord {
	if len(txDataInfo) <= spEntrustTransferAddressIndex {
		log.Warn("spEntrustTransferPledge", "paramter error", len(txDataInfo))
		return entrustPledge
	}
	entrustTransferPledge := SpEntrustPledgeRecord{
		Hash:         common.Hash{},
		Address:      txSender,
		PledgeAmount: big.NewInt(0),
		Capacity:     big.NewInt(0),
		PledgeHash:   tx.Hash(),
		SpType:       spEntrustTypeTransfer,
		LockAmount:   big.NewInt(0),
	}
	if isInCurrentEntrustPledge(entrustPledge, entrustTransferPledge.Address) {
		log.Warn("spEntrustTransferPledge", "Address is in entrustPledge", entrustTransferPledge.Address)
		return entrustPledge
	}
	if err := entrustTransferPledge.Hash.UnmarshalText1([]byte(txDataInfo[spEntrustTransferSpIndex])); err != nil {
		log.Warn("spEntrustTransferPledge", "Hash error", txDataInfo[spEntrustTransferSpIndex])
		return entrustPledge
	}
	if sp, ok := snap.SpData.PoolPledge[entrustTransferPledge.Hash]; ok {
		if sp.Manager == txSender {
			log.Warn("spEntrustTransferPledge", "manager address no role", txDataInfo[spEntrustTransferSpIndex])
			return entrustPledge
		}
		if sp.Status != spStatusActive {
			log.Warn("spEntrustTransferPledge", "SP Status  is need active ", txSender)
			return entrustPledge
		}
		transAmount := big.NewInt(0)
		pledgeMinBLock := a.getEntrustPledgeMinBLock(spEntrustMinDay)
		for _, detail := range sp.EtDetail {
			if detail.Address == txSender {
				pledgeBLock := new(big.Int).Sub(new(big.Int).SetUint64(snap.Number), detail.Height)
				if pledgeBLock.Cmp(pledgeMinBLock) < 0 {
					log.Warn("spEntrustTransferPledge", "Entrust hash illegality", txSender)
					return entrustPledge
				}
				transAmount = new(big.Int).Add(transAmount, detail.Amount)
			}
		}
		if transAmount.Cmp(big.NewInt(0)) <= 0 {
			log.Warn("spEntrustTransferPledge", "TxSender does not have a transferable deposit ", txSender)
			return entrustPledge
		}
		entrustTransferPledge.PledgeAmount = transAmount

	} else {
		log.Warn("spEntrustTransferPledge", "sp not exit ", entrustTransferPledge.Hash)
		return entrustPledge
	}

	entrustTransferPledge.TargetType = txDataInfo[spEntrustTransferTypeIndex]
	if TargetTypePos == entrustTransferPledge.TargetType {
		if err := entrustTransferPledge.TargetAddress.UnmarshalText1([]byte(txDataInfo[spEntrustTransferAddressIndex])); err != nil {
			log.Warn("spEntrustTransferPledge", "PoS target Address error", txDataInfo[spEntrustTransferAddressIndex])
			return entrustPledge
		}
		if _, ok := snap.PosPledge[entrustTransferPledge.TargetAddress]; !ok {
			log.Warn("spEntrustTransferPledge", "PoS node not exit ", entrustTransferPledge.Address)
			return entrustPledge
		}
		if _, ok := snap.PosPledge[entrustTransferPledge.Address]; ok {
			log.Warn("spEntrustTransferPledge", "txSender is miner address", entrustTransferPledge.Address)
			return entrustPledge
		}
		targetMiner := snap.findPosTargetMiner(txSender)
		nilAddr := common.Address{}
		if targetMiner != nilAddr && targetMiner != entrustTransferPledge.TargetAddress {
			log.Warn("spEntrustTransferPledge", "one address can only pledge one miner ", targetMiner)
			return entrustPledge
		}
	} else if TargetTypeSp == entrustTransferPledge.TargetType {
		if err := entrustTransferPledge.TargetHash.UnmarshalText1([]byte(txDataInfo[spEntrustTransferAddressIndex])); err != nil {
			log.Warn("spEntrustTransferPledge", "Sp target Hash error", txDataInfo[spEntrustTransferAddressIndex])
			return entrustPledge
		}
		if _, ok := snap.SpData.PoolPledge[entrustTransferPledge.TargetHash]; !ok {
			log.Warn("spEntrustTransferPledge", "Sp target not exit ", entrustTransferPledge.TargetHash)
			return entrustPledge
		}
	} else if TargetTypeSn == entrustTransferPledge.TargetType {
		if _, ok := snap.StorageData.StoragePledge[entrustTransferPledge.Address]; ok {
			log.Warn("spEntrustTransferPledge", "txSender is Storage address", entrustTransferPledge.Address)
			return entrustPledge
		}
		if err := entrustTransferPledge.TargetAddress.UnmarshalText1([]byte(txDataInfo[spEntrustTransferAddressIndex])); err != nil {
			log.Warn("spEntrustTransferPledge", "SN target Address error", txDataInfo[spEntrustTransferAddressIndex])
			return entrustPledge
		}
		if _, ok := snap.StorageData.StoragePledge[entrustTransferPledge.Address]; ok {
			log.Warn("spEntrustTransferPledge", "txSender is Storage address", entrustTransferPledge.Address)
			return entrustPledge
		}
		targetMiner := snap.findStorageTargetMiner(txSender)
		nilAddr := common.Address{}
		if targetMiner != nilAddr && targetMiner != entrustTransferPledge.TargetAddress {
			log.Warn("spEntrustTransferPledge", "one address can only pledge one miner ", targetMiner)
			return entrustPledge
		}
		currBlockTranAmount := big.NewInt(0)
		for _, item := range entrustPledge {
			if item.SpType == spEntrustTypeTransfer && "SN" == item.TargetType {
				currBlockTranAmount = new(big.Int).Add(currBlockTranAmount, item.PledgeAmount)
			}
		}
		if snEtPledge, ok := snap.StorageData.StorageEntrust[entrustTransferPledge.TargetAddress]; ok {
			if snItem, ok1 := snap.StorageData.StoragePledge[entrustTransferPledge.TargetAddress]; ok1 {
				if snItem.PledgeStatus.Cmp(big.NewInt(SPledgeInactive))!=0{
					log.Warn("spEntrustTransferPledge", "Sn is not inactive", entrustTransferPledge.TargetAddress)
					return entrustPledge
				}
				estimateAmountV1 := new(big.Int).Add(currBlockTranAmount, snEtPledge.PledgeAmount)
				if estimateAmountV1.Cmp(snItem.SpaceDeposit) >= 0 {
					log.Warn("spEntrustTransferPledge", "Sn entrusted pledge is full", txDataInfo[spEntrustTransferAddressIndex])
					return entrustPledge
				}
				estimateAmountV2 := new(big.Int).Add(estimateAmountV1, entrustTransferPledge.PledgeAmount)
				lockAmount := big.NewInt(0)
				if estimateAmountV2.Cmp(snItem.SpaceDeposit) > 0 {
					lockAmount = new(big.Int).Sub(estimateAmountV2, snItem.SpaceDeposit)
					entrustTransferPledge.PledgeAmount = new(big.Int).Sub(entrustTransferPledge.PledgeAmount, lockAmount)
				}
				//SN deposit must be an integer   get non integer parts
				depositMode := new(big.Int).Mod(entrustTransferPledge.PledgeAmount, utgOneValue)
				// Add non integer parts to the lock compartment
				if depositMode.Cmp(big.NewInt(0)) > 0 {
					lockAmount = new(big.Int).Add(lockAmount, depositMode)
					entrustTransferPledge.PledgeAmount = new(big.Int).Sub(entrustTransferPledge.PledgeAmount, depositMode)
				}
				if lockAmount.Cmp(big.NewInt(0)) > 0 {
					entrustTransferPledge.LockAmount = lockAmount
				}
			}
		} else {
			log.Warn("spEntrustTransferPledge", "SN node not exit", entrustTransferPledge.TargetAddress)
			return entrustPledge
		}
	} else {
		log.Warn("spEntrustTransferPledge", "TargetType is illegal", entrustTransferPledge.TargetType)
		return entrustPledge
	}

	entrustPledge = append(entrustPledge, entrustTransferPledge)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21620"))
	topics[1].SetBytes(entrustTransferPledge.LockAmount.Bytes())
	topics[2].SetBytes(entrustTransferPledge.PledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return entrustPledge
}

func (a *Alien) getEntrustPledgeMinBLock(limitDay *big.Int) *big.Int {
	return new(big.Int).Mul(big.NewInt(int64(a.blockPerDay())), limitDay)
}
func (s *Snapshot) updateSpEntrustTransferData(entrustRecord []SpEntrustPledgeRecord, db ethdb.Database, number *big.Int) {
	if len(entrustRecord) == 0 {
		return
	}
	spCount := 0
	snCount := 0

	for _, record := range entrustRecord {
		if record.SpType == spEntrustTypeTransfer {
			if TargetTypePos == record.TargetType {
				if posItem, ok := s.PosPledge[record.TargetAddress]; ok {
					posItem.Detail[record.PledgeHash] = &PledgeDetail{
						Address: record.Address,
						Height:  number.Uint64(),
						Amount:  record.PledgeAmount,
					}
					posItem.TotalAmount = new(big.Int).Add(posItem.TotalAmount, record.PledgeAmount)
				}
			} else if TargetTypeSp == record.TargetType {
				if targetSp, ok := s.SpData.PoolPledge[record.TargetHash]; ok {
					targetSp.TotalAmount = new(big.Int).Add(targetSp.TotalAmount, record.PledgeAmount)
					preCapacity := getCapacity(targetSp.TotalAmount)
					if preCapacity.Cmp(targetSp.TotalCapacity) > 0 {
						targetSp.TotalCapacity = preCapacity
					}
					targetSp.EtDetail[record.PledgeHash] = &EntrustDetail{
						Address: record.Address,
						Height:  new(big.Int).Set(number),
						Amount:  record.PledgeAmount,
						Hash:    getHash(changeOxToUx(record.Address.String()) + record.PledgeAmount.String() + number.String()),
					}
					s.SpData.accumulateSpPledgelHash(record.TargetHash, false)
					spCount++
				}
			} else if TargetTypeSn == record.TargetType {
				if targetSn, ok := s.StorageData.StorageEntrust[record.TargetAddress]; ok {
					targetSn.PledgeAmount = new(big.Int).Add(targetSn.PledgeAmount, record.PledgeAmount)
					targetSn.Detail[record.PledgeHash] = &SEntrustDetail{
						Address: record.Address,
						Height:  new(big.Int).Set(number),
						Amount:  record.PledgeAmount,
					}
					if record.Address==targetSn.Manager{
						targetSn.ManagerAmount=new(big.Int).Add(targetSn.ManagerAmount,record.PledgeAmount)
						targetSn.Managerheight=new(big.Int).Set(number)
					}
					if stp, ok1 := s.StorageData.StoragePledge[record.TargetAddress]; ok1 {
						if targetSn.PledgeAmount.Cmp(stp.SpaceDeposit)>=0&&stp.PledgeStatus.Cmp(big.NewInt(SPledgeInactive))==0{
							s.StorageData.StoragePledge[record.TargetAddress].PledgeStatus=big.NewInt(SPledgeNormal)
							s.StorageData.accumulatePledgeHash(record.TargetAddress)
						}
					}
				}
				snCount++
			}
			if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
				delHash := make([]common.Hash, 0)
				for etHash, detail := range sp.EtDetail {
					if record.Address == detail.Address {
						delHash = append(delHash, etHash)
					}
				}
				for _, removeHash := range delHash {
					delete(sp.EtDetail, removeHash)
				}
				sp.TotalAmount = new(big.Int).Sub(sp.TotalAmount, new(big.Int).Add(record.LockAmount, record.PledgeAmount))
			}
			if spCount > 0 {
				s.SpData.accumulateSpDataHash()
			}
			if record.LockAmount.Cmp(common.Big0) > 0 {
				lockRecord := LockRewardNewRecord{
					Target:   record.Address,
					Amount:   record.LockAmount,
					IsReward: sscSpEntrustExitLockReward,
					RevenueAddress: record.Address,
					SourceAddress: common.BigToAddress(record.Hash.Big()),
				}
				s.FlowRevenue.SpEntrustExitLock.updateLockDataV1(s,lockRecord,number)
			}
		}
	}
}
func (a *Alien) spEntrustExitPledge(entrustPledge []SpEntrustPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpEntrustPledgeRecord {
	if len(txDataInfo) <= spEntrustExitEtHashIndex {
		log.Warn("spEntrustExitPledge", "paramter error", len(txDataInfo))
		return entrustPledge
	}
	entrustExitPledge := SpEntrustPledgeRecord{
		Address: txSender,
		Hash:       common.Hash{},
		PledgeHash: common.Hash{},
		LockAmount: big.NewInt(0),
		SpType:     spEntrustTypeExit,
	}
	if err := entrustExitPledge.Hash.UnmarshalText1([]byte(txDataInfo[spEntrustExitSpHashIndex])); err != nil {
		log.Warn("spEntrustExitPledge", "SP Hash error", txDataInfo[spEntrustExitSpHashIndex])
		return entrustPledge
	}
	if err := entrustExitPledge.PledgeHash.UnmarshalText1([]byte(txDataInfo[spEntrustExitEtHashIndex])); err != nil {
		log.Warn("spEntrustExitPledge", "SP Hash error", txDataInfo[spEntrustExitEtHashIndex])
		return entrustPledge
	}

	if isInCurrentSpEntrustExit(entrustPledge, entrustExitPledge.PledgeHash) {
		log.Warn("storageEntrustedPledgeExit", "Hash is in currentSEExit", entrustExitPledge.PledgeHash)
		return entrustPledge
	}

	if sp, ok := snap.SpData.PoolPledge[entrustExitPledge.Hash]; ok {
		if sp.Manager == txSender {
			log.Warn("spEntrustExitPledge", "SP manager no role", txSender)
			return entrustPledge
		}
		if sp.Status >= spStatusExited {
			log.Warn("spEntrustTransferPledge", "SP Status  is exiting or exited ", txSender)
			return entrustPledge
		}
		if entrustItem, ok1 := sp.EtDetail[entrustExitPledge.PledgeHash]; ok1 {
			if txSender != entrustItem.Address {
				log.Warn("spEntrustExitPledge", "txSender no role", txSender)
				return entrustPledge
			}
			entrustExitPledge.LockAmount = entrustItem.Amount
			pledgeBLock := new(big.Int).Sub(big.NewInt(int64(snap.Number)), entrustItem.Height)
			if pledgeBLock.Cmp(a.getEntrustPledgeMinBLock(spEntrustMinDay)) < 0 {
				log.Warn("spEntrustTransferPledge", "Entrust Pledge time limit 7 days", txSender)
				return entrustPledge
			}
		} else {
			log.Warn("spEntrustExitPledge", "not find entrust pledge ", entrustExitPledge.PledgeHash)
			return entrustPledge
		}
	} else {
		log.Warn("spEntrustExitPledge", "SP not find ", entrustExitPledge.Hash)
		return entrustPledge
	}
	entrustPledge = append(entrustPledge, entrustExitPledge)

	topics := make([]common.Hash, 2)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21020"))
	topics[1].SetBytes(entrustExitPledge.LockAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return entrustPledge
}

func (s *Snapshot) updateSpEntrustExitData(entrustRecord []SpEntrustPledgeRecord, db ethdb.Database, number *big.Int) {
	if len(entrustRecord) == 0 {
		return
	}
	for _, record := range entrustRecord {
		if record.SpType == spEntrustTypeExit {
			lockRecord := LockRewardNewRecord{
				Target:  record.Address,
				Amount: record.LockAmount,
				IsReward: sscSpEntrustExitLockReward,
				SourceAddress: common.BigToAddress(record.Hash.Big()),
				RevenueAddress: record.Address,
			}
			s.FlowRevenue.SpEntrustExitLock.updateLockDataV1(s,lockRecord,number)
			if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
				delete(sp.EtDetail, record.PledgeHash)
				sp.TotalAmount = new(big.Int).Sub(sp.TotalAmount, record.LockAmount)
				s.SpData.accumulateSpPledgelHash(record.Hash, false)
			}
		}
	}
	s.SpData.accumulateSpDataHash()


}

func getCapacity(amount *big.Int) *big.Int {
	return new(big.Int).Mul(new(big.Int).Div(amount, spSpacePgPrice), capacityOneTb)
}

func (a *Alien) spExitPledge(exitPledge []common.Hash, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []common.Hash {
	if len(txDataInfo) <= spExitSpHashIndex {
		log.Warn("spExitPledge", "parameter error", len(txDataInfo))
		return exitPledge
	}
	exitHash := common.Hash{}
	if err := exitHash.UnmarshalText1([]byte(txDataInfo[spExitSpHashIndex])); err != nil {
		log.Warn("spExitPledge", "SP Hash error", txDataInfo[spExitSpHashIndex])
		return exitPledge
	}
	if sp, ok := snap.SpData.PoolPledge[exitHash]; ok {
		if sp.Manager != txSender {
			log.Warn("spExitPledge", "txSender no role ", txSender)
			return exitPledge
		}
		if sp.Status >= spStatusExited {
			log.Warn("spEntrustTransferPledge", "SP Status  is exiting or exited ", txSender)
			return exitPledge
		}
		pledgeBLock := new(big.Int).Sub(big.NewInt(int64(snap.Number)), sp.Number)
		if pledgeBLock.Cmp(a.getEntrustPledgeMinBLock(spPledgeMinDay)) < 0 {
			log.Warn("spExitPledge", "Pledge time limit 90 days", txSender)
			return exitPledge
		}

	} else {
		log.Warn("spExitPledge", "not find Sp ", exitHash)
		return exitPledge
	}

	exitPledge = append(exitPledge, exitHash)
	topics := make([]common.Hash, 2)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c21033"))
	topics[1].SetBytes(exitHash.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return exitPledge
}
func (s *Snapshot) updateSpExitPledgeData(spExitPledge []common.Hash, db ethdb.Database, number *big.Int) {
	if len(spExitPledge) == 0 {
		return
	}
	clearSpMap:=make(map[common.Hash]int,0)
	for _, hash := range spExitPledge {
		if sp, ok := s.SpData.PoolPledge[hash]; ok {
			sp.Status = spStatusExited
			clearSpMap[hash]=1
			s.SpData.accumulateSpPledgelHash(hash, false)

		}
	}
	for _, item := range s.StorageData.StorageEntrust {
		if _,ok:=clearSpMap[item.Sphash];ok{
			item.Sphash=common.Hash{}
			item.Spheight=common.Big0
		}

	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) spSetFee(feeRecord []SpFeeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpFeeRecord {
	if len(txDataInfo) <= spFeeSetFeeIndex {
		log.Warn("spSetFee", "parameter error", len(txDataInfo))
		return feeRecord
	}
	apFee := SpFeeRecord{
		Hash: common.Hash{},
		Fee:  uint64(0),
	}
	if err := apFee.Hash.UnmarshalText1([]byte(txDataInfo[spFeeSetSpHashIndex])); err != nil {
		log.Warn("spSetFee", "SP Hash error", txDataInfo[spFeeSetSpHashIndex])
		return feeRecord
	}
	if sp, ok := snap.SpData.PoolPledge[apFee.Hash]; ok {
		if sp.Status == spStatusExited {
			log.Warn("spSetFee", "sp is exited ", apFee.Hash)
			return feeRecord
		}
		if sp.Manager != txSender {
			log.Warn("spSetFee", "txSender no role ", txSender)
			return feeRecord
		}
	} else {
		log.Warn("spSetFee", "SP not exit ", apFee.Hash)
		return feeRecord
	}
	if fee, err := strconv.Atoi(txDataInfo[spFeeSetFeeIndex]); err != nil {
		log.Warn("spSetFee", "fee format error ", txDataInfo[spFeeSetFeeIndex])
		return feeRecord
	} else if fee < 0 ||fee > 100 {
		log.Warn("spSetFee", "fee < 0 or fee > 100", fee)
		return feeRecord
	}else {
		apFee.Fee = uint64(fee)
	}
	feeRecord = append(feeRecord, apFee)
	topics := make([]common.Hash, 3)
	//web3.sha3("sp set fee")
	topics[0].UnmarshalText([]byte("0xd1403b31a7af62317dc2a1a77026bc002a99fa7a27418b53f5eb1ffdeba6b0bd"))
	topics[1].SetBytes(apFee.Hash.Bytes())
	topics[2].SetBytes([]byte(txDataInfo[spFeeSetFeeIndex]))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return feeRecord
}

func (s *Snapshot) updateSpFeeData(spFee []SpFeeRecord, db ethdb.Database, number *big.Int) {
	if len(spFee) == 0 {
		return
	}
	for _, record := range spFee {
		if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
			sp.Fee = record.Fee
			s.SpData.accumulateSpPledgelHash(record.Hash, false)
		}
	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) spSetEntrustRate(entrustRateRecord []SpEntrustRateRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SpEntrustRateRecord {
	if len(txDataInfo) <= spEntrustRateIndex {
		log.Warn("spSetEntrustRate", "parameter error", len(txDataInfo))
		return entrustRateRecord
	}
	apEtRate := SpEntrustRateRecord{
		Hash:        common.Hash{},
		EntrustRate: uint64(0),
	}
	if err := apEtRate.Hash.UnmarshalText1([]byte(txDataInfo[spEntrustRateSpHashIndex])); err != nil {
		log.Warn("spSetEntrustRate", "SP Hash error", txDataInfo[spEntrustRateSpHashIndex])
		return entrustRateRecord
	}
	if sp, ok := snap.SpData.PoolPledge[apEtRate.Hash]; ok {
		if sp.Status == spStatusExited {
			log.Warn("spSetEntrustRate", "sp is exited ", apEtRate.Hash)
			return entrustRateRecord
		}
		if sp.Manager != txSender {
			log.Warn("spSetEntrustRate", "txSender no role ", txSender)
			return entrustRateRecord
		}
	} else {
		log.Warn("spSetEntrustRate", "SP not exit ", apEtRate.Hash)
		return entrustRateRecord
	}
	if entrustRate, err := strconv.Atoi(txDataInfo[spEntrustRateIndex]); err != nil {
		log.Warn("spSetEntrustRate", "EntrustRate format error ", txDataInfo[spEntrustRateIndex])
		return entrustRateRecord
	} else if entrustRate < 0 ||entrustRate > 100{
		log.Warn("spSetEntrustRate", "EntrustRate< 0 or entrustRate > 100", entrustRate)
		return entrustRateRecord
	} else {
		apEtRate.EntrustRate = uint64(entrustRate)
	}
	entrustRateRecord = append(entrustRateRecord, apEtRate)
	topics := make([]common.Hash, 3)
	//web3.sha3("sp set entrustrate")
	topics[0].UnmarshalText([]byte("0x4f4433d18725bd48f7616428155279c51cc81a5952d3e0df41fa84ce778c24b6"))
	topics[1].SetBytes(apEtRate.Hash.Bytes())
	topics[2].SetBytes([]byte(txDataInfo[spEntrustRateIndex]))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return entrustRateRecord
}
func (s *Snapshot) updateSpEntrustRateData(spEtRate []SpEntrustRateRecord, db ethdb.Database, number *big.Int) {
	if len(spEtRate) == 0 {
		return
	}
	for _, record := range spEtRate {
		if sp, ok := s.SpData.PoolPledge[record.Hash]; ok {
			sp.EntrustRate = record.EntrustRate
			s.SpData.accumulateSpPledgelHash(record.Hash, false)
		}
	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) processSpBind(currentSpBind [] SpBindRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, snap *Snapshot, number uint64) []SpBindRecord {
 if len(txDataInfo) <=spBindTypeIndex {
	 log.Warn("processSpBind","parameter error",len(txDataInfo))
	 return currentSpBind
 }
	spBind :=SpBindRecord{
         Hash: common.Hash{},
		 RevenueAddress: common.Address{},
		 Bind: false,
	}
	if err := spBind.Hash.UnmarshalText1([]byte(txDataInfo[spBindSpHashIndex])); err != nil {
		log.Warn("processSpBind", "SP Hash error",err,"Hash", txDataInfo[spBindSpHashIndex])
		return currentSpBind
	}
	if sp,ok:=snap.SpData.PoolPledge[spBind.Hash];ok{
		 if sp.Manager!=txSender {
			 log.Warn("processSpBind", "txSender no role", txSender,"manager",sp.Manager)
			 return currentSpBind
		 }
	}else {
		log.Warn("processSpBind", "SP not find ", txDataInfo[spBindSpHashIndex])
		return currentSpBind
	}
	 bindType:=txDataInfo[spBindTypeIndex]
	 if bindType== "bind"{
		 spBind.Bind=true
		 if len(txDataInfo) <=spBindReveAddrIndex{
			 log.Warn("processSpBind","parameter error",len(txDataInfo))
			 return currentSpBind
		 }
		 if err := spBind.RevenueAddress.UnmarshalText1([]byte(txDataInfo[spBindReveAddrIndex])); err != nil {
			 log.Warn("processSpBind", "SP RevenueAddress error", txDataInfo[spBindReveAddrIndex])
			 return currentSpBind
		 }
	 }else if bindType== "unbind"{
		 spBind.Bind=false
	 }else {
		 log.Warn("processSpBind", "Illegal bind Type", txDataInfo[spBindTypeIndex])
		 return currentSpBind
	 }

	currentSpBind = append(currentSpBind, spBind)
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7er1521"))
	topics[1].SetBytes([]byte(bindType))
	topics[2].SetBytes(spBind.RevenueAddress.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return currentSpBind
}
func (s *Snapshot) updateSpBindData(spBind []SpBindRecord, db ethdb.Database, number *big.Int) {
	if len(spBind) == 0 {
		return
	}
	for _, record := range spBind {
		if sp,ok:=s.SpData.PoolPledge[record.Hash];ok{
			if record.Bind {
				sp.RevenueAddress=record.RevenueAddress
			}else{
				sp.RevenueAddress=common.Address{}
			}
			s.SpData.accumulateSpPledgelHash(record.Hash,false)
		}
	}
	s.SpData.accumulateSpDataHash()
}
func (s *Snapshot) spAccumulatePublish(number *big.Int) {
	clearSp:=make(map[common.Hash]int,0)
	for spHash, item := range s.SpData.PoolPledge {
		if item.Status== spStatusExited {
			continue
		}
		if item.Status== spStatusIllegalExit {
			clearSp[spHash]=1
			continue
		}
		pledgeCapacity := getCapacity(item.TotalAmount)
		if pledgeCapacity.Cmp(item.TotalCapacity) < 0 {
			if item.PunishNumber.Cmp(big.NewInt(0)) == 0 {
				item.PunishNumber = number
			}
		} else {
			if item.PunishNumber.Cmp(big.NewInt(0)) > 0 {
				item.PunishNumber = big.NewInt(0)
			}
			if pledgeCapacity.Cmp(item.TotalCapacity) > 0 {
				item.TotalCapacity = new(big.Int).Set(pledgeCapacity)
			}
		}
		totalCapacity := new(big.Int).Set(item.TotalCapacity)
		if item.PunishNumber.Cmp(big.NewInt(0)) > 0 {
			punishBlock := number.Uint64() - item.PunishNumber.Uint64()
			punishDay1 := s.getBlockByDay(7)
			if punishBlock <= punishDay1 {
				totalCapacity = new(big.Int).Set(pledgeCapacity)
			} else if punishBlock > punishDay1 && punishBlock <= punishDay1*2 {
				totalCapacity = big.NewInt(0)
			} else {
				item.Status = spStatusIllegalExit
				clearSp[spHash]=1
			}
		}
		if totalCapacity.Cmp(big.NewInt(0)) > 0 {
			item.SnRatio = s.StorageData.calStorageRatio(totalCapacity, number.Uint64()).Mul(SnDefaultRatioDigit).BigInt()
		}else{
			item.SnRatio=big.NewInt(0)
		}
		s.SpData.accumulateSpPledgelHash(spHash,false)
	}
	for _, item := range s.StorageData.StorageEntrust {
		if _,ok:=clearSp[item.Sphash];ok{
			item.Sphash=common.Hash{}
			item.Spheight=common.Big0
		}

	}
	s.SpData.accumulateSpDataHash()
}
func (s *Snapshot) dealSpExit(number *big.Int, db ethdb.Database,headerHash common.Hash){
	delSpRecord :=make([]common.Hash,0)
	burnSpMap :=make(map[common.Address]map[common.Address]uint64,0)
	currentLockReward :=make([]LockRewardNewRecord,0)
	for spHash, sp := range s.SpData.PoolPledge {
		if sp.Status == spStatusExited || sp.Status == spStatusIllegalExit{
			spAddr := common.BigToAddress(spHash.Big())
			etAddrMap := calculateEtPledge(sp.EtDetail)
			revenueAddr := sp.Manager
			if sp.RevenueAddress!=common.BigToAddress(common.Big0){
				revenueAddr = sp.RevenueAddress
			}
			if sp.Status == spStatusExited {
				delSpRecord=append(delSpRecord, spHash)
				for address, pledgeAmount := range etAddrMap {
					isReward := uint32(sscSpEntrustExitLockReward)
					revAddr := address
					if address == sp.Address || address == sp.Manager {
						isReward = uint32(sscSpExitLockReward)
					}
					currentLockReward = append(currentLockReward, LockRewardNewRecord{
						Target:         address,
						Amount:         pledgeAmount,
						IsReward:       isReward,
						SourceAddress:  spAddr,
						RevenueAddress: revAddr,
					})
				}
			}else if sp.Status == spStatusIllegalExit {
				delSpRecord=append(delSpRecord, spHash)
				burnSpMap[spAddr]=make(map[common.Address]uint64,0)
				burnSpMap[spAddr][spAddr]=uint64(1)
				if _,ok:=burnSpMap[sp.Address];!ok {
					burnSpMap[sp.Address]=make(map[common.Address]uint64,0)
				}
				if _,ok:=burnSpMap[sp.Manager];!ok {
					burnSpMap[sp.Manager]=make(map[common.Address]uint64,0)
				}
				burnSpMap[sp.Manager][spAddr]=uint64(1)
				burnSpMap[sp.Address][spAddr]=uint64(1)
				if sp.RevenueAddress!=common.BigToAddress(common.Big0) {
					if  _,ok:=burnSpMap[sp.RevenueAddress];!ok {
						burnSpMap[sp.RevenueAddress]=make(map[common.Address]uint64,0)
					}
					burnSpMap[sp.RevenueAddress][spAddr]=uint64(1)
				}
				for address, pledgeAmount := range etAddrMap {
					isReward := uint32(sscSpEntrustExitLockReward)
					if address != sp.Address && address != sp.Manager && address != revenueAddr {
						currentLockReward = append(currentLockReward, LockRewardNewRecord{
							Target:         address,
							Amount:         pledgeAmount,
							IsReward:       isReward,
							SourceAddress:  spAddr,
							RevenueAddress: address,
						})
					}
				}
			}
		}
	}
	s.FlowRevenue.updateLockDataV1(s, currentLockReward, number)
	if len(burnSpMap)>0 {
     s.FlowRevenue.SpLock.setSpIllegalLockPunish(burnSpMap,db,headerHash,number.Uint64(),sscSpLockReward)
	 s.FlowRevenue.SpEntrustLock.setSpIllegalLockPunish(burnSpMap,db,headerHash,number.Uint64(),sscSpEntrustLockReward)
	}
	if len(delSpRecord)>0 {
		for _,spHash:=range delSpRecord{
			delete(s.SpData.PoolPledge, spHash)
		}
	}
	s.SpData.accumulateSpDataHash()
}
func (s *Snapshot) dealSpExitFinalize(number *big.Int, headerHash common.Hash){
	delSpRecord :=make([]common.Hash,0)
	for spHash, sp := range s.SpData.PoolPledge {
		if sp.Status == spStatusExited || sp.Status == spStatusIllegalExit{
			if sp.Status == spStatusExited {
				delSpRecord=append(delSpRecord, spHash)

			}else if sp.Status == spStatusIllegalExit {
				delSpRecord=append(delSpRecord, spHash)

			}
		}
	}
	if len(delSpRecord)>0 {
		for _,spHash:=range delSpRecord{
			delete(s.SpData.PoolPledge, spHash)
		}
	}
	s.SpData.accumulateSpDataHash()
}
func (s *Snapshot) accumulateSpExitBurnAmount( number *big.Int, state *state.StateDB) {
	if !isStorageVerificationCheck(number.Uint64(), s.Period) {
		return
	}
	burnAmount := big.NewInt(0)
	for spHash, sp := range s.SpData.PoolPledge {
		if sp.Status == spStatusIllegalExit {
			spAddr := common.BigToAddress(spHash.Big())
			etAddrMap := calculateEtPledge(sp.EtDetail)
			revenueAddr := sp.Manager
			if sp.RevenueAddress!=common.BigToAddress(big.NewInt(0)) {
				revenueAddr = sp.RevenueAddress
			}
			for etAddress,Amount:=range etAddrMap{
				if etAddress==sp.Address ||etAddress==sp.Manager || etAddress== revenueAddr{
					burnAmount=new(big.Int).Add(burnAmount,Amount)
				}
			}
			burnReward:=s.FlowRevenue.SpLock.calBurnSpIllegalReward(spAddr,spAddr,uint32(sscSpLockReward))
			burnEtReward:=s.FlowRevenue.SpEntrustLock.calBurnSpIllegalReward(sp.Manager,spAddr,uint32(sscSpEntrustLockReward))
			burnEtRvReward:=common.Big0
			if sp.Manager!=sp.RevenueAddress{
				burnEtRvReward=s.FlowRevenue.SpEntrustLock.calBurnSpIllegalReward(sp.RevenueAddress,spAddr,uint32(sscSpEntrustLockReward))
			}
			if burnReward.Cmp(common.Big0) >0 {
				burnAmount=new(big.Int).Add(burnAmount,burnReward)
			}
			if burnEtReward.Cmp(common.Big0) >0 {
				burnAmount=new(big.Int).Add(burnAmount,burnEtReward)
			}
			if burnEtRvReward.Cmp(common.Big0) >0 {
				burnAmount=new(big.Int).Add(burnAmount,burnEtRvReward)
			}
		}
	}
	if state!= nil {
		state.AddBalance(common.BigToAddress(common.Big0),burnAmount)
	}
}
func (l *LockData) calBurnSpIllegalReward(target common.Address,spAddr common.Address,isReward uint32) *big.Int{
	if balanceReward,ok:=l.FlowRevenue[target];ok {
		if rewardMap,ok1:=balanceReward.RewardBalanceV1[isReward];ok1 {
				if tmpData,ok2:=rewardMap[spAddr];ok2{
					return tmpData.Amount
				}
		}
	}
	return common.Big0
}
func (s *Snapshot) getBlockByDay(days uint64) uint64 {
	return days * s.getBlockPreDay()
}


func isInCurrentEntrustPledge(currentEntrustPledge []SpEntrustPledgeRecord, txSender common.Address) bool {
	has := false
	for _, currentItem := range currentEntrustPledge {
		if currentItem.Address == txSender {
			has = true
			break
		}
	}
	return has
}

func isInCurrentSpEntrustExit(currentSpEntrustExit []SpEntrustPledgeRecord, PledgeHash common.Hash) bool {
	has := false
	for _, currentItem := range currentSpEntrustExit {
		if currentItem.PledgeHash == PledgeHash {
			has = true
			break
		}
	}
	return has
}