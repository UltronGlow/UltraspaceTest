package alien

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/common/hexutil"
	"github.com/UltronGlow/UltronGlow-Origin/consensus"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"github.com/shopspring/decimal"
	"golang.org/x/crypto/sha3"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

const (
	utgStorageDeclare          = "stReq"
	utgStorageExit             = "stExit"
	utgRentRequest             = "stRent"
	utgRentPg                  = "stRentPg"
	utgRentReNew               = "stReNew"
	utgRentReNewPg             = "stReNewPg"
	utgRentRescind             = "stRescind"
	utgStorageRecoverValid     = "stReValid"
	utgStorageProof            = "stProof"
	utgStoragePrice            = "chPrice"
	utgStorageBw               = "chbw"
	utgSRTExch                 = "Exch"
	utgStoragePledgeCatchUp    = "stCatchUp"
	utgStoragePledgeEditmgaddr = "editmgaddr"
	utgStoragePledgeStchpg     = "stchpg"
	utgStoragePledgeStwtreward = "stwtreward"
	utgStoragePledgeSetsp      = "setsp"
	utgStoragePledgeExitsp     = "exitsp"
	utgStoragePledgeStreplace  = "streplace"
	utgStoragePledgeStwtpg     = "stwtpg"
	utgStoragePledgeWtfd       = "wtfd"
	utgStoragePledgeWtpgexit   = "wtpgexit"

	storagePledgeRewardkey   = "storagePledgeReward-%d"
	storageLeaseRewardkey    = "storageLeaseReward-%d"
	revertSpaceLockRewardkey = "revertSpaceLockReward-%d"
	storageRatioskey         = "storageRatios-%d"
	revertExchangeSRTkey     = "revertExchangeSRT-%d"
	originalTotalCapacityKey = "originalTotalCapacity-%d"
	totalPledgeRewardKey     = "totalPledgeReward-%d"
	storageHarvestKey        = "storageHarvest-%d"
	totalLeaseSpaceKey       = "totalLeaseSpace-%d"
	leaseHarvestKey          = "leaseHarvest-%d"
	storagePleageKey         = "storagePleage-%d"
	storageContractKey       = "storageContract-%d"
	storageCapSuccAddrsKey   = "storageCapSuccAddrs-%d"

	SPledgeNormal    = 0
	SPledgeExit      = 1
	SPledgeRemoving  = 5 //30-day verification failed
	SPledgeRetrun    = 6 //SRT and pledge deposit have been returned
	SPledgeInactive  = 7
	LeaseNotPledged  = 0
	LeaseNormal      = 1
	LeaseUserRescind = 2
	LeaseExpiration  = 3
	LeaseBreach      = 4
	LeaseReturn      = 6
)

var (
	totalSpaceProfitReward          = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(10500000))
	gbTob                           = big.NewInt(1073741824)
	tb1b                            = big.NewInt(1099511627776)
	minPledgeStorageCapacity        = decimal.NewFromInt(1099511627776)
	maxPledgeStorageCapacity        = decimal.NewFromInt(1099511627776).Mul(decimal.NewFromInt(80))
	maxPledgeStorageCapacityV1      = decimal.NewFromInt(1099511627776).Mul(decimal.NewFromInt(100))
	maxPledgeStorageCapacityV2      = decimal.NewFromInt(1099511627776).Mul(decimal.NewFromInt(1000))
	proofTimeOut                    = big.NewInt(1800) //second
	storageBlockSize                = "20"
	maxBoundStorageSpace            = new(big.Int).Mul(tb1b, big.NewInt(1048576))
	capSucNeedPer                   = big.NewInt(80)
	minRentSpace                    = new(big.Int).Mul(gbTob, big.NewInt(1))
	eb1b                            = new(big.Int).Mul(tb1b, big.NewInt(1048576))
	pb1b                            = new(big.Int).Mul(big.NewInt(1024), tb1b)
	BurnBase                        = big.NewInt(10000)
	BandwidthMakeupPunishDay        = uint64(30)
	bigInt20                        = big.NewInt(20)
	IncentivePeriod                 = big.NewInt(30)
	BandwidthAdjustPeriodDay        = uint64(7)
	minRentRewardRatio              = big.NewInt(4000) //25%
	bandwidthPunishLine             = big.NewInt(20)
	bandwidthAdjustThreshold        = big.NewInt(100)
	storageRewardGainRatio          = big.NewInt(11)
	storageRewardAdjRatio           = big.NewInt(200)  //0.02*10000
	storageRentPriceRatio           = big.NewInt(400)  //0.04*10000
	storageRentAdjRatio             = big.NewInt(8000) //0.8*10000
	storagePledgefactor             = decimal.NewFromFloat(0.4)
	rentLeftSpace                   = new(big.Int).Mul(gbTob, big.NewInt(5))
	initTotalLeaseSpace             = new(big.Int).Add(new(big.Int).Mul(eb1b, big.NewInt(10)), new(big.Int).SetUint64(uint64(692813346857858718)))
	sPPoollockDay                   = uint64(7)
	MinimumThresholdForPledgeAmount = big.NewInt(30)
	sPDistributionDefaultRate       = big.NewInt(10000)
	stpEntrustMinDay                = big.NewInt(7)
)

type StorageData struct {
	StoragePledge  map[common.Address]*SPledge  `json:"spledge"`
	Hash           common.Hash                  `json:"validhash"`
	StorageEntrust map[common.Address]*SEntrust `json:"sentrust"`
}

/*
*
Storage pledge struct
*/
type SPledge struct {
	Address                     common.Address         `json:"address"`
	StorageSpaces               *SPledgeSpaces         `json:"storagespaces"`
	Number                      *big.Int               `json:"number"`
	TotalCapacity               *big.Int               `json:"totalcapacity"`
	Bandwidth                   *big.Int               `json:"bandwidth"`
	Price                       *big.Int               `json:"price"`
	StorageSize                 *big.Int               `json:"storagesize"`
	SpaceDeposit                *big.Int               `json:"spacedeposit"`
	Lease                       map[common.Hash]*Lease `json:"lease"`
	LastVerificationTime        *big.Int               `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int               `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int               `json:"validationfailuretotaltime"`
	PledgeStatus                *big.Int               `json:"pledgeStatus"`
	Hash                        common.Hash            `json:"validhash"`
}

/**
 * Storage  space
 */
type SPledgeSpaces struct {
	Address                     common.Address               `json:"address"`
	StorageCapacity             *big.Int                     `json:"storagecapacity"`
	RootHash                    common.Hash                  `json:"roothash"`
	StorageFile                 map[common.Hash]*StorageFile `json:"storagefile"`
	LastVerificationTime        *big.Int                     `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int                     `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int                     `json:"validationfailuretotaltime"`
	Hash                        common.Hash                  `json:"validhash"`
}

/**
 *Rental structure
 */
type Lease struct {
	Address                     common.Address               `json:"address"`
	DepositAddress              common.Address               `json:"depositaddress"`
	Capacity                    *big.Int                     `json:"capacity"`
	RootHash                    common.Hash                  `json:"roothash"`
	Deposit                     *big.Int                     `json:"deposit"`
	UnitPrice                   *big.Int                     `json:"unitprice"`
	Cost                        *big.Int                     `json:"cost"`
	Duration                    *big.Int                     `json:"duration"`
	StorageFile                 map[common.Hash]*StorageFile `json:"storagefile"`
	LeaseList                   map[common.Hash]*LeaseDetail `json:"leaselist"`
	LastVerificationTime        *big.Int                     `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int                     `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int                     `json:"validationfailuretotaltime"`
	Status                      int                          `json:"status"`
	Hash                        common.Hash                  `json:"validhash"`
}

/**
 * Rental structure
 */
type StorageFile struct {
	Capacity                    *big.Int    `json:"capacity"`
	CreateTime                  *big.Int    `json:"createtime"`
	LastVerificationTime        *big.Int    `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int    `json:"lastverificationsuccesstime"`
	ValidationFailureTotalTime  *big.Int    `json:"validationfailuretotaltime"`
	Hash                        common.Hash `json:"validhash"`
}

/**
 *  Lease list
 */
type LeaseDetail struct {
	RequestHash                common.Hash `json:"requesthash"`
	PledgeHash                 common.Hash `json:"pledgehash"`
	RequestTime                *big.Int    `json:"requesttime"`
	StartTime                  *big.Int    `json:"starttime"`
	Duration                   *big.Int    `json:"duration"`
	Cost                       *big.Int    `json:"cost"`
	Deposit                    *big.Int    `json:"deposit"`
	ValidationFailureTotalTime *big.Int    `json:"validationfailuretotaltime"`
	Revert                     int         `json:"revert"`
	Hash                       common.Hash `json:"validhash"`
}
type SPledgeRecord struct {
	PledgeAddr      common.Address `json:"pledgeAddr"`
	Address         common.Address `json:"address"`
	Price           *big.Int       `json:"price"`
	SpaceDeposit    *big.Int       `json:"spacedeposit"`
	StorageCapacity *big.Int       `json:"storagecapacity"`
	StorageSize     *big.Int       `json:"storagesize"`
	RootHash        common.Hash    `json:"roothash"`
	PledgeNumber    *big.Int       `json:"pledgeNumber"`
	Bandwidth       *big.Int       `json:"bandwidth"`
}
type SPledgeExitRecord struct {
	Address      common.Address `json:"address"`
	PledgeStatus *big.Int       `json:"pledgeStatus"`
}

type LeaseRequestRecord struct {
	Tenant   common.Address `json:"tenant"`
	Address  common.Address `json:"address"`
	Capacity *big.Int       `json:"capacity"`
	Duration *big.Int       `json:"duration"`
	Price    *big.Int       `json:"price"`
	Hash     common.Hash    `json:"hash"`
}

type LeasePledgeRecord struct {
	Address        common.Address `json:"address"`
	DepositAddress common.Address `json:"depositaddress"`
	Hash           common.Hash    `json:"hash"`
	Capacity       *big.Int       `json:"capacity"`
	RootHash       common.Hash    `json:"roothash"`
	BurnSRTAmount  *big.Int       `json:"burnsrtamount"`
	BurnAmount     *big.Int       `json:"burnamount"`
	Duration       *big.Int       `json:"duration"`
	BurnSRTAddress common.Address `json:"burnsrtaddress"`
	PledgeHash     common.Hash    `json:"pledgehash"`
	LeftCapacity   *big.Int       `json:"leftcapacity"`
	LeftRootHash   common.Hash    `json:"leftroothash"`
}
type LeaseRenewalPledgeRecord struct {
	Address        common.Address `json:"address"`
	Hash           common.Hash    `json:"hash"`
	Capacity       *big.Int       `json:"capacity"`
	RootHash       common.Hash    `json:"roothash"`
	BurnSRTAmount  *big.Int       `json:"burnsrtamount"`
	BurnAmount     *big.Int       `json:"burnamount"`
	Duration       *big.Int       `json:"duration"`
	BurnSRTAddress common.Address `json:"burnsrtaddress"`
	PledgeHash     common.Hash    `json:"pledgehash"`
}

type LeaseRenewalRecord struct {
	Address  common.Address `json:"address"`
	Duration *big.Int       `json:"duration"`
	Hash     common.Hash    `json:"hash"`
	Price    *big.Int       `json:"price"`
	Tenant   common.Address `json:"tenant"`
	NewHash  common.Hash    `json:"newhash"`
	Capacity *big.Int       `json:"capacity"`
}

type LeaseRescindRecord struct {
	Address common.Address `json:"address"`
	Hash    common.Hash    `json:"hash"`
}
type SExpireRecord struct {
	Address common.Address `json:"address"`
	Hash    common.Hash    `json:"hash"`
}
type SPledgeRecoveryRecord struct {
	Address       common.Address `json:"address"`
	LeaseHash     []common.Hash  `json:"leaseHash"`
	SpaceCapacity *big.Int       `json:"spaceCapacity"`
	RootHash      common.Hash    `json:"rootHash"`
	ValidNumber   *big.Int       `json:"validNumber"`
}
type StorageProofRecord struct {
	Address                     common.Address `json:"address"`
	LeaseHash                   common.Hash    `json:"leaseHash"`
	RootHash                    common.Hash    `json:"rootHash"`
	LastVerificationTime        *big.Int       `json:"lastverificationtime"`
	LastVerificationSuccessTime *big.Int       `json:"lastverificationsuccesstime"`
}
type StorageExchangePriceRecord struct {
	Address common.Address `json:"address"`
	Price   *big.Int       `json:"price"`
}

type StorageRatio struct {
	Capacity *big.Int        `json:"capacity"`
	Ratio    decimal.Decimal `json:"ratio"`
}

type SpaceRewardRecord struct {
	Target  common.Address `json:"target"`
	Amount  *big.Int       `json:"amount"`
	Revenue common.Address `json:"revenue"`
}
type StorageExchangeBwRecord struct {
	Address   common.Address `json:"address"`
	Bandwidth *big.Int       `json:"bandwidth"`
}
type ExchangeSRTRecord struct {
	Target common.Address `json:"target"`
	Amount *big.Int       `json:"amount"`
}
type StorageBwPayRecord struct {
	Address common.Address `json:"address"`
	Amount  *big.Int       `json:"amount"`
}

type BandwidthMakeup struct {
	OldBandwidth  *big.Int `json:"oldbandwidth"`
	BurnRatio     *big.Int `json:"burnratio"`
	DepositMakeup *big.Int `json:"depositmakeup"`
	AdjustCount   uint64   `json:"adjustCount"`
}

type SEntrustDetail struct {
	Address common.Address `json:"address"`
	Height  *big.Int       `json:"height"`
	Amount  *big.Int       `json:"amount"`
}

type SEntrust struct {
	Manager       common.Address                  `json:"manager"`
	Sphash        common.Hash                     `json:"sphash"`
	Spheight      *big.Int                        `json:"spheight"`
	EntrustRate   *big.Int                        `json:"entrustRate"`
	PledgeAmount  *big.Int                        `json:"pledgeAmount"`
	ManagerAmount *big.Int                        `json:"managerAmount"`
	Managerheight *big.Int                        `json:"managerheight"`
	Detail        map[common.Hash]*SEntrustDetail `json:"detail"`
}

type ModifySManagerRecord struct {
	Pledge  common.Address `json:"pledge"`
	Manager common.Address `json:"manager"`
}
type CompleteSPledgeRecord struct {
	Pledge common.Address `json:"pledge"`
	Amount *big.Int       `json:"amount"`
	Hash   common.Hash    `json:"hash"`
}

type SPRewardRatioRecord struct {
	Pledge common.Address `json:"pledge"`
	Rate   *big.Int       `json:"rate"`
}
type SPPoolRecord struct {
	Pledge common.Address `json:"pledge"`
	Hash   common.Hash    `json:"hash"`
}

type SPMigrationRecord struct {
	Pledge   common.Address `json:"pledge"`
	RootHash common.Hash    `json:"hash"`
}

type SPledge2Record struct {
	PledgeAddr      common.Address `json:"pledgeAddr"`
	Address         common.Address `json:"address"`
	Price           *big.Int       `json:"price"`
	SpaceDeposit    *big.Int       `json:"spacedeposit"`
	StorageCapacity *big.Int       `json:"storagecapacity"`
	StorageSize     *big.Int       `json:"storagesize"`
	RootHash        common.Hash    `json:"roothash"`
	PledgeNumber    *big.Int       `json:"pledgeNumber"`
	Bandwidth       *big.Int       `json:"bandwidth"`
	PledgeAmount    *big.Int       `json:"pledgeAmount"`
	EntrustRate     *big.Int       `json:"entrustRate"`
	Hash            common.Hash    `json:"hash"`
}

type SPEntrustRecord struct {
	Target  common.Address
	Amount  *big.Int
	Address common.Address
	Hash    common.Hash
}
type SETransferRecord struct {
	Address    common.Address
	PledgeHash common.Hash
	Original   common.Address
	//PoS SN SP
	TargetType   string
	Target       common.Address
	TargetHash   common.Hash
	PledgeAmount *big.Int
	LockAmount   *big.Int
}
type SEExitRecord struct {
	Target  common.Address
	Hash    common.Hash
	Address common.Address
	Amount  *big.Int
}

func (a *Alien) processStorageCustomTx(txDataInfo []string, headerExtra HeaderExtra, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, snapCache *Snapshot, number *big.Int, state *state.StateDB, chain consensus.ChainHeaderReader) HeaderExtra {
	if txDataInfo[posCategory] == utgRentRequest {
		headerExtra.LeaseRequest = a.processRentRequest(headerExtra.LeaseRequest, txDataInfo, txSender, tx, receipts, snapCache, number.Uint64())
	} else if txDataInfo[posCategory] == utgSRTExch {
		headerExtra.ExchangeSRT = a.processExchangeSRT(headerExtra.ExchangeSRT, txDataInfo, txSender, tx, receipts, state, snapCache)
	} else if txDataInfo[posCategory] == utgStorageDeclare {
		if isGEInitStorageManagerNumber(number.Uint64()) {
			headerExtra.StoragePledge2 = a.declareStoragePledge2(headerExtra.StoragePledge2, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		} else {
			headerExtra.StoragePledge = a.declareStoragePledge(headerExtra.StoragePledge, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		}
	} else if txDataInfo[posCategory] == utgStorageExit {
		headerExtra.StoragePledgeExit, headerExtra.ExchangeSRT = a.storagePledgeExit(headerExtra.StoragePledgeExit, headerExtra.ExchangeSRT, txDataInfo, txSender, tx, receipts, state, snapCache, number)
	} else if txDataInfo[posCategory] == utgRentPg {
		headerExtra.LeasePledge = a.processLeasePledge(headerExtra.LeasePledge, txDataInfo, txSender, tx, receipts, state, snapCache, number.Uint64(), chain)
	} else if txDataInfo[posCategory] == utgRentReNew {
		headerExtra.LeaseRenewal = a.processLeaseRenewal(headerExtra.LeaseRenewal, txDataInfo, txSender, tx, receipts, state, snapCache, number.Uint64())
	} else if txDataInfo[posCategory] == utgRentReNewPg {
		headerExtra.LeaseRenewalPledge = a.processLeaseRenewalPledge(headerExtra.LeaseRenewalPledge, txDataInfo, txSender, tx, receipts, state, snapCache, number.Uint64(), chain)
	} else if txDataInfo[posCategory] == utgRentRescind {
		headerExtra.LeaseRescind, headerExtra.ExchangeSRT = a.processLeaseRescind(headerExtra.LeaseRescind, headerExtra.ExchangeSRT, txDataInfo, txSender, tx, receipts, state, snapCache, number.Uint64())
	} else if txDataInfo[posCategory] == utgStorageRecoverValid {
		headerExtra.StorageRecoveryData = a.storageRecoveryCertificate(headerExtra.StorageRecoveryData, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	} else if txDataInfo[posCategory] == utgStorageProof {
		headerExtra.StorageProofRecord = a.applyStorageProof(headerExtra.StorageProofRecord, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
	} else if txDataInfo[posCategory] == utgStoragePrice {
		headerExtra.StorageExchangePrice = a.exchangeStoragePrice(headerExtra.StorageExchangePrice, txDataInfo, txSender, tx, receipts, state, snapCache, number)

	} else if txDataInfo[posCategory] == utgStorageBw {
		if a.changeBandwidthEnable(number.Uint64()) {
			headerExtra.StorageExchangeBw, headerExtra.StorageBwPay = a.changeStorageBandwidth(headerExtra.StorageExchangeBw, headerExtra.StorageBwPay, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
	} else if txDataInfo[posCategory] == utgStoragePledgeCatchUp {
		if a.isEffectPayPledge(number.Uint64()) {
			headerExtra.StorageBwPay = a.payStorageBWPledge(headerExtra.StorageBwPay, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
	}
	if isGEInitStorageManagerNumber(number.Uint64()) {
		if txDataInfo[posCategory] == utgStoragePledgeEditmgaddr {
			headerExtra.ModifySManager = a.modifyStorageManager(headerExtra.ModifySManager, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
		if txDataInfo[posCategory] == utgStoragePledgeStchpg {
			headerExtra.CompleteSPledge = a.completeStoragePledge(headerExtra.CompleteSPledge, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
		if txDataInfo[posCategory] == utgStoragePledgeStwtreward {
			headerExtra.SPRewardRatio = a.storageSetRewardRatio(headerExtra.SPRewardRatio, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
		if txDataInfo[posCategory] == utgStoragePledgeSetsp {
			headerExtra.SPPool = a.storageSetStoragePools(headerExtra.SPPool, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
		if txDataInfo[posCategory] == utgStoragePledgeExitsp {
			headerExtra.SPEPool = a.storageExitPool(headerExtra.SPEPool, txDataInfo, txSender, tx, receipts, state, snapCache, number)
		}
		if txDataInfo[posCategory] == utgStoragePledgeStreplace {
			headerExtra.SPMigration, headerExtra.LockReward, headerExtra.ExchangeSRT = a.storageMigration(headerExtra.SPMigration, headerExtra.LockReward, headerExtra.ExchangeSRT, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		}
		if txDataInfo[posCategory] == utgStoragePledgeStwtpg {
			headerExtra.SPEntrust = a.storageSPEntrust(headerExtra.SPEntrust, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		}
		if txDataInfo[posCategory] == utgStoragePledgeWtfd {
			headerExtra.SETransfer = a.storageEntrustedPledgeTransfer(headerExtra.SETransfer, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		}
		if txDataInfo[posCategory] == utgStoragePledgeWtpgexit {
			headerExtra.SEExit = a.storageEntrustedPledgeExit(headerExtra.SEExit, txDataInfo, txSender, tx, receipts, state, snapCache, number, chain)
		}
	}
	return headerExtra
}
func (snap *Snapshot) storageApply(headerExtra HeaderExtra, header *types.Header, db ethdb.Database) (*Snapshot, error) {
	calsnap, err := snap.calStorageVerificationCheck(headerExtra.StorageDataRoot, header.Number.Uint64(), snap.getBlockPreDay(), db, header)
	if err != nil {
		log.Error("calStorageVerificationCheck", "err", err)
		return calsnap, err
	}
	if isGEInitStorageManagerNumber(header.Number.Uint64()) {
		if isSpVerificationCheck(header.Number.Uint64(), snap.Period) {
			snap.spAccumulatePublish(header.Number)
		}
		if isSpDelExit(header.Number.Uint64(), snap.Period) {
			snap.dealSpExit(header.Number, db, header.Hash())
		}
		calRootHash := snap.SpData.Hash
		if calRootHash != headerExtra.SpDataRoot {
			err := errors.New("SpDataRoot root hash is not same,head:" + headerExtra.SpDataRoot.String() + "cal:" + calRootHash.String())
			log.Error("SpDataRoot root hash is not same:", "head", headerExtra.SpDataRoot.String(), "cal", calRootHash.String())
			return calsnap, err
		}
	}
	calsnap2, err2 := snap.calSRTHashVer(headerExtra.SRTDataRoot, header.Number.Uint64(), db)
	if err2 != nil {
		log.Error("calSRTHash apply", "err", err)
		return calsnap2, err2
	}
	snap.updateExchangeSRT(headerExtra.ExchangeSRT, header.Number, db)
	snap.updateStorageData(headerExtra.StoragePledge, db)
	snap.updateStoragePledgeExit(headerExtra.StoragePledgeExit, header.Number, db)
	snap.updateLeaseRequest(headerExtra.LeaseRequest, header.Number, db)
	snap.updateLeasePledge(headerExtra.LeasePledge, header.Number, db)
	snap.updateLeaseRenewal(headerExtra.LeaseRenewal, header.Number, db)
	snap.updateLeaseRenewalPledge(headerExtra.LeaseRenewalPledge, header.Number, db)
	snap.updateLeaseRescind(headerExtra.LeaseRescind, header.Number, db)
	snap.updateStorageRecoveryData(headerExtra.StorageRecoveryData, header.Number, db)
	snap.updateStorageProof(headerExtra.StorageProofRecord, header.Number, db)
	snap.updateStoragePrice(headerExtra.StorageExchangePrice, header.Number, db)
	snap.updateBwPledgePayData(headerExtra.StorageBwPay, header.Number, db)
	if header.Number.Uint64() == PosrIncentiveEffectNumber {
		snap.adjustStorageOldPrice()
	}
	if isGEInitStorageManagerNumber(header.Number.Uint64()) {
		snap.updateStorageManager(headerExtra.ModifySManager, header.Number, db)
		snap.updateCompleteSPledge(headerExtra.CompleteSPledge, header.Number, db)
		snap.updateSPRewardRatio(headerExtra.SPRewardRatio, header.Number, db)
		snap.updateSPPool(headerExtra.SPPool, header.Number, db)
		snap.updateSPEPool(headerExtra.SPEPool, header.Number, db)
		snap.updateSPMigration(headerExtra.SPMigration, header.Number, db)
		snap.updateStorageData2(headerExtra.StoragePledge2, db)
		snap.updateSPEntrust(headerExtra.SPEntrust, header.Number, db)
		snap.updateSETransfer(headerExtra.SETransfer, header.Number, db)
		snap.updateSEExit(headerExtra.SEExit, header.Number, db)
	}
	return snap, nil
}
func (s *StorageData) checkSRent(sRent []LeaseRequestRecord, rent LeaseRequestRecord, number uint64) bool {
	if _, ok := s.StoragePledge[rent.Address]; !ok {
		log.Info("checkSRent", "address not exist", rent.Address)
		return false
	}
	if s.StoragePledge[rent.Address].PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 {
		log.Info("checkSRent", "address PledgeStatus is not normal", rent.Address)
		return false
	}
	//check Capacity
	rentCapacity := new(big.Int).Set(rent.Capacity)
	for _, item := range sRent {
		if item.Address == rent.Address {
			rentCapacity = new(big.Int).Add(rentCapacity, item.Capacity)
		}
	}
	storageSpaces := s.StoragePledge[rent.Address].StorageSpaces
	if isGEPosAutoExitPunishChange(number) {
		rentCapacity = new(big.Int).Add(rentCapacity, rentLeftSpace)
		if storageSpaces.StorageCapacity.Cmp(rentCapacity) < 0 {
			log.Info("checkSRent", "rentCapacity add rentLeftSpace is greater than storageSpaces", rentCapacity)
			return false
		}
	} else {
		if storageSpaces.StorageCapacity.Cmp(rentCapacity) < 0 {
			log.Info("checkSRent", "rentCapacity is greater than storageSpaces", rentCapacity)
			return false
		}
	}
	if number >= StoragePledgeOptEffectNumber {
		price := s.StoragePledge[rent.Address].Price
		if rent.Price.Cmp(price) != 0 {
			log.Info("checkSRent", "price is not equal", rent.Price)
			return false
		}
	}
	return true
}

func (s *StorageData) updateLeaseRequest(sRent []LeaseRequestRecord, number *big.Int, db ethdb.Database) {
	for _, item := range sRent {
		if _, ok := s.StoragePledge[item.Address]; !ok {
			continue
		}
		spledge, _ := s.StoragePledge[item.Address]
		if _, ok := spledge.Lease[item.Hash]; !ok {
			zero := big.NewInt(0)
			leaseDetail := LeaseDetail{
				RequestHash:                item.Hash,
				PledgeHash:                 common.Hash{},
				RequestTime:                number,
				StartTime:                  big.NewInt(0),
				Duration:                   item.Duration,
				Cost:                       zero,
				Deposit:                    zero,
				ValidationFailureTotalTime: big.NewInt(0),
			}
			LeaseList := make(map[common.Hash]*LeaseDetail)
			LeaseList[item.Hash] = &leaseDetail
			spledge.Lease[item.Hash] = &Lease{
				Address:                     item.Tenant,
				Capacity:                    item.Capacity,
				RootHash:                    common.Hash{},
				Deposit:                     zero,
				UnitPrice:                   item.Price,
				Cost:                        zero,
				Duration:                    zero,
				StorageFile:                 make(map[common.Hash]*StorageFile),
				LeaseList:                   LeaseList,
				LastVerificationTime:        zero,
				LastVerificationSuccessTime: zero,
				ValidationFailureTotalTime:  zero,
				Status:                      LeaseNotPledged,
			}
			s.accumulateLeaseDetailHash(item.Address, item.Hash, LeaseList[item.Hash])
		}
	}
	s.accumulateHeaderHash()
}
func (s *StorageData) checkSRentPg(currentSRentPg []LeasePledgeRecord, sRentPg LeasePledgeRecord, txSender common.Address, revenueStorage map[common.Address]*RevenueParameter, exchRate uint32, passTime *big.Int, number uint64) (*big.Int, *big.Int, *big.Int, common.Address, bool) {
	nilHash := common.Address{}
	for _, item := range currentSRentPg {
		if item.Address == sRentPg.Address {
			log.Info("checkSRentPg", "rent pledge only one in one block", sRentPg.Address)
			return nil, nil, nil, nilHash, false
		}
	}
	//checkCapacity
	if _, ok := s.StoragePledge[sRentPg.Address]; !ok {
		log.Info("checkSRentPg", "address not exist", sRentPg.Address)
		return nil, nil, nil, nilHash, false
	}
	if _, ok := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]; !ok {
		log.Info("checkSRentPg", "hash not exist", sRentPg.Hash)
		return nil, nil, nil, nilHash, false
	}
	lease := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]
	if lease.Capacity.Cmp(sRentPg.Capacity) != 0 {
		log.Info("checkSRentPg", "lease Capacity is not equal", sRentPg.Capacity)
		return nil, nil, nil, nilHash, false
	}
	storageCapacity := s.StoragePledge[sRentPg.Address].StorageSpaces.StorageCapacity
	leftCapacity := new(big.Int).Sub(storageCapacity, sRentPg.Capacity)
	if leftCapacity.Cmp(common.Big0) < 0 { //can be 0
		log.Info("checkSRentPg", "LeftCapacity is less than 0", leftCapacity)
		return nil, nil, nil, nilHash, false
	}
	if isGEPosAutoExitPunishChange(number) {
		if leftCapacity.Cmp(rentLeftSpace) < 0 {
			log.Warn("checkSRentPg", "LeftCapacity less rentLeftSpace", sRentPg.Capacity)
			return nil, nil, nil, nilHash, false
		}
	}
	if leftCapacity.Cmp(sRentPg.LeftCapacity) != 0 {
		log.Info("checkSRentPg", "LeftCapacity is not equal", sRentPg.LeftCapacity)
		return nil, nil, nil, nilHash, false
	}
	if lease.Deposit.Cmp(big.NewInt(0)) > 0 {
		log.Info("checkSRentPg", "Deposit is greater than 0", lease.Deposit)
		return nil, nil, nil, nilHash, false
	}
	//checkowner
	sRentPg.DepositAddress = txSender
	//Calculate the pledge deposit
	leaseDetail := lease.LeaseList[sRentPg.Hash]
	requestTime := leaseDetail.RequestTime
	requestPassTime := new(big.Int).Add(requestTime, passTime)
	if requestPassTime.Cmp(new(big.Int).SetUint64(number)) < 0 {
		log.Info("checkSRentPg", "request time pass", requestTime)
		return nil, nil, nil, nilHash, false
	}
	srtAmount := new(big.Int).Mul(leaseDetail.Duration, lease.UnitPrice)
	srtAmount = new(big.Int).Mul(srtAmount, lease.Capacity)
	srtAmount = new(big.Int).Div(srtAmount, gbTob)
	amount := new(big.Int).Div(new(big.Int).Mul(srtAmount, big.NewInt(10000)), big.NewInt(int64(exchRate)))
	return srtAmount, amount, leaseDetail.Duration, lease.Address, true
}

func (s *StorageData) updateLeasePledge(pg []LeasePledgeRecord, number *big.Int, db ethdb.Database) {
	for _, sRentPg := range pg {
		if _, ok := s.StoragePledge[sRentPg.Address]; !ok {
			continue
		}
		if _, ok := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]; !ok {
			continue
		}
		lease := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]
		lease.RootHash = sRentPg.RootHash
		lease.Deposit = new(big.Int).Add(lease.Deposit, sRentPg.BurnAmount)
		lease.Cost = new(big.Int).Add(lease.Cost, sRentPg.BurnSRTAmount)
		lease.Duration = new(big.Int).Add(lease.Duration, sRentPg.Duration)
		if _, ok := lease.StorageFile[sRentPg.RootHash]; !ok {
			lease.StorageFile[sRentPg.RootHash] = &StorageFile{
				Capacity:                    lease.Capacity,
				CreateTime:                  number,
				LastVerificationTime:        number,
				LastVerificationSuccessTime: number,
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			s.accumulateLeaseStorageFileHash(sRentPg.Address, sRentPg.Hash, lease.StorageFile[sRentPg.RootHash])
		}
		leaseDetail := lease.LeaseList[sRentPg.Hash]
		leaseDetail.Cost = new(big.Int).Add(leaseDetail.Cost, sRentPg.BurnSRTAmount)
		leaseDetail.Deposit = new(big.Int).Add(leaseDetail.Deposit, sRentPg.BurnAmount)
		leaseDetail.PledgeHash = sRentPg.PledgeHash
		leaseDetail.StartTime = number
		lease.LastVerificationTime = number
		lease.LastVerificationSuccessTime = number
		lease.DepositAddress = sRentPg.DepositAddress
		lease.Status = LeaseNormal
		s.accumulateLeaseDetailHash(sRentPg.Address, sRentPg.Hash, leaseDetail)
		storageSpaces := s.StoragePledge[sRentPg.Address].StorageSpaces
		storageSpaces.StorageCapacity = sRentPg.LeftCapacity
		if sRentPg.LeftCapacity.Cmp(common.Big0) == 0 {
			storageSpaces.RootHash = common.Hash{}
			storageSpaces.StorageFile = make(map[common.Hash]*StorageFile, 0)
			s.accumulateSpaceHash(sRentPg.Address)
		} else {
			storageSpaces.RootHash = sRentPg.LeftRootHash
			storageSpaces.StorageFile = make(map[common.Hash]*StorageFile, 1)
			storageSpaces.StorageFile[sRentPg.LeftRootHash] = &StorageFile{
				Capacity:                    sRentPg.LeftCapacity,
				CreateTime:                  number,
				LastVerificationTime:        number,
				LastVerificationSuccessTime: number,
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			s.accumulateSpaceStorageFileHash(sRentPg.Address, storageSpaces.StorageFile[sRentPg.LeftRootHash])
		}
	}
	s.accumulateHeaderHash()
}
func (s *StorageData) checkSRentReNew(currentSRentReNew []LeaseRenewalRecord, sRentReNew LeaseRenewalRecord, txSender common.Address, number uint64, blockPerday uint64) (common.Address, bool) {
	nilHash := common.Address{}
	if _, ok := s.StoragePledge[sRentReNew.Address]; !ok {
		log.Info("checkSRentReNew", "address not exist", sRentReNew.Address)
		return nilHash, false
	}
	if s.StoragePledge[sRentReNew.Address].PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 {
		log.Info("checkSRentReNew", "address PledgeStatus is not normal", sRentReNew.Address)
		return nilHash, false
	}
	if _, ok := s.StoragePledge[sRentReNew.Address].Lease[sRentReNew.Hash]; !ok {
		log.Info("checkSRentReNew", "hash not exist", sRentReNew.Hash)
		return nilHash, false
	}
	lease := s.StoragePledge[sRentReNew.Address].Lease[sRentReNew.Hash]
	if lease.Address != txSender {
		log.Info("checkSRentReNew", "txSender is not lease renter", txSender)
		return nilHash, false
	}
	if _, ok := lease.LeaseList[sRentReNew.Hash]; !ok {
		log.Info("checkSRentReNew", "LeaseList hash not exist", sRentReNew.Hash)
		return nilHash, false
	}
	if lease.Status == LeaseNotPledged || lease.Status == LeaseUserRescind || lease.Status == LeaseExpiration || lease.Status == LeaseReturn {
		log.Info("checkSRentReNew", "lease Status can not renew", lease.Status)
		return nilHash, false
	}
	for _, rentnew := range currentSRentReNew {
		if rentnew.Hash == sRentReNew.Hash {
			log.Info("checkSRentReNew", "rent Hash only one in one block", sRentReNew.Hash)
			return nilHash, false
		}
	}
	for _, detail := range lease.LeaseList {
		if detail.Deposit.Cmp(big.NewInt(0)) <= 0 {
			log.Info("checkSRentReNew", "has not Deposit lease", detail.Deposit)
			return nilHash, false
		}
	}
	startTime := big.NewInt(0)
	duration := big.NewInt(0)
	for _, leaseDetail := range lease.LeaseList {
		if leaseDetail.Deposit.Cmp(big.NewInt(0)) > 0 && leaseDetail.StartTime.Cmp(startTime) > 0 {
			startTime = leaseDetail.StartTime
			duration = new(big.Int).Mul(leaseDetail.Duration, new(big.Int).SetUint64(blockPerday))
		}
	}
	if startTime.Cmp(big.NewInt(0)) == 0 {
		log.Info("checkSRentReNew", "startTime is 0 ", startTime)
		return nilHash, false
	}
	duration90 := new(big.Int).Mul(duration, big.NewInt(rentRenewalExpires))
	duration90 = new(big.Int).Div(duration90, big.NewInt(100))
	reNewNumber := new(big.Int).Add(startTime, duration90)

	fStartTime := lease.LeaseList[sRentReNew.Hash].StartTime
	if fStartTime == nil || fStartTime.Cmp(common.Big0) == 0 {
		log.Info("checkSRentReNew", "fStartTime is zero ", fStartTime)
		return nilHash, false
	}
	lEndNumber := new(big.Int).Add(startTime, duration)
	if fStartTime.Cmp(startTime) != 0 {
		reNewNumber = new(big.Int).Sub(reNewNumber, common.Big1)
		lEndNumber = new(big.Int).Sub(lEndNumber, common.Big1)
	}
	if reNewNumber.Cmp(new(big.Int).SetUint64(number)) > 0 {
		log.Info("checkSRentReNew", "duration is not enough ", reNewNumber)
		return nilHash, false
	}

	if lEndNumber.Cmp(new(big.Int).SetUint64(number)) < 0 {
		log.Info("checkSRentReNew", "duration is pass ", lEndNumber)
		return nilHash, false
	}
	return lease.Address, true
}

func (s *StorageData) updateLeaseRenewal(reNew []LeaseRenewalRecord, number *big.Int, db ethdb.Database, blockPerDay uint64) {
	for _, item := range reNew {
		if _, ok := s.StoragePledge[item.Address]; !ok {
			continue
		}
		spledge, _ := s.StoragePledge[item.Address]
		if lease, ok := spledge.Lease[item.Hash]; ok {
			zero := big.NewInt(0)
			leaseDetail := LeaseDetail{
				RequestHash:                item.NewHash,
				PledgeHash:                 common.Hash{},
				RequestTime:                number,
				StartTime:                  big.NewInt(0),
				Duration:                   item.Duration,
				Cost:                       zero,
				Deposit:                    zero,
				ValidationFailureTotalTime: zero,
			}
			LeaseList := lease.LeaseList
			LeaseList[item.NewHash] = &leaseDetail
			s.accumulateLeaseDetailHash(item.Address, item.Hash, LeaseList[item.NewHash])
		}
	}
	s.accumulateHeaderHash()
}
func NewStorageSnap() *StorageData {
	return &StorageData{
		StoragePledge: make(map[common.Address]*SPledge),
	}
}
func (s *StorageData) copy() *StorageData {
	clone := &StorageData{
		StoragePledge: make(map[common.Address]*SPledge),
		Hash:          s.Hash,
	}
	for address, spledge := range s.StoragePledge {
		clone.StoragePledge[address] = &SPledge{
			Address: spledge.Address,
			StorageSpaces: &SPledgeSpaces{
				Address:                     spledge.StorageSpaces.Address,
				StorageCapacity:             new(big.Int).Set(spledge.StorageSpaces.StorageCapacity),
				RootHash:                    spledge.StorageSpaces.RootHash,
				StorageFile:                 make(map[common.Hash]*StorageFile),
				LastVerificationTime:        new(big.Int).Set(spledge.StorageSpaces.LastVerificationTime),
				LastVerificationSuccessTime: new(big.Int).Set(spledge.StorageSpaces.LastVerificationSuccessTime),
				ValidationFailureTotalTime:  new(big.Int).Set(spledge.StorageSpaces.ValidationFailureTotalTime),
				Hash:                        spledge.StorageSpaces.Hash,
			},
			Number:                      new(big.Int).Set(spledge.Number),
			TotalCapacity:               new(big.Int).Set(spledge.TotalCapacity),
			Bandwidth:                   new(big.Int).Set(spledge.Bandwidth),
			Price:                       new(big.Int).Set(spledge.Price),
			StorageSize:                 new(big.Int).Set(spledge.StorageSize),
			SpaceDeposit:                new(big.Int).Set(spledge.SpaceDeposit),
			Lease:                       make(map[common.Hash]*Lease),
			LastVerificationTime:        new(big.Int).Set(spledge.LastVerificationTime),
			LastVerificationSuccessTime: new(big.Int).Set(spledge.LastVerificationSuccessTime),
			ValidationFailureTotalTime:  new(big.Int).Set(spledge.ValidationFailureTotalTime),
			PledgeStatus:                new(big.Int).Set(spledge.PledgeStatus),
			Hash:                        spledge.Hash,
		}

		storageFiles := s.StoragePledge[address].StorageSpaces.StorageFile
		for hash, storageFile := range storageFiles {
			if _, ok := clone.StoragePledge[address].StorageSpaces.StorageFile[hash]; !ok {
				clone.StoragePledge[address].StorageSpaces.StorageFile[hash] = &StorageFile{
					Capacity:                    new(big.Int).Set(storageFile.Capacity),
					CreateTime:                  new(big.Int).Set(storageFile.CreateTime),
					LastVerificationTime:        new(big.Int).Set(storageFile.LastVerificationTime),
					LastVerificationSuccessTime: new(big.Int).Set(storageFile.LastVerificationSuccessTime),
					ValidationFailureTotalTime:  new(big.Int).Set(storageFile.ValidationFailureTotalTime),
					Hash:                        storageFile.Hash,
				}
			}
		}
		leases := s.StoragePledge[address].Lease
		for hash, lease := range leases {
			if _, ok := clone.StoragePledge[address].Lease[hash]; !ok {
				clone.StoragePledge[address].Lease[hash] = &Lease{
					Address:                     lease.Address,
					DepositAddress:              lease.DepositAddress,
					Capacity:                    new(big.Int).Set(lease.Capacity),
					RootHash:                    lease.RootHash,
					Deposit:                     new(big.Int).Set(lease.Deposit),
					UnitPrice:                   new(big.Int).Set(lease.UnitPrice),
					Cost:                        new(big.Int).Set(lease.Cost),
					Duration:                    new(big.Int).Set(lease.Duration),
					StorageFile:                 make(map[common.Hash]*StorageFile),
					LeaseList:                   make(map[common.Hash]*LeaseDetail),
					LastVerificationTime:        new(big.Int).Set(lease.LastVerificationTime),
					LastVerificationSuccessTime: new(big.Int).Set(lease.LastVerificationSuccessTime),
					ValidationFailureTotalTime:  new(big.Int).Set(lease.ValidationFailureTotalTime),
					Status:                      lease.Status,
					Hash:                        lease.Hash,
				}

				storageFiles2 := lease.StorageFile
				cloneSF := clone.StoragePledge[address].Lease[hash]
				for hash2, storageFile2 := range storageFiles2 {
					if _, ok2 := cloneSF.StorageFile[hash2]; !ok2 {
						cloneSF.StorageFile[hash2] = &StorageFile{
							Capacity:                    new(big.Int).Set(storageFile2.Capacity),
							CreateTime:                  new(big.Int).Set(storageFile2.CreateTime),
							LastVerificationTime:        new(big.Int).Set(storageFile2.LastVerificationTime),
							LastVerificationSuccessTime: new(big.Int).Set(storageFile2.LastVerificationSuccessTime),
							ValidationFailureTotalTime:  new(big.Int).Set(storageFile2.ValidationFailureTotalTime),
							Hash:                        storageFile2.Hash,
						}
					}
				}

				leaseLists := lease.LeaseList
				cloneLease := clone.StoragePledge[address].Lease[hash]
				for hash3, leaseDetail3 := range leaseLists {
					if _, ok2 := cloneLease.LeaseList[hash3]; !ok2 {
						cloneLease.LeaseList[hash3] = &LeaseDetail{
							RequestHash:                leaseDetail3.RequestHash,
							PledgeHash:                 leaseDetail3.PledgeHash,
							RequestTime:                new(big.Int).Set(leaseDetail3.RequestTime),
							StartTime:                  new(big.Int).Set(leaseDetail3.StartTime),
							Duration:                   new(big.Int).Set(leaseDetail3.Duration),
							Cost:                       new(big.Int).Set(leaseDetail3.Cost),
							Deposit:                    new(big.Int).Set(leaseDetail3.Deposit),
							ValidationFailureTotalTime: new(big.Int).Set(leaseDetail3.ValidationFailureTotalTime),
							Revert:                     leaseDetail3.Revert,
							Hash:                       leaseDetail3.Hash,
						}
					}
				}
			}
		}
	}
	if s.StorageEntrust != nil {
		clone.StorageEntrust = make(map[common.Address]*SEntrust)
		for address, sentrust := range s.StorageEntrust {
			clone.StorageEntrust[address] = &SEntrust{
				Manager:       sentrust.Manager,
				Sphash:        sentrust.Sphash,
				Spheight:      new(big.Int).Set(sentrust.Spheight),
				EntrustRate:   new(big.Int).Set(sentrust.EntrustRate),
				PledgeAmount:  new(big.Int).Set(sentrust.PledgeAmount),
				ManagerAmount: new(big.Int).Set(sentrust.ManagerAmount),
				Managerheight: new(big.Int).Set(sentrust.Managerheight),
				Detail:        make(map[common.Hash]*SEntrustDetail),
			}

			entrustDetail := s.StorageEntrust[address].Detail
			for hash, detail := range entrustDetail {
				if _, ok := clone.StorageEntrust[address].Detail[hash]; !ok {
					clone.StorageEntrust[address].Detail[hash] = &SEntrustDetail{
						Address: detail.Address,
						Height:  new(big.Int).Set(detail.Height),
						Amount:  new(big.Int).Set(detail.Amount),
					}
				}
			}
		}
	}

	return clone
}

func (a *Alien) declareStoragePledge(currStoragePledge []SPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SPledgeRecord {
	if len(txDataInfo) < 11 {
		log.Warn("declareStoragePledge", "parameter error len=", len(txDataInfo))
		return currStoragePledge
	}
	peledgeAddr := common.HexToAddress(txDataInfo[3])
	if _, ok := snap.StorageData.StoragePledge[peledgeAddr]; ok {
		log.Warn("Storage Pledge repeat", " peledgeAddr", peledgeAddr)
		return currStoragePledge
	}
	var bigPrice *big.Int
	if price, err := decimal.NewFromString(txDataInfo[4]); err != nil {
		log.Warn("Storage Pledge price wrong", "price", txDataInfo[4])
		return currStoragePledge
	} else {
		bigPrice = price.BigInt()
	}
	basePrice := decimal.NewFromBigInt(snap.SystemConfig.Deposit[sscEnumStoragePrice], 0)
	minPrice := basePrice.BigInt()
	maxPrice := basePrice.Mul(decimal.NewFromInt(10)).BigInt()
	if blocknumber.Uint64() >= PledgeRevertLockEffectNumber {
		minPrice = (basePrice.Mul(decimal.NewFromFloat(0.1))).BigInt()
	}
	if bigPrice.Cmp(minPrice) < 0 || bigPrice.Cmp(maxPrice) > 0 {
		log.Warn("price is set too high", " price", bigPrice)
		return currStoragePledge
	}
	storageCapacity, err := decimal.NewFromString(txDataInfo[5])
	if err != nil {
		log.Warn("Storage Pledge storageCapacity format error", "storageCapacity", txDataInfo[5])
		return currStoragePledge
	}
	maxPledgeCapacity := maxPledgeStorageCapacity
	if blocknumber.Uint64() >= StorageChBwEffectNumber {
		maxPledgeCapacity = maxPledgeStorageCapacityV1
	}
	if blocknumber.Uint64() >= PosNewEffectNumber {
		maxPledgeCapacity = maxPledgeStorageCapacityV2
	}
	minStorageCapacity := minPledgeStorageCapacity
	if blocknumber.Uint64() >= 101900 {
		minStorageCapacity = decimal.NewFromInt(1)
	}
	if storageCapacity.Cmp(minStorageCapacity) < 0 || storageCapacity.Cmp(maxPledgeCapacity) > 0 {
		log.Warn("Storage Pledge storageCapacity error", "storageCapacity", storageCapacity, "minPledgeStorageCapacity", minPledgeStorageCapacity, "maxPledgeStorageCapacity", maxPledgeStorageCapacity)
		return currStoragePledge
	}
	startPkNumber := txDataInfo[6]
	pkNonce, err := decimal.NewFromString(txDataInfo[7])
	if err != nil {
		log.Warn("Storage Pledge package nonce error", "pkNonce", txDataInfo[7])
		return currStoragePledge
	}
	pkBlockHash := txDataInfo[8]
	verifyData := txDataInfo[9]
	verifyType := ""
	if blocknumber.Uint64() >= storageVerifyNewEffectNumber {
		if strings.HasPrefix(verifyData, "v1") {
			verifyType = "v1"
			verifyData = verifyData[3:]
		}
	}
	verifyDataArr := strings.Split(verifyData, ",")
	if len(verifyDataArr) < 10 {
		log.Warn("Storage Pledge verifyData format error", "verifyData", verifyData, "verifyDataArr", verifyDataArr)
		return currStoragePledge
	}
	if !a.notVerifyPkHeader(blocknumber.Uint64()) {
		pkHeader := chain.GetHeaderByHash(common.HexToHash(pkBlockHash))
		if pkHeader == nil {
			log.Warn("Storage Pledge", "pkBlockHash is not exist", pkBlockHash)
			return currStoragePledge
		}
		if verifyDataArr[4] != storageBlockSize {
			log.Warn("Storage Pledge storageBlockSize error", "storageBlockSize", storageBlockSize, "verifyDataArr[4]", verifyDataArr[4])
			return currStoragePledge
		}
		if pkHeader.Number.String() != startPkNumber || pkHeader.Nonce.Uint64() != pkNonce.BigInt().Uint64() {
			log.Warn("Storage Pledge  packege param compare error", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash, " chain", pkHeader.Number)

			return currStoragePledge
		}
	}
	rootHash := verifyDataArr[len(verifyDataArr)-1]
	if verifyType == "v1" {
		if !verifyPocStringV1(startPkNumber, txDataInfo[7], pkBlockHash, txDataInfo[9], rootHash, txDataInfo[3]) {
			log.Warn("Storage Pledge  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currStoragePledge
		}
	} else {
		if !verifyPocString(startPkNumber, txDataInfo[7], pkBlockHash, verifyData, rootHash, txDataInfo[3]) {
			log.Warn("Storage Pledge  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currStoragePledge
		}
	}

	storageSize, err := decimal.NewFromString(verifyDataArr[4])
	if err != nil || storageSize.Cmp(decimal.Zero) <= 0 {
		log.Warn("Storage Pledge storageSize format error", "storageSize", verifyDataArr[4])
		return currStoragePledge
	}
	if blocknumber.Uint64() >= SPledgeRevertFixBlockNumber {
		blocknum, err := decimal.NewFromString(verifyDataArr[5])
		if err != nil || blocknum.Cmp(decimal.Zero) <= 0 {
			log.Warn("Storage Pledge blocknum format error", "blocknum", verifyDataArr[5])
			return currStoragePledge
		}
		actblocknum := storageCapacity.Div(storageSize)
		if actblocknum.Cmp(blocknum) != 0 {
			log.Warn("Storage Pledge storageCapacity not same in verify", "actblocknum", actblocknum, "blocknum", blocknum.Mul(storageSize))
			return currStoragePledge
		}
	}

	bandwidth, err := decimal.NewFromString(txDataInfo[10])

	if err != nil || bandwidth.BigInt().Cmp(big.NewInt(0)) <= 0 {
		log.Warn("Storage Pledge  bandwidth error", "bandwidth", bandwidth)
		return currStoragePledge
	}

	if err := a.checkPledgeMaxStorageSpace(currStoragePledge, peledgeAddr, snap, blocknumber, storageCapacity.BigInt()); err != nil {
		log.Warn("Storage Pledge", "checkRevenueStorageBind", err.Error())
		return currStoragePledge
	}
	totalStorage := big.NewInt(0)
	for _, spledge := range snap.StorageData.StoragePledge {
		totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
	}
	pledgeAmount := big.NewInt(0)
	if blocknumber.Uint64() < StoragePledgeOptEffectNumber {
		pledgeAmount = calStPledgeAmount(storageCapacity, snap, decimal.NewFromBigInt(totalStorage, 0), blocknumber)
	} else {
		pledgeAmount = getSotragePledgeAmount(storageCapacity, bandwidth, decimal.NewFromBigInt(totalStorage, 0), blocknumber, snap)
	}

	if state.GetBalance(txSender).Cmp(pledgeAmount) < 0 {
		log.Warn("Claimed sotrage", "balance", state.GetBalance(txSender))
		return currStoragePledge
	}
	state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), pledgeAmount))
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x6d385a58ea1e7560a01c5a9d543911d47c1b86c5899c0b2df932dab4d7c2f323"))
	topics[1].SetBytes(peledgeAddr.Bytes())
	topics[2].SetBytes(pledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	storageRecord := SPledgeRecord{
		PledgeAddr:      txSender,
		Address:         peledgeAddr,
		Price:           bigPrice,
		SpaceDeposit:    pledgeAmount,
		StorageCapacity: storageCapacity.BigInt(),
		StorageSize:     storageSize.BigInt(),
		RootHash:        common.HexToHash(rootHash),
		PledgeNumber:    blocknumber,
		Bandwidth:       bandwidth.BigInt(),
	}
	currStoragePledge = append(currStoragePledge, storageRecord)
	return currStoragePledge
}
func (s *Snapshot) updateStorageData(pledgeRecord []SPledgeRecord, db ethdb.Database) {
	if pledgeRecord == nil || len(pledgeRecord) == 0 {
		return
	}
	for _, record := range pledgeRecord {
		storageFile := make(map[common.Hash]*StorageFile)
		storageFile[record.RootHash] = &StorageFile{
			Capacity:                    record.StorageCapacity,
			CreateTime:                  record.PledgeNumber,
			LastVerificationTime:        record.PledgeNumber,
			LastVerificationSuccessTime: record.PledgeNumber,
			ValidationFailureTotalTime:  big.NewInt(0),
		}

		space := &SPledgeSpaces{
			Address:                     record.Address,
			StorageCapacity:             record.StorageCapacity,
			RootHash:                    record.RootHash,
			StorageFile:                 storageFile,
			LastVerificationTime:        record.PledgeNumber,
			LastVerificationSuccessTime: record.PledgeNumber,
			ValidationFailureTotalTime:  big.NewInt(0),
		}
		storagepledge := &SPledge{
			Address:                     record.PledgeAddr,
			StorageSpaces:               space,
			Number:                      record.PledgeNumber,
			TotalCapacity:               record.StorageCapacity,
			Price:                       record.Price,
			StorageSize:                 record.StorageSize,
			SpaceDeposit:                record.SpaceDeposit,
			Lease:                       make(map[common.Hash]*Lease),
			LastVerificationTime:        record.PledgeNumber,
			LastVerificationSuccessTime: record.PledgeNumber,
			ValidationFailureTotalTime:  big.NewInt(0),
			PledgeStatus:                big.NewInt(SPledgeNormal),
			Bandwidth:                   record.Bandwidth,
		}
		s.StorageData.StoragePledge[record.Address] = storagepledge
		s.StorageData.accumulateSpaceStorageFileHash(record.Address, storageFile[record.RootHash]) //update file -->  space -- pledge
	}
	s.StorageData.accumulateHeaderHash() //update all  to header valid root
}

func calStPledgeAmount(totalCapacity decimal.Decimal, snap *Snapshot, total decimal.Decimal, blockNumPer *big.Int) *big.Int {
	scale := decimal.NewFromBigInt(snap.SystemConfig.Deposit[sscEnumPStoragePledgeID], 0).Div(decimal.NewFromInt(10)) //0.1
	blockNumPerYear := secondsPerYear / snap.config.Period
	//1.25 UTG
	defaultTbAmount := decimal.NewFromFloat(1250000000000000000)
	tbPledgeNum := defaultTbAmount //1TB  UTG
	if blockNumPer.Uint64() > blockNumPerYear {
		totalSpace := total.Div(decimal.NewFromInt(1099511627776)) // B-> TB
		if totalSpace.Cmp(decimal.NewFromInt(0)) > 0 {
			calTbPledgeNum := decimal.NewFromBigInt(snap.FlowHarvest, 0).Mul(scale).Div(totalSpace)
			if calTbPledgeNum.Cmp(defaultTbAmount) < 0 {
				tbPledgeNum = calTbPledgeNum
			}
		}
	}

	return (totalCapacity.Div(decimal.NewFromInt(1099511627776))).Mul(tbPledgeNum).BigInt()
}

func (a *Alien) storagePledgeExit(storagePledgeExit []SPledgeExitRecord, exchangeSRT []ExchangeSRTRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int) ([]SPledgeExitRecord, []ExchangeSRTRecord) {
	if blocknumber.Uint64() >= PosrExitNewRuleEffectNumber {
		return a.storagePledgeNewExit(storagePledgeExit, exchangeSRT, txDataInfo, txSender, tx, receipts, state, snap, blocknumber)
	}
	if len(txDataInfo) < 4 {
		log.Warn("storage Pledge exit", "parameter error", len(txDataInfo))
		return storagePledgeExit, exchangeSRT
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if revenue, ok := snap.RevenueStorage[pledgeAddr]; ok {
		log.Warn("storage Pledge exit", "bind Revenue address", revenue.RevenueAddress)
		return storagePledgeExit, exchangeSRT
	}
	if pledgeAddr != txSender {
		log.Warn("storagePledgeExit  no role", " txSender", txSender)
		return storagePledgeExit, exchangeSRT
	}
	storagepledge := snap.StorageData.StoragePledge[pledgeAddr]
	if storagepledge == nil {
		log.Warn("storagePledgeExit  pledgeAddr not find  ", " pledgeAddr", pledgeAddr)
		return storagePledgeExit, exchangeSRT
	}
	if storagepledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
		log.Warn("storagePledgeExit  has exit", " pledgeAddr", pledgeAddr)
		return storagePledgeExit, exchangeSRT
	}
	if blocknumber.Uint64() >= StoragePledgeOptEffectNumber {
		blockNumPerYear := secondsPerYear / snap.config.Period
		pledgeTime := new(big.Int).Sub(blocknumber, storagepledge.Number)
		if pledgeTime.Uint64() <= blockNumPerYear {
			log.Warn("storagePledgeExit", "  Online for at least one year ")
			return storagePledgeExit, exchangeSRT
		}
	}
	leaseStatus := false
	for _, lease := range storagepledge.Lease {
		if lease.Status != LeaseUserRescind && lease.Status != LeaseExpiration && lease.Status != LeaseReturn {
			leaseStatus = true
			break
		}
	}
	if leaseStatus {
		log.Warn("storagePledgeExit There are still open leases ", " pledgeAddr", pledgeAddr)
		return storagePledgeExit, exchangeSRT
	}
	storagePledgeExit = append(storagePledgeExit, SPledgeExitRecord{
		Address:      pledgeAddr,
		PledgeStatus: big.NewInt(1),
	})
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0Xff21066efa593b073738a132cf978c90bcbae2c98f6956e8a9e8663ade52f33c"))
	topics[1].SetBytes(pledgeAddr.Bytes())
	topics[2].SetBytes([]byte("0"))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return storagePledgeExit, exchangeSRT
}
func (a *Alien) storagePledgeNewExit(storagePledgeExit []SPledgeExitRecord, exchangeSRT []ExchangeSRTRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int) ([]SPledgeExitRecord, []ExchangeSRTRecord) {
	if len(txDataInfo) < 4 {
		log.Warn("storage Pledge exit", "parameter error", len(txDataInfo))
		return storagePledgeExit, exchangeSRT
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if isGEInitStorageManagerNumber(blocknumber.Uint64()) {
		if entrustItem, ok := snap.StorageData.StorageEntrust[pledgeAddr]; ok {
			if snap.StorageData.StorageEntrust[pledgeAddr].Manager != txSender {
				log.Warn("isStorageManager", "txSender is not manager", txSender)
				return storagePledgeExit, exchangeSRT
			}
			if entrustItem.Sphash != common.BigToHash(common.Big0) {
				if blocknumber.Uint64()-entrustItem.Spheight.Uint64() <= sPPoollockDay*snap.getBlockPreDay() {
					log.Warn("storagePledgeNewExit", "sPPoollockDay not pass", sPPoollockDay)
					return storagePledgeExit, exchangeSRT
				}
			}
		} else {
			log.Warn("storage Pledge exit", "manager is empty", pledgeAddr)
			return storagePledgeExit, exchangeSRT
		}
	} else {
		if pledgeAddr == txSender {
			if revenue, ok := snap.RevenueStorage[pledgeAddr]; ok {
				if revenue.RevenueAddress != txSender {
					log.Warn("storage Pledge exit", "bind Revenue address", revenue.RevenueAddress)
					return storagePledgeExit, exchangeSRT
				}
			}
		} else {
			if revenue, ok := snap.RevenueStorage[pledgeAddr]; ok {
				if revenue.RevenueAddress != txSender {
					log.Warn("storage Pledge exit", "txSender no role", txSender)
					return storagePledgeExit, exchangeSRT
				}
			} else {
				log.Warn("storage Pledge exit", "txSender no role", txSender)
				return storagePledgeExit, exchangeSRT
			}
		}
	}
	storagepledge := snap.StorageData.StoragePledge[pledgeAddr]
	if storagepledge == nil {
		log.Warn("storagePledgeExit  pledgeAddr not find  ", " pledgeAddr", pledgeAddr)
		return storagePledgeExit, exchangeSRT
	}
	if storagepledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
		log.Warn("storagePledgeExit  has exit", " pledgeAddr", pledgeAddr)
		return storagePledgeExit, exchangeSRT
	}
	storagePledgeExit = append(storagePledgeExit, SPledgeExitRecord{
		Address:      pledgeAddr,
		PledgeStatus: big.NewInt(1),
	})
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0Xff21066efa593b073738a132cf978c90bcbae2c98f6956e8a9e8663ade52f33c"))
	topics[1].SetBytes(pledgeAddr.Bytes())
	topics[2].SetBytes([]byte("0"))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return storagePledgeExit, exchangeSRT
}
func (s *Snapshot) updateStoragePledgeExit(storagePledgeExit []SPledgeExitRecord, headerNumber *big.Int, db ethdb.Database) {
	if storagePledgeExit == nil || len(storagePledgeExit) == 0 {
		return
	}
	for _, pledgeExit := range storagePledgeExit {
		if pledgeItem, ok := s.StorageData.StoragePledge[pledgeExit.Address]; ok {
			if headerNumber.Uint64() >= PosrExitNewRuleEffectNumber {
				delete(s.RevenueStorage, pledgeExit.Address)
				for _, lease := range pledgeItem.Lease {
					if lease.Status == LeaseNormal || lease.Status == LeaseBreach {
						lease.Status = LeaseUserRescind
						s.StorageData.accumulateLeaseHash(pledgeExit.Address, lease)
					}
				}
			}
			pledgeItem.PledgeStatus = pledgeExit.PledgeStatus
			s.StorageData.accumulatePledgeHash(pledgeExit.Address)

		}
	}
	s.StorageData.accumulateHeaderHash()
}
func (a *Alien) processRentRequest(currentSRent []LeaseRequestRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, snap *Snapshot, number uint64) []LeaseRequestRecord {
	if len(txDataInfo) < 7 {
		log.Warn("sRent", "parameter number", len(txDataInfo))
		return currentSRent
	}
	sRent := LeaseRequestRecord{
		Tenant:   txSender,
		Address:  common.Address{},
		Capacity: big.NewInt(0),
		Duration: big.NewInt(0),
		Price:    big.NewInt(0),
		Hash:     tx.Hash(),
	}
	postion := 3
	if err := sRent.Address.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("sRent", "address", txDataInfo[postion])
		return currentSRent
	}
	postion++
	if capacity, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("sRent", "Capacity", txDataInfo[postion])
		return currentSRent
	} else {
		sRent.Capacity = capacity.BigInt()
	}
	if sRent.Capacity.Cmp(common.Big0) <= 0 {
		log.Warn("sRent", "Capacity less than or equal 0", txDataInfo[postion], "Capacity", sRent.Capacity)
		return currentSRent
	}
	if sRent.Capacity.Cmp(minRentSpace) < 0 {
		log.Warn("sRent", "Capacity less than minRentSpace", txDataInfo[postion], "Capacity", sRent.Capacity)
		return currentSRent
	}
	postion++
	if duration, err := strconv.ParseUint(txDataInfo[postion], 10, 64); err != nil {
		log.Warn("sRent", "duration", txDataInfo[postion])
		return currentSRent
	} else {
		sRent.Duration = new(big.Int).SetUint64(duration)
	}
	if sRent.Duration.Cmp(snap.SystemConfig.Deposit[sscEnumMinimumRent]) < 0 {
		log.Warn("sRent", "Duration to small", sRent.Duration)
		return currentSRent
	}
	if sRent.Duration.Cmp(snap.SystemConfig.Deposit[sscEnumMaximumRent]) > 0 {
		log.Warn("sRent", "Duration to big", sRent.Duration)
		return currentSRent
	}
	postion++
	if price, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("sRent", "price", txDataInfo[postion])
		return currentSRent
	} else {
		sRent.Price = price.BigInt()
	}
	if number < StoragePledgeOptEffectNumber {
		if sRent.Price.Cmp(new(big.Int).Mul(snap.SystemConfig.Deposit[sscEnumStoragePrice], big.NewInt(10))) > 0 {
			log.Warn("price is set too high", " price", sRent.Price)
			return currentSRent
		}
		//check price 0.1
		minPrice := new(big.Int).Mul(snap.SystemConfig.Deposit[sscEnumStoragePrice], big.NewInt(10))
		minPrice = new(big.Int).Div(minPrice, big.NewInt(100))
		if sRent.Price.Cmp(minPrice) < 0 {
			log.Info("price is set too low", "price", sRent.Price)
			return currentSRent
		}
	}
	//checkSRT
	if !snap.checkEnoughSRT(currentSRent, sRent, number-1, a.db) {
		log.Warn("sRent", "checkEnoughSRT fail", sRent.Tenant)
		return currentSRent
	}
	//checkPledge
	if snap.StorageData.checkSRent(currentSRent, sRent, number) {
		topics := make([]common.Hash, 2)
		topics[0].UnmarshalText([]byte("0x24d91fe07adb5ec81f7c1724a69e7c307c289ff524f9ecb2519e631ba3f7f3d1"))
		topics[1].SetBytes(sRent.Address.Bytes())
		a.addCustomerTxLog(tx, receipts, topics, nil)
		currentSRent = append(currentSRent, sRent)
	} else {
		log.Warn("sRent", "checkSRent fail", sRent.Address)
	}
	return currentSRent
}
func (a *Alien) processExchangeSRT(currentExchangeSRT []ExchangeSRTRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot) []ExchangeSRTRecord {
	utgPosExchValue := 4
	if len(txDataInfo) <= utgPosExchValue {
		log.Warn("Exchange UTG to SRT fail", "parameter number", len(txDataInfo))
		return currentExchangeSRT
	}
	exchangeSRT := ExchangeSRTRecord{
		Target: common.Address{},
		Amount: big.NewInt(0),
	}
	if err := exchangeSRT.Target.UnmarshalText1([]byte(txDataInfo[3])); err != nil {
		log.Warn("Exchange UTG to SRT fail", "address", txDataInfo[3])
		return currentExchangeSRT
	}
	amount := big.NewInt(0)
	var err error
	if amount, err = hexutil.UnmarshalText1([]byte(txDataInfo[utgPosExchValue])); err != nil {
		log.Warn("Exchange UTG to SRT fail", "number", txDataInfo[utgPosExchValue])
		return currentExchangeSRT
	}
	if amount.Cmp(common.Big0) <= 0 {
		log.Warn("Exchange UTG to SRT fail", "amount less than or equal 0", txDataInfo[utgPosExchValue])
		return currentExchangeSRT
	}
	if state.GetBalance(txSender).Cmp(amount) < 0 {
		log.Warn("Exchange UTG to SRT fail", "balance", state.GetBalance(txSender))
		return currentExchangeSRT
	}
	exchangeSRT.Amount = new(big.Int).Div(new(big.Int).Mul(amount, big.NewInt(int64(snap.SystemConfig.ExchRate))), big.NewInt(10000))
	state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), amount))
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x1ebef91bab080007829976060bb3c203fd4d5b8395c552e10f5134e188428147")) //web3.sha3("ExchangeSRT(address,uint256)")
	topics[1].SetBytes(txSender.Bytes())
	topics[2].SetBytes(exchangeSRT.Target.Bytes())
	dataList := make([]common.Hash, 2)
	dataList[0].SetBytes(amount.Bytes())
	dataList[1].SetBytes(exchangeSRT.Amount.Bytes())
	data := dataList[0].Bytes()
	data = append(data, dataList[1].Bytes()...)
	a.addCustomerTxLog(tx, receipts, topics, data)
	currentExchangeSRT = append(currentExchangeSRT, exchangeSRT)
	return currentExchangeSRT
}

func (a *Alien) processLeasePledge(currentSRentPg []LeasePledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number uint64, chain consensus.ChainHeaderReader) []LeasePledgeRecord {
	if len(txDataInfo) < 9 {
		log.Warn("sRentPg", "parameter number", len(txDataInfo))
		return currentSRentPg
	}
	sRentPg := LeasePledgeRecord{
		Address:        common.Address{},
		DepositAddress: txSender,
		Hash:           common.Hash{},
		Capacity:       big.NewInt(0),
		RootHash:       common.Hash{},
		BurnSRTAmount:  big.NewInt(0),
		Duration:       big.NewInt(0),
		BurnSRTAddress: common.Address{},
		PledgeHash:     tx.Hash(),
		LeftCapacity:   big.NewInt(0),
		LeftRootHash:   common.Hash{},
	}
	postion := 3
	if err := sRentPg.Address.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("sRentPg", "Hash", txDataInfo[postion])
		return currentSRentPg
	}
	postion++
	sRentPg.Hash = common.HexToHash(txDataInfo[postion])
	postion++
	if capacity, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("sRentPg", "Capacity", txDataInfo[postion])
		return currentSRentPg
	} else {
		sRentPg.Capacity = capacity.BigInt()
	}
	if sRentPg.Capacity.Cmp(common.Big0) <= 0 {
		log.Warn("sRentPg Capacity less or equal 0", " Capacity", sRentPg.Capacity)
		return currentSRentPg
	}
	postion++
	if rootHash, ok := snap.StorageData.verifyParamsStoragePoc(txDataInfo, postion, chain, number); !ok {
		log.Warn("sRentPg verify fail", " RootHash1", rootHash)
		return currentSRentPg
	} else {
		sRentPg.RootHash = rootHash
	}
	postion++
	if leftCapacity, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("sRentPg", "Capacity", txDataInfo[postion])
		return currentSRentPg
	} else {
		sRentPg.LeftCapacity = leftCapacity.BigInt()
	}
	if sRentPg.LeftCapacity.Cmp(common.Big0) < 0 { //can be 0
		log.Warn("sRentPg LeftCapacity less 0", " LeftCapacity", sRentPg.LeftCapacity)
		return currentSRentPg
	}
	if isGEPosAutoExitPunishChange(number) {
		if sRentPg.LeftCapacity.Cmp(rentLeftSpace) < 0 {
			log.Warn("sRentPg LeftCapacity less rentLeftSpace", " LeftCapacity", sRentPg.LeftCapacity)
			return currentSRentPg
		}
	}
	if sRentPg.LeftCapacity.Cmp(common.Big0) != 0 {
		postion++
		if rootHash, ok := snap.StorageData.verifyParamsStoragePoc(txDataInfo, postion, chain, number); !ok {
			log.Warn("sRentPg verify fail", " RootHash2", rootHash)
			return currentSRentPg
		} else {
			sRentPg.LeftRootHash = rootHash
		}
	}
	//checkPledge
	passTime := new(big.Int).Mul(snap.SystemConfig.Deposit[sscEnumLeaseExpires], new(big.Int).SetUint64(snap.getBlockPreDay()))
	if srtAmount, amount, duration, burnSRTAddress, ok := snap.StorageData.checkSRentPg(currentSRentPg, sRentPg, txSender, snap.RevenueStorage, snap.SystemConfig.ExchRate, passTime, number); ok {
		sRentPg.BurnSRTAmount = srtAmount
		sRentPg.BurnAmount = amount
		sRentPg.Duration = duration
		sRentPg.BurnSRTAddress = burnSRTAddress

		if !snap.checkEnoughSRTPg(currentSRentPg, sRentPg, number-1, a.db) {
			log.Warn("sRent", "checkEnoughSRT fail", sRentPg.BurnSRTAddress)
			return currentSRentPg
		}
		if state.GetBalance(txSender).Cmp(amount) < 0 {
			log.Warn("sRent", "balance", state.GetBalance(txSender))
			return currentSRentPg
		}
		state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), amount))
		topics := make([]common.Hash, 2)
		topics[0].UnmarshalText([]byte("0xf145aaf8213a13521c09380bc80e9f77d4aa86f181a31bdf688f4693e95b6647"))
		topics[1].SetBytes(sRentPg.Hash.Bytes())
		dataList := make([]common.Hash, 3)
		dataList[0].SetBytes(sRentPg.Address.Bytes())
		dataList[1].SetBytes(sRentPg.Capacity.Bytes())
		dataList[2].SetBytes(sRentPg.RootHash.Bytes())
		data := dataList[0].Bytes()
		data = append(data, dataList[1].Bytes()...)
		a.addCustomerTxLog(tx, receipts, topics, data)
		currentSRentPg = append(currentSRentPg, sRentPg)
	} else {
		log.Warn("sRentPg", "checkSRentPg fail", sRentPg.Hash)
	}
	return currentSRentPg
}
func (a *Alien) processLeaseRenewal(currentSRentReNew []LeaseRenewalRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number uint64) []LeaseRenewalRecord {
	if len(txDataInfo) < 6 {
		log.Warn("sRentReNew", "parameter number", len(txDataInfo))
		return currentSRentReNew
	}
	sRentReNew := LeaseRenewalRecord{
		Address:  common.Address{},
		Hash:     common.Hash{},
		Duration: big.NewInt(0),
		Price:    big.NewInt(0),
		Tenant:   common.Address{},
		NewHash:  common.Hash{},
		Capacity: big.NewInt(0),
	}
	postion := 3
	if err := sRentReNew.Address.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("sRentReNew", "Hash", txDataInfo[postion])
		return currentSRentReNew
	}
	postion++
	sRentReNew.Hash = common.HexToHash(txDataInfo[postion])
	postion++
	if duration, err := strconv.ParseUint(txDataInfo[postion], 10, 32); err != nil {
		log.Warn("sRentReNew", "duration", txDataInfo[postion])
		return currentSRentReNew
	} else {
		sRentReNew.Duration = new(big.Int).SetUint64(duration)
	}
	if sRentReNew.Duration.Cmp(snap.SystemConfig.Deposit[sscEnumMinimumRent]) < 0 {
		log.Warn("sRentReNew", "Duration to small", sRentReNew.Duration)
		return currentSRentReNew
	}
	if sRentReNew.Duration.Cmp(snap.SystemConfig.Deposit[sscEnumMaximumRent]) > 0 {
		log.Warn("sRentReNew", "Duration to big", sRentReNew.Duration)
		return currentSRentReNew
	}
	if tenant, ok := snap.StorageData.checkSRentReNew(currentSRentReNew, sRentReNew, txSender, number, a.blockPerDay()); ok {
		sRentReNew.Tenant = tenant
	} else {
		log.Warn("sRentReNew", "checkSRentReNew fail", sRentReNew.Hash)
		return currentSRentReNew
	}
	lease := snap.StorageData.StoragePledge[sRentReNew.Address].Lease
	l := lease[sRentReNew.Hash]
	sRentReNew.Price = l.UnitPrice
	sRentReNew.Capacity = l.Capacity
	if !snap.checkEnoughSRTReNew(currentSRentReNew, sRentReNew, number-1, a.db) {
		log.Warn("sRentReNew", "checkEnoughSRT fail", sRentReNew.Tenant)
		return currentSRentReNew
	}
	sRentReNew.NewHash = tx.Hash()
	topics := make([]common.Hash, 2)
	topics[0].UnmarshalText([]byte("0xad3545265bff0a514f14821359a92d5b238073e1058ef0f7d83cd3ddcc7306cb")) //web3.sha3("stReNew(address)")
	topics[1].SetBytes(sRentReNew.Hash.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentSRentReNew = append(currentSRentReNew, sRentReNew)
	return currentSRentReNew
}
func (a *Alien) processLeaseRenewalPledge(currentSRentReNewPg []LeaseRenewalPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number uint64, chain consensus.ChainHeaderReader) []LeaseRenewalPledgeRecord {
	if len(txDataInfo) < 7 {
		log.Warn("sRentReNewPg", "parameter number", len(txDataInfo))
		return currentSRentReNewPg
	}
	sRentPg := LeaseRenewalPledgeRecord{
		Address:    common.Address{},
		Hash:       common.Hash{},
		Capacity:   big.NewInt(0),
		RootHash:   common.Hash{},
		Duration:   big.NewInt(0),
		PledgeHash: tx.Hash(),
	}
	postion := 3
	if err := sRentPg.Address.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("sRentReNewPg", "Hash", txDataInfo[postion])
		return currentSRentReNewPg
	}
	postion++
	sRentPg.Hash = common.HexToHash(txDataInfo[postion])
	postion++
	if capacity, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("sRentReNewPg", "Capacity", txDataInfo[postion])
		return currentSRentReNewPg
	} else {
		sRentPg.Capacity = capacity.BigInt()
	}
	if sRentPg.Capacity.Cmp(common.Big0) <= 0 {
		log.Warn("sRentReNewPg Capacity less or equal 0", " Capacity", sRentPg.Capacity)
		return currentSRentReNewPg
	}
	postion++
	if rootHash, ok := snap.StorageData.verifyParamsStoragePoc(txDataInfo, postion, chain, number); !ok {
		log.Warn("sRentReNewPg verify fail", " RootHash", rootHash)
		return currentSRentReNewPg
	} else {
		sRentPg.RootHash = rootHash
	}
	postion++
	//checkPledge
	passTime := new(big.Int).Mul(snap.SystemConfig.Deposit[sscEnumLeaseExpires], new(big.Int).SetUint64(snap.getBlockPreDay()))
	if srtAmount, amount, duration, burnSRTAddress, ok := snap.StorageData.checkSRentReNewPg(currentSRentReNewPg, sRentPg, txSender, snap.RevenueStorage, snap.SystemConfig.ExchRate, passTime, number, snap.getBlockPreDay()); ok {
		sRentPg.BurnSRTAmount = srtAmount
		sRentPg.BurnAmount = amount
		sRentPg.Duration = duration
		sRentPg.BurnSRTAddress = burnSRTAddress
		if !snap.checkEnoughSRTReNewPg(currentSRentReNewPg, sRentPg, number-1, a.db) {
			log.Warn("sRentReNewPg", "checkEnoughSRT fail", sRentPg.BurnSRTAddress)
			return currentSRentReNewPg
		}
		if state.GetBalance(txSender).Cmp(amount) < 0 {
			log.Warn("sRentReNewPg", "balance", state.GetBalance(txSender))
			return currentSRentReNewPg
		}
		state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), amount))
		topics := make([]common.Hash, 2)
		topics[0].UnmarshalText([]byte("0x24461fc75f60084c7cefe35795e6365d21728afd90a7eee606bac1f92013baec"))
		topics[1].SetBytes(sRentPg.Hash.Bytes())
		dataList := make([]common.Hash, 3)
		dataList[0].SetBytes(sRentPg.Address.Bytes())
		dataList[1].SetBytes(sRentPg.Capacity.Bytes())
		dataList[2].SetBytes(sRentPg.RootHash.Bytes())
		data := dataList[0].Bytes()
		data = append(data, dataList[1].Bytes()...)
		a.addCustomerTxLog(tx, receipts, topics, data)
		currentSRentReNewPg = append(currentSRentReNewPg, sRentPg)
	} else {
		log.Warn("sRentReNewPg", "checkSRentReNewPg fail", sRentPg.Hash)
	}
	return currentSRentReNewPg
}

func (a *Alien) processLeaseRescind(currentSRescind []LeaseRescindRecord, currentExchangeSRT []ExchangeSRTRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number uint64) ([]LeaseRescindRecord, []ExchangeSRTRecord) {
	if len(txDataInfo) < 5 {
		log.Warn("stRescind", "parameter number", len(txDataInfo))
		return currentSRescind, currentExchangeSRT
	}
	sRescind := LeaseRescindRecord{
		Address: common.Address{},
		Hash:    common.Hash{},
	}
	postion := 3
	if err := sRescind.Address.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("stRescind", "Hash", txDataInfo[postion])
		return currentSRescind, currentExchangeSRT
	}
	postion++
	sRescind.Hash = common.HexToHash(txDataInfo[postion])
	//checkSRescind
	if ok := snap.StorageData.checkSRescind(currentSRescind, sRescind, txSender, snap.SystemConfig.ExchRate, number, a.blockPerDay()); ok {
		topics := make([]common.Hash, 2)
		topics[0].UnmarshalText([]byte("0x3bfad54852baf2b8be1ae9452a2b1d07e9c03e139b622817417852cc78d06100"))
		topics[1].SetBytes(sRescind.Hash.Bytes())
		a.addCustomerTxLog(tx, receipts, topics, nil)
		currentSRescind = append(currentSRescind, sRescind)
	} else {
		log.Warn("stRescind", "checkSRescind fail", sRescind.Hash)
	}
	return currentSRescind, currentExchangeSRT
}

func (s *StorageData) checkSRescind(currentSRescind []LeaseRescindRecord, sRescind LeaseRescindRecord, txSender common.Address, exchRate uint32, number uint64, blockPerDay uint64) bool {
	for _, item := range currentSRescind {
		if item.Hash == sRescind.Hash {
			log.Info("checkSRescind", "rent sRescind only one in one block", sRescind.Hash)
			return false
		}
	}
	if _, ok := s.StoragePledge[sRescind.Address]; !ok {
		log.Info("checkSRescind", "address not exist", sRescind.Address)
		return false
	}
	if _, ok := s.StoragePledge[sRescind.Address].Lease[sRescind.Hash]; !ok {
		log.Info("checkSRescind", "hash not exist", sRescind.Hash)
		return false
	}
	lease := s.StoragePledge[sRescind.Address].Lease[sRescind.Hash]
	if lease.Address != txSender {
		log.Info("checkSRescind", "lease.Address is not txSender", txSender)
		return false
	}
	status := lease.Status
	if status != LeaseBreach {
		log.Info("checkSRescind", "lease.Status is not breach", status)
		return false
	}

	startTime := big.NewInt(0)
	duration := big.NewInt(0)
	for _, leaseDetail := range lease.LeaseList {
		if leaseDetail.Deposit.Cmp(big.NewInt(0)) > 0 && leaseDetail.StartTime.Cmp(startTime) > 0 {
			startTime = leaseDetail.StartTime
			duration = new(big.Int).Mul(leaseDetail.Duration, new(big.Int).SetUint64(blockPerDay))
		}
	}
	if startTime.Cmp(big.NewInt(0)) == 0 {
		log.Info("checkSRescind", "startTime is 0 ", startTime)
		return false
	}

	lEndNumber := new(big.Int).Add(startTime, duration)
	if lEndNumber.Cmp(new(big.Int).SetUint64(number)) < 0 {
		log.Info("checkSRescind", "duration is pass ", lEndNumber)
		return false
	}

	return true
}

func (s *StorageData) updateLeaseRescind(sRescinds []LeaseRescindRecord, number *big.Int, db ethdb.Database) {
	for _, sRescind := range sRescinds {
		if _, ok := s.StoragePledge[sRescind.Address]; !ok {
			continue
		}
		if _, ok := s.StoragePledge[sRescind.Address].Lease[sRescind.Hash]; !ok {
			continue
		}
		lease := s.StoragePledge[sRescind.Address].Lease[sRescind.Hash]
		lease.Status = LeaseUserRescind
		s.accumulateLeaseHash(sRescind.Address, lease)
	}
	s.accumulateHeaderHash()
}

func (s *StorageData) storageVerificationCheck(number uint64, blockPerday uint64, passTime *big.Int, rate uint32, revenueStorage map[common.Address]*RevenueParameter, period uint64, db ethdb.Database, basePrice *big.Int, currentLockReward []LockRewardRecord, snapTotalLeaseSpace *big.Int, spData *SpData, snap *Snapshot) ([]LockRewardRecord, []ExchangeSRTRecord, *big.Int, error, *big.Int, *big.Int) {

	sussSPAddrs, sussRentHashs, storageRatios, capSuccAddrs := s.storageVerify(number, blockPerday, revenueStorage)

	err := s.saveSPledgeSuccTodb(sussSPAddrs, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	err = s.saveRentSuccTodb(sussRentHashs, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	if capSuccAddrs != nil {
		err = s.saveCapSuccAddrsTodb(capSuccAddrs, db, number)
		if err != nil {
			return currentLockReward, nil, nil, err, nil, nil
		}
	}
	var burnAmount *big.Int
	revertSpaceLockReward, revertExchangeSRT, bAmount := s.dealLeaseStatus(number, rate, blockPerday, revenueStorage, snap)
	if bAmount != nil && bAmount.Cmp(common.Big0) > 0 {
		burnAmount = new(big.Int).Set(bAmount)
	}
	err = s.saveRevertSpaceLockRewardTodb(revertSpaceLockReward, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	err = s.saveRevertExchangeSRTTodb(revertExchangeSRT, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	if isLtInitStorageManagerNumber(number) {
		storageRatios = s.calcStorageRatio(storageRatios, number)
		err = s.saveStorageRatiosTodb(storageRatios, db, number)
		if err != nil {
			return currentLockReward, nil, nil, err, nil, nil
		}
	}
	harvest := big.NewInt(0)
	zero := big.NewInt(0)
	spaceLockReward, spaceHarvest, leftAmount := s.calcStoragePledgeReward(storageRatios, revenueStorage, number, period, sussSPAddrs, capSuccAddrs, db, snap)
	if leftAmount != nil && leftAmount.Cmp(common.Big0) > 0 {
		if burnAmount == nil {
			burnAmount = new(big.Int).Set(leftAmount)
		} else {
			burnAmount = new(big.Int).Add(burnAmount, leftAmount)
		}
	}
	if spaceHarvest.Cmp(zero) > 0 {
		harvest = new(big.Int).Add(harvest, spaceHarvest)
	}
	err = s.saveSpaceLockRewardTodb(spaceLockReward, revenueStorage, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	s.deletePasstimeLease(number, blockPerday, passTime)
	LockLeaseReward, leaseHarvest, totalLeaseSpace, feeBurnAmount := s.accumulateLeaseRewards(storageRatios, sussRentHashs, basePrice, revenueStorage, number, db, snapTotalLeaseSpace, spData, snap)
	if feeBurnAmount != nil && feeBurnAmount.Cmp(common.Big0) > 0 {
		if burnAmount == nil {
			burnAmount = new(big.Int).Set(feeBurnAmount)
		} else {
			burnAmount = new(big.Int).Add(burnAmount, feeBurnAmount)
		}
	}
	if leaseHarvest.Cmp(zero) > 0 {
		harvest = new(big.Int).Add(harvest, leaseHarvest)
	}
	err = s.saveLeaseLockRewardTodb(LockLeaseReward, db, number)
	if err != nil {
		return currentLockReward, nil, nil, err, nil, nil
	}
	if currentLockReward != nil {
		for _, item := range revertSpaceLockReward {
			if number < PledgeRevertLockEffectNumber {
				currentLockReward = append(currentLockReward, LockRewardRecord{
					Target:   item.Target,
					Amount:   item.Amount,
					IsReward: sscEnumBandwidthReward,
				})
			} else {
				currentLockReward = append(currentLockReward, LockRewardRecord{
					Target:   item.Target,
					Amount:   item.Amount,
					IsReward: sscEnumStoragePledgeRedeemLock,
				})
			}

		}
		for _, item := range spaceLockReward {
			currentLockReward = append(currentLockReward, LockRewardRecord{
				Target:   item.Target,
				Amount:   item.Amount,
				IsReward: sscEnumBandwidthReward,
			})
		}

		for _, item := range LockLeaseReward {
			currentLockReward = append(currentLockReward, LockRewardRecord{
				Target:   item.Target,
				Amount:   item.Amount,
				IsReward: sscEnumFlwReward,
			})
		}
	}
	return currentLockReward, revertExchangeSRT, harvest, nil, burnAmount, totalLeaseSpace
}

/**
 *Storage space recovery certificate
 */
func (a *Alien) storageRecoveryCertificate(storageRecoveryData []SPledgeRecoveryRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SPledgeRecoveryRecord {
	//log.Info("storageRecoveryCertificate", "txDataInfo", txDataInfo)
	if len(txDataInfo) < 6 {
		log.Warn("storage Recovery Certificate", "parameter error", len(txDataInfo))
		return storageRecoveryData
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if pledgeAddr != txSender {
		log.Warn("storage Recovery Certificate  no role", " txSender", txSender)
		return storageRecoveryData
	}
	storagepledge := snap.StorageData.StoragePledge[pledgeAddr]
	if storagepledge == nil {
		log.Warn("storage Recovery Certificate  not find pledge", " pledgeAddr", pledgeAddr)
		return storageRecoveryData
	}
	if len(txDataInfo[4]) == 0 || txDataInfo[4] == "" {
		log.Warn("storage Recovery Certificate  not any rent hash", " pledgeAddr", pledgeAddr)
		return storageRecoveryData
	}
	leaseHashStr := strings.Split(txDataInfo[4], ",")
	currNumber := big.NewInt(int64(snap.Number))
	var delLeaseHash []common.Hash
	totalReCapacity := decimal.Zero
	for _, hashStr := range leaseHashStr {
		leaseHash := common.HexToHash(hashStr)
		if lease, ok := storagepledge.Lease[leaseHash]; ok {
			if lease.Status == LeaseReturn {
				delLeaseHash = append(delLeaseHash, leaseHash)
				totalReCapacity = totalReCapacity.Add(decimal.NewFromBigInt(lease.Capacity, 0))
			}
		}
	}
	if len(delLeaseHash) != len(leaseHashStr) {
		log.Warn("storage  Recovery Certificate  There are leases that have not expired ", " leaseHash", txDataInfo[4])
		return storageRecoveryData
	}
	storageCapacity := decimal.Zero // new(big.Int).Add(storagepledge.TotalCapacity,totalReCapacity.BigInt())
	validData := txDataInfo[5]
	verifyType := ""
	if blocknumber.Uint64() >= storageVerifyNewEffectNumber {
		if strings.HasPrefix(validData, "v1") {
			verifyType = "v1"
			validData = validData[3:]
		}
	}
	verifydatas := strings.Split(validData, ",")
	if len(verifydatas) < 10 {
		log.Warn("verifyStoragePoc", "invalide poc string format")
		return storageRecoveryData
	}
	rootHash := verifydatas[len(verifydatas)-1]
	if isLtPosAutoExitPunishChange(blocknumber.Uint64()) {
		blockSize, err := decimal.NewFromString(verifydatas[4])
		if err != nil || blockSize.Cmp(decimal.Zero) <= 0 {
			log.Warn("applyStorageProof blocksize err ", "blockSize", blockSize, "set storageBlockSize", storageBlockSize)
			return storageRecoveryData
		}
		blockNum, err := decimal.NewFromString(verifydatas[5])
		if err != nil || blockNum.Cmp(decimal.Zero) <= 0 {
			log.Warn("applyStorageProof blockNum err ", "blockNum", blockNum)
			return storageRecoveryData
		}
		storageCapacity = blockSize.Mul(blockNum)
		if storageCapacity.Cmp(decimal.Zero) <= 0 {
			log.Warn("applyStorageProof storageCapacity err ", "storageCapacity", storageCapacity)
			return storageRecoveryData
		}
		freecapacity := decimal.Zero
		if storagef, ok := storagepledge.StorageSpaces.StorageFile[storagepledge.StorageSpaces.RootHash]; ok {
			freecapacity = decimal.NewFromBigInt(storagef.Capacity, 0)
		}
		totalcapacity := storagepledge.TotalCapacity
		if storageCapacity.BigInt().Cmp(totalcapacity) > 0 || storageCapacity.Cmp(totalReCapacity.Add(freecapacity)) != 0 {
			log.Warn("storage  Recovery storageCapacity is error", " storageCapacity", txDataInfo[5])
			return storageRecoveryData
		}

		verifyHeader := chain.GetHeaderByHash(common.HexToHash(verifydatas[2]))
		if verifyHeader == nil || verifyHeader.Number.String() != verifydatas[0] || strconv.FormatInt(int64(verifyHeader.Nonce.Uint64()), 10) != verifydatas[1] {
			log.Warn("storageRecoveryCertificate  GetHeaderByHash not find by hash  ", "verifydatas", verifydatas)
			return storageRecoveryData
		}
		if verifyType == "v1" {
			if !verifyStoragePocV1(txDataInfo[5], rootHash, verifyHeader.Nonce.Uint64()) {
				log.Warn("storageRecoveryCertificate   verify  faild", "roothash", storagepledge.StorageSpaces.RootHash.String())
				return storageRecoveryData
			}
		} else {
			if !verifyStoragePoc(txDataInfo[5], rootHash, verifyHeader.Nonce.Uint64()) {
				log.Warn("storageRecoveryCertificate   verify  faild", "roothash", storagepledge.StorageSpaces.RootHash.String())
				return storageRecoveryData
			}
		}

	} else {
		verifyHeader := chain.GetHeaderByHash(common.HexToHash(verifydatas[2]))
		if verifyHeader == nil || verifyHeader.Number.String() != verifydatas[0] || strconv.FormatInt(int64(verifyHeader.Nonce.Uint64()), 10) != verifydatas[1] {
			log.Warn("storageRecoveryCertificate  GetHeaderByHash not find by hash  ", "verifydatas", verifydatas)
			return storageRecoveryData
		}
		//
		storageCapacity = totalReCapacity.Add(decimal.NewFromBigInt(storagepledge.StorageSpaces.StorageCapacity, 0)) // new(big.Int).Add(storagepledge.TotalCapacity,totalReCapacity.BigInt())
	}

	storageRecoveryData = append(storageRecoveryData, SPledgeRecoveryRecord{
		Address:       pledgeAddr,
		LeaseHash:     delLeaseHash,
		SpaceCapacity: storageCapacity.BigInt(),
		RootHash:      common.HexToHash(rootHash),
		ValidNumber:   currNumber,
	})
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0Xf145aaf8213a13521c09380bc80e9f77d4aa86f181a31bdf684532e95b6647"))
	topics[1].SetBytes(pledgeAddr.Bytes())
	topics[2].SetBytes([]byte(storageCapacity.String()))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return storageRecoveryData
}
func (s *Snapshot) updateStorageRecoveryData(storageRecoveryData []SPledgeRecoveryRecord, headerNumber *big.Int, db ethdb.Database) {
	if storageRecoveryData == nil || len(storageRecoveryData) == 0 {
		return
	}
	for _, storageRvdata := range storageRecoveryData {

		if pledgeData, ok := s.StorageData.StoragePledge[storageRvdata.Address]; ok {
			for _, leaseHash := range storageRvdata.LeaseHash {
				delete(pledgeData.Lease, leaseHash)
			}
			delete(pledgeData.StorageSpaces.StorageFile, pledgeData.StorageSpaces.RootHash)
			pledgeData.StorageSpaces.RootHash = storageRvdata.RootHash
			pledgeData.StorageSpaces.StorageFile[storageRvdata.RootHash] = &StorageFile{
				Capacity:                    storageRvdata.SpaceCapacity,
				CreateTime:                  storageRvdata.ValidNumber,
				LastVerificationTime:        storageRvdata.ValidNumber,
				LastVerificationSuccessTime: storageRvdata.ValidNumber,
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			pledgeData.StorageSpaces.StorageCapacity = storageRvdata.SpaceCapacity
			pledgeData.StorageSpaces.ValidationFailureTotalTime = big.NewInt(0)
			pledgeData.StorageSpaces.LastVerificationSuccessTime = storageRvdata.ValidNumber
			pledgeData.StorageSpaces.LastVerificationTime = storageRvdata.ValidNumber
			s.StorageData.accumulatePledgeHash(storageRvdata.Address)
		}
	}
	s.StorageData.accumulateHeaderHash()

}

func (a *Alien) applyStorageProof(storageProofRecord []StorageProofRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []StorageProofRecord {
	//log.Debug("applyStorageProof", "txDataInfo", txDataInfo)
	if len(txDataInfo) < 7 {
		log.Warn("Storage Proof", "parameter error", len(txDataInfo))
		return storageProofRecord
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if pledgeAddr != txSender {
		log.Warn("Storage Proof txSender no role", " txSender", txSender, "pledgeAddr", pledgeAddr)
		return storageProofRecord

	}
	storagepledge := snap.StorageData.StoragePledge[pledgeAddr]
	if storagepledge == nil {
		log.Warn("Storage Proof not find pledge", " pledgeAddr", pledgeAddr)
		return storageProofRecord
	}
	var verifyResult []string
	currNumber := big.NewInt(int64(snap.Number))
	if blocknumber.Uint64() >= PledgeRevertLockEffectNumber {
		verifyResult, storageProofRecord = a.StorageProofNew(storageProofRecord, txDataInfo[6], pledgeAddr, storagepledge, chain, blocknumber)
	} else {
		var capacity *big.Int
		if capvalue, err := decimal.NewFromString(txDataInfo[5]); err != nil {
			log.Warn("Storage Proof capvalue format error", "Capacity", txDataInfo[5])
			return storageProofRecord
		} else {
			capacity = capvalue.BigInt()
		}
		var tragetCapacity *big.Int
		validData := txDataInfo[6]
		verifyType := ""
		if blocknumber.Uint64() >= storageVerifyNewEffectNumber {
			if strings.HasPrefix(validData, "v1") {
				verifyType = "v1"
				validData = validData[3:]
			}
		}
		verifydatas := strings.Split(validData, ",")
		rootHash := common.HexToHash(verifydatas[len(verifydatas)-1])
		leaseHash := common.Hash{}
		if len(txDataInfo[4]) > 10 {
			leaseHash = common.HexToHash(txDataInfo[4])
			if _, ok := storagepledge.Lease[leaseHash]; !ok {
				log.Warn("Storage Proof not find leaseHash", " leaseHash", leaseHash)
				return storageProofRecord
			}
			storageFile := storagepledge.Lease[leaseHash].StorageFile
			if _, ok := storageFile[rootHash]; !ok {
				log.Warn("Storage Proof lease not find rootHash", " rootHash", rootHash)
				return storageProofRecord
			}
			lease := storagepledge.Lease[leaseHash]
			tragetCapacity = lease.Capacity
		} else {
			storageFile := storagepledge.StorageSpaces.StorageFile
			if _, ok := storageFile[rootHash]; !ok {
				log.Warn("applyStorageProof not find rootHash", " rootHash", rootHash)
				return storageProofRecord
			}
			tragetCapacity = storageFile[rootHash].Capacity
		}
		if tragetCapacity == nil || tragetCapacity.Cmp(capacity) != 0 {
			log.Warn("applyStorageProof  capacity not same", " capacity", capacity)
			return storageProofRecord
		}
		pocs := strings.Split(validData, ",")
		if len(pocs) < 10 {
			log.Warn("verifyStoragePoc", "invalide poc string format")
			return storageProofRecord
		}
		verifyHeader := chain.GetHeaderByHash(common.HexToHash(pocs[2]))
		if verifyHeader == nil || verifyHeader.Number.String() != pocs[0] || strconv.FormatInt(int64(verifyHeader.Nonce.Uint64()), 10) != pocs[1] {
			log.Warn("applyStorageProof  GetHeaderByHash not find by hash  ", "poc", pocs)
			return storageProofRecord
		}
		if currNumber.Cmp(new(big.Int).Add(proofTimeOut, verifyHeader.Number)) > 0 {
			log.Warn("applyStorageProof data timeout  ", "TimeOut", proofTimeOut, "currNumber", currNumber, "proof number", verifyHeader.Number)
			return storageProofRecord
		}
		if verifyType == "v1" {
			if !verifyStoragePocV1(txDataInfo[6], storagepledge.StorageSpaces.RootHash.String(), verifyHeader.Nonce.Uint64()) {
				log.Warn("applyStorageProof   verify  faild", "roothash", storagepledge.StorageSpaces.RootHash.String())
				return storageProofRecord
			}
		} else {
			if !verifyStoragePoc(validData, storagepledge.StorageSpaces.RootHash.String(), verifyHeader.Nonce.Uint64()) {
				log.Warn("applyStorageProof   verify  faild", "roothash", storagepledge.StorageSpaces.RootHash.String())
				return storageProofRecord
			}
		}
		storageProofRecord = append(storageProofRecord, StorageProofRecord{
			Address:                     pledgeAddr,
			RootHash:                    rootHash,
			LeaseHash:                   leaseHash,
			LastVerificationTime:        currNumber,
			LastVerificationSuccessTime: currNumber,
		})

	}
	if blocknumber.Uint64() >= PledgeRevertLockEffectNumber {
		if blocknumber.Uint64() >= StoragePledgeOptEffectNumber {
			if len(verifyResult) > 0 {
				topicdata := ""
				sort.Strings(verifyResult)
				for _, val := range verifyResult {
					if topicdata == "" {
						topicdata = val
					} else {
						topicdata += "," + val
					}
				}

				//fmt.Println("topicdata",topicdata,"verifyResult","verifyResult")
				topics := make([]common.Hash, 3)
				topics[0].UnmarshalText([]byte("0xb259d26eb65071ded303add129ecef7af12cf17a8ea9d41f7ff0cfa5af3123f8"))
				topics[1].SetBytes([]byte(changeOxToUx(pledgeAddr.String())))
				topics[2].SetBytes([]byte(currNumber.String()))
				a.addCustomerTxLog(tx, receipts, topics, []byte(topicdata))
			}
		} else {
			topicdata := ""
			sort.Strings(verifyResult)
			for _, val := range verifyResult {
				if topicdata == "" {
					topicdata = val
				} else {
					topicdata += "," + val
				}
			}
			//fmt.Println("topicdata",topicdata,"verifyResult","verifyResult")
			topics := make([]common.Hash, 3)
			topics[0].UnmarshalText([]byte("0xb259d26eb65071ded303add129ecef7af12cf17a8ea9d41f7ff0cfa5af3123f8"))
			topics[1].SetBytes([]byte(changeOxToUx(pledgeAddr.String())))
			topics[2].SetBytes([]byte(currNumber.String()))
			a.addCustomerTxLog(tx, receipts, topics, []byte(topicdata))
		}

	} else {
		topics := make([]common.Hash, 3)
		topics[0].UnmarshalText([]byte("0xb259d26eb65071ded303add129ecef7af12cf17a8ea9d41f7ff0cfa5af3123f8"))
		topics[1].SetBytes(pledgeAddr.Bytes())
		topics[2].SetBytes([]byte(currNumber.String()))
		a.addCustomerTxLog(tx, receipts, topics, nil)
	}

	return storageProofRecord
}

func (a *Alien) StorageProofNew(storageProofRecord []StorageProofRecord, verifyInfo string, pledgeAddr common.Address, storagepledge *SPledge, chain consensus.ChainHeaderReader, currNumber *big.Int) ([]string, []StorageProofRecord) {
	verifyArr := strings.Split(verifyInfo, "|")
	var verifyResult []string // 1 verify success
	for index, verifydata := range verifyArr {
		if verifydata == "" {
			continue
		}
		rootHash := storagepledge.StorageSpaces.RootHash
		leaseHash := ""
		capacity := storagepledge.StorageSpaces.StorageCapacity
		verifyData := verifydata
		if index > 0 { //storage verify
			if verifydata == "" {
				continue
			}
			hashIndex := strings.Index(verifydata, ",")
			leaseHash = verifydata[0:hashIndex]
			verifyData = verifydata[hashIndex+1:]
			if lease, ok := storagepledge.Lease[common.HexToHash(leaseHash)]; !ok {
				log.Warn("Storage Proof not find leaseHash", " leaseHash", leaseHash)
				continue
			} else {
				if isGEPosAutoExitPunishChange(currNumber.Uint64()) {
					if lease.Status != LeaseNormal && lease.Status != LeaseBreach {
						log.Warn("lease  not pledge or breach", " leaseHash", leaseHash)
						continue
					}
				}
				capacity = lease.Capacity
				rootHash = lease.RootHash
			}
		}
		verifyType := ""
		verifyNewData := verifyData
		if strings.HasPrefix(verifyData, "v1") {
			verifyType = "v1"
			verifyNewData = verifyData[3:]
		}
		pocs := strings.Split(verifyNewData, ",")
		if len(pocs) < 10 {
			log.Warn("verifyStoragePoc", "invalide poc string format")
			continue
		}
		verifyHeader := chain.GetHeaderByHash(common.HexToHash(pocs[2]))
		if verifyHeader == nil || verifyHeader.Number.String() != pocs[0] || strconv.FormatInt(int64(verifyHeader.Nonce.Uint64()), 10) != pocs[1] {
			log.Warn("applyStorageProof  GetHeaderByHash not find by hash  ", "poc", pocs)
			continue
		}
		if currNumber.Cmp(new(big.Int).Add(proofTimeOut, verifyHeader.Number)) > 0 {
			log.Warn("applyStorageProof data timeout  ", "TimeOut", proofTimeOut, "currNumber", currNumber, "proof number", verifyHeader.Number)
			continue
		}
		blockSize, err := decimal.NewFromString(pocs[4])
		if err != nil {
			log.Warn("applyStorageProof blocksize err ", "blockSize", blockSize, "set storageBlockSize", storageBlockSize)
			continue
		}
		blockNum, err := decimal.NewFromString(pocs[5])
		if err != nil {
			log.Warn("applyStorageProof blockNum err ", "blockNum", blockNum)
			continue
		}
		if isLtPosAutoExitPunishChange(currNumber.Uint64()) {
			verifyCapacity := blockSize.Mul(blockNum)
			if verifyCapacity.Cmp(decimal.NewFromBigInt(capacity, 0)) != 0 {
				log.Warn("applyStorageProof capacity not same ", "verifyCapacity", verifyCapacity, "snap capacity", capacity)
				continue
			}
			blockNonce := verifyHeader.Nonce.Uint64()
			if verifyType == "v1" {
				if !verifyStoragePocV1(verifyData, rootHash.String(), blockNonce) {
					log.Warn("applyStorageProof   verify  faild", "roothash", rootHash)
					continue
				}
			} else {
				if !verifyStoragePoc(verifyData, rootHash.String(), blockNonce) {
					log.Warn("applyStorageProof   verify  faild", "roothash", rootHash)
					continue
				}
			}
		}
		if index == 0 {
			verifyResult = append(verifyResult, changeOxToUx(pledgeAddr.String())+":1")
		} else {
			verifyResult = append(verifyResult, leaseHash+":1")
		}
		storageProofRecord = append(storageProofRecord, StorageProofRecord{
			Address:                     pledgeAddr,
			RootHash:                    rootHash,
			LeaseHash:                   common.HexToHash(leaseHash),
			LastVerificationTime:        currNumber,
			LastVerificationSuccessTime: currNumber,
		})
	}

	return verifyResult, storageProofRecord
}
func (s *Snapshot) updateStorageProof(proofDatas []StorageProofRecord, headerNumber *big.Int, db ethdb.Database) {
	if proofDatas == nil || len(proofDatas) == 0 {
		return
	}
	nilHash := common.Hash{}
	for _, proof := range proofDatas {
		storagePledge := s.StorageData.StoragePledge[proof.Address]
		if storagePledge != nil {
			if proof.LeaseHash == nilHash {
				if stpgfile, ok := storagePledge.StorageSpaces.StorageFile[proof.RootHash]; ok {
					stpgfile.LastVerificationSuccessTime = proof.LastVerificationSuccessTime
					stpgfile.LastVerificationTime = proof.LastVerificationTime
					s.StorageData.accumulateSpaceStorageFileHash(proof.Address, stpgfile)
				}

			} else {
				if lease, ok := storagePledge.Lease[proof.LeaseHash]; ok {
					lease.StorageFile[proof.RootHash].LastVerificationTime = proof.LastVerificationTime
					lease.StorageFile[proof.RootHash].LastVerificationSuccessTime = proof.LastVerificationSuccessTime
					s.StorageData.accumulateLeaseStorageFileHash(proof.Address, proof.LeaseHash, lease.StorageFile[proof.RootHash])
				}

			}
		}
	}
	s.StorageData.accumulateHeaderHash()
}

func (s *StorageData) calStorageLeaseReward(capacity decimal.Decimal, bandwidthIndex decimal.Decimal, storageIndex decimal.Decimal,
	rentPrice decimal.Decimal, basePrice decimal.Decimal, totalLeaseSpace decimal.Decimal, blockNumber uint64) decimal.Decimal {
	if blockNumber >= PosrIncentiveEffectNumber {
		return s.calStorageLeaseNewReward(capacity, bandwidthIndex, storageIndex,
			rentPrice, basePrice, totalLeaseSpace)
	}
	oneEb := decimal.NewFromBigInt(tb1b, 0).Mul(decimal.NewFromInt(1048576)) //1eb= B
	modeeb := totalLeaseSpace.Mod(oneEb)
	neb := big.NewInt(1)
	if totalLeaseSpace.Cmp(oneEb) > 0 {
		neb = totalLeaseSpace.Div(oneEb).BigInt()
		if modeeb.Cmp(decimal.NewFromInt(0)) > 0 {
			neb = new(big.Int).Add(neb, big.NewInt(1))
		}
	}
	pwern, _ := decimal.NewFromString("0.9986146661010289") //0.5^1/500
	//Total_UTG(PoTS)×(1−0.5^n/400)  1EB rewards
	ebReward := decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(neb, 0))))
	beforebReward := decimal.NewFromInt(0)
	if neb.Cmp(big.NewInt(1)) > 0 {
		beforeNeb := new(big.Int).Sub(neb, big.NewInt(1))
		beforebReward = decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(beforeNeb, 0))))
	}
	ebReward = ebReward.Sub(beforebReward)
	gbUTGRate := ebReward.Div(decimal.NewFromInt(1073741824))
	priceIndex := decimal.NewFromInt(1)
	priceRate := rentPrice.Div(basePrice)
	if rentPrice.Cmp(basePrice) > 0 {
		priceIndex, _ = decimal.NewFromString("1.05")
	} else if rentPrice.Cmp(basePrice) < 0 {
		priceIndex, _ = decimal.NewFromString("0.9523809523809524")
	}
	return gbUTGRate.Mul(capacity).Mul(priceRate).Mul(priceIndex).Mul(bandwidthIndex).Mul(storageIndex)

}
func (s *StorageData) calStorageLeaseNewReward(capacity decimal.Decimal, bandwidthIndex decimal.Decimal, storageIndex decimal.Decimal,
	rentPrice decimal.Decimal, basePrice decimal.Decimal, totalLeaseSpace decimal.Decimal) decimal.Decimal {
	oneEb := decimal.NewFromBigInt(tb1b, 0).Mul(decimal.NewFromInt(1048576)) //1eb= B
	modeeb := totalLeaseSpace.Mod(oneEb)
	neb := big.NewInt(1)
	if totalLeaseSpace.Cmp(oneEb) > 0 {
		neb = totalLeaseSpace.Div(oneEb).BigInt()
		if modeeb.Cmp(decimal.NewFromInt(0)) > 0 {
			neb = new(big.Int).Add(neb, big.NewInt(1))
		}
	}
	pwern, _ := decimal.NewFromString("0.9986146661010289") //0.5^1/500
	//Total_UTG(PoTS)×(1−0.5^n/400)  1EB rewards
	ebReward := decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(neb, 0))))
	beforebReward := decimal.NewFromInt(0)
	if neb.Cmp(big.NewInt(1)) > 0 {
		beforeNeb := new(big.Int).Sub(neb, big.NewInt(1))
		beforebReward = decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(beforeNeb, 0))))
	}
	ebReward = ebReward.Sub(beforebReward)
	gbUTGRate := ebReward.Div(decimal.NewFromInt(1073741824))
	priceIndex := decimal.NewFromInt(1)
	priceRate := rentPrice.Div(basePrice)
	if rentPrice.Cmp(basePrice) > 0 {
		priceIndex = decimal.NewFromBigInt(storageRentPriceRatio, 0).Div(decimal.NewFromInt(10000)).Add(decimal.NewFromInt(1))
	} else if rentPrice.Cmp(basePrice) < 0 {
		priceIndex = decimal.NewFromBigInt(storageRentPriceRatio, 0).Div(decimal.NewFromInt(10000)).Add(decimal.NewFromInt(1))
		priceIndex = decimal.NewFromInt(1).Div(priceIndex)
	}
	return gbUTGRate.Mul(capacity).Mul(priceRate).Mul(priceIndex).Mul(bandwidthIndex.Add(storageIndex)).Mul(decimal.NewFromBigInt(storageRentAdjRatio, 0).Div(decimal.NewFromInt(10000)))
}

func (s *StorageData) accumulateLeaseRewards(ratios map[common.Address]*StorageRatio,
	addrs []common.Hash, basePrice *big.Int, revenueStorage map[common.Address]*RevenueParameter, blocknumber uint64, db ethdb.Database, snapTotalLeaseSpace *big.Int, spData *SpData, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int, *big.Int) {
	if isGEPoCrsAccCalNumber(blocknumber) {
		return s.accumulateLeaseRewards2(ratios, addrs, basePrice, revenueStorage, blocknumber, db, snapTotalLeaseSpace, spData, snap)
	}
	var LockReward []SpaceRewardRecord
	//basePrice := // SRT /TB.day
	storageHarvest := big.NewInt(0)
	if nil == addrs || len(addrs) == 0 {
		return LockReward, storageHarvest, nil, nil
	}
	totalLeaseSpace := decimal.NewFromInt(0) //B
	validSuccLesae := make(map[common.Hash]uint64)
	for _, leaseHash := range addrs {
		validSuccLesae[leaseHash] = 1
	}
	for _, storage := range s.StoragePledge {
		for leaseHash, lease := range storage.Lease {
			if _, ok := validSuccLesae[leaseHash]; ok {
				totalLeaseSpace = totalLeaseSpace.Add(decimal.NewFromBigInt(lease.Capacity, 0))
			}
		}
	}
	err := s.saveDecimalValueTodb(totalLeaseSpace, db, blocknumber, totalLeaseSpaceKey)
	if err != nil {
		log.Error("saveTotalLeaseSpace", "err", err, "number", blocknumber)
	}
	for pledgeAddr, storage := range s.StoragePledge {
		totalReward := big.NewInt(0)
		bandwidthIndex := getBandwaith(storage.Bandwidth, blocknumber)
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			for leaseHash, lease := range storage.Lease {
				if _, ok2 := validSuccLesae[leaseHash]; ok2 {
					leaseCapacity := decimal.NewFromBigInt(lease.Capacity, 0).Div(decimal.NewFromInt(1073741824)) //to GB
					//priceIndex := decimal.NewFromBigInt(lease.UnitPrice, 0).Div(decimal.NewFromBigInt(basePrice, 0))//RT/GB.day
					if item, ok3 := ratios[revenue.RevenueAddress]; ok3 {
						reward := s.calStorageLeaseReward(leaseCapacity, bandwidthIndex, item.Ratio, decimal.NewFromBigInt(lease.UnitPrice, 0), decimal.NewFromBigInt(basePrice, 0), totalLeaseSpace, blocknumber)
						totalReward = new(big.Int).Add(totalReward, reward.BigInt())
					}
				}
			}
			if totalReward.Cmp(big.NewInt(0)) > 0 {
				LockReward = append(LockReward, SpaceRewardRecord{
					Target:  pledgeAddr,
					Amount:  totalReward,
					Revenue: revenue.RevenueAddress,
				})
				storageHarvest = new(big.Int).Add(storageHarvest, totalReward)
			}
		}
	}
	err = s.saveTotalValueTodb(storageHarvest, db, blocknumber, leaseHarvestKey)
	if err != nil {
		log.Error("saveleaseHarvest", "err", err, "number", blocknumber)
	}
	return LockReward, storageHarvest, nil, nil
}

func getBandwaith(bandwidth *big.Int, blockNumber uint64) decimal.Decimal {
	if blockNumber >= PosrIncentiveEffectNumber {
		return getBandwidthRewardNewRatio(bandwidth)
	}
	if blockNumber >= StoragePledgeOptEffectNumber {
		return getBandwidthRewardRatio(bandwidth)
	}
	if blockNumber < StorageChBwEffectNumber {
		if bandwidth.Cmp(big.NewInt(29)) <= 0 {
			return decimal.NewFromInt(0)
		}
	} else {
		if bandwidth.Cmp(big.NewInt(19)) <= 0 {
			return decimal.NewFromInt(0)
		}
		if bandwidth.Cmp(big.NewInt(20)) >= 0 && bandwidth.Cmp(big.NewInt(29)) <= 0 {
			return decimal.NewFromFloat(0.3)
		}
	}

	if bandwidth.Cmp(big.NewInt(30)) >= 0 && bandwidth.Cmp(big.NewInt(50)) <= 0 {
		return decimal.NewFromFloat(0.7)
	}
	if bandwidth.Cmp(big.NewInt(51)) >= 0 && bandwidth.Cmp(big.NewInt(99)) <= 0 {
		return decimal.NewFromFloat(0.9)
	}
	if bandwidth.Cmp(big.NewInt(100)) == 0 {
		return decimal.NewFromFloat(1)
	}
	if bandwidth.Cmp(big.NewInt(101)) >= 0 && bandwidth.Cmp(big.NewInt(500)) <= 0 {
		return decimal.NewFromFloat(1.1)
	}
	if bandwidth.Cmp(big.NewInt(501)) >= 0 && bandwidth.Cmp(big.NewInt(1023)) <= 0 {
		return decimal.NewFromFloat(1.3)
	}
	return decimal.NewFromFloat(1.5)

}

func (s *StorageData) nYearSpaceProfitReward(n uint64) decimal.Decimal {
	decimalN := decimal.NewFromBigInt(new(big.Int).SetUint64(n), 0)
	yearScale, _ := decimal.NewFromString("0.7937005259840998") //1/2^(1/3)
	yearScale = decimal.New(1, 0).Sub(yearScale.Pow(decimalN))
	yearReward := yearScale.Mul(decimal.NewFromBigInt(totalSpaceProfitReward, 0))
	return yearReward.Truncate(18)
}

func (s *StorageData) checkSRentReNewPg(currentSRentReNewPg []LeaseRenewalPledgeRecord, sRentReNewPg LeaseRenewalPledgeRecord, txSender common.Address, revenueStorage map[common.Address]*RevenueParameter, exchRate uint32, passTime *big.Int, number uint64, blockPerday uint64) (*big.Int, *big.Int, *big.Int, common.Address, bool) {
	nilHash := common.Address{}
	for _, item := range currentSRentReNewPg {
		if item.Address == sRentReNewPg.Address {
			log.Info("checkSRentReNewPg", "rent pledge only one in one block", sRentReNewPg.Address)
			return nil, nil, nil, nilHash, false
		}
	}
	//checkCapacity
	if _, ok := s.StoragePledge[sRentReNewPg.Address]; !ok {
		log.Info("checkSRentReNewPg", "address not exist", sRentReNewPg.Address)
		return nil, nil, nil, nilHash, false
	}
	if _, ok := s.StoragePledge[sRentReNewPg.Address].Lease[sRentReNewPg.Hash]; !ok {
		log.Info("checkSRentReNewPg", "hash not exist", sRentReNewPg.Hash)
		return nil, nil, nil, nilHash, false
	}
	lease := s.StoragePledge[sRentReNewPg.Address].Lease[sRentReNewPg.Hash]
	if lease.Capacity.Cmp(sRentReNewPg.Capacity) != 0 {
		log.Info("checkSRentReNewPg", "lease Capacity is not equal", sRentReNewPg.Capacity)
		return nil, nil, nil, nilHash, false
	}
	//checkowner

	if lease.DepositAddress != txSender {
		log.Info("checkSRentReNewPg", "DepositAddress is not txSender", txSender)
		return nil, nil, nil, nilHash, false
	}

	hasRent := false
	duration := big.NewInt(0)
	unitPrice := lease.UnitPrice
	requestTime := common.Big0
	for _, detail := range lease.LeaseList {
		if detail.Deposit.Cmp(big.NewInt(0)) <= 0 {
			hasRent = true
			duration = detail.Duration
			requestTime = detail.RequestTime
		}
	}
	if !hasRent {
		log.Info("checkSRentReNewPg", "not has 0 Deposit", sRentReNewPg.Hash)
		return nil, nil, nil, nilHash, false
	}
	requestPassTime := new(big.Int).Add(requestTime, passTime)
	if requestPassTime.Cmp(new(big.Int).SetUint64(number)) < 0 {
		log.Info("checkSRentReNewPg", "request time pass", requestTime)
		return nil, nil, nil, nilHash, false
	}

	fStartTime := lease.LeaseList[sRentReNewPg.Hash].StartTime
	if fStartTime == nil || fStartTime.Cmp(common.Big0) == 0 {
		log.Info("checkSRentReNewPg", "fStartTime is zero ", fStartTime)
		return nil, nil, nil, nilHash, false
	}
	lDuration := new(big.Int).Mul(lease.Duration, new(big.Int).SetUint64(blockPerday))
	lEndNumber := new(big.Int).Add(fStartTime, lDuration)
	if lEndNumber.Cmp(new(big.Int).SetUint64(number)) <= 0 {
		log.Info("checkSRentReNewPg", "duration is pass ", lEndNumber)
		return nil, nil, nil, nilHash, false
	}

	//Calculate the pledge deposit
	srtAmount := new(big.Int).Mul(duration, unitPrice)
	srtAmount = new(big.Int).Mul(srtAmount, lease.Capacity)
	srtAmount = new(big.Int).Div(srtAmount, gbTob)
	amount := new(big.Int).Div(new(big.Int).Mul(srtAmount, big.NewInt(10000)), big.NewInt(int64(exchRate)))
	return srtAmount, amount, duration, lease.Address, true
}

func (a *Alien) exchangeStoragePrice(storageExchangePriceRecord []StorageExchangePriceRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int) []StorageExchangePriceRecord {
	if len(txDataInfo) < 5 {
		log.Warn("exchange   Price  of Storage", "parameter error", len(txDataInfo))
		return storageExchangePriceRecord
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if isGEInitStorageManagerNumber(blocknumber.Uint64()) {
		if _, ok := snap.StorageData.StorageEntrust[pledgeAddr]; ok {
			if snap.StorageData.StorageEntrust[pledgeAddr].Manager != txSender {
				log.Warn("isStorageManager", "txSender is not manager", txSender)
				return storageExchangePriceRecord
			}
		} else {
			log.Warn("isStorageManager", "manager is empty", pledgeAddr)
			return storageExchangePriceRecord
		}
	} else {
		if pledgeAddr != txSender {
			if revenue, ok := snap.RevenueStorage[pledgeAddr]; !ok || revenue.RevenueAddress != txSender {
				log.Warn("exchange   Price  of Storage  [no role]", " txSender", txSender)
				return storageExchangePriceRecord
			}
		}
	}
	if _, ok := snap.StorageData.StoragePledge[pledgeAddr]; !ok {
		log.Warn("exchange  Price not find Pledge", " pledgeAddr", pledgeAddr)
		return storageExchangePriceRecord
	}
	price, err := decimal.NewFromString(txDataInfo[4])
	if err != nil {
		log.Warn("exchange  Price is wrong", " price", txDataInfo[4])
		return storageExchangePriceRecord
	}
	basePrice := snap.SystemConfig.Deposit[sscEnumStoragePrice]
	minThreshold := basePrice
	if blocknumber.Uint64() >= PosrIncentiveEffectNumber {
		minThreshold = new(big.Int).Div(basePrice, big.NewInt(10))
	}
	if price.BigInt().Cmp(minThreshold) < 0 || price.BigInt().Cmp(new(big.Int).Mul(big.NewInt(10), basePrice)) > 0 {
		log.Warn("exchange  Price not legal", " pledgeAddr", pledgeAddr, "price", price, "basePrice", basePrice)
		return storageExchangePriceRecord
	}

	storageExchangePriceRecord = append(storageExchangePriceRecord, StorageExchangePriceRecord{
		Address: pledgeAddr,
		Price:   price.BigInt(),
	})
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0xb12bf5b909b60bb08c3e990dcb437a238072a91629c666541b667da82b3ee49b"))
	topics[1].SetBytes(pledgeAddr.Bytes())
	topics[2].SetBytes([]byte(txDataInfo[4]))
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return storageExchangePriceRecord
}

func (s *Snapshot) updateStoragePrice(storageExchangePriceRecord []StorageExchangePriceRecord, headerNumber *big.Int, db ethdb.Database) {
	if storageExchangePriceRecord == nil || len(storageExchangePriceRecord) == 0 {
		return
	}
	for _, exchangeprice := range storageExchangePriceRecord {
		if _, ok := s.StorageData.StoragePledge[exchangeprice.Address]; ok {
			s.StorageData.StoragePledge[exchangeprice.Address].Price = exchangeprice.Price
			s.StorageData.accumulatePledgeHash(exchangeprice.Address)
		}
	}
	s.StorageData.accumulateHeaderHash()
}

func (s *StorageData) updateLeaseRenewalPledge(pg []LeaseRenewalPledgeRecord, number *big.Int, db ethdb.Database, blockPerday uint64) {
	for _, sRentPg := range pg {
		if _, ok := s.StoragePledge[sRentPg.Address]; !ok {
			continue
		}
		if _, ok := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]; !ok {
			continue
		}
		lease := s.StoragePledge[sRentPg.Address].Lease[sRentPg.Hash]
		if lease.LeaseList == nil {
			continue
		}
		if len(lease.LeaseList) <= 1 {
			continue
		}
		if _, ok := lease.LeaseList[sRentPg.Hash]; !ok {
			continue
		}
		startTime := lease.LeaseList[sRentPg.Hash].StartTime
		if startTime.Cmp(common.Big0) == 0 {
			continue
		}
		duration := new(big.Int).Mul(lease.Duration, new(big.Int).SetUint64(blockPerday))

		lease.RootHash = sRentPg.RootHash
		lease.Deposit = new(big.Int).Add(lease.Deposit, sRentPg.BurnAmount)
		lease.Cost = new(big.Int).Add(lease.Cost, sRentPg.BurnSRTAmount)
		lease.Duration = new(big.Int).Add(lease.Duration, sRentPg.Duration)
		if _, ok := lease.StorageFile[sRentPg.RootHash]; !ok {
			lease.StorageFile[sRentPg.RootHash] = &StorageFile{
				Capacity:                    lease.Capacity,
				CreateTime:                  number,
				LastVerificationTime:        number,
				LastVerificationSuccessTime: number,
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			s.accumulateLeaseStorageFileHash(sRentPg.Address, sRentPg.Hash, lease.StorageFile[sRentPg.RootHash])
		}

		startTime = new(big.Int).Add(startTime, duration)
		startTime = new(big.Int).Add(startTime, big.NewInt(1))
		for _, detail := range lease.LeaseList {
			if detail.Deposit.Cmp(big.NewInt(0)) == 0 {
				detail.Cost = new(big.Int).Add(detail.Cost, sRentPg.BurnSRTAmount)
				detail.Deposit = new(big.Int).Add(detail.Deposit, sRentPg.BurnAmount)
				detail.PledgeHash = sRentPg.PledgeHash
				detail.StartTime = startTime
				s.accumulateLeaseDetailHash(sRentPg.Address, sRentPg.Hash, detail)
				break
			}
		}
		lease.Status = LeaseNormal
		s.accumulateLeaseHash(sRentPg.Address, lease)
	}
	s.accumulateHeaderHash()
}

func (s *StorageData) accumulateSpaceStorageFileHash(pledgeAddr common.Address, storagefile *StorageFile) common.Hash {
	storagefile.Hash = getHash(storagefile.LastVerificationTime.String() + storagefile.LastVerificationSuccessTime.String() +
		storagefile.ValidationFailureTotalTime.String() + storagefile.Capacity.String() + storagefile.CreateTime.String())
	s.accumulateSpaceHash(pledgeAddr)
	return storagefile.Hash
}

func (s *StorageData) accumulateLeaseStorageFileHash(pledgeAddr common.Address, leaseKey common.Hash, storagefile *StorageFile) {
	storagePledge := s.StoragePledge[pledgeAddr]
	lease := storagePledge.Lease[leaseKey]
	storagefile.Hash = getHash(storagefile.LastVerificationTime.String() + storagefile.LastVerificationSuccessTime.String() +
		storagefile.ValidationFailureTotalTime.String() + storagefile.Capacity.String() + storagefile.CreateTime.String())
	s.accumulateLeaseHash(pledgeAddr, lease)
}
func (s *StorageData) accumulateLeaseDetailHash(pledgeAddr common.Address, leaseKey common.Hash, leasedetail *LeaseDetail) {
	storagePledge := s.StoragePledge[pledgeAddr]
	lease := storagePledge.Lease[leaseKey]
	leasedetail.Hash = getHash(leasedetail.ValidationFailureTotalTime.String() + leasedetail.Duration.String() + leasedetail.Cost.String() +
		leasedetail.Deposit.String() + leasedetail.StartTime.String() + changeOxToUx(leasedetail.PledgeHash.String()) + changeOxToUx(leasedetail.RequestHash.String()) + leasedetail.RequestTime.String() +
		strconv.Itoa(leasedetail.Revert))
	s.accumulateLeaseHash(pledgeAddr, lease)
}
func (s *StorageData) accumulateLeaseHash(pledgeAddr common.Address, lease *Lease) common.Hash {
	var hashs []string
	for _, storagefile := range lease.StorageFile {
		hashs = append(hashs, changeOxToUx(storagefile.Hash.String()))
	}
	for _, detail := range lease.LeaseList {
		hashs = append(hashs, changeOxToUx(detail.Hash.String()))
	}
	hashs = append(hashs, changeOxToUx(lease.DepositAddress.String())+lease.UnitPrice.String()+lease.Capacity.String()+changeOxToUx(lease.RootHash.String())+changeOxToUx(lease.Address.String())+lease.Deposit.String()+strconv.Itoa(lease.Status)+lease.Cost.String()+
		lease.ValidationFailureTotalTime.String()+lease.LastVerificationSuccessTime.String()+lease.LastVerificationTime.String()+lease.Duration.String())
	sort.Strings(hashs)
	lease.Hash = getHash(hashs)
	s.accumulatePledgeHash(pledgeAddr) //accumulate  valid hash of Pledge
	return lease.Hash
}

/**
 *
 */
func (s *StorageData) accumulateSpaceHash(pledgeAddr common.Address) common.Hash {
	storageSpaces := s.StoragePledge[pledgeAddr].StorageSpaces
	var hashs []string
	for _, storagefile := range storageSpaces.StorageFile {
		hashs = append(hashs, changeOxToUx(storagefile.Hash.String()))
	}
	hashs = append(hashs, storageSpaces.ValidationFailureTotalTime.String()+storageSpaces.LastVerificationSuccessTime.String()+storageSpaces.LastVerificationTime.String()+
		changeOxToUx(storageSpaces.Address.String())+changeOxToUx(storageSpaces.RootHash.String())+storageSpaces.StorageCapacity.String())
	sort.Strings(hashs)
	storageSpaces.Hash = getHash(hashs)
	s.accumulatePledgeHash(pledgeAddr) //accumulate  valid hash of Pledge
	return storageSpaces.Hash
}
func (s *StorageData) accumulatePledgeHash(pledgeAddr common.Address) common.Hash {
	storagePledge := s.StoragePledge[pledgeAddr]
	var hashs []string
	for _, lease := range storagePledge.Lease {
		hashs = append(hashs, changeOxToUx(lease.Hash.String()))
	}
	hashs = append(hashs, changeOxToUx(storagePledge.Address.String())+
		storagePledge.LastVerificationTime.String()+
		storagePledge.LastVerificationSuccessTime.String()+
		storagePledge.ValidationFailureTotalTime.String()+
		storagePledge.Bandwidth.String()+
		storagePledge.PledgeStatus.String()+
		storagePledge.Number.String()+
		storagePledge.SpaceDeposit.String()+
		changeOxToUx(storagePledge.StorageSpaces.Hash.String())+
		storagePledge.Price.String()+
		storagePledge.StorageSize.String()+
		storagePledge.TotalCapacity.String())
	sort.Strings(hashs)
	storagePledge.Hash = getHash(hashs)
	return storagePledge.Hash
}

/**
*    accumulate   Validhash  of root hash
 */
func (s *StorageData) accumulateHeaderHash() common.Hash {
	var hashs []string
	for address, storagePledge := range s.StoragePledge {
		hashs = append(hashs, changeOxToUx(storagePledge.Hash.String()), changeOxToUx(address.Hash().String()))
	}
	sort.Strings(hashs)
	s.Hash = getHash(hashs)
	return s.Hash
}

func getHash(obj interface{}) common.Hash {
	hasher := sha3.NewLegacyKeccak256()
	rlp.Encode(hasher, obj)
	var hash common.Hash
	hasher.Sum(hash[:0])
	return hash
}

func (s *StorageData) storageVerify(number uint64, blockPerday uint64, revenueStorage map[common.Address]*RevenueParameter) ([]common.Address, []common.Hash, map[common.Address]*StorageRatio, map[common.Address]*big.Int) {
	if number > PledgeRevertLockEffectNumber {
		return s.storageVerify2(number, blockPerday, revenueStorage)
	}
	sussSPAddrs := make([]common.Address, 0)
	sussRentHashs := make([]common.Hash, 0)
	storageRatios := make(map[common.Address]*StorageRatio, 0)

	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	bigOne := big.NewInt(1)
	for pledgeAddr, sPledge := range s.StoragePledge {
		isSfVerSucc := true
		capSucc := big.NewInt(0)
		rentSuccCount := 0
		storagespaces := s.StoragePledge[pledgeAddr].StorageSpaces
		sfiles := storagespaces.StorageFile
		for _, sfile := range sfiles {
			lastVerSuccTime := sfile.LastVerificationSuccessTime
			if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
				isSfVerSucc = false
				sfile.ValidationFailureTotalTime = new(big.Int).Add(sfile.ValidationFailureTotalTime, bigOne)
				s.accumulateSpaceStorageFileHash(pledgeAddr, sfile)
			} else {
				capSucc = new(big.Int).Add(capSucc, sfile.Capacity)
			}
		}
		if isSfVerSucc {
			storagespaces.LastVerificationSuccessTime = beforeZeroTime
		} else {
			storagespaces.ValidationFailureTotalTime = new(big.Int).Add(storagespaces.ValidationFailureTotalTime, bigOne)
		}
		storagespaces.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
		leases := make(map[common.Hash]*Lease)
		for lhash, l := range sPledge.Lease {
			if l.Status == LeaseNormal || l.Status == LeaseBreach {
				leases[lhash] = l
			}
		}
		for lhash, lease := range leases {
			isVerSucc := true
			storageFile := lease.StorageFile
			for _, file := range storageFile {
				lastVerSuccTime := file.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
					isVerSucc = false
					file.ValidationFailureTotalTime = new(big.Int).Add(file.ValidationFailureTotalTime, bigOne)
					s.accumulateLeaseStorageFileHash(pledgeAddr, lhash, file)
				} else {
					capSucc = new(big.Int).Add(capSucc, file.Capacity)
				}
			}
			leaseLists := lease.LeaseList
			expireNumber := big.NewInt(0)
			for _, leaseDetail := range leaseLists {
				deposit := leaseDetail.Deposit
				if deposit.Cmp(big.NewInt(0)) > 0 {
					startTime := leaseDetail.StartTime
					duration := leaseDetail.Duration
					leaseDetailEndNumber := new(big.Int).Add(startTime, new(big.Int).Mul(duration, new(big.Int).SetUint64(blockPerday)))
					if startTime.Cmp(beforeZeroTime) <= 0 && leaseDetailEndNumber.Cmp(beforeZeroTime) >= 0 {
						if !isVerSucc {
							leaseDetail.ValidationFailureTotalTime = new(big.Int).Add(lease.ValidationFailureTotalTime, bigOne)
							s.accumulateLeaseDetailHash(pledgeAddr, lhash, leaseDetail)
						}
					}
					if expireNumber.Cmp(leaseDetailEndNumber) < 0 {
						expireNumber = leaseDetailEndNumber
					}
				}
			}
			if expireNumber.Cmp(beforeZeroTime) <= 0 {
				lease.Status = LeaseExpiration
			}
			//cal ROOT HASH

			if isVerSucc {
				lease.LastVerificationSuccessTime = beforeZeroTime
				sussRentHashs = append(sussRentHashs, lhash)
				rentSuccCount++
				if lease.Status == LeaseBreach {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if lease.ValidationFailureTotalTime.Cmp(duration10) < 0 {
						lease.Status = LeaseNormal
					}
				}
			} else {
				lease.ValidationFailureTotalTime = new(big.Int).Add(lease.ValidationFailureTotalTime, bigOne)
				if lease.Status == LeaseNormal {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if lease.ValidationFailureTotalTime.Cmp(duration10) > 0 {
						lease.Status = LeaseBreach
					}
				}
			}
			lease.LastVerificationTime = beforeZeroTime
			s.accumulateLeaseHash(pledgeAddr, lease)
		}
		storageCapacity := storagespaces.StorageCapacity
		rent51 := len(leases) * 51 / 100
		isPledgeVerSucc := false
		cap90 := new(big.Int).Mul(big.NewInt(90), sPledge.TotalCapacity)
		cap90 = new(big.Int).Div(cap90, big.NewInt(100))
		if len(leases) == 0 {
			if capSucc.Cmp(cap90) >= 0 {
				isPledgeVerSucc = true
			}
		} else if storageCapacity.Cmp(big.NewInt(0)) == 0 {
			if rentSuccCount >= rent51 {
				isPledgeVerSucc = true
			}
		} else {
			if rentSuccCount >= rent51 && capSucc.Cmp(cap90) >= 0 {
				isPledgeVerSucc = true
			}
		}
		if isPledgeVerSucc {
			sussSPAddrs = append(sussSPAddrs, pledgeAddr)
			if revenue, ok := revenueStorage[pledgeAddr]; ok {
				if _, ok2 := storageRatios[revenue.RevenueAddress]; !ok2 {
					storageRatios[revenue.RevenueAddress] = &StorageRatio{
						Capacity: sPledge.TotalCapacity,
						Ratio:    decimal.NewFromInt(0),
					}
				} else {
					storageRatios[revenue.RevenueAddress].Capacity = new(big.Int).Add(storageRatios[revenue.RevenueAddress].Capacity, sPledge.TotalCapacity)
				}
			}
			sPledge.LastVerificationSuccessTime = beforeZeroTime
		} else {
			sPledge.ValidationFailureTotalTime = new(big.Int).Add(sPledge.ValidationFailureTotalTime, bigOne)
			maxFailNum := maxStgVerContinueDayFail * blockPerday
			bigMaxFailNum := new(big.Int).SetUint64(maxFailNum)
			if beforeZeroTime.Cmp(bigMaxFailNum) >= 0 {
				beforeSevenDayNumber := new(big.Int).Sub(beforeZeroTime, bigMaxFailNum)
				lastVerSuccTime := sPledge.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeSevenDayNumber) <= 0 {
					sPledge.PledgeStatus = big.NewInt(SPledgeRemoving)
				}
			}
		}
		sPledge.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
	}
	//cal ROOT HASH
	s.accumulateHeaderHash()
	return sussSPAddrs, sussRentHashs, storageRatios, nil
}
func (s *StorageData) storageVerify2(number uint64, blockPerday uint64, revenueStorage map[common.Address]*RevenueParameter) ([]common.Address, []common.Hash, map[common.Address]*StorageRatio, map[common.Address]*big.Int) {
	if isGEInitStorageManagerNumber(number) {
		return s.storageVerify3(number, blockPerday, revenueStorage)
	}
	sussSPAddrs := make([]common.Address, 0)
	sussRentHashs := make([]common.Hash, 0)
	storageRatios := make(map[common.Address]*StorageRatio, 0)
	capSuccAddrs := make(map[common.Address]*big.Int, 0)

	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	beforeZeroTime = new(big.Int).Add(beforeZeroTime, common.Big1)
	bigOne := big.NewInt(1)
	for pledgeAddr, sPledge := range s.StoragePledge {
		isSfVerSucc := true
		capSucc := big.NewInt(0)
		storagespaces := s.StoragePledge[pledgeAddr].StorageSpaces
		sfiles := storagespaces.StorageFile
		for _, sfile := range sfiles {
			lastVerSuccTime := sfile.LastVerificationSuccessTime
			if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
				isSfVerSucc = false
				sfile.ValidationFailureTotalTime = new(big.Int).Add(sfile.ValidationFailureTotalTime, bigOne)
				s.accumulateSpaceStorageFileHash(pledgeAddr, sfile)
			} else {
				capSucc = new(big.Int).Add(capSucc, sfile.Capacity)
			}
		}
		if isSfVerSucc {
			storagespaces.LastVerificationSuccessTime = beforeZeroTime
		} else {
			storagespaces.ValidationFailureTotalTime = new(big.Int).Add(storagespaces.ValidationFailureTotalTime, bigOne)
		}
		storagespaces.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
		leases := make(map[common.Hash]*Lease)
		for lhash, l := range sPledge.Lease {
			if l.Status == LeaseNormal || l.Status == LeaseBreach {
				leases[lhash] = l
			}
		}
		for lhash, lease := range leases {
			isVerSucc := true
			storageFile := lease.StorageFile
			for _, file := range storageFile {
				lastVerSuccTime := file.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
					isVerSucc = false
					file.ValidationFailureTotalTime = new(big.Int).Add(file.ValidationFailureTotalTime, bigOne)
					s.accumulateLeaseStorageFileHash(pledgeAddr, lhash, file)
				} else {
					capSucc = new(big.Int).Add(capSucc, file.Capacity)
				}
			}
			leaseLists := lease.LeaseList
			expireNumber := big.NewInt(0)
			for ldhash, leaseDetail := range leaseLists {
				deposit := leaseDetail.Deposit
				if deposit.Cmp(big.NewInt(0)) > 0 {
					startTime := leaseDetail.StartTime
					duration := leaseDetail.Duration
					leaseDetailEndNumber := new(big.Int).Add(startTime, new(big.Int).Mul(duration, new(big.Int).SetUint64(blockPerday)))
					if ldhash != lhash {
						leaseDetailEndNumber = new(big.Int).Sub(leaseDetailEndNumber, common.Big1)
					}
					if startTime.Cmp(beforeZeroTime) <= 0 && leaseDetailEndNumber.Cmp(beforeZeroTime) >= 0 {
						if !isVerSucc {
							leaseDetail.ValidationFailureTotalTime = new(big.Int).Add(leaseDetail.ValidationFailureTotalTime, bigOne)
							s.accumulateLeaseDetailHash(pledgeAddr, lhash, leaseDetail)
						}
					}
					if expireNumber.Cmp(leaseDetailEndNumber) < 0 {
						expireNumber = leaseDetailEndNumber
					}
				}
			}
			if expireNumber.Cmp(bigNumber) <= 0 {
				lease.Status = LeaseExpiration
			}
			//cal ROOT HASH

			if isVerSucc {
				lease.LastVerificationSuccessTime = beforeZeroTime
				sussRentHashs = append(sussRentHashs, lhash)
				if lease.Status == LeaseBreach {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if lease.ValidationFailureTotalTime.Cmp(duration10) < 0 {
						lease.Status = LeaseNormal
					}
				}
			} else {
				lease.ValidationFailureTotalTime = new(big.Int).Add(lease.ValidationFailureTotalTime, bigOne)
				if lease.Status == LeaseNormal {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if isGTIncentiveEffect(number) {
						if lease.ValidationFailureTotalTime.Cmp(duration10) >= 0 {
							lease.Status = LeaseBreach
						}
					} else {
						if lease.ValidationFailureTotalTime.Cmp(duration10) > 0 {
							lease.Status = LeaseBreach
						}
					}
				}
			}
			lease.LastVerificationTime = beforeZeroTime
			s.accumulateLeaseHash(pledgeAddr, lease)
		}

		isPledgeVerSucc := false
		cap80 := new(big.Int).Mul(capSucNeedPer, sPledge.TotalCapacity)
		cap80 = new(big.Int).Div(cap80, big.NewInt(100))
		if capSucc.Cmp(cap80) > 0 {
			isPledgeVerSucc = true
		}
		if isPledgeVerSucc {
			sussSPAddrs = append(sussSPAddrs, pledgeAddr)
			if _, ok := revenueStorage[pledgeAddr]; ok {
				if _, ok3 := capSuccAddrs[pledgeAddr]; !ok3 {
					capSuccAddrs[pledgeAddr] = capSucc
				}
			}
			sPledge.LastVerificationSuccessTime = beforeZeroTime
		} else {
			sPledge.ValidationFailureTotalTime = new(big.Int).Add(sPledge.ValidationFailureTotalTime, bigOne)
			maxFailNum := maxStgVerContinueDayFail * blockPerday
			bigMaxFailNum := new(big.Int).SetUint64(maxFailNum)
			if beforeZeroTime.Cmp(bigMaxFailNum) >= 0 {
				beforeSevenDayNumber := new(big.Int).Sub(beforeZeroTime, bigMaxFailNum)
				lastVerSuccTime := sPledge.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeSevenDayNumber) <= 0 {
					sPledge.PledgeStatus = big.NewInt(SPledgeRemoving)
				}
			}
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if capSucc.Cmp(common.Big0) > 0 {
				if _, ok2 := storageRatios[revenue.RevenueAddress]; !ok2 {
					storageRatios[revenue.RevenueAddress] = &StorageRatio{
						Capacity: capSucc,
						Ratio:    decimal.NewFromInt(0),
					}
				} else {
					storageRatios[revenue.RevenueAddress].Capacity = new(big.Int).Add(storageRatios[revenue.RevenueAddress].Capacity, capSucc)
				}
			}
		}
		sPledge.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
	}
	//cal ROOT HASH
	s.accumulateHeaderHash()
	return sussSPAddrs, sussRentHashs, storageRatios, capSuccAddrs
}

func (s *StorageData) dealLeaseStatus(number uint64, rate uint32, blockPerday uint64, revenueStorage map[common.Address]*RevenueParameter, snap *Snapshot) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	revertLockReward := make([]SpaceRewardRecord, 0)
	revertExchangeSRT := make([]ExchangeSRTRecord, 0)
	delPledge := make([]common.Address, 0)
	bAmount := common.Big0
	for pledgeAddress, sPledge := range s.StoragePledge {
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRetrun)) == 0 {
			continue
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 || sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
			sPledgePledgeStatus := new(big.Int).Set(sPledge.PledgeStatus)
			sPledge.PledgeStatus = big.NewInt(SPledgeRetrun)
			revertLockReward, revertExchangeSRT, bAmount = s.dealSPledgeRevert(revertLockReward, revertExchangeSRT, sPledge, rate, number, blockPerday, sPledgePledgeStatus, bAmount, revenueStorage, pledgeAddress, snap)
			delPledge = append(delPledge, pledgeAddress)
			s.accumulateSpaceHash(pledgeAddress)
			continue
		}

		leases := sPledge.Lease
		for lHash, lease := range leases {
			if lease.Status == LeaseReturn {
				continue
			}
			if lease.Status == LeaseUserRescind || lease.Status == LeaseExpiration {
				lStatus := lease.Status
				lease.Status = LeaseReturn
				revertLockReward, revertExchangeSRT, bAmount = s.dealLeaseRevert(lease, revertLockReward, revertExchangeSRT, rate, lStatus, number, lHash, blockPerday, bAmount)
				s.accumulateLeaseHash(pledgeAddress, lease)
			}
		}
	}
	for _, delAddr := range delPledge {
		if isGEInitStorageManagerNumber(number) {
			s.deleteSpCapAndRs(delAddr, snap)
		}
		delete(s.StoragePledge, delAddr)
	}
	snap.SpData.accumulateSpDataHash()
	s.accumulateHeaderHash()
	return revertLockReward, revertExchangeSRT, bAmount
}

func (s *StorageData) dealSPledgeRevert(revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, pledge *SPledge, rate uint32, number uint64, blockPerday uint64, sPledgePledgeStatus *big.Int, bAmount *big.Int, revenueStorage map[common.Address]*RevenueParameter, pledgeAddress common.Address, snap *Snapshot) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	revertLockReward, revertExchangeSRT, bAmount = s.dealSPledgeRevert2(pledge, revertLockReward, revertExchangeSRT, rate, number, blockPerday, bAmount, revenueStorage, pledgeAddress, snap)
	leases := pledge.Lease
	for lHash, l := range leases {
		lStatus := l.Status
		if l.Status == LeaseReturn {
			continue
		}
		if isGTIncentiveEffect(number) {
			if sPledgePledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 {
				lStatus = LeaseUserRescind
			}
		}
		revertLockReward, revertExchangeSRT, bAmount = s.dealLeaseRevert(l, revertLockReward, revertExchangeSRT, rate, lStatus, number, lHash, blockPerday, bAmount)
	}
	return revertLockReward, revertExchangeSRT, bAmount
}
func (s *StorageData) dealSPledgeRevert2(pledge *SPledge, revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, rate uint32, number uint64, blockPerday uint64, bAmount *big.Int, revenueStorage map[common.Address]*RevenueParameter, pledgeAddress common.Address, snap *Snapshot) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	if number > SPledgeRevertFixBlockNumber {
		return s.dealSPledgeRevert3(pledge, revertLockReward, revertExchangeSRT, rate, number, blockPerday, bAmount, revenueStorage, pledgeAddress, snap)
	}
	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay)
	startNumber := pledge.Number
	duration := new(big.Int).Sub(zeroTime, startNumber)
	duration = new(big.Int).Div(duration, bigblockPerDay)
	zero := big.NewInt(0)
	vFTT := pledge.ValidationFailureTotalTime
	deposit := pledge.SpaceDeposit
	depositAddress := pledge.Address
	revertDeposit := big.NewInt(0)
	if vFTT.Cmp(zero) > 0 {
		if duration.Cmp(vFTT) > 0 {
			revertAmount := new(big.Int).Mul(deposit, vFTT)
			revertAmount = new(big.Int).Div(revertAmount, duration)
			revertDeposit = new(big.Int).Sub(deposit, revertAmount)
		}
	} else {
		revertDeposit = deposit
	}
	if revertDeposit.Cmp(zero) > 0 {
		revertLockReward = append(revertLockReward, SpaceRewardRecord{
			Target:  depositAddress,
			Amount:  revertDeposit,
			Revenue: depositAddress,
		})
	}
	return revertLockReward, revertExchangeSRT, nil
}

func (s *StorageData) dealLeaseRevert(l *Lease, revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, rate uint32, lStatus int, number uint64, lHash common.Hash, blockPerday uint64, bAmount *big.Int) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	if isGTIncentiveEffect(number) {
		if lStatus == LeaseUserRescind {
			return s.dealLeaseRevertRescind(l, revertLockReward, revertExchangeSRT, rate, number, lHash, blockPerday, bAmount)
		}
	}
	zero := big.NewInt(0)
	vFTT := l.ValidationFailureTotalTime
	deposit := l.Deposit
	duration := l.Duration
	address := l.Address
	depositAddress := l.DepositAddress
	revertSRTAmount := big.NewInt(0)
	revertDeposit := big.NewInt(0)
	if vFTT.Cmp(zero) > 0 {
		if duration.Cmp(vFTT) > 0 {
			revertAmount := new(big.Int).Mul(deposit, vFTT)
			revertAmount = new(big.Int).Div(revertAmount, duration)
			revertDeposit = new(big.Int).Sub(deposit, revertAmount)
			revertAmount = new(big.Int).Div(new(big.Int).Mul(revertAmount, big.NewInt(int64(rate))), big.NewInt(10000))
			revertSRTAmount = new(big.Int).Add(revertSRTAmount, revertAmount)
		} else {
			revertAmount := new(big.Int).Div(new(big.Int).Mul(deposit, big.NewInt(int64(rate))), big.NewInt(10000))
			revertSRTAmount = new(big.Int).Add(revertSRTAmount, revertAmount)
		}
	} else {
		revertDeposit = deposit
	}
	if revertDeposit.Cmp(zero) > 0 {
		revertLockReward = append(revertLockReward, SpaceRewardRecord{
			Target:  depositAddress,
			Amount:  revertDeposit,
			Revenue: depositAddress,
		})

	}

	if revertSRTAmount.Cmp(zero) > 0 {
		revertExchangeSRT = append(revertExchangeSRT, ExchangeSRTRecord{
			Target: address,
			Amount: revertSRTAmount,
		})
	}
	return revertLockReward, revertExchangeSRT, bAmount
}

func (s *StorageData) calcStorageRatio(ratios map[common.Address]*StorageRatio, number uint64) map[common.Address]*StorageRatio {
	for _, ratio := range ratios {
		ratio.Ratio = s.calStorageRatio(ratio.Capacity, number)
	}
	return ratios
}
func (s *StorageData) calStorageNewRatio(totalCapacity *big.Int) decimal.Decimal {
	calCapacity := new(big.Int).Set(totalCapacity)
	if calCapacity.Cmp(eb1b) > 0 {
		calCapacity = new(big.Int).Set(eb1b)
	}
	log2Value, _ := decimal.NewFromString("0.3010299956639812")
	storageRatio := decimal.NewFromInt(0)
	if calCapacity.Cmp(tb1b) > 0 {
		tbcapity, _ := decimal.NewFromBigInt(new(big.Int).Div(calCapacity, tb1b), 0).Float64()
		storageRatio = decimal.NewFromFloat(math.Log10(tbcapity)).Div(log2Value)
	}
	return storageRatio.Div(decimal.NewFromBigInt(storageRewardGainRatio, 0)).Add(decimal.NewFromBigInt(storageRewardAdjRatio, 0).Div(decimal.NewFromInt(10000))).Round(6)
}
func (s *StorageData) calStorageRatio(totalCapacity *big.Int, blockNumber uint64) decimal.Decimal {
	if blockNumber >= PosrIncentiveEffectNumber {
		return s.calStorageNewRatio(totalCapacity)
	}
	tb1b1024 := new(big.Int).Mul(big.NewInt(1024), tb1b)
	tb1b500 := new(big.Int).Mul(big.NewInt(500), tb1b)
	tb1b50 := new(big.Int).Mul(big.NewInt(50), tb1b)
	pd50 := new(big.Int).Mul(big.NewInt(50), tb1b1024)
	pd500 := new(big.Int).Mul(big.NewInt(500), tb1b1024)
	pd1024 := new(big.Int).Mul(big.NewInt(1024), tb1b1024)
	if totalCapacity.Cmp(pd1024) >= 0 {
		return decimal.NewFromInt(2)
	}
	if totalCapacity.Cmp(pd1024) < 0 && totalCapacity.Cmp(pd500) > 0 {
		return decimal.NewFromFloat(1.8)
	}

	if totalCapacity.Cmp(pd500) <= 0 && totalCapacity.Cmp(pd50) > 0 {
		return decimal.NewFromFloat(1.5)
	}

	if totalCapacity.Cmp(pd50) <= 0 && totalCapacity.Cmp(tb1b1024) > 0 {
		return decimal.NewFromFloat(1.2)
	}
	if totalCapacity.Cmp(tb1b1024) == 0 {
		return decimal.NewFromInt(1)
	}
	if totalCapacity.Cmp(tb1b1024) < 0 && totalCapacity.Cmp(tb1b500) > 0 {
		return decimal.NewFromFloat(0.7)
	}
	if totalCapacity.Cmp(tb1b500) <= 0 && totalCapacity.Cmp(tb1b50) > 0 {
		return decimal.NewFromFloat(0.5)
	}
	if totalCapacity.Cmp(tb1b50) <= 0 && totalCapacity.Cmp(tb1b) > 0 {
		return decimal.NewFromFloat(0.3)
	}
	if totalCapacity.Cmp(tb1b) == 0 {
		return decimal.NewFromFloat(0.1)
	}
	return decimal.NewFromInt(0)
}

func (s *StorageData) calcStoragePledgeReward(ratios map[common.Address]*StorageRatio, revenueStorage map[common.Address]*RevenueParameter, number uint64, period uint64, sussSPAddrs []common.Address, capSuccAddrs map[common.Address]*big.Int, db ethdb.Database, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int) {
	if number > PledgeRevertLockEffectNumber {
		return s.calcStoragePledgeReward2(ratios, revenueStorage, number, period, sussSPAddrs, capSuccAddrs, db, snap)
	}
	reward := make([]SpaceRewardRecord, 0)
	storageHarvest := big.NewInt(0)
	leftAmount := common.Big0

	blockNumPerYear := secondsPerYear / period
	yearCount := (number - StorageEffectBlockNumber) / blockNumPerYear

	var yearReward decimal.Decimal
	yearCount++
	if yearCount == 1 {
		yearReward = s.nYearSpaceProfitReward(yearCount)
	} else {
		yearReward = s.nYearSpaceProfitReward(yearCount).Sub(s.nYearSpaceProfitReward(yearCount - 1))
	}
	spaceProfitReward := yearReward.Div(decimal.NewFromInt(365))
	if number > AdjustSPRBlockNumber {
		leftAmount = new(big.Int).Set(spaceProfitReward.BigInt())
	}
	if nil == ratios || len(ratios) == 0 {
		return reward, storageHarvest, leftAmount
	}
	validSuccSPAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range sussSPAddrs {
		validSuccSPAddrs[sPAddrs] = 1
	}

	totalPledgeReward := big.NewInt(0)
	for pledgeAddr, sPledge := range s.StoragePledge {
		if number > SPledgeRevertFixBlockNumber {
			if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
				continue
			}
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if ratio, ok2 := ratios[revenue.RevenueAddress]; ok2 {
				bandwidthIndex := getBandwaith(sPledge.Bandwidth, number)
				pledgeReward := decimal.NewFromBigInt(sPledge.TotalCapacity, 0).Mul(bandwidthIndex).BigInt()
				pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(ratio.Ratio).BigInt()
				totalPledgeReward = new(big.Int).Add(totalPledgeReward, pledgeReward)

			}
		}
	}
	if totalPledgeReward.Cmp(common.Big0) == 0 {
		return reward, storageHarvest, leftAmount
	}

	if number > AdjustSPRBlockNumber {
		tb1b1024 := new(big.Int).Mul(big.NewInt(1024), tb1b)
		pt100 := new(big.Int).Mul(big.NewInt(100), tb1b1024)
		if totalPledgeReward.Cmp(pt100) < 0 {
			totalPledgeReward = pt100
		}
	}

	for pledgeAddr, sPledge := range s.StoragePledge {
		if number > SPledgeRevertFixBlockNumber {
			if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
				continue
			}
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if ratio, ok2 := ratios[revenue.RevenueAddress]; ok2 {
				bandwidthIndex := getBandwaith(sPledge.Bandwidth, number)
				pledgeReward := decimal.NewFromBigInt(sPledge.TotalCapacity, 0).Mul(bandwidthIndex).BigInt()
				pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(ratio.Ratio).BigInt()
				pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(spaceProfitReward).BigInt()
				pledgeReward = new(big.Int).Div(pledgeReward, totalPledgeReward)
				reward = append(reward, SpaceRewardRecord{
					Target:  pledgeAddr,
					Amount:  pledgeReward,
					Revenue: revenue.RevenueAddress,
				})
				storageHarvest = new(big.Int).Add(storageHarvest, pledgeReward)
			}
		}
	}

	if number > AdjustSPRBlockNumber {
		bigSPR := spaceProfitReward.BigInt()
		if bigSPR.Cmp(storageHarvest) > 0 {
			leftAmount = new(big.Int).Sub(bigSPR, storageHarvest)
		}
	}
	return reward, storageHarvest, leftAmount
}

func (s *StorageData) calcStoragePledgeReward2(ratios map[common.Address]*StorageRatio, revenueStorage map[common.Address]*RevenueParameter, number uint64, period uint64, sussSPAddrs []common.Address, capSuccAddrs map[common.Address]*big.Int, db ethdb.Database, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int) {
	if isGEGrantEffectNumber(number) {
		return s.calcStoragePledgeReward3(ratios, revenueStorage, number, period, sussSPAddrs, capSuccAddrs, db, snap)
	}
	reward := make([]SpaceRewardRecord, 0)
	storageHarvest := big.NewInt(0)
	leftAmount := common.Big0

	blockNumPerYear := secondsPerYear / period
	yearCount := (number - StorageEffectBlockNumber) / blockNumPerYear

	var yearReward decimal.Decimal
	yearCount++
	if yearCount == 1 {
		yearReward = s.nYearSpaceProfitReward(yearCount)
	} else {
		yearReward = s.nYearSpaceProfitReward(yearCount).Sub(s.nYearSpaceProfitReward(yearCount - 1))
	}
	spaceProfitReward := yearReward.Div(decimal.NewFromInt(365))
	leftAmount = new(big.Int).Set(spaceProfitReward.BigInt())
	if nil == ratios || len(ratios) == 0 {
		return reward, storageHarvest, leftAmount
	}
	validSuccSPAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range sussSPAddrs {
		validSuccSPAddrs[sPAddrs] = 1
	}
	originalTotalCapacity := common.Big0
	totalPledgeReward := big.NewInt(0)
	for pledgeAddr, sPledge := range s.StoragePledge {
		if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
			continue
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if ratio, ok2 := ratios[revenue.RevenueAddress]; ok2 {
				bandwidthIndex := getBandwaith(sPledge.Bandwidth, number)
				pledgeReward := decimal.NewFromBigInt(sPledge.TotalCapacity, 0).Mul(bandwidthIndex).BigInt()
				pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(ratio.Ratio).BigInt()
				totalPledgeReward = new(big.Int).Add(totalPledgeReward, pledgeReward)
				originalTotalCapacity = new(big.Int).Add(originalTotalCapacity, sPledge.TotalCapacity)
			}
		}
	}
	err := s.saveTotalValueTodb(originalTotalCapacity, db, number, originalTotalCapacityKey)
	if err != nil {
		log.Error("saveOriginalTotalCapacity", "err", err, "number", number)
	}
	err = s.saveTotalValueTodb(totalPledgeReward, db, number, totalPledgeRewardKey)
	if err != nil {
		log.Error("saveTotalPledgeReward", "err", err, "number", number)
	}
	if totalPledgeReward.Cmp(common.Big0) <= 0 {
		return reward, storageHarvest, leftAmount
	}

	if totalPledgeReward.Cmp(eb1b) <= 0 {
		totalPledgeReward = new(big.Int).Add(totalPledgeReward, getAddPB(totalPledgeReward, number))
	}
	for pledgeAddr, sPledge := range s.StoragePledge {
		if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
			continue
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if ratio, ok2 := ratios[revenue.RevenueAddress]; ok2 {
				if capSucc, ok3 := capSuccAddrs[pledgeAddr]; ok3 {
					if capSucc.Cmp(common.Big0) > 0 {
						bandwidthIndex := getBandwaith(sPledge.Bandwidth, number)
						pledgeReward := decimal.NewFromBigInt(sPledge.TotalCapacity, 0).Mul(bandwidthIndex).BigInt()
						pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(ratio.Ratio).BigInt()
						pledgeReward = decimal.NewFromBigInt(pledgeReward, 0).Mul(spaceProfitReward).BigInt()
						pledgeReward = new(big.Int).Div(pledgeReward, totalPledgeReward)
						reward = append(reward, SpaceRewardRecord{
							Target:  pledgeAddr,
							Amount:  pledgeReward,
							Revenue: revenue.RevenueAddress,
						})
						storageHarvest = new(big.Int).Add(storageHarvest, pledgeReward)
					}
				}
			}
		}
	}

	bigSPR := spaceProfitReward.BigInt()
	if bigSPR.Cmp(storageHarvest) > 0 {
		leftAmount = new(big.Int).Sub(bigSPR, storageHarvest)
	} else {
		leftAmount = common.Big0
	}
	err = s.saveTotalValueTodb(storageHarvest, db, number, storageHarvestKey)
	if err != nil {
		log.Error("saveStorageHarvest", "err", err, "number", number)
	}
	return reward, storageHarvest, leftAmount
}

func (s *StorageData) saveSpaceLockRewardTodb(reward []SpaceRewardRecord, storage map[common.Address]*RevenueParameter, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storagePledgeRewardkey, number)
	blob, err := json.Marshal(reward)
	if err != nil {
		return err
	}

	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) loadLockReward(db ethdb.Database, number uint64, rewardKey string) ([]SpaceRewardRecord, error) {
	key := fmt.Sprintf(rewardKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadLockReward Get", "err", err)
		return nil, err
	}
	reward := make([]SpaceRewardRecord, 0)
	if err := json.Unmarshal(blob, &reward); err != nil {
		log.Info("loadLockReward Unmarshal", "err", err)
		return nil, err
	}
	return reward, nil
}

func (s *StorageData) saveLeaseLockRewardTodb(reward []SpaceRewardRecord, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storageLeaseRewardkey, number)
	blob, err := json.Marshal(reward)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) deletePasstimeLease(number uint64, blockPerday uint64, passTime *big.Int) {
	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	for pledgeAddr, sPledge := range s.StoragePledge {
		leases := sPledge.Lease
		delLeases := make([]common.Hash, 0)
		for h, lease := range leases {
			leaseDetails := lease.LeaseList
			delLeaseDetails := make([]common.Hash, 0)
			for hash, detail := range leaseDetails {
				deposit := detail.Deposit
				if deposit.Cmp(big.NewInt(0)) <= 0 {
					requestTime := detail.RequestTime
					requestTimeAddPassTime := new(big.Int).Add(requestTime, passTime)
					if requestTimeAddPassTime.Cmp(zeroTime) < 0 {
						delLeaseDetails = append(delLeaseDetails, hash)
					}
				}
			}
			for _, hash := range delLeaseDetails {
				delete(leaseDetails, hash)
				s.accumulateLeaseHash(pledgeAddr, lease)
			}
			if len(leaseDetails) == 0 {
				delLeases = append(delLeases, h)
			}
		}
		for _, hash := range delLeases {
			delete(leases, hash)
			s.accumulatePledgeHash(pledgeAddr)
		}
	}
	s.accumulateHeaderHash()
}

func (s *StorageData) saveSPledgeSuccTodb(addrs []common.Address, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storagePleageKey, number)
	blob, err := json.Marshal(addrs)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) loadSPledgeSucc(db ethdb.Database, number uint64) ([]common.Address, error) {
	key := fmt.Sprintf(storagePleageKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadSPledgeSucc Get", "err", err)
		return nil, err
	}
	addrs := make([]common.Address, 0)
	if err := json.Unmarshal(blob, &addrs); err != nil {
		log.Info("loadSPledgeSucc Unmarshal", "err", err)
		return nil, err
	}
	return addrs, nil
}

func (s *StorageData) saveRentSuccTodb(addrs []common.Hash, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storageContractKey, number)
	blob, err := json.Marshal(addrs)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) loadRentSucc(db ethdb.Database, number uint64) ([]common.Hash, error) {
	key := fmt.Sprintf(storageContractKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadRentSucc Get", "err", err)
		return nil, err
	}
	addrs := make([]common.Hash, 0)
	if err := json.Unmarshal(blob, &addrs); err != nil {
		log.Info("loadRentSucc Unmarshal", "err", err)
		return nil, err
	}
	return addrs, nil
}

func (s *StorageData) saveRevertSpaceLockRewardTodb(reward []SpaceRewardRecord, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(revertSpaceLockRewardkey, number)
	blob, err := json.Marshal(reward)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) saveRevertExchangeSRTTodb(exchangeSRT []ExchangeSRTRecord, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(revertExchangeSRTkey, number)
	blob, err := json.Marshal(exchangeSRT)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) lockStorageRatios(db ethdb.Database, number uint64) (map[common.Address]*StorageRatio, error) {
	key := fmt.Sprintf(storageRatioskey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("lockStorageRatios Get", "err", err)
		return nil, err
	}
	ratios := make(map[common.Address]*StorageRatio)
	if err := json.Unmarshal(blob, &ratios); err != nil {
		log.Info("lockStorageRatios Unmarshal", "err", err)
		return nil, err
	}
	return ratios, nil
}

func (s *StorageData) lockRevertSRT(db ethdb.Database, number uint64) ([]ExchangeSRTRecord, error) {
	key := fmt.Sprintf(revertExchangeSRTkey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("lockRevertSRT Get", "err", err)
		return nil, err
	}
	exchangeSRT := make([]ExchangeSRTRecord, 0)
	if err := json.Unmarshal(blob, &exchangeSRT); err != nil {
		log.Info("lockRevertSRT Unmarshal", "err", err)
		return nil, err
	}
	return exchangeSRT, nil
}

func (s *StorageData) saveStorageRatiosTodb(ratios map[common.Address]*StorageRatio, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storageRatioskey, number)
	blob, err := json.Marshal(ratios)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) verifyParamsStoragePoc(txDataInfo []string, postion int, chain consensus.ChainHeaderReader, number uint64) (common.Hash, bool) {
	verifyType := ""
	verifyData := txDataInfo[postion]

	if strings.HasPrefix(verifyData, "v1") {
		verifyType = "v1"
		verifyData = verifyData[3:]
	}
	pocs := strings.Split(verifyData, ",")
	if len(pocs) < 3 {
		log.Warn("verifyParamsStoragePoc", "invalid len", len(pocs))
		return common.Hash{}, false
	}
	verifyHeader := chain.GetHeaderByHash(common.HexToHash(pocs[2]))
	if verifyHeader == nil || verifyHeader.Number.String() != pocs[0] || strconv.FormatInt(int64(verifyHeader.Nonce.Uint64()), 10) != pocs[1] {
		log.Warn("verifyParamsStoragePoc  GetHeaderByHash not find by hash  ", "poc", pocs)
		return common.Hash{}, false
	}
	verifyDataArr := strings.Split(verifyData, ",")
	RootHash := verifyDataArr[len(verifyDataArr)-1]
	if isLtPosAutoExitPunishChange(number) {
		if verifyType == "v1" {
			if !verifyStoragePocV1(txDataInfo[postion], RootHash, verifyHeader.Nonce.Uint64()) {
				return common.Hash{}, false
			}
		} else {
			if !verifyStoragePoc(verifyData, RootHash, verifyHeader.Nonce.Uint64()) {
				return common.Hash{}, false
			}
		}
	}
	return common.HexToHash(RootHash), true
}
func (s *Snapshot) updateExchangeSRT(exchangeSRT []ExchangeSRTRecord, headerNumber *big.Int, db ethdb.Database) {
	if s.SRT != nil {
		for _, item := range exchangeSRT {
			s.SRT.Add(item.Target, item.Amount)
		}
	}
}
func (s *Snapshot) updateLeaseRequest(rent []LeaseRequestRecord, number *big.Int, db ethdb.Database) {
	if rent == nil || len(rent) == 0 {
		return
	}
	s.StorageData.updateLeaseRequest(rent, number, db)
}

func (s *Snapshot) updateLeasePledge(pg []LeasePledgeRecord, headerNumber *big.Int, db ethdb.Database) {
	if pg == nil || len(pg) == 0 {
		return
	}
	s.StorageData.updateLeasePledge(pg, headerNumber, db)
	s.burnSRTAmount(pg, headerNumber.Uint64(), db)
}

func (s *Snapshot) updateLeaseRenewal(reNew []LeaseRenewalRecord, number *big.Int, db ethdb.Database) {
	if reNew == nil || len(reNew) == 0 {
		return
	}
	s.StorageData.updateLeaseRenewal(reNew, number, db, s.getBlockPreDay())
}

func (s *Snapshot) updateLeaseRenewalPledge(pg []LeaseRenewalPledgeRecord, headerNumber *big.Int, db ethdb.Database) {
	if pg == nil || len(pg) == 0 {
		return
	}
	s.StorageData.updateLeaseRenewalPledge(pg, headerNumber, db, s.getBlockPreDay())
	s.burnSRTAmountReNew(pg, headerNumber.Uint64(), db)
}

func (s *Snapshot) updateLeaseRescind(rescinds []LeaseRescindRecord, number *big.Int, db ethdb.Database) {
	if rescinds == nil || len(rescinds) == 0 {
		return
	}
	s.StorageData.updateLeaseRescind(rescinds, number, db)
}

func (s *Snapshot) storageVerificationCheck(number uint64, blockPerday uint64, db ethdb.Database, currentLockReward []LockRewardRecord, state *state.StateDB) ([]LockRewardRecord, []ExchangeSRTRecord, *big.Int, error, *big.Int, *big.Int) {
	if isFixLeaseCapacity(number) {
		return s.StorageData.fixLeaseCapacity(currentLockReward, state)
	}
	if isStorageVerificationCheck(number, s.Period) {
		passTime := new(big.Int).Mul(s.SystemConfig.Deposit[sscEnumLeaseExpires], new(big.Int).SetUint64(blockPerday))
		basePrice := s.SystemConfig.Deposit[sscEnumStoragePrice]
		return s.StorageData.storageVerificationCheck(number, blockPerday, passTime, s.SystemConfig.ExchRate, s.RevenueStorage, s.Period, db, basePrice, currentLockReward, s.TotalLeaseSpace, s.SpData, s)
	}
	return currentLockReward, nil, nil, nil, nil, nil
}

func (snap *Snapshot) updateHarvest(harvest *big.Int) {
	if 0 < harvest.Cmp(big.NewInt(0)) {
		if nil == snap.FlowHarvest {
			snap.FlowHarvest = new(big.Int).Set(harvest)
		} else {
			snap.FlowHarvest = new(big.Int).Add(snap.FlowHarvest, harvest)
		}
	}
}

func (s *Snapshot) calStorageVerificationCheck(roothash common.Hash, number uint64, blockPerday uint64, db ethdb.Database, header *types.Header) (*Snapshot, error) {
	if isFixLeaseCapacity(number) {
		s.StorageData.fixLeaseCapacity(nil, nil)
		calRootHash := s.StorageData.Hash
		if calRootHash != roothash {
			return s, errors.New("Storage root hash is not same,head:" + roothash.String() + "cal:" + calRootHash.String())
		}
	}
	if isStorageVerificationCheck(number, s.Period) {
		passTime := new(big.Int).Mul(s.SystemConfig.Deposit[sscEnumLeaseExpires], new(big.Int).SetUint64(blockPerday))
		calRootHash := s.StorageData.calStorageVerificationCheck(number, blockPerday, passTime, s.RevenueStorage, s, db, header)
		if calRootHash != roothash {
			return s, errors.New("Storage root hash is not same,head:" + roothash.String() + "cal:" + calRootHash.String())
		}
	}
	return s, nil
}

func (s *StorageData) calStorageVerificationCheck(number uint64, blockPerday uint64, passTime *big.Int, revenueStorage map[common.Address]*RevenueParameter, snap *Snapshot, db ethdb.Database, header *types.Header) common.Hash {
	s.storageVerify(number, blockPerday, revenueStorage)
	s.calDealLeaseStatus(number, snap, db, header, revenueStorage)
	s.deletePasstimeLease(number, blockPerday, passTime)
	return s.Hash
}

func (s *StorageData) calDealLeaseStatus(number uint64, snap *Snapshot, db ethdb.Database, header *types.Header, revenueStorage map[common.Address]*RevenueParameter) {
	if isGEInitStorageManagerNumber(number) {
		s.calDealLeaseStatus2(number, snap, db, header, revenueStorage)
		return
	}
	delPledge := make([]common.Address, 0)
	removePledge := make([]common.Address, 0)
	for pledgeAddress, sPledge := range s.StoragePledge {
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 {
			removePledge = append(removePledge, pledgeAddress)
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRetrun)) == 0 {
			continue
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 || sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
			sPledge.PledgeStatus = big.NewInt(SPledgeRetrun)
			delPledge = append(delPledge, pledgeAddress)
			s.accumulateSpaceHash(pledgeAddress)
			continue
		}

		leases := sPledge.Lease
		for _, lease := range leases {
			if lease.Status == LeaseReturn {
				continue
			}
			if lease.Status == LeaseUserRescind || lease.Status == LeaseExpiration {
				lease.Status = LeaseReturn
				s.accumulateLeaseHash(pledgeAddress, lease)
			}
		}
	}

	for _, delAddr := range delPledge {
		delete(s.StoragePledge, delAddr)
	}
	s.accumulateHeaderHash()
	if number >= StoragePledgeOptEffectNumber && len(removePledge) > 0 {
		snap.setStorageRemovePunish(removePledge, number, db, header)
	}
	return
}

func (s *StorageData) dealSPledgeRevert3(pledge *SPledge, revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, rate uint32, number uint64, blockPerday uint64, bAmount *big.Int, revenueStorage map[common.Address]*RevenueParameter, pledgeAddress common.Address, snap *Snapshot) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	if isGEInitStorageManagerNumber(number) {
		return s.dealSPledgeRevert4(pledge, revertLockReward, revertExchangeSRT, rate, number, blockPerday, bAmount, revenueStorage, pledgeAddress, snap)
	}
	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	if number > PledgeRevertLockEffectNumber {
		beforeZeroTime = new(big.Int).Add(beforeZeroTime, common.Big1)
	}
	maxFailNum := maxStgVerContinueDayFail * blockPerday
	bigMaxFailNum := new(big.Int).SetUint64(maxFailNum)
	deposit := pledge.SpaceDeposit
	depositAddress := pledge.Address
	revertDeposit := deposit

	if beforeZeroTime.Cmp(bigMaxFailNum) >= 0 {
		beforeSevenDayNumber := new(big.Int).Sub(beforeZeroTime, bigMaxFailNum)
		lastVerSuccTime := pledge.LastVerificationSuccessTime
		if lastVerSuccTime.Cmp(beforeSevenDayNumber) <= 0 {
			revertDeposit = big.NewInt(0)
			if number > PosrIncentiveEffectNumber {
				bAmount = new(big.Int).Add(bAmount, deposit)
			} else if number > StoragePledgeOptEffectNumber {
				bAmount = new(big.Int).Set(deposit)
			}
		}
	}

	zero := big.NewInt(0)
	if revertDeposit.Cmp(zero) > 0 {
		revertLockReward = append(revertLockReward, SpaceRewardRecord{
			Target:  depositAddress,
			Amount:  revertDeposit,
			Revenue: depositAddress,
		})
	}
	return revertLockReward, revertExchangeSRT, bAmount
}

func (a *Alien) changeStorageBandwidth(storageExchangeBwRecord []StorageExchangeBwRecord, storageBwPayRecord []StorageBwPayRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int) ([]StorageExchangeBwRecord, []StorageBwPayRecord) {
	if len(txDataInfo) < 5 {
		log.Warn("exchange   bw  of Storage", "parameter error", len(txDataInfo))
		return storageExchangeBwRecord, storageBwPayRecord
	}
	pledgeAddr := common.HexToAddress(txDataInfo[3])
	if blocknumber.Uint64() < PosrIncentiveEffectNumber {
		if pledgeAddr != txSender {
			if revenue, ok := snap.RevenueStorage[pledgeAddr]; !ok || revenue.RevenueAddress != txSender {
				log.Warn("exchange  bw no role  to change  ", " txSender", txSender)
				return storageExchangeBwRecord, storageBwPayRecord
			}
		}

	}
	storagePg := snap.StorageData.StoragePledge[pledgeAddr]
	if storagePg == nil || storagePg.PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 {
		log.Warn("exchange  bw not find Pledge", " pledgeAddr", pledgeAddr)
		return storageExchangeBwRecord, storageBwPayRecord
	}
	if blocknumber.Uint64() >= PosrIncentiveEffectNumber {
		//if _,ok:=snap.STGBandwidthMakeup[pledgeAddr];!ok ||snap.STGBandwidthMakeup[pledgeAddr].AdjustCount>0{
		//	log.Warn("exchange   bw only set once", " txSender", txSender,"pledgeAddr",pledgeAddr)
		//	return storageExchangeBwRecord,storageBwPayRecord
		//}
		if storagePg.Address != txSender {
			log.Warn("exchange  bw no role  to change  ", " pledgeAddr", pledgeAddr, "Address", storagePg.Address)
			return storageExchangeBwRecord, storageBwPayRecord
		}
		//if storagePg.Bandwidth.Cmp(bandwidthAdjustThreshold)<=0 {
		//	log.Warn("exchange  bw no role  ,must >","bandwidthAdjustThreshold",bandwidthAdjustThreshold, " pledgeAddr", pledgeAddr,"old Bandwidth",storagePg.Bandwidth)
		//	return storageExchangeBwRecord,storageBwPayRecord
		//}
	}
	bandwidth, err := decimal.NewFromString(txDataInfo[4])
	if err != nil {
		log.Warn("  bw format error", " bandwidth", txDataInfo[4])
		return storageExchangeBwRecord, storageBwPayRecord
	}

	if bandwidth.Cmp(decimal.Zero) < 0 {
		log.Warn("exchange  bandwidth < 0", " pledgeAddr", pledgeAddr, "bandwidth", bandwidth)
		return storageExchangeBwRecord, storageBwPayRecord
	}
	totalPledgeAmount := big.NewInt(0)
	if blocknumber.Uint64() >= PosrIncentiveEffectNumber {
		if bandwidth.Cmp(decimal.NewFromInt(20)) < 0 {
			log.Warn("exchange  bandwidth < 20", " pledgeAddr", pledgeAddr, "bandwidth", bandwidth)
			return storageExchangeBwRecord, storageBwPayRecord
		}
		totalStorage := big.NewInt(0)
		for _, spledge := range snap.StorageData.StoragePledge {
			totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
		}
		totalPledgeAmount = getSotragePledgeAmount(decimal.NewFromBigInt(storagePg.TotalCapacity, 0), bandwidth, decimal.NewFromBigInt(totalStorage, 0), blocknumber, snap)
		payPledgeAmount := new(big.Int).Sub(totalPledgeAmount, storagePg.SpaceDeposit)
		if payPledgeAmount.Cmp(big.NewInt(0)) > 0 {
			if state.GetBalance(txSender).Cmp(payPledgeAmount) < 0 {
				log.Warn("exchange  bandwidth  Insufficient funds", " pledgeAddr", pledgeAddr, "payPledgeAmount", payPledgeAmount, "txSender", txSender, "Balance", state.GetBalance(txSender))
				return storageExchangeBwRecord, storageBwPayRecord
			}
			state.SubBalance(txSender, payPledgeAmount)
			storageBwPayRecord = append(storageBwPayRecord, StorageBwPayRecord{
				Address: pledgeAddr,
				Amount:  totalPledgeAmount,
			})
		} else {
			totalPledgeAmount = new(big.Int).Set(storagePg.SpaceDeposit)
		}

	}
	storageExchangeBwRecord = append(storageExchangeBwRecord, StorageExchangeBwRecord{
		Address:   pledgeAddr,
		Bandwidth: bandwidth.BigInt(),
	})
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0xb12bf5b909b60bb08c3e990dcb437a238072a91629c666541b667da82b3ee422"))
	topics[1].SetBytes(pledgeAddr.Bytes())
	topics[2].SetBytes([]byte(txDataInfo[4]))
	if blocknumber.Uint64() < PosrIncentiveEffectNumber {
		a.addCustomerTxLog(tx, receipts, topics, nil)
	} else {
		reData := totalPledgeAmount.Bytes()
		a.addCustomerTxLog(tx, receipts, topics, reData)
	}
	return storageExchangeBwRecord, storageBwPayRecord

}

func (s *Snapshot) updateStorageBandWidth(storageExchangeBwRecord []StorageExchangeBwRecord, headerNumber *big.Int, db ethdb.Database) {
	if storageExchangeBwRecord == nil || len(storageExchangeBwRecord) == 0 {
		return
	}
	for _, exchangeBw := range storageExchangeBwRecord {
		if pledgeItem, ok := s.StorageData.StoragePledge[exchangeBw.Address]; ok {
			pledgeItem.Bandwidth = new(big.Int).Set(exchangeBw.Bandwidth)
			delete(s.STGBandwidthMakeup, exchangeBw.Address)
			s.StorageData.accumulatePledgeHash(exchangeBw.Address)
		}
	}
	s.StorageData.accumulateHeaderHash()
}
func (a *Alien) checkPledgeMaxStorageSpace(currStoragePledge []SPledgeRecord, targetDev common.Address, snap *Snapshot, number *big.Int, totalCapacity *big.Int) error {
	if number.Uint64() > PledgeRevertLockEffectNumber {
		targetRevenueAddress := common.Address{}
		findRevenue := false
		for device, revenue := range snap.RevenueStorage {
			if targetDev == device {
				targetRevenueAddress = revenue.RevenueAddress
				findRevenue = true
				break
			}
		}
		if findRevenue {
			alreadybind := make(map[common.Address]uint64)
			devToRevenue := make(map[common.Address]common.Address)
			for device, revenue := range snap.RevenueStorage {
				revenueAddress := revenue.RevenueAddress
				if targetRevenueAddress == revenueAddress {
					alreadybind[device] = 1
				}
				devToRevenue[device] = revenueAddress
			}
			for _, item := range currStoragePledge {
				if revenueAddress, ok := devToRevenue[item.Address]; ok {
					if targetRevenueAddress == revenueAddress {
						totalCapacity = new(big.Int).Add(totalCapacity, item.StorageCapacity)
					}
				}
			}
			return a.checkMaxStorageSpaceByAddr(alreadybind, snap, totalCapacity)
		}
	}
	return nil
}

func (a *Alien) checkMaxStorageSpaceByAddr(alreadybind map[common.Address]uint64, snap *Snapshot, totalCapacity *big.Int) error {
	for pledgeAddr, sPledge := range snap.StorageData.StoragePledge {
		if _, ok := alreadybind[pledgeAddr]; ok {
			totalCapacity = new(big.Int).Add(totalCapacity, sPledge.TotalCapacity)
		}
	}
	if totalCapacity.Cmp(maxBoundStorageSpace) > 0 {
		return errors.New("revenueAddress totalCapacity greater than 1EB")
	}
	return nil
}

func (s *StorageData) saveCapSuccAddrsTodb(addrs map[common.Address]*big.Int, db ethdb.Database, number uint64) error {
	key := fmt.Sprintf(storageCapSuccAddrsKey, number)
	blob, err := json.Marshal(addrs)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageData) loadCapSuccAddrs(db ethdb.Database, number uint64) (map[common.Address]*big.Int, error) {
	key := fmt.Sprintf(storageCapSuccAddrsKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadCapSuccAddrs Get", "err", err)
		return nil, err
	}
	addrs := make(map[common.Address]*big.Int)
	if err := json.Unmarshal(blob, &addrs); err != nil {
		log.Info("loadCapSuccAddrs Unmarshal", "err", err)
		return nil, err
	}
	return addrs, nil
}

func (s *Snapshot) calSRTHashVer(roothash common.Hash, number uint64, db ethdb.Database) (*Snapshot, error) {
	if number >= PledgeRevertLockEffectNumber {
		if s.SRT.Root() != roothash {
			return s, errors.New("SRT root hash is not same,head:" + roothash.String() + "cal:" + s.SRT.Root().String())
		}
	}
	return s, nil
}

func (s *StorageData) saveTotalValueTodb(totalValue *big.Int, db ethdb.Database, number uint64, keyStr string) interface{} {
	key := fmt.Sprintf(keyStr, number)
	blob, err := json.Marshal(totalValue)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	log.Info("saveTotalValueTodb", "key", key, "totalValue", totalValue)
	return nil
}

func (s *StorageData) saveDecimalValueTodb(totalValue decimal.Decimal, db ethdb.Database, number uint64, keyStr string) interface{} {
	key := fmt.Sprintf(keyStr, number)
	blob, err := json.Marshal(totalValue)
	if err != nil {
		return err
	}
	err = db.Put([]byte(key), blob)
	if err != nil {
		return err
	}
	log.Info("saveDecimalValueTodb", "key", key, "totalValue", totalValue)
	return nil
}

func (s *StorageData) loadSpledgeValue(db ethdb.Database, number uint64, rewardKey string) (*big.Int, error) {
	key := fmt.Sprintf(rewardKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadSpledgeValue Get", "err", err)
		return nil, err
	}
	value := common.Big0
	if err := json.Unmarshal(blob, &value); err != nil {
		log.Info("loadSpledgeValue Unmarshal", "err", err)
		return nil, err
	}
	return value, nil
}

func (s *StorageData) loadSpledgeDecimalValue(db ethdb.Database, number uint64, rewardKey string) (decimal.Decimal, error) {
	key := fmt.Sprintf(rewardKey, number)
	blob, err := db.Get([]byte(key))
	if err != nil {
		log.Info("loadSpledgeDecimalValue Get", "err", err)
		return decimal.Zero, err
	}
	value := decimal.Zero
	if err := json.Unmarshal(blob, &value); err != nil {
		log.Info("loadSpledgeDecimalValue Unmarshal", "err", err)
		return decimal.Zero, err
	}
	return value, nil
}

func getAddPB(reward *big.Int, number uint64) *big.Int {
	if number >= StoragePledgeOptEffectNumber {
		return getAddPB2(reward, number)
	}
	pt300 := new(big.Int).Mul(big.NewInt(300), pb1b)
	if reward.Cmp(pt300) <= 0 {
		return new(big.Int).Mul(big.NewInt(30), pb1b)
	}
	pt600 := new(big.Int).Mul(big.NewInt(600), pb1b)
	if reward.Cmp(pt300) > 0 && reward.Cmp(pt600) <= 0 {
		return new(big.Int).Mul(big.NewInt(60), pb1b)
	}
	if reward.Cmp(pt600) > 0 && reward.Cmp(eb1b) <= 0 {
		return new(big.Int).Mul(big.NewInt(100), pb1b)
	}
	return common.Big0
}

func getAddPB2(reward *big.Int, number uint64) *big.Int {
	if isGTPOSRNewCalEffect(number) {
		return getAddPB3(reward, number)
	}
	pt200 := new(big.Int).Mul(big.NewInt(200), pb1b)
	pt300 := new(big.Int).Mul(big.NewInt(300), pb1b)
	addReward := common.Big0
	if reward.Cmp(pt200) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(225))
	}
	if reward.Cmp(pt200) >= 0 && reward.Cmp(pt300) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(200))
	}
	pt400 := new(big.Int).Mul(big.NewInt(400), pb1b)
	if reward.Cmp(pt300) >= 0 && reward.Cmp(pt400) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(175))
	}
	pt500 := new(big.Int).Mul(big.NewInt(500), pb1b)
	if reward.Cmp(pt400) >= 0 && reward.Cmp(pt500) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(150))
	}
	pt600 := new(big.Int).Mul(big.NewInt(600), pb1b)
	if reward.Cmp(pt500) >= 0 && reward.Cmp(pt600) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(125))
	}
	pt700 := new(big.Int).Mul(big.NewInt(700), pb1b)
	if reward.Cmp(pt600) >= 0 && reward.Cmp(pt700) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(100))
	}
	pt800 := new(big.Int).Mul(big.NewInt(800), pb1b)
	if reward.Cmp(pt700) >= 0 && reward.Cmp(pt800) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(75))
	}
	pt900 := new(big.Int).Mul(big.NewInt(900), pb1b)
	if reward.Cmp(pt800) >= 0 && reward.Cmp(pt900) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(50))
	}
	pt1000 := new(big.Int).Mul(big.NewInt(1000), pb1b)
	if reward.Cmp(pt900) >= 0 && reward.Cmp(pt1000) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(25))
	}
	addReward = new(big.Int).Div(addReward, big.NewInt(1000))
	return addReward
}
func getAddPB3(reward *big.Int, number uint64) *big.Int {
	pt200 := new(big.Int).Mul(big.NewInt(200), pb1b)
	pt300 := new(big.Int).Mul(big.NewInt(300), pb1b)
	addReward := common.Big0
	if reward.Cmp(pt200) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(425))
	}
	if reward.Cmp(pt200) >= 0 && reward.Cmp(pt300) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(400))
	}
	pt400 := new(big.Int).Mul(big.NewInt(400), pb1b)
	if reward.Cmp(pt300) >= 0 && reward.Cmp(pt400) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(375))
	}
	pt500 := new(big.Int).Mul(big.NewInt(500), pb1b)
	if reward.Cmp(pt400) >= 0 && reward.Cmp(pt500) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(350))
	}
	pt600 := new(big.Int).Mul(big.NewInt(600), pb1b)
	if reward.Cmp(pt500) >= 0 && reward.Cmp(pt600) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(325))
	}
	pt700 := new(big.Int).Mul(big.NewInt(700), pb1b)
	if reward.Cmp(pt600) >= 0 && reward.Cmp(pt700) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(300))
	}
	pt800 := new(big.Int).Mul(big.NewInt(800), pb1b)
	if reward.Cmp(pt700) >= 0 && reward.Cmp(pt800) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(275))
	}
	pt900 := new(big.Int).Mul(big.NewInt(900), pb1b)
	if reward.Cmp(pt800) >= 0 && reward.Cmp(pt900) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(250))
	}
	pt1000 := new(big.Int).Mul(big.NewInt(1000), pb1b)
	if reward.Cmp(pt900) >= 0 && reward.Cmp(pt1000) < 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(225))
	}
	if reward.Cmp(pt1000) >= 0 {
		addReward = new(big.Int).Mul(reward, big.NewInt(200))
	}
	addReward = new(big.Int).Div(addReward, big.NewInt(1000))
	return addReward
}
func getBandwidthPledgeRatio(bandwidth *big.Int) decimal.Decimal {
	if bandwidth.Cmp(big.NewInt(1000)) > 0 {
		return decimal.NewFromFloat(4.9829)
	}
	logindex := 0.6532125137753437
	if bandwidth.Cmp(big.NewInt(100)) >= 0 {
		logindex = 0.6020599913279624
	}
	bw, _ := decimal.NewFromBigInt(bandwidth, 0).Float64()
	bwRatio := decimal.NewFromFloat(math.Log10(bw) / logindex)
	return bwRatio.Round(4)
}
func getBandwidthPledgeNewRatio(bandwidth *big.Int) decimal.Decimal {
	if bandwidth.Cmp(big.NewInt(1024)) >= 0 {
		return decimal.NewFromFloat(4.06598)
	}
	logindex := 0.7403626894942439
	bw, _ := decimal.NewFromBigInt(bandwidth, 0).Float64()
	bwRatio := decimal.NewFromFloat(math.Log10(bw) / logindex)
	return bwRatio.Round(5)
}
func getSotragePledgeNewAmount(declareCapacity decimal.Decimal, bandwidth decimal.Decimal, total decimal.Decimal, blockNumPer *big.Int, snap *Snapshot) *big.Int {
	plbwRatio := getBandwidthPledgeNewRatio(bandwidth.BigInt())
	blockNumPerYear := secondsPerYear / snap.config.Period
	defaultTbAmount, _ := decimal.NewFromString("1250000000000000000") //1.25 UTG
	tbPledgeNum := defaultTbAmount
	if blockNumPer.Uint64() > StorageEffectBlockNumber+blockNumPerYear {
		totalSpace := total.Div(decimal.NewFromBigInt(tb1b, 0)) // B-> TB
		if totalSpace.Cmp(decimal.NewFromInt(0)) > 0 {
			calTbPledgeNum := decimal.NewFromBigInt(snap.FlowHarvest, 0).Div(totalSpace)
			if calTbPledgeNum.Cmp(defaultTbAmount) < 0 && calTbPledgeNum.Cmp(decimal.NewFromInt(0)) > 0 {
				tbPledgeNum = calTbPledgeNum
			}
		}
	}
	return (declareCapacity.Div(decimal.NewFromBigInt(tb1b, 0))).Mul(tbPledgeNum).Mul(plbwRatio).Mul(storagePledgefactor).BigInt()
}

func getSotragePledgeAmount(declareCapacity decimal.Decimal, bandwidth decimal.Decimal, total decimal.Decimal, blockNumPer *big.Int, snap *Snapshot) *big.Int {
	if blockNumPer.Uint64() >= PosrIncentiveEffectNumber {
		return getSotragePledgeNewAmount(declareCapacity, bandwidth, total, blockNumPer, snap)
	}
	plbwRatio := getBandwidthPledgeRatio(bandwidth.BigInt())
	factor := decimal.NewFromFloat(0.5)
	if bandwidth.Cmp(decimal.NewFromInt(100)) >= 0 {
		factor = decimal.NewFromFloat(0.6)
	}
	blockNumPerYear := secondsPerYear / snap.config.Period
	defaultTbAmount, _ := decimal.NewFromString("1250000000000000000") //1.25 UTG
	tbPledgeNum := defaultTbAmount
	if blockNumPer.Uint64() > StorageEffectBlockNumber+blockNumPerYear {
		totalSpace := total.Div(decimal.NewFromBigInt(tb1b, 0)) // B-> TB
		if totalSpace.Cmp(decimal.NewFromInt(0)) > 0 {
			calTbPledgeNum := decimal.NewFromBigInt(snap.FlowHarvest, 0).Div(totalSpace)
			if calTbPledgeNum.Cmp(defaultTbAmount) < 0 {
				tbPledgeNum = calTbPledgeNum
			}
		}
	}
	return (declareCapacity.Div(decimal.NewFromBigInt(tb1b, 0))).Mul(tbPledgeNum).Mul(plbwRatio).Mul(factor).BigInt()
}

func getBandwidthRewardRatio(bandwidth *big.Int) decimal.Decimal {
	if bandwidth.Cmp(big.NewInt(1000)) > 0 {
		return decimal.NewFromFloat(1.5610)
	}
	if bandwidth.Cmp(big.NewInt(20)) < 0 {
		return decimal.Zero
	}
	bwroleval := decimal.NewFromFloat(2.5)
	correctVal := decimal.NewFromFloat(0.3)
	if bandwidth.Cmp(big.NewInt(100)) >= 0 {
		bwroleval = decimal.NewFromInt(3)
		correctVal = decimal.NewFromFloat(0.1)
	}
	plbwRatio := getBandwidthPledgeRatio(bandwidth)
	rewardRatio := plbwRatio.Div(bwroleval)
	rewardRatio = rewardRatio.Sub(correctVal)

	return rewardRatio.Round(5)
}
func getBandwidthRewardNewRatio(bandwidth *big.Int) decimal.Decimal {
	if bandwidth.Cmp(big.NewInt(1024)) >= 0 {
		return decimal.NewFromFloat(1.32639)
	}
	if bandwidth.Cmp(big.NewInt(20)) < 0 {
		return decimal.Zero
	}
	bwroleval := decimal.NewFromFloat(2.5)
	correctVal := decimal.NewFromFloat(0.3)

	plbwRatio := getBandwidthPledgeNewRatio(bandwidth)
	rewardRatio := plbwRatio.Div(bwroleval)
	rewardRatio = rewardRatio.Sub(correctVal)

	return rewardRatio.Round(5)
}
func (a *Alien) payStorageBWPledge(storageBwPayRecord []StorageBwPayRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int) []StorageBwPayRecord {
	if len(txDataInfo) < 4 {
		log.Warn("payStorageBWPledge", "parameter error need 4 act", len(txDataInfo))
		return storageBwPayRecord
	}
	storageAddress := common.HexToAddress(txDataInfo[3])
	storageNode := snap.StorageData.StoragePledge[storageAddress]
	if storageNode == nil {
		log.Warn("payStorageBWPledge", "storage not exit storageAddress", storageAddress)
		return storageBwPayRecord
	}
	if storageNode.Address != txSender {
		log.Warn("payStorageBWPledge", "pledge address no role", storageAddress)
		return storageBwPayRecord
	}
	totalStorage := big.NewInt(0)
	for _, spledge := range snap.StorageData.StoragePledge {
		totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
	}
	payAmount := big.NewInt(0)
	needPledgeAmount := big.NewInt(0)
	if bwrecord, ok := snap.STGBandwidthMakeup[storageAddress]; ok {
		needPledgeAmount = getSotragePledgeAmount(decimal.NewFromBigInt(storageNode.TotalCapacity, 0), decimal.NewFromBigInt(bwrecord.OldBandwidth, 0), decimal.NewFromBigInt(totalStorage, 0), blocknumber, snap)
		payAmount = new(big.Int).Sub(needPledgeAmount, storageNode.SpaceDeposit)
	}
	if payAmount.Cmp(big.NewInt(0)) <= 0 {
		log.Warn("payStorageBWPledge", "not need pay pledgeAmount", needPledgeAmount, "act pledgeAmount ", storageNode.SpaceDeposit)
		return storageBwPayRecord
	}
	sendBalance := state.GetBalance(txSender)
	if sendBalance.Cmp(payAmount) <= 0 {
		log.Warn("payStorageBWPledge", "balance not enough", txSender, "sendBalance ", sendBalance, "payAmount", payAmount)
		return storageBwPayRecord
	}
	state.SetBalance(txSender, new(big.Int).Sub(sendBalance, payAmount))
	topics := make([]common.Hash, 3)
	topics[0].UnmarshalText([]byte("0x79f9d3ae89c89c61e3d4eb211fbd2766ee1c78b064b0d8853997b3d19c290af5"))
	topics[1].SetBytes(txSender.Bytes())
	topics[2].SetBytes(payAmount.Bytes())
	data := needPledgeAmount.Bytes()
	a.addCustomerTxLog(tx, receipts, topics, data)
	if blocknumber.Uint64() < PosrIncentiveEffectNumber {
		storageBwPayRecord = append(storageBwPayRecord, StorageBwPayRecord{
			Address: storageAddress,
			Amount:  payAmount,
		})
	} else {
		storageBwPayRecord = append(storageBwPayRecord, StorageBwPayRecord{
			Address: storageAddress,
			Amount:  needPledgeAmount,
		})
	}

	return storageBwPayRecord
}
func (s *Snapshot) updateBwPledgePayData(storageBwPayRecord []StorageBwPayRecord, headerNumber *big.Int, db ethdb.Database) {
	if storageBwPayRecord == nil || len(storageBwPayRecord) == 0 {
		return
	}
	for _, bwPayRecord := range storageBwPayRecord {
		if storagePledge, ok := s.StorageData.StoragePledge[bwPayRecord.Address]; ok {
			if bwRecord, ok := s.STGBandwidthMakeup[bwPayRecord.Address]; ok {
				if headerNumber.Uint64() < PosrIncentiveEffectNumber {
					storagePledge.SpaceDeposit = bwRecord.DepositMakeup
				} else {
					storagePledge.SpaceDeposit = bwPayRecord.Amount
					s.StorageData.accumulatePledgeHash(bwPayRecord.Address)
				}
				delete(s.STGBandwidthMakeup, bwPayRecord.Address)
			} else if headerNumber.Uint64() >= PosrIncentiveEffectNumber {
				storagePledge.SpaceDeposit = bwPayRecord.Amount
				s.StorageData.accumulatePledgeHash(bwPayRecord.Address)
			}
		}
	}
	if headerNumber.Uint64() >= PosrIncentiveEffectNumber {
		s.StorageData.accumulateHeaderHash()
	}

}

func (s *Snapshot) initBandwidthMakeup(blocknumber *big.Int) {
	s.STGBandwidthMakeup = make(map[common.Address]*BandwidthMakeup)
	totalStorage := big.NewInt(0)
	for _, spledge := range s.StorageData.StoragePledge {
		totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
	}
	for pledgeAddr, sPledge := range s.StorageData.StoragePledge {
		burnRatio := new(big.Int).Sub(sPledge.Bandwidth, bigInt20)
		burnRatio = new(big.Int).Mul(burnRatio, BurnBase)
		burnRatio = new(big.Int).Div(burnRatio, sPledge.Bandwidth)
		s.STGBandwidthMakeup[pledgeAddr] = &BandwidthMakeup{
			OldBandwidth:  new(big.Int).Set(sPledge.Bandwidth),
			BurnRatio:     burnRatio,
			DepositMakeup: getSotragePledgeAmount(decimal.NewFromBigInt(sPledge.TotalCapacity, 0), decimal.NewFromBigInt(sPledge.Bandwidth, 0), decimal.NewFromBigInt(totalStorage, 0), blocknumber, s),
		}

	}
}

func (s *Snapshot) setBandwidthMakeupPunish(header *types.Header, db ethdb.Database) {
	pledgeBw := s.setBandwidth20MakeupPunish()

	err := s.FlowRevenue.BandwidthLock.setBandwidthMakeupPunish(s.STGBandwidthMakeup, s.StorageData, db, header.Hash(), header.Number.Uint64(), pledgeBw)
	if err != nil {
		log.Warn("setBandwidthMakeupPunish BandwidthLock Error", "err", err)
	}
	err = s.FlowRevenue.FlowLock.setBandwidthMakeupPunish(s.STGBandwidthMakeup, s.StorageData, db, header.Hash(), header.Number.Uint64(), pledgeBw)
	if err != nil {
		log.Warn("setBandwidthMakeupPunish FlowLock Error", "err", err)
	}
	s.STGBandwidthMakeup = make(map[common.Address]*BandwidthMakeup)
}

func (s *Snapshot) setBandwidth20MakeupPunish() map[common.Address]*big.Int {
	pledgeBw := make(map[common.Address]*big.Int)
	for pledgeAddr, sPledge := range s.StorageData.StoragePledge {
		if bMakeup, ok := s.STGBandwidthMakeup[pledgeAddr]; ok {
			if isGTBandwidthPunishLine(bMakeup) {
				if sPledge.Bandwidth.Cmp(bMakeup.OldBandwidth) == 0 {
					sPledge.Bandwidth = new(big.Int).Set(bigInt20)
					s.StorageData.accumulateSpaceHash(pledgeAddr)
					pledgeBw[pledgeAddr] = new(big.Int).Set(bigInt20)
				} else {
					pledgeBw[pledgeAddr] = new(big.Int).Set(sPledge.Bandwidth)
				}
			}
		}
	}
	s.StorageData.accumulateHeaderHash()
	return pledgeBw
}

func (s *Snapshot) setStorageRemovePunish(pledge []common.Address, number uint64, db ethdb.Database, header *types.Header) {
	err := s.FlowRevenue.BandwidthLock.setStorageRemovePunish(pledge, db, header.Hash(), header.Number.Uint64())
	if err != nil {
		log.Warn("setStorageRemovePunish BandwidthLock Error", "err", err)
	}
	err = s.FlowRevenue.FlowLock.setStorageRemovePunish(pledge, db, header.Hash(), header.Number.Uint64())
	if err != nil {
		log.Warn("setStorageRemovePunish FlowLock Error", "err", err)
	}
}

func (s *StorageData) fixLeaseCapacity(currentLockReward []LockRewardRecord, state *state.StateDB) ([]LockRewardRecord, []ExchangeSRTRecord, *big.Int, error, *big.Int, *big.Int) {
	revertExchangeSRT := make([]ExchangeSRTRecord, 0)
	for pledgeAddress, sPledge := range s.StoragePledge {
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRetrun)) == 0 {
			continue
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 || sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
			continue
		}
		leases := sPledge.Lease
		for _, lease := range leases {
			if lease.Status == LeaseReturn {
				continue
			}
			if lease.Status == LeaseNormal || lease.Status == LeaseBreach {
				leaseCapacity := new(big.Int).Set(lease.Capacity)
				lCapMod := new(big.Int).Mod(leaseCapacity, big.NewInt(5))
				if lCapMod.Cmp(common.Big0) != 0 {
					if state != nil {
						state.AddBalance(lease.DepositAddress, lease.Deposit)
					}
					revertExchangeSRT = append(revertExchangeSRT, ExchangeSRTRecord{
						Target: lease.Address,
						Amount: lease.Cost,
					})
					lease.Status = LeaseReturn
					s.accumulateLeaseHash(pledgeAddress, lease)
				}
			}
		}
	}
	s.accumulateHeaderHash()
	return currentLockReward, revertExchangeSRT, nil, nil, nil, nil
}

func isIncentivePeriod(pledge *SPledge, number uint64, period uint64) bool {
	numberBig := new(big.Int).SetUint64(number)
	pledgePeriod := new(big.Int).Sub(numberBig, pledge.Number)
	dayBlock := secondsPerDay / period
	numberBig = getZeroTime(numberBig, dayBlock)
	if pledgePeriod.Cmp(new(big.Int).Mul(IncentivePeriod, big.NewInt(int64(dayBlock)))) <= 0 { //<=30 days
		return true
	}
	return false
}

func (s *StorageData) dealLeaseRevertRescind(l *Lease, revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, rate uint32, number uint64, lHash common.Hash, blockPerday uint64, bAmount *big.Int) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	zero := big.NewInt(0)
	revertDay := l.ValidationFailureTotalTime
	deposit := l.Deposit
	duration := l.Duration
	address := l.Address
	revertSRTAmount := big.NewInt(0)
	blockPerDayBig := new(big.Int).SetUint64(blockPerday)
	startTime := l.LeaseList[lHash].StartTime
	durationBlockNumber := new(big.Int).Mul(duration, blockPerDayBig)
	beforeZeroTime := s.getBeforeZeroTime(new(big.Int).SetUint64(number), blockPerday)
	endNumber := new(big.Int).Add(startTime, durationBlockNumber)
	if beforeZeroTime.Cmp(endNumber) < 0 {
		leftDay := new(big.Int).Sub(endNumber, beforeZeroTime)
		leftDay = new(big.Int).Div(leftDay, blockPerDayBig)
		revertDay = new(big.Int).Add(revertDay, leftDay)
	}
	revertDeposit := big.NewInt(0)
	if revertDay.Cmp(zero) > 0 {
		if duration.Cmp(revertDay) > 0 {
			revertAmount := new(big.Int).Mul(deposit, revertDay)
			revertAmount = new(big.Int).Div(revertAmount, duration)
			revertDeposit = new(big.Int).Sub(deposit, revertAmount)
			revertAmount = new(big.Int).Div(new(big.Int).Mul(revertAmount, big.NewInt(int64(rate))), big.NewInt(10000))
			revertSRTAmount = new(big.Int).Add(revertSRTAmount, revertAmount)
		} else {
			revertAmount := new(big.Int).Div(new(big.Int).Mul(deposit, big.NewInt(int64(rate))), big.NewInt(10000))
			revertSRTAmount = new(big.Int).Add(revertSRTAmount, revertAmount)
		}
	} else {
		revertDeposit = deposit
	}
	if revertSRTAmount.Cmp(zero) > 0 {
		revertExchangeSRT = append(revertExchangeSRT, ExchangeSRTRecord{
			Target: address,
			Amount: revertSRTAmount,
		})
	}
	if revertDeposit.Cmp(zero) > 0 {
		bAmount = new(big.Int).Add(bAmount, revertDeposit)
	}
	return revertLockReward, revertExchangeSRT, bAmount
}

func (s *StorageData) getBeforeZeroTime(bigNumber *big.Int, blockPerday uint64) *big.Int {
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	beforeZeroTime = new(big.Int).Add(beforeZeroTime, common.Big1)
	return beforeZeroTime
}

func isGTBandwidthPunishLine(bMakeup *BandwidthMakeup) bool {
	bandwidth := new(big.Int).Set(bMakeup.OldBandwidth)
	if bandwidth.Cmp(bandwidthPunishLine) > 0 {
		return true
	}
	return false
}
func (s *Snapshot) adjustStorageOldPrice() {
	oldPrice := new(big.Int).Mul(big.NewInt(1e+14), big.NewInt(5))
	basePrice := s.SystemConfig.Deposit[sscEnumStoragePrice]
	for pledgeAddr, item := range s.StorageData.StoragePledge {
		if item.Price.Cmp(oldPrice) == 0 {
			item.Price = new(big.Int).Set(basePrice)
			s.StorageData.accumulatePledgeHash(pledgeAddr)
		}
	}
	s.StorageData.accumulateHeaderHash()
}

func (s *Snapshot) initBandwidthMakeup2(blocknumber *big.Int) {
	s.STGBandwidthMakeup = make(map[common.Address]*BandwidthMakeup)
	totalStorage := big.NewInt(0)
	for _, spledge := range s.StorageData.StoragePledge {
		totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
	}
	for pledgeAddr, sPledge := range s.StorageData.StoragePledge {
		depositMakeup := getSotragePledgeAmount(decimal.NewFromBigInt(sPledge.TotalCapacity, 0), decimal.NewFromBigInt(sPledge.Bandwidth, 0), decimal.NewFromBigInt(totalStorage, 0), blocknumber, s)
		burnRatio := new(big.Int).Sub(sPledge.Bandwidth, bigInt20)
		burnRatio = new(big.Int).Mul(burnRatio, BurnBase)
		burnRatio = new(big.Int).Div(burnRatio, sPledge.Bandwidth)
		s.STGBandwidthMakeup[pledgeAddr] = &BandwidthMakeup{
			OldBandwidth:  new(big.Int).Set(sPledge.Bandwidth),
			BurnRatio:     burnRatio,
			DepositMakeup: depositMakeup,
			AdjustCount:   0,
		}
	}
}

func getZeroTime(bigNumber *big.Int, blockPerDay uint64) *big.Int {
	bigBlockPerDay := new(big.Int).SetUint64(blockPerDay)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigBlockPerDay), bigBlockPerDay)
	return zeroTime
}

func changeOxToUx(str string) string {
	return "ux" + str[2:]
}

func (s *StorageData) calcStoragePledgeReward3(ratios map[common.Address]*StorageRatio, revenueStorage map[common.Address]*RevenueParameter, number uint64, period uint64, sussSPAddrs []common.Address, capSuccAddrs map[common.Address]*big.Int, db ethdb.Database, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int) {
	if isGEInitStorageManagerNumber(number) {
		return s.calcStoragePledgeReward4(ratios, revenueStorage, number, period, sussSPAddrs, capSuccAddrs, db, snap)
	}
	reward := make([]SpaceRewardRecord, 0)
	storageHarvest := big.NewInt(0)
	leftAmount := common.Big0
	validSuccSPAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range sussSPAddrs {
		validSuccSPAddrs[sPAddrs] = 1
	}
	for pledgeAddr, sPledge := range s.StoragePledge {
		if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
			continue
		}
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			if capSucc, ok3 := capSuccAddrs[pledgeAddr]; ok3 {
				if capSucc.Cmp(common.Big0) > 0 {
					if s.isSPledgeIncentivePeriod(sPledge.Number, number, period) {
						if s.isSPledgeFrontIncentivePeriod(sPledge.Number, number, period) || s.isSPledgeRentalThreshold(sPledge) {
							apr := getApr(sPledge.Number, period)
							pledgeReward := decimal.NewFromBigInt(sPledge.SpaceDeposit, 0).Mul(apr).Div(decimal.NewFromInt(365))
							pledgeRewardBigInt := pledgeReward.BigInt()
							if pledgeRewardBigInt.Cmp(common.Big0) > 0 {
								reward = append(reward, SpaceRewardRecord{
									Target:  pledgeAddr,
									Amount:  pledgeRewardBigInt,
									Revenue: revenue.RevenueAddress,
								})
								storageHarvest = new(big.Int).Add(storageHarvest, pledgeRewardBigInt)
							}
						}
					}
				}
			}
		}

	}
	return reward, storageHarvest, leftAmount
}

func (s *StorageData) isSPledgeIncentivePeriod(sPledgeNumber *big.Int, number uint64, period uint64) bool {
	blockNumPerYear := secondsPerYear / period
	return number-sPledgeNumber.Uint64() <= blockNumPerYear
}

func (s *StorageData) isSPledgeFrontIncentivePeriod(sPledgeNumber *big.Int, number uint64, period uint64) bool {
	blockNumPerDay := secondsPerDay / period
	return number-sPledgeNumber.Uint64() <= 30*blockNumPerDay
}

func getApr(sPledgeNumber *big.Int, period uint64) decimal.Decimal {
	blockNumPerYear := secondsPerYear / period
	yearCount := (sPledgeNumber.Uint64() - StorageEffectBlockNumber) / blockNumPerYear
	yearCount++
	apr := decimal.NewFromFloat(float64(0.15))
	if yearCount > 1 {
		decimalN := decimal.NewFromBigInt(new(big.Int).SetUint64(yearCount-1), 0)
		yearScale, _ := decimal.NewFromString("0.85") //1-0.15
		apr = apr.Mul(yearScale.Pow(decimalN))
	}
	return apr.Truncate(6)
}

func (s *StorageData) isSPledgeRentalThreshold(sPledge *SPledge) bool {
	rentSpace := common.Big0
	for _, l := range sPledge.Lease {
		if l.Status == LeaseNormal || l.Status == LeaseBreach {
			rentSpace = new(big.Int).Add(rentSpace, l.Capacity)
		}
	}
	totalCapacity := new(big.Int).Set(sPledge.TotalCapacity)
	thresholdSpace := new(big.Int).Mul(totalCapacity, big.NewInt(50))
	thresholdSpace = new(big.Int).Div(thresholdSpace, big.NewInt(100))
	return rentSpace.Cmp(common.Big0) > 0 && rentSpace.Cmp(thresholdSpace) >= 0
}

func (s *StorageData) accumulateLeaseRewards2(ratios map[common.Address]*StorageRatio, addrs []common.Hash, basePrice *big.Int, revenueStorage map[common.Address]*RevenueParameter, blocknumber uint64, db ethdb.Database, snapTotalLeaseSpace *big.Int, spData *SpData, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int, *big.Int) {
	if isGEInitStorageManagerNumber(blocknumber) {
		return s.accumulateLeaseRewards3(ratios, addrs, basePrice, revenueStorage, blocknumber, db, snapTotalLeaseSpace, spData, snap)
	}
	var LockReward []SpaceRewardRecord
	//basePrice := // SRT /TB.day
	storageHarvest := big.NewInt(0)
	if nil == addrs || len(addrs) == 0 {
		return LockReward, storageHarvest, nil, nil
	}
	validSuccLesae := make(map[common.Hash]uint64)
	for _, leaseHash := range addrs {
		validSuccLesae[leaseHash] = 1
	}
	decimalBasePrice := decimal.NewFromBigInt(basePrice, 0)
	totalLeaseSpace := s.getTotalLeaseSpace(revenueStorage, validSuccLesae, ratios, blocknumber, decimalBasePrice)
	err := s.saveDecimalValueTodb(totalLeaseSpace, db, blocknumber, totalLeaseSpaceKey)
	if err != nil {
		log.Error("saveTotalLeaseSpace", "err", err, "number", blocknumber)
	}
	AddTotalLeaseSpace := totalLeaseSpace.Add(decimal.NewFromBigInt(snapTotalLeaseSpace, 0))
	for pledgeAddr, storage := range s.StoragePledge {
		totalReward := big.NewInt(0)
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			for leaseHash, lease := range storage.Lease {
				if _, ok2 := validSuccLesae[leaseHash]; ok2 {
					leaseCapacity := decimal.NewFromBigInt(lease.Capacity, 0).Div(decimal.NewFromInt(1073741824)) //to GB
					if item, ok3 := ratios[revenue.RevenueAddress]; ok3 {
						bandwidthIndex := getBandwaith(storage.Bandwidth, blocknumber)
						reward := s.calStorageLeaseNewReward2(leaseCapacity, bandwidthIndex, item.Ratio, decimal.NewFromBigInt(lease.UnitPrice, 0), decimalBasePrice, AddTotalLeaseSpace)
						totalReward = new(big.Int).Add(totalReward, reward.BigInt())
					}
				}
			}
			if totalReward.Cmp(big.NewInt(0)) > 0 {
				LockReward = append(LockReward, SpaceRewardRecord{
					Target:  pledgeAddr,
					Amount:  totalReward,
					Revenue: revenue.RevenueAddress,
				})
				storageHarvest = new(big.Int).Add(storageHarvest, totalReward)
			}
		}
	}
	err = s.saveTotalValueTodb(storageHarvest, db, blocknumber, leaseHarvestKey)
	if err != nil {
		log.Error("saveleaseHarvest", "err", err, "number", blocknumber)
	}
	return LockReward, storageHarvest, totalLeaseSpace.BigInt(), nil
}

func (s *StorageData) getTotalLeaseSpace(revenueStorage map[common.Address]*RevenueParameter, validSuccLesae map[common.Hash]uint64, ratios map[common.Address]*StorageRatio, blocknumber uint64, basePrice decimal.Decimal) decimal.Decimal {
	totalLeaseSpace := decimal.NewFromInt(0) //B
	for pledgeAddr, storage := range s.StoragePledge {
		if revenue, ok := revenueStorage[pledgeAddr]; ok {
			for leaseHash, lease := range storage.Lease {
				if _, ok2 := validSuccLesae[leaseHash]; ok2 {
					capacity := decimal.NewFromBigInt(lease.Capacity, 0)
					if item, ok3 := ratios[revenue.RevenueAddress]; ok3 {
						bandwidthIndex := getBandwaith(storage.Bandwidth, blocknumber)
						rentPrice := decimal.NewFromBigInt(lease.UnitPrice, 0)
						priceIndex, priceRate := s.getPriceIndex(rentPrice, basePrice)
						calCapacity := s.getRegulate(capacity, bandwidthIndex, item.Ratio, priceRate, priceIndex)
						totalLeaseSpace = totalLeaseSpace.Add(calCapacity)
					}
				}
			}
		}
	}
	return totalLeaseSpace
}
func (s *StorageData) calStorageLeaseNewReward2(capacity decimal.Decimal, bandwidthIndex decimal.Decimal, storageIndex decimal.Decimal,
	rentPrice decimal.Decimal, basePrice decimal.Decimal, totalLeaseSpace decimal.Decimal) decimal.Decimal {
	gbUTGRate := s.getGbUTGRate(totalLeaseSpace)
	priceIndex, priceRate := s.getPriceIndex(rentPrice, basePrice)
	return s.getRegulate(gbUTGRate.Mul(capacity), bandwidthIndex, storageIndex, priceRate, priceIndex)
}

func (s *StorageData) getRegulate(source decimal.Decimal, bandwidthIndex decimal.Decimal, storageIndex decimal.Decimal, priceRate decimal.Decimal, priceIndex decimal.Decimal) decimal.Decimal {
	return source.Mul(priceRate).Mul(priceIndex).Mul(bandwidthIndex.Add(storageIndex)).Mul(decimal.NewFromBigInt(storageRentAdjRatio, 0).Div(decimal.NewFromInt(10000)))
}

func (s *StorageData) getPriceIndex(rentPrice decimal.Decimal, basePrice decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	priceIndex := decimal.NewFromInt(1)
	priceRate := rentPrice.Div(basePrice)
	if rentPrice.Cmp(basePrice) > 0 {
		priceIndex = decimal.NewFromBigInt(storageRentPriceRatio, 0).Div(decimal.NewFromInt(10000)).Add(decimal.NewFromInt(1))
	} else if rentPrice.Cmp(basePrice) < 0 {
		priceIndex = decimal.NewFromBigInt(storageRentPriceRatio, 0).Div(decimal.NewFromInt(10000)).Add(decimal.NewFromInt(1))
		priceIndex = decimal.NewFromInt(1).Div(priceIndex)
	}
	return priceIndex, priceRate
}

func (s *StorageData) getGbUTGRate(totalLeaseSpace decimal.Decimal) decimal.Decimal {
	oneEb := decimal.NewFromBigInt(tb1b, 0).Mul(decimal.NewFromInt(1048576)) //1eb= B
	modeeb := totalLeaseSpace.Mod(oneEb)
	neb := big.NewInt(1)
	if totalLeaseSpace.Cmp(oneEb) > 0 {
		neb = totalLeaseSpace.Div(oneEb).BigInt()
		if modeeb.Cmp(decimal.NewFromInt(0)) > 0 {
			neb = new(big.Int).Add(neb, big.NewInt(1))
		}
	}
	pwern, _ := decimal.NewFromString("0.9986146661010289") //0.5^1/500
	//Total_UTG(PoTS)×(1−0.5^n/400)  1EB rewards
	ebReward := decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(neb, 0))))
	beforebReward := decimal.NewFromInt(0)
	if neb.Cmp(big.NewInt(1)) > 0 {
		beforeNeb := new(big.Int).Sub(neb, big.NewInt(1))
		beforebReward = decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromInt(1).Sub(pwern.Pow(decimal.NewFromBigInt(beforeNeb, 0))))
	}
	ebReward = ebReward.Sub(beforebReward)
	gbUTGRate := ebReward.Div(decimal.NewFromInt(1073741824))
	return gbUTGRate
}

func (s *Snapshot) initStorageManager() {
	s.StorageData.StorageEntrust = make(map[common.Address]*SEntrust)
	for pledgeAddr, item := range s.StorageData.StoragePledge {
		s.StorageData.StorageEntrust[pledgeAddr] = &SEntrust{
			Manager:       item.Address,
			Sphash:        common.Hash{},
			Spheight:      common.Big0,
			EntrustRate:   common.Big0,
			PledgeAmount:  new(big.Int).Set(item.SpaceDeposit),
			ManagerAmount: new(big.Int).Set(item.SpaceDeposit),
			Managerheight: new(big.Int).Set(item.Number),
			Detail:        make(map[common.Hash]*SEntrustDetail),
		}
		s.StorageData.StorageEntrust[pledgeAddr].Detail[common.BytesToHash(pledgeAddr.Bytes())] = &SEntrustDetail{
			Address: item.Address,
			Height:  new(big.Int).Set(item.Number),
			Amount:  new(big.Int).Set(item.SpaceDeposit),
		}
	}
}

func (a *Alien) modifyStorageManager(currentManager []ModifySManagerRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, db *state.StateDB, snap *Snapshot, number *big.Int) []ModifySManagerRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("modifyStorageManager", "parameter error need 4 act", len(txDataInfo))
		return currentManager
	}
	postion := 3
	storageAddress := common.HexToAddress(txDataInfo[postion])
	storageNode := snap.StorageData.StoragePledge[storageAddress]
	if storageNode == nil {
		log.Warn("modifyStorageManager", "storage not exit storageAddress", storageAddress)
		return currentManager
	}
	storageNum := new(big.Int).Set(storageNode.Number)
	if isGEInitStorageManagerNumber(storageNum.Uint64()) {
		log.Warn("modifyStorageManager", "storage can not change manager", storageAddress, "storageNum", storageNum)
		return currentManager
	}
	if storageAddress != txSender {
		log.Warn("modifyStorageManager", "pledge address no role", storageAddress)
		return currentManager
	}
	storageNodePaddr := storageNode.Address
	storageEntrust := snap.StorageData.StorageEntrust[storageAddress]
	if storageEntrust == nil {
		log.Warn("modifyStorageManager", "storage not exit storageEntrust", storageAddress)
		return currentManager
	}
	curManager := storageEntrust.Manager
	if curManager != storageNodePaddr {
		log.Warn("modifyStorageManager", "pledge address has change manager already", storageAddress)
		return currentManager
	}
	postion++
	manager := common.HexToAddress(txDataInfo[postion])

	currentManager = append(currentManager, ModifySManagerRecord{
		Pledge:  storageAddress,
		Manager: manager,
	})
	topics := make([]common.Hash, 3)
	//web3.sha3("Modify Storage Manager")
	topics[0].UnmarshalText([]byte("0xd88e4545d35bedc1b14e926ef55b18780759fb1559dfdaf43c24386ddece40b0"))
	topics[1].SetBytes(storageAddress.Bytes())
	topics[2].SetBytes(manager.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return currentManager
}

func (s *Snapshot) updateStorageManager(modifySManagerRecord []ModifySManagerRecord, number *big.Int, db ethdb.Database) {
	if modifySManagerRecord == nil || len(modifySManagerRecord) == 0 {
		return
	}
	for _, modifySManager := range modifySManagerRecord {
		if _, ok := s.StorageData.StorageEntrust[modifySManager.Pledge]; ok {
			s.StorageData.StorageEntrust[modifySManager.Pledge].Manager = modifySManager.Manager
		}
	}
}

func (a *Alien) completeStoragePledge(currentCSPledge []CompleteSPledgeRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int) []CompleteSPledgeRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("completeStoragePledge", "parameter number", len(txDataInfo))
		return currentCSPledge
	}
	completeSPledge := CompleteSPledgeRecord{
		Pledge: common.Address{},
		Amount: common.Big0,
		Hash:   tx.Hash(),
	}
	postion := 3
	if err := completeSPledge.Pledge.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("completeSPledge", "Pledge", txDataInfo[postion])
		return currentCSPledge
	}
	if _, ok := snap.StorageData.StorageEntrust[completeSPledge.Pledge]; ok {
		if snap.StorageData.StorageEntrust[completeSPledge.Pledge].Manager != txSender {
			log.Warn("completeSPledge", "txSender is not manager", txSender)
			return currentCSPledge
		}
	} else {
		log.Warn("completeSPledge", "manager is empty", completeSPledge.Pledge)
		return currentCSPledge
	}
	postion++
	if amount, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("completeSPledge", "amount", txDataInfo[postion])
		return currentCSPledge
	} else {
		if amount.Cmp(decimal.Zero) < 0 {
			log.Warn("completeSPledge", "amount small than 0", txDataInfo[postion])
			return currentCSPledge
		}
		amountBig := amount.BigInt()
		if amountBig.Cmp(common.Big0) < 0 {
			log.Warn("completeSPledge", "amountBig small than 0", txDataInfo[postion])
			return currentCSPledge
		}

		completeSPledge.Amount = amount.BigInt()
		modValue := new(big.Int).Mod(completeSPledge.Amount, utgOneValue)
		if modValue.Cmp(common.Big0) != 0 {
			log.Warn("completeSPledge", "amount must rounding ", txDataInfo[postion])
			return currentCSPledge
		}
	}
	if _, ok := snap.StorageData.StoragePledge[completeSPledge.Pledge]; ok {
		spaceDeposit := new(big.Int).Set(snap.StorageData.StoragePledge[completeSPledge.Pledge].SpaceDeposit)
		addAmount := new(big.Int).Add(snap.StorageData.StorageEntrust[completeSPledge.Pledge].PledgeAmount, completeSPledge.Amount)
		for _, item := range currentCSPledge {
			if item.Pledge == completeSPledge.Pledge {
				addAmount = new(big.Int).Add(addAmount, item.Amount)
			}
		}
		if addAmount.Cmp(spaceDeposit) > 0 {
			log.Warn("completeSPledge", "pledgeAmount is too big", completeSPledge.Amount)
			return currentCSPledge
		}
	} else {
		log.Warn("completeSPledge", "StoragePledge is empty", completeSPledge.Pledge)
		return currentCSPledge
	}

	if state.GetBalance(txSender).Cmp(completeSPledge.Amount) < 0 {
		log.Warn("completeSPledge", "balance", state.GetBalance(txSender))
		return currentCSPledge
	}
	state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), completeSPledge.Amount))
	topics := make([]common.Hash, 3)
	//web3.sha3("Complete storage pledge")
	topics[0].UnmarshalText([]byte("0x3f9d7503e51b1d0f990bf7c7998dd0c3f978a730fcb972a08664a34a1ea4e029"))
	topics[1].SetBytes(completeSPledge.Pledge.Bytes())
	topics[2].SetBytes(completeSPledge.Amount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentCSPledge = append(currentCSPledge, completeSPledge)
	return currentCSPledge
}

func (s *Snapshot) updateCompleteSPledge(completeSPledgeRecord []CompleteSPledgeRecord, number *big.Int, db ethdb.Database) {
	if completeSPledgeRecord == nil || len(completeSPledgeRecord) == 0 {
		return
	}
	for _, completeSPledge := range completeSPledgeRecord {
		if _, ok := s.StorageData.StorageEntrust[completeSPledge.Pledge]; ok {
			pledgeAmount := new(big.Int).Add(s.StorageData.StorageEntrust[completeSPledge.Pledge].PledgeAmount, completeSPledge.Amount)
			s.StorageData.StorageEntrust[completeSPledge.Pledge].PledgeAmount = pledgeAmount
			s.StorageData.StorageEntrust[completeSPledge.Pledge].Detail[completeSPledge.Hash] = &SEntrustDetail{
				Address: s.StorageData.StorageEntrust[completeSPledge.Pledge].Manager,
				Height:  new(big.Int).Set(number),
				Amount:  completeSPledge.Amount,
			}
			s.StorageData.StorageEntrust[completeSPledge.Pledge].ManagerAmount = new(big.Int).Add(s.StorageData.StorageEntrust[completeSPledge.Pledge].ManagerAmount, completeSPledge.Amount)
			s.StorageData.StorageEntrust[completeSPledge.Pledge].Managerheight = new(big.Int).Set(number)
			spaceDeposit := s.StorageData.StoragePledge[completeSPledge.Pledge].SpaceDeposit
			if pledgeAmount.Cmp(spaceDeposit) >= 0 {
				if s.StorageData.StoragePledge[completeSPledge.Pledge].PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
					s.StorageData.StoragePledge[completeSPledge.Pledge].PledgeStatus = big.NewInt(SPledgeNormal)
				}
			}
		}
	}
}

func (a *Alien) storageSetRewardRatio(currentRatio []SPRewardRatioRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int) []SPRewardRatioRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("storageSetRewardRatio", "parameter number", len(txDataInfo))
		return currentRatio
	}
	sPRewardRatio := SPRewardRatioRecord{
		Pledge: common.Address{},
		Rate:   common.Big0,
	}
	postion := 3
	if err := sPRewardRatio.Pledge.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageSetRewardRatio", "Pledge", txDataInfo[postion])
		return currentRatio
	}
	if _, ok := snap.StorageData.StorageEntrust[sPRewardRatio.Pledge]; ok {
		if snap.StorageData.StorageEntrust[sPRewardRatio.Pledge].Manager != txSender {
			log.Warn("storageSetRewardRatio", "txSender is not manager", txSender)
			return currentRatio
		}
	} else {
		log.Warn("storageSetRewardRatio", "manager is empty", sPRewardRatio.Pledge)
		return currentRatio
	}
	postion++
	if rate, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("storageSetRewardRatio", "rate", txDataInfo[postion])
		return currentRatio
	} else {
		rateBig := rate.BigInt()
		if rateBig.Cmp(common.Big0) < 0 {
			log.Warn("storageSetRewardRatio", "rate small than 0", txDataInfo[postion])
			return currentRatio
		}
		if rateBig.Cmp(sPDistributionDefaultRate) > 0 {
			log.Warn("storageSetRewardRatio", "rate is too big", rateBig)
			return currentRatio
		}
		sPRewardRatio.Rate = rateBig
	}
	if sp, ok := snap.StorageData.StoragePledge[sPRewardRatio.Pledge]; ok {
		if sp.PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 && sp.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) != 0 {
			log.Warn("storageSetRewardRatio", "pledgeStatus is not normal or inactive", sPRewardRatio.Pledge)
			return currentRatio
		}
	} else {
		log.Warn("storageSetRewardRatio", "StoragePledge is empty", sPRewardRatio.Pledge)
		return currentRatio
	}

	topics := make([]common.Hash, 3)
	//web3.sha3("storage Set reward ratio")
	topics[0].UnmarshalText([]byte("0xb50c03de89e6521a1074a15148f92448e99b11ab80923be2516c81072b8cef0d"))
	topics[1].SetBytes(sPRewardRatio.Pledge.Bytes())
	topics[2].SetBytes(sPRewardRatio.Rate.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentRatio = append(currentRatio, sPRewardRatio)
	return currentRatio
}

func (s *Snapshot) updateSPRewardRatio(sPRewardRatioRecord []SPRewardRatioRecord, number *big.Int, db ethdb.Database) {
	if sPRewardRatioRecord == nil || len(sPRewardRatioRecord) == 0 {
		return
	}
	for _, sPRewardRatio := range sPRewardRatioRecord {
		if _, ok := s.StorageData.StorageEntrust[sPRewardRatio.Pledge]; ok {
			s.StorageData.StorageEntrust[sPRewardRatio.Pledge].EntrustRate = new(big.Int).Set(sPRewardRatio.Rate)
		}
	}
}

func (a *Alien) storageSetStoragePools(currentSPPool []SPPoolRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int) []SPPoolRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("storageSetStoragePools", "parameter number", len(txDataInfo))
		return currentSPPool
	}
	sPPool := SPPoolRecord{
		Pledge: common.Address{},
		Hash:   common.Hash{},
	}
	postion := 3
	if err := sPPool.Pledge.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageSetStoragePools", "Pledge", txDataInfo[postion])
		return currentSPPool
	}
	if _, ok := snap.StorageData.StorageEntrust[sPPool.Pledge]; ok {
		if snap.StorageData.StorageEntrust[sPPool.Pledge].Manager != txSender {
			log.Warn("storageSetStoragePools", "txSender is not manager", txSender)
			return currentSPPool
		}
	} else {
		log.Warn("storageSetStoragePools", "manager is empty", sPPool.Pledge)
		return currentSPPool
	}
	postion++
	sPPool.Hash = common.HexToHash(txDataInfo[postion])
	if _, ok := snap.SpData.PoolPledge[sPPool.Hash]; ok {

	} else {
		log.Warn("storageSetStoragePools", "StoragePledge is empty", sPPool.Pledge)
		return currentSPPool
	}

	if se, ok := snap.StorageData.StorageEntrust[sPPool.Pledge]; ok {
		nilHash := common.Hash{}
		if se.Sphash != nilHash {
			spheight := se.Spheight
			if number.Uint64()-spheight.Uint64() <= sPPoollockDay*snap.getBlockPreDay() {
				log.Warn("storageSetStoragePools", "sPPoollockDay not pass", sPPool.Pledge)
				return currentSPPool
			}
			if se.Sphash == sPPool.Hash {
				log.Warn("storageSetStoragePools", "address is in target pool", sPPool.Pledge)
				return currentSPPool
			}
		}
	} else {
		log.Warn("storageSetStoragePools", "StoragePledge is empty", sPPool.Pledge)
		return currentSPPool
	}
	if sp, ok := snap.SpData.PoolPledge[sPPool.Hash]; ok {
		if sp.Status != spStatusActive {
			log.Warn("storageSetStoragePools", "pool is not active", sPPool.Hash)
			return currentSPPool
		}
	} else {
		log.Warn("storageSetStoragePools", "pool is empty", sPPool.Hash)
		return currentSPPool
	}
	if _, ok := snap.StorageData.StoragePledge[sPPool.Pledge]; ok {
		if snap.StorageData.StoragePledge[sPPool.Pledge].PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 {
			log.Warn("storageSetStoragePools", "pledgeStatus is not normal", sPPool.Pledge)
			return currentSPPool
		}
		sCapacity := new(big.Int).Set(snap.StorageData.StoragePledge[sPPool.Pledge].TotalCapacity)
		poolTotalCapacity := new(big.Int).Set(snap.SpData.PoolPledge[sPPool.Hash].TotalCapacity)
		usedCapacity := new(big.Int).Set(snap.SpData.PoolPledge[sPPool.Hash].UsedCapacity)
		addCapacity := new(big.Int).Add(sCapacity, usedCapacity)
		for _, item := range currentSPPool {
			if item.Hash == sPPool.Hash {
				otherSCapacity := new(big.Int).Set(snap.StorageData.StoragePledge[item.Pledge].TotalCapacity)
				addCapacity = new(big.Int).Add(addCapacity, otherSCapacity)
			}
		}
		if addCapacity.Cmp(poolTotalCapacity) > 0 {
			log.Warn("storageSetStoragePools", "capacity oversize", sPPool.Pledge)
			return currentSPPool
		}
	} else {
		log.Warn("storageSetStoragePools", "StoragePledge is empty", sPPool.Pledge)
		return currentSPPool
	}
	topics := make([]common.Hash, 3)
	//web3.sha3("Setting Up Storage Pools")
	topics[0].UnmarshalText([]byte("0xac69a6e8c79546d40bdef6496045dbec819d23213cd31f50e1a8d04c7b3c34b7"))
	topics[1].SetBytes(sPPool.Pledge.Bytes())
	topics[2].SetBytes(sPPool.Hash.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentSPPool = append(currentSPPool, sPPool)
	return currentSPPool
}

func (s *Snapshot) updateSPPool(sPPoolRecord []SPPoolRecord, number *big.Int, db ethdb.Database) {
	if sPPoolRecord == nil || len(sPPoolRecord) == 0 {
		return
	}
	for _, sPPool := range sPPoolRecord {
		if _, ok := s.StorageData.StoragePledge[sPPool.Pledge]; ok {
			totalCapacity := new(big.Int).Set(s.StorageData.StoragePledge[sPPool.Pledge].TotalCapacity)
			if _, ok2 := s.StorageData.StorageEntrust[sPPool.Pledge]; ok2 {
				oldSphash := s.StorageData.StorageEntrust[sPPool.Pledge].Sphash
				s.StorageData.StorageEntrust[sPPool.Pledge].Sphash = sPPool.Hash
				s.StorageData.StorageEntrust[sPPool.Pledge].Spheight = new(big.Int).Set(number)
				if _, ok3 := s.SpData.PoolPledge[oldSphash]; ok3 {
					s.SpData.PoolPledge[oldSphash].UsedCapacity = new(big.Int).Sub(s.SpData.PoolPledge[oldSphash].UsedCapacity, totalCapacity)
					s.SpData.accumulateSpPledgelHash(oldSphash, false)
				}
				if _, ok4 := s.SpData.PoolPledge[sPPool.Hash]; ok4 {
					s.SpData.PoolPledge[sPPool.Hash].UsedCapacity = new(big.Int).Add(s.SpData.PoolPledge[sPPool.Hash].UsedCapacity, totalCapacity)
					s.SpData.accumulateSpPledgelHash(sPPool.Hash, false)
				}
			}
		}

	}
	s.SpData.accumulateSpDataHash()
}

func (a *Alien) storageMigration(currentMigration []SPMigrationRecord, currentLockReward []LockRewardRecord, currentExchangeSRT []ExchangeSRTRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int, chain consensus.ChainHeaderReader) ([]SPMigrationRecord, []LockRewardRecord, []ExchangeSRTRecord) {
	if len(txDataInfo) <= 8 {
		log.Warn("storageMigration", "parameter error len=", len(txDataInfo))
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	postion := 3
	peledgeAddr := common.HexToAddress(txDataInfo[postion])
	if _, ok := snap.StorageData.StoragePledge[peledgeAddr]; !ok {
		log.Warn("storageMigration", " peledgeAddr is not exist", peledgeAddr)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	if _, ok := snap.StorageData.StorageEntrust[peledgeAddr]; !ok {
		log.Warn("storageMigration", " StorageEntrust is not exist", peledgeAddr)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	for _, item := range currentMigration {
		if item.Pledge == peledgeAddr {
			log.Warn("storageMigration", " peledgeAddr is in exit", peledgeAddr)
			return currentMigration, currentLockReward, currentExchangeSRT
		}
	}
	manager := snap.StorageData.StorageEntrust[peledgeAddr].Manager
	if txSender != manager {
		log.Warn("storageMigration", " txSender is not manager", manager)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	postion++
	storageCapacity, err := decimal.NewFromString(txDataInfo[postion])
	if err != nil {
		log.Warn("storageMigration", "storageCapacity", txDataInfo[postion])
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	totalCapacity := snap.StorageData.StoragePledge[peledgeAddr].TotalCapacity
	if totalCapacity.Cmp(storageCapacity.BigInt()) != 0 {
		log.Warn("storageMigration", "storageCapacity not equal", txDataInfo[postion])
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	maxPledgeCapacity := maxPledgeStorageCapacityV2
	if storageCapacity.Cmp(minPledgeStorageCapacity) < 0 || storageCapacity.Cmp(maxPledgeCapacity) > 0 {
		log.Warn("storageMigration", "storageCapacity", storageCapacity, "minPledgeStorageCapacity", minPledgeStorageCapacity, "maxPledgeStorageCapacity", maxPledgeStorageCapacity)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	postion++
	startPkNumber := txDataInfo[postion]
	postion++
	pkNonce, err := decimal.NewFromString(txDataInfo[postion])
	if err != nil {
		log.Warn("storageMigration", "pkNonce", txDataInfo[postion])
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	postion++
	pkBlockHash := txDataInfo[postion]
	postion++
	verifyData := txDataInfo[postion]
	verifyType := ""
	if strings.HasPrefix(verifyData, "v1") {
		verifyType = "v1"
		verifyData = verifyData[3:]
	}
	verifyDataArr := strings.Split(verifyData, ",")
	if len(verifyDataArr) < 10 {
		log.Warn("storageMigration verifyData format error", "verifyData", verifyData, "verifyDataArr", verifyDataArr)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	pkHeader := chain.GetHeaderByHash(common.HexToHash(pkBlockHash))
	if pkHeader == nil {
		log.Warn("storageMigration", "pkBlockHash is not exist", pkBlockHash)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	if verifyDataArr[4] != storageBlockSize {
		log.Warn("storageMigration storageBlockSize error", "storageBlockSize", storageBlockSize, "verifyDataArr[4]", verifyDataArr[4])
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	if pkHeader.Number.String() != startPkNumber || pkHeader.Nonce.Uint64() != pkNonce.BigInt().Uint64() {
		log.Warn("storageMigration  packege param compare error", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash, " chain", pkHeader.Number)
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	rootHash := verifyDataArr[len(verifyDataArr)-1]
	if verifyType == "v1" {
		if !verifyPocStringV1(startPkNumber, txDataInfo[6], pkBlockHash, txDataInfo[8], rootHash, txDataInfo[3]) {
			log.Warn("storageMigration  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currentMigration, currentLockReward, currentExchangeSRT
		}
	} else {
		if !verifyPocString(startPkNumber, txDataInfo[6], pkBlockHash, verifyData, rootHash, txDataInfo[3]) {
			log.Warn("storageMigration  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currentMigration, currentLockReward, currentExchangeSRT
		}
	}

	storageSize, err := decimal.NewFromString(verifyDataArr[4])
	if err != nil || storageSize.Cmp(decimal.Zero) <= 0 {
		log.Warn("storageMigration storageSize format error", "storageSize", verifyDataArr[4])
		return currentMigration, currentLockReward, currentExchangeSRT
	}

	blocknum, err := decimal.NewFromString(verifyDataArr[5])
	if err != nil || blocknum.Cmp(decimal.Zero) <= 0 {
		log.Warn("storageMigration blocknum format error", "blocknum", verifyDataArr[5])
		return currentMigration, currentLockReward, currentExchangeSRT
	}
	actblocknum := storageCapacity.Div(storageSize)
	if actblocknum.Cmp(blocknum) != 0 {
		log.Warn("storageMigration storageCapacity not same in verify", "actblocknum", actblocknum, "blocknum", blocknum.Mul(storageSize))
		return currentMigration, currentLockReward, currentExchangeSRT
	}

	leases := snap.StorageData.StoragePledge[peledgeAddr].Lease
	if leases != nil {
		for lhash, lease := range leases {
			if lease.Status == LeaseReturn || lease.Status == LeaseNotPledged {
				continue
			}
			leaseStatus := LeaseUserRescind
			if lease.Status == LeaseNormal || lease.Status == LeaseBreach {
				leaseLists := lease.LeaseList
				expireNumber := big.NewInt(0)
				for ldhash, leaseDetail := range leaseLists {
					deposit := leaseDetail.Deposit
					if deposit.Cmp(big.NewInt(0)) > 0 {
						startTime := leaseDetail.StartTime
						duration := leaseDetail.Duration
						leaseDetailEndNumber := new(big.Int).Add(startTime, new(big.Int).Mul(duration, new(big.Int).SetUint64(snap.getBlockPreDay())))
						if ldhash != lhash {
							leaseDetailEndNumber = new(big.Int).Sub(leaseDetailEndNumber, common.Big1)
						}
						if expireNumber.Cmp(leaseDetailEndNumber) < 0 {
							expireNumber = leaseDetailEndNumber
						}
					}
				}
				if expireNumber.Cmp(number) <= 0 {
					leaseStatus = LeaseExpiration
				}
			}
			revertLockReward := make([]SpaceRewardRecord, 0)
			revertExchangeSRT := make([]ExchangeSRTRecord, 0)
			bAmount := common.Big0
			revertLockReward, revertExchangeSRT, bAmount = snap.StorageData.dealLeaseRevert(lease, revertLockReward, revertExchangeSRT, snap.SystemConfig.ExchRate, leaseStatus, number.Uint64(), lhash, snap.getBlockPreDay(), bAmount)
			for _, item := range revertLockReward {
				currentLockReward = append(currentLockReward, LockRewardRecord{
					Target:   item.Target,
					Amount:   item.Amount,
					IsReward: sscEnumStoragePledgeRedeemLock,
				})
			}
			if len(revertExchangeSRT) > 0 {
				currentExchangeSRT = append(currentExchangeSRT, revertExchangeSRT...)
			}
			if bAmount != nil && bAmount.Cmp(common.Big0) > 0 {
				state.AddBalance(common.BigToAddress(big.NewInt(0)), bAmount)
			}
		}
	}

	migration := SPMigrationRecord{
		Pledge:   peledgeAddr,
		RootHash: common.HexToHash(rootHash),
	}
	topics := make([]common.Hash, 2)
	//web3.sha3("Storage Migration")
	topics[0].UnmarshalText([]byte("0x0d8986f9238b0afa7af83a2a2bf2696611ec35e24572634ff99aad5eb62e30d4"))
	topics[1].SetBytes(peledgeAddr.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentMigration = append(currentMigration, migration)
	return currentMigration, currentLockReward, currentExchangeSRT
}

func (s *Snapshot) updateSPMigration(migrationRecord []SPMigrationRecord, number *big.Int, db ethdb.Database) {
	if migrationRecord == nil || len(migrationRecord) == 0 {
		return
	}
	for _, record := range migrationRecord {
		if _, ok := s.StorageData.StoragePledge[record.Pledge]; ok {
			storageFile := make(map[common.Hash]*StorageFile)
			storageCapacity := s.StorageData.StoragePledge[record.Pledge].TotalCapacity
			storageFile[record.RootHash] = &StorageFile{
				Capacity:                    new(big.Int).Set(storageCapacity),
				CreateTime:                  new(big.Int).Set(number),
				LastVerificationTime:        new(big.Int).Set(number),
				LastVerificationSuccessTime: new(big.Int).Set(number),
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			storageSpaces := s.StorageData.StoragePledge[record.Pledge].StorageSpaces
			space := &SPledgeSpaces{
				Address:                     storageSpaces.Address,
				StorageCapacity:             new(big.Int).Set(storageCapacity),
				RootHash:                    record.RootHash,
				StorageFile:                 storageFile,
				LastVerificationTime:        new(big.Int).Set(number),
				LastVerificationSuccessTime: new(big.Int).Set(number),
				ValidationFailureTotalTime:  big.NewInt(0),
			}
			oldStoragePledge := s.StorageData.StoragePledge[record.Pledge]
			storagepledge := &SPledge{
				Address:                     oldStoragePledge.Address,
				StorageSpaces:               space,
				Number:                      new(big.Int).Set(oldStoragePledge.Number),
				TotalCapacity:               new(big.Int).Set(storageCapacity),
				Price:                       new(big.Int).Set(oldStoragePledge.Price),
				StorageSize:                 new(big.Int).Set(oldStoragePledge.StorageSize),
				SpaceDeposit:                new(big.Int).Set(oldStoragePledge.SpaceDeposit),
				Lease:                       make(map[common.Hash]*Lease),
				LastVerificationTime:        new(big.Int).Set(number),
				LastVerificationSuccessTime: new(big.Int).Set(number),
				ValidationFailureTotalTime:  big.NewInt(0),
				PledgeStatus:                big.NewInt(SPledgeNormal),
				Bandwidth:                   new(big.Int).Set(oldStoragePledge.Bandwidth),
			}
			if oldStoragePledge.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
				storagepledge.PledgeStatus = big.NewInt(SPledgeInactive)
			}
			s.StorageData.StoragePledge[record.Pledge] = storagepledge
			s.StorageData.accumulateSpaceStorageFileHash(record.Pledge, storageFile[record.RootHash]) //update file -->  space -- pledge
		}
	}
	s.StorageData.accumulateHeaderHash() //update all  to header valid root
}

func (a *Alien) declareStoragePledge2(currStoragePledge2 []SPledge2Record, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, blocknumber *big.Int, chain consensus.ChainHeaderReader) []SPledge2Record {
	if len(txDataInfo) < 13 {
		log.Warn("declareStoragePledge2", "parameter error len=", len(txDataInfo))
		return currStoragePledge2
	}
	peledgeAddr := common.HexToAddress(txDataInfo[3])
	if _, ok := snap.StorageData.StoragePledge[peledgeAddr]; ok {
		log.Warn("Storage Pledge2 repeat", " peledgeAddr", peledgeAddr)
		return currStoragePledge2
	}
	var bigPrice *big.Int
	if price, err := decimal.NewFromString(txDataInfo[4]); err != nil {
		log.Warn("Storage Pledge2 price wrong", "price", txDataInfo[4])
		return currStoragePledge2
	} else {
		bigPrice = price.BigInt()
	}
	basePrice := decimal.NewFromBigInt(snap.SystemConfig.Deposit[sscEnumStoragePrice], 0)
	minPrice := basePrice.BigInt()
	maxPrice := basePrice.Mul(decimal.NewFromInt(10)).BigInt()
	minPrice = (basePrice.Mul(decimal.NewFromFloat(0.1))).BigInt()
	if bigPrice.Cmp(minPrice) < 0 || bigPrice.Cmp(maxPrice) > 0 {
		log.Warn("price is set too high 2", " price", bigPrice)
		return currStoragePledge2
	}
	storageCapacity, err := decimal.NewFromString(txDataInfo[5])
	if err != nil {
		log.Warn("Storage Pledge2 storageCapacity format error", "storageCapacity", txDataInfo[5])
		return currStoragePledge2
	}
	maxPledgeCapacity := maxPledgeStorageCapacity
	maxPledgeCapacity = maxPledgeStorageCapacityV2
	minStorageCapacity := minPledgeStorageCapacity
	if blocknumber.Uint64() >= 101900 {
		minStorageCapacity = decimal.NewFromInt(1)
	}
	if storageCapacity.Cmp(minStorageCapacity) < 0 || storageCapacity.Cmp(maxPledgeCapacity) > 0 {
		log.Warn("Storage Pledge2 storageCapacity error", "storageCapacity", storageCapacity, "minPledgeStorageCapacity", minStorageCapacity, "maxPledgeStorageCapacity", maxPledgeStorageCapacity)
		return currStoragePledge2
	}
	startPkNumber := txDataInfo[6]
	pkNonce, err := decimal.NewFromString(txDataInfo[7])
	if err != nil {
		log.Warn("Storage Pledge2 package nonce error", "pkNonce", txDataInfo[7])
		return currStoragePledge2
	}
	pkBlockHash := txDataInfo[8]
	verifyData := txDataInfo[9]
	verifyType := ""
	if strings.HasPrefix(verifyData, "v1") {
		verifyType = "v1"
		verifyData = verifyData[3:]
	}
	verifyDataArr := strings.Split(verifyData, ",")
	if len(verifyDataArr) < 10 {
		log.Warn("Storage Pledge2 verifyData format error", "verifyData", verifyData, "verifyDataArr", verifyDataArr)
		return currStoragePledge2
	}

	pkHeader := chain.GetHeaderByHash(common.HexToHash(pkBlockHash))
	if pkHeader == nil {
		log.Warn("Storage Pledge2", "pkBlockHash is not exist", pkBlockHash)
		return currStoragePledge2
	}
	if verifyDataArr[4] != storageBlockSize {
		log.Warn("Storage Pledge2 storageBlockSize error", "storageBlockSize", storageBlockSize, "verifyDataArr[4]", verifyDataArr[4])
		return currStoragePledge2
	}
	if pkHeader.Number.String() != startPkNumber || pkHeader.Nonce.Uint64() != pkNonce.BigInt().Uint64() {
		log.Warn("Storage Pledge2  packege param compare error", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash, " chain", pkHeader.Number)
		return currStoragePledge2
	}

	rootHash := verifyDataArr[len(verifyDataArr)-1]
	if verifyType == "v1" {
		if !verifyPocStringV1(startPkNumber, txDataInfo[7], pkBlockHash, txDataInfo[9], rootHash, txDataInfo[3]) {
			log.Warn("Storage Pledge2  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currStoragePledge2
		}
	} else {
		if !verifyPocString(startPkNumber, txDataInfo[7], pkBlockHash, verifyData, rootHash, txDataInfo[3]) {
			log.Warn("Storage Pledge2  verifyPoc Faild", "startPkNumber", startPkNumber, "pkNonce", pkNonce, "pkBlockHash", pkBlockHash)
			return currStoragePledge2
		}
	}

	storageSize, err := decimal.NewFromString(verifyDataArr[4])
	if err != nil || storageSize.Cmp(decimal.Zero) <= 0 {
		log.Warn("Storage Pledge2 storageSize format error", "storageSize", verifyDataArr[4])
		return currStoragePledge2
	}

	blocknum, err := decimal.NewFromString(verifyDataArr[5])
	if err != nil || blocknum.Cmp(decimal.Zero) <= 0 {
		log.Warn("Storage Pledge2 blocknum format error", "blocknum", verifyDataArr[5])
		return currStoragePledge2
	}
	actblocknum := storageCapacity.Div(storageSize)
	if actblocknum.Cmp(blocknum) != 0 {
		log.Warn("Storage Pledge2 storageCapacity not same in verify", "actblocknum", actblocknum, "blocknum", blocknum.Mul(storageSize))
		return currStoragePledge2
	}

	bandwidth, err := decimal.NewFromString(txDataInfo[10])

	if err != nil || bandwidth.BigInt().Cmp(big.NewInt(0)) <= 0 {
		log.Warn("Storage Pledge2  bandwidth error", "bandwidth", bandwidth)
		return currStoragePledge2
	}

	if err := a.checkPledgeMaxStorageSpace2(currStoragePledge2, peledgeAddr, snap, blocknumber, storageCapacity.BigInt()); err != nil {
		log.Warn("Storage Pledge2", "checkRevenueStorageBind", err.Error())
		return currStoragePledge2
	}
	totalStorage := big.NewInt(0)
	for _, spledge := range snap.StorageData.StoragePledge {
		totalStorage = new(big.Int).Add(totalStorage, spledge.TotalCapacity)
	}
	pledgeAllAmount := getSotragePledgeAmount(storageCapacity, bandwidth, decimal.NewFromBigInt(totalStorage, 0), blocknumber, snap)
	pledgeRateDec, err := decimal.NewFromString(txDataInfo[11])
	if err != nil || pledgeRateDec.BigInt().Cmp(MinimumThresholdForPledgeAmount) < 0 || pledgeRateDec.BigInt().Cmp(big.NewInt(100)) > 0 {
		log.Warn("Storage Pledge2  pledgeRate error", "pledgeRate", txDataInfo[11])
		return currStoragePledge2
	}
	pledgeRate := pledgeRateDec.BigInt()
	pledgeAmount := pledgeAllAmount
	if pledgeRate.Cmp(big.NewInt(100)) < 0 {
		leftPer := new(big.Int).Sub(big.NewInt(100), pledgeRate)
		leftAmount := new(big.Int).Mul(pledgeAllAmount, leftPer)
		leftAmount = new(big.Int).Div(leftAmount, big.NewInt(100))
		leftMod := new(big.Int).Mod(leftAmount, utgOneValue)
		leftAmount = new(big.Int).Sub(leftAmount, leftMod)
		pledgeAmount = new(big.Int).Sub(pledgeAllAmount, leftAmount)
	}

	entrustRate, err := decimal.NewFromString(txDataInfo[12])
	if err != nil || entrustRate.BigInt().Cmp(big.NewInt(0)) < 0 {
		log.Warn("Storage Pledge2  entrustRate error", "entrustRate", entrustRate)
		return currStoragePledge2
	}
	entrustRateBig := entrustRate.BigInt()
	if entrustRateBig.Cmp(sPDistributionDefaultRate) > 0 {
		log.Warn("Storage Pledge2  entrustRate error", "entrustRate is too big", entrustRateBig)
		return currStoragePledge2
	}

	if state.GetBalance(txSender).Cmp(pledgeAmount) < 0 {
		log.Warn("Claimed sotrage2", "balance", state.GetBalance(txSender), "need", pledgeAmount)
		return currStoragePledge2
	}
	state.SetBalance(txSender, new(big.Int).Sub(state.GetBalance(txSender), pledgeAmount))
	topics := make([]common.Hash, 5)
	//web3.sha3("declareStoragePledge2")
	topics[0].UnmarshalText([]byte("0x33f1b782df697f77c462dee9d98bf443fcc8fab5fcb897d67ef0c69e8fd623a6"))
	topics[1].SetBytes(peledgeAddr.Bytes())
	topics[2].SetBytes(pledgeAllAmount.Bytes())
	topics[3].SetBytes(pledgeAmount.Bytes())
	topics[4].SetBytes(entrustRateBig.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	storageRecord := SPledge2Record{
		PledgeAddr:      txSender,
		Address:         peledgeAddr,
		Price:           bigPrice,
		SpaceDeposit:    pledgeAllAmount,
		StorageCapacity: storageCapacity.BigInt(),
		StorageSize:     storageSize.BigInt(),
		RootHash:        common.HexToHash(rootHash),
		PledgeNumber:    blocknumber,
		Bandwidth:       bandwidth.BigInt(),
		PledgeAmount:    pledgeAmount,
		EntrustRate:     entrustRateBig,
		Hash:            tx.Hash(),
	}
	currStoragePledge2 = append(currStoragePledge2, storageRecord)
	return currStoragePledge2
}

func (a *Alien) checkPledgeMaxStorageSpace2(currStoragePledge []SPledge2Record, targetDev common.Address, snap *Snapshot, number *big.Int, totalCapacity *big.Int) error {
	if number.Uint64() > PledgeRevertLockEffectNumber {
		targetRevenueAddress := common.Address{}
		findRevenue := false
		for device, revenue := range snap.RevenueStorage {
			if targetDev == device {
				targetRevenueAddress = revenue.RevenueAddress
				findRevenue = true
				break
			}
		}
		if findRevenue {
			alreadybind := make(map[common.Address]uint64)
			devToRevenue := make(map[common.Address]common.Address)
			for device, revenue := range snap.RevenueStorage {
				revenueAddress := revenue.RevenueAddress
				if targetRevenueAddress == revenueAddress {
					alreadybind[device] = 1
				}
				devToRevenue[device] = revenueAddress
			}
			for _, item := range currStoragePledge {
				if revenueAddress, ok := devToRevenue[item.Address]; ok {
					if targetRevenueAddress == revenueAddress {
						totalCapacity = new(big.Int).Add(totalCapacity, item.StorageCapacity)
					}
				}
			}
			return a.checkMaxStorageSpaceByAddr(alreadybind, snap, totalCapacity)
		}
	}
	return nil
}

func (s *Snapshot) updateStorageData2(pledgeRecord []SPledge2Record, db ethdb.Database) {
	if pledgeRecord == nil || len(pledgeRecord) == 0 {
		return
	}
	for _, record := range pledgeRecord {
		storageFile := make(map[common.Hash]*StorageFile)
		storageFile[record.RootHash] = &StorageFile{
			Capacity:                    new(big.Int).Set(record.StorageCapacity),
			CreateTime:                  new(big.Int).Set(record.PledgeNumber),
			LastVerificationTime:        new(big.Int).Set(record.PledgeNumber),
			LastVerificationSuccessTime: new(big.Int).Set(record.PledgeNumber),
			ValidationFailureTotalTime:  big.NewInt(0),
		}
		space := &SPledgeSpaces{
			Address:                     record.Address,
			StorageCapacity:             new(big.Int).Set(record.StorageCapacity),
			RootHash:                    record.RootHash,
			StorageFile:                 storageFile,
			LastVerificationTime:        new(big.Int).Set(record.PledgeNumber),
			LastVerificationSuccessTime: new(big.Int).Set(record.PledgeNumber),
			ValidationFailureTotalTime:  big.NewInt(0),
		}
		pledgeStatus := big.NewInt(SPledgeNormal)
		if record.PledgeAmount.Cmp(record.SpaceDeposit) < 0 {
			pledgeStatus = big.NewInt(SPledgeInactive)
		}
		storagepledge := &SPledge{
			Address:                     record.PledgeAddr,
			StorageSpaces:               space,
			Number:                      new(big.Int).Set(record.PledgeNumber),
			TotalCapacity:               new(big.Int).Set(record.StorageCapacity),
			Price:                       new(big.Int).Set(record.Price),
			StorageSize:                 new(big.Int).Set(record.StorageSize),
			SpaceDeposit:                new(big.Int).Set(record.SpaceDeposit),
			Lease:                       make(map[common.Hash]*Lease),
			LastVerificationTime:        new(big.Int).Set(record.PledgeNumber),
			LastVerificationSuccessTime: new(big.Int).Set(record.PledgeNumber),
			ValidationFailureTotalTime:  big.NewInt(0),
			PledgeStatus:                pledgeStatus,
			Bandwidth:                   new(big.Int).Set(record.Bandwidth),
		}
		s.StorageData.StoragePledge[record.Address] = storagepledge
		s.StorageData.accumulateSpaceStorageFileHash(record.Address, storageFile[record.RootHash])
		s.StorageData.accumulatePledgeHash(record.Address)
		storageEntrustDetail := make(map[common.Hash]*SEntrustDetail)
		storageEntrustDetail[record.Hash] = &SEntrustDetail{
			Address: record.PledgeAddr,
			Height:  new(big.Int).Set(record.PledgeNumber),
			Amount:  new(big.Int).Set(record.PledgeAmount),
		}
		storageEntrust := &SEntrust{
			Manager:       record.PledgeAddr,
			Sphash:        common.Hash{},
			Spheight:      common.Big0,
			EntrustRate:   new(big.Int).Set(record.EntrustRate),
			PledgeAmount:  new(big.Int).Set(record.PledgeAmount),
			ManagerAmount: new(big.Int).Set(record.PledgeAmount),
			Managerheight: new(big.Int).Set(record.PledgeNumber),
			Detail:        storageEntrustDetail,
		}
		s.StorageData.StorageEntrust[record.Address] = storageEntrust
	}
	s.StorageData.accumulateHeaderHash()
}

func (a *Alien) storageSPEntrust(currentSPEntrust []SPEntrustRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int, chain consensus.ChainHeaderReader) []SPEntrustRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("storageSPEntrust", "parameter number", len(txDataInfo))
		return currentSPEntrust
	}
	sPEntrust := SPEntrustRecord{
		Target:  common.Address{},
		Amount:  common.Big0,
		Address: txSender,
		Hash:    tx.Hash(),
	}
	postion := 3
	if err := sPEntrust.Target.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageSPEntrust", "miner address", txDataInfo[postion])
		return currentSPEntrust
	}
	if _, ok := snap.StorageData.StoragePledge[sPEntrust.Target]; !ok {
		log.Warn("storageSPEntrust", "StoragePledge is not exist", sPEntrust.Target)
		return currentSPEntrust
	}
	if _, ok := snap.StorageData.StorageEntrust[sPEntrust.Target]; !ok {
		log.Warn("storageSPEntrust", "StorageEntrust is not exist", sPEntrust.Target)
		return currentSPEntrust
	}

	if _, ok := snap.StorageData.StoragePledge[sPEntrust.Address]; ok {
		log.Warn("storageSPEntrust", "txSender is Storage address", sPEntrust.Address)
		return currentSPEntrust
	}
	postion++
	if amount, err := decimal.NewFromString(txDataInfo[postion]); err != nil {
		log.Warn("storageSPEntrust", "amount", txDataInfo[postion])
		return currentSPEntrust
	} else {
		if amount.Cmp(decimal.Zero) < 0 {
			log.Warn("storageSPEntrust", "amount small than 0", txDataInfo[postion])
			return currentSPEntrust
		}
		sPEntrust.Amount = amount.BigInt()
	}
	if sPEntrust.Amount.Cmp(utgOneValue) < 0 {
		log.Warn("storageSPEntrust", "amountBig small than 1 utg", txDataInfo[postion])
		return currentSPEntrust
	}
	modValue := new(big.Int).Mod(sPEntrust.Amount, utgOneValue)
	if modValue.Cmp(common.Big0) != 0 {
		log.Warn("storageSPEntrust", "amount must rounding", txDataInfo[postion])
		return currentSPEntrust
	}
	storagePledge := snap.StorageData.StoragePledge[sPEntrust.Target]
	if storagePledge.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) != 0 {
		log.Warn("storageSPEntrust", "pledgeStatus is not inactive", sPEntrust.Target)
		return currentSPEntrust
	}
	spaceDeposit := new(big.Int).Set(storagePledge.SpaceDeposit)
	storageEntrust := snap.StorageData.StorageEntrust[sPEntrust.Target]
	pledgeAmount := new(big.Int).Set(storageEntrust.PledgeAmount)
	pledgeAmount = new(big.Int).Add(pledgeAmount, sPEntrust.Amount)
	for _, item := range currentSPEntrust {
		if item.Target == sPEntrust.Target {
			pledgeAmount = new(big.Int).Add(pledgeAmount, item.Amount)
		}
	}
	if pledgeAmount.Cmp(spaceDeposit) > 0 {
		log.Warn("storageSPEntrust", "pledgeAmount bigger than spaceDeposit", pledgeAmount)
		return currentSPEntrust
	}
	targetMiner := snap.findStorageTargetMiner(txSender)
	nilAddr := common.Address{}
	if targetMiner != nilAddr && targetMiner != sPEntrust.Target {
		log.Warn("storageSPEntrust", "one address can only pledge one miner ", targetMiner)
		return currentSPEntrust
	}
	if state.GetBalance(txSender).Cmp(sPEntrust.Amount) < 0 {
		log.Warn("storageSPEntrust", "balance", state.GetBalance(txSender))
		return currentSPEntrust
	}
	state.SubBalance(txSender, sPEntrust.Amount)
	topics := make([]common.Hash, 2)
	//web3.sha3("SN Entrusted Pledge")
	topics[0].UnmarshalText([]byte("0x57e4a12eae9236c75aa3a85b2537ba16c8109de35fed13b6c4e392ffa860dd07"))
	topics[1].SetBytes(sPEntrust.Target.Bytes())
	data := common.Hash{}
	data.SetBytes(sPEntrust.Amount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, data.Bytes())
	currentSPEntrust = append(currentSPEntrust, sPEntrust)
	return currentSPEntrust
}

func (snap *Snapshot) findStorageTargetMiner(txSender common.Address) common.Address {
	for miner, item := range snap.StorageData.StorageEntrust {
		details := item.Detail
		for _, detail := range details {
			if detail.Address == txSender {
				return miner
			}
		}
	}
	return common.Address{}
}

func (s *Snapshot) updateSPEntrust(entrustRecord []SPEntrustRecord, number *big.Int, db ethdb.Database) {
	if entrustRecord == nil || len(entrustRecord) == 0 {
		return
	}
	for _, entrust := range entrustRecord {
		if _, ok := s.StorageData.StorageEntrust[entrust.Target]; ok {
			s.StorageData.StorageEntrust[entrust.Target].PledgeAmount = new(big.Int).Add(s.StorageData.StorageEntrust[entrust.Target].PledgeAmount, entrust.Amount)
			storageEntrustDetail := s.StorageData.StorageEntrust[entrust.Target].Detail
			storageEntrustDetail[entrust.Hash] = &SEntrustDetail{
				Address: entrust.Address,
				Height:  new(big.Int).Set(number),
				Amount:  new(big.Int).Set(entrust.Amount),
			}
			if entrust.Address == s.StorageData.StorageEntrust[entrust.Target].Manager {
				s.StorageData.StorageEntrust[entrust.Target].ManagerAmount = new(big.Int).Add(s.StorageData.StorageEntrust[entrust.Target].ManagerAmount, entrust.Amount)
				s.StorageData.StorageEntrust[entrust.Target].Managerheight = new(big.Int).Set(number)
			}

			storagePledge := s.StorageData.StoragePledge[entrust.Target]
			spaceDeposit := new(big.Int).Set(storagePledge.SpaceDeposit)
			storageEntrust := s.StorageData.StorageEntrust[entrust.Target]
			pledgeAmount := new(big.Int).Set(storageEntrust.PledgeAmount)

			if pledgeAmount.Cmp(spaceDeposit) >= 0 {
				if s.StorageData.StoragePledge[entrust.Target].PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
					s.StorageData.StoragePledge[entrust.Target].PledgeStatus = big.NewInt(SPledgeNormal)
					s.StorageData.accumulatePledgeHash(entrust.Target)
				}
			}
		}
	}
	s.StorageData.accumulateHeaderHash()
}

func (a *Alien) storageEntrustedPledgeTransfer(currentSETransfer []SETransferRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int, chain consensus.ChainHeaderReader) []SETransferRecord {
	if len(txDataInfo) <= 5 {
		log.Warn("storageEntrustedPledgeTransfer", "parameter error", len(txDataInfo))
		return currentSETransfer
	}
	sETransfer := SETransferRecord{
		Address:      txSender,
		PledgeHash:   tx.Hash(),
		Original:     common.Address{},
		Target:       common.Address{},
		PledgeAmount: big.NewInt(0),
		LockAmount:   big.NewInt(0),
	}
	if isInCurrentSETransfer(currentSETransfer, sETransfer.Address) {
		log.Warn("storageEntrustedPledgeTransfer", "Address is in currentSETransfer", sETransfer.Address)
		return currentSETransfer
	}
	postion := 3
	if err := sETransfer.Original.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageEntrustedPledgeTransfer", "Target error", txDataInfo[postion])
		return currentSETransfer
	}
	if se, ok := snap.StorageData.StorageEntrust[sETransfer.Original]; ok {
		if se.Manager == txSender {
			log.Warn("storageEntrustedPledgeTransfer", "manager address no role", txDataInfo[postion])
			return currentSETransfer
		}
		stp := snap.StorageData.StoragePledge[sETransfer.Original]
		if stp == nil {
			log.Warn("storageEntrustedPledgeTransfer", "storagePledge is not exist", txDataInfo[postion])
			return currentSETransfer
		}
		if stp.PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) != 0 && stp.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) != 0 {
			log.Warn("storageEntrustedPledgeTransfer", "stp Status  is exiting or exited ", txSender)
			return currentSETransfer
		}
		transAmount := big.NewInt(0)
		pledgeMinBLock := a.getEntrustPledgeMinBLock(stpEntrustMinDay)
		for _, detail := range se.Detail {
			if detail.Address == txSender {
				pledgeBLock := new(big.Int).Sub(new(big.Int).SetUint64(snap.Number), detail.Height)
				if pledgeBLock.Cmp(pledgeMinBLock) < 0 {
					log.Warn("storageEntrustedPledgeTransfer", "Entrust hash illegality", txSender)
					return currentSETransfer
				}
				transAmount = new(big.Int).Add(transAmount, detail.Amount)
			}
		}
		if transAmount.Cmp(big.NewInt(0)) <= 0 {
			log.Warn("storageEntrustedPledgeTransfer", "TxSender does not have a transferable deposit ", txSender)
			return currentSETransfer
		}
		sETransfer.PledgeAmount = transAmount

	} else {
		log.Warn("storageEntrustedPledgeTransfer", "se not exist ", sETransfer.Original)
		return currentSETransfer
	}

	postion++
	sETransfer.TargetType = txDataInfo[postion]
	postion++
	if TargetTypePos == sETransfer.TargetType {
		if err := sETransfer.Target.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
			log.Warn("storageEntrustedPledgeTransfer", "PoS target Address error", txDataInfo[postion])
			return currentSETransfer
		}
		if _, ok := snap.PosPledge[sETransfer.Target]; !ok {
			log.Warn("storageEntrustedPledgeTransfer", "PoS node not exit ", sETransfer.Target)
			return currentSETransfer
		}
		if _, ok := snap.PosPledge[sETransfer.Address]; ok {
			log.Warn("storageEntrustedPledgeTransfer", "txSender is miner address", sETransfer.Address)
			return currentSETransfer
		}
		targetMiner := snap.findPosTargetMiner(txSender)
		nilAddr := common.Address{}
		if targetMiner != nilAddr && targetMiner != sETransfer.Target {
			log.Warn("storageEntrustedPledgeTransfer", "one address can only pledge one pos miner ", targetMiner)
			return currentSETransfer
		}
	} else if TargetTypeSp == sETransfer.TargetType {
		if err := sETransfer.TargetHash.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
			log.Warn("storageEntrustedPledgeTransfer", "Sp target Hash error", txDataInfo[postion])
			return currentSETransfer
		}
		if sp, ok := snap.SpData.PoolPledge[sETransfer.TargetHash]; !ok {
			log.Warn("storageEntrustedPledgeTransfer", "Sp target not exit ", sETransfer.Target)
			return currentSETransfer
		} else {
			if sp.Status != spStatusActive {
				log.Warn("storageEntrustedPledgeTransfer", "SP Status  is need active ", txSender)
				return currentSETransfer
			}
		}
		targetPool := snap.findSPTargetMiner(txSender)
		nilAddr := common.Hash{}
		if targetPool != nilAddr && targetPool != sETransfer.TargetHash {
			log.Warn("storageEntrustedPledgeTransfer", "one address can only pledge one pool ", targetPool)
			return currentSETransfer
		}
	} else if TargetTypeSn == sETransfer.TargetType {
		if _, ok := snap.StorageData.StoragePledge[sETransfer.Address]; ok {
			log.Warn("storageEntrustedPledgeTransfer", "txSender is Storage address", sETransfer.Address)
			return currentSETransfer
		}
		if err := sETransfer.Target.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
			log.Warn("storageEntrustedPledgeTransfer", "SN target Address error", txDataInfo[postion])
			return currentSETransfer
		}
		currBlockTranAmount := big.NewInt(0)
		for _, item := range currentSETransfer {
			if item.Target == sETransfer.Target && "SN" == item.TargetType {
				currBlockTranAmount = new(big.Int).Add(currBlockTranAmount, item.PledgeAmount)
			}
		}
		if snEtPledge, ok := snap.StorageData.StorageEntrust[sETransfer.Target]; ok {
			if snItem, ok1 := snap.StorageData.StoragePledge[sETransfer.Target]; ok1 {
				if snItem.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) != 0 {
					log.Warn("storageEntrustedPledgeTransfer", "Sn is not inactive", sETransfer.Target)
					return currentSETransfer
				}
				estimateAmountV1 := new(big.Int).Add(currBlockTranAmount, snEtPledge.PledgeAmount)
				if estimateAmountV1.Cmp(snItem.SpaceDeposit) >= 0 {
					log.Warn("storageEntrustedPledgeTransfer", "Sn entrusted pledge is full", txDataInfo[postion])
					return currentSETransfer
				}
				estimateAmountV2 := new(big.Int).Add(estimateAmountV1, sETransfer.PledgeAmount)
				lockAmount := big.NewInt(0)
				if estimateAmountV2.Cmp(snItem.SpaceDeposit) > 0 {
					lockAmount = new(big.Int).Sub(estimateAmountV2, snItem.SpaceDeposit)
					sETransfer.PledgeAmount = new(big.Int).Sub(sETransfer.PledgeAmount, lockAmount)
				}
				//SN deposit must be an integer   get non integer parts
				depositMode := new(big.Int).Mod(sETransfer.PledgeAmount, utgOneValue)
				// Add non integer parts to the lock compartment
				if depositMode.Cmp(big.NewInt(0)) > 0 {
					lockAmount = new(big.Int).Add(lockAmount, depositMode)
					sETransfer.PledgeAmount = new(big.Int).Sub(sETransfer.PledgeAmount, depositMode)
				}
				if lockAmount.Cmp(big.NewInt(0)) > 0 {
					sETransfer.LockAmount = lockAmount
				}
			}
		} else {
			log.Warn("storageEntrustedPledgeTransfer", "SN node not exit", sETransfer.Target)
			return currentSETransfer
		}
	} else {
		log.Warn("storageEntrustedPledgeTransfer", "TargetType is illegal", sETransfer.Target)
		return currentSETransfer
	}

	currentSETransfer = append(currentSETransfer, sETransfer)
	topics := make([]common.Hash, 3)
	//web3.sha3("SN transfer")
	topics[0].UnmarshalText([]byte("0xf68b680cedfcbf256334ac11f459a46ca138442b9d8a174a97bed31884eece4e"))
	topics[1].SetBytes(sETransfer.LockAmount.Bytes())
	topics[2].SetBytes(sETransfer.PledgeAmount.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	return currentSETransfer
}

func (s *Snapshot) updateSETransfer(entrustRecord []SETransferRecord, number *big.Int, db ethdb.Database) {
	if len(entrustRecord) == 0 {
		return
	}
	spCount := 0
	snCount := 0

	for _, record := range entrustRecord {
		if TargetTypePos == record.TargetType {
			if posItem, ok := s.PosPledge[record.Target]; ok {
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
				if targetSp.Manager == record.Address {
					targetSp.ManagerAmount = new(big.Int).Add(targetSp.ManagerAmount, record.PledgeAmount)
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
			if targetSn, ok := s.StorageData.StorageEntrust[record.Target]; ok {
				targetSn.PledgeAmount = new(big.Int).Add(targetSn.PledgeAmount, record.PledgeAmount)
				targetSn.Detail[record.PledgeHash] = &SEntrustDetail{
					Address: record.Address,
					Height:  new(big.Int).Set(number),
					Amount:  record.PledgeAmount,
				}
				if record.Address == targetSn.Manager {
					targetSn.ManagerAmount = new(big.Int).Add(targetSn.ManagerAmount, record.PledgeAmount)
					targetSn.Managerheight = new(big.Int).Set(number)
				}
				if stp, ok1 := s.StorageData.StoragePledge[record.Target]; ok1 {
					if targetSn.PledgeAmount.Cmp(stp.SpaceDeposit) >= 0 && stp.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
						s.StorageData.StoragePledge[record.Target].PledgeStatus = big.NewInt(SPledgeNormal)
						s.StorageData.accumulatePledgeHash(record.Target)
					}
				}
			}
			snCount++
		}
		if se, ok := s.StorageData.StorageEntrust[record.Original]; ok {
			delHash := make([]common.Hash, 0)
			for etHash, detail := range se.Detail {
				if record.Address == detail.Address {
					delHash = append(delHash, etHash)
				}
			}
			for _, removeHash := range delHash {
				delete(se.Detail, removeHash)
			}
			se.PledgeAmount = new(big.Int).Sub(se.PledgeAmount, new(big.Int).Add(record.LockAmount, record.PledgeAmount))
			if record.LockAmount.Cmp(common.Big0) > 0 {
				s.FlowRevenue.STPEntrustExitLock.updateSTPEntrustTransferLockData(s, record, number)
			}
			if stp, ok1 := s.StorageData.StoragePledge[record.Original]; ok1 {
				if se.PledgeAmount.Cmp(stp.SpaceDeposit) < 0 {
					if s.StorageData.StoragePledge[record.Original].PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) == 0 {
						s.StorageData.StoragePledge[record.Original].PledgeStatus = big.NewInt(SPledgeInactive)
						s.StorageData.accumulatePledgeHash(record.Original)
					}
				}
			}
		}
		if spCount > 0 {
			s.SpData.accumulateSpDataHash()
		}
		if snCount > 0 {

		}

	}
}

func (a *Alien) storageEntrustedPledgeExit(currentSEExit []SEExitRecord, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int, chain consensus.ChainHeaderReader) []SEExitRecord {
	if len(txDataInfo) <= 4 {
		log.Warn("storageEntrustedPledgeExit", "parameter number", len(txDataInfo))
		return currentSEExit
	}
	sEExit := SEExitRecord{
		Target:  common.Address{},
		Hash:    common.Hash{},
		Address: common.Address{},
		Amount:  common.Big0,
	}
	postion := 3
	if err := sEExit.Target.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageEntrustedPledgeExit", "miner address", txDataInfo[postion])
		return currentSEExit
	}
	postion++
	sEExit.Hash = common.HexToHash(txDataInfo[postion])

	if se, ok := snap.StorageData.StorageEntrust[sEExit.Target]; !ok {
		log.Warn("storageEntrustedPledgeExit", "StorageEntrust is not exist", sEExit.Target)
		return currentSEExit
	} else {
		if se.Manager == txSender {
			log.Warn("storageEntrustedPledgeExit", "txSender is Manager", sEExit.Target)
			return currentSEExit
		}
	}

	if _, ok := snap.StorageData.StorageEntrust[sEExit.Target].Detail[sEExit.Hash]; !ok {
		log.Warn("storageEntrustedPledgeExit", "Hash is not exist", sEExit.Hash)
		return currentSEExit
	} else {
		pledgeDetail := snap.StorageData.StorageEntrust[sEExit.Target].Detail[sEExit.Hash]
		if pledgeDetail.Address != txSender {
			log.Warn("storageEntrustedPledgeExit", "txSender is not right", txSender)
			return currentSEExit
		}
		pledgeMinBLock := a.getEntrustPledgeMinBLock(stpEntrustMinDay)
		pledgeBLock := new(big.Int).Sub(number, pledgeDetail.Height)
		if pledgeBLock.Cmp(pledgeMinBLock) < 0 {
			log.Warn("storageEntrustedPledgeExit", "Entrust hash not pass time", pledgeDetail.Height)
			return currentSEExit
		}
		sEExit.Address = pledgeDetail.Address
		sEExit.Amount = pledgeDetail.Amount
	}

	if isInCurrentSEExit(currentSEExit, sEExit.Hash) {
		log.Warn("storageEntrustedPledgeExit", "Hash is in currentSEExit", sEExit.Hash)
		return currentSEExit
	}

	topics := make([]common.Hash, 3)
	//web3.sha3("storageEntrustedPledgeExit")
	topics[0].UnmarshalText([]byte("0xc521ac477094ac2b7dcbd15657cf772d3ede04ec238fd38682f43decabb922bf"))
	topics[1].SetBytes(sEExit.Target.Bytes())
	topics[2].SetBytes(sEExit.Amount.Bytes())
	data := common.Hash{}
	data.SetBytes(sEExit.Hash.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, data.Bytes())
	currentSEExit = append(currentSEExit, sEExit)
	return currentSEExit
}

func isInCurrentSEExit(currentSEExit []SEExitRecord, hash common.Hash) bool {
	has := false
	for _, currentItem := range currentSEExit {
		if currentItem.Hash == hash {
			has = true
			break
		}
	}
	return has
}

func (s *Snapshot) updateSEExit(sEExitRecord []SEExitRecord, number *big.Int, db ethdb.Database) {
	if len(sEExitRecord) == 0 {
		return
	}
	for _, record := range sEExitRecord {
		if se, ok := s.StorageData.StorageEntrust[record.Target]; ok {
			delete(se.Detail, record.Hash)
			se.PledgeAmount = new(big.Int).Sub(se.PledgeAmount, record.Amount)
			if record.Amount.Cmp(common.Big0) > 0 {
				s.FlowRevenue.STPEntrustExitLock.updateSTPEExitLockData(s, record, number)
			}
			if stp, ok1 := s.StorageData.StoragePledge[record.Target]; ok1 {
				if se.PledgeAmount.Cmp(stp.SpaceDeposit) < 0 {
					if s.StorageData.StoragePledge[record.Target].PledgeStatus.Cmp(big.NewInt(SPledgeNormal)) == 0 {
						s.StorageData.StoragePledge[record.Target].PledgeStatus = big.NewInt(SPledgeInactive)
						s.StorageData.accumulatePledgeHash(record.Target)
					}
				}
			}
		}
	}
	s.StorageData.accumulateHeaderHash()
}

func (s *StorageData) dealSPledgeRevert4(pledge *SPledge, revertLockReward []SpaceRewardRecord, revertExchangeSRT []ExchangeSRTRecord, rate uint32, number uint64, blockPerday uint64, bAmount *big.Int, revenueStorage map[common.Address]*RevenueParameter, pledgeAddress common.Address, snap *Snapshot) ([]SpaceRewardRecord, []ExchangeSRTRecord, *big.Int) {
	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	beforeZeroTime = new(big.Int).Add(beforeZeroTime, common.Big1)
	maxFailNum := maxStgVerContinueDayFail * blockPerday
	bigMaxFailNum := new(big.Int).SetUint64(maxFailNum)
	revertDeposit := common.Big0
	var depositAddress common.Address
	if se, ok := s.StorageEntrust[pledgeAddress]; ok {
		depositAddress = se.Manager
		revertDeposit = new(big.Int).Set(se.ManagerAmount)
		if beforeZeroTime.Cmp(bigMaxFailNum) >= 0 {
			beforeSevenDayNumber := new(big.Int).Sub(beforeZeroTime, bigMaxFailNum)
			lastVerSuccTime := pledge.LastVerificationSuccessTime
			if lastVerSuccTime.Cmp(beforeSevenDayNumber) <= 0 {
				bAmount = new(big.Int).Add(bAmount, new(big.Int).Set(se.ManagerAmount))
				revertDeposit = big.NewInt(0)
				revenueAddress := common.Address{}
				if revenue, ok1 := revenueStorage[pledgeAddress]; ok1 {
					revenueAddress = revenue.RevenueAddress
				}
				for _, item := range se.Detail {
					if (item.Address == revenueAddress || item.Address == pledgeAddress) && item.Address != se.Manager {
						bAmount = new(big.Int).Add(bAmount, item.Amount)
					}
				}

				burnFlowReward := snap.FlowRevenue.FlowLock.calBurnSpIllegalReward(pledgeAddress, pledgeAddress, uint32(sscEnumFlwReward))
				burnBandwidthReward := snap.FlowRevenue.BandwidthLock.calBurnSpIllegalReward(pledgeAddress, pledgeAddress, uint32(sscEnumBandwidthReward))
				burnSTPEnRevenueReward := snap.FlowRevenue.STPEntrustLock.calBurnSpIllegalReward(revenueAddress, pledgeAddress, uint32(sscEnumSTEntrustLockReward))
				burnSTPEnManagerReward := common.Big0
				if revenueAddress != se.Manager {
					burnSTPEnManagerReward = snap.FlowRevenue.STPEntrustLock.calBurnSpIllegalReward(se.Manager, pledgeAddress, uint32(sscEnumSTEntrustLockReward))
				}
				if burnFlowReward.Cmp(common.Big0) > 0 {
					bAmount = new(big.Int).Add(bAmount, burnFlowReward)
				}
				if burnBandwidthReward.Cmp(common.Big0) > 0 {
					bAmount = new(big.Int).Add(bAmount, burnBandwidthReward)
				}
				if burnSTPEnRevenueReward.Cmp(common.Big0) > 0 {
					bAmount = new(big.Int).Add(bAmount, burnSTPEnRevenueReward)
				}
				if burnSTPEnManagerReward.Cmp(common.Big0) > 0 {
					bAmount = new(big.Int).Add(bAmount, burnSTPEnManagerReward)
				}
			}
		}
		if revertDeposit.Cmp(common.Big0) > 0 {
			revertLockReward = append(revertLockReward, SpaceRewardRecord{
				Target:  depositAddress,
				Amount:  revertDeposit,
				Revenue: depositAddress,
			})
		}
	}

	return revertLockReward, revertExchangeSRT, bAmount
}

func (s *StorageData) storageVerify3(number uint64, blockPerday uint64, revenueStorage map[common.Address]*RevenueParameter) ([]common.Address, []common.Hash, map[common.Address]*StorageRatio, map[common.Address]*big.Int) {
	sussSPAddrs := make([]common.Address, 0)
	sussRentHashs := make([]common.Hash, 0)
	storageRatios := make(map[common.Address]*StorageRatio, 0)
	capSuccAddrs := make(map[common.Address]*big.Int, 0)

	bigNumber := new(big.Int).SetUint64(number)
	bigblockPerDay := new(big.Int).SetUint64(blockPerday)
	zeroTime := new(big.Int).Mul(new(big.Int).Div(bigNumber, bigblockPerDay), bigblockPerDay) //0:00 every day
	beforeZeroTime := new(big.Int).Sub(zeroTime, bigblockPerDay)
	beforeZeroTime = new(big.Int).Add(beforeZeroTime, common.Big1)
	bigOne := big.NewInt(1)
	for pledgeAddr, sPledge := range s.StoragePledge {
		isSfVerSucc := true
		capSucc := big.NewInt(0)
		storagespaces := s.StoragePledge[pledgeAddr].StorageSpaces
		sfiles := storagespaces.StorageFile
		for _, sfile := range sfiles {
			lastVerSuccTime := sfile.LastVerificationSuccessTime
			if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
				isSfVerSucc = false
				sfile.ValidationFailureTotalTime = new(big.Int).Add(sfile.ValidationFailureTotalTime, bigOne)
				s.accumulateSpaceStorageFileHash(pledgeAddr, sfile)
			} else {
				capSucc = new(big.Int).Add(capSucc, sfile.Capacity)
			}
		}
		if isSfVerSucc {
			storagespaces.LastVerificationSuccessTime = beforeZeroTime
		} else {
			storagespaces.ValidationFailureTotalTime = new(big.Int).Add(storagespaces.ValidationFailureTotalTime, bigOne)
		}
		storagespaces.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
		leases := make(map[common.Hash]*Lease)
		for lhash, l := range sPledge.Lease {
			if l.Status == LeaseNormal || l.Status == LeaseBreach {
				leases[lhash] = l
			}
		}
		for lhash, lease := range leases {
			isVerSucc := true
			storageFile := lease.StorageFile
			for _, file := range storageFile {
				lastVerSuccTime := file.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeZeroTime) < 0 {
					isVerSucc = false
					file.ValidationFailureTotalTime = new(big.Int).Add(file.ValidationFailureTotalTime, bigOne)
					s.accumulateLeaseStorageFileHash(pledgeAddr, lhash, file)
				} else {
					capSucc = new(big.Int).Add(capSucc, file.Capacity)
				}
			}
			leaseLists := lease.LeaseList
			expireNumber := big.NewInt(0)
			for ldhash, leaseDetail := range leaseLists {
				deposit := leaseDetail.Deposit
				if deposit.Cmp(big.NewInt(0)) > 0 {
					startTime := leaseDetail.StartTime
					duration := leaseDetail.Duration
					leaseDetailEndNumber := new(big.Int).Add(startTime, new(big.Int).Mul(duration, new(big.Int).SetUint64(blockPerday)))
					if ldhash != lhash {
						leaseDetailEndNumber = new(big.Int).Sub(leaseDetailEndNumber, common.Big1)
					}
					if startTime.Cmp(beforeZeroTime) <= 0 && leaseDetailEndNumber.Cmp(beforeZeroTime) >= 0 {
						if !isVerSucc {
							leaseDetail.ValidationFailureTotalTime = new(big.Int).Add(leaseDetail.ValidationFailureTotalTime, bigOne)
							s.accumulateLeaseDetailHash(pledgeAddr, lhash, leaseDetail)
						}
					}
					if expireNumber.Cmp(leaseDetailEndNumber) < 0 {
						expireNumber = leaseDetailEndNumber
					}
				}
			}
			if expireNumber.Cmp(bigNumber) <= 0 {
				lease.Status = LeaseExpiration
			}
			//cal ROOT HASH

			if isVerSucc {
				lease.LastVerificationSuccessTime = beforeZeroTime
				sussRentHashs = append(sussRentHashs, lhash)
				if lease.Status == LeaseBreach {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if lease.ValidationFailureTotalTime.Cmp(duration10) < 0 {
						lease.Status = LeaseNormal
					}
				}
			} else {
				lease.ValidationFailureTotalTime = new(big.Int).Add(lease.ValidationFailureTotalTime, bigOne)
				if lease.Status == LeaseNormal {
					duration10 := new(big.Int).Mul(lease.Duration, big.NewInt(rentFailToRescind))
					duration10 = new(big.Int).Div(duration10, big.NewInt(100))
					if isGTIncentiveEffect(number) {
						if lease.ValidationFailureTotalTime.Cmp(duration10) >= 0 {
							lease.Status = LeaseBreach
						}
					} else {
						if lease.ValidationFailureTotalTime.Cmp(duration10) > 0 {
							lease.Status = LeaseBreach
						}
					}
				}
			}
			lease.LastVerificationTime = beforeZeroTime
			s.accumulateLeaseHash(pledgeAddr, lease)
		}

		isPledgeVerSucc := false
		cap80 := new(big.Int).Mul(capSucNeedPer, sPledge.TotalCapacity)
		cap80 = new(big.Int).Div(cap80, big.NewInt(100))
		if capSucc.Cmp(cap80) > 0 {
			isPledgeVerSucc = true
		}
		if isPledgeVerSucc {
			sussSPAddrs = append(sussSPAddrs, pledgeAddr)
			if _, ok3 := capSuccAddrs[pledgeAddr]; !ok3 {
				capSuccAddrs[pledgeAddr] = capSucc
			}
			sPledge.LastVerificationSuccessTime = beforeZeroTime
		} else {
			sPledge.ValidationFailureTotalTime = new(big.Int).Add(sPledge.ValidationFailureTotalTime, bigOne)
			maxFailNum := maxStgVerContinueDayFail * blockPerday
			bigMaxFailNum := new(big.Int).SetUint64(maxFailNum)
			if beforeZeroTime.Cmp(bigMaxFailNum) >= 0 {
				beforeSevenDayNumber := new(big.Int).Sub(beforeZeroTime, bigMaxFailNum)
				lastVerSuccTime := sPledge.LastVerificationSuccessTime
				if lastVerSuccTime.Cmp(beforeSevenDayNumber) <= 0 {
					sPledge.PledgeStatus = big.NewInt(SPledgeRemoving)
				}
			}
		}
		sPledge.LastVerificationTime = beforeZeroTime
		s.accumulateSpaceHash(pledgeAddr)
	}
	//cal ROOT HASH
	s.accumulateHeaderHash()
	return sussSPAddrs, sussRentHashs, storageRatios, capSuccAddrs
}

func (s *StorageData) accumulateLeaseRewards3(ratios map[common.Address]*StorageRatio, addrs []common.Hash, basePrice *big.Int, revenueStorage map[common.Address]*RevenueParameter, blocknumber uint64, db ethdb.Database, snapTotalLeaseSpace *big.Int, spData *SpData, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int, *big.Int) {
	var LockReward []SpaceRewardRecord
	//basePrice := // SRT /TB.day
	storageHarvest := common.Big0
	feeBurnAmount := common.Big0
	if nil == addrs || len(addrs) == 0 {
		return LockReward, storageHarvest, nil, nil
	}
	validSuccLesae := make(map[common.Hash]uint64)
	for _, leaseHash := range addrs {
		validSuccLesae[leaseHash] = 1
	}
	decimalBasePrice := decimal.NewFromBigInt(basePrice, 0)
	totalLeaseSpace := s.getTotalLeaseSpace2(validSuccLesae, blocknumber, decimalBasePrice, spData)
	err := s.saveDecimalValueTodb(totalLeaseSpace, db, blocknumber, totalLeaseSpaceKey)
	if err != nil {
		log.Error("saveTotalLeaseSpace", "err", err, "number", blocknumber)
	}
	AddTotalLeaseSpace := totalLeaseSpace.Add(decimal.NewFromBigInt(snapTotalLeaseSpace, 0))
	for pledgeAddr, storage := range s.StoragePledge {
		if storage.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
			continue
		}
		totalReward := big.NewInt(0)
		for leaseHash, lease := range storage.Lease {
			if _, ok2 := validSuccLesae[leaseHash]; ok2 {
				leaseCapacity := decimal.NewFromBigInt(lease.Capacity, 0).Div(decimal.NewFromInt(1073741824)) //to GB
				var storageIndex decimal.Decimal
				if se, ok3 := s.StorageEntrust[pledgeAddr]; ok3 {
					if sp, ok4 := spData.PoolPledge[se.Sphash]; ok4 && sp.Status == spStatusActive {
						storageIndex = decimal.NewFromBigInt(sp.SnRatio, 0).Div(SnDefaultRatioDigit)
					} else {
						storageIndex = s.calStorageRatio(storage.TotalCapacity, blocknumber)
					}
				} else {
					storageIndex = s.calStorageRatio(storage.TotalCapacity, blocknumber)
				}
				bandwidthIndex := getBandwaith(storage.Bandwidth, blocknumber)
				reward := s.calStorageLeaseNewReward2(leaseCapacity, bandwidthIndex, storageIndex, decimal.NewFromBigInt(lease.UnitPrice, 0), decimalBasePrice, AddTotalLeaseSpace)
				totalReward = new(big.Int).Add(totalReward, reward.BigInt())
			}
		}
		if totalReward.Cmp(big.NewInt(0)) > 0 {
			revenueAddress := pledgeAddr
			if revenue, ok := revenueStorage[pledgeAddr]; ok {
				revenueAddress = revenue.RevenueAddress
			} else {
				if se, ok3 := s.StorageEntrust[pledgeAddr]; ok3 {
					revenueAddress = se.Manager
				}
			}
			LockReward = append(LockReward, SpaceRewardRecord{
				Target:  pledgeAddr,
				Amount:  totalReward,
				Revenue: revenueAddress,
			})
			storageHarvest = new(big.Int).Add(storageHarvest, totalReward)
			feeBurnAmount = s.getFeeBurnAmount(pledgeAddr, snap, totalReward, feeBurnAmount)
		}
	}
	err = s.saveTotalValueTodb(storageHarvest, db, blocknumber, leaseHarvestKey)
	if err != nil {
		log.Error("saveleaseHarvest", "err", err, "number", blocknumber)
	}
	return LockReward, storageHarvest, totalLeaseSpace.BigInt(), feeBurnAmount
}

func (s *StorageData) calcStoragePledgeReward4(ratios map[common.Address]*StorageRatio, revenueStorage map[common.Address]*RevenueParameter, number uint64, period uint64, sussSPAddrs []common.Address, capSuccAddrs map[common.Address]*big.Int, db ethdb.Database, snap *Snapshot) ([]SpaceRewardRecord, *big.Int, *big.Int) {
	reward := make([]SpaceRewardRecord, 0)
	storageHarvest := big.NewInt(0)
	leftAmount := common.Big0
	validSuccSPAddrs := make(map[common.Address]uint64)
	for _, sPAddrs := range sussSPAddrs {
		validSuccSPAddrs[sPAddrs] = 1
	}
	for pledgeAddr, sPledge := range s.StoragePledge {
		if _, ok := validSuccSPAddrs[pledgeAddr]; !ok {
			continue
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeInactive)) == 0 {
			continue
		}
		if capSucc, ok3 := capSuccAddrs[pledgeAddr]; ok3 {
			if capSucc.Cmp(common.Big0) > 0 {
				if s.isSPledgeIncentivePeriod(sPledge.Number, number, period) {
					if s.isSPledgeFrontIncentivePeriod(sPledge.Number, number, period) || s.isSPledgeRentalThreshold(sPledge) {
						apr := getApr(sPledge.Number, period)
						pledgeReward := decimal.NewFromBigInt(sPledge.SpaceDeposit, 0).Mul(apr).Div(decimal.NewFromInt(365))
						pledgeRewardBigInt := pledgeReward.BigInt()
						if pledgeRewardBigInt.Cmp(common.Big0) > 0 {
							revenueAddress := pledgeAddr
							if revenue, ok := revenueStorage[pledgeAddr]; ok {
								revenueAddress = revenue.RevenueAddress
							} else {
								if se, ok4 := s.StorageEntrust[pledgeAddr]; ok4 {
									revenueAddress = se.Manager
								}
							}
							reward = append(reward, SpaceRewardRecord{
								Target:  pledgeAddr,
								Amount:  pledgeRewardBigInt,
								Revenue: revenueAddress,
							})
							storageHarvest = new(big.Int).Add(storageHarvest, pledgeRewardBigInt)
							leftAmount = s.getFeeBurnAmount(pledgeAddr, snap, pledgeRewardBigInt, leftAmount)
						}
					}
				}
			}
		}

	}
	return reward, storageHarvest, leftAmount
}

func (s *StorageData) calDealLeaseStatus2(number uint64, snap *Snapshot, db ethdb.Database, header *types.Header, revenueStorage map[common.Address]*RevenueParameter) {
	delPledge := make([]common.Address, 0)
	removePledge := make([]common.Address, 0)
	normalExitPledge := make([]common.Address, 0)
	for pledgeAddress, sPledge := range s.StoragePledge {
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 {
			removePledge = append(removePledge, pledgeAddress)
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
			normalExitPledge = append(normalExitPledge, pledgeAddress)
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRetrun)) == 0 {
			continue
		}
		if sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeRemoving)) == 0 || sPledge.PledgeStatus.Cmp(big.NewInt(SPledgeExit)) == 0 {
			sPledge.PledgeStatus = big.NewInt(SPledgeRetrun)
			delPledge = append(delPledge, pledgeAddress)
			s.accumulateSpaceHash(pledgeAddress)
			continue
		}

		leases := sPledge.Lease
		for _, lease := range leases {
			if lease.Status == LeaseReturn {
				continue
			}
			if lease.Status == LeaseUserRescind || lease.Status == LeaseExpiration {
				lease.Status = LeaseReturn
				s.accumulateLeaseHash(pledgeAddress, lease)
			}
		}
	}

	if len(removePledge) > 0 {
		burnSTPMap := make(map[common.Address]map[common.Address]uint64, 0)
		for _, pledgeAddress := range removePledge {
			revenueAddress := common.Address{}
			if revenue, ok1 := revenueStorage[pledgeAddress]; ok1 {
				revenueAddress = revenue.RevenueAddress
				if _, ok := burnSTPMap[revenueAddress]; !ok {
					burnSTPMap[revenueAddress] = make(map[common.Address]uint64, 0)
				}
				burnSTPMap[revenueAddress][pledgeAddress] = uint64(1)
			}
			if se, ok2 := s.StorageEntrust[pledgeAddress]; ok2 {
				if _, ok := burnSTPMap[se.Manager]; !ok {
					burnSTPMap[se.Manager] = make(map[common.Address]uint64, 0)
				}
				burnSTPMap[se.Manager][pledgeAddress] = uint64(1)
				for _, item := range se.Detail {
					if item.Address != revenueAddress && item.Address != se.Manager && item.Address != pledgeAddress {
						snap.FlowRevenue.STPEntrustExitLock.updateSTPExitLockData(snap, item, new(big.Int).SetUint64(number), pledgeAddress)
					}
				}
			}
		}
		err := snap.FlowRevenue.STPEntrustLock.setSpIllegalLockPunish(burnSTPMap, db, header.Hash(), header.Number.Uint64(), uint32(sscEnumSTEntrustLockReward))
		if err != nil {
			log.Warn("setSpIllegalLockPunish STPEntrustLock Error", "err", err)
		}
		snap.setStorageRemovePunish2(removePledge, number, db, header)
	}
	if len(normalExitPledge) > 0 {
		for _, pledgeAddress := range normalExitPledge {
			if se, ok2 := s.StorageEntrust[pledgeAddress]; ok2 {
				detail := se.Detail
				for _, item := range detail {
					if item.Address != se.Manager {
						snap.FlowRevenue.STPEntrustExitLock.updateSTPExitLockData(snap, item, new(big.Int).SetUint64(number), pledgeAddress)
					}
				}
			}
		}
	}
	for _, delAddr := range delPledge {
		s.deleteSpCapAndRs(delAddr, snap)
		delete(s.StoragePledge, delAddr)
	}
	snap.SpData.accumulateSpDataHash()
	s.accumulateHeaderHash()
	return
}

func (a *Alien) storageExitPool(currentExitPool []common.Address, txDataInfo []string, txSender common.Address, tx *types.Transaction, receipts []*types.Receipt, state *state.StateDB, snap *Snapshot, number *big.Int) []common.Address {
	if len(txDataInfo) <= 3 {
		log.Warn("storageExitPool", "parameter number", len(txDataInfo))
		return currentExitPool
	}
	postion := 3
	var target common.Address
	if err := target.UnmarshalText1([]byte(txDataInfo[postion])); err != nil {
		log.Warn("storageExitPool", "Pledge", txDataInfo[postion])
		return currentExitPool
	}
	nilHash := common.Hash{}
	if _, ok := snap.StorageData.StorageEntrust[target]; ok {
		if snap.StorageData.StorageEntrust[target].Manager != txSender {
			log.Warn("storageExitPool", "txSender is not manager", txSender)
			return currentExitPool
		}
		if snap.StorageData.StorageEntrust[target].Sphash == nilHash {
			log.Warn("storageExitPool", "Sphash is nilHash", target)
			return currentExitPool
		}
	} else {
		log.Warn("storageExitPool", "manager is empty", target)
		return currentExitPool
	}

	if _, ok := snap.StorageData.StorageEntrust[target]; ok {
		if snap.StorageData.StorageEntrust[target].Sphash != nilHash {
			spheight := snap.StorageData.StorageEntrust[target].Spheight
			if number.Uint64()-spheight.Uint64() <= sPPoollockDay*snap.getBlockPreDay() {
				log.Warn("storageExitPool", "sPPoollockDay not pass", target)
				return currentExitPool
			}
		}
	} else {
		log.Warn("storageExitPool", "StoragePledge is empty", target)
		return currentExitPool
	}
	if isInCurrentExitPool(currentExitPool, target) {
		log.Warn("storageExitPool", "target is in currentExitPool", target)
		return currentExitPool
	}
	topics := make([]common.Hash, 2)
	//web3.sha3("Exit Storage Pool")
	topics[0].UnmarshalText([]byte("0xcf0eb0e1ba306c396193c82bbba8de529adf625b69730027078c6aedc8264ef0"))
	topics[1].SetBytes(target.Bytes())
	a.addCustomerTxLog(tx, receipts, topics, nil)
	currentExitPool = append(currentExitPool, target)
	return currentExitPool
}

func isInCurrentExitPool(currentExitPool []common.Address, target common.Address) bool {
	has := false
	for _, currentItem := range currentExitPool {
		if currentItem == target {
			has = true
			break
		}
	}
	return has
}

func (s *Snapshot) updateSPEPool(sPEPoolRecord []common.Address, number *big.Int, db ethdb.Database) {
	if sPEPoolRecord == nil || len(sPEPoolRecord) == 0 {
		return
	}
	for _, target := range sPEPoolRecord {
		if _, ok := s.StorageData.StoragePledge[target]; ok {
			total := new(big.Int).Set(s.StorageData.StoragePledge[target].TotalCapacity)
			if _, ok2 := s.StorageData.StorageEntrust[target]; ok2 {
				oldSphash := s.StorageData.StorageEntrust[target].Sphash
				s.StorageData.StorageEntrust[target].Sphash = common.Hash{}
				s.StorageData.StorageEntrust[target].Spheight = common.Big0
				if _, ok3 := s.SpData.PoolPledge[oldSphash]; ok3 {
					s.SpData.PoolPledge[oldSphash].UsedCapacity = new(big.Int).Sub(s.SpData.PoolPledge[oldSphash].UsedCapacity, total)
					s.SpData.accumulateSpPledgelHash(oldSphash, false)
				}
			}
		}
	}
	s.SpData.accumulateSpDataHash()
}

func isInCurrentSETransfer(currentTransfer []SETransferRecord, txSender common.Address) bool {
	has := false
	for _, currentItem := range currentTransfer {
		if currentItem.Address == txSender {
			has = true
			break
		}
	}
	return has
}

func (s *StorageData) getTotalLeaseSpace2(validSuccLesae map[common.Hash]uint64, blocknumber uint64, basePrice decimal.Decimal, spData *SpData) decimal.Decimal {
	totalLeaseSpace := decimal.NewFromInt(0) //B
	for pledgeAddr, storage := range s.StoragePledge {
		for leaseHash, lease := range storage.Lease {
			if _, ok2 := validSuccLesae[leaseHash]; ok2 {
				capacity := decimal.NewFromBigInt(lease.Capacity, 0)
				var storageIndex decimal.Decimal
				if se, ok3 := s.StorageEntrust[pledgeAddr]; ok3 {
					if sp, ok4 := spData.PoolPledge[se.Sphash]; ok4 {
						storageIndex = decimal.NewFromBigInt(sp.SnRatio, 0).Div(SnDefaultRatioDigit)
					} else {
						storageIndex = s.calStorageRatio(storage.TotalCapacity, blocknumber)
					}
				} else {
					storageIndex = s.calStorageRatio(storage.TotalCapacity, blocknumber)
				}
				bandwidthIndex := getBandwaith(storage.Bandwidth, blocknumber)
				rentPrice := decimal.NewFromBigInt(lease.UnitPrice, 0)
				priceIndex, priceRate := s.getPriceIndex(rentPrice, basePrice)
				calCapacity := s.getRegulate(capacity, bandwidthIndex, storageIndex, priceRate, priceIndex)
				totalLeaseSpace = totalLeaseSpace.Add(calCapacity)
			}
		}
	}
	return totalLeaseSpace
}

func (s *StorageData) getFeeBurnAmount(pledgeAddr common.Address, snap *Snapshot, pledgeRewardBigInt *big.Int, leftAmount *big.Int) *big.Int {
	if se, ok5 := s.StorageEntrust[pledgeAddr]; ok5 {
		if sp, ok6 := snap.SpData.PoolPledge[se.Sphash]; ok6 {
			if sp.SnRatio.Cmp(common.Big0) <= 0 && sp.Status == spStatusActive {
				stpAmount := new(big.Int).Set(pledgeRewardBigInt)
				spFeeAmount := new(big.Int).Mul(stpAmount, new(big.Int).SetUint64(sp.Fee))
				spFeeAmount = new(big.Int).Mul(spFeeAmount, big.NewInt(100))
				leftAmount = new(big.Int).Add(leftAmount, spFeeAmount)
			}
		}
	}
	return leftAmount
}

func (s *Snapshot) setStorageRemovePunish2(pledge []common.Address, number uint64, db ethdb.Database, header *types.Header) {
	err := s.FlowRevenue.BandwidthLock.setStorageRemovePunish2(pledge, db, header.Hash(), header.Number.Uint64(), uint32(sscEnumBandwidthReward))
	if err != nil {
		log.Warn("setStorageRemovePunish BandwidthLock Error", "err", err)
	}
	err = s.FlowRevenue.FlowLock.setStorageRemovePunish2(pledge, db, header.Hash(), header.Number.Uint64(), uint32(sscEnumFlwReward))
	if err != nil {
		log.Warn("setStorageRemovePunish FlowLock Error", "err", err)
	}
}

func (s *StorageData) subSpoolUsedCapacity(snap *Snapshot, se *SEntrust, pledgeAddress common.Address) {
	if sp, ok3 := snap.SpData.PoolPledge[se.Sphash]; ok3 {
		if stp, ok4 := s.StoragePledge[pledgeAddress]; ok4 {
			if sp.UsedCapacity.Cmp(stp.TotalCapacity) > 0 {
				sp.UsedCapacity = new(big.Int).Sub(sp.UsedCapacity, stp.TotalCapacity)
			} else {
				sp.UsedCapacity = common.Big0
			}
			snap.SpData.accumulateSpPledgelHash(se.Sphash, false)
		}
	}
}

func (s *StorageData) deleteSpCapAndRs(delAddr common.Address, snap *Snapshot) {
	if se, ok1 := s.StorageEntrust[delAddr]; ok1 {
		s.subSpoolUsedCapacity(snap, se, delAddr)
		delete(s.StorageEntrust, delAddr)
	}
	if _, ok2 := snap.RevenueStorage[delAddr]; ok2 {
		delete(snap.RevenueStorage, delAddr)
	}
}
