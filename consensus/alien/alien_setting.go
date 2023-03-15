package alien

import "math/big"

const (
	checkpointInterval = 360              //360        // About N hours if config.period is N

	secondsPerDay                    = 24 * 60 * 60 // Number of seconds for one day
	accumulateFlowRewardInterval     = 2 * 60 * 60  // accumulate flow reward interval every day
	accumulateBandwithRewardInterval = 1 * 60 * 60             // accumulate flow reward interval every day

	paySignerRewardInterval = 0 // pay singer reward  interval every day
	payFlowRewardInterval      = 2*60*60 + 30*60 //  pay flow reward  interval every day
	payBandwidthRewardInterval = 1 * 60 * 60 + 30*60  //  pay bandwidth reward  interval every day

	signerPledgeLockParamPeriod    = 180 * 24 * 60 * 60
	signerPledgeLockParamRlsPeriod = 0
	signerPledgeLockParamInterval  = 0

	flowPledgeLockParamPeriod    = 180 * 24 * 60 * 60
	flowPledgeLockParamRlsPeriod = 0
	flowPledgeLockParamInterval  = 0

	rewardLockParamPeriod    = 30 * 24 * 60 * 60
	rewardLockParamRlsPeriod = 180 * 24 * 60 * 60
	rewardLockParamInterval  = 24 * 60 * 60
	maxCandidateMiner = 500  //	The maximum number of candidate nodes participating in each election is 500
	electionPartitionThreshold = 36 //Election partition threshold
	signFixBlockNumber = 21
	grantProfitOneTimeBlockNumber = 30
	lockSimplifyEffectBlocknumber = 38

	lockMergeNumber = 44
	tallyRevenueEffectBlockNumber = 48
	SigerQueueFixBlockNumber = 53
	SigerElectNewEffectBlockNumber = 66
	MinerUpdateStateFixBlockNumber = 75
	TallyPunishdProcessEffectBlockNumber = 84
	TallyPunishdFixBlockNumber = 92
	StorageEffectBlockNumber = 101
	//storage
	storageVerificationCheck    = 1 * 60 * 60   //return funds where contract expiration
	rentRenewalExpires=50
	rentFailToRescind=10
	maxStgVerContinueDayFail      =3    //storage Verification failed and failed for 7 consecutive days

	SPledgeRevertFixBlockNumber = 122
	AdjustSPRBlockNumber = 132 //Adjust calc StoragePledgeReward
	CompareGrantProfitHash=false
	storageVerifyNewEffectNumber = 147
	storagePledgeTmpVerifyEffectNumber = 154
	StorageChBwEffectNumber = 184
	storagePledgeTmpVerifyEffectNumberV2 = 214
	PledgeRevertLockEffectNumber = 254
	payPOSPGRedeemInterval = 1 * 60 * 60 + 40*60  //  pay bandwidth reward  interval every day
	StoragePledgeOptEffectNumber = 289
	FixLeaseCapacityNumber = 300
	PosrIncentiveEffectNumber = 321
	PosrExitNewRuleEffectNumber = 332
	PosrNewCalEffectNumber=342
	PosNewEffectNumber= 352
	payPOSExitInterval = 1 * 60 * 60 + 50*60  //  pay bandwidth reward  interval every day
	checkPOSAutoExit= 1 * 60 * 60 + 60*60
	PosLastPunishFixNumber= 362
	PosAutoExitPunishChangeNumber=372
	GrantEffectNumber =154520
)

var (
	minCndPledgeBalance = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(20)) // candidate pledge balance
	minSignerLockBalance    = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(0)) // signer reward lock balance
	minFlwLockBalance       = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(0)) // flow reward lock balance
	minBandwidthLockBalance = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(0)) // bandwidth reward lock balance
	baseStoragePrice = new(big.Int).Mul(big.NewInt(1e+14), big.NewInt(5))
	clearSignNumberPerid = uint64(60480)
	storagePledgeIndex    = big.NewInt(1)
	defaultLeaseExpires=big.NewInt(1)
	minimumRentDay=big.NewInt(30)
	novalidPktime=uint64(7)
	novalidVfPktime = uint64(30)
	maximumRentDay=big.NewInt(360)
	posCommitPeriod=big.NewInt(365+365/2) //1.5 year
	posBeyondCommitPeriod=big.NewInt(30) //30 day
	posWithinCommitPeriod=big.NewInt(30) //30 day
	posMaxMainCandidateNum = 11
	posCandidateAvgRate = big.NewInt(70) //70%
	minCndEntrustPledgeBalance = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(1))
	maxPosContinueDayFail      =uint64(30)
	posDistributionDefaultRate =big.NewInt(10000)
	//How many days to keep snapshots
	retainedLastSnapshot=uint64(100)*(secondsPerDay/defaultBlockPeriod)
)

func (a *Alien) blockPerDay() uint64 {
	return secondsPerDay / a.config.Period
}

func (a *Alien) blockAccumulateFlowRewardInterval() uint64 {
	return accumulateFlowRewardInterval / a.config.Period
}

func (a *Alien) blockAccumulateBandwithRewardInterval() uint64 {
	return accumulateBandwithRewardInterval / a.config.Period
}

func (a *Alien) blockPaySignerRewardInterval() uint64 {
	return paySignerRewardInterval / a.config.Period
}

func (a *Alien) blockPayFlowRewardInterval() uint64 {
	return payFlowRewardInterval / a.config.Period
}

func (a *Alien) isAccumulateFlowRewards(number uint64) bool {
	block := a.blockAccumulateFlowRewardInterval()
	heigtPerDay := a.blockPerDay()
	return block == number%heigtPerDay && block != number
}

func (a *Alien) isAccumulateBandWidthRewards(number uint64) bool {
	block := a.blockAccumulateBandwithRewardInterval()
	blockPerDay :=  a.blockPerDay()
	return block == number%blockPerDay && block != number
}

func isPayBandWidthRewards(number uint64, period uint64) bool {
	block := payBandwidthRewardInterval / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}

func isPayFlowRewards(number uint64, period uint64) bool {
	block := payFlowRewardInterval / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}
func isPaySignerRewards(number uint64, period uint64) bool {
	block := paySignerRewardInterval / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}
func  islockSimplifyEffectBlocknumber(number uint64) bool {
	return number>=lockSimplifyEffectBlocknumber
}

func isStorageVerificationCheck(number uint64, period uint64) bool {
	block := storageVerificationCheck / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}
func isPayPosPledgeExit(number uint64, period uint64) bool {
	if number < PledgeRevertLockEffectNumber {
		return false
	}
	block := payPOSPGRedeemInterval / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}

func isPayPosExit(number uint64, period uint64) bool {
	if number < PosNewEffectNumber {
		return false
	}
	block := payPOSExitInterval / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}

func isCheckPOSAutoExit(number uint64, period uint64) bool {
	if number < PosNewEffectNumber {
		return false
	}
	block := checkPOSAutoExit / period
	blockPerDay := secondsPerDay / period
	return block == number%blockPerDay && block != number
}

func  (a *Alien) notVerifyPkHeader(number uint64) bool{
	r1:=	number >=storagePledgeTmpVerifyEffectNumber && number <=storagePledgeTmpVerifyEffectNumber+a.blockPerDay()*novalidPktime
	r2:= number >=storagePledgeTmpVerifyEffectNumberV2 && number <=storagePledgeTmpVerifyEffectNumberV2+a.blockPerDay()*novalidVfPktime
	return r1 || r2
}
func (a *Alien) isEffectPayPledge(number uint64) bool{
	return number>= StoragePledgeOptEffectNumber && number <= StoragePledgeOptEffectNumber+BandwidthMakeupPunishDay*a.blockPerDay()
}
func (a *Alien) changeBandwidthEnable(number uint64) bool{
	r1:= number >= StorageChBwEffectNumber && number < StoragePledgeOptEffectNumber
	r2:= number >= PosrIncentiveEffectNumber
	return r1 || r2
}
func isGTIncentiveEffect(number uint64) bool{
	return number> PosrIncentiveEffectNumber
}

func isFixLeaseCapacity(number uint64) bool{
	return number ==FixLeaseCapacityNumber
}

func isGTPOSRNewCalEffect(number uint64) bool{
	return number >PosrNewCalEffectNumber
}
func isGEPOSNewEffect(number uint64) bool{
	return number >=PosNewEffectNumber
}

func isLtPosAutoExitPunishChange(number uint64) bool{
	return number <PosAutoExitPunishChangeNumber
}

func isGEPosAutoExitPunishChange(number uint64) bool{
	return number >=PosAutoExitPunishChangeNumber
}

func isLtGrantEffectNumber(number uint64) bool{
	return number < GrantEffectNumber
}

func isGEGrantEffectNumber(number uint64) bool{
	return number >= GrantEffectNumber
}