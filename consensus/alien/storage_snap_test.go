package alien

import (
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/params"
	"github.com/shopspring/decimal"
	"math/big"
	"testing"
)

func TestAlien_calStPledgeAmount(t *testing.T) {
	tests := []struct {
		PledgeCapacity decimal.Decimal
		FlowHarvest    *big.Int
		total          decimal.Decimal
		blockNumPer    *big.Int
		targetAmount   *big.Int
	}{
		{
			PledgeCapacity: decimal.NewFromInt(1234),
			FlowHarvest:    big.NewInt(222222222222222),
			total:          decimal.NewFromInt(22),
			blockNumPer:    big.NewInt(0),
			targetAmount:   big.NewInt(1402895577),
		},
		{
			PledgeCapacity: decimal.NewFromInt(1099511627776),
			FlowHarvest:    big.NewInt(222222222222222),
			total:          decimal.NewFromInt(22),
			blockNumPer:    big.NewInt(0),
			targetAmount:   big.NewInt(1250000000000000000),
		},
		{
			PledgeCapacity: decimal.NewFromInt(1099511627776),
			FlowHarvest:    big.NewInt(10995116277760000),
			total:          decimal.NewFromInt(1099511627776),
			blockNumPer:    big.NewInt(3153601),
			targetAmount:   new(big.Int).Mul(big.NewInt(1e+3), big.NewInt(1099511627776)),
		},
		{
			PledgeCapacity: decimal.NewFromInt(109951169277760000),
			FlowHarvest:    big.NewInt(10995116277766),
			total:          decimal.NewFromInt(1099511627776),
			blockNumPer:    big.NewInt(3153601),
			targetAmount:   new(big.Int).Mul(big.NewInt(1e+4), big.NewInt(10995116927782)),
		},
		{
			PledgeCapacity: decimal.NewFromInt(109951160),
			FlowHarvest:    big.NewInt(10995116277766),
			total:          decimal.NewFromInt(1099511627776),
			blockNumPer:    big.NewInt(3153601),
			targetAmount:   big.NewInt(109951160),
		},
	}
	for _, tt := range tests {
		pledgeCapacity := tt.PledgeCapacity
		snap := &Snapshot{
			FlowHarvest: tt.FlowHarvest,
			SystemConfig: SystemParameter{
				Deposit: make(map[uint32]*big.Int),
			},
			config: &params.AlienConfig{
				Period: uint64(10),
			},
		}
		snap.SystemConfig.Deposit[sscEnumPStoragePledgeID] = common.Big1
		total := tt.total
		blockNumPer := tt.blockNumPer
		targetAmount := tt.targetAmount
		amount := calStPledgeAmount(pledgeCapacity, snap, total, blockNumPer)
		cutMax := big.NewInt(200)
		cut := big.NewInt(200)
		if amount.Cmp(targetAmount) >= 0 {
			cut = new(big.Int).Sub(amount, targetAmount)
		} else {
			cut = new(big.Int).Sub(targetAmount, amount)
		}
		t.Logf(" cut %v", cut)
		if cut.Cmp(cutMax) > 0 {
			t.Errorf("cal %v targetAmount %v  ", amount, targetAmount)
		} else {
			t.Logf(" pass")
		}
	}
}
func Test_PledgeRewardAcal(t *testing.T) {
	val, _ := decimal.NewFromString("5934642403197129041095")
	val1, _ := decimal.NewFromString("4710328796945103400468")
	val2, _ := decimal.NewFromString("3738590443693380592138")

	tests := []struct {
		StorageCapacity *big.Int
		Price           *big.Int
		blocknumber     *big.Int
		bandwidth       *big.Int
		rewardNumber    *big.Int
		expectValue     *big.Int
		expectValue1    *big.Int
		expectValue2    *big.Int
	}{
		{
			StorageCapacity: big.NewInt(int64(1099511627776)),
			Price:           new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:     big.NewInt(30),
			bandwidth:       big.NewInt(300),
			rewardNumber:    big.NewInt(8750),
			expectValue:     val.BigInt(),
		}, {
			StorageCapacity: big.NewInt(int64(1099511627776)),
			Price:           new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:     big.NewInt(365*8640 + 100),
			bandwidth:       big.NewInt(300),
			rewardNumber:    big.NewInt(365*8640 + 100 + 8750),
			expectValue:     val1.BigInt(),
		}, {
			StorageCapacity: big.NewInt(int64(1099511627776)),
			Price:           new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:     big.NewInt(365*8640 + 365*8640 + 100),
			bandwidth:       big.NewInt(300),
			rewardNumber:    big.NewInt(365*8640 + 365*8640 + 100 + 8750),
			expectValue:     val2.BigInt(),
		},
	}
	snap := &Snapshot{
		StorageData:    NewStorageSnap(),
		RevenueStorage: make(map[common.Address]*RevenueParameter),
		Period:         10,
	}
	for _, tt := range tests {
		storageRatios := make(map[common.Address]*StorageRatio)
		sussSPAddrs := make([]common.Address, 0)
		pledgeAddrs := make([]common.Address, 0)
		pledgeAddrs = append(pledgeAddrs, common.HexToAddress("ux1eb90474374f2ea4deb3961cdd0a391821c0321b"))
		pledgeAddrs = append(pledgeAddrs, common.HexToAddress("ux1eb90474374f2ea4deb3961cdd0a391821c0321w"))
		storageRatios, sussSPAddrs = snap.addPledge(pledgeAddrs, tt.StorageCapacity, tt.Price, tt.blocknumber, tt.bandwidth, storageRatios, sussSPAddrs)
		storageRatios = snap.StorageData.calcStorageRatio(storageRatios)
		harvest := big.NewInt(0)
		rewards := make([]SpaceRewardRecord, 0)
		capSuccAddrs := make(map[common.Address]*big.Int)
		rewards, harvest, _ = snap.StorageData.calcStoragePledgeReward(storageRatios, snap.RevenueStorage, tt.blocknumber.Uint64(), snap.Period, sussSPAddrs, capSuccAddrs)
		totalReward := big.NewInt(0)
		totalSPaceIndex := decimal.NewFromFloat(0)
		for _, reward := range rewards {
			index := decimal.NewFromBigInt(storageRatios[reward.Revenue].Capacity, 0).Mul(storageRatios[reward.Revenue].Ratio.Mul(getBandwaith(tt.bandwidth, tt.blocknumber.Uint64())))
			totalSPaceIndex = totalSPaceIndex.Add(index)
		}
		for _, reward := range rewards {
			totalReward = new(big.Int).Add(totalReward, reward.Amount)
			index := decimal.NewFromBigInt(storageRatios[reward.Revenue].Capacity, 0).Mul(storageRatios[reward.Revenue].Ratio.Mul(getBandwaith(tt.bandwidth, tt.blocknumber.Uint64())))
			actval := decimal.NewFromBigInt(tt.expectValue, 0).Mul(index).Div(totalSPaceIndex)

			fmt.Println("blocknumber", tt.blocknumber, "reward", reward, "actval", actval, "harvest", harvest, storageRatios[reward.Revenue].Ratio)
		}
		if totalReward.Cmp(tt.expectValue) == 0 {
			fmt.Println("compare successfully", "totalReward", totalReward)
		} else {
			fmt.Println("error", "totalReward", totalReward, "tt.expectValue", tt.expectValue)
		}

	}

}

func (snap *Snapshot) addPledge(pledgeAddrs []common.Address, StorageCapacity *big.Int, Price *big.Int, blocknumber *big.Int, bandwidth *big.Int, storageRatios map[common.Address]*StorageRatio, addrsSucc []common.Address) (map[common.Address]*StorageRatio, []common.Address) {
	pledgeRecord := make([]SPledgeRecord, 0)
	deviceBind := make([]DeviceBindRecord, 0)
	for _, pledgeAddr := range pledgeAddrs {
		pledgeRecord = append(pledgeRecord, SPledgeRecord{
			PledgeAddr:      pledgeAddr,
			Address:         pledgeAddr,
			Price:           Price,
			SpaceDeposit:    new(big.Int).Div(StorageCapacity, big.NewInt(1073741824)),
			StorageCapacity: StorageCapacity,
			StorageSize:     big.NewInt(20),
			RootHash:        common.HexToHash(pledgeAddr.String()),
			PledgeNumber:    blocknumber,
			Bandwidth:       bandwidth,
		})

		deviceBind = append(deviceBind, DeviceBindRecord{
			Device:    pledgeAddr,
			Revenue:   pledgeAddr,
			Contract:  common.Address{},
			MultiSign: common.Address{},
			Type:      1,
			Bind:      true,
		})
		storageRatios[pledgeAddr] = &StorageRatio{
			Capacity: StorageCapacity,
		}
		addrsSucc = append(addrsSucc, pledgeAddr)
	}
	snap.updateStorageData(pledgeRecord, nil)
	snap.updateDeviceBind(deviceBind, blocknumber.Uint64())
	return storageRatios, addrsSucc
}

func Test_calspaceProfitReward(t *testing.T) {
	s := NewStorageSnap()
	period := uint64(10)
	number := uint64(8640 * 366)
	blockNumPerYear := secondsPerYear / period
	yearCount := (number - StorageEffectBlockNumber) / blockNumPerYear

	var yearReward decimal.Decimal
	yearCount++
	if yearCount == 1 {
		yearReward = s.nYearSpaceProfitReward(yearCount)
	} else {
		yearReward = s.nYearSpaceProfitReward(yearCount).Sub(s.nYearSpaceProfitReward(yearCount - 1))
	}
	fmt.Println("yearReward", yearReward)
	fmt.Println("yearCount", yearCount)
	spaceProfitReward := yearReward.Div(decimal.NewFromInt(365))
	fmt.Println("spaceProfitReward", "spaceProfitReward", spaceProfitReward, "number", number)
}
func Test_CalLeaseReward(t *testing.T) {
	tests := []struct {
		leaseCapacity decimal.Decimal
		capacity      decimal.Decimal
		Price         *big.Int
		blocknumber   *big.Int
		bandwidth     *big.Int
		RatioIndex    decimal.Decimal
		Duration      *big.Int
		expectValue   decimal.Decimal
	}{
		{ //<=1EB
			capacity:      decimal.NewFromBigInt(new(big.Int).Mul(big.NewInt(1024), tb1b), 0),
			leaseCapacity: decimal.NewFromInt(12683853395132),
			Price:         new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:   big.NewInt(30),
			bandwidth:     big.NewInt(100),
			Duration:      big.NewInt(1),
			expectValue:   decimal.NewFromFloat(11812.758),
		}, { //EB <leaseCapacity<2EB
			capacity:      decimal.NewFromBigInt(new(big.Int).Mul(big.NewInt(1024), tb1b), 0),
			leaseCapacity: decimal.NewFromInt(1026 * 1024 * 1099511627776),
			Price:         new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:   big.NewInt(894037),
			bandwidth:     big.NewInt(100),
			Duration:      big.NewInt(1),
			expectValue:   decimal.NewFromFloat(11833.246),
		}, { //    2EB <leaseCapacity<3EB
			capacity:      decimal.NewFromBigInt(new(big.Int).Mul(big.NewInt(1024), tb1b), 0),
			leaseCapacity: decimal.NewFromInt(2*1024*1024*1099511627776 + 1099511627776),
			Price:         new(big.Int).Mul(big.NewInt(1), big.NewInt(1e+18)),
			blocknumber:   big.NewInt(894037),
			bandwidth:     big.NewInt(100),
			Duration:      big.NewInt(1),
			expectValue:   decimal.NewFromFloat(11853.769),
		},
	}
	snap := &Snapshot{
		StorageData:    NewStorageSnap(),
		RevenueStorage: make(map[common.Address]*RevenueParameter),
		Period:         10,
	}
	for _, tt := range tests {
		pledgeAddrs := make([]common.Address, 0)
		pledgeAddrs = append(pledgeAddrs, common.HexToAddress("ux1eb90474374f2ea4deb3961cdd0a391821c0321b"))
		storageRatios := make(map[common.Address]*StorageRatio)
		sussSPAddrs := make([]common.Address, 0)
		storageRatios, sussSPAddrs = snap.addPledge(pledgeAddrs, tt.capacity.BigInt(), tt.Price, tt.blocknumber, tt.bandwidth, storageRatios, sussSPAddrs)
		snap.insLeaseRequest(pledgeAddrs, tt.blocknumber, tt.leaseCapacity.BigInt(), tt.Duration)
		storageRatios = snap.StorageData.calcStorageRatio(storageRatios)
		totalLeaseSpace := decimal.NewFromInt(0)
		for _, item := range snap.StorageData.StoragePledge {
			for _, leaseItem := range item.Lease {
				totalLeaseSpace = totalLeaseSpace.Add(decimal.NewFromBigInt(leaseItem.Capacity, 0))
			}
		}
		for deviceipaddr, item := range snap.StorageData.StoragePledge {
			for _, leaseItem := range item.Lease {
				if revenue, ok := snap.RevenueStorage[deviceipaddr]; ok {
					leaseCapacity := decimal.NewFromBigInt(leaseItem.Capacity, 0).Div(decimal.NewFromInt(1073741824))           //to GB
					priceIndex := decimal.NewFromBigInt(leaseItem.UnitPrice, 0).Div(decimal.NewFromBigInt(baseStoragePrice, 0)) //RT/GB.day
					bandwidthIndex := getBandwaith(item.Bandwidth, tt.blocknumber.Uint64())
					if raitem, ok3 := storageRatios[revenue.RevenueAddress]; ok3 {
						value := snap.StorageData.calStorageLeaseReward(leaseCapacity, bandwidthIndex, raitem.Ratio, priceIndex, decimal.NewFromBigInt(tt.Duration, 0), totalLeaseSpace)
						actvalue := value.Div(decimal.NewFromInt(1e+18))
						utgGBrate := leaseCapacity.Div(actvalue).Truncate(3)
						fmt.Println("deviceipaddr", deviceipaddr, "leaseCapacity", leaseCapacity, "reward", value, "actvalue", actvalue, "utgGBrate", utgGBrate, "raitem.Ratio", raitem.Ratio, "priceIndex", priceIndex, "bandwidthIndex", bandwidthIndex)
						if utgGBrate.Cmp(tt.expectValue) == 0 {
							t.Logf("blocknumber=%dth, calStorageLeaseReward success amount= %s", snap.Number, utgGBrate)
						} else {
							t.Errorf("blocknumber=%dth, calStorageLeaseReward Failed amount= %s expectValue=%s ", snap.Number, utgGBrate, tt.expectValue)
						}
					}
				}

			}

		}
	}

}

func (snap *Snapshot) insLeaseRequest(pledgeAddrs []common.Address, blocknumber *big.Int, capacity *big.Int, duration *big.Int) {
	LeasePledge := make([]LeasePledgeRecord, 0)
	sRent := make([]LeaseRequestRecord, 0)
	for _, pledgeAddr := range pledgeAddrs {
		sRent = append(sRent, LeaseRequestRecord{
			Tenant:   pledgeAddr,
			Address:  pledgeAddr,
			Capacity: capacity,
			Duration: duration,
			Price:    baseStoragePrice,
			Hash:     common.HexToHash("aar" + pledgeAddr.String()),
		})
		LeasePledge = append(LeasePledge, LeasePledgeRecord{
			Address:        pledgeAddr,
			DepositAddress: pledgeAddr,
			Capacity:       capacity,
			RootHash:       common.HexToHash("aaaaaaaaaaaaaaaa"),
			BurnSRTAmount:  big.NewInt(0),
			BurnAmount:     big.NewInt(0),
			Duration:       big.NewInt(30),
			BurnSRTAddress: common.HexToAddress("bbt" + pledgeAddr.String()),
			PledgeHash:     common.HexToHash("aa"),
		})

	}

	snap.updateLeaseRequest(sRent, blocknumber, nil)
	snap.updateLeasePledge(LeasePledge, blocknumber, nil)
}
