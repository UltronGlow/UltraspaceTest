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

package alien

import (
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/core/rawdb"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/params"
	"github.com/shopspring/decimal"
	"math/big"
	"testing"

	"github.com/UltronGlow/UltronGlow-Origin/common"
)

func TestAlien_PenaltyTrantor(t *testing.T) {
	tests := []struct {
		last    string
		current string
		queue   []string
		lastQ   []string
		result  []string // the result of missing

	}{
		{
			/* 	Case 0:
			 *  simple loop order, miss nothing
			 *  A -> B -> C
			 */
			last:    "A",
			current: "B",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{},
			result:  []string{},
		},
		{
			/* 	Case 1:
			 *  same loop, missing B
			 *  A -> B -> C
			 */
			last:    "A",
			current: "C",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{},
			result:  []string{"B"},
		},
		{
			/* 	Case 2:
			 *  same loop, not start from the first one
			 *  C -> A -> B
			 */
			last:    "C",
			current: "B",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{},
			result:  []string{"A"},
		},
		{
			/* 	Case 3:
			 *  same loop, missing two
			 *  A -> B -> C
			 */
			last:    "C",
			current: "C",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{},
			result:  []string{"A", "B"},
		},
		{
			/* 	Case 4:
			 *  cross loop
			 *  B -> A -> B -> C -> A
			 */
			last:    "B",
			current: "B",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{"C", "A", "B"},
			result:  []string{"A"},
		},
		{
			/* 	Case 5:
			 *  cross loop, nothing missing
			 *  A -> C -> A -> B -> C
			 */
			last:    "A",
			current: "C",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{"C", "A", "B"},
			result:  []string{},
		},
		{
			/* 	Case 6:
			 *  cross loop, two signers missing in last loop
			 *  C -> B -> C -> A
			 */
			last:    "C",
			current: "A",
			queue:   []string{"A", "B", "C"},
			lastQ:   []string{"C", "A", "B"},
			result:  []string{"B", "C"},
		},
	}

	// Run through the test
	for i, tt := range tests {
		// Create the account pool and generate the initial set of all address in addrNames
		accounts := newTesterAccountPool()
		addrQueue := make([]common.Address, len(tt.queue))
		for j, signer := range tt.queue {
			addrQueue[j] = accounts.address(signer)
		}

		extra := HeaderExtra{SignerQueue: addrQueue}
		var lastExtra HeaderExtra
		if len(tt.lastQ) > 0 {
			lastAddrQueue := make([]common.Address, len(tt.lastQ))
			for j, signer := range tt.lastQ {
				lastAddrQueue[j] = accounts.address(signer)
			}
			lastExtra = HeaderExtra{SignerQueue: lastAddrQueue}
		}

		missing := getSignerMissingTrantor(accounts.address(tt.last), accounts.address(tt.current), &extra, &lastExtra)

		signersMissing := make(map[string]bool)
		for _, signer := range missing {
			signersMissing[accounts.name(signer)] = true
		}
		if len(missing) != len(tt.result) {
			t.Errorf("test %d: the length of missing not equal to the length of result, Result is %v not %v  ", i, signersMissing, tt.result)
		}

		for j := 0; j < len(missing); j++ {
			if _, ok := signersMissing[tt.result[j]]; !ok {
				t.Errorf("test %d: the signersMissing is not equal Result is %v not %v ", i, signersMissing, tt.result)
			}
		}
	}
}

func TestAlien_Penalty(t *testing.T) {
	tests := []struct {
		last    string
		current string
		queue   []string
		newLoop bool
		result  []string // the result of current snapshot
	}{
		{
			/* 	Case 0:
			 *  simple loop order
			 */
			last:    "A",
			current: "B",
			queue:   []string{"A", "B", "C"},
			newLoop: false,
			result:  []string{},
		},
		{
			/* 	Case 1:
			 * simple loop order, new loop, no matter which one is current signer
			 */
			last:    "C",
			current: "A",
			queue:   []string{"A", "B", "C"},
			newLoop: true,
			result:  []string{},
		},
		{
			/* 	Case 2:
			 * simple loop order, new loop, no matter which one is current signer
			 */
			last:    "C",
			current: "B",
			queue:   []string{"A", "B", "C"},
			newLoop: true,
			result:  []string{},
		},
		{
			/* 	Case 3:
			 * simple loop order, new loop, missing in last loop
			 */
			last:    "B",
			current: "C",
			queue:   []string{"A", "B", "C"},
			newLoop: true,
			result:  []string{"C"},
		},
		{
			/* 	Case 4:
			 * simple loop order, new loop, two signers missing in last loop
			 */
			last:    "A",
			current: "C",
			queue:   []string{"A", "B", "C"},
			newLoop: true,
			result:  []string{"B", "C"},
		},
	}

	// Run through the test
	for i, tt := range tests {
		// Create the account pool and generate the initial set of all address in addrNames
		accounts := newTesterAccountPool()
		addrQueue := make([]common.Address, len(tt.queue))
		for j, signer := range tt.queue {
			addrQueue[j] = accounts.address(signer)
		}

		extra := HeaderExtra{SignerQueue: addrQueue}
		//missing := getSignerMissing(accounts.address(tt.last), accounts.address(tt.current), extra, tt.newLoop)
		missing := getSignerMissing(0,0,0,accounts.address(tt.last), accounts.address(tt.current), extra, tt.newLoop)

		signersMissing := make(map[string]bool)
		for _, signer := range missing {
			signersMissing[accounts.name(signer)] = true
		}
		if len(missing) != len(tt.result) {
			t.Errorf("test %d: the length of missing not equal to the length of result, Result is %v not %v  ", i, signersMissing, tt.result)
		}

		for j := 0; j < len(missing); j++ {
			if _, ok := signersMissing[tt.result[j]]; !ok {
				t.Errorf("test %d: the signersMissing is not equal Result is %v not %v ", i, signersMissing, tt.result)
			}
		}

	}
}

func  TestAlien_BlockReward(t *testing.T) {
	var period =uint64(3)
	blockNum := uint64(7980000)
	expectValue1:=float64(0.5000)
	expectValue2:=float64(0.2398)
	tests := []struct {
		maxSignerCount uint64
		number         uint64
		coinbase    common.Address
		Period      uint64
		expectValue  decimal.Decimal
	}{
		{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("ux7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			expectValue:decimal.NewFromFloat(expectValue1).Round(4),
		},{
			maxSignerCount :3,
			number     :blockNum,
			coinbase : common.HexToAddress("ux7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			expectValue:decimal.NewFromFloat(expectValue2).Round(4),
		},{
			maxSignerCount :3,
			number     :blockNum,
			coinbase : common.HexToAddress("ux7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			expectValue:decimal.NewFromFloat(expectValue2).Round(4),
		}}

	for _, tt := range tests {
		snap := &Snapshot{
			config:   &params.AlienConfig{
				MaxSignerCount: tt.maxSignerCount,
				Period: tt.Period,
			},
			Number:   tt.number,
			LCRS:     1,
			Tally:    make(map[common.Address]*big.Int),
			Punished: make(map[common.Address]uint64),
		}
		header := &types.Header{
			Coinbase: tt.coinbase,
			Number: big.NewInt(int64(tt.number)),
		}
		db := rawdb.NewMemoryDatabase()
		state, _ := state.New(common.Hash{}, state.NewDatabase(db), nil)
		alienConfig :=&params.AlienConfig{
			Period: tt.Period,
		}
		config := &params.ChainConfig{
			ChainID:big.NewInt(128),
			Alien:alienConfig,
		}
		refundGas:=RefundGas{}
		gasReward:=big.NewInt(0)

		currentHeaderExtra := HeaderExtra{}
		LockRewardRecord ,_:=accumulateRewards(currentHeaderExtra.LockReward, config,state, header, snap,refundGas,gasReward)

		yearCount := header.Number.Uint64()
		for index := range LockRewardRecord {
			actReward:=decimal.NewFromBigInt(LockRewardRecord[index].Amount,0).Div(decimal.NewFromInt(1E+18)).Round(4)
			if actReward.Cmp(tt.expectValue)==0{
				t.Logf("blocknumber : %d ,%d th years,coinbase %s,Block reward calculation pass,expect %s UTG,act %s" ,header.Number,yearCount,(LockRewardRecord[index].Target).Hex(),tt.expectValue.String(),actReward.String())
			}else {
				t.Errorf("blocknumber : %d ,%d th years,coinbase %s,Block reward calculation error,expect %s UTG,but act %s" ,header.Number,yearCount,(LockRewardRecord[index].Target).Hex(),tt.expectValue.String(),actReward.String())
			}

		}

	}
}
func  TestAlien_bandwidthReward(t *testing.T) {
	var period =uint64(10)
	//var bandwidth =1000
	blockNumPerYear := secondsPerYear /period
	dayReward1:=10448.5809
	dayReward2:=7918.5436
	tests := []struct {
		maxSignerCount uint64
		number         uint64
		coinbase    common.Address
		Period      uint64
		expectValue  decimal.Decimal
		bandwidth uint64
	}{
		{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(100),
			expectValue: decimal.NewFromFloat(dayReward1).Round(4),

		},{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(400),
			expectValue: decimal.NewFromFloat(dayReward1).Round(4),

		},{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(1000),
			expectValue: decimal.NewFromFloat( dayReward1).Round(4),

		},{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(2000),
			expectValue: decimal.NewFromFloat(dayReward1).Round(4),

		},
		{
			maxSignerCount :3,
			number     :blockNumPerYear*2-1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(100),
			expectValue: decimal.NewFromFloat( dayReward2).Round(4),
		},{
			maxSignerCount :3,
			number     :blockNumPerYear*2-1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(400),
			expectValue: decimal.NewFromFloat(dayReward2).Round(4),
		},{
			maxSignerCount :3,
			number     :blockNumPerYear*2-1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(1000),
			expectValue: decimal.NewFromFloat( dayReward2).Round(4),
		},{
			maxSignerCount :3,
			number     :blockNumPerYear*2-1,
			coinbase : common.HexToAddress("NX7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:uint64(2000),
			expectValue: decimal.NewFromFloat( dayReward2).Round(4),
		},
	}

	for _, tt := range tests {
		snap := &Snapshot{
			config:   &params.AlienConfig{
				MaxSignerCount: tt.maxSignerCount,
				Period: tt.Period,
			},
			Number:   tt.number,
			LCRS:     1,
			Tally:    make(map[common.Address]*big.Int),
			Punished: make(map[common.Address]uint64),
			Bandwidth:make(map[common.Address]*ClaimedBandwidth),
		}
		header := &types.Header{
			Coinbase: tt.coinbase,
			Number: big.NewInt(int64(tt.number)),
		}

		alienConfig :=&params.AlienConfig{
			Period: tt.Period,
		}
		config := &params.ChainConfig{
			ChainID:big.NewInt(128),
			Alien:alienConfig,
		}
		oldBandwidth100 :=&ClaimedBandwidth{
			ISPQosID:4,
			BandwidthClaimed:uint32(tt.bandwidth),
		}
		snap.Bandwidth[tt.coinbase] = oldBandwidth100


		currentHeaderExtra := HeaderExtra{}
		LockRewardRecord ,_:=accumulateBandwidthRewards(currentHeaderExtra.LockReward, config, header, snap,nil)
		yearCount := header.Number.Uint64() / blockNumPerYear
		if yearCount*blockNumPerYear!=header.Number.Uint64() {
			yearCount++
		}
		for index := range LockRewardRecord {
			actReward:=decimal.NewFromBigInt(LockRewardRecord[index].Amount,0).Div(decimal.NewFromInt(1E+18)).Round(4)
			if actReward.Cmp(tt.expectValue)==0{
				t.Logf("blocknumber : %d ,%d th years,coinbase %s,Bandwidth reward calculation pass,expect %d UTG,but act %d" ,header.Number,yearCount,(LockRewardRecord[index].Target).Hex(),tt.expectValue.BigFloat(),actReward.BigFloat())
			}else {
				t.Errorf("blocknumber : %d ,%d th years,coinbase %s,Bandwidth reward calculation error,expect %d UTG,but act %d" ,header.Number,yearCount,(LockRewardRecord[index].Target).Hex(),tt.expectValue.BigFloat(),actReward.BigFloat())
			}

		}

	}
}
func  TestAlien_bandwidthReward2(t *testing.T) {
	var period =uint64(10)
	dayReward1:=10448.5809
	bandwidth1:=uint64(100)
	bandwidth2:=uint64(400)
	tolbandwidth:=bandwidth1+bandwidth2
	tests := []struct {
		maxSignerCount uint64
		number         uint64
		coinbase    common.Address
		Period      uint64
		expectValue  decimal.Decimal
		bandwidth uint64
	}{
		{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("ux7a4539ed8a0b8b4583ead1e5a3f604e83419f502"),
			Period :period,
			bandwidth:bandwidth1,
			expectValue: decimal.NewFromFloat(dayReward1*float64(bandwidth1)/float64(tolbandwidth)).Round(4),

		},{
			maxSignerCount :3,
			number     :1,
			coinbase : common.HexToAddress("ux2aD0559Afade09a22364F6380f52BF57E9057B8D"),
			Period :period,
			bandwidth:bandwidth2,
			expectValue: decimal.NewFromFloat(dayReward1*float64(bandwidth2)/float64(tolbandwidth)).Round(4),

		},
	}

	snap := &Snapshot{
		LCRS:      1,
		Tally:     make(map[common.Address]*big.Int),
		Punished:  make(map[common.Address]uint64),
		Bandwidth: make(map[common.Address]*ClaimedBandwidth),
	}
	for _, tt := range tests {
		oldBandwidth100 := &ClaimedBandwidth{
			ISPQosID:         4,
			BandwidthClaimed: uint32(tt.bandwidth),
		}
		snap.Bandwidth[tt.coinbase] = oldBandwidth100
	}
	var LockRewardRecord[] LockRewardRecord
	for _, tt := range tests {
		snap.config = &params.AlienConfig{
			MaxSignerCount: tt.maxSignerCount,
			Period:         tt.Period,
		}
		snap.Number = tt.number
		header := &types.Header{
			Coinbase: tt.coinbase,
			Number:   big.NewInt(int64(tt.number)),
		}
		alienConfig := &params.AlienConfig{
			Period: tt.Period,
		}
		config := &params.ChainConfig{
			ChainID: big.NewInt(128),
			Alien:   alienConfig,
		}
		currentHeaderExtra := HeaderExtra{}
		LockRewardRecord, _ = accumulateBandwidthRewards(currentHeaderExtra.LockReward, config, header, snap, nil)
	}
	for _, tt := range tests {
		for index := range LockRewardRecord {
			if tt.coinbase==LockRewardRecord[index].Target{
				actReward:=decimal.NewFromBigInt(LockRewardRecord[index].Amount,0).Div(decimal.NewFromInt(1E+18)).Round(4)
				if actReward.Cmp(tt.expectValue)==0{
					t.Logf("coinbase %s,Bandwidth reward calculation pass,expect %d utg,but act %d" ,(LockRewardRecord[index].Target).Hex(),tt.expectValue.BigFloat(),actReward.BigFloat())
				}else {
					t.Errorf("coinbase %s,Bandwidth reward calculation error,expect %d utg,but act %d" ,(LockRewardRecord[index].Target).Hex(),tt.expectValue.BigFloat(),actReward.BigFloat())
				}
			}
		}
	}
}
func    TestAlien_FwReward(t *testing.T) {
	//
	var ebval =float64(1099511627776)
	tests := []struct {
		flowTotal decimal.Decimal
		expectValue decimal.Decimal
	}{
		{
			flowTotal:decimal.NewFromFloat(10995116),
			expectValue:decimal.NewFromFloat(60),
		},{
			flowTotal:decimal.NewFromFloat(ebval+1024),
			expectValue:decimal.NewFromFloat(60.945),
		},{
			flowTotal:decimal.NewFromFloat(ebval*2+1024),
			expectValue:decimal.NewFromFloat(61.904),
		},{
			flowTotal:decimal.NewFromFloat(ebval*3+1024),
			expectValue:decimal.NewFromFloat(62.878),
		},{
			flowTotal:decimal.NewFromFloat(ebval*4+1024),
			expectValue:decimal.NewFromFloat(63.868),
		},
	}
	for _, tt := range tests {
		fwreward := getFlowRewardScale(tt.flowTotal)
		rewardgb:= decimal.NewFromFloat(1).Div(fwreward.Mul(decimal.NewFromFloat(1024)).Div(decimal.NewFromFloat(1e+18))).Round(3)
		totalEb :=tt.flowTotal.Div(decimal.NewFromInt(1099511627776))
		var nebCount=totalEb.Round(0)
		if totalEb.Cmp(nebCount)>0 {
			nebCount= nebCount.Add(decimal.NewFromInt(1))
		}
		if rewardgb.Cmp(tt.expectValue)==0 {
			fmt.Println("Flow mining reward test pass ，",nebCount,"th EB，1 UTG=",rewardgb,"GB flow")
		}else{
			//t.Errorf("Flow mining reward test failed #{%d},",nebCount,"th EB，1 UTG=",rewardgb,"GB,But the actual need",tt.expectValue,"GB")
			t.Errorf("test: Flow mining reward test failed,theory 1 UTG need %d GB act need %d GB",tt.expectValue.BigFloat(),rewardgb.BigFloat())
		}

	}

}