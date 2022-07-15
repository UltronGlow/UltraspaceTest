package alien

import (
	"encoding/json"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"math/big"
	"testing"
)

func TestNewFlowMinerSnap_copy(t *testing.T) {
	s := NewFlowMinerSnap(100)
	s.FlowMinerCache = []string{"1", "2", "3"}
	s.FlowMinerPrevCache = []string{"4", "5", "6"}
	clone := s.copy()
	ss, err := json.Marshal(s)
	if err != nil {
		t.Errorf("json marshal error %v", err)
	}
	cloness, err := json.Marshal(clone)
	if err != nil {
		t.Errorf("json marshal error %v", err)
	}
	if string(ss) != string(cloness) {
		t.Error("FlowMinerPrevCache not equals")
		t.Errorf("snap: %s", string(ss))
		t.Errorf("clone: %s", string(cloness))
	}
}

func TestUpdateFlowMinerDaily(t *testing.T) {
	s := NewFlowMinerSnap(100)
	cached := []string{"1", "2", "3"}
	s.FlowMinerCache = cached
	s.FlowMiner[common.Address{}] = make(map[common.Hash]*FlowMinerReport)
	s.FlowMiner[common.Address{}][common.Hash{}] = &FlowMinerReport{
		Target:       common.Address{},
		Hash:         common.Hash{},
		ReportNumber: 1,
		FlowValue1:   2,
		FlowValue2:   3,
	}
	header := &types.Header{
		ParentHash:  common.Hash{},
		UncleHash:   common.Hash{},
		Coinbase:    common.Address{},
		Root:        common.Hash{},
		TxHash:      common.Hash{},
		ReceiptHash: common.Hash{},
		Bloom:       types.Bloom{},
		Difficulty:  nil,
		Number:      big.NewInt(100),
		GasLimit:    0,
		GasUsed:     0,
		Time:        0,
		Extra:       nil,
		MixDigest:   common.Hash{},
		Nonce:       types.BlockNonce{},
		Initial:     nil,
		BaseFee:     nil,
	}
	s.updateFlowMinerDaily(100, header)
	ss, err := json.Marshal(s)
	if err != nil {
		t.Errorf("json marshal error %v", err)
	}
	t.Log("json: ", string(ss))
	if len(s.FlowMinerCache) != 0 {
		t.Errorf("error in FlowMinerCache. len=%d ", len(s.FlowMinerCache))
	}
	if len(s.FlowMinerPrevCache) != len(cached) {
		t.Errorf("error in FlowMinerPrevCache. len=%d ", len(s.FlowMinerPrevCache))
	}
	for i, v := range s.FlowMinerPrevCache {
		if v != cached[i] {
			t.Errorf("FlowMinerPrevCache not equals, index=%d value=%v should=%v", i, v, cached[i])
		}
	}
	if len(s.FlowMiner) != 0 {
		t.Errorf("error in FlowMiner. len=%d ", len(s.FlowMiner))
	}
	report := s.FlowMinerPrev[common.Address{}][common.Hash{}]
	if report.ReportNumber != 1 || report.FlowValue1 != 2 || report.FlowValue2 != 3 {
		t.Errorf("error in FlowMinerPrev data")
	}
}
