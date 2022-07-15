package alien

import (
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/params"
	"github.com/shopspring/decimal"
	"math/big"
	"strings"
	"testing"
)

func TestAlien_calBwPledgeAmount(t *testing.T) {
	 tests := []struct {
		 bandwidth uint32
		 expectValue *big.Int
		 flowHarvest *big.Int
		 blockNumber uint64
		 total *big.Int
	}{
		 {
			bandwidth: 1024,
			expectValue:new(big.Int).SetUint64(uint64(1220703124999168)),
			flowHarvest:new(big.Int).SetUint64(2000),
			blockNumber:uint64(100),
			total:new(big.Int).SetUint64(2000),
		 },
		 {
			bandwidth: 1024,
			expectValue:new(big.Int).SetUint64(uint64(1220703124999168)),
			flowHarvest:new(big.Int).Mul(new(big.Int).SetUint64(2000),big.NewInt(1e+18)),
			blockNumber:uint64(100),
			total:new(big.Int).SetUint64(2000),
		 },
		 {
			 bandwidth: 1024,
			 expectValue:new(big.Int).SetUint64(uint64(341)),
			 flowHarvest:new(big.Int).Mul(new(big.Int).SetUint64(2000),big.NewInt(1e+16)),
			 blockNumber:uint64(3153601),
			 total:new(big.Int).Mul(new(big.Int).SetUint64(6),big.NewInt(1e+18)),
		 },
		 {
			 bandwidth: 10240000,
			 expectValue:new(big.Int).SetUint64(uint64(3413333)),
			 flowHarvest:new(big.Int).Mul(new(big.Int).SetUint64(2000),big.NewInt(1e+16)),
			 blockNumber:uint64(3153601),
			 total:new(big.Int).Mul(new(big.Int).SetUint64(6),big.NewInt(1e+18)),
		 },
	 }
	snap :=&Snapshot{
		config:&params.AlienConfig{
			Period:10,
	    },
	}
	for _, tt := range tests {
		snap.FlowHarvest=tt.flowHarvest
		snap.Number=tt.blockNumber
		amount :=calBwPledgeAmount(tt.bandwidth, snap,tt.total)
		if amount.Cmp(big.NewInt(1e+15))<0{
			if amount.Cmp(tt.expectValue)==0 {
				t.Logf("blocknumber=%dth, caclPayPeriodAmount success amount= %s",snap.Number,amount)
			}else  {
				t.Errorf("blocknumber=%dth, caclPayPeriodAmount Failed amount= %s expectValue=%s ",snap.Number,amount,tt.expectValue)
			}
		}else{
			actAmount:=decimal.NewFromBigInt(amount,0).Div(decimal.NewFromInt(1E+18)).Round(10)
			expectValue:=decimal.NewFromBigInt(tt.expectValue,0).Div(decimal.NewFromInt(1E+18)).Round(10)
			if actAmount.Cmp(expectValue)==0 {
				t.Logf("blocknumber=%dth, caclPayPeriodAmount success amount= %s",snap.Number,actAmount)
			}else  {
				t.Errorf("blocknumber=%dth, caclPayPeriodAmount Failed amount= %s expectValue=%s ",snap.Number,actAmount,expectValue)
			}
		}
	}
}


func TestAlien_checkRevenueNormalBind(t *testing.T) {
	device:=common.HexToAddress("NX1E0E2B42595Cb6046566F77Fb0c67a9D109aBE1D")
	revenue:=common.HexToAddress("NXa63b29EBe0A141B87A87e39dE17F17346e11e1b7")
	deviceBind:=DeviceBindRecord{
		Device:device,
		Revenue:revenue,
		Type:0,
	}
	alien:=&Alien{
	}
	snap :=&Snapshot{
		RevenueNormal:make(map[common.Address]*RevenueParameter),
	}
	err:=alien.checkRevenueNormalBind(deviceBind,snap)
	if err==nil{
		t.Logf("checkRevenueNormalBind pass")
	}else {
		t.Errorf("checkRevenueNormalBind fail,error msg:"+err.Error())
	}
	snap.RevenueNormal[device]=&RevenueParameter{
		RevenueAddress:revenue,
	}
	err=alien.checkRevenueNormalBind(deviceBind,snap)
	if err!=nil{
		t.Logf("checkRevenueNormalBind pass,error msg:"+err.Error())
	}else {
		t.Errorf("checkRevenueNormalBind fail,error msg is empty:")
	}
	deviceBind.Type=1
	err=alien.checkRevenueNormalBind(deviceBind,snap)
	if err==nil{
		t.Logf("checkRevenueNormalBind pass")
	}else {
		t.Errorf("checkRevenueNormalBind fail,error msg:"+err.Error())
	}
}


func TestAlien_processDeviceBind(t *testing.T) {

	currentDeviceBind:=make([]DeviceBindRecord,0)
	dev:="bec92229b1bd96919c8ffc993171fa6504121dc6"
	devAddr:=common.HexToAddress(dev)
	txData := "UTG:1:Bind:"+dev+":1:0000000000000000000000000000000000000000:0000000000000000000000000000000000000000"
	txDataInfo := strings.Split(txData, ":")
	txSender:= common.HexToAddress("NXa63b29EBe0A141B87A87e39dE17F17346e11e1b7")
	tx:=&types.Transaction{}
	receipts:=make([]*types.Receipt,0)
	snap :=&Snapshot{
		RevenueNormal:make(map[common.Address]*RevenueParameter),
		RevenueFlow:make(map[common.Address]*RevenueParameter),
	}
	alien:=&Alien{
	}
	currentDeviceBind=alien.processDeviceBind(currentDeviceBind, txDataInfo, txSender, tx, receipts, snap, 0)
	for index := range currentDeviceBind {
		if txSender==currentDeviceBind[index].Revenue&&devAddr==currentDeviceBind[index].Device{
			t.Logf("1 pass,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}else {
			t.Errorf("1 fail,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}
	}
	txData= "UTG:1:Bind:"+dev+":0:0000000000000000000000000000000000000000:0000000000000000000000000000000000000000"
	txDataInfo= strings.Split(txData, ":")
	currentDeviceBind=make([]DeviceBindRecord,0)
	currentDeviceBind=alien.processDeviceBind(currentDeviceBind, txDataInfo, txSender, tx, receipts, snap, 0)
	for index := range currentDeviceBind {
		if txSender==currentDeviceBind[index].Revenue&&devAddr==currentDeviceBind[index].Device{
			t.Logf("2 pass,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}else {
			t.Errorf("2 fail,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}
	}
	txData= "UTG:1:Bind:"+dev+":0:0000000000000000000000000000000000000000:0000000000000000000000000000000000000000"
	txDataInfo= strings.Split(txData, ":")
	currentDeviceBind=make([]DeviceBindRecord,0)
	currentDeviceBind=alien.processDeviceBind(currentDeviceBind, txDataInfo, txSender, tx, receipts, snap, 0)
	if len(currentDeviceBind)==0{
		t.Logf("3 pass" )
	}else{
		t.Errorf("3 fail")
	}

	state := &state.StateDB{
	}
	currentDeviceBind=alien.processDeviceRebind(currentDeviceBind, txDataInfo, txSender, tx, receipts, state, snap, 0)
	if len(currentDeviceBind)==0{
		t.Logf("4 pass" )
	}else{
		t.Errorf("4 fail")
	}

	newrev:="0Ff6e773Ff893fF39ed9352160889df13BDfc896"
	newrevAddr:=common.HexToAddress(newrev)
	txData= "UTG:1:Rebind:"+dev+":0:0000000000000000000000000000000000000000:0000000000000000000000000000000000000000:"+newrev
	txDataInfo= strings.Split(txData, ":")
	currentDeviceBind=alien.processDeviceRebind(currentDeviceBind, txDataInfo, txSender, tx, receipts, state, snap, 0)
	for index := range currentDeviceBind {
		if newrevAddr==currentDeviceBind[index].Revenue&&devAddr==currentDeviceBind[index].Device{
			t.Logf("5 pass,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}else {
			t.Errorf("5 fail,Revenue=%s,Device=%s" ,currentDeviceBind[index].Revenue.String(),currentDeviceBind[index].Device.String())
		}
	}

}