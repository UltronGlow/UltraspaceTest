package alien

import (
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"hash/crc32"
	"hash/fnv"
	"math/big"
	"testing"
	"time"
)

func TestBigInt(t *testing.T) {
	a := new(big.Int).SetUint64(9999999999)
	blob, err := rlp.EncodeToBytes(a)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("biging len:", len(blob))
}

func _TestSetRatio(t *testing.T) {
	oldRatio := 5000
	newRatio := 8000

	newRatio = 10000 - (10000-oldRatio)*(10000-newRatio)/10000
	fmt.Println("new ratio:", newRatio)
}

func _TestLock(t *testing.T) {
	addr1 := common.BigToAddress(new(big.Int).SetUint64(1))
	addr0 := common.BigToAddress(new(big.Int).SetUint64(0))
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	trie, err := NewLockTrie(common.Hash{}, common.Hash{}, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}

	for i := 0; i < 2400; i++ {
		addr := common.BigToAddress(new(big.Int).SetUint64(uint64(i + 1)))
		for k := 0; k < 210; k++ {
			trie.AddBalance(10, addr0, addr, new(big.Int).SetUint64(uint64(2000)), 1, 50, 50, 30, 180)
		}
	}
	root, _ := trie.CommitLockInfo()
	r_root, _ := trie.CommitReleaseInfo()

	obj := trie.GetOrNewAccount(addr1, 1)
	if obj == nil {
		return
	}
	robj := trie.GetOrNewReleaseInfo(obj.GetReleaseNumber())
	if robj == nil {
		return
	}
	t.Log("number:", robj.GetReleaseNumber(), "release addrs:", len(robj.GetReleaseList()))
	t.Log("root:", root, "r_root:", r_root)
}

func _TestReleasePeroid(t *testing.T) {
	var number uint64 = 11
	root := common.HexToHash("ux3ae058c2df21f5cce1b17addba37bf5bb636a1177d71f3f533ccd890c02fd293")
	r_root := common.HexToHash("uxcb680eb17894667be7e27fbf1ae918e6d50464ea00637ec71b4ec75b900661e3")

	ethdb, err := newEthDataBase("")
	if err != nil {
		fmt.Println("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()
	/*
		for i:=0; i<100; i++ {
			begin := time.Now()
			testReleasePeroid(root, r_root, 98, ethdb)
			fmt.Println("idx:", i, "spend:", time.Now().Sub(begin).Milliseconds(),"ms")
			time.Sleep(1 * time.Second)
		}
		return*/
	for i := 0; i < 2400; i++ {
		root, r_root = testReleasePeroid(root, r_root, number+uint64(i), ethdb)
	}
	//testReleasePeroid(root, r_root, number+uint64(209))
	/*
		testReleasePeroid2(common.HexToHash("ux6c3876193ed7dbeb3ee92ee42b846e5d9bb064dc828f8a621636fd86779eee7e"),
			common.HexToHash("uxc26e4942458c8c1a60876797a888d5bc7c89529a6cedaa90389c10e7fb97a36a"))
	*/
}

func _TestDecodeObj(t *testing.T) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	//addr0 := common.BigToAddress(new(big.Int).SetUint64(0))
	root := common.HexToHash("uxe0dd41fecd05b0ac10acd58439c13d03aeb80122b09ccd8f33f88978f5477e51")
	r_root := common.HexToHash("uxc26e4942458c8c1a60876797a888d5bc7c89529a6cedaa90389c10e7fb97a36a")
	trie, err := NewLockTrie(root, r_root, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}

	for i := 0; i < 2048; i++ {
		addr := common.BigToAddress(new(big.Int).SetUint64(uint64(i + 1)))
		begin := time.Now()
		obj := trie.GetAccount(addr, 1)

		fmt.Println("idx:", i, "deoce object:", addr, "spend:", time.Now().Sub(begin).Milliseconds(), "ms, records:", len(obj.LockRecords))
	}
}

func _TestCheckRelease(t *testing.T) {
	var root, r_root common.Hash
	//addr := common.BigToAddress(new(big.Int).SetUint64(1))
	pledgeAddr := common.BigToAddress(new(big.Int).SetUint64(0))

	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	root = common.HexToHash("ux8025d0a9d85b768fc4a9a3423d1de4e1b216e5e362638a653bde28c51f0d227a")
	r_root = common.HexToHash("ux824f4462b02cb1ccea15673332dee1d8bcf3b2e785d9cb0953b4235b355cfa04")
	trie, err := NewLockTrie(root, r_root, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}

	obj := trie.GetOrNewAccount(pledgeAddr, 1)
	if obj == nil {
		return
	}
	t.Log("addr:", obj.PledgeAddr, "locknumber", len(obj.LockRecords), "releasenumber:", obj.GetReleaseNumber(), "lockbalance:", obj.Balance)

	robj := trie.GetOrNewReleaseInfo(1)
	if robj == nil {
		return
	}
	var number uint64 = 10
	t.Log("number:", robj.GetReleaseNumber(), "at releasenumber", number2ReleaseNumber(number), "release addrs:", len(robj.GetReleaseList()))

	result := trie.ReleaseBalance(number, nil)
	if len(result) > 0 {
		t.Log("relase:", len(result))
	} else {
		t.Log("no release info")
	}
	for _, v := range result {
		t.Log(v)
		break
	}
	obj = trie.GetOrNewAccount(pledgeAddr, 1)
	if obj == nil {
		return
	}
	t.Log("after release addr:", obj.PledgeAddr, "locknumber", len(obj.LockRecords), "releasenumber:", obj.GetReleaseNumber(), "lockbalance:", obj.Balance)
	root, _ = trie.CommitLockInfo()
	r_root, _ = trie.CommitReleaseInfo()
	t.Log("root:", root, "r_root:", r_root)
}

func _TestHash(t *testing.T) {
	//var data []byte = []byte("123456")
	var addrs map[uint32]uint32 = make(map[uint32]uint32, 8640)
	begin := time.Now()
	//for i:=0; i<500000; i++ {
	//fnvHash(data)
	//}
	//t.Log("fnvHash spend:", time.Now().Sub(begin).Microseconds())

	//begin = time.Now()
	for i := 0; i < 500000; i++ {
		addr := common.BigToAddress(new(big.Int).SetUint64(uint64(i)))
		number := crc32Hash(addr.Bytes())
		idx := uint32(number % blockperday)
		addrs[idx]++
	}
	t.Log("crc32Hash spend:", time.Now().Sub(begin).Microseconds())
	t.Log("addrs:", addrs)
}

func fnvHash(value []byte) {
	a := fnv.New32()
	a.Write(value)
	a.Sum32()
}

func crc32Hash(value []byte) uint32 {
	return crc32.ChecksumIEEE(value)
}

func _TestLockTrieWrite(t *testing.T) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	var lockcount int = 1000
	var addrs []common.Address = make([]common.Address, 0, lockcount)
	pledgeAddr := common.BigToAddress(new(big.Int).SetUint64(1000))

	for i := lockcount - 1; i >= 0; {
		addr := common.BigToAddress(new(big.Int).SetInt64((int64(i))))
		addrs = append(addrs, addr)
		i -= 1
	}

	begin := time.Now()
	trie, err := NewLockTrie(common.Hash{}, common.Hash{}, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}
	for _, addr := range addrs {
		trie.AddBalance(10, addr, pledgeAddr, new(big.Int).SetUint64(uint64(21000)), 1, 50, 50, 30, 210)
	}
	root, _ := trie.CommitLockInfo()
	r_root, _ := trie.CommitReleaseInfo()
	t.Log(lockcount, "account write spend", time.Now().Sub(begin).Seconds(), "seconds", "lock root:", root, "release root:", r_root)
}
func _TestLockTrieRead(t *testing.T) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	/*begin := time.Now()
	trie, err := NewLockTrie(common.HexToHash("uxb6282104795d8999f270f67e77749d00152f56dbde73933830e7bab8bef4eb92"), common.Hash{}, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}

	found := trie.GetAllLockBalance()
	t.Log(len(found), "account read spend", time.Now().Sub(begin).Seconds(), "seconds")*/
}

func testReleasePeroid(root, r_root common.Hash, number uint64, ethdb ethdb.Database) (common.Hash, common.Hash) {
	begin := time.Now()
	trie, err := NewLockTrie(root, r_root, ethdb)
	if err != nil {
		fmt.Println("open trie failed", err)
		return common.BigToHash(common.Big0), common.BigToHash(common.Big0)
	}

	var total int = 0
	result := trie.ReleaseBalance(number, nil)
	if len(result) > 0 {
		for k, _ := range result {
			total += len(result[k])
		}
	}
	root, _ = trie.CommitLockInfo()
	r_root, _ = trie.CommitReleaseInfo()
	if len(result) > 0 {
		fmt.Println("number:", number, ", releaseIdx:", number2ReleaseNumber(number), ", release addrs:", len(result), ", total records:", total, ", releaseDay:", (number-10)/blockperday, ", spend:", time.Now().Sub(begin).Milliseconds(), "ms")
		fmt.Println("number:", number, "root:", root, "r_root:", r_root)
	}
	return root, r_root
}

func testReleasePeroid2(root, r_root common.Hash) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		fmt.Println("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	trie, err := NewLockTrie(root, r_root, ethdb)
	if err != nil {
		fmt.Println("open trie failed", err)
		return
	}

	var number uint64 = 11
	var total int
	var releaseTimes map[common.Address]int = make(map[common.Address]int)

	for i := 0; i < 2200; i++ {
		begin := time.Now()
		total = 0
		result := trie.ReleaseBalance(number+uint64(i), nil)

		if len(result) > 0 {
			for k, _ := range result {
				total += len(result[k])
				releaseTimes[k] += 1
			}
			fmt.Println(time.Now().Format("2006-01-02 15:04:11"), "number:", number+uint64(i), "block in perday:", number2ReleaseNumber(number+uint64(i)), "release addrs:", len(result), "total records:", total, "releaseDay:", (number+uint64(i)-10)/blockperday, "spend:", time.Now().Sub(begin).Milliseconds(), "ms")
		}
	}
}
