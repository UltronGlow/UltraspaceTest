package alien

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"github.com/UltronGlow/UltronGlow-Origin/trie"
	"hash/crc32"
	"math/big"
	"sort"
)

const (
	blockperoid        = 10
	blockperday        = secondsPerDay / blockperoid
	payStartNumber     = 1260
	payEndNumber       = 200
	payPeroid          = blockperday - payStartNumber - payEndNumber

	defaultBaseRatio    = 10000

	posLockType         = common.AddressLength
)

var (
	errInvalideRatio    = errors.New("invalide ratio param")
	errLockAmountIsZero = errors.New("lock amount is zero")
)

const (
	prefixRelease = "releaseNumber-"
)

type LockRecord struct {
	Number        uint64
	Address       common.Address
	TotalBalance  *big.Int
	Released      *big.Int
	Pledgeed      *big.Int
	Destroyed     *big.Int
	PledgeRatio   uint16
	DestroyRatio  uint16
	LockDays      uint16
	ReleaseDays   uint16
	ReleaseIdx    uint16
	ReleaseNumber uint64
}

type LockAccount struct {
	PledgeAddr          common.Address
	LockType            uint8
	Balance             *big.Int
	ReleaseNumberPerDay uint32 // release index in everyday
	LockRecords         []LockRecord
}

type ReleaseRecord struct {
	Number     uint64
	Addr       common.Address
	PledgeAddr common.Address
	LockType   uint8
	Released   *big.Int
	Pledged    *big.Int
	Destroyed  *big.Int
}

func newLockAccount(pledgeaddr common.Address, locktype uint8) *LockAccount {
	key := generateKey(pledgeaddr, locktype)
	obj := &LockAccount{
		PledgeAddr:          pledgeaddr,
		LockType:            locktype,
		Balance:             common.Big0,
		LockRecords:         make([]LockRecord, 0),
		ReleaseNumberPerDay: crc32.ChecksumIEEE(key)%payPeroid + payStartNumber,
	}
	return obj
}

func generateKey(pledgeaddr common.Address, locktype uint8) []byte {
	key := append(pledgeaddr.Bytes(), locktype)
	return key
}

func decodeLockAccount(buf []byte) *LockAccount {
	if len(buf) == 0 {
		return nil
	}
	l := &LockAccount{}
	err := rlp.DecodeBytes(buf, l)
	if err != nil {
		//log.Warn("decode lockaccoutn", "error", err)
		return nil
	} else {
		return l
	}
}

func (l *LockAccount) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(l)
}

func (l *LockAccount) KeyBytes() []byte {
	return generateKey(l.PledgeAddr, l.LockType)
}

func (l *LockAccount) GetReleaseNumber() uint32 {
	return l.ReleaseNumberPerDay
}

func (l *LockAccount) GetBalance() *big.Int {
	return l.Balance
}

func (l *LockAccount) SetPledgeRatio(pledgeRatio uint16) error {
	if pledgeRatio > defaultBaseRatio {
		return errInvalideRatio
	}
	for k, _ := range l.LockRecords {
		l.LockRecords[k].PledgeRatio = pledgeRatio
	}
	return nil
}

func (l *LockAccount) SetDestroyRatio(destroyRatio uint16) error {
	if destroyRatio > defaultBaseRatio {
		return errInvalideRatio
	}
	for k, _ := range l.LockRecords {
		l.LockRecords[k].DestroyRatio = defaultBaseRatio - (defaultBaseRatio-l.LockRecords[k].DestroyRatio)*(defaultBaseRatio-destroyRatio)/defaultBaseRatio
	}
	return nil
}

func (l *LockAccount) SetPledgeRatioWithOption(start, end uint64, pledgeRatio uint16) error {
	if pledgeRatio > defaultBaseRatio {
		return errInvalideRatio
	}
	for k, _ := range l.LockRecords {
		if l.LockRecords[k].Number < start || l.LockRecords[k].Number > end {
			continue
		}
		l.LockRecords[k].PledgeRatio = pledgeRatio
	}
	return nil
}

func (l *LockAccount) SetDestroyRatioWithOption(start, end uint64, destroyRatio uint16) error {
	if destroyRatio > defaultBaseRatio {
		return errInvalideRatio
	}
	for k, _ := range l.LockRecords {
		if l.LockRecords[k].Number < start || l.LockRecords[k].Number > end {
			continue
		}
		l.LockRecords[k].DestroyRatio = defaultBaseRatio - (defaultBaseRatio-l.LockRecords[k].DestroyRatio)*(defaultBaseRatio-destroyRatio)/defaultBaseRatio
	}
	return nil
}

func (l *LockAccount) SetBalance(amount *big.Int) {
	l.Balance = amount
}

func (l *LockAccount) AddBalance(number uint64, addr common.Address, amount *big.Int, pledgeRatio, destroyRatio uint16, lockDays, releaseDays uint16) error {
	if amount.Sign() == 0 {
		return errLockAmountIsZero
	}
	if pledgeRatio > defaultBaseRatio || destroyRatio > defaultBaseRatio {
		return errInvalideRatio
	}
	l.Balance = new(big.Int).Add(l.Balance, amount)

	for idx := len(l.LockRecords)-1; idx >= 0; idx-- {
		if l.LockRecords[idx].Number == number && l.LockRecords[idx].Address == addr {
			l.LockRecords[idx].TotalBalance = new(big.Int).Add(l.LockRecords[idx].TotalBalance, amount)
			return nil
		}
	}
	record := LockRecord{
		Number:       number,
		Address:      addr,
		PledgeRatio:  pledgeRatio,
		DestroyRatio: destroyRatio,
		TotalBalance: amount,
		LockDays:     lockDays,
		ReleaseIdx:   0,
		ReleaseDays:  releaseDays,
	}
	l.LockRecords = append(l.LockRecords, record)
	return nil
}

func (l *LockAccount) ReleaseBalance(number uint64, state *state.StateDB) []ReleaseRecord {
	var (
		records []ReleaseRecord
		amounts map[common.Address]*big.Int = make(map[common.Address]*big.Int)
	)
	if l.Balance.Sign() == 0 {
		return []ReleaseRecord{}
	}
	for k, _ := range l.LockRecords {
		var amount *big.Int
		if l.LockRecords[k].ReleaseIdx >= l.LockRecords[k].ReleaseDays {
			continue
		}
		if (number-l.LockRecords[k].Number)/blockperday <= uint64(l.LockRecords[k].LockDays) {
			continue
		}
		if number <= l.LockRecords[k].ReleaseNumber {
			continue
		}

		if l.LockRecords[k].ReleaseIdx+1 < l.LockRecords[k].ReleaseDays {
			amount = new(big.Int).Div(l.LockRecords[k].TotalBalance, new(big.Int).SetUint64(uint64(l.LockRecords[k].ReleaseDays)))
		} else {
			avg := new(big.Int).Div(l.LockRecords[k].TotalBalance, new(big.Int).SetUint64(uint64(l.LockRecords[k].ReleaseDays)))
			amount = new(big.Int).Sub(l.LockRecords[k].TotalBalance, new(big.Int).Mul(avg, new(big.Int).SetUint64(uint64(l.LockRecords[k].ReleaseDays-1))))
		}

		var record ReleaseRecord
		record.Destroyed = new(big.Int).Div(new(big.Int).Mul(amount, new(big.Int).SetUint64(uint64(l.LockRecords[k].DestroyRatio))), new(big.Int).SetUint64(defaultBaseRatio))
		record.Pledged = new(big.Int).Div(new(big.Int).Mul(new(big.Int).Sub(amount, record.Destroyed), new(big.Int).SetUint64(uint64(l.LockRecords[k].PledgeRatio))), new(big.Int).SetUint64(defaultBaseRatio))
		record.Released = new(big.Int).Sub(new(big.Int).Sub(amount, record.Destroyed), record.Pledged)
		record.PledgeAddr = l.PledgeAddr
		record.Addr = l.LockRecords[k].Address
		record.Number = l.LockRecords[k].Number
		record.LockType = l.LockType
		records = append(records, record)

		l.LockRecords[k].Released = new(big.Int).Add(l.LockRecords[k].Released, record.Released)
		l.LockRecords[k].Pledgeed = new(big.Int).Add(l.LockRecords[k].Pledgeed, record.Pledged)
		l.LockRecords[k].Destroyed = new(big.Int).Add(l.LockRecords[k].Destroyed, record.Destroyed)
		l.LockRecords[k].ReleaseIdx += 1
		l.LockRecords[k].ReleaseNumber = number

		if l.Balance.Cmp(amount) > 0 {
			l.Balance = new(big.Int).Sub(l.Balance, amount)
		}else {
			l.Balance = common.Big0
		}

		if _, ok := amounts[l.LockRecords[k].Address]; !ok {
			amounts[l.LockRecords[k].Address] = record.Released
		}else {
			amounts[l.LockRecords[k].Address] = new(big.Int).Add(amounts[l.LockRecords[k].Address], record.Released)
		}
	}

	if state != nil {
		for addr, _ := range amounts {
			state.AddBalance(addr, amounts[addr])
		}
	}
	var locks []LockRecord
	for k, _ := range l.LockRecords {
		if l.LockRecords[k].ReleaseIdx == l.LockRecords[k].ReleaseDays {
			continue
		}
		locks = append(locks, l.LockRecords[k])
	}
	l.LockRecords = locks
	return records
}

func (l *LockAccount) Destroy() *big.Int {
	amount := l.Balance
	l.Balance = common.Big0

	return amount
}

//=========================================================================
type AddrList [][]byte

func (a AddrList) Len() int      { return len(a) }
func (a AddrList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a AddrList) Less(i, j int) bool {
	return bytes.Compare(a[i], a[j]) > 0
}

type ReleaseInfo struct {
	NumberPerday uint32
	AddrList     AddrList
	addrs        map[string]struct{} `rlp:"-"`
}

func newReleaseInfo(numberPerday uint32) *ReleaseInfo {
	return &ReleaseInfo{
		NumberPerday: numberPerday,
		AddrList:     [][]byte{},
		addrs:        make(map[string]struct{}),
	}
}

func decodeReleaseInfo(data []byte) *ReleaseInfo {
	if len(data) == 0 {
		return nil
	}
	r := &ReleaseInfo{}
	err := rlp.DecodeBytes(data, r)
	if err != nil {
		return nil
	}

	r.addrs = make(map[string]struct{})
	for _, addr := range r.AddrList {
		r.addrs[hex.EncodeToString(addr)] = struct{}{}
	}
	return r
}

func (r *ReleaseInfo) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(r)
}

func (r *ReleaseInfo) GetKey() []byte {
	return []byte(fmt.Sprintf("%s%d", prefixRelease, r.NumberPerday))
}

func (r *ReleaseInfo) setAddr(keyBytes []byte) []byte {
	if _, ok := r.addrs[hex.EncodeToString(keyBytes)]; ok {
		return []byte{}
	}
	r.AddrList = append(r.AddrList, keyBytes)
	sort.Sort(r.AddrList)
	r.addrs[hex.EncodeToString(keyBytes)] = struct{}{}
	return keyBytes
}

func (r *ReleaseInfo) deleteAddr(keyBytes []byte) {
	if _, ok := r.addrs[hex.EncodeToString(keyBytes)]; !ok {
		return
	}
	delete(r.addrs, hex.EncodeToString(keyBytes))
	var idx int
	for k, v := range r.AddrList {
		if bytes.Compare(keyBytes, v) == 0 {
			idx = k
			break
		}
	}
	r.AddrList = append(r.AddrList[:idx], r.AddrList[idx+1:]...)
}

func (r *ReleaseInfo) GetReleaseList() [][]byte {
	var result [][]byte = make([][]byte, len(r.AddrList))
	copy(result, r.AddrList)
	return result
}

func (r *ReleaseInfo) GetReleaseNumber() uint32 {
	return r.NumberPerday
}

//=========================================================================
type LockTrie struct {
	trie   *trie.SecureTrie
	r_trie *trie.SecureTrie
	rdirty bool
	db     ethdb.Database
	triedb *trie.Database
}

func (l *LockTrie) GetAccount(pledgeaddr common.Address, locktype uint8) *LockAccount {
	var obj *LockAccount
	objData := l.trie.Get(generateKey(pledgeaddr, locktype))
	obj = decodeLockAccount(objData)
	if obj != nil {
		return obj
	}
	return nil
}

func (l *LockTrie) GetOrNewAccount(pledgeaddr common.Address, locktype uint8) *LockAccount {
	var obj *LockAccount

	objData := l.trie.Get(generateKey(pledgeaddr, locktype))
	obj = decodeLockAccount(objData)
	if obj != nil {
		return obj
	}
	obj = newLockAccount(pledgeaddr, locktype)
	robj := l.GetOrNewReleaseInfo(obj.GetReleaseNumber())
	b := robj.setAddr(obj.KeyBytes())
	if len(b) > 0 {
		value, _ := robj.Encode()
		l.r_trie.Update(robj.GetKey(), value)
	}
	return obj
}

func (l *LockTrie) GetAccountByKey(key []byte) *LockAccount {
	objData := l.trie.Get(key)
	return decodeLockAccount(objData)
}

func (l *LockTrie) GetOrNewReleaseInfo(releaseNumber uint32) *ReleaseInfo {
	var rObj *ReleaseInfo
	rObjData := l.r_trie.Get([]byte(fmt.Sprintf("%s%d", prefixRelease, releaseNumber)))
	rObj = decodeReleaseInfo(rObjData)
	if rObj != nil {
		return rObj
	}
	rObj = newReleaseInfo(releaseNumber)
	return rObj
}

func (l *LockTrie) TireDB() *trie.Database {
	return l.triedb
}

func (l *LockTrie) GetBalance(pledgeaddr common.Address, locktype uint8) *big.Int {
	obj := l.GetAccount(pledgeaddr, locktype)
	if obj == nil {
		return common.Big0
	}
	return obj.GetBalance()
}

func (l *LockTrie) AddBalance(number uint64, addr, pledgeaddr common.Address, amount *big.Int, lockType uint8, releaseRatio, destroyRatio, lockDays, releaseDays uint16) {
	obj := l.GetOrNewAccount(pledgeaddr, lockType)
	if obj == nil {
		log.Warn("lockTrie AddBalance", "result", "failed")
		return
	}
	obj.AddBalance(number, addr, amount, releaseRatio, destroyRatio, lockDays, releaseDays)
	value, _ := obj.Encode()
	l.trie.Update(obj.KeyBytes(), value)
}

func (l *LockTrie) Hash() common.Hash {
	return l.trie.Hash()
}

func (l *LockTrie) RHash() common.Hash {
	return l.r_trie.Hash()
}

func (l *LockTrie) CommitLockInfo() (root common.Hash, err error) {
	hash, err := l.trie.Commit(nil)
	if err != nil {
		return common.Hash{}, err
	}
	l.triedb.Commit(hash, true, nil)
	l.rdirty = false
	return hash, nil
}

func (l *LockTrie) CommitReleaseInfo() (common.Hash, error) {
	hash, err := l.r_trie.Commit(nil)
	if err != nil {
		return common.Hash{}, err
	}
	l.triedb.Commit(hash, true, nil)
	l.rdirty = false
	return hash, nil
}

func (l *LockTrie) Commit() (common.Hash, common.Hash, error) {
	root, err := l.CommitLockInfo()
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	r_root, err := l.CommitReleaseInfo()
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	return root, r_root, nil
}

func (l *LockTrie) Copy() *LockTrie {
	root, _ := l.CommitLockInfo()
	r_root, _ := l.CommitReleaseInfo()

	trie, _ := NewLockTrie(root, r_root, l.db)
	return trie
}

func (l *LockTrie) ReleaseBalance(number uint64, state *state.StateDB) map[common.Address][]ReleaseRecord {
	var released map[common.Address][]ReleaseRecord = make(map[common.Address][]ReleaseRecord)

	if number2ReleaseNumber(number) < payStartNumber || number2ReleaseNumber(number) > blockperday - payEndNumber {
		return released
	}
	robj := l.GetOrNewReleaseInfo(number2ReleaseNumber(number))
	addrList := robj.GetReleaseList()
	for idx, _ := range addrList {
		if len(addrList[idx]) < 21 {
			continue
		}
		obj := l.GetAccountByKey(addrList[idx])
		if obj != nil {
			records := obj.ReleaseBalance(number, state)
			if len(records) > 0 {
				if _, ok := released[obj.PledgeAddr]; !ok {
					released[obj.PledgeAddr] = records
				}else {
					released[obj.PledgeAddr] = append(released[obj.PledgeAddr],records...)
				}
				value, _ := obj.Encode()
				if obj.GetBalance().Cmp(common.Big0) <= 0 {
					l.trie.Delete(obj.KeyBytes())
					robj.deleteAddr(obj.KeyBytes())
					rvalue, _ := robj.Encode()
					l.r_trie.Update(robj.GetKey(), rvalue)
				} else {
					l.trie.Update(obj.KeyBytes(), value)
				}
			}
		}
	}

	return released
}

func (l *LockTrie) SetPledgeRatio(pledgeaddr common.Address, lockType uint8, pledgeRatio uint16) {
	obj := l.GetOrNewAccount(pledgeaddr, lockType)
	if obj == nil {
		//log.Warn("SetReleaseRatio", "result", "failed")
		return
	}
	obj.SetPledgeRatio(pledgeRatio)
	value, _ := obj.Encode()
	l.trie.Update(obj.KeyBytes(), value)
}

func (l *LockTrie) SetDestroyRatio(pledgeaddr common.Address, lockType uint8, destroyRatio uint16) {
	obj := l.GetOrNewAccount(pledgeaddr, lockType)
	if obj == nil {
		//log.Warn("SetDestroyRatio", "result", "failed")
		return
	}
	obj.SetDestroyRatio(destroyRatio)
	value, _ := obj.Encode()
	l.trie.Update(obj.KeyBytes(), value)
}

func (l *LockTrie) SetPledgeRatioWithOption(pledgeaddr common.Address, start, end uint64, lockType uint8, pledgeRatio uint16) {
	obj := l.GetOrNewAccount(pledgeaddr, lockType)
	if obj == nil {
		//log.Warn("SetReleaseRatio", "result", "failed")
		return
	}
	obj.SetPledgeRatioWithOption(start, end, pledgeRatio)
	value, _ := obj.Encode()
	l.trie.Update(obj.KeyBytes(), value)
}

func (l *LockTrie) SetDestroyRatioWithOption(pledgeaddr common.Address, start, end uint64, lockType uint8, destroyRatio uint16) {
	obj := l.GetOrNewAccount(pledgeaddr, lockType)
	if obj == nil {
		//log.Warn("SetDestroyRatio", "result", "failed")
		return
	}
	obj.SetDestroyRatioWithOption(start, end, destroyRatio)
	value, _ := obj.Encode()
	l.trie.Update(obj.KeyBytes(), value)
}

func (l *LockTrie) GetLockRecords(lockType uint8) map[common.Address][]LockRecord {
	lockDetail := make(map[common.Address][]LockRecord)
	it := trie.NewIterator(l.r_trie.NodeIterator(nil))
	for it.Next() {
		releaseObj := decodeReleaseInfo(it.Value)
		objList := releaseObj.GetReleaseList()
		for _, v := range objList{
			if v[posLockType] == lockType {
				lockObj := l.GetAccountByKey(v)
				if lockObj == nil || len(lockObj.LockRecords) == 0{
					continue
				}
				lockDetail[common.BytesToAddress(v[:common.AddressLength])] = make([]LockRecord, len(lockObj.LockRecords))
				copy(lockDetail[common.BytesToAddress(v[:common.AddressLength])], lockObj.LockRecords)
			}
		}
	}
	return lockDetail
}

//====================================================================================
func NewLockTrie(root, r_root common.Hash, db ethdb.Database) (*LockTrie, error) {
	triedb := trie.NewDatabase(db)
	tr, err := trie.NewSecure(root, triedb)
	if err != nil {
		log.Warn("locktrie open lock trie failed", "root", root)
		return nil, err
	}

	r_tr, err := trie.NewSecure(r_root, triedb)
	if err != nil {
		log.Warn("locktrie open lock trie failed", "root", root)
		return nil, err
	}

	return &LockTrie{
		trie:   tr,
		r_trie: r_tr,
		db:     db,
		rdirty: false,
		triedb: triedb,
	}, nil
}

func number2ReleaseNumber(number uint64) uint32 {
	return uint32(number % blockperday)
}
