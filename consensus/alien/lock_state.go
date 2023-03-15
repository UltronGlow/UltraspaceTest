package alien

import (
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
)

const (
	signerRewardKey        = "signerReward-%d"
)
type LockState interface {
	PayLockReward(LockAccountsRoot common.Hash, number uint64, state *state.StateDB) error
	CommitData() (common.Hash, common.Hash, error)
	AddLockReward(LockReward []LockRewardRecord, snap *Snapshot, db ethdb.Database, number uint64) ([]LockRewardRecord,error)
}

func NewLockState(root, lockaccounts common.Hash,number uint64) (LockState, error) {
	return NewDefaultLockState(root)
}

/**
 * ExtraStateDB
 */
type DefaultLockState struct {
}

func NewDefaultLockState(root common.Hash) (*DefaultLockState, error) {
	return &DefaultLockState{}, nil
}

func (c *DefaultLockState) PayLockReward(LockAccountsRoot common.Hash, number uint64, state *state.StateDB) error {
	return nil
}

func (c *DefaultLockState) CommitData() (common.Hash, common.Hash, error) {
	return common.Hash{}, common.Hash{}, nil
}

func (c *DefaultLockState) AddLockReward(LockReward []LockRewardRecord, snap *Snapshot, db ethdb.Database, number uint64) ([]LockRewardRecord,error) {
	return LockReward,nil
}