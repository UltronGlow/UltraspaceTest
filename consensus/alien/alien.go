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

// Package alien implements the delegated-proof-of-stake consensus engine.
package alien

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"io"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/UltronGlow/UltronGlow-Origin/accounts"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/consensus"
	"github.com/UltronGlow/UltronGlow-Origin/core/state"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/crypto"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/log"
	"github.com/UltronGlow/UltronGlow-Origin/params"
	"github.com/UltronGlow/UltronGlow-Origin/rlp"
	"github.com/UltronGlow/UltronGlow-Origin/rpc"
	"github.com/UltronGlow/UltronGlow-Origin/trie"
	"github.com/shopspring/decimal"
	"golang.org/x/crypto/sha3"
)

const (
	inMemorySnapshots  = 128             // Number of recent vote snapshots to keep in memory
	inMemorySignatures = 4096            // Number of recent block signatures to keep in memory
	secondsPerYear     = 365 * 24 * 3600 // Number of seconds for one year
	scUnconfirmLoop    = 3               // First count of Loop not send confirm tx to main chain
)

// Alien delegated-proof-of-stake protocol constants.
var (
	totalBlockReward                 = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(52500000))   // Block reward in wei
	totalBandwidthReward             = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(15750000))   // Block reward in wei
	totalFlowReward                  = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(1155000000)) // Block reward in wei
	defaultEpochLength               = uint64(60480)                                               // Default number of blocks after which vote's period of validity, About one week if period is 10
	defaultBlockPeriod               = uint64(10)                                                  // Default minimum difference between two consecutive block's timestamps
	defaultMaxSignerCount            = uint64(21)                                                  //
	minVoterBalance                  = new(big.Int).Mul(big.NewInt(100), big.NewInt(1e+18))
	extraVanity                      = 32                                                                // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal                        = 65                                                                // Fixed number of extra-data suffix bytes reserved for signer seal
	uncleHash                        = types.CalcUncleHash(nil)                                          // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.
	defaultDifficulty                = big.NewInt(1)                                                     // Default difficulty
	defaultLoopCntRecalculateSigners = uint64(10)                                                        // Default loop count to recreate signers from top tally
	minerRewardPerThousand           = uint64(618)                                                       // Default reward for miner in each block from block reward (618/1000)
	candidateNeedPD                  = true                                                              // is new candidate need Proposal & Declare process
	mcNetVersion                     = uint64(0)                                                         // the net version of main chain
	mcLoopStartTime                  = uint64(0)                                                         // the loopstarttime of main chain
	mcPeriod                         = uint64(0)                                                         // the period of main chain
	mcSignerLength                   = uint64(0)                                                         // the maxsinger of main chain config
	mcNonce                          = uint64(0)                                                         // the current Nonce of coinbase on main chain
	mcTxDefaultGasPrice              = big.NewInt(30000000)                                              // default gas price to build transaction for main chain
	mcTxDefaultGasLimit              = uint64(3000000)                                                   // default limit to build transaction for main chain
	proposalDeposit                  = new(big.Int).Mul(big.NewInt(1e+18), big.NewInt(1e+4))             // default current proposalDeposit
	scRentLengthRecommend            = uint64(0)                                                         // block number for split each side chain rent fee
	managerAddressExchRate           = common.HexToAddress("ux8e4dB3E0AB9942890974C6860bd9BF7785817012") ////TODO seaskycheng
	managerAddressSystem             = common.HexToAddress("uxf8891B90305ca6aaD69687E1420a5B2930aF4b4E") ////TODO seaskycheng
	managerAddressWdthPnsh           = common.HexToAddress("ux245Ee49D3Def85c29FA34b79ab2405591D937a44") ////TODO seaskycheng
	managerAddressFlowReport         = common.HexToAddress("uxfDd486d7CfEc04d8468d4cC2F821a53144918F05") ////TODO seaskycheng
	managerAddressManager            = common.HexToAddress("ux02C37850bDa531EcBb7edADF30DBadd89Ad5A824") ////TODO seaskycheng
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte suffix signature missing")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// ErrInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// errInvalidVotingChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errInvalidVotingChain = errors.New("invalid voting chain")

	// errUnauthorized is returned if a header is signed by a non-authorized entity.
	errUnauthorized = errors.New("unauthorized")

	// errPunishedMissing is returned if a header calculate punished signer is wrong.
	errPunishedMissing = errors.New("punished signer missing")

	// errWaitTransactions is returned if an empty block is attempted to be sealed
	// on an instant chain (0 second period). It's important to refuse these as the
	// block reward is zero, so an empty block just bloats the chain... fast.
	errWaitTransactions = errors.New("waiting for transactions")

	// errUnclesNotAllowed is returned if uncles exists
	errUnclesNotAllowed = errors.New("uncles not allowed")

	// errCreateSignerQueueNotAllowed is returned if called in (block number + 1) % maxSignerCount != 0
	errCreateSignerQueueNotAllowed = errors.New("create signer queue not allowed")

	// errInvalidSignerQueue is returned if verify SignerQueue fail
	errInvalidSignerQueue = errors.New("invalid signer queue")

	// errSignerQueueEmpty is returned if no signer when calculate
	errSignerQueueEmpty = errors.New("signer queue is empty")

	// errGetLastLoopInfoFail is returned if get last loop info fail
	errGetLastLoopInfoFail = errors.New("get last loop info fail")

	// errInvalidNeighborSigner is returned if two neighbor block signed by same miner and time diff less period
	errInvalidNeighborSigner = errors.New("invalid neighbor signer")

	// errMissingGenesisLightConfig is returned only in light syncmode if light config missing
	errMissingGenesisLightConfig = errors.New("light config in genesis is missing")

	// errLastLoopHeaderFail is returned when try to get header of last loop fail
	errLastLoopHeaderFail = errors.New("get last loop header fail")
)

// Alien is the delegated-proof-of-stake consensus engine.
type Alien struct {
	config     *params.AlienConfig // Consensus engine configuration parameters
	db         ethdb.Database      // Database to store and retrieve snapshot checkpoints
	recents    *lru.ARCCache       // Snapshots for recent block to speed up reorgs
	signatures *lru.ARCCache       // Signatures of recent blocks to speed up mining
	signer     common.Address      // Ethereum address of the signing key
	signFn     SignerFn            // Signer function to authorize hashes with
	signTxFn   SignTxFn            // Sign transaction function to sign tx
	lock       sync.RWMutex        // Protects the signer fields
	lcsc       uint64              // Last confirmed side chain
}

// SignerFn hashes and signs the data to be signed by a backing account.
type SignerFn func(signer accounts.Account, mimeType string, message []byte) ([]byte, error)

// SignTxFn is a signTx
type SignTxFn func(accounts.Account, *types.Transaction, *big.Int) (*types.Transaction, error)

// sigHash returns the hash which is used as input for the delegated-proof-of-stake
// signing. It is the hash of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func sigHash(header *types.Header) (hash common.Hash, err error) {
	hasher := sha3.NewLegacyKeccak256()
	if err := rlp.Encode(hasher, []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}); err != nil {
		return common.Hash{}, err
	}

	hasher.Sum(hash[:0])
	return hash, nil
}

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigcache *lru.ARCCache) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigcache.Get(hash); known {
		return address.(common.Address), nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	headerSigHash, err := sigHash(header)
	if err != nil {
		return common.Address{}, err
	}
	pubkey, err := crypto.Ecrecover(headerSigHash.Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)
	return signer, nil
}

// New creates a Alien delegated-proof-of-stake consensus engine with the initial
// signers set to the ones provided by the user.
func New(config *params.AlienConfig, db ethdb.Database) *Alien {
	// Set any missing consensus parameters to their defaults
	conf := *config
	if conf.Epoch == 0 {
		conf.Epoch = defaultEpochLength
	}
	if conf.Period == 0 {
		conf.Period = defaultBlockPeriod
	}
	if conf.MaxSignerCount == 0 {
		conf.MaxSignerCount = defaultMaxSignerCount
	}
	if conf.MinVoterBalance.Uint64() > 0 {
		minVoterBalance = conf.MinVoterBalance
	}
	// Allocate the snapshot caches and create the engine
	recents, _ := lru.NewARC(inMemorySnapshots)
	signatures, _ := lru.NewARC(inMemorySignatures)

	return &Alien{
		config:     &conf,
		db:         db,
		recents:    recents,
		signatures: signatures,
	}
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (a *Alien) Author(header *types.Header) (common.Address, error) {
	return ecrecover(header, a.signatures)
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (a *Alien) VerifyHeader(chain consensus.ChainHeaderReader, state *state.StateDB, header *types.Header, seal bool) error {
	return a.verifyHeader(chain, state, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (a *Alien) VerifyHeaders(chain consensus.ChainHeaderReader, state *state.StateDB, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))

	go func() {
		for i, header := range headers {
			err := a.verifyHeader(chain, state, header, headers[:i])

			select {
			case <-abort:
				return
			case results <- err:
			}
		}
	}()
	return abort, results
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (a *Alien) verifyHeader(chain consensus.ChainHeaderReader, state *state.StateDB, header *types.Header, parents []*types.Header) error {
	if header.Number == nil {
		return errUnknownBlock
	}

	// Don't waste time checking blocks from the future
	if header.Time > uint64(time.Now().Unix()) {
		return consensus.ErrFutureBlock
	}

	// Check that the extra-data contains both the vanity and signature
	if len(header.Extra) < extraVanity {
		return errMissingVanity
	}
	if len(header.Extra) < extraVanity+extraSeal {
		return errMissingSignature
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}
	// Ensure that the block doesn't contain any uncles which are meaningless in PoA
	if header.UncleHash != uncleHash {
		return errInvalidUncleHash
	}

	// All basic checks passed, verify cascading fields
	return a.verifyCascadingFields(chain, state, header, parents)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (a *Alien) verifyCascadingFields(chain consensus.ChainHeaderReader, state *state.StateDB, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}
	// Ensure that the block's timestamp isn't too close to it's parent
	var parent *types.Header
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}
	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}
	if parent.Time > header.Time {
		return ErrInvalidTimestamp
	}
	// Retrieve the snapshot needed to verify this header and cache it
	_, err := a.snapshot(chain, number-1, header.ParentHash, parents, nil, defaultLoopCntRecalculateSigners)
	if err != nil {
		return err
	}

	// All basic checks passed, verify the seal and return
	return a.verifySeal(chain, state, header, parents)
}

// snapshot retrieves the authorization snapshot at a given point in time.
func (a *Alien) snapshot(chain consensus.ChainHeaderReader, number uint64, hash common.Hash, parents []*types.Header, genesisVotes []*Vote, lcrs uint64) (*Snapshot, error) {
	// Don't keep snapshot for side chain
	//if chain.Config().Alien.SideChain {
	//	return nil, nil
	//}
	// Search for a snapshot in memory or on disk for checkpoints
	var (
		headers []*types.Header
		snap    *Snapshot
	)

	for snap == nil {
		// If an in-memory snapshot was found, use that
		if s, ok := a.recents.Get(hash); ok {
			snap = s.(*Snapshot)
			break
		}
		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(a.config, a.signatures, a.db, hash); err == nil {
				log.Trace("Loaded voting snapshot from disk", "number", number, "hash", hash)
				snap = s
				break
			} else {
				log.Debug("Loaded voting snapshot from disk", "number", number, "err", err)
			}
		}
		// If we're at block zero, make a snapshot
		if number == 0 {
			genesis := chain.GetHeaderByNumber(0)
			if err := a.VerifyHeader(chain, nil, genesis, false); err != nil {
				return nil, err
			}
			a.config.Period = chain.Config().Alien.Period
			snap = newSnapshot(a.config, a.signatures, genesis.Hash(), genesisVotes, lcrs)
			if err := snap.store(a.db); err != nil {
				return nil, err
			}
			log.Trace("Stored genesis voting snapshot to disk")
			break
		}
		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		if len(parents) > 0 {
			// If we have explicit parents, pick from there (enforced)
			header = parents[len(parents)-1]
			if header.Hash() != hash || header.Number.Uint64() != number {
				return nil, consensus.ErrUnknownAncestor
			}
			parents = parents[:len(parents)-1]
		} else {
			// No explicit parents (or no more left), reach out to the database
			header = chain.GetHeader(hash, number)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}
		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
	}
	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers, a.db, chain)
	if err != nil {
		return nil, err
	}

	a.recents.Add(snap.Hash, snap)

	// If we've generated a new checkpoint snapshot, save to disk
	if snap.Number%checkpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(a.db); err != nil {
			return nil, err
		}
		log.Trace("Stored voting snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return snap, err
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (a *Alien) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > 0 {
		return errUnclesNotAllowed
	}
	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (a *Alien) VerifySeal(chain consensus.ChainHeaderReader, state *state.StateDB, header *types.Header) error {
	return a.verifySeal(chain, state, header, nil)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (a *Alien) verifySeal(chain consensus.ChainHeaderReader, state *state.StateDB, header *types.Header, parents []*types.Header) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := a.snapshot(chain, number-1, header.ParentHash, parents, nil, defaultLoopCntRecalculateSigners)
	if err != nil {
		return err
	}

	// Resolve the authorization key and check against signers
	signer, err := ecrecover(header, a.signatures)
	if err != nil {
		return err
	}

	// check the coinbase == signer
	if header.Number.Cmp(big.NewInt(bugFixBlockNumber)) > 0 {
		if signer != header.Coinbase {
			return errUnauthorized
		}
	}

	if !chain.Config().Alien.SideChain {

		if number > a.config.MaxSignerCount {
			var parent *types.Header
			if len(parents) > 0 {
				parent = parents[len(parents)-1]
			} else {
				parent = chain.GetHeader(header.ParentHash, number-1)
			}
			parentHeaderExtra := HeaderExtra{}
			err = decodeHeaderExtra(a.config, parent.Number, parent.Extra[extraVanity:len(parent.Extra)-extraSeal], &parentHeaderExtra)
			if err != nil {
				log.Info("Fail to decode parent header", "err", err)
				return err
			}
			currentHeaderExtra := HeaderExtra{}
			err = decodeHeaderExtra(a.config, header.Number, header.Extra[extraVanity:len(header.Extra)-extraSeal], &currentHeaderExtra)
			if err != nil {
				log.Info("Fail to decode header", "err", err)
				return err
			}
			// verify signerqueue
			if number%a.config.MaxSignerCount == 0 {
				if number < MinerUpdateStateFixBlockNumber {
					if state != nil {
						snap.updateMinerState(state)
					} else {
						log.Debug("verifySeal can't updateMinerState. stateDB is nil")
					}
				}

				err := snap.verifySignerQueue(currentHeaderExtra.SignerQueue)
				if err != nil {
					if number >= MinerUpdateStateFixBlockNumber {
						return err
					}

				}

			} else {
				for i := 0; i < int(a.config.MaxSignerCount); i++ {
					if parentHeaderExtra.SignerQueue[i] != currentHeaderExtra.SignerQueue[i] {
						return errInvalidSignerQueue
					}
				}
				if signer == parent.Coinbase && header.Time-parent.Time < chain.Config().Alien.Period {
					return errInvalidNeighborSigner
				}

			}

			// verify missing signer for punish
			var parentSignerMissing []common.Address
			if a.config.IsTrantor(header.Number) {
				var grandParentHeaderExtra HeaderExtra
				if number%a.config.MaxSignerCount == 1 {
					var grandParent *types.Header
					if len(parents) > 1 {
						grandParent = parents[len(parents)-2]
					} else {
						grandParent = chain.GetHeader(parent.ParentHash, number-2)
					}
					if grandParent == nil {
						return errLastLoopHeaderFail
					}
					err := decodeHeaderExtra(a.config, grandParent.Number, grandParent.Extra[extraVanity:len(grandParent.Extra)-extraSeal], &grandParentHeaderExtra)
					if err != nil {
						log.Info("Fail to decode parent header", "err", err)
						return err
					}
				}
				parentSignerMissing = getSignerMissingTrantor(parent.Coinbase, header.Coinbase, &parentHeaderExtra, &grandParentHeaderExtra)
			} else {
				newLoop := false
				if number%a.config.MaxSignerCount == 0 {
					newLoop = true
				}
				realityIndex := parent.Number.Uint64() % a.config.MaxSignerCount
				parentIndex := (parent.Time - parentHeaderExtra.LoopStartTime) / a.config.Period
				currentIndex := (header.Time - parentHeaderExtra.LoopStartTime) / a.config.Period
				parentSignerMissing = getSignerMissing(realityIndex, parentIndex, currentIndex, parent.Coinbase, header.Coinbase, parentHeaderExtra, newLoop)
			}

			if len(parentSignerMissing) != len(currentHeaderExtra.SignerMissing) {
				return errPunishedMissing
			}
			for i, signerMissing := range currentHeaderExtra.SignerMissing {
				if parentSignerMissing[i] != signerMissing {
					return errPunishedMissing
				}
			}

			// add LockReward check
			//err = a.VerifyLockReward(header, snap, chain, currentHeaderExtra)
			//if err != nil {
			//	return err
			//}
		}
		if !snap.inturn(signer, header.Time) {
			return errUnauthorized
		}
	} else {
		if notice, loopStartTime, period, signerLength, _, err := a.mcSnapshot(chain, signer, header.Time); err != nil {
			return err
		} else {
			mcLoopStartTime = loopStartTime
			mcPeriod = period
			mcSignerLength = signerLength
			// check gas charging
			if notice != nil {
				currentHeaderExtra := HeaderExtra{}
				err = decodeHeaderExtra(a.config, header.Number, header.Extra[extraVanity:len(header.Extra)-extraSeal], &currentHeaderExtra)
				if err != nil {
					return err
				}
				if len(notice.CurrentCharging) != len(currentHeaderExtra.SideChainCharging) {
					return errMCGasChargingInvalid
				} else {
					for _, charge := range currentHeaderExtra.SideChainCharging {
						if v, ok := notice.CurrentCharging[charge.Hash]; !ok {
							return err
						} else {
							if v.Volume != charge.Volume || v.Target != charge.Target {
								return errMCGasChargingInvalid
							}
						}
					}
				}

			}
		}
	}

	return nil
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (a *Alien) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {

	// Set the correct difficulty
	header.Difficulty = new(big.Int).Set(defaultDifficulty)

	number := header.Number.Uint64()
	if number >= StorageEffectBlockNumber {
		// Ensure the timestamp has the correct delay
		parent := chain.GetHeader(header.ParentHash, number-1)
		if parent == nil {
			return consensus.ErrUnknownAncestor
		}
		header.Time = parent.Time + uint64(a.config.Period)
		if header.Time < uint64(time.Now().Unix()) {
			header.Time = uint64(time.Now().Unix())
		}
	}
	// If now is later than genesis timestamp, skip prepare
	if a.config.GenesisTimestamp < uint64(time.Now().Unix()) {
		return nil
	}
	// Count down for start
	if header.Number.Uint64() == 1 {
		for {
			delay := time.Unix(int64(a.config.GenesisTimestamp-2), 0).Sub(time.Now())
			if delay <= time.Duration(0) {
				log.Info("Ready for seal block", "time", time.Now())
				break
			} else if delay > time.Duration(a.config.Period)*time.Second {
				delay = time.Duration(a.config.Period) * time.Second
			}
			log.Info("Waiting for seal block", "delay", common.PrettyDuration(time.Unix(int64(a.config.GenesisTimestamp-2), 0).Sub(time.Now())))
			select {
			case <-time.After(delay):
				continue
			}
		}
	}

	return nil
}

// get the snapshot info from main chain and check if current signer inturn, if inturn then update the info
func (a *Alien) mcSnapshot(chain consensus.ChainHeaderReader, signer common.Address, headerTime uint64) (*CCNotice, uint64, uint64, uint64, uint64, error) {

	if chain.Config().Alien.SideChain {
		chainHash := chain.GetHeaderByNumber(0).ParentHash
		ms, err := a.getMainChainSnapshotByTime(chain, headerTime, chainHash)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		} else if len(ms.Signers) == 0 {
			return nil, 0, 0, 0, 0, errSignerQueueEmpty
		} else if ms.Period == 0 {
			return nil, 0, 0, 0, 0, errMCPeriodMissing
		}

		loopIndex := int((headerTime-ms.LoopStartTime)/ms.Period) % len(ms.Signers)
		if loopIndex >= len(ms.Signers) {
			return nil, 0, 0, 0, 0, errInvalidSignerQueue
		} else if *ms.Signers[loopIndex] != signer {
			return nil, 0, 0, 0, 0, errUnauthorized
		}
		notice := &CCNotice{}
		if mcNotice, ok := ms.SCNoticeMap[chainHash]; ok {
			notice = mcNotice
		}
		return notice, ms.LoopStartTime, ms.Period, uint64(len(ms.Signers)), ms.Number, nil
	}
	return nil, 0, 0, 0, 0, errNotSideChain
}

func (a *Alien) parseNoticeInfo(notice *CCNotice) string {
	// if other notice exist, return string may be more than one
	if notice != nil {
		var charging []string
		for hash := range notice.CurrentCharging {
			charging = append(charging, hash.Hex())
		}
		return strings.Join(charging, "#")
	}
	return ""
}

func (a *Alien) getLastLoopInfo(chain consensus.ChainHeaderReader, header *types.Header) (string, error) {
	if chain.Config().Alien.SideChain && mcLoopStartTime != 0 && mcPeriod != 0 && a.config.Period != 0 {
		var loopHeaderInfo []string
		inLastLoop := false
		extraTime := (header.Time - mcLoopStartTime) % (mcPeriod * mcSignerLength)
		for i := uint64(0); i < a.config.MaxSignerCount*2*(mcPeriod/a.config.Period); i++ {
			header = chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
			if header == nil {
				return "", consensus.ErrUnknownAncestor
			}
			newTime := (header.Time - mcLoopStartTime) % (mcPeriod * mcSignerLength)
			if newTime > extraTime {
				if !inLastLoop {
					inLastLoop = true
				} else {
					break
				}
			}
			extraTime = newTime
			if inLastLoop {
				loopHeaderInfo = append(loopHeaderInfo, fmt.Sprintf("%d#%s", header.Number.Uint64(), header.Coinbase.Hex()))
			}
		}
		if len(loopHeaderInfo) > 0 {
			return strings.Join(loopHeaderInfo, "#"), nil
		}
	}
	return "", errGetLastLoopInfoFail
}

func (a *Alien) mcConfirmBlock(chain consensus.ChainHeaderReader, header *types.Header, notice *CCNotice) {

	a.lock.RLock()
	signer, signTxFn := a.signer, a.signTxFn
	a.lock.RUnlock()

	if signer != (common.Address{}) {
		// todo update gaslimit , gasprice ,and get ChainID need to get from mainchain
		if header.Number.Uint64() > a.lcsc && header.Number.Uint64() > a.config.MaxSignerCount*scUnconfirmLoop {
			nonce, err := a.getTransactionCountFromMainChain(chain, signer)
			if err != nil {
				log.Info("Confirm tx sign fail", "err", err)
				return
			}

			lastLoopInfo, err := a.getLastLoopInfo(chain, header)
			if err != nil {
				log.Info("Confirm tx sign fail", "err", err)
				return
			}

			chargingInfo := a.parseNoticeInfo(notice)

			txData := a.buildSCEventConfirmData(chain.GetHeaderByNumber(0).ParentHash, header.Number, new(big.Int).SetUint64(header.Time), lastLoopInfo, chargingInfo)
			tx := types.NewTransaction(nonce, header.Coinbase, big.NewInt(0), mcTxDefaultGasLimit, mcTxDefaultGasPrice, txData)

			if mcNetVersion == 0 {
				mcNetVersion, err = a.getNetVersionFromMainChain(chain)
				if err != nil {
					log.Info("Query main chain net version fail", "err", err)
				}
			}

			signedTx, err := signTxFn(accounts.Account{Address: signer}, tx, big.NewInt(int64(mcNetVersion)))
			if err != nil {
				log.Info("Confirm tx sign fail", "err", err)
			}
			txHash, err := a.sendTransactionToMainChain(chain, signedTx)
			if err != nil {
				log.Info("Confirm tx send fail", "err", err)
			} else {
				log.Info("Confirm tx result", "txHash", txHash)
				a.lcsc = header.Number.Uint64()
			}
		}
	}

}

func (a *Alien) GrantProfit(chain consensus.ChainHeaderReader, header *types.Header, state *state.StateDB) ([]consensus.GrantProfitRecord, []consensus.GrantProfitRecord) {
	var genesisVotes []*Vote
	number := header.Number.Uint64()
	if number == 1 {
		alreadyVote := make(map[common.Address]struct{})
		for _, unPrefixVoter := range a.config.SelfVoteSigners {
			voter := common.Address(unPrefixVoter)
			if _, ok := alreadyVote[voter]; !ok {
				genesisVotes = append(genesisVotes, &Vote{
					Voter:     voter,
					Candidate: voter,
					Stake:     state.GetBalance(voter),
				})
				alreadyVote[voter] = struct{}{}
			}
		}
	}
	// Assemble the voting snapshot to check which votes make sense
	snap, err := a.snapshot(chain, number-1, header.ParentHash, nil, genesisVotes, defaultLoopCntRecalculateSigners)
	if err != nil {
		log.Info("alien Finalize exit 3")
		return nil, nil
	}
	timeNow := time.Now()
	var playGrantProfit []consensus.GrantProfitRecord
	var currentGrantProfit []consensus.GrantProfitRecord
	payAddressAll := make(map[common.Address]*big.Int)
	for address, item := range snap.CandidatePledge {
		result, amount := paymentPledge(false, item, state, header, payAddressAll)
		if 0 == result {
			playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
				Which:           sscEnumCndLock,
				MinerAddress:    address,
				BlockNumber:     0,
				Amount:          new(big.Int).Set(amount),
				RevenueAddress:  item.RevenueAddress,
				RevenueContract: item.RevenueContract,
				MultiSignature:  item.MultiSignature,
			})

		} else if 1 == result {
			currentGrantProfit = append(currentGrantProfit, consensus.GrantProfitRecord{
				Which:           sscEnumCndLock,
				MinerAddress:    address,
				BlockNumber:     0,
				Amount:          new(big.Int).Set(amount),
				RevenueAddress:  item.RevenueAddress,
				RevenueContract: item.RevenueContract,
				MultiSignature:  item.MultiSignature,
			})
		}
	}
	for address, item := range snap.FlowPledge {
		result, amount := paymentPledge(true, item, state, header, payAddressAll)
		if 0 == result {
			playGrantProfit = append(playGrantProfit, consensus.GrantProfitRecord{
				Which:           sscEnumFlwLock,
				MinerAddress:    address,
				BlockNumber:     0,
				Amount:          new(big.Int).Set(amount),
				RevenueAddress:  item.RevenueAddress,
				RevenueContract: item.RevenueContract,
				MultiSignature:  item.MultiSignature,
			})
		} else if 1 == result {
			currentGrantProfit = append(currentGrantProfit, consensus.GrantProfitRecord{
				Which:           sscEnumFlwLock,
				MinerAddress:    address,
				BlockNumber:     0,
				Amount:          new(big.Int).Set(amount),
				RevenueAddress:  item.RevenueAddress,
				RevenueContract: item.RevenueContract,
				MultiSignature:  item.MultiSignature,
			})
		}
	}
	currentGrantProfit, playGrantProfit, err = snap.FlowRevenue.payProfit(a.db, chain.Config().Alien.Period, number, currentGrantProfit, playGrantProfit, header, state, payAddressAll)
	if err != nil {
		log.Warn("worker GrantProfit payProfit", "err", err)
	}
	toPayAddressBalance(header, payAddressAll, state)
	log.Info("payProfit payAddressAll", "len(payAddressAll)", len(payAddressAll), "elapsed", time.Since(timeNow), "number", header.Number.Uint64())
	return currentGrantProfit, playGrantProfit
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given.
func (a *Alien) Finalize(chain consensus.ChainHeaderReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt, grantProfit []consensus.GrantProfitRecord, gasReward *big.Int) error {
	number := header.Number.Uint64()

	// Mix digest is reserved for now, set to empty
	header.MixDigest = common.Hash{}

	// Ensure the timestamp has the correct delay
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrPrunedAncestor
	}
	header.Time = parent.Time + a.config.Period
	if int64(header.Time) < time.Now().Unix() {
		header.Time = uint64(time.Now().Unix())
	}

	// Ensure the extra data has all it's components
	if len(header.Extra) < extraVanity {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, extraVanity-len(header.Extra))...)
	}
	header.Extra = header.Extra[:extraVanity]

	// genesisVotes write direct into snapshot, which number is 1
	var genesisVotes []*Vote
	parentHeaderExtra := HeaderExtra{}
	currentHeaderExtra := HeaderExtra{}

	if number == 1 {
		alreadyVote := make(map[common.Address]struct{})
		for _, unPrefixVoter := range a.config.SelfVoteSigners {
			voter := common.Address(unPrefixVoter)
			if _, ok := alreadyVote[voter]; !ok {
				genesisVotes = append(genesisVotes, &Vote{
					Voter:     voter,
					Candidate: voter,
					Stake:     state.GetBalance(voter),
				})
				alreadyVote[voter] = struct{}{}
			}
		}
	} else {
		// decode extra from last header.extra
		err := decodeHeaderExtra(a.config, parent.Number, parent.Extra[extraVanity:len(parent.Extra)-extraSeal], &parentHeaderExtra)
		if err != nil {
			log.Info("Fail to decode parent header", "err", err)
			return err
		}
		currentHeaderExtra.ConfirmedBlockNumber = parentHeaderExtra.ConfirmedBlockNumber
		currentHeaderExtra.SignerQueue = parentHeaderExtra.SignerQueue
		currentHeaderExtra.LoopStartTime = parentHeaderExtra.LoopStartTime

		if a.config.IsTrantor(header.Number) {
			var grandParentHeaderExtra HeaderExtra
			if number%a.config.MaxSignerCount == 1 {
				grandParent := chain.GetHeader(parent.ParentHash, number-2)
				if grandParent == nil {
					return errLastLoopHeaderFail
				}
				err := decodeHeaderExtra(a.config, grandParent.Number, grandParent.Extra[extraVanity:len(grandParent.Extra)-extraSeal], &grandParentHeaderExtra)
				if err != nil {
					log.Warn("Fail to decode parent header", "err", err)
					return err
				}
			}
			currentHeaderExtra.SignerMissing = getSignerMissingTrantor(parent.Coinbase, header.Coinbase, &parentHeaderExtra, &grandParentHeaderExtra)
		} else {
			newLoop := false
			if number%a.config.MaxSignerCount == 0 {
				newLoop = true
			}
			realityIndex := parent.Number.Uint64() % a.config.MaxSignerCount
			parentIndex := (parent.Time - parentHeaderExtra.LoopStartTime) / a.config.Period
			currentIndex := (header.Time - parentHeaderExtra.LoopStartTime) / a.config.Period
			currentHeaderExtra.SignerMissing = getSignerMissing(realityIndex, parentIndex, currentIndex, parent.Coinbase, header.Coinbase, parentHeaderExtra, newLoop)
		}

	}

	// Assemble the voting snapshot to check which votes make sense
	snap, err := a.snapshot(chain, number-1, header.ParentHash, nil, genesisVotes, defaultLoopCntRecalculateSigners)
	if err != nil {
		return err
	}
	if !chain.Config().Alien.SideChain {
		var es LockState
		if number >= StorageEffectBlockNumber {
			es, err = NewLockState(parentHeaderExtra.ExtraStateRoot, parentHeaderExtra.LockAccountsRoot, number)
			if err != nil {
				//log.Error("extrastate open failed", "root", parent.MixDigest, "err", err)
				return err
			}
		}
		// calculate votes write into header.extra
		mcCurrentHeaderExtra, refundGas, err := a.processCustomTx(currentHeaderExtra, chain, header, state, txs, receipts)
		if err != nil {
			return err
		}
		currentHeaderExtra = mcCurrentHeaderExtra
		currentHeaderExtra.ConfirmedBlockNumber = snap.getLastConfirmedBlockNumber(currentHeaderExtra.CurrentBlockConfirmations).Uint64()
		// write signerQueue in first header, from self vote signers in genesis block
		if number == 1 {
			currentHeaderExtra.LoopStartTime = a.config.GenesisTimestamp
			if len(a.config.SelfVoteSigners) > 0 {
				for i := 0; i < int(a.config.MaxSignerCount); i++ {
					currentHeaderExtra.SignerQueue = append(currentHeaderExtra.SignerQueue, common.Address(a.config.SelfVoteSigners[i%len(a.config.SelfVoteSigners)]))
				}
			}
		} else if number%a.config.MaxSignerCount == 0 {
			//currentHeaderExtra.LoopStartTime = header.Time.Uint64()
			currentHeaderExtra.LoopStartTime = currentHeaderExtra.LoopStartTime + a.config.Period*a.config.MaxSignerCount
			// create random signersQueue in currentHeaderExtra by snapshot.Tall
			snap1 := snap.copy()
			if number < MinerUpdateStateFixBlockNumber {
				currentHeaderExtra.MinerStake = snap1.updateMinerState(state)
			}
			currentHeaderExtra.SignerQueue = []common.Address{}
			newSignerQueue, err := snap1.createSignerQueue()
			if err != nil {
				return err
			}
			currentHeaderExtra.SignerQueue = newSignerQueue
		}

		// play pledge
		currentHeaderExtra.GrantProfit = []consensus.GrantProfitRecord{}
		if nil != grantProfit {
			currentHeaderExtra.GrantProfit = append(currentHeaderExtra.GrantProfit, grantProfit...)
		}
		if number >= PosrIncentiveEffectNumber {
			currentHeaderExtra.GrantProfitHash = snap.calGrantProfitHash(currentHeaderExtra.GrantProfit)
			currentHeaderExtra.GrantProfit = []consensus.GrantProfitRecord{}
		}
		flowHarvest := big.NewInt(0)
		// Accumulate any block rewards and commit the final state root
		currentHeaderExtra.LockReward, flowHarvest = accumulateRewards(currentHeaderExtra.LockReward, chain.Config(), state, header, snap, refundGas, gasReward)
		if nil == currentHeaderExtra.FlowHarvest {
			currentHeaderExtra.FlowHarvest = new(big.Int).Set(flowHarvest)
		} else {
			currentHeaderExtra.FlowHarvest = new(big.Int).Add(currentHeaderExtra.FlowHarvest, flowHarvest)
		}

		for proposer, refund := range snap.calculateProposalRefund() {
			state.AddBalance(proposer, refund)
		}
		if es != nil {
			harvest := big.NewInt(0)
			var revertSrt []ExchangeSRTRecord
			snap1 := snap.copy()
			leftAmount := common.Big0
			curLeaseSpace := common.Big0
			currentHeaderExtra.LockReward, revertSrt, harvest, err, leftAmount, curLeaseSpace = snap1.storageVerificationCheck(header.Number.Uint64(), snap1.getBlockPreDay(), a.db, currentHeaderExtra.LockReward, state)
			if err != nil {
				return err
			}
			if nil != revertSrt {
				currentHeaderExtra.ExchangeSRT = append(currentHeaderExtra.ExchangeSRT, revertSrt...)
			}
			if nil != harvest {
				if nil == currentHeaderExtra.FlowHarvest {
					currentHeaderExtra.FlowHarvest = new(big.Int).Set(harvest)
				} else {
					currentHeaderExtra.FlowHarvest = new(big.Int).Add(currentHeaderExtra.FlowHarvest, harvest)
				}
			}
			if isGEInitStorageManagerNumber(number) {
				snap1.accumulateSpExitBurnAmount(header.Number, state)
				if isSpVerificationCheck(header.Number.Uint64(), snap.Period) {
					snap1.spAccumulatePublish(header.Number)
				}
				if isSpDelExit(header.Number.Uint64(), snap.Period) {
					snap1.dealSpExitFinalize(header.Number, header.Hash())
				}
			}

			currentHeaderExtra.LockReward, err = es.AddLockReward(currentHeaderExtra.LockReward, snap1, a.db, number)
			if err != nil {
				return err
			}
			err = es.PayLockReward(parentHeaderExtra.LockAccountsRoot, number, state)
			if err != nil {
				log.Error("extrastate addlockreward", "error", err)
				return err
			}
			stateRoot, lockAccountRoot, err := es.CommitData()
			if err != nil {
				log.Info("extrastate commit failed", "error", err)
				return err
			}

			currentHeaderExtra.ExtraStateRoot = stateRoot
			currentHeaderExtra.LockAccountsRoot = lockAccountRoot
			currentHeaderExtra.StorageDataRoot = snap1.StorageData.Hash
			if snap1.SpData != nil {
				currentHeaderExtra.SpDataRoot = snap1.SpData.Hash
			}
			if leftAmount != nil && leftAmount.Cmp(common.Big0) > 0 {
				state.AddBalance(common.BigToAddress(big.NewInt(0)), leftAmount)
			}
			log.Info("extrastate commit", "number", number, "esstateRoot", stateRoot, "lockaccountsRoot", lockAccountRoot)
			if header.Number.Uint64() >= PledgeRevertLockEffectNumber {
				currentHeaderExtra.SRTDataRoot = snap1.SRT.Root()
			}
			if isGEPOSNewEffect(number) {
				currentHeaderExtra.CandidateAutoExit, currentHeaderExtra.CandidatePEntrustExit = snap1.checkCandidateAutoExit(header.Number.Uint64(), currentHeaderExtra.CandidateAutoExit, state, currentHeaderExtra.CandidatePEntrustExit)
			}
			if isGEPoCrsAccCalNumber(number) {
				if nil != curLeaseSpace && curLeaseSpace.Cmp(common.Big0) > 0 {
					if nil == currentHeaderExtra.CurLeaseSpace {
						currentHeaderExtra.CurLeaseSpace = new(big.Int).Set(curLeaseSpace)
					} else {
						currentHeaderExtra.CurLeaseSpace = new(big.Int).Add(currentHeaderExtra.CurLeaseSpace, curLeaseSpace)
					}
				}
			}
		}

		a.RepairBal(state, number)
		if number%(snap.config.MaxSignerCount*snap.LCRS) == (snap.config.MaxSignerCount*snap.LCRS - 1) {
			if number > tallyRevenueEffectBlockNumber {
				if number < PosNewEffectNumber {
					currentHeaderExtra.ModifyPredecessorVotes = snap.updateTallyState(state)
				} else {
					currentHeaderExtra.ModifyPredecessorVotes = snap.updateTallyStateV2()
				}

			}
			if number >= MinerUpdateStateFixBlockNumber {
				if number < PosNewEffectNumber {
					currentHeaderExtra.MinerStake = snap.updateMinerState(state)
				} else {
					currentHeaderExtra.MinerStake = snap.updateMinerStateV2()
				}
			}
		}
	} else {
		// use currentHeaderExtra.SignerQueue as signer queue
		currentHeaderExtra.SignerQueue = append([]common.Address{header.Coinbase}, parentHeaderExtra.SignerQueue...)
		if len(currentHeaderExtra.SignerQueue) > int(a.config.MaxSignerCount) {
			currentHeaderExtra.SignerQueue = currentHeaderExtra.SignerQueue[:int(a.config.MaxSignerCount)]
		}
		sideChainRewards(chain.Config(), state, header, snap)
	}
	// encode header.extra
	currentHeaderExtraEnc, err := encodeHeaderExtra(a.config, header.Number, currentHeaderExtra)
	if err != nil {
		return err
	}

	header.Extra = append(header.Extra, currentHeaderExtraEnc...)
	header.Extra = append(header.Extra, make([]byte, extraSeal)...)

	header.Root = state.IntermediateRoot(chain.Config().IsEIP158(header.Number))
	return nil
}

func (a *Alien) FinalizeAndAssemble(chain consensus.ChainHeaderReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt, grantProfit []consensus.GrantProfitRecord, gasReward *big.Int) (*types.Block, error) {
	err := a.Finalize(chain, header, state, txs, uncles, receipts, grantProfit, gasReward)
	if nil != err {
		return nil, err
	}
	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, nil, receipts, trie.NewStackTrie(nil)), nil
}

// Authorize injects a private key into the consensus engine to mint new blocks with.
func (a *Alien) Authorize(signer common.Address, signFn SignerFn, signTxFn SignTxFn) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.signer = signer
	a.signFn = signFn
	a.signTxFn = signTxFn
}

// ApplyGenesis
func (a *Alien) ApplyGenesis(chain consensus.ChainHeaderReader, genesisHash common.Hash) error {
	if a.config.LightConfig != nil {
		var genesisVotes []*Vote
		alreadyVote := make(map[common.Address]struct{})
		for _, unPrefixVoter := range a.config.SelfVoteSigners {
			voter := common.Address(unPrefixVoter)
			if genesisAccount, ok := a.config.LightConfig.Alloc[unPrefixVoter]; ok {
				if _, ok := alreadyVote[voter]; !ok {
					stake := new(big.Int)
					stake.UnmarshalText([]byte(genesisAccount.Balance))
					genesisVotes = append(genesisVotes, &Vote{
						Voter:     voter,
						Candidate: voter,
						Stake:     stake,
					})
					alreadyVote[voter] = struct{}{}
				}
			}
		}
		// Assemble the voting snapshot to check which votes make sense
		if _, err := a.snapshot(chain, 0, genesisHash, nil, genesisVotes, defaultLoopCntRecalculateSigners); err != nil {
			return err
		}
		return nil
	}
	return errMissingGenesisLightConfig
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (a *Alien) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()
	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	if a.config.Period == 0 && len(block.Transactions()) == 0 {
		return errWaitTransactions
	}
	// Don't hold the signer fields for the entire sealing procedure
	a.lock.RLock()
	signer, signFn := a.signer, a.signFn
	a.lock.RUnlock()

	// Bail out if we're unauthorized to sign a block
	snap, err := a.snapshot(chain, number-1, header.ParentHash, nil, nil, defaultLoopCntRecalculateSigners)
	if err != nil {
		return err
	}

	if !chain.Config().Alien.SideChain {
		if !snap.inturn(signer, header.Time) {
			//			<-stop
			return errUnauthorized
		}
	} else {
		if notice, loopStartTime, period, signerLength, _, err := a.mcSnapshot(chain, signer, header.Time); err != nil {
			//			<-stop
			return err
		} else {
			mcLoopStartTime = loopStartTime
			mcPeriod = period
			mcSignerLength = signerLength
			if notice != nil {
				// rebuild the header.Extra for gas charging
				currentHeaderExtra := HeaderExtra{}
				if err = decodeHeaderExtra(a.config, header.Number, header.Extra[extraVanity:len(header.Extra)-extraSeal], &currentHeaderExtra); err != nil {
					return err
				}
				for _, charge := range notice.CurrentCharging {
					currentHeaderExtra.SideChainCharging = append(currentHeaderExtra.SideChainCharging, charge)
				}
				currentHeaderExtraEnc, err := encodeHeaderExtra(a.config, header.Number, currentHeaderExtra)
				if err != nil {
					return err
				}
				header.Extra = header.Extra[:extraVanity]
				header.Extra = append(header.Extra, currentHeaderExtraEnc...)
				header.Extra = append(header.Extra, make([]byte, extraSeal)...)
			}
			// send tx to main chain to confirm this block
			a.mcConfirmBlock(chain, header, notice)
		}
	}

	// correct the time
	delay := time.Unix(int64(header.Time), 0).Sub(time.Now())

	sighash, err := signFn(accounts.Account{Address: signer}, accounts.MimetypeAlien, AlienRLP(header))
	if err != nil {
		return err
	}

	copy(header.Extra[len(header.Extra)-extraSeal:], sighash)
	// Wait until sealing is terminated or delay timeout.
	log.Trace("Waiting for slot to sign and propagate", "delay", common.PrettyDuration(delay))
	go func() {
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}

		select {
		case results <- block.WithSeal(header):
		default:
			log.Warn("Sealing result is not read by miner", "sealhash", SealHash(header))
		}
	}()

	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func (a *Alien) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {

	return new(big.Int).Set(defaultDifficulty)
}

// SealHash returns the hash of a block prior to it being sealed.
func (a *Alien) SealHash(header *types.Header) common.Hash {
	return SealHash(header)
}

// Close implements consensus.Engine. It's a noop for clique as there are no background threads.
func (a *Alien) Close() error {
	return nil
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (a *Alien) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "alien",
		Version:   ufoVersion,
		Service:   &API{chain: chain, alien: a, sCache: list.New()},
		Public:    false,
	}}
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header)
	hasher.Sum(hash[:0])
	return hash
}

// CliqueRLP returns the rlp bytes which needs to be signed for the proof-of-authority
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func AlienRLP(header *types.Header) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header)
	return b.Bytes()
}

func encodeSigHeader(w io.Writer, header *types.Header) {
	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-crypto.SignatureLength], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}
	if header.BaseFee != nil {
		enc = append(enc, header.BaseFee)
	}
	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

func sideChainRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, snap *Snapshot) {
	// vanish gas fee
	gasUsed := new(big.Int).SetUint64(header.GasUsed)
	if state.GetBalance(header.Coinbase).Cmp(gasUsed) >= 0 {
		state.SubBalance(header.Coinbase, gasUsed)
	}
	// gas charging
	for target, volume := range snap.calculateGasCharging() {
		state.AddBalance(target, volume)
	}
}

func paymentReward(minerAddress common.Address, amount *big.Int, state *state.StateDB, snap *Snapshot) {
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return
	}
	if revenue, ok := snap.RevenueNormal[minerAddress]; ok {
		nilHash := common.Address{}
		zeroHash := common.BigToAddress(big.NewInt(0))
		if nilHash == revenue.MultiSignature || zeroHash == revenue.MultiSignature {
			state.AddBalance(revenue.RevenueAddress, amount)
		} else {
			state.AddBalance(revenue.MultiSignature, amount)
		}
	} else {
		state.AddBalance(minerAddress, amount)
	}
}

func caclPayPeriodAmount(pledge *PledgeItem, headerNumber *big.Int) *big.Int {
	lockExpire := new(big.Int).Add(big.NewInt(int64(pledge.StartHigh)), big.NewInt(int64(pledge.LockPeriod)))
	amount := big.NewInt(0)
	if 0 == pledge.RlsPeriod || 0 == pledge.Interval || 0 <= headerNumber.Cmp(new(big.Int).Add(lockExpire, big.NewInt(int64(pledge.RlsPeriod)))) {
		amount = new(big.Int).Sub(pledge.Amount, pledge.Playment)
	} else {
		currentPeriod := new(big.Int).Div(new(big.Int).Sub(headerNumber, lockExpire), big.NewInt(int64(pledge.Interval)))
		totalPeriod := (pledge.RlsPeriod + pledge.Interval - 1) / pledge.Interval
		totalPlayment := new(big.Int).Div(new(big.Int).Mul(pledge.Amount, currentPeriod), big.NewInt(int64(totalPeriod)))
		amount = new(big.Int).Sub(totalPlayment, pledge.Playment)
	}
	return amount
}

func paymentPledge(hasContract bool, pledge *PledgeItem, state *state.StateDB, header *types.Header, payAddressAll map[common.Address]*big.Int) (int, *big.Int) {
	nilHash := common.Address{}
	if 0 == pledge.StartHigh {
		return -1, nil
	}
	lockExpire := new(big.Int).Add(big.NewInt(int64(pledge.StartHigh)), big.NewInt(int64(pledge.LockPeriod)))
	if 0 > header.Number.Cmp(lockExpire) {
		return -1, nil
	}
	amount := caclPayPeriodAmount(pledge, header.Number)
	if amount.Cmp(big.NewInt(0)) <= 0 {
		if islockSimplifyEffectBlocknumber(header.Number.Uint64()) {
			return -1, nil
		}
		return 0, amount
	}
	zeroHash := common.BigToAddress(big.NewInt(0))
	payAddress := nilHash
	if !hasContract || isRevenueContractNil(pledge, header.Number.Uint64()) {
		if nilHash == pledge.MultiSignature || zeroHash == pledge.MultiSignature {
			payAddress = pledge.RevenueAddress
		} else {
			payAddress = pledge.MultiSignature
		}
		if isGrantProfitOneTimeBlockNumber(header) {
			payAmount := new(big.Int).Set(amount)
			burnAmount := calBurnAmount(pledge, amount)
			if burnAmount.Cmp(common.Big0) > 0 {
				addPayAddressBalance(pledge.BurnAddress, payAddressAll, burnAmount)
				payAmount = new(big.Int).Sub(payAmount, burnAmount)
			}
			addPayAddressBalance(payAddress, payAddressAll, payAmount)
			return 0, amount
		} else {
			state.AddBalance(payAddress, amount)
			log.Info("pay", "Address", payAddress, "amount", amount)
			return 0, amount
		}

	}
	return 1, amount
}

func calPaymentPledge(pledge *PledgeItem, header *types.Header) *big.Int {
	if 0 == pledge.StartHigh {
		return nil
	}
	lockExpire := new(big.Int).Add(big.NewInt(int64(pledge.StartHigh)), big.NewInt(int64(pledge.LockPeriod)))
	if 0 > header.Number.Cmp(lockExpire) {
		return nil
	}
	amount := caclPayPeriodAmount(pledge, header.Number)
	if amount.Cmp(big.NewInt(0)) <= 0 {
		return nil
	}
	return amount
}

func nYearBandwidthReward(n float64) decimal.Decimal {
	onecut := float64(1) - math.Pow(float64(0.5), n/float64(2.5))
	yearScale := decimal.NewFromFloat(onecut)
	yearReward := yearScale.Mul(decimal.NewFromBigInt(totalBandwidthReward, 0))
	return yearReward
}

func accumulateBandwidthRewards(currentLockReward []LockRewardRecord, config *params.ChainConfig, header *types.Header, snap *Snapshot, db ethdb.Database) ([]LockRewardRecord, *big.Int) {
	blockNumPerYear := secondsPerYear / config.Alien.Period
	yearCount := header.Number.Uint64() / blockNumPerYear
	var yearReward decimal.Decimal
	yearCount++
	if yearCount == 1 {
		yearReward = nYearBandwidthReward(float64(yearCount))
	} else {
		yearReward = nYearBandwidthReward(float64(yearCount)).Sub(nYearBandwidthReward(float64(yearCount - 1)))
	}
	bandwidthReward := yearReward.Div(decimal.NewFromInt(365))
	totalBandwidth := big.NewInt(0)
	for _, bandwidth := range snap.Bandwidth {
		totalBandwidth = new(big.Int).Add(totalBandwidth, big.NewInt(int64(bandwidth.BandwidthClaimed)))
	}
	flowHarvest := big.NewInt(0)
	for minerAddress, bandwidth := range snap.Bandwidth {
		reward := bandwidthReward.Mul(decimal.NewFromInt(int64(bandwidth.BandwidthClaimed))).Div(decimal.NewFromBigInt(totalBandwidth, 0)).BigInt()
		currentLockReward = append(currentLockReward, LockRewardRecord{
			Target:   minerAddress,
			Amount:   new(big.Int).Set(reward),
			IsReward: sscEnumBandwidthReward,
		})
		flowHarvest = new(big.Int).Add(flowHarvest, reward)
	}
	return currentLockReward, flowHarvest
}

// Which EB is the current flow obtained
func getFlowRewardScale(flowTotal decimal.Decimal) decimal.Decimal {
	totalEb := flowTotal.Div(decimal.NewFromInt(1073741824 * 1024))
	var nebCount = totalEb.Round(0)
	if totalEb.Cmp(nebCount) >= 0 {
		nebCount = nebCount.Add(decimal.NewFromInt(1))
	}
	//flow reward power
	var powern, _ = nebCount.Div(decimal.NewFromFloat(44.389)).Float64()
	//At the nth EB, the cumulative total number of passes issued by flow mining   tfn :=PFn*(1-power(0.5, n/44.389)
	tfnCount := decimal.NewFromBigInt(totalFlowReward, 0).Mul(decimal.NewFromFloat(1).Sub(decimal.NewFromFloat(math.Pow(0.5, powern))))
	//At the n_1th EB, the cumulative total number of passes issued by flow mining   tfn :=PFn*(1-power(0.5, (n-1)/44.389)
	var powern1, _ = nebCount.Sub(decimal.NewFromFloat(1)).Div(decimal.NewFromFloat(44.389)).Float64()
	tfn1count := decimal.NewFromBigInt(totalFlowReward, 0).Mul(decimal.NewFromFloat(1).Sub(decimal.NewFromFloat(math.Pow(0.5, powern1))))
	//Get NFC reward per M traffic
	return tfnCount.Sub(tfn1count).Div(decimal.NewFromInt(1073741824 * 1024))

}

//
//// AccumulateRewards credits the coinbase of the given block with the mining reward.
//func accumulateRewards(currentLockReward []LockRewardRecord, config *params.ChainConfig, header *types.Header, snap *Snapshot) []LockRewardRecord {
//	// Calculate the block reword by year
//	blockNumPerYear := secondsPerYear / config.Alien.Period
//	yearCount := header.Number.Uint64() / blockNumPerYear
//	if yearCount*blockNumPerYear != header.Number.Uint64() {
//		yearCount++
//	}
//
//	//Calculate the cumulative reward in year n  TB_n=PB_n×(1−0.5^n/6)
//	tbncount := decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromFloat(1).Sub(decimal.NewFromFloat(math.Pow(0.5, float64(yearCount)/float64(6)))))
//	//Calculate the cumulative reward in year n-1  TB_n_1=PB_n×(1−0.5^(n-1)/6)
//	tbn_1count := decimal.NewFromBigInt(totalBlockReward, 0).Mul(decimal.NewFromFloat(1).Sub(decimal.NewFromFloat(math.Pow(0.5, float64(yearCount-1)/float64(6)))))
//	blockReward := (tbncount.Sub(tbn_1count)).Div(decimal.NewFromInt(int64(blockNumPerYear)))
//	minerReward := blockReward.BigInt()
//	log.Info("block with the mining reward", "number", header.Number.Uint64(), "header.Coinbase", header.Coinbase, "minerReward", minerReward)
//
//	// calc total bandwidth
//	flowHarvest := big.NewInt(0)
//	for _, bandwidth := range snap.Bandwidth {
//		flowHarvest = new(big.Int).Add(flowHarvest, big.NewInt(int64(bandwidth.BandwidthClaimed)))
//	}
//	snap.FlowHarvest = new(big.Int).Set(flowHarvest)
//	// rewards for the miner, check minerReward value for refund gas
//	currentLockReward = append(currentLockReward, LockRewardRecord{
//		Target:   header.Coinbase,
//		Amount:   new(big.Int).Set(minerReward),
//		IsReward: sscEnumSignerReward,
//	})
//	return currentLockReward
//}

// AccumulateRewards credits the coinbase of the given block with the mining reward.
func accumulateRewards(currentLockReward []LockRewardRecord, config *params.ChainConfig, state *state.StateDB, header *types.Header, snap *Snapshot, refundGas RefundGas, gasReward *big.Int) ([]LockRewardRecord, *big.Int) {
	n := new(big.Int).Div(header.Number, big.NewInt(420000))
	if new(big.Int).Mul(n, big.NewInt(420000)).Cmp(header.Number) < 0 {
		n = new(big.Int).Add(n, big.NewInt(1))
	}
	rewardScale, _ := big.NewFloat(1e+18 * 0.5 * math.Pow(0.96, float64(n.Int64()-1))).Int64()
	minerReward := big.NewInt(rewardScale)
	// refund gas for custom txs
	for sender, gas := range refundGas {
		state.AddBalance(sender, gas)
		if 0 < minerReward.Cmp(gas) {
			minerReward.Sub(minerReward, gas)
		}
	}
	balance := state.GetBalance(header.Coinbase)
	if nil == gasReward {
		currentLockReward = append(currentLockReward, LockRewardRecord{
			Target:   header.Coinbase,
			Amount:   new(big.Int).Set(minerReward),
			IsReward: sscEnumSignerReward,
		})
	} else if 0 < balance.Cmp(gasReward) {
		state.SubBalance(header.Coinbase, gasReward)
		if isGTPOSRNewCalEffect(header.Number.Uint64()) {
			halfGasReward := new(big.Int).Div(gasReward, common.Big2)
			state.AddBalance(common.BigToAddress(big.NewInt(0)), halfGasReward)
			gasReward = new(big.Int).Sub(gasReward, halfGasReward)
		}
		minerReward = new(big.Int).Add(minerReward, gasReward)
		currentLockReward = append(currentLockReward, LockRewardRecord{
			Target:   header.Coinbase,
			Amount:   new(big.Int).Set(minerReward),
			IsReward: sscEnumSignerReward,
		})
	} else {
		state.SubBalance(header.Coinbase, balance)
		gasReward = new(big.Int).Sub(gasReward, balance)
		if 0 < minerReward.Cmp(gasReward) {
			minerReward = new(big.Int).Sub(minerReward, gasReward)
			currentLockReward = append(currentLockReward, LockRewardRecord{
				Target:   header.Coinbase,
				Amount:   new(big.Int).Set(minerReward),
				IsReward: sscEnumSignerReward,
			})
		}
	}
	log.Info("block with the mining reward", "number", header.Number.Uint64(), "header.Coinbase", header.Coinbase, "minerReward", minerReward, "gasReward", gasReward)
	return currentLockReward, minerReward
}

// Get the signer missing from last signer till header.Coinbase
func getSignerMissing(realityIndex uint64, parentIndex uint64, currentIndex uint64, lastSigner common.Address, currentSigner common.Address, extra HeaderExtra, newLoop bool) []common.Address {

	var signerMissing []common.Address
	/*
		if newLoop {
			for i, qlen := 0, len(extra.SignerQueue); i < len(extra.SignerQueue); i++ {
				if lastSigner == extra.SignerQueue[qlen-1-i] {
					break
				} else {
					signerMissing = append(signerMissing, extra.SignerQueue[qlen-1-i])
				}
			}
		} else {
			recordMissing := false
			for _, signer := range extra.SignerQueue {
				if signer == lastSigner {
					recordMissing = true
					continue
				}
				if signer == currentSigner {
					break
				}
				if recordMissing {
					signerMissing = append(signerMissing, signer)
				}
			}

		}
	*/
	if newLoop {
		for i := 0; i < len(extra.SignerQueue)-int(realityIndex)-1; i++ {
			signerMissing = append(signerMissing, extra.SignerQueue[(int(parentIndex+1)+i)%len(extra.SignerQueue)])
		}
	} else {
		for i := int(parentIndex) + 1; i < int(currentIndex); i++ {
			signerMissing = append(signerMissing, extra.SignerQueue[i%len(extra.SignerQueue)])
		}
	}
	return signerMissing
}

// Get the signer missing from last signer till header.Coinbase
func getSignerMissingTrantor(lastSigner common.Address, currentSigner common.Address, extra *HeaderExtra, gpExtra *HeaderExtra) []common.Address {

	var signerMissing []common.Address
	signerQueue := append(extra.SignerQueue, extra.SignerQueue...)
	if gpExtra != nil {
		for i, v := range gpExtra.SignerQueue {
			if v == lastSigner {
				signerQueue[i] = lastSigner
				signerQueue = signerQueue[i:]
				break
			}
		}
	}

	recordMissing := false
	for _, signer := range signerQueue {
		if !recordMissing && signer == lastSigner {
			recordMissing = true
			continue
		}
		if recordMissing && signer == currentSigner {
			break
		}
		if recordMissing {
			signerMissing = append(signerMissing, signer)
		}
	}

	return signerMissing

}

func (a *Alien) VerifyHeaderExtra(chain consensus.ChainHeaderReader, header *types.Header, verifyExtra []byte) error {
	err := doVerifyHeaderExtra(header, verifyExtra, a)
	if err != nil {
		log.Error("VerifyHeaderExtra error", "error", err)
	}
	return err
}

func doVerifyHeaderExtra(header *types.Header, verifyExtra []byte, a *Alien) error {
	currentHExtra := HeaderExtra{}
	err := decodeHeaderExtra(a.config, header.Number, header.Extra[extraVanity:len(header.Extra)-extraSeal], &currentHExtra)
	if err != nil {
		log.Warn("Fail to decode current header", "err", err, "extra len", len(header.Extra), "extra", header.Extra)
		return err
	}
	verifyHExtra := HeaderExtra{}
	err = decodeHeaderExtra(a.config, header.Number, verifyExtra[extraVanity:len(verifyExtra)-extraSeal], &verifyHExtra)
	if err != nil {
		log.Warn("Fail to decode verify header", "err", err, "extra len", len(verifyExtra), "extra", verifyExtra)
		return err
	}
	return verifyHeaderExtern(&currentHExtra, &verifyHExtra)
}

func addPayAddressBalance(addBalanceAddress common.Address, payAddressAll map[common.Address]*big.Int, amount *big.Int) {
	if amount.Cmp(common.Big0) <= 0 {
		return
	}
	if _, ok := payAddressAll[addBalanceAddress]; !ok {
		payAddressAll[addBalanceAddress] = amount
	} else {
		payAddressAll[addBalanceAddress] = new(big.Int).Add(payAddressAll[addBalanceAddress], amount)
	}
	return
}

func toPayAddressBalance(header *types.Header, payAddressAll map[common.Address]*big.Int, state *state.StateDB) {
	if isGrantProfitOneTimeBlockNumber(header) {
		for payAddress, amount := range payAddressAll {
			state.AddBalance(payAddress, amount)
			log.Info("payAddressAll", "payAddress", payAddress, "amount", amount)
		}
	}
	return
}

func isGrantProfitOneTimeBlockNumber(header *types.Header) bool {
	if header.Number.Uint64() > grantProfitOneTimeBlockNumber {
		return true
	}
	return false
}

func isRevenueContractNil(pledge *PledgeItem, number uint64) bool {
	if isGEInitStorageManagerNumber(number) && (pledge.PledgeType == sscEnumSTEntrustExitLock || pledge.PledgeType == sscEnumSTEntrustLockReward || pledge.PledgeType == sscSpLockReward || pledge.PledgeType == sscSpEntrustLockReward ||
		pledge.PledgeType == sscSpEntrustExitLockReward || pledge.PledgeType == sscSpExitLockReward || pledge.PledgeType == sscEnumSignerReward || pledge.PledgeType == sscEnumPosExitLock) {
		return true
	} else {
		if isGEPOSNewEffect(number) && (pledge.PledgeType == sscEnumSignerReward || pledge.PledgeType == sscEnumPosExitLock) {
			return true
		} else {
			nilHash := common.Address{}
			zeroHash := common.BigToAddress(big.NewInt(0))
			return nilHash == pledge.RevenueContract || zeroHash == pledge.RevenueContract
		}
	}
}
