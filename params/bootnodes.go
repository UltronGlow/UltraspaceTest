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

package params

import "github.com/UltronGlow/UltronGlow-Origin/common"

// MainnetBootnodes are the enode URLs of the P2P bootstrap nodes running on
// the main utg network.
var MainnetBootnodes = []string{
	// utg Foundation Go Bootnodes
	"enode://996b48b1d2cc3866c8d54decd0e3b2c2700faaed104b044e84d208588f4875ba4bdcaaef69460fd3f723b5c4763dc8af69c88b278e50d700627cd10b45ca4adb@65.19.174.250:30350",
	"enode://403c1bea029ff44bf50d60a4e14386f16195f99201bdd096ca3b15321b072e58f566d0d69a5899e428b470c15a44b5ac1e516238ca394a6d88cabc1128c51661@65.19.174.250:30351",
	"enode://827592c0c32549b9a833be2b1a179bcd55080ff2954c4367294971d72a28d1f51a5c68209c669c88df0aa429029b642fa9db4653854c39feccf4900ab2d025e2@65.19.174.250:30352",
	"enode://f65677b975b04bc479453c5e0eefc5288b2c3f41906ce6dd85b5d03d2147ecb4444839108ddbfb1eec8f1a3a98a9cd42f6d69bf73f8e5aa6f017309d6bd994d5@3.134.9.128:30321",
	"enode://6fdb74a1c310d403b9edf746078948c93ced61a068bedeb0c7689a1d572b09e6bfe1e1fb8919860faf47ff000fec2a4ad992ae1ae0e8a9da7d4722c82c714ac9@3.23.245.208:30321",
}

// TestnetBootnodes are the enode URLs of the P2P bootstrap nodes running on the
// Testnet test network.
var TestnetBootnodes = []string{
	"enode://2ffed1bb6b475259c1448dc93b639569886999e51ade144451877a706d2a9b71eff8eb067d289fde48ba4807370034d851553746fac8816af27f5a922703e2e4@127.0.0.1:30321",
}

var V5Bootnodes = []string{
}

const dnsPrefix = "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@"

// KnownDNSNetwork returns the address of a public DNS-based node list for the given
// genesis hash and protocol. See https://github.com/ethereum/discv4-dns-lists for more
// information.
func KnownDNSNetwork(genesis common.Hash, protocol string) string {
	var net string
	switch genesis {
	case MainnetGenesisHash:
		net = "mainnet"
	case TestnetGenesisHash:
		net = "testnet"
	default:
		return ""
	}
	return dnsPrefix + protocol + "." + net + ".ethdisco.net"
}
