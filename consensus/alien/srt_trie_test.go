package alien

import (
	"context"
	"fmt"
	"github.com/UltronGlow/UltronGlow-Origin/common"
	"github.com/UltronGlow/UltronGlow-Origin/core/rawdb"
	"github.com/UltronGlow/UltronGlow-Origin/core/types"
	"github.com/UltronGlow/UltronGlow-Origin/crypto"
	"github.com/UltronGlow/UltronGlow-Origin/ethclient"
	"github.com/UltronGlow/UltronGlow-Origin/ethdb"
	"github.com/UltronGlow/UltronGlow-Origin/trie"
	"log"
	"math/big"
	"os"
	"os/user"
	"testing"
)

var  (
	address1 = common.HexToAddress("0x823140710bf13990e4500136726d8b55")
	address2 = common.HexToAddress("0x823140710bf13990e4500136726d8b56")
	address3 = common.HexToAddress("0x823140710bf13990e4500136726d8b57")
)

func TestSrtExTx(t *testing.T) {
	from := common.HexToAddress("UXe96cE9aA288ED1cF42d4939193c142848Cf7C0B7")
	to := common.HexToAddress("UXe96cE9aA288ED1cF42d4939193c142848Cf7C0B7")
	privateKey, _ := crypto.HexToECDSA("cb48f903f85ac392c07199444ffed718bdf6b916fb98f38ed2beb3fbdb95d10f")

	client, err := ethclient.Dial("http://192.168.9.114:8545")
	if err != nil {
		log.Fatal("dail rpc server failed:", err)
		return
	}
	defer client.Close()

	nonce, err := client.PendingNonceAt(context.Background(), from)
	if err != nil {
		log.Fatal("get nonce failed:", err)
		return
	}

	value := big.NewInt(0)
	gasLimit := uint64(210000) // in units
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Fatal("get gasprice failed:", err)
		return
	}

	txData := []byte("UTG:1:Exch:e96cE9aA288ED1cF42d4939193c142848Cf7C0B7:100")
	tx := types.NewTransaction(nonce, to, value, gasLimit, gasPrice, txData)
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(int64(188))), privateKey)
	if err != nil {
		log.Fatal("signed tx failed:", err)
		return
	}
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal("send tx failed:", err)
		return
	}

/*	recp, err := bind.WaitMined(context.Background(),client,signedTx)
	if err != nil {
		log.Fatal("get receipt failed:", err)
	}
	log.Println("tx receipt", recp.TxHash)*/
	return
}


func TestSrtExTx2(t *testing.T) {
	from := common.HexToAddress("UXfEe28DA813980ED992c1000dB494cd83e14151bB")
	to := common.HexToAddress("UXfEe28DA813980ED992c1000dB494cd83e14151bB")
	privateKey, _ := crypto.HexToECDSA("31f4867f4c6dbcbde8ef1c838781aa08f962984eddd53605b4e2dc0216db68fa")

	client, err := ethclient.Dial("http://192.168.9.114:8545")
	if err != nil {
		log.Fatal("dail rpc server failed:", err)
		return
	}
	defer client.Close()

	nonce, err := client.PendingNonceAt(context.Background(), from)
	if err != nil {
		log.Fatal("get nonce failed:", err)
		return
	}

	value := big.NewInt(0)
	gasLimit := uint64(210000) // in units
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Fatal("get gasprice failed:", err)
		return
	}

	txData := []byte("UTG:1:Exch:UXfEe28DA813980ED992c1000dB494cd83e14151bB:10")
	tx := types.NewTransaction(nonce, to, value, gasLimit, gasPrice, txData)
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(int64(188))), privateKey)
	if err != nil {
		log.Fatal("signed tx failed:", err)
		return
	}
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal("send tx failed:", err)
		return
	}

	/*	recp, err := bind.WaitMined(context.Background(),client,signedTx)
		if err != nil {
			log.Fatal("get receipt failed:", err)
		}
		log.Println("tx receipt", recp.TxHash)*/
	return
}

func _TestTriy(t *testing.T) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()
	address1 := common.HexToAddress("0x823140710bf13990e4500136726d8b55")

	trie, err := NewSrtTrie(common.Hash{}, ethdb)
	if err != nil {
		t.Error("open trie failed", err)
		return
	}
	trie.setBalance(address1, common.Big3)
	root := trie.Hash()
	trie.commit()
	t.Log("root:", root)
	trie.addBalance(address1, common.Big2)
	root2 := trie.Hash()
	t.Log("root:", root2)
	//trie.addBalance(address2, common.Big2)
	//trie.addBalance(address3, common.Big2)
	root, err = trie.commit()
	if err != nil {
		t.Log("commit trie failed, err=", err)
		return
	}
	t.Log("commit root =", root)

	amount := trie.getBalance(address1)
	t.Log("addr:", address1, "balance:", amount)
}

func _TestLoadTrie(t *testing.T) {
	ethdb, err := newEthDataBase("")
	if err != nil {
		t.Log("newEthDataBase failed:", err)
		return
	}
	defer ethdb.Close()

	root := common.HexToHash("ux537b2408a944b71ec1cea89e437203814601a46edadbf44ba0b693e9a7aa49e4")
	tr, err := NewSrtTrie(root, ethdb)
	if err != nil {
		t.Error("open trie failed, err =", err)
		return
	}

	//amount := tr.getBalance(address1)
	//t.Log("addr:", address1, "balance:", amount)

	found := make(map[string]string)
	it := trie.NewIterator(tr.trie.NodeIterator(nil))
	for it.Next() {
		found[string(it.Key)] = string(it.Value)
		acc := decodeSrtAccount(it.Value)
		if nil != acc {
			t.Log("addr:", acc.Address, "balance:", acc.Balance)
		}
	}
}

func newEthDataBase(path string) (ethdb.Database, error) {
	var (
		//db  ethdb.DatabaseA
		err error
		dbpath string
	)
	if path == "" {
		dbpath = "e:\\home\\utg\\extrastate"
	}else {
		dbpath = path
	}
	ldb, err := rawdb.NewLevelDBDatabase(dbpath, 1, 0, "extrastate", false)
	if nil != err {
		panic(fmt.Sprintf("open extrastate database failed, err=%s", err.Error()))
	}

	return ldb, nil
}

func homeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}
