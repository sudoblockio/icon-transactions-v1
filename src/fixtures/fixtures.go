package fixtures

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/models"
)

const (
	transactionRawFixturesPath = "transactions_raw.json"
)

// Fixtures - slice of Fixture
type Fixtures []Fixture

// Fixture - loaded from fixture file
type Fixture map[string]interface{}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// LoadTransactionFixtures - load transaction fixtures from disk
func LoadTransactionFixtures() []*models.Transaction {
	transactions := make([]*models.Transaction, 0)

	fixtures, err := loadFixtures(transactionRawFixturesPath)
	check(err)

	for _, fixture := range fixtures {
		transactions = append(transactions, parseFixtureToTransaction(fixture))
	}

	return transactions
}

func loadFixtures(file string) (Fixtures, error) {
	var fs Fixtures

	dat, err := ioutil.ReadFile(getFixtureDir() + file)
	check(err)
	err = json.Unmarshal(dat, &fs)

	return fs, err
}

func getFixtureDir() string {

	callDir, _ := os.Getwd()
	callDirSplit := strings.Split(callDir, "/")

	for i := len(callDirSplit) - 1; i >= 0; i-- {
		if callDirSplit[i] != "src" {
			callDirSplit = callDirSplit[:len(callDirSplit)-1]
		} else {
			break
		}
	}

	callDirSplit = append(callDirSplit, "fixtures")
	fixtureDir := strings.Join(callDirSplit, "/")
	fixtureDir = fixtureDir + "/"
	zap.S().Info(fixtureDir)

	return fixtureDir
}

func parseFixtureToTransaction(m map[string]interface{}) *models.Transaction {

  // These fields may be null
  nonce, ok := m["nonce"].(float64)
  if ok == false {
    nonce = 0
  }
  fee, ok := m["fee"].(string)
  if ok == false {
    fee = ""
  }
  itemTimestamp, ok := m["itemtimestamp"].(string)
  if ok == false {
    itemTimestamp = ""
  }

  return &models.Transaction {
    Type: m["type"].(string),
    Version: m["version"].(string),
    FromAddress: m["from_address"].(string),
    ToAddress: m["to_address"].(string),
    Value: m["value"].(string),
    StepLimit: m["step_limit"].(string),
    Timestamp: m["timestamp"].(string),
    BlockTimestamp: uint64(m["block_timestamp"].(float64)),
    Nid: uint32(m["nid"].(float64)),
    Nonce: uint64(nonce),
    Hash: m["hash"].(string),
    TransactionIndex: uint32(m["transaction_index"].(float64)),
    BlockHash: m["block_hash"].(string),
    BlockNumber: uint64(m["block_number"].(float64)),
    Fee: fee,
    Signature: m["signature"].(string),
    DataType: m["data_type"].(string),
    Data: m["data"].(string),
    ReceiptCumulativeStepUsed: m["receipt_cumulative_step_used"].(string),
    ReceiptStepUsed: m["receipt_step_used"].(string),
    ReceiptStepPrice: m["receipt_step_price"].(string),
    ReceiptScoreAddress: m["receipt_score_address"].(string),
    ReceiptLogs: m["receipt_logs"].(string),
    ReceiptStatus: uint32(m["receipt_status"].(float64)),
    ItemId: m["item_id"].(string),
    ItemTimestamp: itemTimestamp,
  }
}
