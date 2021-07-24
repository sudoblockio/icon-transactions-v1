package fixtures

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/models"
)

type Fixtures []Fixture
type Fixture struct {
	Input    map[string]interface{}
	Expected map[string]interface{}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func LoadTestFixtures(file string) (Fixtures, error) {
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

func ReadCurrentDir() {
	file, err := os.Open(".")
	if err != nil {
		zap.S().Fatalf("failed opening directory: %s", err)
	}
	defer file.Close()

	list, _ := file.Readdirnames(0) // 0 to read all files and folders
	for _, name := range list {
		zap.S().Info(name)
	}
}

func (f *Fixture) GetTransaction(data map[string]interface{}) *models.Transaction {
	block := models.Transaction{
		Type:                      data["type"].(string),
		Version:                   returnString(data["version"]),
		FromAddress:               data["from_address"].(string),
		ToAddress:                 data["to_address"].(string),
		Value:                     data["value"].(string),
		StepLimit:                 returnString(data["step_limit"]),
		Timestamp:                 data["timestamp"].(string),
		BlockTimestamp:            data["block_timestamp"].(float64),
		Nid:                       returnString(data["nid"]),
		Nonce:                     returnString(data["nonce"]),
		Hash:                      data["hash"].(string),
		TransactionIndex:          data["transaction_index"].(float64),
		BlockHash:                 data["block_hash"].(string),
		BlockNumber:               data["block_number"].(float64),
		Fee:                       data["fee"].(string),
		Signature:                 data["signature"].(string),
		DataType:                  returnString(data["data_type"]),
		Data:                      returnBytes(data["data"]),
		ReceiptCumulativeStepUsed: data["receipt_cumulative_step_used"].(string),
		ReceiptStepUsed:           data["receipt_step_used"].(string),
		ReceiptStepPrice:          data["receipt_step_price"].(string),
		ReceiptScoreAddress:       returnString(data["receipt_score_address"]),
		ReceiptLogs:               returnString(data["receipt_logs"]),
		ReceiptStatus:             data["receipt_status"].(float64),
		ItemId:                    data["item_id"].(string),
		ItemTimestamp:             data["item_timestamp"].(string),
	}
	return &block
}

func returnString(i interface{}) string {
	if i == nil {
		return ""
	}
	return i.(string)
}

func returnBytes(i interface{}) []byte {
	if i == nil {
		return []byte{}
	}
	return i.([]byte)
}
