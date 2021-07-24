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
		Type: data["type"].(string),
		Version: data["version"].(string),
		FromAddress: data["fromAddress"].(string),
		ToAddress: data["toAddress"].(string),
		Value: data["value"].(string),
		StepLimit: data["stepLimit"].(string),
		Timestamp: data["timestamp"].(string),
		BlockTimestamp: data["blockTimestamp"].(uint64),
		Nid: data["nid"].(string),
		Nonce: data["nonce"].(string),
		Hash: data["hash"].(string),
		TransactionIndex: data["transactionIndex"].(uint64),
		BlockHash: data["blockHash"].(string),
		BlockNumber: data["blockNumber"].(uint64),
		Fee: data["fee"].(string),
		Signature: data["signature"].(string),
		DataType: data["dataType"].(string),
		Data: data["data"].([]byte),
		ReceiptCumulativeStepUsed: data["receiptCumulativeStepUsed"].(string),
		ReceiptStepUsed: data["receiptStepUsed"].(string),
		ReceiptStepPrice: data["receiptStepPrice"].(string),
		ReceiptScoreAddress: data["receiptScoreAddress"].(string),
		ReceiptLogs: data["receiptLogs"].(string),
		ReceiptStatus: data["receiptStatus"].(uint32),
		ItemId: data["itemId"].(string),
		ItemTimestamp: data["itemTimestamp"].(string),
	}
	return &block
}

func returnString(interface{} i) {

}