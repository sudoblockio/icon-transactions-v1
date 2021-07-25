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
	Input    models.Transaction
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

func (f *Fixture) GetTransaction(data models.Transaction) *models.Transaction {
	block := models.Transaction{
		Type:                      data.Type,
		Version:                   data.Version,
		FromAddress:               data.FromAddress,
		ToAddress:                 data.ToAddress,
		Value:                     data.Value,
		StepLimit:                 data.StepLimit,
		Timestamp:                 data.Timestamp,
		BlockTimestamp:            data.BlockTimestamp,
		Nid:                       data.Nid,
		Nonce:                     data.Nonce,
		Hash:                      data.Hash,
		TransactionIndex:          data.TransactionIndex,
		BlockHash:                 data.BlockHash,
		BlockNumber:               data.BlockNumber,
		Fee:                       data.Fee,
		Signature:                 data.Signature,
		DataType:                  data.DataType,
		Data:                      data.Data,
		ReceiptCumulativeStepUsed: data.ReceiptCumulativeStepUsed,
		ReceiptStepUsed:           data.ReceiptStepUsed,
		ReceiptStepPrice:          data.ReceiptStepPrice,
		ReceiptScoreAddress:       data.ReceiptScoreAddress,
		ReceiptLogs:               data.ReceiptLogs,
		ReceiptStatus:             data.ReceiptStatus,
		ItemId:                    data.ItemId,
		ItemTimestamp:             data.ItemTimestamp,
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

//func returnStructpb(i interface{}) *structpb.Struct {
//	log.Println(i)
//	c := structpb.Struct{}
//	c.Fields = i
//}
