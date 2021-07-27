package crud

import (
	"go.mongodb.org/mongo-driver/bson"

	"github.com/geometry-labs/icon-transactions/models"
)

func convertMapToBsonD(v map[string]interface{}) (*bson.D, error) {
  var doc *bson.D
  var err error

	data, err := bson.Marshal(v)
	if err != nil {
		return doc, err
	}

	err = bson.Unmarshal(data, &doc)
	return doc, err
}

func convertBsonMToTransaction(m bson.M) models.Transaction {

  // Data field may be null
  var data []byte
  dataString, ok := m["data"].(string)
  if ok == false {
    data = []byte{}
  } else {
    data = []byte(dataString)
  }

  return models.Transaction {
    Type: m["type"].(string),
    Version: m["version"].(string),
    FromAddress: m["fromaddress"].(string),
    ToAddress: m["toaddress"].(string),
    Value: m["value"].(string),
    StepLimit: m["steplimit"].(string),
    Timestamp: m["timestamp"].(string),
    BlockTimestamp: m["blocktimestamp"].(float64),
    Nid: m["nid"].(string),
    Nonce: m["nonce"].(float64),
    Hash: m["hash"].(string),
    TransactionIndex: m["transactionindex"].(float64),
    BlockHash: m["blockhash"].(string),
    BlockNumber: m["blocknumber"].(float64),
    Fee: m["fee"].(string),
    Signature: m["signature"].(string),
    DataType: m["datatype"].(string),
    Data: data,
    ReceiptCumulativeStepUsed: m["receiptcumulativestepused"].(string),
    ReceiptStepUsed: m["receiptstepused"].(string),
    ReceiptStepPrice: m["receiptstepprice"].(string),
    ReceiptScoreAddress: m["receiptscoreaddress"].(string),
    ReceiptLogs: m["receiptlogs"].(string),
    ReceiptStatus: m["receiptstatus"].(float64),
    ItemId: m["itemid"].(string),
    ItemTimestamp: m["itemtimestamp"].(string),
  }
}
