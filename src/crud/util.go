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
  data, ok := m["data"].(string)
  if ok == false {
    data = ""
  }

  return models.Transaction {
    Type: m["type"].(string),
    Version: m["version"].(string),
    FromAddress: m["fromaddress"].(string),
    ToAddress: m["toaddress"].(string),
    Value: m["value"].(string),
    StepLimit: m["steplimit"].(string),
    Timestamp: m["timestamp"].(string),
    BlockTimestamp: uint64(m["blocktimestamp"].(int64)),
    Nid: uint32(m["nid"].(int64)),
    Nonce: uint64(m["nonce"].(int64)),
    Hash: m["hash"].(string),
    TransactionIndex: uint32(m["transactionindex"].(int64)),
    BlockHash: m["blockhash"].(string),
    BlockNumber: uint64(m["blocknumber"].(int64)),
    Fee: m["fee"].(string),
    Signature: m["signature"].(string),
    DataType: m["datatype"].(string),
    Data: data,
    ReceiptCumulativeStepUsed: m["receiptcumulativestepused"].(string),
    ReceiptStepUsed: m["receiptstepused"].(string),
    ReceiptStepPrice: m["receiptstepprice"].(string),
    ReceiptScoreAddress: m["receiptscoreaddress"].(string),
    ReceiptLogs: m["receiptlogs"].(string),
    ReceiptStatus: uint32(m["receiptstatus"].(int64)),
    ItemId: m["itemid"].(string),
    ItemTimestamp: m["itemtimestamp"].(string),
  }
}
