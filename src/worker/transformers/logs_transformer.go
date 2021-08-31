package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartLogsTransformer() {
	go logsTransformer()
}

func logsTransformer() {
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChan

	// Output channels
	transactionLoaderChan := crud.GetTransactionModel().WriteChan
	transactionCountLoaderChan := crud.GetTransactionCountModel().WriteChan

	zap.S().Debug("Logs Transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var transaction *models.Transaction

		consumerTopicMsg = <-consumerTopicChanLogs
		// Log message from ETL
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Logs Transformer: Processing log in tx hash=", logRaw.TransactionHash)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to LogRaw, err: ", err.Error())
		}

		transaction = transformLogRaw(logRaw)

		// Not and internal transaction
		if transaction == nil {
			continue
		}

		// Load log counter to Postgres
		transactionCount := &models.TransactionCount{
			Count: 1, // Adds with current
			Id:    1, // Only one row
		}
		transactionCountLoaderChan <- transactionCount

		// Load to Postgres
		transactionLoaderChan <- transaction
	}
}

func convertBytesToLogRawProtoBuf(value []byte) (*models.LogRaw, error) {
	log := models.LogRaw{}
	err := proto.Unmarshal(value[6:], &log)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &log, err
}

// Business logic goes here
func transformLogRaw(logRaw *models.LogRaw) *models.Transaction {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	method := strings.Split(indexed[0], "(")[0]

	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	return &models.Transaction{
		Type:                      logRaw.Type,
		Version:                   "",
		FromAddress:               indexed[1],
		ToAddress:                 indexed[2],
		Value:                     indexed[3],
		StepLimit:                 0,
		Timestamp:                 "",
		BlockTimestamp:            logRaw.BlockTimestamp,
		Nid:                       0,
		Nonce:                     "",
		Hash:                      logRaw.TransactionHash,
		TransactionIndex:          logRaw.TransactionIndex,
		BlockHash:                 logRaw.BlockHash,
		BlockNumber:               logRaw.BlockNumber,
		Fee:                       0,
		Signature:                 "",
		DataType:                  "",
		Data:                      logRaw.Data,
		ReceiptCumulativeStepUsed: 0,
		ReceiptStepUsed:           0,
		ReceiptStepPrice:          0,
		ReceiptScoreAddress:       "",
		ReceiptLogs:               "",
		ReceiptStatus:             1,
		ItemId:                    logRaw.ItemId,
		ItemTimestamp:             logRaw.ItemTimestamp,
		LogIndex:                  int32(logRaw.LogIndex),
	}
}
