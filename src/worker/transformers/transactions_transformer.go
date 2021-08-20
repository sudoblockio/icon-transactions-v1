package transformers

import (
	"encoding/hex"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
)

func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := "transactions"

	// Check topic names
	if utils.StringInSlice(consumerTopicNameTransactions, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", consumerTopicNameTransactions, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}

	// Input channels
	consumerTopicChanTransactions := make(chan *sarama.ConsumerMessage)

	// Output channels
	transactionLoaderChan := crud.GetTransactionModel().WriteChan
	transactionCountLoaderChan := crud.GetTransactionCountModel().WriteChan

	// Register input channels
	broadcaster_output_chan_id_transaction := kafka.Broadcasters[consumerTopicNameTransactions].AddBroadcastChannel(consumerTopicChanTransactions)
	defer func() {
		kafka.Broadcasters[consumerTopicNameTransactions].RemoveBroadcastChannel(broadcaster_output_chan_id_transaction)
	}()

	zap.S().Debug("Transactions Transformer: started working")
	for {
		// Read from kafka
		var consumerTopicMsg *sarama.ConsumerMessage
		var transaction *models.Transaction

		consumerTopicMsg = <-consumerTopicChanTransactions
		// Transaction message from ETL
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Transactions Transformer: Processing transaction hash=", transactionRaw.Hash)
		if err != nil {
			zap.S().Fatal("Transactions Transformer: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		// Transform logic
		transaction = transformTransactionRaw(transactionRaw)

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

func convertBytesToTransactionRawProtoBuf(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}
	err := proto.Unmarshal(value[6:], &tx)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &tx, err
}

// Business logic goes here
func transformTransactionRaw(txRaw *models.TransactionRaw) *models.Transaction {

	return &models.Transaction{
		Type:                      txRaw.Type,
		Version:                   txRaw.Version,
		FromAddress:               txRaw.FromAddress,
		ToAddress:                 txRaw.ToAddress,
		Value:                     txRaw.Value,
		StepLimit:                 txRaw.StepLimit,
		Timestamp:                 txRaw.Timestamp,
		BlockTimestamp:            txRaw.BlockTimestamp,
		Nid:                       txRaw.Nid,
		Nonce:                     txRaw.Nonce,
		Hash:                      txRaw.Hash,
		TransactionIndex:          txRaw.TransactionIndex,
		BlockHash:                 txRaw.BlockHash,
		BlockNumber:               txRaw.BlockNumber,
		Fee:                       txRaw.Fee,
		Signature:                 txRaw.Signature,
		DataType:                  txRaw.DataType,
		Data:                      txRaw.Data,
		ReceiptCumulativeStepUsed: txRaw.ReceiptCumulativeStepUsed,
		ReceiptStepUsed:           txRaw.ReceiptStepUsed,
		ReceiptStepPrice:          txRaw.ReceiptStepPrice,
		ReceiptScoreAddress:       txRaw.ReceiptScoreAddress,
		ReceiptLogs:               txRaw.ReceiptLogs,
		ReceiptStatus:             txRaw.ReceiptStatus,
		ItemId:                    txRaw.ItemId,
		ItemTimestamp:             txRaw.ItemTimestamp,
	}
}
