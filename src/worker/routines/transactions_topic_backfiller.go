package routines

import (
	"encoding/hex"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func StartTransactionMissingBlockNumbers() {

	// routine every day
	go transactionMissingBlockNumbers()
}

func transactionMissingBlockNumbers() {

	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumer.TopicChannels[consumerTopicNameTransactions]

	// Loop every duration
	txProccessed := uint64(0)
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanTransactions
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Transactions Transformer: Processing transaction hash=", transactionRaw.Hash)
		if err != nil {
			zap.S().Fatal("Transactions Transformer: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		if transactionRaw.FromAddress != "None" {
			txFromAddress := &models.TransactionCountByAddressIndex{
				TransactionHash: transactionRaw.Hash,
				Address:         transactionRaw.FromAddress,
				BlockNumber:     transactionRaw.BlockNumber,
			}
			crud.GetTransactionCountByAddressIndexModel().UpsertOne(txFromAddress)
		}

		if transactionRaw.ToAddress != "None" {
			txToAddress := &models.TransactionCountByAddressIndex{
				TransactionHash: transactionRaw.Hash,
				Address:         transactionRaw.ToAddress,
				BlockNumber:     transactionRaw.BlockNumber,
			}
			crud.GetTransactionCountByAddressIndexModel().UpsertOne(txToAddress)
		}

		txProccessed += 1
		if txProccessed%1000 == 0 {
			zap.S().Info("TransactionMissingBlockNumbers: Processed ", txProccessed, " transactions...")
		}
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
