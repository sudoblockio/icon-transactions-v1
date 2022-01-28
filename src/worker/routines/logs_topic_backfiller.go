package routines

import (
	"encoding/hex"
	"encoding/json"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func StartLogsTopicBackfiller() {

	// routine every day
	go logsTopicBackfiller()
}

func logsTopicBackfiller() {

	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumer.TopicChannels[consumerTopicNameLogs]

	logsProccessed := uint64(0)
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanLogs
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Logs Transformer: Processing log in tx hash=", logRaw.TransactionHash)
		if err != nil {
			zap.S().Fatal("Unable to proceed cannot convert kafka msg value to LogRaw, err: ", err.Error())
		}

		// Token Transfer
		tokenTransfer := transformLogRawToTokenTransfer(logRaw)
		if tokenTransfer != nil {
			crud.GetTokenTransferModel().UpsertOne(tokenTransfer)
		}

		logsProccessed += 1
		if logsProccessed%1000 == 0 {
			zap.S().Info("LogMissingBlockNumbers: Processed ", logsProccessed, " logs...")
		}
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

func transformLogRawToTokenTransfer(logRaw *models.LogRaw) *models.TokenTransfer {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	if indexed[0] != "Transfer(Address,Address,int,bytes)" || len(indexed) != 4 {
		// Not token transfer
		return nil
	}

	// Token Contract Address
	tokenContractAddress := logRaw.Address

	// Token Contract Symbol
	tokenContractSymbol, err := utils.IconNodeServiceGetTokenContractSymbol(tokenContractAddress)
	if err != nil {
		zap.S().Fatal(err)
	}

	return &models.TokenTransfer{
		TransactionHash:     logRaw.TransactionHash,
		LogIndex:            int32(logRaw.LogIndex),
		TokenContractSymbol: tokenContractSymbol,
	}
}
