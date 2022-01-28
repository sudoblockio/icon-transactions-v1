package routines

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func StartLogMissingBlockNumbers() {

	// routine every day
	go logMissingBlockNumbers()
}

func logMissingBlockNumbers() {

	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumer.TopicChannels[consumerTopicNameLogs]

	// Loop every duration
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

		// Internal Transaction
		transaction := transformLogRawToTransaction(logRaw)
		if transaction != nil {
			if transaction.FromAddress != "None" {
				txFromAddress := &models.TransactionInternalCountByAddressIndex{
					TransactionHash: transaction.Hash,
					LogIndex:        uint64(transaction.LogIndex),
					Address:         transaction.FromAddress,
					BlockNumber:     transaction.BlockNumber,
				}
				crud.GetTransactionInternalCountByAddressIndexModel().UpsertOne(txFromAddress)
			}

			if transaction.ToAddress != "None" {
				txToAddress := &models.TransactionInternalCountByAddressIndex{
					TransactionHash: transaction.Hash,
					LogIndex:        uint64(transaction.LogIndex),
					Address:         transaction.ToAddress,
					BlockNumber:     transaction.BlockNumber,
				}
				crud.GetTransactionInternalCountByAddressIndexModel().UpsertOne(txToAddress)
			}
		}

		// Token Transfer
		tokenTransfer := transformLogRawToTokenTransfer(logRaw)
		if tokenTransfer != nil {
			if tokenTransfer.FromAddress != "None" {
				txFromAddress := &models.TokenTransferCountByAddressIndex{
					TransactionHash: tokenTransfer.TransactionHash,
					LogIndex:        uint64(tokenTransfer.LogIndex),
					Address:         tokenTransfer.FromAddress,
					BlockNumber:     tokenTransfer.BlockNumber,
				}
				crud.GetTokenTransferCountByAddressIndexModel().UpsertOne(txFromAddress)
			}

			if tokenTransfer.ToAddress != "None" {
				txToAddress := &models.TokenTransferCountByAddressIndex{
					TransactionHash: tokenTransfer.TransactionHash,
					LogIndex:        uint64(tokenTransfer.LogIndex),
					Address:         tokenTransfer.ToAddress,
					BlockNumber:     tokenTransfer.BlockNumber,
				}
				crud.GetTokenTransferCountByAddressIndexModel().UpsertOne(txToAddress)
			}
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

func transformLogRawToTransaction(logRaw *models.LogRaw) *models.Transaction {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	// Method
	method := strings.Split(indexed[0], "(")[0]
	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	// From Address
	fromAddress := indexed[1]

	// To Address
	toAddress := indexed[2]

	// Value
	value := indexed[3]

	// Transaction Decimal Value
	// Hex -> float64
	valueDecimal := utils.StringHexToFloat64(value, 18)

	return &models.Transaction{
		Type:                      logRaw.Type,
		Version:                   "",
		FromAddress:               fromAddress,
		ToAddress:                 toAddress,
		Value:                     value,
		StepLimit:                 0,
		Timestamp:                 "",
		BlockTimestamp:            logRaw.BlockTimestamp,
		Nid:                       0,
		Nonce:                     "",
		Hash:                      logRaw.TransactionHash,
		TransactionIndex:          logRaw.TransactionIndex,
		BlockHash:                 logRaw.BlockHash,
		BlockNumber:               logRaw.BlockNumber,
		TransactionFee:            "",
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
		Method:                    method,
		ValueDecimal:              valueDecimal,
	}
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

	// From Address
	fromAddress := indexed[1]

	// To Address
	toAddress := indexed[2]

	// Value
	value := indexed[3]

	// Transaction Decimal Value
	// NOTE every token has a different decimal base
	tokenDecimalBase, err := utils.IconNodeServiceGetTokenDecimalBase(tokenContractAddress)
	if err != nil {
		zap.S().Fatal(err)
	}

	valueDecimal := utils.StringHexToFloat64(value, tokenDecimalBase)

	// Block Timestamp
	blockTimestamp := logRaw.BlockTimestamp

	// Token Contract Name
	tokenContractName, err := utils.IconNodeServiceGetTokenContractName(tokenContractAddress)
	if err != nil {
		zap.S().Fatal(err)
	}

	return &models.TokenTransfer{
		TokenContractAddress: tokenContractAddress,
		FromAddress:          fromAddress,
		ToAddress:            toAddress,
		Value:                value,
		TransactionHash:      logRaw.TransactionHash,
		LogIndex:             int32(logRaw.LogIndex),
		BlockNumber:          logRaw.BlockNumber,
		ValueDecimal:         valueDecimal,
		BlockTimestamp:       blockTimestamp,
		TokenContractName:    tokenContractName,
		TransactionFee:       "",
	}
}
