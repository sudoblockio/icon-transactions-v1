package transformers

import (
	"encoding/hex"
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/metrics"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumers[consumerTopicNameTransactions].TopicChannel

	// Output channels
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	transactionWebsocketLoaderChan := crud.GetTransactionWebsocketIndexModel().LoaderChannel
	transactionCountLoaderChan := crud.GetTransactionCountModel().LoaderChannel

	zap.S().Debug("Transactions Transformer: started working")
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

		/////////////
		// Loaders //
		/////////////

		// Loads to: transactions
		transaction := transformTransactionRawToTransaction(transactionRaw)
		transactionLoaderChan <- transaction

		// Loads to: transaction_websocket_indices
		transactionWebsocket := transformTransactionToTransactionWS(transaction)
		transactionWebsocketLoaderChan <- transactionWebsocket

		// Loads to: transaction_counts
		transactionCount := transformTransactionToTransactionCount(transaction)
		transactionCountLoaderChan <- transactionCount

		/////////////
		// Metrics //
		/////////////
		metrics.MaxBlockNumberTransactionsRawGauge.Set(float64(transactionRaw.BlockNumber))
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
func transformTransactionRawToTransaction(txRaw *models.TransactionRaw) *models.Transaction {

	// Method
	method := ""
	if txRaw.Data != "" {
		dataJSON := map[string]interface{}{}
		err := json.Unmarshal([]byte(txRaw.Data), &dataJSON)
		if err == nil {
			// Parsing successful
			if methodInterface, ok := dataJSON["method"]; ok {
				// Method field is in dataJSON
				method = methodInterface.(string)
			}
		} else {
			// Parsing error
			zap.S().Warn("Transaction data field parsing error: ", err.Error(), ",Hash=", txRaw.Hash)
		}
	}

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
		LogIndex:                  -1,
		Method:                    method,
	}
}

// Business logic goes here
func transformTransactionToTransactionWS(tx *models.Transaction) *models.TransactionWebsocket {

	return &models.TransactionWebsocket{
		FromAddress:               tx.FromAddress,
		ToAddress:                 tx.ToAddress,
		Value:                     tx.Value,
		StepLimit:                 tx.StepLimit,
		BlockTimestamp:            tx.BlockTimestamp,
		Nonce:                     tx.Nonce,
		Hash:                      tx.Hash,
		TransactionIndex:          tx.TransactionIndex,
		BlockHash:                 tx.BlockHash,
		BlockNumber:               tx.BlockNumber,
		Fee:                       tx.Fee,
		Signature:                 tx.Signature,
		DataType:                  tx.DataType,
		Data:                      tx.Data,
		ReceiptCumulativeStepUsed: tx.ReceiptCumulativeStepUsed,
		ReceiptStepUsed:           tx.ReceiptStepUsed,
		ReceiptStepPrice:          tx.ReceiptStepPrice,
		ReceiptScoreAddress:       tx.ReceiptScoreAddress,
		ReceiptLogs:               tx.ReceiptLogs,
		ReceiptStatus:             tx.ReceiptStatus,
		Method:                    tx.Method,
	}
}

func transformTransactionToTransactionCount(tx *models.Transaction) *models.TransactionCount {

	return &models.TransactionCount{
		TransactionHash: tx.Hash,
		LogIndex:        tx.LogIndex,
	}
}
