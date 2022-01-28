package transformers

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/metrics"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
)

func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumer.TopicChannels[consumerTopicNameTransactions]

	// Output channels
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	transactionCreateScoreLoaderChan := crud.GetTransactionCreateScoreModel().LoaderChannel
	transactionWebsocketLoaderChan := crud.GetTransactionWebsocketIndexModel().LoaderChannel
	//transactionCountLoaderChan := crud.GetTransactionCountModel().LoaderChannel
	transactionCountByAddressLoaderChan := crud.GetTransactionCountByAddressModel().LoaderChannel

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

		// Loads to: transaction_contract_creations
		transactionCreateScore := transformTransactionToTransactionCreateScore(transaction)
		if transactionCreateScore != nil {
			transactionCreateScoreLoaderChan <- transactionCreateScore
		}

		// Loads to: transaction_websocket_indices
		transactionWebsocket := transformTransactionToTransactionWS(transaction)
		transactionWebsocketLoaderChan <- transactionWebsocket

		// Loads to: transaction_counts
		//transactionCount := transformTransactionToTransactionCount(transaction)
		//transactionCountLoaderChan <- transactionCount

		// Loads to: transaction_count_by_addresses (from address)
		transactionCountByFromAddress := transformTransactionToTransactionCountByAddress(transaction, true)
		if transactionCountByFromAddress != nil {
			transactionCountByAddressLoaderChan <- transactionCountByFromAddress
		}

		// Loads to: transaction_count_by_addresses (to address)
		transactionCountByToAddress := transformTransactionToTransactionCountByAddress(transaction, false)
		if transactionCountByToAddress != nil {
			transactionCountByAddressLoaderChan <- transactionCountByToAddress
		}

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

	// Is contract creation transaction?
	if txRaw.ToAddress == "cx0000000000000000000000000000000000000000" &&
		txRaw.ReceiptScoreAddress != "None" {
		txRaw.ToAddress = txRaw.ReceiptScoreAddress

		// NOTE method used by crud transactions loader
		method = "_create_contract_"
	}

	// Transaction fee calculation
	// Use big int
	// NOTE: transaction fees, once calculated (price*used) may be too large for postgres
	receiptStepPriceBig := big.NewInt(int64(txRaw.ReceiptStepPrice))
	receiptStepUsedBig := big.NewInt(int64(txRaw.ReceiptStepUsed))
	transactionFeesBig := receiptStepUsedBig.Mul(receiptStepUsedBig, receiptStepPriceBig)

	// to hex
	transactionFee := fmt.Sprintf("0x%x", transactionFeesBig)

	// Transaction Decimal Value
	// Hex -> float64
	valueDecimal := utils.StringHexToFloat64(txRaw.Value, 18)

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
		TransactionFee:            transactionFee,
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
		ValueDecimal:              valueDecimal,
	}
}

func transformTransactionToTransactionCreateScore(tx *models.Transaction) *models.TransactionCreateScore {

	if !(tx.Method == "acceptScore" || tx.Method == "rejectScore") {
		// Not contract accept/reject transaction
		return nil
	}

	// Accept Transaction Hash
	acceptTransactionHash := ""
	if tx.Method == "acceptScore" {
		acceptTransactionHash = tx.Hash
	}

	// Reject Transaction Hash
	rejectTransactionHash := ""
	if tx.Method == "rejectScore" {
		rejectTransactionHash = tx.Hash
	}

	// Creation Transaction Hash
	creationTransactionHash := ""
	if tx.Data != "" {
		dataJSON := map[string]interface{}{}
		err := json.Unmarshal([]byte(tx.Data), &dataJSON)
		if err == nil {

			paramsInterface, ok := dataJSON["params"]
			if ok {
				// Params field is in dataJSON
				params := paramsInterface.(map[string]interface{})

				creationTransactionHashInterface, ok := params["txHash"]
				if ok {
					// Parsing successful
					creationTransactionHash = creationTransactionHashInterface.(string)
				}
			}
			if ok == false {
				// Parsing error
				zap.S().Fatal("Transaction params field parsing error: ", err.Error(), ",Hash=", tx.Hash)
			}
		} else {
			// Parsing error
			zap.S().Fatal("Transaction data field parsing error: ", err.Error(), ",Hash=", tx.Hash)
		}
	}

	return &models.TransactionCreateScore{
		CreationTransactionHash: creationTransactionHash,
		AcceptTransactionHash:   acceptTransactionHash,
		RejectTransactionHash:   rejectTransactionHash,
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
		TransactionFee:            tx.TransactionFee,
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
		Type:            "regular",
	}
}

func transformTransactionToTransactionCountByAddress(tx *models.Transaction, isFromAddress bool) *models.TransactionCountByAddress {

	// Address
	address := ""

	if isFromAddress == true {
		address = tx.FromAddress
	} else {
		address = tx.ToAddress
	}
	if address == "None" {
		return nil
	}

	return &models.TransactionCountByAddress{
		TransactionHash: tx.Hash,
		Address:         address,
		Count:           0, // Adds in loader
		BlockNumber:     tx.BlockNumber,
	}
}
