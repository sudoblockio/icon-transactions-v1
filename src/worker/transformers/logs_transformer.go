package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/metrics"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartLogsTransformer() {
	go logsTransformer()
}

func logsTransformer() {
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChannel

	// Output channels
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	tokenTransferLoaderChan := crud.GetTokenTransferModel().LoaderChannel
	transactionWebsocketLoaderChan := crud.GetTransactionWebsocketIndexModel().LoaderChannel
	transactionCountLoaderChan := crud.GetTransactionCountModel().LoaderChannel
	transactionInternalCountByAddressLoaderChan := crud.GetTransactionInternalCountByAddressModel().LoaderChannel
	tokenTransferCountByAddressLoaderChan := crud.GetTokenTransferCountByAddressModel().LoaderChannel
	tokenTransferCountByTokenContractLoaderChan := crud.GetTokenTransferCountByTokenContractModel().LoaderChannel

	zap.S().Debug("Logs Transformer: started working")
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

		/////////////
		// Loaders //
		/////////////

		transaction := transformLogRawToTransaction(logRaw)

		// Internal transaction
		if transaction != nil {

			// Loads to: transactions
			transactionLoaderChan <- transaction

			// Loads to: transaction_websocket_indices
			transactionWebsocket := transformTransactionToTransactionWS(transaction)
			transactionWebsocketLoaderChan <- transactionWebsocket

			// Loads to: transaction_counts
			transactionCountInternal := transformTransactionToTransactionCountInternal(transaction)
			transactionCountLoaderChan <- transactionCountInternal

			// Loads to: transaction_internal_count_by_addresses (from address)
			transactionInternalCountByFromAddress := transformTransactionToTransactionInternalCountByAddress(transaction, true)
			transactionInternalCountByAddressLoaderChan <- transactionInternalCountByFromAddress

			// Loads to: transaction_internal_count_by_addresses (to address)
			transactionInternalCountByToAddress := transformTransactionToTransactionInternalCountByAddress(transaction, false)
			transactionInternalCountByAddressLoaderChan <- transactionInternalCountByToAddress
		}

		// Loads to: token_transfers
		tokenTransfer := transformLogRawToTokenTransfer(logRaw)
		if tokenTransfer != nil {
			tokenTransferLoaderChan <- tokenTransfer

			// Loads to: token_transfers_count
			transactionCountTokenTransfer := transformTokenTransferToTransactionCountTokenTransfer(tokenTransfer)
			transactionCountLoaderChan <- transactionCountTokenTransfer

			// Loads to: token_transfer_by_addresses (from address)
			tokenTransferCountByFromAddress := transformTokenTransferToTokenTransferCountByAddress(tokenTransfer, true)
			tokenTransferCountByAddressLoaderChan <- tokenTransferCountByFromAddress

			// Loads to: token_transfer_by_addresses (to address)
			tokenTransferCountByToAddress := transformTokenTransferToTokenTransferCountByAddress(tokenTransfer, false)
			tokenTransferCountByAddressLoaderChan <- tokenTransferCountByToAddress

			// Loads to: token_transfer_by_token_contract
			tokenTransferCountByTokenContract := transformTokenTransferToTokenTransferCountByTokenContract(tokenTransfer)
			tokenTransferCountByTokenContractLoaderChan <- tokenTransferCountByTokenContract
		}

		/////////////
		// Metrics //
		/////////////
		metrics.MaxBlockNumberLogsRawGauge.Set(float64(logRaw.BlockNumber))
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
	}
}

func transformTransactionToTransactionInternalCountByAddress(tx *models.Transaction, isFromAddress bool) *models.TransactionInternalCountByAddress {

	// Address
	address := ""

	if isFromAddress == true {
		address = tx.FromAddress
	} else {
		address = tx.ToAddress
	}

	return &models.TransactionInternalCountByAddress{
		TransactionHash: tx.Hash,
		LogIndex:        uint64(tx.LogIndex),
		Address:         address,
		Count:           0, // Adds in loader
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

	return &models.TokenTransfer{
		TokenContractAddress: logRaw.Address,
		FromAddress:          indexed[1],
		ToAddress:            indexed[2],
		Value:                indexed[3],
		TransactionHash:      logRaw.TransactionHash,
		LogIndex:             int32(logRaw.LogIndex),
		BlockNumber:          logRaw.BlockNumber,
	}
}

func transformTransactionToTransactionCountInternal(transaction *models.Transaction) *models.TransactionCount {

	return &models.TransactionCount{
		TransactionHash: transaction.Hash,
		LogIndex:        transaction.LogIndex,
		Type:            "internal",
	}
}

func transformTokenTransferToTransactionCountTokenTransfer(tokenTransfer *models.TokenTransfer) *models.TransactionCount {

	return &models.TransactionCount{
		TransactionHash: tokenTransfer.TransactionHash,
		LogIndex:        tokenTransfer.LogIndex,
		Type:            "token_transfer",
	}
}

func transformTokenTransferToTokenTransferCountByAddress(tokenTransfer *models.TokenTransfer, isFromAddress bool) *models.TokenTransferCountByAddress {

	// Address
	address := ""

	if isFromAddress == true {
		address = tokenTransfer.FromAddress
	} else {
		address = tokenTransfer.ToAddress
	}

	return &models.TokenTransferCountByAddress{
		TransactionHash: tokenTransfer.TransactionHash,
		LogIndex:        uint64(tokenTransfer.LogIndex),
		Address:         address,
		Count:           0, // Adds in loader
	}
}

func transformTokenTransferToTokenTransferCountByTokenContract(tokenTransfer *models.TokenTransfer) *models.TokenTransferCountByTokenContract {

	return &models.TokenTransferCountByTokenContract{
		TransactionHash:      tokenTransfer.TransactionHash,
		LogIndex:             uint64(tokenTransfer.LogIndex),
		TokenContractAddress: tokenTransfer.TokenContractAddress,
		Count:                0, // Adds in loader
	}
}
