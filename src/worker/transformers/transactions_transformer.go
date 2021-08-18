package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
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
	consumer_topic_name_transactions := "transactions"
	consumer_topic_name_logs := "logs"
	producer_topic_name := "transactions-ws"

	// Check topic names
	if utils.StringInSlice(consumer_topic_name_transactions, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", consumer_topic_name_transactions, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(consumer_topic_name_logs, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", consumer_topic_name_logs, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(producer_topic_name, config.Config.ProducerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", producer_topic_name, " topic found in PRODUCER_TOPICS=", config.Config.ConsumerTopics)
	}

	consumer_topic_chan_transactions := make(chan *sarama.ConsumerMessage)
	consumer_topic_chan_logs := make(chan *sarama.ConsumerMessage)
	producer_topic_chan := kafka.KafkaTopicProducers[producer_topic_name].TopicChan
	transactionLoaderChan := crud.GetTransactionModel().WriteChan
	transactionCountLoaderChan := crud.GetTransactionCountModel().WriteChan

	// Register consumer channel transactions
	broadcaster_output_chan_id_transaction := kafka.Broadcasters[consumer_topic_name_transactions].AddBroadcastChannel(consumer_topic_chan_transactions)
	defer func() {
		kafka.Broadcasters[consumer_topic_name_transactions].RemoveBroadcastChannel(broadcaster_output_chan_id_transaction)
	}()

	broadcaster_output_chan_id_log := kafka.Broadcasters[consumer_topic_name_logs].AddBroadcastChannel(consumer_topic_chan_logs)
	defer func() {
		kafka.Broadcasters[consumer_topic_name_transactions].RemoveBroadcastChannel(broadcaster_output_chan_id_log)
	}()

	zap.S().Debug("Transactions Worker: started working")
	for {
		// Read from kafka
		var consumer_topic_msg *sarama.ConsumerMessage
		var transaction *models.Transaction

		select {
		case consumer_topic_msg = <-consumer_topic_chan_transactions:
			// Transaction message from ETL
			transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumer_topic_msg.Value)
			if err != nil {
				zap.S().Fatal("Transactions Worker: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
			}

			// Transform logic
			transaction = transformTransactionRaw(transactionRaw)
		case consumer_topic_msg = <-consumer_topic_chan_logs:
			// Log message from ETL
			logRaw, err := convertBytesToLogRawProtoBuf(consumer_topic_msg.Value)
			if err != nil {
				zap.S().Fatal("Unable to proceed cannot convert kafka msg value to LogRaw, err: ", err.Error())
			}

			transaction = transformLogRaw(logRaw)

			// Not and internal transaction
			if transaction == nil {
				continue
			}
		}

		// Produce to Kafka
		producer_topic_msg := &sarama.ProducerMessage{
			Topic: producer_topic_name,
			Key:   sarama.ByteEncoder(consumer_topic_msg.Key),
			Value: sarama.ByteEncoder(consumer_topic_msg.Value),
		}

		producer_topic_chan <- producer_topic_msg

		// Load to Postgres
		transactionLoaderChan <- transaction

		// Load log counter to Postgres
		transactionCount := &models.TransactionCount{
			Count: 1, // Adds with current
			Id:    1, // Only one row
		}
		transactionCountLoaderChan <- transactionCount

		zap.S().Debug("Transactions worker: last seen transaction #", string(consumer_topic_msg.Key))
	}
}

func convertBytesToTransactionRawJSON(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}

	err := protojson.Unmarshal(value, &tx)
	if err != nil {
		zap.S().Panic("Error: ", err.Error(), " Value: ", string(value))
	}

	return &tx, nil
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

func convertBytesToLogRawJSON(value []byte) (*models.LogRaw, error) {
	log := models.LogRaw{}

	err := protojson.Unmarshal(value, &log)
	if err != nil {
		zap.S().Panic("Error: ", err.Error(), " Value: ", string(value))
	}

	return &log, nil
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
		Nonce:                     0,
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
	}
}
