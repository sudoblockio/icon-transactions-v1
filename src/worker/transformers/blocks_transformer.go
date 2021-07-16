package transformers

import (
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/Shopify/sarama.v1"
)

func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumer_topic_name := "transactions"
	producer_topic_name := "transactions-ws"

	// TODO: Need to move all of the config validations to config.go
	// Check topic names
	if utils.StringInSlice(consumer_topic_name, config.Config.ConsumerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", consumer_topic_name, " topic found in CONSUMER_TOPICS=", config.Config.ConsumerTopics)
	}
	if utils.StringInSlice(producer_topic_name, config.Config.ProducerTopics) == false {
		zap.S().Panic("Transactions Worker: no ", producer_topic_name, " topic found in PRODUCER_TOPICS=", config.Config.ConsumerTopics)
	}

	consumer_topic_chan := make(chan *sarama.ConsumerMessage)
	producer_topic_chan := kafka.KafkaTopicProducers[producer_topic_name].TopicChan
	mongoLoaderChan := global.GetGlobal().Transactions.GetWriteChan()

	// Register consumer channel
	broadcaster_output_chan_id := kafka.Broadcasters[consumer_topic_name].AddBroadcastChannel(consumer_topic_chan)
	defer func() {
		kafka.Broadcasters[consumer_topic_name].RemoveBroadcastChannel(broadcaster_output_chan_id)
	}()

	zap.S().Debug("Transactions Worker: started working")
	for {
		// Read from kafka
		consumer_topic_msg := <-consumer_topic_chan
		transactionRaw, err := ConvertBytesToTransactionRaw(consumer_topic_msg.Value)
		if err != nil {
			zap.S().Error("Transactions Worker: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		// Transform logic
		transformedTransaction, _ := transform(transactionRaw)

		// Produce to Kafka
		producer_topic_msg := &sarama.ProducerMessage{
			Topic: producer_topic_name,
			Key:   sarama.ByteEncoder(consumer_topic_msg.Key),
			Value: sarama.ByteEncoder(consumer_topic_msg.Value),
		}

		producer_topic_chan <- producer_topic_msg

		// Load to Postgres
		mongoLoaderChan <- transformedTransaction

		zap.S().Debug("Transactions worker: last seen block #", string(consumer_topic_msg.Key))
	}
}

// Business logic goes here
func transform(txRaw *models.TransactionRaw) (*models.Transaction, error) {
	//time.Sleep(time.Minute)
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
	}, nil
}

func ConvertBytesToTransactionRaw(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}
	err := protojson.Unmarshal(value, &tx)
	if err != nil {
		zap.S().Error("Transaction_raw_helper: Error in ConvertBytesToTransactionRaw: %v", err)
	}
	return &tx, err
}
