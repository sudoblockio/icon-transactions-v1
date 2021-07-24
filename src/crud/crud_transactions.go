package crud

import (
	"sync"

	"go.uber.org/zap"
	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/models"
)

type TransactionModelMongo struct {
	mongoConn *MongoConn
	model     *models.Transaction
	//databaseHandle   *mongo.Database
	collectionHandle *mongo.Collection
	writeChan        chan *models.Transaction
}

var transactionModelMongoInstance *TransactionModelMongo
var transactionModelMongoOnce sync.Once

var collectionName string = "transactions"

func GetTransactionModelMongo() *TransactionModelMongo {
	transactionModelMongoOnce.Do(func() {
		transactionModelMongoInstance = &TransactionModelMongo{
			mongoConn:        GetMongoConn(),
			model:            &models.Transaction{},
			collectionHandle: GetMongoConn().DatabaseHandle(config.Config.DbName).Collection(collectionName),
			writeChan:        make(chan *models.Transaction, 1),
		}

    // Create indexes
		transactionModelMongoInstance.CreateIndex("from_address", true, false)
	  transactionModelMongoInstance.CreateIndex("to_address", true, false)
		transactionModelMongoInstance.CreateIndex("method", true, false)
	})
	return transactionModelMongoInstance
}

func (b *TransactionModelMongo) GetMongoConn() *MongoConn {
	return b.mongoConn
}

func (b *TransactionModelMongo) GetModel() *models.Transaction {
	return b.model
}

func (b *TransactionModelMongo) GetWriteChan() chan *models.Transaction {
	return b.writeChan
}

func (b *TransactionModelMongo) GetCollectionHandle() *mongo.Collection {
	return b.collectionHandle
}

func (b *TransactionModelMongo) CreateIndex(column string, isAscending bool, isUnique bool){

	ascending := 1
	if !isAscending {
		ascending = -1
	}

	indexModel := mongo.IndexModel{
		Keys:    bson.M{column: ascending},
		Options: options.Index().SetUnique(isUnique),
	}

	_, err := b.collectionHandle.Indexes().CreateOne(b.mongoConn.ctx, indexModel)
	if err != nil {
		zap.S().Errorf("Unable to create Index: %s, err: %s", column, err.Error())
	}
}

func (b *TransactionModelMongo) InsertOne(block *models.Transaction) (*mongo.InsertOneResult, error) {
	one, err := b.collectionHandle.InsertOne(b.mongoConn.ctx, block)
	return one, err
}

func (b *TransactionModelMongo) RetryCreate(transaction *models.Transaction) (*mongo.InsertOneResult, error) {
	var insertOneResult *mongo.InsertOneResult
	operation := func() error {

		tx, err := b.InsertOne(transaction)
		if err != nil {
			zap.S().Info("MongoDb RetryCreate Error : ", err.Error())
		} else {
			insertOneResult = tx
			return nil
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)
	return insertOneResult, err
}

func (b *TransactionModelMongo) Select(
	limit int64,
	skip int64,
	from string,
	to string,
	txType string,
) []bson.M {
	b.mongoConn.retryPing()

	// Building KeyValue pairs
	kvPairs := make(map[string]interface{})
	// from
	if from != "" {
		kvPairs["fromaddress"] = from
	}
	// to
	if to != "" {
		kvPairs["toaddress"] = to
	}
	// type
	if txType != "" {
		kvPairs["type"] = txType
	}
	// limit
	if limit <= 0 {
		limit = 1
	} else if limit > 100 {
		limit = 100
	}
	// skip
	if skip < 0 {
		skip = 0
	}

  // TODO make query
	result := []bson.M{}

	return result
}

func convertMapToBsonD(v map[string]interface{}) (doc *bson.D, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)
	return
}

func StartTransactionLoader() {
	go func() {
    var transaction *models.Transaction
    mongoLoaderChan := GetTransactionModelMongo().writeChan
    for {
      transaction = <-mongoLoaderChan
      GetTransactionModelMongo().RetryCreate(transaction) // inserted here !!
      zap.S().Debug("Loader Transaction: Loaded in postgres table Transactions, Block Number", transaction.BlockNumber)
    }
  }()
}
