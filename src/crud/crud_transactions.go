package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-transactions/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"sync"
)

type TransactionModelMongo struct {
	mongoConn *MongoConn
	model     *models.Transaction
	//databaseHandle   *mongo.Database
	collectionHandle *mongo.Collection
	writeChan        chan *models.Transaction
}

type KeyValue struct {
	Key   string
	Value interface{}
}

var transactionModelMongoInstance *TransactionModelMongo
var transactionModelMongoOnce sync.Once

func GetTransactionModelMongo() *TransactionModelMongo {
	transactionModelMongoOnce.Do(func() {
		transactionModelMongoInstance = &TransactionModelMongo{
			mongoConn: GetMongoConn(),
			model:     &models.Transaction{},
			writeChan: make(chan *models.Transaction, 1),
		}
		// TODO: Set from Config var
		err := transactionModelMongoInstance.setCollectionHandle("local", "transactions")
		if err != nil {
			zap.S().Info("Unable to set collection: \"transactions\" in database: \"local\"")
		}
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

func (b *TransactionModelMongo) setCollectionHandle(database string, collection string) *mongo.Collection {
	b.collectionHandle = b.mongoConn.DatabaseHandle(database).Collection(collection)
	return b.collectionHandle
}

func (b *TransactionModelMongo) GetCollectionHandle() *mongo.Collection {
	return b.collectionHandle
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
	limit int,
	skip int,
	from string,
	to string,
	_type string,
) []bson.M {
	transactionsModel := GetTransactionModelMongo() //global.GetGlobal().Transactions

	// TODO: build key-value pairs

	result := transactionsModel.FindAll(&KeyValue{})

	zap.S().Debug("Transactions: ", result)
	return result
}

//
//func (m *BlockModel) Select(
//	limit         int,
//	skip          int,
//	number        uint32,
//	start_number  uint32,
//	end_number    uint32,
//	hash          string,
//	created_by    string,
//) (*[]models.Block) {
//	db := m.db
//
//	// Limit is required and defaulted to 1
//	db = db.Limit(limit)
//
//	// Skip
//	if skip != 0 {
//		db = db.Offset(skip)
//	}
//
//	// Height
//	if number != 0 {
//		db = db.Where("number = ?", number)
//	}
//
//	// Start number and end number
//	if start_number != 0 && end_number != 0 {
//		db = db.Where("number BETWEEN ? AND ?", start_number, end_number)
//	} else if start_number != 0 {
//		db = db.Where("number > ?", start_number)
//	} else if end_number != 0 {
//		db = db.Where("number < ?", end_number)
//	}
//
//	// Hash
//	if hash != "" {
//		db = db.Where("hash = ?", hash)
//	}
//
//	// Created By
//	if created_by != "" {
//		db = db.Where("created_by = ?", created_by)
//	}
//
//	blocks := &[]models.Block{}
//	db.Find(blocks)
//
//	return blocks
//}

func (b *TransactionModelMongo) find(kv *KeyValue) (*mongo.Cursor, error) {
	cursor, err := b.collectionHandle.Find(b.mongoConn.ctx, bson.D{{kv.Key, kv.Value}})
	return cursor, err
}

func (b *TransactionModelMongo) FindAll(kv *KeyValue) []bson.M {
	cursor, err := b.find(kv)
	if err != nil {
		zap.S().Info("Exception in getting a curser to a find in mongodb: ", err)
	}
	var results []bson.M
	if err = cursor.All(b.mongoConn.ctx, &results); err != nil {
		zap.S().Info("Exception in find all: ", err)
	}
	return results

}

func StartTransactionLoader() {
	go transactionLoader()
}

func transactionLoader() {
	var transaction *models.Transaction
	mongoLoaderChan := GetTransactionModelMongo().writeChan
	for {
		transaction = <-mongoLoaderChan
		GetTransactionModelMongo().RetryCreate(transaction) // inserted here !!
		zap.S().Debug("Loader Transaction: Loaded in postgres table Transactions, Block Number", transaction.BlockNumber)
	}
}
