package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/models"
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
			zap.S().Info("Unable to set collection: \"transactions\" in database: \"icon\"")
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

func (b *TransactionModelMongo) DeleteMany(kv *KeyValue) (*mongo.DeleteResult, error) {
	delR, err := b.collectionHandle.DeleteMany(b.mongoConn.ctx, bson.D{{kv.Key, kv.Value}})
	return delR, err
}

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
