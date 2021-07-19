package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

var transactionModelMongoInstance *TransactionModelMongo
var transactionModelMongoOnce sync.Once

func GetTransactionModelMongo() *TransactionModelMongo {
	transactionModelMongoOnce.Do(func() {
		transactionModelMongoInstance = &TransactionModelMongo{
			mongoConn:        GetMongoConn(),
			model:            &models.Transaction{},
			collectionHandle: GetMongoConn().DatabaseHandle(config.Config.DbName).Collection(config.Config.DbCollection),
			writeChan:        make(chan *models.Transaction, 1),
		}

		for _, index := range config.Config.DbIndex {
			indexName, _ := transactionModelMongoInstance.CreateIndex(index, true, false)
			zap.S().Info("Created Index: ", indexName)
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

//func (b *TransactionModelMongo) setCollectionHandle(database string, collection string) *mongo.Collection {
//	b.collectionHandle = b.mongoConn.DatabaseHandle(database).Collection(collection)
//	return b.collectionHandle
//}

func (b *TransactionModelMongo) GetCollectionHandle() *mongo.Collection {
	return b.collectionHandle
}

func (b *TransactionModelMongo) CreateIndex(column string, isAscending bool, isUnique bool) (string, error) {
	ascending := 1
	if !isAscending {
		ascending = -1
	}
	indexModel := mongo.IndexModel{
		Keys:    bson.M{column: ascending},
		Options: options.Index().SetUnique(isUnique),
	}
	indexName, err := b.collectionHandle.Indexes().CreateOne(b.mongoConn.ctx, indexModel)
	if err != nil {
		zap.S().Errorf("Unable to create Index: %s, err: %s", column, err.Error())
	}
	return indexName, err
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
	_type string,
) []bson.M {
	transactionsModel := GetTransactionModelMongo()

	_ = b.mongoConn.retryPing()

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
	if _type != "" {
		kvPairs["type"] = _type
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
	// Building FindOptions
	opts := options.FindOptions{
		Skip:  &skip,
		Limit: &limit,
	}

	kvPairsD, err := convertMapToBsonD(kvPairs)
	if err != nil {
		zap.S().Info("Error in converting key value pairs to bson.D, err:", err.Error())
		kvPairsD = &bson.D{}
	}

	result := transactionsModel.FindAll(kvPairsD, &opts)

	zap.S().Debug("Transactions: ", result)
	return result
}

func convertMapToBsonD(v map[string]interface{}) (doc *bson.D, err error) {
	zap.S().Debug("Query before Marshall: ", v)
	data, err := bson.Marshal(v)
	zap.S().Debug("Query in bson: ", string(data))
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)
	return
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

//func (b *TransactionModelMongo) find(kv *KeyValue) (*mongo.Cursor, error) {
//	cursor, err := b.collectionHandle.Find(b.mongoConn.ctx, bson.D{{kv.Key, kv.Value}})
//	return cursor, err
//}

func (b *TransactionModelMongo) FindAll(kvPairsD *bson.D, opts *options.FindOptions) []bson.M {
	cursor, err := b.collectionHandle.Find(b.mongoConn.ctx, kvPairsD, opts)
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
