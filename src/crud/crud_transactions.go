package crud

import (
	"sync"
	"context"

	"go.uber.org/zap"
	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/models"
)

type TransactionModelMongo struct {
	mongoConn        *MongoConn
	writeChan        chan *models.Transaction
}

var transactionModelMongoInstance *TransactionModelMongo
var transactionModelMongoOnce sync.Once

func GetTransactionModelMongo() *TransactionModelMongo {
	transactionModelMongoOnce.Do(func() {
		transactionModelMongoInstance = &TransactionModelMongo{
			mongoConn:        GetMongoConn(),
			writeChan:        make(chan *models.Transaction, 1),
		}

		transactionModelMongoInstance.CreateIndex("blocknumber", true, false)
	})
	return transactionModelMongoInstance
}

func (b *TransactionModelMongo) getCollectionHandle() *mongo.Collection {
  dbName := config.Config.DbName
  dbCollection := config.Config.DbCollection

  return GetMongoConn().DatabaseHandle(dbName).Collection(dbCollection)
}

func (b *TransactionModelMongo) GetWriteChan() chan *models.Transaction {
	return b.writeChan
}

func (b *TransactionModelMongo) CreateIndex(column string, isAscending bool, isUnique bool) {
	ascending := 1
	if !isAscending {
		ascending = -1
	}
	indexModel := mongo.IndexModel{
		Keys:    bson.M{column: ascending},
		Options: options.Index().SetUnique(isUnique),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.getCollectionHandle().Indexes().CreateOne(ctx, indexModel)
}

func (b *TransactionModelMongo) InsertOne(ctx context.Context, transaction *models.Transaction) (*mongo.InsertOneResult, error) {
	one, err := b.getCollectionHandle().InsertOne(ctx, transaction)
	return one, err
}

func (b *TransactionModelMongo) RetryCreate(ctx context.Context, transaction *models.Transaction) (*mongo.InsertOneResult, error) {
	var insertOneResult *mongo.InsertOneResult
	operation := func() error {
		tx, err := b.InsertOne(ctx, transaction)
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
	ctx context.Context,
	limit int64,
	skip int64,
	from string,
	to string,
	_type string,
) (*[]models.Transaction, error) {
  err := b.mongoConn.retryPing(ctx)
  if err != nil {
    return nil, err
  }

	if limit <= 0 {
		limit = 1
	} else if limit > 100 {
		limit = 100
	}
	if skip < 0 {
		skip = 0
	}

	// Building KeyValue pairs
	queryParams := make(map[string]interface{})
	// from
	if from != "" {
		queryParams["fromaddress"] = from
	}
	// to
	if to != "" {
		queryParams["toaddress"] = to
	}
	// type
	if _type != "" {
		queryParams["type"] = _type
	}

	// Building FindOptions
	opts := options.FindOptions{
		Skip:  &skip,
		Limit: &limit,
	}
  opts.SetSort(bson.D{{"blocknumber", -1}})

	queryParamsD, err := convertMapToBsonD(queryParams)
	if err != nil {
    return nil, err
	}

	cursor, err := b.getCollectionHandle().Find(ctx, queryParamsD, &opts)
	if err != nil {
    return nil, err
	}

	var results []bson.M
  err = cursor.All(ctx, &results)
	if err != nil {
    return nil, err
	}

  // convert bson to model
  transactions := make([]models.Transaction, 0)
  for _, r := range results {
    transactions = append(transactions, convertBsonMToTransaction(r))
  }

	return &transactions, nil
}

func StartTransactionLoader() {
	go transactionLoader()
}

func transactionLoader() {
	var transaction *models.Transaction
	mongoLoaderChan := GetTransactionModelMongo().writeChan

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		transaction = <-mongoLoaderChan
		GetTransactionModelMongo().RetryCreate(ctx, transaction) // inserted here !!
    zap.S().Info("Loader: Loaded in collection Transactions - BlockNumber=", transaction.BlockNumber)
	}
}
