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

type TransactionModel struct {
	mongoConn        *MongoConn
	WriteChan        chan *models.Transaction
}

var transactionModelInstance *TransactionModel
var transactionModelOnce sync.Once

func GetTransactionModel() *TransactionModel {
	transactionModelOnce.Do(func() {
		transactionModelInstance = &TransactionModel{
			mongoConn:        GetMongoConn(),
			WriteChan:        make(chan *models.Transaction, 1),
		}

		transactionModelInstance.CreateNumberIndex("blocknumber", false, false)
		transactionModelInstance.CreateStringIndex("hash")
		transactionModelInstance.CreateStringIndex("fromaddress")
		transactionModelInstance.CreateStringIndex("toaddress")
	})
	return transactionModelInstance
}

func (b *TransactionModel) getCollectionHandle() *mongo.Collection {
  dbName := config.Config.DbName
  dbCollection := config.Config.DbCollection

  return GetMongoConn().DatabaseHandle(dbName).Collection(dbCollection)
}

func (b *TransactionModel) CreateNumberIndex(field string, isAscending bool, isUnique bool) {
	ascending := 1
	if !isAscending {
		ascending = -1
	}

	indexModel := mongo.IndexModel{
		Keys:    bson.M{field: ascending},
		Options: options.Index().SetUnique(isUnique),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := b.getCollectionHandle().Indexes().CreateOne(ctx, indexModel)
  if err != nil {
    zap.S().Fatal("CREATENUMBERINDEX PANIC: ", err.Error())
  }
}

func (b *TransactionModel) CreateStringIndex(field string) {

	indexModel := mongo.IndexModel{
		Keys:    bson.M{field: "hashed"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := b.getCollectionHandle().Indexes().CreateOne(ctx, indexModel)
  if err != nil {
    zap.S().Fatal("CREATESTRINGINDEX PANIC: ", err.Error())
  }
}

func (b *TransactionModel) Insert(ctx context.Context, transaction *models.Transaction) error {

  err := backoff.Retry(func() error {
	  _, err := b.getCollectionHandle().InsertOne(ctx, transaction)

		if err != nil {
			zap.S().Info("MongoDb RetryCreate Error : ", err.Error())
		}

    return err
	}, backoff.NewExponentialBackOff())

	return err
}

func (b *TransactionModel) Select(
	ctx context.Context,
	limit int64,
	skip int64,
	hash string,
	from string,
	to string,
) ([]models.Transaction, error) {
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
	// hash
	if hash != "" {
		queryParams["hash"] = hash
	}
	// from
	if from != "" {
		queryParams["fromaddress"] = from
	}
	// to
	if to != "" {
		queryParams["toaddress"] = to
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

	return transactions, nil
}

func StartTransactionLoader() {

  go func() {
    var transaction *models.Transaction
    mongoLoaderChan := GetTransactionModel().WriteChan

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    for {
      transaction = <-mongoLoaderChan
      GetTransactionModel().Insert(ctx, transaction)

      zap.S().Info("Loader: Loaded in collection Transactions - BlockNumber=", transaction.BlockNumber)
    }
  }()
}
