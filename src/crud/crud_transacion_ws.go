package crud

import (
	"encoding/json"
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/redis"
)

// TransactionWebsocketIndexModel - type for transactionWebsocketIndex table model
type TransactionWebsocketIndexModel struct {
	db        *gorm.DB
	model     *models.TransactionWebsocketIndex
	modelORM  *models.TransactionWebsocketIndexORM
	WriteChan chan *models.TransactionWebsocket // Write TransactionWebsocket to create a TransactionWebsocketIndex
}

var transactionWebsocketIndexModel *TransactionWebsocketIndexModel
var transactionWebsocketIndexModelOnce sync.Once

// GetTransactionWebsocketIndexModel - create and/or return the transactionWebsocketIndexs table model
func GetTransactionWebsocketIndexModel() *TransactionWebsocketIndexModel {
	transactionWebsocketIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionWebsocketIndexModel = &TransactionWebsocketIndexModel{
			db:        dbConn,
			model:     &models.TransactionWebsocketIndex{},
			WriteChan: make(chan *models.TransactionWebsocket, 1),
		}

		err := transactionWebsocketIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionWebsocketIndexModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionWebsocketIndexLoader()
	})

	return transactionWebsocketIndexModel
}

// Migrate - migrate transactionWebsocketIndexs table
func (m *TransactionWebsocketIndexModel) Migrate() error {
	// Only using TransactionWebsocketIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionWebsocketIndex into table
func (m *TransactionWebsocketIndexModel) Insert(transactionWebsocketIndex *models.TransactionWebsocketIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionWebsocketIndex{})

	db = db.Create(transactionWebsocketIndex)

	return db.Error
}

func (m *TransactionWebsocketIndexModel) SelectOne(
	hash string,
) (*models.TransactionWebsocketIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionWebsocketIndex{})

	db = db.Where("hash = ?", hash)

	transactionWebsocketIndex := &models.TransactionWebsocketIndex{}
	db = db.First(transactionWebsocketIndex)

	return transactionWebsocketIndex, db.Error
}

// StartTransactionWebsocketIndexLoader starts loader
func StartTransactionWebsocketIndexLoader() {
	go func() {

		for {
			// Read transaction
			newTransactionWebsocket := <-GetTransactionWebsocketIndexModel().WriteChan

			// TransactionWebsocket -> TransactionWebsocketIndex
			newTransactionWebsocketIndex := &models.TransactionWebsocketIndex{
				Hash: newTransactionWebsocket.Hash,
			}

			// Update/Insert
			_, err := GetTransactionWebsocketIndexModel().SelectOne(newTransactionWebsocketIndex.Hash)
			if errors.Is(err, gorm.ErrRecordNotFound) {

				// Insert
				GetTransactionWebsocketIndexModel().Insert(newTransactionWebsocketIndex)

				// Publish to redis
				newTransactionWebsocketJSON, _ := json.Marshal(newTransactionWebsocket)
				redis.GetRedisClient().Publish(newTransactionWebsocketJSON)
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
